/*
 * MoveKeys.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flow/Util.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/MoveKeys.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

using std::min;
using std::max;

ACTOR Future<MoveKeysLock> takeMoveKeysLock( Database cx, UID masterId ) {
	state Transaction tr(cx);
	loop {
		try {
			state MoveKeysLock lock;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			if( !g_network->isSimulated() ) {
				UID id(g_random->randomUniqueID());
				TraceEvent("TakeMoveKeysLockTransaction", masterId)
					.detail("TransactionUID", id);
				tr.debugTransaction( id );
			}
			Optional<Value> readVal = wait( tr.get( moveKeysLockOwnerKey ) );
			lock.prevOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			Optional<Value> readVal = wait( tr.get( moveKeysLockWriteKey ) );
			lock.prevWrite = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			lock.myOwner = g_random->randomUniqueID();
			return lock;
		} catch (Error &e){
			wait(tr.onError(e));
			TEST(true);  // takeMoveKeysLock retry
		}
	}
}

ACTOR Future<Void> checkMoveKeysLock( Transaction* tr, MoveKeysLock lock, bool isWrite = true ) {
	Optional<Value> readVal = wait( tr->get( moveKeysLockOwnerKey ) );
	UID currentOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();

	if (currentOwner == lock.prevOwner) {
		// Check that the previous owner hasn't touched the lock since we took it
		Optional<Value> readVal = wait( tr->get( moveKeysLockWriteKey ) );
		UID lastWrite = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
		if (lastWrite != lock.prevWrite) {
			TEST(true);  // checkMoveKeysLock: Conflict with previous owner
			throw movekeys_conflict();
		}

		// Take the lock
		if(isWrite) {
			BinaryWriter wrMyOwner(Unversioned()); wrMyOwner << lock.myOwner;
			tr->set( moveKeysLockOwnerKey, wrMyOwner.toStringRef() );
			BinaryWriter wrLastWrite(Unversioned()); wrLastWrite << g_random->randomUniqueID();
			tr->set( moveKeysLockWriteKey, wrLastWrite.toStringRef() );
		}

		return Void();
	} else if (currentOwner == lock.myOwner) {
		if(isWrite) {
			// Touch the lock, preventing overlapping attempts to take it
			BinaryWriter wrLastWrite(Unversioned()); wrLastWrite << g_random->randomUniqueID();
			tr->set( moveKeysLockWriteKey, wrLastWrite.toStringRef() );
			// Make this transaction self-conflicting so the database will not execute it twice with the same write key
			tr->makeSelfConflicting();
		}

		return Void();
	} else {
		TEST(true);  // checkMoveKeysLock: Conflict with new owner
		throw movekeys_conflict();
	}
}

Future<Void> checkMoveKeysLockReadOnly( Transaction* tr, MoveKeysLock lock ) {
	return checkMoveKeysLock(tr, lock, false);
}

ACTOR Future<Optional<UID>> checkReadWrite( Future< ErrorOr<std::pair<Version,Version>> > fReply, UID uid, Version version ) {
	ErrorOr<std::pair<Version,Version>> reply = wait( fReply );
	if (!reply.present() || reply.get().first < version)
		return Optional<UID>();
	return Optional<UID>(uid);
}

Future<Void> removeOldDestinations(Transaction *tr, UID oldDest, VectorRef<KeyRangeRef> shards, KeyRangeRef currentKeys) {
	KeyRef beginKey = currentKeys.begin;

	vector<Future<Void>> actors;
	for(int i = 0; i < shards.size(); i++) {
		if(beginKey < shards[i].begin)
			actors.push_back(krmSetRangeCoalescing(tr, serverKeysPrefixFor(oldDest), KeyRangeRef(beginKey, shards[i].begin), allKeys, serverKeysFalse));

		beginKey = shards[i].end;
	}

	if(beginKey < currentKeys.end)
		actors.push_back(krmSetRangeCoalescing(tr, serverKeysPrefixFor(oldDest), KeyRangeRef(beginKey, currentKeys.end), allKeys, serverKeysFalse));

	return waitForAll(actors);
}

ACTOR Future<vector<UID>> addReadWriteDestinations(KeyRangeRef shard, vector<StorageServerInterface> srcInterfs, vector<StorageServerInterface> destInterfs, Version version, int desiredHealthy, int maxServers) {
	if(srcInterfs.size() >= maxServers) {
		return vector<UID>();
	}

	state vector< Future<Optional<UID>> > srcChecks;
	for(int s=0; s<srcInterfs.size(); s++) {
		srcChecks.push_back( checkReadWrite( srcInterfs[s].getShardState.getReplyUnlessFailedFor( GetShardStateRequest( shard, GetShardStateRequest::NO_WAIT), SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL, 0, TaskMoveKeys ), srcInterfs[s].id(), 0 ) );
	}

	state vector< Future<Optional<UID>> > destChecks;
	for(int s=0; s<destInterfs.size(); s++) {
		destChecks.push_back( checkReadWrite( destInterfs[s].getShardState.getReplyUnlessFailedFor( GetShardStateRequest( shard, GetShardStateRequest::NO_WAIT), SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL, 0, TaskMoveKeys ), destInterfs[s].id(), version ) );
	}

	wait( waitForAll(srcChecks) && waitForAll(destChecks) );

	int healthySrcs = 0;
	for(auto it : srcChecks) {
		if( it.get().present() ) {
			healthySrcs++;
		}
	}

	vector<UID> result;
	int totalDesired = std::min<int>(desiredHealthy - healthySrcs, maxServers - srcInterfs.size());
	for(int s = 0; s < destInterfs.size() && result.size() < totalDesired; s++) {
		if(destChecks[s].get().present()) {
			result.push_back(destChecks[s].get().get());
		}
	}

	return result;
}

ACTOR Future<vector<vector<UID>>> additionalSources(Standalone<RangeResultRef> shards, Transaction* tr, int desiredHealthy, int maxServers) {
	vector<Future<Optional<Value>>> serverListEntries;
	std::set<UID> fetching;
	for(int i = 0; i < shards.size() - 1; ++i) {
		vector<UID> src;
		vector<UID> dest;

		decodeKeyServersValue( shards[i].value, src, dest );

		for(int s=0; s<src.size(); s++) {
			if(!fetching.count(src[s])) {
				fetching.insert(src[s]);
				serverListEntries.push_back( tr->get( serverListKeyFor(src[s]) ) );
			}
		}

		for(int s=0; s<dest.size(); s++) {
			if(!fetching.count(dest[s])) {
				fetching.insert(dest[s]);
				serverListEntries.push_back( tr->get( serverListKeyFor(dest[s]) ) );
			}
		}
	}

	vector<Optional<Value>> serverListValues = wait( getAll(serverListEntries) );

	std::map<UID, StorageServerInterface> ssiMap;
	for(int s=0; s<serverListValues.size(); s++) {
		auto si = decodeServerListValue(serverListValues[s].get());
		StorageServerInterface ssi = decodeServerListValue(serverListValues[s].get());
		ssiMap[ssi.id()] = ssi;
	}

	vector<Future<vector<UID>>> allChecks;
	for(int i = 0; i < shards.size() - 1; ++i) {
		KeyRangeRef rangeIntersectKeys( shards[i].key, shards[i+1].key );
		vector<UID> src;
		vector<UID> dest;
		vector<StorageServerInterface> srcInterfs;
		vector<StorageServerInterface> destInterfs;

		decodeKeyServersValue( shards[i].value, src, dest );

		for(int s=0; s<src.size(); s++) {
			srcInterfs.push_back( ssiMap[src[s]] );
		}

		for(int s=0; s<dest.size(); s++) {
			if( std::find(src.begin(), src.end(), dest[s]) == dest.end() ) {
				destInterfs.push_back( ssiMap[dest[s]] );
			}
		}

		allChecks.push_back(addReadWriteDestinations(rangeIntersectKeys, srcInterfs, destInterfs, tr->getReadVersion().get(), desiredHealthy, maxServers));
	}

	vector<vector<UID>> result = wait(getAll(allChecks));
	return result;
}

// Set keyServers[keys].dest = servers
// Set serverKeys[servers][keys] = active for each subrange of keys that the server did not already have, complete for each subrange that it already has
// Set serverKeys[dest][keys] = "" for the dest servers of each existing shard in keys (unless that destination is a member of servers OR if the source list is sufficiently degraded)
ACTOR Future<Void> startMoveKeys( Database occ, KeyRange keys, vector<UID> servers, MoveKeysLock lock, FlowLock *startMoveKeysLock, UID relocationIntervalId ) {
	state TraceInterval interval("RelocateShard_StartMoveKeys");
	//state TraceInterval waitInterval("");

	wait( startMoveKeysLock->take( TaskDataDistributionLaunch ) );
	state FlowLock::Releaser releaser( *startMoveKeysLock );

	TraceEvent(SevDebug, interval.begin(), relocationIntervalId);

	try {
		state Key begin = keys.begin;
		state int batches = 0;
		state int shards = 0;
		state int maxRetries = 0;

		//This process can be split up into multiple transactions if there are too many existing overlapping shards
		//In that case, each iteration of this loop will have begin set to the end of the last processed shard
		while(begin < keys.end) {
			TEST(begin > keys.begin); //Multi-transactional startMoveKeys
			batches++;

			state Transaction tr( occ );
			state int retries = 0;

			loop {
				try {
					retries++;

					//Keep track of old dests that may need to have ranges removed from serverKeys
					state std::set<UID> oldDests;

					//Keep track of shards for all src servers so that we can preserve their values in serverKeys
					state Map<UID, VectorRef<KeyRangeRef>> shardMap;

					tr.info.taskID = TaskMoveKeys;
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					wait( checkMoveKeysLock(&tr, lock) );

					vector< Future< Optional<Value> > > serverListEntries;
					for(int s=0; s<servers.size(); s++)
						serverListEntries.push_back( tr.get( serverListKeyFor(servers[s]) ) );
					state vector<Optional<Value>> serverListValues = wait( getAll(serverListEntries) );

					for(int s=0; s<serverListValues.size(); s++) {
						if (!serverListValues[s].present()) {
							// Attempt to move onto a server that isn't in serverList (removed or never added to the database)
							// This can happen (why?) and is handled by the data distribution algorithm
							TEST(true); //start move keys moving to a removed server
							throw move_to_removed_server();
						}
					}

					//Get all existing shards overlapping keys (exclude any that have been processed in a previous iteration of the outer loop)
					state KeyRange currentKeys = KeyRangeRef(begin, keys.end);
					state Standalone<RangeResultRef> old = wait( krmGetRanges( &tr, keyServersPrefix, currentKeys, SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT, SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES) );

					//Determine the last processed key (which will be the beginning for the next iteration)
					state Key endKey = old.end()[-1].key;
					currentKeys = KeyRangeRef(currentKeys.begin, endKey);

					/*TraceEvent("StartMoveKeysBatch", relocationIntervalId)
						.detail("KeyBegin", printable(currentKeys.begin).c_str())
						.detail("KeyEnd", printable(currentKeys.end).c_str());*/

					//printf("Moving '%s'-'%s' (%d) to %d servers\n", keys.begin.toString().c_str(), keys.end.toString().c_str(), old.size(), servers.size());
					//for(int i=0; i<old.size(); i++)
					//	printf("'%s': '%s'\n", old[i].key.toString().c_str(), old[i].value.toString().c_str());

					//Check that enough servers for each shard are in the correct state
					vector<vector<UID>> addAsSource = wait(additionalSources(old, &tr, servers.size(), SERVER_KNOBS->MAX_ADDED_SOURCES_MULTIPLIER*servers.size()));

					// For each intersecting range, update keyServers[range] dest to be servers and clear existing dest servers from serverKeys
					for(int i = 0; i < old.size() - 1; ++i) {
						KeyRangeRef rangeIntersectKeys( old[i].key, old[i+1].key );
						vector<UID> src;
						vector<UID> dest;
						decodeKeyServersValue( old[i].value, src, dest );

						/*TraceEvent("StartMoveKeysOldRange", relocationIntervalId)
							.detail("KeyBegin", printable(rangeIntersectKeys.begin).c_str())
							.detail("KeyEnd", printable(rangeIntersectKeys.end).c_str())
							.detail("OldSrc", describe(src))
							.detail("OldDest", describe(dest))
							.detail("ReadVersion", tr.getReadVersion().get());*/

						for(auto& uid : addAsSource[i]) {
							src.push_back(uid);
						}
						uniquify(src);

						//Update dest servers for this range to be equal to servers
						krmSetPreviouslyEmptyRange( &tr, keyServersPrefix, rangeIntersectKeys, keyServersValue(src, servers), old[i+1].value );

						//Track old destination servers.  They may be removed from serverKeys soon, since they are about to be overwritten in keyServers
						for(auto s = dest.begin(); s != dest.end(); ++s) {
							oldDests.insert(*s);
							/*TraceEvent("StartMoveKeysOldDestAdd", relocationIntervalId)
								.detail("Server", s->id());*/
						}

						//Keep track of src shards so that we can preserve their values when we overwrite serverKeys
						for(auto& uid : src) {
							shardMap[uid].push_back(old.arena(), rangeIntersectKeys);
							/*TraceEvent("StartMoveKeysShardMapAdd", relocationIntervalId)
								.detail("Server", *s);*/
						}
					}

					state std::set<UID>::iterator oldDest;

					//Remove old dests from serverKeys.  In order for krmSetRangeCoalescing to work correctly in the same prefix for a single transaction, we must
					//do most of the coalescing ourselves.  Only the shards on the boundary of currentRange are actually coalesced with the ranges outside of currentRange.
					//For all shards internal to currentRange, we overwrite all consecutive keys whose value is or should be serverKeysFalse in a single write
					vector<Future<Void>> actors;
					for(oldDest = oldDests.begin(); oldDest != oldDests.end(); ++oldDest)
						if( std::find(servers.begin(), servers.end(), *oldDest) == servers.end() )
							actors.push_back( removeOldDestinations( &tr, *oldDest, shardMap[*oldDest], currentKeys ) );

					//Update serverKeys to include keys (or the currently processed subset of keys) for each SS in servers
					for(int i = 0; i < servers.size(); i++ ) {
						// Since we are setting this for the entire range, serverKeys and keyServers aren't guaranteed to have the same shard boundaries
						// If that invariant was important, we would have to move this inside the loop above and also set it for the src servers
						actors.push_back( krmSetRangeCoalescing( &tr, serverKeysPrefixFor( servers[i] ), currentKeys, allKeys, serverKeysTrue) );
					}

					wait( waitForAll( actors ) );

					wait( tr.commit() );

					/*TraceEvent("StartMoveKeysCommitDone", relocationIntervalId)
						.detail("CommitVersion", tr.getCommittedVersion())
						.detail("ShardsInBatch", old.size() - 1);*/
					begin = endKey;
					shards += old.size() - 1;
					break;
				} catch (Error& e) {
					state Error err = e;
					if (err.code() == error_code_move_to_removed_server)
						throw;
					wait( tr.onError(e) );

					if(retries%10 == 0) {
						TraceEvent(retries == 50 ? SevWarnAlways : SevWarn, "StartMoveKeysRetrying", relocationIntervalId)
							.error(err)
							.detail("Keys", printable(keys))
							.detail("BeginKey", printable(begin))
							.detail("NumTries", retries);
					}
				}
			}

			if(retries > maxRetries) {
				maxRetries = retries;
			}
		}

		//printf("Committed moving '%s'-'%s' (version %lld)\n", keys.begin.toString().c_str(), keys.end.toString().c_str(), tr.getCommittedVersion());
		TraceEvent(SevDebug, interval.end(), relocationIntervalId)
			.detail("Batches", batches)
			.detail("Shards", shards)
			.detail("MaxRetries", maxRetries);
	} catch( Error& e ) {
		TraceEvent(SevDebug, interval.end(), relocationIntervalId).error(e, true);
		throw;
	}

	return Void();
}

ACTOR Future<Void> waitForShardReady( StorageServerInterface server, KeyRange keys, Version minVersion, Version recoveryVersion, GetShardStateRequest::waitMode mode){
	loop {
		try {
			std::pair<Version,Version> rep = wait( server.getShardState.getReply( GetShardStateRequest(keys, mode), TaskMoveKeys ) );
			if (rep.first >= minVersion && (recoveryVersion == invalidVersion || rep.second >= recoveryVersion)) {
				return Void();
			}
			wait( delayJittered( SERVER_KNOBS->SHARD_READY_DELAY, TaskMoveKeys ) );
		}
		catch (Error& e) {
			if( e.code() != error_code_timed_out ) {
				if (e.code() != error_code_broken_promise)
					throw e;
				wait(Never());  // Never return: A storage server which has failed will never be ready
				throw internal_error();  // does not happen
			}
		}
	}
}

ACTOR Future<Void> checkFetchingState( Database cx, vector<UID> dest, KeyRange keys,
		Promise<Void> dataMovementComplete, UID relocationIntervalId ) {
	state Transaction tr(cx);

	loop {
		try {
			if (BUGGIFY) wait(delay(5));

			tr.info.taskID = TaskMoveKeys;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			vector< Future< Optional<Value> > > serverListEntries;
			for(int s=0; s<dest.size(); s++)
				serverListEntries.push_back( tr.get( serverListKeyFor(dest[s]) ) );
			state vector<Optional<Value>> serverListValues = wait( getAll(serverListEntries) );
			vector<Future<Void>> requests;
			for(int s=0; s<serverListValues.size(); s++) {
				if( !serverListValues[s].present() ) {
					// FIXME: Is this the right behavior?  dataMovementComplete will never be sent!
					TEST(true); //check fetching state moved to removed server
					throw move_to_removed_server();
				}
				auto si = decodeServerListValue(serverListValues[s].get());
				ASSERT( si.id() == dest[s] );
				requests.push_back( waitForShardReady( si, keys, tr.getReadVersion().get(), invalidVersion, GetShardStateRequest::FETCHING ) );
			}

			wait( timeoutError( waitForAll( requests ),
					SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT, TaskMoveKeys ) );

			dataMovementComplete.send(Void());
			return Void();
		} catch( Error& e ) {
			if( e.code() == error_code_timed_out )
				tr.reset();
			else
				wait( tr.onError(e) );
		}
	}
}

// Set keyServers[keys].src = keyServers[keys].dest and keyServers[keys].dest=[], return when successful
// keyServers[k].dest must be the same for all k in keys
// Set serverKeys[dest][keys] = true; serverKeys[src][keys] = false for all src not in dest
// Should be cancelled and restarted if keyServers[keys].dest changes (?so this is no longer true?)
ACTOR Future<Void> finishMoveKeys( Database occ, KeyRange keys, vector<UID> destinationTeam, MoveKeysLock lock, FlowLock *finishMoveKeysParallelismLock, Version recoveryVersion, bool hasRemote, UID relocationIntervalId )
{
	state TraceInterval interval("RelocateShard_FinishMoveKeys");
	state TraceInterval waitInterval("");
	state Key begin = keys.begin;
	state int retries = 0;
	state FlowLock::Releaser releaser;

	ASSERT (!destinationTeam.empty());

	try {
		TraceEvent(SevDebug, interval.begin(), relocationIntervalId).detail("KeyBegin", printable(keys.begin)).detail("KeyEnd", printable(keys.end));

		//This process can be split up into multiple transactions if there are too many existing overlapping shards
		//In that case, each iteration of this loop will have begin set to the end of the last processed shard
		while(begin < keys.end) {
			TEST(begin > keys.begin); //Multi-transactional finishMoveKeys

			state Transaction tr( occ );

			//printf("finishMoveKeys( '%s'-'%s' )\n", keys.begin.toString().c_str(), keys.end.toString().c_str());
			loop {
				try {
					tr.info.taskID = TaskMoveKeys;
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					releaser.release();
					wait( finishMoveKeysParallelismLock->take( TaskDataDistributionLaunch ) );
					releaser = FlowLock::Releaser( *finishMoveKeysParallelismLock );

					wait( checkMoveKeysLock(&tr, lock) );

					state KeyRange currentKeys = KeyRangeRef(begin, keys.end);
					state Standalone<RangeResultRef> keyServers = wait( krmGetRanges( &tr, keyServersPrefix, currentKeys, SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT, SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES ) );

					//Determine the last processed key (which will be the beginning for the next iteration)
					state Key endKey = keyServers.end()[-1].key;
					currentKeys = KeyRangeRef(currentKeys.begin, endKey);

					//printf("  finishMoveKeys( '%s'-'%s' ): read keyServers at %lld\n", keys.begin.toString().c_str(), keys.end.toString().c_str(), tr.getReadVersion().get());

					// Decode and sanity check the result (dest must be the same for all ranges)
					bool alreadyMoved = true;

					state vector<UID> dest;
					state std::set<UID> allServers;
					state std::set<UID> intendedTeam(destinationTeam.begin(), destinationTeam.end());
					state vector<UID> src;
					vector<UID> completeSrc;

					//Iterate through the beginning of keyServers until we find one that hasn't already been processed
					int currentIndex;
					for(currentIndex = 0; currentIndex < keyServers.size() - 1 && alreadyMoved; currentIndex++) {
						decodeKeyServersValue( keyServers[currentIndex].value, src, dest );

						std::set<UID> srcSet;
						for(int s = 0; s < src.size(); s++) {
							srcSet.insert(src[s]);
						}

						if(currentIndex == 0) {
							completeSrc = src;
						} else {
							for(int i = 0; i < completeSrc.size(); i++) {
								if(!srcSet.count(completeSrc[i])) {
									swapAndPop(&completeSrc, i--);
								}
							}
						}

						std::set<UID> destSet;
						for(int s = 0; s < dest.size(); s++) {
							destSet.insert(dest[s]);
						}

						allServers.insert(srcSet.begin(), srcSet.end());
						allServers.insert(destSet.begin(), destSet.end());

						alreadyMoved = destSet.empty() && srcSet == intendedTeam;
						if(destSet != intendedTeam && !alreadyMoved) {
							TraceEvent(SevWarn, "MoveKeysDestTeamNotIntended", relocationIntervalId)
								.detail("KeyBegin", printable(keys.begin))
								.detail("KeyEnd", printable(keys.end))
								.detail("IterationBegin", printable(begin))
								.detail("IterationEnd", printable(endKey))
								.detail("DestSet", describe(destSet))
								.detail("IntendedTeam", describe(intendedTeam))
								.detail("KeyServers", printable(keyServers));
							//ASSERT( false );

							ASSERT(!dest.empty()); //The range has already been moved, but to a different dest (or maybe dest was cleared)

							intendedTeam.clear();
							for(int i = 0; i < dest.size(); i++)
								intendedTeam.insert(dest[i]);
						}
						else if(alreadyMoved) {
							dest.clear();
							src.clear();
							TEST(true); //FinishMoveKeys first key in iteration sub-range has already been processed
						}
					}

					//Process the rest of the key servers
					for(; currentIndex < keyServers.size() - 1; currentIndex++) {
						vector<UID> src2, dest2;
						decodeKeyServersValue( keyServers[currentIndex].value, src2, dest2 );

						std::set<UID> srcSet;
						for(int s = 0; s < src2.size(); s++)
							srcSet.insert(src2[s]);

						for(int i = 0; i < completeSrc.size(); i++) {
							if(!srcSet.count(completeSrc[i])) {
								swapAndPop(&completeSrc, i--);
							}
						}

						allServers.insert(srcSet.begin(), srcSet.end());

						alreadyMoved = dest2.empty() && srcSet == intendedTeam;
						if (dest2 != dest && !alreadyMoved) {
							TraceEvent(SevError,"FinishMoveKeysError", relocationIntervalId)
								.detail("Reason", "dest mismatch")
								.detail("Dest", describe(dest))
								.detail("Dest2", describe(dest2));
							ASSERT(false);
						}
					}
					if (!dest.size()) {
						TEST(true); // A previous finishMoveKeys for this range committed just as it was cancelled to start this one?
						TraceEvent("FinishMoveKeysNothingToDo", relocationIntervalId)
							.detail("KeyBegin", printable(keys.begin))
							.detail("KeyEnd", printable(keys.end))
							.detail("IterationBegin", printable(begin))
							.detail("IterationEnd", printable(endKey));
						begin = keyServers.end()[-1].key;
						break;
					}

					waitInterval = TraceInterval("RelocateShard_FinishMoveKeysWaitDurable");
					TraceEvent(SevDebug, waitInterval.begin(), relocationIntervalId)
						.detail("KeyBegin", printable(keys.begin))
						.detail("KeyEnd", printable(keys.end));

					// Wait for a durable quorum of servers in destServers to have keys available (readWrite)
					// They must also have at least the transaction read version so they can't "forget" the shard between
					//   now and when this transaction commits.
					state vector< Future<Void> > serverReady;	// only for count below
					state vector<UID> newDestinations;
					std::set<UID> completeSrcSet(completeSrc.begin(), completeSrc.end());
					for(auto& it : dest) {
						if(!hasRemote || !completeSrcSet.count(it)) {
							newDestinations.push_back(it);
						}
					}

					// for smartQuorum
					state vector<StorageServerInterface> storageServerInterfaces;
					vector< Future< Optional<Value> > > serverListEntries;
					for(int s=0; s<newDestinations.size(); s++)
						serverListEntries.push_back( tr.get( serverListKeyFor(newDestinations[s]) ) );
					state vector<Optional<Value>> serverListValues = wait( getAll(serverListEntries) );

					releaser.release();

					for(int s=0; s<serverListValues.size(); s++) {
						ASSERT( serverListValues[s].present() );  // There should always be server list entries for servers in keyServers
						auto si = decodeServerListValue(serverListValues[s].get());
						ASSERT( si.id() == newDestinations[s] );
						storageServerInterfaces.push_back( si );
					}

					for(int s=0; s<storageServerInterfaces.size(); s++)
						serverReady.push_back( waitForShardReady( storageServerInterfaces[s], keys, tr.getReadVersion().get(), recoveryVersion, GetShardStateRequest::READABLE) );
					wait( timeout( waitForAll( serverReady ), SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT, Void(), TaskMoveKeys ) );
					int count = dest.size() - newDestinations.size();
					for(int s=0; s<serverReady.size(); s++)
						count += serverReady[s].isReady() && !serverReady[s].isError();

					//printf("  fMK: moved data to %d/%d servers\n", count, serverReady.size());
					TraceEvent(SevDebug, waitInterval.end(), relocationIntervalId).detail("ReadyServers", count);

					if( count == dest.size() ) {
						// update keyServers, serverKeys
						// SOMEDAY: Doing these in parallel is safe because none of them overlap or touch (one per server)
						wait( krmSetRangeCoalescing( &tr, keyServersPrefix, currentKeys, keys, keyServersValue( dest ) ) );

						std::set<UID>::iterator asi = allServers.begin();
						std::vector<Future<Void>> actors;
						while (asi != allServers.end()) {
							bool destHasServer = std::find(dest.begin(), dest.end(), *asi) != dest.end();
							actors.push_back( krmSetRangeCoalescing( &tr, serverKeysPrefixFor(*asi), currentKeys, allKeys, destHasServer ? serverKeysTrue : serverKeysFalse ) );
							++asi;
						}

						wait(waitForAll(actors));
						wait( tr.commit() );

						begin = endKey;
						break;
					}
					tr.reset();
				} catch (Error& error) {
					if (error.code() == error_code_actor_cancelled) throw;
					state Error err = error;
					wait( tr.onError(error) );
					retries++;
					if(retries%10 == 0) {
						TraceEvent(retries == 20 ? SevWarnAlways : SevWarn, "RelocateShard_FinishMoveKeysRetrying", relocationIntervalId)
							.error(err)
							.detail("KeyBegin", printable(keys.begin))
							.detail("KeyEnd", printable(keys.end))
							.detail("IterationBegin", printable(begin))
							.detail("IterationEnd", printable(endKey));
					}
				}
			}
		}

		TraceEvent(SevDebug, interval.end(), relocationIntervalId);
	} catch(Error &e) {
		TraceEvent(SevDebug, interval.end(), relocationIntervalId).error(e, true);
		throw;
	}
	return Void();
}

ACTOR Future<std::pair<Version, Tag>> addStorageServer( Database cx, StorageServerInterface server )
{
	state Transaction tr( cx );
	state int maxSkipTags = 1;
	loop {
		try {
			state Future<Standalone<RangeResultRef>> fTagLocalities = tr.getRange( tagLocalityListKeys, CLIENT_KNOBS->TOO_MANY );
			state Future<Optional<Value>> fv = tr.get( serverListKeyFor(server.id()) );
			state Future<Optional<Value>> fExclProc = tr.get(
				StringRef(encodeExcludedServersKey( AddressExclusion( server.address().ip, server.address().port ))) );
			state Future<Optional<Value>> fExclIP = tr.get(
				StringRef(encodeExcludedServersKey( AddressExclusion( server.address().ip ))) );
			state Future<Standalone<RangeResultRef>> fTags = tr.getRange( serverTagKeys, CLIENT_KNOBS->TOO_MANY, true);
			state Future<Standalone<RangeResultRef>> fHistoryTags = tr.getRange( serverTagHistoryKeys, CLIENT_KNOBS->TOO_MANY, true);

			wait( success(fTagLocalities) && success(fv) && success(fExclProc) && success(fExclIP) && success(fTags) && success(fHistoryTags) );

			// If we have been added to the excluded state servers list, we have to fail
			if (fExclProc.get().present() || fExclIP.get().present())
				throw recruitment_failed();

			if(fTagLocalities.get().more || fTags.get().more || fHistoryTags.get().more)
				ASSERT(false);

			int8_t maxTagLocality = 0;
			state int8_t locality = -1;
			for(auto& kv : fTagLocalities.get()) {
				int8_t loc = decodeTagLocalityListValue( kv.value );
				if( decodeTagLocalityListKey( kv.key ) == server.locality.dcId() ) {
					locality = loc;
					break;
				}
				maxTagLocality = std::max(maxTagLocality, loc);
			}

			if(locality == -1) {
				locality = maxTagLocality + 1;
				if(locality < 0)
					throw recruitment_failed();
				tr.set( tagLocalityListKeyFor(server.locality.dcId()), tagLocalityListValue(locality) );
			}

			int skipTags = g_random->randomInt(0, maxSkipTags);

			state uint16_t tagId = 0;
			std::vector<uint16_t> usedTags;
			for(auto& it : fTags.get()) {
				Tag t = decodeServerTagValue( it.value );
				if(t.locality == locality) {
					usedTags.push_back(t.id);
				}
			}
			for(auto& it : fHistoryTags.get()) {
				Tag t = decodeServerTagValue( it.value );
				if(t.locality == locality) {
					usedTags.push_back(t.id);
				}
			}
			std::sort(usedTags.begin(), usedTags.end());

			int usedIdx = 0;
			for(; usedTags.size() > 0 && tagId <= usedTags.end()[-1]; tagId++) {
				if(tagId < usedTags[usedIdx]) {
					if(skipTags == 0)
						break;
					skipTags--;
				} else {
					usedIdx++;
				}
			}
			tagId += skipTags;

			state Tag tag(locality, tagId);
			tr.set( serverTagKeyFor(server.id()), serverTagValue(tag) );
			tr.set( serverListKeyFor(server.id()), serverListValue(server) );
			KeyRange conflictRange = singleKeyRange(serverTagConflictKeyFor(tag));
			tr.addReadConflictRange( conflictRange );
			tr.addWriteConflictRange( conflictRange );

			wait( tr.commit() );
			return std::make_pair(tr.getCommittedVersion(), tag);
		} catch (Error& e) {
			if(e.code() == error_code_commit_unknown_result)
				throw recruitment_failed();  // There is a remote possibility that we successfully added ourselves and then someone removed us, so we have to fail

			if(e.code() == error_code_not_committed) {
				maxSkipTags = SERVER_KNOBS->MAX_SKIP_TAGS;
			}

			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<bool> canRemoveStorageServer( Transaction* tr, UID serverID ) {
	Standalone<RangeResultRef> keys = wait( krmGetRanges( tr, serverKeysPrefixFor(serverID), allKeys, 2 ) );

	ASSERT(keys.size() >= 2);

	if(keys[0].value == keys[1].value && keys[1].key != allKeys.end) {
		TraceEvent("ServerKeysCoalescingError", serverID).detail("Key1", printable(keys[0].key)).detail("Key2", printable(keys[1].key)).detail("Value", printable(keys[0].value));
		ASSERT(false);
	}

	//Return true if the entire range is false.  Since these values are coalesced, we can return false if there is more than one result
	return keys[0].value == serverKeysFalse && keys[1].key == allKeys.end;
}

ACTOR Future<Void> removeStorageServer( Database cx, UID serverID, MoveKeysLock lock )
{
	state Transaction tr( cx );
	state bool retry = false;
	state int noCanRemoveCount = 0;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			wait( checkMoveKeysLock(&tr, lock) );
			TraceEvent("RemoveStorageServerLocked").detail("ServerID", serverID).detail("Version", tr.getReadVersion().get());

			state bool canRemove = wait( canRemoveStorageServer( &tr, serverID ) );
			if (!canRemove) {
				TEST(true); // The caller had a transaction in flight that assigned keys to the server.  Wait for it to reverse its mistake.
				TraceEvent(SevWarn,"NoCanRemove").detail("Count", noCanRemoveCount++).detail("ServerID", serverID);
				wait( delayJittered(SERVER_KNOBS->REMOVE_RETRY_DELAY, TaskDataDistributionLaunch) );
				tr.reset();
				TraceEvent("RemoveStorageServerRetrying").detail("CanRemove", canRemove);
			} else {

				state Future<Optional<Value>> fListKey = tr.get( serverListKeyFor(serverID) );
				state Future<Standalone<RangeResultRef>> fTags = tr.getRange( serverTagKeys, CLIENT_KNOBS->TOO_MANY );
				state Future<Standalone<RangeResultRef>> fHistoryTags = tr.getRange( serverTagHistoryKeys, CLIENT_KNOBS->TOO_MANY );
				state Future<Standalone<RangeResultRef>> fTagLocalities = tr.getRange( tagLocalityListKeys, CLIENT_KNOBS->TOO_MANY );
				state Future<Standalone<RangeResultRef>> fTLogDatacenters = tr.getRange( tLogDatacentersKeys, CLIENT_KNOBS->TOO_MANY );

				wait( success(fListKey) && success(fTags) && success(fHistoryTags) && success(fTagLocalities) && success(fTLogDatacenters) );

				if (!fListKey.get().present()) {
					if (retry) {
						TEST(true);  // Storage server already removed after retrying transaction
						return Void();
					}
					ASSERT(false);  // Removing an already-removed server?  A never added server?
				}

				int8_t locality = -100;
				std::set<int8_t> allLocalities;
				for(auto& it : fTags.get()) {
					UID sId = decodeServerTagKey( it.key );
					Tag t = decodeServerTagValue( it.value );
					if(sId == serverID) {
						locality = t.locality;
					} else {
						allLocalities.insert(t.locality);
					}
				}
				for(auto& it : fHistoryTags.get()) {
					Tag t = decodeServerTagValue( it.value );
					allLocalities.insert(t.locality);
				}

				std::map<Optional<Value>,int8_t> dcId_locality;
				for(auto& kv : fTagLocalities.get()) {
					dcId_locality[decodeTagLocalityListKey(kv.key)] = decodeTagLocalityListValue(kv.value);
				}
				for(auto& it : fTLogDatacenters.get()) {
					allLocalities.insert(dcId_locality[decodeTLogDatacentersKey(it.key)]);
				}

				if(locality >= 0 && !allLocalities.count(locality) ) {
					for(auto& it : fTagLocalities.get()) {
						if( locality == decodeTagLocalityListValue(it.value) ) {
							tr.clear(it.key);
							break;
						}
					}
				}

				tr.clear( serverListKeyFor(serverID) );
				tr.clear( serverTagKeyFor(serverID) );
				tr.clear( serverTagHistoryRangeFor(serverID) );
				retry = true;
				wait( tr.commit() );
				return Void();
			}
		} catch (Error& e) {
			state Error err = e;
			wait( tr.onError(e) );
			TraceEvent("RemoveStorageServerRetrying").error(err);
		}
	}
}

ACTOR Future<Void> moveKeys(
	Database cx,
	KeyRange keys,
	vector<UID> destinationTeam,
	vector<UID> healthyDestinations,
	MoveKeysLock lock,
	Promise<Void> dataMovementComplete,
	FlowLock *startMoveKeysParallelismLock,
	FlowLock *finishMoveKeysParallelismLock,
	Version recoveryVersion,
	bool hasRemote,
	UID relocationIntervalId)
{
	ASSERT( destinationTeam.size() );
	std::sort( destinationTeam.begin(), destinationTeam.end() );
	wait( startMoveKeys( cx, keys, destinationTeam, lock, startMoveKeysParallelismLock, relocationIntervalId ) );

	state Future<Void> completionSignaller = checkFetchingState( cx, healthyDestinations, keys, dataMovementComplete, relocationIntervalId );

	wait( finishMoveKeys( cx, keys, destinationTeam, lock, finishMoveKeysParallelismLock, recoveryVersion, hasRemote, relocationIntervalId ) );

	//This is defensive, but make sure that we always say that the movement is complete before moveKeys completes
	completionSignaller.cancel();
	if(!dataMovementComplete.isSet())
		dataMovementComplete.send(Void());

	return Void();
}

void seedShardServers(
	Arena& arena,
	CommitTransactionRef &tr,
	vector<StorageServerInterface> servers )
{
	std::map<Optional<Value>, Tag> dcId_locality;
	std::map<UID, Tag> server_tag;
	int8_t nextLocality = 0;
	for(auto& s : servers) {
		if(!dcId_locality.count(s.locality.dcId())) {
			tr.set(arena, tagLocalityListKeyFor(s.locality.dcId()), tagLocalityListValue(nextLocality));
			dcId_locality[s.locality.dcId()] = Tag(nextLocality, 0);
			nextLocality++;
		}
		Tag& t = dcId_locality[s.locality.dcId()];
		server_tag[s.id()] = Tag(t.locality, t.id);
		t.id++;
	}
	std::sort(servers.begin(), servers.end());

	// This isn't strictly necessary, but make sure this is the first transaction
	tr.read_snapshot = 0;
	tr.read_conflict_ranges.push_back_deep( arena, allKeys );

	for(int s=0; s<servers.size(); s++) {
		tr.set(arena, serverTagKeyFor(servers[s].id()), serverTagValue(server_tag[servers[s].id()]));
		tr.set(arena, serverListKeyFor(servers[s].id()), serverListValue(servers[s]));
	}

	std::vector<UID> serverIds;
	for(int i=0;i<servers.size();i++)
		serverIds.push_back(servers[i].id());

	// We have to set this range in two blocks, because the master tracking of "keyServersLocations" depends on a change to a specific
	//   key (keyServersKeyServersKey)
	krmSetPreviouslyEmptyRange( tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), keyServersValue( serverIds ), Value() );

	for(int s=0; s<servers.size(); s++)
		krmSetPreviouslyEmptyRange( tr, arena, serverKeysPrefixFor( servers[s].id() ), allKeys, serverKeysTrue, serverKeysFalse );
}
