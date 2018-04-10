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

#include "flow/actorcompiler.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/SystemData.h"
#include "MoveKeys.h"
#include "Knobs.h"

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
			Void _ = wait(tr.onError(e));
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

ACTOR Future<Optional<UID>> checkReadWrite( Future< ErrorOr<Version> > fReply, UID uid ) {
	ErrorOr<Version> reply = wait( fReply );
	if (!reply.present())
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

ACTOR Future<vector<vector<Optional<UID>>>> findReadWriteDestinations(Standalone<RangeResultRef> shards, UID relocationIntervalId, Transaction* tr) {
	vector<Future<Optional<Value>>> serverListEntries;
	for(int i = 0; i < shards.size() - 1; ++i) {
		vector<UID> src;
		vector<UID> dest;

		decodeKeyServersValue( shards[i].value, src, dest );

		for(int s=0; s<dest.size(); s++) {
			serverListEntries.push_back( tr->get( serverListKeyFor(dest[s]) ) );
		}
	}

	vector<Optional<Value>> serverListValues = wait( getAll(serverListEntries) );

	std::map<UID, StorageServerInterface> ssiMap;
	for(int s=0; s<serverListValues.size(); s++) {
		auto si = decodeServerListValue(serverListValues[s].get());
		StorageServerInterface ssi = decodeServerListValue(serverListValues[s].get());
		ssiMap[ssi.id()] = ssi;
	}

	vector<Future<vector<Optional<UID>>>> allChecks;
	for(int i = 0; i < shards.size() - 1; ++i) {
		KeyRangeRef rangeIntersectKeys( shards[i].key, shards[i+1].key );
		vector<UID> src;
		vector<UID> dest;
		vector<StorageServerInterface> storageServerInterfaces;

		decodeKeyServersValue( shards[i].value, src, dest );

		for(int s=0; s<dest.size(); s++)
			storageServerInterfaces.push_back( ssiMap[dest[s]] );

		vector< Future<Optional<UID>> > checks;
		for(int s=0; s<storageServerInterfaces.size(); s++) {
			checks.push_back( checkReadWrite( storageServerInterfaces[s].getShardState.getReplyUnlessFailedFor(
				GetShardStateRequest( rangeIntersectKeys, GetShardStateRequest::NO_WAIT), SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL, 0, TaskMoveKeys ), dest[s] ) );
		}

		allChecks.push_back(getAll(checks));
	}

	vector<vector<Optional<UID>>> readWriteDestinations = wait(getAll(allChecks));
	return readWriteDestinations;
}

// Set keyServers[keys].dest = servers
// Set serverKeys[servers][keys] = active for each subrange of keys that the server did not already have, complete for each subrange that it already has
// Set serverKeys[dest][keys] = "" for the dest servers of each existing shard in keys (unless that destination is a member of servers OR if the source list is sufficiently degraded)
ACTOR Future<Void> startMoveKeys( Database occ, KeyRange keys, vector<UID> servers,
								 MoveKeysLock lock, int durableStorageQuorum,
								 FlowLock *startMoveKeysLock, UID relocationIntervalId ) {
	state TraceInterval interval("RelocateShard_StartMoveKeys");
	//state TraceInterval waitInterval("");

	Void _ = wait( startMoveKeysLock->take( TaskDataDistributionLaunch ) );
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

					Void _ = wait( checkMoveKeysLock(&tr, lock) );

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
					vector<vector<Optional<UID>>> readWriteDestinations = wait(findReadWriteDestinations(old, relocationIntervalId, &tr));

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

						for(auto& uid : readWriteDestinations[i]) {
							if(uid.present()) {
								src.push_back(uid.get());
							}
						}

						std::sort( src.begin(), src.end() );
						src.resize( std::unique( src.begin(), src.end() ) - src.begin() );

						//Update dest servers for this range to be equal to servers
						krmSetPreviouslyEmptyRange( &tr, keyServersPrefix, rangeIntersectKeys, keyServersValue(src, servers), old[i+1].value );

						//Track old destination servers.  They may be removed from serverKeys soon, since they are about to be overwritten in keyServers
						for(auto s = dest.begin(); s != dest.end(); ++s) {
							oldDests.insert(*s);
							/*TraceEvent("StartMoveKeysOldDestAdd", relocationIntervalId)
								.detail("Server", s->id());*/
						}

						//Keep track of src shards so that we can preserve their values when we overwrite serverKeys
						std::set<UID> sources;
						for(auto s = src.begin(); s != src.end(); ++s)
							sources.insert(*s);
						for(auto s = sources.begin(); s != sources.end(); ++s) {
							shardMap[*s].push_back(old.arena(), rangeIntersectKeys);
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

					Void _ = wait( waitForAll( actors ) );

					Void _ = wait( tr.commit() );

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
					Void _ = wait( tr.onError(e) );

					if(retries%10 == 0) {
						TraceEvent(retries == 50 ? SevWarnAlways : SevWarn, "startMoveKeysRetrying", relocationIntervalId)
							.detail("Keys", printable(keys))
							.detail("BeginKey", printable(begin))
							.detail("NumTries", retries)
							.error(err);
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

ACTOR Future<Void> waitForShardReady( StorageServerInterface server, KeyRange keys, Version minVersion, GetShardStateRequest::waitMode mode){
	loop {
		try {
			Version rep = wait( server.getShardState.getReply( GetShardStateRequest(keys, mode), TaskMoveKeys ) );
			if (rep >= minVersion) {
				return Void();
			}
			Void _ = wait( delayJittered( SERVER_KNOBS->SHARD_READY_DELAY, TaskMoveKeys ) );
		}
		catch (Error& e) {
			if( e.code() != error_code_timed_out ) {
				if (e.code() != error_code_broken_promise)
					throw e;
				Void _ = wait(Never());  // Never return: A storage server which has failed will never be ready
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
			if (BUGGIFY) Void _ = wait(delay(5));

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
				requests.push_back( waitForShardReady( si, keys, tr.getReadVersion().get(), GetShardStateRequest::FETCHING ) );
			}

			Void _ = wait( timeoutError( waitForAll( requests ),
					SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT, TaskMoveKeys ) );

			dataMovementComplete.send(Void());
			return Void();
		} catch( Error& e ) {
			if( e.code() == error_code_timed_out )
				tr.reset();
			else
				Void _ = wait( tr.onError(e) );
		}
	}
}

// Set keyServers[keys].src = keyServers[keys].dest and keyServers[keys].dest=[], return when successful
// keyServers[k].dest must be the same for all k in keys
// Set serverKeys[dest][keys] = true; serverKeys[src][keys] = false for all src not in dest
// Should be cancelled and restarted if keyServers[keys].dest changes (?so this is no longer true?)
ACTOR Future<Void> finishMoveKeys( Database occ, KeyRange keys, vector<UID> destinationTeam,
		MoveKeysLock lock, int durableStorageQuorum, FlowLock *finishMoveKeysParallelismLock, UID relocationIntervalId )
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
					Void _ = wait( finishMoveKeysParallelismLock->take( TaskDataDistributionLaunch ) );
					releaser = FlowLock::Releaser( *finishMoveKeysParallelismLock );

					Void _ = wait( checkMoveKeysLock(&tr, lock) );

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

					//Iterate through the beginning of keyServers until we find one that hasn't already been processed
					int currentIndex;
					for(currentIndex = 0; currentIndex < keyServers.size() - 1 && alreadyMoved; currentIndex++) {
						decodeKeyServersValue( keyServers[currentIndex].value, src, dest );

						std::set<UID> srcSet;
						for(int s = 0; s < src.size(); s++)
							srcSet.insert(src[s]);

						std::set<UID> destSet;
						for(int s = 0; s < dest.size(); s++)
							destSet.insert(dest[s]);

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

					if (dest.size() < durableStorageQuorum) {
						TraceEvent(SevError,"FinishMoveKeysError", relocationIntervalId)
							.detailf("Reason", "dest size too small (%d)", dest.size());
						ASSERT(false);
					}

					waitInterval = TraceInterval("RelocateShard_FinishMoveKeys_WaitDurable");
					TraceEvent(SevDebug, waitInterval.begin(), relocationIntervalId)
						.detail("KeyBegin", printable(keys.begin))
						.detail("KeyEnd", printable(keys.end));

					// Wait for a durable quorum of servers in destServers to have keys available (readWrite)
					// They must also have at least the transaction read version so they can't "forget" the shard between
					//   now and when this transaction commits.
					state vector< Future<Void> > serverReady;	// only for count below

					// for smartQuorum
					state vector<StorageServerInterface> storageServerInterfaces;
					vector< Future< Optional<Value> > > serverListEntries;
					for(int s=0; s<dest.size(); s++)
						serverListEntries.push_back( tr.get( serverListKeyFor(dest[s]) ) );
					state vector<Optional<Value>> serverListValues = wait( getAll(serverListEntries) );

					releaser.release();

					for(int s=0; s<serverListValues.size(); s++) {
						ASSERT( serverListValues[s].present() );  // There should always be server list entries for servers in keyServers
						auto si = decodeServerListValue(serverListValues[s].get());
						ASSERT( si.id() == dest[s] );
						storageServerInterfaces.push_back( si );
					}

					for(int s=0; s<storageServerInterfaces.size(); s++)
						serverReady.push_back( waitForShardReady( storageServerInterfaces[s], keys, tr.getReadVersion().get(), GetShardStateRequest::READABLE) );
					Void _ = wait( timeout(
						smartQuorum( serverReady, durableStorageQuorum, SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL, TaskMoveKeys ),
						SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT, Void(), TaskMoveKeys ) );
					int count = 0;
					for(int s=0; s<serverReady.size(); s++)
						count += serverReady[s].isReady() && !serverReady[s].isError();

					//printf("  fMK: moved data to %d/%d servers\n", count, serverReady.size());
					TraceEvent(SevDebug, waitInterval.end(), relocationIntervalId).detail("ReadyServers", count);

					if( count >= durableStorageQuorum ) {
						// update keyServers, serverKeys
						// SOMEDAY: Doing these in parallel is safe because none of them overlap or touch (one per server)
						Void _ = wait( krmSetRangeCoalescing( &tr, keyServersPrefix, currentKeys, keys, keyServersValue( dest ) ) );

						std::set<UID>::iterator asi = allServers.begin();
						std::vector<Future<Void>> actors;
						while (asi != allServers.end()) {
							bool destHasServer = std::find(dest.begin(), dest.end(), *asi) != dest.end();
							actors.push_back( krmSetRangeCoalescing( &tr, serverKeysPrefixFor(*asi), currentKeys, allKeys, destHasServer ? serverKeysTrue : serverKeysFalse ) );
							++asi;
						}

						Void _ = wait(waitForAll(actors));

						//printf("  fMK: committing\n");

						Void _ = wait( tr.commit() );

						begin = endKey;
						break;
					}
					tr.reset();
				} catch (Error& error) {
					if (error.code() == error_code_actor_cancelled) throw;
					state Error err = error;
					Void _ = wait( tr.onError(error) );
					retries++;
					if(retries%10 == 0) {
						TraceEvent(retries == 20 ? SevWarnAlways : SevWarn, "RelocateShard_finishMoveKeysRetrying", relocationIntervalId)
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
	//printf("Moved keys: ( '%s'-'%s' )\n", keys.begin.toString().c_str(), keys.end.toString().c_str());

	return Void();
}

ACTOR Future<std::pair<Version, Tag>> addStorageServer( Database cx, StorageServerInterface server )
{
	state Transaction tr( cx );
	state int maxSkipTags = 1;
	loop {
		try {
			state Future<Optional<Value>> fv = tr.get( serverListKeyFor(server.id()) );
			state Future<Optional<Value>> fExclProc = tr.get(
				StringRef(encodeExcludedServersKey( AddressExclusion( server.address().ip, server.address().port ))) );
			state Future<Optional<Value>> fExclIP = tr.get(
				StringRef(encodeExcludedServersKey( AddressExclusion( server.address().ip ))) );
			state Future<Standalone<RangeResultRef>> fTags( tr.getRange( serverTagKeys, CLIENT_KNOBS->TOO_MANY, true) );

			Void _ = wait( success(fv) && success(fExclProc) && success(fExclIP) && success(fTags) );

			// If we have been added to the excluded state servers list, we have to fail
			if (fExclProc.get().present() || fExclIP.get().present())
				throw recruitment_failed();

			if(fTags.get().more)
				ASSERT(false);

			int skipTags = g_random->randomInt(0, maxSkipTags);

			state Tag tag = 0;
			std::vector<Tag> usedTags;
			for(auto it : fTags.get())
				usedTags.push_back(decodeServerTagValue( it.value ));
			std::sort(usedTags.begin(), usedTags.end());

			int usedIdx = 0;
			for(; tag <= usedTags.end()[-1]; tag++) {
				if(tag < usedTags[usedIdx]) {
					if(skipTags == 0)
						break;
					skipTags--;
				} else {
					usedIdx++;
				}
			}
			tag += skipTags;

			tr.set( serverTagKeyFor(server.id()), serverTagValue(tag) );
			tr.set( serverListKeyFor(server.id()), serverListValue(server) );
			KeyRange conflictRange = singleKeyRange(serverTagConflictKeyFor(tag));
			tr.addReadConflictRange( conflictRange );
			tr.addWriteConflictRange( conflictRange );
			tr.atomicOp( serverTagMaxKey, serverTagMaxValue(tag), MutationRef::Max );

			Void _ = wait( tr.commit() );
			return std::make_pair(tr.getCommittedVersion(), tag);
		} catch (Error& e) {
			if(e.code() == error_code_commit_unknown_result)
				throw recruitment_failed();  // There is a remote possibility that we successfully added ourselves and then someone removed us, so we have to fail

			Void _ = wait( tr.onError(e) );

			maxSkipTags = std::min<int>(maxSkipTags * SERVER_KNOBS->SKIP_TAGS_GROWTH_RATE, SERVER_KNOBS->MAX_SKIP_TAGS);
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
			Void _ = wait( checkMoveKeysLock(&tr, lock) );
			TraceEvent("RemoveStorageServerLocked").detail("ServerID", serverID).detail("Version", tr.getReadVersion().get());

			state bool canRemove = wait( canRemoveStorageServer( &tr, serverID ) );
			if (!canRemove) {
				TEST(true); // The caller had a transaction in flight that assigned keys to the server.  Wait for it to reverse its mistake.
				TraceEvent(SevWarn,"NoCanRemove").detail("Count", noCanRemoveCount++).detail("ServerID", serverID);
				Void _ = wait( delayJittered(SERVER_KNOBS->REMOVE_RETRY_DELAY, TaskDataDistributionLaunch) );
				tr.reset();
				TraceEvent("RemoveStorageServerRetrying").detail("canRemove", canRemove);
			} else {
				Optional<Value> v = wait( tr.get( serverListKeyFor(serverID) ) );
				if (!v.present()) {
					if (retry) {
						TEST(true);  // Storage server already removed after retrying transaction
						return Void();
					}
					ASSERT(false);  // Removing an already-removed server?  A never added server?
				}

				tr.clear( serverListKeyFor(serverID) );
				tr.clear( serverTagKeyFor(serverID) );
				retry = true;
				Void _ = wait( tr.commit() );
				return Void();
			}
		} catch (Error& e) {
			state Error err = e;
			Void _ = wait( tr.onError(e) );
			TraceEvent("RemoveStorageServerRetrying").error(err);
		}
	}
}

ACTOR Future<Void> moveKeys(
	Database cx,
	KeyRange keys,
	vector<UID> destinationTeam,
	MoveKeysLock lock,
	int durableStorageQuorum,
	Promise<Void> dataMovementComplete,
	FlowLock *startMoveKeysParallelismLock,
	FlowLock *finishMoveKeysParallelismLock,
	UID relocationIntervalId)
{
	ASSERT( destinationTeam.size() );
	std::sort( destinationTeam.begin(), destinationTeam.end() );
	Void _ = wait( startMoveKeys( cx, keys, destinationTeam, lock, durableStorageQuorum, startMoveKeysParallelismLock, relocationIntervalId ) );

	state Future<Void> completionSignaller = checkFetchingState( cx, destinationTeam, keys, dataMovementComplete, relocationIntervalId );

	Void _ = wait( finishMoveKeys( cx, keys, destinationTeam, lock, durableStorageQuorum, finishMoveKeysParallelismLock, relocationIntervalId ) );

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
	std::map<UID, Tag> server_tag;
	for(Tag s=0; s<servers.size(); s++)
		server_tag[servers[s].id()] = s;
	std::sort(servers.begin(), servers.end());

	// This isn't strictly necessary, but make sure this is the first transaction
	tr.read_snapshot = 0;
	tr.read_conflict_ranges.push_back_deep( arena, allKeys );

	for(int s=0; s<servers.size(); s++) {
		tr.set(arena, serverTagKeyFor(servers[s].id()), serverTagValue(server_tag[servers[s].id()]));
		tr.set(arena, serverListKeyFor(servers[s].id()), serverListValue(servers[s]));
	}
	tr.set(arena, serverTagMaxKey, serverTagMaxValue(servers.size()-1));

	std::vector<UID> serverIds;
	for(int i=0;i<servers.size();i++)
		serverIds.push_back(servers[i].id());

	// We have to set this range in two blocks, because the master tracking of "keyServersLocations" depends on a change to a specific
	//   key (keyServersKeyServersKey)
	krmSetPreviouslyEmptyRange( tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), keyServersValue( serverIds ), Value() );

	for(int s=0; s<servers.size(); s++)
		krmSetPreviouslyEmptyRange( tr, arena, serverKeysPrefixFor( servers[s].id() ), allKeys, serverKeysTrue, serverKeysFalse );
}
