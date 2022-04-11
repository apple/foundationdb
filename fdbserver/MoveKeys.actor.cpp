/*
 * MoveKeys.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <vector>

#include "fdbclient/FDBOptions.g.h"
#include "flow/Util.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TSSMappingUtil.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

bool DDEnabledState::isDDEnabled() const {
	return ddEnabled;
}

bool DDEnabledState::setDDEnabled(bool status, UID snapUID) {
	TraceEvent("SetDDEnabled").detail("Status", status).detail("SnapUID", snapUID);
	ASSERT(snapUID != UID());
	if (!status) {
		// disabling DD
		if (ddEnabledStatusUID != UID()) {
			// disable DD when a disable is already in progress not allowed
			return false;
		}
		ddEnabled = status;
		ddEnabledStatusUID = snapUID;
		return true;
	}
	// enabling DD
	if (snapUID != ddEnabledStatusUID) {
		// enabling DD not allowed if UID does not match with the disable request
		return false;
	}
	// reset to default status
	ddEnabled = status;
	ddEnabledStatusUID = UID();
	return true;
}

ACTOR Future<MoveKeysLock> takeMoveKeysLock(Database cx, UID ddId) {
	state Transaction tr(cx);
	loop {
		try {
			state MoveKeysLock lock;
			state UID txnId;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (!g_network->isSimulated()) {
				txnId = deterministicRandom()->randomUniqueID();
				tr.debugTransaction(txnId);
			}
			{
				Optional<Value> readVal = wait(tr.get(moveKeysLockOwnerKey));
				lock.prevOwner =
				    readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			}
			{
				Optional<Value> readVal = wait(tr.get(moveKeysLockWriteKey));
				lock.prevWrite =
				    readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			}
			lock.myOwner = deterministicRandom()->randomUniqueID();
			tr.set(moveKeysLockOwnerKey, BinaryWriter::toValue(lock.myOwner, Unversioned()));
			wait(tr.commit());
			TraceEvent("TakeMoveKeysLockTransaction", ddId)
			    .detail("TransactionUID", txnId)
			    .detail("PrevOwner", lock.prevOwner.toString())
			    .detail("PrevWrite", lock.prevWrite.toString())
			    .detail("MyOwner", lock.myOwner.toString());
			return lock;
		} catch (Error& e) {
			wait(tr.onError(e));
			TEST(true); // takeMoveKeysLock retry
		}
	}
}

ACTOR static Future<Void> checkMoveKeysLock(Transaction* tr,
                                            MoveKeysLock lock,
                                            const DDEnabledState* ddEnabledState,
                                            bool isWrite = true) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	if (!ddEnabledState->isDDEnabled()) {
		TraceEvent(SevDebug, "DDDisabledByInMemoryCheck").log();
		throw movekeys_conflict();
	}
	Optional<Value> readVal = wait(tr->get(moveKeysLockOwnerKey));
	UID currentOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();

	if (currentOwner == lock.prevOwner) {
		// Check that the previous owner hasn't touched the lock since we took it
		Optional<Value> readVal = wait(tr->get(moveKeysLockWriteKey));
		UID lastWrite = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
		if (lastWrite != lock.prevWrite) {
			TEST(true); // checkMoveKeysLock: Conflict with previous owner
			throw movekeys_conflict();
		}

		// Take the lock
		if (isWrite) {
			BinaryWriter wrMyOwner(Unversioned());
			wrMyOwner << lock.myOwner;
			tr->set(moveKeysLockOwnerKey, wrMyOwner.toValue());
			BinaryWriter wrLastWrite(Unversioned());
			UID lastWriter = deterministicRandom()->randomUniqueID();
			wrLastWrite << lastWriter;
			tr->set(moveKeysLockWriteKey, wrLastWrite.toValue());
			TraceEvent("CheckMoveKeysLock")
			    .detail("PrevOwner", lock.prevOwner.toString())
			    .detail("PrevWrite", lock.prevWrite.toString())
			    .detail("MyOwner", lock.myOwner.toString())
			    .detail("Writer", lastWriter.toString());
		}

		return Void();
	} else if (currentOwner == lock.myOwner) {
		if (isWrite) {
			// Touch the lock, preventing overlapping attempts to take it
			BinaryWriter wrLastWrite(Unversioned());
			wrLastWrite << deterministicRandom()->randomUniqueID();
			tr->set(moveKeysLockWriteKey, wrLastWrite.toValue());
			// Make this transaction self-conflicting so the database will not execute it twice with the same write key
			tr->makeSelfConflicting();
		}

		return Void();
	} else {
		TEST(true); // checkMoveKeysLock: Conflict with new owner
		throw movekeys_conflict();
	}
}

Future<Void> checkMoveKeysLockReadOnly(Transaction* tr, MoveKeysLock lock, const DDEnabledState* ddEnabledState) {
	return checkMoveKeysLock(tr, lock, ddEnabledState, false);
}

ACTOR Future<Optional<UID>> checkReadWrite(Future<ErrorOr<GetShardStateReply>> fReply, UID uid, Version version) {
	ErrorOr<GetShardStateReply> reply = wait(fReply);
	if (!reply.present() || reply.get().first < version)
		return Optional<UID>();
	return Optional<UID>(uid);
}

Future<Void> removeOldDestinations(Reference<ReadYourWritesTransaction> tr,
                                   UID oldDest,
                                   VectorRef<KeyRangeRef> shards,
                                   KeyRangeRef currentKeys) {
	KeyRef beginKey = currentKeys.begin;

	std::vector<Future<Void>> actors;
	for (int i = 0; i < shards.size(); i++) {
		if (beginKey < shards[i].begin)
			actors.push_back(krmSetRangeCoalescing(
			    tr, serverKeysPrefixFor(oldDest), KeyRangeRef(beginKey, shards[i].begin), allKeys, serverKeysFalse));

		beginKey = shards[i].end;
	}

	if (beginKey < currentKeys.end)
		actors.push_back(krmSetRangeCoalescing(
		    tr, serverKeysPrefixFor(oldDest), KeyRangeRef(beginKey, currentKeys.end), allKeys, serverKeysFalse));

	return waitForAll(actors);
}

ACTOR Future<std::vector<UID>> addReadWriteDestinations(KeyRangeRef shard,
                                                        std::vector<StorageServerInterface> srcInterfs,
                                                        std::vector<StorageServerInterface> destInterfs,
                                                        Version version,
                                                        int desiredHealthy,
                                                        int maxServers) {
	if (srcInterfs.size() >= maxServers) {
		return std::vector<UID>();
	}

	state std::vector<Future<Optional<UID>>> srcChecks;
	srcChecks.reserve(srcInterfs.size());
	for (int s = 0; s < srcInterfs.size(); s++) {
		srcChecks.push_back(checkReadWrite(srcInterfs[s].getShardState.getReplyUnlessFailedFor(
		                                       GetShardStateRequest(shard, GetShardStateRequest::NO_WAIT),
		                                       SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL,
		                                       0,
		                                       TaskPriority::MoveKeys),
		                                   srcInterfs[s].id(),
		                                   0));
	}

	state std::vector<Future<Optional<UID>>> destChecks;
	destChecks.reserve(destInterfs.size());
	for (int s = 0; s < destInterfs.size(); s++) {
		destChecks.push_back(checkReadWrite(destInterfs[s].getShardState.getReplyUnlessFailedFor(
		                                        GetShardStateRequest(shard, GetShardStateRequest::NO_WAIT),
		                                        SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL,
		                                        0,
		                                        TaskPriority::MoveKeys),
		                                    destInterfs[s].id(),
		                                    version));
	}

	wait(waitForAll(srcChecks) && waitForAll(destChecks));

	int healthySrcs = 0;
	for (auto it : srcChecks) {
		if (it.get().present()) {
			healthySrcs++;
		}
	}

	std::vector<UID> result;
	int totalDesired = std::min<int>(desiredHealthy - healthySrcs, maxServers - srcInterfs.size());
	for (int s = 0; s < destInterfs.size() && result.size() < totalDesired; s++) {
		if (destChecks[s].get().present()) {
			result.push_back(destChecks[s].get().get());
		}
	}

	return result;
}

// Returns storage servers selected from 'candidates', who is serving a read-write copy of 'range'.
ACTOR Future<std::vector<UID>> pickReadWriteServers(Transaction* tr, std::vector<UID> candidates, KeyRangeRef range) {
	std::vector<Future<Optional<Value>>> serverListEntries;

	for (const UID id : candidates) {
		serverListEntries.push_back(tr->get(serverListKeyFor(id)));
	}

	std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));

	std::vector<StorageServerInterface> ssis;
	for (auto& v : serverListValues) {
		ssis.push_back(decodeServerListValue(v.get()));
	}

	state std::vector<Future<Optional<UID>>> checks;
	checks.reserve(ssis.size());
	for (auto& ssi : ssis) {
		checks.push_back(checkReadWrite(
		    ssi.getShardState.getReplyUnlessFailedFor(GetShardStateRequest(range, GetShardStateRequest::NO_WAIT),
		                                              SERVER_KNOBS->SERVER_READY_QUORUM_INTERVAL,
		                                              0,
		                                              TaskPriority::MoveKeys),
		    ssi.id(),
		    0));
	}

	wait(waitForAll(checks));

	std::vector<UID> result;
	for (const auto& it : checks) {
		if (it.get().present()) {
			result.push_back(it.get().get());
		}
	}

	return result;
}

ACTOR Future<std::vector<std::vector<UID>>> additionalSources(RangeResult shards,
                                                              Reference<ReadYourWritesTransaction> tr,
                                                              int desiredHealthy,
                                                              int maxServers) {
	state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
	std::vector<Future<Optional<Value>>> serverListEntries;
	std::set<UID> fetching;
	for (int i = 0; i < shards.size() - 1; ++i) {
		std::vector<UID> src;
		std::vector<UID> dest;

		decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest);

		for (int s = 0; s < src.size(); s++) {
			if (!fetching.count(src[s])) {
				fetching.insert(src[s]);
				serverListEntries.push_back(tr->get(serverListKeyFor(src[s])));
			}
		}

		for (int s = 0; s < dest.size(); s++) {
			if (!fetching.count(dest[s])) {
				fetching.insert(dest[s]);
				serverListEntries.push_back(tr->get(serverListKeyFor(dest[s])));
			}
		}
	}

	std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));

	std::map<UID, StorageServerInterface> ssiMap;
	for (int s = 0; s < serverListValues.size(); s++) {
		StorageServerInterface ssi = decodeServerListValue(serverListValues[s].get());
		ssiMap[ssi.id()] = ssi;
	}

	std::vector<Future<std::vector<UID>>> allChecks;
	for (int i = 0; i < shards.size() - 1; ++i) {
		KeyRangeRef rangeIntersectKeys(shards[i].key, shards[i + 1].key);
		std::vector<UID> src;
		std::vector<UID> dest;
		std::vector<StorageServerInterface> srcInterfs;
		std::vector<StorageServerInterface> destInterfs;

		decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest);

		srcInterfs.reserve(src.size());
		for (int s = 0; s < src.size(); s++) {
			srcInterfs.push_back(ssiMap[src[s]]);
		}

		for (int s = 0; s < dest.size(); s++) {
			if (std::find(src.begin(), src.end(), dest[s]) == src.end()) {
				destInterfs.push_back(ssiMap[dest[s]]);
			}
		}

		allChecks.push_back(addReadWriteDestinations(
		    rangeIntersectKeys, srcInterfs, destInterfs, tr->getReadVersion().get(), desiredHealthy, maxServers));
	}

	std::vector<std::vector<UID>> result = wait(getAll(allChecks));
	return result;
}

ACTOR Future<Void> logWarningAfter(const char* context, double duration, std::vector<UID> servers) {
	state double startTime = now();
	loop {
		wait(delay(duration));
		TraceEvent(SevWarnAlways, context).detail("Duration", now() - startTime).detail("Servers", describe(servers));
	}
}

// keyServer: map from keys to destination servers
// serverKeys: two-dimension map: [servers][keys], value is the servers' state of having the keys: active(not-have),
// complete(already has), ""(). Set keyServers[keys].dest = servers Set serverKeys[servers][keys] = active for each
// subrange of keys that the server did not already have, complete for each subrange that it already has Set
// serverKeys[dest][keys] = "" for the dest servers of each existing shard in keys (unless that destination is a member
// of servers OR if the source list is sufficiently degraded)
ACTOR static Future<Void> startMoveKeys(Database occ,
                                        KeyRange keys,
                                        std::vector<UID> servers,
                                        MoveKeysLock lock,
                                        FlowLock* startMoveKeysLock,
                                        UID relocationIntervalId,
                                        std::map<UID, StorageServerInterface>* tssMapping,
                                        const DDEnabledState* ddEnabledState) {
	state TraceInterval interval("RelocateShard_StartMoveKeys");
	state Future<Void> warningLogger = logWarningAfter("StartMoveKeysTooLong", 600, servers);
	// state TraceInterval waitInterval("");

	wait(startMoveKeysLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser(*startMoveKeysLock);
	state bool loadedTssMapping = false;

	TraceEvent(SevDebug, interval.begin(), relocationIntervalId);

	try {
		state Key begin = keys.begin;
		state int batches = 0;
		state int shards = 0;
		state int maxRetries = 0;

		// If it's multiple transaction, how do we achieve atomicity?
		// This process can be split up into multiple transactions if there are too many existing overlapping shards
		// In that case, each iteration of this loop will have begin set to the end of the last processed shard
		while (begin < keys.end) {
			TEST(begin > keys.begin); // Multi-transactional startMoveKeys
			batches++;

			// RYW to optimize re-reading the same key ranges
			state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(occ);
			state int retries = 0;

			loop {
				try {
					retries++;

					// Keep track of old dests that may need to have ranges removed from serverKeys
					state std::set<UID> oldDests;

					// Keep track of shards for all src servers so that we can preserve their values in serverKeys
					state Map<UID, VectorRef<KeyRangeRef>> shardMap;

					tr->getTransaction().trState->taskID = TaskPriority::MoveKeys;
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					wait(checkMoveKeysLock(&(tr->getTransaction()), lock, ddEnabledState));

					if (!loadedTssMapping) {
						// share transaction for loading tss mapping with the rest of start move keys
						wait(readTSSMappingRYW(tr, tssMapping));
						loadedTssMapping = true;
					}

					std::vector<Future<Optional<Value>>> serverListEntries;
					serverListEntries.reserve(servers.size());
					for (int s = 0; s < servers.size(); s++)
						serverListEntries.push_back(tr->get(serverListKeyFor(servers[s])));
					state std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));

					for (int s = 0; s < serverListValues.size(); s++) {
						if (!serverListValues[s].present()) {
							// Attempt to move onto a server that isn't in serverList (removed or never added to the
							// database) This can happen (why?) and is handled by the data distribution algorithm
							// FIXME: Answer why this can happen?
							TEST(true); // start move keys moving to a removed server
							throw move_to_removed_server();
						}
					}

					// Get all existing shards overlapping keys (exclude any that have been processed in a previous
					// iteration of the outer loop)
					state KeyRange currentKeys = KeyRangeRef(begin, keys.end);

					state RangeResult old = wait(krmGetRanges(tr,
					                                          keyServersPrefix,
					                                          currentKeys,
					                                          SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
					                                          SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));

					// Determine the last processed key (which will be the beginning for the next iteration)
					state Key endKey = old.end()[-1].key;
					currentKeys = KeyRangeRef(currentKeys.begin, endKey);

					// TraceEvent("StartMoveKeysBatch", relocationIntervalId)
					//     .detail("KeyBegin", currentKeys.begin.toString())
					//     .detail("KeyEnd", currentKeys.end.toString());

					// printf("Moving '%s'-'%s' (%d) to %d servers\n", keys.begin.toString().c_str(),
					// keys.end.toString().c_str(), old.size(), servers.size()); for(int i=0; i<old.size(); i++)
					// 	printf("'%s': '%s'\n", old[i].key.toString().c_str(), old[i].value.toString().c_str());

					// Check that enough servers for each shard are in the correct state
					state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					std::vector<std::vector<UID>> addAsSource = wait(additionalSources(
					    old, tr, servers.size(), SERVER_KNOBS->MAX_ADDED_SOURCES_MULTIPLIER * servers.size()));

					// For each intersecting range, update keyServers[range] dest to be servers and clear existing dest
					// servers from serverKeys
					for (int i = 0; i < old.size() - 1; ++i) {
						KeyRangeRef rangeIntersectKeys(old[i].key, old[i + 1].key);
						std::vector<UID> src;
						std::vector<UID> dest;
						decodeKeyServersValue(UIDtoTagMap, old[i].value, src, dest);

						// TraceEvent("StartMoveKeysOldRange", relocationIntervalId)
						//     .detail("KeyBegin", rangeIntersectKeys.begin.toString())
						//     .detail("KeyEnd", rangeIntersectKeys.end.toString())
						//     .detail("OldSrc", describe(src))
						//     .detail("OldDest", describe(dest))
						//     .detail("ReadVersion", tr->getReadVersion().get());

						for (auto& uid : addAsSource[i]) {
							src.push_back(uid);
						}
						uniquify(src);

						// Update dest servers for this range to be equal to servers
						krmSetPreviouslyEmptyRange(&(tr->getTransaction()),
						                           keyServersPrefix,
						                           rangeIntersectKeys,
						                           keyServersValue(UIDtoTagMap, src, servers),
						                           old[i + 1].value);

						// Track old destination servers.  They may be removed from serverKeys soon, since they are
						// about to be overwritten in keyServers
						for (auto s = dest.begin(); s != dest.end(); ++s) {
							oldDests.insert(*s);
							// TraceEvent("StartMoveKeysOldDestAdd", relocationIntervalId).detail("Server", *s);
						}

						// Keep track of src shards so that we can preserve their values when we overwrite serverKeys
						for (auto& uid : src) {
							shardMap[uid].push_back(old.arena(), rangeIntersectKeys);
							// TraceEvent("StartMoveKeysShardMapAdd", relocationIntervalId).detail("Server", uid);
						}
					}

					state std::set<UID>::iterator oldDest;

					// Remove old dests from serverKeys.  In order for krmSetRangeCoalescing to work correctly in the
					// same prefix for a single transaction, we must do most of the coalescing ourselves.  Only the
					// shards on the boundary of currentRange are actually coalesced with the ranges outside of
					// currentRange. For all shards internal to currentRange, we overwrite all consecutive keys whose
					// value is or should be serverKeysFalse in a single write
					std::vector<Future<Void>> actors;
					for (oldDest = oldDests.begin(); oldDest != oldDests.end(); ++oldDest)
						if (std::find(servers.begin(), servers.end(), *oldDest) == servers.end())
							actors.push_back(removeOldDestinations(tr, *oldDest, shardMap[*oldDest], currentKeys));

					// Update serverKeys to include keys (or the currently processed subset of keys) for each SS in
					// servers
					for (int i = 0; i < servers.size(); i++) {
						// Since we are setting this for the entire range, serverKeys and keyServers aren't guaranteed
						// to have the same shard boundaries If that invariant was important, we would have to move this
						// inside the loop above and also set it for the src servers
						actors.push_back(krmSetRangeCoalescing(
						    tr, serverKeysPrefixFor(servers[i]), currentKeys, allKeys, serverKeysTrue));
					}

					wait(waitForAll(actors));

					wait(tr->commit());

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
					wait(tr->onError(e));

					if (retries % 10 == 0) {
						TraceEvent(
						    retries == 50 ? SevWarnAlways : SevWarn, "StartMoveKeysRetrying", relocationIntervalId)
						    .error(err)
						    .detail("Keys", keys)
						    .detail("BeginKey", begin)
						    .detail("NumTries", retries);
					}
				}
			}

			if (retries > maxRetries) {
				maxRetries = retries;
			}
		}

		// printf("Committed moving '%s'-'%s' (version %lld)\n", keys.begin.toString().c_str(),
		// keys.end.toString().c_str(), tr->getCommittedVersion());
		TraceEvent(SevDebug, interval.end(), relocationIntervalId)
		    .detail("Batches", batches)
		    .detail("Shards", shards)
		    .detail("MaxRetries", maxRetries);
	} catch (Error& e) {
		TraceEvent(SevDebug, interval.end(), relocationIntervalId).errorUnsuppressed(e);
		throw;
	}

	return Void();
}

ACTOR Future<Void> waitForShardReady(StorageServerInterface server,
                                     KeyRange keys,
                                     Version minVersion,
                                     GetShardStateRequest::waitMode mode) {
	loop {
		try {
			GetShardStateReply rep =
			    wait(server.getShardState.getReply(GetShardStateRequest(keys, mode), TaskPriority::MoveKeys));
			if (rep.first >= minVersion) {
				return Void();
			}
			wait(delayJittered(SERVER_KNOBS->SHARD_READY_DELAY, TaskPriority::MoveKeys));
		} catch (Error& e) {
			if (e.code() != error_code_timed_out) {
				if (e.code() != error_code_broken_promise)
					throw e;
				wait(Never()); // Never return: A storage server which has failed will never be ready
				throw internal_error(); // does not happen
			}
		}
	}
}

// best effort to also wait for TSS on data move

ACTOR Future<Void> checkFetchingState(Database cx,
                                      std::vector<UID> dest,
                                      KeyRange keys,
                                      Promise<Void> dataMovementComplete,
                                      UID relocationIntervalId,
                                      std::map<UID, StorageServerInterface> tssMapping) {
	state Transaction tr(cx);

	loop {
		try {
			if (BUGGIFY)
				wait(delay(5));

			tr.trState->taskID = TaskPriority::MoveKeys;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			std::vector<Future<Optional<Value>>> serverListEntries;
			serverListEntries.reserve(dest.size());
			for (int s = 0; s < dest.size(); s++)
				serverListEntries.push_back(tr.get(serverListKeyFor(dest[s])));
			state std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));
			std::vector<Future<Void>> requests;
			state std::vector<Future<Void>> tssRequests;
			for (int s = 0; s < serverListValues.size(); s++) {
				if (!serverListValues[s].present()) {
					// FIXME: Is this the right behavior?  dataMovementComplete will never be sent!
					TEST(true); // check fetching state moved to removed server
					throw move_to_removed_server();
				}
				auto si = decodeServerListValue(serverListValues[s].get());
				ASSERT(si.id() == dest[s]);
				requests.push_back(
				    waitForShardReady(si, keys, tr.getReadVersion().get(), GetShardStateRequest::FETCHING));

				auto tssPair = tssMapping.find(si.id());
				if (tssPair != tssMapping.end()) {
					tssRequests.push_back(waitForShardReady(
					    tssPair->second, keys, tr.getReadVersion().get(), GetShardStateRequest::FETCHING));
				}
			}

			wait(timeoutError(waitForAll(requests), SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT, TaskPriority::MoveKeys));

			// If normal servers return normally, give TSS data movement a bit of a chance, but don't block on it, and
			// ignore errors in tss requests
			if (tssRequests.size()) {
				wait(timeout(waitForAllReady(tssRequests),
				             SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT / 2,
				             Void(),
				             TaskPriority::MoveKeys));
			}

			dataMovementComplete.send(Void());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_timed_out)
				tr.reset();
			else
				wait(tr.onError(e));
		}
	}
}

// Set keyServers[keys].src = keyServers[keys].dest and keyServers[keys].dest=[], return when successful
// keyServers[k].dest must be the same for all k in keys
// Set serverKeys[dest][keys] = true; serverKeys[src][keys] = false for all src not in dest
// Should be cancelled and restarted if keyServers[keys].dest changes (?so this is no longer true?)
ACTOR static Future<Void> finishMoveKeys(Database occ,
                                         KeyRange keys,
                                         std::vector<UID> destinationTeam,
                                         MoveKeysLock lock,
                                         FlowLock* finishMoveKeysParallelismLock,
                                         bool hasRemote,
                                         UID relocationIntervalId,
                                         std::map<UID, StorageServerInterface> tssMapping,
                                         const DDEnabledState* ddEnabledState) {
	state TraceInterval interval("RelocateShard_FinishMoveKeys");
	state TraceInterval waitInterval("");
	state Future<Void> warningLogger = logWarningAfter("FinishMoveKeysTooLong", 600, destinationTeam);
	state Key begin = keys.begin;
	state Key endKey;
	state int retries = 0;
	state FlowLock::Releaser releaser;

	state std::unordered_set<UID> tssToIgnore;
	// try waiting for tss for a 2 loops, give up if they're behind to not affect the rest of the cluster
	state int waitForTSSCounter = 2;

	ASSERT(!destinationTeam.empty());

	try {
		TraceEvent(SevDebug, interval.begin(), relocationIntervalId)
		    .detail("KeyBegin", keys.begin)
		    .detail("KeyEnd", keys.end);

		// This process can be split up into multiple transactions if there are too many existing overlapping shards
		// In that case, each iteration of this loop will have begin set to the end of the last processed shard
		while (begin < keys.end) {
			TEST(begin > keys.begin); // Multi-transactional finishMoveKeys

			state Transaction tr(occ);

			// printf("finishMoveKeys( '%s'-'%s' )\n", begin.toString().c_str(), keys.end.toString().c_str());
			loop {
				try {

					tr.trState->taskID = TaskPriority::MoveKeys;
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					releaser.release();
					wait(finishMoveKeysParallelismLock->take(TaskPriority::DataDistributionLaunch));
					releaser = FlowLock::Releaser(*finishMoveKeysParallelismLock);

					wait(checkMoveKeysLock(&tr, lock, ddEnabledState));

					state KeyRange currentKeys = KeyRangeRef(begin, keys.end);
					state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					state RangeResult keyServers = wait(krmGetRanges(&tr,
					                                                 keyServersPrefix,
					                                                 currentKeys,
					                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
					                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));

					// Determine the last processed key (which will be the beginning for the next iteration)
					endKey = keyServers.end()[-1].key;
					currentKeys = KeyRangeRef(currentKeys.begin, endKey);

					// printf("  finishMoveKeys( '%s'-'%s' ): read keyServers at %lld\n", keys.begin.toString().c_str(),
					// keys.end.toString().c_str(), tr.getReadVersion().get());

					// Decode and sanity check the result (dest must be the same for all ranges)
					bool alreadyMoved = true;

					state std::vector<UID> dest;
					state std::set<UID> allServers;
					state std::set<UID> intendedTeam(destinationTeam.begin(), destinationTeam.end());
					state std::vector<UID> src;
					std::vector<UID> completeSrc;

					// Iterate through the beginning of keyServers until we find one that hasn't already been processed
					int currentIndex;
					for (currentIndex = 0; currentIndex < keyServers.size() - 1 && alreadyMoved; currentIndex++) {
						decodeKeyServersValue(UIDtoTagMap, keyServers[currentIndex].value, src, dest);

						std::set<UID> srcSet;
						for (int s = 0; s < src.size(); s++) {
							srcSet.insert(src[s]);
						}

						if (currentIndex == 0) {
							completeSrc = src;
						} else {
							for (int i = 0; i < completeSrc.size(); i++) {
								if (!srcSet.count(completeSrc[i])) {
									swapAndPop(&completeSrc, i--);
								}
							}
						}

						std::set<UID> destSet;
						for (int s = 0; s < dest.size(); s++) {
							destSet.insert(dest[s]);
						}

						allServers.insert(srcSet.begin(), srcSet.end());
						allServers.insert(destSet.begin(), destSet.end());

						// Because marking a server as failed can shrink a team, do not check for exact equality
						// Instead, check for a subset of the intended team, which also covers the equality case
						bool isSubset =
						    std::includes(intendedTeam.begin(), intendedTeam.end(), srcSet.begin(), srcSet.end());
						alreadyMoved = destSet.empty() && isSubset;
						if (destSet != intendedTeam && !alreadyMoved) {
							TraceEvent(SevWarn, "MoveKeysDestTeamNotIntended", relocationIntervalId)
							    .detail("KeyBegin", keys.begin)
							    .detail("KeyEnd", keys.end)
							    .detail("IterationBegin", begin)
							    .detail("IterationEnd", endKey)
							    .detail("SrcSet", describe(srcSet))
							    .detail("DestSet", describe(destSet))
							    .detail("IntendedTeam", describe(intendedTeam))
							    .detail("KeyServers", keyServers);
							// ASSERT( false );

							ASSERT(!dest.empty()); // The range has already been moved, but to a different dest (or
							                       // maybe dest was cleared)

							intendedTeam.clear();
							for (int i = 0; i < dest.size(); i++)
								intendedTeam.insert(dest[i]);
						} else if (alreadyMoved) {
							dest.clear();
							src.clear();
							TEST(true); // FinishMoveKeys first key in iteration sub-range has already been processed
						}
					}

					// Process the rest of the key servers
					for (; currentIndex < keyServers.size() - 1; currentIndex++) {
						std::vector<UID> src2, dest2;
						decodeKeyServersValue(UIDtoTagMap, keyServers[currentIndex].value, src2, dest2);

						std::set<UID> srcSet;
						for (int s = 0; s < src2.size(); s++)
							srcSet.insert(src2[s]);

						for (int i = 0; i < completeSrc.size(); i++) {
							if (!srcSet.count(completeSrc[i])) {
								swapAndPop(&completeSrc, i--);
							}
						}

						allServers.insert(srcSet.begin(), srcSet.end());

						// Because marking a server as failed can shrink a team, do not check for exact equality
						// Instead, check for a subset of the intended team, which also covers the equality case
						bool isSubset =
						    std::includes(intendedTeam.begin(), intendedTeam.end(), srcSet.begin(), srcSet.end());
						alreadyMoved = dest2.empty() && isSubset;
						if (dest2 != dest && !alreadyMoved) {
							TraceEvent(SevError, "FinishMoveKeysError", relocationIntervalId)
							    .detail("Reason", "dest mismatch")
							    .detail("Dest", describe(dest))
							    .detail("Dest2", describe(dest2));
							ASSERT(false);
						}
					}
					if (!dest.size()) {
						TEST(true); // A previous finishMoveKeys for this range committed just as it was cancelled to
						            // start this one?
						TraceEvent("FinishMoveKeysNothingToDo", relocationIntervalId)
						    .detail("KeyBegin", keys.begin)
						    .detail("KeyEnd", keys.end)
						    .detail("IterationBegin", begin)
						    .detail("IterationEnd", endKey);
						begin = keyServers.end()[-1].key;
						break;
					}

					waitInterval = TraceInterval("RelocateShard_FinishMoveKeysWaitDurable");
					TraceEvent(SevDebug, waitInterval.begin(), relocationIntervalId)
					    .detail("KeyBegin", keys.begin)
					    .detail("KeyEnd", keys.end);

					// Wait for a durable quorum of servers in destServers to have keys available (readWrite)
					// They must also have at least the transaction read version so they can't "forget" the shard
					// between now and when this transaction commits.
					state std::vector<Future<Void>> serverReady; // only for count below
					state std::vector<Future<Void>> tssReady; // for waiting in parallel with tss
					state std::vector<StorageServerInterface> tssReadyInterfs;
					state std::vector<UID> newDestinations;
					std::set<UID> completeSrcSet(completeSrc.begin(), completeSrc.end());
					for (auto& it : dest) {
						if (!hasRemote || !completeSrcSet.count(it)) {
							newDestinations.push_back(it);
						}
					}

					// for smartQuorum
					state std::vector<StorageServerInterface> storageServerInterfaces;
					std::vector<Future<Optional<Value>>> serverListEntries;
					serverListEntries.reserve(newDestinations.size());
					for (int s = 0; s < newDestinations.size(); s++)
						serverListEntries.push_back(tr.get(serverListKeyFor(newDestinations[s])));
					state std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));

					releaser.release();

					for (int s = 0; s < serverListValues.size(); s++) {
						ASSERT(serverListValues[s]
						           .present()); // There should always be server list entries for servers in keyServers
						auto si = decodeServerListValue(serverListValues[s].get());
						ASSERT(si.id() == newDestinations[s]);
						storageServerInterfaces.push_back(si);
					}

					// update client info in case tss mapping changed or server got updated

					// Wait for new destination servers to fetch the keys

					serverReady.reserve(storageServerInterfaces.size());
					tssReady.reserve(storageServerInterfaces.size());
					tssReadyInterfs.reserve(storageServerInterfaces.size());
					for (int s = 0; s < storageServerInterfaces.size(); s++) {
						serverReady.push_back(waitForShardReady(storageServerInterfaces[s],
						                                        keys,
						                                        tr.getReadVersion().get(),
						                                        GetShardStateRequest::READABLE));

						auto tssPair = tssMapping.find(storageServerInterfaces[s].id());

						if (tssPair != tssMapping.end() && waitForTSSCounter > 0 &&
						    !tssToIgnore.count(tssPair->second.id())) {
							tssReadyInterfs.push_back(tssPair->second);
							tssReady.push_back(waitForShardReady(
							    tssPair->second, keys, tr.getReadVersion().get(), GetShardStateRequest::READABLE));
						}
					}

					// Wait for all storage server moves, and explicitly swallow errors for tss ones with
					// waitForAllReady If this takes too long the transaction will time out and retry, which is ok
					wait(timeout(waitForAll(serverReady) && waitForAllReady(tssReady),
					             SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT,
					             Void(),
					             TaskPriority::MoveKeys));

					// Check to see if we're waiting only on tss. If so, decrement the waiting counter.
					// If the waiting counter is zero, ignore the slow/non-responsive tss processes before finalizing
					// the data move.
					if (tssReady.size()) {
						bool allSSDone = true;
						for (auto& f : serverReady) {
							allSSDone &= f.isReady() && !f.isError();
							if (!allSSDone) {
								break;
							}
						}

						if (allSSDone) {
							bool anyTssNotDone = false;

							for (auto& f : tssReady) {
								if (!f.isReady() || f.isError()) {
									anyTssNotDone = true;
									waitForTSSCounter--;
									break;
								}
							}

							if (anyTssNotDone && waitForTSSCounter == 0) {
								for (int i = 0; i < tssReady.size(); i++) {
									if (!tssReady[i].isReady() || tssReady[i].isError()) {
										tssToIgnore.insert(tssReadyInterfs[i].id());
									}
								}
							}
						}
					}

					int count = dest.size() - newDestinations.size();
					for (int s = 0; s < serverReady.size(); s++)
						count += serverReady[s].isReady() && !serverReady[s].isError();

					int tssCount = 0;
					for (int s = 0; s < tssReady.size(); s++)
						tssCount += tssReady[s].isReady() && !tssReady[s].isError();

					TraceEvent readyServersEv(SevDebug, waitInterval.end(), relocationIntervalId);
					readyServersEv.detail("ReadyServers", count);
					if (tssReady.size()) {
						readyServersEv.detail("ReadyTSS", tssCount);
					}

					if (count == dest.size()) {
						// update keyServers, serverKeys
						// SOMEDAY: Doing these in parallel is safe because none of them overlap or touch (one per
						// server)
						wait(krmSetRangeCoalescing(
						    &tr, keyServersPrefix, currentKeys, keys, keyServersValue(UIDtoTagMap, dest)));

						std::set<UID>::iterator asi = allServers.begin();
						std::vector<Future<Void>> actors;
						while (asi != allServers.end()) {
							bool destHasServer = std::find(dest.begin(), dest.end(), *asi) != dest.end();
							actors.push_back(krmSetRangeCoalescing(&tr,
							                                       serverKeysPrefixFor(*asi),
							                                       currentKeys,
							                                       allKeys,
							                                       destHasServer ? serverKeysTrue : serverKeysFalse));
							++asi;
						}

						wait(waitForAll(actors));
						wait(tr.commit());

						begin = endKey;
						break;
					}
					tr.reset();
				} catch (Error& error) {
					if (error.code() == error_code_actor_cancelled)
						throw;
					state Error err = error;
					wait(tr.onError(error));
					retries++;
					if (retries % 10 == 0) {
						TraceEvent(retries == 20 ? SevWarnAlways : SevWarn,
						           "RelocateShard_FinishMoveKeysRetrying",
						           relocationIntervalId)
						    .error(err)
						    .detail("KeyBegin", keys.begin)
						    .detail("KeyEnd", keys.end)
						    .detail("IterationBegin", begin)
						    .detail("IterationEnd", endKey);
					}
				}
			}
		}

		TraceEvent(SevDebug, interval.end(), relocationIntervalId);
	} catch (Error& e) {
		TraceEvent(SevDebug, interval.end(), relocationIntervalId).errorUnsuppressed(e);
		throw;
	}
	return Void();
}

ACTOR Future<std::pair<Version, Tag>> addStorageServer(Database cx, StorageServerInterface server) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	state KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(serverMetadataKeys.begin,
	                                                                                           IncludeVersion());

	state int maxSkipTags = 1;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			// FIXME: don't fetch tag localities, all tags, and history tags if tss. Just fetch pair's tag
			state Future<RangeResult> fTagLocalities = tr->getRange(tagLocalityListKeys, CLIENT_KNOBS->TOO_MANY);
			state Future<Optional<Value>> fv = tr->get(serverListKeyFor(server.id()));

			state Future<Optional<Value>> fExclProc = tr->get(
			    StringRef(encodeExcludedServersKey(AddressExclusion(server.address().ip, server.address().port))));
			state Future<Optional<Value>> fExclIP =
			    tr->get(StringRef(encodeExcludedServersKey(AddressExclusion(server.address().ip))));
			state Future<Optional<Value>> fFailProc = tr->get(
			    StringRef(encodeFailedServersKey(AddressExclusion(server.address().ip, server.address().port))));
			state Future<Optional<Value>> fFailIP =
			    tr->get(StringRef(encodeFailedServersKey(AddressExclusion(server.address().ip))));

			state Future<Optional<Value>> fExclProc2 =
			    server.secondaryAddress().present()
			        ? tr->get(StringRef(encodeExcludedServersKey(
			              AddressExclusion(server.secondaryAddress().get().ip, server.secondaryAddress().get().port))))
			        : Future<Optional<Value>>(Optional<Value>());
			state Future<Optional<Value>> fExclIP2 =
			    server.secondaryAddress().present()
			        ? tr->get(StringRef(encodeExcludedServersKey(AddressExclusion(server.secondaryAddress().get().ip))))
			        : Future<Optional<Value>>(Optional<Value>());
			state Future<Optional<Value>> fFailProc2 =
			    server.secondaryAddress().present()
			        ? tr->get(StringRef(encodeFailedServersKey(
			              AddressExclusion(server.secondaryAddress().get().ip, server.secondaryAddress().get().port))))
			        : Future<Optional<Value>>(Optional<Value>());
			state Future<Optional<Value>> fFailIP2 =
			    server.secondaryAddress().present()
			        ? tr->get(StringRef(encodeFailedServersKey(AddressExclusion(server.secondaryAddress().get().ip))))
			        : Future<Optional<Value>>(Optional<Value>());

			state std::vector<Future<Optional<Value>>> localityExclusions;
			std::map<std::string, std::string> localityData = server.locality.getAllData();
			for (const auto& l : localityData) {
				localityExclusions.push_back(tr->get(StringRef(encodeExcludedLocalityKey(
				    LocalityData::ExcludeLocalityPrefix.toString() + l.first + ":" + l.second))));
			}

			state Future<RangeResult> fTags = tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY, Snapshot::True);
			state Future<RangeResult> fHistoryTags =
			    tr->getRange(serverTagHistoryKeys, CLIENT_KNOBS->TOO_MANY, Snapshot::True);

			wait(success(fTagLocalities) && success(fv) && success(fTags) && success(fHistoryTags) &&
			     success(fExclProc) && success(fExclIP) && success(fFailProc) && success(fFailIP) &&
			     success(fExclProc2) && success(fExclIP2) && success(fFailProc2) && success(fFailIP2));

			for (const auto& exclusion : localityExclusions) {
				wait(success(exclusion));
			}

			// If we have been added to the excluded/failed state servers or localities list, we have to fail
			if (fExclProc.get().present() || fExclIP.get().present() || fFailProc.get().present() ||
			    fFailIP.get().present() || fExclProc2.get().present() || fExclIP2.get().present() ||
			    fFailProc2.get().present() || fFailIP2.get().present()) {
				throw recruitment_failed();
			}

			for (const auto& exclusion : localityExclusions) {
				if (exclusion.get().present()) {
					throw recruitment_failed();
				}
			}

			if (fTagLocalities.get().more || fTags.get().more || fHistoryTags.get().more)
				ASSERT(false);

			state Tag tag;
			if (server.isTss()) {
				bool foundTag = false;
				for (auto& it : fTags.get()) {
					UID key = decodeServerTagKey(it.key);
					if (key == server.tssPairID.get()) {
						tag = decodeServerTagValue(it.value);
						foundTag = true;
						break;
					}
				}
				if (!foundTag) {
					throw recruitment_failed();
				}

				tssMapDB.set(tr, server.tssPairID.get(), server.id());

			} else {
				int8_t maxTagLocality = 0;
				state int8_t locality = -1;
				for (auto& kv : fTagLocalities.get()) {
					int8_t loc = decodeTagLocalityListValue(kv.value);
					if (decodeTagLocalityListKey(kv.key) == server.locality.dcId()) {
						locality = loc;
						break;
					}
					maxTagLocality = std::max(maxTagLocality, loc);
				}

				if (locality == -1) {
					locality = maxTagLocality + 1;
					if (locality < 0) {
						throw recruitment_failed();
					}
					tr->set(tagLocalityListKeyFor(server.locality.dcId()), tagLocalityListValue(locality));
				}

				int skipTags = deterministicRandom()->randomInt(0, maxSkipTags);

				state uint16_t tagId = 0;
				std::vector<uint16_t> usedTags;
				for (auto& it : fTags.get()) {
					Tag t = decodeServerTagValue(it.value);
					if (t.locality == locality) {
						usedTags.push_back(t.id);
					}
				}
				for (auto& it : fHistoryTags.get()) {
					Tag t = decodeServerTagValue(it.value);
					if (t.locality == locality) {
						usedTags.push_back(t.id);
					}
				}
				std::sort(usedTags.begin(), usedTags.end());

				int usedIdx = 0;
				for (; usedTags.size() > 0 && tagId <= usedTags.end()[-1]; tagId++) {
					if (tagId < usedTags[usedIdx]) {
						if (skipTags == 0)
							break;
						skipTags--;
					} else {
						usedIdx++;
					}
				}
				tagId += skipTags;

				tag = Tag(locality, tagId);

				tr->set(serverTagKeyFor(server.id()), serverTagValue(tag));
				KeyRange conflictRange = singleKeyRange(serverTagConflictKeyFor(tag));
				tr->addReadConflictRange(conflictRange);
				tr->addWriteConflictRange(conflictRange);

				StorageMetadataType metadata(StorageMetadataType::currentTime());
				metadataMap.set(tr, server.id(), metadata);

				if (SERVER_KNOBS->TSS_HACK_IDENTITY_MAPPING) {
					// THIS SHOULD NEVER BE ENABLED IN ANY NON-TESTING ENVIRONMENT
					TraceEvent(SevError, "TSSIdentityMappingEnabled").log();
					tssMapDB.set(tr, server.id(), server.id());
				}
			}

			tr->set(serverListKeyFor(server.id()), serverListValue(server));
			wait(tr->commit());
			TraceEvent("AddedStorageServerSystemKey").detail("ServerID", server.id());

			return std::make_pair(tr->getCommittedVersion(), tag);
		} catch (Error& e) {
			if (e.code() == error_code_commit_unknown_result)
				throw recruitment_failed(); // There is a remote possibility that we successfully added ourselves and
				                            // then someone removed us, so we have to fail

			if (e.code() == error_code_not_committed) {
				maxSkipTags = SERVER_KNOBS->MAX_SKIP_TAGS;
			}

			wait(tr->onError(e));
		}
	}
}
// A SS can be removed only if all data (shards) on the SS have been moved away from the SS.
ACTOR Future<bool> canRemoveStorageServer(Reference<ReadYourWritesTransaction> tr, UID serverID) {
	RangeResult keys = wait(krmGetRanges(tr, serverKeysPrefixFor(serverID), allKeys, 2));

	ASSERT(keys.size() >= 2);

	if (keys[0].value == keys[1].value && keys[1].key != allKeys.end) {
		TraceEvent("ServerKeysCoalescingError", serverID)
		    .detail("Key1", keys[0].key)
		    .detail("Key2", keys[1].key)
		    .detail("Value", keys[0].value);
		ASSERT(false);
	}

	// Return true if the entire range is false.  Since these values are coalesced, we can return false if there is more
	// than one result
	return keys[0].value == serverKeysFalse && keys[1].key == allKeys.end;
}

ACTOR Future<Void> removeStorageServer(Database cx,
                                       UID serverID,
                                       Optional<UID> tssPairID,
                                       MoveKeysLock lock,
                                       const DDEnabledState* ddEnabledState) {
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	state KeyBackedObjectMap<UID, StorageMigrationType, decltype(IncludeVersion())> metadataMap(
	    serverMetadataKeys.begin, IncludeVersion());
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state bool retry = false;
	state int noCanRemoveCount = 0;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(checkMoveKeysLock(&(tr->getTransaction()), lock, ddEnabledState));
			TraceEvent("RemoveStorageServerLocked")
			    .detail("ServerID", serverID)
			    .detail("Version", tr->getReadVersion().get());

			state bool canRemove = wait(canRemoveStorageServer(tr, serverID));
			if (!canRemove) {
				TEST(true); // The caller had a transaction in flight that assigned keys to the server.  Wait for it to
				            // reverse its mistake.
				TraceEvent(SevWarn, "NoCanRemove").detail("Count", noCanRemoveCount++).detail("ServerID", serverID);
				wait(delayJittered(SERVER_KNOBS->REMOVE_RETRY_DELAY, TaskPriority::DataDistributionLaunch));
				tr->reset();
				TraceEvent("RemoveStorageServerRetrying").detail("CanRemove", canRemove);
			} else {
				state Future<Optional<Value>> fListKey = tr->get(serverListKeyFor(serverID));
				state Future<RangeResult> fTags = tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> fHistoryTags = tr->getRange(serverTagHistoryKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> fTagLocalities = tr->getRange(tagLocalityListKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> fTLogDatacenters = tr->getRange(tLogDatacentersKeys, CLIENT_KNOBS->TOO_MANY);

				wait(success(fListKey) && success(fTags) && success(fHistoryTags) && success(fTagLocalities) &&
				     success(fTLogDatacenters));

				if (!fListKey.get().present()) {
					if (retry) {
						TEST(true); // Storage server already removed after retrying transaction
						return Void();
					}
					TraceEvent(SevError, "RemoveInvalidServer").detail("ServerID", serverID);
					ASSERT(false); // Removing an already-removed server?  A never added server?
				}

				int8_t locality = -100;
				std::set<int8_t> allLocalities;
				for (auto& it : fTags.get()) {
					UID sId = decodeServerTagKey(it.key);
					Tag t = decodeServerTagValue(it.value);
					if (sId == serverID) {
						locality = t.locality;
					} else {
						allLocalities.insert(t.locality);
					}
				}
				for (auto& it : fHistoryTags.get()) {
					Tag t = decodeServerTagValue(it.value);
					allLocalities.insert(t.locality);
				}

				std::map<Optional<Value>, int8_t> dcId_locality;
				for (auto& kv : fTagLocalities.get()) {
					dcId_locality[decodeTagLocalityListKey(kv.key)] = decodeTagLocalityListValue(kv.value);
				}
				for (auto& it : fTLogDatacenters.get()) {
					allLocalities.insert(dcId_locality[decodeTLogDatacentersKey(it.key)]);
				}

				if (locality >= 0 && !allLocalities.count(locality)) {
					for (auto& it : fTagLocalities.get()) {
						if (locality == decodeTagLocalityListValue(it.value)) {
							tr->clear(it.key);
							break;
						}
					}
				}

				tr->clear(serverListKeyFor(serverID));
				tr->clear(serverTagKeyFor(serverID)); // A tss uses this to communicate shutdown but it never has a
				                                      // server tag key set in the first place
				tr->clear(serverTagHistoryRangeFor(serverID));

				if (SERVER_KNOBS->TSS_HACK_IDENTITY_MAPPING) {
					// THIS SHOULD NEVER BE ENABLED IN ANY NON-TESTING ENVIRONMENT
					TraceEvent(SevError, "TSSIdentityMappingEnabled").log();
					tssMapDB.erase(tr, serverID);
				} else if (tssPairID.present()) {
					// remove the TSS from the mapping
					tssMapDB.erase(tr, tssPairID.get());
					// remove the TSS from quarantine, if it was in quarantine
					Key tssQuarantineKey = tssQuarantineKeyFor(serverID);
					Optional<Value> tssInQuarantine = wait(tr->get(tssQuarantineKey));
					if (tssInQuarantine.present()) {
						tr->clear(tssQuarantineKeyFor(serverID));
					}
				}

				metadataMap.erase(tr, serverID);

				retry = true;
				wait(tr->commit());
				return Void();
			}
		} catch (Error& e) {
			state Error err = e;
			wait(tr->onError(e));
			TraceEvent("RemoveStorageServerRetrying").error(err);
		}
	}
}
// Remove the server from keyServer list and set serverKeysFalse to the server's serverKeys list.
// Changes to keyServer and serverKey must happen symmetrically in a transaction.
// If serverID is the last source server for a shard, the shard will be erased, and then be assigned
// to teamForDroppedRange.
ACTOR Future<Void> removeKeysFromFailedServer(Database cx,
                                              UID serverID,
                                              std::vector<UID> teamForDroppedRange,
                                              MoveKeysLock lock,
                                              const DDEnabledState* ddEnabledState) {
	state Key begin = allKeys.begin;

	state std::vector<UID> src;
	state std::vector<UID> dest;
	// Multi-transactional removal in case of large number of shards, concern in violating 5s transaction limit
	while (begin < allKeys.end) {
		state Transaction tr(cx);
		loop {
			try {
				tr.trState->taskID = TaskPriority::MoveKeys;
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(checkMoveKeysLock(&tr, lock, ddEnabledState));
				TraceEvent("RemoveKeysFromFailedServerLocked")
				    .detail("ServerID", serverID)
				    .detail("Version", tr.getReadVersion().get())
				    .detail("Begin", begin);
				// Get all values of keyServers and remove serverID from every occurrence
				// Very inefficient going over every entry in keyServers
				// No shortcut because keyServers and serverKeys are not guaranteed same shard boundaries
				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				state RangeResult keyServers = wait(krmGetRanges(&tr,
				                                                 keyServersPrefix,
				                                                 KeyRangeRef(begin, allKeys.end),
				                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
				                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
				state KeyRange currentKeys = KeyRangeRef(begin, keyServers.end()[-1].key);
				state int i = 0;
				for (; i < keyServers.size() - 1; ++i) {
					state KeyValueRef it = keyServers[i];
					decodeKeyServersValue(UIDtoTagMap, it.value, src, dest);

					// The failed server is not present
					if (std::find(src.begin(), src.end(), serverID) == src.end() &&
					    std::find(dest.begin(), dest.end(), serverID) == dest.end()) {
						continue;
					}

					// Update the vectors to remove failed server then set the value again
					// Dest is usually empty, but keep this in case there is parallel data movement
					src.erase(std::remove(src.begin(), src.end(), serverID), src.end());
					dest.erase(std::remove(dest.begin(), dest.end(), serverID), dest.end());

					// If the last src server is to be removed, first check if there are dest servers who is
					// hosting a read-write copy of the keyrange, and move such dest servers to the src list.
					if (src.empty() && !dest.empty()) {
						std::vector<UID> newSources =
						    wait(pickReadWriteServers(&tr, dest, KeyRangeRef(it.key, keyServers[i + 1].key)));
						for (const UID& id : newSources) {
							TraceEvent(SevWarn, "FailedServerAdditionalSourceServer", serverID)
							    .detail("Key", it.key)
							    .detail("NewSourceServerFromDest", id);
							dest.erase(std::remove(dest.begin(), dest.end(), id), dest.end());
							src.push_back(id);
						}
					}

					// Move the keyrange to teamForDroppedRange if the src list becomes empty, and also remove the shard
					// from all dest servers.
					if (src.empty()) {
						if (teamForDroppedRange.empty()) {
							TraceEvent(SevError, "ShardLossAllReplicasNoDestinationTeam", serverID)
							    .detail("Begin", it.key)
							    .detail("End", keyServers[i + 1].key);
							throw internal_error();
						}

						// Assign the shard to teamForDroppedRange in keyServer space.
						tr.set(keyServersKey(it.key), keyServersValue(UIDtoTagMap, teamForDroppedRange, {}));

						std::vector<Future<Void>> actors;

						// Unassign the shard from the dest servers.
						for (const UID& id : dest) {
							actors.push_back(krmSetRangeCoalescing(&tr,
							                                       serverKeysPrefixFor(id),
							                                       KeyRangeRef(it.key, keyServers[i + 1].key),
							                                       allKeys,
							                                       serverKeysFalse));
						}

						// Assign the shard to the new team as an empty range.
						// Note, there could be data loss.
						for (const UID& id : teamForDroppedRange) {
							actors.push_back(krmSetRangeCoalescing(&tr,
							                                       serverKeysPrefixFor(id),
							                                       KeyRangeRef(it.key, keyServers[i + 1].key),
							                                       allKeys,
							                                       serverKeysTrueEmptyRange));
						}

						wait(waitForAll(actors));

						TraceEvent trace(SevWarnAlways, "ShardLossAllReplicasDropShard", serverID);
						trace.detail("Begin", it.key);
						trace.detail("End", keyServers[i + 1].key);
						if (!dest.empty()) {
							trace.detail("DropedDest", describe(dest));
						}
						trace.detail("NewTeamForDroppedShard", describe(teamForDroppedRange));
					} else {
						TraceEvent(SevDebug, "FailedServerSetKey", serverID)
						    .detail("Key", it.key)
						    .detail("ValueSrc", describe(src))
						    .detail("ValueDest", describe(dest));
						tr.set(keyServersKey(it.key), keyServersValue(UIDtoTagMap, src, dest));
					}
				}

				// Set entire range for our serverID in serverKeys keyspace to false to signal erasure
				TraceEvent(SevDebug, "FailedServerSetRange", serverID)
				    .detail("Begin", currentKeys.begin)
				    .detail("End", currentKeys.end);
				wait(krmSetRangeCoalescing(&tr, serverKeysPrefixFor(serverID), currentKeys, allKeys, serverKeysFalse));
				wait(tr.commit());
				TraceEvent(SevDebug, "FailedServerCommitSuccess", serverID)
				    .detail("Begin", currentKeys.begin)
				    .detail("End", currentKeys.end)
				    .detail("CommitVersion", tr.getCommittedVersion());
				// Update beginning of next iteration's range
				begin = currentKeys.end;
				break;
			} catch (Error& e) {
				TraceEvent("FailedServerError", serverID).error(e);
				wait(tr.onError(e));
			}
		}
	}
	return Void();
}

ACTOR Future<Void> moveKeys(Database cx,
                            KeyRange keys,
                            std::vector<UID> destinationTeam,
                            std::vector<UID> healthyDestinations,
                            MoveKeysLock lock,
                            Promise<Void> dataMovementComplete,
                            FlowLock* startMoveKeysParallelismLock,
                            FlowLock* finishMoveKeysParallelismLock,
                            bool hasRemote,
                            UID relocationIntervalId,
                            const DDEnabledState* ddEnabledState) {
	ASSERT(destinationTeam.size());
	std::sort(destinationTeam.begin(), destinationTeam.end());

	state std::map<UID, StorageServerInterface> tssMapping;

	wait(startMoveKeys(cx,
	                   keys,
	                   destinationTeam,
	                   lock,
	                   startMoveKeysParallelismLock,
	                   relocationIntervalId,
	                   &tssMapping,
	                   ddEnabledState));

	state Future<Void> completionSignaller =
	    checkFetchingState(cx, healthyDestinations, keys, dataMovementComplete, relocationIntervalId, tssMapping);

	wait(finishMoveKeys(cx,
	                    keys,
	                    destinationTeam,
	                    lock,
	                    finishMoveKeysParallelismLock,
	                    hasRemote,
	                    relocationIntervalId,
	                    tssMapping,
	                    ddEnabledState));

	// This is defensive, but make sure that we always say that the movement is complete before moveKeys completes
	completionSignaller.cancel();
	if (!dataMovementComplete.isSet())
		dataMovementComplete.send(Void());

	return Void();
}

// Called by the master server to write the very first transaction to the database
// establishing a set of shard servers and all invariants of the systemKeys.
void seedShardServers(Arena& arena, CommitTransactionRef& tr, std::vector<StorageServerInterface> servers) {
	std::map<Optional<Value>, Tag> dcId_locality;
	std::map<UID, Tag> server_tag;
	int8_t nextLocality = 0;
	for (auto& s : servers) {
		if (!dcId_locality.count(s.locality.dcId())) {
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
	tr.read_conflict_ranges.push_back_deep(arena, allKeys);
	KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(serverMetadataKeys.begin,
	                                                                                     IncludeVersion());
	StorageMetadataType metadata(StorageMetadataType::currentTime());

	for (auto& s : servers) {
		tr.set(arena, serverTagKeyFor(s.id()), serverTagValue(server_tag[s.id()]));
		tr.set(arena, serverListKeyFor(s.id()), serverListValue(s));
		tr.set(arena, metadataMap.serializeKey(s.id()), metadataMap.serializeValue(metadata));

		if (SERVER_KNOBS->TSS_HACK_IDENTITY_MAPPING) {
			// THIS SHOULD NEVER BE ENABLED IN ANY NON-TESTING ENVIRONMENT
			TraceEvent(SevError, "TSSIdentityMappingEnabled").log();
			// hack key-backed map here since we can't really change CommitTransactionRef to a RYW transaction
			Key uidRef = Codec<UID>::pack(s.id()).pack();
			tr.set(arena, uidRef.withPrefix(tssMappingKeys.begin), uidRef);
		}
	}

	std::vector<Tag> serverTags;
	std::vector<UID> serverSrcUID;
	serverTags.reserve(servers.size());
	for (auto& s : servers) {
		serverTags.push_back(server_tag[s.id()]);
		serverSrcUID.push_back(s.id());
	}

	auto ksValue = CLIENT_KNOBS->TAG_ENCODE_KEY_SERVERS ? keyServersValue(serverTags)
	                                                    : keyServersValue(RangeResult(), serverSrcUID);
	// We have to set this range in two blocks, because the master tracking of "keyServersLocations" depends on a change
	// to a specific
	//   key (keyServersKeyServersKey)
	krmSetPreviouslyEmptyRange(tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), ksValue, Value());

	for (auto& s : servers) {
		krmSetPreviouslyEmptyRange(tr, arena, serverKeysPrefixFor(s.id()), allKeys, serverKeysTrue, serverKeysFalse);
	}
}
