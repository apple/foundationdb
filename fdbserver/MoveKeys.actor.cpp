/*
 * MoveKeys.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include <limits.h>

#include "fdbclient/BlobRestoreCommon.h"
#include "fdbclient/FDBOptions.g.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/TSSMappingUtil.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

template <typename... T>
static inline void dprint(fmt::format_string<T...> fmt, T&&... args) {
	if (g_network->isSimulated())
		fmt::print(fmt, std::forward<T>(args)...);
}

namespace {
struct Shard {
	Shard() = default;
	Shard(KeyRangeRef range, const UID& id) : range(range), id(id) {}

	KeyRange range;
	UID id;
};

bool shouldCreateCheckpoint(const UID& dataMoveId) {
	bool assigned, emptyRange;
	DataMoveType type;
	DataMovementReason reason;
	decodeDataMoveId(dataMoveId, assigned, emptyRange, type, reason);
	return (type == DataMoveType::PHYSICAL || type == DataMoveType::PHYSICAL_EXP);
}

// Unassigns keyrange `range` from server `ssId`, except ranges in `shards`.
// Note: krmSetRangeCoalescing() doesn't work in this case since each shard is assigned an ID.
ACTOR Future<Void> unassignServerKeys(Transaction* tr, UID ssId, KeyRange range, std::vector<Shard> shards, UID logId) {
	state Key mapPrefix = serverKeysPrefixFor(ssId);
	if (shards.empty()) {
		wait(krmSetRangeCoalescing(tr, mapPrefix, range, allKeys, serverKeysFalse));
		return Void();
	}

	state KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + range.begin.toString(), mapPrefix.toString() + range.end.toString());
	state KeyRange maxWithPrefix =
	    KeyRangeRef(mapPrefix.toString() + allKeys.begin.toString(), mapPrefix.toString() + allKeys.end.toString());

	state std::vector<Future<RangeResult>> keys;
	keys.push_back(
	    tr->getRange(lastLessThan(withPrefix.begin), firstGreaterOrEqual(withPrefix.begin), 1, Snapshot::True));
	keys.push_back(
	    tr->getRange(lastLessOrEqual(withPrefix.end), firstGreaterThan(withPrefix.end) + 1, 2, Snapshot::True));
	wait(waitForAll(keys));

	// Determine how far to extend this range at the beginning
	auto beginRange = keys[0].get();
	bool hasBegin = beginRange.size() > 0 && beginRange[0].key.startsWith(mapPrefix);
	Value beginValue = hasBegin ? beginRange[0].value : serverKeysFalse;

	Key beginKey = withPrefix.begin;
	Value value = range.begin == shards[0].range.begin ? serverKeysValue(shards[0].id) : serverKeysFalse;
	if (beginValue == value) {
		bool outsideRange = !hasBegin || beginRange[0].key < maxWithPrefix.begin;
		beginKey = outsideRange ? maxWithPrefix.begin : beginRange[0].key;
	}

	std::vector<KeyValue> kvs;
	if (beginKey < withPrefix.begin) {
		kvs.push_back(KeyValueRef(beginKey, value));
	}
	Key preEnd = range.begin;
	for (int i = 0; i < shards.size(); ++i) {
		const Shard& shard = shards[i];
		if (shard.range.begin > preEnd && (kvs.empty() || kvs.back().value != serverKeysFalse)) {
			kvs.push_back(KeyValueRef(preEnd.withPrefix(mapPrefix), serverKeysFalse));
		}
		preEnd = shard.range.end;
		Value cv = serverKeysValue(shard.id);
		if (kvs.empty() || cv != kvs.back().value) {
			kvs.push_back(KeyValueRef(shard.range.begin.withPrefix(mapPrefix), cv));
		}
	}
	if (range.end > preEnd) {
		kvs.push_back(KeyValueRef(preEnd.withPrefix(mapPrefix), serverKeysFalse));
	}

	// Determine how far to extend this range at the end
	auto endRange = keys[1].get();
	bool hasEnd = endRange.size() >= 1 && endRange[0].key.startsWith(mapPrefix) && endRange[0].key <= withPrefix.end;
	bool hasNext = (endRange.size() == 2 && endRange[1].key.startsWith(mapPrefix)) ||
	               (endRange.size() == 1 && withPrefix.end < endRange[0].key && endRange[0].key.startsWith(mapPrefix));
	Value existingValue = hasEnd ? endRange[0].value : serverKeysFalse;

	Key endKey;
	Value endValue;
	const bool valueMatches = kvs.back().value == existingValue;

	// Case 1: Coalesce completely with the following range
	if (hasNext && endRange.back().key <= maxWithPrefix.end && valueMatches) {
		endKey = endRange.back().key;
		endValue = endRange.back().value;
	}

	// Case 2: Coalesce with the following range only up to the end of allKeys
	else if (valueMatches) {
		endKey = maxWithPrefix.end;
		endValue = existingValue;
	}

	// Case 3: Don't coalesce
	else {
		endKey = withPrefix.end;
		endValue = existingValue;
	}

	kvs.push_back(KeyValueRef(endKey, endValue));

	for (int i = 0; i < kvs.size(); ++i) {
		TraceEvent(SevDebug, "UnassignServerKeys", logId)
		    .detail("SSID", ssId)
		    .detail("Range", range)
		    .detail("Point", kvs[i]);
	}

	KeyRange conflictRange = KeyRangeRef(hasBegin ? beginRange[0].key : mapPrefix, withPrefix.begin);
	if (!conflictRange.empty()) {
		tr->addReadConflictRange(conflictRange);
	}
	conflictRange = KeyRangeRef(hasEnd ? endRange[0].key : mapPrefix,
	                            hasNext ? keyAfter(endRange.end()[-1].key) : strinc(mapPrefix));
	if (!conflictRange.empty()) {
		tr->addReadConflictRange(conflictRange);
	}

	tr->clear(KeyRangeRef(beginKey, endKey));

	for (int i = 0; i < kvs.size() - 1; ++i) {
		ASSERT(kvs[i].value != kvs[i + 1].value || kvs[i + 1].key.removePrefix(mapPrefix) == allKeys.end);
		tr->set(kvs[i].key, kvs[i].value);
		tr->set(kvs[i + 1].key, kvs[i + 1].value);
	}

	return Void();
}

ACTOR Future<Void> deleteCheckpoints(Transaction* tr, std::set<UID> checkpointIds, UID dataMoveId) {
	if (!shouldCreateCheckpoint(dataMoveId)) {
		return Void();
	}
	TraceEvent(SevDebug, "DataMoveDeleteCheckpoints", dataMoveId).detail("Checkpoints", describe(checkpointIds));
	std::vector<Future<Optional<Value>>> checkpointEntries;
	for (const UID& id : checkpointIds) {
		checkpointEntries.push_back(tr->get(checkpointKeyFor(id)));
	}
	std::vector<Optional<Value>> checkpointValues = wait(getAll(checkpointEntries));

	for (int i = 0; i < checkpointIds.size(); ++i) {
		const auto& value = checkpointValues[i];
		if (!value.present()) {
			TraceEvent(SevWarnAlways, "CheckpointNotFound", dataMoveId);
			continue;
		}
		CheckpointMetaData checkpoint = decodeCheckpointValue(value.get());
		ASSERT(checkpointIds.find(checkpoint.checkpointID) != checkpointIds.end());
		const Key key = checkpointKeyFor(checkpoint.checkpointID);
		checkpoint.setState(CheckpointMetaData::Deleting);
		tr->set(key, checkpointValue(checkpoint));
		tr->clear(singleKeyRange(key));
		TraceEvent(SevDebug, "DataMoveDeleteCheckpoint", dataMoveId).detail("Checkpoint", checkpoint.toString());
	}

	return Void();
}
} // namespace

bool DDEnabledState::sameId(const UID& id) const {
	return ddEnabledStatusUID == id;
}
bool DDEnabledState::isEnabled() const {
	return stateValue == ENABLED;
}

bool DDEnabledState::isBlobRestorePreparing() const {
	return stateValue == BLOB_RESTORE_PREPARING;
}

bool DDEnabledState::trySetSnapshot(UID requesterId) {
	ASSERT(requesterId != UID());
	// disabling DD
	if (!isEnabled()) {
		// only allow state modification to snapshot when DD is enabled.
		return false;
	}
	ddEnabledStatusUID = requesterId;
	stateValue = SNAPSHOT;
	TraceEvent("SetDDSnapshot").detail("RequesterUID", requesterId);

	return true;
}

bool DDEnabledState::trySetEnabled(UID requesterId) {
	ASSERT(requesterId != UID());
	// enabling DD
	if (!sameId(requesterId)) {
		// enabling DD not allowed if UID does not match with the previous request
		return false;
	}
	// reset to default status
	ddEnabledStatusUID = UID();
	stateValue = ENABLED;
	TraceEvent("SetDDEnabled").detail("RequesterUID", requesterId);
	return true;
}

bool DDEnabledState::trySetBlobRestorePreparing(UID requesterId) {
	ASSERT(requesterId != UID());
	if (!isEnabled()) {
		// only allow state modification to RestorePreparing when DD is enabled.
		return false;
	}
	ddEnabledStatusUID = requesterId;
	stateValue = BLOB_RESTORE_PREPARING;
	TraceEvent("SetDDBlobRestorePreparing").detail("RequesterUID", requesterId);
	return true;
}

ACTOR Future<Void> readMoveKeysLock(Transaction* tr, MoveKeysLock* lock) {
	{
		Optional<Value> readVal = wait(tr->get(moveKeysLockOwnerKey));
		lock->prevOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
	}
	{
		Optional<Value> readVal = wait(tr->get(moveKeysLockWriteKey));
		lock->prevWrite = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
	}
	return Void();
}

ACTOR Future<MoveKeysLock> readMoveKeysLock(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			state MoveKeysLock lock;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(readMoveKeysLock(&tr, &lock));
			return lock;
		} catch (Error& e) {
			wait(tr.onError(e));
			CODE_PROBE(true, "readMoveKeysLock retry");
		}
	}
}

ACTOR Future<MoveKeysLock> takeMoveKeysLock(Database cx, UID ddId) {
	state Transaction tr(cx);
	loop {
		try {
			state MoveKeysLock lock;
			state UID txnId;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			if (!g_network->isSimulated()) {
				txnId = deterministicRandom()->randomUniqueID();
				tr.debugTransaction(txnId);
			}
			wait(readMoveKeysLock(&tr, &lock));
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
			CODE_PROBE(true, "takeMoveKeysLock retry");
		}
	}
}

ACTOR static Future<Void> checkPersistentMoveKeysLock(Transaction* tr, MoveKeysLock lock, bool isWrite = true) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

	Optional<Value> readVal = wait(tr->get(moveKeysLockOwnerKey));
	state UID currentOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();

	if (currentOwner == lock.prevOwner) {
		// Check that the previous owner hasn't touched the lock since we took it
		Optional<Value> readVal = wait(tr->get(moveKeysLockWriteKey));
		UID lastWrite = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
		if (lastWrite != lock.prevWrite) {
			CODE_PROBE(true, "checkMoveKeysLock: Conflict with previous owner");
			TraceEvent(SevDebug, "CheckPersistentMoveKeysWritterConflict")
			    .errorUnsuppressed(movekeys_conflict())
			    .detail("PrevOwner", lock.prevOwner.toString())
			    .detail("PrevWrite", lock.prevWrite.toString())
			    .detail("MyOwner", lock.myOwner.toString())
			    .detail("CurrentOwner", currentOwner.toString())
			    .detail("Writer", lastWrite.toString());
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
		CODE_PROBE(true, "checkMoveKeysLock: Conflict with new owner");
		TraceEvent(SevDebug, "CheckPersistentMoveKeysLockOwnerConflict")
		    .errorUnsuppressed(movekeys_conflict())
		    .detail("PrevOwner", lock.prevOwner.toString())
		    .detail("PrevWrite", lock.prevWrite.toString())
		    .detail("MyOwner", lock.myOwner.toString())
		    .detail("CurrentOwner", currentOwner.toString());
		throw movekeys_conflict();
	}
}

Future<Void> checkMoveKeysLock(Transaction* tr,
                               MoveKeysLock const& lock,
                               const DDEnabledState* ddEnabledState,
                               bool isWrite) {
	if (!ddEnabledState->isEnabled()) {
		TraceEvent(SevDebug, "DDDisabledByInMemoryCheck").log();
		throw movekeys_conflict();
	}
	return checkPersistentMoveKeysLock(tr, lock, isWrite);
}

Future<Void> checkMoveKeysLockReadOnly(Transaction* tr, MoveKeysLock lock, const DDEnabledState* ddEnabledState) {
	return checkMoveKeysLock(tr, lock, ddEnabledState, false);
}

namespace {
ACTOR Future<Optional<UID>> checkReadWrite(Future<ErrorOr<GetShardStateReply>> fReply, UID uid, Version version) {
	ErrorOr<GetShardStateReply> reply = wait(fReply);
	if (!reply.present() || reply.get().first < version)
		return Optional<UID>();
	return Optional<UID>(uid);
}

// Must propagate corruption signal to outside
ACTOR Future<bool> validateRangeAssignment(Database occ,
                                           Transaction* tr,
                                           KeyRange range,
                                           UID ssid,
                                           std::string context,
                                           UID dataMoveId) {
	ASSERT(!range.empty());
	state Key toReadRangeBegin = range.begin;
	state bool allCorrect = true;
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	loop {
		RangeResult readResult = wait(krmGetRanges(tr,
		                                           serverKeysPrefixFor(ssid),
		                                           KeyRangeRef(toReadRangeBegin, range.end),
		                                           CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
		                                           CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
		// We do not want to reset tr
		for (int i = 0; i < readResult.size() - 1; i++) {
			UID shardId;
			bool assigned, emptyRange;
			DataMoveType dataMoveType = DataMoveType::LOGICAL;
			DataMovementReason dataMoveReason = DataMovementReason::INVALID;
			decodeServerKeysValue(readResult[i].value, assigned, emptyRange, dataMoveType, shardId, dataMoveReason);
			if (!assigned) {
				TraceEvent(SevError, "ValidateRangeAssignmentCorruptionDetected")
				    .setMaxFieldLength(-1)
				    .setMaxEventLength(-1)
				    .detail("DataMoveID", dataMoveId)
				    .detail("Context", context)
				    .detail("AuditRange", range)
				    .detail("ErrorMessage", "KeyServers has range but ServerKeys does not have")
				    .detail("CurrentEmptyRange", emptyRange)
				    .detail("CurrentAssignment", assigned)
				    .detail("DataMoveType", static_cast<uint8_t>(dataMoveType))
				    .detail("ServerID", ssid)
				    .detail("ShardID", shardId);
				allCorrect = false;
			}
		}
		if (!allCorrect) {
			break; // Stop checking even for incomplete read
		}
		if (readResult.back().key < range.end) {
			toReadRangeBegin = readResult.back().key;
			TraceEvent(SevWarnAlways, "ValidateRangeAssignmentMultipleReads")
			    .detail("DataMoveID", dataMoveId)
			    .detail("Range", range)
			    .detail("StorageServer", ssid);
			continue;
		} else {
			break;
		}
	}
	if (!allCorrect) {
		try {
			// If corruption detected, enter security mode which
			// stops using data moves and only allow auditStorage
			wait(success(setDDMode(occ, 2)));
			TraceEvent(SevInfo, "ValidateRangeAssignmentCorruptionDetectedAndDDStopped")
			    .detail("DataMoveID", dataMoveId)
			    .detail("Range", range)
			    .detail("StorageServer", ssid);
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "ValidateRangeAssignmentCorruptionDetectedButFailedToStopDD")
			    .detail("DataMoveID", dataMoveId)
			    .detail("Range", range)
			    .detail("StorageServer", ssid);
			// We do not want failure of setDDMode hide the corruption signal
		}
	}
	return allCorrect;
}

ACTOR Future<Void> auditLocationMetadataPreCheck(Database occ,
                                                 Transaction* tr,
                                                 KeyRange range,
                                                 std::vector<UID> servers,
                                                 std::string context,
                                                 UID dataMoveId) {
	if (range.empty()) {
		TraceEvent(SevWarn, "CheckLocationMetadataEmptyInputRange").detail("By", "PreCheck").detail("Range", range);
		return Void();
	}
	state std::vector<Future<Void>> actors;
	state std::unordered_map<UID, Optional<bool>> results;
	TraceEvent(SevVerbose, "CheckLocationMetadataStart")
	    .detail("By", "PreCheck")
	    .detail("DataMoveID", dataMoveId)
	    .detail("Servers", describe(servers))
	    .detail("Context", context)
	    .detail("Range", range);
	try {
		actors.clear();
		results.clear();
		for (const auto& ssid : servers) {
			actors.push_back(store(results[ssid], validateRangeAssignment(occ, tr, range, ssid, context, dataMoveId)));
		}
		wait(waitForAllReadyThenThrow(actors));
		for (const auto& [ssid, res] : results) {
			ASSERT(res.present());
			if (!res.get()) { // Stop check if corruption detected
				TraceEvent(SevError, "CheckLocationMetadataCorruptionDetected")
				    .detail("By", "PreCheck")
				    .detail("DataMoveID", dataMoveId)
				    .detail("Servers", describe(servers))
				    .detail("Context", context)
				    .detail("Range", range);
				throw location_metadata_corruption();
			}
		}
		TraceEvent(SevVerbose, "CheckLocationMetadataComplete")
		    .detail("By", "PreCheck")
		    .detail("DataMoveID", dataMoveId)
		    .detail("Servers", describe(servers))
		    .detail("Context", context)
		    .detail("Range", range);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_location_metadata_corruption) {
			throw e;
		} else {
			TraceEvent(SevInfo, "CheckLocationMetadataFailed")
			    .errorUnsuppressed(e)
			    .detail("By", "PreCheck")
			    .detail("DataMoveID", dataMoveId)
			    .detail("Context", context)
			    .detail("Range", range);
			// Check any existing result when failure presents
			for (const auto& [ssid, res] : results) {
				if (res.present() && !res.get()) {
					TraceEvent(SevError, "CheckLocationMetadataCorruptionDetectedWhenFailed")
					    .detail("By", "PreCheck")
					    .detail("DataMoveID", dataMoveId)
					    .detail("Servers", describe(servers))
					    .detail("Context", context)
					    .detail("Range", range);
					throw location_metadata_corruption();
				}
			}
			// If no corruption detected, exit silently
		}
	}
	return Void();
}

ACTOR Future<Void> auditLocationMetadataPostCheck(Database occ, KeyRange range, std::string context, UID dataMoveId) {
	if (range.empty()) {
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "CheckLocationMetadataEmptyInputRange")
		    .detail("By", "PostCheck")
		    .detail("Range", range)
		    .detail("Context", context)
		    .detail("DataMoveId", dataMoveId.toString());
		return Void();
	}
	state std::vector<Future<Void>> actors;
	state std::unordered_map<uint64_t, Optional<bool>> results;
	state Key rangeToReadBegin = range.begin;
	state RangeResult readResultKS;
	state RangeResult UIDtoTagMap;
	state Transaction tr(occ);
	state int retryCount = 0;
	TraceEvent(SevVerbose, "CheckLocationMetadataStart")
	    .detail("By", "PostCheck")
	    .detail("Context", context)
	    .detail("Range", range);
	loop {
		try {
			loop {
				try {
					actors.clear();
					readResultKS.clear();
					results.clear();
					tr.trState->taskID = TaskPriority::MoveKeys;
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					// Read keyServers
					actors.push_back(store(readResultKS,
					                       krmGetRanges(&tr,
					                                    keyServersPrefix,
					                                    KeyRangeRef(rangeToReadBegin, range.end),
					                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
					                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES)));
					actors.push_back(store(UIDtoTagMap, tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY)));
					wait(waitForAll(actors));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					TraceEvent(SevVerbose, "CheckLocationMetadataReadDone")
					    .detail("By", "PostCheck")
					    .detail("ResultSize", readResultKS.size());
					// Read serverKeys
					actors.clear();
					state uint64_t resIdx = 0;
					for (int i = 0; i < readResultKS.size() - 1; ++i) {
						std::vector<UID> src;
						std::vector<UID> dest;
						UID srcID;
						UID destID;
						decodeKeyServersValue(UIDtoTagMap, readResultKS[i].value, src, dest, srcID, destID);
						std::vector<UID> servers(src.size() + dest.size());
						std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
						for (const auto& ssid : servers) {
							actors.push_back(
							    store(results[resIdx],
							          validateRangeAssignment(occ,
							                                  &tr,
							                                  KeyRangeRef(readResultKS[i].key, readResultKS[i + 1].key),
							                                  ssid,
							                                  context,
							                                  dataMoveId)));
							++resIdx;
						}
					}
					wait(waitForAllReadyThenThrow(actors));
					for (const auto& [idx, res] : results) {
						ASSERT(res.present());
						if (!res.get()) { // Stop check if corruption detected
							throw location_metadata_corruption();
						}
					}
					if (readResultKS.back().key < range.end) {
						rangeToReadBegin = readResultKS.back().key;
						continue;
					} else {
						TraceEvent(SevVerbose, "CheckLocationMetadataComplete")
						    .detail("By", "PostCheck")
						    .detail("DataMoveID", dataMoveId)
						    .detail("Context", context)
						    .detail("Range", range);
						break;
					}
				} catch (Error& e) {
					wait(tr.onError(e));
					retryCount++;
				}
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_location_metadata_corruption) {
				throw e;
			} else {
				// Check corruptions for the current (failed) round
				for (const auto& [idx, res] : results) {
					if (res.present() && !res.get()) {
						TraceEvent(SevError, "CheckLocationMetadataCorruptionDetectedWhenFailed")
						    .detail("By", "PostCheck")
						    .detail("DataMoveID", dataMoveId)
						    .detail("Context", context)
						    .detail("Range", range);
						throw location_metadata_corruption();
					}
				}
				if (retryCount > SERVER_KNOBS->AUDIT_DATAMOVE_POST_CHECK_RETRY_COUNT_MAX) {
					TraceEvent(SevInfo, "CheckLocationMetadataFailed")
					    .errorUnsuppressed(e)
					    .detail("By", "PostCheck")
					    .detail("DataMoveID", dataMoveId)
					    .detail("Context", context)
					    .detail("Range", range);
					// If no corruption detected, exit silently
				} else {
					wait(delay(0.5));
					retryCount++;
				}
			}
		}
	}
	return Void();
}

// Cleans up dest servers of a single shard, and unassigns the keyrange from the dest servers if necessary.
ACTOR Future<Void> cleanUpSingleShardDataMove(Database occ,
                                              KeyRange keys,
                                              MoveKeysLock lock,
                                              FlowLock* cleanUpDataMoveParallelismLock,
                                              UID dataMoveId,
                                              const DDEnabledState* ddEnabledState) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	TraceEvent(SevInfo, "CleanUpSingleShardDataMoveBegin", dataMoveId).detail("Range", keys);

	state bool runPreCheck = true;

	loop {
		state Transaction tr(occ);

		try {
			tr.trState->taskID = TaskPriority::MoveKeys;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state RangeResult currentShards = wait(krmGetRanges(&tr,
			                                                    keyServersPrefix,
			                                                    keys,
			                                                    SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
			                                                    SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT));
			ASSERT(!currentShards.empty() && !currentShards.more);

			state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

			if (KeyRangeRef(currentShards[0].key, currentShards[1].key) != keys) {
				throw operation_cancelled();
			}

			state std::vector<UID> src;
			state std::vector<UID> dest;
			state UID srcId;
			state UID destId;
			decodeKeyServersValue(UIDtoTagMap, currentShards[0].value, src, dest, srcId, destId);

			if (dest.empty() || destId != anonymousShardId) {
				return Void();
			}

			// Pre validate consistency of update of keyServers and serverKeys
			if (SERVER_KNOBS->AUDIT_DATAMOVE_PRE_CHECK && runPreCheck) {
				std::vector<UID> servers(src.size() + dest.size());
				std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
				wait(auditLocationMetadataPreCheck(
				    occ, &tr, keys, servers, "cleanUpSingleShardDataMove_precheck", dataMoveId));
			}

			TraceEvent(SevInfo, "CleanUpSingleShardDataMove", dataMoveId)
			    .detail("Range", keys)
			    .detail("Src", describe(src))
			    .detail("Dest", describe(dest))
			    .detail("SrcID", srcId)
			    .detail("DestID", destId)
			    .detail("ReadVersion", tr.getReadVersion().get());

			krmSetPreviouslyEmptyRange(
			    &tr, keyServersPrefix, keys, keyServersValue(UIDtoTagMap, src, {}), currentShards[1].value);

			std::vector<Future<Void>> actors;
			for (const auto& uid : dest) {
				if (std::find(src.begin(), src.end(), uid) == src.end()) {
					actors.push_back(
					    krmSetRangeCoalescing(&tr, serverKeysPrefixFor(uid), keys, allKeys, serverKeysFalse));
				}
			}

			wait(waitForAll(actors));

			wait(tr.commit());

			// Post validate consistency of update of keyServers and serverKeys
			if (SERVER_KNOBS->AUDIT_DATAMOVE_POST_CHECK) {
				wait(auditLocationMetadataPostCheck(occ, keys, "cleanUpSingleShardDataMove_postcheck", dataMoveId));
			}
			break;
		} catch (Error& e) {
			state Error err = e;
			if (err.code() == error_code_location_metadata_corruption) {
				throw location_metadata_corruption();
			} else {
				runPreCheck = false;
				wait(tr.onError(err));
			}

			TraceEvent(SevWarn, "CleanUpSingleShardDataMoveRetriableError", dataMoveId)
			    .error(err)
			    .detail("Range", keys);
		}
	}

	TraceEvent(SevInfo, "CleanUpSingleShardDataMoveEnd", dataMoveId).detail("Range", keys);

	return Void();
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
			if (!fetching.contains(src[s])) {
				fetching.insert(src[s]);
				serverListEntries.push_back(tr->get(serverListKeyFor(src[s])));
			}
		}

		for (int s = 0; s < dest.size(); s++) {
			if (!fetching.contains(dest[s])) {
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
// complete(already has), ""(). Set keyServers[keys].dest = servers. Set serverKeys[servers][keys] = active for each
// subrange of keys that the server did not already have, = complete for each subrange that it already has. Set
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
			CODE_PROBE(begin > keys.begin, "Multi-transactional startMoveKeys");
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
						// This can happen if a SS is removed after a shard move. See comments on PR #10110.
						if (!serverListValues[s].present()) {
							CODE_PROBE(true, "start move keys moving to a removed server", probe::decoration::rare);
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
			TraceEvent("GetShardStateReadyError", server.id()).error(e).log();
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
				// We don't think this condition should be triggered, but we're not sure if there are conditions
				// that might cause it to trigger. Adding this assertion to find any such cases via testing.
				ASSERT_WE_THINK(serverListValues[s].present());
				if (!serverListValues[s].present()) {
					// FIXME: Is this the right behavior?  dataMovementComplete will never be sent!
					// CODE_PROBE(true, "check fetching state moved to removed server", probe::decoration::rare);
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
			CODE_PROBE(begin > keys.begin, "Multi-transactional finishMoveKeys");

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
								if (!srcSet.contains(completeSrc[i])) {
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
							CODE_PROBE(true,
							           "FinishMoveKeys first key in iteration sub-range has already been processed");
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
							if (!srcSet.contains(completeSrc[i])) {
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
						CODE_PROBE(true,
						           "A previous finishMoveKeys for this range committed just as it was cancelled to "
						           "start this one?");
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
						if (!hasRemote || !completeSrcSet.contains(it)) {
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
						    !tssToIgnore.contains(tssPair->second.id())) {
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
					readyServersEv.detail("ReadyServers", count).detail("Dests", dest.size());
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

// keyServer: map from keys to destination servers.
// serverKeys: two-dimension map: [servers][keys], value is the servers' state of having the keys: active(not-have),
// complete(already has), ""().
// Set keyServers[keys].dest = servers Set serverKeys[servers][keys] = dataMoveId for each
// subrange of keys.
// Set dataMoves[dataMoveId] = DataMoveMetaData.
ACTOR static Future<Void> startMoveShards(Database occ,
                                          UID dataMoveId,
                                          std::vector<KeyRange> ranges,
                                          std::vector<UID> servers,
                                          MoveKeysLock lock,
                                          FlowLock* startMoveKeysLock,
                                          UID relocationIntervalId,
                                          std::map<UID, StorageServerInterface>* tssMapping,
                                          const DDEnabledState* ddEnabledState,
                                          CancelConflictingDataMoves cancelConflictingDataMoves,
                                          Optional<BulkLoadTaskState> bulkLoadTaskState) {
	state Future<Void> warningLogger = logWarningAfter("StartMoveShardsTooLong", 600, servers);

	wait(startMoveKeysLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser(*startMoveKeysLock);
	state bool loadedTssMapping = false;
	state DataMoveMetaData dataMove;
	state Severity sevDm = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);

	TraceEvent(SevInfo, "StartMoveShardsBegin", relocationIntervalId)
	    .detail("DataMoveID", dataMoveId)
	    .detail("TargetRange", describe(ranges))
	    .detail("BulkLoadTaskState", bulkLoadTaskState.present() ? bulkLoadTaskState.get().toString() : "");

	// TODO: make startMoveShards work with multiple ranges.
	ASSERT(ranges.size() == 1);
	state KeyRangeRef keys = ranges[0];
	state bool cancelDataMove = false;
	state bool runPreCheck = true;
	try {
		loop {
			state Key begin = keys.begin;
			state KeyRange currentKeys = keys;

			state Transaction tr(occ);

			try {
				tr.trState->taskID = TaskPriority::MoveKeys;
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(checkMoveKeysLock(&tr, lock, ddEnabledState));

				Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
				if (val.present()) {
					DataMoveMetaData dmv = decodeDataMoveValue(val.get()); // dmv: Data move value.
					dataMove = dmv;
					TraceEvent(sevDm, "StartMoveShardsFoundDataMove", relocationIntervalId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMove", dataMove.toString())
					    .detail("CancelDataMove", cancelDataMove);
					if (dataMove.getPhase() == DataMoveMetaData::Deleting) {
						TraceEvent(sevDm, "StartMoveShardsDataMoveDeleted", relocationIntervalId)
						    .detail("DataMove", dataMove.toString())
						    .detail("BackgroundCleanUp", dataMove.ranges.empty());
						throw data_move_cancelled();
					}
					ASSERT(!dataMove.ranges.empty() && dataMove.ranges.front().begin == keys.begin);
					if (cancelDataMove) {
						dataMove.setPhase(DataMoveMetaData::Deleting);
						tr.set(dataMoveKeyFor(dataMoveId), dataMoveValue(dataMove));
						wait(tr.commit());
						throw movekeys_conflict();
					}
					if (dataMove.getPhase() == DataMoveMetaData::Running) {
						TraceEvent(sevDm, "StartMoveShardsDataMove", relocationIntervalId)
						    .detail("DataMoveAlreadyCommitted", dataMoveId);
						ASSERT(keys == dataMove.ranges.front());
						return Void();
					}
					begin = dataMove.ranges.front().end;
				} else {
					if (cancelDataMove) {
						throw movekeys_conflict();
					}
					dataMove = DataMoveMetaData();
					dataMove.id = dataMoveId;
					TraceEvent(sevDm, "StartMoveShardssNewDataMove", relocationIntervalId)
					    .detail("DataMoveRange", keys)
					    .detail("DataMoveID", dataMoveId);
				}

				if (!loadedTssMapping) {
					// share transaction for loading tss mapping with the rest of start move keys
					wait(readTSSMapping(&tr, tssMapping));
					loadedTssMapping = true;
				}

				std::vector<Future<Optional<Value>>> serverListEntries;
				serverListEntries.reserve(servers.size());
				for (int s = 0; s < servers.size(); s++) {
					serverListEntries.push_back(tr.get(serverListKeyFor(servers[s])));
				}
				std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));

				for (int s = 0; s < serverListValues.size(); s++) {
					if (!serverListValues[s].present()) {
						// Attempt to move onto a server that isn't in serverList (removed or never added to the
						// database) This can happen (why?) and is handled by the data distribution algorithm
						// FIXME: Answer why this can happen?
						// TODO(psm): Mark the data move as 'deleting'.
						throw move_to_removed_server();
					}
				}

				currentKeys = KeyRangeRef(begin, keys.end);
				state std::vector<Future<Void>> actors;

				if (!currentKeys.empty()) {
					const int rowLimit = SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT;
					const int byteLimit = SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT;
					state RangeResult old = wait(krmGetRanges(&tr, keyServersPrefix, currentKeys, rowLimit, byteLimit));

					state Key endKey = old.back().key;
					currentKeys = KeyRangeRef(currentKeys.begin, endKey);

					// Check that enough servers for each shard are in the correct state
					state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

					// For each intersecting range, update keyServers[range] dest to be servers and clear existing dest
					// servers from serverKeys
					state int oldIndex = 0;
					for (; oldIndex < old.size() - 1; ++oldIndex) {
						state KeyRangeRef rangeIntersectKeys(old[oldIndex].key, old[oldIndex + 1].key);
						state std::vector<UID> src;
						state std::vector<UID> dest;
						state UID srcId;
						state UID destId;
						decodeKeyServersValue(UIDtoTagMap, old[oldIndex].value, src, dest, srcId, destId);
						TraceEvent(sevDm, "StartMoveShardsProcessingShard", relocationIntervalId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("Range", rangeIntersectKeys)
						    .detail("OldSrc", describe(src))
						    .detail("OldDest", describe(dest))
						    .detail("SrcID", srcId)
						    .detail("DestID", destId)
						    .detail("ReadVersion", tr.getReadVersion().get());

						if (bulkLoadTaskState.present()) {
							state std::vector<UID> owners(src.size() + dest.size());
							std::merge(src.begin(), src.end(), dest.begin(), dest.end(), owners.begin());
							for (const auto& ssid : servers) {
								if (std::find(owners.begin(), owners.end(), ssid) != owners.end()) {
									TraceEvent(SevWarn, "DDBulkLoadEngineTaskStartMoveShardsMoveInConflict")
									    .detail("BulkLoadTaskState", bulkLoadTaskState.get().toString())
									    .detail("DestServerId", ssid)
									    .detail("OwnerIds", describe(owners))
									    .detail("DataMove", dataMove.toString());
									cancelDataMove = true;
									throw retry();
								}
							}
						}

						// Pre validate consistency of update of keyServers and serverKeys
						if (SERVER_KNOBS->AUDIT_DATAMOVE_PRE_CHECK && runPreCheck) {
							std::vector<UID> servers(src.size() + dest.size());
							std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
							wait(auditLocationMetadataPreCheck(
							    occ, &tr, rangeIntersectKeys, servers, "startMoveShards_precheck", dataMoveId));
						}

						if (destId.isValid()) {
							TraceEvent(SevWarn, "StartMoveShardsDestIDExist", relocationIntervalId)
							    .detail("Range", rangeIntersectKeys)
							    .detail("DataMoveID", dataMoveId.toString())
							    .detail("DestID", destId.toString())
							    .log();
							ASSERT(!dest.empty());

							if (destId == dataMoveId) {
								TraceEvent(SevWarn, "StartMoveShardsRangeAlreadyCommitted", relocationIntervalId)
								    .detail("Range", rangeIntersectKeys)
								    .detail("DataMoveID", dataMoveId);
								continue;
							}

							if (destId == anonymousShardId) {
								wait(cleanUpSingleShardDataMove(
								    occ, rangeIntersectKeys, lock, startMoveKeysLock, dataMoveId, ddEnabledState));
								throw retry();
							} else {
								if (cancelConflictingDataMoves) {
									TraceEvent(
									    SevWarn, "StartMoveShardsCancelConflictingDataMove", relocationIntervalId)
									    .detail("Range", rangeIntersectKeys)
									    .detail("DataMoveID", dataMoveId.toString())
									    .detail("ExistingDataMoveID", destId.toString());
									wait(cleanUpDataMove(occ, destId, lock, startMoveKeysLock, keys, ddEnabledState));
									throw retry();
								} else {
									Optional<Value> val = wait(tr.get(dataMoveKeyFor(destId)));
									ASSERT(val.present());
									DataMoveMetaData dmv = decodeDataMoveValue(val.get());
									TraceEvent(
									    SevWarnAlways, "StartMoveShardsFoundConflictingDataMove", relocationIntervalId)
									    .detail("Range", rangeIntersectKeys)
									    .detail("DataMoveID", dataMoveId.toString())
									    .detail("ExistingDataMoveID", destId.toString())
									    .detail("ExistingDataMove", dmv.toString());
									cancelDataMove = true;
									throw retry();
								}
							}
						}

						// Update dest servers for this range to be equal to servers
						krmSetPreviouslyEmptyRange(&tr,
						                           keyServersPrefix,
						                           rangeIntersectKeys,
						                           keyServersValue(src, servers, srcId, dataMoveId),
						                           old[oldIndex + 1].value);

						dataMove.src.insert(src.begin(), src.end());

						// If this is a bulk load data move, need not create checkpoint on the source servers
						if (shouldCreateCheckpoint(dataMoveId) && !bulkLoadTaskState.present()) {
							const UID checkpointId = UID(deterministicRandom()->randomUInt64(), srcId.first());
							CheckpointMetaData checkpoint(std::vector<KeyRange>{ rangeIntersectKeys },
							                              DataMoveRocksCF,
							                              src,
							                              checkpointId,
							                              dataMoveId);
							checkpoint.setState(CheckpointMetaData::Pending);
							tr.set(checkpointKeyFor(checkpointId), checkpointValue(checkpoint));
							dataMove.checkpoints.insert(checkpointId);
							TraceEvent(sevDm, "InitiatedCheckpoint", relocationIntervalId)
							    .detail("CheckpointID", checkpointId.toString())
							    .detail("Range", rangeIntersectKeys)
							    .detail("DataMoveID", dataMoveId)
							    .detail("SrcServers", describe(src))
							    .detail("ReadVersion", tr.getReadVersion().get());
						}
					}

					// Update serverKeys to include keys (or the currently processed subset of keys) for each SS in
					// servers.
					for (int i = 0; i < servers.size(); i++) {
						// Since we are setting this for the entire range, serverKeys and keyServers aren't guaranteed
						// to have the same shard boundaries If that invariant was important, we would have to move this
						// inside the loop above and also set it for the src servers.
						actors.push_back(krmSetRangeCoalescing(
						    &tr, serverKeysPrefixFor(servers[i]), currentKeys, allKeys, serverKeysValue(dataMoveId)));
					}

					dataMove.ranges.clear();
					dataMove.ranges.push_back(KeyRangeRef(keys.begin, currentKeys.end));
					dataMove.dest.insert(servers.begin(), servers.end());
				}

				if (currentKeys.end == keys.end) {
					if (bulkLoadTaskState.present()) {
						state BulkLoadTaskState newBulkLoadTaskState;
						try {
							wait(store(newBulkLoadTaskState,
							           getBulkLoadTask(&tr,
							                           bulkLoadTaskState.get().getRange(),
							                           bulkLoadTaskState.get().getTaskId(),
							                           { BulkLoadPhase::Triggered, BulkLoadPhase::Running })));
							// It is possible that the previous data move is cancelled but has updated the
							// task phase as running. In this case, we update the phase from Running to Running
							newBulkLoadTaskState.phase = BulkLoadPhase::Running;
						} catch (Error& e) {
							if (e.code() == error_code_bulkload_task_outdated) {
								cancelDataMove = true;
								throw retry();
							}
							throw e;
						}
						newBulkLoadTaskState.setDataMoveId(dataMoveId);
						newBulkLoadTaskState.startTime = now();
						wait(krmSetRange(&tr,
						                 bulkLoadTaskPrefix,
						                 newBulkLoadTaskState.getRange(),
						                 bulkLoadTaskStateValue(newBulkLoadTaskState)));
						TraceEvent(SevInfo, "DDBulkLoadEngineTaskRunningPersist", relocationIntervalId)
						    .detail("BulkLoadTaskState", newBulkLoadTaskState.toString());
						dataMove.bulkLoadTaskState = newBulkLoadTaskState;
					}
					dataMove.setPhase(DataMoveMetaData::Running);
					TraceEvent(sevDm, "StartMoveShardsDataMoveComplete", relocationIntervalId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMove", dataMove.toString())
					    .detail("BulkLoadTaskState",
					            bulkLoadTaskState.present() ? bulkLoadTaskState.get().toString() : "");
				} else {
					dataMove.setPhase(DataMoveMetaData::Prepare);
					TraceEvent(sevDm, "StartMoveShardsDataMovePartial", relocationIntervalId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("CurrentRange", currentKeys)
					    .detail("DataMoveRange", keys)
					    .detail("NewDataMoveMetaData", dataMove.toString());
				}

				tr.set(dataMoveKeyFor(dataMoveId), dataMoveValue(dataMove));

				wait(waitForAll(actors));

				wait(tr.commit());

				TraceEvent(sevDm, "DataMoveMetaDataCommit", relocationIntervalId)
				    .detail("DataMoveID", dataMoveId)
				    .detail("DataMoveKey", dataMoveKeyFor(dataMoveId))
				    .detail("CommitVersion", tr.getCommittedVersion())
				    .detail("DeltaRange", currentKeys.toString())
				    .detail("Range", describe(dataMove.ranges))
				    .detail("DataMove", dataMove.toString())
				    .detail("BulkLoadTaskState", bulkLoadTaskState.present() ? bulkLoadTaskState.get().toString() : "");

				dataMove = DataMoveMetaData();
				if (currentKeys.end == keys.end) {
					// Post validate consistency of update of keyServers and serverKeys
					if (SERVER_KNOBS->AUDIT_DATAMOVE_POST_CHECK) {
						wait(auditLocationMetadataPostCheck(occ, keys, "startMoveShards_postcheck", dataMoveId));
					}
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_location_metadata_corruption) {
					throw location_metadata_corruption();
				} else if (e.code() == error_code_retry) {
					runPreCheck = false;
					wait(delay(1));
				} else {
					TraceEvent(SevWarn, "StartMoveShardsError", relocationIntervalId)
					    .errorUnsuppressed(e)
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMoveRange", keys)
					    .detail("CurrentDataMoveMetaData", dataMove.toString());
					runPreCheck = false;
					wait(tr.onError(e));
				}
			}
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "StartMoveShardsError", relocationIntervalId)
		    .errorUnsuppressed(e)
		    .detail("DataMoveID", dataMoveId);
		throw;
	}

	TraceEvent(SevInfo, "StartMoveShardsEnd", relocationIntervalId).detail("DataMoveID", dataMoveId);

	return Void();
}

ACTOR static Future<Void> checkDataMoveComplete(Database occ, UID dataMoveId, KeyRange keys, UID relocationIntervalId) {
	try {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(occ);
		state Key begin = keys.begin;
		while (begin < keys.end) {
			loop {
				try {
					tr->getTransaction().trState->taskID = TaskPriority::MoveKeys;
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					// Get all existing shards overlapping keys (exclude any that have been processed in a previous
					// iteration of the outer loop)
					state KeyRange currentKeys = KeyRangeRef(begin, keys.end);

					state RangeResult keyServers = wait(krmGetRanges(tr,
					                                                 keyServersPrefix,
					                                                 currentKeys,
					                                                 SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
					                                                 SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT));

					// Determine the last processed key (which will be the beginning for the next iteration)
					state Key endKey = keyServers.back().key;
					currentKeys = KeyRangeRef(currentKeys.begin, endKey);

					// Check that enough servers for each shard are in the correct state
					state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

					for (int i = 0; i < keyServers.size() - 1; ++i) {
						KeyRangeRef rangeIntersectKeys(keyServers[i].key, keyServers[i + 1].key);
						std::vector<UID> src;
						std::vector<UID> dest;
						UID srcId;
						UID destId;
						decodeKeyServersValue(UIDtoTagMap, keyServers[i].value, src, dest, srcId, destId);
						const KeyRange currentRange = KeyRangeRef(keyServers[i].key, keyServers[i + 1].key);
						TraceEvent(SevVerbose, "CheckDataMoveCompleteShard", relocationIntervalId)
						    .detail("Range", currentRange)
						    .detail("SrcID", srcId)
						    .detail("Src", describe(src))
						    .detail("DestID", destId)
						    .detail("Dest", describe(dest));
						if (!dest.empty() || srcId != dataMoveId) {
							// There is ongoing data move, or the data move is complete, but moved to a different shard.
							throw data_move_cancelled();
						}
					}

					begin = endKey;
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}
	} catch (Error& e) {
		TraceEvent(SevDebug, "CheckDataMoveCompleteError", relocationIntervalId).errorUnsuppressed(e);
		throw;
	}

	return Void();
}

// Set keyServers[keys].src = keyServers[keys].dest and keyServers[keys].dest=[], return when successful
// keyServers[k].dest must be the same for all k in keys.
// Set serverKeys[dest][keys] = dataMoveId; serverKeys[src][keys] = false for all src not in dest.
// Clear dataMoves[dataMoveId].
ACTOR static Future<Void> finishMoveShards(Database occ,
                                           UID dataMoveId,
                                           std::vector<KeyRange> targetRanges,
                                           std::vector<UID> destinationTeam,
                                           MoveKeysLock lock,
                                           FlowLock* finishMoveKeysParallelismLock,
                                           bool hasRemote,
                                           UID relocationIntervalId,
                                           std::map<UID, StorageServerInterface> tssMapping,
                                           const DDEnabledState* ddEnabledState,
                                           Optional<BulkLoadTaskState> bulkLoadTaskState) {
	// TODO: make startMoveShards work with multiple ranges.
	ASSERT(targetRanges.size() == 1);
	state KeyRange keys = targetRanges[0];
	state Future<Void> warningLogger = logWarningAfter("FinishMoveShardsTooLong", 600, destinationTeam);
	state int retries = 0;
	state DataMoveMetaData dataMove;
	state bool cancelDataMove = false;
	state Severity sevDm = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);

	wait(finishMoveKeysParallelismLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser = FlowLock::Releaser(*finishMoveKeysParallelismLock);
	state bool runPreCheck = true;
	state bool skipTss = false;
	state double ssReadyTime = std::numeric_limits<double>::max();

	ASSERT(!destinationTeam.empty());

	try {
		TraceEvent(SevInfo, "FinishMoveShardsBegin", relocationIntervalId)
		    .detail("DataMoveID", dataMoveId)
		    .detail("TargetRange", keys);

		// This process can be split up into multiple transactions if getRange() doesn't return the entire
		// target range.
		loop {
			state std::vector<UID> completeSrc;
			state std::vector<UID> destServers;
			state std::unordered_set<UID> allServers;
			state KeyRange range;
			state Transaction tr(occ);
			try {
				tr.trState->taskID = TaskPriority::MoveKeys;
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(checkMoveKeysLock(&tr, lock, ddEnabledState));

				Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
				if (val.present()) {
					dataMove = decodeDataMoveValue(val.get());
					TraceEvent(sevDm, "FinishMoveShardsFoundDataMove", relocationIntervalId)
					    .detail("AtVerison", tr.getReadVersion().get())
					    .detail("DataMoveKey", dataMoveKeyFor(dataMoveId))
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMove", dataMove.toString())
					    .detail("CancelDataMove", cancelDataMove);
					if (cancelDataMove) {
						dataMove.setPhase(DataMoveMetaData::Deleting);
						tr.set(dataMoveKeyFor(dataMoveId), dataMoveValue(dataMove));
						wait(tr.commit());
						throw movekeys_conflict();
					}
					destServers.insert(destServers.end(), dataMove.dest.begin(), dataMove.dest.end());
					std::sort(destServers.begin(), destServers.end());
					if (dataMove.getPhase() == DataMoveMetaData::Deleting) {
						TraceEvent(SevWarn, "FinishMoveShardsDataMoveDeleting", relocationIntervalId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("DataMove", dataMove.toString());
						throw data_move_cancelled();
					}
					ASSERT(dataMove.getPhase() == DataMoveMetaData::Running);
					ASSERT(!dataMove.ranges.empty());
					range = dataMove.ranges.front();
				} else {
					TraceEvent(SevWarn, "FinishMoveShardsDataMoveDeleted", relocationIntervalId)
					    .detail("AtVerison", tr.getReadVersion().get())
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMove", dataMove.toString());
					wait(checkDataMoveComplete(occ, dataMoveId, keys, relocationIntervalId));
					return Void();
				}

				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult keyServers = wait(krmGetRanges(&tr,
				                                                 keyServersPrefix,
				                                                 range,
				                                                 SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
				                                                 SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT));
				ASSERT(!keyServers.empty());
				range = KeyRangeRef(range.begin, keyServers.back().key);
				ASSERT(!range.empty());

				state int currentIndex = 0;
				for (; currentIndex < keyServers.size() - 1; ++currentIndex) {
					state std::vector<UID> src;
					state std::vector<UID> dest;
					state UID srcId;
					state UID destId;
					decodeKeyServersValue(UIDtoTagMap, keyServers[currentIndex].value, src, dest, srcId, destId);
					const KeyRange currentRange =
					    KeyRangeRef(keyServers[currentIndex].key, keyServers[currentIndex + 1].key);
					TraceEvent(sevDm, "FinishMoveShardsProcessingShard", relocationIntervalId)
					    .detail("Range", currentRange)
					    .detail("SrcID", srcId)
					    .detail("Src", describe(src))
					    .detail("DestID", destId)
					    .detail("Dest", describe(dest))
					    .detail("DataMove", dataMove.toString());
					allServers.insert(src.begin(), src.end());
					allServers.insert(dest.begin(), dest.end());
					if (destId != dataMoveId) {
						TraceEvent(SevWarnAlways, "FinishMoveShardsInconsistentIDs", relocationIntervalId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("ExistingShardID", destId)
						    .detail("DataMove", dataMove.toString());
						cancelDataMove = true;
						throw retry();
					}

					// Pre validate consistency of update of keyServers and serverKeys
					if (SERVER_KNOBS->AUDIT_DATAMOVE_PRE_CHECK && runPreCheck) {
						std::vector<UID> servers(src.size() + dest.size());
						std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
						wait(auditLocationMetadataPreCheck(
						    occ, &tr, currentRange, servers, "finishMoveShards_precheck", dataMoveId));
					}

					std::sort(dest.begin(), dest.end());
					ASSERT(std::equal(destServers.begin(), destServers.end(), dest.begin(), dest.end()));

					std::set<UID> srcSet;
					for (int s = 0; s < src.size(); s++) {
						srcSet.insert(src[s]);
					}

					if (currentIndex == 0) {
						completeSrc = src;
					} else {
						for (int i = 0; i < completeSrc.size(); i++) {
							if (!srcSet.contains(completeSrc[i])) {
								swapAndPop(&completeSrc, i--);
							}
						}
					}
				}

				// Wait for a durable quorum of servers in destServers to have keys available (readWrite)
				// They must also have at least the transaction read version so they can't "forget" the shard
				// between now and when this transaction commits.
				state std::vector<Future<Void>> serverReady; // only for count below
				state std::vector<Future<Void>> tssReady; // for waiting in parallel with tss
				state std::vector<StorageServerInterface> tssReadyInterfs;
				state std::vector<UID> newDestinations;
				std::set<UID> completeSrcSet(completeSrc.begin(), completeSrc.end());
				for (const UID& id : destServers) {
					if (!hasRemote || !completeSrcSet.contains(id)) {
						newDestinations.push_back(id);
					}
				}

				state std::vector<StorageServerInterface> storageServerInterfaces;
				std::vector<Future<Optional<Value>>> serverListEntries;
				serverListEntries.reserve(newDestinations.size());
				for (const UID& id : newDestinations) {
					serverListEntries.push_back(tr.get(serverListKeyFor(id)));
				}
				state std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));

				releaser.release();

				for (int s = 0; s < serverListValues.size(); s++) {
					// TODO: if the server is removed,
					if (!serverListValues[s].present()) {
						throw retry();
					}
					auto si = decodeServerListValue(serverListValues[s].get());
					ASSERT(si.id() == newDestinations[s]);
					storageServerInterfaces.push_back(si);
				}

				// update client info in case tss mapping changed or server got updated

				// Wait for new destination servers to fetch the data range.
				serverReady.reserve(storageServerInterfaces.size());
				for (int s = 0; s < storageServerInterfaces.size(); s++) {
					serverReady.push_back(waitForShardReady(
					    storageServerInterfaces[s], range, tr.getReadVersion().get(), GetShardStateRequest::READABLE));

					if (skipTss)
						continue;

					auto tssPair = tssMapping.find(storageServerInterfaces[s].id());

					if (tssPair != tssMapping.end()) {
						tssReadyInterfs.push_back(tssPair->second);
						tssReady.push_back(waitForShardReady(
						    tssPair->second, range, tr.getReadVersion().get(), GetShardStateRequest::READABLE));
					}
				}

				TraceEvent(sevDm, "FinishMoveShardsWaitingServers", relocationIntervalId)
				    .detail("DataMoveID", dataMoveId)
				    .detail("NewDestinations", describe(newDestinations))
				    .detail("DataMove", dataMove.toString());

				// Wait for all storage server moves, and explicitly swallow errors for tss ones with
				// waitForAllReady If this takes too long the transaction will time out and retry, which is ok
				wait(timeout(waitForAll(serverReady) && waitForAllReady(tssReady),
				             SERVER_KNOBS->SERVER_READY_QUORUM_TIMEOUT,
				             Void(),
				             TaskPriority::MoveKeys));

				state std::vector<UID> readyServers;
				for (int s = 0; s < serverReady.size(); ++s) {
					if (serverReady[s].isReady() && !serverReady[s].isError()) {
						readyServers.push_back(storageServerInterfaces[s].uniqueID);
					}
				}
				int tssCount = 0;
				for (int s = 0; s < tssReady.size(); s++) {
					if (tssReady[s].isReady() && !tssReady[s].isError()) {
						tssCount += 1;
					}
				}

				if (readyServers.size() == serverReady.size() && !skipTss) {
					ssReadyTime = std::min(now(), ssReadyTime);
					if (tssCount < tssReady.size() &&
					    now() - ssReadyTime >= SERVER_KNOBS->DD_WAIT_TSS_DATA_MOVE_DELAY) {
						skipTss = true;
						TraceEvent(SevWarnAlways, "FinishMoveShardsSkipTSS")
						    .detail("DataMoveID", dataMoveId)
						    .detail("ReadyServers", describe(readyServers))
						    .detail("NewDestinations", describe(newDestinations))
						    .detail("ReadyTSS", tssCount)
						    .detail("TSSInfo", describe(tssReadyInterfs))
						    .detail("SSReadyTime", ssReadyTime);
					}
				}

				TraceEvent(sevDm, "FinishMoveShardsWaitedServers", relocationIntervalId)
				    .detail("DataMoveID", dataMoveId)
				    .detail("ReadyServers", describe(readyServers))
				    .detail("NewDestinations", describe(newDestinations))
				    .detail("ReadyTSS", tssCount)
				    .detail("DataMove", dataMove.toString());

				if (readyServers.size() == newDestinations.size()) {

					std::vector<Future<Void>> actors;
					actors.push_back(krmSetRangeCoalescing(
					    &tr, keyServersPrefix, range, allKeys, keyServersValue(destServers, {}, dataMoveId, UID())));

					for (const UID& ssId : allServers) {
						const bool destHasServer =
						    std::find(destServers.begin(), destServers.end(), ssId) != destServers.end();
						actors.push_back(
						    krmSetRangeCoalescing(&tr,
						                          serverKeysPrefixFor(ssId),
						                          range,
						                          allKeys,
						                          destHasServer ? serverKeysValue(dataMoveId) : serverKeysFalse));
						TraceEvent(sevDm, "FinishMoveShardsSetServerKeyRange", relocationIntervalId)
						    .detail("StorageServerID", ssId)
						    .detail("KeyRange", range)
						    .detail("ShardID", destHasServer ? dataMoveId : UID())
						    .detail("DataMove", dataMove.toString());
					}

					wait(waitForAll(actors));

					if (range.end == dataMove.ranges.front().end) {
						if (bulkLoadTaskState.present()) {
							state BulkLoadTaskState newBulkLoadTaskState;
							try {
								wait(store(newBulkLoadTaskState,
								           getBulkLoadTask(&tr,
								                           bulkLoadTaskState.get().getRange(),
								                           bulkLoadTaskState.get().getTaskId(),
								                           { BulkLoadPhase::Running, BulkLoadPhase::Complete })));
								newBulkLoadTaskState.phase = BulkLoadPhase::Complete;
							} catch (Error& e) {
								if (e.code() == error_code_bulkload_task_outdated) {
									cancelDataMove = true;
									throw retry();
								}
								throw e;
							}
							ASSERT(newBulkLoadTaskState.getDataMoveId().present() &&
							       newBulkLoadTaskState.getDataMoveId().get() == dataMoveId);
							newBulkLoadTaskState.completeTime = now();
							wait(krmSetRange(&tr,
							                 bulkLoadTaskPrefix,
							                 newBulkLoadTaskState.getRange(),
							                 bulkLoadTaskStateValue(newBulkLoadTaskState)));
							TraceEvent(SevInfo, "DDBulkLoadEngineTaskCompletePersist", relocationIntervalId)
							    .detail("BulkLoadTaskState", newBulkLoadTaskState.toString());
							dataMove.bulkLoadTaskState = newBulkLoadTaskState;
						}
						wait(deleteCheckpoints(&tr, dataMove.checkpoints, dataMoveId));
						tr.clear(dataMoveKeyFor(dataMoveId));
						TraceEvent(sevDm, "FinishMoveShardsDeleteMetaData", relocationIntervalId)
						    .detail("DataMove", dataMove.toString());
					} else if (!bulkLoadTaskState.present()) {
						// Bulk Loading data move does not allow partial complete
						TraceEvent(SevInfo, "FinishMoveShardsPartialComplete", relocationIntervalId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("CurrentRange", range)
						    .detail("NewDataMoveMetaData", dataMove.toString())
						    .detail("DataMove", dataMove.toString());
						dataMove.ranges.front() = KeyRangeRef(range.end, dataMove.ranges.front().end);
						tr.set(dataMoveKeyFor(dataMoveId), dataMoveValue(dataMove));
					}

					wait(tr.commit());

					if (range.end == dataMove.ranges.front().end) {
						// Post validate consistency of update of keyServers and serverKeys
						if (SERVER_KNOBS->AUDIT_DATAMOVE_POST_CHECK) {
							wait(auditLocationMetadataPostCheck(
							    occ, dataMove.ranges.front(), "finishMoveShards_postcheck", relocationIntervalId));
						}
						break;
					}
				} else {
					tr.reset();
				}
			} catch (Error& error) {
				TraceEvent(SevWarn, "TryFinishMoveShardsError", relocationIntervalId)
				    .errorUnsuppressed(error)
				    .detail("DataMoveID", dataMoveId);
				if (error.code() == error_code_location_metadata_corruption) {
					throw location_metadata_corruption();
				} else if (error.code() == error_code_retry) {
					runPreCheck = false;
					++retries;
					wait(delay(1));
				} else if (error.code() == error_code_actor_cancelled) {
					throw;
				} else {
					state Error err = error;
					runPreCheck = false;
					wait(tr.onError(err));
					retries++;
					if (retries % 10 == 0) {
						TraceEvent(retries == 20 ? SevWarnAlways : SevWarn,
						           "RelocateShard_FinishMoveShardsRetrying",
						           relocationIntervalId)
						    .error(err)
						    .detail("DataMoveID", dataMoveId);
					}
				}
			}
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "FinishMoveShardsError", relocationIntervalId).errorUnsuppressed(e);
		throw;
	}

	TraceEvent(SevInfo, "FinishMoveShardsEnd", relocationIntervalId)
	    .detail("DataMoveID", dataMoveId)
	    .detail("BulkLoadTaskState", bulkLoadTaskState.present() ? bulkLoadTaskState.get().toString() : "")
	    .detail("DataMove", dataMove.toString());
	return Void();
}

}; // anonymous namespace

ACTOR Future<std::pair<Version, Tag>> addStorageServer(Database cx, StorageServerInterface server) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	state KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(serverMetadataKeys.begin,
	                                                                                           IncludeVersion());

	state int maxSkipTags = 1;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

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

				if (SERVER_KNOBS->TSS_HACK_IDENTITY_MAPPING) {
					// THIS SHOULD NEVER BE ENABLED IN ANY NON-TESTING ENVIRONMENT
					TraceEvent(SevError, "TSSIdentityMappingEnabled").log();
					tssMapDB.set(tr, server.id(), server.id());
				}
			}

			StorageMetadataType metadata(StorageMetadataType::currentTime());
			metadataMap.set(tr, server.id(), metadata);
			tr->set(serverMetadataChangeKey, deterministicRandom()->randomUniqueID().toString());

			tr->set(serverListKeyFor(server.id()), serverListValue(server));
			wait(tr->commit());
			TraceEvent("AddedStorageServerSystemKey")
			    .detail("ServerID", server.id())
			    .detail("CommitVersion", tr->getCommittedVersion());

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
	UID shardId;
	bool assigned, emptyRange;
	DataMoveType dataMoveType = DataMoveType::LOGICAL;
	DataMovementReason dataMoveReason = DataMovementReason::INVALID;
	decodeServerKeysValue(keys[0].value, assigned, emptyRange, dataMoveType, shardId, dataMoveReason);
	TraceEvent(SevVerbose, "CanRemoveStorageServer")
	    .detail("ServerID", serverID)
	    .detail("Key1", keys[0].key)
	    .detail("Value1", keys[0].value)
	    .detail("Key2", keys[1].key)
	    .detail("Value2", keys[1].value);
	return !assigned && keys[1].key == allKeys.end;
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
			TraceEvent("RemoveStorageServer")
			    .detail("State", "Locked")
			    .detail("ServerID", serverID)
			    .detail("Version", tr->getReadVersion().get());

			state bool canRemove = wait(canRemoveStorageServer(tr, serverID));
			if (!canRemove) {
				CODE_PROBE(true,
				           "The caller had a transaction in flight that assigned keys to the server.  Wait for it to "
				           "reverse its mistake.");
				TraceEvent(SevWarn, "RemoveStorageServer")
				    .detail("State", "CanRemoveFailed")
				    .detail("ServerID", serverID)
				    .detail("Count", noCanRemoveCount++);
				wait(delayJittered(SERVER_KNOBS->REMOVE_RETRY_DELAY, TaskPriority::DataDistributionLaunch));
				tr->reset();
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
						CODE_PROBE(true, "Storage server already removed after retrying transaction");
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

				if (locality >= 0 && !allLocalities.contains(locality)) {
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
				tr->set(serverMetadataChangeKey, deterministicRandom()->randomUniqueID().toString());

				retry = true;
				wait(tr->commit());
				TraceEvent("RemoveStorageServer")
				    .detail("State", "Success")
				    .detail("ServerID", serverID)
				    .detail("CommitVersion", tr->getCommittedVersion());
				return Void();
			}
		} catch (Error& e) {
			state Error err = e;
			wait(tr->onError(e));
			TraceEvent("RemoveStorageServer").error(err).detail("State", "Retry").detail("ServerID", serverID);
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
	state UID srcId;
	state UID destId;
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
					decodeKeyServersValue(UIDtoTagMap, it.value, src, dest, srcId, destId);

					// The failed server is not present
					if (std::find(src.begin(), src.end(), serverID) == src.end() &&
					    std::find(dest.begin(), dest.end(), serverID) == dest.end()) {
						continue;
					}

					// Update the vectors to remove failed server then set the value again
					// Dest is usually empty, but keep this in case there is parallel data movement
					src.erase(std::remove(src.begin(), src.end(), serverID), src.end());
					dest.erase(std::remove(dest.begin(), dest.end(), serverID), dest.end());

					state KeyRangeRef range(it.key, keyServers[i + 1].key);

					// If the last src server is to be removed, first check if there are dest servers who is
					// hosting a read-write copy of the keyrange, and move such dest servers to the src list.
					if (src.empty() && !dest.empty()) {
						std::vector<UID> newSources = wait(pickReadWriteServers(&tr, dest, range));
						for (const UID& id : newSources) {
							TraceEvent(SevWarn, "FailedServerAdditionalSourceServer", serverID)
							    .detail("Range", range)
							    .detail("NewSourceServerFromDest", id);
							if (destId == anonymousShardId) {
								dest.erase(std::remove(dest.begin(), dest.end(), id), dest.end());
							}
							src.push_back(id);
							srcId = anonymousShardId;
						}
						// TODO(psm): We may need to cancel the data move since all sources servers are gone.
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

						if (destId.isValid() && destId != anonymousShardId) {
							Optional<Value> val = wait(tr.get(dataMoveKeyFor(destId)));
							if (val.present()) {
								state DataMoveMetaData dataMove = decodeDataMoveValue(val.get());
								ASSERT(!dataMove.ranges.empty());
								TraceEvent(SevVerbose, "RemoveRangeFoundDataMove", serverID)
								    .detail("DataMoveMetaData", dataMove.toString());
								if (range == dataMove.ranges.front()) {
									tr.clear(dataMoveKeyFor(destId));
								} else {
									dataMove.setPhase(DataMoveMetaData::Deleting);
									tr.set(dataMoveKeyFor(destId), dataMoveValue(dataMove));
								}
							} else {
								TraceEvent(SevWarnAlways, "DataMoveMissing", serverID)
								    .detail("DataMoveID", destId)
								    .detail("Range", range);
							}
						}

						const UID shardId = newDataMoveId(deterministicRandom()->randomUInt64(),
						                                  AssignEmptyRange::True,
						                                  DataMoveType::LOGICAL,
						                                  DataMovementReason::ASSIGN_EMPTY_RANGE);

						// Assign the shard to teamForDroppedRange in keyServer space.
						if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
							tr.set(keyServersKey(it.key), keyServersValue(teamForDroppedRange, {}, shardId, UID()));
						} else {
							tr.set(keyServersKey(it.key), keyServersValue(UIDtoTagMap, teamForDroppedRange));
						}

						std::vector<Future<Void>> actors;

						// Unassign the shard from the dest servers.
						for (const UID& id : dest) {
							actors.push_back(
							    krmSetRangeCoalescing(&tr, serverKeysPrefixFor(id), range, allKeys, serverKeysFalse));
						}

						// Assign the shard to the new team as an empty range.
						// Note, there could be data loss.
						for (const UID& id : teamForDroppedRange) {
							if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
								actors.push_back(krmSetRangeCoalescing(
								    &tr, serverKeysPrefixFor(id), range, allKeys, serverKeysValue(shardId)));
							} else {
								actors.push_back(krmSetRangeCoalescing(
								    &tr, serverKeysPrefixFor(id), range, allKeys, serverKeysTrueEmptyRange));
							}
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
						if (srcId != anonymousShardId) {
							if (dest.empty())
								destId = UID();
							tr.set(keyServersKey(it.key), keyServersValue(src, dest, srcId, destId));
						} else {
							tr.set(keyServersKey(it.key), keyServersValue(UIDtoTagMap, src, dest));
						}
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

// In cleanUpDataMoveCore, to do the actual cleanup, we suppose the target data move already update its
// information to the metadata. However, this does not always happen.
// Background cleanup is used to handle the case where the normal cleanup (cleanUpDataMoveCore)
// and the moveShard (startMoveShard) has race on update of metadata.
// Background cleanup is triggered when the normal cleanup (cleanUpDataMoveCore) with a succeed transaction
// is failed to see the update of metadata (datamove key space) by the startMoveShard
// For this case, the startMoveShard must exit without update the meta data
// This background cleanup is used to clean the placehold left by the normal cleanup
// To understand this trick of cleanup place holder, we have three cases:
// (1) Race condition of dataMove metadata between cleanUpDataMoveCore and startMoveShard, and
// cleanUpDataMoveCore wins the race. Then startMoveShard retries and see the place holder on the metadata
// put by cleanUpDataMoveCore, and startMoveShard gives up and exits. No update to the metadata
// (2) Race condition of dataMove metadata between cleanUpDataMoveCore and startMoveShard, and
// startMoveShard wins the race. Then cleanUpDataMoveCore retries and see the update of metadata by
// startMoveShard. Then cleanUpDataMoveCore does the cleanup as normal
// (3) cleanUpDataMoveCore happens before startMoveShard. No race happens. Then, cleanUpDataMoveCore sees
// the place holder on the metadata put by cleanUpDataMoveCore. Then, startMoveShard gives up and exits.
// No update to the metadata by the startMoveShard
// For all three cases, the background cleanup only needs to cleanup the place holder
ACTOR Future<Void> cleanUpDataMoveBackground(Database occ,
                                             UID dataMoveId,
                                             MoveKeysLock lock,
                                             FlowLock* cleanUpDataMoveParallelismLock,
                                             KeyRange keys,
                                             const DDEnabledState* ddEnabledState,
                                             double delaySeconds) {
	wait(delay(std::max(10.0, delaySeconds)));
	TraceEvent(SevDebug, "CleanUpDataMoveBackgroundBegin", dataMoveId)
	    .detail("DataMoveID", dataMoveId)
	    .detail("Range", keys);
	wait(cleanUpDataMoveParallelismLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser = FlowLock::Releaser(*cleanUpDataMoveParallelismLock);
	state DataMoveMetaData dataMove;
	state Transaction tr(occ);
	loop {
		try {
			tr.trState->taskID = TaskPriority::MoveKeys;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(checkMoveKeysLock(&tr, lock, ddEnabledState));

			Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
			if (!val.present()) {
				break;
			}
			dataMove = decodeDataMoveValue(val.get());
			ASSERT(dataMove.ranges.empty());
			ASSERT(dataMove.getPhase() == DataMoveMetaData::Deleting);
			tr.clear(dataMoveKeyFor(dataMoveId));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "CleanUpDataMoveBackgroundFail", dataMoveId).errorUnsuppressed(e);
			wait(tr.onError(e));
		}
	}

	TraceEvent(SevDebug, "CleanUpDataMoveBackgroundEnd", dataMoveId)
	    .detail("DataMoveID", dataMoveId)
	    .detail("DataMoveRange", keys);

	return Void();
}

ACTOR Future<Void> cleanUpDataMoveCore(Database occ,
                                       UID dataMoveId,
                                       MoveKeysLock lock,
                                       FlowLock* cleanUpDataMoveParallelismLock,
                                       KeyRange keys,
                                       const DDEnabledState* ddEnabledState) {
	state KeyRange range;
	state Severity sevDm = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);
	TraceEvent(SevInfo, "CleanUpDataMoveBegin", dataMoveId).detail("DataMoveID", dataMoveId).detail("Range", keys);
	state Error lastError;
	state bool runPreCheck = true;

	wait(cleanUpDataMoveParallelismLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser = FlowLock::Releaser(*cleanUpDataMoveParallelismLock);

	try {
		loop {
			state Transaction tr(occ);
			state std::unordered_map<UID, std::vector<Shard>> physicalShardMap;
			state std::set<UID> oldDests;
			state DataMoveMetaData dataMove;

			range = KeyRange();

			try {
				tr.trState->taskID = TaskPriority::MoveKeys;
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkMoveKeysLock(&tr, lock, ddEnabledState));

				Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
				if (val.present()) {
					dataMove = decodeDataMoveValue(val.get());
					if (dataMove.ranges.empty()) {
						// Need a background cleanup
						throw retry_clean_up_datamove_tombstone_added();
					}
					TraceEvent(sevDm, "CleanUpDataMoveMetaData", dataMoveId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMoveMetaData", dataMove.toString());
					ASSERT(!dataMove.ranges.empty());
					range = dataMove.ranges.front();
					ASSERT(!range.empty());
				} else {
					if (lastError.code() == error_code_commit_unknown_result) {
						// It means the commit was succeed last time
						// For this case, safely do nothing
						TraceEvent(sevDm, "CleanUpDataMoveHaveDoneExit", dataMoveId).detail("DataMoveID", dataMoveId);
						return Void();
					}
					// If a normal cleanup sees nothing, triggers background cleanup
					dataMove = DataMoveMetaData(dataMoveId);
					dataMove.setPhase(DataMoveMetaData::Deleting);
					tr.set(dataMoveKeyFor(dataMoveId), dataMoveValue(dataMove));
					wait(tr.commit());
					TraceEvent(sevDm, "CleanUpDataMovePlaceHolder", dataMoveId).detail("DataMoveID", dataMoveId);
					throw retry_clean_up_datamove_tombstone_added();
				}

				dataMove.setPhase(DataMoveMetaData::Deleting);

				state RangeResult currentShards = wait(krmGetRanges(&tr,
				                                                    keyServersPrefix,
				                                                    range,
				                                                    SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
				                                                    SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT));
				ASSERT(!currentShards.empty());
				ASSERT(range.begin == currentShards.front().key);
				range = KeyRangeRef(range.begin, currentShards.back().key);

				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				// For each intersecting range, clear existing dest servers and checkpoints on all src servers.
				state int i = 0;
				for (; i < currentShards.size() - 1; ++i) {
					state KeyRangeRef rangeIntersectKeys(currentShards[i].key, currentShards[i + 1].key);
					state std::vector<UID> src;
					state std::vector<UID> dest;
					state UID srcId;
					state UID destId;
					decodeKeyServersValue(UIDtoTagMap, currentShards[i].value, src, dest, srcId, destId);

					// Pre validate consistency of update of keyServers and serverKeys
					if (SERVER_KNOBS->AUDIT_DATAMOVE_PRE_CHECK && runPreCheck) {
						std::vector<UID> servers(src.size() + dest.size());
						std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
						wait(auditLocationMetadataPreCheck(
						    occ, &tr, rangeIntersectKeys, servers, "cleanUpDataMoveCore_precheck", dataMoveId));
					}

					for (const auto& uid : src) {
						physicalShardMap[uid].push_back(Shard(rangeIntersectKeys, srcId));
					}

					TraceEvent(sevDm, "CleanUpDataMoveShard", dataMoveId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("ShardRange", rangeIntersectKeys)
					    .detail("Src", describe(src))
					    .detail("Dest", describe(dest))
					    .detail("SrcID", srcId)
					    .detail("DestID", destId)
					    .detail("ReadVersion", tr.getReadVersion().get());

					if (destId != dataMoveId) {
						for (const auto& uid : dest) {
							physicalShardMap[uid].push_back(Shard(rangeIntersectKeys, destId));
						}
						TraceEvent(SevWarn, "CleanUpDataMoveSkipShard", dataMoveId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("ShardRange", rangeIntersectKeys)
						    .detail("Src", describe(src))
						    .detail("Dest", describe(dest))
						    .detail("SrcID", srcId)
						    .detail("DestID", destId)
						    .detail("ReadVersion", tr.getReadVersion().get());
						continue;
					}

					for (const auto& uid : dest) {
						oldDests.insert(uid);
					}

					krmSetPreviouslyEmptyRange(&tr,
					                           keyServersPrefix,
					                           rangeIntersectKeys,
					                           keyServersValue(src, {}, srcId, UID()),
					                           currentShards[i + 1].value);
				}

				if (range.end == dataMove.ranges.front().end) {
					wait(deleteCheckpoints(&tr, dataMove.checkpoints, dataMoveId));
					tr.clear(dataMoveKeyFor(dataMoveId));
					TraceEvent(sevDm, "CleanUpDataMoveDeleteMetaData", dataMoveId)
					    .detail("DataMoveID", dataMove.toString());

				} else {
					dataMove.ranges.front() = KeyRangeRef(range.end, dataMove.ranges.front().end);
					dataMove.setPhase(DataMoveMetaData::Deleting);
					tr.set(dataMoveKeyFor(dataMoveId), dataMoveValue(dataMove));
					TraceEvent(sevDm, "CleanUpDataMovePartial", dataMoveId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("CurrentRange", range)
					    .detail("NewDataMove", dataMove.toString());
				}

				std::vector<Future<Void>> actors;
				for (const auto& uid : oldDests) {
					actors.push_back(unassignServerKeys(&tr, uid, range, physicalShardMap[uid], dataMoveId));
				}
				wait(waitForAll(actors));

				wait(tr.commit());

				TraceEvent(sevDm, "CleanUpDataMoveCommitted", dataMoveId)
				    .detail("DataMoveID", dataMoveId)
				    .detail("Range", range);

				if (range.end == dataMove.ranges.front().end) {
					// Post validate consistency of update of keyServers and serverKeys
					if (SERVER_KNOBS->AUDIT_DATAMOVE_POST_CHECK) {
						wait(auditLocationMetadataPostCheck(
						    occ, dataMove.ranges.front(), "cleanUpDataMoveCore_postcheck", dataMoveId));
					}
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_location_metadata_corruption) {
					throw location_metadata_corruption();
				} else {
					runPreCheck = false;
					lastError = e;
					wait(tr.onError(e)); // throw error if retry_clean_up_datamove_tombstone_added
					TraceEvent(SevWarn, "CleanUpDataMoveRetriableError", dataMoveId)
					    .error(lastError)
					    .detail("DataMoveRange", range.toString());
				}
			}
		}
	} catch (Error& e) {
		throw;
	}

	TraceEvent(SevInfo, "CleanUpDataMoveEnd", dataMoveId)
	    .detail("DataMoveID", dataMoveId)
	    .detail("DataMoveRange", range.toString());

	return Void();
}

ACTOR Future<Void> cleanUpDataMove(Database occ,
                                   UID dataMoveId,
                                   MoveKeysLock lock,
                                   FlowLock* cleanUpDataMoveParallelismLock,
                                   KeyRange keys,
                                   const DDEnabledState* ddEnabledState,
                                   Optional<PromiseStream<Future<Void>>> addCleanUpDataMoveActor) {
	state Severity sevDm = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);
	try {
		wait(cleanUpDataMoveCore(occ, dataMoveId, lock, cleanUpDataMoveParallelismLock, keys, ddEnabledState));
	} catch (Error& e) {
		if (e.code() == error_code_retry_clean_up_datamove_tombstone_added) {
			if (addCleanUpDataMoveActor.present()) {
				TraceEvent(SevDebug, "CleanUpDataMoveTriggerBackground", dataMoveId).detail("DataMoveID", dataMoveId);
				addCleanUpDataMoveActor.get().send(cleanUpDataMoveBackground(occ,
				                                                             dataMoveId,
				                                                             lock,
				                                                             cleanUpDataMoveParallelismLock,
				                                                             keys,
				                                                             ddEnabledState,
				                                                             /*backgroundDelaySeconds=*/10));
			} else {
				TraceEvent(SevWarn, "CleanUpDataMoveNotFound", dataMoveId).errorUnsuppressed(e);
			}
		} else {
			TraceEvent(sevDm, "CleanUpDataMoveFail", dataMoveId).errorUnsuppressed(e);
			throw e;
		}
	}

	return Void();
}

Future<Void> rawStartMovement(Database occ,
                              const MoveKeysParams& params,
                              std::map<UID, StorageServerInterface>& tssMapping) {
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		ASSERT(params.ranges.present());
		return startMoveShards(std::move(occ),
		                       params.dataMoveId,
		                       params.ranges.get(),
		                       params.destinationTeam,
		                       params.lock,
		                       params.startMoveKeysParallelismLock,
		                       params.relocationIntervalId,
		                       &tssMapping,
		                       params.ddEnabledState,
		                       params.cancelConflictingDataMoves,
		                       params.bulkLoadTaskState);
	}
	ASSERT(params.keys.present());
	return startMoveKeys(std::move(occ),
	                     params.keys.get(),
	                     params.destinationTeam,
	                     params.lock,
	                     params.startMoveKeysParallelismLock,
	                     params.relocationIntervalId,
	                     &tssMapping,
	                     params.ddEnabledState);
}

Future<Void> rawCheckFetchingState(const Database& cx,
                                   const MoveKeysParams& params,
                                   const std::map<UID, StorageServerInterface>& tssMapping) {
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		ASSERT(params.ranges.present());
		// TODO: make startMoveShards work with multiple ranges.
		ASSERT(params.ranges.get().size() == 1);
		return checkFetchingState(cx,
		                          params.healthyDestinations,
		                          params.ranges.get().at(0),
		                          params.dataMovementComplete,
		                          params.relocationIntervalId,
		                          tssMapping);
	}
	ASSERT(params.keys.present());
	return checkFetchingState(cx,
	                          params.healthyDestinations,
	                          params.keys.get(),
	                          params.dataMovementComplete,
	                          params.relocationIntervalId,
	                          tssMapping);
}

Future<Void> rawFinishMovement(Database occ,
                               const MoveKeysParams& params,
                               const std::map<UID, StorageServerInterface>& tssMapping) {
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		ASSERT(params.ranges.present());
		return finishMoveShards(std::move(occ),
		                        params.dataMoveId,
		                        params.ranges.get(),
		                        params.destinationTeam,
		                        params.lock,
		                        params.finishMoveKeysParallelismLock,
		                        params.hasRemote,
		                        params.relocationIntervalId,
		                        tssMapping,
		                        params.ddEnabledState,
		                        params.bulkLoadTaskState);
	}
	ASSERT(params.keys.present());
	return finishMoveKeys(std::move(occ),
	                      params.keys.get(),
	                      params.destinationTeam,
	                      params.lock,
	                      params.finishMoveKeysParallelismLock,
	                      params.hasRemote,
	                      params.relocationIntervalId,
	                      tssMapping,
	                      params.ddEnabledState);
}

ACTOR Future<Void> moveKeys(Database occ, MoveKeysParams params) {
	ASSERT(params.destinationTeam.size());
	std::sort(params.destinationTeam.begin(), params.destinationTeam.end());

	state std::map<UID, StorageServerInterface> tssMapping;

	wait(rawStartMovement(occ, params, tssMapping));

	state Future<Void> completionSignaller = rawCheckFetchingState(occ, params, tssMapping);

	wait(rawFinishMovement(occ, params, tssMapping));

	// This is defensive, but make sure that we always say that the movement is complete before moveKeys completes
	completionSignaller.cancel();
	if (!params.dataMovementComplete.isSet())
		params.dataMovementComplete.send(Void());

	return Void();
}

// Called by the master server to write the very first transaction to the database
// establishing a set of shard servers and all invariants of the systemKeys.
void seedShardServers(Arena& arena, CommitTransactionRef& tr, std::vector<StorageServerInterface> servers) {
	std::map<Optional<Value>, Tag> dcId_locality;
	std::map<UID, Tag> server_tag;
	int8_t nextLocality = 0;
	for (auto& s : servers) {
		if (!dcId_locality.contains(s.locality.dcId())) {
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
		tr.set(arena, metadataMap.packKey(s.id()), metadataMap.packValue(metadata));

		if (SERVER_KNOBS->TSS_HACK_IDENTITY_MAPPING) {
			// THIS SHOULD NEVER BE ENABLED IN ANY NON-TESTING ENVIRONMENT
			TraceEvent(SevError, "TSSIdentityMappingEnabled").log();
			// hack key-backed map here since we can't really change CommitTransactionRef to a RYW transaction
			Key uidRef = TupleCodec<UID>::pack(s.id());
			tr.set(arena, uidRef.withPrefix(tssMappingKeys.begin), uidRef);
		}
	}
	tr.set(arena, serverMetadataChangeKey, deterministicRandom()->randomUniqueID().toString());

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
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		const UID shardId = newDataMoveId(deterministicRandom()->randomUInt64(),
		                                  AssignEmptyRange(false),
		                                  DataMoveType::PHYSICAL,
		                                  DataMovementReason::SEED_SHARD_SERVER,
		                                  UnassignShard(false));
		ksValue = keyServersValue(serverSrcUID, /*dest=*/std::vector<UID>(), shardId, UID());
		krmSetPreviouslyEmptyRange(tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), ksValue, Value());

		for (auto& s : servers) {
			krmSetPreviouslyEmptyRange(
			    tr, arena, serverKeysPrefixFor(s.id()), allKeys, serverKeysValue(shardId), serverKeysFalse);
		}
	} else {
		krmSetPreviouslyEmptyRange(tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), ksValue, Value());

		for (auto& s : servers) {
			krmSetPreviouslyEmptyRange(
			    tr, arena, serverKeysPrefixFor(s.id()), allKeys, serverKeysTrue, serverKeysFalse);
		}
	}
}

// Unassign given key range from its current storage servers
ACTOR template <typename TrType = Transaction*>
Future<Void> unassignServerKeys(UID traceId, TrType tr, KeyRangeRef keys, std::set<UID> ignoreServers) {
	state RangeResult serverList = wait(tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY, Snapshot::True));
	ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
	for (auto& server : serverList) {
		state UID id = decodeServerListValue(server.value).id();
		Optional<Value> tag = wait(tr->get(serverTagKeyFor(id)));
		if (!tag.present()) {
			dprint("Server {} no tag\n", id.shortString());
			continue;
		}

		if (ignoreServers.contains(id)) {
			dprint("Ignore un-assignment from {} .\n", id.toString());
			continue;
		}
		RangeResult ranges = wait(krmGetRanges(tr, serverKeysPrefixFor(id), keys));

		bool owning = false;
		for (auto& r : ranges) {
			if (r.value != serverKeysFalse) {
				owning = true;
				break;
			}
		}
		if (owning) {
			wait(krmSetRangeCoalescing(tr, serverKeysPrefixFor(id), keys, allKeys, serverKeysFalse));
			dprint("Unassign {} from storage server {}\n", keys.toString(), id.toString());
			TraceEvent("UnassignKeys", traceId).detail("Keys", keys).detail("SS", id);
		}
	}
	return Void();
}

// Assign given key range to specified storage server.
ACTOR template <typename TrType = Transaction*>
Future<Void> assignKeysToServer(UID traceId, TrType tr, KeyRangeRef keys, UID serverUID) {
	state Value value = keyServersValue(std::vector<UID>({ serverUID }), std::vector<UID>(), UID(), UID());
	wait(krmSetRangeCoalescing(tr, keyServersPrefix, keys, allKeys, value));
	wait(krmSetRangeCoalescing(tr, serverKeysPrefixFor(serverUID), keys, allKeys, serverKeysTrue));
	dprint("Assign {} to server {}\n", normalKeys.toString(), serverUID.toString());
	TraceEvent("AssignKeys", traceId).detail("Keys", keys).detail("SS", serverUID);
	return Void();
}

ACTOR Future<Void> prepareBlobRestore(Database occ,
                                      MoveKeysLock lock,
                                      const DDEnabledState* ddEnabledState,
                                      UID traceId,
                                      KeyRangeRef keys,
                                      UID bmId,
                                      UID reqId) {
	state int retries = 0;
	state Transaction tr = Transaction(occ);
	ASSERT(ddEnabledState->isBlobRestorePreparing());
	loop {
		tr.debugTransaction(reqId);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			wait(checkPersistentMoveKeysLock(&tr, lock));
			UID currentOwnerId = wait(BlobGranuleRestoreConfig().lock().getD(&tr));
			if (currentOwnerId != bmId) {
				CODE_PROBE(true, "Blob migrator replaced in prepareBlobRestore");
				dprint("Blob migrator {} is replaced by {}\n", bmId.toString(), currentOwnerId.toString());
				TraceEvent("BlobMigratorReplaced", traceId).detail("Current", currentOwnerId).detail("BM", bmId);
				throw blob_migrator_replaced();
			}
			wait(unassignServerKeys(traceId, &tr, keys, { bmId }));
			wait(assignKeysToServer(traceId, &tr, keys, bmId));
			wait(tr.commit());
			TraceEvent("BlobRestorePrepare", traceId)
			    .detail("State", "PrepareTxnCommitted")
			    .detail("ReqId", reqId)
			    .detail("BM", bmId);
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));

			if (++retries > SERVER_KNOBS->BLOB_MIGRATOR_ERROR_RETRIES) {
				throw restore_error();
			}
		}
	}
}