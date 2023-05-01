/*
 * AuditUtils.actor.cpp
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

#include "fdbclient/AuditUtils.actor.h"

#include "fdbclient/Audit.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/ClientKnobs.h"
#include <fmt/format.h>

#include "flow/actorcompiler.h" // has to be last include

ACTOR static Future<std::vector<AuditStorageState>> getLatestAuditStatesImpl(Transaction* tr, AuditType type, int num) {
	state std::vector<AuditStorageState> auditStates;
	loop {
		auditStates.clear();
		try {
			RangeResult res = wait(tr->getRange(auditKeyRange(type), num, Snapshot::False, Reverse::True));
			for (int i = 0; i < res.size(); ++i) {
				auditStates.push_back(decodeAuditStorageState(res[i].value));
			}
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return auditStates;
}

ACTOR static Future<Void> checkMoveKeysLock(Transaction* tr,
                                            MoveKeyLockInfo lock,
                                            bool isDDEnabled,
                                            bool isWrite = true) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	if (!isDDEnabled) {
		TraceEvent(SevDebug, "DDDisabledByInMemoryCheck").log();
		throw movekeys_conflict(); // need a new name
	}
	Optional<Value> readVal = wait(tr->get(moveKeysLockOwnerKey));
	UID currentOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();

	if (currentOwner == lock.prevOwner) {
		// Check that the previous owner hasn't touched the lock since we took it
		Optional<Value> readVal = wait(tr->get(moveKeysLockWriteKey));
		UID lastWrite = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
		if (lastWrite != lock.prevWrite) {
			CODE_PROBE(true, "checkMoveKeysLock: Conflict with previous owner");
			TraceEvent(SevDebug, "ConflictWithPreviousOwner");
			throw movekeys_conflict(); // need a new name
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
		TraceEvent(SevDebug, "ConflictWithNewOwner");
		throw movekeys_conflict(); // need a new name
	}
}

ACTOR Future<UID> persistNewAuditState(Database cx,
                                       AuditStorageState auditState,
                                       MoveKeyLockInfo lock,
                                       bool ddEnabled) {
	ASSERT(!auditState.id.isValid());
	state Transaction tr(cx);
	state UID auditId;

	loop {
		try {
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			std::vector<AuditStorageState> auditStates = wait(getLatestAuditStatesImpl(&tr, auditState.getType(), 1));
			uint64_t nextId = 1;
			if (!auditStates.empty()) {
				nextId = auditStates.front().id.first() + 1;
			}
			auditId = UID(nextId, 0LL);
			auditState.id = auditId;
			tr.set(auditKey(auditState.getType(), auditId), auditStorageStateValue(auditState));
			wait(tr.commit());
			TraceEvent(SevDebug, "PersistedNewAuditState", auditId)
			    .detail("AuditKey", auditKey(auditState.getType(), auditId));
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "PersistedNewAuditStateError", auditId)
			    .errorUnsuppressed(e)
			    .detail("AuditKey", auditKey(auditState.getType(), auditId));
			wait(tr.onError(e));
		}
	}

	return auditId;
}

ACTOR Future<Void> persistAuditState(Database cx,
                                     AuditStorageState auditState,
                                     std::string context,
                                     MoveKeyLockInfo lock,
                                     bool ddEnabled) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			auto auditPhase = auditState.getPhase();
			if (auditPhase == AuditPhase::Complete || auditPhase == AuditPhase::Failed ||
			    auditPhase == AuditPhase::Error) {
				wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			}

			tr.set(auditKey(auditState.getType(), auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			TraceEvent(SevDebug, "PersistAuditState", auditState.id)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditKey", auditKey(auditState.getType(), auditState.id))
			    .detail("Context", context);
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "PersistAuditStateError", auditState.id)
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditKey", auditKey(auditState.getType(), auditState.id))
			    .detail("Context", context);
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<AuditStorageState> getAuditState(Database cx, AuditType type, UID id) {
	state Transaction tr(cx);
	state Optional<Value> res;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> res_ = wait(tr.get(auditKey(type, id)));
			res = res_;
			TraceEvent(SevDebug, "ReadAuditState", id)
			    .detail("AuditID", id)
			    .detail("AuditType", type)
			    .detail("AuditKey", auditKey(type, id));
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "ReadAuditStateError", id)
			    .errorUnsuppressed(e)
			    .detail("AuditID", id)
			    .detail("AuditType", type)
			    .detail("AuditKey", auditKey(type, id));
			wait(tr.onError(e));
		}
	}

	if (!res.present()) {
		throw key_not_found();
	}

	return decodeAuditStorageState(res.get());
}

ACTOR Future<std::vector<AuditStorageState>> getLatestAuditStates(Database cx, AuditType type, int num) {
	Transaction tr(cx);
	std::vector<AuditStorageState> auditStates = wait(getLatestAuditStatesImpl(&tr, type, num));
	return auditStates;
}

ACTOR Future<Void> persistAuditStateByRange(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(krmSetRange(&tr,
			                 auditRangePrefixFor(auditState.getType(), auditState.id),
			                 auditState.range,
			                 auditStorageStateValue(auditState)));
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<std::vector<AuditStorageState>> getAuditStateByRange(Database cx,
                                                                  AuditType type,
                                                                  UID auditId,
                                                                  KeyRange range) {
	state RangeResult auditStates;
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     auditRangePrefixFor(type, auditId),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "GetAuditStateForRangeError").errorUnsuppressed(e).detail("AuditID", auditId);
			wait(tr.onError(e));
		}
	}

	// For a range of state that have value read from auditRangePrefixFor
	// add the state with the same range to res (these states are persisted by auditServers)
	// For a range of state that does not have value read from auditRangePrefixFor
	// add an default (Invalid phase) state with the same range to res (DD will start audit for these ranges)
	std::vector<AuditStorageState> res;
	for (int i = 0; i < auditStates.size() - 1; ++i) {
		KeyRange currentRange = KeyRangeRef(auditStates[i].key, auditStates[i + 1].key);
		AuditStorageState auditState(auditId, currentRange, type);
		if (!auditStates[i].value.empty()) {
			AuditStorageState auditState_ = decodeAuditStorageState(auditStates[i].value);
			auditState.setPhase(auditState_.getPhase());
			auditState.error = auditState_.error;
		}
		res.push_back(auditState);
	}

	return res;
}

ACTOR Future<Void> persistAuditStateByServer(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(krmSetRange(&tr,
			                 auditServerPrefixFor(auditState.getType(), auditState.id, auditState.auditServerId),
			                 auditState.range,
			                 auditStorageStateValue(auditState)));
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<std::vector<AuditStorageState>> getAuditStateByServer(Database cx,
                                                                   AuditType type,
                                                                   UID auditId,
                                                                   UID auditServerId,
                                                                   KeyRange range) {
	state RangeResult auditStates;
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     auditServerPrefixFor(type, auditId, auditServerId),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "GetAuditStateForRangeError").errorUnsuppressed(e).detail("AuditID", auditId);
			wait(tr.onError(e));
		}
	}

	// For a range of state that have value read from auditServerPrefixFor
	// add the state with the same range to res (these states are persisted by auditServers)
	// For a range of state that does not have value read from auditServerPrefixFor
	// add an default (Invalid phase) state with the same range to res (DD will start audit for these ranges)
	std::vector<AuditStorageState> res;
	for (int i = 0; i < auditStates.size() - 1; ++i) {
		KeyRange currentRange = KeyRangeRef(auditStates[i].key, auditStates[i + 1].key);
		AuditStorageState auditState(auditId, currentRange, type);
		if (!auditStates[i].value.empty()) {
			AuditStorageState auditState_ = decodeAuditStorageState(auditStates[i].value);
			auditState.setPhase(auditState_.getPhase());
			auditState.error = auditState_.error;
		}
		res.push_back(auditState);
	}

	return res;
}

ACTOR Future<std::string> checkMigrationProgress(Database cx) {
	state Key begin = allKeys.begin;
	state int numShards = 0;
	state int numPhysicalShards = 0;

	while (begin < allKeys.end) {
		// RYW to optimize re-reading the same key ranges
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				KeyRangeRef currentKeys(begin, allKeys.end);
				RangeResult shards = wait(
				    krmGetRanges(tr, keyServersPrefix, currentKeys, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY));

				for (int i = 0; i < shards.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId;
					UID destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);
					if (srcId != anonymousShardId) {
						++numPhysicalShards;
					}
				}

				begin = shards.back().key;
				numShards += shards.size() - 1;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	return fmt::format("Total number of shards: {}, number of physical shards: {}", numShards, numPhysicalShards);
}