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

void clearAuditProgressMetadata(Transaction* tr, AuditType auditType, UID auditId) {
	// There are two possible places to store AuditProgressMetadata:
	// (1) auditServerBasedProgressRangeFor or (2) auditRangeBasedProgressRangeFor
	// Which place stores the progress metadata is decided by DDAudit design
	// This function enforces the DDAudit design when clear the progress metadata
	// Design: for replica/ha/locationMetadata, the audit always writes to RangeBased space
	// for SSShard, the audit always writes to ServerBased space
	// This function clears the progress metadata accordingly
	if (auditType == AuditType::ValidateStorageServerShard) {
		tr->clear(auditServerBasedProgressRangeFor(auditType, auditId));
	} else if (auditType == AuditType::ValidateHA) {
		tr->clear(auditRangeBasedProgressRangeFor(auditType, auditId));
	} else if (auditType == AuditType::ValidateReplica) {
		tr->clear(auditRangeBasedProgressRangeFor(auditType, auditId));
	} else if (auditType == AuditType::ValidateLocationMetadata) {
		tr->clear(auditRangeBasedProgressRangeFor(auditType, auditId));
	} else {
		UNREACHABLE();
	}
	return;
}

ACTOR Future<Void> clearAuditMetadata(Database cx, AuditType auditType, UID auditId, bool clearProgressMetadata) {
	state Transaction tr(cx);
	TraceEvent(SevDebug, "AuditUtilClearAuditMetadataStart", auditId).detail("AuditKey", auditKey(auditType, auditId));

	try {
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				// Clear audit metadata
				tr.clear(auditKey(auditType, auditId));
				// Clear progress metadata
				if (clearProgressMetadata) {
					clearAuditProgressMetadata(&tr, auditType, auditId);
				}
				wait(tr.commit());
				TraceEvent(SevDebug, "AuditUtilClearAuditMetadataEnd", auditId)
				    .detail("AuditKey", auditKey(auditType, auditId));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevInfo, "AuditUtilClearAuditMetadataError", auditId)
		    .errorUnsuppressed(e)
		    .detail("AuditKey", auditKey(auditType, auditId));
		// We do not want audit cleanup effects DD
	}

	return Void();
}

// This is not transactional
ACTOR Future<std::vector<AuditStorageState>> getAuditStates(Database cx,
                                                            AuditType auditType,
                                                            bool newFirst,
                                                            Optional<int> num = Optional<int>()) {
	state Transaction tr(cx);
	state std::vector<AuditStorageState> auditStates;
	state Key readBegin;
	state Key readEnd;
	state RangeResult res;
	state Reverse reverse = newFirst ? Reverse::True : Reverse::False;

	loop {
		try {
			readBegin = auditKeyRange(auditType).begin;
			readEnd = auditKeyRange(auditType).end;
			auditStates.clear();
			while (true) {
				res.clear();
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				KeyRangeRef rangeToRead(readBegin, readEnd);
				if (num.present()) {
					wait(store(res, tr.getRange(rangeToRead, num.get(), Snapshot::False, reverse)));
				} else {
					wait(store(res, tr.getRange(rangeToRead, GetRangeLimits(), Snapshot::False, reverse)));
				}
				for (int i = 0; i < res.size(); ++i) {
					auditStates.push_back(decodeAuditStorageState(res[i].value));
				}
				if (!res.more) {
					break;
				}
				if (newFirst) {
					readEnd = res.front().key; // we are reversely reading the range
				} else {
					readBegin = keyAfter(res.back().key);
				}
				tr.reset();
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return auditStates;
}

ACTOR Future<Void> clearAuditMetadataForType(Database cx,
                                             AuditType auditType,
                                             UID maxAuditIdToClear,
                                             int numFinishAuditToKeep) {
	state Transaction tr(cx);
	state int numFinishAuditCleaned = 0; // We regard "Complete" and "Failed" audits as finish audits
	TraceEvent(SevDebug, "AuditUtilClearAuditMetadataForTypeStart")
	    .detail("AuditType", auditType)
	    .detail("MaxAuditIdToClear", maxAuditIdToClear);

	try {
		loop { // Cleanup until succeed or facing unretriable error
			try {
				state std::vector<AuditStorageState> auditStates =
				    wait(getAuditStates(cx, auditType, /*newFirst=*/false));
				// auditStates has ascending order of auditIds

				// Read and clear are not atomic
				int numFinishAudit = 0;
				for (const auto& auditState : auditStates) {
					if (auditState.id.first() > maxAuditIdToClear.first()) {
						continue; // ignore any audit with a larger auditId than the input threshold
					}
					if (auditState.getPhase() == AuditPhase::Complete || auditState.getPhase() == AuditPhase::Failed) {
						numFinishAudit++;
					}
				}
				const int numFinishAuditToClean = numFinishAudit - numFinishAuditToKeep;
				numFinishAuditCleaned = 0;
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				for (const auto& auditState : auditStates) {
					if (auditState.id.first() > maxAuditIdToClear.first()) {
						continue; // ignore any audit with a larger auditId than the input threshold
					}
					ASSERT(auditState.getType() == auditType);
					if (auditState.getPhase() == AuditPhase::Complete &&
					    numFinishAuditCleaned < numFinishAuditToClean) {
						// Clear audit metadata
						tr.clear(auditKey(auditType, auditState.id));
						// No need to clear progress metadata of Complete audits
						// which has been done when Complete phase persistent
						numFinishAuditCleaned++;
					} else if (auditState.getPhase() == AuditPhase::Failed &&
					           numFinishAuditCleaned < numFinishAuditToClean) {
						// Clear audit metadata
						tr.clear(auditKey(auditType, auditState.id));
						// Clear progress metadata
						clearAuditProgressMetadata(&tr, auditType, auditState.id);
						numFinishAuditCleaned++;
					}
				}
				wait(tr.commit());
				TraceEvent(SevDebug, "AuditUtilClearAuditMetadataForTypeEnd")
				    .detail("AuditType", auditType)
				    .detail("NumCleanedFinishAudits", numFinishAuditCleaned);
				break;

			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevInfo, "AuditUtilClearAuditMetadataForTypeError")
		    .detail("AuditType", auditType)
		    .errorUnsuppressed(e);
		// We do not want audit cleanup effects DD
	}

	return Void();
}

ACTOR static Future<Void> checkMoveKeysLock(Transaction* tr,
                                            MoveKeyLockInfo lock,
                                            bool isDDEnabled,
                                            bool isWrite = true) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	if (!isDDEnabled) {
		TraceEvent(SevDebug, "AuditUtilDisabledByInMemoryCheck").log();
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
			TraceEvent("AuditUtilCheckMoveKeysLock")
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
		TraceEvent(SevDebug, "AuditUtilConflictWithNewOwner");
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
	state AuditStorageState latestExistingAuditState;
	TraceEvent(SevDebug, "AuditUtilPersistedNewAuditStateStart", auditId);
	try {
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
				RangeResult res =
				    wait(tr.getRange(auditKeyRange(auditState.getType()), 1, Snapshot::False, Reverse::True));
				ASSERT(res.size() == 0 || res.size() == 1);
				uint64_t nextId = 1;
				if (!res.empty()) {
					latestExistingAuditState = decodeAuditStorageState(res[0].value);
					if (auditId.isValid()) { // new audit state persist gets failed last time
						// Check to confirm no other actor can persist new audit state
						ASSERT(latestExistingAuditState.id.first() <= auditId.first());
						if (latestExistingAuditState.id.first() == auditId.first()) {
							// The new audit Id has been successfully persisted
							// No more action needed
							return auditId;
						} else {
							// When latestExistingAuditState.id.first() < auditId
							// The new audit Id is failed to persist
							// Check to confirm no other actor can persist new audit state
							ASSERT(auditId.first() == latestExistingAuditState.id.first() + 1);
						}
					}
					nextId = latestExistingAuditState.id.first() + 1;
				}
				auditId = UID(nextId, 0LL);
				auditState.id = auditId;
				TraceEvent(SevVerbose, "AuditUtilPersistedNewAuditStateIdSelected", auditId)
				    .detail("AuditKey", auditKey(auditState.getType(), auditId));
				tr.set(auditKey(auditState.getType(), auditId), auditStorageStateValue(auditState));
				wait(tr.commit());
				TraceEvent(SevDebug, "AuditUtilPersistedNewAuditState", auditId)
				    .detail("AuditKey", auditKey(auditState.getType(), auditId));
				break;

			} catch (Error& e) {
				TraceEvent(SevDebug, "AuditUtilPersistedNewAuditStateError", auditId)
				    .errorUnsuppressed(e)
				    .detail("AuditKey", auditKey(auditState.getType(), auditId));
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevDebug, "AuditUtilPersistedNewAuditStateUnretriableError", auditId)
		    .errorUnsuppressed(e)
		    .detail("AuditKey", auditKey(auditState.getType(), auditId));
		ASSERT_WE_THINK(e.code() == error_code_actor_cancelled || e.code() == error_code_movekeys_conflict);
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		} else {
			throw persist_new_audit_metadata_error();
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
	state AuditPhase auditPhase = auditState.getPhase();
	ASSERT(auditPhase == AuditPhase::Complete || auditPhase == AuditPhase::Failed || auditPhase == AuditPhase::Error);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			// Clear persistent progress data of the new audit if complete
			if (auditPhase == AuditPhase::Complete) {
				clearAuditProgressMetadata(&tr, auditState.getType(), auditState.id);
			} // We keep the progess metadata of Failed and Error audits for further investigations
			// Persist audit result
			tr.set(auditKey(auditState.getType(), auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			TraceEvent(SevDebug, "AuditUtilPersistAuditState", auditState.id)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditPhase)
			    .detail("AuditKey", auditKey(auditState.getType(), auditState.id))
			    .detail("Context", context);
			break;

		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilPersistAuditStateError", auditState.id)
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditPhase)
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
			TraceEvent(SevDebug, "AuditUtilReadAuditState", id)
			    .detail("AuditID", id)
			    .detail("AuditType", type)
			    .detail("AuditKey", auditKey(type, id));
			break;

		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilReadAuditStateError", id)
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

ACTOR Future<Void> persistAuditStateByRange(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(krmSetRange(&tr,
			                 auditRangeBasedProgressPrefixFor(auditState.getType(), auditState.id),
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
			                                     auditRangeBasedProgressPrefixFor(type, auditId),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;

		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilGetAuditStateForRangeError").errorUnsuppressed(e).detail("AuditID", auditId);
			wait(tr.onError(e));
		}
	}

	// For a range of state that have value read from auditRangeBasedProgressPrefixFor
	// add the state with the same range to res (these states are persisted by auditServers)
	// For a range of state that does not have value read from auditRangeBasedProgressPrefixFor
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
			wait(krmSetRange(
			    &tr,
			    auditServerBasedProgressPrefixFor(auditState.getType(), auditState.id, auditState.auditServerId),
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
			                                     auditServerBasedProgressPrefixFor(type, auditId, auditServerId),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;

		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilGetAuditStateForRangeError").errorUnsuppressed(e).detail("AuditID", auditId);
			wait(tr.onError(e));
		}
	}

	// For a range of state that have value read from auditServerBasedProgressPrefixFor
	// add the state with the same range to res (these states are persisted by auditServers)
	// For a range of state that does not have value read from auditServerBasedProgressPrefixFor
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
