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
	} else if (auditType == AuditType::ValidateRiskyReplica) {
		tr->clear(auditRangeBasedProgressRangeFor(auditType, auditId));
	} else {
		UNREACHABLE();
	}
	return;
}

ACTOR Future<bool> checkStorageServerRemoved(Database cx, UID ssid) {
	state bool res = false;
	state Transaction tr(cx);
	TraceEvent(SevDebug, "AuditUtilCheckStorageServerRemovedStart").detail("StorageServer", ssid);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> serverListValue = wait(tr.get(serverListKeyFor(ssid)));
			if (!serverListValue.present()) {
				res = true; // SS is removed
			}
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilStorageServerRemovedError")
			    .errorUnsuppressed(e)
			    .detail("StorageServer", ssid);
			wait(tr.onError(e));
		}
	}

	TraceEvent(SevDebug, "AuditUtilCheckStorageServerRemovedEnd").detail("StorageServer", ssid).detail("Removed", res);
	return res;
}

ACTOR Future<Void> cancelAuditMetadata(Database cx, AuditType auditType, UID auditId) {
	try {
		state Transaction tr(cx);
		TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
		           "AuditUtilCancelAuditMetadataStart",
		           auditId)
		    .detail("AuditKey", auditKey(auditType, auditId));
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> res_ = wait(tr.get(auditKey(auditType, auditId)));
				if (!res_.present()) { // has been cancelled
					break; // Nothing to cancel
				}
				state AuditStorageState toCancelState = decodeAuditStorageState(res_.get());
				// For a zombie audit, it is in running state
				ASSERT(toCancelState.id == auditId && toCancelState.getType() == auditType);
				toCancelState.setPhase(AuditPhase::Failed);
				tr.set(auditKey(toCancelState.getType(), toCancelState.id), auditStorageStateValue(toCancelState));
				clearAuditProgressMetadata(&tr, toCancelState.getType(), toCancelState.id);
				wait(tr.commit());
				TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
				           "AuditUtilCancelAuditMetadataEnd",
				           auditId)
				    .detail("AuditKey", auditKey(auditType, auditId));
				break;
			} catch (Error& e) {
				TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
				           "AuditUtilCancelAuditMetadataError",
				           auditId)
				    .detail("AuditKey", auditKey(auditType, auditId));
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		throw cancel_audit_storage_failed();
	}
	return Void();
}

AuditPhase stringToAuditPhase(std::string auditPhaseStr) {
	// Convert chars of auditPhaseStr to lower case
	std::transform(auditPhaseStr.begin(), auditPhaseStr.end(), auditPhaseStr.begin(), [](unsigned char c) {
		return std::tolower(c);
	});
	if (auditPhaseStr == "running") {
		return AuditPhase::Running;
	} else if (auditPhaseStr == "complete") {
		return AuditPhase::Complete;
	} else if (auditPhaseStr == "failed") {
		return AuditPhase::Failed;
	} else if (auditPhaseStr == "error") {
		return AuditPhase::Error;
	} else {
		return AuditPhase::Invalid;
	}
}

// This is not transactional
ACTOR Future<std::vector<AuditStorageState>> getAuditStates(Database cx,
                                                            AuditType auditType,
                                                            bool newFirst,
                                                            Optional<int> num,
                                                            Optional<AuditPhase> phase) {
	state Transaction tr(cx);
	state std::vector<AuditStorageState> auditStates;
	state Key readBegin;
	state Key readEnd;
	state Reverse reverse = newFirst ? Reverse::True : Reverse::False;
	if (num.present() && num.get() == 0) {
		return auditStates;
	}
	loop {
		try {
			readBegin = auditKeyRange(auditType).begin;
			readEnd = auditKeyRange(auditType).end;
			auditStates.clear();
			while (true) {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				KeyRangeRef rangeToRead(readBegin, readEnd);
				state RangeResult res = wait(tr.getRange(rangeToRead,
				                                         num.present() ? GetRangeLimits(num.get()) : GetRangeLimits(),
				                                         Snapshot::False,
				                                         reverse));
				for (int i = 0; i < res.size(); ++i) {
					const AuditStorageState auditState = decodeAuditStorageState(res[i].value);
					if (phase.present() && auditState.getPhase() != phase.get()) {
						continue;
					}
					auditStates.push_back(auditState);
					if (num.present() && auditStates.size() == num.get()) {
						return auditStates; // since res.more is not reliable when GetRangeLimits is set to 1
					}
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
	TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
	           "AuditUtilClearAuditMetadataForTypeStart")
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
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
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
					// For a zombie audit, it is in running state
				}
				wait(tr.commit());
				TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
				           "AuditUtilClearAuditMetadataForTypeEnd")
				    .detail("AuditType", auditType)
				    .detail("NumCleanedFinishAudits", numFinishAuditCleaned);
				break;

			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevInfo,
		           "AuditUtilClearAuditMetadataForTypeError")
		    .detail("AuditType", auditType)
		    .errorUnsuppressed(e);
		// We do not want audit cleanup effects DD
	}

	return Void();
}

ACTOR static Future<Void> checkMoveKeysLockForAudit(Transaction* tr,
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
			TraceEvent(SevVerbose, "AuditUtilCheckMoveKeysLock")
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
		TraceEvent(SevVerbose, "AuditUtilConflictWithNewOwner")
		    .detail("CurrentOwner", currentOwner.toString())
		    .detail("PrevOwner", lock.prevOwner.toString())
		    .detail("PrevWrite", lock.prevWrite.toString())
		    .detail("MyOwner", lock.myOwner.toString());
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
	TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
	           "AuditUtilPersistedNewAuditStateStart",
	           auditId);
	try {
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkMoveKeysLockForAudit(&tr, lock, ddEnabled, true));
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
				TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
				           "AuditUtilPersistedNewAuditState",
				           auditId)
				    .detail("AuditKey", auditKey(auditState.getType(), auditId));
				break;
			} catch (Error& e) {
				TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
				           "AuditUtilPersistedNewAuditStateError",
				           auditId)
				    .errorUnsuppressed(e)
				    .detail("AuditKey", auditKey(auditState.getType(), auditId));
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevWarn,
		           "AuditUtilPersistedNewAuditStateUnretriableError",
		           auditId)
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLockForAudit(&tr, lock, ddEnabled, true));
			// Clear persistent progress data of the new audit if complete
			if (auditPhase == AuditPhase::Complete) {
				clearAuditProgressMetadata(&tr, auditState.getType(), auditState.id);
			} // We keep the progess metadata of Failed and Error audits for further investigations
			// Check existing state
			Optional<Value> res_ = wait(tr.get(auditKey(auditState.getType(), auditState.id)));
			if (!res_.present()) { // has been cancelled
				throw audit_storage_cancelled();
			} else {
				const AuditStorageState currentState = decodeAuditStorageState(res_.get());
				ASSERT(currentState.id == auditState.id && currentState.getType() == auditState.getType());
				if (currentState.getPhase() == AuditPhase::Failed) {
					throw audit_storage_cancelled();
				}
			}
			// Persist audit result
			tr.set(auditKey(auditState.getType(), auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
			           "AuditUtilPersistAuditState",
			           auditState.id)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditPhase)
			    .detail("AuditKey", auditKey(auditState.getType(), auditState.id))
			    .detail("Context", context);
			break;
		} catch (Error& e) {
			TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
			           "AuditUtilPersistAuditStateError",
			           auditState.id)
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(auditKey(type, id)));
			res = res_;
			TraceEvent(type == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug, "AuditUtilReadAuditState", id)
			    .detail("AuditID", id)
			    .detail("AuditType", type)
			    .detail("AuditKey", auditKey(type, id));
			break;
		} catch (Error& e) {
			TraceEvent(
			    type == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug, "AuditUtilReadAuditStateError", id)
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

ACTOR Future<Void> persistAuditStateByRange(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> ddAuditState_ = wait(tr.get(auditKey(auditState.getType(), auditState.id)));
			if (!ddAuditState_.present()) {
				throw audit_storage_cancelled();
			}
			AuditStorageState ddAuditState = decodeAuditStorageState(ddAuditState_.get());
			ASSERT(ddAuditState.ddId.isValid());
			if (ddAuditState.ddId != auditState.ddId) {
				throw audit_storage_task_outdated(); // a new dd starts and this audit task is outdated
			}
			// It is possible ddAuditState is complete while some progress is about to persist
			// Since doAuditOnStorageServer may repeatedly issue multiple requests (see getReplyUnlessFailedFor)
			// For this case, no need to proceed. Sliently exit
			if (ddAuditState.getPhase() == AuditPhase::Complete) {
				break;
			}
			// If this is the same dd, the phase must be following
			ASSERT(ddAuditState.getPhase() == AuditPhase::Running || ddAuditState.getPhase() == AuditPhase::Failed);
			if (ddAuditState.getPhase() == AuditPhase::Failed) {
				throw audit_storage_cancelled();
			}
			wait(krmSetRange(&tr,
			                 auditRangeBasedProgressPrefixFor(auditState.getType(), auditState.id),
			                 auditState.range,
			                 auditStorageStateValue(auditState)));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
			           "AuditUtilPersistAuditStateByRangeError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditState.getPhase());
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     auditRangeBasedProgressPrefixFor(type, auditId),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;
		} catch (Error& e) {
			TraceEvent(type == AuditType::ValidateRiskyReplica ? SevVerbose : SevDebug,
			           "AuditUtilGetAuditStateForRangeError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId);
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> ddAuditState_ = wait(tr.get(auditKey(auditState.getType(), auditState.id)));
			if (!ddAuditState_.present()) {
				throw audit_storage_cancelled();
			}
			AuditStorageState ddAuditState = decodeAuditStorageState(ddAuditState_.get());
			ASSERT(ddAuditState.ddId.isValid());
			if (ddAuditState.ddId != auditState.ddId) {
				throw audit_storage_task_outdated(); // a new dd starts and this audit task is outdated
			}
			// It is possible ddAuditState is complete while some progress is about to persist
			// Since doAuditOnStorageServer may repeatedly issue multiple requests (see getReplyUnlessFailedFor)
			// For this case, no need to proceed. Sliently exit
			if (ddAuditState.getPhase() == AuditPhase::Complete) {
				break;
			}
			// If this is the same dd, the phase must be following
			ASSERT(ddAuditState.getPhase() == AuditPhase::Running || ddAuditState.getPhase() == AuditPhase::Failed);
			if (ddAuditState.getPhase() == AuditPhase::Failed) {
				throw audit_storage_cancelled();
			}
			wait(krmSetRange(
			    &tr,
			    auditServerBasedProgressPrefixFor(auditState.getType(), auditState.id, auditState.auditServerId),
			    auditState.range,
			    auditStorageStateValue(auditState)));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilPersistAuditStateByRangeError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditState.getPhase())
			    .detail("AuditServerID", auditState.auditServerId);
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     auditServerBasedProgressPrefixFor(type, auditId, auditServerId),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilGetAuditStateForRangeError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId)
			    .detail("AuditType", type)
			    .detail("AuditServerID", auditServerId);
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

ACTOR Future<bool> checkAuditProgressCompleteByRange(Database cx,
                                                     AuditType auditType,
                                                     UID auditId,
                                                     KeyRange auditRange) {
	ASSERT(auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica ||
	       auditType == AuditType::ValidateLocationMetadata || auditType == AuditType::ValidateRiskyReplica);
	state KeyRange rangeToRead = auditRange;
	state Key rangeToReadBegin = auditRange.begin;
	state int retryCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		loop {
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				state std::vector<AuditStorageState> auditStates =
				    wait(getAuditStateByRange(cx, auditType, auditId, rangeToRead));
				for (int i = 0; i < auditStates.size(); i++) {
					AuditPhase phase = auditStates[i].getPhase();
					if (phase == AuditPhase::Invalid) {
						TraceEvent(SevWarn, "AuditUtilCheckAuditProgressNotFinished")
						    .detail("AuditID", auditId)
						    .detail("AuditRange", auditRange)
						    .detail("AuditType", auditType)
						    .detail("UnfinishedRange", auditStates[i].range);
						return false;
					}
				}
				rangeToReadBegin = auditStates.back().range.end;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				if (retryCount > 30) {
					TraceEvent(SevWarn, "AuditUtilCheckAuditProgressFailed")
					    .detail("AuditID", auditId)
					    .detail("AuditRange", auditRange)
					    .detail("AuditType", auditType);
					throw audit_storage_failed();
				}
				wait(delay(0.5));
				retryCount++;
			}
		}
	}
	TraceEvent(auditType == AuditType::ValidateRiskyReplica ? SevVerbose : SevInfo, "AuditUtilCheckAuditProgressFinish")
	    .detail("AuditID", auditId)
	    .detail("AuditRange", auditRange)
	    .detail("AuditType", auditType);
	return true;
}

ACTOR Future<bool> checkAuditProgressCompleteByServer(Database cx,
                                                      AuditType auditType,
                                                      UID auditId,
                                                      KeyRange auditRange,
                                                      UID serverId,
                                                      std::shared_ptr<AsyncVar<int>> checkProgressBudget) {
	ASSERT(auditType == AuditType::ValidateStorageServerShard);
	state KeyRange rangeToRead = auditRange;
	state Key rangeToReadBegin = auditRange.begin;
	state int retryCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		loop {
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				state std::vector<AuditStorageState> auditStates =
				    wait(getAuditStateByServer(cx, auditType, auditId, serverId, rangeToRead));
				for (int i = 0; i < auditStates.size(); i++) {
					AuditPhase phase = auditStates[i].getPhase();
					if (phase == AuditPhase::Invalid) {
						TraceEvent(SevWarn, "AuditUtilCheckAuditProgressNotFinished")
						    .detail("ServerID", serverId)
						    .detail("AuditID", auditId)
						    .detail("AuditRange", auditRange)
						    .detail("AuditType", auditType)
						    .detail("UnfinishedRange", auditStates[i].range);
						checkProgressBudget->set(checkProgressBudget->get() + 1);
						return false;
					}
				}
				rangeToReadBegin = auditStates.back().range.end;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				if (retryCount > 30) {
					TraceEvent(SevWarn, "AuditUtilCheckAuditProgressFailed")
					    .detail("ServerID", serverId)
					    .detail("AuditID", auditId)
					    .detail("AuditRange", auditRange)
					    .detail("AuditType", auditType);
					checkProgressBudget->set(checkProgressBudget->get() + 1);
					throw audit_storage_failed();
				}
				wait(delay(0.5));
				retryCount++;
			}
		}
	}
	checkProgressBudget->set(checkProgressBudget->get() + 1);
	TraceEvent(SevInfo, "AuditUtilCheckAuditProgressFinish")
	    .detail("ServerID", serverId)
	    .detail("AuditID", auditId)
	    .detail("AuditRange", auditRange)
	    .detail("AuditType", auditType);
	return true;
}

// Load RUNNING audit states to resume, clean up COMPLETE and FAILED audit states
// Update ddId for RUNNING audit states
ACTOR Future<std::vector<AuditStorageState>> initAuditMetadata(Database cx,
                                                               MoveKeyLockInfo lock,
                                                               bool ddEnabled,
                                                               UID dataDistributorId,
                                                               int persistFinishAuditCount) {
	state std::unordered_map<AuditType, std::vector<AuditStorageState>> existingAuditStates;
	state std::vector<AuditStorageState> auditStatesToResume;
	state Transaction tr(cx);
	state int retryCount = 0;
	loop {
		try {
			// Load existing audit states and update ddId in audit states
			existingAuditStates.clear();
			auditStatesToResume.clear();
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLockForAudit(&tr, lock, ddEnabled, true));
			RangeResult result = wait(tr.getRange(auditKeys, CLIENT_KNOBS->TOO_MANY));
			if (result.more || result.size() >= CLIENT_KNOBS->TOO_MANY) {
				TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
				           "AuditUtilLoadMetadataIncomplete",
				           dataDistributorId)
				    .detail("ResMore", result.more)
				    .detail("ResSize", result.size());
			}
			for (int i = 0; i < result.size(); ++i) {
				auto auditState = decodeAuditStorageState(result[i].value);
				TraceEvent(SevVerbose, "AuditUtilLoadMetadataEach", dataDistributorId)
				    .detail("CurrentDDID", dataDistributorId)
				    .detail("AuditDDID", auditState.ddId)
				    .detail("AuditType", auditState.getType())
				    .detail("AuditID", auditState.id)
				    .detail("AuditPhase", auditState.getPhase());
				if (auditState.getPhase() == AuditPhase::Running) {
					AuditStorageState toUpdate = auditState;
					toUpdate.ddId = dataDistributorId;
					tr.set(auditKey(toUpdate.getType(), toUpdate.id), auditStorageStateValue(toUpdate));
				}
				existingAuditStates[auditState.getType()].push_back(auditState);
			}
			// Cleanup Complete/Failed audit metadata for each type separately
			for (const auto& [auditType, _] : existingAuditStates) {
				int numFinishAudit = 0; // "finish" audits include Complete/Failed audits
				for (const auto& auditState : existingAuditStates[auditType]) {
					if (auditState.getPhase() == AuditPhase::Complete || auditState.getPhase() == AuditPhase::Failed) {
						numFinishAudit++;
					}
				}
				const int numFinishAuditsToClear = numFinishAudit - persistFinishAuditCount;
				int numFinishAuditsCleared = 0;
				std::sort(existingAuditStates[auditType].begin(),
				          existingAuditStates[auditType].end(),
				          [](AuditStorageState a, AuditStorageState b) {
					          return a.id < b.id; // Inplacement sort in ascending order
				          });
				for (const auto& auditState : existingAuditStates[auditType]) {
					if (auditState.getPhase() == AuditPhase::Failed) {
						if (numFinishAuditsCleared < numFinishAuditsToClear) {
							// Clear both audit metadata and corresponding progress metadata
							tr.clear(auditKey(auditState.getType(), auditState.id));
							clearAuditProgressMetadata(&tr, auditState.getType(), auditState.id);
							numFinishAuditsCleared++;
							TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevInfo,
							           "AuditUtilMetadataCleared",
							           dataDistributorId)
							    .detail("AuditID", auditState.id)
							    .detail("AuditType", auditState.getType())
							    .detail("AuditRange", auditState.range);
						}
					} else if (auditState.getPhase() == AuditPhase::Complete) {
						if (numFinishAuditsCleared < numFinishAuditsToClear) {
							// Clear audit metadata only
							// No need to clear the corresponding progress metadata
							// since it has been cleared for Complete audits
							tr.clear(auditKey(auditState.getType(), auditState.id));
							numFinishAuditsCleared++;
							TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevInfo,
							           "AuditUtilMetadataCleared",
							           dataDistributorId)
							    .detail("AuditID", auditState.id)
							    .detail("AuditType", auditState.getType())
							    .detail("AuditRange", auditState.range);
						}
					} else if (auditState.getPhase() == AuditPhase::Running) {
						auditStatesToResume.push_back(auditState);
						TraceEvent(auditState.getType() == AuditType::ValidateRiskyReplica ? SevVerbose : SevInfo,
						           "AuditUtilMetadataAddedToResume",
						           dataDistributorId)
						    .detail("AuditID", auditState.id)
						    .detail("AuditType", auditState.getType())
						    .detail("AuditRange", auditState.range);
					}
				}
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_movekeys_conflict) {
				throw e;
			}
			if (retryCount > 50) {
				TraceEvent(SevWarnAlways, "AuditUtilInitAuditMetadataExceedRetryMax", dataDistributorId)
				    .errorUnsuppressed(e);
				break;
			}
			try {
				wait(tr.onError(e));
			} catch (Error& e) {
				retryCount++;
				tr.reset();
			}
		}
	}
	return auditStatesToResume;
}

// Check if any pair of ranges are exclusive with each other
// This is not a part in consistency check of audit metadata
// This is used for checking the validity of inputs to rangesSame()
bool elementsAreExclusiveWithEachOther(std::vector<KeyRange> ranges) {
	ASSERT(std::is_sorted(ranges.begin(), ranges.end(), KeyRangeRef::ArbitraryOrder()));
	for (int i = 0; i < ranges.size() - 1; ++i) {
		if (ranges[i].end > ranges[i + 1].begin) {
			TraceEvent(SevError, "AuditUtilElementsAreNotExclusiveWithEachOther").detail("Ranges", describe(ranges));
			return false;
		}
	}
	return true;
}

// Check if any range is empty in the given list of ranges
// This is not a part in consistency check of audit metadata
// This is used for checking the validity of inputs to rangesSame()
bool noEmptyRangeInRanges(std::vector<KeyRange> ranges) {
	for (const auto& range : ranges) {
		if (range.empty()) {
			return false;
		}
	}
	return true;
}

// Given a list of ranges, where ranges can overlap with each other
// Return a list of exclusive ranges which covers the ranges exactly
// the same as the input list of ranges
std::vector<KeyRange> coalesceRangeList(std::vector<KeyRange> ranges) {
	std::sort(ranges.begin(), ranges.end(), [](KeyRange a, KeyRange b) { return a.begin < b.begin; });
	std::vector<KeyRange> res;
	for (const auto& range : ranges) {
		if (res.empty()) {
			res.push_back(range);
			continue;
		}
		if (range.begin <= res.back().end) {
			if (range.end > res.back().end) { // update res.back if current range extends the back range
				KeyRange newBack = Standalone(KeyRangeRef(res.back().begin, range.end));
				res.pop_back();
				res.push_back(newBack);
			}
		} else {
			res.push_back(range);
		}
	}
	return res;
}

// Given two lists of ranges --- rangesA and rangesB, check if two lists are identical
// If not, return the mismatched two ranges of rangeA and rangeB respectively
Optional<std::pair<KeyRange, KeyRange>> rangesSame(std::vector<KeyRange> rangesA, std::vector<KeyRange> rangesB) {
	if (g_network->isSimulated()) {
		ASSERT(noEmptyRangeInRanges(rangesA));
		ASSERT(noEmptyRangeInRanges(rangesB));
	}
	KeyRange emptyRange;
	if (rangesA.empty() && rangesB.empty()) { // no mismatch
		return Optional<std::pair<KeyRange, KeyRange>>();
	} else if (rangesA.empty() && !rangesB.empty()) { // rangesA is empty while rangesB has a range
		return std::make_pair(emptyRange, rangesB.front());
	} else if (!rangesA.empty() && rangesB.empty()) { // rangesB is empty while rangesA has a range
		return std::make_pair(rangesA.front(), emptyRange);
	}
	TraceEvent(SevVerbose, "AuditUtilRangesSameBeforeSort").detail("RangesA", rangesA).detail("Rangesb", rangesB);
	// sort in ascending order
	std::sort(rangesA.begin(), rangesA.end(), [](KeyRange a, KeyRange b) { return a.begin < b.begin; });
	std::sort(rangesB.begin(), rangesB.end(), [](KeyRange a, KeyRange b) { return a.begin < b.begin; });
	TraceEvent(SevVerbose, "AuditUtilRangesSameAfterSort").detail("RangesA", rangesA).detail("Rangesb", rangesB);
	if (g_network->isSimulated()) {
		ASSERT(elementsAreExclusiveWithEachOther(rangesA));
		ASSERT(elementsAreExclusiveWithEachOther(rangesB));
	}
	if (rangesA.front().begin != rangesB.front().begin) { // rangeList heads mismatch
		return std::make_pair(rangesA.front(), rangesB.front());
	} else if (rangesA.back().end != rangesB.back().end) { // rangeList backs mismatch
		return std::make_pair(rangesA.back(), rangesB.back());
	}
	int ia = 0;
	int ib = 0;
	KeyRangeRef rangeA = rangesA[0];
	KeyRangeRef rangeB = rangesB[0];
	KeyRange lastRangeA = Standalone(rangeA);
	KeyRange lastRangeB = Standalone(rangeB);
	while (true) {
		if (rangeA.begin == rangeB.begin) {
			if (rangeA.end == rangeB.end) {
				if (rangeA.end == rangesA.back().end) {
					break;
				}
				++ia;
				++ib;
				rangeA = rangesA[ia];
				rangeB = rangesB[ib];
				lastRangeA = Standalone(rangeA);
				lastRangeB = Standalone(rangeB);
			} else if (rangeA.end > rangeB.end) {
				rangeA = KeyRangeRef(rangeB.end, rangeA.end);
				++ib;
				rangeB = rangesB[ib];
				lastRangeB = Standalone(rangeB);
			} else {
				rangeB = KeyRangeRef(rangeA.end, rangeB.end);
				++ia;
				rangeA = rangesA[ia];
				lastRangeA = Standalone(rangeA);
			}
		} else {
			return std::make_pair(lastRangeA, lastRangeB);
		}
	}
	return Optional<std::pair<KeyRange, KeyRange>>();
}

// Given an input server, get ranges within the input range via the input transaction
// from the perspective of ServerKeys system key space
// Input: (1) SS id; (2) transaction tr; (3) within range
// Return AuditGetServerKeysRes, including: (1) complete range by a single read range;
// (2) verison of the read; (3) ranges of the input SS
ACTOR Future<AuditGetServerKeysRes> getThisServerKeysFromServerKeys(UID serverID, Transaction* tr, KeyRange range) {
	state RangeResult readResult;
	state AuditGetServerKeysRes res;

	try {
		wait(store(readResult,
		           krmGetRanges(tr,
		                        serverKeysPrefixFor(serverID),
		                        range,
		                        CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
		                        CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES)));
		Future<Version> grvF = tr->getReadVersion();
		if (!grvF.isReady()) {
			TraceEvent(SevWarnAlways, "AuditUtilReadServerKeysGRVError", serverID);
			throw audit_storage_cancelled();
		}
		Version readAtVersion = grvF.get();

		TraceEvent(SevVerbose, "AuditUtilGetThisServerKeysFromServerKeysReadDone", serverID)
		    .detail("Range", range)
		    .detail("Prefix", serverKeysPrefixFor(serverID))
		    .detail("ResultSize", readResult.size())
		    .detail("AduitServerID", serverID);

		std::vector<KeyRange> ownRanges;
		for (int i = 0; i < readResult.size() - 1; ++i) {
			TraceEvent(SevVerbose, "AuditUtilGetThisServerKeysFromServerKeysAddToResult", serverID)
			    .detail("ValueIsServerKeysFalse", readResult[i].value == serverKeysFalse)
			    .detail("ServerHasKey", serverHasKey(readResult[i].value))
			    .detail("Range", KeyRangeRef(readResult[i].key, readResult[i + 1].key))
			    .detail("AduitServerID", serverID);
			if (serverHasKey(readResult[i].value)) {
				KeyRange shardRange;
				ownRanges.push_back(Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key)));
			}
		}
		const KeyRange completeRange = Standalone(KeyRangeRef(range.begin, readResult.back().key));
		TraceEvent(SevVerbose, "AuditUtilGetThisServerKeysFromServerKeysEnd", serverID)
		    .detail("AduitServerID", serverID)
		    .detail("Range", range)
		    .detail("Prefix", serverKeysPrefixFor(serverID))
		    .detail("ReadAtVersion", readAtVersion)
		    .detail("CompleteRange", completeRange)
		    .detail("ResultSize", ownRanges.size());
		res = AuditGetServerKeysRes(completeRange, readAtVersion, serverID, ownRanges, readResult.logicalSize());

	} catch (Error& e) {
		TraceEvent(SevDebug, "AuditUtilGetThisServerKeysError", serverID)
		    .errorUnsuppressed(e)
		    .detail("AduitServerID", serverID);
		throw e;
	}

	return res;
}

// Given an input server, get ranges within the input range via the input transaction
// from the perspective of KeyServers system key space
// Input: (1) Audit Server ID (for logging); (2) transaction tr; (3) within range
// Return AuditGetKeyServersRes, including : (1) complete range by a single read range; (2) verison of the read;
// (3) map between SSes and their ranges --- in KeyServers space, a range corresponds to multiple SSes
ACTOR Future<AuditGetKeyServersRes> getShardMapFromKeyServers(UID auditServerId, Transaction* tr, KeyRange range) {
	state AuditGetKeyServersRes res;
	state std::vector<Future<Void>> actors;
	state RangeResult readResult;
	state RangeResult UIDtoTagMap;
	state int64_t totalShardsCount = 0;
	state int64_t shardsInAnonymousPhysicalShardCount = 0;

	try {
		// read
		actors.push_back(store(readResult,
		                       krmGetRanges(tr,
		                                    keyServersPrefix,
		                                    range,
		                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
		                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES)));
		actors.push_back(store(UIDtoTagMap, tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY)));
		wait(waitForAll(actors));
		if (UIDtoTagMap.more || UIDtoTagMap.size() >= CLIENT_KNOBS->TOO_MANY) {
			TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
			           "AuditUtilReadKeyServersReadTagError",
			           auditServerId);
			throw audit_storage_cancelled();
		}
		Future<Version> grvF = tr->getReadVersion();
		if (!grvF.isReady()) {
			TraceEvent(SevWarnAlways, "AuditUtilReadKeyServersGRVError", auditServerId);
			throw audit_storage_cancelled();
		}
		Version readAtVersion = grvF.get();

		TraceEvent(SevVerbose, "AuditUtilGetThisServerKeysFromKeyServersReadDone", auditServerId)
		    .detail("Range", range)
		    .detail("ResultSize", readResult.size())
		    .detail("AduitServerID", auditServerId);

		// produce result
		std::unordered_map<UID, std::vector<KeyRange>> serverOwnRanges;
		for (int i = 0; i < readResult.size() - 1; ++i) {
			std::vector<UID> src;
			std::vector<UID> dest;
			UID srcID;
			UID destID;
			decodeKeyServersValue(UIDtoTagMap, readResult[i].value, src, dest, srcID, destID);
			if (srcID == anonymousShardId) {
				shardsInAnonymousPhysicalShardCount++;
			}
			totalShardsCount++;
			std::vector<UID> servers(src.size() + dest.size());
			std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
			for (auto& ssid : servers) {
				serverOwnRanges[ssid].push_back(Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key)));
			}
		}
		const KeyRange completeRange = Standalone(KeyRangeRef(range.begin, readResult.back().key));
		TraceEvent(SevInfo, "AuditUtilGetThisServerKeysFromKeyServersEnd", auditServerId)
		    .detail("Range", range)
		    .detail("CompleteRange", completeRange)
		    .detail("AtVersion", readAtVersion)
		    .detail("ShardsInAnonymousPhysicalShardCount", shardsInAnonymousPhysicalShardCount)
		    .detail("TotalShardsCount", totalShardsCount);
		res = AuditGetKeyServersRes(completeRange, readAtVersion, serverOwnRanges, readResult.logicalSize());

	} catch (Error& e) {
		TraceEvent(SevDebug, "AuditUtilGetThisServerKeysFromKeyServersError", auditServerId)
		    .errorUnsuppressed(e)
		    .detail("AuditServerId", auditServerId);
		throw e;
	}

	return res;
}
