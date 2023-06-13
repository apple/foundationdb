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

ACTOR Future<bool> checkStorageServerRemoved(Database cx, UID ssid) {
	state bool res = false;
	state Transaction tr(cx);
	TraceEvent(SevDebug, "AuditUtilStorageServerRemovedStart").detail("StorageServer", ssid);

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

	TraceEvent(SevDebug, "AuditUtilStorageServerRemovedEnd").detail("StorageServer", ssid).detail("Removed", res);
	return res;
}

ACTOR Future<Void> clearAuditMetadata(Database cx, AuditType auditType, UID auditId, bool clearProgressMetadata) {
	try {
		state Transaction tr(cx);
		TraceEvent(SevDebug, "AuditUtilClearAuditMetadataStart", auditId)
		    .detail("AuditKey", auditKey(auditType, auditId));
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> res_ = wait(tr.get(auditKey(auditType, auditId)));
				if (!res_.present()) { // has been cleared
					break; // Nothing to clear
				}
				state AuditStorageState toClearState = decodeAuditStorageState(res_.get());
				ASSERT(toClearState.id == auditId && toClearState.getType() == auditType);
				// For a zombie audit, it is in running state
				// Clear audit metadata
				tr.clear(auditKey(auditType, auditId));
				// clear progress metadata
				if (clearProgressMetadata) {
					clearAuditProgressMetadata(&tr, auditType, auditId);
				}
				wait(tr.commit());
				TraceEvent(SevDebug, "AuditUtilClearAuditMetadataEnd", auditId)
				    .detail("AuditKey", auditKey(auditType, auditId));
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "AuditUtilClearAuditMetadataError", auditId)
				    .detail("AuditKey", auditKey(auditType, auditId));
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		// We do not want audit cleanup effects DD
		// pass
	}
	return Void();
}

ACTOR Future<Void> cancelAuditMetadata(Database cx, AuditType auditType, UID auditId) {
	try {
		state Transaction tr(cx);
		TraceEvent(SevDebug, "AuditUtilCancelAuditMetadataStart", auditId)
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
				wait(tr.commit());
				TraceEvent(SevDebug, "AuditUtilCancelAuditMetadataEnd", auditId)
				    .detail("AuditKey", auditKey(auditType, auditId));
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "AuditUtilCancelAuditMetadataError", auditId)
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

ACTOR Future<Void> updateAuditState(Database cx, AuditStorageState auditState, MoveKeyLockInfo lock, bool ddEnabled) {
	state Transaction tr(cx);
	state bool hasCancelled = false;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			// Check existing state
			Optional<Value> res_ = wait(tr.get(auditKey(auditState.getType(), auditState.id)));
			if (!res_.present()) { // has been cancelled
				hasCancelled = true;
				break; // exit
			} else {
				const AuditStorageState currentState = decodeAuditStorageState(res_.get());
				ASSERT(currentState.id == auditState.id && currentState.getType() == auditState.getType());
				if (currentState.getPhase() == AuditPhase::Failed) {
					hasCancelled = true;
					break; // exit
				}
			}
			// Persist audit result
			tr.set(auditKey(auditState.getType(), auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilUpdateAuditStateError", auditState.id)
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditState.getPhase())
			    .detail("AuditKey", auditKey(auditState.getType(), auditState.id));
			wait(tr.onError(e));
		}
	}

	TraceEvent(SevDebug, "AuditUtilUpdateAuditStateEnd", auditState.id)
	    .detail("Cancelled", hasCancelled)
	    .detail("AuditID", auditState.id)
	    .detail("AuditType", auditState.getType())
	    .detail("AuditPhase", auditState.getPhase())
	    .detail("AuditKey", auditKey(auditState.getType(), auditState.id));

	return Void();
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
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
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
				throw audit_storage_failed(); // a new dd starts and this audit task is outdated
			}
			// It is possible ddAuditState is complete while some progress is about to persist
			// Since doAuditOnStorageServer may repeatedly issue multiple requests (see getReplyUnlessFailedFor)
			// For this case, no need to proceed. Sliently exit
			if (ddAuditState.getPhase() == AuditPhase::Complete) {
				break;
			}
			// If this is the same dd, the phase must be following
			TraceEvent("PersistAuditStateByRange")
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditState.getPhase());
			ASSERT(ddAuditState.getPhase() == AuditPhase::Running || ddAuditState.getPhase() == AuditPhase::Failed);
			if (ddAuditState.getPhase() == AuditPhase::Failed) {
				throw audit_storage_cancelled();
			}
			wait(krmSetRange(&tr,
			                 auditRangeBasedProgressPrefixFor(auditState.getType(), auditState.id),
			                 auditState.range,
			                 auditStorageStateValue(auditState)));
			break;
		} catch (Error& e) {
			TraceEvent("PersistAuditStateByRangeError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getPhase())
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> ddAuditState_ = wait(tr.get(auditKey(auditState.getType(), auditState.id)));
			if (!ddAuditState_.present()) {
				throw audit_storage_cancelled();
			}
			AuditStorageState ddAuditState = decodeAuditStorageState(ddAuditState_.get());
			ASSERT(ddAuditState.ddId.isValid());
			if (ddAuditState.ddId != auditState.ddId) {
				throw audit_storage_failed(); // a new dd starts and this audit task is outdated
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
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
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

ACTOR Future<Void> persistNewAuditScheduleState(Database cx,
                                                AuditStorageScheduleState auditScheduleState,
                                                MoveKeyLockInfo lock,
                                                bool ddEnabled) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			tr.set(auditStorageScheduleKey(auditScheduleState.getType()),
			       auditStorageScheduleStateValue(auditScheduleState));
			wait(tr.commit());
			TraceEvent(SevDebug, "AuditUtilPersistAuditSchedule")
			    .detail("AuditScheduleType", auditScheduleState.getType())
			    .detail("AuditScheduleKey", auditStorageScheduleKey(auditScheduleState.getType()));
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilPersistAuditScheduleError")
			    .errorUnsuppressed(e)
			    .detail("AuditScheduleType", auditScheduleState.getType())
			    .detail("AuditScheduleKey", auditStorageScheduleKey(auditScheduleState.getType()));
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<bool> updateAuditScheduleState(Database cx,
                                            AuditStorageScheduleState auditScheduleState,
                                            MoveKeyLockInfo lock,
                                            bool ddEnabled) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			Optional<Value> res = wait(tr.get(auditStorageScheduleKey(auditScheduleState.getType())));
			if (!res.present()) {
				return false;
			}
			AuditStorageScheduleState persistScheduleState = decodeAuditStorageScheduleState(res.get());
			if (auditScheduleState.id != persistScheduleState.id) {
				return false;
			} else if (auditScheduleState.cancelled) {
				return false;
			}
			tr.set(auditStorageScheduleKey(auditScheduleState.getType()),
			       auditStorageScheduleStateValue(auditScheduleState));
			wait(tr.commit());
			TraceEvent(SevDebug, "AuditUtilUpdateAuditSchedule")
			    .detail("AuditScheduleType", auditScheduleState.getType())
			    .detail("AuditScheduleKey", auditStorageScheduleKey(auditScheduleState.getType()));
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilUpdateAuditScheduleError")
			    .errorUnsuppressed(e)
			    .detail("AuditScheduleType", auditScheduleState.getType())
			    .detail("AuditScheduleKey", auditStorageScheduleKey(auditScheduleState.getType()));
			wait(tr.onError(e));
		}
	}

	return true;
}

ACTOR Future<Void> cancelAuditScheduleState(Database cx, AuditType auditType, MoveKeyLockInfo lock, bool ddEnabled) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			Optional<Value> res = wait(tr.get(auditStorageScheduleKey(auditType)));
			if (!res.present()) {
				TraceEvent(SevDebug, "AuditUtilCancelAuditScheduleAlreadyCancelled")
				    .detail("AuditScheduleType", auditType)
				    .detail("AuditScheduleKey", auditStorageScheduleKey(auditType));
				break; // already cancelled
			}
			AuditStorageScheduleState auditScheduleState = decodeAuditStorageScheduleState(res.get());
			if (auditScheduleState.cancelled) {
				TraceEvent(SevDebug, "AuditUtilCancelAuditScheduleAlreadyCancelled2")
				    .detail("AuditScheduleType", auditType)
				    .detail("AuditScheduleKey", auditStorageScheduleKey(auditType));
				break; // already cancelled
			}
			auditScheduleState.cancelled = true;
			tr.set(auditStorageScheduleKey(auditScheduleState.getType()),
			       auditStorageScheduleStateValue(auditScheduleState));
			wait(tr.commit());
			TraceEvent(SevDebug, "AuditUtilCancelAuditSchedule")
			    .detail("AuditScheduleType", auditType)
			    .detail("AuditScheduleKey", auditStorageScheduleKey(auditType));
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "AuditUtilCancelAuditScheduleError")
			    .errorUnsuppressed(e)
			    .detail("AuditScheduleType", auditType)
			    .detail("AuditScheduleKey", auditStorageScheduleKey(auditType));
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<std::vector<AuditStorageScheduleState>> getAuditSchedules(Database cx) {
	state Transaction tr(cx);
	state std::vector<AuditStorageScheduleState> auditScheduleStates;

	loop {
		try {
			auditScheduleStates.clear();
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state RangeResult res = wait(tr.getRange(auditScheduleKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!res.more);
			for (int i = 0; i < res.size(); ++i) {
				const AuditStorageScheduleState auditScheduleState = decodeAuditStorageScheduleState(res[i].value);
				auditScheduleStates.push_back(auditScheduleState);
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return auditScheduleStates;
}
