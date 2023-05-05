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
#include "fdbclient/ClientKnobs.h"

#include "flow/actorcompiler.h" // has to be last include

ACTOR static Future<std::vector<AuditStorageState>> getAuditStatesImpl(Transaction* tr,
                                                                       AuditType type,
                                                                       int num,
                                                                       bool newFirst) {
	state std::vector<AuditStorageState> auditStates;
	loop {
		auditStates.clear();
		try {
			auto reverse = newFirst ? Reverse::True : Reverse::False;
			RangeResult res = wait(tr->getRange(auditKeyRange(type), num, Snapshot::False, reverse));
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

ACTOR Future<Void> clearAuditMetadata(Database cx, AuditType auditType, UID auditId, bool clearProgressMetadata) {
	state Transaction tr(cx);
	TraceEvent(SevDebug, "ClearAuditMetadataStart", auditId).detail("AuditKey", auditKey(auditType, auditId));

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			// Clear audit
			tr.clear(auditKey(auditType, auditId));
			if (clearProgressMetadata) {
				// Here, we do not distinguish which range exactly used by an audit
				// Simply clear any possible place of storing the progress of the audit
				tr.clear(auditServerBasedProgressRangeFor(auditType, auditId));
				tr.clear(auditRangeBasedProgressRangeFor(auditType, auditId));
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent(SevInfo, "ClearAuditMetadataError", auditId)
			    .errorUnsuppressed(e)
			    .detail("AuditKey", auditKey(auditType, auditId));
			wait(tr.onError(e));
		}
	}
	TraceEvent(SevDebug, "ClearAuditMetadataEnd", auditId).detail("AuditKey", auditKey(auditType, auditId));

	return Void();
}

ACTOR Future<Void> clearAuditMetadataForType(Database cx, AuditType auditType, int numFinishAuditToKeep) {
	state Transaction tr(cx);
	state int numFinishAuditCleaned = 0; // We regard "Complete" and "Failed" audits as finish audits
	TraceEvent(SevDebug, "DDClearAuditMetadataForTypeStart").detail("AuditType", auditType);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<AuditStorageState> auditStates =
			    wait(getAuditStatesImpl(&tr, auditType, CLIENT_KNOBS->TOO_MANY, /*newFirst=*/false));
			int numFinishAudit = 0;
			for (const auto& auditState : auditStates) { // UID from small to large since newFirst=false
				if (auditState.getPhase() == AuditPhase::Complete || auditState.getPhase() == AuditPhase::Failed) {
					numFinishAudit++;
				}
			}
			const int numFinishAuditToClean = numFinishAudit - numFinishAuditToKeep;
			numFinishAuditCleaned = 0;
			for (const auto& auditState : auditStates) {
				ASSERT(auditState.getType() == auditType);
				if (auditState.getPhase() == AuditPhase::Complete && numFinishAuditCleaned < numFinishAuditToClean) {
					tr.clear(auditKey(auditType, auditState.id));
					// No need to clear progress metadata of Complete audits
					// which has been done when Complete phase persistent
					numFinishAuditCleaned++;
				} else if (auditState.getPhase() == AuditPhase::Failed &&
				           numFinishAuditCleaned < numFinishAuditToClean) {
					tr.clear(auditKey(auditType, auditState.id));
					// Clear progress metadata of Failed audits
					// Here, we do not distinguish which range exactly used by an audit
					// Simply clear any possible place of storing the progress of the audit
					tr.clear(auditServerBasedProgressRangeFor(auditType, auditState.id));
					tr.clear(auditRangeBasedProgressRangeFor(auditType, auditState.id));
					numFinishAuditCleaned++;
				}
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent(SevInfo, "DDClearAuditMetadataForTypeError").detail("AuditType", auditType).errorUnsuppressed(e);
			wait(tr.onError(e));
		}
	}
	TraceEvent(SevDebug, "DDClearAuditMetadataForTypeEnd")
	    .detail("AuditType", auditType)
	    .detail("NumCleanedFinishAudits", numFinishAuditCleaned);

	return Void();
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
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			std::vector<AuditStorageState> auditStates =
			    wait(getAuditStatesImpl(&tr, auditState.getType(), 1, /*newFirst=*/true));
			uint64_t nextId = 1;
			if (!auditStates.empty()) {
				nextId = auditStates.front().id.first() + 1;
			}
			auditId = UID(nextId, 0LL);
			auditState.id = auditId;
			TraceEvent(SevVerbose, "PersistedNewAuditStateIdSelected", auditId)
			    .detail("AuditKey", auditKey(auditState.getType(), auditId));
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
	state AuditPhase auditPhase = auditState.getPhase();
	ASSERT(auditPhase == AuditPhase::Complete || auditPhase == AuditPhase::Failed || auditPhase == AuditPhase::Error);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(checkMoveKeysLock(&tr, lock, ddEnabled, true));
			// Clear persistent progress data of the new audit if complete
			if (auditPhase == AuditPhase::Complete) {
				// Here, we do not distinguish which range exactly used by an audit
				// Simply clear any possible place of storing the progress of the audit
				tr.clear(auditServerBasedProgressRangeFor(auditState.getType(), auditState.id));
				tr.clear(auditRangeBasedProgressRangeFor(auditState.getType(), auditState.id));
			} // We keep the progess metadata of Failed and Error audits for further investigations
			// Persist audit result
			tr.set(auditKey(auditState.getType(), auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			TraceEvent(SevDebug, "PersistAuditState", auditState.id)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditPhase", auditPhase)
			    .detail("AuditKey", auditKey(auditState.getType(), auditState.id))
			    .detail("Context", context);
			break;
		} catch (Error& e) {
			TraceEvent(SevDebug, "PersistAuditStateError", auditState.id)
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
	std::vector<AuditStorageState> auditStates = wait(getAuditStatesImpl(&tr, type, num, /*newFirst=*/true));
	return auditStates;
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
			TraceEvent(SevDebug, "GetAuditStateForRangeError").errorUnsuppressed(e).detail("AuditID", auditId);
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
			TraceEvent(SevDebug, "GetAuditStateForRangeError").errorUnsuppressed(e).detail("AuditID", auditId);
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
