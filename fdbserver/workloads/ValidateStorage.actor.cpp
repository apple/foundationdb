/*
 * ValidateStorage.actor.cpp
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

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include <cstdint>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
std::string printValue(const ErrorOr<Optional<Value>>& value) {
	if (value.isError()) {
		return value.getError().name();
	}
	return value.get().present() ? value.get().get().toString() : "Value Not Found.";
}
} // namespace

std::vector<KeyRange> shuffleRanges(std::vector<KeyRange> inputRanges) {
	std::vector<KeyRange> outputRanges;
	while (!inputRanges.empty()) {
		int idx = deterministicRandom()->randomInt(0, inputRanges.size());
		outputRanges.push_back(inputRanges[idx]);
		inputRanges.erase(inputRanges.begin() + idx);
	}
	return outputRanges;
}

const KeyRangeRef partialKeys1 = KeyRangeRef(KeyRef(), "\x01"_sr);
const KeyRangeRef partialKeys2 = KeyRangeRef("\x01"_sr, "\x02"_sr);
const KeyRangeRef partialKeys3 = KeyRangeRef("\x02"_sr, "\x03"_sr);
const KeyRangeRef partialKeys4 = KeyRangeRef("\x03"_sr, "\xfe"_sr);
const KeyRangeRef partialKeys5 = KeyRangeRef("\xfe"_sr, "\xff"_sr);
const KeyRangeRef partialKeys6 = KeyRangeRef("\x05"_sr, "\xaa"_sr);
const KeyRangeRef partialKeys7 = KeyRangeRef(KeyRef(), KeyRef());
struct ValidateStorage : TestWorkload {
	static constexpr auto NAME = "ValidateStorageWorkload";

	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	FlowLock cleanUpDataMoveParallelismLock;
	const bool enabled;
	bool pass;

	// We disable failure injection because there is an irrelevant issue:
	// Remote tLog is failed to rejoin to CC
	// Once this issue is fixed, we should be able to enable the failure injection
	// This workload is not compatible with following workload because they will race in changing the DD mode
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomMoveKeys",
		             "DataLossRecovery",
		             "IDDTxnProcessorApiCorrectness",
		             "PerpetualWiggleStatsWorkload",
		             "PhysicalShardMove",
		             "StorageCorruption",
		             "StorageServerCheckpointRestoreTest",
		             "Attrition" });
	}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	ValidateStorage(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<UID> triggerAuditStorageForType(Database cx,
	                                             AuditType type,
	                                             std::string context,
	                                             KeyRange auditRange = allKeys) {
		// Send audit request until the cluster accepts the request
		state UID auditId;
		std::vector<KeyValueStoreType> storageEngineCollection = { KeyValueStoreType::SSD_ROCKSDB_V1,
			                                                       KeyValueStoreType::SSD_SHARDED_ROCKSDB,
			                                                       KeyValueStoreType::SSD_BTREE_V2,
			                                                       KeyValueStoreType::END };
		state KeyValueStoreType storageEngine = deterministicRandom()->randomChoice(storageEngineCollection);
		loop {
			try {
				UID auditId_ = wait(auditStorage(cx->getConnectionRecord(),
				                                 auditRange,
				                                 type,
				                                 storageEngine,
				                                 /*timeoutSecond=*/300));
				auditId = auditId_;
				TraceEvent("TestAuditStorageTriggered")
				    .detail("Context", context)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type)
				    .detail("AuditStorageEngine", storageEngine)
				    .detail("AuditRange", auditRange);
				break;
			} catch (Error& e) {
				TraceEvent(SevWarn, "TestAuditStorageError")
				    .errorUnsuppressed(e)
				    .detail("Context", context)
				    .detail("AuditType", type)
				    .detail("AuditStorageEngine", storageEngine)
				    .detail("AuditRange", auditRange);
				if (auditRange.empty() && e.code() == error_code_audit_storage_failed) {
					break;
				}
				wait(delay(1));
			}
		}
		return auditId;
	}

	ACTOR Future<Void> waitAuditStorageUntilComplete(Database cx,
	                                                 AuditType type,
	                                                 UID auditId,
	                                                 std::string context,
	                                                 bool stopWaitWhenCleared) {
		state AuditStorageState auditState;
		loop {
			try {
				AuditStorageState auditState_ = wait(getAuditState(cx, type, auditId));
				auditState = auditState_;
				if (auditState.getPhase() == AuditPhase::Complete) {
					break;
				} else if (auditState.getPhase() == AuditPhase::Running) {
					TraceEvent("TestAuditStorageWait")
					    .detail("Context", context)
					    .detail("AuditID", auditId)
					    .detail("AuditType", type);
					wait(delay(30));
					continue;
				} else if (auditState.getPhase() == AuditPhase::Error) {
					break;
				} else if (auditState.getPhase() == AuditPhase::Failed) {
					break;
				} else {
					UNREACHABLE();
				}
			} catch (Error& e) {
				if (stopWaitWhenCleared && e.code() == error_code_key_not_found) {
					break; // this audit has been cleared
				}
				TraceEvent("TestAuditStorageWaitError")
				    .errorUnsuppressed(e)
				    .detail("Context", context)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type)
				    .detail("AuditState", auditState.toString());
				wait(delay(1));
			}
		}
		TraceEvent("TestAuditStorageEnd")
		    .detail("Context", context)
		    .detail("AuditID", auditId)
		    .detail("AuditType", type)
		    .detail("AuditState", auditState.toString());
		return Void();
	}

	ACTOR Future<Void> checkAuditStorageInternalState(Database cx, AuditType type, UID auditId, std::string context) {
		// Check the number of existing persisted audits is no more than PERSIST_FINISH_AUDIT_COUNT
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state RangeResult res = wait(tr.getRange(auditKeyRange(type), GetRangeLimits()));
				ASSERT(!res.more);
				state int i = 0;
				for (; i < res.size(); ++i) {
					AuditStorageState existingAuditState = decodeAuditStorageState(res[i].value);
					TraceEvent("TestAuditStorageCheckPersistStateExists")
					    .detail("Context", context)
					    .detail("ExistAuditID", existingAuditState.id)
					    .detail("ExistAuditPhase", existingAuditState.getPhase())
					    .detail("AuditID", auditId)
					    .detail("AuditType", type);
					ASSERT(existingAuditState.getPhase() != AuditPhase::Invalid);
					if (existingAuditState.getPhase() == AuditPhase::Complete) {
						if (type == AuditType::ValidateStorageServerShard) {
							RangeResult serverBasedRes = wait(tr.getRange(
							    auditServerBasedProgressRangeFor(type, existingAuditState.id), GetRangeLimits()));
							ASSERT(serverBasedRes.empty() && !serverBasedRes.more);
						} else {
							RangeResult rangeBasedRes = wait(tr.getRange(
							    auditRangeBasedProgressRangeFor(type, existingAuditState.id), GetRangeLimits()));
							ASSERT(rangeBasedRes.empty() && !rangeBasedRes.more);
						}
					}
				}
				if (res.size() > SERVER_KNOBS->PERSIST_FINISH_AUDIT_COUNT + 5) {
					// Note that 5 is the sum of 4 + 1
					// In the test, we issue at most 4 concurrent audits at the same time
					// The 4 concurrent audits may not be complete in time
					// So, the cleanup does not precisely guarantee PERSIST_FINISH_AUDIT_COUNT
					TraceEvent("TestAuditStorageCheckPersistStateWaitClean")
					    .detail("ExistCount", res.size())
					    .detail("Context", context)
					    .detail("AuditID", auditId)
					    .detail("AuditType", type);
					wait(delay(30));
					tr.reset();
					continue;
				}
				break;
			} catch (Error& e) {
				TraceEvent("TestAuditStorageCheckPersistStateError")
				    .errorUnsuppressed(e)
				    .detail("Context", context)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type);
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> waitCancelAuditStorageUntilComplete(Database cx, AuditType type, UID auditId) {
		loop {
			try {
				state UID auditId_ = wait(cancelAuditStorage(cx->getConnectionRecord(),
				                                             type,
				                                             auditId,
				                                             /*timeoutSecond=*/300));
				ASSERT(auditId == auditId_);
				TraceEvent("TestAuditStorageWaitCancelAuditStorageUntilComplete")
				    .detail("AuditID", auditId)
				    .detail("AuditType", type);
				break;
			} catch (Error& e) {
				TraceEvent(SevWarn, "TestAuditStorageWaitCancelAuditStorageUntilCompleteError")
				    .errorUnsuppressed(e)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type);
				wait(delay(1));
			}
		}
		return Void();
	}

	ACTOR Future<Void> _start(ValidateStorage* self, Database cx) {
		TraceEvent("ValidateStorageTestBegin");
		state std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                                 { "TestKeyB"_sr, "TestValueB"_sr },
		                                 { "TestKeyC"_sr, "TestValueC"_sr },
		                                 { "TestKeyD"_sr, "TestValueD"_sr },
		                                 { "TestKeyE"_sr, "TestValueE"_sr },
		                                 { "TestKeyF"_sr, "TestValueF"_sr } });

		Version ver = wait(self->populateData(self, cx, &kvs));

		TraceEvent("TestValueWritten").detail("AtVersion", ver);

		if (g_network->isSimulated()) {
			// NOTE: the value will be reset after consistency check
			disableConnectionFailures("AuditStorage");
		}

		/*self->testStringToAuditPhaseFunctionality();
		TraceEvent("TestAuditStorageStringToAuditPhaseFuncionalityDone");

		wait(self->testSSUserDataValidation(self, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr)));
		TraceEvent("TestAuditStorageValidateValueDone");

		wait(self->testAuditStorageFunctionality(self, cx));
		TraceEvent("TestAuditStorageFunctionalityDone");

		wait(self->testAuditStorageIDGenerator(self, cx));
		TraceEvent("TestAuditStorageIDGeneratorDone");

		wait(self->testAuditStorageConcurrentRunForDifferentType(self, cx));
		TraceEvent("TestAuditStorageConcurrentRunForDifferentTypeDone");

		wait(self->testAuditStorageConcurrentRunForSameType(self, cx));
		TraceEvent("TestAuditStorageConcurrentRunForSameTypeDone");

		wait(self->testAuditStorageCancellation(self, cx));
		TraceEvent("TestAuditStorageCancellationDone");

		wait(self->testAuditStorageProgress(self, cx));
		TraceEvent("TestAuditStorageProgressDone");

		wait(self->testAuditStorageWhenDDSecurityMode(self, cx));
		TraceEvent("TestAuditStorageWhenDDSecurityModeDone");

		wait(self->testAuditStorageWhenDDBackToNormalMode(self, cx));
		TraceEvent("TestAuditStorageWhenDDBackToNormalModeDone");*/

		wait(self->testInjectInconsistency(self, cx));
		TraceEvent("TestInjectInconsistencyDone");

		return Void();
	}

	ACTOR Future<Version> populateData(ValidateStorage* self, Database cx, std::map<Key, Value>* kvs) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state Version version;
		state UID debugID;

		loop {
			debugID = deterministicRandom()->randomUniqueID();
			try {
				tr->debugTransaction(debugID);
				for (const auto& [key, value] : *kvs) {
					tr->set(key, value);
				}
				wait(tr->commit());
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr->onError(e));
			}
		}

		TraceEvent("PopulateTestDataDone")
		    .detail("CommitVersion", tr->getCommittedVersion())
		    .detail("DebugID", debugID);

		return version;
	}

	void testStringToAuditPhaseFunctionality() {
		AuditPhase phase = AuditPhase::Invalid;
		std::string inputStr = "RUNNING";
		phase = stringToAuditPhase(inputStr);
		if (phase != AuditPhase::Running) {
			TraceEvent(SevError, "TestStringToAuditPhaseError").detail("Input", inputStr);
		}
		inputStr = "RUnnING";
		phase = stringToAuditPhase(inputStr);
		if (phase != AuditPhase::Running) {
			TraceEvent(SevError, "TestStringToAuditPhaseError").detail("Input", inputStr);
		}
		inputStr = "error";
		phase = stringToAuditPhase(inputStr);
		if (phase != AuditPhase::Error) {
			TraceEvent(SevError, "TestStringToAuditPhaseError").detail("Input", inputStr);
		}
		inputStr = "error123";
		phase = stringToAuditPhase(inputStr);
		if (phase != AuditPhase::Invalid) {
			TraceEvent(SevError, "TestStringToAuditPhaseError").detail("Input", inputStr);
		}
		inputStr = "123";
		phase = stringToAuditPhase(inputStr);
		if (phase != AuditPhase::Invalid) {
			TraceEvent(SevError, "TestStringToAuditPhaseError").detail("Input", inputStr);
		}
		inputStr = "12Failed";
		phase = stringToAuditPhase(inputStr);
		if (phase != AuditPhase::Invalid) {
			TraceEvent(SevError, "TestStringToAuditPhaseError").detail("Input", inputStr);
		}
		return;
	}

	ACTOR Future<Void> testSSUserDataValidation(ValidateStorage* self, Database cx, KeyRange range) {
		TraceEvent("TestSSUserDataValidationBegin").detail("Range", range);
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		state int retryCount = 0;
		loop {
			try {
				state RangeResult shards =
				    wait(krmGetRanges(&tr, keyServersPrefix, range, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!shards.empty() && !shards.more);

				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				state int i = 0;
				for (i = 0; i < shards.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId, destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);

					const int idx = deterministicRandom()->randomInt(0, src.size());
					Optional<Value> serverListValue = wait(tr.get(serverListKeyFor(src[idx])));
					ASSERT(serverListValue.present());
					const StorageServerInterface ssi = decodeServerListValue(serverListValue.get());
					TraceEvent("TestSSUserDataValidationSendingRequest")
					    .detail("Range", range)
					    .detail("StorageServer", ssi.toString());
					AuditStorageRequest req(deterministicRandom()->randomUniqueID(),
					                        KeyRangeRef(shards[i].key, shards[i + 1].key),
					                        AuditType::ValidateHA);
					req.ddId = deterministicRandom()->randomUniqueID();
					Optional<AuditStorageState> vResult =
					    wait(timeout<AuditStorageState>(ssi.auditStorage.getReply(req), 5));
					if (!vResult.present()) {
						return Void();
					}
				}
				break;
			} catch (Error& e) {
				if (retryCount > 5) {
					TraceEvent(SevWarnAlways, "TestSSUserDataValidationFailed")
					    .errorUnsuppressed(e)
					    .detail("Range", range);
					break;
				} else {
					TraceEvent(SevWarn, "TestSSUserDataValidationFailedRetry")
					    .errorUnsuppressed(e)
					    .detail("Range", range)
					    .detail("RetryCount", retryCount);
					wait(delay(1));
					retryCount++;
					continue;
				}
			}
		}

		TraceEvent("TestSSUserDataValidationDone").detail("Range", range);

		return Void();
	}

	ACTOR Future<UID> auditStorageForType(ValidateStorage* self,
	                                      Database cx,
	                                      AuditType type,
	                                      std::string context,
	                                      bool checkAllKeys = false,
	                                      bool stopWaitWhenCleared = false) {
		std::vector<KeyRangeRef> auditKeysCollection = { partialKeys1, partialKeys2, partialKeys3, partialKeys4,
			                                             partialKeys5, partialKeys6, partialKeys7, allKeys };
		state KeyRangeRef auditRange = deterministicRandom()->randomChoice(auditKeysCollection);
		if (checkAllKeys) {
			auditRange = allKeys;
		}
		// Send audit request until the server accepts the request
		state UID auditId = wait(self->triggerAuditStorageForType(cx, type, context, auditRange));
		if (auditRange.empty()) {
			ASSERT(!auditId.isValid());
			return UID();
		}
		// Wait until the request completes
		wait(self->waitAuditStorageUntilComplete(cx, type, auditId, context, stopWaitWhenCleared));
		// Check internal persist state
		wait(self->checkAuditStorageInternalState(cx, type, auditId, context));
		return auditId;
	}

	ACTOR Future<Void> testAuditStorageFunctionality(ValidateStorage* self, Database cx) {
		UID auditIdA =
		    wait(self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestAuditStorageFunctionality"));
		TraceEvent("TestFunctionalityHADone", auditIdA);
		UID auditIdB =
		    wait(self->auditStorageForType(self, cx, AuditType::ValidateReplica, "TestAuditStorageFunctionality"));
		TraceEvent("TestFunctionalityReplicaDone", auditIdB);
		UID auditIdC = wait(
		    self->auditStorageForType(self, cx, AuditType::ValidateLocationMetadata, "TestAuditStorageFunctionality"));
		TraceEvent("TestFunctionalityShardLocationMetadataDone", auditIdC);
		UID auditIdD = wait(self->auditStorageForType(
		    self, cx, AuditType::ValidateStorageServerShard, "TestAuditStorageFunctionality"));
		TraceEvent("TestFunctionalitySSShardInfoDone", auditIdD);
		wait(self->testGetAuditStateWhenNoOngingAudit(self, cx));
		TraceEvent("TestGetAuditStateDone");
		return Void();
	}

	ACTOR Future<Void> testAuditStorageIDGenerator(ValidateStorage* self, Database cx) {
		state AuditType type = AuditType::ValidateReplica;
		TraceEvent("TestAuditStorageIDGeneratorBegin").detail("AuditType", type);
		state UID auditIdA = wait(self->auditStorageForType(self, cx, type, "FirstRunInTestIDGenerator"));
		state UID auditIdB = wait(self->auditStorageForType(self, cx, type, "SecondRunInTestIDGenerator"));
		if (auditIdA == auditIdB && auditIdA.isValid()) {
			TraceEvent(SevError, "TestAuditStorageIDGeneratorError")
			    .detail("AuditType", type)
			    .detail("AuditIDA", auditIdA)
			    .detail("AuditIDB", auditIdB);
		}
		TraceEvent("TestAuditStorageIDGeneratorEnd").detail("AuditType", type);
		;
		return Void();
	}

	ACTOR Future<Void> testGetAuditStateWhenNoOngingAuditForType(ValidateStorage* self, Database cx, AuditType type) {
		TraceEvent("TestGetAuditStateBegin").detail("AuditType", type);
		std::vector<AuditStorageState> res1 = wait(getAuditStates(cx, type, /*newFirst=*/true, 1));
		if (res1.size() > 1) { // == 0 if empty range when testAuditStorageFunctionality
			TraceEvent(SevError, "TestGetAuditStatesError").detail("ActualResSize", res1.size());
		}
		std::vector<AuditStorageState> res2 =
		    wait(getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Invalid));
		if (res2.size() != 0) {
			TraceEvent(SevError, "TestExistingInvalidAudit")
			    .detail("ActualResSize", res2.size())
			    .detail("InputPhase", AuditPhase::Invalid);
		}
		std::vector<AuditStorageState> res3 =
		    wait(getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Running));
		if (res3.size() != 0) {
			TraceEvent(SevError, "TestExistingRunningAudit")
			    .detail("ActualResSize", res3.size())
			    .detail("InputPhase", AuditPhase::Running);
		}
		std::vector<AuditStorageState> res4 =
		    wait(getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Complete));
		for (const auto& auditState : res4) {
			if (auditState.getPhase() != AuditPhase::Complete) {
				TraceEvent(SevError, "TestGetAuditStatesByPhaseError")
				    .detail("ActualPhase", auditState.getPhase())
				    .detail("InputPhase", AuditPhase::Complete);
			}
		}
		std::vector<AuditStorageState> res5 =
		    wait(getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Failed));
		for (const auto& auditState : res5) {
			if (auditState.getPhase() != AuditPhase::Failed) {
				TraceEvent(SevError, "TestGetAuditStatesByPhaseError")
				    .detail("ActualPhase", auditState.getPhase())
				    .detail("InputPhase", AuditPhase::Failed);
			}
		}
		std::vector<AuditStorageState> res6 =
		    wait(getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Error));
		for (const auto& auditState : res6) {
			if (auditState.getPhase() != AuditPhase::Error) {
				TraceEvent(SevError, "TestGetAuditStatesByPhaseError")
				    .detail("ActualPhase", auditState.getPhase())
				    .detail("InputPhase", AuditPhase::Error);
			}
		}
		TraceEvent("TestGetAuditStateEnd").detail("AuditType", type);
		return Void();
	}

	ACTOR Future<Void> testGetAuditStateWhenNoOngingAudit(ValidateStorage* self, Database cx) {
		wait(self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateHA));
		TraceEvent("TestGetAuditStateHADone");

		wait(self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateReplica));
		TraceEvent("TestGetAuditStateReplicaDone");

		wait(self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateLocationMetadata));
		TraceEvent("TestGetAuditStateShardLocationMetadataDone");

		wait(self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateStorageServerShard));
		TraceEvent("TestGetAuditStateSSShardInfoDone");
		return Void();
	}

	ACTOR Future<Void> testAuditStorageConcurrentRunForDifferentType(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageConcurrentRunForDifferentTypeBegin");
		state std::vector<Future<Void>> fs;
		state std::vector<UID> auditIds = { UID(), UID(), UID(), UID() };
		fs.push_back(
		    store(auditIds[0],
		          self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestConcurrentRunForDifferentType")));
		fs.push_back(store(
		    auditIds[1],
		    self->auditStorageForType(self, cx, AuditType::ValidateReplica, "TestConcurrentRunForDifferentType")));
		fs.push_back(store(auditIds[2],
		                   self->auditStorageForType(
		                       self, cx, AuditType::ValidateLocationMetadata, "TestConcurrentRunForDifferentType")));
		fs.push_back(store(auditIds[3],
		                   self->auditStorageForType(
		                       self, cx, AuditType::ValidateStorageServerShard, "TestConcurrentRunForDifferentType")));
		wait(waitForAll(fs));
		fs.clear();
		TraceEvent("TestAuditStorageConcurrentRunForDifferentTypeEnd");
		return Void();
	}

	ACTOR Future<Void> testAuditStorageConcurrentRunForSameType(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageConcurrentRunForSameTypeBegin");
		state std::vector<Future<Void>> fs;
		state std::vector<UID> auditIds = { UID(), UID(), UID(), UID() };
		fs.push_back(store(auditIds[0],
		                   self->auditStorageForType(self,
		                                             cx,
		                                             AuditType::ValidateReplica,
		                                             "TestConcurrentRunForSameType",
		                                             /*checkAllKeys=*/false,
		                                             /*stopWaitWhenCleared=*/true)));
		fs.push_back(store(auditIds[1],
		                   self->auditStorageForType(self,
		                                             cx,
		                                             AuditType::ValidateReplica,
		                                             "TestConcurrentRunForSameType",
		                                             /*checkAllKeys=*/false,
		                                             /*stopWaitWhenCleared=*/true)));
		fs.push_back(store(auditIds[2],
		                   self->auditStorageForType(self,
		                                             cx,
		                                             AuditType::ValidateReplica,
		                                             "TestConcurrentRunForSameType",
		                                             /*checkAllKeys=*/false,
		                                             /*stopWaitWhenCleared=*/true)));
		fs.push_back(store(auditIds[3],
		                   self->auditStorageForType(self,
		                                             cx,
		                                             AuditType::ValidateReplica,
		                                             "TestConcurrentRunForSameType",
		                                             /*checkAllKeys=*/false,
		                                             /*stopWaitWhenCleared=*/true)));
		wait(waitForAll(fs));
		TraceEvent("TestAuditStorageConcurrentRunForSameTypeEnd");
		return Void();
	}

	ACTOR Future<Void> testAuditStorageCancellation(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageCancellationBegin");
		state UID auditId = wait(self->triggerAuditStorageForType(cx, AuditType::ValidateHA, "TestAuditCancellation"));
		state std::vector<Future<Void>> fs;
		fs.push_back(self->waitCancelAuditStorageUntilComplete(cx, AuditType::ValidateHA, auditId));
		fs.push_back(self->waitCancelAuditStorageUntilComplete(cx, AuditType::ValidateHA, auditId));
		fs.push_back(self->waitCancelAuditStorageUntilComplete(cx, AuditType::ValidateReplica, auditId));
		fs.push_back(self->checkAuditStorageInternalState(cx, AuditType::ValidateHA, auditId, "TestAuditCancellation"));
		fs.push_back(
		    self->checkAuditStorageInternalState(cx, AuditType::ValidateReplica, auditId, "TestAuditCancellation"));
		wait(waitForAll(fs));
		fs.clear();
		fs.push_back(self->checkAuditStorageInternalState(cx, AuditType::ValidateHA, auditId, "TestAuditCancellation"));
		fs.push_back(
		    self->checkAuditStorageInternalState(cx, AuditType::ValidateReplica, auditId, "TestAuditCancellation"));
		wait(waitForAll(fs));
		state std::vector<AuditStorageState> currentAuditStates1 =
		    wait(getAuditStates(cx, AuditType::ValidateHA, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY));
		for (const auto& auditState : currentAuditStates1) {
			if (auditState.id == auditId && auditState.getPhase() != AuditPhase::Failed) {
				TraceEvent(SevError, "TestAuditStorageCancellation1Error")
				    .detail("AuditType", auditState.getType())
				    .detail("AuditID", auditId)
				    .detail("AuditPhase", auditState.getPhase());
			}
		}
		state std::vector<AuditStorageState> currentAuditStates2 =
		    wait(getAuditStates(cx, AuditType::ValidateReplica, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY));
		for (const auto& auditState : currentAuditStates2) {
			if (auditState.id == auditId && auditState.getPhase() != AuditPhase::Failed) {
				TraceEvent(SevError, "TestAuditStorageCancellation2Error")
				    .detail("AuditType", auditState.getType())
				    .detail("AuditID", auditId)
				    .detail("AuditPhase", auditState.getPhase());
			}
		}
		TraceEvent("TestAuditStorageCancellationEnd");
		return Void();
	}

	ACTOR Future<Void> persistAuditStateByRange(ValidateStorage* self, Database cx, AuditStorageState auditState) {
		state Transaction tr(cx);
		state RangeResult auditStates;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(krmSetRange(&tr,
				                 auditRangeBasedProgressPrefixFor(auditState.getType(), auditState.id),
				                 auditState.range,
				                 auditStorageStateValue(auditState)));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	// Test audit progress persist invariant
	ACTOR Future<Void> testAuditStorageProgress(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageProgressBegin");
		state UID auditId = deterministicRandom()->randomUniqueID();
		state AuditType auditType = AuditType::ValidateHA;
		state UID ddId = deterministicRandom()->randomUniqueID();
		std::vector<KeyRange> progressRangesCollection = {
			KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr),   KeyRangeRef("TestKeyB"_sr, "TestKeyBB"_sr),
			KeyRangeRef("TestKeyA"_sr, "TestKeyBBB"_sr), KeyRangeRef("TestKeyE"_sr, "TestKeyF"_sr),
			KeyRangeRef("TestKeyC"_sr, "TestKeyD"_sr),   KeyRangeRef("TestKeyBBB"_sr, "TestKeyC"_sr),
			KeyRangeRef("TestKeyBB"_sr, "TestKeyBC"_sr),
		};
		state std::vector<KeyRange> progressRanges = shuffleRanges(progressRangesCollection);
		state int i = 0;
		state std::vector<KeyRange> alreadyPersisteRanges;
		for (; i < progressRanges.size(); i++) {
			state AuditStorageState auditState(auditId, auditType);
			auditState.range = progressRanges[i];
			auditState.ddId = ddId;
			auditState.setPhase(AuditPhase::Complete);
			wait(self->persistAuditStateByRange(self, cx, auditState));
			alreadyPersisteRanges.push_back(progressRanges[i]);
			std::vector<AuditStorageState> auditStates = wait(getAuditStateByRange(cx, auditType, auditId, allKeys));
			for (int i = 0; i < auditStates.size(); i++) {
				KeyRange toCompare = auditStates[i].range;
				bool overlapped = false;
				bool fullyCovered = false;
				std::vector<KeyRange> unCoveredRanges;
				unCoveredRanges.push_back(toCompare);
				// check if toCompare is overlapped/fullyCovered by alreadyPersisteRanges
				for (const auto& persistedRange : alreadyPersisteRanges) {
					KeyRange overlappedRange = toCompare & persistedRange;
					if (!overlappedRange.empty()) {
						overlapped = true;
					}
					std::vector<KeyRange> unCoveredRangesNow;
					for (const auto& unCoveredRange : unCoveredRanges) {
						std::vector<KeyRangeRef> tmp = unCoveredRange - persistedRange;
						for (const auto& item : tmp) {
							unCoveredRangesNow.push_back(item);
						}
					}
					unCoveredRanges = unCoveredRangesNow;
				}
				fullyCovered = unCoveredRanges.empty();
				if (fullyCovered) { // toCompare is fully covered by alreadyPersisteRanges
					ASSERT(auditStates[i].getPhase() == AuditPhase::Complete);
				} else {
					// toCompare cannot be partially covered by alreadyPersisteRanges
					ASSERT(!overlapped);
					ASSERT(auditStates[i].getPhase() == AuditPhase::Invalid);
				}
			}
		}
		TraceEvent("TestAuditStorageProgressEnd");
		return Void();
	}

	ACTOR Future<Void> testAuditStorageWhenDDSecurityMode(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageWhenDDSecurityModeBegin");
		wait(success(setDDMode(cx, 2)));
		UID auditIdA =
		    wait(self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestAuditStorageWhenDDSecurityMode"));
		TraceEvent("TestFunctionalityHADoneWhenDDSecurityMode", auditIdA);
		UID auditIdB =
		    wait(self->auditStorageForType(self, cx, AuditType::ValidateReplica, "TestAuditStorageWhenDDSecurityMode"));
		TraceEvent("TestFunctionalityReplicaDoneWhenDDSecurityMode", auditIdB);
		UID auditIdC = wait(self->auditStorageForType(
		    self, cx, AuditType::ValidateLocationMetadata, "TestAuditStorageWhenDDSecurityMode"));
		TraceEvent("TestFunctionalityShardLocationMetadataDoneWhenDDSecurityMode", auditIdC);
		UID auditIdD = wait(self->auditStorageForType(
		    self, cx, AuditType::ValidateStorageServerShard, "TestAuditStorageWhenDDSecurityMode"));
		TraceEvent("TestFunctionalitySSShardInfoDoneWhenDDSecurityMode", auditIdD);
		TraceEvent("TestAuditStorageWhenDDSecurityModeEnd");
		return Void();
	}

	ACTOR Future<Void> testAuditStorageWhenDDBackToNormalMode(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageWhenDDBackToNormalModeBegin");
		wait(success(setDDMode(cx, 1)));
		UID auditIdA =
		    wait(self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestAuditStorageWhenDDBackToNormalMode"));
		TraceEvent("TestFunctionalityHADoneWhenDDBackToNormalMode", auditIdA);
		UID auditIdB = wait(
		    self->auditStorageForType(self, cx, AuditType::ValidateReplica, "TestAuditStorageWhenDDBackToNormalMode"));
		TraceEvent("TestFunctionalityReplicaDoneWhenDDBackToNormalMode", auditIdB);
		UID auditIdC = wait(self->auditStorageForType(
		    self, cx, AuditType::ValidateLocationMetadata, "TestAuditStorageWhenDDBackToNormalMode"));
		TraceEvent("TestFunctionalityShardLocationMetadataDoneWhenDDBackToNormalMode", auditIdC);
		UID auditIdD = wait(self->auditStorageForType(
		    self, cx, AuditType::ValidateStorageServerShard, "TestAuditStorageWhenDDBackToNormalMode"));
		TraceEvent("TestFunctionalitySSShardInfoDoneWhenDDBackToNormalMode", auditIdD);
		TraceEvent("TestAuditStorageWhenDDBackToNormalModeEnd");
		return Void();
	}

	ACTOR Future<Void> testInjectInconsistency(ValidateStorage* self, Database cx) {
		TraceEvent("TestInjectInconsistencyBegin");
		state Transaction tr(cx);
		state RangeResult readResult;
		state RangeResult UIDtoTagMap;
		state KeyRange selectedRange = KeyRangeRef("A"_sr, "B"_sr);
		state UID selectedSrc;
		state UID selectedPSID = deterministicRandom()->randomUniqueID();
		state std::vector<UID> servers;
		state bool newServerFound = false;
		state bool addInjection = false;
		loop {
			servers.clear();
			newServerFound = false;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				std::vector<Future<Void>> fs;
				fs.push_back(store(readResult,
				                   krmGetRanges(&tr,
				                                keyServersPrefix,
				                                selectedRange,
				                                CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                                CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES)));
				fs.push_back(store(UIDtoTagMap, tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY)));
				wait(waitForAll(fs));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				state int i = 0;
				for (; i < readResult.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcID;
					UID destID;
					decodeKeyServersValue(UIDtoTagMap, readResult[i].value, src, dest, srcID, destID);
					servers = std::vector<UID>(src.size() + dest.size());
					std::merge(src.begin(), src.end(), dest.begin(), dest.end(), servers.begin());
					std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
					for (const auto& interf : interfs) {
						if (std::find(servers.begin(), servers.end(), interf.uniqueID) == servers.end()) {
							newServerFound = true;
							selectedSrc = interf.uniqueID;
							break;
						}
					}
					if (!newServerFound) {
						selectedSrc = deterministicRandom()->randomChoice(servers);
					}
					selectedRange = Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key));
					break;
				}
				TraceEvent("TestInjectInconsistencyInjectConfig")
				    .detail("SelectedSrc", selectedSrc.toString())
				    .detail("SelectedRange", selectedRange)
				    .detail("OriginalServers", describe(servers))
				    .detail("InjectionType",
				            newServerFound ? (addInjection ? "AddSelectedNewServer" : "ChangeToSelectedNewServer")
				                           : "RemainSelectedExistingServer");
				if (newServerFound) {
					if (!addInjection) {
						servers.clear();
					}
				} else {
					servers.clear();
				}
				servers.push_back(selectedSrc);
				std::sort(servers.begin(), servers.end());
				tr.set(keyServersKey(readResult[i].key), keyServersValue(servers, {}, selectedPSID, UID()));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		try {
			UID ignored = wait(self->auditStorageForType(
			    self, cx, AuditType::ValidateStorageServerShard, "TestInjectedCorruption", true, true));
			UID ignored = wait(self->auditStorageForType(
			    self, cx, AuditType::ValidateLocationMetadata, "TestInjectedCorruption", true, true));
			UID ignored = wait(
			    self->auditStorageForType(self, cx, AuditType::ValidateReplica, "TestInjectedCorruption", true, true));
			UID ignored =
			    wait(self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestInjectedCorruption", true, true));
		} catch (Error& e) {
			TraceEvent("TestInjectInconsistencyAuditError").error(e);
		}
		TraceEvent("TestInjectInconsistencyValidationDone");
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ValidateStorage> ValidateStorageFactory;