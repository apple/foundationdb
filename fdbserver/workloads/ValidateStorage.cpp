/*
 * ValidateStorage.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/AuditUtils.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/QuietDatabase.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include <cstdint>
#include <limits>

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
		return _start(cx);
	}

	Future<UID> triggerAuditStorageForType(Database cx,
	                                       AuditType type,
	                                       std::string context,
	                                       KeyRange auditRange = allKeys) {
		// Send audit request until the cluster accepts the request
		UID auditId;
		std::vector<KeyValueStoreType> storageEngineCollection = { KeyValueStoreType::SSD_ROCKSDB_V1,
			                                                       KeyValueStoreType::SSD_SHARDED_ROCKSDB,
			                                                       KeyValueStoreType::SSD_BTREE_V2,
			                                                       KeyValueStoreType::END };
		KeyValueStoreType storageEngine = deterministicRandom()->randomChoice(storageEngineCollection);
		while (true) {
			Error err;
			try {
				UID auditId_ = co_await auditStorage(cx->getConnectionRecord(),
				                                     auditRange,
				                                     type,
				                                     storageEngine,
				                                     /*timeoutSecond=*/300);
				auditId = auditId_;
				TraceEvent("TestAuditStorageTriggered")
				    .detail("Context", context)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type)
				    .detail("AuditStorageEngine", storageEngine)
				    .detail("AuditRange", auditRange);
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent(SevWarn, "TestAuditStorageError")
			    .errorUnsuppressed(err)
			    .detail("Context", context)
			    .detail("AuditType", type)
			    .detail("AuditStorageEngine", storageEngine)
			    .detail("AuditRange", auditRange);
			if (auditRange.empty() && err.code() == error_code_audit_storage_failed) {
				break;
			}
			co_await delay(1);
		}
		co_return auditId;
	}

	Future<Void> waitAuditStorageUntilComplete(Database cx,
	                                           AuditType type,
	                                           UID auditId,
	                                           std::string context,
	                                           bool stopWaitWhenCleared) {
		AuditStorageState auditState;
		while (true) {
			Error err;
			try {
				AuditStorageState auditState_ = co_await getAuditState(cx, type, auditId);
				auditState = auditState_;
				if (auditState.getPhase() == AuditPhase::Complete) {
					break;
				} else if (auditState.getPhase() == AuditPhase::Running) {
					TraceEvent("TestAuditStorageWait")
					    .detail("Context", context)
					    .detail("AuditID", auditId)
					    .detail("AuditType", type);
					co_await delay(30);
					continue;
				} else if (auditState.getPhase() == AuditPhase::Error) {
					break;
				} else if (auditState.getPhase() == AuditPhase::Failed) {
					break;
				} else {
					UNREACHABLE();
				}
			} catch (Error& e) {
				err = e;
			}
			if (stopWaitWhenCleared && err.code() == error_code_key_not_found) {
				break; // this audit has been cleared
			}
			TraceEvent("TestAuditStorageWaitError")
			    .errorUnsuppressed(err)
			    .detail("Context", context)
			    .detail("AuditID", auditId)
			    .detail("AuditType", type)
			    .detail("AuditState", auditState.toString());
			co_await delay(1);
		}
		TraceEvent("TestAuditStorageEnd")
		    .detail("Context", context)
		    .detail("AuditID", auditId)
		    .detail("AuditType", type)
		    .detail("AuditState", auditState.toString());
	}

	Future<Void> checkAuditStorageInternalState(Database cx, AuditType type, UID auditId, std::string context) {
		// Check the number of existing persisted audits is no more than PERSIST_FINISH_AUDIT_COUNT
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult res = co_await tr.getRange(auditKeyRange(type), GetRangeLimits());
				ASSERT(!res.more);
				for (int i = 0; i < res.size(); ++i) {
					AuditStorageState existingAuditState = decodeAuditStorageState(res[i].value);
					TraceEvent("TestAuditStorageCheckPersistStateExists")
					    .detail("Context", context)
					    .detail("ExistAuditID", existingAuditState.id)
					    .detail("ExistAuditPhase", existingAuditState.getPhase())
					    .detail("AuditID", auditId)
					    .detail("AuditType", type);
					ASSERT(existingAuditState.getPhase() == AuditPhase::Complete ||
					       existingAuditState.getPhase() == AuditPhase::Failed ||
					       existingAuditState.getPhase() == AuditPhase::Running);
					if (existingAuditState.getPhase() == AuditPhase::Complete) {
						if (type == AuditType::ValidateStorageServerShard) {
							RangeResult serverBasedRes = co_await tr.getRange(
							    auditServerBasedProgressRangeFor(type, existingAuditState.id), GetRangeLimits());
							ASSERT(serverBasedRes.empty() && !serverBasedRes.more);
						} else {
							RangeResult rangeBasedRes = co_await tr.getRange(
							    auditRangeBasedProgressRangeFor(type, existingAuditState.id), GetRangeLimits());
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
					co_await delay(30);
					tr.reset();
					continue;
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("TestAuditStorageCheckPersistStateError")
			    .errorUnsuppressed(err)
			    .detail("Context", context)
			    .detail("AuditID", auditId)
			    .detail("AuditType", type);
			co_await tr.onError(err);
		}
	}

	Future<Void> waitCancelAuditStorageUntilComplete(Database cx, AuditType type, UID auditId) {
		while (true) {
			Error err;
			try {
				UID auditId_ = co_await cancelAuditStorage(cx->getConnectionRecord(),
				                                           type,
				                                           auditId,
				                                           /*timeoutSecond=*/300);
				ASSERT(auditId == auditId_);
				TraceEvent("TestAuditStorageWaitCancelAuditStorageUntilComplete")
				    .detail("AuditID", auditId)
				    .detail("AuditType", type);
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent(SevWarn, "TestAuditStorageWaitCancelAuditStorageUntilCompleteError")
			    .errorUnsuppressed(err)
			    .detail("AuditID", auditId)
			    .detail("AuditType", type);
			co_await delay(1);
		}
	}

	Future<Void> _start(Database cx) {
		TraceEvent("ValidateStorageTestBegin");
		std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                           { "TestKeyB"_sr, "TestValueB"_sr },
		                           { "TestKeyC"_sr, "TestValueC"_sr },
		                           { "TestKeyD"_sr, "TestValueD"_sr },
		                           { "TestKeyE"_sr, "TestValueE"_sr },
		                           { "TestKeyF"_sr, "TestValueF"_sr } });

		Version ver = co_await populateData(this, cx, &kvs);

		TraceEvent("TestValueWritten").detail("AtVersion", ver);

		if (g_network->isSimulated()) {
			// NOTE: the value will be reset after consistency check
			disableConnectionFailures("AuditStorage");
		}

		testStringToAuditPhaseFunctionality();
		TraceEvent("TestAuditStorageStringToAuditPhaseFuncionalityDone");

		co_await testSSUserDataValidation(this, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr));
		TraceEvent("TestAuditStorageValidateValueDone");

		co_await testAuditStorageFunctionality(this, cx);
		TraceEvent("TestAuditStorageFunctionalityDone");

		co_await testAuditStorageIDGenerator(this, cx);
		TraceEvent("TestAuditStorageIDGeneratorDone");

		co_await testAuditStorageConcurrentRunForDifferentType(this, cx);
		TraceEvent("TestAuditStorageConcurrentRunForDifferentTypeDone");

		co_await testAuditStorageConcurrentRunForSameType(this, cx);
		TraceEvent("TestAuditStorageConcurrentRunForSameTypeDone");

		co_await testAuditStorageCancellation(this, cx);
		TraceEvent("TestAuditStorageCancellationDone");

		co_await testAuditStorageProgress(this, cx);
		TraceEvent("TestAuditStorageProgressDone");

		co_await testAuditStorageWhenDDSecurityMode(this, cx);
		TraceEvent("TestAuditStorageWhenDDSecurityModeDone");

		co_await testAuditStorageWhenDDBackToNormalMode(this, cx);
		TraceEvent("TestAuditStorageWhenDDBackToNormalModeDone");
	}

	Future<Version> populateData(ValidateStorage* self, Database cx, std::map<Key, Value>* kvs) {
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		Version version{ 0 };
		UID debugID;

		while (true) {
			debugID = deterministicRandom()->randomUniqueID();
			Error err;
			try {
				tr->debugTransaction(debugID);
				for (const auto& [key, value] : *kvs) {
					tr->set(key, value);
				}
				co_await tr->commit();
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("TestCommitError").errorUnsuppressed(err);
			co_await tr->onError(err);
		}

		TraceEvent("PopulateTestDataDone")
		    .detail("CommitVersion", tr->getCommittedVersion())
		    .detail("DebugID", debugID);

		co_return version;
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

	Future<Void> testSSUserDataValidation(ValidateStorage* self, Database cx, KeyRange range) {
		TraceEvent("TestSSUserDataValidationBegin").detail("Range", range);
		Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		int retryCount = 0;
		while (true) {
			Error err;
			try {
				RangeResult shards =
				    co_await krmGetRanges(&tr, keyServersPrefix, range, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!shards.empty() && !shards.more);

				RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				int i = 0;
				for (i = 0; i < shards.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId, destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);

					const int idx = deterministicRandom()->randomInt(0, src.size());
					Optional<Value> serverListValue = co_await tr.get(serverListKeyFor(src[idx]));
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
					    co_await timeout<AuditStorageState>(ssi.auditStorage.getReply(req), 5);
					if (!vResult.present()) {
						co_return;
					}
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			if (retryCount > 5) {
				TraceEvent(SevWarnAlways, "TestSSUserDataValidationFailed")
				    .errorUnsuppressed(err)
				    .detail("Range", range);
				break;
			} else {
				TraceEvent(SevWarn, "TestSSUserDataValidationFailedRetry")
				    .errorUnsuppressed(err)
				    .detail("Range", range)
				    .detail("RetryCount", retryCount);
				co_await delay(1);
				retryCount++;
				continue;
			}
		}

		TraceEvent("TestSSUserDataValidationDone").detail("Range", range);
	}

	Future<UID> auditStorageForType(ValidateStorage* self,
	                                Database cx,
	                                AuditType type,
	                                std::string context,
	                                bool stopWaitWhenCleared = false) {
		std::vector<KeyRangeRef> auditKeysCollection = { partialKeys1, partialKeys2, partialKeys3, partialKeys4,
			                                             partialKeys5, partialKeys6, partialKeys7, allKeys };
		KeyRangeRef auditRange = deterministicRandom()->randomChoice(auditKeysCollection);
		// Send audit request until the server accepts the request
		UID auditId = co_await self->triggerAuditStorageForType(cx, type, context, auditRange);
		if (auditRange.empty()) {
			ASSERT(!auditId.isValid());
			co_return UID();
		}
		// Wait until the request completes
		co_await self->waitAuditStorageUntilComplete(cx, type, auditId, context, stopWaitWhenCleared);
		// Check internal persist state
		co_await self->checkAuditStorageInternalState(cx, type, auditId, context);
		co_return auditId;
	}

	Future<Void> testAuditStorageFunctionality(ValidateStorage* self, Database cx) {
		UID auditIdA =
		    co_await self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestAuditStorageFunctionality");
		TraceEvent("TestFunctionalityHADone", auditIdA);
		UID auditIdB =
		    co_await self->auditStorageForType(self, cx, AuditType::ValidateReplica, "TestAuditStorageFunctionality");
		TraceEvent("TestFunctionalityReplicaDone", auditIdB);
		UID auditIdC = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateLocationMetadata, "TestAuditStorageFunctionality");
		TraceEvent("TestFunctionalityShardLocationMetadataDone", auditIdC);
		UID auditIdD = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateStorageServerShard, "TestAuditStorageFunctionality");
		TraceEvent("TestFunctionalitySSShardInfoDone", auditIdD);
		co_await self->testGetAuditStateWhenNoOngingAudit(self, cx);
		TraceEvent("TestGetAuditStateDone");
	}

	Future<Void> testAuditStorageIDGenerator(ValidateStorage* self, Database cx) {
		AuditType type = AuditType::ValidateReplica;
		TraceEvent("TestAuditStorageIDGeneratorBegin").detail("AuditType", type);
		UID auditIdA = co_await self->auditStorageForType(self, cx, type, "FirstRunInTestIDGenerator");
		UID auditIdB = co_await self->auditStorageForType(self, cx, type, "SecondRunInTestIDGenerator");
		if (auditIdA == auditIdB && auditIdA.isValid()) {
			TraceEvent(SevError, "TestAuditStorageIDGeneratorError")
			    .detail("AuditType", type)
			    .detail("AuditIDA", auditIdA)
			    .detail("AuditIDB", auditIdB);
		}
		TraceEvent("TestAuditStorageIDGeneratorEnd").detail("AuditType", type);
		;
	}

	Future<Void> testGetAuditStateWhenNoOngingAuditForType(ValidateStorage* self, Database cx, AuditType type) {
		TraceEvent("TestGetAuditStateBegin").detail("AuditType", type);
		std::vector<AuditStorageState> res1 = co_await getAuditStates(cx, type, /*newFirst=*/true, 1);
		if (res1.size() > 1) { // == 0 if empty range when testAuditStorageFunctionality
			TraceEvent(SevError, "TestGetAuditStatesError").detail("ActualResSize", res1.size());
		}
		std::vector<AuditStorageState> res2 =
		    co_await getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Invalid);
		if (res2.size() != 0) {
			TraceEvent(SevError, "TestExistingInvalidAudit")
			    .detail("ActualResSize", res2.size())
			    .detail("InputPhase", AuditPhase::Invalid);
		}
		std::vector<AuditStorageState> res3 =
		    co_await getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Running);
		if (res3.size() != 0) {
			TraceEvent(SevError, "TestExistingRunningAudit")
			    .detail("ActualResSize", res3.size())
			    .detail("InputPhase", AuditPhase::Running);
		}
		std::vector<AuditStorageState> res4 =
		    co_await getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Complete);
		for (const auto& auditState : res4) {
			if (auditState.getPhase() != AuditPhase::Complete) {
				TraceEvent(SevError, "TestGetAuditStatesByPhaseError")
				    .detail("ActualPhase", auditState.getPhase())
				    .detail("InputPhase", AuditPhase::Complete);
			}
		}
		std::vector<AuditStorageState> res5 =
		    co_await getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Failed);
		for (const auto& auditState : res5) {
			if (auditState.getPhase() != AuditPhase::Failed) {
				TraceEvent(SevError, "TestGetAuditStatesByPhaseError")
				    .detail("ActualPhase", auditState.getPhase())
				    .detail("InputPhase", AuditPhase::Failed);
			}
		}
		std::vector<AuditStorageState> res6 =
		    co_await getAuditStates(cx, type, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY, AuditPhase::Error);
		for (const auto& auditState : res6) {
			if (auditState.getPhase() != AuditPhase::Error) {
				TraceEvent(SevError, "TestGetAuditStatesByPhaseError")
				    .detail("ActualPhase", auditState.getPhase())
				    .detail("InputPhase", AuditPhase::Error);
			}
		}
		TraceEvent("TestGetAuditStateEnd").detail("AuditType", type);
	}

	Future<Void> testGetAuditStateWhenNoOngingAudit(ValidateStorage* self, Database cx) {
		co_await self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateHA);
		TraceEvent("TestGetAuditStateHADone");

		co_await self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateReplica);
		TraceEvent("TestGetAuditStateReplicaDone");

		co_await self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateLocationMetadata);
		TraceEvent("TestGetAuditStateShardLocationMetadataDone");

		co_await self->testGetAuditStateWhenNoOngingAuditForType(self, cx, AuditType::ValidateStorageServerShard);
		TraceEvent("TestGetAuditStateSSShardInfoDone");
	}

	Future<Void> testAuditStorageConcurrentRunForDifferentType(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageConcurrentRunForDifferentTypeBegin");
		std::vector<Future<Void>> fs;
		std::vector<UID> auditIds = { UID(), UID(), UID(), UID() };
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
		co_await waitForAll(fs);
		fs.clear();
		TraceEvent("TestAuditStorageConcurrentRunForDifferentTypeEnd");
	}

	Future<Void> testAuditStorageConcurrentRunForSameType(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageConcurrentRunForSameTypeBegin");
		std::vector<Future<Void>> fs;
		std::vector<UID> auditIds = { UID(), UID(), UID(), UID() };
		fs.push_back(store(
		    auditIds[0],
		    self->auditStorageForType(
		        self, cx, AuditType::ValidateReplica, "TestConcurrentRunForSameType", /*stopWaitWhenCleared=*/true)));
		fs.push_back(store(
		    auditIds[1],
		    self->auditStorageForType(
		        self, cx, AuditType::ValidateReplica, "TestConcurrentRunForSameType", /*stopWaitWhenCleared=*/true)));
		fs.push_back(store(
		    auditIds[2],
		    self->auditStorageForType(
		        self, cx, AuditType::ValidateReplica, "TestConcurrentRunForSameType", /*stopWaitWhenCleared=*/true)));
		fs.push_back(store(
		    auditIds[3],
		    self->auditStorageForType(
		        self, cx, AuditType::ValidateReplica, "TestConcurrentRunForSameType", /*stopWaitWhenCleared=*/true)));
		co_await waitForAll(fs);
		TraceEvent("TestAuditStorageConcurrentRunForSameTypeEnd");
	}

	Future<Void> testAuditStorageCancellation(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageCancellationBegin");
		UID auditId = co_await self->triggerAuditStorageForType(cx, AuditType::ValidateHA, "TestAuditCancellation");
		std::vector<Future<Void>> fs;
		fs.push_back(self->waitCancelAuditStorageUntilComplete(cx, AuditType::ValidateHA, auditId));
		fs.push_back(self->waitCancelAuditStorageUntilComplete(cx, AuditType::ValidateHA, auditId));
		fs.push_back(self->waitCancelAuditStorageUntilComplete(cx, AuditType::ValidateReplica, auditId));
		fs.push_back(self->checkAuditStorageInternalState(cx, AuditType::ValidateHA, auditId, "TestAuditCancellation"));
		fs.push_back(
		    self->checkAuditStorageInternalState(cx, AuditType::ValidateReplica, auditId, "TestAuditCancellation"));
		co_await waitForAll(fs);
		fs.clear();
		fs.push_back(self->checkAuditStorageInternalState(cx, AuditType::ValidateHA, auditId, "TestAuditCancellation"));
		fs.push_back(
		    self->checkAuditStorageInternalState(cx, AuditType::ValidateReplica, auditId, "TestAuditCancellation"));
		co_await waitForAll(fs);
		std::vector<AuditStorageState> currentAuditStates1 =
		    co_await getAuditStates(cx, AuditType::ValidateHA, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY);
		for (const auto& auditState : currentAuditStates1) {
			if (auditState.id == auditId && auditState.getPhase() != AuditPhase::Failed) {
				TraceEvent(SevError, "TestAuditStorageCancellation1Error")
				    .detail("AuditType", auditState.getType())
				    .detail("AuditID", auditId)
				    .detail("AuditPhase", auditState.getPhase());
			}
		}
		std::vector<AuditStorageState> currentAuditStates2 =
		    co_await getAuditStates(cx, AuditType::ValidateReplica, /*newFirst=*/true, CLIENT_KNOBS->TOO_MANY);
		for (const auto& auditState : currentAuditStates2) {
			if (auditState.id == auditId && auditState.getPhase() != AuditPhase::Failed) {
				TraceEvent(SevError, "TestAuditStorageCancellation2Error")
				    .detail("AuditType", auditState.getType())
				    .detail("AuditID", auditId)
				    .detail("AuditPhase", auditState.getPhase());
			}
		}
		TraceEvent("TestAuditStorageCancellationEnd");
	}

	Future<Void> persistAuditStateByRange(ValidateStorage* self, Database cx, AuditStorageState auditState) {
		Transaction tr(cx);
		RangeResult auditStates;
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				co_await krmSetRange(&tr,
				                     auditRangeBasedProgressPrefixFor(auditState.getType(), auditState.id),
				                     auditState.range,
				                     auditStorageStateValue(auditState));
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Test audit progress persist invariant
	Future<Void> testAuditStorageProgress(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageProgressBegin");
		UID auditId = deterministicRandom()->randomUniqueID();
		AuditType auditType = AuditType::ValidateHA;
		UID ddId = deterministicRandom()->randomUniqueID();
		std::vector<KeyRange> progressRangesCollection = {
			KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr),   KeyRangeRef("TestKeyB"_sr, "TestKeyBB"_sr),
			KeyRangeRef("TestKeyA"_sr, "TestKeyBBB"_sr), KeyRangeRef("TestKeyE"_sr, "TestKeyF"_sr),
			KeyRangeRef("TestKeyC"_sr, "TestKeyD"_sr),   KeyRangeRef("TestKeyBBB"_sr, "TestKeyC"_sr),
			KeyRangeRef("TestKeyBB"_sr, "TestKeyBC"_sr),
		};
		std::vector<KeyRange> progressRanges = shuffleRanges(progressRangesCollection);
		std::vector<KeyRange> alreadyPersisteRanges;
		for (int i = 0; i < progressRanges.size(); ++i) {
			AuditStorageState auditState(auditId, auditType);
			auditState.range = progressRanges[i];
			auditState.ddId = ddId;
			auditState.setPhase(AuditPhase::Complete);
			co_await self->persistAuditStateByRange(self, cx, auditState);
			alreadyPersisteRanges.push_back(progressRanges[i]);
			std::vector<AuditStorageState> auditStates = co_await getAuditStateByRange(cx, auditType, auditId, allKeys);
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
	}

	Future<Void> testAuditStorageWhenDDSecurityMode(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageWhenDDSecurityModeBegin");
		co_await setDDMode(cx, 2);
		UID auditIdA =
		    co_await self->auditStorageForType(self, cx, AuditType::ValidateHA, "TestAuditStorageWhenDDSecurityMode");
		TraceEvent("TestFunctionalityHADoneWhenDDSecurityMode", auditIdA);
		UID auditIdB = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateReplica, "TestAuditStorageWhenDDSecurityMode");
		TraceEvent("TestFunctionalityReplicaDoneWhenDDSecurityMode", auditIdB);
		UID auditIdC = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateLocationMetadata, "TestAuditStorageWhenDDSecurityMode");
		TraceEvent("TestFunctionalityShardLocationMetadataDoneWhenDDSecurityMode", auditIdC);
		UID auditIdD = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateStorageServerShard, "TestAuditStorageWhenDDSecurityMode");
		TraceEvent("TestFunctionalitySSShardInfoDoneWhenDDSecurityMode", auditIdD);
		TraceEvent("TestAuditStorageWhenDDSecurityModeEnd");
	}

	Future<Void> testAuditStorageWhenDDBackToNormalMode(ValidateStorage* self, Database cx) {
		TraceEvent("TestAuditStorageWhenDDBackToNormalModeBegin");
		co_await setDDMode(cx, 1);
		UID auditIdA = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateHA, "TestAuditStorageWhenDDBackToNormalMode");
		TraceEvent("TestFunctionalityHADoneWhenDDBackToNormalMode", auditIdA);
		UID auditIdB = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateReplica, "TestAuditStorageWhenDDBackToNormalMode");
		TraceEvent("TestFunctionalityReplicaDoneWhenDDBackToNormalMode", auditIdB);
		UID auditIdC = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateLocationMetadata, "TestAuditStorageWhenDDBackToNormalMode");
		TraceEvent("TestFunctionalityShardLocationMetadataDoneWhenDDBackToNormalMode", auditIdC);
		UID auditIdD = co_await self->auditStorageForType(
		    self, cx, AuditType::ValidateStorageServerShard, "TestAuditStorageWhenDDBackToNormalMode");
		TraceEvent("TestFunctionalitySSShardInfoDoneWhenDDBackToNormalMode", auditIdD);
		TraceEvent("TestAuditStorageWhenDDBackToNormalModeEnd");
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ValidateStorage> ValidateStorageFactory;
