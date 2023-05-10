/*
 * ValidateStorage.actor.cpp
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

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/Knobs.h"
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
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.emplace("Attrition"); }

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

	ACTOR Future<UID> auditStorageForType(Database cx, AuditType type, std::string context) {
		// Check client API
		// Send audit request until the server accepts the request
		state UID auditId;
		loop {
			try {
				UID auditId_ = wait(auditStorage(cx->getConnectionRecord(),
				                                 allKeys,
				                                 type,
				                                 /*timeoutSecond=*/300));
				auditId = auditId_;
				TraceEvent("TestAuditStorageTriggered")
				    .detail("Context", context)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type);
				break;
			} catch (Error& e) {
				TraceEvent(SevWarn, "TestAuditStorageError")
				    .errorUnsuppressed(e)
				    .detail("Context", context)
				    .detail("AuditType", type);
				wait(delay(1));
			}
		}
		// Wait until the request completes
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
				TraceEvent("TestAuditStorageWaitError")
				    .errorUnsuppressed(e)
				    .detail("Context", context)
				    .detail("AuditID", auditId)
				    .detail("AuditType", type)
				    .detail("AuditState", auditState.toString());
				wait(delay(1));
			}
		}
		// Check internal persist state
		// Check no audit is in Running or Error phase
		// Check the number of existing persisted audits is no more than PERSIST_FINISH_AUDIT_COUNT
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult res = wait(tr.getRange(auditKeyRange(type), GetRangeLimits()));
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
					       existingAuditState.getPhase() == AuditPhase::Failed);
				}
				if (res.size() > SERVER_KNOBS->PERSIST_FINISH_AUDIT_COUNT + 1) {
					TraceEvent("TestAuditStorageCheckPersistStateWaitClean")
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
		// Check no audit progress metadata exists
		tr.reset();
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult rangeBasedRes = wait(tr.getRange(auditRangeBasedProgressRangeFor(type), GetRangeLimits()));
				ASSERT(rangeBasedRes.empty() && !rangeBasedRes.more);
				RangeResult serverBasedRes =
				    wait(tr.getRange(auditServerBasedProgressRangeFor(type), GetRangeLimits()));
				ASSERT(serverBasedRes.empty() && !serverBasedRes.more);
				break;

			} catch (Error& e) {
				TraceEvent(SevDebug, "TestAuditStorageCheckPersistProgressStateError")
				    .errorUnsuppressed(e)
				    .detail("AuditID", auditId);
				wait(tr.onError(e));
			}
		}
		TraceEvent("TestAuditStorageEnd")
		    .detail("Context", context)
		    .detail("AuditID", auditId)
		    .detail("AuditType", type)
		    .detail("AuditState", auditState.toString());
		return auditId;
	}

	ACTOR Future<Void> testAuditStorageForType(ValidateStorage* self, Database cx, AuditType type) {
		state UID auditIdA = wait(self->auditStorageForType(cx, type, "FirstRun"));
		state UID auditIdB = wait(self->auditStorageForType(cx, type, "SecondRun"));
		if (auditIdA == auditIdB) {
			TraceEvent(SevError, "TestAuditStorageAuditIdError")
			    .detail("AuditType", type)
			    .detail("AuditIDA", auditIdA)
			    .detail("AuditIDB", auditIdB);
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

		wait(self->validateData(self, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr)));
		TraceEvent("TestValidateValueVerified");

		wait(self->testAuditStorageForType(self, cx, AuditType::ValidateHA));
		TraceEvent("TestValidateHADone");

		wait(self->testAuditStorageForType(self, cx, AuditType::ValidateReplica));
		TraceEvent("TestValidateReplicaDone");

		wait(self->testAuditStorageForType(self, cx, AuditType::ValidateLocationMetadata));
		TraceEvent("TestValidateShardKeyServersDone");

		wait(self->testAuditStorageForType(self, cx, AuditType::ValidateStorageServerShard));
		TraceEvent("TestValidateShardSSShardInfoDone");

		std::vector<Future<Void>> fs;
		fs.push_back(self->testAuditStorageForType(self, cx, AuditType::ValidateHA));
		fs.push_back(self->testAuditStorageForType(self, cx, AuditType::ValidateReplica));
		fs.push_back(self->testAuditStorageForType(self, cx, AuditType::ValidateLocationMetadata));
		fs.push_back(self->testAuditStorageForType(self, cx, AuditType::ValidateStorageServerShard));
		wait(waitForAll(fs));
		TraceEvent("TestValidatesConcurrentRunDone");

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

	ACTOR Future<Void> validateData(ValidateStorage* self, Database cx, KeyRange range) {
		TraceEvent("TestValidateStorageBegin").detail("Range", range);
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
					TraceEvent("TestValidateStorageSendingRequest")
					    .detail("Range", range)
					    .detail("StorageServer", ssi.toString());
					AuditStorageRequest req(deterministicRandom()->randomUniqueID(),
					                        KeyRangeRef(shards[i].key, shards[i + 1].key),
					                        AuditType::ValidateHA);
					Optional<AuditStorageState> vResult =
					    wait(timeout<AuditStorageState>(ssi.auditStorage.getReply(req), 5));
					if (!vResult.present()) {
						return Void();
					}
				}
				break;
			} catch (Error& e) {
				if (retryCount > 5) {
					TraceEvent(SevWarnAlways, "TestValidateStorageFailed").errorUnsuppressed(e).detail("Range", range);
					break;
				} else {
					TraceEvent(SevWarn, "TestValidateStorageFailedRetry")
					    .errorUnsuppressed(e)
					    .detail("Range", range)
					    .detail("RetryCount", retryCount);
					wait(delay(1));
					retryCount++;
					continue;
				}
			}
		}

		TraceEvent("TestValidateStorageDone").detail("Range", range);

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ValidateStorage> ValidateStorageFactory;