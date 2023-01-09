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

	ACTOR Future<Void> _start(ValidateStorage* self, Database cx) {
		TraceEvent("ValidateStorageTestBegin");
		state std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                                 { "TestKeyB"_sr, "TestValueB"_sr },
		                                 { "TestKeyC"_sr, "TestValueC"_sr },
		                                 { "TestKeyD"_sr, "TestValueD"_sr },
		                                 { "TestKeyE"_sr, "TestValueE"_sr },
		                                 { "TestKeyF"_sr, "TestValueF"_sr } });
		state UID auditId;

		Version _ = wait(self->populateData(self, cx, &kvs));

		TraceEvent("TestValueWritten");

		wait(self->validateData(self, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr)));
		TraceEvent("TestValueVerified");

		loop {
			try {
				Optional<UID> auditId_ = wait(timeout(
				    auditStorage(cx->getConnectionRecord(), allKeys, AuditType::ValidateHA, /*async=*/true), 30));
				if (!auditId_.present()) {
					throw audit_storage_failed();
				}
				auditId = auditId_.get();
				TraceEvent("TestValidateEnd").detail("AuditID", auditId);
				break;
			} catch (Error& e) {
				TraceEvent(SevWarn, "StartAuditStorageError").errorUnsuppressed(e);
				wait(delay(1));
			}
		}

		loop {
			try {
				AuditStorageState auditState = wait(getAuditState(cx, AuditType::ValidateHA, auditId));
				if (auditState.getPhase() != AuditPhase::Complete) {
					ASSERT(auditState.getPhase() == AuditPhase::Running);
					wait(delay(30));
				} else {
					ASSERT(auditState.getPhase() == AuditPhase::Complete);
					break;
				}
			} catch (Error& e) {
				TraceEvent("WaitAuditStorageError").errorUnsuppressed(e).detail("AuditID", auditId);
				wait(delay(1));
			}
		}

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
						throw audit_storage_failed();
					}
				}
				break;
			} catch (Error& e) {
				TraceEvent(SevWarnAlways, "TestValidateStorageError").errorUnsuppressed(e).detail("Range", range);
				try {
					wait(tr.onError(e));
				} catch (Error& e) {
					if (e.code() != error_code_audit_storage_failed && e.code() != error_code_broken_promise) {
						throw e;
					}
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