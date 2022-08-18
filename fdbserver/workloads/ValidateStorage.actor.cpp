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

	std::string description() const override { return "ValidateStorage"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(ValidateStorage* self, Database cx) {
		// int ignore = wait(setDDMode(cx, 0));
		state std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                                 { "TestKeyB"_sr, "TestValueB"_sr },
		                                 { "TestKeyC"_sr, "TestValueC"_sr },
		                                 { "TestKeyD"_sr, "TestValueD"_sr },
		                                 { "TestKeyE"_sr, "TestValueE"_sr },
		                                 { "TestKeyF"_sr, "TestValueF"_sr } });

		Version _ = wait(self->populateData(self, cx, &kvs));

        std::cout << "TestValueWritten" << std::endl;

		TraceEvent("TestValueWritten");

		wait(self->validateData(self, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr)));
		TraceEvent("TestValueVerified");

		int ignore = wait(setDDMode(cx, 1));
		return Void();
	}

	ACTOR Future<Version> populateData(ValidateStorage* self, Database cx, std::map<Key, Value>* kvs) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state Version version;
		loop {
			state UID debugID = deterministicRandom()->randomUniqueID();
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
        std::cout << "0" << std::endl;
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

		loop {
			try {
        std::cout << "1" << std::endl;
				state RangeResult shards =
				    wait(krmGetRanges(&tr, keyServersPrefix, range, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!shards.empty() && !shards.more);

				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				state int i = 0;
				for (i = 0; i < shards.size() - 1; ++i) {

                    std::cout << "2" << std::endl;
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId, destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);

					const int idx = deterministicRandom()->randomInt(0, src.size());
					Optional<Value> serverListValue = wait(tr.get(serverListKeyFor(src[idx])));
					ASSERT(serverListValue.present());
					const StorageServerInterface ssi = decodeServerListValue(serverListValue.get());
					ValidateStorageRequest req(deterministicRandom()->randomUniqueID(),
					                           KeyRangeRef(shards[i].key, shards[i + 1].key));
					ValidateStorageResult vResult = wait(ssi.validateStorage.getReply(req));

                    std::cout << "3" << std::endl;
				}
				break;
			} catch (Error& e) {
				TraceEvent(SevWarnAlways, "TestValidateStorageError").errorUnsuppressed(e).detail("Range", range);
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestValidateStorageDone").detail("Range", range);

		return Void();
	}

	ACTOR Future<Void> readAndVerify(ValidateStorage* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

		loop {
			try {
				state Version readVersion = wait(tr.getReadVersion());
				state Optional<Value> res = wait(timeoutError(tr.get(key), 30.0));
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				TraceEvent("TestReadError").errorUnsuppressed(e);
				if (expectedValue.isError() && expectedValue.getError().code() == e.code()) {
					break;
				}
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestReadSuccess").detail("Version", readVersion);

		return Void();
	}

	ACTOR Future<Version> writeAndVerify(ValidateStorage* self, Database cx, Key key, Optional<Value> value) {
		// state Transaction tr(cx);
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state Version version;
		loop {
			state UID debugID = deterministicRandom()->randomUniqueID();
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->debugTransaction(debugID);
				if (value.present()) {
					tr->set(key, value.get());
					tr->set("Test?"_sr, value.get());
					tr->set(key, value.get());
				} else {
					tr->clear(key);
				}
				wait(timeoutError(tr->commit(), 30.0));
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr->onError(e));
			}
		}

		TraceEvent("TestCommitSuccess").detail("CommitVersion", tr->getCommittedVersion()).detail("DebugID", debugID);

		wait(self->readAndVerify(self, cx, key, value));

		return version;
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ValidateStorage> ValidateStorageFactory("ValidateStorageWorkload");