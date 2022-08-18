/*
 * ValidateStorage.cpp
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
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include <cstdint>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

struct ValidateStorage : TestWorkload {
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	FlowLock cleanUpDataMoveParallelismLock;
	const bool enabled;
	bool pass;

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

		TraceEvent("TestValueWritten").log();

		wait(self->validateData(self, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr), &kvs));
		TraceEvent("TestValueVerified").log();

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

	ACTOR Future<Void> validateData(ValidateStorage* self,
	                                Database cx,
	                                KeyRange range,
	                                std::map<Key, Value>* kvs) {
		state Transaction tr(cx);
		loop {
			state UID debugID = deterministicRandom()->randomUniqueID();
			try {
				tr.debugTransaction(debugID);
				RangeResult res = wait(tr.getRange(range, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!res.more && res.size() < CLIENT_KNOBS->TOO_MANY);

				for (const auto& kv : res) {
					ASSERT((*kvs)[kv.key] == kv.value);
				}
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}

		TraceEvent("ValidateTestDataDone").detail("DebugID", debugID);

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

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ValidateStorage> ValidateStorageFactory("PhysicalShardMove");