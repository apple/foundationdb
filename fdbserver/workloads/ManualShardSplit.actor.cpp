/*
 * ManualShardSplit.actor.cpp
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
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ManualShardSplitWorkload : TestWorkload {
	static constexpr auto NAME = "ManualShardSplitWorkload";
	const bool enabled;

	ManualShardSplitWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId) {}

	std::string description() const override { return "ManualShardSplit"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(ManualShardSplitWorkload* self, Database cx) {
		TraceEvent("ManualShardSplitTestBegin");
		state std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                                 { "TestKeyB"_sr, "TestValueB"_sr },
		                                 { "TestKeyC"_sr, "TestValueC"_sr },
		                                 { "TestKeyD"_sr, "TestValueD"_sr },
		                                 { "TestKeyE"_sr, "TestValueE"_sr },
		                                 { "TestKeyF"_sr, "TestValueF"_sr } });

		Version ver = wait(self->populateData(self, cx, &kvs));
		TraceEvent("ManualShardSplitPopulateDataDone");
		loop {
			try {
				wait(moveShard(
				    cx->getConnectionRecord(), KeyRangeRef("TestKeyA"_sr, "TestKeyD"_sr), /*timeoutSeconds=*/30));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_dd_not_initialized || e.code() == error_code_timed_out ||
				    e.code() == error_code_broken_promise) {
					wait(delay(1.0));
					continue;
				} else {
					throw e;
				}
			}
		}
		loop {
			try {
				wait(moveShard(
				    cx->getConnectionRecord(), KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr), /*timeoutSeconds=*/30));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_dd_not_initialized || e.code() == error_code_timed_out ||
				    e.code() == error_code_broken_promise) {
					wait(delay(1.0));
					continue;
				} else {
					throw e;
				}
			}
		}
		TraceEvent("ManualShardSplitTestComplete");
		return Void();
	}

	ACTOR Future<Version> populateData(ManualShardSplitWorkload* self, Database cx, std::map<Key, Value>* kvs) {
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

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ManualShardSplitWorkload> ManualShardSplitWorkloadFactory("ManualShardSplit");
