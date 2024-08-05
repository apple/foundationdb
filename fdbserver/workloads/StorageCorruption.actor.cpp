/*
 * StorageCorruptionBug.h
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/StorageCorruptionBug.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/ProcessEvents.h"

#include "flow/actorcompiler.h" // has to be last include

namespace {

struct StorageCorruptionWorkload : TestWorkload {
	constexpr static auto NAME = "StorageCorruption";
	using Self = StorageCorruptionWorkload;
	std::shared_ptr<StorageCorruptionBug> bug;
	SimBugInjector bugInjector;
	double testDuration;

	StorageCorruptionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		bugInjector.enable();
		bug = bugInjector.enable<StorageCorruptionBug>(StorageCorruptionBugID());
		bugInjector.disable();
		bug->corruptionProbability = getOption(options, "corruptionProbability"_sr, 0.001);
		testDuration = getOption(options, "testDuration"_sr, 60.0);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	ACTOR static Future<Void> _start(Self* self, Database cx) {
		wait(success(setDDMode(cx, 0)));
		self->bugInjector.enable();
		wait(delay(self->testDuration));
		self->bug->corruptionProbability = 0.0;
		TraceEvent("CorruptionInjections").detail("NumCorruptions", self->bug->numHits()).log();
		self->bugInjector.disable();
		ProcessEvents::uncancellableEvent("ConsistencyCheckFailure"_sr,
		                                  [](StringRef, std::any const& data, Error const&) {
			                                  if (std::any_cast<BaseTraceEvent*>(data)->getSeverity() == SevError) {
				                                  TraceEvent("NegativeTestSuccess");
			                                  }
		                                  });
		wait(success(setDDMode(cx, 1)));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return _start(this, cx->clone());
	}
	Future<bool> check(Database const& cx) override { return true; }

private:
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<StorageCorruptionWorkload> factory;

} // namespace