/*
 * DummyWorkload.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// The workload that do nothing. It can be used for waiting for quiescence
struct DummyWorkload : TestWorkload {
	static constexpr auto NAME = "DummyWorkload";
	bool displayWorkers;
	double displayDelay;

	DummyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		displayWorkers = getOption(options, "displayWorkers"_sr, true);
		displayDelay = getOption(options, "displayDelay"_sr, 0.0);
	}

	Future<Void> start(Database const& cx) override {
		if ((clientId == 0) && (displayWorkers)) {
			return _start(this, cx);
		}
		return Void();
	}

	ACTOR static Future<Void> _start(DummyWorkload* self, Database cx) {
		if (self->displayDelay > 0.0)
			wait(delay(self->displayDelay));
		g_simulator->displayWorkers();
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DummyWorkload> DummyWorkloadFactory;
