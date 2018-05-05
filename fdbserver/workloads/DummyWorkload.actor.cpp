/*
 * DummyWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "workloads.h"

// The workload that do nothing. It can be used for waiting for quiescence
struct DummyWorkload : TestWorkload {
	bool displayWorkers;
	double displayDelay;

	DummyWorkload(WorkloadContext const& wcx)
	: TestWorkload(wcx) {
		displayWorkers = getOption(options, LiteralStringRef("displayWorkers"), true);
		displayDelay = getOption(options, LiteralStringRef("displayDelay"), 0.0);
	}

	virtual std::string description() {
		return "DummyWorkload";
	}

	virtual Future<Void> start(Database const& cx) {
		if ((clientId == 0) && (displayWorkers)) {
			return _start(this, cx);
		}
		return Void();
	}

	ACTOR static Future<Void> _start( DummyWorkload* self, Database cx) {
		if (self->displayDelay > 0.0)
			Void _ = wait(delay(self->displayDelay));
		g_simulator.displayWorkers();
		return Void();
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}
};

WorkloadFactory<DummyWorkload> DummyWorkloadFactory("DummyWorkload");
