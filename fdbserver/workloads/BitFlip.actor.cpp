/*
 * BitFlip.actor.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/DeterministicRandom.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// A simulation workload that flips random memory bit of the data in the system.
struct BitFlipWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "BitFlip";
	bool enabled;
	bool success = true;

	// How long to run the workload before starting
	double initialDelay = 0.0;

	// How long the workload should be run; if <= 0 then it will run until the workload's check function is called
	double duration = 10.0;

	BitFlipWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated() && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM &&
		          CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM;
	}

	BitFlipWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		// only do this on the "first" client in simulation
		enabled = !clientId && g_network->isSimulated() && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM &&
		          CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM;
		initialDelay = getOption(options, "initialDelay"_sr, 0.0);
		duration = getOption(options, "testDuration"_sr, 20.0);
	}

	bool shouldInject(DeterministicRandom& random,
	                  const WorkloadRequest& work,
	                  const unsigned alreadyAdded) const override {
		return alreadyAdded < 1 && work.useDatabase && 0.1 / (1 + alreadyAdded) > random.random01();
	}
	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR Future<Void> _start(Database cx, BitFlipWorkload* self) {
		if (!self->enabled) {
			return Void();
		}

		wait(delay(self->initialDelay));
		TraceEvent("BitFlipOn").log();
		g_simulator->enableBitFlipInjection();

		// If a duration was given, let the duration elapse and then shut the profiler off
		if (self->duration > 0) {
			wait(delay(self->duration));
		}
		g_simulator->disableBitFlipInjection();
		TraceEvent("BitFlipOff").log();

		return Void();
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BitFlipWorkload> BitFlipWorkloadFactory;
FailureInjectorFactory<BitFlipWorkload> BitFlipFailureInjectorFactory;
