/*
 * RandomClogging.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RandomCloggingWorkload : TestWorkload {
	bool enabled;
	double testDuration;
	double scale, clogginess;
	int swizzleClog;

	RandomCloggingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		scale = getOption(options, LiteralStringRef("scale"), 1.0);
		clogginess = getOption(options, LiteralStringRef("clogginess"), 1.0);
		swizzleClog = getOption(options, LiteralStringRef("swizzle"), 0);
	}

	std::string description() const override {
		if (&g_simulator == g_network)
			return "RandomClogging";
		else
			return "NoRC";
	}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (&g_simulator == g_network && enabled)
			return timeout(
			    reportErrors(swizzleClog ? swizzleClogClient(this) : clogClient(this), "RandomCloggingError"),
			    testDuration,
			    Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR void doClog(ISimulator::ProcessInfo* machine, double t, double delay = 0.0) {
		wait(::delay(delay));
		g_simulator.clogInterface(machine->address.ip, t);
	}

	void clogRandomPair(double t) {
		auto m1 = deterministicRandom()->randomChoice(g_simulator.getAllProcesses());
		auto m2 = deterministicRandom()->randomChoice(g_simulator.getAllProcesses());
		if (m1->address.ip != m2->address.ip)
			g_simulator.clogPair(m1->address.ip, m2->address.ip, t);
	}

	ACTOR Future<Void> clogClient(RandomCloggingWorkload* self) {
		state double lastTime = now();
		state double workloadEnd = now() + self->testDuration;
		loop {
			wait(poisson(&lastTime, self->scale / self->clogginess));
			auto machine = deterministicRandom()->randomChoice(g_simulator.getAllProcesses());
			double t = self->scale * 10.0 * exp(-10.0 * deterministicRandom()->random01());
			t = std::max(0.0, std::min(t, workloadEnd - now()));
			self->doClog(machine, t);

			t = self->scale * 20.0 * exp(-10.0 * deterministicRandom()->random01());
			t = std::max(0.0, std::min(t, workloadEnd - now()));
			self->clogRandomPair(t);
		}
	}

	ACTOR Future<Void> swizzleClogClient(RandomCloggingWorkload* self) {
		state double lastTime = now();
		state double workloadEnd = now() + self->testDuration;
		loop {
			wait(poisson(&lastTime, self->scale / self->clogginess));
			double t = self->scale * 10.0 * exp(-10.0 * deterministicRandom()->random01());
			t = std::max(0.0, std::min(t, workloadEnd - now()));

			// randomly choose half of the machines in the cluster to all clog up,
			//  then unclog in a different order over the course of t seconds
			std::vector<ISimulator::ProcessInfo*> swizzled;
			std::vector<double> starts, ends;
			for (int m = 0; m < g_simulator.getAllProcesses().size(); m++)
				if (deterministicRandom()->random01() < 0.5) {
					swizzled.push_back(g_simulator.getAllProcesses()[m]);
					starts.push_back(deterministicRandom()->random01() * t / 2);
					ends.push_back(deterministicRandom()->random01() * t / 2 + t / 2);
				}
			for (int i = 0; i < 10; i++)
				self->clogRandomPair(t);

			std::vector<Future<Void>> cloggers;
			for (int i = 0; i < swizzled.size(); i++)
				self->doClog(swizzled[i], ends[i] - starts[i], starts[i]);
		}
	}
};

WorkloadFactory<RandomCloggingWorkload> RandomCloggingWorkloadFactory("RandomClogging");
