/*
 * RandomClogging.actor.cpp
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

#include "flow/DeterministicRandom.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RandomCloggingWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "RandomClogging";

	bool enabled;
	double testDuration = 10.0;
	double scale = 1.0, clogginess = 1.0;
	int swizzleClog = 0;
	bool iterate = false;
	double maxRunDuration = 60.0, backoff = 1.5, suspend = 10.0;

	RandomCloggingWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {}

	RandomCloggingWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, testDuration);
		scale = getOption(options, "scale"_sr, scale);
		clogginess = getOption(options, "clogginess"_sr, clogginess);
		swizzleClog = getOption(options, "swizzle"_sr, swizzleClog);
	}

	bool shouldInject(DeterministicRandom& random,
	                  const WorkloadRequest& work,
	                  const unsigned alreadyAdded) const override {
		return work.useDatabase && 0.25 / (1 + alreadyAdded) > random.random01();
	}

	void initFailureInjectionMode(DeterministicRandom& random) override {
		enabled = this->clientId == 0;
		scale = std::max(random.random01(), 0.1);
		clogginess = std::max(random.random01(), 0.1);
		swizzleClog = random.random01() < 0.3;
		iterate = random.random01() < 0.5;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && enabled) {
			return _start(this);
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> _start(RandomCloggingWorkload* self) {
		state Future<Void> done = delay(self->maxRunDuration);
		loop {
			wait(done ||
			     timeout(reportErrors(self->swizzleClog ? self->swizzleClogClient(self) : self->clogClient(self),
			                          "RandomCloggingError"),
			             self->testDuration,
			             Void()));
			if (!done.isReady() && self->iterate) {
				wait(delay(self->suspend));
				self->suspend *= self->backoff;
			} else {
				return Void();
			}
		}
	}

	ACTOR void doClog(ISimulator::ProcessInfo* machine, double t, double delay = 0.0) {
		wait(::delay(delay));
		g_simulator->clogInterface(machine->address.ip, t);
	}

	void clogRandomPair(double t) {
		auto m1 = deterministicRandom()->randomChoice(g_simulator->getAllProcesses());
		auto m2 = deterministicRandom()->randomChoice(g_simulator->getAllProcesses());
		if (m1->address.ip != m2->address.ip)
			g_simulator->clogPair(m1->address.ip, m2->address.ip, t);
	}

	ACTOR Future<Void> clogClient(RandomCloggingWorkload* self) {
		state double lastTime = now();
		state double workloadEnd = now() + self->testDuration;
		loop {
			wait(poisson(&lastTime, self->scale / self->clogginess));
			auto machine = deterministicRandom()->randomChoice(g_simulator->getAllProcesses());
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
			for (int m = 0; m < g_simulator->getAllProcesses().size(); m++)
				if (deterministicRandom()->random01() < 0.5) {
					swizzled.push_back(g_simulator->getAllProcesses()[m]);
					starts.push_back(deterministicRandom()->random01() * t / 2);
					ends.push_back(deterministicRandom()->random01() * t / 2 + t / 2);
				}
			for (int i = 0; i < 10; i++)
				self->clogRandomPair(t);

			for (int i = 0; i < swizzled.size(); i++)
				self->doClog(swizzled[i], ends[i] - starts[i], starts[i]);
		}
	}
};

WorkloadFactory<RandomCloggingWorkload> RandomCloggingWorkloadFactory;
FailureInjectorFactory<RandomCloggingWorkload> RandomCloggingFailureInjectionFactory;
