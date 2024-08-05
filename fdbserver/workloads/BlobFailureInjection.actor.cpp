/*
 * BlobFailureInjection.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/FaultInjection.h"
#include "flow/DeterministicRandom.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * The BlobFailureInjection workload is designed to simulate blob storage becoming temporarily flaky or unavailable,
 * from a single host to the whole cluster.
 * TODO: add blob storage becoming permanently flaky or unavailable on a single host, to ensure the system moves work
 * away accordingly. Could also handle that through attrition workload maybe?
 * FIXME: make this work outside simulation. Talk to workers like DiskFailureInjection does and add S3BlobStore and
 * AzureBlobStore fault injection points.
 */
struct BlobFailureInjectionWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "BlobFailureInjection";

	bool enabled;
	double enableProbability = 0.5;
	double testDuration = 10.0;

	std::vector<ISimulator::ProcessInfo*> currentlyAffected;

	BlobFailureInjectionWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated() && faultInjectionActivated;
	}

	BlobFailureInjectionWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		// only do this on the "first" client, and only when in simulation and only when fault injection is enabled
		enabled = !clientId && g_network->isSimulated() && faultInjectionActivated;
		enableProbability = getOption(options, "enableProbability"_sr, enableProbability);
		testDuration = getOption(options, "testDuration"_sr, testDuration);
		enabled = (enabled && deterministicRandom()->random01() < enableProbability);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	bool shouldInject(DeterministicRandom& random,
	                  const WorkloadRequest& work,
	                  const unsigned alreadyAdded) const override {
		return alreadyAdded < 1 && work.useDatabase && 0.1 / (1 + alreadyAdded) > random.random01();
	}

	void undoFaultInjection() {
		if (!currentlyAffected.empty()) {
			TraceEvent("BlobFailureInjectionUnFailing").detail("Count", currentlyAffected.size());
		}
		for (auto& it : currentlyAffected) {
			TraceEvent("BlobFailureInjectionUnFailingProcess").detail("Addr", it->address);
			g_simulator->processStopInjectBlobFault(it);
		}
		currentlyAffected.clear();
	}

	ACTOR Future<Void> _start(Database cx, BlobFailureInjectionWorkload* self) {
		if (!self->enabled) {
			return Void();
		}

		CODE_PROBE(true, "Running workload with blob failure injection");
		TraceEvent("BlobFailureInjectionBegin").log();

		auto processes = getServers();
		deterministicRandom()->randomShuffle(processes);

		wait(timeout(reportErrors(self->worker(cx, self, processes), "BlobFailureInjectionWorkerError"),
		             self->testDuration,
		             Void()));

		// Undo all fault injection before exiting, if worker didn't
		self->undoFaultInjection();
		TraceEvent("BlobFailureInjectionEnd").log();

		return Void();
	}

	// TODO: share code with machine attrition
	static std::vector<ISimulator::ProcessInfo*> getServers() {
		std::vector<ISimulator::ProcessInfo*> machines;
		std::vector<ISimulator::ProcessInfo*> all = g_simulator->getAllProcesses();
		for (int i = 0; i < all.size(); i++)
			if (!all[i]->failed && all[i]->name == std::string("Server") &&
			    all[i]->startingClass != ProcessClass::TesterClass)
				machines.push_back(all[i]);
		return machines;
	}

	ACTOR Future<Void> worker(Database cx,
	                          BlobFailureInjectionWorkload* self,
	                          std::vector<ISimulator::ProcessInfo*> processes) {
		int minFailureDuration = 5;
		int maxFailureDuration = std::max(10, (int)(self->testDuration / 2));

		state double failureDuration =
		    deterministicRandom()->randomSkewedUInt32(minFailureDuration, maxFailureDuration);
		// add a random amount between 0 and 1, otherwise it's a whole number
		failureDuration += deterministicRandom()->random01();
		state double delayBefore =
		    deterministicRandom()->random01() * (std::max<double>(0.0, self->testDuration - failureDuration));

		wait(delay(delayBefore));

		// TODO: pick one random worker, a subset of workers, or entire cluster randomly

		int amountToFail = 1;
		if (deterministicRandom()->coinflip()) {
			if (deterministicRandom()->coinflip()) {
				// fail all processes
				amountToFail = processes.size();
			} else if (processes.size() > 3) {
				// fail a random amount of processes up to half
				amountToFail = deterministicRandom()->randomInt(2, std::max<int>(3, processes.size() / 2));
			}
		} // fail 1 process 50% of the time
		ASSERT(amountToFail <= processes.size());
		ASSERT(amountToFail > 0);

		double failureRate;
		if (deterministicRandom()->coinflip()) {
			// fail all requests - blob store is completely unreachable
			failureRate = 1.0;
		} else {
			// fail a random percentage of requests, biasing towards low percentages.
			// This is based on the intuition that failing 98% of requests is not very different than failing 99%, but
			// failing 0.1% vs 1% is different
			failureRate = deterministicRandom()->randomSkewedUInt32(1, 1000) / 1000.0;
		}

		CODE_PROBE(true, "blob failure injection killing processes");

		TraceEvent("BlobFailureInjectionFailing")
		    .detail("Count", amountToFail)
		    .detail("Duration", failureDuration)
		    .detail("FailureRate", failureRate)
		    .log();
		for (int i = 0; i < amountToFail; i++) {
			TraceEvent("BlobFailureInjectionFailingProcess").detail("Addr", processes[i]->address);
			self->currentlyAffected.push_back(processes[i]);
			g_simulator->processInjectBlobFault(processes[i], failureRate);
		}

		wait(delay(failureDuration));

		self->undoFaultInjection();

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobFailureInjectionWorkload> BlobFailureInjectionWorkloadFactory;
// TODO enable once bugs fixed!
// FailureInjectorFactory<BlobFailureInjectionWorkload> BlobFailureInjectionFailureWorkloadFactory;
