/*
 * RandomClogging.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
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

	virtual std::string description() {
		if (&g_simulator == g_network)
			return "RandomClogging";
		else
			return "NoRC";
	}
	virtual Future<Void> setup(Database const& cx) { return Void(); }
	virtual Future<Void> start(Database const& cx) {
		if (&g_simulator == g_network && enabled)
			return timeout(reportErrors(swizzleClog ? swizzleClogClient<ISimulator::ProcessInfo*>(this)
			                                        : clogClient<ISimulator::ProcessInfo*>(this),
			                            "RandomCloggingError"),
			               testDuration,
			               Void());
		else if (enabled) {
			return timeout(
			    reportErrors(swizzleClog ? swizzleClogClient<WorkerInterface>(this) : clogClient<WorkerInterface>(this),
			                 "RandomCloggingError"),
			    testDuration,
			    Void());
		} else
			return Void();
	}
	virtual Future<bool> check(Database const& cx) { return true; }
	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR void doClog(ISimulator::ProcessInfo* machine, double t, double delay = 0.0) {
		wait(::delay(delay));
		g_simulator.clogInterface(machine->address.ip, t);
	}

	static void checkClogResult(Future<Void> res, WorkerInterface worker) {
		if (res.isError()) {
			auto err = res.getError();
			if (err.code() == error_code_client_invalid_operation) {
				TraceEvent(SevError, "ChaosDisabled")
				    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString());
			} else {
				TraceEvent(SevError, "CloggingFailed")
				    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString())
				    .error(err);
			}
		}
	}

	ACTOR void doClog(WorkerInterface worker, double t, double delay = 0.0) {
		state Future<Void> res;
		wait(::delay(delay));
		SetFailureInjection::ClogCommand clog;
		clog.time = t;
		SetFailureInjection req;
		req.clog = clog;
		res = worker.clientInterface.setFailureInjection.getReply(req);
		wait(ready(res));
		checkClogResult(res, worker);
	}

	static Future<Void> getAllWorkers(RandomCloggingWorkload* self, std::vector<ISimulator::ProcessInfo*>* result) {
		result->clear();
		*result = g_simulator.getAllProcesses();
		return Void();
	}

	ACTOR static Future<Void> getAllWorkers(RandomCloggingWorkload* self, std::vector<WorkerInterface>* result) {
		result->clear();
		std::vector<WorkerDetails> res =
		    wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest{}));
		for (auto& worker : res) {
			result->emplace_back(worker.interf);
		}
		return Void();
	}

	void clogPair(ISimulator::ProcessInfo* m1, ISimulator::ProcessInfo* m2, double t) {
		if (m1->address.ip != m2->address.ip)
			g_simulator.clogPair(m1->address.ip, m2->address.ip, t);
	}

	ACTOR void clogPair(WorkerInterface m1, WorkerInterface m2, double t) {
		state SetFailureInjection req1, req2;
		state SetFailureInjection::ClogCommand clog1, clog2;
		state Future<Void> resp1, resp2;
		if (m1.waitFailure.getEndpoint().addresses.address == m2.waitFailure.getEndpoint().addresses.address) {
			return;
		}
		clog1.address = m2.waitFailure.getEndpoint().addresses.address;
		clog2.address = m1.waitFailure.getEndpoint().addresses.address;
		req1.clog = clog1;
		req2.clog = clog2;
		resp1 = m1.clientInterface.setFailureInjection.getReply(req1);
		resp2 = m2.clientInterface.setFailureInjection.getReply(req2);
		wait(ready(resp1) && ready(resp2));
		checkClogResult(resp1, m1);
		checkClogResult(resp2, m2);
	}

	template <class W>
	void clogRandomPair(std::vector<W> const& workers, double t) {
		auto m1 = deterministicRandom()->randomChoice(workers);
		auto m2 = deterministicRandom()->randomChoice(workers);
		clogPair(m1, m2, t);
	}

	ACTOR template <class W>
	Future<Void> clogClient(RandomCloggingWorkload* self) {
		state double lastTime = now();
		state double workloadEnd = now() + self->testDuration;
		state std::vector<W> machines;
		loop {
			wait(poisson(&lastTime, self->scale / self->clogginess));
			wait(RandomCloggingWorkload::getAllWorkers(self, &machines));
			auto machine = deterministicRandom()->randomChoice(machines);
			double t = self->scale * 10.0 * exp(-10.0 * deterministicRandom()->random01());
			t = std::max(0.0, std::min(t, workloadEnd - now()));
			self->doClog(machine, t);

			t = self->scale * 20.0 * exp(-10.0 * deterministicRandom()->random01());
			t = std::max(0.0, std::min(t, workloadEnd - now()));
			self->clogRandomPair(machines, t);
		}
	}

	ACTOR template <class W>
	Future<Void> swizzleClogClient(RandomCloggingWorkload* self) {
		state double lastTime = now();
		state double workloadEnd = now() + self->testDuration;
		state double t;
		state vector<W> allProcesses;
		state vector<W> swizzled;
		loop {
			allProcesses.clear();
			swizzled.clear();
			wait(poisson(&lastTime, self->scale / self->clogginess));
			t = self->scale * 10.0 * exp(-10.0 * deterministicRandom()->random01());
			t = std::max(0.0, std::min(t, workloadEnd - now()));

			// randomly choose half of the machines in the cluster to all clog up,
			//  then unclog in a different order over the course of t seconds
			wait(RandomCloggingWorkload::getAllWorkers(self, &allProcesses));
			vector<double> starts, ends;
			for (int m = 0; m < allProcesses.size(); m++)
				if (deterministicRandom()->random01() < 0.5) {
					swizzled.push_back(allProcesses[m]);
					starts.push_back(deterministicRandom()->random01() * t / 2);
					ends.push_back(deterministicRandom()->random01() * t / 2 + t / 2);
				}
			for (int i = 0; i < 10; i++)
				self->clogRandomPair(allProcesses, t);

			vector<Future<Void>> cloggers;
			for (int i = 0; i < swizzled.size(); i++)
				self->doClog(swizzled[i], ends[i] - starts[i], starts[i]);
		}
	}
};

WorkloadFactory<RandomCloggingWorkload> RandomCloggingWorkloadFactory("RandomClogging");
