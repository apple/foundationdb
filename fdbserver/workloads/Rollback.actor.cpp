/*
 * Rollback.actor.cpp
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
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/MasterInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RollbackWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "Rollback";

	bool enableFailures = false, multiple = true, enabled;
	double meanDelay = 20.0, clogDuration = clogDuration = 3.0, testDuration = 10.0;

	RollbackWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {}

	RollbackWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		meanDelay = getOption(options, "meanDelay"_sr, meanDelay); // Only matters if multiple==true
		clogDuration = getOption(options, "clogDuration"_sr, clogDuration);
		testDuration = getOption(options, "testDuration"_sr, testDuration);
		enableFailures = getOption(options, "enableFailures"_sr, enableFailures);
		multiple = getOption(options, "multiple"_sr, multiple);
	}

	void initFailureInjectionMode(DeterministicRandom& random) override {
		enabled = clientId == 0;
		multiple = random.coinflip();
		enableFailures = random.random01() < 0.2;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (g_simulator == g_network && enabled)
			return timeout(reportErrors(rollbackFailureWorker(cx, this, meanDelay), "RollbackFailureWorkerError"),
			               testDuration,
			               Void());
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> simulateFailure(Database cx, RollbackWorkload* self) {
		state ServerDBInfo system = self->dbInfo->get();
		auto tlogs = system.logSystemConfig.allPresentLogs();

		if (tlogs.empty() || system.client.commitProxies.empty()) {
			TraceEvent(SevInfo, "UnableToTriggerRollback").detail("Reason", "No tlogs in System Map");
			return Void();
		}

		state CommitProxyInterface proxy = deterministicRandom()->randomChoice(system.client.commitProxies);

		int utIndex = deterministicRandom()->randomInt(0, tlogs.size());
		state NetworkAddress uncloggedTLog = tlogs[utIndex].address();

		for (int t = 0; t < tlogs.size(); t++)
			if (t != utIndex)
				if (tlogs[t].address().ip == proxy.address().ip) {
					TraceEvent(SevInfo, "UnableToTriggerRollback").detail("Reason", "proxy-clogged tLog shared IPs");
					return Void();
				}

		TraceEvent("AttemptingToTriggerRollback")
		    .detail("CommitProxy", proxy.address())
		    .detail("UncloggedTLog", uncloggedTLog);

		for (int t = 0; t < tlogs.size(); t++) {
			if (t != utIndex) {
				g_simulator->clogPair(proxy.address().ip, tlogs[t].address().ip, self->clogDuration);
				// g_simulator->clogInterface( g_simulator->getProcess( system.tlogs[t].commit.getEndpoint() ),
				// self->clogDuration, ClogAll );
			}
		}

		// While the clogged machines are still clogged...
		wait(delay(self->clogDuration / 3));
		system = self->dbInfo->get();

		if (self->enableFailures) {
			// Reboot the proxy and clog the unclogged tlog.
			g_simulator->rebootProcess(g_simulator->getProcessByAddress(proxy.address()), ISimulator::KillType::Reboot);
			g_simulator->clogInterface(uncloggedTLog.ip, self->clogDuration, ClogAll);
		} else {
			// Alternatively, if we're not injecting machine failures, clog the proxy and the unclogged tlog.
			g_simulator->clogInterface(proxy.address().ip, self->clogDuration, ClogAll);
			g_simulator->clogInterface(uncloggedTLog.ip, self->clogDuration, ClogAll);
		}
		return Void();
	}

	ACTOR Future<Void> rollbackFailureWorker(Database cx, RollbackWorkload* self, double delay) {
		state PromiseStream<Void> events;
		if (self->multiple) {
			state double lastTime = now();
			loop {
				wait(poisson(&lastTime, delay));
				wait(self->simulateFailure(cx, self));
			}
		} else {
			wait(::delay(deterministicRandom()->random01() *
			             std::max(0.0, self->testDuration - self->clogDuration * 13.0)));
			wait(self->simulateFailure(cx, self));
		}
		return Void();
	}
};

WorkloadFactory<RollbackWorkload> RollbackWorkloadFactory;
FailureInjectorFactory<RollbackWorkload> RollbackFailureInjectorFactory;
