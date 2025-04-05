/*
 * Failover.actor.cpp
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
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last include.

struct FailoverWorkload : TestWorkload {
	static constexpr auto NAME = "Failover";
	bool enabled;
	double testDuration;
	double preFailoverDuration;
	double afterFailoverDuration;
	double afterFailbackDuration;

	FailoverWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled =
		    !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		testDuration = getOption(options, "testDuration"_sr, 400.0);
		preFailoverDuration = getOption(options, "preFailoverDuration"_sr, 100.0);
		afterFailoverDuration = getOption(options, "afterFailoverDuration"_sr, 100.0);
		afterFailbackDuration = getOption(options, "afterFailbackDuration"_sr, 100.0);
		g_simulator->usableRegions = 2;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return _start(this, cx, enabled);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> waitForFullRecovered(FailoverWorkload* self) {
		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}
		TraceEvent("FailoverWorkload").detail("Phase", "Fully recovered");
		return Void();
	}

	// Fail over, then failback after delay.
	// If recovery stuck, it will cause DCLag and quiet database will not pass.
	// See DCLag trace event for details.
	ACTOR static Future<Void> _start(FailoverWorkload* self, Database cx, bool enabled) {
		if (g_network->isSimulated()) {
			disableConnectionFailures("Failover");
		}

		wait(waitForFullRecovered(self));

		wait(delay(self->preFailoverDuration));
		wait(failover(self, cx));

		wait(delay(self->afterFailoverDuration));
		wait(failback(self, cx));

		wait(waitForFullRecovered(self));

		wait(delay(self->afterFailbackDuration));
		return Void();
	}

	ACTOR static Future<Void> failover(FailoverWorkload* self, Database cx) {
		TraceEvent("FailoverWorkload").detail("Phase", "Failover begin");

		std::string modes = g_simulator->disablePrimary;
		wait(success(ManagementAPI::changeConfig(cx.getReference(), modes, true)));
		TraceEvent("FailoverWorkload").detail("Phase", "Disable primary changeConfig done");

		// When failover, primaryDC should change to 1
		wait(waitForPrimaryDC(cx, "1"_sr));
		TraceEvent("FailoverWorkload").detail("Phase", "Failover done");
		return Void();
	}

	ACTOR static Future<Void> failback(FailoverWorkload* self, Database cx) {
		TraceEvent("FailoverWorkload").detail("Phase", "Failback begin");

		std::string modes = g_simulator->disableRemote;
		wait(success(ManagementAPI::changeConfig(cx.getReference(), modes, true)));
		TraceEvent("FailoverWorkload").detail("Phase", "Disable remote changeConfig done");

		// When failback, primaryDC should change to 0
		wait(waitForPrimaryDC(cx, "0"_sr));
		TraceEvent("FailoverWorkload").detail("Phase", "Failback done");
		return Void();
	}
};

WorkloadFactory<FailoverWorkload> FailoverWorkloadFactory;
