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
			return run(this, cx, enabled);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> run(FailoverWorkload* self, Database cx, bool enabled) {
		// fail over, then failback after delay
		wait(delay(self->preFailoverDuration));
		choose {
			when(wait(delay(200.0))) {
				return Void();
			}
			when(wait(failover(self, cx, false))) {}
		}
		wait(delay(self->afterFailoverDuration));
		choose {
			when(wait(delay(200.0))) {

				return Void();
			}
			when(wait(failover(self, cx, true))) {}
		}
		wait(delay(self->afterFailbackDuration));
		return Void();
	}

	ACTOR static Future<Void> failover(FailoverWorkload* self, Database cx, bool failback) {
		TraceEvent("FailoverBegin").detail("Failback", failback).log();

		std::string modes = failback ? g_simulator->disableRemote : g_simulator->disablePrimary;
		wait(success(ManagementAPI::changeConfig(cx.getReference(), modes, true)));
		TraceEvent("Failover_WaitFor_PrimaryDatacenterKey").log();

		// when failover, primaryDC should change to 1
		// when failback, primaryDC should change to 0
		StringRef newPrimaryDC = failback ? "0"_sr : "1"_sr;
		wait(waitForPrimaryDC(cx, newPrimaryDC));
		TraceEvent("FailoverComplete").detail("Failback", failback).log();
		return Void();
	}
};

WorkloadFactory<FailoverWorkload> FailoverWorkloadFactory;
