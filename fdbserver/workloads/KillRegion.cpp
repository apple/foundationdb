/*
 * KillRegion.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.h"
#include "flow/CoroUtils.h"

struct KillRegionWorkload : TestWorkload {
	static constexpr auto NAME = "KillRegion";
	bool enabled;
	double testDuration;

	KillRegionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled =
		    !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		fdbSimulationPolicyState().usableRegions = 1;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override {
		if (enabled) {
			return _setup(cx);
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return killRegion(cx);
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> _setup(Database cx) {
		TraceEvent("ForceRecovery_DisablePrimaryBegin").log();
		co_await ManagementAPI::changeConfig(cx.getReference(), g_simulator->disablePrimary, true);
		TraceEvent("ForceRecovery_WaitForRemote").log();
		co_await waitForPrimaryDC(cx, "1"_sr);
		TraceEvent("ForceRecovery_DisablePrimaryComplete").log();
	}

	Future<Void> waitForStorageRecovered() {
		while (dbInfo->get().recoveryState < RecoveryState::STORAGE_RECOVERED) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> killRegion(Database cx) {
		ASSERT(g_network->isSimulated());
		if (deterministicRandom()->random01() < 0.5) {
			TraceEvent("ForceRecovery_DisableRemoteBegin").log();
			co_await ManagementAPI::changeConfig(cx.getReference(), g_simulator->disableRemote, true);
			TraceEvent("ForceRecovery_WaitForPrimary").log();
			co_await waitForPrimaryDC(cx, "0"_sr);
			TraceEvent("ForceRecovery_DisableRemoteComplete").log();
			co_await ManagementAPI::changeConfig(cx.getReference(), g_simulator->originalRegions, true);
		}
		TraceEvent("ForceRecovery_Wait").log();
		co_await delay(deterministicRandom()->random01() * testDuration);

		// FIXME: killDataCenter breaks simulation if forceKill=false, since some processes can survive and
		// partially complete a recovery
		g_simulator->killDataCenter("0"_sr,
		                            deterministicRandom()->random01() < 0.5 ? ISimulator::KillType::KillInstantly
		                                                                    : ISimulator::KillType::RebootAndDelete,
		                            true);
		g_simulator->killDataCenter("2"_sr,
		                            deterministicRandom()->random01() < 0.5 ? ISimulator::KillType::KillInstantly
		                                                                    : ISimulator::KillType::RebootAndDelete,
		                            true);
		g_simulator->killDataCenter("4"_sr,
		                            deterministicRandom()->random01() < 0.5 ? ISimulator::KillType::KillInstantly
		                                                                    : ISimulator::KillType::RebootAndDelete,
		                            true);

		TraceEvent("ForceRecovery_Begin").log();

		co_await forceRecovery(cx->getConnectionRecord(), "1"_sr);

		TraceEvent("ForceRecovery_UsableRegions").log();

		DatabaseConfiguration conf = co_await getDatabaseConfiguration(cx);

		TraceEvent("ForceRecovery_GotConfig")
		    .setMaxEventLength(11000)
		    .setMaxFieldLength(10000)
		    .detail("Conf", conf.toString());

		if (conf.usableRegions > 1) {
			while (true) {
				// only needed if force recovery was unnecessary and we killed the secondary
				co_await ManagementAPI::changeConfig(
				    cx.getReference(), g_simulator->disablePrimary + " repopulate_anti_quorum=1", true);
				auto result = co_await race(waitForStorageRecovered(), delay(300.0));
				if (result.index() == 0) {
					break;
				}
			}
			co_await ManagementAPI::changeConfig(cx.getReference(), "usable_regions=1", true);
		}

		TraceEvent("ForceRecovery_Complete").log();
	}
};

WorkloadFactory<KillRegionWorkload> KillRegionWorkloadFactory;
