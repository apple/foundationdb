/*
 * KillRegion.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last include.

struct KillRegionWorkload : TestWorkload {
	static constexpr auto NAME = "KillRegion";
	bool enabled;
	double testDuration;

	KillRegionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled =
		    !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		g_simulator->usableRegions = 1;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override {
		if (enabled) {
			return _setup(this, cx);
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return killRegion(this, cx);
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> _setup(KillRegionWorkload* self, Database cx) {
		TraceEvent("ForceRecovery_DisablePrimaryBegin").log();
		wait(success(ManagementAPI::changeConfig(cx.getReference(), g_simulator->disablePrimary, true)));
		TraceEvent("ForceRecovery_WaitForRemote").log();
		wait(waitForPrimaryDC(cx, "1"_sr));
		TraceEvent("ForceRecovery_DisablePrimaryComplete").log();
		return Void();
	}

	ACTOR static Future<Void> waitForStorageRecovered(KillRegionWorkload* self) {
		while (self->dbInfo->get().recoveryState < RecoveryState::STORAGE_RECOVERED) {
			wait(self->dbInfo->onChange());
		}
		return Void();
	}

	ACTOR static Future<Void> killRegion(KillRegionWorkload* self, Database cx) {
		ASSERT(g_network->isSimulated());
		if (deterministicRandom()->random01() < 0.5) {
			TraceEvent("ForceRecovery_DisableRemoteBegin").log();
			wait(success(ManagementAPI::changeConfig(cx.getReference(), g_simulator->disableRemote, true)));
			TraceEvent("ForceRecovery_WaitForPrimary").log();
			wait(waitForPrimaryDC(cx, "0"_sr));
			TraceEvent("ForceRecovery_DisableRemoteComplete").log();
			wait(success(ManagementAPI::changeConfig(cx.getReference(), g_simulator->originalRegions, true)));
		}
		TraceEvent("ForceRecovery_Wait").log();
		wait(delay(deterministicRandom()->random01() * self->testDuration));

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

		wait(forceRecovery(cx->getConnectionRecord(), "1"_sr));

		TraceEvent("ForceRecovery_UsableRegions").log();

		DatabaseConfiguration conf = wait(getDatabaseConfiguration(cx));

		TraceEvent("ForceRecovery_GotConfig")
		    .setMaxEventLength(11000)
		    .setMaxFieldLength(10000)
		    .detail("Conf", conf.toString());

		if (conf.usableRegions > 1) {
			loop {
				// only needed if force recovery was unnecessary and we killed the secondary
				wait(success(ManagementAPI::changeConfig(
				    cx.getReference(), g_simulator->disablePrimary + " repopulate_anti_quorum=1", true)));
				choose {
					when(wait(waitForStorageRecovered(self))) {
						break;
					}
					when(wait(delay(300.0))) {}
				}
			}
			wait(success(ManagementAPI::changeConfig(cx.getReference(), "usable_regions=1", true)));
		}

		TraceEvent("ForceRecovery_Complete").log();

		return Void();
	}
};

WorkloadFactory<KillRegionWorkload> KillRegionWorkloadFactory;
