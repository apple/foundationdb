/*
 * DegradedMultiRegionStatus.cpp
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

// Integration test for the "degraded multi-region" status signal.
//
// Setup: a two-usable-region cluster (generateFearless) with primary region "0"
// (with satellite "2") and remote region "1". Coordinators live in the non-primary
// regions because minimumRegions > 1.
//
// Scenario mirroring production: kill ONLY the primary datacenter "0" of the first
// region, leaving its satellite "2" and the remote region "1"/"3" alive. The cluster
// fails over to the remote region, storage servers recover data from the surviving
// satellite "2", and recovery gets stuck at accepting_commits because the dead
// primary's log set cannot be recruited (allLogs == false => RemoteRegionLogsMissing=1).
//
// The test then reads the status JSON and asserts:
//   - cluster.degraded_multi_region == true
//   - cluster.data.state.description contains "Degraded multiregional"
//
// Finally it resets usable_regions=1 so subsequent checks are not stuck.

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbrpc/simulator.h"
#include "flow/CoroUtils.h"

struct DegradedMultiRegionStatusWorkload : TestWorkload {
	static constexpr auto NAME = "DegradedMultiRegionStatus";
	bool enabled;
	double testDuration;
	bool testSucceeded;

	explicit DegradedMultiRegionStatusWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated();
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		testSucceeded = false;
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
			return killPrimaryKeepSatellites(cx);
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override { return enabled ? testSucceeded : true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Wait until the cluster is fully recovered (stable starting point) before killing anything.
	Future<Void> _setup(Database cx) {
		double failedWait = 0.0;
		while (dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			if (failedWait >= 300.0) {
				TraceEvent(SevError, "DegradedMultiRegionStatus_FullRecoveryTimeout")
				    .detail("Elapsed", failedWait)
				    .detail("RecoveryState", dbInfo->get().recoveryState);
				ASSERT(false);
			}
			co_await delay(1.0);
			failedWait += 1.0;
		}
		TraceEvent("DegradedMultiRegionStatus_Setup").log();
	}

	// Wait until status JSON reports the desired degraded multi-region state.
	Future<bool> waitForDegradedStatus(Database cx) {
		double tStart = now();
		while (true) {
			ReadYourWritesTransaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> statusVal = co_await tr.get("\xff\xff/status/json"_sr);
				if (statusVal.present()) {
					json_spirit::mValue mv;
					json_spirit::read_string(statusVal.get().toString(), mv);
					auto& root = mv.get_obj();
					if (root.contains("cluster")) {
						auto& clusterObj = root["cluster"].get_obj();
						bool degraded = false;
						if (clusterObj.contains("degraded_multi_region")) {
							degraded = clusterObj["degraded_multi_region"].get_bool();
						}
						std::string dataStateDesc;
						if (clusterObj.contains("data") &&
						    clusterObj["data"].get_obj().contains("state") &&
						    clusterObj["data"].get_obj()["state"].get_obj().contains("description")) {
							dataStateDesc =
							    clusterObj["data"].get_obj()["state"].get_obj()["description"].get_str();
						}

						TraceEvent("DegradedMultiRegionStatus_Probe")
						    .detail("Degraded", degraded)
						    .detail("DataStateDesc", dataStateDesc);

						if (degraded && dataStateDesc.find("Degraded multiregional") != std::string::npos) {
							TraceEvent("DegradedMultiRegionStatus_Success")
							    .detail("Elapsed", now() - tStart)
							    .detail("Degraded", degraded)
							    .detail("DataStateDesc", dataStateDesc);
							printf("\n=== Degraded Multi-Region Status Found ===\n");
							printf("Warning: one region is unavailable; committed data remains safe in the surviving "
							       "region.\n");
							printf("Please restart following tlog interfaces, otherwise storage servers may never be "
							       "able to catch up.\n");
							printf("\nData:\n");
							printf("  Replication health - %s\n", dataStateDesc.c_str());
							printf("========================================\n\n");
							fflush(stdout);
							co_return true;
						}
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				// Transient read errors: keep polling.
			}

			if (now() - tStart > testDuration) {
				TraceEvent(SevError, "DegradedMultiRegionStatus_Timeout")
				    .detail("Elapsed", now() - tStart);
				co_return false;
			}
			co_await delay(5.0);
		}
	}

	Future<Void> killPrimaryKeepSatellites(Database cx) {
		ASSERT(g_network->isSimulated());

		// The primary region of the first region is datacenter "0" in the fearless
		// topology (with primary satellite "2"). Kill only "0"; keep "2", "1" and
		// "3" alive so the remote region can recover data from the surviving
		// satellite of the (dead) primary region.
		g_simulator->killDataCenter("0"_sr, ISimulator::KillType::KillInstantly, true);
		TraceEvent("DegradedMultiRegionStatus_KilledPrimaryDC").log();

		// Wait for the recovery to reach accepting_commits (the degraded state).
		Future<bool> degraded = waitForDegradedStatus(cx);
		testSucceeded = co_await degraded;

		// Reset to single usable region so the cluster does not stay stuck for the
		// remaining workloads / checks (mirrors KillRegion's cleanup).
		try {
			co_await ManagementAPI::changeConfig(cx.getReference(), "usable_regions=1", true);
			TraceEvent("DegradedMultiRegionStatus_Reset").log();
		} catch (Error& e) {
			TraceEvent(SevWarn, "DegradedMultiRegionStatus_ResetFailed").error(e);
		}
	}
};

WorkloadFactory<DegradedMultiRegionStatusWorkload> DegradedMultiRegionStatusWorkloadFactory;