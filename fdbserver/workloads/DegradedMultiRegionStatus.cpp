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
// After the stall persists past DEGRADED_MULTI_REGION_MIN_STALL_SECONDS the test reads
// the status JSON and asserts:
//   - cluster.degraded_multi_region == true
//   - cluster.data.state.description contains "Degraded multiregional"
//
// Before killing the primary DC the test also exercises the false-positive regression
// from the remote log set being transiently absent at accepting_commits: it verifies
// (a) a fully healthy cluster is not flagged, and (b) a *normal* recovery of a healthy
// cluster (triggered by killing only the sequencer, no region loss) keeps
// degraded_multi_region == false throughout the recovery.
//
// Finally it resets usable_regions=1 so subsequent checks are not stuck.

#include "fdbclient/NativeAPI.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.h"
#include "flow/CoroUtils.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/json_spirit/json_spirit_value.h"

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

	// Read the degraded_multi_region flag from the status JSON. Returns false on any
	// transient read/parse error (treated as "not degraded") so callers only fail on an
	// explicit true.
	Future<bool> readDegradedFlag(Database cx) {
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
					if (clusterObj.contains("degraded_multi_region")) {
						co_return clusterObj["degraded_multi_region"].get_bool();
					}
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			// Transient read errors: treat as not degraded.
		}
		co_return false;
	}

	// Verify that a healthy (both regions up) cluster is not falsely flagged as
	// degraded multi-region while idle.
	Future<bool> verifyHealthyNotDegraded(Database cx) {
		double tStart = now();
		while (true) {
			bool degraded = co_await readDegradedFlag(cx);
			if (degraded) {
				TraceEvent(SevError, "DegradedMultiRegionStatus_HealthyFalsePositive")
				    .detail("Elapsed", now() - tStart)
				    .detail("Phase", "idle");
				co_return false;
			}
			if (now() - tStart > 10.0) {
				co_return true;
			}
			co_await delay(1.0);
		}
	}

	// Trigger a *normal* recovery with no region loss by killing only the sequencer
	// (master), then poll the status across the recovery window and assert that
	// degraded_multi_region stays false the whole time. This exercises the exact
	// regression where the remote log set is transiently absent at accepting_commits.
	Future<bool> verifyNormalRecoveryNotDegraded(Database cx) {
		ASSERT(g_network->isSimulated());

		// Kill the current sequencer to force a recovery while keeping every region alive.
		NetworkAddress masterAddr = dbInfo->get().master.address();
		ISimulator::ProcessInfo* masterProcess = g_simulator->getProcessByAddress(masterAddr);
		if (masterProcess == nullptr) {
			TraceEvent(SevError, "DegradedMultiRegionStatus_MasterProcessNotFound").detail("Address", masterAddr);
			co_return false;
		}
		LifetimeToken preKillMasterLifetime = dbInfo->get().masterLifetime;
		TraceEvent("DegradedMultiRegionStatus_KillSequencer").detail("Address", masterAddr);
		g_simulator->killProcess(masterProcess, ISimulator::KillType::KillInstantly);

		// Wait until the kill is observed as a new recovery start. A changed ServerDBInfo
		// alone is not a reliable recovery marker (broadcasts also happen for unrelated
		// reasons), so require either a new masterLifetime or a drop from FULLY_RECOVERED.
		// onChange() is awaited in a loop because any single change may not belong to
		// this recovery; without this wait the polling below could pass on the stale
		// fully-recovered state without ever observing the new accepting_commits window.
		double observedAt = now();
		constexpr double observeTimeout = 60.0;
		while (true) {
			// isEqual is not const-qualified, so compare against a local copy.
			LifetimeToken currentMasterLifetime = dbInfo->get().masterLifetime;
			if (!currentMasterLifetime.isEqual(preKillMasterLifetime) ||
			    dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
				break; // recovery started: new lifetime or the state dropped
			}
			if (now() - observedAt > observeTimeout) {
				TraceEvent(SevError, "DegradedMultiRegionStatus_NormalRecoveryNotObserved")
				    .detail("RecoveryState", dbInfo->get().recoveryState);
				co_return false;
			}
			co_await race(dbInfo->onChange(), delay(1.0));
		}

		double tStart = now();
		double deadline = 120.0;
		uint8_t cycles = 10;
		while (true) {
			bool degraded = co_await readDegradedFlag(cx);
			if (degraded) {
				TraceEvent(SevError, "DegradedMultiRegionStatus_NormalRecoveryFalsePositive")
				    .detail("Elapsed", now() - tStart)
				    .detail("RecoveryState", dbInfo->get().recoveryState);
				co_return false;
			}
			if (dbInfo->get().recoveryState >= RecoveryState::ALL_LOGS_RECRUITED) {
				TraceEvent("DegradedMultiRegionStatus_NormalRecoveryClean")
				    .detail("Elapsed", now() - tStart)
				    .detail("RecoveryState", dbInfo->get().recoveryState);
				if (cycles == 0) {
					co_return true;
				}
				cycles--;
			} else {
				cycles = 10; // reset the hold count if recovery is still in progress
			}
			if (now() - tStart > deadline) {
				TraceEvent(SevError, "DegradedMultiRegionStatus_NormalRecoveryTimeout")
				    .detail("Elapsed", now() - tStart)
				    .detail("RecoveryState", dbInfo->get().recoveryState);
				co_return false;
			}
			co_await delay(1.0);
		}
	}

	// Wait until status JSON reports the desired degraded multi-region state.
	Future<bool> waitForDegradedStatus(Database cx) {
		double tStart = now();
		Optional<double> degradedSince;
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
						if (clusterObj.contains("data") && clusterObj["data"].get_obj().contains("state") &&
						    clusterObj["data"].get_obj()["state"].get_obj().contains("description")) {
							dataStateDesc = clusterObj["data"].get_obj()["state"].get_obj()["description"].get_str();
						}

						bool acceptingCommits =
						    clusterObj.contains("recovery_state") &&
						    clusterObj["recovery_state"].get_obj().contains("name") &&
						    clusterObj["recovery_state"].get_obj()["name"].get_str() == "accepting_commits";

						const bool expectedState = degraded && acceptingCommits &&
						                           dataStateDesc.find("Degraded multiregional") != std::string::npos;

						std::string recoveryStateJson;
						if (clusterObj.contains("recovery_state")) {
							recoveryStateJson = json_spirit::write_string(clusterObj["recovery_state"],
							                                              json_spirit::Output_options::none);
						}
						TraceEvent("DegradedMultiRegionStatus_Probe")
						    .detail("Degraded", degraded)
						    .detail("DataStateDesc", dataStateDesc)
						    .detail("AcceptingCommits", acceptingCommits)
						    .detail("RecoveryStateJson", recoveryStateJson)
						    .detail("Elapsed", now() - tStart)
						    .detail("ExpectedState", expectedState);
						if (expectedState) {
							if (!degradedSince.present()) {
								degradedSince = now();
								TraceEvent("DegradedMultiRegionStatus_DegradedHoldStarted")
								    .detail("Elapsed", now() - tStart);
							}

							const double holdDuration = now() - degradedSince.get();
							if (holdDuration >= 10.0) {
								TraceEvent("DegradedMultiRegionStatus_Success")
								    .detail("Elapsed", now() - tStart)
								    .detail("Degraded", degraded)
								    .detail("DataStateDesc", dataStateDesc);
								printf("\n=== Degraded Multi-Region Status Found ===\n");
								printf(
								    "Warning: one region is unavailable; committed data remains safe in the surviving "
								    "region.\n");
								printf(
								    "Please restart following tlog interfaces, otherwise storage servers may never be "
								    "able to catch up.\n");
								printf("\nData:\n");
								printf("  Replication health - %s\n", dataStateDesc.c_str());
								printf("========================================\n\n");
								fflush(stdout);
								co_return true;
							}
						} else {
							if (degradedSince.present()) {
								TraceEvent("DegradedMultiRegionStatus_DegradedHoldLost")
								    .detail("Elapsed", now() - tStart)
								    .detail("HoldDuration", now() - degradedSince.get());
								co_return false;
							}
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
				TraceEvent(SevError, "DegradedMultiRegionStatus_Timeout").detail("Elapsed", now() - tStart);
				co_return false;
			}
			co_await delay(5.0);
		}
	}

	Future<Void> killPrimaryKeepSatellites(Database cx) {
		ASSERT(g_network->isSimulated());

		// The cluster starts fully healthy (both regions up). Confirm it is not
		// falsely marked degraded while idle.
		testSucceeded = co_await verifyHealthyNotDegraded(cx);
		if (!testSucceeded) {
			co_return;
		}

		// Trigger a normal recovery with no region loss and confirm the flag stays
		// false throughout. This guards the transient accepting_commits window.
		testSucceeded = co_await verifyNormalRecoveryNotDegraded(cx);
		if (!testSucceeded) {
			co_return;
		}

		// Give the cluster a moment to settle back to fully recovered before the
		// destructive kill below.
		co_await _setup(cx);

		// The primary region of the first region is datacenter "0" in the fearless
		// topology (with primary satellite "2"). Kill only "0"; keep "2", "1" and
		// "3" alive so the remote region can recover data from the surviving
		// satellite of the (dead) primary region.
		LifetimeToken previousMasterLifetime = dbInfo->get().masterLifetime;
		g_simulator->killDataCenter("0"_sr, ISimulator::KillType::KillInstantly, true);
		TraceEvent("DegradedMultiRegionStatus_KilledPrimaryDC").log();

		bool failoverReady = true;

		try {
			co_await timeoutError(waitForPrimaryDC(cx, "1"_sr), 120.0);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevError, "DegradedMultiRegionStatus_WaitForPrimaryDCFailed").error(e);
			failoverReady = false;
		}

		if (failoverReady) {
			double start = now();
			LifetimeToken currentMasterLifetime = dbInfo->get().masterLifetime;
			while (currentMasterLifetime.isEqual(previousMasterLifetime)) {
				if (now() - start > 120.0) {
					TraceEvent(SevError, "DegradedMultiRegionStatus_MasterChangeTimeout").log();
					failoverReady = false;
					break;
				}
				co_await dbInfo->onChange();
				currentMasterLifetime = dbInfo->get().masterLifetime;
			}
		}

		if (failoverReady) {
			testSucceeded = co_await waitForDegradedStatus(cx);
		} else {
			testSucceeded = false;
		}

		// Unstick the cluster regardless of the assertion outcome: recovery is parked
		// at accepting_commits while region "0" is dead. A forced recovery in region
		// "1" runs updateConfigForForcedRecovery, which forces usable_regions=1 and
		// lets the cluster fully recover, so subsequent workloads/checks do not hang.
		// This is cleanup, not part of the assertion: a cleanup failure is logged but
		// does not overwrite the degraded-status result.
		try {
			co_await forceRecovery(cx->getConnectionRecord(), "1"_sr);
			TraceEvent("DegradedMultiRegionStatus_Unstick").log();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevWarnAlways, "DegradedMultiRegionStatus_UnstickFailed").error(e);
		}

		// Safety net in case the forced recovery did not take effect.
		try {
			co_await ManagementAPI::changeConfig(cx.getReference(), "usable_regions=1", true);
			TraceEvent("DegradedMultiRegionStatus_Reset").log();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevWarnAlways, "DegradedMultiRegionStatus_ResetFailed").error(e);
		}
	}
};

WorkloadFactory<DegradedMultiRegionStatusWorkload> DegradedMultiRegionStatusWorkloadFactory;
