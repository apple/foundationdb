/*
 * GcGenerations.cpp
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
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"
#include "flow/CodeProbe.h"
#include "flow/NetworkAddress.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"

// This workload tests that when TRACK_TLOG_RECOVERY is turned on, older TLog generations can be garbage collected
// during the recovery before reaching fully_recovered.
struct GcGenerationsWorkload : TestWorkload {
	static constexpr auto NAME = "GcGenerations";
	bool enabled;
	double testDuration;
	double startDelay;
	std::vector<std::pair<IPAddress, IPAddress>> cloggedPairs;

	GcGenerationsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 1000.0);
		startDelay = getOption(options, "startDelay"_sr, 30.0);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert("Attrition");
		out.insert("RandomClogging");
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && enabled)
			return timeout(reportErrors(gcGenerationsTestClient(this, cx), "GcGenerationsError"), testDuration, Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Ensure simulator state is cleaned up even if the workload is cancelled by timeout.
	// Without this, a timeout leaves the cluster permanently degraded: remote DC clogged,
	// connection failures active, disableTLogRecoveryFinish=true — causing Cycle check to fail.
	~GcGenerationsWorkload() {
		if (g_network && g_network->isSimulated()) {
			unclogAll();
			disableConnectionFailures("GcGenerations");
			g_simulator->disableTLogRecoveryFinish = false;
		}
	}

	void unclogAll() {
		TraceEvent("GcGenerationsUnclogRemote").detail("UnclogConnectionCount", cloggedPairs.size());
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			g_simulator->unclogPair(pair.first, pair.second);
		}
		cloggedPairs.clear();
	}

	Future<Void> clogRemoteDc(GcGenerationsWorkload* self, Database cx) {
		Optional<ClusterConnectionString> csOptional = co_await getConnectionString(cx);
		std::vector<NetworkAddress> coordinators;
		if (csOptional.present()) {
			ClusterConnectionString cs = csOptional.get();
			coordinators = co_await cs.tryResolveHostnames();
		}

		auto isCoordinator = [](const std::vector<NetworkAddress>& coordinators, const IPAddress& ip) {
			for (const auto& c : coordinators) {
				if (c.ip == ip) {
					return true;
				}
			}
			return false;
		};

		std::vector<IPAddress> ips; // all non-remote process IPs
		std::vector<IPAddress> remoteIps; // all remote process IPs
		for (const auto& process : g_simulator->getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (process->locality.dcId().present() && process->locality.dcId() == g_simulator->remoteDcId &&
			    !isCoordinator(coordinators, ip)) {
				remoteIps.push_back(ip);
			} else {
				ips.push_back(ip);
			}
		}
		ASSERT(!ips.empty());
		ASSERT(!remoteIps.empty());

		for (const auto& ip : ips) {
			for (const auto& remoteIp : remoteIps) {
				g_simulator->clogPair(ip, remoteIp, 10000);
				g_simulator->clogPair(remoteIp, ip, 10000);
				self->cloggedPairs.emplace_back(ip, remoteIp);
				self->cloggedPairs.emplace_back(remoteIp, ip);
			}
		}

		TraceEvent("PartitionRemoteDc")
		    .detail("RemoteDc", g_simulator->remoteDcId)
		    .detail("CloggedRemoteProcess", describe(remoteIps));
	}

	bool isMasterInRemoteDc(GcGenerationsWorkload* self) {
		auto masterAddr = self->dbInfo->get().master.address();
		auto* masterProc = g_simulator->getProcessByAddress(masterAddr);
		return !masterProc || !masterProc->locality.dcId().present() ||
		       masterProc->locality.dcId() == g_simulator->remoteDcId;
	}

	// Wait for the DB to reach ACCEPTING_COMMITS. If the master is in the clogged
	// remote DC, reboot it to force the CC to elect a primary DC master — otherwise
	// recovery can never complete and we'd block forever.
	Future<Void> dbAvailable(GcGenerationsWorkload* self) {
		while (self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			co_await self->dbInfo->onChange();
			if (self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS &&
			    self->isMasterInRemoteDc(self)) {
				auto masterAddr = self->dbInfo->get().master.address();
				auto* masterProc = g_simulator->getProcessByAddress(masterAddr);
				TraceEvent("DbAvailableRebootRemoteMaster").detail("MasterAddr", masterAddr);
				if (masterProc) {
					g_simulator->rebootProcess(masterProc, ISimulator::KillType::Reboot);
				}
			}
		}
	}

	Future<Void> generateMultipleTxnGenerations(GcGenerationsWorkload* self, Database cx) {
		co_await self->clogRemoteDc(self, cx);
		int generationCount = 0;
		int successfulReboots = 0;
		while (successfulReboots < 6) {
			// Re-enable connection failures each iteration to keep the partition active.
			// Using enableConnectionFailures (not extendConnectionFailures) resets
			// connectionFailureEnableTime, which prevents the peek cursor assertion
			// in LogSystemPeekCursor from firing while clogged pairs are still active.
			enableConnectionFailures("GcGenerations", FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS);

			co_await delay(30);
			TraceEvent("WaitingForDbAvailable")
			    .detail("Iteration", successfulReboots)
			    .detail("RecoveryState", self->dbInfo->get().recoveryState);
			co_await self->dbAvailable(self);
			generationCount = self->dbInfo->get().logSystemConfig.oldTLogs.size();

			// Only reboot the master if it's in the primary DC. If it's in the clogged
			// remote DC, recovery will stall because the master can't communicate with
			// primary DC processes. Loop back and try again.
			if (self->isMasterInRemoteDc(self)) {
				TraceEvent("RetryingRemoteDcMaster")
				    .detail("Iteration", successfulReboots)
				    .detail("MasterAddr", self->dbInfo->get().master.address());
				continue;
			}

			auto masterAddr = self->dbInfo->get().master.address();
			TraceEvent("RebootingPrimaryDcMaster").detail("Iteration", successfulReboots).detail("Master", masterAddr);
			g_simulator->rebootProcess(g_simulator->getProcessByAddress(masterAddr), ISimulator::KillType::Reboot);

			// Wait for recovery to create a new generation.
			while (self->dbInfo->get().logSystemConfig.oldTLogs.size() == generationCount ||
			       self->dbInfo->get().recoveryState < RecoveryState::RECOVERY_TRANSACTION) {
				co_await self->dbInfo->onChange();
			}
			TraceEvent("CurrentGenerations")
			    .detail("Iteration", successfulReboots)
			    .detail("PrevCount", generationCount)
			    .detail("New", self->dbInfo->get().logSystemConfig.oldTLogs.size());
			ASSERT(self->dbInfo->get().logSystemConfig.oldTLogs.size() > generationCount);
			generationCount = self->dbInfo->get().logSystemConfig.oldTLogs.size();
			++successfulReboots;
		}
		TraceEvent("AfterMultipleRecovery")
		    .detail("OldGenerationCount", self->dbInfo->get().logSystemConfig.oldTLogs.size());
	}

	Future<Void> gcGenerationsTestClient(GcGenerationsWorkload* self, Database cx) {
		co_await delay(self->startDelay);

		TraceEvent("WaitingForDbAvailable").detail("RecoveryState", self->dbInfo->get().recoveryState);
		while (self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			co_await self->dbInfo->onChange();
		}

		double startTime = now();
		double workloadEnd = now() + self->testDuration;
		TraceEvent("GcGenerations").detail("StartTime", startTime).detail("EndTime", workloadEnd);

		// Block TLog recovery while creating generations to test generation accumulation during recovery
		g_simulator->disableTLogRecoveryFinish = true;

		co_await self->generateMultipleTxnGenerations(self, cx);
		self->unclogAll();
		disableConnectionFailures("GcGenerations");

		// Unblock TLogs before waiting for generation reduction.
		// The trackRecoveryReq blocking prevented TLogs from reporting recovered state
		// during accumulation. The current recovery's tracking is now stale (FinalUpdate
		// will never fire), so we must trigger a fresh recovery by rebooting the master.
		// The new recovery starts with clean tracking state, allowing GC to proceed.
		g_simulator->disableTLogRecoveryFinish = false;

		// Reboot the master to trigger fresh recoveries with clean tracking state.
		// GC may need multiple recovery cycles: remote TLogs must catch up from old
		// generations before remoteRecoveredVersion advances past their recoverAt,
		// and purgeOldRecoveredGenerationsCoreState only purges generations below that.
		// Retry periodically until oldTLogs is reduced.
		// Note: the remote DC is unclogged now, so any master (including remote DC)
		// can coordinate recovery. No need for the primary-DC-only guard here.
		while (self->dbInfo->get().logSystemConfig.oldTLogs.size() > 1) {
			co_await self->dbAvailable(self);
			auto masterAddr = self->dbInfo->get().master.address();
			TraceEvent("RebootMasterForGC").detail("Master", masterAddr);
			g_simulator->rebootProcess(g_simulator->getProcessByAddress(masterAddr), ISimulator::KillType::Reboot);
			// Give this recovery cycle time to GC before retrying.
			co_await delay(60);
			co_await self->dbAvailable(self);
			TraceEvent("GcGenerationsWaitingForReduction")
			    .detail("OldTLogs", self->dbInfo->get().logSystemConfig.oldTLogs.size())
			    .detail("RecoveryState", self->dbInfo->get().recoveryState);
		}

		TraceEvent("WaitingForDbFullyRecovered").detail("RecoveryState", self->dbInfo->get().recoveryState);
		while (self->dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			co_await self->dbInfo->onChange();
		}

		TraceEvent("GcGenerationsWorkloadFinish").log();
	}
};

WorkloadFactory<GcGenerationsWorkload> GcGenerationsWorkloadFactory;
