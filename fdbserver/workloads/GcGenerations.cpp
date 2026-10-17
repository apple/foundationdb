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

#include "fdbclient/NativeAPI.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"
#include "flow/CodeProbe.h"
#include "flow/NetworkAddress.h"
#include "flow/ScopeExit.h"
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
	bool completed = false;
	bool forceCloggedDcMasterRetry;
	bool checkQueueCleanup;
	int cleanupRestarts;
	std::vector<std::pair<IPAddress, IPAddress>> cloggedPairs;
	Optional<Standalone<StringRef>> cloggedDcId;

	explicit GcGenerationsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 1000.0);
		startDelay = getOption(options, "startDelay"_sr, 30.0);
		forceCloggedDcMasterRetry = getOption(options, "forceCloggedDcMasterRetry"_sr, false);
		checkQueueCleanup = getOption(options, "checkQueueCleanup"_sr, false);
		cleanupRestarts = getOption(options, "cleanupRestarts"_sr, 0);
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
	Future<bool> check(Database const& cx) override { return !g_network->isSimulated() || !enabled || completed; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Ensure simulator state is cleaned up even if the workload is cancelled by timeout.
	// Without this, a timeout leaves the cluster permanently degraded: remote DC clogged,
	// connection failures active, disableTLogRecoveryFinish=true — causing Cycle check to fail.
	~GcGenerationsWorkload() override {
		if (g_network && g_network->isSimulated()) {
			unclogAll();
			disableConnectionFailures("GcGenerations");
			fdbSimulationPolicyState().disableTLogRecoveryFinish = false;
		}
	}

	void unclogAll() {
		TraceEvent("GcGenerationsUnclogRemote").detail("UnclogConnectionCount", cloggedPairs.size());
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			g_simulator->unclogPair(pair.first, pair.second);
		}
		cloggedPairs.clear();
		cloggedDcId.reset();
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

		auto& simPolicy = fdbSimulationPolicyState();
		Optional<Standalone<StringRef>> inactiveDcId = simPolicy.remoteDcId;
		// A region failover can make the configured remote DC the active primary. Always partition the inactive DC.
		if (self->dbInfo->get().master.locality.dcId() == inactiveDcId) {
			inactiveDcId = simPolicy.primaryDcId;
		}
		self->cloggedDcId = inactiveDcId;

		std::vector<IPAddress> ips; // all non-remote process IPs
		std::vector<IPAddress> remoteIps; // all remote process IPs
		for (const auto& process : g_simulator->getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (process->locality.dcId().present() && process->locality.dcId() == inactiveDcId &&
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
		    .detail("RemoteDc", inactiveDcId)
		    .detail("CloggedRemoteProcess", describe(remoteIps));
	}

	bool isMasterInCloggedDc(GcGenerationsWorkload* self) {
		auto masterAddr = self->dbInfo->get().master.address();
		auto* masterProc = g_simulator->getProcessByAddress(masterAddr);
		return !masterProc || !masterProc->locality.dcId().present() ||
		       masterProc->locality.dcId() == self->cloggedDcId;
	}

	// Wait for the DB to reach ACCEPTING_COMMITS. If rebootCloggedDcMaster is true and
	// the master is in the clogged DC, reboot it to force the CC to elect an active DC
	// master. This is required when the inactive DC is clogged (otherwise recovery can
	// never complete), but must be disabled once that DC is unclogged — otherwise
	// every CC re-election that lands there triggers another reboot, producing
	// a tight loop that prevents recovery from ever reaching ACCEPTING_COMMITS.
	Future<Void> dbAvailable(GcGenerationsWorkload* self, bool rebootCloggedDcMaster) {
		while (self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			co_await self->dbInfo->onChange();
			if (rebootCloggedDcMaster && self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS &&
			    self->isMasterInCloggedDc(self)) {
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
			co_await self->dbAvailable(self, /*rebootCloggedDcMaster=*/true);

			// Only reboot the master if it's in the active DC. If it's in the clogged
			// DC, recovery will stall because the master can't communicate with active
			// DC processes. Force a new master election before retrying.
			const bool forcedRetry = self->forceCloggedDcMasterRetry;
			self->forceCloggedDcMasterRetry = false;
			if (forcedRetry || self->isMasterInCloggedDc(self)) {
				auto masterAddr = self->dbInfo->get().master.address();
				auto* masterProc = g_simulator->getProcessByAddress(masterAddr);
				TraceEvent("RetryingRemoteDcMaster")
				    .detail("Iteration", successfulReboots)
				    .detail("MasterAddr", masterAddr)
				    .detail("Forced", forcedRetry);
				if (masterProc) {
					g_simulator->rebootProcess(masterProc, ISimulator::KillType::Reboot);
				}
				continue;
			}

			const LogEpoch previousEpoch = self->dbInfo->get().logSystemConfig.epoch;
			const int previousGenerationCount = self->dbInfo->get().logSystemConfig.oldTLogs.size();
			auto masterAddr = self->dbInfo->get().master.address();
			TraceEvent("RebootingPrimaryDcMaster").detail("Iteration", successfulReboots).detail("Master", masterAddr);
			g_simulator->rebootProcess(g_simulator->getProcessByAddress(masterAddr), ISimulator::KillType::Reboot);

			// Wait for recovery to create a new generation.
			while (self->dbInfo->get().logSystemConfig.epoch <= previousEpoch ||
			       self->dbInfo->get().recoveryState < RecoveryState::RECOVERY_TRANSACTION) {
				co_await self->dbInfo->onChange();
			}
			TraceEvent("CurrentGenerations")
			    .detail("Iteration", successfulReboots)
			    .detail("PreviousEpoch", previousEpoch)
			    .detail("NewEpoch", self->dbInfo->get().logSystemConfig.epoch)
			    .detail("PreviousCount", previousGenerationCount)
			    .detail("NewCount", self->dbInfo->get().logSystemConfig.oldTLogs.size());
			ASSERT(self->dbInfo->get().logSystemConfig.epoch > previousEpoch);
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
		ScopeExit cleanup([self]() {
			self->unclogAll();
			disableConnectionFailures("GcGenerations");
			fdbSimulationPolicyState().disableTLogRecoveryFinish = false;
		});
		fdbSimulationPolicyState().disableTLogRecoveryFinish = true;
		fdbSimulationPolicyState().retainOldLogRoles = self->checkQueueCleanup;

		co_await self->generateMultipleTxnGenerations(self, cx);
		self->unclogAll();
		disableConnectionFailures("GcGenerations");

		// Unblock TLogs before waiting for generation reduction.
		// The trackRecoveryReq blocking prevented TLogs from reporting recovered state
		// during accumulation. The current recovery's tracking is now stale (FinalUpdate
		// will never fire), so we must trigger a fresh recovery by rebooting the master.
		// The new recovery starts with clean tracking state, allowing GC to proceed.
		fdbSimulationPolicyState().disableTLogRecoveryFinish = false;
		if (self->checkQueueCleanup) {
			co_await self->checkRetainedQueueCleanup(cx);
			self->completed = true;
			co_return;
		}

		// Reboot the master to trigger fresh recoveries with clean tracking state.
		// GC may need multiple recovery cycles: remote TLogs must catch up from old
		// generations before remoteRecoveredVersion advances past their recoverAt,
		// and purgeOldRecoveredGenerationsCoreState only purges generations below that.
		// Retry periodically until oldTLogs is reduced.
		// Note: the remote DC is unclogged now, so any master (including remote DC)
		// can coordinate recovery. No need for the primary-DC-only guard here.
		while (self->dbInfo->get().logSystemConfig.oldTLogs.size() > 1) {
			co_await self->dbAvailable(self, /*rebootCloggedDcMaster=*/false);
			auto masterAddr = self->dbInfo->get().master.address();
			TraceEvent("RebootMasterForGC").detail("Master", masterAddr);
			g_simulator->rebootProcess(g_simulator->getProcessByAddress(masterAddr), ISimulator::KillType::Reboot);
			// Give this recovery cycle time to GC before retrying.
			co_await delay(60);
			co_await self->dbAvailable(self, /*rebootCloggedDcMaster=*/false);
			TraceEvent("GcGenerationsWaitingForReduction")
			    .detail("OldTLogs", self->dbInfo->get().logSystemConfig.oldTLogs.size())
			    .detail("RecoveryState", self->dbInfo->get().recoveryState);
		}

		TraceEvent("WaitingForDbFullyRecovered").detail("RecoveryState", self->dbInfo->get().recoveryState);
		while (self->dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			co_await self->dbInfo->onChange();
		}

		self->completed = true;
		TraceEvent("GcGenerationsWorkloadFinish").log();
	}

	Future<Void> checkRetainedQueueCleanup(Database cx) {
		co_await dbAvailable(this, false);
		TLogInterface target;
		for (const auto& set : dbInfo->get().logSystemConfig.tLogs) {
			for (const auto& log : set.tLogs) {
				if (set.isLocal && log.present() && log.interf().address() != dbInfo->get().master.address()) {
					target = log.interf();
					break;
				}
			}
		}
		ASSERT(target.getSharedTLogID() != UID());
		const AddressExclusion exclusion(target.address().ip, target.address().port);
		co_await excludeServers(cx, { exclusion });
		Future<double> replacementCrash = cleanupRestarts > 0
		                                      ? crashQueueReplacement(target, deterministicRandom()->randomInt(1, 3))
		                                      : Future<double>(now());
		UID previousMaster = dbInfo->get().master.id();
		g_simulator->rebootProcess(g_simulator->getProcessByAddress(target.address()), ISimulator::KillType::Reboot);
		g_simulator->rebootProcess(g_simulator->getProcessByAddress(dbInfo->get().master.address()),
		                           ISimulator::KillType::Reboot);
		while (dbInfo->get().master.id() == previousMaster ||
		       dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			co_await dbInfo->onChange();
		}
		Optional<Error> error;
		try {
			double restoredAt = now();
			// Restart through the normal machine failure path, including recovery and retirement I/O.
			for (int i = 0; i < cleanupRestarts; ++i) {
				UID previousWorker;
				for (const auto& worker : co_await getWorkers(dbInfo)) {
					if (worker.interf.address() == target.address()) {
						previousWorker = worker.interf.id();
					}
				}
				co_await delay(deterministicRandom()->random01() * 0.1);
				g_simulator->rebootProcess(g_simulator->getProcessByAddress(target.address()),
				                           ISimulator::KillType::Reboot);
				while (true) {
					bool restored = false;
					for (const auto& worker : co_await getWorkers(dbInfo)) {
						if (worker.interf.address() == target.address() && worker.interf.id() != previousWorker &&
						    worker.recoveredDiskFiles) {
							restored = true;
						}
					}
					if (restored) {
						break;
					}
					co_await delay(0.1);
				}
				restoredAt = now();
			}
			co_await checkIdleQueue(target.getSharedTLogID(), restoredAt);
			if (cleanupRestarts > 0) {
				const double rebootedAt = co_await timeoutError(replacementCrash, 120.0);
				co_await checkIdleQueue(target.getSharedTLogID(), std::max(restoredAt, rebootedAt));
			}
		} catch (Error& e) {
			replacementCrash.cancel();
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			error = e;
		}
		co_await includeServers(cx, { exclusion });
		// The old master can still advertise its retained graph. Restore normal behavior only in a new recovery.
		previousMaster = dbInfo->get().master.id();
		fdbSimulationPolicyState().disableTLogRecoveryFinish = true;
		g_simulator->rebootProcess(g_simulator->getProcessByAddress(dbInfo->get().master.address()),
		                           ISimulator::KillType::Reboot);
		while (dbInfo->get().master.id() == previousMaster ||
		       dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
			co_await dbInfo->onChange();
		}
		fdbSimulationPolicyState().retainOldLogRoles = false;
		fdbSimulationPolicyState().disableTLogRecoveryFinish = false;
		while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			co_await dbInfo->onChange();
		}
		if (error.present()) {
			throw error.get();
		}
		TraceEvent("GcGenerationsQueueCleanupComplete").detail("Restarts", cleanupRestarts);
	}

	Future<Void> checkIdleQueue(UID sharedId, double restoredAt) {
		// Require an observed idle queue so role deletion cannot make this check pass without exercising cleanup.
		const double deadline = now() + 120.0;
		while (!getIdleTLogQueueDiskBytes(restoredAt).contains(sharedId)) {
			if (now() >= deadline) {
				TraceEvent(SevError, "GcGenerationsQueueNotIdle", sharedId);
				throw timed_out();
			}
			co_await delay(0.5);
		}
		TraceEvent("GcGenerationsIdleQueue", sharedId)
		    .detail("QueueDiskBytesUsed", getIdleTLogQueueDiskBytes().at(sharedId))
		    .detail("Restarts", cleanupRestarts);
		co_await waitForTLogQueueCleanup();
	}

	Future<double> crashQueueReplacement(TLogInterface target, int replacement) {
		std::set<std::string> observed;
		while (true) {
			auto* process = g_simulator->getProcessByAddress(target.address());
			Optional<std::string> replacementFile;
			for (auto& [name, weakFile] : process->machine->openFiles) {
				if (name.find("logqueue-") == std::string::npos ||
				    name.find(target.getSharedTLogID().toString()) == std::string::npos ||
				    !name.ends_with(".fdq.part") ||
				    !process->machine->openFiles.contains(name.substr(0, name.size() - 5))) {
					continue;
				}
				auto file = weakFile.get();
				if (file.isReady() && !file.isError() && observed.insert(name).second &&
				    observed.size() == replacement) {
					replacementFile = name;
					break;
				}
			}
			if (replacementFile.present()) {
				TraceEvent("GcGenerationsQueueFileCrash", target.getSharedTLogID())
				    .detail("Replacement", replacement)
				    .detail("Filename", replacementFile.get())
				    .detail("Address", target.address());
				UID previousProcess = process->uid;
				g_simulator->rebootProcess(process, ISimulator::KillType::Reboot);
				while (g_simulator->getProcessByAddress(target.address())->uid == previousProcess) {
					co_await delay(0.1);
				}
				co_return now();
			}
			co_await delay(0.0001);
		}
	}
};

WorkloadFactory<GcGenerationsWorkload> GcGenerationsWorkloadFactory;
