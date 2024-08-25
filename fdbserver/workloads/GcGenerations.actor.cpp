/*
 * GcGenerations.actor.cpp
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/CodeProbe.h"
#include "flow/NetworkAddress.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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

	void unclogAll() {
		TraceEvent("GcGenerationsUnclogRemote").detail("UnclogConnectionCount", cloggedPairs.size());
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			g_simulator->unclogPair(pair.first, pair.second);
		}
		cloggedPairs.clear();
	}

	ACTOR Future<Void> clogRemoteDc(GcGenerationsWorkload* self, Database cx) {
		Optional<ClusterConnectionString> csOptional = wait(getConnectionString(cx));
		state std::vector<NetworkAddress> coordinators;
		if (csOptional.present()) {
			ClusterConnectionString cs = csOptional.get();
			wait(store(coordinators, cs.tryResolveHostnames()));
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
		ASSERT(ips.size() > 0);
		ASSERT(remoteIps.size() > 0);

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
		return Void();
	}

	ACTOR Future<Void> dbAvailable(GcGenerationsWorkload* self) {
		while (self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			wait(self->dbInfo->onChange());
		}
		return Void();
	}

	ACTOR Future<Void> generateMultipleTxnGenerations(GcGenerationsWorkload* self, Database cx) {
		wait(self->clogRemoteDc(self, cx));
		state int i = 0;
		// state int generationCount = self->dbInfo->get().logSystemConfig.oldTLogs.size();
		state int generationCount = 0;
		// ASSERT(generationCount == 0);
		for (; i < 6; ++i) {
			wait(delay(30));
			TraceEvent("WaitingForDbAvailable")
			    .detail("Index", i)
			    .detail("RecoveryState", self->dbInfo->get().recoveryState);
			wait(self->dbAvailable(self));
			generationCount = self->dbInfo->get().logSystemConfig.oldTLogs.size();
			TraceEvent("WaitingForDbAvailableDone")
			    .detail("Index", i)
			    .detail("Master", self->dbInfo->get().master.address());
			g_simulator->rebootProcess(g_simulator->getProcessByAddress(self->dbInfo->get().master.address()),
			                           ISimulator::KillType::Reboot);

			// Wait for recovery
			while (self->dbInfo->get().logSystemConfig.oldTLogs.size() == generationCount ||
			       self->dbInfo->get().recoveryState < RecoveryState::RECOVERY_TRANSACTION) {
				wait(self->dbInfo->onChange());
			}
			TraceEvent("CurrentGenerations")
			    .detail("PrevCount", generationCount)
			    .detail("New", self->dbInfo->get().logSystemConfig.oldTLogs.size());
			ASSERT(self->dbInfo->get().logSystemConfig.oldTLogs.size() > generationCount);
			generationCount = self->dbInfo->get().logSystemConfig.oldTLogs.size();
		}
		TraceEvent("AfterMultipleRecovery")
		    .detail("OldGenerationCount", self->dbInfo->get().logSystemConfig.oldTLogs.size());
		return Void();
	}

	ACTOR Future<Void> generationReduced(GcGenerationsWorkload* self) {
		TraceEvent("WaitForGenerationReduction")
		    .detail("GenerationCount", self->dbInfo->get().logSystemConfig.oldTLogs.size())
		    .detail("RecoveryState", self->dbInfo->get().recoveryState);
		while (self->dbInfo->get().logSystemConfig.oldTLogs.size() > 1 ||
		       self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			TraceEvent("WaitForGenerationReduction")
			    .detail("GenerationCount", self->dbInfo->get().logSystemConfig.oldTLogs.size())
			    .detail("RecoveryState", self->dbInfo->get().recoveryState);
			wait(self->dbInfo->onChange());
		}
		TraceEvent("WaitForGenerationReduction")
		    .detail("GenerationCount", self->dbInfo->get().logSystemConfig.oldTLogs.size())
		    .detail("RecoveryState", self->dbInfo->get().recoveryState);
		return Void();
	}

	ACTOR Future<Void> gcGenerationsTestClient(GcGenerationsWorkload* self, Database cx) {
		g_simulator->disableTLogRecoveryFinish = true;

		wait(delay(self->startDelay));

		TraceEvent("WaitingForDbAvailable").detail("RecoveryState", self->dbInfo->get().recoveryState);
		while (self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			wait(self->dbInfo->onChange());
		}

		double startTime = now();
		double workloadEnd = now() + self->testDuration;
		TraceEvent("GcGenerations").detail("StartTime", startTime).detail("EndTime", workloadEnd);

		wait(self->generateMultipleTxnGenerations(self, cx));
		self->unclogAll();

		wait(self->generationReduced(self));

		g_simulator->disableTLogRecoveryFinish = false;

		TraceEvent("WaitingForDbFullyRecovered").detail("RecoveryState", self->dbInfo->get().recoveryState);
		while (self->dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		TraceEvent("GcGenerationsWorkloadFinish").log();
		return Void();
	}
};

WorkloadFactory<GcGenerationsWorkload> GcGenerationsWorkloadFactory;