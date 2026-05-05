/*
 * TargetedKill.cpp
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
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/core/MasterInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/QuietDatabase.h"

struct TargetedKillWorkload : TestWorkload {
	static constexpr auto NAME = "TargetedKill";

	std::string machineToKill;
	bool enabled, killAllMachineProcesses;
	int numKillStorages;
	double killAt;
	bool reboot;
	double suspendDuration;

	explicit TargetedKillWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		killAt = getOption(options, "killAt"_sr, 5.0);
		reboot = getOption(options, "reboot"_sr, false);
		suspendDuration = getOption(options, "suspendDuration"_sr, 1.0);
		machineToKill = getOption(options, "machineToKill"_sr, "master"_sr).toString();
		killAllMachineProcesses = getOption(options, "killWholeMachine"_sr, false);
		numKillStorages = getOption(options, "numKillStorages"_sr, 1);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (enabled)
			return assassin(cx, this);
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> killEndpoint(std::vector<WorkerDetails> workers,
	                          NetworkAddress address,
	                          Database cx,
	                          TargetedKillWorkload* self) {
		if (g_simulator == g_network) {
			g_simulator->killInterface(address, ISimulator::KillType::KillInstantly);
			return Void();
		}

		int killed = 0;
		RebootRequest rbReq;
		if (self->reboot) {
			rbReq.waitForDuration = self->suspendDuration;
		} else {
			rbReq.waitForDuration = std::numeric_limits<uint32_t>::max();
		}
		for (int i = 0; i < workers.size(); i++) {
			if (workers[i].interf.master.getEndpoint().getPrimaryAddress() == address ||
			    (self->killAllMachineProcesses &&
			     workers[i].interf.master.getEndpoint().getPrimaryAddress().ip == address.ip &&
			     workers[i].processClass != ProcessClass::TesterClass)) {
				TraceEvent("WorkerKill").detail("TargetedMachine", address).detail("Worker", workers[i].interf.id());
				workers[i].interf.clientInterface.reboot.send(rbReq);
				killed++;
			}
		}

		if (!killed)
			TraceEvent(SevWarn, "WorkerNotFoundAtEndpoint").detail("Address", address);
		else
			TraceEvent("WorkersKilledAtEndpoint").detail("Address", address).detail("KilledProcesses", killed);

		return Void();
	}

	Future<Void> assassin(Database cx, TargetedKillWorkload* self) {
		co_await delay(self->killAt);
		std::vector<StorageServerInterface> storageServers = co_await getStorageServers(cx);
		std::vector<WorkerDetails> workers = co_await getWorkers(self->dbInfo);

		NetworkAddress machine;
		NetworkAddress ccAddr;
		int killed = 0;
		int s = 0;
		int j = 0;
		if (self->machineToKill == "master") {
			machine = self->dbInfo->get().master.address();
		} else if (self->machineToKill == "commitproxy") {
			auto commitProxies = cx->getCommitProxies(UseProvisionalProxies::False);
			int o = deterministicRandom()->randomInt(0, commitProxies->size());
			for (int i = 0; i < commitProxies->size(); i++) {
				CommitProxyInterface mpi = commitProxies->getInterface(o);
				machine = mpi.address();
				if (machine != self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().getPrimaryAddress())
					break;
				o = ++o % commitProxies->size();
			}
		} else if (self->machineToKill == "grvproxy") {
			auto grvProxies = cx->getGrvProxies(UseProvisionalProxies::False);
			int o = deterministicRandom()->randomInt(0, grvProxies->size());
			for (int i = 0; i < grvProxies->size(); i++) {
				GrvProxyInterface gpi = grvProxies->getInterface(o);
				machine = gpi.address();
				if (machine != self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().getPrimaryAddress())
					break;
				o = ++o % grvProxies->size();
			}
		} else if (self->machineToKill == "tlog") {
			auto tlogs = self->dbInfo->get().logSystemConfig.allPresentLogs();
			int o = deterministicRandom()->randomInt(0, tlogs.size());
			for (int i = 0; i < tlogs.size(); i++) {
				TLogInterface tli = tlogs[o];
				machine = tli.address();
				if (machine != self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().getPrimaryAddress())
					break;
				o = ++o % tlogs.size();
			}
		} else if (self->machineToKill == "storage" || self->machineToKill == "ss" ||
		           self->machineToKill == "storageserver") {
			s = deterministicRandom()->randomInt(0, storageServers.size());
			ccAddr = self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().getPrimaryAddress();
			for (j = 0; j < storageServers.size(); j++) {
				StorageServerInterface ssi = storageServers[s];
				machine = ssi.address();
				if (machine != ccAddr) {
					TraceEvent("IsolatedMark").detail("TargetedMachine", machine).detail("Role", self->machineToKill);
					co_await self->killEndpoint(workers, machine, cx, self);
					killed++;
					if (killed == self->numKillStorages)
						co_return;
				}
				s = ++s % storageServers.size();
			}
		} else if (self->machineToKill == "clustercontroller" || self->machineToKill == "cc") {
			machine = self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().getPrimaryAddress();
		}

		TraceEvent("IsolatedMark").detail("TargetedMachine", machine).detail("Role", self->machineToKill);

		co_await self->killEndpoint(workers, machine, cx, self);
	}
};

WorkloadFactory<TargetedKillWorkload> TargetedKillWorkloadFactory;
