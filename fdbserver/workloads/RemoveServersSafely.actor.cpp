/*
 * RemoveServersSafely.actor.cpp
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
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

template <>
std::string describe(uint32_t const& item) {
	return format("%d", item);
}

struct RemoveServersSafelyWorkload : TestWorkload {
	static constexpr auto NAME = "RemoveServersSafely";

	bool enabled, killProcesses;
	int minMachinesToKill, maxMachinesToKill, maxSafetyCheckRetries;
	double minDelay, maxDelay;
	double kill1Timeout, kill2Timeout;

	std::set<AddressExclusion> toKill1, toKill2;
	std::map<AddressExclusion, Optional<Standalone<StringRef>>> machine_ids; // ip -> Locality Zone id
	std::map<AddressExclusion, std::set<AddressExclusion>> machineProcesses; // ip -> ip:port

	RemoveServersSafelyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled =
		    !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		minMachinesToKill = getOption(options, "minMachinesToKill"_sr, 1);
		maxMachinesToKill = getOption(options, "maxMachinesToKill"_sr, 10);
		maxMachinesToKill = std::max(minMachinesToKill, maxMachinesToKill);
		maxSafetyCheckRetries = getOption(options, "maxSafetyCheckRetries"_sr, 50);
		minDelay = getOption(options, "minDelay"_sr, 0.0);
		maxDelay = getOption(options, "maxDelay"_sr, 60.0);
		kill1Timeout = getOption(options, "kill1Timeout"_sr, 60.0);
		kill2Timeout = getOption(options, "kill2Timeout"_sr, 6000.0);
		killProcesses = deterministicRandom()->random01() < 0.5;
		if (g_network->isSimulated()) {
			g_simulator->allowLogSetKills = false;
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();

		std::map<Optional<Standalone<StringRef>>, AddressExclusion> machinesMap; // Locality Zone Id -> ip address
		std::vector<AddressExclusion>
		    processAddrs; // IF (killProcesses) THEN ip:port ELSE ip addresses   unique list of the machines
		std::map<IPAddress, Optional<Standalone<StringRef>>> ip_dcid;
		auto processes = getServers();
		for (auto& it : processes) {
			AddressExclusion machineIp(it->address.ip);
			AddressExclusion pAddr(it->address.ip, it->address.port);

			TraceEvent("RemoveAndKill")
			    .detail("Step", "listAddresses")
			    .detail("Address", pAddr.toString())
			    .detail("Process", describe(*it))
			    .detail("Dcid", it->locality.dcId().get().toString())
			    .detail("Zoneid", it->locality.zoneId().get().toString())
			    .detail("MachineId", it->locality.machineId().get().toString());

			if (g_simulator->protectedAddresses.count(it->address) == 0)
				processAddrs.push_back(pAddr);
			machineProcesses[machineIp].insert(pAddr);

			// add only one entry for each machine
			if (!machinesMap.count(it->locality.zoneId()))
				machinesMap[it->locality.zoneId()] = machineIp;

			machine_ids[machineIp] = it->locality.zoneId();
			ip_dcid[it->address.ip] = it->locality.dcId();
		}

		int processCount = processAddrs.size();
		int nToKill1 = deterministicRandom()->randomInt(std::min(processCount, minMachinesToKill),
		                                                std::min(processCount, maxMachinesToKill) + 1);
		int nToKill2 = deterministicRandom()->randomInt(std::min(processCount, minMachinesToKill),
		                                                std::min(processCount, maxMachinesToKill) + 1);
		toKill1 = random_subset(processAddrs, nToKill1);
		toKill2 = random_subset(processAddrs, nToKill2);

		if (!killProcesses) {
			std::set<AddressExclusion> processSet;

			for (auto k1 : toKill1) {
				AddressExclusion machineIp(k1.ip);
				ASSERT(machineProcesses.count(machineIp));
				// kill all processes on this machine even if it has a different ip address
				std::copy(machineProcesses[machineIp].begin(),
				          machineProcesses[machineIp].end(),
				          std::inserter(processSet, processSet.end()));
			}
			toKill1.insert(processSet.begin(), processSet.end());

			processSet.clear();
			for (auto k2 : toKill2) {
				AddressExclusion machineIp(k2.ip);
				ASSERT(machineProcesses.count(machineIp));
				std::copy(machineProcesses[machineIp].begin(),
				          machineProcesses[machineIp].end(),
				          std::inserter(processSet, processSet.end()));
			}
			toKill2.insert(processSet.begin(), processSet.end());
		}

		for (AddressExclusion ex : toKill1) {
			AddressExclusion machineIp(ex.ip);
			ASSERT(machine_ids.count(machineIp));
			g_simulator->disableSwapToMachine(machine_ids[machineIp]);
		}

		for (AddressExclusion ex : toKill2) {
			AddressExclusion machineIp(ex.ip);
			ASSERT(machine_ids.count(machineIp));
			g_simulator->disableSwapToMachine(machine_ids[machineIp]);
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();
		double delay = deterministicRandom()->random01() * (maxDelay - minDelay) + minDelay;
		return workloadMain(this, cx, delay, toKill1, toKill2);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>&) override {}

	std::set<AddressExclusion> getNetworks(std::vector<ISimulator::ProcessInfo*> const& processes) {
		std::set<AddressExclusion> processAddrs;

		for (auto& processInfo : processes) {
			processAddrs.insert(AddressExclusion(processInfo->address.ip, processInfo->address.port));
		}
		return processAddrs;
	}

	// Get the list of processes whose ip:port or ip matches netAddrs.
	// Note: item in netAddrs may be ip (representing a machine) or ip:port (representing a process)
	std::vector<ISimulator::ProcessInfo*> getProcesses(std::set<AddressExclusion> const& netAddrs) {
		std::vector<ISimulator::ProcessInfo*> processes;
		std::set<AddressExclusion> processAddrs;
		UID functionId = nondeterministicRandom()->randomUniqueID();

		// Get the list of process network addresses
		for (auto& netAddr : netAddrs) {
			auto machineIpPorts = machineProcesses.find(netAddr);
			if (machineIpPorts != machineProcesses.end()) {
				ASSERT(machineIpPorts->second.size());
				for (auto& processAdd : machineIpPorts->second)
					processAddrs.insert(processAdd);
			} else {
				processAddrs.insert(netAddr);
			}
		}
		// Get the list of processes matching network address
		for (auto processInfo : g_simulator->getAllProcesses()) {
			auto processNet = AddressExclusion(processInfo->address.ip, processInfo->address.port);
			if (processAddrs.find(processNet) != processAddrs.end()) {
				processes.push_back(processInfo);
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "ProcessToKill")
				    .detail("ProcessAddress", processInfo->address)
				    .detail("Process", describe(*processInfo))
				    .detail("Failed", processInfo->failed)
				    .detail("Excluded", processInfo->excluded)
				    .detail("Rebooting", processInfo->rebooting)
				    .detail("Protected", g_simulator->protectedAddresses.count(processInfo->address));
			} else {
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "ProcessNotToKill")
				    .detail("ProcessAddress", processInfo->address)
				    .detail("Process", describe(*processInfo))
				    .detail("Failed", processInfo->failed)
				    .detail("Excluded", processInfo->excluded)
				    .detail("Rebooting", processInfo->rebooting)
				    .detail("Protected", g_simulator->protectedAddresses.count(processInfo->address));
			}
		}
		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "getProcesses")
		    .detail("NetAddrSize", netAddrs.size())
		    .detail("ProcessAddrSize", processAddrs.size())
		    .detail("NetAddrs", describe(netAddrs))
		    .detail("ProcessAddrs", describe(processAddrs))
		    .detail("Processes", processes.size())
		    .detail("MachineProcesses", machineProcesses.size());

		return processes;
	}

	std::vector<ISimulator::ProcessInfo*> excludeAddresses(std::set<AddressExclusion> const& procAddrs) {
		// Get the updated list of processes which may have changed due to reboots, deletes, etc
		std::vector<ISimulator::ProcessInfo*> procArray = getProcesses(procAddrs);

		// Include all of the excluded machines because the first command of the next section is includeall
		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude addresses")
		    .detail("AddrTotal", procAddrs.size())
		    .detail("ProcTotal", procArray.size())
		    .detail("Addresses", describe(procAddrs))
		    .detail("ClusterAvailable", g_simulator->isAvailable());
		for (auto& procAddr : procAddrs) {
			g_simulator->excludeAddress(NetworkAddress(procAddr.ip, procAddr.port, true, false));
		}
		for (auto& procRecord : procArray) {
			procRecord->excluded = true;
			TraceEvent("RemoveAndKill")
			    .detail("Step", "ExcludeAddress")
			    .detail("ProcessAddress", procRecord->address)
			    .detail("Process", describe(*procRecord))
			    .detail("Failed", procRecord->failed)
			    .detail("Rebooting", procRecord->rebooting)
			    .detail("ClusterAvailable", g_simulator->isAvailable());
		}
		return procArray;
	}

	std::vector<ISimulator::ProcessInfo*> includeAddresses(std::set<AddressExclusion> const& procAddrs) {
		// Get the updated list of processes which may have changed due to reboots, deletes, etc
		std::vector<ISimulator::ProcessInfo*> procArray = getProcesses(procAddrs);

		// Include all of the excluded machines because the first command of the next section is includeall
		TraceEvent("RemoveAndKill")
		    .detail("Step", "include addresses")
		    .detail("AddrTotal", procAddrs.size())
		    .detail("ProcTotal", procArray.size())
		    .detail("Addresses", describe(procAddrs))
		    .detail("ClusterAvailable", g_simulator->isAvailable());
		for (auto& procAddr : procAddrs) {
			g_simulator->includeAddress(NetworkAddress(procAddr.ip, procAddr.port, true, false));
		}
		for (auto& procRecord : procArray) {
			// Only change the exclusion member, if not failed since it will require a reboot to revive it
			if (!procRecord->failed)
				procRecord->excluded = false;
			TraceEvent("RemoveAndKill")
			    .detail("Step", "IncludeAddress")
			    .detail("ProcessAddress", procRecord->address)
			    .detail("Process", describe(*procRecord))
			    .detail("Failed", procRecord->failed)
			    .detail("Rebooting", procRecord->rebooting)
			    .detail("ClusterAvailable", g_simulator->isAvailable());
		}
		return procArray;
	}

	// Return processes that are intersection of killAddrs and allServers and that are safe to kill together;
	// killAddrs does not guarantee the addresses are safe to kill simultaneously.
	std::vector<ISimulator::ProcessInfo*> protectServers(std::set<AddressExclusion> const& killAddrs) {
		std::vector<ISimulator::ProcessInfo*> processes;
		std::set<AddressExclusion> processAddrs;
		std::vector<AddressExclusion> killableAddrs;
		std::vector<ISimulator::ProcessInfo*> killProcArray, killableProcesses, processesLeft, processesDead;

		// Get the list of processes matching network address
		for (auto processInfo : getServers()) {
			auto processNet = AddressExclusion(processInfo->address.ip, processInfo->address.port);
			// Mark all of the unavailable as dead
			if (!processInfo->isAvailable() || processInfo->isCleared())
				processesDead.push_back(processInfo);
			// Save all processes not specified within set
			else if (killAddrs.find(processNet) == killAddrs.end())
				processesLeft.push_back(processInfo);
			else
				killProcArray.push_back(processInfo);
		}

		// Identify the largest set of processes which can be killed
		int randomIndex;
		bool bCanKillProcess;
		ISimulator::ProcessInfo* randomProcess;

		for (int killsLeft = killProcArray.size(); killsLeft > 0; killsLeft--) {
			// Select a random kill process
			randomIndex = deterministicRandom()->randomInt(0, killsLeft);
			randomProcess = killProcArray[randomIndex];
			processesDead.push_back(randomProcess);
			killProcArray[randomIndex] = killProcArray.back();
			killProcArray.pop_back();
			// Add all of the remaining processes the leftover array
			processesLeft.insert(processesLeft.end(), killProcArray.begin(), killProcArray.end());

			// Check if we can kill the added process
			bCanKillProcess = g_simulator->canKillProcesses(
			    processesLeft, processesDead, ISimulator::KillType::KillInstantly, nullptr);

			// Remove the added processes
			processesLeft.resize(processesLeft.size() - killProcArray.size());

			if (bCanKillProcess) {
				killableProcesses.push_back(randomProcess);
				killableAddrs.push_back(AddressExclusion(randomProcess->address.ip, randomProcess->address.port));
				TraceEvent("RemoveAndKill")
				    .detail("Step", "IdentifyVictim")
				    .detail("VictimCount", killableAddrs.size())
				    .detail("Victim", randomProcess->toString())
				    .detail("Victims", describe(killableAddrs));
			}
			// Move the process to the keep array
			else {
				processesLeft.push_back(randomProcess);
				processesDead.pop_back();
			}
		}

		return killableProcesses;
	}

	// toKill1 and toKill2 are two random subsets of all processes. If simply kill all processes in toKill1 or toKill2,
	// we may kill too many processes to make the cluster unavailable and stuck.
	ACTOR static Future<Void> workloadMain(RemoveServersSafelyWorkload* self,
	                                       Database cx,
	                                       double waitSeconds,
	                                       std::set<AddressExclusion> toKill1,
	                                       std::set<AddressExclusion> toKill2) {
		wait(updateProcessIds(cx));
		wait(delay(waitSeconds));

		// Removing the first set of machines might legitimately bring the database down, so a timeout is not an error
		state std::vector<NetworkAddress> firstCoordinators;
		state std::vector<ISimulator::ProcessInfo*> killProcArray;
		state bool bClearedFirst;

		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude list first")
		    .detail("ToKill", describe(toKill1))
		    .detail("KillTotal", toKill1.size())
		    .detail("ClusterAvailable", g_simulator->isAvailable());

		// toKill1 may kill too many servers to make cluster unavailable.
		// Get the processes in toKill1 that are safe to kill
		killProcArray = self->protectServers(toKill1);
		// Update the kill networks to the killable processes
		toKill1 = self->getNetworks(killProcArray);
		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude list first")
		    .detail("ToKillModified", describe(toKill1))
		    .detail("KillTotalModified", toKill1.size())
		    .detail("ClusterAvailable", g_simulator->isAvailable());

		self->excludeAddresses(toKill1);

		Optional<Void> result = wait(timeout(removeAndKill(self, cx, toKill1, nullptr, false), self->kill1Timeout));

		bClearedFirst = result.present();
		TraceEvent("RemoveAndKill")
		    .detail("Step", "excluded list first")
		    .detail("ExcludeResult", bClearedFirst ? "succeeded" : "failed")
		    .detail("KillTotal", toKill1.size())
		    .detail("Processes", killProcArray.size())
		    .detail("ToKill1", describe(toKill1))
		    .detail("ClusterAvailable", g_simulator->isAvailable());

		// Include the servers, if unable to exclude
		// Reinclude when buggify is on to increase the surface area of the next set of excludes
		state bool failed = true;
		if (!bClearedFirst || BUGGIFY) {
			// Get the updated list of processes which may have changed due to reboots, deletes, etc
			TraceEvent("RemoveAndKill")
			    .detail("Step", "include all first")
			    .detail("KillTotal", toKill1.size())
			    .detail("ToKill", describe(toKill1))
			    .detail("ClusterAvailable", g_simulator->isAvailable());
			wait(includeServers(cx, std::vector<AddressExclusion>(1)));
			wait(includeLocalities(cx, std::vector<std::string>(), failed, true));
			wait(includeLocalities(cx, std::vector<std::string>(), !failed, true));
			self->includeAddresses(toKill1);
		}

		// toKill2 may kill too many servers to make cluster unavailable.
		// Get the processes in toKill2 that are safe to kill
		killProcArray = self->protectServers(toKill2);

		// Update the kill networks to the killable processes
		toKill2 = self->getNetworks(killProcArray);

		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude list second")
		    .detail("KillTotal", toKill2.size())
		    .detail("ToKill", describe(toKill2))
		    .detail("ClusterAvailable", g_simulator->isAvailable());
		self->excludeAddresses(toKill2);

		// The second set of machines is selected so that we can always make progress without it, even after the
		// permitted number of other permanent failures so we expect to succeed after a finite amount of time
		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude second list")
		    .detail("ToKill2", describe(toKill2))
		    .detail("KillTotal", toKill2.size())
		    .detail("Processes", killProcArray.size())
		    .detail("ClusterAvailable", g_simulator->isAvailable());
		wait(reportErrors(timeoutError(removeAndKill(self, cx, toKill2, bClearedFirst ? &toKill1 : nullptr, true),
		                               self->kill2Timeout),
		                  "RemoveServersSafelyError",
		                  UID()));

		TraceEvent("RemoveAndKill")
		    .detail("Step", "excluded second list")
		    .detail("KillTotal", toKill2.size())
		    .detail("ToKill", describe(toKill2))
		    .detail("ClusterAvailable", g_simulator->isAvailable());

		// Get the updated list of processes which may have changed due to reboots, deletes, etc
		TraceEvent("RemoveAndKill")
		    .detail("Step", "include all second")
		    .detail("KillTotal", toKill2.size())
		    .detail("ToKill", describe(toKill2))
		    .detail("ClusterAvailable", g_simulator->isAvailable());
		wait(includeServers(cx, std::vector<AddressExclusion>(1)));
		wait(includeLocalities(cx, std::vector<std::string>(), failed, true));
		wait(includeLocalities(cx, std::vector<std::string>(), !failed, true));
		self->includeAddresses(toKill2);

		return Void();
	}

	std::vector<ISimulator::ProcessInfo*> killAddresses(std::set<AddressExclusion> const& killAddrs) {
		UID functionId = nondeterministicRandom()->randomUniqueID();
		bool removeViaClear = !BUGGIFY;
		std::vector<ISimulator::ProcessInfo*> killProcArray;
		std::vector<AddressExclusion> toKillArray;

		std::copy(killAddrs.begin(), killAddrs.end(), std::back_inserter(toKillArray));
		killProcArray = getProcesses(killAddrs);

		// Reboot and delete or kill the servers
		if (killProcesses) {
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", removeViaClear ? "ClearProcesses" : "IgnoreProcesses")
			    .detail("Addresses", describe(killAddrs))
			    .detail("Processes", killProcArray.size())
			    .detail("ClusterAvailable", g_simulator->isAvailable())
			    .detail("RemoveViaClear", removeViaClear);
			for (auto& killProcess : killProcArray) {
				if (g_simulator->protectedAddresses.count(killProcess->address))
					TraceEvent("RemoveAndKill", functionId)
					    .detail("Step", "NoKill Process")
					    .detail("Process", describe(*killProcess))
					    .detail("Failed", killProcess->failed)
					    .detail("Rebooting", killProcess->rebooting)
					    .detail("ClusterAvailable", g_simulator->isAvailable())
					    .detail("Protected", g_simulator->protectedAddresses.count(killProcess->address));
				else if (removeViaClear) {
					g_simulator->rebootProcess(killProcess, ISimulator::KillType::RebootProcessAndDelete);
					TraceEvent("RemoveAndKill", functionId)
					    .detail("Step", "Clear Process")
					    .detail("Process", describe(*killProcess))
					    .detail("Failed", killProcess->failed)
					    .detail("Rebooting", killProcess->rebooting)
					    .detail("ClusterAvailable", g_simulator->isAvailable())
					    .detail("Protected", g_simulator->protectedAddresses.count(killProcess->address));
				}
				/*
				                else {
				                    g_simulator->killProcess( killProcess, ISimulator::KillType::KillInstantly );
				                    TraceEvent("RemoveAndKill", functionId).detail("Step", "Kill Process").detail("Process", describe(*killProcess)).detail("Failed", killProcess->failed).detail("Rebooting", killProcess->rebooting).detail("ClusterAvailable", g_simulator->isAvailable()).detail("Protected", g_simulator->protectedAddresses.count(killProcess->address));
				                }
				*/
			}
		} else {
			std::set<Optional<Standalone<StringRef>>> zoneIds;
			bool killedMachine;
			for (auto& killProcess : killProcArray) {
				zoneIds.insert(killProcess->locality.zoneId());
			}
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", removeViaClear ? "ClearMachines" : "KillMachines")
			    .detail("Addresses", describe(killAddrs))
			    .detail("Processes", killProcArray.size())
			    .detail("Zones", zoneIds.size())
			    .detail("ClusterAvailable", g_simulator->isAvailable());
			for (auto& zoneId : zoneIds) {
				killedMachine = g_simulator->killZone(zoneId,
				                                      removeViaClear ? ISimulator::KillType::RebootAndDelete
				                                                     : ISimulator::KillType::KillInstantly);
				TraceEvent(killedMachine ? SevInfo : SevWarn, "RemoveAndKill")
				    .detail("Step", removeViaClear ? "Clear Machine" : "Kill Machine")
				    .detail("ZoneId", zoneId)
				    .detail(removeViaClear ? "Cleared" : "Killed", killedMachine)
				    .detail("ClusterAvailable", g_simulator->isAvailable());
			}
		}

		return killProcArray;
	}

	// If a process is rebooted, it's processid will change. So we need to monitor
	// such changes and re-issue the locality-based exclusion again.
	ACTOR static Future<Void> checkLocalityChange(RemoveServersSafelyWorkload* self,
	                                              Database cx,
	                                              std::vector<AddressExclusion> toKillArray,
	                                              std::unordered_set<std::string> origKillLocalities,
	                                              bool markExcludeAsFailed) {
		state std::unordered_set<std::string> killLocalities = origKillLocalities;

		loop {
			wait(delay(10.0));
			wait(self->updateProcessIds(cx));
			std::unordered_set<std::string> toKillLocalities = self->getLocalitiesFromAddresses(toKillArray);
			if (toKillLocalities == killLocalities) {
				continue;
			}

			// The kill localities have changed.
			TraceEvent("RemoveAndKill")
			    .detail("Step", "localities changed")
			    .detail("OrigKillLocalities", describe(origKillLocalities))
			    .detail("KillLocalities", describe(killLocalities))
			    .detail("ToKillLocalities", describe(toKillLocalities))
			    .detail("Failed", markExcludeAsFailed);
			killLocalities = toKillLocalities;

			// Include back the localities that are no longer in the kill list
			state bool failed = true;
			wait(includeLocalities(cx, std::vector<std::string>(), failed, true));
			wait(includeLocalities(cx, std::vector<std::string>(), !failed, true));

			// Exclude the localities that are now in the kill list
			wait(excludeLocalities(cx, killLocalities, markExcludeAsFailed));
			TraceEvent("RemoveAndKill")
			    .detail("Step", "new localities excluded")
			    .detail("Localities", describe(killLocalities));
		}
	}

	// Attempts to exclude a set of processes, and once the exclusion is successful it kills them.
	// If markExcludeAsFailed is true, then it is an error if we cannot complete the exclusion.
	ACTOR static Future<Void> removeAndKill(RemoveServersSafelyWorkload* self,
	                                        Database cx,
	                                        std::set<AddressExclusion> toKill,
	                                        std::set<AddressExclusion>* pIncAddrs,
	                                        bool markExcludeAsFailed) {
		state UID functionId = nondeterministicRandom()->randomUniqueID();

		// First clear the exclusion list and exclude the given list
		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "Including all")
		    .detail("ClusterAvailable", g_simulator->isAvailable())
		    .detail("MarkExcludeAsFailed", markExcludeAsFailed);
		state bool failed = true;
		wait(includeServers(cx, std::vector<AddressExclusion>(1)));
		wait(includeLocalities(cx, std::vector<std::string>(), failed, true));
		wait(includeLocalities(cx, std::vector<std::string>(), !failed, true));
		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "Included all")
		    .detail("ClusterAvailable", g_simulator->isAvailable())
		    .detail("MarkExcludeAsFailed", markExcludeAsFailed);
		// Reinclude the addresses that were excluded, if present
		if (pIncAddrs) {
			self->includeAddresses(*pIncAddrs);
		}

		state std::vector<ISimulator::ProcessInfo*> killProcArray;
		state std::vector<AddressExclusion> toKillArray;
		state std::vector<AddressExclusion> toKillMarkFailedArray;
		state AddressExclusion coordExcl;
		// Exclude a coordinator under buggify, but only if fault tolerance is > 0 and kill set is non-empty already
		if (BUGGIFY && toKill.size()) {
			Optional<ClusterConnectionString> csOptional = wait(getConnectionString(cx));
			state std::vector<NetworkAddress> coordinators;
			if (csOptional.present()) {
				ClusterConnectionString cs = csOptional.get();
				wait(store(coordinators, cs.tryResolveHostnames()));
			}
			if (coordinators.size() > 2) {
				auto randomCoordinator = deterministicRandom()->randomChoice(coordinators);
				coordExcl = AddressExclusion(randomCoordinator.ip, randomCoordinator.port);
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "ChooseCoordinator")
				    .detail("Coordinator", describe(coordExcl));
			}
		}
		std::copy(toKill.begin(), toKill.end(), std::back_inserter(toKillArray));
		if (markExcludeAsFailed) {
			state int retries = 0;
			loop {
				state bool safe = false;
				state std::set<AddressExclusion> failSet =
				    random_subset(toKillArray, deterministicRandom()->randomInt(0, toKillArray.size() + 1));
				toKillMarkFailedArray.resize(failSet.size());
				std::copy(failSet.begin(), failSet.end(), toKillMarkFailedArray.begin());
				std::sort(toKillMarkFailedArray.begin(), toKillMarkFailedArray.end());
				if (coordExcl.isValid()) {
					toKillMarkFailedArray.push_back(coordExcl);
				}
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "SafetyCheck")
				    .detail("Exclusions", describe(toKillMarkFailedArray));
				choose {
					when(bool _safe = wait(checkSafeExclusions(cx, toKillMarkFailedArray))) {
						safe = _safe && self->protectServers(std::set<AddressExclusion>(toKillMarkFailedArray.begin(),
						                                                                toKillMarkFailedArray.end()))
						                        .size() == toKillMarkFailedArray.size();
					}
					when(wait(delay(5.0))) {
						TraceEvent("RemoveAndKill", functionId)
						    .detail("Step", "SafetyCheckTimedOut")
						    .detail("Exclusions", describe(toKillMarkFailedArray));
					}
				}
				if (retries == self->maxSafetyCheckRetries) {
					// Do not mark as failed if limit is reached
					TraceEvent("RemoveAndKill", functionId)
					    .detail("Step", "SafetyCheckLimitReached")
					    .detail("Retries", retries);
					markExcludeAsFailed = false;
					safe = true;
				}
				if (safe)
					break;
				retries++;
			}
		}
		// Swap coordinator with one server in the kill set to ensure the number of processes to kill does not increase.
		// This is needed only if a new coordinator is added to the toKill set in this function and safety check passes
		if (markExcludeAsFailed && coordExcl.isValid()) {
			// Situation where the entirety of original kill set is selected and extra coordinator is added
			// Shrink down failed vector to maintain size guarantees
			if (toKillMarkFailedArray.size() > toKillArray.size()) {
				auto removeServer = toKillMarkFailedArray.begin();
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "ShrinkFailedKillSet")
				    .detail("Removing", removeServer->toString());
				toKillMarkFailedArray.erase(removeServer);
			}
			ASSERT(toKillMarkFailedArray.size() <= toKillArray.size());
			std::sort(toKillArray.begin(), toKillArray.end());
			auto removeServer = toKill.begin();
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", "ReplaceNonFailedKillSet")
			    .detail("Removing", removeServer->toString())
			    .detail("Adding", coordExcl.toString());
			toKillArray.erase(std::remove(toKillArray.begin(), toKillArray.end(), *removeServer), toKillArray.end());
			toKillArray.push_back(coordExcl);
			toKill.erase(removeServer);
			toKill.insert(coordExcl);
		}
		killProcArray = self->getProcesses(toKill);
		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "Activate Server Exclusion")
		    .detail("KillAddrs", toKill.size())
		    .detail("KillProcs", killProcArray.size())
		    .detail("MissingProcs", toKill.size() != killProcArray.size())
		    .detail("ToKill", describe(toKill))
		    .detail("Addresses", describe(toKillArray))
		    .detail("FailedAddresses", describe(toKillMarkFailedArray))
		    .detail("ClusterAvailable", g_simulator->isAvailable())
		    .detail("MarkExcludeAsFailed", markExcludeAsFailed);

		state bool excludeLocalitiesInsteadOfServers = deterministicRandom()->coinflip();
		state std::unordered_set<std::string> toKillLocalitiesFailed;
		if (markExcludeAsFailed) {
			toKillLocalitiesFailed = self->getLocalitiesFromAddresses(toKillMarkFailedArray);
			if (excludeLocalitiesInsteadOfServers && toKillLocalitiesFailed.size() > 0) {
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "Excluding localities with failed option")
				    .detail("FailedAddressesSize", toKillMarkFailedArray.size())
				    .detail("FailedAddresses", describe(toKillMarkFailedArray))
				    .detail("FailedLocaitiesSize", toKillLocalitiesFailed.size())
				    .detail("FailedLocaities", describe(toKillLocalitiesFailed));

				wait(excludeLocalities(cx, toKillLocalitiesFailed, true));
			} else {
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "Excluding servers with failed option")
				    .detail("FailedAddressesSize", toKillMarkFailedArray.size())
				    .detail("FailedAddresses", describe(toKillMarkFailedArray));

				wait(excludeServers(cx, toKillMarkFailedArray, true));
			}
		}

		state std::unordered_set<std::string> toKillLocalities = self->getLocalitiesFromAddresses(toKillArray);
		if (excludeLocalitiesInsteadOfServers && toKillLocalities.size() > 0) {
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", "Excluding localities without failed option")
			    .detail("AddressesSize", toKillArray.size())
			    .detail("Addresses", describe(toKillArray))
			    .detail("LocaitiesSize", toKillLocalities.size())
			    .detail("Locaities", describe(toKillLocalities));

			wait(excludeLocalities(cx, toKillLocalities, false));
		} else {
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", "Excluding servers without failed option")
			    .detail("AddressesSize", toKillArray.size())
			    .detail("Addresses", describe(toKillArray));

			wait(excludeServers(cx, toKillArray));
		}

		// We need to skip at least the quorum change if there's nothing to kill, because there might not be enough
		// servers left alive to do a coordinators auto (?)
		if (toKill.size()) {
			// Wait for removal to be safe
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", "Wait For Server Exclusion")
			    .detail("Addresses", describe(toKill))
			    .detail("ClusterAvailable", g_simulator->isAvailable());
			if (excludeLocalitiesInsteadOfServers) {
				wait(success(checkForExcludingServers(cx, toKillArray, true /* wait for exclusion */)) ||
				     checkLocalityChange(self,
				                         cx,
				                         toKillArray,
				                         toKillLocalities,
				                         markExcludeAsFailed && toKillLocalitiesFailed.size() > 0));
			} else {
				wait(success(checkForExcludingServers(cx, toKillArray, true /* wait for exclusion */)));
			}

			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", "coordinators auto")
			    .detail("DesiredCoordinators", g_simulator->desiredCoordinators)
			    .detail("ClusterAvailable", g_simulator->isAvailable());

			// Setup the coordinators BEFORE the exclusion
			// Otherwise, we may end up with NotEnoughMachinesForCoordinators
			state int cycle = 0;
			state int nQuorum;
			while (true) {
				cycle++;
				nQuorum = ((g_simulator->desiredCoordinators + 1) / 2) * 2 - 1;
				CoordinatorsResult result = wait(changeQuorum(cx, autoQuorumChange(nQuorum)));
				TraceEvent(result == CoordinatorsResult::SUCCESS || result == CoordinatorsResult::SAME_NETWORK_ADDRESSES
				               ? SevInfo
				               : SevWarn,
				           "RemoveAndKillQuorumChangeResult")
				    .detail("Step", "coordinators auto")
				    .detail("Result", (int)result)
				    .detail("Attempt", cycle)
				    .detail("Quorum", nQuorum)
				    .detail("DesiredCoordinators", g_simulator->desiredCoordinators);
				if (result == CoordinatorsResult::SUCCESS || result == CoordinatorsResult::SAME_NETWORK_ADDRESSES)
					break;
			}

			self->killAddresses(toKill);
		} else {
			TraceEvent("RemoveAndKill", functionId)
			    .detail("Step", "nothing to clear")
			    .detail("ClusterAvailable", g_simulator->isAvailable());
		}

		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "done")
		    .detail("ClusterAvailable", g_simulator->isAvailable());

		return Void();
	}

	static std::vector<ISimulator::ProcessInfo*> getServers() {
		std::vector<ISimulator::ProcessInfo*> machines;
		std::vector<ISimulator::ProcessInfo*> all = g_simulator->getAllProcesses();
		for (int i = 0; i < all.size(); i++) {
			if (all[i]->name == std::string("Server") && all[i]->isAvailableClass()) {
				machines.push_back(all[i]);
			}
		}
		return machines;
	}

	template <class T>
	static std::set<T> random_subset(std::vector<T> v, int n) {
		std::set<T> subset;
		// No, this isn't efficient!
		deterministicRandom()->randomShuffle(v);
		v.resize(n);
		std::copy(v.begin(), v.end(), std::inserter(subset, subset.end()));
		return subset;
	}

	bool killContainsProcess(AddressExclusion kill, NetworkAddress process) {
		return kill.excludes(process) || (machineProcesses.find(kill) != machineProcesses.end() &&
		                                  machineProcesses[kill].count(AddressExclusion(process.ip, process.port)) > 0);
	}

	// Finds the localities list that can be excluded from the safe killable addresses list.
	// If excluding based on a particular locality of the safe process, kills any other process, that
	// particular locality is not included in the killable localities list.
	std::unordered_set<std::string> getLocalitiesFromAddresses(const std::vector<AddressExclusion>& addresses) {
		std::unordered_map<std::string, int> allLocalitiesCount;
		std::unordered_map<std::string, int> killableLocalitiesCount;
		auto processes = getServers();
		for (const auto& processInfo : processes) {
			std::map<std::string, std::string> localityData = processInfo->locality.getAllData();
			for (const auto& l : localityData) {
				allLocalitiesCount[LocalityData::ExcludeLocalityPrefix.toString() + l.first + ":" + l.second]++;
			}

			AddressExclusion pAddr(processInfo->address.ip, processInfo->address.port);
			if (std::find(addresses.begin(), addresses.end(), pAddr) != addresses.end()) {
				for (const auto& l : localityData) {
					killableLocalitiesCount[LocalityData::ExcludeLocalityPrefix.toString() + l.first + ":" +
					                        l.second]++;
				}
			}
		}

		std::unordered_set<std::string> toKillLocalities;
		for (const auto& l : killableLocalitiesCount) {
			if (l.second == allLocalitiesCount[l.first]) {
				toKillLocalities.insert(l.first);
			}
		}

		for (const auto& processInfo : processes) {
			AddressExclusion pAddr(processInfo->address.ip, processInfo->address.port);
			if (std::find(addresses.begin(), addresses.end(), pAddr) != addresses.end()) {
				std::map<std::string, std::string> localityData = processInfo->locality.getAllData();
				bool found = false;
				for (const auto& l : localityData) {
					if (toKillLocalities.count(LocalityData::ExcludeLocalityPrefix.toString() + l.first + ":" +
					                           l.second)) {
						found = true;
						break;
					}
				}
				if (!found) {
					return std::unordered_set<std::string>();
				}
			}
		}

		return toKillLocalities;
	}

	// Update the g_simulator processes list with the process ids
	// of the workers, that are generated as part of worker creation.
	ACTOR static Future<Void> updateProcessIds(Database cx) {
		std::vector<ProcessData> workers = wait(getWorkers(cx));
		std::unordered_map<NetworkAddress, int> addressToIndexMap;
		for (int i = 0; i < workers.size(); i++) {
			addressToIndexMap[workers[i].address] = i;
		}

		std::vector<ISimulator::ProcessInfo*> processes = g_simulator->getAllProcesses();
		for (auto process : processes) {
			if (addressToIndexMap.find(process->address) != addressToIndexMap.end()) {
				if (workers[addressToIndexMap[process->address]].locality.processId().present()) {
					process->locality.set(LocalityData::keyProcessId,
					                      workers[addressToIndexMap[process->address]].locality.processId());
				}
			}
		}

		return Void();
	}
};

WorkloadFactory<RemoveServersSafelyWorkload> RemoveServersSafelyWorkloadFactory;
