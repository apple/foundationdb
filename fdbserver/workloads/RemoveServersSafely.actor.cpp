/*
 * RemoveServersSafely.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

template <>
std::string describe( uint32_t const& item ) {
	return format("%d", item);
}

struct RemoveServersSafelyWorkload : TestWorkload {
	bool enabled, killProcesses;
	int minMachinesToKill, maxMachinesToKill, maxSafetyCheckRetries;
	double minDelay, maxDelay;
	double kill1Timeout, kill2Timeout;

	std::set<AddressExclusion> toKill1, toKill2;
	std::map<AddressExclusion, Optional<Standalone<StringRef>>> machine_ids; // ip -> Locality Zone id
	std::map<AddressExclusion, std::set<AddressExclusion>> machineProcesses;	// ip -> ip:port

	RemoveServersSafelyWorkload( WorkloadContext const& wcx )
		: TestWorkload(wcx)
	{
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		minMachinesToKill = getOption( options, LiteralStringRef("minMachinesToKill"), 1 );
		maxMachinesToKill = getOption( options, LiteralStringRef("maxMachinesToKill"), 10 );
		maxMachinesToKill = std::max(minMachinesToKill, maxMachinesToKill);
		maxSafetyCheckRetries = getOption(options, LiteralStringRef("maxSafetyCheckRetries"), 50);
		minDelay = getOption( options, LiteralStringRef("minDelay"), 0.0 );
		maxDelay = getOption( options, LiteralStringRef("maxDelay"), 60.0 );
		kill1Timeout = getOption( options, LiteralStringRef("kill1Timeout"), 60.0 );
		kill2Timeout = getOption( options, LiteralStringRef("kill2Timeout"), 6000.0 );
		killProcesses = deterministicRandom()->random01() < 0.5;
		if(g_network->isSimulated()) {
			g_simulator.allowLogSetKills = false;
		}
	}

	virtual std::string description() { return "RemoveServersSafelyWorkload"; }
	virtual Future<Void> setup( Database const& cx ) {
		if( !enabled )
			return Void();

		std::map<Optional<Standalone<StringRef>>, AddressExclusion> machinesMap; // Locality Zone Id -> ip address
		std::vector<AddressExclusion> processAddrs;	// IF (killProcesses) THEN ip:port ELSE ip addresses   unique list of the machines
		std::map<IPAddress, Optional<Standalone<StringRef>>> ip_dcid;
		auto processes = getServers();
		for(auto& it : processes) {
			AddressExclusion machineIp(it->address.ip);
			AddressExclusion pAddr(it->address.ip, it->address.port);

			TraceEvent("RemoveAndKill").detail("Step", "listAddresses")
				.detail("Address", pAddr.toString()).detail("Process",describe(*it));

			if (g_simulator.protectedAddresses.count(it->address) == 0)
				processAddrs.push_back(pAddr);
			machineProcesses[machineIp].insert(pAddr);

			// add only one entry for each machine
			if (!machinesMap.count(it->locality.zoneId()))
				machinesMap[it->locality.zoneId()] = machineIp;

			machine_ids[machineIp] = it->locality.zoneId();
			ip_dcid[it->address.ip] = it->locality.dcId();
		}

		int processCount = processAddrs.size();
		int nToKill1 = deterministicRandom()->randomInt( std::min(processCount,minMachinesToKill), std::min(processCount,maxMachinesToKill)+1 );
		int nToKill2 = deterministicRandom()->randomInt( std::min(processCount,minMachinesToKill), std::min(processCount,maxMachinesToKill)+1 );
		toKill1 = random_subset( processAddrs, nToKill1 );
		toKill2 = random_subset( processAddrs, nToKill2 );

		if (!killProcesses) {
			std::set<AddressExclusion> processSet;

			for (auto k1 : toKill1) {
				AddressExclusion machineIp(k1.ip);
				ASSERT(machineProcesses.count(machineIp));
				// kill all processes on this machine even if it has a different ip address
				std::copy(machineProcesses[machineIp].begin(), machineProcesses[machineIp].end(), std::inserter(processSet,processSet.end()));
			}
			toKill1.insert(processSet.begin(), processSet.end());

			processSet.clear();
			for (auto k2 : toKill2) {
				AddressExclusion machineIp(k2.ip);
				ASSERT(machineProcesses.count(machineIp));
				std::copy(machineProcesses[machineIp].begin(), machineProcesses[machineIp].end(), std::inserter(processSet,processSet.end()));
			}
			toKill2.insert(processSet.begin(), processSet.end());
		}

		for( AddressExclusion ex : toKill1 ) {
			AddressExclusion machineIp(ex.ip);
			ASSERT(machine_ids.count(machineIp));
			g_simulator.disableSwapToMachine(machine_ids[machineIp]);
		}

		for( AddressExclusion ex : toKill2 ) {
			AddressExclusion machineIp(ex.ip);
			ASSERT(machine_ids.count(machineIp));
			g_simulator.disableSwapToMachine(machine_ids[machineIp]);
		}

		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		if (!enabled)  return Void();
		double delay = deterministicRandom()->random01() * (maxDelay-minDelay) + minDelay;
		return workloadMain( this, cx, delay, toKill1, toKill2 );
	}

	virtual Future<bool> check( Database const& cx ) { return true; }

	virtual void getMetrics( vector<PerfMetric>& ) {
	}

	virtual std::set<AddressExclusion> getNetworks(std::vector<ISimulator::ProcessInfo*> const& processes)
	{
		std::set<AddressExclusion>	processAddrs;

		for (auto& processInfo : processes) {
			processAddrs.insert(AddressExclusion(processInfo->address.ip, processInfo->address.port));
		}
		return processAddrs;
	}

	// Get the list of processes whose ip:port or ip matches netAddrs.
	// Note: item in netAddrs may be ip (representing a machine) or ip:port (representing a process)
	virtual std::vector<ISimulator::ProcessInfo*> getProcesses(std::set<AddressExclusion> const& netAddrs)
	{
		std::vector<ISimulator::ProcessInfo*>	processes;
		std::set<AddressExclusion>	processAddrs;
		UID functionId = nondeterministicRandom()->randomUniqueID();

		// Get the list of process network addresses
		for (auto& netAddr : netAddrs) {
			auto machineIpPorts = machineProcesses.find(netAddr);
			if (machineIpPorts != machineProcesses.end()) {
				ASSERT(machineIpPorts->second.size());
				for (auto& processAdd : machineIpPorts->second)
					processAddrs.insert(processAdd);
			}
			else {
				processAddrs.insert(netAddr);
			}
		}
		// Get the list of processes matching network address
		for (auto processInfo : g_simulator.getAllProcesses()) {
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
				    .detail("Protected", g_simulator.protectedAddresses.count(processInfo->address));
			}
			else {
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "ProcessNotToKill")
				    .detail("ProcessAddress", processInfo->address)
				    .detail("Process", describe(*processInfo))
				    .detail("Failed", processInfo->failed)
				    .detail("Excluded", processInfo->excluded)
				    .detail("Rebooting", processInfo->rebooting)
				    .detail("Protected", g_simulator.protectedAddresses.count(processInfo->address));
			}
		}
		TraceEvent("RemoveAndKill", functionId).detail("Step", "getProcesses")
			.detail("NetAddrSize",netAddrs.size()).detail("ProcessAddrSize",processAddrs.size())
			.detail("NetAddrs",describe(netAddrs)).detail("ProcessAddrs",describe(processAddrs))
			.detail("Proceses", processes.size()).detail("MachineProcesses", machineProcesses.size());

		return processes;
	}

	virtual std::vector<ISimulator::ProcessInfo*> excludeAddresses(std::set<AddressExclusion> const& procAddrs)
	{
		// Get the updated list of processes which may have changed due to reboots, deletes, etc
		std::vector<ISimulator::ProcessInfo*>	procArray = getProcesses(procAddrs);

		// Include all of the excluded machines because the first command of the next section is includeall
		TraceEvent("RemoveAndKill").detail("Step", "exclude addresses").detail("AddrTotal", procAddrs.size()).detail("ProcTotal", procArray.size()).detail("Addresses", describe(procAddrs)).detail("ClusterAvailable", g_simulator.isAvailable());
		for (auto& procAddr : procAddrs) {
			g_simulator.excludeAddress(NetworkAddress(procAddr.ip, procAddr.port, true, false));
		}
		for (auto& procRecord : procArray) {
			procRecord->excluded = true;
			TraceEvent("RemoveAndKill").detail("Step", "ExcludeAddress").detail("ProcessAddress", procRecord->address).detail("Process", describe(*procRecord)).detail("Failed", procRecord->failed).detail("Rebooting", procRecord->rebooting).detail("ClusterAvailable", g_simulator.isAvailable());
		}
		return procArray;
	}

	virtual std::vector<ISimulator::ProcessInfo*> includeAddresses(std::set<AddressExclusion> const& procAddrs)
	{
		// Get the updated list of processes which may have changed due to reboots, deletes, etc
		std::vector<ISimulator::ProcessInfo*>	procArray = getProcesses(procAddrs);

		// Include all of the excluded machines because the first command of the next section is includeall
		TraceEvent("RemoveAndKill").detail("Step", "include addresses").detail("AddrTotal", procAddrs.size()).detail("ProcTotal", procArray.size()).detail("Addresses", describe(procAddrs)).detail("ClusterAvailable", g_simulator.isAvailable());
		for (auto& procAddr : procAddrs) {
			g_simulator.includeAddress(NetworkAddress(procAddr.ip, procAddr.port, true, false));
		}
		for (auto& procRecord : procArray) {
			// Only change the exclusion member, if not failed since it will require a reboot to revive it
			if (!procRecord->failed)
				procRecord->excluded = false;
			TraceEvent("RemoveAndKill").detail("Step", "IncludeAddress").detail("ProcessAddress", procRecord->address).detail("Process", describe(*procRecord)).detail("Failed", procRecord->failed).detail("Rebooting", procRecord->rebooting).detail("ClusterAvailable", g_simulator.isAvailable());
		}
		return procArray;
	}

	// Return processes that are intersection of killAddrs and allServers and that are safe to kill together;
	// killAddrs does not guarantee the addresses are safe to kill simultaneously.
	virtual std::vector<ISimulator::ProcessInfo*> protectServers(std::set<AddressExclusion> const& killAddrs)
	{
		std::vector<ISimulator::ProcessInfo*>	processes;
		std::set<AddressExclusion>	processAddrs;
		std::vector<AddressExclusion> killableAddrs;
		std::vector<ISimulator::ProcessInfo*>	killProcArray, killableProcesses, processesLeft, processesDead;

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
		int	randomIndex;
		bool bCanKillProcess;
		ISimulator::ProcessInfo* randomProcess;

		for (int killsLeft = killProcArray.size(); killsLeft > 0; killsLeft --)
		{
			// Select a random kill process
			randomIndex = deterministicRandom()->randomInt(0, killsLeft);
			randomProcess = killProcArray[randomIndex];
			processesDead.push_back(randomProcess);
			killProcArray[randomIndex] = killProcArray.back();
			killProcArray.pop_back();
			// Add all of the remaining processes the leftover array
			processesLeft.insert(processesLeft.end(), killProcArray.begin(), killProcArray.end());

			// Check if we can kill the added process
			bCanKillProcess = g_simulator.canKillProcesses(processesLeft, processesDead, ISimulator::KillInstantly, nullptr);

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
	ACTOR static Future<Void> workloadMain( RemoveServersSafelyWorkload* self, Database cx, double waitSeconds,
			std::set<AddressExclusion> toKill1, std::set<AddressExclusion> toKill2 ) {
		wait( delay( waitSeconds ) );

		// Removing the first set of machines might legitimately bring the database down, so a timeout is not an error
		state std::vector<NetworkAddress> firstCoordinators;
		state std::vector<ISimulator::ProcessInfo*>	killProcArray;
		state bool bClearedFirst;

		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude list first")
		    .detail("ToKill", describe(toKill1))
		    .detail("KillTotal", toKill1.size())
		    .detail("ClusterAvailable", g_simulator.isAvailable());

		// toKill1 may kill too many servers to make cluster unavailable.
		// Get the processes in toKill1 that are safe to kill
		killProcArray = self->protectServers(toKill1);
		// Update the kill networks to the killable processes
		toKill1 = self->getNetworks(killProcArray);
		TraceEvent("RemoveAndKill")
		    .detail("Step", "exclude list first")
		    .detail("ToKillModified", describe(toKill1))
		    .detail("KillTotalModified", toKill1.size())
		    .detail("ClusterAvailable", g_simulator.isAvailable());

		self->excludeAddresses(toKill1);

		Optional<Void> result = wait( timeout( removeAndKill( self, cx, toKill1, nullptr, false), self->kill1Timeout ) );

		bClearedFirst = result.present();
		TraceEvent("RemoveAndKill")
		    .detail("Step", "excluded list first")
		    .detail("ExcludeResult", bClearedFirst ? "succeeded" : "failed")
		    .detail("KillTotal", toKill1.size())
		    .detail("Processes", killProcArray.size())
		    .detail("ToKill1", describe(toKill1))
		    .detail("ClusterAvailable", g_simulator.isAvailable());

		// Include the servers, if unable to exclude
		// Reinclude when buggify is on to increase the surface area of the next set of excludes
		if (!bClearedFirst || BUGGIFY) {
			// Get the updated list of processes which may have changed due to reboots, deletes, etc
			TraceEvent("RemoveAndKill").detail("Step", "include all first").detail("KillTotal", toKill1.size()).detail("ToKill", describe(toKill1)).detail("ClusterAvailable", g_simulator.isAvailable());
			wait( includeServers( cx, vector<AddressExclusion>(1) ) );
			self->includeAddresses(toKill1);
		}

		// toKill2 may kill too many servers to make cluster unavailable.
		// Get the processes in toKill2 that are safe to kill
		killProcArray = self->protectServers(toKill2);

		// Update the kill networks to the killable processes
		toKill2 = self->getNetworks(killProcArray);

		TraceEvent("RemoveAndKill").detail("Step", "exclude list second").detail("KillTotal", toKill2.size()).detail("ToKill", describe(toKill2)).detail("ClusterAvailable", g_simulator.isAvailable());
		self->excludeAddresses(toKill2);

		// The second set of machines is selected so that we can always make progress without it, even after the permitted number of other permanent failures
		// so we expect to succeed after a finite amount of time
		TraceEvent("RemoveAndKill").detail("Step", "exclude second list").detail("ToKill2", describe(toKill2)).detail("KillTotal", toKill2.size())
			.detail("Processes", killProcArray.size()).detail("ClusterAvailable", g_simulator.isAvailable());
		wait( reportErrors( timeoutError( removeAndKill( self, cx, toKill2, bClearedFirst ? &toKill1 : nullptr, true), self->kill2Timeout ), "RemoveServersSafelyError", UID() ) );

		TraceEvent("RemoveAndKill").detail("Step", "excluded second list").detail("KillTotal", toKill2.size()).detail("ToKill", describe(toKill2)).detail("ClusterAvailable", g_simulator.isAvailable());

		// Reinclude all of the machine, if buggified
		if (BUGGIFY) {
			// Get the updated list of processes which may have changed due to reboots, deletes, etc
			TraceEvent("RemoveAndKill").detail("Step", "include all second").detail("KillTotal", toKill2.size()).detail("ToKill", describe(toKill2)).detail("ClusterAvailable", g_simulator.isAvailable());
			wait( includeServers( cx, vector<AddressExclusion>(1) ) );
			self->includeAddresses(toKill2);
		}

		return Void();
	}

	virtual std::vector<ISimulator::ProcessInfo*> killAddresses(std::set<AddressExclusion> const& killAddrs)
	{
		UID functionId = nondeterministicRandom()->randomUniqueID();
		bool removeViaClear = !BUGGIFY;
		std::vector<ISimulator::ProcessInfo*>	killProcArray;
		std::vector<AddressExclusion>	toKillArray;

		std::copy(killAddrs.begin(), killAddrs.end(), std::back_inserter(toKillArray));
		killProcArray = getProcesses(killAddrs);

		// Reboot and delete or kill the servers
		if( killProcesses ) {
			TraceEvent("RemoveAndKill", functionId).detail("Step", removeViaClear ? "ClearProcesses" : "IgnoreProcesses").detail("Addresses", describe(killAddrs))
				.detail("Processes", killProcArray.size()).detail("ClusterAvailable", g_simulator.isAvailable()).detail("RemoveViaClear", removeViaClear);
			for (auto& killProcess : killProcArray) {
				if (g_simulator.protectedAddresses.count(killProcess->address))
					TraceEvent("RemoveAndKill", functionId).detail("Step", "NoKill Process").detail("Process", describe(*killProcess)).detail("Failed", killProcess->failed).detail("Rebooting", killProcess->rebooting).detail("ClusterAvailable", g_simulator.isAvailable()).detail("Protected", g_simulator.protectedAddresses.count(killProcess->address));
				else if (removeViaClear) {
					g_simulator.rebootProcess( killProcess, ISimulator::RebootProcessAndDelete);
					TraceEvent("RemoveAndKill", functionId).detail("Step", "Clear Process").detail("Process", describe(*killProcess)).detail("Failed", killProcess->failed).detail("Rebooting", killProcess->rebooting).detail("ClusterAvailable", g_simulator.isAvailable()).detail("Protected", g_simulator.protectedAddresses.count(killProcess->address));
				}
/*
				else {
					g_simulator.killProcess( killProcess, ISimulator::KillInstantly );
					TraceEvent("RemoveAndKill", functionId).detail("Step", "Kill Process").detail("Process", describe(*killProcess)).detail("Failed", killProcess->failed).detail("Rebooting", killProcess->rebooting).detail("ClusterAvailable", g_simulator.isAvailable()).detail("Protected", g_simulator.protectedAddresses.count(killProcess->address));
				}
*/
			}
		}
		else {
			std::set<Optional<Standalone<StringRef>>> zoneIds;
			bool killedMachine;
			for (auto& killProcess : killProcArray) {
				zoneIds.insert(killProcess->locality.zoneId());
			}
			TraceEvent("RemoveAndKill", functionId).detail("Step", removeViaClear ? "ClearMachines" : "KillMachines").detail("Addresses", describe(killAddrs)).detail("Processes", killProcArray.size()).detail("Zones", zoneIds.size()).detail("ClusterAvailable", g_simulator.isAvailable());
			for (auto& zoneId : zoneIds) {
				killedMachine = g_simulator.killZone( zoneId, removeViaClear ? ISimulator::RebootAndDelete : ISimulator::KillInstantly );
				TraceEvent(killedMachine ? SevInfo : SevWarn, "RemoveAndKill").detail("Step", removeViaClear ? "Clear Machine" : "Kill Machine").detail("ZoneId", zoneId).detail(removeViaClear ? "Cleared" : "Killed", killedMachine).detail("ClusterAvailable", g_simulator.isAvailable());
			}
		}

		return killProcArray;
	}

	ACTOR static Future<Void> removeAndKill(RemoveServersSafelyWorkload* self, Database cx,
	                                        std::set<AddressExclusion> toKill, std::set<AddressExclusion>* pIncAddrs,
	                                        bool markExcludeAsFailed) {
		state UID functionId = nondeterministicRandom()->randomUniqueID();

		// First clear the exclusion list and exclude the given list
		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "Including all")
		    .detail("ClusterAvailable", g_simulator.isAvailable())
		    .detail("MarkExcludeAsFailed", markExcludeAsFailed);
		wait( includeServers( cx, vector<AddressExclusion>(1) ) );
		TraceEvent("RemoveAndKill", functionId)
		    .detail("Step", "Included all")
		    .detail("ClusterAvailable", g_simulator.isAvailable())
		    .detail("MarkExcludeAsFailed", markExcludeAsFailed);
		// Reinclude the addresses that were excluded, if present
		if (pIncAddrs) {
			self->includeAddresses(*pIncAddrs);
		}

		state std::vector<ISimulator::ProcessInfo*>	killProcArray;
		state std::vector<AddressExclusion>	toKillArray;
		state std::vector<AddressExclusion>	toKillMarkFailedArray;
		state AddressExclusion coordExcl;
		// Exclude a coordinator under buggify, but only if fault tolerance is > 0 and kill set is non-empty already
		if (BUGGIFY && toKill.size()) {
			std::vector<NetworkAddress> coordinators = wait(getCoordinators(cx));
			if (coordinators.size() > 2) {
				auto randomCoordinator = deterministicRandom()->randomChoice(coordinators);
				coordExcl = AddressExclusion(randomCoordinator.ip, randomCoordinator.port);
			}
		}
		std::copy(toKill.begin(), toKill.end(), std::back_inserter(toKillArray));
		if (markExcludeAsFailed) {
			state int retries = 0;
			loop {
				state bool safe = false;
				state std::set<AddressExclusion> failSet =
				    random_subset(toKillArray, deterministicRandom()->randomInt(0, toKillArray.size() + 1));
				if (coordExcl.isValid()) {
					failSet.insert(coordExcl);
				}
				toKillMarkFailedArray.resize(failSet.size());
				std::copy(failSet.begin(), failSet.end(), toKillMarkFailedArray.begin());
				TraceEvent("RemoveAndKill", functionId)
				    .detail("Step", "SafetyCheck")
				    .detail("Exclusions", describe(toKillMarkFailedArray));
				choose {
					when(bool _safe = wait(checkSafeExclusions(cx, toKillMarkFailedArray))) {
						safe = _safe;
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
				if (safe) break;
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
		    .detail("ClusterAvailable", g_simulator.isAvailable())
		    .detail("MarkExcludeAsFailed", markExcludeAsFailed);
		if (markExcludeAsFailed) {
			wait( excludeServers( cx, toKillMarkFailedArray, true ) );
		}
		wait( excludeServers( cx, toKillArray ) );

		// We need to skip at least the quorum change if there's nothing to kill, because there might not be enough servers left
		// alive to do a coordinators auto (?)
		if (toKill.size()) {
			// Wait for removal to be safe
			TraceEvent("RemoveAndKill", functionId).detail("Step", "Wait For Server Exclusion").detail("Addresses", describe(toKill)).detail("ClusterAvailable", g_simulator.isAvailable());
			wait(success(checkForExcludingServers(cx, toKillArray, true /* wait for exclusion */)));

			TraceEvent("RemoveAndKill", functionId).detail("Step", "coordinators auto").detail("DesiredCoordinators", g_simulator.desiredCoordinators).detail("ClusterAvailable", g_simulator.isAvailable());

			// Setup the coordinators BEFORE the exclusion
			// Otherwise, we may end up with NotEnoughMachinesForCoordinators
			state int cycle=0;
			state int nQuorum;
			while (true) {
				cycle ++;
				nQuorum = ((g_simulator.desiredCoordinators+1)/2)*2-1;
				CoordinatorsResult::Type result = wait( changeQuorum( cx, autoQuorumChange(nQuorum) ) );
				TraceEvent(result==CoordinatorsResult::SUCCESS || result==CoordinatorsResult::SAME_NETWORK_ADDRESSES ? SevInfo : SevWarn, "RemoveAndKillQuorumChangeResult").detail("Step", "coordinators auto").detail("Result", (int)result).detail("Attempt", cycle).detail("Quorum", nQuorum).detail("DesiredCoordinators", g_simulator.desiredCoordinators);
				if (result==CoordinatorsResult::SUCCESS || result==CoordinatorsResult::SAME_NETWORK_ADDRESSES)
					break;
			}

			self->killAddresses(toKill);
		}
		else
		{
			TraceEvent("RemoveAndKill", functionId).detail("Step", "nothing to clear").detail("ClusterAvailable", g_simulator.isAvailable());
		}

		TraceEvent("RemoveAndKill", functionId).detail("Step", "done").detail("ClusterAvailable", g_simulator.isAvailable());

		return Void();
	}

	static vector<ISimulator::ProcessInfo*> getServers() {
		vector<ISimulator::ProcessInfo*> machines;
		vector<ISimulator::ProcessInfo*> all = g_simulator.getAllProcesses();
		for (int i = 0; i < all.size(); i++) {
			if (all[i]->name == std::string("Server") && all[i]->isAvailableClass()) {
				machines.push_back( all[i] );
			}
		}
		return machines;
	}

	template <class T> static std::set<T> random_subset( std::vector<T> v, int n ) {
		std::set<T>	subset;
		// No, this isn't efficient!
		deterministicRandom()->randomShuffle(v);
		v.resize(n);
		std::copy(v.begin(), v.end(), std::inserter(subset,subset.end()));
		return subset;
	}

	bool killContainsProcess(AddressExclusion kill, NetworkAddress process) {
		return kill.excludes(process) || (machineProcesses.find(kill) != machineProcesses.end() && machineProcesses[kill].count(AddressExclusion(process.ip, process.port)) > 0);
	}
};

WorkloadFactory<RemoveServersSafelyWorkload> RemoveServersSafelyWorkloadFactory("RemoveServersSafely");
