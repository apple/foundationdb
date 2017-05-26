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

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.h"
#include "workloads.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.h"

template <>
std::string describe( uint32_t const& item ) {
	return format("%d", item);
}

struct RemoveServersSafelyWorkload : TestWorkload {
	bool enabled, killProcesses;
	int minMachinesToKill, maxMachinesToKill;
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
		minDelay = getOption( options, LiteralStringRef("minDelay"), 0.0 );
		maxDelay = getOption( options, LiteralStringRef("maxDelay"), 60.0 );
		kill1Timeout = getOption( options, LiteralStringRef("kill1Timeout"), 60.0 );
		kill2Timeout = getOption( options, LiteralStringRef("kill2Timeout"), 6000.0 );
		killProcesses = g_random->random01() < 0.5;
	}

	virtual std::string description() { return "RemoveServersSafelyWorkload"; }
	virtual Future<Void> setup( Database const& cx ) {
		if( !enabled )
			return Void();

		std::map<Optional<Standalone<StringRef>>, AddressExclusion> machinesMap; // Locality Zone Id -> ip address
		std::vector<AddressExclusion> processAddrs;	// IF (killProcesses) THEN ip:port ELSE ip addresses   unique list of the machines
		std::map<uint32_t, Optional<Standalone<StringRef>>> ip_dcid;
		auto processes = getServers();
		for(auto& it : processes) {
			AddressExclusion machineIp(it->address.ip);
			AddressExclusion pAddr(it->address.ip, it->address.port);

			processAddrs.push_back(pAddr);
			TraceEvent("RemoveAndKill").detail("Step", "listAddresses")
				.detail("Address", pAddr.toString()).detail("Process",describe(*it));
			machineProcesses[machineIp].insert(pAddr);

			// add only one entry for each machine
			if (!machinesMap.count(it->locality.zoneId()))
				machinesMap[it->locality.zoneId()] = machineIp;

			machine_ids[machineIp] = it->locality.zoneId();
			ip_dcid[it->address.ip] = it->locality.dcId();
		}

		int processCount = processAddrs.size();
		int nToKill1 = g_random->randomInt( std::min(processCount,minMachinesToKill), std::min(processCount,maxMachinesToKill)+1 );
		int nToKill2 = g_random->randomInt( std::min(processCount,minMachinesToKill), std::min(processCount,maxMachinesToKill)+1 );
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

		std::vector<NetworkAddress> disableAddrs1;
		for( AddressExclusion ex : toKill1 ) {
			AddressExclusion machineIp(ex.ip);
			ASSERT(machine_ids.count(machineIp));
			g_simulator.disableSwapToMachine(machine_ids[machineIp]);
		}

		std::vector<NetworkAddress> disableAddrs2;
		for( AddressExclusion ex : toKill2 ) {
			AddressExclusion machineIp(ex.ip);
			ASSERT(machine_ids.count(machineIp));
			g_simulator.disableSwapToMachine(machine_ids[machineIp]);
		}

		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		if (!enabled)  return Void();
		double delay = g_random->random01() * (maxDelay-minDelay) + minDelay;
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

	virtual std::vector<ISimulator::ProcessInfo*> getProcesses(std::set<AddressExclusion> const& netAddrs)
	{
		std::vector<ISimulator::ProcessInfo*>	processes;
		std::set<AddressExclusion>	processAddrs;

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
			if (processAddrs.find(processNet) != processAddrs.end())
				processes.push_back(processInfo);
		}
		TraceEvent("RemoveAndKill").detail("Step", "getProcesses")
			.detail("netAddrs",describe(netAddrs)).detail("processAddrs",describe(processAddrs))
			.detail("Proceses", processes.size()).detail("MachineProcesses", machineProcesses.size());

		// Processes may have been destroyed causing
//		ASSERT(processAddrs.size() == processes.size());
		return processes;
	}

	virtual std::vector<ISimulator::ProcessInfo*> protectServers(std::set<AddressExclusion> const& killAddrs)
	{
		std::vector<ISimulator::ProcessInfo*>	processes;
		std::set<AddressExclusion>	processAddrs;
		std::vector<ISimulator::ProcessInfo*>	killProcesses, killableProcesses, processesLeft, processesDead;

		// Get the list of processes matching network address
		for (auto processInfo : getProcesses(killAddrs)) {
			// Add non-test processes
			if (processInfo->startingClass != ProcessClass::TesterClass) {
				if (g_simulator.protectedAddresses.count(processInfo->address))
					processesLeft.push_back(processInfo);
				// Add reliable machine processes to available group
				else if (processInfo->isAvailable())
					processesLeft.push_back(processInfo);
				else
					processesDead.push_back(processInfo);
			}
		}
		killProcesses = processesLeft;

		// Identify the largest set of processes which can be killed
		int	kills, randomIndex;
		for (int kills = 0; kills < killProcesses.size(); kills ++)
		{
			// Select a random kill process
			randomIndex = g_random->randomInt(0, killProcesses.size());
			processesDead.push_back(killProcesses[randomIndex]);
			killProcesses[randomIndex] = killProcesses.back();
			killProcesses.pop_back();
			processesLeft.insert(processesLeft.end(), killProcesses.begin(), killProcesses.end());

			if (g_simulator.canKillProcesses(processesLeft, processesLeft, ISimulator::KillInstantly, NULL)) {
				killableProcesses.push_back(processesDead.back());
			}
			else {
				processesLeft.push_back(processesDead.back());
				processesDead.pop_back();
			}
			// Remove the added processes
			processesLeft.resize(processesLeft.size() - killProcesses.size());
		}

		return killableProcesses;
	}

	ACTOR static Future<Void> workloadMain( RemoveServersSafelyWorkload* self, Database cx, double waitSeconds,
			std::set<AddressExclusion> toKill1, std::set<AddressExclusion> toKill2 ) {
		Void _ = wait( delay( waitSeconds ) );

		// Removing the first set of machines might legitimately bring the database down, so a timeout is not an error
		state std::vector<NetworkAddress> firstCoordinators;
		state std::vector<ISimulator::ProcessInfo*>	killProcesses;

		TraceEvent("RemoveAndKill").detail("Step", "remove first list").detail("toKill1", describe(toKill1)).detail("KillTotal", toKill1.size())
			.detail("ClusterAvailable", g_simulator.isAvailable());
		Optional<Void> result = wait( timeout( removeAndKill( self, cx, toKill1, &firstCoordinators ), self->kill1Timeout ) );

		TraceEvent("RemoveAndKill").detail("Step", "remove result").detail("result", result.present() ? "succeeded" : "failed");
		killProcesses = self->getProcesses(toKill1);
		TraceEvent("RemoveAndKill").detail("Step", "unexclude first processes").detail("toKill1", describe(toKill1))
			.detail("KillTotal", toKill1.size()).detail("Processes", killProcesses.size());
		for (auto& killProcess : killProcesses) {
			killProcess->excluded = false;
		}

		//	virtual bool killMachine(Optional<Standalone<StringRef>> zoneId, KillType, bool killIsSafe = false, bool forceKill = false ) = 0;
		//	virtual bool canKillProcesses(std::vector<ProcessInfo*> const& availableProcesses, std::vector<ProcessInfo*> const& deadProcesses, KillType kt, KillType* newKillType) = 0;

		// Identify the unique machines within kill list
		//		Remove any machine with a protected process (ie coordinator)
		// Check if we can remove the machines and still have a healthy cluster
		// 		If not, remove one random machine at a time until we have healthy cluster
		// Mark all of the processes protected, store the list of modified processes
		// Exclude the machines
		// Remove the machines Kill instantly
		// Unprotect the changed processes
		killProcesses = self->protectServers(toKill2);

		// Update the kill networks to the killable processes
		toKill2 = self->getNetworks(killProcesses);

		// The second set of machines is selected so that we can always make progress without it, even after the permitted number of other permanent failures
		// so we expect to succeed after a finite amount of time
		state Future<Void> disabler = disableConnectionFailuresAfter( self->kill2Timeout/2, "RemoveServersSafely" );
		TraceEvent("RemoveAndKill").detail("Step", "remove second list").detail("toKill2", describe(toKill2)).detail("KillTotal", toKill2.size())
			.detail("Processes", killProcesses.size()).detail("ClusterAvailable", g_simulator.isAvailable());
		Void _ = wait( reportErrors( timeoutError( removeAndKill( self, cx, toKill2, NULL, firstCoordinators), self->kill2Timeout ), "RemoveServersSafelyError", UID() ) );

		killProcesses = self->getProcesses(toKill2);
		TraceEvent("RemoveAndKill").detail("Step", "unexclude second processes").detail("toKill2", describe(toKill2))
			.detail("KillTotal", toKill2.size()).detail("Processes", killProcesses.size());
		for (auto& killProcess : killProcesses) {
			killProcess->excluded = false;
		}

		TraceEvent("RemoveAndKill").detail("Step", "completed final remove").detail("KillTotal", toKill2.size()).detail("Excluded", killProcesses.size())
			.detail("ClusterAvailable", g_simulator.isAvailable());

		return Void();
	}

	ACTOR static Future<Void> removeAndKill( RemoveServersSafelyWorkload* self, Database cx, std::set<AddressExclusion> toKill, std::vector<NetworkAddress>* outSafeCoordinators, std::vector<NetworkAddress> firstCoordinators = std::vector<NetworkAddress>())
	{
		// First clear the exclusion list and exclude the given list
		TraceEvent("RemoveAndKill").detail("Step", "include all").detail("coordinators", describe(firstCoordinators)).detail("ClusterAvailable", g_simulator.isAvailable());
		Void _ = wait( includeServers( cx, vector<AddressExclusion>(1) ) );

		TraceEvent("RemoveAndKill").detail("Step", "getting coordinators").detail("ClusterAvailable", g_simulator.isAvailable());

		std::vector<NetworkAddress> coordinators = wait( getCoordinators(cx) );
		state std::vector<NetworkAddress> safeCoordinators;

		TraceEvent("RemoveAndKill").detail("Step", "got coordinators").detail("coordinators", describe(coordinators));

		ASSERT(coordinators.size() > (g_simulator.desiredCoordinators-1)/2);

		if(firstCoordinators.size()) {
			ASSERT(firstCoordinators.size() > (g_simulator.desiredCoordinators-1)/2);

			//check that at least one coordinator from the first set is not excluded.
			int firstSafeCount = 0;
			for( auto it : firstCoordinators ) {
				RemoveServersSafelyWorkload *tSelf = self;
				if(std::none_of(toKill.begin(), toKill.end(), [tSelf, it](AddressExclusion exclusion){ return tSelf->killContainsProcess(exclusion, it); })) {
					++firstSafeCount;
				}
			}

			int idx = g_random->randomInt(0, firstCoordinators.size());
			int startIndex = idx;
			while(firstSafeCount <= (g_simulator.desiredCoordinators-1)/2 ) {
				//remove a random coordinator from the kill list
				auto addr = firstCoordinators[idx];

				int removedCount = 0;
				for (auto it = toKill.begin(); it != toKill.end(); ) {
					if(self->killContainsProcess(*it, firstCoordinators[idx])) {
						toKill.erase(it++);
						removedCount++;
					}
					else {
						it ++;
					}
				}
				if(removedCount >= 1) {
					firstSafeCount++;
				}

				idx = (idx + 1) % firstCoordinators.size();
				ASSERT(idx != startIndex || firstSafeCount > (g_simulator.desiredCoordinators-1)/2);
			}
		}

		//check that at least one coordinator is not excluded.
		int safeCount = 0;
		for( auto it : coordinators ) {
			RemoveServersSafelyWorkload *tSelf = self;
			if(std::none_of(toKill.begin(), toKill.end(), [tSelf, it](AddressExclusion exclusion){ return tSelf->killContainsProcess(exclusion, it); })) {
				safeCoordinators.push_back(it);
				++safeCount;
			}
		}

		int idx = g_random->randomInt(0, coordinators.size());
		int startIndex = idx;
		while(safeCount <= (g_simulator.desiredCoordinators-1)/2) {
			//remove a random coordinator from the kill list
			auto addr = coordinators[idx];

			int removedCount = 0;
			for (auto it = toKill.begin(); it != toKill.end(); ) {
				if(self->killContainsProcess(*it, coordinators[idx])) {
					toKill.erase(it++);
					removedCount++;
				}
				else {
					it ++;
				}
			}
			if (removedCount >= 1) {
				TraceEvent("ProtectMachine").detail("Address", addr).detail("Coordinators", coordinators.size()).backtrace();
				g_simulator.protectedAddresses.insert(addr);
				safeCoordinators.push_back(addr);
				safeCount++;
			}

			idx = (idx + 1) % coordinators.size();
			ASSERT(idx != startIndex || safeCount > (g_simulator.desiredCoordinators-1)/2);
		}

		state std::vector<ISimulator::ProcessInfo*>	killProcesses;

		killProcesses = self->getProcesses(toKill);
		TraceEvent("RemoveAndKill").detail("Step", "exclude").detail("Addresses", describe(toKill))
			.detail("AddressTotal", toKill.size()).detail("Processes", killProcesses.size())
			.detail("SafeCount", safeCount).detail("ClusterAvailable", g_simulator.isAvailable());
	//	ASSERT(toKill.size() == killProcesses.size());
		for (auto& killProcess : killProcesses) {
			killProcess->excluded = true;
			TraceEvent("RemoveAndKill").detail("Step", "exclude process").detail("killProcess", describe(*killProcess));
		}

		state std::vector<AddressExclusion>	toKillArray;
		std::copy(toKill.begin(), toKill.end(), std::back_inserter(toKillArray));

		Void _ = wait( excludeServers( cx, toKillArray ) );

		// We need to skip at least the quorum change if there's nothing to kill, because there might not be enough servers left
		// alive to do a coordinators auto (?)
		if (toKill.size()) {
			// Wait for removal to be safe
			TraceEvent("RemoveAndKill").detail("Step", "wait").detail("Addresses", describe(toKill));
			Void _ = wait( waitForExcludedServers( cx, toKillArray ) );

			// Change coordinators if necessary
			TraceEvent("RemoveAndKill").detail("Step", "coordinators auto");
			if(outSafeCoordinators != NULL) {
				for(auto it : safeCoordinators) {
					outSafeCoordinators->push_back(it);
				}
			}
			while (true) {
				CoordinatorsResult::Type result = wait( changeQuorum( cx, autoQuorumChange(((g_simulator.desiredCoordinators+1)/2)*2-1) ) );
				TraceEvent(result==CoordinatorsResult::SUCCESS || result==CoordinatorsResult::SAME_NETWORK_ADDRESSES ? SevInfo : SevWarn, "RemoveAndKillQuorumChangeResult").detail("Step", "coordinators auto").detail("Result", (int)result);
				if (result==CoordinatorsResult::SUCCESS || result==CoordinatorsResult::SAME_NETWORK_ADDRESSES)
					break;
			}

			TraceEvent("RemoveAndKill").detail("Step", "unexclude before clear").detail("Processes", killProcesses.size());
			for (auto& killProcess : killProcesses) {
				killProcess->excluded = false;
			}

			// Reboot and delete the servers
			if( self->killProcesses ) {
				TraceEvent("RemoveAndKill").detail("Step", "ClearProcesses").detail("Addresses", describe(toKill))
					.detail("Processes", killProcesses.size()).detail("ClusterAvailable", g_simulator.isAvailable());
				for (auto& killProcess : killProcesses) {
					TraceEvent("RemoveAndKill").detail("Step", "Clear Process").detail("Process", describe(*killProcess)).detail("ClusterAvailable", g_simulator.isAvailable()).detail("Protected", g_simulator.protectedAddresses.count(killProcess->address) == 0);
					g_simulator.rebootProcess( killProcess, ISimulator::RebootProcessAndDelete );
				}
			}
			else {
				std::set<Optional<Standalone<StringRef>>> zoneIds;
				bool killedMachine;
				for (auto& killProcess : killProcesses) {
					zoneIds.insert(killProcess->locality.zoneId());
				}
				TraceEvent("RemoveAndKill").detail("Step", "ClearMachines").detail("Addresses", describe(toKill)).detail("Processes", killProcesses.size()).detail("Zones", zoneIds.size()).detail("ClusterAvailable", g_simulator.isAvailable());
				for (auto& zoneId : zoneIds) {
					killedMachine = g_simulator.killMachine( zoneId, ISimulator::RebootAndDelete, true );
					TraceEvent(killedMachine ? SevInfo : SevWarn, "RemoveAndKill").detail("Step", "Clear Machine").detailext("ZoneId", zoneId).detail("Cleared", killedMachine).detail("ClusterAvailable", g_simulator.isAvailable());
				}
			}
		}
		else
		{
			TraceEvent("RemoveAndKill").detail("Step", "nothing to clear");
		}

		TraceEvent("RemoveAndKill").detail("Step", "done")
			.detail("ClusterAvailable", g_simulator.isAvailable());

		return Void();
	}

	static vector<ISimulator::ProcessInfo*> getServers() {
		vector<ISimulator::ProcessInfo*> machines;
		vector<ISimulator::ProcessInfo*> all = g_simulator.getAllProcesses();
		for(int i = 0; i < all.size(); i++)
			if (all[i]->name == std::string("Server") && all[i]->startingClass != ProcessClass::TesterClass )
				machines.push_back( all[i] );
		return machines;
	}

	template <class T> static std::set<T> random_subset( std::vector<T> v, int n ) {
		std::set<T>	subset;
		// No, this isn't efficient!
		g_random->randomShuffle(v);
		v.resize(n);
		std::copy(v.begin(), v.end(), std::inserter(subset,subset.end()));
		return subset;
	}

	bool killContainsProcess(AddressExclusion kill, NetworkAddress process) {
		return kill.excludes(process) || (machineProcesses.find(kill) != machineProcesses.end() && machineProcesses[kill].count(AddressExclusion(process.ip, process.port)) > 0);
	}
};

WorkloadFactory<RemoveServersSafelyWorkload> RemoveServersSafelyWorkloadFactory("RemoveServersSafely");
