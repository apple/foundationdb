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

	vector<AddressExclusion> toKill1, toKill2;
	std::map<AddressExclusion, Optional<Standalone<StringRef>>> machine_ids;
	std::map<AddressExclusion, std::set<AddressExclusion>> machineProcesses;

	RemoveServersSafelyWorkload( WorkloadContext const& wcx )
		: TestWorkload(wcx)
	{
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		minMachinesToKill = getOption( options, LiteralStringRef("minMachinesToKill"), 1 );
		maxMachinesToKill = getOption( options, LiteralStringRef("maxMachinesToKill"), 10 );
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

		std::map<Optional<Standalone<StringRef>>, AddressExclusion> machinesMap;
		std::vector<AddressExclusion> processAddrs;
		std::map<uint32_t, Optional<Standalone<StringRef>>> ip_dcid;
		auto processes = getServers();
		for(auto& it : processes) {

			AddressExclusion addr(it->address.ip);

			if (killProcesses){
				AddressExclusion pAddr(it->address.ip, it->address.port);
				processAddrs.push_back(pAddr);
			}
			else {
				machineProcesses[AddressExclusion(it->machine->machineProcess->address.ip)].insert(AddressExclusion(it->address.ip, it->address.port));
			}
			// add only one entry for each machine
			if (!machinesMap.count(it->locality.zoneId()))
				machinesMap[it->locality.zoneId()] = addr;

			machine_ids[addr] = it->locality.zoneId();
			ip_dcid[it->address.ip] = it->locality.dcId();
		}

		if (!killProcesses){
			for (auto m : machinesMap){
				processAddrs.push_back(m.second);
			}
		}

		//data centers can kill too many machines, that when combined with the excluded machines from this workload we cannot chance coordinators
		if(g_network->isSimulated())
			g_simulator.killableDatacenters = 0;

		int nToKill1 = processAddrs.size();
		nToKill1 = g_random->randomInt( std::min(nToKill1,minMachinesToKill), std::min(nToKill1,maxMachinesToKill)+1 );
		int nToKill2 = std::max<int>(0, machinesMap.size() - g_simulator.killableMachines - std::max(g_simulator.machinesNeededForProgress, g_simulator.desiredCoordinators));
		nToKill2 = g_random->randomInt( std::min(nToKill2,minMachinesToKill), std::min(nToKill2,maxMachinesToKill)+1 );
		toKill1 = random_subset( processAddrs, nToKill1 );

		loop {
			toKill2 = random_subset( processAddrs, nToKill2 );
			std::set<Optional<Standalone<StringRef>>> datacenters;
			for(auto& addr : processAddrs)
				if(std::find(toKill2.begin(), toKill2.end(), addr) == toKill2.end())
					datacenters.insert(ip_dcid[addr.ip]);
			if(datacenters.size() >= g_simulator.neededDatacenters) {
				//FIXME: each machine kill could take down a datacenter after exclusion, so for now we need to lower killable machines
				g_simulator.killableMachines = std::min(g_simulator.killableMachines, std::max<int>(0, datacenters.size() - g_simulator.neededDatacenters));
				break;
			}
		}

		if (!killProcesses) {
			std::vector<AddressExclusion> processKills;

			for (auto k1 : toKill1) {
				ASSERT(machineProcesses.count(k1));
				// kill all processes on this machine even if it has a different ip address
				processKills.insert(processKills.end(), machineProcesses[k1].begin(), machineProcesses[k1].end());
			}

			toKill1.insert(toKill1.end(), processKills.begin(), processKills.end());

			processKills.clear();
			for (auto k2 : toKill2) {
				ASSERT(machineProcesses.count(k2));
				processKills.insert(processKills.end(), machineProcesses[k2].begin(), machineProcesses[k2].end());
			}

			toKill2.insert(toKill2.end(), processKills.begin(), processKills.end());
		}

		std::vector<NetworkAddress> disableAddrs1;
		for( AddressExclusion ex : toKill1 ) {
			AddressExclusion machine(ex.ip);
			ASSERT(machine_ids.count(machine));
			g_simulator.disableSwapToMachine(machine_ids[machine]);
		}

		std::vector<NetworkAddress> disableAddrs2;
		for( AddressExclusion ex : toKill2 ) {
			AddressExclusion machine(ex.ip);
			ASSERT(machine_ids.count(machine));
			g_simulator.disableSwapToMachine(machine_ids[machine]);
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

	ACTOR static Future<Void> workloadMain( RemoveServersSafelyWorkload* self, Database cx, double waitSeconds,
			vector<AddressExclusion> toKill1, vector<AddressExclusion> toKill2 ) {
		Void _ = wait( delay( waitSeconds ) );

		// Removing the first set of machines might legitimately bring the database down, so a timeout is not an error
		state std::vector<NetworkAddress> firstCoordinators;
		Void _ = wait( timeout( removeAndKill( self, cx, toKill1, &firstCoordinators ), self->kill1Timeout, Void() ) );

		// The second set of machines is selected so that we can always make progress without it, even after the permitted number of other permanent failures
		// so we expect to succeed after a finite amount of time
		state Future<Void> disabler = disableConnectionFailuresAfter( self->kill2Timeout/2, "RemoveServersSafely" );
		Void _ = wait( reportErrors( timeoutError( removeAndKill( self, cx, toKill2, NULL, firstCoordinators, true ), self->kill2Timeout ), "RemoveServersSafelyError", UID() ) );

		return Void();
	}

	ACTOR static Future<Void> removeAndKill( RemoveServersSafelyWorkload* self, Database cx, vector<AddressExclusion> toKill, std::vector<NetworkAddress>* outSafeCoordinators, std::vector<NetworkAddress> firstCoordinators = std::vector<NetworkAddress>(), bool exitAfterInclude = false ) {
		// First clear the exclusion list and exclude the given list
		TraceEvent("RemoveAndKill").detail("Step", "include all").detail("first", describe(firstCoordinators));
		Void _ = wait( includeServers( cx, vector<AddressExclusion>(1) ) );

		// The actor final boolean argument is a hack to prevent the second part of this function from happening
		// Fix Me
		NOT_IN_CLEAN;
		if (exitAfterInclude) return Void();

		std::vector<NetworkAddress> coordinators = wait( getCoordinators(cx) );
		state std::vector<NetworkAddress> safeCoordinators;

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
				for(int i = 0; i < toKill.size(); i++) {
					if(self->killContainsProcess(toKill[i], firstCoordinators[idx])) {
						std::swap(toKill[i--], toKill.back());
						toKill.pop_back();
						removedCount++;
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
			for(int i = 0; i < toKill.size(); i++) {
				if(self->killContainsProcess(toKill[i], coordinators[idx])) {
					std::swap(toKill[i--], toKill.back());
					toKill.pop_back();
					removedCount++;
				}
			}
			if(removedCount >= 1) {
				safeCoordinators.push_back(addr);
				safeCount++;
			}

			idx = (idx + 1) % coordinators.size();
			ASSERT(idx != startIndex || safeCount > (g_simulator.desiredCoordinators-1)/2);
		}

		TraceEvent("RemoveAndKill").detail("Step", "exclude").detail("Servers", describe(toKill)).detail("SafeCount", safeCount);
		Void _ = wait( excludeServers( cx, toKill ) );

		// We need to skip at least the quorum change if there's nothing to kill, because there might not be enough servers left
		// alive to do a coordinators auto (?)
		if (toKill.size()) {
			// Wait for removal to be safe
			TraceEvent("RemoveAndKill").detail("Step", "wait").detail("Servers", describe(toKill));
			Void _ = wait( waitForExcludedServers( cx, toKill ) );

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

			// Reboot and delete the servers
			TraceEvent("RemoveAndKill").detail("Step", "RebootAndDelete").detail("Servers", describe(toKill));
			for(auto a = toKill.begin(); a != toKill.end(); ++a) {
				if( self->killProcesses ) {
					TraceEvent("RemoveAndKill").detail("Step", "Kill Process").detail("Process", describe(*a));
					g_simulator.rebootProcess( g_simulator.getProcessByAddress(NetworkAddress(a->ip, a->port, true, false)), ISimulator::RebootProcessAndDelete );
				}
				else {
					TraceEvent("RemoveAndKill").detail("Step", "Kill Machine").detail("MachineAddr", describe(*a));
					g_simulator.killMachine( self->machine_ids[ *a ], ISimulator::RebootAndDelete, true );
				}
			}
		}
		else
		{
			TraceEvent("RemoveAndKill").detail("Step", "nothing to kill");
		}

		TraceEvent("RemoveAndKill").detail("Step", "done");

		return Void();
	}

	static vector<ISimulator::ProcessInfo*> getServers() {
		vector<ISimulator::ProcessInfo*> machines;
		vector<ISimulator::ProcessInfo*> all = g_simulator.getAllProcesses();
		for(int i = 0; i < all.size(); i++)
			if( !all[i]->failed && all[i]->name == std::string("Server") && all[i]->startingClass != ProcessClass::TesterClass )
				machines.push_back( all[i] );
		return machines;
	}

	template <class T> static vector<T> random_subset( vector<T> v, int n ) {
		// No, this isn't efficient!
		g_random->randomShuffle(v);
		v.resize(n);
		return v;
	}

	bool killContainsProcess(AddressExclusion kill, NetworkAddress process) {
		return kill.excludes(process) || machineProcesses[kill].count(AddressExclusion(process.ip, process.port)) > 0;
	}
};

WorkloadFactory<RemoveServersSafelyWorkload> RemoveServersSafelyWorkloadFactory("RemoveServersSafely");
