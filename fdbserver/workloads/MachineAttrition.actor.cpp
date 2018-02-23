/*
 * MachineAttrition.actor.cpp
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

static std::set<int> const& normalAttritionErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert( error_code_please_reboot );
		s.insert( error_code_please_reboot_delete );
	}
	return s;
}

struct MachineAttritionWorkload : TestWorkload {
	bool enabled;
	int machinesToKill, machinesToLeave;
	double testDuration;
	bool reboot;
	bool killDc;
	bool killSelf;
	bool replacement;
	bool waitForVersion;
	bool allowFaultInjection;

	// This is set in setup from the list of workers when the cluster is started
	std::vector<LocalityData> machines;

	MachineAttritionWorkload( WorkloadContext const& wcx )
		: TestWorkload(wcx)
	{
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		machinesToKill = getOption( options, LiteralStringRef("machinesToKill"), 2 );
		machinesToLeave = getOption( options, LiteralStringRef("machinesToLeave"), 1 );
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		reboot = getOption( options, LiteralStringRef("reboot"), false );
		killDc = getOption( options, LiteralStringRef("killDc"), g_random->random01() < 0.25 );
		killSelf = getOption( options, LiteralStringRef("killSelf"), false );
		replacement = getOption( options, LiteralStringRef("replacement"), reboot && g_random->random01() < 0.5 );
		waitForVersion = getOption( options, LiteralStringRef("waitForVersion"), false );
		allowFaultInjection = getOption( options, LiteralStringRef("allowFaultInjection"), true );
	}

	static vector<ISimulator::ProcessInfo*> getServers() {
		vector<ISimulator::ProcessInfo*> machines;
		vector<ISimulator::ProcessInfo*> all = g_simulator.getAllProcesses();
		for(int i = 0; i < all.size(); i++)
			if( !all[i]->failed && all[i]->name == std::string("Server") && all[i]->startingClass != ProcessClass::TesterClass)
				machines.push_back( all[i] );
		return machines;
	}

	virtual std::string description() { return "MachineAttritionWorkload"; }
	virtual Future<Void> setup( Database const& cx ) {
		return Void();
	}
	virtual Future<Void> start( Database const& cx ) {
		if (enabled) {
			std::map<Optional<Standalone<StringRef>>,LocalityData> machineIDMap;
			auto processes = getServers();
			for (auto it = processes.begin(); it != processes.end(); ++it) {
				machineIDMap[(*it)->locality.zoneId()] = (*it)->locality;
			}
			machines.clear();
			for (auto it = machineIDMap.begin(); it != machineIDMap.end(); ++it) {
				machines.push_back(it->second);
			}
			g_random->randomShuffle( machines );
			double meanDelay = testDuration / machinesToKill;
			TraceEvent("AttritionStarting")
				.detail("KillDataCenters", killDc)
				.detail("Reboot", reboot)
				.detail("MachinesToLeave", machinesToLeave)
				.detail("MachinesToKill", machinesToKill)
				.detail("MeanDelay", meanDelay);

			return timeout(
				reportErrorsExcept( machineKillWorker( this, meanDelay, cx ), "machineKillWorkerError", UID(), &normalAttritionErrors()),
				testDuration, Void() );
		}
		if(killSelf)
			throw please_reboot();
		return Void();
	}
	virtual Future<bool> check( Database const& cx ) { return true; }
	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	struct UIDPredicate {
		UIDPredicate(StringRef uid ) : uid( uid ) {}
		bool operator() ( WorkerInterface rhs ) { return rhs.locality.zoneId() != uid; }
	private:
		StringRef uid;
	};

	ACTOR static Future<Void> machineKillWorker( MachineAttritionWorkload *self, double meanDelay, Database cx ) {
		state int killedMachines = 0;
		state double delayBeforeKill = g_random->random01() * meanDelay;
		state std::set<UID> killedUIDs;

		ASSERT( g_network->isSimulated() );

		TEST(g_simulator.killableMachines > 0);    // Some machines can be killed
		TEST(g_simulator.killableDatacenters > 0); // Some processes can be killed

		if( self->killDc ) {
			Void _ = wait( delay( delayBeforeKill ) );

			// decide on a machine to kill
			ASSERT( self->machines.size() );
			Optional<Standalone<StringRef>> target = self->machines.back().dcId();

			ISimulator::KillType kt = ISimulator::Reboot;
			if( !self->reboot ) {
				int killType = g_random->randomInt(0,3);
				if( killType == 0 )
					kt = ISimulator::KillInstantly;
				else if( killType == 1 )
					kt = ISimulator::InjectFaults;
				else
					kt = ISimulator::RebootAndDelete;
			}
			TraceEvent("Assassination").detailext("TargetDatacenter", target).detail("Reboot", self->reboot).detail("KillType", kt);

			g_simulator.killDataCenter( target, kt );
		} else {
			while ( killedMachines < self->machinesToKill && self->machines.size() > self->machinesToLeave) {
				TraceEvent("WorkerKillBegin").detail("killedMachines", killedMachines)
					.detail("machinesToKill", self->machinesToKill).detail("machinesToLeave", self->machinesToLeave)
					.detail("machines", self->machines.size());
				TEST(true);  // Killing a machine

				Void _ = wait( delay( delayBeforeKill ) );
				TraceEvent("WorkerKillAfterDelay");

				if(self->waitForVersion) {
					state Transaction tr( cx );
					loop {
						try {
							tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
							tr.setOption(FDBTransactionOptions::LOCK_AWARE);
							Version _ = wait(tr.getReadVersion());
							break;
						} catch( Error &e ) {
							Void _ = wait( tr.onError(e) );
						}
					}
				}

				// decide on a machine to kill
				LocalityData targetMachine = self->machines.back();

				TraceEvent("Assassination").detail("TargetMachine", targetMachine.toString())
					.detailext("zoneId", targetMachine.zoneId())
					.detail("Reboot", self->reboot).detail("killedMachines", killedMachines)
					.detail("machinesToKill", self->machinesToKill).detail("machinesToLeave", self->machinesToLeave)
					.detail("machines", self->machines.size()).detail("Replace", self->replacement);

				if (self->reboot) {
					if( g_random->random01() > 0.5 ) {
						g_simulator.rebootProcess( targetMachine.zoneId(), g_random->random01() > 0.5 );
					} else {
						g_simulator.killMachine( targetMachine.zoneId(), ISimulator::Reboot );
					}
				} else {
					auto randomDouble = g_random->random01();
					TraceEvent("WorkerKill").detail("MachineCount", self->machines.size()).detail("RandomValue", randomDouble);
					if (randomDouble < 0.33 ) {
						TraceEvent("RebootAndDelete").detail("TargetMachine", targetMachine.toString());
						g_simulator.killMachine( targetMachine.zoneId(), ISimulator::RebootAndDelete );
					} else {
						auto kt = (g_random->random01() < 0.5 || !self->allowFaultInjection) ? ISimulator::KillInstantly : ISimulator::InjectFaults;
						g_simulator.killMachine( targetMachine.zoneId(), kt );
					}
				}

				killedMachines++;
				if(!self->replacement)
					self->machines.pop_back();

				Void _ = wait( delay( meanDelay - delayBeforeKill ) );
				delayBeforeKill = g_random->random01() * meanDelay;
				TraceEvent("WorkerKillAfterMeanDelay").detail("DelayBeforeKill", delayBeforeKill);
			}
		}

		if(self->killSelf)
			throw please_reboot();
		return Void();
	}
};

WorkloadFactory<MachineAttritionWorkload> MachineAttritionWorkloadFactory("Attrition");
