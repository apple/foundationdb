/*
 * TargetedKill.actor.cpp
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
#include "workloads.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/MasterInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/WorkerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/QuietDatabase.h"

struct TargetedKillWorkload : TestWorkload {
	std::string machineToKill;
	bool enabled, killAllMachineProcesses;
	double killAt;

	TargetedKillWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		enabled = !clientId; // only do this on the "first" client
		killAt = getOption( options, LiteralStringRef("killAt"), 5.0 );
		machineToKill = getOption( options, LiteralStringRef("machineToKill"), LiteralStringRef("master") ).toString();
		killAllMachineProcesses = getOption( options, LiteralStringRef("killWholeMachine"), false );
	}

	virtual std::string description() { return "TargetedKillWorkload"; }
	virtual Future<Void> setup( Database const& cx ) { return Void(); }
	virtual Future<Void> start( Database const& cx ) {
		TraceEvent("StartTargetedKill").detail("Enabled", enabled);
		if (enabled)
			return assassin( cx, this );
		return Void();
	}
	virtual Future<bool> check( Database const& cx ) { return true; }
	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	ACTOR Future<Void> killEndpoint( NetworkAddress address, Database cx, TargetedKillWorkload* self ) {
		if( &g_simulator == g_network ) {
			g_simulator.killInterface( address, ISimulator::KillInstantly );
			return Void();
		}

		state vector<std::pair<WorkerInterface, ProcessClass>> workers = wait( getWorkers( self->dbInfo ) );

		int killed = 0;
		for( int i = 0; i < workers.size(); i++ ) {
			if( workers[i].first.master.getEndpoint().address == address ||
				( self->killAllMachineProcesses && workers[i].first.master.getEndpoint().address.ip == address.ip && workers[i].second != ProcessClass::TesterClass ) ) {
				TraceEvent("WorkerKill").detail("TargetedMachine", address).detail("Worker", workers[i].first.id());
				workers[i].first.clientInterface.reboot.send( RebootRequest() );
			}
		}

		if( !killed )
			TraceEvent(SevWarn, "WorkerNotFoundAtEndpoint").detail("Address", address);
		else
			TraceEvent("WorkersKilledAtEndpoint").detail("Address", address).detail("KilledProcesses", killed);

		return Void();
	}

	ACTOR Future<Void> assassin( Database cx, TargetedKillWorkload* self ) {
		Void _ = wait( delay( self->killAt ) );
		state vector<StorageServerInterface> storageServers = wait( getStorageServers( cx ) );

		NetworkAddress machine;
		if( self->machineToKill == "master" ) {
			machine = self->dbInfo->get().master.address();
		}
		else if( self->machineToKill == "masterproxy" ) {
			auto proxies = cx->getMasterProxies();
			int o = g_random->randomInt(0, proxies->size());
			for( int i = 0; i < proxies->size(); i++) {
				MasterProxyInterface mpi = proxies->getInterface(o);
				machine = mpi.address();
				if(machine != self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().address)
					break;
				o = ++o%proxies->size();
			}
		}
		else if( self->machineToKill == "tlog" ) {
			auto tlogs = self->dbInfo->get().logSystemConfig.allPresentLogs();
			int o = g_random->randomInt(0, tlogs.size());
			for( int i = 0; i < tlogs.size(); i++) {
				TLogInterface tli = tlogs[o];
				machine = tli.address();
				if(machine != self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().address)
					break;
				o = ++o%tlogs.size();
			}
		}
		else if( self->machineToKill == "storage" || self->machineToKill == "ss"  || self->machineToKill == "storageserver" ) {
			int o = g_random->randomInt(0,storageServers.size());
			for( int i = 0; i < storageServers.size(); i++) {
				StorageServerInterface ssi = storageServers[o];
				machine = ssi.address();
				if(machine != self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().address)
					break;
				o = ++o%storageServers.size();
			}
		}
		else if( self->machineToKill == "clustercontroller" || self->machineToKill == "cc" ) {
			machine = self->dbInfo->get().clusterInterface.getWorkers.getEndpoint().address;
		}

		TraceEvent("IsolatedMark").detail("TargetedMachine", machine).detail("Role", self->machineToKill);

		Void _ = wait( self->killEndpoint( machine, cx, self ) );

		return Void();
	}
};

WorkloadFactory<TargetedKillWorkload> TargetedKillWorkloadFactory("TargetedKill");
