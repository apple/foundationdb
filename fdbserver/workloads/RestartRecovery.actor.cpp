/*
 * RestartRecovery.actor.cpp
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

struct RestartRecoveryWorkload : TestWorkload {
	std::string machineToKill;
	bool enabled;
	double killAt;

	RestartRecoveryWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		enabled = !clientId; // only do this on the "first" client
		killAt = getOption( options, LiteralStringRef("killAt"), 10.0 );
	}

	virtual std::string description() { return "RestartRecoveryWorkload"; }
	virtual Future<Void> setup( Database const& cx ) { return Void(); }
	virtual Future<Void> start( Database const& cx ) {
		if (enabled)
			return assassin( cx, this );
		return Void();
	}
	virtual Future<bool> check( Database const& cx ) { return true; }
	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	ACTOR Future<Void> assassin( Database cx, RestartRecoveryWorkload* self ) {
		Void _ = wait( delay( self->killAt ) );
		state std::vector<TLogInterface> logs = self->dbInfo->get().logSystemConfig.allPresentLogs();
		if(logs.size() > 2 && g_simulator.killableMachines > 0) {
			TraceEvent("RestartRecoveryReboot").detail("addr", logs[2].address());
			g_simulator.rebootProcess( g_simulator.getProcessByAddress(NetworkAddress(logs[2].address().ip, logs[2].address().port, true, false)), ISimulator::RebootProcess );
			Void _ = wait( delay(8.0) );
			TraceEvent("RestartRecoveryKill");
			g_simulator.rebootProcess( g_simulator.getProcessByAddress(NetworkAddress(logs[0].address().ip, logs[0].address().port, true, false)), ISimulator::RebootProcessAndDelete );
		}
		return Void();
	}
};

WorkloadFactory<RestartRecoveryWorkload> RestartRecoveryWorkloadFactory("RestartRecovery");
