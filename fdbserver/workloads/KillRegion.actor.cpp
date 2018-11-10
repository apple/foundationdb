/*
 * KillRegion.actor.cpp
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
#include "fdbserver/workloads/workloads.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.h"

struct KillRegionWorkload : TestWorkload {
	bool enabled;
	double testDuration;

	KillRegionWorkload( WorkloadContext const& wcx )
		: TestWorkload(wcx)
	{
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		g_simulator.usableRegions = 1;
	}

	virtual std::string description() { return "KillRegionWorkload"; }
	virtual Future<Void> setup( Database const& cx ) {
		if(enabled) {
			return _setup( this, cx );
		}
		return Void();
	}
	virtual Future<Void> start( Database const& cx ) {
		if(enabled) {
			return killRegion( this, cx );
		}
		return Void();
	}
	virtual Future<bool> check( Database const& cx ) { return true; }
	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	ACTOR static Future<Void> _setup( KillRegionWorkload *self, Database cx ) {
		TraceEvent("ForceRecovery_DisablePrimaryBegin");
		ConfigurationResult::Type _ = wait( changeConfig( cx, g_simulator.disablePrimary, true ) );
		TraceEvent("ForceRecovery_WaitForRemote");
		wait( waitForPrimaryDC(cx, LiteralStringRef("1")) );
		TraceEvent("ForceRecovery_DisablePrimaryComplete");
		return Void();
	}

	ACTOR static Future<Void> killRegion( KillRegionWorkload *self, Database cx ) {
		ASSERT( g_network->isSimulated() );
		TraceEvent("ForceRecovery_DisableRemoteBegin");
		ConfigurationResult::Type _ = wait( changeConfig( cx, g_simulator.disableRemote, true ) );
		TraceEvent("ForceRecovery_WaitForPrimary");
		wait( waitForPrimaryDC(cx, LiteralStringRef("0")) );
		TraceEvent("ForceRecovery_DisableRemoteComplete");
		ConfigurationResult::Type _ = wait( changeConfig( cx, g_simulator.originalRegions, true ) );
		TraceEvent("ForceRecovery_RestoreOriginalComplete");
		wait( delay( g_random->random01() * self->testDuration ) );

		g_simulator.killDataCenter( LiteralStringRef("0"), ISimulator::RebootAndDelete, true );
		g_simulator.killDataCenter( LiteralStringRef("2"), ISimulator::RebootAndDelete, true );
		g_simulator.killDataCenter( LiteralStringRef("4"), ISimulator::RebootAndDelete, true );

		state bool first = true;
		loop {
			state Transaction tr(cx);
			loop {
				try {
					tr.addWriteConflictRange(KeyRangeRef(LiteralStringRef(""), LiteralStringRef("\x00")));
					choose {
						when( wait(tr.commit()) ) {
							TraceEvent("ForceRecovery_Complete");
							g_simulator.killDataCenter( LiteralStringRef("1"), ISimulator::Reboot );
							g_simulator.killDataCenter( LiteralStringRef("3"), ISimulator::Reboot );
							g_simulator.killDataCenter( LiteralStringRef("5"), ISimulator::Reboot );
							return Void();
						}
						when( wait(delay(first ? 30.0 : 300.0)) ) {
							break;
						}
					}
				} catch( Error &e ) {
					wait( tr.onError(e) );
				}
			}
			TraceEvent("ForceRecovery_Begin");
			wait( forceRecovery(cx->cluster->getConnectionFile()) );
			first = false;
			TraceEvent("ForceRecovery_Attempted");
		}
	}
};

WorkloadFactory<KillRegionWorkload> KillRegionWorkloadFactory("KillRegion");
