/*
 * ChangeConfig.actor.cpp
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
#include "fdbclient/ClusterInterface.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct ChangeConfigWorkload : TestWorkload {
	double minDelayBeforeChange, maxDelayBeforeChange;
	std::string configMode; //<\"single\"|\"double\"|\"triple\">
	std::string networkAddresses; //comma separated list e.g. "127.0.0.1:4000,127.0.0.1:4001"

	ChangeConfigWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		minDelayBeforeChange = getOption( options, LiteralStringRef("minDelayBeforeChange"), 0 );
		maxDelayBeforeChange = getOption( options, LiteralStringRef("maxDelayBeforeChange"), 0 );
		ASSERT( maxDelayBeforeChange >= minDelayBeforeChange );
		configMode = getOption( options, LiteralStringRef("configMode"), StringRef() ).toString();
		networkAddresses = getOption( options, LiteralStringRef("coordinators"), StringRef() ).toString();
	}

	std::string description() const override { return "ChangeConfig"; }

	Future<Void> start(Database const& cx) override {
		if( this->clientId != 0 ) return Void();
		return ChangeConfigClient( cx->clone(), this );
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}

	ACTOR Future<Void> extraDatabaseConfigure(ChangeConfigWorkload *self) {
		if (g_network->isSimulated() && g_simulator.extraDB) {
			auto extraFile = makeReference<ClusterConnectionFile>(*g_simulator.extraDB);
			state Database extraDB = Database::createDatabase(extraFile, -1);

			wait(delay(5*deterministicRandom()->random01()));
			if (self->configMode.size()) {
				wait(success(changeConfig(extraDB, self->configMode, true)));
				TraceEvent("WaitForReplicasExtra");
				wait( waitForFullReplication( extraDB ) );
				TraceEvent("WaitForReplicasExtraEnd");
			} if (self->networkAddresses.size()) {
				if (self->networkAddresses == "auto")
					wait(success(changeQuorum(extraDB, autoQuorumChange())));
				else
					wait(success(changeQuorum(extraDB, specifiedQuorumChange(NetworkAddress::parseList(self->networkAddresses)))));
			}
			wait(delay(5*deterministicRandom()->random01()));
		}
		return Void();
	}

	ACTOR Future<Void> ChangeConfigClient( Database cx, ChangeConfigWorkload *self) {
		wait( delay( self->minDelayBeforeChange + deterministicRandom()->random01() * ( self->maxDelayBeforeChange - self->minDelayBeforeChange ) ) );

		state bool extraConfigureBefore = deterministicRandom()->random01() < 0.5;

		if(extraConfigureBefore) {
			wait( self->extraDatabaseConfigure(self) );
		}

		if( self->configMode.size() ) {
			wait(success( changeConfig( cx, self->configMode, true ) ));
			TraceEvent("WaitForReplicas");
			wait( waitForFullReplication( cx ) );
			TraceEvent("WaitForReplicasEnd");
		}
		if( self->networkAddresses.size() ) {
			if (self->networkAddresses == "auto")
				wait(success( changeQuorum( cx, autoQuorumChange() ) ));
			else
				wait(success( changeQuorum( cx, specifiedQuorumChange(NetworkAddress::parseList( self->networkAddresses )) ) ));
		}

		if(!extraConfigureBefore) {
			wait( self->extraDatabaseConfigure(self) );
		}

		return Void();
	}
};

WorkloadFactory<ChangeConfigWorkload> ChangeConfigWorkloadFactory("ChangeConfig");
