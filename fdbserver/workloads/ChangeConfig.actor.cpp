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

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ManagementAPI.h"
#include "workloads.h"
#include "fdbrpc/simulator.h"

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

	virtual std::string description() { return "ChangeConfig"; }

	virtual Future<Void> start( Database const& cx ) {
		if( this->clientId != 0 ) return Void();
		return ChangeConfigClient( cx->clone(), this );
	}

	virtual Future<bool> check( Database const& cx ) {
		return true;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {}

	ACTOR Future<Void> extraDatabaseConfigure(ChangeConfigWorkload *self) {
		if (g_network->isSimulated() && g_simulator.extraDB) {
			Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
			Reference<Cluster> cluster = Cluster::createCluster(extraFile, -1);
			state Database extraDB = cluster->createDatabase(LiteralStringRef("DB")).get();

			Void _ = wait(delay(5*g_random->random01()));
			if (self->configMode.size())
				ConfigurationResult::Type _ = wait(changeConfig(extraDB, self->configMode));
			if (self->networkAddresses.size()) {
				if (self->networkAddresses == "auto")
					CoordinatorsResult::Type _ = wait(changeQuorum(extraDB, autoQuorumChange()));
				else
					CoordinatorsResult::Type _ = wait(changeQuorum(extraDB, specifiedQuorumChange(NetworkAddress::parseList(self->networkAddresses))));
			}
			Void _ = wait(delay(5*g_random->random01()));
		}
		return Void();
	}

	ACTOR Future<Void> ChangeConfigClient( Database cx, ChangeConfigWorkload *self) {
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "ChangeConfig");
		Void _ = wait( delay( self->minDelayBeforeChange + g_random->random01() * ( self->maxDelayBeforeChange - self->minDelayBeforeChange ) ) );

		state bool extraConfigureBefore = g_random->random01() < 0.5;

		if(extraConfigureBefore) {
			Void _ = wait( self->extraDatabaseConfigure(self) );
		}

		if( self->configMode.size() )
			ConfigurationResult::Type _ = wait( changeConfig( cx, self->configMode ) );
		if( self->networkAddresses.size() ) {
			if (self->networkAddresses == "auto")
				CoordinatorsResult::Type _ = wait( changeQuorum( cx, autoQuorumChange() ) );
			else
				CoordinatorsResult::Type _ = wait( changeQuorum( cx, specifiedQuorumChange(NetworkAddress::parseList( self->networkAddresses )) ) );
		}

		if(!extraConfigureBefore) {
			Void _ = wait( self->extraDatabaseConfigure(self) );
		}

		return Void();
	}
};

WorkloadFactory<ChangeConfigWorkload> ChangeConfigWorkloadFactory("ChangeConfig");
