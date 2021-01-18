/*
 * CoordinatorsForce.actor.cpp
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
#include "flow/Arena.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct CoordinatorsForceWorkload : TestWorkload {

	int numExtra;
	CoordinatorsForceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numExtra = getOption(options, LiteralStringRef("numExtra"), 1);
	}

	std::string description() const override { return "CoordinatorsForce"; }

	ACTOR static Future<Void> _start(Database cx, CoordinatorsForceWorkload* self) {
		state std::vector<NetworkAddress> newCoordinators;
		state int protectedAdded = 0;
		state int normalAdded = 0;

		for (auto pAddress : g_simulator.protectedAddresses) {
			if (protectedAdded == g_simulator.desiredCoordinators) break;
			newCoordinators.push_back(pAddress);
			++protectedAdded;
		}

		for (auto processInfo : g_simulator.getAllProcesses()) {
			if (normalAdded == self->numExtra) break;
			if (g_simulator.protectedAddresses.count(processInfo->address)) {
				// Don't add a protected address in this phase
				continue;
			}
			newCoordinators.push_back(processInfo->address);
			++normalAdded;
		}
		TraceEvent("CoordinatorsForceSet").detail("NewCoordinators", describe(newCoordinators));
		state Reference<IQuorumChange> change = specifiedQuorumChange(newCoordinators);

		CoordinatorsResult result = wait(changeQuorum(cx, change, true));
		TraceEvent("CoordinatorsForceResult").detail("Result", result);
		return Void();
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<CoordinatorsForceWorkload> CoordinatorsForceWorkloadFactory("CoordinatorsForce");