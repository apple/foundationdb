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
	
	int numFailed;
	CoordinatorsForceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numFailed = getOption(options, LiteralStringRef("numFailed"), 1);
	}

	std::string description() const override { return "CoordinatorsForce"; }

	ACTOR static Future<Void> _start(Database cx, CoordinatorsForceWorkload* self) {
		state std::vector<NetworkAddress> newCoordinators;
		state int aliveAdded = 0;
		state int failedAdded = 0;
		for (auto processInfo : g_simulator.getAllProcesses()) {
			TraceEvent("Checkpoint1")
				.detail("Process", processInfo->address)
				.detail("Available", processInfo->isAvailable());
			if (aliveAdded == g_simulator.desiredCoordinators &&
				failedAdded == self->numFailed) {
				break;
			}
			if (failedAdded != self->numFailed) {
				newCoordinators.push_back(processInfo->address);
				g_simulator.killProcess(processInfo, ISimulator::RebootProcess);
				TraceEvent("Checkpoint1")
					.detail("Process", processInfo->address)
					.detail("Available", processInfo->isAvailable());
				++failedAdded;
				continue;
			}
			if (processInfo->isAvailable()) {
				if (aliveAdded != g_simulator.desiredCoordinators) {
					newCoordinators.push_back(processInfo->address);
					++aliveAdded;
				}
			} 

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