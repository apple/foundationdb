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
	double testDelay;
	CoordinatorsForceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numExtra = getOption(options, LiteralStringRef("numExtra"), 1);
		testDelay = getOption(options, LiteralStringRef("testDelay"), 30.0);
	}

	std::string description() const override { return "CoordinatorsForce"; }

	ACTOR static Future<Void> _start(Database cx, CoordinatorsForceWorkload* self) {
		state std::vector<NetworkAddress> newCoordinators;
		state int protectedAdded = 0;
		state int normalAdded = 0;

		// Similar to the check in ConsistencyCheck::checkCoordinators,
		// ensure that two coordinators do not have the same zoneID
		state vector<ProcessData> workers = wait(getWorkers(cx));

		// Wait and let simulation run a little bit first
		wait(delay(self->testDelay));

		state std::map<NetworkAddress, LocalityData> addr_locality;
		for (auto w : workers) {
			addr_locality[w.address] = w.locality;
		}
		state std::set<Optional<Standalone<StringRef>>> checkDuplicates;
		for (auto pAddress : g_simulator.protectedAddresses) {
			if (protectedAdded == g_simulator.desiredCoordinators) break;
			auto findResult = addr_locality.find(pAddress);
			if (findResult == addr_locality.end() || checkDuplicates.count(findResult->second.zoneId())) continue;
			newCoordinators.push_back(pAddress);
			checkDuplicates.insert(findResult->second.zoneId());
			++protectedAdded;
		}
		state vector<ISimulator::ProcessInfo*> rebootProcesses;
		for (auto processInfo : g_simulator.getAllProcesses()) {
			if (normalAdded == self->numExtra) break;
			if (g_simulator.protectedAddresses.count(processInfo->address)) {
				// Don't add a protected address in this phase
				continue;
			}
			auto findResult = addr_locality.find(processInfo->address);
			if (findResult == addr_locality.end() || checkDuplicates.count(findResult->second.zoneId())) continue;
			rebootProcesses.push_back(processInfo);
			newCoordinators.push_back(processInfo->address);
			checkDuplicates.insert(findResult->second.zoneId());
			++normalAdded;
		}
		TraceEvent("CoordinatorsForceSet").detail("NewCoordinators", describe(newCoordinators));
		state Reference<IQuorumChange> change = specifiedQuorumChange(newCoordinators);

		// Send reboot request right before attempting to change
		state int procNum;
		for (procNum = 0; procNum < rebootProcesses.size(); ++procNum) {
			g_simulator.rebootProcess(rebootProcesses[procNum], ISimulator::KillType::Reboot);
			// Wait in order to allow the actor to actually perform the reboot
			wait(delay(1.0));
			ASSERT(!rebootProcesses[procNum]->isAvailable());
		}

		CoordinatorsResult result = wait(changeQuorum(cx, change, true));
		TraceEvent("CoordinatorsForceResult").detail("Result", result);
		ASSERT(result == CoordinatorsResult::SUCCESS);
		return Void();
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<CoordinatorsForceWorkload> CoordinatorsForceWorkloadFactory("CoordinatorsForce");