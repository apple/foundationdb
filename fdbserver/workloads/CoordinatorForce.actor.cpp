/*
 * CoordinatorForce.actor.cpp
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
	// Address to add to the set of existing coordinators.
	// A dummy address should fail unless "FORCE" is used
	std::string additionalAddress;
	CoordinatorsForceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		additionalAddress =
		    getOption(options, LiteralStringRef("additionalAddress"), StringRef("127.1.1.1")).toString();
	}

	std::string description() const override { return "CoordinatorsForce"; }

	ACTOR static Future<Void> _start(Database cx, CoordinatorsForceWorkload* self) {
		state std::vector<NetworkAddress> curCoordinators = wait(getCoordinators(cx));
		curCoordinators.push_back(NetworkAddress::parse(self->additionalAddress));
		state Reference<IQuorumChange> change = specifiedQuorumChange(curCoordinators);
		state bool force = false;
		loop {
			// Attempt to change without FORCE should fail if using dummy address
			CoordinatorsResult result = wait(changeQuorum(cx, change, force));
			if (result == CoordinatorsResult::COORDINATOR_UNREACHABLE) {
				force = true;
			} else if (result == CoordinatorsResult::SUCCESS) {
				return Void();
			}
		}
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<CoordinatorsForceWorkload> CoordinatorsForceWorkloadFactory("CoordinatorsForce");