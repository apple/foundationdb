/*
 * RandomRangeLock.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>

struct RandomRangeLockWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "RandomRangeLock";

	bool enabled;
	double testDuration = deterministicRandom()->random01() * 60.0;
	double testStartDelay = deterministicRandom()->random01() * 300.0;

	RandomRangeLockWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {
		enabled = (clientId == 0) && g_network->isSimulated();
	}

	RandomRangeLockWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		enabled = (clientId == 0) && g_network->isSimulated();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	bool shouldInject(DeterministicRandom& random,
	                  const WorkloadRequest& work,
	                  const unsigned alreadyAdded) const override {
		// Inject this workload with 10% probability given that the workload uses database
		return alreadyAdded == 0 && work.useDatabase && random.random01() < 0.1;
	}

	KeyRange getRandomRange() const {
		char* startKeyBuffer = new char[1];
		char* endKeyBuffer = new char[1];
		int startPoint = deterministicRandom()->randomInt(0, 254);
		int endPoint = deterministicRandom()->randomInt(startPoint + 1, 255);
		startKeyBuffer[0] = static_cast<char>(startPoint);
		endKeyBuffer[0] = static_cast<char>(endPoint);
		Key beginKey = StringRef(std::string(startKeyBuffer));
		Key endKey = StringRef(std::string(endKeyBuffer));
		KeyRange res = Standalone(KeyRangeRef(beginKey, endKey));
		delete[] startKeyBuffer;
		delete[] endKeyBuffer;
		return res;
	}

	ACTOR Future<Void> _start(Database cx, RandomRangeLockWorkload* self) {
		if (self->enabled) {
			wait(delay(self->testStartDelay));
			state KeyRange range = self->getRandomRange();
			TraceEvent(SevWarnAlways, "InjectRangeLockSubmit").detail("Range", range);
			wait(lockCommitUserRange(cx, range));
			TraceEvent(SevWarnAlways, "InjectRangeLocked")
			    .detail("Range", range)
			    .detail("LockTime", self->testDuration);
			wait(delay(self->testDuration));
			wait(unlockCommitUserRange(cx, range));
			TraceEvent(SevWarnAlways, "InjectRangeUnlocked").detail("Range", range);
		}
		return Void();
	}
};

FailureInjectorFactory<RandomRangeLockWorkload> RangeLockFailureInjectionFactory;
