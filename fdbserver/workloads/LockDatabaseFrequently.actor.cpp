/*
 * LockDatabaseFrequently.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct LockDatabaseFrequentlyWorkload : TestWorkload {
	static constexpr auto NAME = "LockDatabaseFrequently";

	double delayBetweenLocks;
	double testDuration;
	PerfIntCounter lockCount{ "LockCount" };

	LockDatabaseFrequentlyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		delayBetweenLocks = getOption(options, "delayBetweenLocks"_sr, 0.1);
		testDuration = getOption(options, "testDuration"_sr, 60);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId == 0 ? worker(this, cx) : Void(); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId == 0) {
			m.push_back(lockCount.getMetric());
		}
	}

	ACTOR static Future<Void> worker(LockDatabaseFrequentlyWorkload* self, Database cx) {
		state Future<Void> end = delay(self->testDuration);
		state double lastLock = g_network->now();
		state double lastUnlock = g_network->now() + self->delayBetweenLocks / 2;
		loop {
			wait(lockAndUnlock(self, cx, &lastLock, &lastUnlock));
			++self->lockCount;
			if (end.isReady()) {
				return Void();
			}
		}
	}

	ACTOR static Future<Void> lockAndUnlock(LockDatabaseFrequentlyWorkload* self,
	                                        Database cx,
	                                        double* lastLock,
	                                        double* lastUnlock) {
		state UID uid = deterministicRandom()->randomUniqueID();
		wait(lockDatabase(cx, uid) && poisson(lastLock, self->delayBetweenLocks));
		wait(unlockDatabase(cx, uid) && poisson(lastUnlock, self->delayBetweenLocks));
		return Void();
	}
};

WorkloadFactory<LockDatabaseFrequentlyWorkload> LockDatabaseFrequentlyWorkloadFactory;
