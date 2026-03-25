/*
 * LockDatabaseFrequently.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbclient/ManagementAPI.h"

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

	Future<Void> start(Database const& cx) override { return clientId == 0 ? worker(cx) : Void(); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId == 0) {
			m.push_back(lockCount.getMetric());
		}
	}

	Future<Void> worker(Database cx) {
		Future<Void> end = delay(testDuration);
		double lastLock = g_network->now();
		double lastUnlock = g_network->now() + delayBetweenLocks / 2;
		while (true) {
			co_await lockAndUnlock(cx, &lastLock, &lastUnlock);
			++lockCount;
			if (end.isReady()) {
				co_return;
			}
		}
	}

	Future<Void> lockAndUnlock(Database cx, double* lastLock, double* lastUnlock) {
		UID uid = deterministicRandom()->randomUniqueID();
		co_await (lockDatabase(cx, uid) && poisson(lastLock, delayBetweenLocks));
		co_await (unlockDatabase(cx, uid) && poisson(lastUnlock, delayBetweenLocks));
	}
};

WorkloadFactory<LockDatabaseFrequentlyWorkload> LockDatabaseFrequentlyWorkloadFactory;
