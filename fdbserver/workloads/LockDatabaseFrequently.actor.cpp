/*
 * LockDatabaseFrequently.actor.cpp
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
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct LockDatabaseFrequentlyWorkload : TestWorkload {
	double delayBetweenLocks;
	double testDuration;

	LockDatabaseFrequentlyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		delayBetweenLocks = getOption(options, LiteralStringRef("delayBetweenLocks"), 0.1);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60);
	}

	std::string description() override { return "LockDatabaseFrequently"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId == 0 ? worker(this, cx) : Void(); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> worker(LockDatabaseFrequentlyWorkload* self, Database cx) {
		state Future<Void> end = delay(self->testDuration);
		loop {
			wait(lockAndUnlock(self, cx));
			if (end.isReady()) {
				return Void();
			}
		}
	}

	ACTOR static Future<Void> lockAndUnlock(LockDatabaseFrequentlyWorkload* self, Database cx) {
		state UID uid = deterministicRandom()->randomUniqueID();
		wait(lockDatabase(cx, uid) && success(delay(self->delayBetweenLocks)));
		wait(unlockDatabase(cx, uid) && success(delay(self->delayBetweenLocks)));
		return Void();
	}
};

WorkloadFactory<LockDatabaseFrequentlyWorkload> LockDatabaseFrequentlyWorkloadFactory("LockDatabaseFrequently");
