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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>

struct RandomRangeLockWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "RandomRangeLock";

	bool enabled;
	double maxLockDuration = 60.0;
	double maxStartDelay = 300.0;
	double testDuration = deterministicRandom()->random01() * maxLockDuration;
	double testStartDelay = deterministicRandom()->random01() * maxStartDelay;

	RandomRangeLockWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {
		enabled = (clientId == 0) && g_network->isSimulated();
	}

	RandomRangeLockWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		enabled = (clientId == 0) && g_network->isSimulated();
		maxLockDuration = getOption(options, "maxLockDuration"_sr, maxLockDuration);
		maxStartDelay = getOption(options, "maxStartDelay"_sr, maxStartDelay);
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
		int KeyALength = deterministicRandom()->randomInt(1, 10);
		Standalone<StringRef> keyA = makeString(KeyALength);
		deterministicRandom()->randomBytes(mutateString(keyA), KeyALength);
		int KeyBLength = deterministicRandom()->randomInt(1, 10);
		Standalone<StringRef> keyB = makeString(KeyBLength);
		deterministicRandom()->randomBytes(mutateString(keyB), KeyBLength);
		if (keyA < keyB) {
			return Standalone(KeyRangeRef(keyA, keyB));
		} else if (keyA > keyB) {
			return Standalone(KeyRangeRef(keyB, keyA));
		} else {
			return singleKeyRange(keyA);
		}
	}

	ACTOR Future<Void> _start(Database cx, RandomRangeLockWorkload* self) {
		if (self->enabled) {
			wait(delay(self->testStartDelay));
			state KeyRange range = self->getRandomRange();
			TraceEvent(SevWarnAlways, "InjectRangeLockSubmit").detail("Range", range);
			try {
				wait(lockCommitUserRange(cx, range));
				TraceEvent(SevWarnAlways, "InjectRangeLocked")
				    .detail("Range", range)
				    .detail("LockTime", self->testDuration);
				ASSERT(range.end <= normalKeys.end);
			} catch (Error& e) {
				if (e.code() != error_code_range_lock_failed) {
					throw e;
				} else {
					ASSERT(range.end > normalKeys.end);
				}
			}
			wait(delay(self->testDuration));
			try {
				wait(unlockCommitUserRange(cx, range));
				TraceEvent(SevWarnAlways, "InjectRangeUnlocked").detail("Range", range);
				ASSERT(range.end <= normalKeys.end);
			} catch (Error& e) {
				if (e.code() != error_code_range_lock_failed) {
					throw e;
				} else {
					ASSERT(range.end > normalKeys.end);
				}
			}
		}
		return Void();
	}
};

FailureInjectorFactory<RandomRangeLockWorkload> RangeLockFailureInjectionFactory;
WorkloadFactory<RandomRangeLockWorkload> RandomRangeLockWorkloadFactory;
