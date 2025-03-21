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
#include "fdbclient/RangeLock.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RandomRangeLockWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "RandomRangeLock";

	bool enabled;
	double maxLockDuration = 60.0;
	double maxStartDelay = 300.0;
	int lockActorCount = 10;

	RandomRangeLockWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {
		enabled = SERVER_KNOBS->ENABLE_READ_LOCK_ON_RANGE && !SERVER_KNOBS->ENABLE_VERSION_VECTOR &&
		          !SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST &&
		          !SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS;
		enabled &= (clientId == 0) && g_network->isSimulated();
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

	Standalone<StringRef> getRandomStringRef() const {
		int stringLength = deterministicRandom()->randomInt(1, 10);
		Standalone<StringRef> stringBuffer = makeString(stringLength);
		deterministicRandom()->randomBytes(mutateString(stringBuffer), stringLength);
		return stringBuffer;
	}

	KeyRange getRandomRange(RandomRangeLockWorkload* self) const {
		Standalone<StringRef> keyA = self->getRandomStringRef();
		Standalone<StringRef> keyB = self->getRandomStringRef();
		if (keyA < keyB) {
			return Standalone(KeyRangeRef(keyA, keyB));
		} else if (keyA > keyB) {
			return Standalone(KeyRangeRef(keyB, keyA));
		} else {
			return singleKeyRange(keyA);
		}
	}

	ACTOR Future<Void> lockActor(Database cx, RandomRangeLockWorkload* self, std::string rangeLockOwnerNamePrefix) {
		state double testDuration = deterministicRandom()->random01() * self->maxLockDuration;
		state double testStartDelay = deterministicRandom()->random01() * self->maxStartDelay;
		state std::string rangeLockOwnerName =
		    rangeLockOwnerNamePrefix + "-" + std::to_string(deterministicRandom()->randomInt(0, self->lockActorCount));
		// Here we intentionally introduced duplicated owner name between different lockActor
		std::string lockOwnerDescription = rangeLockOwnerName + ":" + self->getRandomStringRef().toString();
		wait(registerRangeLockOwner(cx, rangeLockOwnerName, lockOwnerDescription));
		wait(delay(testStartDelay));
		state KeyRange range = self->getRandomRange(self);
		TraceEvent(SevWarnAlways, "InjectRangeLockSubmit")
		    .detail("RangeLockOwnerName", rangeLockOwnerName)
		    .detail("Range", range)
		    .detail("LockStartDelayTime", testStartDelay)
		    .detail("LockTime", testDuration);
		try {
			Optional<RangeLockOwner> owner = wait(getRangeLockOwner(cx, rangeLockOwnerName));
			ASSERT(owner.present());
			ASSERT(owner.get().getOwnerUniqueId() == rangeLockOwnerName);
			wait(takeExclusiveReadLockOnRange(cx, range, rangeLockOwnerName));
			TraceEvent(SevWarnAlways, "InjectRangeLocked")
			    .detail("RangeLockOwnerName", rangeLockOwnerName)
			    .detail("Range", range)
			    .detail("LockTime", testDuration);
			ASSERT(range.end <= normalKeys.end);
		} catch (Error& e) {
			if (e.code() == error_code_range_lock_failed) {
				TraceEvent(SevWarnAlways, "InjectRangeLockFailed")
				    .detail("RangeLockOwnerName", rangeLockOwnerName)
				    .detail("Range", range)
				    .detail("LockTime", testDuration);
				ASSERT(range.end > normalKeys.end);
			} else if (e.code() == error_code_range_lock_reject) {
				TraceEvent(SevWarnAlways, "InjectRangeLockRejected")
				    .detail("RangeLockOwnerName", rangeLockOwnerName)
				    .detail("Range", range)
				    .detail("LockTime", testDuration);
				// pass
			} else {
				TraceEvent(SevError, "InjectRangeLockError")
				    .errorUnsuppressed(e)
				    .detail("RangeLockOwnerName", rangeLockOwnerName)
				    .detail("Range", range)
				    .detail("LockTime", testDuration);
				throw e;
			}
		}
		wait(delay(testDuration));

		TraceEvent(SevWarnAlways, "InjectRangeUnlockSubmit")
		    .detail("RangeLockOwnerName", rangeLockOwnerName)
		    .detail("Range", range)
		    .detail("LockStartDelayTime", testStartDelay)
		    .detail("LockTime", testDuration);
		try {
			wait(releaseExclusiveReadLockOnRange(cx, range, rangeLockOwnerName));
			TraceEvent(SevWarnAlways, "InjectRangeUnlocked")
			    .detail("RangeLockOwnerName", rangeLockOwnerName)
			    .detail("Range", range);
			ASSERT(range.end <= normalKeys.end);
		} catch (Error& e) {
			if (e.code() == error_code_range_lock_failed) {
				TraceEvent(SevWarnAlways, "InjectRangeUnlockFailed")
				    .detail("RangeLockOwnerName", rangeLockOwnerName)
				    .detail("Range", range)
				    .detail("LockTime", testDuration);
				ASSERT(range.end > normalKeys.end);
			} else if (e.code() == error_code_range_unlock_reject) {
				TraceEvent(SevWarnAlways, "InjectRangeUnlockRejected")
				    .detail("RangeLockOwnerName", rangeLockOwnerName)
				    .detail("Range", range)
				    .detail("LockTime", testDuration);
				// pass
			} else {
				TraceEvent(SevError, "InjectRangeUnlockError")
				    .errorUnsuppressed(e)
				    .detail("RangeLockOwnerName", rangeLockOwnerName)
				    .detail("Range", range)
				    .detail("LockTime", testDuration);
				throw e;
			}
		}

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, RandomRangeLockWorkload* self) {
		if (self->enabled) {
			// Run lockActorCount number of actor concurrently.
			// Each actor conducts (1) locking a range for a while and (2) unlocking the range.
			// Each actor randomly generate a uniqueId as the lock owner.
			// It is possible that different actors have the same lock owner.
			// The range to be locked is randomly generated.
			// It is possible that different actors have overlapped range to lock.
			// The rangeLock mechanism should approperiately handled those conflict.
			// When all actors complete, it is expected that all locks are removed,
			// and this injected workload should not block other workloads.
			state std::string rangeLockOwnerNamePrefix = "Owner" + std::to_string(self->clientId);
			std::vector<Future<Void>> actors;
			for (int i = 0; i < self->lockActorCount; i++) {
				actors.push_back(self->lockActor(cx, self, rangeLockOwnerNamePrefix));
			}
			wait(waitForAll(actors));

			// Make sure all ranges locked by the workload client are unlocked
			state int j = 0;
			for (; j < self->lockActorCount; j++) {
				std::vector<std::pair<KeyRange, RangeLockState>> res = wait(
				    findExclusiveReadLockOnRange(cx, normalKeys, rangeLockOwnerNamePrefix + "-" + std::to_string(j)));
				ASSERT(res.empty());
			}

			TraceEvent("RandomRangeLockWorkloadEnd").detail("OwnerPrefix", rangeLockOwnerNamePrefix);
		}
		return Void();
	}
};

FailureInjectorFactory<RandomRangeLockWorkload> RangeLockFailureInjectionFactory;
WorkloadFactory<RandomRangeLockWorkload> RandomRangeLockWorkloadFactory;
