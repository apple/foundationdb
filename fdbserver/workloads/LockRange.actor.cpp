/*
 * LockRange.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/LockRange.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

struct LockRangeWorkload : TestWorkload {
	double lockAfter, unlockAfter;
	Key keyPrefix;
	KeyRange range;
	bool ok = true;
	bool writeLocked = true;
	UID uid;

	LockRangeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		lockAfter = getOption(options, "lockAfter"_sr, 0.0);
		unlockAfter = getOption(options, "unlockAfter"_sr, 10.0);
		ASSERT(unlockAfter > lockAfter);
		keyPrefix = getOption(options, "prefix"_sr, "LR_"_sr);
		Key keyEnd = endOfRange(keyPrefix);
		range = KeyRangeRef(keyPrefix, keyEnd);
		uid = deterministicRandom()->randomUniqueID();
		writeLocked = deterministicRandom()->coinflip();
		if (clientId == 0) {
			std::cout << "LockRange workload range = " << printable(range) << "\n";
		}
	}

	virtual std::string description() { return "LockRange"; }

	virtual Future<Void> setup(Database const& cx) { return Void(); }

	virtual Future<Void> start(Database const& cx) {
		if (clientId == 0) return _start(cx, this);
		return Void();
	}

	virtual Future<bool> check(Database const& cx) {
		return ok;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}

	static Key endOfRange(Key startOfRange) {
		int n = startOfRange.size();
		Key result = makeString(n);
		uint8_t* data = mutateString(result);
		uint8_t* src = mutateString(startOfRange);
		memcpy(data, src, n);
		data[n - 1] += 1;
		return result;
	}

	// Writes to the locked range should be blocked.
	ACTOR static Future<Void> checkLocked(Database cx, LockRangeWorkload* self) {
		loop {
			state Transaction tr(cx);
			state bool reset = false;
			try {
				Version ver = wait(tr.getReadVersion());

				std::string str = deterministicRandom()->randomAlphaNumeric(30);
				Key key = self->range.begin.withSuffix(StringRef(str));
				Key endKey = key.withSuffix("_more"_sr);

				ASSERT(self->range.begin < key && self->range.end > key);
				state int choice = deterministicRandom()->randomInt(0, 5);
				if (choice == 0) {
					// SET
					TraceEvent("LockRangeWorkloadSet", self->uid).detail("Key", key).detail("GRV", ver);
					tr.set(key, "setxxx"_sr);
				} else if (choice == 1) {
					// GET
					TraceEvent("LockRangeWorkloadGet", self->uid).detail("Key", key).detail("GRV", ver);
					Optional<Value> v = wait(tr.get(key));
				} else if (choice == 2) {
					// GET_RANGE
					TraceEvent("LockRangeWorkloadGetRange", self->uid).detail("Key", key).detail("GRV", ver);
					Standalone<RangeResultRef> r = wait(tr.getRange(KeyRangeRef(key, endKey), 10));
				} else if (choice == 3) {
					// CLEAR
					TraceEvent("LockRangeWorkloadClear", self->uid).detail("Key", key).detail("GRV", ver);
					tr.clear(KeyRangeRef(key, endKey));
				} else if (choice == 4) {
					// ATOMIC OP
					TraceEvent("LockRangeWorkloadAtomicOp", self->uid).detail("Key", key).detail("GRV", ver);
					uint64_t value = 1000;
					Value val = StringRef((const uint8_t*)&value, sizeof(uint64_t));
					tr.atomicOp(key, val, MutationRef::AddValue);
				}

				wait(tr.commit());
				Version commitVer = tr.getCommittedVersion();
				if (self->writeLocked || (choice == 0 || choice == 3 || choice == 4)) {
					self->ok = false;
					TraceEvent(SevError, "LockRangeWorkloadAccessToLockedRange", self->uid).detail("CommittedVersion", commitVer);
					return Void();
				}
			} catch (Error& e) {
				if (e.code() == error_code_range_locks_access_denied) {
					TEST(true); // The range confirmed locked
					tr.reset();
					reset = true;
				} else {
					wait(tr.onError(e));
				}
			}
			if (reset) wait(delay(0.01));
		}
	}

	ACTOR static Future<Void> _start(Database cx, LockRangeWorkload* self) {
		wait(delay(self->lockAfter));

		const LockMode mode = self->writeLocked ? LockMode::LOCK_EXCLUSIVE : LockMode::LOCK_READ_SHARED;
		wait(lockRange(cx, LockRequest(self->range, mode)));
		TraceEvent("LockRangeWorkload", self->uid).detail("LockedRange", self->range.toString());

		state Future<Void> checker = checkLocked(cx, self);
		wait(delay(self->unlockAfter - self->lockAfter));
		checker.cancel();

		const LockMode unlockMode = self->writeLocked ? LockMode::UNLOCK_EXCLUSIVE : LockMode::UNLOCK_READ_SHARED;
		wait(lockRange(cx, LockRequest(self->range, unlockMode)));
		TraceEvent("LockRangeWorkload", self->uid).detail("UnlockedRange", self->range.toString());

		return Void();
	}
};

WorkloadFactory<LockRangeWorkload> LockRangeWorkloadFactory("LockRange");
