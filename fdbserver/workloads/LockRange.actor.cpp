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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

struct LockRangeWorkload : TestWorkload {
	double lockAfter, unlockAfter;
	Key keyPrefix;
	KeyRange range;
	bool ok = true;

	LockRangeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		lockAfter = getOption(options, LiteralStringRef("lockAfter"), 0.0);
		unlockAfter = getOption(options, LiteralStringRef("unlockAfter"), 10.0);
		ASSERT(unlockAfter > lockAfter);
		keyPrefix = getOption(options, LiteralStringRef("prefix"), LiteralStringRef("LR_"));
		Key keyEnd = endOfRange(keyPrefix);
		range = KeyRangeRef(keyPrefix, keyEnd);
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

	ACTOR static Future<Standalone<RangeResultRef>> lockAndSave(Database cx, LockRangeWorkload* self) {
		state Transaction tr(cx);
		loop {
			try {
				wait(lockRange(&tr, self->range));
				state Standalone<RangeResultRef> data = wait(tr.getRange(self->range, 50000));
				ASSERT(!data.more);
				wait(tr.commit());
				return data;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> unlockAndCheck(Database cx, LockRangeWorkload* self, UID lockID,
	                                         Standalone<RangeResultRef> data) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				// Optional<Value> val = wait( tr.get(databaseLockedKey) );
				// if(!val.present())
				//	return Void();

				wait(unlockRange(&tr, self->range));
				state Standalone<RangeResultRef> data2 = wait(tr.getRange(self->range, 50000));
				if (data.size() != data2.size()) {
					TraceEvent(SevError, "DataChangedWhileLocked")
					    .detail("BeforeSize", data.size())
					    .detail("AfterSize", data2.size());
					self->ok = false;
				} else if (data != data2) {
					TraceEvent(SevError, "DataChangedWhileLocked").detail("Size", data.size());
					for (int i = 0; i < data.size(); i++) {
						if (data[i] != data2[i]) {
							TraceEvent(SevError, "DataChangedWhileLocked")
							    .detail("I", i)
							    .detail("Before", printable(data[i]))
							    .detail("After", printable(data2[i]));
						}
					}
					self->ok = false;
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Writes to the locked range should be blocked.
	ACTOR static Future<Void> checkLocked(Database cx, LockRangeWorkload* self) {
		state Transaction tr(cx);
		loop {
			try {
				// TODO: write to the range
				wait(Never());
				self->ok = false;
				return Void();
			} catch (Error& e) {
				TEST(e.code() == error_code_database_locked); // Database confirmed locked
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(Database cx, LockRangeWorkload* self) {
		state UID lockID = deterministicRandom()->randomUniqueID();
		wait(delay(self->lockAfter));
		state Standalone<RangeResultRef> data = wait(lockAndSave(cx, self));
		state Future<Void> checker = checkLocked(cx, self);
		wait(delay(self->unlockAfter - self->lockAfter));
		checker.cancel();
		wait(unlockAndCheck(cx, self, lockID, data));

		// TODO: verify when database is locked, can't lock/unlock ranges.

		return Void();
	}
};

WorkloadFactory<LockRangeWorkload> LockRangeWorkloadFactory("LockRange");
