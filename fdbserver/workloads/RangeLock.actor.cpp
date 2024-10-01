/*
 * RangeLock.actor.cpp
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

#include "fdbclient/RangeLock.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RangeLocking : TestWorkload {
	static constexpr auto NAME = "RangeLocking";
	const bool enabled;
	bool pass;

	RangeLocking(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> setKey(Database cx, Key key, Value value) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.set(key, value);
				wait(tr.commit());
				TraceEvent("RangeLockWorkLoadSetKey").detail("Key", key).detail("Value", value);
				return Void();
			} catch (Error& e) {
				TraceEvent("RangeLockWorkLoadSetKeyError")
				    .errorUnsuppressed(e)
				    .detail("Key", key)
				    .detail("Value", value);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> clearKey(Database cx, Key key) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(key);
				wait(tr.commit());
				TraceEvent("RangeLockWorkLoadClearKey").detail("Key", key);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Optional<Value>> getKey(Database cx, Key key) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = wait(tr.get(key));
				TraceEvent("RangeLockWorkLoadGetKey")
				    .detail("Key", key)
				    .detail("Value", value.present() ? value.get() : Value());
				return value;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> _start(RangeLocking* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		state Key keyUpdate = "11"_sr;
		state KeyRange rangeLock = KeyRangeRef("1"_sr, "2"_sr);
		state Optional<Value> value;
		state std::vector<RangeLockState> rangeLockStates;

		wait(self->setKey(cx, keyUpdate, "1"_sr));

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(value.present() && value.get() == "1"_sr);

		wait(self->clearKey(cx, keyUpdate));

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(!value.present());

		wait(lockUserRange(cx, rangeLock));
		TraceEvent("RangeLockWorkLoadLockRange").detail("Range", rangeLock);

		wait(store(rangeLockStates, getUserRangeLockStates(cx, normalKeys)));
		TraceEvent("RangeLockWorkLoadGetLockedRange")
		    .detail("Range", rangeLock)
		    .detail("LockState", describe(rangeLockStates));

		try {
			wait(self->setKey(cx, keyUpdate, "2"_sr));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_transaction_rejected_range_locked);
		}

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(!value.present());

		wait(unLockUserRange(cx, rangeLock));
		TraceEvent("RangeLockWorkLoadUnlockRange").detail("Range", rangeLock);

		rangeLockStates.clear();
		wait(store(rangeLockStates, getUserRangeLockStates(cx, normalKeys)));
		TraceEvent("RangeLockWorkLoadGetLockedRange")
		    .detail("Range", rangeLock)
		    .detail("LockState", describe(rangeLockStates));

		wait(self->setKey(cx, keyUpdate, "3"_sr));

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(value.present() && value.get() == "3"_sr);

		return Void();
	}
};

WorkloadFactory<RangeLocking> RangeLockingFactory;
