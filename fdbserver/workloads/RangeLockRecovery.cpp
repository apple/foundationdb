/*
 * RangeLockRecovery.cpp
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
#include "fdbclient/RangeLock.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"

class RangeLockRecovery : public TestWorkload {
public:
	static constexpr auto NAME = "RangeLockRecovery";

	explicit RangeLockRecovery(WorkloadContext const& wcx)
	  : TestWorkload(wcx), phase(getOption(options, "phase"_sr, "prepare"_sr).toString()) {
		ASSERT(phase == "prepare" || phase == "verify");
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0 || phase != "prepare") {
			co_return;
		}
		ASSERT(SERVER_KNOBS->ENABLE_READ_LOCK_ON_RANGE);
		co_await registerRangeLockOwner(cx, ownerName, "Lock-held recovery regression");
		co_await write(cx, "before"_sr, false);
		co_await takeExclusiveReadLockOnRange(cx, protectedRange, ownerName);
		co_await write(cx, "rejected-before-restart"_sr, true);
		TraceEvent("RangeLockRecoveryPrepared").detail("Range", protectedRange);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0 || phase != "verify") {
			co_return;
		}
		ASSERT(!SERVER_KNOBS->ENABLE_READ_LOCK_ON_RANGE || SERVER_KNOBS->ENABLE_VERSION_VECTOR ||
		       SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST);
		const auto locks = co_await findExclusiveReadLockOnRange(cx, protectedRange, ownerName);
		ASSERT_EQ(locks.size(), 1);
		ASSERT(locks.front().first == protectedRange);
		co_await write(cx, "rejected-after-restart"_sr, true);
		ASSERT(co_await read(cx) == Optional<Value>(Value("before"_sr)));

		bool admissionRejected = false;
		try {
			co_await takeExclusiveReadLockOnRange(cx, unusedRange, ownerName);
		} catch (Error& e) {
			if (e.code() != error_code_range_lock_not_ready) {
				throw;
			}
			admissionRejected = true;
		}
		ASSERT(admissionRejected);

		// Disabling admission must never strand an existing acquisition.
		co_await releaseExclusiveReadLockOnRange(cx, protectedRange, ownerName);
		co_await write(cx, "after"_sr, false);
		ASSERT(co_await read(cx) == Optional<Value>(Value("after"_sr)));
		co_await removeRangeLockOwner(cx, ownerName);
		TraceEvent("RangeLockRecoveryPassed")
		    .detail("AdmissionKnob", SERVER_KNOBS->ENABLE_READ_LOCK_ON_RANGE)
		    .detail("VersionVector", SERVER_KNOBS->ENABLE_VERSION_VECTOR)
		    .detail("TLogUnicast", SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST);
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}

private:
	const std::string phase;
	const std::string ownerName = "RangeLockRecovery";
	const Key key = "rangeLockRecovery/held/key"_sr;
	const KeyRange protectedRange = KeyRangeRef("rangeLockRecovery/held/"_sr, "rangeLockRecovery/held0"_sr);
	const KeyRange unusedRange = KeyRangeRef("rangeLockRecovery/new/"_sr, "rangeLockRecovery/new0"_sr);

	Future<Void> write(Database cx, Value value, bool expectRejection) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.set(key, value);
				co_await tr.commit();
				ASSERT(!expectRejection);
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_transaction_rejected_range_locked) {
					ASSERT(expectRejection);
					co_return;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Optional<Value>> read(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				co_return co_await tr.get(key);
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}
};

WorkloadFactory<RangeLockRecovery> RangeLockRecoveryFactory;
