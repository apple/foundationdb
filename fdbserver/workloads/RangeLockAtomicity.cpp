/*
 * RangeLockAtomicity.cpp
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

#include "fdbclient/RangeLock.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"

class RangeLockAtomicity : public TestWorkload {
public:
	static constexpr auto NAME = "RangeLockAtomicity";

	explicit RangeLockAtomicity(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			co_await registerRangeLockOwner(cx, ownerName, "Range-lock transaction atomicity test");
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}

		co_await writeWithMetadataVersion(cx, "before"_sr, false);
		const Optional<Value> metadataBefore = co_await readValue(cx, metadataVersionKey);
		ASSERT(metadataBefore.present());
		ASSERT(co_await readStoredMetadataVersion(cx) == metadataBefore);

		co_await takeExclusiveReadLockOnRange(cx, singleKeyRange(lockedKey), ownerName);
		co_await writeWithMetadataVersion(cx, "rejected"_sr, true);

		// A later durable commit distinguishes an aborted metadata update from
		// an update whose original client received its error before log push.
		co_await commitBarrier(cx);
		ASSERT(co_await readValue(cx, metadataVersionKey) == metadataBefore);
		ASSERT(co_await readStoredMetadataVersion(cx) == metadataBefore);
		const Optional<Value> lockedValue = co_await readValue(cx, lockedKey);
		ASSERT(lockedValue.present() && lockedValue.get() == "before"_sr);

		co_await releaseExclusiveReadLockOnRange(cx, singleKeyRange(lockedKey), ownerName);
		co_await writeWithMetadataVersion(cx, "after"_sr, false);
		const Optional<Value> metadataAfter = co_await readValue(cx, metadataVersionKey);
		ASSERT(metadataAfter.present() && metadataAfter != metadataBefore);
		ASSERT(co_await readStoredMetadataVersion(cx) == metadataAfter);
		const Optional<Value> finalValue = co_await readValue(cx, lockedKey);
		ASSERT(finalValue.present() && finalValue.get() == "after"_sr);
		TraceEvent("RangeLockAtomicityPassed")
		    .detail("ResolverPrivateMutations", SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS);
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}

private:
	const std::string ownerName = "RangeLockAtomicity";
	const Key lockedKey = "rangeLockAtomicity/locked"_sr;
	const Key barrierKey = "rangeLockAtomicity/barrier"_sr;

	Future<Void> writeWithMetadataVersion(Database cx, Value value, bool expectRangeLockRejection) {
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::REPORT_CONFLICTING_KEYS);
				tr.atomicOp(metadataVersionKey, metadataVersionRequiredValue, MutationRef::SetVersionstampedValue);
				tr.set(lockedKey, value);
				co_await tr.commit();
				ASSERT(!expectRangeLockRejection);
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_transaction_rejected_range_locked) {
					ASSERT(expectRangeLockRejection);
					co_return;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> commitBarrier(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.set(barrierKey, "durable"_sr);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Optional<Value>> readValue(Database cx, Key key) {
		ReadYourWritesTransaction tr(cx);
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

	Future<Optional<Value>> readStoredMetadataVersion(Database cx) {
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult result = co_await tr.getRange(singleKeyRange(metadataVersionKey), 1);
				co_return result.empty() ? Optional<Value>() : Optional<Value>(Value(result.front().value));
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}
};

WorkloadFactory<RangeLockAtomicity> RangeLockAtomicityFactory;
