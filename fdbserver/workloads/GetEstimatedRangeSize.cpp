/*
 * GetEstimatedRangeSize.cpp
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

#include <cstring>

#include "fdbrpc/simulator.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"

#include "flow/Error.h"
#include "flow/Trace.h"

struct GetEstimatedRangeSizeWorkload : TestWorkload {
	static constexpr auto NAME = "GetEstimatedRangeSize";
	int nodeCount;
	double testDuration;
	Key keyPrefix;
	bool checkOnly;

	explicit GetEstimatedRangeSizeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		nodeCount = getOption(options, "nodeCount"_sr, 10000);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		checkOnly = getOption(options, "checkOnly"_sr, false);
	}

	Future<Void> setup(Database const& cx) override {
		if (checkOnly) {
			return Void();
		}
		return bulkSetup(cx,
		                 this,
		                 nodeCount,
		                 Promise<double>(),
		                 /*valuesInconsequential=*/true,
		                 /*postSetupWarming=*/0.0,
		                 /*maxKeyInsertRate=*/1e12,
		                 /*insertionCountsToMeasure=*/std::vector<uint64_t>(),
		                 /*ratesAtKeyCounts=*/Promise<std::vector<std::pair<uint64_t, double>>>(),
		                 /*keySaveIncrement=*/0,
		                 /*keyCheckInterval=*/0.1,
		                 /*startNodeIndex=*/0,
		                 /*endNodeIdx=*/0);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId > 0) {
			return Void();
		}
		return checkSize(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }
	int fromValue(const ValueRef& v) { return testKeyToDouble(v, keyPrefix); }
	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }

	Future<Void> checkSize(Database cx) {
		int64_t size = co_await getSize(cx);
		ASSERT(sizeIsAsExpected(size));
	}

	bool sizeIsAsExpected(int64_t size) {
		int nodeSize = key(0).size() + value(0).size();
		// We use a wide range to avoid flakiness because the underlying function
		// is making an estimation.
		return size > nodeCount * nodeSize / 2 && size < nodeCount * nodeSize * 5;
	}

	Future<int64_t> getSize(Database cx) {
		ReadYourWritesTransaction tr(cx);
		double totalDelay = 0.0;

		while (true) {
			Error err;
			try {
				int64_t size = co_await tr.getEstimatedRangeSizeBytes(normalKeys);
				TraceEvent(SevDebug, "GetSizeResult").detail("Size", size);
				if (!sizeIsAsExpected(size) && totalDelay < 300.0) {
					totalDelay += 5.0;
					co_await delay(5.0);
					continue;
				} else {
					co_return size;
				}
			} catch (Error& e) {
				err = e;
			}
			TraceEvent(SevDebug, "GetSizeError").errorUnsuppressed(err);
			co_await tr.onError(err);
		}
	}
};

WorkloadFactory<GetEstimatedRangeSizeWorkload> GetEstimatedRangeSizeWorkloadFactory;
