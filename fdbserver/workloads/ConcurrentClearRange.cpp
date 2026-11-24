/*
 * ConcurrentClearRange.cpp
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

#include "fdbclient/NativeAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"
#include "BulkSetup.h"
#include "YCSBCommon.h"

// Loads a second copy of a YCSB dataset under its own nodePrefix, then commits a single clear of the whole copy, so a
// concurrent YCSB workload on another nodePrefix shows how the storage engine absorbs a large clear. With clear=false
// it only loads the copy, so the load can happen in an earlier test than the clear.
struct ConcurrentClearRangeWorkload : YCSBBase {
	static constexpr auto NAME = "ConcurrentClearRange";

	double clearDelay{ 0.0 };
	bool clear{ true };

	double clearLatency{ 0.0 };
	int clearRetries{ 0 };

	explicit ConcurrentClearRangeWorkload(WorkloadContext const& wcx) : YCSBBase(wcx) {
		clearDelay = getOption(options, "clearDelay"_sr, 10.0);
		clear = getOption(options, "clear"_sr, true);
		ASSERT(clearDelay >= 0);
		// Without a prefix the clear would cover every other dataset too
		ASSERT(nodePrefix > 0);
	}

	// All of this dataset's keys, and only them: every key starts with the 16 hex digit nodePrefix
	KeyRange datasetRange() const { return prefixRange(rowKey(0).substr(0, 16)); }

	Future<Void> setup(Database const& cx) override {
		Promise<double> setupTime;
		co_await bulkSetup(cx, this, kvCount(), setupTime);
	}

	Future<Void> start(Database const& cx) override {
		CODE_PROBE(!clear, "ConcurrentClearRange load only");
		if (clientId != 0 || !clear) {
			co_return;
		}
		co_await delay(clearDelay);

		KeyRange const range = datasetRange();
		TraceEvent("ConcurrentClearRangeBegin").detail("Range", range).detail("KVs", kvCount());
		double const startTime = timer_monotonic();
		int attempts = 0;
		co_await runRYWTransactionVoid(cx, [&attempts, range](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			++attempts;
			tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
			tr->clear(range);
			return Void();
		});
		clearRetries = attempts - 1;
		clearLatency = timer_monotonic() - startTime;
		CODE_PROBE(clearRetries > 0, "ConcurrentClearRange retried clear");
		TraceEvent("ConcurrentClearRangeCommitted")
		    .detail("Range", range)
		    .detail("Latency", clearLatency)
		    .detail("Retries", clearRetries);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0 || !clear) {
			co_return true;
		}
		KeyRange const range = datasetRange();
		RangeResult const res = co_await runRYWTransaction(
		    cx, [range](Reference<ReadYourWritesTransaction> tr) { return tr->getRange(range, 1); });
		if (!res.empty()) {
			TraceEvent(SevError, "ConcurrentClearRangeNotEmpty").detail("Range", range).detail("Key", res[0].key);
		}
		co_return res.empty();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId != 0 || !clear) {
			return;
		}
		m.emplace_back("Clear Latency (ms)", 1000 * clearLatency, Averaged::False);
		m.emplace_back("Clear Retries", clearRetries, Averaged::False);
	}
};

WorkloadFactory<ConcurrentClearRangeWorkload> ConcurrentClearRangeWorkloadFactory;
