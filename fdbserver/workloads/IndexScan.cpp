/*
 * IndexScan.cpp
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
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"
#include "fdbclient/ReadYourWrites.h"

struct IndexScanWorkload : KVWorkload {
	constexpr static auto NAME = "IndexScan";
	uint64_t rowsRead, chunks;
	int bytesPerRead, failedTransactions, scans;
	double totalTimeFetching, testDuration, transactionDuration;
	bool singleProcess, readYourWrites;

	explicit IndexScanWorkload(WorkloadContext const& wcx)
	  : KVWorkload(wcx), rowsRead(0), chunks(0), failedTransactions(0), scans(0) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		bytesPerRead = getOption(options, "bytesPerRead"_sr, 80000);
		transactionDuration = getOption(options, "transactionDuration"_sr, 1.0);
		singleProcess = getOption(options, "singleProcess"_sr, true);
		readYourWrites = getOption(options, "readYourWrites"_sr, true);
	}

	Future<Void> setup(Database const& cx) override {
		// this will be set up by and external force!
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (singleProcess && clientId != 0) {
			return Void();
		}
		return _start(cx);
	}

	Future<bool> check(const Database&) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (singleProcess && clientId != 0)
			return;

		m.emplace_back("FailedTransactions", failedTransactions, Averaged::False);
		m.emplace_back("RowsRead", rowsRead, Averaged::False);
		m.emplace_back("Scans", scans, Averaged::False);
		m.emplace_back("Chunks", chunks, Averaged::False);
		m.emplace_back("TimeFetching", totalTimeFetching, Averaged::True);
		m.emplace_back("Rows/sec", totalTimeFetching == 0 ? 0 : rowsRead / totalTimeFetching, Averaged::True);
		m.emplace_back("Rows/chunk", chunks == 0 ? 0 : rowsRead / (double)chunks, Averaged::True);
	}

	Future<Void> _start(Database cx) {
		// Boilerplate: "warm" the location cache so that the location of all keys is known before test starts
		double startTime = now();
		while (true) {
			Transaction tr(cx);
			Error err;
			try {
				co_await tr.warmRange(allKeys);
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		// Wait some small amount of time for things to "settle". Maybe this is historical?
		co_await delay(std::max(0.1, 1.0 - (now() - startTime)));

		co_await timeout(serialScans(cx), testDuration, Void());
	}

	Future<Void> serialScans(Database cx) {
		double start = now();
		try {
			while (true) {
				co_await scanDatabase(cx);
			}
		} catch (...) {
			totalTimeFetching = now() - start;
			throw;
		}
	}

	Future<Void> scanDatabase(Database cx) {
		int startNode = deterministicRandom()->randomInt(0, nodeCount / 2); // start in the first half of the database
		KeySelector begin = firstGreaterOrEqual(keyForIndex(startNode));
		KeySelector end = firstGreaterThan(keyForIndex(nodeCount));
		GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED, bytesPerRead);

		int rowsRead{ 0 };
		int chunks{ 0 };
		double startTime{ 0 };
		while (true) {
			ReadYourWritesTransaction tr(cx);
			if (!readYourWrites)
				tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
			startTime = now();
			rowsRead = 0;
			chunks = 0;

			Error err;
			try {
				while (true) {
					RangeResult r = co_await tr.getRange(begin, end, limits);
					chunks++;
					rowsRead += r.size();
					if (r.empty() || !r.more || (now() - startTime) > transactionDuration) {
						break;
					}
					begin = firstGreaterThan(r[r.size() - 1].key);
				}

				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() != error_code_actor_cancelled)
				++failedTransactions;
			co_await tr.onError(err);
		}

		this->rowsRead += rowsRead;
		this->chunks += chunks;
		this->scans++;
	}
};

WorkloadFactory<IndexScanWorkload> IndexScanWorkloadFactory;
