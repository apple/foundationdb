/*
 * YCSB.cpp
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

// YCSB against a cluster: a fixed mix of row reads and single-field updates over a pre-loaded dataset, with overall
// and per-window latency distributions. Rows are laid out as one KV (singleCF) or one KV per field (multiCF); see
// YCSBCommon.h.

#include <optional>
#include <vector>

#include "fdbclient/NativeAPI.h"
#include "fdbrpc/DDSketch.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"
#include "flow/DeterministicRandom.h"
#include "BulkSetup.h"
#include "YCSBCommon.h"

struct YCSBWorkload : YCSBBase {
	static constexpr auto NAME = "YCSB";

	double testDuration{ 0.0 };
	int targetTps{ 0 }; // 0 = unlimited
	int totalThreads{ 0 }; // across all clients
	int myThreads{ 0 };
	bool randomInsertionOrder{ false };

	// Maps the distribution's key index to a row, so with insertionOrder=random the hot keys of a zipfian
	// distribution are spread over the keyspace instead of clustered at its start
	std::vector<uint64_t> insertionOrder;
	std::optional<YCSBZipfianGenerator> zipfian;

	PerfIntCounter operations{ "operations" };
	DDSketch<double> latencies, readLatencies, updateLatencies;

	explicit YCSBWorkload(WorkloadContext const& wcx) : YCSBBase(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0); // 0 = load only
		ASSERT(testDuration >= 0);
		totalThreads = getOption(options, "threads"_sr, 1);
		targetTps = getOption(options, "tps"_sr, 10);
		ASSERT(targetTps >= 0);
		std::string const insertionOrderStr = getOption(options, "insertionOrder"_sr, "random"_sr).toString();
		ASSERT(insertionOrderStr == "sequential" || insertionOrderStr == "random");
		randomInsertionOrder = insertionOrderStr == "random";
		if (distribution == YCSB_ZIPFIAN) {
			zipfian.emplace(0, nodeCount - 1, zipfConstant);
		}
	}

	uint64_t nextKeyIdx() const {
		if (distribution == YCSB_ZIPFIAN) {
			return zipfian->next();
		}
		return deterministicRandom()->randomInt64(0, nodeCount);
	}

	Future<Void> setup(Database const& cx) override {
		Promise<double> setupTime;
		co_await bulkSetup(cx, this, kvCount(), setupTime);
	}

	Future<Void> start(Database const& cx) override {
		CODE_PROBE(testDuration == 0, "YCSB load only");
		if (testDuration == 0) {
			co_return;
		}
		TraceEvent("YCSBWorkloadGenInsertionOrder").log();
		insertionOrder.reserve(nodeCount);
		for (uint64_t i = 0; i < nodeCount; i++) {
			insertionOrder.push_back(i);
			if (i % 10000 == 0) {
				co_await yield(TaskPriority::Low);
			}
		}
		if (randomInsertionOrder) {
			// Shuffled with the shared seed so every client maps key indexes to the same rows
			Reference<IRandom> sharedRandom = makeReference<DeterministicRandom>(sharedRandomNumber);
			for (uint64_t i = 0; i < nodeCount; i++) {
				std::swap(insertionOrder[i], insertionOrder[sharedRandom->randomInt64(i, nodeCount)]);
				if (i % 1000 == 0) {
					co_await yield(TaskPriority::Low);
				}
			}
		}
		co_await yield(TaskPriority::DefaultYield);

		UID const windowId = deterministicRandom()->randomUniqueID();
		TraceEvent("YCSBWorkloadStart").detail("WindowID", windowId).detail("ClientId", clientId);
		startLatencyWindows(NAME, windowId);
		std::vector<Future<Void>> workers;
		for (int i = clientId; i < totalThreads; i += clientCount) {
			workers.push_back(runWorker(cx));
		}
		myThreads = workers.size();

		double const startWait = now();
		co_await timeout(waitForAll(workers), testDuration, Void());
		testDuration = now() - startWait;
		stopLatencyWindows();
		TraceEvent("YCSBWorkloadEnd").detail("TestDuration", testDuration);
	}

	Future<Void> runWorker(Database cx) {
		double lastTime = now();
		double const targetLocalTps = static_cast<double>(targetTps) / totalThreads;

		while (true) {
			Future<Void> rateDelay =
			    targetLocalTps > 0 ? poisson(&lastTime, 1.0 / targetLocalTps) : Future<Void>(Void());

			YCSBOperation const opType =
			    deterministicRandom()->randomInt(0, readWeight + updateWeight) < readWeight ? YCSB_READ : YCSB_UPDATE;
			uint64_t const row = insertionOrder[nextKeyIdx()];
			Key key = rowKey(row);
			if (variant == YCSB_MULTI_CF && opType == YCSB_UPDATE) {
				key = fieldKey(row, deterministicRandom()->randomInt(0, fieldCount));
			}

			double const startTime = timer_monotonic();
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					bool present;
					if (variant == YCSB_MULTI_CF && opType == YCSB_READ) {
						RangeResult res = co_await tr.getRange(prefixRange(key), fieldCount + 1);
						present = res.size() == fieldCount && !res.more;
					} else {
						Optional<Value> res = co_await tr.get(key);
						present = res.present();
					}
					if (!present) {
						TraceEvent(SevError, "YCSBKeyMissing")
						    .detail("Key", key)
						    .detail("Row", row)
						    .detail("Variant", variant)
						    .detail("ClientId", clientId);
					}
					ASSERT(present);
					if (opType == YCSB_UPDATE) {
						tr.set(key, randomValue());
					}
					co_await tr.commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
			double const duration = timer_monotonic() - startTime;
			latencies.addSample(duration);
			addWindowLatency(opType, duration);
			(opType == YCSB_READ ? readLatencies : updateLatencies).addSample(duration);
			++operations;

			co_await rateDelay;
		}
	}

	Future<bool> check(Database const& cx) override { return true; }

	static void addLatencyMetrics(std::vector<PerfMetric>& m, DDSketch<double>& sketch, std::string const& prefix) {
		m.emplace_back(prefix + " Mean Latency (ms)", 1000 * sketch.mean(), Averaged::True);
		m.emplace_back(prefix + " Median Latency (ms, averaged)", 1000 * sketch.median(), Averaged::True);
		m.emplace_back(prefix + " 90% Latency (ms, averaged)", 1000 * sketch.percentile(0.90), Averaged::True);
		m.emplace_back(prefix + " 99% Latency (ms, averaged)", 1000 * sketch.percentile(0.99), Averaged::True);
		m.emplace_back(prefix + " 99.9% Latency (ms, averaged)", 1000 * sketch.percentile(0.999), Averaged::True);
		m.emplace_back(prefix + " Max Latency (ms, averaged)", 1000 * sketch.max(), Averaged::True);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		// With fewer threads than clients some clients run nothing, and their 0/0 ops/sec and empty-sketch latencies
		// would skew the cross-client aggregates
		CODE_PROBE(myThreads == 0, "YCSB client with no threads reports no metrics");
		if (myThreads == 0) {
			return;
		}
		m.push_back(operations.getMetric());
		double const opsPerSec = operations.getValue() / testDuration;
		m.emplace_back("Ops/sec", opsPerSec, Averaged::True);
		// With unlimited tps there is no target to compare against
		if (targetTps > 0) {
			double const targetOpsPerSec = (1.0 * myThreads * targetTps) / totalThreads;
			m.emplace_back("Expected Ops/sec", targetOpsPerSec, Averaged::True);
			m.emplace_back("Rows/sec Target Diff Percent",
			               100.0 * (opsPerSec - targetOpsPerSec) / targetOpsPerSec,
			               Averaged::True);
		}

		addLatencyMetrics(m, latencies, "Total");
		if (readWeight > 0) {
			addLatencyMetrics(m, readLatencies, "Read");
		}
		if (updateWeight > 0) {
			addLatencyMetrics(m, updateLatencies, "Update");
		}
	}
};

WorkloadFactory<YCSBWorkload> YCSBWorkloadFactory;
