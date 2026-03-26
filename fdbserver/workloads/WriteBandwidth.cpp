/*
 * WriteBandwidth.cpp
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

#include <boost/lexical_cast.hpp>

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/tester/workloads.actor.h"
#include "BulkSetup.h"

struct WriteBandwidthWorkload : KVWorkload {
	static constexpr auto NAME = "WriteBandwidth";

	int keysPerTransaction;
	double testDuration, warmingDelay, loadTime, maxInsertRate;
	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;
	DDSketch<double> commitLatencies, GRVLatencies;

	WriteBandwidthWorkload(WorkloadContext const& wcx)
	  : KVWorkload(wcx), loadTime(0.0), transactions("Transactions"), retries("Retries"), commitLatencies(),
	    GRVLatencies() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		keysPerTransaction = getOption(options, "keysPerTransaction"_sr, 100);
		warmingDelay = getOption(options, "warmingDelay"_sr, 0.0);
		maxInsertRate = getOption(options, "maxInsertRate"_sr, 1e12);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = testDuration;
		int writes = transactions.getValue() * keysPerTransaction;
		m.emplace_back("Measured Duration", duration, Averaged::True);
		m.emplace_back("Transactions/sec", transactions.getValue() / duration, Averaged::False);
		m.emplace_back("Operations/sec", writes / duration, Averaged::False);
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.emplace_back("Mean load time (seconds)", loadTime, Averaged::True);
		m.emplace_back("Write rows", writes, Averaged::False);

		m.emplace_back("Mean GRV Latency (ms)", 1000 * GRVLatencies.mean(), Averaged::True);
		m.emplace_back("Median GRV Latency (ms, averaged)", 1000 * GRVLatencies.median(), Averaged::True);
		m.emplace_back("90% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile(0.98), Averaged::True);

		m.emplace_back("Mean Commit Latency (ms)", 1000 * commitLatencies.mean(), Averaged::True);
		m.emplace_back("Median Commit Latency (ms, averaged)", 1000 * commitLatencies.median(), Averaged::True);
		m.emplace_back("90% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile(0.98), Averaged::True);

		m.emplace_back("Write rows/sec", writes / duration, Averaged::False);
		m.emplace_back("Bytes written/sec",
		               (writes * (keyBytes + (minValueBytes + maxValueBytes) * 0.5)) / duration,
		               Averaged::False);
	}

	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n, false), randomValue()); }

	Future<Void> setup(Database const& cx) override {
		Promise<double> loadTime;
		Promise<std::vector<std::pair<uint64_t, double>>> ratesAtKeyCounts;

		co_await bulkSetup(cx, this, nodeCount, loadTime, true, warmingDelay, maxInsertRate);
		this->loadTime = loadTime.getFuture().get();
	}

	Future<Void> start(Database const& cx) override {
		for (int i = 0; i < actorCount; i++) {
			clients.push_back(writeClient(cx, this));
		}

		co_await timeout(waitForAll(clients), testDuration, Void());
		clients.clear();
	}

	Future<Void> writeClient(Database cx, WriteBandwidthWorkload* self) {
		while (true) {
			Transaction tr(cx);
			uint64_t startIdx = deterministicRandom()->random01() * (self->nodeCount - self->keysPerTransaction);
			while (true) {
				Error err;
				try {
					double start = now();
					co_await tr.getReadVersion();
					self->GRVLatencies.addSample(now() - start);

					// Predefine a single large write conflict range over the whole key space
					tr.addWriteConflictRange(
					    KeyRangeRef(self->keyForIndex(startIdx, false),
					                keyAfter(self->keyForIndex(startIdx + self->keysPerTransaction - 1, false))));

					for (int i = 0; i < self->keysPerTransaction; i++)
						tr.set(self->keyForIndex(startIdx + i, false), self->randomValue(), AddConflictRange::False);

					start = now();
					co_await tr.commit();
					self->commitLatencies.addSample(now() - start);
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
				++self->retries;
			}
			++self->transactions;
		}
	}
};

WorkloadFactory<WriteBandwidthWorkload> WriteBandwidthWorkloadFactory;
