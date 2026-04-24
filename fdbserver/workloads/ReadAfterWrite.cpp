/*
 * ReadAfterWrite.cpp
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

#include <vector>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/workloads.h"
#include "flow/genericactors.actor.h"

// If the log->storage propagation delay is longer than 1 second, then it's likely that our read
// will see a `future_version` error from the storage server.  We need to retry the read until
// a value is returned, or a different error is thrown.
Future<double> latencyOfRead(Transaction* tr, Key k) {
	double start = timer();
	while (true) {
		try {
			co_await tr->get(k);
			break;
		} catch (Error& e) {
			if (e.code() == error_code_future_version) {
				continue;
			}
			throw;
		}
	}
	co_return timer() - start;
}

// Measure the latency of a storage server making a committed value available for reading.
struct ReadAfterWriteWorkload : KVWorkload {
	static constexpr auto NAME = "ReadAfterWrite";

	double testDuration;
	DDSketch<double> propagationLatency;

	explicit ReadAfterWriteWorkload(WorkloadContext const& wcx) : KVWorkload(wcx), propagationLatency() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> benchmark(Database cx) {
		while (true) {
			Key key = getRandomKey();
			Transaction writeTr(cx);
			Transaction baselineReadTr(cx);
			Transaction afterWriteTr(cx);

			Error err;
			try {
				Version readVersion = co_await writeTr.getReadVersion();

				// We do a read in this writeTransaction only to enforce that `readVersion` is already on a storage
				// server after we commit.  Its existence or non-existence is irrelevant.  We write back the exact
				// same value (or clear the key, if empty) so that the database state is not mutated.  This means
				// this workload can be paired with any other workload, and it won't affect any results.
				Optional<Value> value = co_await writeTr.get(key);
				if (value.present()) {
					writeTr.set(key, value.get());
				} else {
					writeTr.clear(key);
				}

				co_await writeTr.commit();

				Version commitVersion = writeTr.getCommittedVersion();

				baselineReadTr.setVersion(readVersion);
				afterWriteTr.setVersion(commitVersion);

				double baselineLatency = 0;
				double afterWriteLatency = 0;

				co_await (store(baselineLatency, latencyOfRead(&baselineReadTr, key)) &&
				          store(afterWriteLatency, latencyOfRead(&afterWriteTr, key)));

				// By reading the same key at two different versions, we should be able to measure the latency of
				// the network, the storage server overhead, and the propagation delay, and then with our baseline
				// read, subtract out the network and the storage server overhead, leaving only the propagation
				// delay.
				propagationLatency.addSample(std::max<double>(afterWriteLatency - baselineLatency, 0));
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await writeTr.onError(err);
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		Future<Void> lifetime = benchmark(cx);
		co_await delay(testDuration);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Mean Latency (ms)", 1000 * propagationLatency.mean(), Averaged::True);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * propagationLatency.median(), Averaged::True);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * propagationLatency.percentile(0.90), Averaged::True);
		m.emplace_back("99% Latency (ms, averaged)", 1000 * propagationLatency.percentile(0.99), Averaged::True);
		m.emplace_back("Max Latency (ms, averaged)", 1000 * propagationLatency.max(), Averaged::True);
	}
};

WorkloadFactory<ReadAfterWriteWorkload> ReadAfterWriteWorkloadFactory;
