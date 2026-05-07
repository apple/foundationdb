/*
 * QueuePush.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"

const int keyBytes = 16;

struct QueuePushWorkload : TestWorkload {
	static constexpr auto NAME = "QueuePush";

	int actorCount, valueBytes;
	double testDuration;
	bool forward;
	std::string valueString;
	Key endingKey, startingKey;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;
	DDSketch<double> commitLatencies, GRVLatencies;

	explicit QueuePushWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), commitLatencies(), GRVLatencies() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		actorCount = getOption(options, "actorCount"_sr, 50);

		valueBytes = getOption(options, "valueBytes"_sr, 96);
		valueString = deterministicRandom()->randomAlphaNumeric(valueBytes);

		forward = getOption(options, "forward"_sr, true);

		endingKey = "9999999900000001"_sr;
		startingKey = "0000000000000001"_sr;
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = testDuration;
		int writes = transactions.getValue();
		m.emplace_back("Measured Duration", duration, Averaged::True);
		m.emplace_back("Operations/sec", writes / duration, Averaged::False);
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());

		m.emplace_back("Mean GRV Latency (ms)", 1000 * GRVLatencies.mean(), Averaged::True);
		m.emplace_back("Median GRV Latency (ms, averaged)", 1000 * GRVLatencies.median(), Averaged::True);
		m.emplace_back("90% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile(0.98), Averaged::True);

		m.emplace_back("Mean Commit Latency (ms)", 1000 * commitLatencies.mean(), Averaged::True);
		m.emplace_back("Median Commit Latency (ms, averaged)", 1000 * commitLatencies.median(), Averaged::True);
		m.emplace_back("90% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile(0.98), Averaged::True);

		m.emplace_back("Bytes written/sec", (writes * (keyBytes + valueBytes)) / duration, Averaged::False);
	}

	static Key keyForIndex(int base, int offset) { return StringRef(format("%08x%08x", base, offset)); }

	static std::pair<int, int> valuesForKey(KeyRef value) {
		int base, offset;
		ASSERT(value.size() == 16);

		if (sscanf(value.substr(0, 8).toString().c_str(), "%x", &base) &&
		    sscanf(value.substr(8, 8).toString().c_str(), "%x", &offset)) {
			return std::make_pair(base, offset);
		} else
			// SOMEDAY: what should this really be?  Should we rely on exceptions for control flow here?
			throw client_invalid_operation();
	}

	Future<Void> start(Database const& cx) override {
		for (int i = 0; i < actorCount; i++) {
			clients.push_back(writeClient(cx, this));
		}

		co_await timeout(waitForAll(clients), testDuration, Void());
		clients.clear();
	}

	Future<Void> writeClient(Database cx, QueuePushWorkload* self) {
		while (true) {
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					double start = now();
					co_await tr.getReadVersion();
					self->GRVLatencies.addSample(now() - start);

					// Get the last key in the database with a snapshot read
					Key lastKey;

					if (self->forward) {
						Key _lastKey = co_await tr.getKey(lastLessThan(self->endingKey), Snapshot::True);
						lastKey = _lastKey;
						if (lastKey.empty())
							lastKey = self->startingKey;
					} else {
						Key _lastKey = co_await tr.getKey(firstGreaterThan(self->startingKey), Snapshot::True);
						lastKey = _lastKey;
						if (!normalKeys.contains(lastKey))
							lastKey = self->endingKey;
					}

					std::pair<int, int> unpacked = valuesForKey(lastKey);

					if (self->forward)
						tr.set(keyForIndex(unpacked.first + unpacked.second, deterministicRandom()->randomInt(1, 1000)),
						       StringRef(self->valueString));
					else
						tr.set(keyForIndex(unpacked.first - unpacked.second, deterministicRandom()->randomInt(1, 1000)),
						       StringRef(self->valueString));

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

WorkloadFactory<QueuePushWorkload> QueuePushWorkloadFactory;
