/*
 * StreamingRead.actor.cpp
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

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct StreamingReadWorkload : TestWorkload {
	static constexpr auto NAME = "StreamingRead";

	int actorCount, keyBytes, valueBytes, readsPerTransaction, nodeCount;
	int rangesPerTransaction;
	bool readSequentially;
	double testDuration, warmingDelay;
	Value constantValue;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, readKeys;
	PerfIntCounter readValueBytes;
	DDSketch<double> latencies;

	StreamingReadWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), readKeys("Keys Read"), readValueBytes("Value Bytes Read"),
	    latencies() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		actorCount = getOption(options, "actorCount"_sr, 20);
		readsPerTransaction = getOption(options, "readsPerTransaction"_sr, 10);
		rangesPerTransaction = getOption(options, "rangesPerTransaction"_sr, 1);
		nodeCount = getOption(options, "nodeCount"_sr, 100000);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);
		valueBytes = std::max(getOption(options, "valueBytes"_sr, 96), 16);
		std::string valueFormat = "%016llx" + std::string(valueBytes - 16, '.');
		warmingDelay = getOption(options, "warmingDelay"_sr, 0.0);
		constantValue = Value(format(valueFormat.c_str(), 42));
		readSequentially = getOption(options, "readSequentially"_sr, false);
	}

	Future<Void> setup(Database const& cx) override {
		return bulkSetup(cx, this, nodeCount, Promise<double>(), true, warmingDelay);
	}

	Future<Void> start(Database const& cx) override {
		for (int c = clientId; c < actorCount; c += clientCount)
			clients.push_back(timeout(streamingReadClient(cx, this, clientId, c), testDuration, Void()));
		return waitForAll(clients);
	}

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(readKeys.getMetric());
		m.emplace_back("Bytes read/sec",
		               (readKeys.getValue() * keyBytes + readValueBytes.getValue()) / testDuration,
		               Averaged::False);

		m.emplace_back("Mean Latency (ms)", 1000 * latencies.mean(), Averaged::True);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * latencies.median(), Averaged::True);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * latencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% Latency (ms, averaged)", 1000 * latencies.percentile(0.98), Averaged::True);
	}

	Key keyForIndex(uint64_t index) {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodeCount;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}

	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(keyForIndex(n), constantValue); }

	ACTOR Future<Void> streamingReadClient(Database cx, StreamingReadWorkload* self, int clientId, int actorId) {
		state int minIndex = actorId * self->nodeCount / self->actorCount;
		state int maxIndex = std::min((actorId + 1) * self->nodeCount / self->actorCount, self->nodeCount);
		state int currentIndex = minIndex;

		loop {
			state double tstart = now();
			state Transaction tr(cx);
			state int rangeSize = (double)self->readsPerTransaction / self->rangesPerTransaction + 0.5;
			state int range = 0;
			loop {
				state int thisRangeSize =
				    (range < self->rangesPerTransaction - 1)
				        ? rangeSize
				        : self->readsPerTransaction - (self->rangesPerTransaction - 1) * rangeSize;
				if (self->readSequentially && thisRangeSize > maxIndex - minIndex)
					thisRangeSize = maxIndex - minIndex;
				loop {
					try {
						if (!self->readSequentially)
							currentIndex = deterministicRandom()->randomInt(0, self->nodeCount - thisRangeSize);
						else if (currentIndex > maxIndex - thisRangeSize)
							currentIndex = minIndex;

						RangeResult values =
						    wait(tr.getRange(firstGreaterOrEqual(self->keyForIndex(currentIndex)),
						                     firstGreaterOrEqual(self->keyForIndex(currentIndex + thisRangeSize)),
						                     thisRangeSize));

						for (int i = 0; i < values.size(); i++)
							self->readValueBytes += values[i].value.size();

						if (self->readSequentially)
							currentIndex += values.size();

						self->readKeys += values.size();
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}

				if (now() - tstart > 3)
					break;

				if (++range == self->rangesPerTransaction)
					break;
			}
			self->latencies.addSample(now() - tstart);
			++self->transactions;
		}
	}
};

WorkloadFactory<StreamingReadWorkload> StreamingReadWorkloadFactory;
