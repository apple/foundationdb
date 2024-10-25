/*
 * BulkLoad.actor.cpp
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
#include "flow/actorcompiler.h" // This must be the last #include.

struct BulkLoadWorkload : TestWorkload {
	static constexpr auto NAME = "BulkLoad";
	int clientCount, actorCount, writesPerTransaction, valueBytes;
	double testDuration;
	Value value;
	uint64_t targetBytes;
	Key keyPrefix;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;
	DDSketch<double> latencies;

	BulkLoadWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), clientCount(wcx.clientCount), transactions("Transactions"), retries("Retries"), latencies() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		actorCount = getOption(options, "actorCount"_sr, 20);
		writesPerTransaction = getOption(options, "writesPerTransaction"_sr, 10);
		valueBytes = std::max(getOption(options, "valueBytes"_sr, 96), 16);
		value = Value(std::string(valueBytes, '.'));
		targetBytes = getOption(options, "targetBytes"_sr, std::numeric_limits<uint64_t>::max());
		keyPrefix = getOption(options, "keyPrefix"_sr, ""_sr);
		keyPrefix = unprintable(keyPrefix.toString());
	}

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorCount; c++)
			clients.push_back(timeout(bulkLoadClient(cx, this, clientId, c), testDuration, Void()));
		return waitForAll(clients);
	}

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.emplace_back("Rows written", transactions.getValue() * writesPerTransaction, Averaged::False);
		m.emplace_back("Transactions/sec", transactions.getValue() / testDuration, Averaged::False);
		m.emplace_back(
		    "Write rows/sec", transactions.getValue() * writesPerTransaction / testDuration, Averaged::False);
		double keysPerSecond = transactions.getValue() * writesPerTransaction / testDuration;
		m.emplace_back("Keys written/sec", keysPerSecond, Averaged::False);
		m.emplace_back("Bytes written/sec", keysPerSecond * (valueBytes + 16), Averaged::False);

		m.emplace_back("Mean Latency (ms)", 1000 * latencies.mean(), Averaged::True);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * latencies.median(), Averaged::True);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * latencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% Latency (ms, averaged)", 1000 * latencies.percentile(0.98), Averaged::True);
	}

	ACTOR Future<Void> bulkLoadClient(Database cx, BulkLoadWorkload* self, int clientId, int actorId) {
		state uint64_t totalBytes = 0;
		state int idx = 0;
		loop {
			state double tstart = now();
			state Transaction tr(cx);
			loop {
				state uint64_t txnBytes = 0;
				try {
					for (int i = 0; i < self->writesPerTransaction; i++) {
						std::string key = format("%s/bulkload/%04x/%04x/%08x",
						                         self->keyPrefix.toString().c_str(),
						                         self->clientId,
						                         actorId,
						                         idx + i);
						tr.set(key, self->value);
						txnBytes += key.size() + self->value.size();
					}
					tr.makeSelfConflicting();
					wait(success(tr.getReadVersion()));
					wait(tr.commit());
					totalBytes += txnBytes;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
					++self->retries;
				}
			}
			self->latencies.addSample(now() - tstart);
			++self->transactions;
			idx += self->writesPerTransaction;
			if (totalBytes > self->targetBytes / self->clientCount / self->actorCount)
				return Void();
		}
	}
};

WorkloadFactory<BulkLoadWorkload> BulkLoadWorkloadFactory;
