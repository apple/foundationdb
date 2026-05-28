/*
 * MinimumThroughput.cpp
 *
 * Do not mix this with Attrition or similar error injection workloads.
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

struct MinimumThroughputWorkload : TestWorkload {
	static constexpr auto NAME = "MinimumThroughput";

	int actorCount, nodeCount;
	double testDuration, transactionsPerSecond, minExpectedTransactionsPerSecond, readFraction;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;
	PerfDoubleCounter totalLatency;

	explicit MinimumThroughputWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), totalLatency("Latency") {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 200.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, std::max(1, int(transactionsPerSecond / 5)));
		nodeCount = getOption(options, "nodeCount"_sr, 100000);
		readFraction = getOption(options, "readFraction"_sr, 0.5);
		minExpectedTransactionsPerSecond = transactionsPerSecond * getOption(options, "expectedRate"_sr, 0.8);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorCount; c++) {
			clients.push_back(
			    timeout(client(cx->clone(), this, actorCount / transactionsPerSecond), testDuration, Void()));
		}
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		int errors = 0;
		for (auto& c : clients)
			errors += c.isError();
		clients.clear();
		if (errors) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were client errors.");
			return false;
		}
		int64_t achieved = transactions.getMetric().value();
		int64_t minRequired = (int64_t)(testDuration * minExpectedTransactionsPerSecond);
		int64_t target = (int64_t)(testDuration * transactionsPerSecond);
		if (achieved < minRequired) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "Throughput below minimum")
			    .detail("Achieved", achieved)
			    .detail("MinRequired", minRequired)
			    .detail("Target", target);
			return false;
		}
		return true;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.emplace_back("Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), Averaged::True);
	}

	static Key keyForIndex(int i) { return StringRef(format("MTP/%08d", i)); }

	Future<Void> client(Database cx, MinimumThroughputWorkload* self, double delay) {
		double lastTime = now();
		try {
			while (true) {
				co_await poisson(&lastTime, delay);
				double tstart = now();
				Key key = keyForIndex(deterministicRandom()->randomInt(0, self->nodeCount));
				bool readOnly = deterministicRandom()->random01() < self->readFraction;
				Transaction tr(cx);
				while (true) {
					Error err;
					try {
						co_await tr.get(key);
						if (!readOnly) {
							tr.set(key, "x"_sr);
							co_await tr.commit();
						}
						break;
					} catch (Error& e) {
						err = e;
					}
					co_await tr.onError(err);
					++self->retries;
				}
				++self->transactions;
				self->totalLatency += now() - tstart;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "MinimumThroughputClient").error(e);
			throw;
		}
	}
};

WorkloadFactory<MinimumThroughputWorkload> MinimumThroughputWorkloadFactory;
