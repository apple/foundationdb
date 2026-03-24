/*
 * LowLatency.cpp
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

#include "fdbclient/IKnobCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.actor.h"

struct LowLatencyWorkload : TestWorkload {
	static constexpr auto NAME = "LowLatency";

	double testDuration;
	double maxGRVLatency;
	double maxCommitLatency;
	double checkDelay;
	PerfIntCounter operations, retries;
	bool testWrites;
	Key testKey;
	bool ok;

	LowLatencyWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), operations("Operations"), retries("Retries"), ok(true) {
		testDuration = getOption(options, "testDuration"_sr, 600.0);
		maxGRVLatency = getOption(options, "maxGRVLatency"_sr, 20.0);
		maxCommitLatency = getOption(options, "maxCommitLatency"_sr, 30.0);
		checkDelay = getOption(options, "checkDelay"_sr, 1.0);
		testWrites = getOption(options, "testWrites"_sr, true);
		testKey = getOption(options, "testKey"_sr, "testKey"_sr);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	Future<Void> setup(Database const& cx) override {
		if (g_network->isSimulated()) {
			IKnobCollection::getMutableGlobalKnobCollection().setKnob("min_delay_cc_worst_fit_candidacy_seconds",
			                                                          KnobValueRef::create(double{ 5.0 }));
			IKnobCollection::getMutableGlobalKnobCollection().setKnob("max_delay_cc_worst_fit_candidacy_seconds",
			                                                          KnobValueRef::create(double{ 10.0 }));
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx);
		return Void();
	}

	Future<Void> _start(Database cx) {
		double testStart = now();
		try {
			while (true) {
				co_await delay(checkDelay);
				Transaction tr(cx);
				double operationStart = now();
				bool doCommit = testWrites && deterministicRandom()->coinflip();
				double maxLatency = doCommit ? maxCommitLatency : maxGRVLatency;
				++operations;
				while (true) {
					Error err;
					try {
						TraceEvent("LowLatencyTransactionStart").detail("Retries", retries.getValue());
						tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						if (doCommit) {
							TraceEvent("LowLatencyTransactionCommitStart");
							tr.set(testKey, ""_sr);
							co_await tr.commit();
							TraceEvent("LowLatencyTransactionCommitFinish");
						} else {
							TraceEvent("LowLatencyTransactionGRVStart");
							co_await tr.getReadVersion();
							TraceEvent("LowLatencyTransactionGRVFinish");
						}
						break;
					} catch (Error& e) {
						err = e;
					}
					TraceEvent("LowLatencyTransactionFailed").errorUnsuppressed(err);
					co_await tr.onError(err);
					++retries;
				}
				if (now() - operationStart > maxLatency) {
					TraceEvent(SevError, "LatencyTooLarge")
					    .detail("MaxLatency", maxLatency)
					    .detail("ObservedLatency", now() - operationStart)
					    .detail("IsCommit", doCommit);
					ok = false;
				}
				if (now() - testStart > testDuration)
					break;
			}
			co_return;
		} catch (Error& e) {
			TraceEvent(SevError, "LowLatencyError").errorUnsuppressed(e);
			throw;
		}
	}

	Future<bool> check(Database const& cx) override { return ok; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = testDuration;
		m.emplace_back("Operations/sec", operations.getValue() / duration, Averaged::False);
		m.push_back(operations.getMetric());
		m.push_back(retries.getMetric());
	}
};

WorkloadFactory<LowLatencyWorkload> LowLatencyWorkloadFactory;
