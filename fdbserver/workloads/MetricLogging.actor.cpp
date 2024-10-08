/*
 * MetricLogging.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "flow/TDMetric.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MetricLoggingWorkload : TestWorkload {
	static constexpr auto NAME = "MetricLogging";
	int actorCount, metricCount;
	double testDuration;
	bool testBool, enabled;

	std::vector<Future<Void>> clients;
	PerfIntCounter changes;
	std::vector<BoolMetricHandle> boolMetrics;
	std::vector<Int64MetricHandle> int64Metrics;

	MetricLoggingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), changes("Changes") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		actorCount = getOption(options, "actorCount"_sr, 1);
		metricCount = getOption(options, "metricCount"_sr, 1);
		testBool = getOption(options, "testBool"_sr, true);
		enabled = getOption(options, "enabled"_sr, true);

		for (int i = 0; i < metricCount; i++) {
			if (testBool) {
				boolMetrics.push_back(BoolMetricHandle("TestBool"_sr, format("%d", i)));
			} else {
				int64Metrics.push_back(Int64MetricHandle("TestInt"_sr, format("%d", i)));
			}
		}
	}

	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	ACTOR Future<Void> _setup(MetricLoggingWorkload* self, Database cx) {
		wait(delay(2.0));
		for (int i = 0; i < self->metricCount; i++) {
			if (self->testBool) {
				self->boolMetrics[i]->setConfig(true);
			} else {
				self->int64Metrics[i]->setConfig(true);
			}
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorCount; c++)
			clients.push_back(timeout(MetricLoggingClient(cx, this, clientId, c), testDuration, Void()));
		return waitForAll(clients);
	}

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(changes.getMetric());
		m.emplace_back("Changes/sec", changes.getValue() / testDuration, Averaged::False);
	}

	ACTOR Future<Void> MetricLoggingClient(Database cx, MetricLoggingWorkload* self, int clientId, int actorId) {
		state BinaryWriter writer(Unversioned());
		loop {
			for (int i = 0; i < 100; i++) {
				if (self->testBool) {
					self->boolMetrics[self->changes.getValue() % self->metricCount]->toggle();
				} else {
					self->int64Metrics[self->changes.getValue() % self->metricCount] = (self->changes.getValue());
				}
				++self->changes;
			}
			wait(yield());
		}
	}
};

WorkloadFactory<MetricLoggingWorkload> MetricLoggingWorkloadFactory;
