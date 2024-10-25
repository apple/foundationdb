/*
 * Performance.actor.cpp
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
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct PerformanceWorkload : TestWorkload {
	static constexpr auto NAME = "Performance";

	Value probeWorkload;
	Standalone<VectorRef<KeyValueRef>> savedOptions;

	std::vector<PerfMetric> metrics;
	std::vector<TesterInterface> testers;
	PerfMetric latencyBaseline, latencySaturation;
	PerfMetric maxAchievedTPS;

	PerformanceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		probeWorkload = getOption(options, "probeWorkload"_sr, "ReadWrite"_sr);

		// "Consume" all options and save for later tests
		for (int i = 0; i < options.size(); i++) {
			if (options[i].value.size()) {
				savedOptions.push_back_deep(savedOptions.arena(), KeyValueRef(options[i].key, options[i].value));
				printf("saved option (%d): '%s'='%s'\n",
				       i,
				       printable(options[i].key).c_str(),
				       printable(options[i].value).c_str());
				options[i].value = ""_sr;
			}
		}
		printf("saved %d options\n", savedOptions.size());
	}

	Future<Void> setup(Database const& cx) override {
		if (!clientId)
			return _setup(cx, this);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (!clientId)
			return _start(cx, this);
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		for (int i = 0; i < metrics.size(); i++)
			m.push_back(metrics[i]);
		if (!clientId) {
			m.emplace_back("Baseline Latency (average, ms)", latencyBaseline.value(), Averaged::False);
			m.emplace_back("Saturation Transactions/sec", maxAchievedTPS.value(), Averaged::False);
			m.emplace_back("Saturation Median Latency (average, ms)", latencySaturation.value(), Averaged::False);
		}
	}

	Standalone<VectorRef<VectorRef<KeyValueRef>>> getOpts(double transactionsPerSecond) {
		Standalone<VectorRef<KeyValueRef>> options;
		Standalone<VectorRef<VectorRef<KeyValueRef>>> opts;
		options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, probeWorkload));
		options.push_back_deep(options.arena(),
		                       KeyValueRef("transactionsPerSecond"_sr, format("%f", transactionsPerSecond)));
		for (int i = 0; i < savedOptions.size(); i++) {
			options.push_back_deep(options.arena(), savedOptions[i]);
			printf("option [%d]: '%s'='%s'\n",
			       i,
			       printable(savedOptions[i].key).c_str(),
			       printable(savedOptions[i].value).c_str());
		}
		opts.push_back_deep(opts.arena(), options);
		return opts;
	}

	void logOptions(Standalone<VectorRef<VectorRef<KeyValueRef>>> options) {
		TraceEvent start("PerformaceSetupStarting");
		for (int i = 0; i < options.size(); i++) {
			for (int j = 0; j < options[i].size(); j++) {
				start.detail(format("Option-%d-%d", i, j).c_str(),
				             printable(options[i][j].key) + "=" + printable(options[i][j].value));
			}
		}
	}

	// FIXME: does not use testers which are recruited on workers
	ACTOR Future<std::vector<TesterInterface>> getTesters(PerformanceWorkload* self) {
		state std::vector<WorkerDetails> workers;

		loop {
			choose {
				when(
				    std::vector<WorkerDetails> w = wait(
				        brokenPromiseToNever(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest(
				            GetWorkersRequest::TESTER_CLASS_ONLY | GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY))))) {
					workers = w;
					break;
				}
				when(wait(self->dbInfo->onChange())) {}
			}
		}

		std::vector<TesterInterface> ts;
		ts.reserve(workers.size());
		for (int i = 0; i < workers.size(); i++)
			ts.push_back(workers[i].interf.testerInterface);
		return ts;
	}

	ACTOR Future<Void> _setup(Database cx, PerformanceWorkload* self) {
		state Standalone<VectorRef<VectorRef<KeyValueRef>>> options = self->getOpts(1000.0);
		self->logOptions(options);

		std::vector<TesterInterface> testers = wait(self->getTesters(self));
		self->testers = testers;

		TestSpec spec("PerformanceSetup"_sr, false, false);
		spec.options = options;
		spec.phases = TestWorkload::SETUP;
		DistributedTestResults results = wait(runWorkload(cx, testers, spec, Optional<TenantName>()));

		return Void();
	}

	PerfMetric getNamedMetric(std::string name, std::vector<PerfMetric> metrics) {
		for (int i = 0; i < metrics.size(); i++) {
			if (metrics[i].name() == name) {
				return metrics[i];
			}
		}
		return PerfMetric();
	}

	ACTOR Future<Void> getSaturation(Database cx, PerformanceWorkload* self) {
		state double tps = 400;
		state bool reported = false;
		state bool retry = false;
		state double multiplier = 2.0;

		loop {
			Standalone<VectorRef<VectorRef<KeyValueRef>>> options = self->getOpts(tps);
			TraceEvent start("PerformaceProbeStarting");
			start.detail("RateTarget", tps);
			for (int i = 0; i < options.size(); i++) {
				for (int j = 0; j < options[i].size(); j++) {
					start.detail(format("Option-%d-%d", i, j).c_str(),
					             printable(options[i][j].key) + "=" + printable(options[i][j].value));
				}
			}
			state DistributedTestResults results;
			try {
				TestSpec spec("PerformanceRun"_sr, false, false);
				spec.phases = TestWorkload::EXECUTION | TestWorkload::METRICS;
				spec.options = options;
				DistributedTestResults r = wait(runWorkload(cx, self->testers, spec, Optional<TenantName>()));
				results = r;
			} catch (Error& e) {
				TraceEvent("PerformanceRunError")
				    .errorUnsuppressed(e)
				    .detail("Workload", printable(self->probeWorkload));
				break;
			}
			PerfMetric tpsMetric = self->getNamedMetric("Transactions/sec", results.metrics);
			PerfMetric latencyMetric = self->getNamedMetric("Median Latency (ms, averaged)", results.metrics);

			logMetrics(results.metrics);

			if (!reported || self->latencyBaseline.value() > latencyMetric.value())
				self->latencyBaseline = latencyMetric;
			if (!reported || self->maxAchievedTPS.value() < tpsMetric.value()) {
				self->maxAchievedTPS = tpsMetric;
				self->latencySaturation = latencyMetric;
				self->metrics = results.metrics;
			}
			reported = true;

			TraceEvent evt("PerformanceProbeComplete");
			evt.detail("RateTarget", tps)
			    .detail("AchievedRate", tpsMetric.value())
			    .detail("Multiplier", multiplier)
			    .detail("Retry", retry);
			if (tpsMetric.value() < (tps * .95) - 100) {
				evt.detail("LimitReached", 1);
				if (!retry) {
					retry = true;
				} else if (multiplier < 2.0) {
					evt.detail("Saturation", "final");
					return Void();
				} else {
					tps /= 2;
					multiplier = 1.189;
					retry = false;
				}
			} else {
				retry = false;
			}
			tps *= retry ? 1.0 : multiplier;
		}

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, PerformanceWorkload* self) {
		wait(self->getSaturation(cx, self));
		TraceEvent("PerformanceSaturation")
		    .detail("SaturationRate", self->maxAchievedTPS.value())
		    .detail("SaturationLatency", self->latencySaturation.value());
		return Void();
	}
};

WorkloadFactory<PerformanceWorkload> PerformanceWorkloadFactory;
