/*
 * Performance.cpp
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
#include "fdbserver/tester/workloads.actor.h"
#include "fdbserver/core/QuietDatabase.actor.h"
#include "fdbserver/tester/WorkloadUtils.h"
#include "flow/CoroUtils.h"

// TODO: explain purpose of this workload. Obviously simulation is aimed at correctness,
// not performance, so a workload literally named Performance has some explaining to do.
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
	Future<std::vector<TesterInterface>> getTesters(PerformanceWorkload* self) {
		std::vector<WorkerDetails> workers;

		while (true) {
			auto choice = co_await race(
			    brokenPromiseToNever(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest(
			        GetWorkersRequest::TESTER_CLASS_ONLY | GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY))),
			    self->dbInfo->onChange());
			if (choice.index() == 0) {
				std::vector<WorkerDetails> w = std::get<0>(std::move(choice));

				workers = w;
				break;
			}
		}

		std::vector<TesterInterface> ts;
		ts.reserve(workers.size());
		for (int i = 0; i < workers.size(); i++)
			ts.push_back(workers[i].interf.testerInterface);
		co_return ts;
	}

	Future<Void> _setup(Database cx, PerformanceWorkload* self) {
		Standalone<VectorRef<VectorRef<KeyValueRef>>> options = self->getOpts(1000.0);
		self->logOptions(options);

		std::vector<TesterInterface> testers = co_await self->getTesters(self);
		self->testers = testers;

		TestSpec spec("PerformanceSetup"_sr, false, false);
		spec.options = options;
		spec.phases = TestWorkload::SETUP;
		DistributedTestResults results = co_await runWorkload(cx, testers, spec);
	}

	PerfMetric getNamedMetric(std::string name, std::vector<PerfMetric> metrics) {
		for (int i = 0; i < metrics.size(); i++) {
			if (metrics[i].name() == name) {
				return metrics[i];
			}
		}
		return PerfMetric();
	}

	Future<Void> getSaturation(Database cx, PerformanceWorkload* self) {
		double tps = 400;
		bool reported = false;
		bool retry = false;
		double multiplier = 2.0;

		while (true) {
			Standalone<VectorRef<VectorRef<KeyValueRef>>> options = self->getOpts(tps);
			TraceEvent start("PerformaceProbeStarting");
			start.detail("RateTarget", tps);
			for (int i = 0; i < options.size(); i++) {
				for (int j = 0; j < options[i].size(); j++) {
					start.detail(format("Option-%d-%d", i, j).c_str(),
					             printable(options[i][j].key) + "=" + printable(options[i][j].value));
				}
			}
			DistributedTestResults results;
			try {
				TestSpec spec("PerformanceRun"_sr, false, false);
				spec.phases = TestWorkload::EXECUTION | TestWorkload::METRICS;
				spec.options = options;
				DistributedTestResults r = co_await runWorkload(cx, self->testers, spec);
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
					co_return;
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
	}

	Future<Void> _start(Database cx, PerformanceWorkload* self) {
		co_await self->getSaturation(cx, self);
		TraceEvent("PerformanceSaturation")
		    .detail("SaturationRate", self->maxAchievedTPS.value())
		    .detail("SaturationLatency", self->latencySaturation.value());
	}
};

WorkloadFactory<PerformanceWorkload> PerformanceWorkloadFactory;
