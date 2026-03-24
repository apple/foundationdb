/*
 * CpuProfiler.cpp
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

#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/QuietDatabase.actor.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.actor.h"

// A workload which starts the CPU profiler at a given time and duration on all workers in a cluster
struct CpuProfilerWorkload : TestWorkload {
	static constexpr auto NAME = "CpuProfiler";
	bool success;

	// How long to run the workload before starting the profiler
	double initialDelay;

	// How long the profiler should be run; if <= 0 then it will run until the workload's check function is called
	double duration;

	// What process classes should be profiled as part of this run?
	// See Locality.h for the list of valid strings to provide.
	std::vector<std::string> roles;

	// A list of worker interfaces which have had profiling turned on
	std::vector<WorkerInterface> profilingWorkers;

	CpuProfilerWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		initialDelay = getOption(options, "initialDelay"_sr, 0.0);
		duration = getOption(options, "duration"_sr, -1.0);
		roles = getOption(options, "roles"_sr, std::vector<std::string>());
		success = true;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	// Turns the profiler on or off
	Future<Void> updateProfiler(bool enabled, Database cx, CpuProfilerWorkload* self) {
		if (self->clientId == 0) {
			// If we are turning the profiler on, get a list of workers in the system
			if (enabled) {
				std::vector<WorkerDetails> _workers = co_await getWorkers(self->dbInfo);
				std::vector<WorkerInterface> workers;
				for (int i = 0; i < _workers.size(); i++) {
					if (self->roles.empty() ||
					    std::find(self->roles.cbegin(), self->roles.cend(), _workers[i].processClass.toString()) !=
					        self->roles.cend()) {
						workers.push_back(_workers[i].interf);
					}
				}
				self->profilingWorkers = workers;
			}

			std::vector<Future<ErrorOr<Void>>> replies;
			int i{ 0 };
			// Send a ProfilerRequest to each worker
			for (i = 0; i < self->profilingWorkers.size(); i++) {
				ProfilerRequest req;
				req.type = ProfilerRequest::Type::FLOW;
				req.action = enabled ? ProfilerRequest::Action::ENABLE : ProfilerRequest::Action::DISABLE;
				req.duration = 0; // unused

				// The profiler output name will be the ip.port.prof
				req.outputFile = StringRef(self->profilingWorkers[i].address().ip.toString() + "." +
				                           format("%d", self->profilingWorkers[i].address().port) + ".profile.bin");

				replies.push_back(self->profilingWorkers[i].clientInterface.profiler.tryGetReply(req));
			}

			co_await waitForAll(replies);

			// Check that all workers succeeded if turning the profiler on
			if (enabled)
				for (i = 0; i < replies.size(); i++)
					if (!replies[i].get().present())
						self->success = false;

			TraceEvent("DoneSignalingProfiler").log();
		}
	}

	Future<Void> start(Database const& cx) override {
		co_await delay(initialDelay);
		if (clientId == 0)
			TraceEvent("SignalProfilerOn").log();
		co_await timeoutError(updateProfiler(true, cx, this), 60.0);

		// If a duration was given, let the duration elapse and then shut the profiler off
		if (duration > 0) {
			co_await delay(duration);
			if (clientId == 0)
				TraceEvent("SignalProfilerOff").log();
			co_await timeoutError(updateProfiler(false, cx, this), 60.0);
		}
	}

	Future<bool> check(Database const& cx) override {
		// If no duration was given, then shut the profiler off now
		if (duration <= 0) {
			if (clientId == 0)
				TraceEvent("SignalProfilerOff").log();
			co_await timeoutError(updateProfiler(false, cx, this), 60.0);
		}

		co_return success;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<CpuProfilerWorkload> CpuProfilerWorkloadFactory;
