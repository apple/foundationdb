/*
 * CpuProfiler.actor.cpp
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

#include "fdbserver/TesterInterface.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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
	ACTOR Future<Void> updateProfiler(bool enabled, Database cx, CpuProfilerWorkload* self) {
		if (self->clientId == 0) {
			// If we are turning the profiler on, get a list of workers in the system
			if (enabled) {
				std::vector<WorkerDetails> _workers = wait(getWorkers(self->dbInfo));
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

			state std::vector<Future<ErrorOr<Void>>> replies;
			state int i;
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

			wait(waitForAll(replies));

			// Check that all workers succeeded if turning the profiler on
			if (enabled)
				for (i = 0; i < replies.size(); i++)
					if (!replies[i].get().present())
						self->success = false;

			TraceEvent("DoneSignalingProfiler").log();
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR Future<Void> _start(Database cx, CpuProfilerWorkload* self) {
		wait(delay(self->initialDelay));
		if (self->clientId == 0)
			TraceEvent("SignalProfilerOn").log();
		wait(timeoutError(self->updateProfiler(true, cx, self), 60.0));

		// If a duration was given, let the duration elapse and then shut the profiler off
		if (self->duration > 0) {
			wait(delay(self->duration));
			if (self->clientId == 0)
				TraceEvent("SignalProfilerOff").log();
			wait(timeoutError(self->updateProfiler(false, cx, self), 60.0));
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }

	ACTOR Future<bool> _check(Database cx, CpuProfilerWorkload* self) {
		// If no duration was given, then shut the profiler off now
		if (self->duration <= 0) {
			if (self->clientId == 0)
				TraceEvent("SignalProfilerOff").log();
			wait(timeoutError(self->updateProfiler(false, cx, self), 60.0));
		}

		return self->success;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<CpuProfilerWorkload> CpuProfilerWorkloadFactory;
