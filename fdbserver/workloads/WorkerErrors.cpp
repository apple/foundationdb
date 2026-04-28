/*
 * WorkerErrors.cpp
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

#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/ServerDBInfo.h"

struct WorkerErrorsWorkload : TestWorkload {
	static constexpr auto NAME = "WorkerErrors";

	WorkerErrorsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return Void(); }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<std::vector<TraceEventFields>> latestEventOnWorkers(std::vector<WorkerDetails> workers) {
		std::vector<Future<TraceEventFields>> eventTraces;
		eventTraces.reserve(workers.size());
		for (int c = 0; c < workers.size(); c++) {
			eventTraces.push_back(workers[c].interf.eventLogRequest.getReply(EventLogRequest()));
		}

		co_await timeoutError(waitForAll(eventTraces), 2.0);

		std::vector<TraceEventFields> results;
		results.reserve(eventTraces.size());
		for (int i = 0; i < eventTraces.size(); i++) {
			results.push_back(eventTraces[i].get());
		}

		co_return results;
	}

	Future<Void> start(Database const& cx) override {
		std::vector<WorkerDetails> workers = co_await getWorkers(dbInfo);
		std::vector<TraceEventFields> errors = co_await latestEventOnWorkers(workers);
		for (const auto& e : errors) {
			printf("%s\n", e.toString().c_str());
		}
	}

	Future<bool> check(Database const& cx) override { return true; }
};

WorkloadFactory<WorkerErrorsWorkload> WorkerErrorsWorkloadFactory;
