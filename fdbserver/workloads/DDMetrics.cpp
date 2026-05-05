/*
 * DDMetrics.cpp
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
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"

struct DDMetricsWorkload : TestWorkload {
	static constexpr auto NAME = "DDMetrics";
	double startDelay, ddDone;

	explicit DDMetricsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), ddDone(0.0) {
		startDelay = getOption(options, "beginPoll"_sr, 10.0);
	}

	Future<int> getHighPriorityRelocationsInFlight(Database cx, DDMetricsWorkload* self) {
		WorkerInterface masterWorker = co_await getMasterWorker(cx, self->dbInfo);

		TraceEvent("GetHighPriorityReliocationsInFlight").detail("Stage", "ContactingMaster");
		TraceEventFields md =
		    co_await timeoutError(masterWorker.eventLogRequest.getReply(EventLogRequest("MovingData"_sr)), 1.0);
		int relocations;
		sscanf(md.getValue("UnhealthyRelocations").c_str(), "%d", &relocations);
		co_return relocations;
	}

	Future<Void> work(Database cx, DDMetricsWorkload* self) {
		try {
			TraceEvent("DDMetricsWaiting").detail("StartDelay", self->startDelay);
			co_await delay(self->startDelay);
			TraceEvent("DDMetricsStarting").log();
			double startTime = now();
			while (true) {
				co_await delay(2.5);
				int dif = co_await self->getHighPriorityRelocationsInFlight(cx, self);
				TraceEvent("DDMetricsCheck").detail("DIF", dif);
				if (dif == 0) {
					self->ddDone = now() - startTime;
					co_return;
				}
			}
		} catch (Error& e) {
			TraceEvent("DDMetricsError").error(e);
		}
	}

	Future<Void> start(Database const& cx) override { return clientId == 0 ? work(cx, this) : Void(); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override { m.emplace_back("DDDuration", ddDone, Averaged::False); }
};

WorkloadFactory<DDMetricsWorkload> DDMetricsWorkloadFactory;
