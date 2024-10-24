/*
 * LogMetrics.actor.cpp
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

#include "flow/SystemMonitor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/MasterInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct LogMetricsWorkload : TestWorkload {
	static constexpr auto NAME = "LogMetrics";

	std::string dataFolder;
	double logAt, logDuration, logsPerSecond;

	LogMetricsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		logAt = getOption(options, "logAt"_sr, 0.0);
		logDuration = getOption(options, "logDuration"_sr, 30.0);
		logsPerSecond = getOption(options, "logsPerSecond"_sr, 20);
		dataFolder = getOption(options, "dataFolder"_sr, ""_sr).toString();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (clientId)
			return Void();
		return _start(cx, this);
	}

	ACTOR Future<Void> setSystemRate(LogMetricsWorkload* self, Database cx, uint32_t rate) {
		// set worker interval and ss interval
		state BinaryWriter br(Unversioned());
		std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
		// std::vector<Future<Void>> replies;
		TraceEvent("RateChangeTrigger").log();
		SetMetricsLogRateRequest req(rate);
		for (int i = 0; i < workers.size(); i++) {
			workers[i].interf.setMetricsRate.send(req);
		}
		// wait( waitForAll( replies ) );

		br << rate;
		loop {
			state Transaction tr(cx);
			try {
				wait(success(tr.getReadVersion()));
				tr.set(fastLoggingEnabled, br.toValue());
				tr.makeSelfConflicting();
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, LogMetricsWorkload* self) {
		wait(delay(self->logAt));

		wait(self->setSystemRate(self, cx, self->logsPerSecond));
		wait(timeout(recurring(&systemMonitor, 1.0 / self->logsPerSecond), self->logDuration, Void()));

		// We're done, set everything back
		wait(self->setSystemRate(self, cx, 1.0));

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<LogMetricsWorkload> LogMetricsWorkloadFactory;
