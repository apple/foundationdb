/*
 * DDMetricsExclude.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/StatusClient.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DDMetricsExcludeWorkload : TestWorkload {
	double ddDone;
	Value excludeIp;
	int excludePort;
	double peakMovingData;
	double peakInQueue;
	double peakInFlight;
	double movingDataPerSec;

	DDMetricsExcludeWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), ddDone(0.0), peakMovingData(0.0), peakInQueue(0.0), peakInFlight(0.0),
	    movingDataPerSec(0.0) {
		excludeIp = getOption(options, LiteralStringRef("excludeIp"), Value(LiteralStringRef("127.0.0.1")));
		excludePort = getOption(options, LiteralStringRef("excludePort"), 4500);
	}

	static Value getRandomValue() {
		return Standalone<StringRef>(format("Value/%080d", deterministicRandom()->randomInt(0, 10e6)));
	}

	ACTOR static Future<double> getMovingDataAmount(Database cx, DDMetricsExcludeWorkload* self) {
		try {
			StatusObject statusObj = wait(StatusClient::statusFetcher(cx));
			StatusObjectReader statusObjCluster;
			((StatusObjectReader)statusObj).get("cluster", statusObjCluster);
			StatusObjectReader statusObjData;
			statusObjCluster.get("data", statusObjData);
			if (statusObjData.has("moving_data")) {
				StatusObjectReader movingData = statusObjData.last();
				double dataInQueue, dataInFlight;
				if (movingData.get("in_queue_bytes", dataInQueue) && movingData.get("in_flight_bytes", dataInFlight)) {
					self->peakInQueue = std::max(self->peakInQueue, dataInQueue);
					self->peakInFlight = std::max(self->peakInFlight, dataInFlight);
					return dataInQueue + dataInFlight;
				}
			}
		} catch (Error& e) {
			TraceEvent("DDMetricsExcludeGetMovingDataError").error(e);
			throw;
		}
		return -1.0;
	}

	ACTOR static Future<Void> _start(Database cx, DDMetricsExcludeWorkload* self) {
		try {
			state std::vector<AddressExclusion> excluded;
			excluded.push_back(AddressExclusion(IPAddress::parse(self->excludeIp.toString()).get(), self->excludePort));
			wait(excludeServers(cx, excluded));
			state double startTime = now();
			loop {
				wait(delay(2.5));
				double movingData = wait(self->getMovingDataAmount(cx, self));
				self->peakMovingData = std::max(self->peakMovingData, movingData);
				TraceEvent("DDMetricsExcludeCheck").detail("MovingData", movingData);
				if (movingData == 0.0) {
					self->ddDone = now() - startTime;
					return Void();
				}
			}
		} catch (Error& e) {
			TraceEvent("DDMetricsExcludeError").error(e);
		}
		return Void();
	}

	std::string description() const override { return "Data Distribution Metrics Exclude"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	Future<bool> check(Database const& cx) override {
		movingDataPerSec = peakMovingData / ddDone;
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("peakMovingData", peakMovingData, Averaged::False);
		m.emplace_back("peakInQueue", peakInQueue, Averaged::False);
		m.emplace_back("peakInFlight", peakInFlight, Averaged::False);
		m.emplace_back("DDDuration", ddDone, Averaged::False);
		m.emplace_back("movingDataPerSec", movingDataPerSec, Averaged::False);
	}
};

WorkloadFactory<DDMetricsExcludeWorkload> DDMetricsExcludeWorkloadFactory("DDMetricsExclude");
