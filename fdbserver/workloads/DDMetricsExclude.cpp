/*
 * DDMetricsExclude.cpp
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

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbclient/StatusClient.h"
#include "fdbserver/tester/workloads.actor.h"

struct DDMetricsExcludeWorkload : TestWorkload {
	static constexpr auto NAME = "DDMetricsExclude";
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
		excludeIp = getOption(options, "excludeIp"_sr, Value("127.0.0.1"_sr));
		excludePort = getOption(options, "excludePort"_sr, 4500);
	}

	static Value getRandomValue() {
		return Standalone<StringRef>(format("Value/%080d", deterministicRandom()->randomInt(0, 10e6)));
	}

	Future<double> getMovingDataAmount(Database cx) {
		try {
			StatusObject statusObj = co_await StatusClient::statusFetcher(cx);
			StatusObjectReader statusObjCluster;
			((StatusObjectReader)statusObj).get("cluster", statusObjCluster);
			StatusObjectReader statusObjData;
			statusObjCluster.get("data", statusObjData);
			if (statusObjData.has("moving_data")) {
				StatusObjectReader movingData = statusObjData.last();
				double dataInQueue, dataInFlight;
				if (movingData.get("in_queue_bytes", dataInQueue) && movingData.get("in_flight_bytes", dataInFlight)) {
					peakInQueue = std::max(peakInQueue, dataInQueue);
					peakInFlight = std::max(peakInFlight, dataInFlight);
					co_return dataInQueue + dataInFlight;
				}
			}
		} catch (Error& e) {
			TraceEvent("DDMetricsExcludeGetMovingDataError").error(e);
			throw;
		}
		co_return -1.0;
	}

	Future<Void> start(Database const& cx) override {
		try {
			std::vector<AddressExclusion> excluded;
			excluded.push_back(AddressExclusion(IPAddress::parse(excludeIp.toString()).get(), excludePort));
			co_await excludeServers(cx, excluded);
			double startTime = now();
			while (true) {
				co_await delay(2.5);
				double movingData = co_await getMovingDataAmount(cx);
				peakMovingData = std::max(peakMovingData, movingData);
				TraceEvent("DDMetricsExcludeCheck").detail("MovingData", movingData);
				if (movingData == 0.0) {
					ddDone = now() - startTime;
					co_return;
				}
			}
		} catch (Error& e) {
			TraceEvent("DDMetricsExcludeError").error(e);
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
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

WorkloadFactory<DDMetricsExcludeWorkload> DDMetricsExcludeWorkloadFactory;
