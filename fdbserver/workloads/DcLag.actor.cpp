/*
 * DcLag.actor.cpp
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
#include "fdbclient/StatusClient.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/CodeProbe.h"
#include "flow/NetworkAddress.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// This workload tests a gray failure scenario: a satellite TLog is have network issue
// for sending packets to the remote data center's log routers. This will cause these
// log routers to fail to progress, and causing data center lag, i.e., lag between the
// primary and remote DC's tlogs. With changes to log routers, they can detect that the
// peek is taking a long time (> LOG_ROUTER_PEEK_SWITCH_DC_TIME) and switch to use
// another DC to get data, thus recovering from the data center lag.
struct DcLagWorkload : TestWorkload {
	static constexpr auto NAME = "DcLag";
	bool enabled;
	double testDuration;
	double startDelay;
	std::vector<std::pair<IPAddress, IPAddress>> cloggedPairs;

	DcLagWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 1000.0);
		startDelay = getOption(options, "startDelay"_sr, 10.0);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && enabled)
			return timeout(reportErrors(clogClient(this, cx), "DcLagError"), testDuration, Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Clog a satellite tlog with all remote processes so that this triggers high
	// data center lag.
	bool clogTlog(double seconds) {
		ASSERT(dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);

		std::vector<IPAddress> ips; // all remote process IPs
		for (const auto& process : g_simulator->getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (process->locality.dcId().present() && process->locality.dcId() == g_simulator->remoteDcId) {
				ips.push_back(ip);
			}
		}
		ASSERT(ips.size() > 0);

		// Find all primary satellite tlogs
		std::vector<NetworkAddress> logs; // all primary satellite logs
		for (const auto& tlogset : dbInfo->get().logSystemConfig.tLogs) {
			if (!tlogset.isLocal || tlogset.locality != tagLocalitySatellite)
				continue;
			for (const auto& log : tlogset.tLogs) {
				const NetworkAddress& addr = log.interf().address();
				logs.push_back(addr);
			}
		}
		if (logs.empty()) {
			// No satellite tlog found: fearless config may not have satellites
			return false;
		}

		// clog pairs
		auto tlog = logs[0].ip;
		for (const auto& ip : ips) {
			if (tlog != ip) {
				// Clog TLogReply messages, but allow peek/pop requests
				// g_simulator->clogPair(ip, tlog, seconds);
				g_simulator->clogPair(tlog, ip, seconds);
				cloggedPairs.emplace_back(tlog, ip);
			}
		}
		return true;
	}

	void unclogAll() {
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			g_simulator->unclogPair(pair.first, pair.second);
		}
		cloggedPairs.clear();
	}

	ACTOR static Future<Optional<double>> fetchDatacenterLag(DcLagWorkload* self, Database cx) {
		StatusObject result = wait(StatusClient::statusFetcher(cx));
		StatusObjectReader statusObj(result);
		StatusObjectReader statusObjCluster;
		if (!statusObj.get("cluster", statusObjCluster)) {
			TraceEvent("DcLagNoCluster");
			return Optional<double>();
		}

		StatusObjectReader dcLag;
		if (!statusObjCluster.get("datacenter_lag", dcLag)) {
			TraceEvent("DcLagNoLagData");
			return Optional<double>();
		}

		Version versions = 0;
		double seconds = 0;
		if (!dcLag.get("versions", versions)) {
			TraceEvent("DcLagNoVersions");
			return Optional<double>();
		}
		if (!dcLag.get("seconds", seconds)) {
			TraceEvent("DcLagNoSeconds");
			return Optional<double>();
		}
		TraceEvent("DcLag").detail("Versions", versions).detail("Seconds", seconds);
		return seconds;
	}

	ACTOR Future<Void> clogClient(DcLagWorkload* self, Database cx) {
		wait(delay(self->startDelay));

		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		double startTime = now();
		double workloadEnd = now() + self->testDuration;
		TraceEvent("DcLag").detail("StartTime", startTime).detail("EndTime", workloadEnd);

		// Clog and wait for recovery to happen
		if (!self->clogTlog(self->testDuration)) {
			return Void(); // skip the test if no satellite found
		}

		state Future<Optional<double>> status = Never();
		state bool lagged = false;
		loop choose {
			when(wait(delay(5.0))) {
				// Fetch DC lag every 5s
				status = fetchDatacenterLag(self, cx);
			}
			when(Optional<double> lag = wait(status)) {
				if (lag.present() && lag.get() >= SERVER_KNOBS->LOG_ROUTER_PEEK_SWITCH_DC_TIME - 10.0) {
					// Detect DC Lag happened before Log router switch DC reactions
					lagged = true;
					TraceEvent("DcLagDetected");
				}
				if (lagged && lag.present() && lag.get() < 5.0) {
					TraceEvent("DcLagRecovered");
					self->unclogAll();
					return Void();
				}
				status = Never();
			}
		}
	}
};

WorkloadFactory<DcLagWorkload> DcLagWorkloadFactory;