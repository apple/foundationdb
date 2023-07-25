/*
 * DisconnectCCToSS.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DisconnectCCToSSWorkload : TestWorkload {
	bool enabled;
	double testDuration;
	bool clogged = false;

	DisconnectCCToSSWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, LiteralStringRef("testDuration"), 100.0);
	}

	std::string description() const override { return "DisconnectCCToSS"; }

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (&g_simulator == g_network && enabled)
			return timeout(reportErrors(runner(this, cx), "DisconnectCCToSSError"), testDuration, Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Clog a random tlog with all other processes so that this triggers a recovery
	// and the recovery may become stuck if the clogged tlog is recruited again.
	ACTOR Future<NetworkAddress> DisconnectCCToSS(Database cx, IPAddress cc, double seconds) {
		state std::map<NetworkAddress, std::vector<int> > servers;
        state Transaction tr(cx);
        tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
        tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
        tr.setOption(FDBTransactionOptions::LOCK_AWARE);
       	std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
        for (auto& ssi : interfs) {
            if (g_simulator.protectedAddresses.count(ssi.address()) == 0) {
                servers[NetworkAddress(ssi.address().ip, 0)].push_back(ssi.address().port);
            }
        }
        Optional<Standalone<StringRef>> value = wait(tr.get(logsKey));
        ASSERT(value.present());
        auto logs = decodeLogsValue(value.get());
        for (auto const& log : logs.first) {
            servers.erase(NetworkAddress(log.second.ip, 0));
        }
        if (servers.empty()) {
            // sometimes all SS are running alongside a TLog
            TraceEvent("Hfu5QuitEarlyNoEligibleSSToClog").log();
            return NetworkAddress();
        }
       	NetworkAddress ss(servers.begin()->first.ip, *(servers.begin()->second.begin()));
        g_simulator.clogPair(cc, ss.ip, seconds);
        g_simulator.clogPair(ss.ip, cc, seconds);

		TraceEvent("Hfu5Clog").detail("Seconds", seconds).detail("CCIP", cc).detail("SSIP", ss.ip).log();
		return ss;
	}

	ACTOR static Future<Void> fetchRoles(Database cx, NetworkAddress ss) {
		StatusObject result = wait(StatusClient::statusFetcher(cx));
		StatusObjectReader statusObj(result);
		StatusObjectReader statusObjCluster;
		state StatusObjectReader processesMap;

		if (!statusObj.get("cluster", statusObjCluster)) {
			TraceEvent(SevError, "Hfu5NoCluster").log();
			return Void();
		}

		if (!statusObjCluster.get("processes", processesMap)) {
			TraceEvent(SevError, "Hfu5NoProcesses").log();
			return Void();
		}
		for (auto proc : processesMap.obj()) {
			StatusObjectReader process(proc.second);
			if (!process.has("address")) {
				TraceEvent("Hfu5NoAddresss").log();
			}
			TraceEvent("Hfu4Compare").detail("Addr1", process["address"].get_str()).detail("Addr2", ss.toString()).log();
			if (process.has("roles")) {
				StatusArray rolesArray = proc.second.get_obj()["roles"].get_array();
				for (StatusObjectReader role : rolesArray) {
					TraceEvent("Hfu6CheckRole")
						.detail("Addr1", process["address"].get_str()).detail("Role", role["role"].get_str()).log();
				}
			} else {
				TraceEvent(SevError, "Hfu5NoRoles-1111").log();
			}
			if (process["address"].get_str() == ss.toString()) {
				TraceEvent("Hfu5Found").detail("Addr1", process["address"].get_str()).detail("Addr2", ss.toString()).log();
				if (process.has("roles")) {
					StatusArray rolesArray = proc.second.get_obj()["roles"].get_array();
					for (StatusObjectReader role : rolesArray) {
						if (role["role"].get_str() == "storage") {
							TraceEvent(SevError, "Hfu5ShouldNotReportStorage").log();
						}
					}
					TraceEvent("Hfu5Succeed").log();
				} else {
					TraceEvent(SevError, "Hfu5NoRoles").log();
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> runner(DisconnectCCToSSWorkload* self, Database cx) {
		// Let cycle workload issue some transactions.
		wait(delay(20.0));
		wait(fetchRoles(cx, NetworkAddress()));

		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		double startTime = now();
		state double workloadEnd = now() + self->testDuration;
		TraceEvent("DisconnectCCToSS").detail("StartTime", startTime).detail("EndTime", workloadEnd);
		IPAddress cc = self->dbInfo->get().clusterInterface.address().ip;
		state NetworkAddress ss = wait(self->DisconnectCCToSS(cx, cc, workloadEnd - now()));
		if (ss == NetworkAddress()) {
			// no eligible SS
			return Void();
		}
		state int i = 0;
		loop {
			wait(delay(20.0));
			wait(fetchRoles(cx, ss));
			if (++i >= 5) {
				break;
			}
		}
		return Void();
	}
};

WorkloadFactory<DisconnectCCToSSWorkload> DisconnectCCToSSWorkloadFactory("DisconnectCCToSS");
