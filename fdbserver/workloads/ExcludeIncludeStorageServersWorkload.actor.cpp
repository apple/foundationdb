/*
 * ExcludeIncludeStorageServersWorkload.actor.cpp
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
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// This test creates a scenario that large number of SS join the cluster, by excluding and including a random
// SS consistently for many times.
// this test would quit early in two scenarios:
// 		1. QuitEarlyNoEligibleSSToExclude: All storage servers are running alongside with a TLog, thus we cannot
//			exclude a SS, otherwise DB would be unavailable. (this is due to simulation uses UNSET process class)
//		2. QuitEarlyNotCompleteServerExclude: Sometimes it takes too long for a SS exclusion to finish(disappear from
//			serverListKeys), there is a timeout_error when that happens, and we quit if it never succeeded( i.e.)
// It makes sense because the purpose of this test is to :
// 		1) create a scenario that large number of SS join/quit.
// 		2) then verify RateKeeper is only bookkeeping required processes. (delete the SS that has quit)
// it's okay to quit if we can't create such scenario in rare cases, as long as in most cases we can create the scenario
// we have seen the case RK bookkeeping large number of unnecessary SS:
// https://github.com/apple/foundationdb/issues/10260
struct ExcludeIncludeStorageServersWorkload : TestWorkload {
	static constexpr auto NAME = "ExcludeIncludeStorageServers";
	bool enabled;

	std::map<AddressExclusion, std::set<AddressExclusion>> machineProcesses; // ip -> ip:port

	ExcludeIncludeStorageServersWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled =
		    !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		if (g_network->isSimulated()) {
			g_simulator->allowLogSetKills = false;
		}
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		// Failure injection workloads like Rollback, Attrition and so on are interfering with the test.
		// In particular, this test expects the storage server can be excluded and included in time
		// to test that ratekeeper's bookkeeping works fine.
		// Consequently, we disable all failure injection workloads in background for this test
		out.insert("all");
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();
		return workloadMain(this, cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>&) override {}

	ACTOR static Future<Void> workloadMain(ExcludeIncludeStorageServersWorkload* self, Database cx) {
		state int round = deterministicRandom()->randomInt(10, 80);
		state std::set<AddressExclusion> servers;
		state std::set<AddressExclusion>::iterator it;
		state Transaction tr(cx);
		state double timeout = 100.0;
		state int timeoutCnt = 0;
		state int maxTimeout = std::min(30, round);
		TraceEvent("WorkloadStart").detail("Round", round).log();
		loop {
			if (timeoutCnt > maxTimeout) {
				TraceEvent("QuitEarlyNotCompleteServerExclude").log();
				break;
			}
			try {
				servers.clear();
				// including an invalid address means include everything(clear all exclude prefix)
				wait(includeServers(cx, std::vector<AddressExclusion>(1)));

				tr = Transaction(cx);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				// get all storage servers
				std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
				    wait(NativeAPI::getServerListAndProcessClasses(&tr));
				for (auto& [ssi, p] : results) {
					if (g_simulator->protectedAddresses.count(ssi.address()) == 0) {
						servers.insert(AddressExclusion(ssi.address().ip, ssi.address().port));
					}
				}

				// get all TLogs
				Optional<Standalone<StringRef>> value = wait(tr.get(logsKey));
				ASSERT(value.present());
				auto logs = decodeLogsValue(value.get());
				for (auto const& log : logs.first) {
					servers.erase(AddressExclusion(log.second.ip, log.second.port));
				}
				if (servers.empty()) {
					// sometimes all SS are running alongside a TLog, cannot exclude any of them, so quit
					TraceEvent("QuitEarlyNoEligibleSSToExclude").log();
					break;
				}

				// find a SS and exclude it
				it = std::next(servers.begin(), deterministicRandom()->randomInt(0, servers.size()));
				wait(excludeServers(cx, std::vector<AddressExclusion>{ *it }));
				// timeoutError() is needed because sometimes excluding process can take forever
				std::set<NetworkAddress> inProgress = wait(
				    timeoutError(checkForExcludingServers(cx, std::vector<AddressExclusion>{ *it }, true), timeout));
				ASSERT(inProgress.empty());
				if (--round <= 0) {
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_timed_out) {
					// it might never be excluded from serverList
					timeoutCnt++;
					continue;
				}
				wait(tr.onError(e));
			}
		}
		// if it is still in the middle of a exclude, then DD cannot finish and test would timeout
		wait(includeServers(cx, std::vector<AddressExclusion>(1)));

		TraceEvent("WorkloadFinish").detail("QuitEarly", round > 0).detail("TimeoutCount", timeoutCnt).log();
		return Void();
	}
};

WorkloadFactory<ExcludeIncludeStorageServersWorkload> ExcludeIncludeStorageServersWorkloadFactory;
