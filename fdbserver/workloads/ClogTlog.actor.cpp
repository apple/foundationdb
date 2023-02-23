/*
 * ClogTlog.actor.cpp
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

#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
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

struct ClogTlogWorkload : TestWorkload {
	bool enabled;
	double testDuration;
	bool processClassUpdated = false;
	Optional<NetworkAddress> tlog; // the tlog to be clogged with all other processes except the CC

	ClogTlogWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, LiteralStringRef("testDuration"), 100.0);
	}

	std::string description() const override {
		if (&g_simulator == g_network)
			return "ClogTlog";
		else
			return "NoRC";
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (&g_simulator == g_network && enabled)
			return timeout(reportErrors(clogClient(this, cx), "ClogTlogError"), testDuration, Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR void doClog(ISimulator::ProcessInfo* machine, double t, double delay = 0.0) {
		wait(::delay(delay));
		g_simulator.clogInterface(machine->address.ip, t);
	}

	// Clog a random tlog with all proxies so that this triggers a recovery
	// and the recovery will become stuck.
	ACTOR static Future<Void> clogTlog(ClogTlogWorkload* self, Database cx, double seconds) {
		ASSERT(self->dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);

		IPAddress cc = self->dbInfo->get().clusterInterface.address().ip;
		std::vector<IPAddress> ips;
		std::set<IPAddress> candidates;
		for (const auto& process : g_simulator.getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (cc == ip) {
				TraceEvent("ClogTlogSkipCC").detail("IP", ip);
			} else {
				if (process->startingClass == ProcessClass::LogClass) {
					candidates.insert(ip);
				}
				ips.push_back(ip);
			}
		}
		ASSERT(ips.size() > 0);

		// Find all primary tlogs
		std::vector<NetworkAddress> logs;
		bool found = false;
		for (int i = 0; i < self->dbInfo->get().logSystemConfig.tLogs.size(); i++) {
			const auto& tlogset = self->dbInfo->get().logSystemConfig.tLogs[i];
			if (!tlogset.isLocal)
				continue;
			for (const auto& log : tlogset.tLogs) {
				const NetworkAddress& addr = log.interf().address();
				if (cc == addr.ip) {
					TraceEvent("ClogTlogSkipCC").detail("IP", addr.ip);
				} else {
					if (self->tlog.present() && self->tlog.get() == addr) {
						found = true;
					}
					logs.push_back(addr);
				}
				// logs.push_back(addr.ip);
			}
		}
		ASSERT(logs.size() > 0);

		// Clog all proxies and one tlogs
		if (!found) {
			// Choose a tlog, preferring the one of "log" class
			self->tlog.reset();
			for (const auto& log : logs) {
				if (candidates.find(log.ip) != candidates.end()) {
					self->tlog = log;
					break;
				}
			}
			if (!self->tlog.present()) {
				self->tlog = logs[0];
			}
		}
		for (const auto& proxy : ips) {
			if (proxy != self->tlog.get().ip) {
				g_simulator.clogPair(proxy, self->tlog.get().ip, seconds);
				g_simulator.clogPair(self->tlog.get().ip, proxy, seconds);
			}
		}
		return Void();
	}

	ACTOR static Future<Void> excludeFailedLog(ClogTlogWorkload* self, Database cx) {
		state Future<Void> timeout = delay(30);

		loop choose {
			when(wait(self->dbInfo->onChange())) {
				if (self->dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
					return Void();
				}
				timeout = delay(30);
			}
			when(wait(timeout)) {
				// recovery state hasn't changed in 10s, exclude the failed tlog
				std::vector<StringRef> tokens;
				state Optional<ConfigureAutoResult> conf;

				// tokens.push_back("configure"_sr);
				TraceEvent("ExcludeFailedLog").detail("TLog", self->tlog.get());
				tokens.push_back(StringRef("exclude=" + self->tlog.get().toString()));
				ConfigurationResult r =
				    wait(ManagementAPI::changeConfig(cx.getReference(), tokens, conf, /*force=*/true));
				TraceEvent("ExcludeFailedLog")
				    .detail("Result", r)
				    .detail("ConfigIsValid", conf.present() && conf.get().isValid());
				return Void();
			}
		}
	}

	ACTOR Future<Void> clogClient(ClogTlogWorkload* self, Database cx) {
		// Let cycle workload issue some transactions.
		wait(delay(20.0));

		double startTime = now();
		state double workloadEnd = now() + self->testDuration;
		TraceEvent("ClogTlog").detail("StartTime", startTime).detail("EndTime", workloadEnd);
		state Future<Void> excludeLog;
		loop {
			while (self->dbInfo->get().recoveryState < RecoveryState::RECOVERY_TRANSACTION) {
				wait(self->dbInfo->onChange());
			}

			wait(clogTlog(self, cx, workloadEnd - now()));
			loop choose {
				when(wait(self->dbInfo->onChange())) {
					if (self->dbInfo->get().recoveryState < RecoveryState::RECOVERY_TRANSACTION) {
						break;
					}
					if (self->dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
						TraceEvent("ClogDoneFullyRecovered");
						return Void();
					}
				}
				when(wait(delayUntil(workloadEnd))) {
					// Expect to reach fully recovered state before workload ends
					TraceEvent(SevError, "ClogTlogFailure").detail("RecoveryState", self->dbInfo->get().recoveryState);
					return Void();
				}
			}
			if (self->dbInfo->get().recoveryState <= RecoveryState::ALL_LOGS_RECRUITED) {
				excludeLog = excludeFailedLog(self, cx);
			}
		}
	}
};

WorkloadFactory<ClogTlogWorkload> ClogTlogWorkloadFactory("ClogTlog");
