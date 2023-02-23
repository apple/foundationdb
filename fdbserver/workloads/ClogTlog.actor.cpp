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
	bool clogged = false;
	Optional<NetworkAddress> tlog; // the tlog to be clogged with all other processes except the CC
	std::vector<std::pair<IPAddress, IPAddress>> cloggedPairs;

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

	// Clog a random tlog with all other processes so that this triggers a recovery
	// and the recovery may become stuck if the clogged tlog is recruited again.
	void clogTlog(double seconds) {
		ASSERT(dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);

		IPAddress cc = dbInfo->get().clusterInterface.address().ip;
		std::vector<IPAddress> ips; // all FDB process IPs
		for (const auto& process : g_simulator.getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (process->startingClass != ProcessClass::TesterClass) {
				ips.push_back(ip);
			}
		}
		ASSERT(ips.size() > 0);

		// Find all primary tlogs
		std::vector<NetworkAddress> logs; // all primary logs except CC
		for (int i = 0; i < dbInfo->get().logSystemConfig.tLogs.size(); i++) {
			const auto& tlogset = dbInfo->get().logSystemConfig.tLogs[i];
			if (!tlogset.isLocal)
				continue;
			for (const auto& log : tlogset.tLogs) {
				const NetworkAddress& addr = log.interf().address();
				if (cc != addr.ip) {
					if (!tlog.present()) {
						tlog = addr;
					}
					logs.push_back(addr);
				}
			}
		}
		ASSERT(logs.size() > 0 && tlog.present());

		// clog pairs
		for (const auto& ip : ips) {
			if (ip != tlog.get().ip && ip != cc) {
				g_simulator.clogPair(ip, tlog.get().ip, seconds);
				g_simulator.clogPair(tlog.get().ip, ip, seconds);
				cloggedPairs.emplace_back(ip, tlog.get().ip);
				cloggedPairs.emplace_back(tlog.get().ip, ip);
			}
		}
		clogged = true;
	}

	void unclogAll() {
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			g_simulator.unclogPair(pair.first, pair.second);
		}
		cloggedPairs.clear();
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
				// recovery state hasn't changed in 30s, exclude the failed tlog
				std::vector<StringRef> tokens;
				state Optional<ConfigureAutoResult> conf;

				TEST(true); // Exclude failed tlog
				TraceEvent("ExcludeFailedLog")
				    .detail("TLog", self->tlog.get())
				    .detail("RecoveryState", self->dbInfo->get().recoveryState);
				tokens.push_back(StringRef("exclude=" + formatIpPort(self->tlog.get().ip, self->tlog.get().port)));
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

		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		double startTime = now();
		state double workloadEnd = now() + self->testDuration;
		TraceEvent("ClogTlog").detail("StartTime", startTime).detail("EndTime", workloadEnd);

		// Clog and wait for recovery to happen
		self->clogTlog(workloadEnd - now());
		while (self->dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		// start exclusion and wait for fully recovery
		state Future<Void> excludeLog = excludeFailedLog(self, cx);
		state Future<Void> onChange = self->dbInfo->onChange();
		loop choose {
			when(wait(onChange)) {
				if (self->dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
					TraceEvent("ClogDoneFullyRecovered");
					self->unclogAll();
					return Void();
				}
				onChange = self->dbInfo->onChange();
			}
			when(wait(delayUntil(workloadEnd))) {
				// Expect to reach fully recovered state before workload ends
				TraceEvent(SevError, "ClogTlogFailure").detail("RecoveryState", self->dbInfo->get().recoveryState);
				return Void();
			}
		}
	}
};

WorkloadFactory<ClogTlogWorkload> ClogTlogWorkloadFactory("ClogTlog");
