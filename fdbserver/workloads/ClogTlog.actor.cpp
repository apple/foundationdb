/*
 * ClogTlog.actor.cpp
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

#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
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

// This workload tests a gray failure scenario: a single TLog is network-partitioned from
// the rest of the cluster, except the cluster controller. This will cause proxies to
// fail to progress, and trigger a recovery. During the recovery, if this TLog is again
// recruited, the recovery can become stuck in the initializing_transaction_servers state.
// If that happens, the workload will start a "configure force exclude=IP:PORT" for the
// TLog, which would unblock the recovery to reach fully_recovered state.
struct ClogTlogWorkload : TestWorkload {
	static constexpr auto NAME = "ClogTlog";
	bool enabled;
	double testDuration;
	bool clogged = false;
	bool useDisconnection = false;
	Optional<NetworkAddress> tlog; // the tlog to be clogged with all other processes except the CC
	std::vector<std::pair<IPAddress, IPAddress>> cloggedPairs;

	ClogTlogWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 1000.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && enabled)
			return timeout(reportErrors(clogClient(this, cx), "ClogTlogError"), testDuration, Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Clog a random tlog with all other processes so that this triggers a recovery
	// and the recovery may become stuck if the clogged tlog is recruited again.
	void clogTlog(double seconds) {
		ASSERT(dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);

		IPAddress cc = dbInfo->get().clusterInterface.address().ip;
		std::vector<IPAddress> ips; // all FDB process IPs
		for (const auto& process : g_simulator->getAllProcesses()) {
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
				if (useDisconnection) {
					g_simulator->disconnectPair(ip, tlog.get().ip, seconds);
					g_simulator->disconnectPair(tlog.get().ip, ip, seconds);
				} else {
					g_simulator->clogPair(ip, tlog.get().ip, seconds);
					g_simulator->clogPair(tlog.get().ip, ip, seconds);
				}
				cloggedPairs.emplace_back(ip, tlog.get().ip);
				cloggedPairs.emplace_back(tlog.get().ip, ip);
			}
		}
		clogged = true;
	}

	void unclogAll() {
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			if (useDisconnection) {
				g_simulator->reconnectPair(pair.first, pair.second);
			} else {
				g_simulator->unclogPair(pair.first, pair.second);
			}
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
				CODE_PROBE(true, "Exclude failed tlog");
				TraceEvent("ExcludeFailedLog")
				    .detail("TLog", self->tlog.get())
				    .detail("RecoveryState", self->dbInfo->get().recoveryState);
				std::string modes = "exclude=" + formatIpPort(self->tlog.get().ip, self->tlog.get().port);
				ConfigurationResult r = wait(ManagementAPI::changeConfig(cx.getReference(), modes, /*force=*/true));
				TraceEvent("ExcludeFailedLog").detail("Result", r);
				return Void();
			}
		}
	}

	ACTOR Future<Void> clogClient(ClogTlogWorkload* self, Database cx) {
		if (deterministicRandom()->coinflip()) {
			self->useDisconnection = true;
		}

		// Let cycle workload issue some transactions.
		wait(delay(20.0));

		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		double startTime = now();
		state double workloadEnd = now() + self->testDuration - 10;
		TraceEvent("ClogTlog").detail("StartTime", startTime).detail("EndTime", workloadEnd);

		// Clog and wait for recovery to happen
		self->clogTlog(workloadEnd - now());
		while (self->dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		state bool useGrayFailureToRecover = false;
		if (deterministicRandom()->coinflip() && self->useDisconnection) {
			// Use gray failure instead of exclusion to recover the cluster.
			TraceEvent("ClogTlogUseGrayFailreToRecover").log();
			useGrayFailureToRecover = true;
		}

		// start exclusion and wait for fully recovery. When using gray failure, the cluster should recover by itself
		// eventually.
		state Future<Void> excludeLog = useGrayFailureToRecover ? Never() : excludeFailedLog(self, cx);
		state Future<Void> onChange = self->dbInfo->onChange();
		loop choose {
			when(wait(onChange)) {
				if (self->dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
					TraceEvent("ClogDoneFullyRecovered").log();
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

WorkloadFactory<ClogTlogWorkload> ClogTlogWorkloadFactory;
