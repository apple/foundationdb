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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/Locality.h"
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
	Optional<IPAddress> tlog; // the tlog to be clogged with all other processes except the CC

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

	// Change satellite tlogs process class to logs. This is needed because
	// simulator starts with "unset" class for all satellite processes
	ACTOR static Future<Void> updateSatelliteProcessClass(ClogTlogWorkload* self, Database cx) {
		if (self->processClassUpdated)
			return Void();
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);

				for (int i = 0; i < self->dbInfo->get().logSystemConfig.tLogs.size(); i++) {
					const auto& tlogset = self->dbInfo->get().logSystemConfig.tLogs[i];
					if (!tlogset.isLocal)
						continue;
					for (const auto& log : tlogset.tLogs) {
						const NetworkAddress& addr = log.interf().address();
						Key key =
						    Key("process/class_type/" + formatIpPort(addr.ip, addr.port))
						        .withPrefix(
						            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
						tx->set(key, "log"_sr);
					}
				}

				wait(tx->commit());
				self->processClassUpdated = true;
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				TraceEvent("ClogTlogSetProcessClassError").error(e);
				wait(tx->onError(e));
			}
		}
	}

	// Clog a random tlog with all proxies so that this triggers a recovery
	// and the recovery will become stuck.
	ACTOR static Future<Void> clogTlog(ClogTlogWorkload* self, Database cx, double seconds) {
		ASSERT(self->dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);
		// wait(updateSatelliteProcessClass(self, cx));

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
		std::vector<IPAddress> logs;
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
					if (self->tlog.present() && self->tlog.get() == addr.ip) {
						found = true;
					}
					logs.push_back(addr.ip);
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
				if (candidates.find(log) != candidates.end()) {
					self->tlog = log;
					break;
				}
			}
			if (!self->tlog.present()) {
				self->tlog = logs[0];
			}
		}
		for (const auto& proxy : ips) {
			g_simulator.clogPair(proxy, self->tlog.get(), seconds);
		}
		return Void();
	}

	ACTOR Future<Void> clogClient(ClogTlogWorkload* self, Database cx) {
		double startTime = now();
		state double workloadEnd = now() + self->testDuration;
		TraceEvent("ClogTlog").detail("StartTime", startTime).detail("EndTime", workloadEnd);
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
				}
				when(wait(delayUntil(workloadEnd))) {
					// Clog a random tlog and all proxies
					TraceEvent("ClogDone").detail("RecoveryState", self->dbInfo->get().recoveryState);
					return Void();
				}
			}
		}
	}
};

WorkloadFactory<ClogTlogWorkload> ClogTlogWorkloadFactory("ClogTlog");
