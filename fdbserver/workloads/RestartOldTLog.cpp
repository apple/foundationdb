/*
 * RestartOldTLog.cpp
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

#include "flow/flow.h"

#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/core/LogSystemConfig.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"
#include "flow/CoroUtils.h"
#include "flow/Trace.h"

// A generation that has completed keeps being advertised through ServerDBInfo for as long as the cluster
// controller lives, because storage servers may still be replaying it. This workload restarts one of those
// TLog processes while the cluster is fully recovered, and requires that the advertised interface be
// replaced by the restarted process's endpoints.
//
// The interface used to go stale permanently: a storage server replaying that generation peeked an endpoint
// that no longer existed, so it never reached the following generation and never learned it had been removed
// from serverList. Its worker then rejected every new storage server for the rest of the run, which surfaces
// far away as a consistency check reporting a worker with no storage server.
struct RestartOldTLogWorkload : TestWorkload {
	static constexpr auto NAME = "RestartOldTLog";

	bool enabled;
	double startDelay;
	double searchTimeout;
	double refreshTimeout;
	int attempts;

	explicit RestartOldTLogWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		startDelay = getOption(options, "startDelay"_sr, 30.0);
		searchTimeout = getOption(options, "searchTimeout"_sr, 120.0);
		refreshTimeout = getOption(options, "refreshTimeout"_sr, 120.0);
		attempts = getOption(options, "attempts"_sr, 5);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && enabled) {
			return restartOldTLog(this);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static std::set<NetworkAddress> currentGenerationAddresses(LogSystemConfig const& config) {
		std::set<NetworkAddress> addresses;
		for (const auto& logSet : config.tLogs) {
			for (const auto& log : logSet.tLogs) {
				if (log.present()) {
					addresses.insert(log.interf().address());
				}
			}
		}
		return addresses;
	}

	// Restarting a process that also serves the current generation forces a recovery, which relearns every
	// interface and would make the test vacuous. The cluster controller and simulator-protected processes are
	// skipped for the same reason: rebooting either risks a recovery.
	static Optional<TLogInterface> findOldGenerationTLog(LogSystemConfig const& config, NetworkAddress ccAddress) {
		const std::set<NetworkAddress> current = currentGenerationAddresses(config);
		for (const auto& oldGeneration : config.oldTLogs) {
			for (const auto& logSet : oldGeneration.tLogs) {
				for (const auto& log : logSet.tLogs) {
					if (!log.present()) {
						continue;
					}
					const NetworkAddress address = log.interf().address();
					if (!current.contains(address) && address != ccAddress &&
					    !g_simulator->isProtectedAddress(address)) {
						return log.interf();
					}
				}
			}
		}
		return Optional<TLogInterface>();
	}

	static Optional<Endpoint> advertisedCommitEndpoint(LogSystemConfig const& config, UID logId) {
		for (const auto& oldGeneration : config.oldTLogs) {
			for (const auto& logSet : oldGeneration.tLogs) {
				for (const auto& log : logSet.tLogs) {
					if (log.id() == logId && log.present()) {
						return log.interf().commit.getEndpoint();
					}
				}
			}
		}
		return Optional<Endpoint>();
	}

	// Outcome of restarting one candidate. A recovery re-locks and relearns every generation, so an attempt
	// it interrupts proves nothing and has to be replaced by another one.
	enum class Outcome { Refreshed, Inconclusive, Stale };

	Future<Void> restartOldTLog(RestartOldTLogWorkload* self) {
		co_await delay(self->startDelay);

		for (int attempt = 0; attempt < self->attempts; ++attempt) {
			Optional<TLogInterface> target = co_await self->awaitCandidate(self);
			if (!target.present()) {
				// A run can legitimately never leave a completed generation advertised on a process of its own.
				TraceEvent("RestartOldTLogNoCandidate").detail("Attempt", attempt);
				co_return;
			}

			Outcome outcome = co_await self->restartAndAwaitRefresh(self, target.get());
			if (outcome == Outcome::Refreshed) {
				co_return;
			}
			if (outcome == Outcome::Stale) {
				TraceEvent(SevError, "RestartOldTLogStaleInterface")
				    .detail("TLog", target.get().id())
				    .detail("Address", target.get().address())
				    .detail("Waited", self->refreshTimeout);
				co_return;
			}
		}
		TraceEvent("RestartOldTLogInconclusive").detail("Attempts", self->attempts);
	}

	Future<Optional<TLogInterface>> awaitCandidate(RestartOldTLogWorkload* self) {
		const double deadline = now() + self->searchTimeout;
		while (true) {
			if (self->dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
				Optional<TLogInterface> target = findOldGenerationTLog(self->dbInfo->get().logSystemConfig,
				                                                       self->dbInfo->get().clusterInterface.address());
				if (target.present()) {
					co_return target;
				}
			}
			if (now() >= deadline) {
				co_return Optional<TLogInterface>();
			}
			co_await race(self->dbInfo->onChange(), delay(1.0));
		}
	}

	Future<Outcome> restartAndAwaitRefresh(RestartOldTLogWorkload* self, TLogInterface target) {
		auto* process = g_simulator->getProcessByAddress(target.address());
		ASSERT(process != nullptr);

		CODE_PROBE(true, "Restarted a TLog process serving only completed generations");
		TraceEvent("RestartOldTLogRestarting").detail("TLog", target.id()).detail("Address", target.address());
		g_simulator->rebootProcess(process, ISimulator::KillType::RebootProcess);

		const Endpoint staleCommit = target.commit.getEndpoint();
		const double deadline = now() + self->refreshTimeout;
		while (true) {
			if (self->dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
				TraceEvent("RestartOldTLogRecoveryIntervened").detail("TLog", target.id());
				co_return Outcome::Inconclusive;
			}
			const Optional<Endpoint> advertised =
			    advertisedCommitEndpoint(self->dbInfo->get().logSystemConfig, target.id());
			if (!advertised.present()) {
				// Nobody can still be replaying a generation that is no longer advertised.
				TraceEvent("RestartOldTLogGenerationDropped").detail("TLog", target.id());
				co_return Outcome::Inconclusive;
			}
			if (advertised.get() != staleCommit) {
				TraceEvent("RestartOldTLogRefreshed").detail("TLog", target.id()).detail("Address", target.address());
				co_return Outcome::Refreshed;
			}
			if (now() >= deadline) {
				co_return Outcome::Stale;
			}
			co_await race(self->dbInfo->onChange(), delay(1.0));
		}
	}
};

WorkloadFactory<RestartOldTLogWorkload> RestartOldTLogWorkloadFactory;
