/*
 * TLogWriteAntiQuorum.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"
#include "flow/ScopeExit.h"

struct TLogWriteAntiQuorumWorkload : TestWorkload {
	static constexpr auto NAME = "TLogWriteAntiQuorum";

	const bool requireUnicast;

	explicit TLogWriteAntiQuorumWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), requireUnicast(getOption(options, "requireUnicast"_sr, false)) {}

	Future<Void> start(Database const& cx) override { return clientId == 0 ? run(cx) : Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> waitForNextEpoch(LogEpoch epoch, RecoveryState state) {
		while (dbInfo->get().logSystemConfig.epoch <= epoch || dbInfo->get().recoveryState < state) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> run(Database cx) {
		ASSERT(g_network->isSimulated());
		if (requireUnicast) {
			ASSERT(SERVER_KNOBS->ENABLE_VERSION_VECTOR);
			ASSERT(SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST);
			ASSERT(SERVER_KNOBS->ENABLE_VERSION_VECTOR_REPLY_RECOVERY);
			ASSERT(SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS);
		}
		const char* phase = "InitialRead";
		try {
			while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
				co_await dbInfo->onChange();
			}
			Key marker("TLogWriteAntiQuorum/" + deterministicRandom()->randomUniqueID().toString());
			Transaction tr(cx);
			while (true) {
				auto before = co_await errorOr(tr.get(marker));
				if (!before.isError()) {
					ASSERT(!before.get().present());
					break;
				}
				TraceEvent("TLogAntiQuorumInitialReadRetry").error(before.getError());
				co_await tr.onError(before.getError());
				tr.setOption(FDBTransactionOptions::SKIP_GRV_CACHE);
			}
			Version readVersion = co_await tr.getReadVersion();
			const ServerDBInfo initial = dbInfo->get();
			ASSERT_EQ(initial.client.commitProxies.size(), 1);
			ASSERT_EQ(initial.logSystemConfig.tLogs.size(), 1);
			const auto& logSet = initial.logSystemConfig.tLogs.front();
			ASSERT(logSet.isLocal);
			ASSERT_EQ(logSet.tLogs.size(), 3);
			ASSERT_EQ(logSet.tLogReplicationFactor, 3);
			ASSERT_EQ(logSet.tLogWriteAntiQuorum, 1);
			const LogEpoch oldEpoch = initial.logSystemConfig.epoch;
			const auto proxy = initial.client.commitProxies.front().address();
			// The proxy must still be able to reach the sequencer and resolvers to commit.
			std::set<IPAddress> excluded{ proxy.ip, initial.master.address().ip, g_network->getLocalAddress().ip };
			for (const auto& resolver : initial.resolvers) {
				excluded.insert(resolver.address().ip);
			}
			auto slow = std::find_if(logSet.tLogs.begin(), logSet.tLogs.end(), [&](const auto& log) {
				return log.present() && !excluded.contains(log.interf().address().ip);
			});
			ASSERT(slow != logSet.tLogs.end());
			const auto slowAddress = slow->interf().address();

			Version committedVersion;
			{
				// Fast simulation bypasses explicit clogs. Keep random connection failures disabled.
				const bool wasFast = g_simulator->speedUpSimulation;
				g_simulator->speedUpSimulation = false;
				g_simulator->clogPair(proxy.ip, slowAddress.ip, 20.0);
				ScopeExit restore([&]() {
					g_simulator->unclogPair(proxy.ip, slowAddress.ip);
					g_simulator->speedUpSimulation = wasFast;
				});
				phase = "CommitWhileClogged";
				double started = now();
				TraceEvent("TLogAntiQuorumClogged")
				    .detail("Epoch", oldEpoch)
				    .detail("CommitProxy", proxy)
				    .detail("SlowTLog", slow->id())
				    .detail("SlowAddress", slowAddress)
				    .detail("Unicast", SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST)
				    .detail("ReplyRecovery", SERVER_KNOBS->ENABLE_VERSION_VECTOR_REPLY_RECOVERY)
				    .detail("AntiQuorum", logSet.tLogWriteAntiQuorum)
				    .detail("ReadVersion", readVersion);
				tr.set(marker, "acknowledged"_sr);
				co_await timeoutError(tr.commit(), 5.0);
				committedVersion = tr.getCommittedVersion();
				ASSERT_GT(committedVersion, readVersion);
				ASSERT_EQ(dbInfo->get().logSystemConfig.epoch, oldEpoch);
				TraceEvent("TLogAntiQuorumClientAck")
				    .detail("Epoch", oldEpoch)
				    .detail("Key", printable(marker))
				    .detail("Version", committedVersion)
				    .detail("Elapsed", now() - started);

				phase = "LockOldLogs";
				// Lock from the tester while the proxy's writes are still in transit.
				std::vector<Future<TLogLockResult>> locks;
				for (const auto& log : logSet.tLogs) {
					locks.push_back(log.interf().lock.getReply<TLogLockResult>());
				}
				auto results = co_await timeoutError(getAll(std::move(locks)), 10.0);
				for (size_t i = 0; i < results.size(); ++i) {
					const auto& result = results[i];
					ASSERT_EQ(result.logId, logSet.tLogs[i].id());
					TraceEvent("TLogAntiQuorumLocked")
					    .detail("Epoch", oldEpoch)
					    .detail("TLog", result.logId)
					    .detail("Slow", result.logId == slow->id())
					    .detail("End", result.end)
					    .detail("KnownCommittedVersion", result.knownCommittedVersion)
					    .detail("AckVersion", committedVersion);
					ASSERT((result.end < committedVersion) == (result.logId == slow->id()));
				}
			}

			phase = "Recover";
			co_await timeoutError(waitForNextEpoch(oldEpoch, RecoveryState::ACCEPTING_COMMITS), 60.0);
			const auto& next = dbInfo->get().logSystemConfig;
			const LogEpoch newEpoch = next.epoch;
			ASSERT(next.recoveredAt.present());
			const Version recoveredAt = next.recoveredAt.get();
			Optional<Version> oldEpochRecoverAt;
			for (const auto& old : next.oldTLogs) {
				if (old.epoch == oldEpoch) {
					oldEpochRecoverAt = old.recoverAt;
				}
			}
			TraceEvent("TLogAntiQuorumRecoveryBoundary")
			    .detail("OldEpoch", oldEpoch)
			    .detail("NewEpoch", newEpoch)
			    .detail("OldEpochRecoverAt", oldEpochRecoverAt.orDefault(invalidVersion))
			    .detail("RecoveredAt", recoveredAt)
			    .detail("AckVersion", committedVersion);
			phase = "ReadAfterRecovery";
			bool readsValid = true;
			auto readMarker = [&](Transaction* read) -> Future<Void> {
				read->setOption(FDBTransactionOptions::SKIP_GRV_CACHE);
				const Version version = co_await read->getReadVersion();
				Optional<Value> value = co_await read->get(marker);
				TraceEvent("TLogAntiQuorumClientRead")
				    .detail("Phase", phase)
				    .detail("Epoch", dbInfo->get().logSystemConfig.epoch)
				    .detail("Key", printable(marker))
				    .detail("AckVersion", committedVersion)
				    .detail("ReadVersion", version)
				    .detail("Present", value.present())
				    .detail("Value", value.present() ? printable(value.get()) : "<absent>");
				readsValid &= version >= committedVersion && value.present() && value.get() == "acknowledged"_sr;
			};
			co_await timeoutError(cx.run(readMarker), 30.0);
			phase = "ReadAfterFullRecovery";
			co_await timeoutError(waitForNextEpoch(oldEpoch, RecoveryState::FULLY_RECOVERED), 60.0);
			co_await timeoutError(cx.run(readMarker), 30.0);
			// Read before checking the recovery boundary so a bad cutoff cannot hide the client result.
			phase = "CheckClientReads";
			ASSERT(readsValid);
			phase = "CheckRecoveryBoundary";
			ASSERT_GE(recoveredAt, committedVersion);
			if (oldEpochRecoverAt.present()) {
				ASSERT_GE(oldEpochRecoverAt.get(), committedVersion);
			}
			TraceEvent("TLogAntiQuorumRecovered")
			    .detail("OldEpoch", oldEpoch)
			    .detail("NewEpoch", newEpoch)
			    .detail("RecoveredAt", recoveredAt)
			    .detail("AckVersion", committedVersion);
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "TLogAntiQuorumFailed").error(e).detail("Phase", phase);
			}
			throw;
		}
	}
};

WorkloadFactory<TLogWriteAntiQuorumWorkload> TLogWriteAntiQuorumWorkloadFactory;
