/*
 * OldTLogRetirement.cpp
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
#include "fdbrpc/simulator.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"
#include "flow/CoroUtils.h"
#include "flow/ScopeExit.h"

#include <algorithm>
#include <array>
#include <set>
#include <vector>

class OldTLogRetirementWorkload : public TestWorkload {
public:
	static constexpr auto NAME = "OldTLogRetirement";
	explicit OldTLogRetirementWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), enabled(!clientId && g_network->isSimulated()),
	    operationTimeout(getOption(options, "operationTimeout"_sr, 120.0)) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }
	Future<Void> start(Database const& cx) override { return enabled ? run(cx) : Void(); }
	Future<bool> check(Database const& cx) override { return !enabled || completed; }
	void getMetrics(std::vector<PerfMetric>&) override {}

private:
	const bool enabled;
	const double operationTimeout;
	bool completed = false;
	static constexpr double cleanupTimeout = 30.0;

	static double stabilityDuration() {
		return SERVER_KNOBS->SINGLETON_RECRUIT_BME_DELAY + SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY +
		       2 * SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL + 1.0;
	}

	static std::array<UID, 3> singletonMembers(const ServerDBInfo& info) {
		return { info.distributor.present() ? info.distributor.get().id() : UID(),
			     info.ratekeeper.present() ? info.ratekeeper.get().id() : UID(),
			     info.consistencyScan.present() ? info.consistencyScan.get().id() : UID() };
	}

	static std::vector<UID> transactionSystemMembers(const ServerDBInfo& info) {
		std::vector<UID> members{ info.master.id(), info.logSystemConfig.recruitmentID };
		for (const auto& logSet : info.logSystemConfig.tLogs) {
			for (const auto& log : logSet.tLogs) {
				members.push_back(log.id());
			}
		}
		for (const auto& proxy : info.client.commitProxies) {
			members.push_back(proxy.id());
		}
		for (const auto& proxy : info.client.grvProxies) {
			members.push_back(proxy.id());
		}
		for (const auto& resolver : info.resolvers) {
			members.push_back(resolver.id());
		}
		std::sort(members.begin(), members.end());
		return members;
	}

	Future<ServerDBInfo> stableBaseline() {
		while (true) {
			while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
				co_await dbInfo->onChange();
			}
			const ServerDBInfo baseline = dbInfo->get();
			const auto members = transactionSystemMembers(baseline);
			const auto singletons = singletonMembers(baseline);
			Future<Void> stable = delay(stabilityDuration());
			while (dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED &&
			       dbInfo->get().recoveryCount == baseline.recoveryCount &&
			       transactionSystemMembers(dbInfo->get()) == members &&
			       singletonMembers(dbInfo->get()) == singletons) {
				if (stable.isReady()) {
					co_return dbInfo->get();
				}
				co_await (dbInfo->onChange() || stable);
			}
		}
	}

	Future<Void> excludeAndCheck(Database cx, AddressExclusion excluded, UID oldLogId, DBRecoveryCount previousCount) {
		TraceEvent("OldTLogRetirementExclude").detail("Address", excluded).detail("RecoveryCount", previousCount);
		co_await excludeServers(cx, { excluded });
		// A successful recovery writes the coordinated-state recovery count twice.
		const DBRecoveryCount expectedCount = previousCount + 2;
		while (true) {
			ASSERT_LE(dbInfo->get().recoveryCount, expectedCount);
			if (dbInfo->get().recoveryCount == expectedCount &&
			    dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
				break;
			}
			co_await dbInfo->onChange();
		}
		const auto members = transactionSystemMembers(dbInfo->get());
		ASSERT(!dbInfo->get().logSystemConfig.hasTLog(oldLogId));
		ASSERT(dbInfo->get().logSystemConfig.oldTLogs.empty());
		for (const auto& log : dbInfo->get().logSystemConfig.allPresentLogs()) {
			ASSERT(!excluded.excludes(log.address()));
		}
		TraceEvent("OldTLogRetirementFirstRecovery").detail("RecoveryCount", expectedCount);
		auto singletons = singletonMembers(dbInfo->get());
		Future<Void> stable = delay(stabilityDuration());
		while (true) {
			ASSERT_EQ(dbInfo->get().recoveryCount, expectedCount);
			ASSERT(dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED);
			ASSERT(dbInfo->get().logSystemConfig.oldTLogs.empty());
			ASSERT(transactionSystemMembers(dbInfo->get()) == members);
			const auto currentSingletons = singletonMembers(dbInfo->get());
			if (currentSingletons != singletons) {
				// Singleton replacement postpones the controller's better-master check.
				singletons = currentSingletons;
				stable = delay(stabilityDuration());
			}
			if (stable.isReady()) {
				break;
			}
			co_await (dbInfo->onChange() || stable);
		}
		CODE_PROBE(true, "Excluded old TLog retires without a second recovery");
		TraceEvent("OldTLogRetirementStable").detail("RecoveryCount", expectedCount);
	}

	Future<Void> run(Database cx) {
		const ServerDBInfo baseline = co_await timeoutError(stableBaseline(), operationTimeout);
		Optional<TLogInterface> selected;
		for (const auto& log : baseline.logSystemConfig.allLocalLogs()) {
			if (log.address() != baseline.clusterInterface.address() && log.address() != baseline.master.address() &&
			    !g_simulator->isProtectedAddress(log.address())) {
				selected = log;
				break;
			}
		}
		ASSERT(selected.present());
		const AddressExclusion excluded(selected.get().address().ip, selected.get().address().port);
		bool cleanupStarted = false;
		ScopeExit cleanupOnCancel([cx, excluded, &cleanupStarted] {
			if (!cleanupStarted) {
				uncancellable(timeoutError(includeServers(cx, { excluded }), cleanupTimeout));
			}
		});
		ErrorOr<Void> result = co_await coro::errorOr(
		    timeoutError(excludeAndCheck(cx, excluded, selected.get().id(), baseline.recoveryCount), operationTimeout));
		cleanupStarted = true;
		ErrorOr<Void> cleanup =
		    co_await coro::errorOr(uncancellable(timeoutError(includeServers(cx, { excluded }), cleanupTimeout)));
		if (result.isError()) {
			throw result.getError();
		}
		if (cleanup.isError()) {
			throw cleanup.getError();
		}
		completed = true;
	}
};

WorkloadFactory<OldTLogRetirementWorkload> OldTLogRetirementWorkloadFactory;
