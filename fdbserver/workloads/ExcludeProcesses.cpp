/*
 * ExcludeProcesses.cpp
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

#include <algorithm>
#include <set>
#include <vector>

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"

// Excludes processes partway through a test and leaves them excluded until the test ends, so a concurrent workload sees
// the cluster move their data elsewhere. Processes are chosen by port (e.g. the storage processes a cluster was
// configured to have started on a given port), or else randomly among the processes hosting storage servers.
struct ExcludeProcessesWorkload : TestWorkload {
	static constexpr auto NAME = "ExcludeProcesses";

	double excludeDelay{ 0.0 };
	std::vector<int> ports;
	int count{ 0 }; // 0 = every matching process
	std::vector<AddressExclusion> excluded;

	explicit ExcludeProcessesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		excludeDelay = getOption(options, "excludeDelay"_sr, 0.0);
		ports = getOption(options, "ports"_sr, std::vector<int>());
		count = getOption(options, "count"_sr, ports.empty() ? 1 : 0);
		ASSERT(count >= 0);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}
		// Choosing after the delay lets processes restarted by an earlier test re-register first
		co_await delay(excludeDelay);
		std::vector<AddressExclusion> exclusions = co_await chooseExclusions(cx);

		if (g_network->isSimulated() && !canExcludeSafely(exclusions)) {
			CODE_PROBE(true, "ExcludeProcesses skipped an unsafe exclusion");
			TraceEvent(SevWarn, "ExcludeProcessesWorkloadUnsafe").detail("Exclusions", describe(exclusions));
			co_return;
		}

		TraceEvent("ExcludeProcessesWorkloadMain")
		    .detail("Status", "Wait for exclusion")
		    .detail("ExcludeDelay", excludeDelay)
		    .detail("Exclusions", describe(exclusions));
		// Returns once the exclusion is durable; the processes' data then moves away in the background
		co_await excludeServers(cx, exclusions);
		excluded = exclusions;
		TraceEvent("ExcludeProcessesWorkloadMain").detail("Status", "Exclusion finished");
	}

	Future<std::vector<AddressExclusion>> chooseExclusions(Database cx) {
		std::set<AddressExclusion> matching;
		if (ports.empty()) {
			for (StorageServerInterface const& ss : co_await getStorageServers(cx)) {
				if (!ss.isTss()) {
					matching.emplace(ss.address().ip, ss.address().port);
				}
			}
		} else {
			for (ProcessData const& w : co_await getWorkers(cx)) {
				if (std::find(ports.begin(), ports.end(), w.address.port) != ports.end()) {
					matching.emplace(w.address.ip, w.address.port);
				}
			}
		}
		std::vector<AddressExclusion> candidates(matching.begin(), matching.end());
		CODE_PROBE(!ports.empty(), "ExcludeProcesses selecting by port");
		TraceEvent(candidates.empty() ? SevError : SevInfo, "ExcludeProcessesWorkloadCandidates")
		    .detail("Ports", describe(ports))
		    .detail("Candidates", describe(candidates));
		if (candidates.empty()) {
			throw operation_failed();
		}
		if (count > 0 && count < candidates.size()) {
			deterministicRandom()->randomShuffle(candidates);
			candidates.resize(count);
		}
		co_return candidates;
	}

	// Whether the simulated cluster stays available with the given processes gone
	static bool canExcludeSafely(std::vector<AddressExclusion> const& exclusions) {
		std::vector<ISimulator::ProcessInfo*> left, dead;
		for (ISimulator::ProcessInfo* p : g_simulator->getAllProcesses()) {
			if (!p->isAvailable() || p->isCleared() ||
			    std::find(exclusions.begin(), exclusions.end(), AddressExclusion(p->address.ip, p->address.port)) !=
			        exclusions.end()) {
				dead.push_back(p);
			} else {
				left.push_back(p);
			}
		}
		return g_simulator->canKillProcesses(left, dead, ISimulator::KillType::KillInstantly, nullptr);
	}

	// Include the processes back once every workload is done, since with them excluded data distribution may not be
	// able to build healthy teams, and the consistency checks that follow the test would never pass
	Future<bool> check(Database const& cx) override {
		if (!excluded.empty()) {
			co_await includeServers(cx, excluded);
			TraceEvent("ExcludeProcessesWorkloadMain").detail("Status", "Inclusion finished");
		}
		co_return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ExcludeProcessesWorkload> ExcludeProcessesWorkloadFactory;
