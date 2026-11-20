/*
 * StaleCoordinatorProtocolMonitor.cpp
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

// Regression test for a hang in the client protocol-version monitor (getClusterProtocolImpl in NativeAPI.cpp).
//
// The monitor resolves the hostname of the current coordinator with Hostname::resolveWithRetry(), which retries
// forever. It used to do so *before* it started watching the `coordinator` AsyncVar for changes. If the monitor
// happened to pick a coordinator hostname that can never be resolved (e.g. a stale connection string that still lists
// coordinators that were removed from DNS), it never noticed that monitorProxies moved on to a working coordinator
// and never reported the protocol version. In the multi-version client this keeps the real database from ever being
// activated, so every transaction hangs until its timeout (error 1031) even though the cluster is healthy.
//
// The workload opens databases with a connection string made of the real coordinators plus several hostnames in the
// `.invalid` TLD (RFC 6761, guaranteed to never resolve) and expects the protocol version to be reported.

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CoroUtils.h"
#include "flow/genericactors.h"

struct StaleCoordinatorProtocolMonitorWorkload : TestWorkload {
	static constexpr auto NAME = "StaleCoordinatorProtocolMonitor";

	// How many databases are opened. The order in which coordinators are tried is random, so more databases make it
	// more likely that at least one of them starts with an unresolvable coordinator.
	int numDatabases;
	// How many unresolvable hostnames are added to the real connection string. Those are appended, so the final
	// connection string will contain the live coordinators + the additional stale coordinator entries.
	int numStaleCoordinators;
	// Simulated seconds to wait for the protocol version before a database is considered stuck.
	double protocolTimeout;

	int stuckDatabases = 0;
	int completedDatabases = 0;

	explicit StaleCoordinatorProtocolMonitorWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numDatabases = getOption(options, "numDatabases"_sr, 20);
		numStaleCoordinators = getOption(options, "numStaleCoordinators"_sr, 9);
		protocolTimeout = getOption(options, "protocolTimeout"_sr, 60.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return run(cx);
	}

	Future<Void> run(Database cx) {
		const std::string liveConnectionString = cx->getConnectionRecord()->getConnectionString().toString();

		// All databases are opened concurrently, so the worst case is a single protocolTimeout.
		std::vector<Future<Void>> databases;
		databases.reserve(numDatabases);
		for (int i = 0; i < numDatabases; ++i) {
			databases.push_back(checkDatabase(liveConnectionString, i));
		}
		co_await waitForAll(databases);
	}

	Future<Void> checkDatabase(std::string liveConnectionString, int index) {
		std::string connectionString = liveConnectionString;
		for (int j = 0; j < numStaleCoordinators; ++j) {
			connectionString += format(",stale-coordinator-%d-%d.invalid:4500", index, j);
		}

		auto record = makeReference<ClusterConnectionMemoryRecord>(ClusterConnectionString(connectionString));
		Database db = Database::createDatabase(record, -1);

		Optional<ProtocolVersion> protocolVersion = co_await timeout(db->getClusterProtocol(), protocolTimeout);
		if (protocolVersion.present()) {
			++completedDatabases;
		} else {
			++stuckDatabases;
			TraceEvent(SevWarnAlways, "StaleCoordinatorProtocolMonitorStuck")
			    .detail("DatabaseIndex", index)
			    .detail("ConnectionString", connectionString)
			    .detail("TimeoutSeconds", protocolTimeout);
		}
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}

		TraceEvent(stuckDatabases > 0 ? SevError : SevInfo, "StaleCoordinatorProtocolMonitorResult")
		    .detail("NumDatabases", numDatabases)
		    .detail("NumStaleCoordinators", numStaleCoordinators)
		    .detail("CompletedDatabases", completedDatabases)
		    .detail("StuckDatabases", stuckDatabases);
		return stuckDatabases == 0;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<StaleCoordinatorProtocolMonitorWorkload> StaleCoordinatorProtocolMonitorWorkloadFactory;
