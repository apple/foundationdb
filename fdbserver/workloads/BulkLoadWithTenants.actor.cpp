/*
 * BulkSetup.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BulkSetupWorkload : TestWorkload {
	static constexpr auto NAME = "BulkLoadWithTenants";

	int nodeCount;
	double transactionsPerSecond;
	Key keyPrefix;
	double maxNumTenants;
	double minNumTenants;
	std::vector<TenantName> tenantNames;

	BulkSetupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0) / clientCount;
		nodeCount = getOption(options, "nodeCount"_sr, transactionsPerSecond * clientCount);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		// maximum and minimum number of tenants per client
		maxNumTenants = getOption(options, "maxNumTenants"_sr, 0);
		minNumTenants = getOption(options, "minNumTenants"_sr, 0);
		ASSERT(minNumTenants <= maxNumTenants);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }

	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }

	ACTOR static Future<Void> _setup(BulkSetupWorkload* workload, Database cx) {
		// create a bunch of tenants (between min and max tenants)
		state int numTenantsToCreate =
		    deterministicRandom()->randomInt(workload->minNumTenants, workload->maxNumTenants + 1);
		TraceEvent("BulkSetupTenantCreation").detail("NumTenants", numTenantsToCreate);
		std::vector<Future<Void>> tenantFutures;
		for (int i = 0; i < numTenantsToCreate; i++) {
			TenantMapEntry entry;
			entry.encrypted = SERVER_KNOBS->ENABLE_ENCRYPTION;
			workload->tenantNames.push_back(TenantName(format("BulkSetupTenant_%04d_%04d", workload->clientId, i)));
			TraceEvent("CreatingTenant")
			    .detail("Tenant", workload->tenantNames.back())
			    .detail("TenantGroup", entry.tenantGroup);
			tenantFutures.push_back(success(TenantAPI::createTenant(cx.getReference(), workload->tenantNames.back())));
		}
		wait(waitForAll(tenantFutures));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		return bulkSetup(cx,
		                 this,
		                 nodeCount,
		                 Promise<double>(),
		                 false,
		                 0.0,
		                 1e12,
		                 std::vector<uint64_t>(),
		                 Promise<std::vector<std::pair<uint64_t, double>>>(),
		                 0,
		                 0.1,
		                 0,
		                 0,
		                 tenantNames);
	}

	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }
};

WorkloadFactory<BulkSetupWorkload> BulkSetupWorkloadFactory;
