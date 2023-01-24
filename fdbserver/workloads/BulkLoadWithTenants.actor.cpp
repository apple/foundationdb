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

#include "fdbclient/TenantEntryCache.actor.h"
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
	bool deleteTenants;
	double testDuration;
	std::unordered_map<TenantName, int> numKVPairsPerTenant;
	bool enableEKPKeyFetchFailure;

	BulkSetupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0) / clientCount;
		nodeCount = getOption(options, "nodeCount"_sr, transactionsPerSecond * clientCount);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		// maximum and minimum number of tenants per client
		maxNumTenants = getOption(options, "maxNumTenants"_sr, 0);
		minNumTenants = getOption(options, "minNumTenants"_sr, 0);
		deleteTenants = getOption(options, "deleteTenants"_sr, false);
		ASSERT(minNumTenants <= maxNumTenants);
		testDuration = getOption(options, "testDuration"_sr, -1);
		enableEKPKeyFetchFailure = getOption(options, "enableEKPKeyFetchFailure"_sr, false);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }

	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }

	ACTOR static Future<int> getNumKeysForTenant(TenantName tenant, Database cx) {
		state KeySelector begin = firstGreaterOrEqual(normalKeys.begin);
		state KeySelector end = firstGreaterOrEqual(normalKeys.end);
		state ReadYourWritesTransaction tr = ReadYourWritesTransaction(cx, tenant);
		state int result = 0;
		loop {
			try {
				RangeResult kvRange = wait(tr.getRange(begin, end, 1000));
				if (!kvRange.more && kvRange.size() == 0) {
					break;
				}
				result += kvRange.size();
				begin = firstGreaterThan(kvRange.end()[-1].key);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return result;
	}

	ACTOR static Future<Void> _setup(BulkSetupWorkload* workload, Database cx) {
		// create a bunch of tenants (between min and max tenants)
		state int numTenantsToCreate =
		    deterministicRandom()->randomInt(workload->minNumTenants, workload->maxNumTenants + 1);
		TraceEvent("BulkSetupTenantCreation").detail("NumTenants", numTenantsToCreate);
		if (numTenantsToCreate > 0) {
			std::vector<Future<Void>> tenantFutures;
			state std::vector<TenantName> tenantNames;
			for (int i = 0; i < numTenantsToCreate; i++) {
				TenantMapEntry entry;
				tenantNames.push_back(TenantName(format("BulkSetupTenant_%04d", i)));
				TraceEvent("CreatingTenant")
				    .detail("Tenant", tenantNames.back())
				    .detail("TenantGroup", entry.tenantGroup);
				tenantFutures.push_back(success(TenantAPI::createTenant(cx.getReference(), tenantNames.back())));
			}
			wait(waitForAll(tenantFutures));
			workload->tenantNames = tenantNames;
		}
		wait(bulkSetup(cx,
		               workload,
		               workload->nodeCount,
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
		               workload->tenantNames));

		state int i;
		for (i = 0; i < workload->tenantNames.size(); i++) {
			int keysForCurTenant = wait(getNumKeysForTenant(workload->tenantNames[i], cx));
			workload->numKVPairsPerTenant[workload->tenantNames[i]] = keysForCurTenant;
		}
		return Void();
	}

	ACTOR static Future<bool> _check(BulkSetupWorkload* workload, Database cx) {
		state int i;
		for (i = 0; i < workload->tenantNames.size(); i++) {
			int keysForCurTenant = wait(getNumKeysForTenant(workload->tenantNames[i], cx));
			if (keysForCurTenant != workload->numKVPairsPerTenant[workload->tenantNames[i]]) {
				TraceEvent(SevError, "BulkSetupNumKeysMistmatch")
				    .detail("TenantName", workload->tenantNames[i])
				    .detail("ActualCount", keysForCurTenant)
				    .detail("ExpectedCount", workload->numKVPairsPerTenant[workload->tenantNames[i]]);
				return false;
			} else {
				TraceEvent("BulkSetupNumKeys")
				    .detail("TenantName", workload->tenantNames[i])
				    .detail("KeysForTenant", keysForCurTenant);
			}
		}
		return true;
	}

	ACTOR static Future<Void> _start(BulkSetupWorkload* workload, Database cx) {
		// We want to ensure that tenant deletion happens before the restore phase starts
		if (workload->deleteTenants) {
			state Reference<TenantEntryCache<Void>> tenantCache =
			    makeReference<TenantEntryCache<Void>>(cx, TenantEntryCacheRefreshMode::WATCH);
			wait(tenantCache->init());
			state int numTenantsToDelete = deterministicRandom()->randomInt(0, workload->tenantNames.size() + 1);
			TraceEvent("BulkSetupTenantDeletion").detail("NumTenants", numTenantsToDelete);
			if (numTenantsToDelete > 0) {
				state int i;
				for (i = 0; i < numTenantsToDelete; i++) {
					state TenantName tenant = deterministicRandom()->randomChoice(workload->tenantNames);
					Optional<TenantEntryCachePayload<Void>> payload = wait(tenantCache->getByName(tenant));
					ASSERT(payload.present());
					state int64_t tenantId = payload.get().entry.id;
					TraceEvent("BulkSetupTenantDeletionClearing")
					    .detail("TenantName", tenant)
					    .detail("TenantId", tenantId)
					    .detail("TotalNumTenants", workload->tenantNames.size());
					for (auto it = workload->tenantNames.begin(); it != workload->tenantNames.end(); it++) {
						if (*it == tenant) {
							workload->tenantNames.erase(it);
							break;
						}
					}
					// clear the tenant
					state ReadYourWritesTransaction tr = ReadYourWritesTransaction(cx, tenant);
					loop {
						try {
							tr.clear(normalKeys);
							wait(tr.commit());
							break;
						} catch (Error& e) {
							wait(tr.onError(e));
						}
					}
					// delete the tenant
					wait(success(TenantAPI::deleteTenant(cx.getReference(), tenant)));
					TraceEvent("BulkSetupTenantDeletionDone")
					    .detail("TenantName", tenant)
					    .detail("TenantId", tenantId)
					    .detail("TotalNumTenants", workload->tenantNames.size());
				}
			}
		}
		return Void();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(this, cx);
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			if (testDuration > 0) {
				return timeout(_start(this, cx), testDuration, Void());
			} else {
				return _start(this, cx);
			}
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(this, cx);
		}
		return true;
	}
};

WorkloadFactory<BulkSetupWorkload> BulkSetupWorkloadFactory;
