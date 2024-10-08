/*
 * BulkSetup.actor.cpp
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

#include "fdbclient/ClientKnobs.h"
#include "fdbclient/TenantEntryCache.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbrpc/TenantInfo.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.h"
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
	std::vector<Reference<Tenant>> tenants;
	bool deleteTenants;
	double testDuration;
	std::unordered_map<int64_t, std::vector<KeyValueRef>> numKVPairsPerTenant;
	bool enableEKPKeyFetchFailure;
	Arena arena;

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

	ACTOR static Future<std::vector<KeyValueRef>> getKVPairsForTenant(BulkSetupWorkload* workload,
	                                                                  Reference<Tenant> tenant,
	                                                                  Database cx) {
		state KeySelector begin = firstGreaterOrEqual(normalKeys.begin);
		state KeySelector end = firstGreaterOrEqual(normalKeys.end);
		state std::vector<KeyValueRef> kvPairs;
		state ReadYourWritesTransaction tr = ReadYourWritesTransaction(cx, tenant);
		loop {
			try {
				RangeResult kvRange = wait(tr.getRange(begin, end, 1000));
				if (!kvRange.more && kvRange.size() == 0) {
					break;
				}
				for (int i = 0; i < kvRange.size(); i++) {
					kvPairs.push_back(KeyValueRef(workload->arena, KeyValueRef(kvRange[i].key, kvRange[i].value)));
				}
				begin = firstGreaterThan(kvRange.end()[-1].key);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return kvPairs;
	}

	ACTOR static Future<Void> _setup(BulkSetupWorkload* workload, Database cx) {
		// create a bunch of tenants (between min and max tenants)
		state int numTenantsToCreate =
		    deterministicRandom()->randomInt(workload->minNumTenants, workload->maxNumTenants + 1);
		TraceEvent("BulkSetupTenantCreation").detail("NumTenants", numTenantsToCreate);

		if (numTenantsToCreate > 0) {
			state std::vector<Future<Optional<TenantMapEntry>>> tenantFutures;
			for (int i = 0; i < numTenantsToCreate; i++) {
				TenantName tenantName = TenantNameRef(format("BulkSetupTenant_%04d", i));
				tenantFutures.push_back(TenantAPI::createTenant(cx.getReference(), tenantName));
			}
			wait(waitForAll(tenantFutures));
			for (auto& f : tenantFutures) {
				ASSERT(f.get().present());
				workload->tenants.push_back(makeReference<Tenant>(f.get().get().id, f.get().get().tenantName));
				TraceEvent("BulkSetupCreatedTenant").detail("Tenant", workload->tenants.back());
			}
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
		               workload->tenants));

		state int i;
		state bool added = false;
		for (i = 0; i < workload->tenants.size(); i++) {
			std::vector<KeyValueRef> keysForCurTenant = wait(getKVPairsForTenant(workload, workload->tenants[i], cx));
			if (workload->enableEKPKeyFetchFailure && keysForCurTenant.size() > 0 && !added) {
				IKnobCollection::getMutableGlobalKnobCollection().setKnob(
				    "simulation_ekp_tenant_ids_to_drop",
				    KnobValueRef::create(std::to_string(workload->tenants[i]->id())));
				TraceEvent("BulkSetupTenantForEKPToDrop")
				    .detail("Tenant", CLIENT_KNOBS->SIMULATION_EKP_TENANT_IDS_TO_DROP);
				added = true;
			}
			workload->numKVPairsPerTenant[workload->tenants[i]->id()] = keysForCurTenant;
		}
		return Void();
	}

	ACTOR static Future<bool> _check(BulkSetupWorkload* workload, Database cx) {
		state int i;
		state std::unordered_set<int64_t> tenantIdsToDrop =
		    parseStringToUnorderedSet<int64_t>(CLIENT_KNOBS->SIMULATION_EKP_TENANT_IDS_TO_DROP, ',');
		for (i = 0; i < workload->tenants.size(); i++) {
			state Reference<Tenant> tenant = workload->tenants[i];
			std::vector<KeyValueRef> keysForCurTenant = wait(getKVPairsForTenant(workload, tenant, cx));
			if (tenantIdsToDrop.count(tenant->id())) {
				// Don't check the tenants that the EKP would throw errors for
				continue;
			}
			std::vector<KeyValueRef> expectedKeysForCurTenant = workload->numKVPairsPerTenant[tenant->id()];
			if (keysForCurTenant.size() != expectedKeysForCurTenant.size()) {
				TraceEvent(SevError, "BulkSetupNumKeysMismatch")
				    .detail("TenantName", tenant)
				    .detail("ActualCount", keysForCurTenant.size())
				    .detail("ExpectedCount", expectedKeysForCurTenant.size());
				return false;
			} else {
				TraceEvent("BulkSetupNumKeys")
				    .detail("TenantName", tenant)
				    .detail("ActualCount", keysForCurTenant.size());
			}

			for (int j = 0; j < expectedKeysForCurTenant.size(); j++) {
				if (expectedKeysForCurTenant[j].key != keysForCurTenant[j].key) {
					TraceEvent(SevError, "BulkSetupNumKeyMismatch")
					    .detail("TenantName", tenant)
					    .detail("ActualKey", keysForCurTenant[j].key)
					    .detail("ExpectedKey", expectedKeysForCurTenant[j].key);
					return false;
				}
				if (expectedKeysForCurTenant[j].value != keysForCurTenant[j].value) {
					TraceEvent(SevError, "BulkSetupNumValueMismatch")
					    .detail("TenantName", tenant)
					    .detail("ActualValue", keysForCurTenant[j].value)
					    .detail("ExpectedValue", expectedKeysForCurTenant[j].value);
					return false;
				}
			}
		}
		return true;
	}

	ACTOR static Future<Void> _start(BulkSetupWorkload* workload, Database cx) {
		// We want to ensure that tenant deletion happens before the restore phase starts
		// If there is only one tenant don't delete that tenant
		if (workload->deleteTenants && workload->tenants.size() > 1) {
			state Reference<TenantEntryCache<Void>> tenantCache =
			    makeReference<TenantEntryCache<Void>>(cx, TenantEntryCacheRefreshMode::WATCH);
			wait(tenantCache->init());
			state int numTenantsToDelete = deterministicRandom()->randomInt(0, workload->tenants.size());
			TraceEvent("BulkSetupTenantDeletion").detail("NumTenants", numTenantsToDelete);
			if (numTenantsToDelete > 0) {
				state int i;
				for (i = 0; i < numTenantsToDelete; i++) {
					state int tenantIndex = deterministicRandom()->randomInt(0, workload->tenants.size());
					state Reference<Tenant> tenant = workload->tenants[tenantIndex];
					workload->tenants.erase(workload->tenants.begin() + tenantIndex);
					TraceEvent("BulkSetupTenantDeletionClearing")
					    .detail("Tenant", tenant)
					    .detail("TotalNumTenants", workload->tenants.size());
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
					wait(success(TenantAPI::deleteTenant(cx.getReference(), tenant->name.get(), tenant->id())));
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
			}
			return _start(this, cx);
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
