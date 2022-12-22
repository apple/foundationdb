/*
 * TenantEntryCacheWorkload.actor.cpp
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"

#include "fdbclient/Knobs.h"
#include "fdbclient/TenantEntryCache.actor.h"
#include "fdbrpc/TenantName.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
TenantEntryCachePayload<int64_t> createPayload(const TenantName& name, const TenantMapEntry& entry) {
	TenantEntryCachePayload<int64_t> payload;
	payload.name = name;
	payload.entry = entry;
	payload.payload = entry.id();

	return payload;
}
} // namespace

struct TenantEntryCacheWorkload : TestWorkload {
	static constexpr auto NAME = "TenantEntryCache";

	const TenantName tenantNamePrefix = "tenant_entry_cache_workload_"_sr;
	TenantName localTenantNamePrefix;
	int maxTenants;
	int clientId;

	TenantEntryCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		clientId = wcx.clientId;
		maxTenants = std::max(3, std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000)));
		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
	}
	~TenantEntryCacheWorkload() {}

	static void compareTenants(Optional<TenantEntryCachePayload<int64_t>> left, TenantMapEntry& right) {
		ASSERT(left.present());
		ASSERT_EQ(left.get().entry.id(), right.id());
		ASSERT_EQ(left.get().entry.prefix.compare(right.prefix), 0);
		ASSERT_EQ(left.get().payload, right.id());
	}

	ACTOR static Future<Void> compareContents(std::vector<std::pair<TenantName, TenantMapEntry>>* tenants,
	                                          Reference<TenantEntryCache<int64_t>> cache) {
		state int i;
		for (i = 0; i < tenants->size(); i++) {
			if (deterministicRandom()->coinflip()) {
				Optional<TenantEntryCachePayload<int64_t>> e =
				    wait(cache->getById(tenants->at(i).second.id()));
				compareTenants(e, tenants->at(i).second);
			} else {
				Optional<TenantEntryCachePayload<int64_t>> e = wait(cache->getByName(tenants->at(i).first));
				compareTenants(e, tenants->at(i).second);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> testTenantNotFound(Database cx, TenantEntryCacheRefreshMode refreshMode) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, refreshMode);
		TraceEvent("TenantNotFoundStart");

		wait(cache->init());
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);

		state TenantMapEntry dummy(std::numeric_limits<int64_t>::max(), "name"_sr, TenantState::READY);
		Optional<TenantEntryCachePayload<int64_t>> value = wait(cache->getById(dummy.id()));
		ASSERT(!value.present());

		Optional<TenantEntryCachePayload<int64_t>> value1 = wait(cache->getByPrefix(dummy.prefix));
		ASSERT(!value1.present());

		TraceEvent("TenantNotFoundEnd");
		return Void();
	}

	ACTOR static Future<Void> testCreateTenantsAndLookup(Database cx,
	                                                     TenantEntryCacheWorkload* self,
	                                                     std::vector<std::pair<TenantName, TenantMapEntry>>* tenantList,
	                                                     TenantEntryCacheRefreshMode refreshMode) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, refreshMode);
		state int nTenants = deterministicRandom()->randomInt(5, self->maxTenants);

		TraceEvent("CreateTenantsAndLookupStart");

		wait(cache->init());
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);
		ASSERT_GE(cache->numCacheRefreshes(), 1);

		tenantList->clear();
		state int i = 0;
		state std::unordered_set<TenantName> tenantNames;
		while (i < nTenants) {
			state TenantName name(format("%s%08d",
			                             self->localTenantNamePrefix.toString().c_str(),
			                             deterministicRandom()->randomInt(0, self->maxTenants)));
			if (tenantNames.find(name) != tenantNames.end()) {
				continue;
			}

			Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), StringRef(name)));
			ASSERT(entry.present());
			tenantList->emplace_back(std::make_pair(name, entry.get()));
			tenantNames.emplace(name);
			i++;
		}

		wait(compareContents(tenantList, cache));

		TraceEvent("CreateTenantsAndLookupEnd");
		return Void();
	}

	ACTOR static Future<Void> testTenantInsert(Database cx,
	                                           TenantEntryCacheWorkload* self,
	                                           std::vector<std::pair<TenantName, TenantMapEntry>>* tenantList,
	                                           TenantEntryCacheRefreshMode refreshMode) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, refreshMode);

		ASSERT(!tenantList->empty() && tenantList->size() >= 2);

		TraceEvent("TestTenantInsertStart");

		wait(cache->init());
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);
		ASSERT_GE(cache->numCacheRefreshes(), 1);

		state std::pair<TenantName, TenantMapEntry> p = tenantList->at(0);
		state Optional<TenantEntryCachePayload<int64_t>> entry;

		// Tenant rename
		p.first = TenantName(format("%s%08d",
		                            self->localTenantNamePrefix.toString().c_str(),
		                            deterministicRandom()->randomInt(self->maxTenants + 100, self->maxTenants + 200)));
		cache->put(p);
		Optional<TenantEntryCachePayload<int64_t>> e = wait(cache->getByName(p.first));
		entry = e;
		compareTenants(entry, p.second);

		// Tenant delete & recreate
		p.second.id() =
		    p.second.id() +
		    deterministicRandom()->randomInt(self->maxTenants + 500, self->maxTenants + 700);
		cache->put(p);
		Optional<TenantEntryCachePayload<int64_t>> e1 = wait(cache->getById(p.second.id()));
		entry = e1;
		compareTenants(entry, p.second);
		ASSERT_EQ(p.first.contents().toString().compare(entry.get().name.contents().toString()), 0);

		// Delete a tenant and rename an existing TenantEntry to reuse the name of deleted tenant
		state std::pair<TenantName, TenantMapEntry> p1 = tenantList->back();
		tenantList->pop_back();
		wait(TenantAPI::deleteTenant(cx.getReference(), p1.first));
		cache->put(std::make_pair(p1.first, p.second));
		Optional<TenantEntryCachePayload<int64_t>> e2 = wait(cache->getById(p.second.id()));
		entry = e2;
		compareTenants(entry, p.second);
		ASSERT_EQ(p1.first.contents().toString().compare(entry.get().name.contents().toString()), 0);

		TraceEvent("TestTenantInsertEnd");
		return Void();
	}

	ACTOR static Future<Void> testCacheReload(Database cx,
	                                          std::vector<std::pair<TenantName, TenantMapEntry>>* tenantList,
	                                          TenantEntryCacheRefreshMode refreshMode) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, refreshMode);

		ASSERT(!tenantList->empty());

		TraceEvent("CacheReloadStart");

		wait(cache->init());
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);
		ASSERT_GE(cache->numCacheRefreshes(), 1);

		wait(compareContents(tenantList, cache));

		TraceEvent("CacheReloadEnd");
		return Void();
	}

	ACTOR static Future<Void> testTenantCacheDefaultFunc(Database cx) {
		wait(delay(0.0));
		return Void();
	}

	ACTOR static Future<Void> testCacheRefresh(Database cx) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(cx, createPayload);

		TraceEvent("TestCacheRefreshStart");

		wait(cache->init());
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);
		ASSERT_GE(cache->numCacheRefreshes(), 1);

		// Fault injection may cause delays in cluster recovery/availability, spin a loop to let cache refresh to
		// trigger with a max wait of 5 mins; timed_out error is thrown if cache refresh isn't triggered.

		state int64_t startTime = now();
		state int64_t waitUntill = startTime + 300; // 5 mins max wait
		loop {
			// InitRefresh + multiple timer based invocations (at least 2 invocations of cache->refresh())
			if (cache->numCacheRefreshes() >= 2) {
				break;
			}

			if (now() > waitUntill) {
				throw timed_out();
			}

			TraceEvent("TestCacheRefreshWait").detail("Elapsed", now() - startTime);
			wait(delay(CLIENT_KNOBS->TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL));
		}

		TraceEvent("TestCacheRefreshEnd").detail("ElapsedTotal", now() - startTime);
		return Void();
	}

	ACTOR static Future<Void> testCacheTenantsDisabled(Database cx) {
		ASSERT(cx->clientInfo->get().tenantMode == TenantMode::DISABLED);
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, TenantEntryCacheRefreshMode::NONE);

		TraceEvent("TestCacheTenantDisabledStart");

		wait(cache->init());
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);
		ASSERT_GE(cache->numCacheRefreshes(), 1);

		Optional<TenantEntryCachePayload<int64_t>> entry = wait(cache->getById(12));
		ASSERT(!entry.present());
		ASSERT_EQ(cache->numCacheRefreshes(), 1);

		TraceEvent("TestCacheTenantDisabledEnd");
		return Void();
	}

	ACTOR static Future<Void> tenantEntryRemove(Database cx,
	                                            std::vector<std::pair<TenantName, TenantMapEntry>>* tenantList) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, TenantEntryCacheRefreshMode::NONE);

		wait(cache->init());

		ASSERT(!tenantList->empty());

		// Remove an entry from the cache
		state int idx = deterministicRandom()->randomInt(0, tenantList->size());
		Optional<TenantEntryCachePayload<int64_t>> entry = wait(cache->getByName(tenantList->at(idx).first));
		ASSERT(entry.present());

		TraceEvent("TestTenantEntryRemoveStart")
		    .detail("Id", tenantList->at(idx).second.id())
		    .detail("Name", tenantList->at(idx).first)
		    .detail("Prefix", tenantList->at(idx).second.prefix);

		wait(TenantAPI::deleteTenant(cx.getReference(), tenantList->at(idx).first));

		if (deterministicRandom()->coinflip()) {
			wait(cache->removeEntryById(tenantList->at(idx).second.id()));
		} else if (deterministicRandom()->coinflip()) {
			wait(cache->removeEntryByPrefix(tenantList->at(idx).second.prefix));
		} else {
			wait(cache->removeEntryByName(tenantList->at(idx).first));
		}

		state Optional<TenantEntryCachePayload<int64_t>> e =
		    wait(cache->getById(tenantList->at(idx).second.id()));
		ASSERT(!e.present());
		state Optional<TenantEntryCachePayload<int64_t>> e1 =
		    wait(cache->getByPrefix(tenantList->at(idx).second.prefix));
		ASSERT(!e1.present());
		state Optional<TenantEntryCachePayload<int64_t>> e2 = wait(cache->getByName(tenantList->at(idx).first));
		ASSERT(!e2.present());

		// Ensure remove-entry is an idempotent operation
		cache->removeEntryByName(tenantList->at(idx).first);
		Optional<TenantEntryCachePayload<int64_t>> e3 =
		    wait(cache->getById(tenantList->at(idx).second.id()));
		ASSERT(!e3.present());

		return Void();
	}

	ACTOR static Future<Void> testCacheWatchRefresh(Database cx) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(
		    cx, deterministicRandom()->randomUniqueID(), createPayload, TenantEntryCacheRefreshMode::WATCH);
		wait(cache->init(true));
		// Ensure associated counter values gets updated
		ASSERT_EQ(cache->numRefreshByInit(), 1);
		ASSERT_GE(cache->numCacheRefreshes(), 1);

		// Create tenant and make sure the cache is updated
		state TenantName name = "TenantEntryCache_WatchRefresh"_sr;
		state Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), name));
		ASSERT(entry.present());

		state int64_t startTime = now();
		state int64_t waitUntill = startTime + 300; // 5 mins max wait
		loop {
			if (cache->numWatchRefreshes() >= 1) {
				break;
			}

			if (now() > waitUntill) {
				throw timed_out();
			}

			TraceEvent("TestCacheRefreshWait").detail("Elapsed", now() - startTime);
			wait(delay(CLIENT_KNOBS->TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL));
		}
		Optional<TenantEntryCachePayload<int64_t>> payload = wait(cache->getByName(name));
		ASSERT(payload.present());
		compareTenants(payload, entry.get());
		return Void();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0 && g_network->isSimulated() && BUGGIFY) {
			IKnobCollection::getMutableGlobalKnobCollection().setKnob("tenant_entry_cache_list_refresh_interval",
			                                                          KnobValueRef::create(int{ 2 }));
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		}
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, TenantEntryCacheWorkload* self) {
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenantList;
		state TenantEntryCacheRefreshMode refreshMode;
		if (deterministicRandom()->coinflip()) {
			refreshMode = TenantEntryCacheRefreshMode::PERIODIC_TASK;
		} else {
			refreshMode = TenantEntryCacheRefreshMode::WATCH;
		}
		// get the tenant mode from db config
		state Transaction tr = Transaction(cx);
		state DatabaseConfiguration configuration;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				RangeResult results = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);
				configuration.fromKeyValues((VectorRef<KeyValueRef>)results);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (configuration.tenantMode == TenantMode::DISABLED) {
			wait(testCacheTenantsDisabled(cx));
		} else {
			wait(testTenantNotFound(cx, refreshMode));
			wait(testCreateTenantsAndLookup(cx, self, &tenantList, refreshMode));
			wait(testTenantInsert(cx, self, &tenantList, refreshMode));
			wait(tenantEntryRemove(cx, &tenantList));
			wait(testTenantCacheDefaultFunc(cx));
			wait(testCacheRefresh(cx));
			wait(testCacheWatchRefresh(cx));
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantEntryCacheWorkload> TenantEntryCacheWorkloadFactory;