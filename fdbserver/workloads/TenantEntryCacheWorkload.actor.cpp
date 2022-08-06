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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TenantEntryCache.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
TenantEntryCachePayload<int64_t> createPayload(const TenantName& name,
                                               const TenantMapEntry& entry,
                                               const TenantEntryCacheGen gen) {
	TenantEntryCachePayload<int64_t> payload;
	payload.name = name;
	payload.entry = entry;
	payload.gen = gen;
	payload.payload = entry.id;

	return payload;
}
} // namespace

struct TenantEntryCacheWorkload : TestWorkload {
	const TenantName tenantNamePrefix = "tenant_entry_cache_workload_"_sr;
	TenantName localTenantNamePrefix;
	int maxTenants;
	int clientId;

	TenantEntryCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		clientId = wcx.clientId;
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
	}
	~TenantEntryCacheWorkload() {}

	static void compareTenants(Optional<TenantEntryCachePayload<int64_t>> left, TenantMapEntry& right) {
		ASSERT(left.present());
		ASSERT_EQ(left.get().entry.id, right.id);
		ASSERT_EQ(left.get().entry.prefix.compare(right.prefix), 0);
		ASSERT_EQ(left.get().payload, right.id);
	}

	ACTOR static Future<Void> compareContents(std::vector<std::pair<TenantName, TenantMapEntry>>* tenants,
	                                          Reference<TenantEntryCache<int64_t>> cache) {
		state int i;
		for (i = 0; i < tenants->size(); i++) {
			if (deterministicRandom()->coinflip()) {
				Optional<TenantEntryCachePayload<int64_t>> e = wait(cache->getById(tenants->at(i).second.id));
				compareTenants(e, tenants->at(i).second);
			} else {
				Optional<TenantEntryCachePayload<int64_t>> e = wait(cache->getByName(tenants->at(i).first));
				compareTenants(e, tenants->at(i).second);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> testTenantNotFound(Database cx) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(cx, createPayload);
		TraceEvent("TenantNotFound_Start").log();

		wait(cache->init());

		state TenantMapEntry dummy(std::numeric_limits<int64_t>::max(), TenantState::READY, true /* encrypted */);
		Optional<TenantEntryCachePayload<int64_t>> value = wait(cache->getById(dummy.id));
		ASSERT(!value.present());

		Optional<TenantEntryCachePayload<int64_t>> value1 = wait(cache->getByPrefix(dummy.prefix));
		ASSERT(!value1.present());

		TraceEvent("TenantNotFound_End").log();
		return Void();
	}

	ACTOR static Future<Void> testCreateTenantsAndLookup(
	    Database cx,
	    TenantEntryCacheWorkload* self,
	    std::vector<std::pair<TenantName, TenantMapEntry>>* tenantList) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(cx, createPayload);
		state int nTenants = deterministicRandom()->randomInt(5, self->maxTenants);

		TraceEvent("CreateTenantsAndLookup_Start").log();

		wait(cache->init());

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

		TraceEvent("CreateTenantsAndLookup_End").log();
		return Void();
	}

	ACTOR static Future<Void> testCacheReload(Database cx,
	                                          std::vector<std::pair<TenantName, TenantMapEntry>>* tenantList) {
		state Reference<TenantEntryCache<int64_t>> cache = makeReference<TenantEntryCache<int64_t>>(cx, createPayload);

		ASSERT(!tenantList->empty());

		TraceEvent("CacheReload_Start").log();

		wait(cache->init());
		wait(compareContents(tenantList, cache));

		TraceEvent("CacheReload_End").log();
		return Void();
	}

	ACTOR static Future<Void> testTenantCacheDefaultFunc(Database cx) {
		wait(delay(0.0));
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

		wait(testTenantNotFound(cx));
		wait(testCreateTenantsAndLookup(cx, self, &tenantList));
		wait(testTenantCacheDefaultFunc(cx));

		return Void();
	}

	std::string description() const override { return "TenantEntryCache"; }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantEntryCacheWorkload> TenantEntryCacheWorkloadFactory("TenantEntryCache");