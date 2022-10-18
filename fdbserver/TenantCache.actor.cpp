/*
 * TenantCache.actor.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/TenantCache.h"
#include "flow/flow.h"
#include <limits>
#include <string>
#include "flow/Trace.h"
#include "flow/actorcompiler.h"

class TenantCacheImpl {

	ACTOR static Future<std::vector<std::pair<TenantName, TenantMapEntry>>> getTenantList(TenantCache* tenantCache,
	                                                                                      Transaction* tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

		KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenantList =
		    wait(TenantMetadata::tenantMap().getRange(
		        tr, Optional<TenantName>(), Optional<TenantName>(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		ASSERT(tenantList.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantList.more);

		return tenantList.results;
	}

public:
	ACTOR static Future<Void> build(TenantCache* tenantCache) {
		state Transaction tr(tenantCache->dbcx());

		TraceEvent(SevInfo, "BuildingTenantCache", tenantCache->id()).log();

		try {
			state std::vector<std::pair<TenantName, TenantMapEntry>> tenantList = wait(getTenantList(tenantCache, &tr));

			for (int i = 0; i < tenantList.size(); i++) {
				tenantCache->insert(tenantList[i].first, tenantList[i].second);

				TraceEvent(SevInfo, "TenantFound", tenantCache->id())
				    .detail("TenantName", tenantList[i].first)
				    .detail("TenantID", tenantList[i].second.id)
				    .detail("TenantPrefix", tenantList[i].second.prefix);
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}

		TraceEvent(SevInfo, "BuiltTenantCache", tenantCache->id()).log();

		return Void();
	}

	ACTOR static Future<Void> monitorTenantMap(TenantCache* tenantCache) {
		TraceEvent(SevInfo, "StartingTenantCacheMonitor", tenantCache->id()).log();

		state Transaction tr(tenantCache->dbcx());

		state double lastTenantListFetchTime = now();

		loop {
			try {
				if (now() - lastTenantListFetchTime > (2 * SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL)) {
					TraceEvent(SevWarn, "TenantListRefreshDelay", tenantCache->id()).log();
				}

				state std::vector<std::pair<TenantName, TenantMapEntry>> tenantList =
				    wait(getTenantList(tenantCache, &tr));

				tenantCache->startRefresh();
				bool tenantListUpdated = false;

				for (int i = 0; i < tenantList.size(); i++) {
					if (tenantCache->update(tenantList[i].first, tenantList[i].second)) {
						tenantListUpdated = true;
						TenantCacheTenantCreated req(tenantList[i].second.prefix);
						tenantCache->tenantCreationSignal.send(req);
					}
				}

				if (tenantCache->cleanup()) {
					tenantListUpdated = true;
				}

				if (tenantListUpdated) {
					TraceEvent(SevInfo, "TenantCache", tenantCache->id()).detail("List", tenantCache->desc());
				}

				lastTenantListFetchTime = now();
				tr.reset();
				wait(delay(SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("TenantCacheGetTenantListError", tenantCache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> monitorStorageUsage(TenantCache* tenantCache) {
		TraceEvent(SevInfo, "StartingTenantCacheStorageUsageMonitor", tenantCache->id()).log();

		state ReadYourWritesTransaction tr(tenantCache->dbcx());

		state int refreshInterval = SERVER_KNOBS->TENANT_CACHE_STORAGE_REFRESH_INTERVAL;
		state double lastTenantListFetchTime = now();

		loop {
			try {
				if (now() - lastTenantListFetchTime > (2 * refreshInterval)) {
					TraceEvent(SevWarn, "TenantCacheGetStorageUsageRefreshDelay", tenantCache->id()).log();
				}

				state std::vector<Key> tenantPrefixList = tenantCache->getTenantPrefixList();

				state int i;
				for (i = 0; i < tenantPrefixList.size(); i++) {
					state int64_t size =
					    wait(tr.getEstimatedRangeSizeBytes(KeyRangeRef::entireRangeWithPrefix(tenantPrefixList[i])));
					tenantCache->updateStorageUsage(tenantPrefixList[i], size);
				}

				lastTenantListFetchTime = now();
				tr.reset();
				wait(delay(refreshInterval));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("TenantCacheGetStorageUsageError", tenantCache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}
};

void TenantCache::insert(TenantName& tenantName, TenantMapEntry& tenant) {
	KeyRef tenantPrefix(tenant.prefix.begin(), tenant.prefix.size());
	ASSERT(tenantCache.find(tenantPrefix) == tenantCache.end());

	TenantInfo tenantInfo(tenantName, Optional<Standalone<StringRef>>(), tenant.id);
	tenantCache[tenantPrefix] = makeReference<TCTenantInfo>(tenantInfo, tenant.prefix);
	tenantCache[tenantPrefix]->updateCacheGeneration(generation);
}

void TenantCache::startRefresh() {
	ASSERT(generation < std::numeric_limits<uint64_t>::max());
	generation++;
}

void TenantCache::keep(TenantName& tenantName, TenantMapEntry& tenant) {
	KeyRef tenantPrefix(tenant.prefix.begin(), tenant.prefix.size());

	ASSERT(tenantCache.find(tenantPrefix) != tenantCache.end());
	tenantCache[tenantPrefix]->updateCacheGeneration(generation);
}

bool TenantCache::update(TenantName& tenantName, TenantMapEntry& tenant) {
	KeyRef tenantPrefix(tenant.prefix.begin(), tenant.prefix.size());

	if (tenantCache.find(tenantPrefix) != tenantCache.end()) {
		keep(tenantName, tenant);
		return false;
	}

	insert(tenantName, tenant);
	return true;
}

int TenantCache::cleanup() {
	int tenantsRemoved = 0;
	std::vector<Key> keysToErase;

	for (auto& t : tenantCache) {
		ASSERT(t.value->cacheGeneration() <= generation);
		if (t.value->cacheGeneration() != generation) {
			keysToErase.push_back(t.key);
		}
	}

	for (auto& k : keysToErase) {
		tenantCache.erase(k);
		tenantsRemoved++;
	}

	return tenantsRemoved;
}

std::vector<Key> TenantCache::getTenantPrefixList() const {
	std::vector<Key> prefixes;
	for (const auto& [prefix, entry] : tenantCache) {
		prefixes.emplace_back(prefix);
	}
	return prefixes;
}

void TenantCache::updateStorageUsage(KeyRef prefix, int64_t size) {
	auto it = tenantCache.find(prefix);
	if (it != tenantCache.end()) {
		it->value->updateStorageUsage(size);
	}
}

std::string TenantCache::desc() const {
	std::string s("@Generation: ");
	s += std::to_string(generation) + " ";
	int count = 0;
	for (auto& [tenantPrefix, tenant] : tenantCache) {
		if (count) {
			s += ", ";
		}

		s += "Name: " + tenant->name().toString() + " Prefix: " + tenantPrefix.toString();
		count++;
	}

	return s;
}

bool TenantCache::isTenantKey(KeyRef key) const {
	auto it = tenantCache.lastLessOrEqual(key);
	if (it == tenantCache.end()) {
		return false;
	}

	if (!key.startsWith(it->key)) {
		return false;
	}

	return true;
}

Future<Void> TenantCache::build() {
	return TenantCacheImpl::build(this);
}

Optional<Reference<TCTenantInfo>> TenantCache::tenantOwning(KeyRef key) const {
	auto it = tenantCache.lastLessOrEqual(key);
	if (it == tenantCache.end()) {
		return {};
	}

	if (!key.startsWith(it->key)) {
		return {};
	}

	return it->value;
}

Future<Void> TenantCache::monitorTenantMap() {
	return TenantCacheImpl::monitorTenantMap(this);
}

Future<Void> TenantCache::monitorStorageUsage() {
	return TenantCacheImpl::monitorStorageUsage(this);
}

class TenantCacheUnitTest {
public:
	ACTOR static Future<Void> InsertAndTestPresence() {
		wait(Future<Void>(Void()));

		Database cx;
		TenantCache tenantCache(cx, UID(1, 0));

		constexpr static uint16_t tenantLimit = 64;

		uint16_t tenantCount = deterministicRandom()->randomInt(1, tenantLimit);
		uint16_t tenantNumber = deterministicRandom()->randomInt(0, std::numeric_limits<uint16_t>::max());

		for (uint16_t i = 0; i < tenantCount; i++) {
			TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantNumber + i));
			TenantMapEntry tenant(tenantNumber + i, TenantState::READY, SERVER_KNOBS->ENABLE_ENCRYPTION);

			tenantCache.insert(tenantName, tenant);
		}

		for (int i = 0; i < tenantLimit; i++) {
			Key k(format("%d", i));
			ASSERT(tenantCache.isTenantKey(k.withPrefix(TenantMapEntry::idToPrefix(tenantNumber + (i % tenantCount)))));
			ASSERT(!tenantCache.isTenantKey(k.withPrefix(allKeys.begin)));
			ASSERT(!tenantCache.isTenantKey(k));
		}

		return Void();
	}

	ACTOR static Future<Void> RefreshAndTestPresence() {
		wait(Future<Void>(Void()));

		Database cx;
		TenantCache tenantCache(cx, UID(1, 0));

		constexpr static uint16_t tenantLimit = 64;

		uint16_t tenantCount = deterministicRandom()->randomInt(1, tenantLimit);
		uint16_t tenantNumber = deterministicRandom()->randomInt(0, std::numeric_limits<uint16_t>::max());

		for (uint16_t i = 0; i < tenantCount; i++) {
			TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantNumber + i));
			TenantMapEntry tenant(tenantNumber + i, TenantState::READY, SERVER_KNOBS->ENABLE_ENCRYPTION);

			tenantCache.insert(tenantName, tenant);
		}

		uint16_t staleTenantFraction = deterministicRandom()->randomInt(1, 8);
		tenantCache.startRefresh();

		int keepCount = 0, removeCount = 0;
		for (int i = 0; i < tenantCount; i++) {
			uint16_t tenantOrdinal = tenantNumber + i;

			if (tenantOrdinal % staleTenantFraction != 0) {
				TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantOrdinal));
				TenantMapEntry tenant(tenantOrdinal, TenantState::READY, SERVER_KNOBS->ENABLE_ENCRYPTION);
				bool newTenant = tenantCache.update(tenantName, tenant);
				ASSERT(!newTenant);
				keepCount++;
			} else {
				removeCount++;
			}
		}
		int tenantsRemoved = tenantCache.cleanup();
		ASSERT(tenantsRemoved == removeCount);

		int keptCount = 0, removedCount = 0;
		for (int i = 0; i < tenantCount; i++) {
			uint16_t tenantOrdinal = tenantNumber + i;
			Key k(format("%d", i));
			if (tenantOrdinal % staleTenantFraction != 0) {
				ASSERT(tenantCache.isTenantKey(k.withPrefix(TenantMapEntry::idToPrefix(tenantOrdinal))));
				keptCount++;
			} else {
				ASSERT(!tenantCache.isTenantKey(k.withPrefix(TenantMapEntry::idToPrefix(tenantOrdinal))));
				removedCount++;
			}
		}

		ASSERT(keepCount == keptCount);
		ASSERT(removeCount == removedCount);

		return Void();
	}
};

TEST_CASE("/TenantCache/InsertAndTestPresence") {
	wait(TenantCacheUnitTest::InsertAndTestPresence());
	return Void();
}

TEST_CASE("/TenantCache/RefreshAndTestPresence") {
	wait(TenantCacheUnitTest::RefreshAndTestPresence());
	return Void();
}
