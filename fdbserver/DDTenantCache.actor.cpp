/*
 * DDTenantCache.actor.cpp
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

#include "fdbserver/DDTenantCache.h"
#include "fdbserver/DDTeamCollection.h"
#include <limits>
#include <string>

class DDTenantCacheImpl {

	ACTOR static Future<RangeResult> getTenantList(DDTenantCache* tenantCache, Transaction* tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

		state Future<RangeResult> tenantList = tr->getRange(tenantMapKeys, CLIENT_KNOBS->TOO_MANY);
		wait(success(tenantList));
		ASSERT(!tenantList.get().more && tenantList.get().size() < CLIENT_KNOBS->TOO_MANY);

		return tenantList.get();
	}

public:
	ACTOR static Future<Void> build(DDTenantCache* tenantCache) {
		state Transaction tr(tenantCache->dbcx());

		TraceEvent(SevInfo, "BuildingDDTenantCache", tenantCache->id()).log();

		try {
			state RangeResult tenantList = wait(getTenantList(tenantCache, &tr));

			for (int i = 0; i < tenantList.size(); i++) {
				TenantName tname = tenantList[i].key.removePrefix(tenantMapPrefix);
				TenantMapEntry t = decodeTenantEntry(tenantList[i].value);

				tenantCache->insert(tname, t);

				TraceEvent(SevInfo, "DDTenantFound", tenantCache->id())
				    .detail("TenantName", tname)
				    .detail("TenantID", t.id)
				    .detail("TenantPrefix", t.prefix);
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}

		TraceEvent(SevInfo, "BuiltDDTenantCache", tenantCache->id()).log();

		return Void();
	}

	ACTOR static Future<Void> monitorTenantMap(DDTenantCache* tenantCache) {
		TraceEvent(SevInfo, "StartingDDTenantCacheMonitor", tenantCache->id()).log();

		state Transaction tr(tenantCache->dbcx());

		state double lastTenantListFetchTime = now();

		loop {
			try {
				if (now() - lastTenantListFetchTime > (2 * SERVER_KNOBS->DD_TENANT_LIST_REFRESH_INTERVAL)) {
					TraceEvent(SevWarn, "DDTenantListRefreshDelay", tenantCache->id()).log();
				}

				state RangeResult tenantList = wait(getTenantList(tenantCache, &tr));

				tenantCache->startRefresh();
				bool tenantListUpdated = false;

				for (int i = 0; i < tenantList.size(); i++) {
					TenantName tname = tenantList[i].key.removePrefix(tenantMapPrefix);
					TenantMapEntry t = decodeTenantEntry(tenantList[i].value);

					if (tenantCache->update(tname, t)) {
						tenantListUpdated = true;
					}
				}

				if (tenantCache->cleanup()) {
					tenantListUpdated = true;
				}

				if (tenantListUpdated) {
					TraceEvent(SevInfo, "DDTenantCache", tenantCache->id()).detail("List", tenantCache->desc());
				}

				lastTenantListFetchTime = now();
				tr.reset();
				wait(delay(SERVER_KNOBS->DD_TENANT_LIST_REFRESH_INTERVAL));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("DDTenantCacheGetTenantListError", tenantCache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}
};

void DDTenantCache::insert(TenantName& tenantName, TenantMapEntry& tenant) {
	KeyRef tenantPrefix(tenant.prefix.begin(), tenant.prefix.size());
	ASSERT(tenantCache.find(tenantPrefix) == tenantCache.end());

	TenantInfo tenantInfo(tenantName, tenant.id);
	tenantCache[tenantPrefix] = makeReference<TCTenantInfo>(tenantInfo, tenant.prefix);
	tenantCache[tenantPrefix]->updateCacheGeneration(generation);
}

void DDTenantCache::startRefresh() {
	ASSERT(generation < std::numeric_limits<uint64_t>::max());
	generation++;
}

void DDTenantCache::keep(TenantName& tenantName, TenantMapEntry& tenant) {
	KeyRef tenantPrefix(tenant.prefix.begin(), tenant.prefix.size());

	ASSERT(tenantCache.find(tenantPrefix) != tenantCache.end());
	tenantCache[tenantPrefix]->updateCacheGeneration(generation);
}

bool DDTenantCache::update(TenantName& tenantName, TenantMapEntry& tenant) {
	KeyRef tenantPrefix(tenant.prefix.begin(), tenant.prefix.size());

	if (tenantCache.find(tenantPrefix) != tenantCache.end()) {
		keep(tenantName, tenant);
		return false;
	}

	insert(tenantName, tenant);
	return true;
}

int DDTenantCache::cleanup() {
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
		ASSERT(tenantCache.find(k) == tenantCache.end());
		tenantsRemoved++;
	}

	return tenantsRemoved;
}

std::string DDTenantCache::desc() const {
	std::string s("@Generation: ");
	s += std::to_string(generation) + " ";
	int count = 0;
	for (auto& [tenantPrefix, tenant] : tenantCache) {
		if (count) {
			s += ", ";
		}

		s += "Name: " + tenant->name().toString() + " Prefix: " + tenantPrefix.printable();
		count++;
	}

	return s;
}

bool DDTenantCache::isTenantKey(KeyRef key) const {
	auto it = tenantCache.lastLessOrEqual(key);
	if (it == tenantCache.end()) {
		return false;
	}

	if (!key.startsWith(it->key)) {
		return false;
	}

	return true;
}

Future<Void> DDTenantCache::build(Database cx) {
	return DDTenantCacheImpl::build(this);
}

Future<Void> DDTenantCache::monitorTenantMap() {
	return DDTenantCacheImpl::monitorTenantMap(this);
}

class DDTenantCacheUnitTest {
public:
	ACTOR static Future<Void> InsertAndTestPresence() {
		wait(Future<Void>(Void()));

		Database cx;
		DDTenantCache tenantCache(cx, UID(1, 0));

		constexpr static uint16_t tenantLimit = 64;

		uint16_t tenantCount = deterministicRandom()->randomInt(1, tenantLimit);
		uint16_t tenantNumber = deterministicRandom()->randomInt(0, std::numeric_limits<uint16_t>::max());

		for (uint16_t i = 0; i < tenantCount; i++) {
			TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantNumber + i));
			TenantMapEntry tenant(tenantNumber + i, KeyRef());

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
		DDTenantCache tenantCache(cx, UID(1, 0));

		constexpr static uint16_t tenantLimit = 64;

		uint16_t tenantCount = deterministicRandom()->randomInt(1, tenantLimit);
		uint16_t tenantNumber = deterministicRandom()->randomInt(0, std::numeric_limits<uint16_t>::max());

		for (uint16_t i = 0; i < tenantCount; i++) {
			TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantNumber + i));
			TenantMapEntry tenant(tenantNumber + i, KeyRef());

			tenantCache.insert(tenantName, tenant);
		}

		uint16_t staleTenantFraction = deterministicRandom()->randomInt(1, 8);
		tenantCache.startRefresh();

		int keepCount = 0, removeCount = 0;
		for (int i = 0; i < tenantCount; i++) {
			uint16_t tenantOrdinal = tenantNumber + i;

			if (tenantOrdinal % staleTenantFraction != 0) {
				TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantOrdinal));
				TenantMapEntry tenant(tenantOrdinal, KeyRef());
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

TEST_CASE("/DataDistribution/TenantCache/InsertAndTestPresence") {
	wait(DDTenantCacheUnitTest::InsertAndTestPresence());
	return Void();
}

TEST_CASE("/DataDistribution/TenantCache/RefreshAndTestPresence") {
	wait(DDTenantCacheUnitTest::RefreshAndTestPresence());
	return Void();
}