/*
 * TenantEntryCache.actor.cpp
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

#include "fdbserver/TenantEntryCache.h"

#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define TEC_DEBUG true

namespace {
ACTOR Future<TenantNameEntryPairVec> getTenantList(Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	KeyBackedRangeResult<TenantNameEntryPair> tenantList = wait(TenantMetadata::tenantMap.getRange(
	    tr, Optional<TenantName>(), Optional<TenantName>(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
	ASSERT(tenantList.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantList.more);

	if (TEC_DEBUG) {
		TraceEvent(SevDebug, "TenantEntryCache_GetTenantList").detail("Num", tenantList.results.size());
	}

	return tenantList.results;
}
} // namespace

template <class T>
class TenantEntryCacheImpl {
public:
	ACTOR static Future<Void> refresh(TenantEntryCache<T>* cache, TenantEntryCacheRefreshReason reason) {
		TraceEvent("TenantEntryCache_RefreshStart", cache->id()).detail("Reason", static_cast<int>(reason));

		// Increment cache generation count
		cache->incGen();
		ASSERT(cache->getCurGen() < std::numeric_limits<uint64_t>::max());

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cache->getDatabase());
		loop {
			try {
				state TenantNameEntryPairVec tenantList = wait(getTenantList(tr));

				for (auto& tenant : tenantList) {
					cache->put(tenant);
				}
				break;
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("TenantEntryCache_RefreshError", cache->id()).errorUnsuppressed(e).suppressFor(1.0);
				}
				wait(tr->onError(e));
			}
		}

		TraceEvent("TenantEntryCache_RefreshEnd", cache->id())
		    .detail("Reason", static_cast<int>(reason))
		    .detail("CurGen", cache->getCurGen());

		return Void();
	}

	ACTOR static Future<Void> sweep(TenantEntryCache<T>* cache) {
		state Future<Void> trigger = cache->waitForSweepTrigger();

		loop {
			wait(trigger);
			wait(delay(.001)); // Coalesce multiple triggers

			trigger = cache->waitForSweepTrigger();
			cache->sweep();
		}
	}

	ACTOR static Future<TenantEntryCachePayload<T>> getById(TenantEntryCache<T>* cache, int64_t tenantId) {
		try {
			return cache->lookupById(tenantId);
		} catch (Error& e) {
			if (e.code() != error_code_key_not_found) {
				TraceEvent(SevInfo, "TenantNotFound", cache->id())
				    .detail("TenantId", tenantId)
				    .detail("ErroCode", e.code());
				throw e;
			}
		}

		// Entry not found. Refresh cacheEntries by scanning underling KeyRange.
		// TODO: Cache will implement a "KeyRange" watch, monitoring notification when a new entry gets added or any
		// existing entry gets updated within the KeyRange of interest. Hence, misses would be very rare
		wait(refresh(cache, TenantEntryCacheRefreshReason::CACHE_MISS));

		try {
			return cache->lookupById(tenantId);
		} catch (Error& e) {
			TraceEvent(SevInfo, "TenantNotFound", cache->id())
			    .detail("TenantId", tenantId)
			    .detail("ErrorCode", e.code());
			throw e;
		}
	}

	ACTOR static Future<TenantEntryCachePayload<T>> getByPrefix(TenantEntryCache<T>* cache, KeyRef tenantPrefix) {
		try {
			return cache->lookupByPrefix(tenantPrefix);
		} catch (Error& e) {
			if (e.code() != error_code_key_not_found) {
				TraceEvent(SevInfo, "TenantEntryCache_TenantNotFound", cache->id())
				    .detail("TenantPrefix", tenantPrefix)
				    .detail("ErrorCode", e.code());
				throw e;
			}
		}

		// Entry not found. Refresh cacheEntries by scanning underling KeyRange.
		// TODO: Cache will implement a "KeyRange" watch, monitoring notification when a new entry gets added or any
		// existing entry gets updated within the KeyRange of interest. Hence, misses would be very rare
		wait(refresh(cache, TenantEntryCacheRefreshReason::CACHE_MISS));

		try {
			return cache->lookupByPrefix(tenantPrefix);
		} catch (Error& e) {
			TraceEvent(SevInfo, "TenantEntryCache_TenantNotFound", cache->id())
			    .detail("TenantPrefix", tenantPrefix)
			    .detail("ErroCode", e.code());
			throw e;
		}
	}
};

template <class T>
TenantEntryCache<T>::TenantEntryCache(Database db, TenantEntryCachePayloadFunc<T> fn)
  : uid(deterministicRandom()->randomUniqueID()), db(db), curGen(INITIAL_CACHE_GEN), createPayloadFunc(fn) {
	TraceEvent("TenantEntryCache_Created", uid).detail("Gen", curGen);
}

template <class T>
void TenantEntryCache<T>::refresh(TenantEntryCacheRefreshReason reason) {
	Future<Void> f = TenantEntryCacheImpl<T>::refresh(this, reason);
}

template <class T>
Future<Void> TenantEntryCache<T>::init() {
	TraceEvent(SevInfo, "TenantEntryCache_Init", uid).log();

	// Launch sweeper to cleanup stale entries.
	sweeper = TenantEntryCacheImpl<T>::sweep(this);

	Future<Void> f = TenantEntryCacheImpl<T>::refresh(this, TenantEntryCacheRefreshReason::INIT);

	// Launch reaper task to periodically refresh cache by scanning database KeyRange
	TenantEntryCacheRefreshReason reason = TenantEntryCacheRefreshReason::PERIODIC_TASK;
	refresher = recurring(
	    [&, reason]() { refresh(reason); }, SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL, TaskPriority::Worker);

	return f;
}

template <class T>
void TenantEntryCache<T>::sweep() {
	TraceEvent(SevInfo, "TenantEntryCache_SweepStart", uid).log();

	// Sweep cache and clean up entries older than current generation
	TenantNameEntryPairVec entriesToRemove;
	for (auto& e : mapByTenantId) {
		ASSERT(e.value.gen <= curGen);

		KeyRef prefixRef(e.value.entry.prefix.begin(), e.value.entry.prefix.size());
		if (e.value.gen != curGen) {
			ASSERT(mapByTenantPrefix.find(prefixRef) != mapByTenantPrefix.end());
			entriesToRemove.emplace_back(std::make_pair(e.value.name, e.value.entry));
		}
	}

	for (auto& r : entriesToRemove) {
		TraceEvent(SevInfo, "TenantEntryCache_Remove", uid)
		    .detail("TenantName", r.first)
		    .detail("TenantID", r.second.id)
		    .detail("TenantPrefix", r.second.prefix);

		mapByTenantId.erase(r.second.id);

		KeyRef prefixRef(r.second.prefix.begin(), r.second.prefix.size());
		mapByTenantPrefix.erase(prefixRef);
	}

	TraceEvent(SevInfo, "TenantEntryCache_SweepDone", uid).detail("Removed", entriesToRemove.size());
}

template <class T>
void TenantEntryCache<T>::put(const TenantNameEntryPair& pair) {
	KeyRef tenantPrefix(pair.second.prefix.begin(), pair.second.prefix.size());

	TenantEntryCachePayload<T> payload = createPayloadFunc(pair.first, pair.second, curGen);
	if (mapByTenantId.find(pair.second.id) == mapByTenantId.end()) {
		// Ensure both caches are in sync
		ASSERT(mapByTenantPrefix.find(tenantPrefix) == mapByTenantPrefix.end());
		mapByTenantId[pair.second.id] = payload;
		mapByTenantPrefix[tenantPrefix] = payload;

		TraceEvent(SevInfo, "TenantEntryCache_NewTenant", uid)
		    .detail("TenantName", pair.first)
		    .detail("TenantID", pair.second.id)
		    .detail("TenantPrefix", pair.second.prefix)
		    .detail("Generation", curGen);
	} else {
		mapByTenantId[pair.second.id] = payload;
		mapByTenantPrefix[tenantPrefix] = payload;

		TraceEvent(SevInfo, "TenantEntryCache_UpdateTenant", uid)
		    .detail("TenantName", pair.first)
		    .detail("TenantID", pair.second.id)
		    .detail("TenantPrefix", pair.second.prefix)
		    .detail("Generation", curGen);
	}
}

template <class T>
TenantEntryCachePayload<T> TenantEntryCache<T>::lookupById(int64_t tenantId) {
	auto itr = mapByTenantId.find(tenantId);
	if (itr == mapByTenantId.end()) {
		throw key_not_found();
	}

	return itr->value;
}

template <class T>
Future<TenantEntryCachePayload<T>> TenantEntryCache<T>::getById(int64_t tenantId) {
	return TenantEntryCacheImpl<T>::getById(this, tenantId);
}

template <class T>
TenantEntryCachePayload<T> TenantEntryCache<T>::lookupByPrefix(const KeyRef& prefix) {
	auto itr = mapByTenantPrefix.find(prefix);
	if (itr == mapByTenantPrefix.end()) {
		throw key_not_found();
	}

	return itr->value;
}

template <class T>
Future<TenantEntryCachePayload<T>> TenantEntryCache<T>::getByPrefix(KeyRef prefix) {
	return TenantEntryCacheImpl<T>::getByPrefix(prefix);
}
