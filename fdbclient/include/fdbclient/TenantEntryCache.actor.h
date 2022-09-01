/*
 * TenantEntryCache.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANTENTRYCACHE_ACTOR_G_H)
#define FDBCLIENT_TENANTENTRYCACHE_ACTOR_G_H
#include "fdbclient/TenantEntryCache.actor.g.h"
#elif !defined(FDBCLIENT_TENANTENTRYCACHE_ACTOR_H)
#define FDBCLIENT_TENANTENTRYCACHE_ACTOR_H

#pragma once

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Knobs.h"
#include "fdbrpc/TenantName.h"
#include "flow/IndexedSet.h"

#include <functional>
#include <unordered_map>

#include "flow/actorcompiler.h" // has to be last include

using TenantNameEntryPair = std::pair<TenantName, TenantMapEntry>;
using TenantNameEntryPairVec = std::vector<TenantNameEntryPair>;

enum class TenantEntryCacheRefreshReason { INIT = 1, PERIODIC_TASK = 2, CACHE_MISS = 3, REMOVE_ENTRY = 4 };
enum class TenantEntryCacheRefreshMode { PERIODIC_TASK = 1, NONE = 2 };

template <class T>
struct TenantEntryCachePayload {
	TenantName name;
	TenantMapEntry entry;
	// Custom client payload
	T payload;
};

template <class T>
using TenantEntryCachePayloadFunc = std::function<TenantEntryCachePayload<T>(const TenantName&, const TenantMapEntry&)>;

// In-memory cache for TenantEntryMap objects. It supports three indices:
// 1. Lookup by 'TenantId'
// 2. Lookup by 'TenantPrefix'
// 3. Lookup by 'TenantName'
//
// TODO:
// ----
// The cache allows user to construct the 'cached object' by supplying a callback. The cache implements a periodic
// refresh mechanism, polling underlying database for updates (add/remove tenants), in future we might want to implement
// database range-watch to monitor such updates

template <class T>
class TenantEntryCache : public ReferenceCounted<TenantEntryCache<T>>, NonCopyable {
private:
	UID uid;
	Database db;
	TenantEntryCachePayloadFunc<T> createPayloadFunc;
	TenantEntryCacheRefreshMode refreshMode;

	Future<Void> refresher;
	Map<int64_t, TenantEntryCachePayload<T>> mapByTenantId;
	Map<TenantName, TenantEntryCachePayload<T>> mapByTenantName;

	CounterCollection metrics;
	Counter hits;
	Counter misses;
	Counter refreshByCacheInit;
	Counter refreshByCacheMiss;
	Counter numRefreshes;

	ACTOR static Future<TenantNameEntryPairVec> getTenantList(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

		KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenantList =
		    wait(TenantMetadata::tenantMap().getRange(
		        tr, Optional<TenantName>(), Optional<TenantName>(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		ASSERT(tenantList.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantList.more);

		TraceEvent(SevDebug, "TenantEntryCacheGetTenantList").detail("Count", tenantList.results.size());

		return tenantList.results;
	}

	static void updateCacheRefreshMetrics(TenantEntryCache<T>* cache, TenantEntryCacheRefreshReason reason) {
		if (reason == TenantEntryCacheRefreshReason::INIT) {
			cache->refreshByCacheInit += 1;
		} else if (reason == TenantEntryCacheRefreshReason::CACHE_MISS) {
			cache->refreshByCacheMiss += 1;
		}

		cache->numRefreshes += 1;
	}

	ACTOR static Future<Void> refreshImpl(TenantEntryCache<T>* cache, TenantEntryCacheRefreshReason reason) {
		TraceEvent(SevDebug, "TenantEntryCacheRefreshStart", cache->id()).detail("Reason", static_cast<int>(reason));

		state Reference<ReadYourWritesTransaction> tr = cache->getDatabase()->createTransaction();
		loop {
			try {
				state TenantNameEntryPairVec tenantList = wait(getTenantList(tr));

				// Refresh cache entries reflecting the latest database state
				cache->clear();
				for (auto& tenant : tenantList) {
					cache->put(tenant);
				}

				updateCacheRefreshMetrics(cache, reason);
				break;
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent(SevInfo, "TenantEntryCacheRefreshError", cache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr->onError(e));
			}
		}

		TraceEvent(SevDebug, "TenantEntryCacheRefreshEnd", cache->id()).detail("Reason", static_cast<int>(reason));

		return Void();
	}

	ACTOR static Future<Optional<TenantEntryCachePayload<T>>> getByIdImpl(TenantEntryCache<T>* cache,
	                                                                      int64_t tenantId) {
		Optional<TenantEntryCachePayload<T>> ret = cache->lookupById(tenantId);
		if (ret.present()) {
			cache->hits += 1;
			return ret;
		}

		TraceEvent(SevInfo, "TenantEntryCacheGetByIdRefresh").detail("TenantId", tenantId);

		// Entry not found. Refresh cacheEntries by scanning underlying KeyRange.
		// TODO: Cache will implement a "KeyRange" watch, monitoring notification when a new entry gets added or any
		// existing entry gets updated within the KeyRange of interest. Hence, misses would be very rare
		wait(refreshImpl(cache, TenantEntryCacheRefreshReason::CACHE_MISS));

		cache->misses += 1;
		return cache->lookupById(tenantId);
	}

	ACTOR static Future<Optional<TenantEntryCachePayload<T>>> getByNameImpl(TenantEntryCache<T>* cache,
	                                                                        TenantName name) {
		Optional<TenantEntryCachePayload<T>> ret = cache->lookupByName(name);
		if (ret.present()) {
			cache->hits += 1;
			return ret;
		}

		TraceEvent("TenantEntryCacheGetByNameRefresh").detail("TenantName", name);

		// Entry not found. Refresh cacheEntries by scanning underlying KeyRange.
		// TODO: Cache will implement a "KeyRange" watch, monitoring notification when a new entry gets added or any
		// existing entry gets updated within the KeyRange of interest. Hence, misses would be very rare
		wait(refreshImpl(cache, TenantEntryCacheRefreshReason::CACHE_MISS));

		cache->misses += 1;
		return cache->lookupByName(name);
	}

	Optional<TenantEntryCachePayload<T>> lookupById(int64_t tenantId) {
		Optional<TenantEntryCachePayload<T>> ret;
		auto itr = mapByTenantId.find(tenantId);
		if (itr == mapByTenantId.end()) {
			return ret;
		}

		return itr->value;
	}

	Optional<TenantEntryCachePayload<T>> lookupByName(TenantName name) {
		Optional<TenantEntryCachePayload<T>> ret;
		auto itr = mapByTenantName.find(name);
		if (itr == mapByTenantName.end()) {
			return ret;
		}

		return itr->value;
	}

	Future<Void> refresh(TenantEntryCacheRefreshReason reason) { return refreshImpl(this, reason); }

	static TenantEntryCachePayload<Void> defaultCreatePayload(const TenantName& name, const TenantMapEntry& entry) {
		TenantEntryCachePayload<Void> payload;
		payload.name = name;
		payload.entry = entry;

		return payload;
	}

	Future<Void> removeEntryInt(Optional<int64_t> tenantId,
	                            Optional<KeyRef> tenantPrefix,
	                            Optional<TenantName> tenantName,
	                            bool refreshCache) {
		typename Map<int64_t, TenantEntryCachePayload<T>>::iterator itrId;
		typename Map<TenantName, TenantEntryCachePayload<T>>::iterator itrName;

		if (tenantId.present() || tenantPrefix.present()) {
			// Ensure either tenantId OR tenantPrefix is valid (but not both)
			ASSERT(tenantId.present() != tenantPrefix.present());
			ASSERT(!tenantName.present());

			int64_t tId = tenantId.present() ? tenantId.get() : TenantMapEntry::prefixToId(tenantPrefix.get());
			TraceEvent("TenantEntryCacheRemoveEntry").detail("Id", tId);
			itrId = mapByTenantId.find(tId);
			if (itrId == mapByTenantId.end()) {
				return Void();
			}
			// Ensure byId and byName cache are in-sync
			itrName = mapByTenantName.find(itrId->value.name);
			ASSERT(itrName != mapByTenantName.end());
		} else if (tenantName.present()) {
			ASSERT(!tenantId.present() && !tenantPrefix.present());

			TraceEvent("TenantEntryCacheRemoveEntry").detail("Name", tenantName.get());
			itrName = mapByTenantName.find(tenantName.get());
			if (itrName == mapByTenantName.end()) {
				return Void();
			}
			// Ensure byId and byName cache are in-sync
			itrId = mapByTenantId.find(itrName->value.entry.id);
			ASSERT(itrId != mapByTenantId.end());
		} else {
			// Invalid input, one of: tenantId, tenantPrefix or tenantName needs to be valid.
			throw operation_failed();
		}

		ASSERT(itrId != mapByTenantId.end() && itrName != mapByTenantName.end());

		TraceEvent("TenantEntryCacheRemoveEntry")
		    .detail("Id", itrId->key)
		    .detail("Prefix", itrId->value.entry.prefix)
		    .detail("Name", itrName->key);

		mapByTenantId.erase(itrId);
		mapByTenantName.erase(itrName);

		if (refreshCache) {
			return refreshImpl(this, TenantEntryCacheRefreshReason::REMOVE_ENTRY);
		}

		return Void();
	}

public:
	TenantEntryCache(Database db)
	  : uid(deterministicRandom()->randomUniqueID()), db(db), createPayloadFunc(defaultCreatePayload),
	    refreshMode(TenantEntryCacheRefreshMode::PERIODIC_TASK), metrics("TenantEntryCacheMetrics", uid.toString()),
	    hits("TenantEntryCacheHits", metrics), misses("TenantEntryCacheMisses", metrics),
	    refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCacheCreatedDefaultFunc", uid);
	}

	TenantEntryCache(Database db, TenantEntryCachePayloadFunc<T> fn)
	  : uid(deterministicRandom()->randomUniqueID()), db(db), createPayloadFunc(fn),
	    refreshMode(TenantEntryCacheRefreshMode::PERIODIC_TASK), metrics("TenantEntryCacheMetrics", uid.toString()),
	    hits("TenantEntryCacheHits", metrics), misses("TenantEntryCacheMisses", metrics),
	    refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCacheCreated", uid);
	}

	TenantEntryCache(Database db, UID id, TenantEntryCachePayloadFunc<T> fn)
	  : uid(id), db(db), createPayloadFunc(fn), refreshMode(TenantEntryCacheRefreshMode::PERIODIC_TASK),
	    metrics("TenantEntryCacheMetrics", uid.toString()), hits("TenantEntryCacheHits", metrics),
	    misses("TenantEntryCacheMisses", metrics), refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCacheCreated", uid);
	}

	TenantEntryCache(Database db, UID id, TenantEntryCachePayloadFunc<T> fn, TenantEntryCacheRefreshMode mode)
	  : uid(id), db(db), createPayloadFunc(fn), refreshMode(mode), metrics("TenantEntryCacheMetrics", uid.toString()),
	    hits("TenantEntryCacheHits", metrics), misses("TenantEntryCacheMisses", metrics),
	    refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCacheCreated", uid);
	}

	Future<Void> init() {
		TraceEvent("TenantEntryCacheInit", uid);

		Future<Void> f = refreshImpl(this, TenantEntryCacheRefreshReason::INIT);

		// Launch reaper task to periodically refresh cache by scanning database KeyRange
		TenantEntryCacheRefreshReason reason = TenantEntryCacheRefreshReason::PERIODIC_TASK;
		if (refreshMode == TenantEntryCacheRefreshMode::PERIODIC_TASK) {
			refresher = recurringAsync([&, reason]() { return refresh(reason); },
			                           CLIENT_KNOBS->TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL, /* interval */
			                           true, /* absoluteIntervalDelay */
			                           CLIENT_KNOBS->TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL, /* intialDelay */
			                           TaskPriority::Worker);
		}

		return f;
	}

	Database getDatabase() const { return db; }
	UID id() const { return uid; }

	void clear() {
		mapByTenantId.clear();
		mapByTenantName.clear();
	}

	Future<Void> removeEntryById(int64_t tenantId, bool refreshCache = false) {
		return removeEntryInt(tenantId, Optional<KeyRef>(), Optional<TenantName>(), refreshCache);
	}
	Future<Void> removeEntryByPrefix(KeyRef tenantPrefix, bool refreshCache = false) {
		return removeEntryInt(Optional<int64_t>(), tenantPrefix, Optional<TenantName>(), refreshCache);
	}
	Future<Void> removeEntryByName(TenantName tenantName, bool refreshCache = false) {
		return removeEntryInt(Optional<int64_t>(), Optional<KeyRef>(), tenantName, refreshCache);
	}

	void put(const TenantNameEntryPair& pair) {
		TenantEntryCachePayload<T> payload = createPayloadFunc(pair.first, pair.second);
		auto idItr = mapByTenantId.find(pair.second.id);
		auto nameItr = mapByTenantName.find(pair.first);

		Optional<TenantName> existingName;
		Optional<int64_t> existingId;
		if (nameItr != mapByTenantName.end()) {
			existingId = nameItr->value.entry.id;
			mapByTenantId.erase(nameItr->value.entry.id);
		}
		if (idItr != mapByTenantId.end()) {
			existingName = idItr->value.name;
			mapByTenantName.erase(idItr->value.name);
		}

		mapByTenantId[pair.second.id] = payload;
		mapByTenantName[pair.first] = payload;

		TraceEvent("TenantEntryCachePut")
		    .detail("TenantName", pair.first)
		    .detail("TenantNameExisting", existingName)
		    .detail("TenantID", pair.second.id)
		    .detail("TenantIDExisting", existingId)
		    .detail("TenantPrefix", pair.second.prefix);

		CODE_PROBE(idItr == mapByTenantId.end() && nameItr == mapByTenantName.end(), "TenantCache new entry");
		CODE_PROBE(idItr != mapByTenantId.end() && nameItr == mapByTenantName.end(), "TenantCache entry name updated");
		CODE_PROBE(idItr == mapByTenantId.end() && nameItr != mapByTenantName.end(), "TenantCache entry id updated");
		CODE_PROBE(idItr != mapByTenantId.end() && nameItr != mapByTenantName.end(),
		           "TenantCache entry id and name updated");
	}

	Future<Optional<TenantEntryCachePayload<T>>> getById(int64_t tenantId) { return getByIdImpl(this, tenantId); }
	Future<Optional<TenantEntryCachePayload<T>>> getByPrefix(KeyRef prefix) {
		int64_t id = TenantMapEntry::prefixToId(prefix);
		return getByIdImpl(this, id);
	}
	Future<Optional<TenantEntryCachePayload<T>>> getByName(TenantName name) { return getByNameImpl(this, name); }

	// Counter access APIs
	Counter::Value numCacheRefreshes() const { return numRefreshes.getValue(); }
	Counter::Value numRefreshByMisses() const { return refreshByCacheMiss.getValue(); }
	Counter::Value numRefreshByInit() const { return refreshByCacheInit.getValue(); }
};

#include "flow/unactorcompiler.h"
#endif // FDBCLIENT_TENANTENTRYCACHE_ACTOR_H