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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TENANTENTRYCACHE_ACTOR_G_H)
#define FDBSERVER_TENANTENTRYCACHE_ACTOR_G_H
#include "fdbserver/TenantEntryCache.actor.g.h"
#elif !defined(FDBSERVER_TENANTENTRYCACHE_ACTOR_H)
#define FDBSERVER_TENANTENTRYCACHE_ACTOR_H

#pragma once

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/IndexedSet.h"

#include <functional>
#include <unordered_map>

#include "flow/actorcompiler.h" // has to be last include

#define INITIAL_CACHE_GEN 1

using TenantEntryCacheGen = uint64_t;
using TenantNameEntryPair = std::pair<TenantName, TenantMapEntry>;
using TenantNameEntryPairVec = std::vector<TenantNameEntryPair>;

enum class TenantEntryCacheRefreshReason { INIT = 1, PERIODIC_TASK = 2, CACHE_MISS = 3 };

template <class T>
struct TenantEntryCachePayload {
	TenantName name;
	TenantMapEntry entry;
	TenantEntryCacheGen gen;
	// Custom client payload
	T payload;
};

template <class T>
using TenantEntryCachePayloadFunc =
    std::function<TenantEntryCachePayload<T>(const TenantName&, const TenantMapEntry&, const TenantEntryCacheGen)>;

// In-memory cache for TenantEntryMap objects. It supports three indices:
// 1. Lookup by 'TenantId'
// 2. Lookup by 'TenantPrefix'
// 3. Lookup by 'TenantName'
//
// TODO:
// ----
// The cache allows user to construct the 'cached object' by supplying a callback to allow constructing the cached
// object. The cache implements a periodic refresh mechanism, polling underlying database for updates (add/remove
// tenants), in future we might want to implement database range-watch to monitor such updates

template <class T>
class TenantEntryCache : public ReferenceCounted<TenantEntryCache<T>>, NonCopyable {
private:
	UID uid;
	Database db;
	TenantEntryCacheGen curGen;
	TenantEntryCachePayloadFunc<T> createPayloadFunc;

	AsyncTrigger sweepTrigger;
	Future<Void> sweeper;
	Future<Void> refresher;
	Map<int64_t, TenantEntryCachePayload<T>> mapByTenantId;
	Map<TenantName, TenantEntryCachePayload<T>> mapByTenantName;

	CounterCollection metrics;
	Counter hits;
	Counter misses;
	Counter refreshByCacheInit;
	Counter refreshByCacheMiss;
	Counter numSweeps;
	Counter numRefreshes;

	ACTOR static Future<TenantNameEntryPairVec> getTenantList(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

		KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenantList =
		    wait(TenantMetadata::tenantMap().getRange(
		        tr, Optional<TenantName>(), Optional<TenantName>(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		ASSERT(tenantList.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantList.more);

		TraceEvent(SevDebug, "TenantEntryCache.GetTenantList").detail("Count", tenantList.results.size());

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
		TraceEvent(SevDebug, "TenantEntryCache.RefreshStart", cache->id()).detail("Reason", static_cast<int>(reason));

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

				cache->triggerSweeper();
				updateCacheRefreshMetrics(cache, reason);
				break;
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent(SevInfo, "TenantEntryCache.RefreshError", cache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr->onError(e));
			}
		}

		TraceEvent(SevDebug, "TenantEntryCache.RefreshEnd", cache->id())
		    .detail("Reason", static_cast<int>(reason))
		    .detail("CurGen", cache->getCurGen());

		return Void();
	}

	ACTOR static Future<Void> sweepImpl(TenantEntryCache<T>* cache) {
		state Future<Void> trigger = cache->waitForSweepTrigger();

		loop {
			wait(trigger);
			wait(delay(.001)); // Coalesce multiple triggers

			trigger = cache->waitForSweepTrigger();
			cache->sweep();
			cache->numSweeps += 1;
		}
	}

	ACTOR static Future<Optional<TenantEntryCachePayload<T>>> getByIdImpl(TenantEntryCache<T>* cache,
	                                                                      int64_t tenantId) {
		Optional<TenantEntryCachePayload<T>> ret = cache->lookupById(tenantId);
		if (ret.present()) {
			cache->hits += 1;
			return ret;
		}

		TraceEvent(SevInfo, "TenantEntryCache.GetByIdRefresh").detail("TenantId", tenantId);

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

		TraceEvent(SevInfo, "TenantEntryCache.GetByNameRefresh").detail("TenantName", name.contents().toString());

		// Entry not found. Refresh cacheEntries by scanning underlying KeyRange.
		// TODO: Cache will implement a "KeyRange" watch, monitoring notification when a new entry gets added or any
		// existing entry gets updated within the KeyRange of interest. Hence, misses would be very rare
		wait(refreshImpl(cache, TenantEntryCacheRefreshReason::CACHE_MISS));

		cache->misses += 1;
		return cache->lookupByName(name);
	}

	void incGen() { curGen++; }
	Future<Void> waitForSweepTrigger() { return sweepTrigger.onTrigger(); }
	void triggerSweeper() { sweepTrigger.trigger(); }

	void sweep() {
		TraceEvent(SevDebug, "TenantEntryCache.SweepStart", uid).log();

		// Sweep cache and clean up entries older than current generation
		TenantNameEntryPairVec entriesToRemove;
		for (auto& e : mapByTenantId) {
			ASSERT(e.value.gen <= curGen);

			if (e.value.gen != curGen) {
				entriesToRemove.emplace_back(std::make_pair(e.value.name, e.value.entry));
			}
		}

		for (auto& r : entriesToRemove) {
			TraceEvent(SevInfo, "TenantEntryCache.Remove", uid)
			    .detail("TenantName", r.first.contents())
			    .detail("TenantID", r.second.id)
			    .detail("TenantPrefix", r.second.prefix);

			mapByTenantId.erase(r.second.id);
			mapByTenantName.erase(r.first);
		}

		TraceEvent(SevInfo, "TenantEntryCache.SweepDone", uid).detail("Removed", entriesToRemove.size());
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

	static TenantEntryCachePayload<Void> defaultCreatePayload(const TenantName& name,
	                                                          const TenantMapEntry& entry,
	                                                          const TenantEntryCacheGen gen) {
		TenantEntryCachePayload<Void> payload;
		payload.name = name;
		payload.entry = entry;
		payload.gen = gen;

		return payload;
	}

public:
	TenantEntryCache(Database db)
	  : uid(deterministicRandom()->randomUniqueID()), db(db), curGen(INITIAL_CACHE_GEN),
	    createPayloadFunc(defaultCreatePayload), metrics("TenantEntryCacheMetrics", uid.toString()),
	    hits("TenantEntryCacheHits", metrics), misses("TenantEntryCacheMisses", metrics),
	    refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics), numSweeps("TenantEntryCacheNumSweeps", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCache.CreatedDefaultFunc", uid).detail("Gen", curGen);
	}

	TenantEntryCache(Database db, TenantEntryCachePayloadFunc<T> fn)
	  : uid(deterministicRandom()->randomUniqueID()), db(db), curGen(INITIAL_CACHE_GEN), createPayloadFunc(fn),
	    metrics("TenantEntryCacheMetrics", uid.toString()), hits("TenantEntryCacheHits", metrics),
	    misses("TenantEntryCacheMisses", metrics), refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics), numSweeps("TenantEntryCacheNumSweeps", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCache.Created", uid).detail("Gen", curGen);
	}

	TenantEntryCache(Database db, UID id, TenantEntryCachePayloadFunc<T> fn)
	  : uid(id), db(db), curGen(INITIAL_CACHE_GEN), createPayloadFunc(fn),
	    metrics("TenantEntryCacheMetrics", uid.toString()), hits("TenantEntryCacheHits", metrics),
	    misses("TenantEntryCacheMisses", metrics), refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics), numSweeps("TenantEntryCacheNumSweeps", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics) {
		TraceEvent("TenantEntryCache.Created", uid).detail("Gen", curGen);
	}

	Future<Void> init() {
		TraceEvent(SevInfo, "TenantEntryCache.Init", uid).log();

		// Launch sweeper to cleanup stale entries.
		sweeper = sweepImpl(this);

		Future<Void> f = refreshImpl(this, TenantEntryCacheRefreshReason::INIT);

		// Launch reaper task to periodically refresh cache by scanning database KeyRange
		TenantEntryCacheRefreshReason reason = TenantEntryCacheRefreshReason::PERIODIC_TASK;
		refresher = recurringAsync([&, reason]() { return refresh(reason); },
		                           SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL, /* interval */
		                           true, /* absoluteIntervalDelay */
		                           SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL, /* intialDelay */
		                           TaskPriority::Worker);

		return f;
	}

	Database getDatabase() const { return db; }
	UID id() const { return uid; }
	TenantEntryCacheGen getCurGen() const { return curGen; }

	void clear() {
		mapByTenantId.clear();
		mapByTenantName.clear();
	}

	void put(const TenantNameEntryPair& pair) {
		TenantEntryCachePayload<T> payload = createPayloadFunc(pair.first, pair.second, curGen);
		if (mapByTenantId.find(pair.second.id) == mapByTenantId.end()) {
			ASSERT(mapByTenantName.find(pair.first) == mapByTenantName.end());

			mapByTenantId[pair.second.id] = payload;
			mapByTenantName[pair.first] = payload;

			TraceEvent(SevInfo, "TenantEntryCache.NewTenant", uid)
			    .detail("TenantName", pair.first.contents().toString())
			    .detail("TenantID", pair.second.id)
			    .detail("TenantPrefix", pair.second.prefix)
			    .detail("Generation", curGen);
		} else {
			mapByTenantId[pair.second.id] = payload;
			mapByTenantName[pair.first] = payload;

			TraceEvent(SevInfo, "TenantEntryCache.UpdateTenant", uid)
			    .detail("TenantName", pair.first.contents().toString())
			    .detail("TenantID", pair.second.id)
			    .detail("TenantPrefix", pair.second.prefix)
			    .detail("Generation", curGen);
		}
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
#endif // FDBSERVER_TENANTENTRYCACHE_ACTOR_H