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
#include "fdbclient/RunRYWTransaction.actor.h"
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

enum class TenantEntryCacheRefreshReason {
	INIT = 1,
	PERIODIC_TASK = 2,
	CACHE_MISS = 3,
	REMOVE_ENTRY = 4,
	WATCH_TRIGGER = 5
};
enum class TenantEntryCacheRefreshMode { PERIODIC_TASK = 1, WATCH = 2, NONE = 3 };

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
// TODO: Currently this cache performs poorly if there are tenant access happening to unknown tenants which happens most
// frequently in optional tenant mode but can also happen in required mode if there are alot of tenants created. Further
// as a consequence of the design we cannot be sure that the state of a given tenant is accurate even if its present in
// the cache.

template <class T>
class TenantEntryCache : public ReferenceCounted<TenantEntryCache<T>>, NonCopyable {
private:
	UID uid;
	Database db;
	TenantEntryCachePayloadFunc<T> createPayloadFunc;
	TenantEntryCacheRefreshMode refreshMode;

	Future<Void> refresher;
	Future<Void> watchRefresher;
	Future<Void> lastTenantIdRefresher;
	Promise<Void> setInitialWatch;
	Optional<int64_t> lastTenantId;
	Map<int64_t, TenantEntryCachePayload<T>> mapByTenantId;
	Map<TenantName, TenantEntryCachePayload<T>> mapByTenantName;

	CounterCollection metrics;
	Counter hits;
	Counter misses;
	Counter refreshByCacheInit;
	Counter refreshByCacheMiss;
	Counter numRefreshes;
	Counter refreshByWatchTrigger;

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

	ACTOR static Future<Void> refreshCacheById(int64_t tenantId,
	                                           TenantEntryCache<T>* cache,
	                                           TenantEntryCacheRefreshReason reason) {
		TraceEvent(SevDebug, "TenantEntryCacheIDRefreshStart", cache->id()).detail("Reason", static_cast<int>(reason));
		state Reference<ReadYourWritesTransaction> tr = cache->getDatabase()->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				state Optional<TenantName> name = wait(TenantMetadata::tenantIdIndex().get(tr, tenantId));
				if (name.present()) {
					Optional<TenantMapEntry> entry = wait(TenantMetadata::tenantMap().get(tr, name.get()));
					if (entry.present()) {
						cache->put(std::make_pair(name.get(), entry.get()));
						updateCacheRefreshMetrics(cache, reason);
					}
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		TraceEvent(SevDebug, "TenantEntryCacheIDRefreshEnd", cache->id()).detail("Reason", static_cast<int>(reason));
		return Void();
	}

	ACTOR static Future<Void> refreshCacheByName(TenantName name,
	                                             TenantEntryCache<T>* cache,
	                                             TenantEntryCacheRefreshReason reason) {
		TraceEvent(SevDebug, "TenantEntryCacheNameRefreshStart", cache->id())
		    .detail("Reason", static_cast<int>(reason));
		state Reference<ReadYourWritesTransaction> tr = cache->getDatabase()->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				Optional<TenantMapEntry> entry = wait(TenantMetadata::tenantMap().get(tr, name));
				if (entry.present()) {
					cache->put(std::make_pair(name, entry.get()));
					updateCacheRefreshMetrics(cache, reason);
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		TraceEvent(SevDebug, "TenantEntryCacheNameRefreshEnd", cache->id()).detail("Reason", static_cast<int>(reason));
		return Void();
	}

	static void updateCacheRefreshMetrics(TenantEntryCache<T>* cache, TenantEntryCacheRefreshReason reason) {
		if (reason == TenantEntryCacheRefreshReason::INIT) {
			cache->refreshByCacheInit += 1;
		} else if (reason == TenantEntryCacheRefreshReason::CACHE_MISS) {
			cache->refreshByCacheMiss += 1;
		} else if (reason == TenantEntryCacheRefreshReason::WATCH_TRIGGER) {
			cache->refreshByWatchTrigger += 1;
		}

		cache->numRefreshes += 1;
	}

	ACTOR static Future<Void> refreshCacheUsingWatch(TenantEntryCache<T>* cache, TenantEntryCacheRefreshReason reason) {
		TraceEvent(SevDebug, "TenantEntryCacheRefreshUsingWatchStart", cache->id())
		    .detail("Reason", static_cast<int>(reason));

		state Reference<ReadYourWritesTransaction> tr = cache->getDatabase()->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				state Future<Void> tenantModifiedWatch = TenantMetadata::lastTenantModification().watch(tr);
				wait(tr->commit());
				TraceEvent(SevDebug, "TenantEntryCacheRefreshWatchSet", cache->id());
				// setInitialWatch is set to indicate that an inital watch has been set for the lastTenantModification
				// key. Currently this is only used in simulation to avoid a race condition where a tenant is created
				// before the inital watch is set. However, it can be enabled by passing waitForInitalWatch = true to
				// the init() method.
				if (cache->setInitialWatch.canBeSet()) {
					cache->setInitialWatch.send(Void());
				}
				wait(tenantModifiedWatch);
				// If watch triggered then refresh the cache as tenant metadata was updated
				TraceEvent(SevDebug, "TenantEntryCacheRefreshUsingWatchTriggered", cache->id())
				    .detail("Reason", static_cast<int>(reason));
				wait(refreshImpl(cache, reason));
				tr->reset();
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("TenantEntryCacheRefreshUsingWatchError", cache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr->onError(e));
				// In case the watch threw an error then refresh the cache just in case it was updated
				wait(refreshImpl(cache, reason));
			}
		}
	}

	static bool tenantsEnabled(TenantEntryCache<T>* cache) {
		// Avoid using the cache if the tenant mode is disabled. However since we use clientInfo, sometimes it may not
		// be fully up to date (i.e it may indicate the tenantMode is disabled when in fact it is required). Thus if
		// there is at least one tenant that has been created on the cluster then use the cache to avoid an incorrect
		// miss.
		if (cache->getDatabase()->clientInfo->get().tenantMode == TenantMode::DISABLED) {
			if (!cache->lastTenantId.present()) {
				return false;
			}
			return cache->lastTenantId.get() > 0;
		}
		return true;
	}

	ACTOR static Future<Void> setLastTenantId(TenantEntryCache<T>* cache) {
		state Reference<ReadYourWritesTransaction> tr = cache->getDatabase()->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Optional<int64_t> lastTenantId = wait(TenantMetadata::lastTenantId().get(tr));
				cache->lastTenantId = lastTenantId;
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> lastTenantIdWatch(TenantEntryCache<T>* cache) {
		TraceEvent(SevDebug, "TenantEntryCacheLastTenantIdWatchStart", cache->id());
		// monitor for any changes on the last tenant id and update it as necessary
		state Reference<ReadYourWritesTransaction> tr = cache->getDatabase()->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				state Future<Void> lastTenantIdWatch = tr->watch(TenantMetadata::lastTenantId().key);
				wait(tr->commit());
				wait(lastTenantIdWatch);
				wait(setLastTenantId(cache));
				tr->reset();
			} catch (Error& e) {
				state Error err(e);
				if (err.code() != error_code_actor_cancelled) {
					TraceEvent("TenantEntryCacheLastTenantIdWatchError", cache->id())
					    .errorUnsuppressed(err)
					    .suppressFor(1.0);
					// In case watch errors out refresh the lastTenantId in case it has changed or we would have missed
					// an update
					wait(setLastTenantId(cache));
				}
				wait(tr->onError(err));
			}
		}
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
					TraceEvent("TenantEntryCacheRefreshError", cache->id()).errorUnsuppressed(e).suppressFor(1.0);
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

		if (!tenantsEnabled(cache)) {
			// If tenants are disabled on the cluster avoid using the cache
			return Optional<TenantEntryCachePayload<T>>();
		}

		TraceEvent("TenantEntryCacheGetByIdRefresh").detail("TenantId", tenantId);

		if (cache->refreshMode == TenantEntryCacheRefreshMode::WATCH) {
			// Entry not found. Do a point refresh
			// TODO: Don't initiate refresh if tenantId < maxTenantId (stored as a system key currently) as we know that
			// such a tenant does not exist (it has either never existed or has been deleted)
			wait(refreshCacheById(tenantId, cache, TenantEntryCacheRefreshReason::CACHE_MISS));
		} else {
			// Entry not found. Refresh cacheEntries by scanning underlying KeyRange.
			wait(refreshImpl(cache, TenantEntryCacheRefreshReason::CACHE_MISS));
		}

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

		if (!tenantsEnabled(cache)) {
			// If tenants are disabled on the cluster avoid using the cache
			return Optional<TenantEntryCachePayload<T>>();
		}

		TraceEvent("TenantEntryCacheGetByNameRefresh").detail("TenantName", name);

		if (cache->refreshMode == TenantEntryCacheRefreshMode::WATCH) {
			// Entry not found. Do a point refresh
			wait(refreshCacheByName(name, cache, TenantEntryCacheRefreshReason::CACHE_MISS));
		} else {
			// Entry not found. Refresh cacheEntries by scanning underlying KeyRange.
			wait(refreshImpl(cache, TenantEntryCacheRefreshReason::CACHE_MISS));
		}

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

			int64_t tId = tenantId.present() ? tenantId.get() : TenantAPI::prefixToId(tenantPrefix.get());
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
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics),
	    refreshByWatchTrigger("TenantEntryCacheRefreshWatchTrigger", metrics) {
		TraceEvent("TenantEntryCacheCreatedDefaultFunc", uid);
	}

	TenantEntryCache(Database db, TenantEntryCacheRefreshMode mode)
	  : uid(deterministicRandom()->randomUniqueID()), db(db), createPayloadFunc(defaultCreatePayload),
	    refreshMode(mode), metrics("TenantEntryCacheMetrics", uid.toString()), hits("TenantEntryCacheHits", metrics),
	    misses("TenantEntryCacheMisses", metrics), refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics),
	    refreshByWatchTrigger("TenantEntryCacheRefreshWatchTrigger", metrics) {
		TraceEvent("TenantEntryCacheCreatedDefaultFunc", uid);
	}

	TenantEntryCache(Database db, TenantEntryCachePayloadFunc<T> fn)
	  : uid(deterministicRandom()->randomUniqueID()), db(db), createPayloadFunc(fn),
	    refreshMode(TenantEntryCacheRefreshMode::PERIODIC_TASK), metrics("TenantEntryCacheMetrics", uid.toString()),
	    hits("TenantEntryCacheHits", metrics), misses("TenantEntryCacheMisses", metrics),
	    refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics),
	    refreshByWatchTrigger("TenantEntryCacheRefreshWatchTrigger", metrics) {
		TraceEvent("TenantEntryCacheCreated", uid);
	}

	TenantEntryCache(Database db, UID id, TenantEntryCachePayloadFunc<T> fn)
	  : uid(id), db(db), createPayloadFunc(fn), refreshMode(TenantEntryCacheRefreshMode::PERIODIC_TASK),
	    metrics("TenantEntryCacheMetrics", uid.toString()), hits("TenantEntryCacheHits", metrics),
	    misses("TenantEntryCacheMisses", metrics), refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics),
	    refreshByWatchTrigger("TenantEntryCacheRefreshWatchTrigger", metrics) {
		TraceEvent("TenantEntryCacheCreated", uid);
	}

	TenantEntryCache(Database db, UID id, TenantEntryCachePayloadFunc<T> fn, TenantEntryCacheRefreshMode mode)
	  : uid(id), db(db), createPayloadFunc(fn), refreshMode(mode), metrics("TenantEntryCacheMetrics", uid.toString()),
	    hits("TenantEntryCacheHits", metrics), misses("TenantEntryCacheMisses", metrics),
	    refreshByCacheInit("TenantEntryCacheRefreshInit", metrics),
	    refreshByCacheMiss("TenantEntryCacheRefreshMiss", metrics),
	    numRefreshes("TenantEntryCacheNumRefreshes", metrics),
	    refreshByWatchTrigger("TenantEntryCacheRefreshWatchTrigger", metrics) {
		TraceEvent("TenantEntryCacheCreated", uid);
	}

	Future<Void> init(bool waitForInitalWatch = false) {
		TraceEvent("TenantEntryCacheInit", uid);

		Future<Void> f = refreshImpl(this, TenantEntryCacheRefreshReason::INIT);

		// Launch reaper task to periodically refresh cache by scanning database KeyRange
		TenantEntryCacheRefreshReason reason = TenantEntryCacheRefreshReason::PERIODIC_TASK;
		Future<Void> initalWatchFuture = Void();
		lastTenantIdRefresher = lastTenantIdWatch(this);
		if (refreshMode == TenantEntryCacheRefreshMode::PERIODIC_TASK) {
			refresher = recurringAsync([&, reason]() { return refresh(reason); },
			                           CLIENT_KNOBS->TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL, /* interval */
			                           true, /* absoluteIntervalDelay */
			                           CLIENT_KNOBS->TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL, /* intialDelay */
			                           TaskPriority::Worker);
		} else if (refreshMode == TenantEntryCacheRefreshMode::WATCH) {
			if (waitForInitalWatch) {
				initalWatchFuture = setInitialWatch.getFuture();
			}
			watchRefresher = refreshCacheUsingWatch(this, TenantEntryCacheRefreshReason::WATCH_TRIGGER);
		}

		Future<Void> setLastTenant = setLastTenantId(this);

		return f && initalWatchFuture && setLastTenant;
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
		const auto& [name, entry] = pair;
		TenantEntryCachePayload<T> payload = createPayloadFunc(name, entry);
		auto idItr = mapByTenantId.find(entry.id);
		auto nameItr = mapByTenantName.find(name);

		Optional<TenantName> existingName;
		Optional<int64_t> existingId;
		if (nameItr != mapByTenantName.end()) {
			existingId = nameItr->value.entry.id;
		}
		if (idItr != mapByTenantId.end()) {
			existingName = idItr->value.name;
		}
		if (existingId.present()) {
			mapByTenantId.erase(existingId.get());
		}
		if (existingName.present()) {
			mapByTenantName.erase(existingName.get());
		}

		mapByTenantId[entry.id] = payload;
		mapByTenantName[name] = payload;

		// TraceEvent("TenantEntryCachePut")
		//     .detail("TenantName", name)
		//     .detail("TenantNameExisting", existingName)
		//     .detail("TenantID", entry.id)
		//     .detail("TenantIDExisting", existingId)
		//     .detail("TenantPrefix", pair.second.prefix);

		CODE_PROBE(idItr == mapByTenantId.end() && nameItr == mapByTenantName.end(), "TenantCache new entry");
		CODE_PROBE(idItr != mapByTenantId.end() && nameItr == mapByTenantName.end(), "TenantCache entry name updated");
		CODE_PROBE(idItr == mapByTenantId.end() && nameItr != mapByTenantName.end(), "TenantCache entry id updated");
		CODE_PROBE(idItr != mapByTenantId.end() && nameItr != mapByTenantName.end(),
		           "TenantCache entry id and name updated");
	}

	Future<Optional<TenantEntryCachePayload<T>>> getById(int64_t tenantId) { return getByIdImpl(this, tenantId); }
	Future<Optional<TenantEntryCachePayload<T>>> getByPrefix(KeyRef prefix) {
		int64_t id = TenantAPI::prefixToId(prefix);
		return getByIdImpl(this, id);
	}
	Future<Optional<TenantEntryCachePayload<T>>> getByName(TenantName name) { return getByNameImpl(this, name); }

	// Counter access APIs
	Counter::Value numCacheRefreshes() const { return numRefreshes.getValue(); }
	Counter::Value numRefreshByMisses() const { return refreshByCacheMiss.getValue(); }
	Counter::Value numRefreshByInit() const { return refreshByCacheInit.getValue(); }
	Counter::Value numWatchRefreshes() const { return refreshByWatchTrigger.getValue(); }
};

#include "flow/unactorcompiler.h"
#endif // FDBCLIENT_TENANTENTRYCACHE_ACTOR_H
