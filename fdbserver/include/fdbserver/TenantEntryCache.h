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
#ifndef FDBSERVER_TENANTENTRY_H
#define FDBSERVER_TENANTENTRY_H

#pragma once

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Tenant.h"

#include "flow/IndexedSet.h"

#include <functional>
#include <unordered_map>

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

template <class T>
class TenantEntryCache : public NonCopyable {
public:
	template <class>
	friend class TenantEntryCacheImpl;

	TenantEntryCache(Database db, TenantEntryCachePayloadFunc<T> fn);

	Future<Void> init();
	Database getDatabase() const { return db; }
	UID id() const { return uid; }

	TenantEntryCacheGen getCurGen() const { return curGen; }
	void put(const TenantNameEntryPair& pair);
	Future<TenantEntryCachePayload<T>> getById(int64_t tenantId);
	Future<TenantEntryCachePayload<T>> getByPrefix(KeyRef prefix);

private:
	UID uid;
	Database db;
	TenantEntryCacheGen curGen;
	TenantEntryCachePayloadFunc<T> createPayloadFunc;

	AsyncTrigger sweepTrigger;
	Future<Void> sweeper;
	Future<Void> refresher;
	Map<int64_t, TenantEntryCachePayload<T>> mapByTenantId;
	Map<KeyRef, TenantEntryCachePayload<T>> mapByTenantPrefix;

	// TODO: add hit/miss/refresh counters

	void incGen() { curGen++; }

	Future<Void> waitForSweepTrigger() { return sweepTrigger.onTrigger(); }
	void triggerSweeper() { sweepTrigger.trigger(); }
	void sweep();
	void refresh(TenantEntryCacheRefreshReason reason);

	TenantEntryCachePayload<T> lookupById(int64_t tenantId);
	TenantEntryCachePayload<T> lookupByPrefix(const KeyRef& prefix);
};

#endif