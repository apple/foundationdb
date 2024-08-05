/*
 * ListTenants.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_LISTTENANTS_ACTOR_G_H)
#define METACLUSTER_LISTTENANTS_ACTOR_G_H
#include "metacluster/ListTenants.actor.g.h"
#elif !defined(METACLUSTER_LISTTENANTS_ACTOR_H)
#define METACLUSTER_LISTTENANTS_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

template <class Transaction>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenantsTransaction(Transaction tr,
                                                                           TenantName begin,
                                                                           TenantName end,
                                                                           int limit,
                                                                           int offset = 0) {
	CODE_PROBE(true, "List tenants");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	auto future = metadata::management::tenantMetadata().tenantNameIndex.getRange(tr, begin, end, limit + offset);
	return fmap(
	    [offset](auto f) {
		    std::vector<std::pair<TenantName, int64_t>>& results = f.results;
		    results.erase(results.begin(), results.begin() + offset);
		    return results;
	    },
	    future);
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenantGroupTenantsTransaction(Transaction tr,
                                                                                      TenantGroupName tenantGroup,
                                                                                      TenantName begin,
                                                                                      TenantName end,
                                                                                      int limit) {
	CODE_PROBE(true, "List tenant group tenants");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	state KeyBackedSet<Tuple>::RangeResultType result =
	    wait(metadata::management::tenantMetadata().tenantGroupTenantIndex.getRange(
	        tr, Tuple::makeTuple(tenantGroup, begin), Tuple::makeTuple(tenantGroup, end), limit));
	std::vector<std::pair<TenantName, int64_t>> returnResult;
	if (!result.results.size()) {
		return returnResult;
	}
	for (auto const& tupleEntry : result.results) {
		returnResult.push_back(std::make_pair(tupleEntry.getString(1), tupleEntry.getInt(2)));
	}
	return returnResult;
}

template <class DB>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenants(Reference<DB> db,
                                                                TenantName begin,
                                                                TenantName end,
                                                                int limit,
                                                                int offset = 0) {
	return runTransaction(db, [=](Reference<typename DB::TransactionT> tr) {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		return listTenantsTransaction(tr, begin, end, limit, offset);
	});
}

// Scan the tenant index to get a list of tenant IDs, and then lookup the metadata for each ID individually
ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>>> listTenantMetadataTransaction(
    Transaction tr,
    std::vector<std::pair<TenantName, int64_t>> tenantIds) {

	CODE_PROBE(true, "List tenant metadata");
	state int idIdx = 0;
	state std::vector<Future<Optional<MetaclusterTenantMapEntry>>> futures;
	for (; idIdx < tenantIds.size(); ++idIdx) {
		futures.push_back(tryGetTenantTransaction(tr, tenantIds[idIdx].second));
	}
	wait(waitForAll(futures));

	std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> results;
	results.reserve(futures.size());
	for (int i = 0; i < futures.size(); ++i) {
		const MetaclusterTenantMapEntry& entry = futures[i].get().get();

		// Tenants being renamed show up in tenantIds twice, once under each name. The destination name will be
		// different from the tenant entry and is filtered from the list
		if (entry.tenantName == tenantIds[i].first) {
			results.emplace_back(entry.tenantName, entry);
		}
	}

	return results;
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>>> listTenantMetadataTransaction(
    Transaction tr,
    TenantNameRef begin,
    TenantNameRef end,
    int limit,
    Optional<TenantGroupName> tenantGroup = Optional<TenantGroupName>()) {
	state std::vector<std::pair<TenantName, int64_t>> matchingTenants;
	if (!tenantGroup.present()) {
		wait(store(matchingTenants, listTenantsTransaction(tr, begin, end, limit)));
	} else {
		wait(store(matchingTenants, listTenantGroupTenantsTransaction(tr, tenantGroup.get(), begin, end, limit)));
	}
	std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> results =
	    wait(listTenantMetadataTransaction(tr, matchingTenants));
	return results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>>> listTenantMetadata(
    Reference<DB> db,
    TenantName begin,
    TenantName end,
    int limit,
    int offset = 0,
    std::vector<TenantState> filters = std::vector<TenantState>(),
    Optional<TenantGroupName> tenantGroup = Optional<TenantGroupName>()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> results;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			if (filters.empty()) {
				state std::vector<std::pair<TenantName, int64_t>> ids;
				if (!tenantGroup.present()) {
					wait(store(ids, listTenantsTransaction(tr, begin, end, limit)));
				} else {
					wait(store(ids, listTenantGroupTenantsTransaction(tr, tenantGroup.get(), begin, end, limit)));
				}
				wait(store(results, listTenantMetadataTransaction(tr, ids)));
				return results;
			}

			// read in batch
			state int count = 0;
			loop {
				std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> tenantBatch =
				    wait(listTenantMetadataTransaction(tr, begin, end, std::max(limit + offset, 1000), tenantGroup));

				if (tenantBatch.empty()) {
					return results;
				}

				for (auto const& [name, entry] : tenantBatch) {
					if (std::count(filters.begin(), filters.end(), entry.tenantState)) {
						++count;
						if (count > offset) {
							results.emplace_back(name, entry);
							if (count - offset == limit) {
								ASSERT(count - offset == results.size());
								return results;
							}
						}
					}
				}

				CODE_PROBE(true, "List tenant multiple batches");
				begin = keyAfter(tenantBatch.back().first);
			}
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif