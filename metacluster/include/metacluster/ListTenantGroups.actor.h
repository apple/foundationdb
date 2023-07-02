/*
 * ListTenantGroups.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_LISTTENANTGROUPS_ACTOR_G_H)
#define METACLUSTER_LISTTENANTGROUPS_ACTOR_G_H
#include "metacluster/ListTenantGroups.actor.g.h"
#elif !defined(METACLUSTER_LISTTENANTGROUPS_ACTOR_H)
#define METACLUSTER_LISTTENANTGROUPS_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>>>
listTenantGroupsTransaction(Transaction tr, TenantGroupName begin, TenantGroupName end, int limit) {
	CODE_PROBE(true, "List tenant groups");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	KeyBackedRangeResult<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>> results =
	    wait(metadata::management::tenantMetadata().tenantGroupMap.getRange(tr, begin, end, limit));

	return results.results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>>> listTenantGroups(Reference<DB> db,
                                                                                              TenantGroupName begin,
                                                                                              TenantGroupName end,
                                                                                              int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			std::vector<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>> tenantGroups =
			    wait(listTenantGroupsTransaction(tr, begin, end, limit));
			return tenantGroups;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif