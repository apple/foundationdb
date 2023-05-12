/*
 * MetaclusterInternal.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_METACLUSTERINTERNAL_ACTOR_G_H)
#define METACLUSTER_METACLUSTERINTERNAL_ACTOR_G_H
#include "metacluster/MetaclusterInternal.actor.g.h"
#elif !defined(METACLUSTER_METACLUSTERINTERNAL_ACTOR_H)
#define METACLUSTER_METACLUSTERINTERNAL_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/TenantName.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"

#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

// This provides internal Metacluster APIs that are not meant to be consumed outside the metacluster project
namespace metacluster::internal {

ACTOR template <class Transaction>
Future<Void> managementClusterCheckEmpty(Transaction tr) {
	state Future<KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>>> tenantsFuture =
	    TenantMetadata::tenantMap().getRange(tr, {}, {}, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type dbContentsFuture =
	    tr->getRange(normalKeys, 1);

	KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants = wait(tenantsFuture);
	if (!tenants.results.empty()) {
		CODE_PROBE(true, "Metacluster emptiness check has tenants", probe::decoration::rare);
		throw cluster_not_empty();
	}

	RangeResult dbContents = wait(safeThreadFutureToFuture(dbContentsFuture));
	if (!dbContents.empty()) {
		CODE_PROBE(true, "Metacluster emptiness check has data");
		throw cluster_not_empty();
	}

	return Void();
}

template <class Transaction>
void updateClusterCapacityIndex(Transaction tr,
                                ClusterName name,
                                DataClusterEntry const& previousEntry,
                                DataClusterEntry const& updatedEntry) {
	CODE_PROBE(updatedEntry.autoTenantAssignment == AutoTenantAssignment::DISABLED,
	           "Update tenant capacity with auto-assignment disabled");
	CODE_PROBE(updatedEntry.autoTenantAssignment == AutoTenantAssignment::ENABLED,
	           "Update tenant capacity with auto-assignment enabled");

	// Entries are put in the cluster capacity index ordered by how many items are already allocated to them
	if (previousEntry.hasCapacity() || updatedEntry.autoTenantAssignment == AutoTenantAssignment::DISABLED) {
		metadata::management::clusterCapacityIndex().erase(
		    tr, Tuple::makeTuple(previousEntry.allocated.numTenantGroups, name));
	}
	if (updatedEntry.hasCapacity() && updatedEntry.autoTenantAssignment == AutoTenantAssignment::ENABLED) {
		metadata::management::clusterCapacityIndex().insert(
		    tr, Tuple::makeTuple(updatedEntry.allocated.numTenantGroups, name));
	}
}

} // namespace metacluster::internal

#include "flow/unactorcompiler.h"
#endif