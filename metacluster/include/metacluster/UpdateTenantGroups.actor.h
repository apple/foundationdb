/*
 * UpdateTenantGroups.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_UPDATETENANTGROUPS_ACTOR_G_H)
#define METACLUSTER_UPDATETENANTGROUPS_ACTOR_G_H
#include "metacluster/UpdateTenantGroups.actor.g.h"
#elif !defined(METACLUSTER_UPDATETENANTGROUPS_ACTOR_H)
#define METACLUSTER_UPDATETENANTGROUPS_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/TenantName.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"

#include "metacluster/ConfigureCluster.h"
#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

// This provides internal Metacluster APIs that are not meant to be consumed outside the metacluster project
namespace metacluster::internal {

FDB_BOOLEAN_PARAM(GroupAlreadyExists);

template <class Transaction>
void managementClusterAddTenantToGroup(Transaction tr,
                                       MetaclusterTenantMapEntry tenantEntry,
                                       DataClusterMetadata* clusterMetadata,
                                       GroupAlreadyExists groupAlreadyExists,
                                       IgnoreCapacityLimit ignoreCapacityLimit = IgnoreCapacityLimit::False,
                                       IsRestoring isRestoring = IsRestoring::False) {
	if (tenantEntry.tenantGroup.present()) {
		if (tenantEntry.tenantGroup.get().startsWith("\xff"_sr)) {
			CODE_PROBE(true, "Invalid tenant group name");
			throw invalid_tenant_group_name();
		}

		if (!groupAlreadyExists) {
			metadata::management::tenantMetadata().tenantGroupMap.set(
			    tr, tenantEntry.tenantGroup.get(), MetaclusterTenantGroupEntry(tenantEntry.assignedCluster));
			metadata::management::clusterTenantGroupIndex().insert(
			    tr, Tuple::makeTuple(tenantEntry.assignedCluster, tenantEntry.tenantGroup.get()));
		}
		metadata::management::tenantMetadata().tenantGroupTenantIndex.insert(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), tenantEntry.tenantName, tenantEntry.id));
	}

	if (!groupAlreadyExists && !isRestoring) {
		ASSERT(ignoreCapacityLimit || clusterMetadata->entry.hasCapacity());

		DataClusterEntry updatedEntry = clusterMetadata->entry;
		++updatedEntry.allocated.numTenantGroups;

		updateClusterMetadata(
		    tr, tenantEntry.assignedCluster, *clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		clusterMetadata->entry = updatedEntry;
	}
}

ACTOR template <class Transaction>
Future<Void> managementClusterRemoveTenantFromGroup(Transaction tr,
                                                    MetaclusterTenantMapEntry tenantEntry,
                                                    DataClusterMetadata* clusterMetadata) {
	state bool updateClusterCapacity = !tenantEntry.tenantGroup.present();
	if (tenantEntry.tenantGroup.present()) {
		metadata::management::tenantMetadata().tenantGroupTenantIndex.erase(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), tenantEntry.tenantName, tenantEntry.id));

		state KeyBackedSet<Tuple>::RangeResultType result =
		    wait(metadata::management::tenantMetadata().tenantGroupTenantIndex.getRange(
		        tr,
		        Tuple::makeTuple(tenantEntry.tenantGroup.get()),
		        Tuple::makeTuple(keyAfter(tenantEntry.tenantGroup.get())),
		        1));

		if (result.results.size() == 0) {
			metadata::management::clusterTenantGroupIndex().erase(
			    tr, Tuple::makeTuple(tenantEntry.assignedCluster, tenantEntry.tenantGroup.get()));

			metadata::management::tenantMetadata().tenantGroupMap.erase(tr, tenantEntry.tenantGroup.get());
			updateClusterCapacity = true;
		}
	}

	// Update the tenant group count information for the assigned cluster if this tenant group was erased so we
	// can use the freed capacity.
	if (updateClusterCapacity) {
		CODE_PROBE(true, "Remove tenant from group deleted group");
		DataClusterEntry updatedEntry = clusterMetadata->entry;
		--updatedEntry.allocated.numTenantGroups;
		updateClusterMetadata(
		    tr, tenantEntry.assignedCluster, *clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		clusterMetadata->entry = updatedEntry;
	}

	return Void();
}

} // namespace metacluster::internal

#include "flow/unactorcompiler.h"
#endif