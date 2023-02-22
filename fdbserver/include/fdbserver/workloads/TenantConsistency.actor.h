
/*
 * TenantConsistency.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/KeyBackedTypes.h"
#include "flow/BooleanParam.h"
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_TENANT_CONSISTENCY_ACTOR_G_H)
#define WORKLOADS_TENANT_CONSISTENCY_ACTOR_G_H
#include "fdbserver/workloads/TenantConsistency.actor.g.h"
#elif !defined(WORKLOADS_TENANT_CONSISTENCY_ACTOR_H)
#define WORKLOADS_TENANT_CONSISTENCY_ACTOR_H

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/workloads/TenantData.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

template <class DB, class TenantTypes>
class TenantConsistencyCheck {
private:
	TenantData<DB, typename TenantTypes::TenantMapEntryT> tenantData;

	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	void validateTenantMetadataImpl() {
		ASSERT_EQ(tenantData.tenantMap.size(), tenantData.tenantCount);

		std::set<int64_t> tenantsInTenantGroupIndex;
		for (auto const& [groupName, tenants] : tenantData.tenantGroupIndex) {
			for (auto const& tenant : tenants) {
				tenantsInTenantGroupIndex.insert(tenant);
			}
		}

		for (auto [tenantId, tenantMapEntry] : tenantData.tenantMap) {
			ASSERT_EQ(tenantId, tenantMapEntry.id);
			ASSERT_EQ(tenantData.tenantNameIndex[tenantMapEntry.tenantName], tenantId);

			// Data clusters do not keep their last tenant ID up to date while part of a metacluster
			if (tenantData.clusterType != ClusterType::METACLUSTER_DATA) {
				if (TenantAPI::getTenantIdPrefix(tenantId) == TenantAPI::getTenantIdPrefix(tenantData.lastTenantId)) {
					ASSERT_LE(tenantId, tenantData.lastTenantId);
				}
			}

			if (tenantMapEntry.tenantGroup.present()) {
				auto tenantGroupMapItr = tenantData.tenantGroupMap.find(tenantMapEntry.tenantGroup.get());
				ASSERT(tenantGroupMapItr != tenantData.tenantGroupMap.end());
				ASSERT(tenantData.tenantGroupIndex[tenantMapEntry.tenantGroup.get()].count(tenantId));
			} else {
				ASSERT(!tenantsInTenantGroupIndex.count(tenantId));
			}
		}
	}

	// Specialization for TenantMapEntry, used on data and standalone clusters
	void validateTenantMetadata(TenantData<DB, TenantMapEntry> tenantData) {
		ASSERT(tenantData.clusterType == ClusterType::METACLUSTER_DATA ||
		       tenantData.clusterType == ClusterType::STANDALONE);
		ASSERT_LE(tenantData.tenantMap.size(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		ASSERT_EQ(tenantData.tenantNameIndex.size(), tenantData.tenantCount);

		validateTenantMetadataImpl();
	}

	// Specialization for MetaclusterTenantMapEntry, used on management clusters
	void validateTenantMetadata(TenantData<DB, MetaclusterTenantMapEntry> tenantData) {
		ASSERT(tenantData.clusterType == ClusterType::METACLUSTER_MANAGEMENT);
		ASSERT_LE(tenantData.tenantMap.size(), metaclusterMaxTenants);

		// Check metacluster specific properties
		int renameCount = 0;
		for (auto [tenantId, tenantMapEntry] : tenantData.tenantMap) {
			if (tenantMapEntry.tenantGroup.present()) {
				auto tenantGroupMapItr = tenantData.tenantGroupMap.find(tenantMapEntry.tenantGroup.get());
				ASSERT(tenantMapEntry.assignedCluster == tenantGroupMapItr->second.assignedCluster.get());
			}
			if (tenantMapEntry.renameDestination.present()) {
				ASSERT(tenantMapEntry.tenantState == MetaclusterAPI::TenantState::RENAMING ||
				       tenantMapEntry.tenantState == MetaclusterAPI::TenantState::REMOVING);

				auto nameIndexItr = tenantData.tenantNameIndex.find(tenantMapEntry.renameDestination.get());
				ASSERT(nameIndexItr != tenantData.tenantNameIndex.end());
				ASSERT_EQ(nameIndexItr->second, tenantMapEntry.id);
				++renameCount;
			} else {
				ASSERT_NE(tenantMapEntry.tenantState, MetaclusterAPI::TenantState::RENAMING);
			}

			// An error string should be set if and only if the tenant state is an error
			ASSERT((tenantMapEntry.tenantState == MetaclusterAPI::TenantState::ERROR) != tenantMapEntry.error.empty());
		}

		ASSERT_EQ(tenantData.tenantCount + renameCount, tenantData.tenantNameIndex.size());

		validateTenantMetadataImpl();
	}

	// Check that the tenant tombstones are properly cleaned up and only present on a metacluster data cluster
	void checkTenantTombstones() {
		if (tenantData.clusterType == ClusterType::METACLUSTER_DATA) {
			if (!tenantData.tombstoneCleanupData.present()) {
				ASSERT(tenantData.tenantTombstones.empty());
			}

			if (!tenantData.tenantTombstones.empty()) {
				ASSERT(*tenantData.tenantTombstones.begin() >
				       tenantData.tombstoneCleanupData.get().tombstonesErasedThrough);
			}
		} else {
			ASSERT(tenantData.tenantTombstones.empty() && !tenantData.tombstoneCleanupData.present());
		}
	}

	ACTOR static Future<Void> run(TenantConsistencyCheck* self) {
		wait(self->tenantData.load());
		self->validateTenantMetadata(self->tenantData);
		self->checkTenantTombstones();

		return Void();
	}

public:
	TenantConsistencyCheck() {}
	TenantConsistencyCheck(Reference<DB> db, TenantMetadataSpecification<TenantTypes>* tenantMetadata)
	  : tenantData(db, tenantMetadata) {}

	Future<Void> run() { return run(this); }
};

#include "flow/unactorcompiler.h"

#endif
