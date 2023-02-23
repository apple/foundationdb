
/*
 * MetaclusterConsistency.actor.h
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
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "flow/BooleanParam.h"
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_G_H)
#define WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_G_H
#include "fdbserver/workloads/MetaclusterConsistency.actor.g.h"
#elif !defined(WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_H)
#define WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_H

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbserver/workloads/MetaclusterData.actor.h"
#include "fdbserver/workloads/TenantConsistency.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DECLARE_BOOLEAN_PARAM(AllowPartialMetaclusterOperations);

template <class DB>
class MetaclusterConsistencyCheck {
private:
	Reference<DB> managementDb;
	AllowPartialMetaclusterOperations allowPartialMetaclusterOperations = AllowPartialMetaclusterOperations::True;
	MetaclusterData<DB> metaclusterData;

	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	ACTOR static Future<Void> checkManagementSystemKeys(MetaclusterConsistencyCheck* self) {
		state Reference<typename DB::TransactionT> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state typename transaction_future_type<typename DB::TransactionT, RangeResult>::type
				    systemTenantSubspaceKeysFuture = tr->getRange(prefixRange(TenantMetadata::subspace()), 2);
				RangeResult systemTenantSubspaceKeys = wait(safeThreadFutureToFuture(systemTenantSubspaceKeysFuture));

				// The only key in the `\xff` tenant subspace should be the tenant id prefix
				ASSERT(systemTenantSubspaceKeys.size() == 1);
				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	void validateManagementCluster() {
		auto const& data = metaclusterData.managementMetadata;

		ASSERT(data.metaclusterRegistration.present());
		ASSERT_EQ(data.metaclusterRegistration.get().clusterType, ClusterType::METACLUSTER_MANAGEMENT);
		ASSERT(data.metaclusterRegistration.get().id == data.metaclusterRegistration.get().metaclusterId &&
		       data.metaclusterRegistration.get().name == data.metaclusterRegistration.get().metaclusterName);
		ASSERT_LE(data.dataClusters.size(), CLIENT_KNOBS->MAX_DATA_CLUSTERS);
		ASSERT_LE(data.tenantData.tenantCount, metaclusterMaxTenants);
		ASSERT(data.clusterTenantCounts.results.size() <= data.dataClusters.size() && !data.clusterTenantCounts.more);
		ASSERT_EQ(data.tenantData.tenantMap.size(), data.tenantData.tenantCount);
		ASSERT_LE(data.tenantData.tenantGroupMap.size(), data.tenantData.tenantCount);
		ASSERT(data.tenantIdPrefix.present());
		ASSERT_LE(data.clusterAllocatedMap.size(), data.dataClusters.size());

		if (data.tenantData.lastTenantId != -1) {
			ASSERT(TenantAPI::getTenantIdPrefix(data.tenantData.lastTenantId) == data.tenantIdPrefix.get());
		}

		// Validate various properties for each data cluster
		int numFoundInAllocatedMap = 0;
		int numFoundInTenantGroupMap = 0;
		for (auto const& [clusterName, clusterMetadata] : data.dataClusters) {
			// If the cluster has capacity, it should be in the capacity index and have the correct count of
			// allocated tenants stored there
			auto allocatedItr = data.clusterAllocatedMap.find(clusterName);
			if (!clusterMetadata.entry.hasCapacity()) {
				ASSERT(allocatedItr == data.clusterAllocatedMap.end());
			} else if (allocatedItr != data.clusterAllocatedMap.end()) {
				ASSERT_EQ(allocatedItr->second, clusterMetadata.entry.allocated.numTenantGroups);
				++numFoundInAllocatedMap;
			} else {
				ASSERT_NE(clusterMetadata.entry.clusterState, DataClusterState::READY);
			}

			// Check that the number of tenant groups in the cluster is smaller than the allocated number of tenant
			// groups.
			auto tenantGroupItr = data.clusterTenantGroupMap.find(clusterName);
			if (tenantGroupItr != data.clusterTenantGroupMap.end()) {
				ASSERT_LE(tenantGroupItr->second.size(), clusterMetadata.entry.allocated.numTenantGroups);
				++numFoundInTenantGroupMap;
			}
		}
		// Check that we exhausted the cluster capacity index and the cluster tenant group index
		ASSERT_EQ(numFoundInAllocatedMap, data.clusterAllocatedMap.size());
		ASSERT_EQ(numFoundInTenantGroupMap, data.clusterTenantGroupMap.size());

		// Check that our cluster tenant counters match the number of tenants in the cluster index
		std::map<ClusterName, int64_t> countsMap(data.clusterTenantCounts.results.begin(),
		                                         data.clusterTenantCounts.results.end());
		int64_t totalTenants = 0;
		for (auto const& [cluster, clusterTenants] : data.clusterTenantMap) {
			auto itr = countsMap.find(cluster);
			ASSERT((clusterTenants.empty() && itr == countsMap.end()) || itr->second == clusterTenants.size());
			totalTenants += clusterTenants.size();
		}
		ASSERT_EQ(totalTenants, data.tenantData.tenantCount);

		// Iterate through all tenants and verify related metadata
		std::map<ClusterName, int> clusterAllocated;
		std::set<TenantGroupName> processedTenantGroups;
		for (auto const& [tenantId, entry] : data.tenantData.tenantMap) {
			// Each tenant should be assigned to the same cluster where it is stored in the cluster tenant index
			auto clusterItr = data.clusterTenantMap.find(entry.assignedCluster);
			ASSERT(clusterItr != data.clusterTenantMap.end());
			ASSERT(clusterItr->second.count(tenantId));

			if (entry.tenantGroup.present()) {
				// Count the number of tenant groups allocated in each cluster
				if (processedTenantGroups.insert(entry.tenantGroup.get()).second) {
					++clusterAllocated[entry.assignedCluster];
				}
				// The tenant group should be stored in the same cluster where it is stored in the cluster tenant
				// group index
				auto clusterTenantGroupItr = data.clusterTenantGroupMap.find(entry.assignedCluster);
				ASSERT(clusterTenantGroupItr != data.clusterTenantGroupMap.end());
				ASSERT(clusterTenantGroupItr->second.count(entry.tenantGroup.get()));
			} else {
				// Track the actual tenant group allocation per cluster (a tenant with no group counts against the
				// allocation)
				++clusterAllocated[entry.assignedCluster];
			}
		}

		// The actual allocation for each cluster should match what is stored in the cluster metadata
		for (auto const& [name, allocated] : clusterAllocated) {
			auto itr = data.dataClusters.find(name);
			ASSERT(itr != data.dataClusters.end());
			ASSERT_EQ(allocated, itr->second.entry.allocated.numTenantGroups);
		}

		// Each tenant group in the tenant group map should be present in the cluster tenant group map
		// and have the correct cluster assigned to it.
		for (auto const& [name, entry] : data.tenantData.tenantGroupMap) {
			ASSERT(entry.assignedCluster.present());
			auto clusterItr = data.clusterTenantGroupMap.find(entry.assignedCluster.get());
			ASSERT(clusterItr->second.count(name));
		}

		// The cluster tenant group map should have the same number of tenant groups as the full tenant group map
		int totalTenantGroups = 0;
		for (auto const& [_, groups] : data.clusterTenantGroupMap) {
			totalTenantGroups += groups.size();
		}
		ASSERT_EQ(totalTenantGroups, data.tenantData.tenantGroupMap.size());
	}

	ACTOR static Future<Void> validateDataCluster(MetaclusterConsistencyCheck* self,
	                                              ClusterName clusterName,
	                                              DataClusterMetadata clusterMetadata) {
		state Reference<IDatabase> dataDb = wait(MetaclusterAPI::openDatabase(clusterMetadata.connectionString));
		state TenantConsistencyCheck<IDatabase, TenantMapEntry> tenantConsistencyCheck(dataDb,
		                                                                               &TenantMetadata::instance());
		wait(tenantConsistencyCheck.run());

		auto dataClusterItr = self->metaclusterData.dataClusterMetadata.find(clusterName);
		ASSERT(dataClusterItr != self->metaclusterData.dataClusterMetadata.end());
		auto const& data = dataClusterItr->second;
		auto const& managementData = self->metaclusterData.managementMetadata;

		ASSERT(data.metaclusterRegistration.present());
		ASSERT_EQ(data.metaclusterRegistration.get().clusterType, ClusterType::METACLUSTER_DATA);
		ASSERT(data.metaclusterRegistration.get().matches(managementData.metaclusterRegistration.get()));
		ASSERT(data.metaclusterRegistration.get().name == clusterName);
		ASSERT(data.metaclusterRegistration.get().id == clusterMetadata.entry.id);

		std::set<int64_t> expectedTenants;
		auto clusterTenantMapItr = managementData.clusterTenantMap.find(clusterName);
		if (clusterTenantMapItr != managementData.clusterTenantMap.end()) {
			expectedTenants = clusterTenantMapItr->second;
		}

		std::set<TenantGroupName> tenantGroupsWithCompletedTenants;
		if (!self->allowPartialMetaclusterOperations) {
			ASSERT_EQ(data.tenantData.tenantMap.size(), expectedTenants.size());
		} else {
			ASSERT_LE(data.tenantData.tenantMap.size(), expectedTenants.size());
			for (auto const& tenantId : expectedTenants) {
				auto tenantMapItr = managementData.tenantData.tenantMap.find(tenantId);
				ASSERT(tenantMapItr != managementData.tenantData.tenantMap.end());
				MetaclusterTenantMapEntry const& metaclusterEntry = tenantMapItr->second;
				if (!data.tenantData.tenantMap.count(tenantId)) {
					ASSERT(metaclusterEntry.tenantState == MetaclusterAPI::TenantState::REGISTERING ||
					       metaclusterEntry.tenantState == MetaclusterAPI::TenantState::REMOVING ||
					       metaclusterEntry.tenantState == MetaclusterAPI::TenantState::ERROR);
				} else if (metaclusterEntry.tenantGroup.present()) {
					tenantGroupsWithCompletedTenants.insert(metaclusterEntry.tenantGroup.get());
				}
			}
		}

		for (auto const& [tenantId, entry] : data.tenantData.tenantMap) {
			ASSERT(expectedTenants.count(tenantId));
			auto tenantMapItr = managementData.tenantData.tenantMap.find(tenantId);
			ASSERT(tenantMapItr != managementData.tenantData.tenantMap.end());
			MetaclusterTenantMapEntry const& metaclusterEntry = tenantMapItr->second;
			ASSERT_EQ(entry.id, metaclusterEntry.id);
			ASSERT(entry.tenantName == metaclusterEntry.tenantName);

			if (!self->allowPartialMetaclusterOperations) {
				ASSERT_EQ(metaclusterEntry.tenantState, MetaclusterAPI::TenantState::READY);
			}
			if (metaclusterEntry.tenantState != MetaclusterAPI::TenantState::UPDATING_CONFIGURATION &&
			    metaclusterEntry.tenantState != MetaclusterAPI::TenantState::REMOVING) {
				ASSERT_EQ(entry.configurationSequenceNum, metaclusterEntry.configurationSequenceNum);
			} else {
				ASSERT_LE(entry.configurationSequenceNum, metaclusterEntry.configurationSequenceNum);
			}

			if (entry.configurationSequenceNum == metaclusterEntry.configurationSequenceNum) {
				ASSERT(entry.tenantGroup == metaclusterEntry.tenantGroup);
			}
		}

		std::set<TenantGroupName> expectedTenantGroups;
		auto clusterTenantGroupItr = managementData.clusterTenantGroupMap.find(clusterName);
		if (clusterTenantGroupItr != managementData.clusterTenantGroupMap.end()) {
			expectedTenantGroups = clusterTenantGroupItr->second;
		}
		if (!self->allowPartialMetaclusterOperations) {
			ASSERT_EQ(data.tenantData.tenantGroupMap.size(), expectedTenantGroups.size());
		} else {
			ASSERT_LE(data.tenantData.tenantGroupMap.size(), expectedTenantGroups.size());
			for (auto const& name : expectedTenantGroups) {
				if (!data.tenantData.tenantGroupMap.count(name)) {
					auto itr = tenantGroupsWithCompletedTenants.find(name);
					ASSERT(itr == tenantGroupsWithCompletedTenants.end());
				}
			}
		}
		for (auto const& [name, entry] : data.tenantData.tenantGroupMap) {
			ASSERT(expectedTenantGroups.count(name));
			ASSERT(!entry.assignedCluster.present());
			expectedTenantGroups.erase(name);
		}

		for (auto const& name : expectedTenantGroups) {
			ASSERT(tenantGroupsWithCompletedTenants.count(name) == 0);
		}

		return Void();
	}

	ACTOR static Future<Void> run(MetaclusterConsistencyCheck* self) {
		state TenantConsistencyCheck<DB, MetaclusterTenantMapEntry> managementTenantConsistencyCheck(
		    self->managementDb, &MetaclusterAPI::ManagementClusterMetadata::tenantMetadata());

		wait(managementTenantConsistencyCheck.run() && self->metaclusterData.load() && checkManagementSystemKeys(self));

		self->validateManagementCluster();

		state std::vector<Future<Void>> dataClusterChecks;
		state std::map<ClusterName, DataClusterMetadata>::iterator dataClusterItr;
		for (auto const& [clusterName, clusterMetadata] : self->metaclusterData.managementMetadata.dataClusters) {
			dataClusterChecks.push_back(validateDataCluster(self, clusterName, clusterMetadata));
		}
		wait(waitForAll(dataClusterChecks));

		return Void();
	}

public:
	MetaclusterConsistencyCheck() {}
	MetaclusterConsistencyCheck(Reference<DB> managementDb,
	                            AllowPartialMetaclusterOperations allowPartialMetaclusterOperations)
	  : managementDb(managementDb), metaclusterData(managementDb),
	    allowPartialMetaclusterOperations(allowPartialMetaclusterOperations) {}

	Future<Void> run() { return run(this); }
};

#include "flow/unactorcompiler.h"

#endif
