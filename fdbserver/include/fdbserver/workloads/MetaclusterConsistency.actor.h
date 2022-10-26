
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
#include "flow/BooleanParam.h"
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_G_H)
#define WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_G_H
#include "fdbserver/workloads/MetaclusterConsistency.actor.g.h"
#elif !defined(WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_H)
#define WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_H

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbserver/workloads/TenantConsistency.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DECLARE_BOOLEAN_PARAM(AllowPartialMetaclusterOperations);

template <class DB>
class MetaclusterConsistencyCheck {
private:
	Reference<DB> managementDb;
	AllowPartialMetaclusterOperations allowPartialMetaclusterOperations = AllowPartialMetaclusterOperations::True;

	struct ManagementClusterData {
		Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		std::map<ClusterName, DataClusterMetadata> dataClusters;
		KeyBackedRangeResult<Tuple> clusterCapacityTuples;
		KeyBackedRangeResult<std::pair<ClusterName, int64_t>> clusterTenantCounts;
		KeyBackedRangeResult<Tuple> clusterTenantTuples;
		KeyBackedRangeResult<Tuple> clusterTenantGroupTuples;

		std::map<TenantName, TenantMapEntry> tenantMap;
		KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroups;

		std::map<ClusterName, std::set<TenantName>> clusterTenantMap;
		std::map<ClusterName, std::set<TenantGroupName>> clusterTenantGroupMap;

		int64_t tenantCount;
		RangeResult systemTenantSubspaceKeys;
	};

	ManagementClusterData managementMetadata;

	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	ACTOR static Future<Void> loadManagementClusterMetadata(MetaclusterConsistencyCheck* self) {
		state Reference<typename DB::TransactionT> managementTr = self->managementDb->createTransaction();
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenantList;
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenantListReady;
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenantListOther;

		state std::vector<TenantState> readyFilter;
		state std::vector<TenantState> otherFilter;

		readyFilter.push_back(TenantState::READY);
		otherFilter.push_back(TenantState::REGISTERING);
		otherFilter.push_back(TenantState::REMOVING);
		otherFilter.push_back(TenantState::UPDATING_CONFIGURATION);
		otherFilter.push_back(TenantState::RENAMING_FROM);
		otherFilter.push_back(TenantState::RENAMING_TO);
		otherFilter.push_back(TenantState::ERROR);
		otherFilter.push_back(TenantState::INVALID);

		loop {
			try {
				managementTr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state typename transaction_future_type<typename DB::TransactionT, RangeResult>::type
				    systemTenantSubspaceKeysFuture = managementTr->getRange(prefixRange(TenantMetadata::subspace()), 1);

				wait(store(self->managementMetadata.metaclusterRegistration,
				           MetaclusterMetadata::metaclusterRegistration().get(managementTr)) &&
				     store(self->managementMetadata.dataClusters,
				           MetaclusterAPI::listClustersTransaction(
				               managementTr, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS + 1)) &&
				     store(self->managementMetadata.clusterCapacityTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterCapacityIndex.getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(self->managementMetadata.clusterTenantCounts,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantCount.getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(self->managementMetadata.clusterTenantTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantIndex.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(self->managementMetadata.clusterTenantGroupTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantGroupIndex.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(self->managementMetadata.tenantCount,
				           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantCount.getD(
				               managementTr, Snapshot::False, 0)) &&
				     store(tenantList,
				           MetaclusterAPI::listTenantsTransaction(
				               managementTr, ""_sr, "\xff\xff"_sr, metaclusterMaxTenants)) &&
				     store(tenantListReady,
				           MetaclusterAPI::listTenantsTransaction(
				               managementTr, ""_sr, "\xff\xff"_sr, metaclusterMaxTenants, readyFilter)) &&
				     store(tenantListOther,
				           MetaclusterAPI::listTenantsTransaction(
				               managementTr, ""_sr, "\xff\xff"_sr, metaclusterMaxTenants, otherFilter)) &&
				     store(self->managementMetadata.tenantGroups,
				           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantGroupMap.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(self->managementMetadata.systemTenantSubspaceKeys,
				           safeThreadFutureToFuture(systemTenantSubspaceKeysFuture)));

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(managementTr->onError(e)));
			}
		}

		ASSERT(tenantListReady.size() + tenantListOther.size() == tenantList.size());

		self->managementMetadata.tenantMap = std::map<TenantName, TenantMapEntry>(tenantList.begin(), tenantList.end());

		for (auto t : self->managementMetadata.clusterTenantTuples.results) {
			ASSERT(t.size() == 3);
			TenantName tenantName = t.getString(1);
			int64_t tenantId = t.getInt(2);
			ASSERT(tenantId == self->managementMetadata.tenantMap[tenantName].id);
			self->managementMetadata.clusterTenantMap[t.getString(0)].insert(tenantName);
		}

		for (auto t : self->managementMetadata.clusterTenantGroupTuples.results) {
			ASSERT(t.size() == 2);
			TenantGroupName tenantGroupName = t.getString(1);
			self->managementMetadata.clusterTenantGroupMap[t.getString(0)].insert(tenantGroupName);
		}

		return Void();
	}

	void validateManagementCluster() {
		ASSERT(managementMetadata.metaclusterRegistration.present());
		ASSERT(managementMetadata.metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_MANAGEMENT);
		ASSERT(managementMetadata.metaclusterRegistration.get().id ==
		           managementMetadata.metaclusterRegistration.get().metaclusterId &&
		       managementMetadata.metaclusterRegistration.get().name ==
		           managementMetadata.metaclusterRegistration.get().metaclusterName);
		ASSERT(managementMetadata.dataClusters.size() <= CLIENT_KNOBS->MAX_DATA_CLUSTERS);
		ASSERT(managementMetadata.tenantCount <= metaclusterMaxTenants);
		ASSERT(managementMetadata.clusterCapacityTuples.results.size() <= managementMetadata.dataClusters.size() &&
		       !managementMetadata.clusterCapacityTuples.more);
		ASSERT(managementMetadata.clusterTenantCounts.results.size() <= managementMetadata.dataClusters.size() &&
		       !managementMetadata.clusterTenantCounts.more);
		ASSERT(managementMetadata.clusterTenantTuples.results.size() == managementMetadata.tenantCount &&
		       !managementMetadata.clusterTenantTuples.more);
		ASSERT(managementMetadata.clusterTenantGroupTuples.results.size() <= managementMetadata.tenantCount &&
		       !managementMetadata.clusterTenantGroupTuples.more);
		ASSERT(managementMetadata.tenantMap.size() == managementMetadata.tenantCount);
		ASSERT(managementMetadata.tenantGroups.results.size() <= managementMetadata.tenantCount &&
		       !managementMetadata.tenantGroups.more);
		ASSERT(managementMetadata.clusterTenantGroupTuples.results.size() ==
		       managementMetadata.tenantGroups.results.size());

		// Parse the cluster capacity index. Check that no cluster is represented in the index more than once.
		std::map<ClusterName, int64_t> clusterAllocatedMap;
		for (auto t : managementMetadata.clusterCapacityTuples.results) {
			ASSERT(t.size() == 2);
			auto result = clusterAllocatedMap.emplace(t.getString(1), t.getInt(0));
			ASSERT(result.second);
		}

		// Validate various properties for each data cluster
		int numFoundInAllocatedMap = 0;
		int numFoundInTenantGroupMap = 0;
		for (auto [clusterName, clusterMetadata] : managementMetadata.dataClusters) {
			// If the cluster has capacity, it should be in the capacity index and have the correct count of
			// allocated tenants stored there
			auto allocatedItr = clusterAllocatedMap.find(clusterName);
			if (!clusterMetadata.entry.hasCapacity()) {
				ASSERT(allocatedItr == clusterAllocatedMap.end());
			} else {
				ASSERT(allocatedItr->second == clusterMetadata.entry.allocated.numTenantGroups);
				++numFoundInAllocatedMap;
			}

			// Check that the number of tenant groups in the cluster is smaller than the allocated number of tenant
			// groups.
			auto tenantGroupItr = managementMetadata.clusterTenantGroupMap.find(clusterName);
			if (tenantGroupItr != managementMetadata.clusterTenantGroupMap.end()) {
				ASSERT(tenantGroupItr->second.size() <= clusterMetadata.entry.allocated.numTenantGroups);
				++numFoundInTenantGroupMap;
			}
		}
		// Check that we exhausted the cluster capacity index and the cluster tenant group index
		ASSERT(numFoundInAllocatedMap == clusterAllocatedMap.size());
		ASSERT(numFoundInTenantGroupMap == managementMetadata.clusterTenantGroupMap.size());

		// Check that our cluster tenant counters match the number of tenants in the cluster index
		std::map<ClusterName, int64_t> countsMap(managementMetadata.clusterTenantCounts.results.begin(),
		                                         managementMetadata.clusterTenantCounts.results.end());
		for (auto [cluster, clusterTenants] : managementMetadata.clusterTenantMap) {
			auto itr = countsMap.find(cluster);
			ASSERT((clusterTenants.empty() && itr == countsMap.end()) || itr->second == clusterTenants.size());
		}

		// Iterate through all tenants and verify related metadata
		std::map<ClusterName, int> clusterAllocated;
		std::set<TenantGroupName> processedTenantGroups;
		for (auto [name, entry] : managementMetadata.tenantMap) {
			ASSERT(entry.assignedCluster.present());

			// Each tenant should be assigned to the same cluster where it is stored in the cluster tenant index
			auto clusterItr = managementMetadata.clusterTenantMap.find(entry.assignedCluster.get());
			ASSERT(clusterItr != managementMetadata.clusterTenantMap.end());
			ASSERT(clusterItr->second.count(name));

			if (entry.tenantGroup.present()) {
				// Count the number of tenant groups allocated in each cluster
				if (processedTenantGroups.insert(entry.tenantGroup.get()).second) {
					++clusterAllocated[entry.assignedCluster.get()];
				}
				// The tenant group should be stored in the same cluster where it is stored in the cluster tenant
				// group index
				auto clusterTenantGroupItr = managementMetadata.clusterTenantGroupMap.find(entry.assignedCluster.get());
				ASSERT(clusterTenantGroupItr != managementMetadata.clusterTenantMap.end());
				ASSERT(clusterTenantGroupItr->second.count(entry.tenantGroup.get()));
			} else {
				// Track the actual tenant group allocation per cluster (a tenant with no group counts against the
				// allocation)
				++clusterAllocated[entry.assignedCluster.get()];
			}
		}

		// The actual allocation for each cluster should match what is stored in the cluster metadata
		for (auto [name, allocated] : clusterAllocated) {
			auto itr = managementMetadata.dataClusters.find(name);
			ASSERT(itr != managementMetadata.dataClusters.end());
			ASSERT(allocated == itr->second.entry.allocated.numTenantGroups);
		}

		// Each tenant group in the tenant group map should be present in the cluster tenant group map
		// and have the correct cluster assigned to it.
		for (auto [name, entry] : managementMetadata.tenantGroups.results) {
			ASSERT(entry.assignedCluster.present());
			auto clusterItr = managementMetadata.clusterTenantGroupMap.find(entry.assignedCluster.get());
			ASSERT(clusterItr->second.count(name));
		}

		// We should not be storing any data in the `\xff` tenant subspace.
		ASSERT(managementMetadata.systemTenantSubspaceKeys.empty());
	}

	ACTOR static Future<Void> validateDataCluster(MetaclusterConsistencyCheck* self,
	                                              ClusterName clusterName,
	                                              DataClusterMetadata clusterMetadata) {
		state Reference<IDatabase> dataDb = wait(MetaclusterAPI::openDatabase(clusterMetadata.connectionString));
		state Reference<ITransaction> dataTr = dataDb->createTransaction();

		state Optional<MetaclusterRegistrationEntry> dataClusterRegistration;
		state std::vector<std::pair<TenantName, TenantMapEntry>> dataClusterTenantList;
		state KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> dataClusterTenantGroups;

		state TenantConsistencyCheck<IDatabase> tenantConsistencyCheck(dataDb);
		wait(tenantConsistencyCheck.run());

		loop {
			try {
				dataTr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(dataClusterRegistration, MetaclusterMetadata::metaclusterRegistration().get(dataTr)) &&
				     store(dataClusterTenantList,
				           TenantAPI::listTenantsTransaction(
				               dataTr, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)) &&
				     store(dataClusterTenantGroups,
				           TenantMetadata::tenantGroupMap().getRange(
				               dataTr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(dataTr->onError(e)));
			}
		}

		state std::map<TenantName, TenantMapEntry> dataClusterTenantMap(dataClusterTenantList.begin(),
		                                                                dataClusterTenantList.end());

		ASSERT(dataClusterRegistration.present());
		ASSERT(dataClusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);
		ASSERT(dataClusterRegistration.get().matches(self->managementMetadata.metaclusterRegistration.get()));
		ASSERT(dataClusterRegistration.get().name == clusterName);
		ASSERT(dataClusterRegistration.get().id == clusterMetadata.entry.id);

		auto& expectedTenants = self->managementMetadata.clusterTenantMap[clusterName];

		if (!self->allowPartialMetaclusterOperations) {
			ASSERT(dataClusterTenantMap.size() == expectedTenants.size());
		} else {
			ASSERT(dataClusterTenantMap.size() <= expectedTenants.size());
			for (auto tenantName : expectedTenants) {
				if (!dataClusterTenantMap.count(tenantName)) {
					TenantMapEntry const& metaclusterEntry = self->managementMetadata.tenantMap[tenantName];
					if (metaclusterEntry.renamePair.present() &&
					    (metaclusterEntry.tenantState == TenantState::RENAMING_FROM ||
					     metaclusterEntry.tenantState == TenantState::RENAMING_TO)) {
						ASSERT(dataClusterTenantMap.count(metaclusterEntry.renamePair.get()));
					} else {
						ASSERT(metaclusterEntry.tenantState == TenantState::REGISTERING ||
						       metaclusterEntry.tenantState == TenantState::REMOVING);
					}
				}
			}
		}

		for (auto [name, entry] : dataClusterTenantMap) {
			ASSERT(expectedTenants.count(name));
			TenantMapEntry const& metaclusterEntry = self->managementMetadata.tenantMap[name];
			ASSERT(!entry.assignedCluster.present());
			ASSERT(entry.id == metaclusterEntry.id);
			ASSERT(entry.encrypted == metaclusterEntry.encrypted);

			ASSERT(entry.tenantState == TenantState::READY);
			ASSERT(self->allowPartialMetaclusterOperations || metaclusterEntry.tenantState == TenantState::READY);
			if (metaclusterEntry.tenantState != TenantState::UPDATING_CONFIGURATION &&
			    metaclusterEntry.tenantState != TenantState::REMOVING) {
				ASSERT(entry.configurationSequenceNum == metaclusterEntry.configurationSequenceNum);
			} else {
				ASSERT(entry.configurationSequenceNum <= metaclusterEntry.configurationSequenceNum);
			}

			if (entry.configurationSequenceNum == metaclusterEntry.configurationSequenceNum) {
				ASSERT(entry.tenantGroup == metaclusterEntry.tenantGroup);
			}
		}

		auto& expectedTenantGroups = self->managementMetadata.clusterTenantGroupMap[clusterName];
		ASSERT(dataClusterTenantGroups.results.size() == expectedTenantGroups.size());
		for (auto [name, entry] : dataClusterTenantGroups.results) {
			ASSERT(expectedTenantGroups.count(name));
			ASSERT(!entry.assignedCluster.present());
		}

		return Void();
	}

	ACTOR static Future<Void> run(MetaclusterConsistencyCheck* self) {
		state TenantConsistencyCheck<DB> managementTenantConsistencyCheck(self->managementDb);
		wait(managementTenantConsistencyCheck.run());
		wait(loadManagementClusterMetadata(self));
		self->validateManagementCluster();

		state std::vector<Future<Void>> dataClusterChecks;
		state std::map<ClusterName, DataClusterMetadata>::iterator dataClusterItr;
		for (auto [clusterName, clusterMetadata] : self->managementMetadata.dataClusters) {
			dataClusterChecks.push_back(validateDataCluster(self, clusterName, clusterMetadata));
		}
		wait(waitForAll(dataClusterChecks));

		return Void();
	}

public:
	MetaclusterConsistencyCheck() {}
	MetaclusterConsistencyCheck(Reference<DB> managementDb,
	                            AllowPartialMetaclusterOperations allowPartialMetaclusterOperations)
	  : managementDb(managementDb), allowPartialMetaclusterOperations(allowPartialMetaclusterOperations) {}

	Future<Void> run() { return run(this); }
};

#include "flow/unactorcompiler.h"

#endif
