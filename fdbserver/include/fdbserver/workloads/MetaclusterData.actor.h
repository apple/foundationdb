
/*
 * MetaclusterData.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "flow/BooleanParam.h"
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_METACLUSTER_DATA_ACTOR_G_H)
#define WORKLOADS_METACLUSTER_DATA_ACTOR_G_H
#include "fdbserver/workloads/MetaclusterData.actor.g.h"
#elif !defined(WORKLOADS_METACLUSTER_DATA_ACTOR_H)
#define WORKLOADS_METACLUSTER_DATA_ACTOR_H

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbserver/workloads/TenantData.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

template <class DB>
class MetaclusterData {
public:
	struct ManagementClusterData {
		Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		std::map<ClusterName, DataClusterMetadata> dataClusters;
		KeyBackedRangeResult<std::pair<ClusterName, int64_t>> clusterTenantCounts;
		KeyBackedRangeResult<UID> registrationTombstones;
		KeyBackedRangeResult<std::pair<ClusterName, UID>> activeRestoreIds;

		std::map<ClusterName, int64_t> clusterCapacityMap;
		std::map<ClusterName, std::set<int64_t>> clusterTenantMap;
		std::map<ClusterName, std::set<TenantGroupName>> clusterTenantGroupMap;

		Optional<int64_t> tenantIdPrefix;
		TenantData<DB> tenantData;

		// Similar to operator==, but useful in assertions for identifying which member is different
		void assertEquals(ManagementClusterData const& other) const {
			ASSERT(metaclusterRegistration == other.metaclusterRegistration);
			ASSERT(dataClusters == other.dataClusters);
			ASSERT(clusterTenantCounts == other.clusterTenantCounts);
			ASSERT(registrationTombstones == other.registrationTombstones);
			ASSERT(activeRestoreIds == other.activeRestoreIds);
			ASSERT(clusterCapacityMap == other.clusterCapacityMap);
			ASSERT(clusterTenantMap == other.clusterTenantMap);
			ASSERT(clusterTenantGroupMap == other.clusterTenantGroupMap);
			ASSERT(tenantIdPrefix == other.tenantIdPrefix);
			tenantData.assertEquals(other.tenantData);
		}

		bool operator==(ManagementClusterData const& other) const {
			return metaclusterRegistration == other.metaclusterRegistration && dataClusters == other.dataClusters &&
			       clusterTenantCounts == other.clusterTenantCounts &&
			       registrationTombstones == other.registrationTombstones &&
			       activeRestoreIds == other.activeRestoreIds && clusterCapacityMap == other.clusterCapacityMap &&
			       clusterTenantMap == other.clusterTenantMap && clusterTenantGroupMap == other.clusterTenantGroupMap &&
			       tenantIdPrefix == other.tenantIdPrefix && tenantData == other.tenantData;
		}

		bool operator!=(ManagementClusterData const& other) const { return !(*this == other); }
	};

	struct DataClusterData {
		Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		TenantData<DB> tenantData;

		// Similar to operator==, but useful in assertions for identifying which member is different
		void assertEquals(DataClusterData const& other) const {
			ASSERT(metaclusterRegistration == other.metaclusterRegistration);
			tenantData.assertEquals(other.tenantData);
		}

		bool operator==(DataClusterData const& other) const {
			return metaclusterRegistration == other.metaclusterRegistration;
		}

		bool operator!=(DataClusterData const& other) const { return !(*this == other); }
	};

	Reference<DB> managementDb;
	ManagementClusterData managementMetadata;
	std::map<ClusterName, DataClusterData> dataClusterMetadata;

private:
	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	ACTOR static Future<Void> loadManagementClusterMetadata(MetaclusterData* self) {
		state Reference<typename DB::TransactionT> managementTr = self->managementDb->createTransaction();

		state KeyBackedRangeResult<Tuple> clusterCapacityTuples;
		state KeyBackedRangeResult<Tuple> clusterTenantTuples;
		state KeyBackedRangeResult<Tuple> clusterTenantGroupTuples;

		self->managementMetadata.tenantData =
		    TenantData<DB>(self->managementDb, &MetaclusterAPI::ManagementClusterMetadata::tenantMetadata());

		loop {
			try {
				managementTr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(self->managementMetadata.tenantIdPrefix,
				           TenantMetadata::tenantIdPrefix().get(managementTr)) &&
				     store(self->managementMetadata.metaclusterRegistration,
				           MetaclusterMetadata::metaclusterRegistration().get(managementTr)) &&
				     store(self->managementMetadata.dataClusters,
				           MetaclusterAPI::listClustersTransaction(
				               managementTr, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS + 1)) &&
				     store(self->managementMetadata.clusterTenantCounts,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantCount.getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(self->managementMetadata.registrationTombstones,
				           MetaclusterMetadata::registrationTombstones().getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->TOO_MANY)) &&
				     store(self->managementMetadata.activeRestoreIds,
				           MetaclusterMetadata::activeRestoreIds().getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(clusterCapacityTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterCapacityIndex.getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(clusterTenantTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantIndex.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(clusterTenantGroupTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantGroupIndex.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     self->managementMetadata.tenantData.load(managementTr));

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(managementTr->onError(e)));
			}
		}

		for (auto t : clusterCapacityTuples.results) {
			ASSERT_EQ(t.size(), 2);
			int64_t capacity = t.getInt(0);
			ClusterName clusterName = t.getString(1);
			ASSERT(self->managementMetadata.clusterCapacityMap.try_emplace(clusterName, capacity).second);
		}

		for (auto t : clusterTenantTuples.results) {
			ASSERT_EQ(t.size(), 3);
			TenantName tenantName = t.getString(1);
			int64_t tenantId = t.getInt(2);
			// ASSERT(tenantName == self->managementMetadata.tenantMap[tenantId].tenantName);
			self->managementMetadata.clusterTenantMap[t.getString(0)].insert(tenantId);
		}

		for (auto t : clusterTenantGroupTuples.results) {
			ASSERT_EQ(t.size(), 2);
			TenantGroupName tenantGroupName = t.getString(1);
			self->managementMetadata.clusterTenantGroupMap[t.getString(0)].insert(tenantGroupName);
		}

		return Void();
	}

	ACTOR static Future<Void> loadDataClusterMetadata(MetaclusterData* self,
	                                                  ClusterName clusterName,
	                                                  ClusterConnectionString connectionString) {
		state std::pair<typename std::map<ClusterName, DataClusterData>::iterator, bool> clusterItr =
		    self->dataClusterMetadata.try_emplace(clusterName);

		if (clusterItr.second) {
			state Reference<IDatabase> dataDb = wait(MetaclusterAPI::openDatabase(connectionString));
			state Reference<ITransaction> tr = dataDb->createTransaction();

			clusterItr.first->second.tenantData = TenantData<IDatabase>(dataDb, &TenantMetadata::instance());
			loop {
				try {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					wait(store(clusterItr.first->second.metaclusterRegistration,
					           MetaclusterMetadata::metaclusterRegistration().get(tr)) &&
					     clusterItr.first->second.tenantData.load(tr));

					break;
				} catch (Error& e) {
					wait(safeThreadFutureToFuture(tr->onError(e)));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> load(MetaclusterData* self) {
		wait(loadManagementClusterMetadata(self));

		state std::vector<Future<Void>> dataClusterFutures;
		for (auto [clusterName, clusterMetadata] : self->managementMetadata.dataClusters) {
			dataClusterFutures.push_back(loadDataClusterMetadata(self, clusterName, clusterMetadata.connectionString));
		}

		wait(waitForAll(dataClusterFutures));

		return Void();
	}

public:
	MetaclusterData() {}
	MetaclusterData(Reference<DB> managementDb) : managementDb(managementDb) {}

	Future<Void> load() { return load(this); }
	Future<Void> loadDataCluster(ClusterName clusterName, ClusterConnectionString connectionString) {
		return loadDataClusterMetadata(this, clusterName, connectionString);
	}

	// Similar to operator==, but useful in assertions for identifying which member is different
	void assertEquals(MetaclusterData const& other) const {
		managementMetadata.assertEquals(other.managementMetadata);

		for (auto const& [name, data] : dataClusterMetadata) {
			auto itr = other.dataClusterMetadata.find(name);
			ASSERT(itr != other.dataClusterMetadata.end());
			data.assertEquals(itr->second);
		}

		ASSERT(dataClusterMetadata.size() == other.dataClusterMetadata.size());
	}

	bool operator==(MetaclusterData const& other) const {
		return managementMetadata == other.managementMetadata && dataClusterMetadata == other.dataClusterMetadata;
	}

	bool operator!=(MetaclusterData const& other) const { return !(*this == other); }
};

#include "flow/unactorcompiler.h"

#endif
