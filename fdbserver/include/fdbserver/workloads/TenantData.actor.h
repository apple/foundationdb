
/*
 * TenantData.actor.h
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
#include "fdbclient/KeyBackedTypes.h"
#include "flow/BooleanParam.h"
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_TENANT_DATA_ACTOR_G_H)
#define WORKLOADS_TENANT_DATA_ACTOR_G_H
#include "fdbserver/workloads/TenantData.actor.g.h"
#elif !defined(WORKLOADS_TENANT_DATA_ACTOR_H)
#define WORKLOADS_TENANT_DATA_ACTOR_H

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

template <class DB>
class TenantData {
	Reference<DB> db;
	TenantMetadataSpecification* tenantMetadata;

	Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
	ClusterType clusterType;

	std::map<int64_t, TenantMapEntry> tenantMap;
	std::map<TenantName, int64_t> tenantNameIndex;
	int64_t lastTenantId;
	int64_t tenantCount;
	std::set<int64_t> tenantTombstones;
	Optional<TenantTombstoneCleanupData> tombstoneCleanupData;
	std::map<TenantGroupName, TenantGroupEntry> tenantGroupMap;
	std::map<TenantGroupName, std::set<int64_t>> tenantGroupIndex;
	std::map<TenantGroupName, int64_t> storageQuotas;

private:
	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	ACTOR template <class Transaction>
	static Future<Void> loadTenantMetadata(TenantData* self, Transaction tr) {
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantList;
		state KeyBackedRangeResult<std::pair<TenantName, int64_t>> tenantNameIndexList;
		state KeyBackedRangeResult<int64_t> tenantTombstoneList;
		state KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroupList;
		state KeyBackedRangeResult<Tuple> tenantGroupTenantTuples;
		state KeyBackedRangeResult<std::pair<TenantGroupName, int64_t>> storageQuotaList;

		wait(store(self->metaclusterRegistration, MetaclusterMetadata::metaclusterRegistration().get(tr)));

		self->clusterType = self->metaclusterRegistration.present() ? self->metaclusterRegistration.get().clusterType
		                                                            : ClusterType::STANDALONE;

		wait(store(tenantList, self->tenantMetadata->tenantMap.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
		     store(tenantNameIndexList,
		           self->tenantMetadata->tenantNameIndex.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
		     store(self->lastTenantId, self->tenantMetadata->lastTenantId.getD(tr, Snapshot::False, -1)) &&
		     store(self->tenantCount, self->tenantMetadata->tenantCount.getD(tr, Snapshot::False, 0)) &&
		     store(tenantTombstoneList,
		           self->tenantMetadata->tenantTombstones.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
		     store(self->tombstoneCleanupData, self->tenantMetadata->tombstoneCleanupData.get(tr)) &&
		     store(tenantGroupTenantTuples,
		           self->tenantMetadata->tenantGroupTenantIndex.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
		     store(tenantGroupList, self->tenantMetadata->tenantGroupMap.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
		     store(storageQuotaList, self->tenantMetadata->storageQuota.getRange(tr, {}, {}, metaclusterMaxTenants)));

		ASSERT(!tenantList.more);
		self->tenantMap = std::map<int64_t, TenantMapEntry>(tenantList.results.begin(), tenantList.results.end());

		ASSERT(!tenantNameIndexList.more);
		self->tenantNameIndex =
		    std::map<TenantName, int64_t>(tenantNameIndexList.results.begin(), tenantNameIndexList.results.end());

		ASSERT(!tenantTombstoneList.more);
		self->tenantTombstones =
		    std::set<int64_t>(tenantTombstoneList.results.begin(), tenantTombstoneList.results.end());

		ASSERT(!tenantGroupList.more);
		self->tenantGroupMap =
		    std::map<TenantGroupName, TenantGroupEntry>(tenantGroupList.results.begin(), tenantGroupList.results.end());

		ASSERT(!storageQuotaList.more);
		self->storageQuotas =
		    std::map<TenantGroupName, int64_t>(storageQuotaList.results.begin(), storageQuotaList.results.end());

		for (auto t : tenantGroupTenantTuples.results) {
			ASSERT_EQ(t.size(), 2);
			TenantGroupName tenantGroupName = t.getString(0);
			int64_t tenantId = t.getInt(1);
			ASSERT(self->tenantGroupMap.count(tenantGroupName));
			ASSERT(self->tenantMap.count(tenantId));
			self->tenantGroupIndex[tenantGroupName].insert(tenantId);
		}
		ASSERT_EQ(self->tenantGroupIndex.size(), self->tenantGroupMap.size());

		return Void();
	}

public:
	TenantData() {}
	TenantData(Reference<DB> db, TenantMetadataSpecification* tenantMetadata)
	  : db(db), tenantMetadata(tenantMetadata) {}

	Future<Void> load() {
		return runTransaction([this](Reference<typename DB::TransactionT> tr) { return loadTenantMetadata(this); });
	}

	template <class Transaction>
	Future<Void> load(Transaction tr) {
		return loadTenantMetadata(this, tr);
	}

	// Similar to operator==, but useful in assertions for identifying which member is different
	void assertEquals(TenantData const& other) const {
		ASSERT(metaclusterRegistration == other.metaclusterRegistration);
		ASSERT_EQ(clusterType, other.clusterType);
		ASSERT(tenantMap == other.tenantMap);
		ASSERT(tenantNameIndex == other.tenantNameIndex);
		ASSERT_EQ(lastTenantId, other.lastTenantId);
		ASSERT_EQ(tenantCount, other.tenantCount);
		ASSERT(tenantTombstones == other.tenantTombstones);
		ASSERT(tombstoneCleanupData == other.tombstoneCleanupData);
		ASSERT(tenantGroupMap == other.tenantGroupMap);
		ASSERT(tenantGroupIndex == other.tenantGroupIndex);
		ASSERT(storageQuotas == other.storageQuotas);
	}

	bool operator==(TenantData const& other) const {
		return metaclusterRegistration == other.metaclusterRegistration && clusterType == other.clusterType &&
		       tenantMap == other.tenantMap && tenantNameIndex == other.tenantNameIndex &&
		       lastTenantId == other.lastTenantId && tenantCount == other.tenantCount &&
		       tenantTombstones == other.tenantTombstones && tombstoneCleanupData == other.tombstoneCleanupData &&
		       tenantGroupMap == other.tenantGroupMap && tenantGroupIndex == other.tenantGroupIndex &&
		       storageQuotas == other.storageQuotas;
	}

	bool operator!=(TenantData const& other) const { return !(*this == other); }
};

#include "flow/unactorcompiler.h"

#endif
