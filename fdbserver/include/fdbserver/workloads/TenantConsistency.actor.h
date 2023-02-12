
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
#include "flow/actorcompiler.h" // This must be the last #include.

template <class DB>
class TenantConsistencyCheck {
private:
	Reference<DB> db;

	struct TenantData {
		Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		std::map<int64_t, TenantMapEntry> tenantMap;
		std::map<TenantName, int64_t> tenantNameIndex;
		int64_t lastTenantId;
		int64_t tenantCount;
		std::set<int64_t> tenantTombstones;
		Optional<TenantTombstoneCleanupData> tombstoneCleanupData;
		std::map<TenantGroupName, TenantGroupEntry> tenantGroupMap;
		std::map<TenantGroupName, std::set<int64_t>> tenantGroupIndex;

		std::set<int64_t> tenantsInTenantGroupIndex;

		ClusterType clusterType;
	};

	TenantData metadata;

	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	ACTOR static Future<Void> loadTenantMetadata(TenantConsistencyCheck* self) {
		state Reference<typename DB::TransactionT> tr = self->db->createTransaction();
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantList;
		state KeyBackedRangeResult<std::pair<TenantName, int64_t>> tenantNameIndexList;
		state KeyBackedRangeResult<int64_t> tenantTombstoneList;
		state KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroupList;
		state KeyBackedRangeResult<Tuple> tenantGroupTenantTuples;
		state TenantMetadataSpecification* tenantMetadata;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(self->metadata.metaclusterRegistration,
				           MetaclusterMetadata::metaclusterRegistration().get(tr)));

				self->metadata.clusterType = self->metadata.metaclusterRegistration.present()
				                                 ? self->metadata.metaclusterRegistration.get().clusterType
				                                 : ClusterType::STANDALONE;

				if (self->metadata.clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
					tenantMetadata = &MetaclusterAPI::ManagementClusterMetadata::tenantMetadata();
				} else {
					tenantMetadata = &TenantMetadata::instance();
				}

				wait(
				    store(tenantList, tenantMetadata->tenantMap.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
				    store(tenantNameIndexList,
				          tenantMetadata->tenantNameIndex.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
				    store(self->metadata.lastTenantId, tenantMetadata->lastTenantId.getD(tr, Snapshot::False, -1)) &&
				    store(self->metadata.tenantCount, tenantMetadata->tenantCount.getD(tr, Snapshot::False, 0)) &&
				    store(tenantTombstoneList,
				          tenantMetadata->tenantTombstones.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
				    store(self->metadata.tombstoneCleanupData, tenantMetadata->tombstoneCleanupData.get(tr)) &&
				    store(tenantGroupTenantTuples,
				          tenantMetadata->tenantGroupTenantIndex.getRange(tr, {}, {}, metaclusterMaxTenants)) &&
				    store(tenantGroupList, tenantMetadata->tenantGroupMap.getRange(tr, {}, {}, metaclusterMaxTenants)));

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		ASSERT(!tenantList.more);
		self->metadata.tenantMap =
		    std::map<int64_t, TenantMapEntry>(tenantList.results.begin(), tenantList.results.end());

		ASSERT(!tenantNameIndexList.more);
		self->metadata.tenantNameIndex =
		    std::map<TenantName, int64_t>(tenantNameIndexList.results.begin(), tenantNameIndexList.results.end());

		ASSERT(!tenantTombstoneList.more);
		self->metadata.tenantTombstones =
		    std::set<int64_t>(tenantTombstoneList.results.begin(), tenantTombstoneList.results.end());

		ASSERT(!tenantGroupList.more);
		self->metadata.tenantGroupMap =
		    std::map<TenantGroupName, TenantGroupEntry>(tenantGroupList.results.begin(), tenantGroupList.results.end());

		for (auto t : tenantGroupTenantTuples.results) {
			ASSERT_EQ(t.size(), 2);
			TenantGroupName tenantGroupName = t.getString(0);
			int64_t tenantId = t.getInt(1);
			ASSERT(self->metadata.tenantGroupMap.count(tenantGroupName));
			ASSERT(self->metadata.tenantMap.count(tenantId));
			self->metadata.tenantGroupIndex[tenantGroupName].insert(tenantId);
			ASSERT(self->metadata.tenantsInTenantGroupIndex.insert(tenantId).second);
		}
		ASSERT_EQ(self->metadata.tenantGroupIndex.size(), self->metadata.tenantGroupMap.size());

		return Void();
	}

	void validateTenantMetadata() {
		if (metadata.clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
			ASSERT_LE(metadata.tenantMap.size(), metaclusterMaxTenants);
		} else {
			ASSERT_LE(metadata.tenantMap.size(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		}

		ASSERT_EQ(metadata.tenantMap.size(), metadata.tenantCount);
		ASSERT_EQ(metadata.tenantNameIndex.size(), metadata.tenantCount);

		int renameCount = 0;
		for (auto [tenantId, tenantMapEntry] : metadata.tenantMap) {
			ASSERT_EQ(tenantId, tenantMapEntry.id);
			if (metadata.clusterType != ClusterType::METACLUSTER_DATA) {
				ASSERT_LE(tenantId, metadata.lastTenantId);
			}
			ASSERT_EQ(metadata.tenantNameIndex[tenantMapEntry.tenantName], tenantId);

			if (tenantMapEntry.tenantGroup.present()) {
				auto tenantGroupMapItr = metadata.tenantGroupMap.find(tenantMapEntry.tenantGroup.get());
				ASSERT(tenantGroupMapItr != metadata.tenantGroupMap.end());
				if (tenantMapEntry.assignedCluster != tenantGroupMapItr->second.assignedCluster) {
					fprintf(stderr,
					        "yanqin is_standalone?=%d 2=%s 3=%s\n",
					        (int)(metadata.clusterType == ClusterType::STANDALONE),
					        tenantMapEntry.assignedCluster.present()
					            ? tenantMapEntry.assignedCluster.get().toString().c_str()
					            : "null",
					        tenantGroupMapItr->second.assignedCluster.present()
					            ? tenantGroupMapItr->second.assignedCluster.get().toString().c_str()
					            : "null");
				}
				ASSERT(tenantMapEntry.assignedCluster == tenantGroupMapItr->second.assignedCluster);
				ASSERT(metadata.tenantGroupIndex[tenantMapEntry.tenantGroup.get()].count(tenantId));
			} else {
				ASSERT(!metadata.tenantsInTenantGroupIndex.count(tenantId));
			}

			if (metadata.clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				ASSERT(tenantMapEntry.assignedCluster.present());
				if (tenantMapEntry.renameDestination.present()) {
					ASSERT(tenantMapEntry.tenantState == TenantState::RENAMING ||
					       tenantMapEntry.tenantState == TenantState::REMOVING);

					auto nameIndexItr = metadata.tenantNameIndex.find(tenantMapEntry.renameDestination.get());
					ASSERT(nameIndexItr != metadata.tenantNameIndex.end());
					ASSERT_EQ(nameIndexItr->second, tenantMapEntry.id);
					++renameCount;
				} else {
					ASSERT_NE(tenantMapEntry.tenantState, TenantState::RENAMING);
				}
			} else {
				ASSERT_EQ(tenantMapEntry.tenantState, TenantState::READY);
				ASSERT(!tenantMapEntry.assignedCluster.present());
				ASSERT(!tenantMapEntry.renameDestination.present());
			}
		}

		ASSERT_EQ(metadata.tenantMap.size() + renameCount, metadata.tenantNameIndex.size());
	}

	// Check that the tenant tombstones are properly cleaned up and only present on a metacluster data cluster
	void checkTenantTombstones() {
		if (metadata.clusterType == ClusterType::METACLUSTER_DATA) {
			if (!metadata.tombstoneCleanupData.present()) {
				ASSERT(metadata.tenantTombstones.empty());
			}

			if (!metadata.tenantTombstones.empty()) {
				ASSERT(*metadata.tenantTombstones.begin() >
				       metadata.tombstoneCleanupData.get().tombstonesErasedThrough);
			}
		} else {
			ASSERT(metadata.tenantTombstones.empty() && !metadata.tombstoneCleanupData.present());
		}
	}

	ACTOR static Future<Void> run(TenantConsistencyCheck* self) {
		wait(loadTenantMetadata(self));
		self->validateTenantMetadata();
		self->checkTenantTombstones();

		return Void();
	}

public:
	TenantConsistencyCheck() {}
	TenantConsistencyCheck(Reference<DB> db) : db(db) {}

	Future<Void> run() { return run(this); }
};

#include "flow/unactorcompiler.h"

#endif
