/*
 * MetaclusterRestoreWorkload.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/MetaclusterData.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterRestoreWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterRestore";

	struct DataClusterData {
		Database db;
		std::set<int64_t> tenants;
		std::set<TenantGroupName> tenantGroups;
		bool restored = false;
		bool restoreHasMessages = false;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	struct RestoreTenantData {
		enum class AccessTime { NONE, BEFORE_BACKUP, DURING_BACKUP, AFTER_BACKUP };

		TenantName name;
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;
		AccessTime createTime = AccessTime::BEFORE_BACKUP;
		AccessTime renameTime = AccessTime::NONE;
		AccessTime configureTime = AccessTime::NONE;

		RestoreTenantData() {}
		RestoreTenantData(TenantName name,
		                  ClusterName cluster,
		                  Optional<TenantGroupName> tenantGroup,
		                  AccessTime createTime)
		  : name(name), cluster(cluster), tenantGroup(tenantGroup), createTime(createTime) {}
	};

	struct TenantGroupData {
		ClusterName cluster;
		std::set<int64_t> tenants;
	};

	Reference<IDatabase> managementDb;
	std::map<ClusterName, DataClusterData> dataDbs;
	std::vector<ClusterName> dataDbIndex;

	std::map<int64_t, RestoreTenantData> createdTenants;
	std::map<TenantName, int64_t> tenantNameIndex;
	std::map<TenantGroupName, TenantGroupData> tenantGroups;

	std::set<int64_t> deletedTenants;
	std::vector<std::pair<int64_t, metacluster::MetaclusterTenantMapEntry>> managementTenantsBeforeRestore;

	int initialTenants;
	int maxTenants;
	int maxTenantGroups;
	int tenantGroupCapacity;

	bool recoverManagementCluster;
	bool recoverDataClusters;

	int initialTenantIdPrefix;
	bool backupComplete = false;
	double postBackupDuration;
	double endTime = std::numeric_limits<double>::max();

	MetaclusterRestoreWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		initialTenants = std::min<int>(maxTenants, getOption(options, "initialTenants"_sr, 40));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		postBackupDuration = getOption(options, "postBackupDuration"_sr, 30);

		tenantGroupCapacity = (initialTenants / 2 + maxTenantGroups - 1) / g_simulator->extraDatabases.size();
		int mode = deterministicRandom()->randomInt(0, 3);
		recoverManagementCluster = (mode != 2);
		recoverDataClusters = (mode != 1);

		initialTenantIdPrefix = deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                                         TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1);
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant%08d", deterministicRandom()->randomInt(0, maxTenants)));
		return tenant;
	}

	Optional<TenantGroupName> chooseTenantGroup(Optional<ClusterName> cluster = Optional<ClusterName>()) {
		Optional<TenantGroupName> tenantGroup;
		if (deterministicRandom()->coinflip()) {
			if (!cluster.present()) {
				tenantGroup =
				    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
			} else {
				auto const& existingGroups = dataDbs[cluster.get()].tenantGroups;
				if (deterministicRandom()->coinflip() && !existingGroups.empty()) {
					tenantGroup = deterministicRandom()->randomChoice(
					    std::vector<TenantGroupName>(existingGroups.begin(), existingGroups.end()));
				} else if (tenantGroups.size() < maxTenantGroups) {
					do {
						tenantGroup = TenantGroupNameRef(
						    format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
					} while (tenantGroups.count(tenantGroup.get()) > 0);
				}
			}
		}

		return tenantGroup;
	}

	// Used to gradually increase capacity so that the tenants are somewhat evenly distributed across the clusters
	ACTOR static Future<Void> increaseMetaclusterCapacity(MetaclusterRestoreWorkload* self) {
		self->tenantGroupCapacity = ceil(self->tenantGroupCapacity * 1.2);
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state int dbIndex;
				for (dbIndex = 0; dbIndex < self->dataDbIndex.size(); ++dbIndex) {
					metacluster::DataClusterMetadata clusterMetadata =
					    wait(metacluster::getClusterTransaction(tr, self->dataDbIndex[dbIndex]));
					metacluster::DataClusterEntry updatedEntry = clusterMetadata.entry;
					updatedEntry.capacity.numTenantGroups = self->tenantGroupCapacity;
					metacluster::updateClusterMetadata(
					    tr, self->dataDbIndex[dbIndex], clusterMetadata, {}, updatedEntry);
				}
				wait(safeThreadFutureToFuture(tr->commit()));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		return Void();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}
	ACTOR static Future<Void> _setup(Database cx, MetaclusterRestoreWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));

		MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
		self->managementDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);
		wait(success(metacluster::createMetacluster(
		    self->managementDb, "management_cluster"_sr, self->initialTenantIdPrefix, false)));

		ASSERT(g_simulator->extraDatabases.size() > 0);
		state std::vector<std::string>::iterator extraDatabasesItr;
		for (extraDatabasesItr = g_simulator->extraDatabases.begin();
		     extraDatabasesItr != g_simulator->extraDatabases.end();
		     ++extraDatabasesItr) {
			ClusterConnectionString ccs(*extraDatabasesItr);
			auto extraFile = makeReference<ClusterConnectionMemoryRecord>(ccs);
			state ClusterName clusterName = ClusterName(format("cluster_%08d", self->dataDbs.size()));
			Database db = Database::createDatabase(extraFile, ApiVersion::LATEST_VERSION);
			self->dataDbIndex.push_back(clusterName);
			self->dataDbs[clusterName] = DataClusterData(db);

			metacluster::DataClusterEntry clusterEntry;
			clusterEntry.capacity.numTenantGroups = self->tenantGroupCapacity;

			wait(metacluster::registerCluster(self->managementDb, clusterName, ccs, clusterEntry));
		}

		TraceEvent(SevDebug, "MetaclusterRestoreWorkloadCreateTenants").detail("NumTenants", self->initialTenants);

		while (self->createdTenants.size() < self->initialTenants) {
			wait(createTenant(self, RestoreTenantData::AccessTime::BEFORE_BACKUP));
		}

		TraceEvent(SevDebug, "MetaclusterRestoreWorkloadCreateTenantsComplete");

		return Void();
	}

	ACTOR static Future<std::string> backupCluster(ClusterName clusterName,
	                                               Database dataDb,
	                                               MetaclusterRestoreWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<StringRef> backupContainer = "file://simfdb/backups/"_sr.withSuffix(clusterName);
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;

		addDefaultBackupRanges(backupRanges);

		TraceEvent("MetaclusterRestoreWorkloadSubmitBackup").detail("ClusterName", clusterName);
		try {
			wait(backupAgent.submitBackup(
			    dataDb, backupContainer, {}, 0, 0, clusterName.toString(), backupRanges, StopWhenDone::True));
		} catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("MetaclusterRestoreWorkloadWaitBackup").detail("ClusterName", clusterName);
		state Reference<IBackupContainer> container;
		wait(success(backupAgent.waitBackup(dataDb, clusterName.toString(), StopWhenDone::True, &container)));
		TraceEvent("MetaclusterRestoreWorkloadBackupComplete").detail("ClusterName", clusterName);
		return container->getURL();
	}

	ACTOR static Future<Void> restoreDataCluster(ClusterName clusterName,
	                                             Database dataDb,
	                                             std::string backupUrl,
	                                             bool addToMetacluster,
	                                             metacluster::ForceJoin forceJoin,
	                                             int simultaneousRestoreCount,
	                                             MetaclusterRestoreWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		state metacluster::ForceReuseTenantIdPrefix forceReuseTenantIdPrefix(deterministicRandom()->coinflip());
		addDefaultBackupRanges(backupRanges);

		TraceEvent("MetaclusterRestoreWorkloadClearDatabase").detail("ClusterName", clusterName);
		wait(runTransaction(dataDb.getReference(),
		                    [backupRanges = backupRanges](Reference<ReadYourWritesTransaction> tr) {
			                    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			                    for (auto range : backupRanges) {
				                    tr->clear(range);
			                    }
			                    return Future<Void>(Void());
		                    }));

		TraceEvent("MetaclusterRestoreWorkloadRestoreCluster").detail("ClusterName", clusterName);
		wait(success(backupAgent.restore(dataDb, dataDb, clusterName, StringRef(backupUrl), {}, backupRanges)));

		state std::vector<std::string> messages;
		if (addToMetacluster) {
			TraceEvent("MetaclusterRestoreWorkloadAddClusterToMetacluster").detail("ClusterName", clusterName);
			if (deterministicRandom()->coinflip()) {
				TraceEvent("MetaclusterRestoreWorkloadAddClusterToMetaclusterDryRun")
				    .detail("ClusterName", clusterName);

				state metacluster::util::MetaclusterData<IDatabase> preDryRunMetaclusterData(self->managementDb);
				wait(preDryRunMetaclusterData.load());

				wait(metacluster::restoreCluster(self->managementDb,
				                                 clusterName,
				                                 dataDb->getConnectionRecord()->getConnectionString(),
				                                 metacluster::ApplyManagementClusterUpdates::True,
				                                 metacluster::RestoreDryRun::True,
				                                 forceJoin,
				                                 forceReuseTenantIdPrefix,
				                                 &messages));

				state metacluster::util::MetaclusterData<IDatabase> postDryRunMetaclusterData(self->managementDb);
				wait(postDryRunMetaclusterData.load());

				// A dry-run shouldn't change anything
				if (simultaneousRestoreCount == 1) {
					preDryRunMetaclusterData.assertEquals(postDryRunMetaclusterData);
				} else {
					preDryRunMetaclusterData.dataClusterMetadata[clusterName].assertEquals(
					    postDryRunMetaclusterData.dataClusterMetadata[clusterName]);
				}

				TraceEvent("MetaclusterRestoreWorkloadAddClusterToMetaclusterDryRunDone")
				    .detail("ClusterName", clusterName);
				messages.clear();
			}

			state int numRestores = deterministicRandom()->randomInt(1, 3);
			state bool successfulRestore = false;
			while (!successfulRestore) {
				state std::vector<Future<ErrorOr<Void>>> restoreFutures;
				for (; numRestores > 0; numRestores--) {
					restoreFutures.push_back(
					    errorOr(metacluster::restoreCluster(self->managementDb,
					                                        clusterName,
					                                        dataDb->getConnectionRecord()->getConnectionString(),
					                                        metacluster::ApplyManagementClusterUpdates::True,
					                                        metacluster::RestoreDryRun::False,
					                                        forceJoin,
					                                        forceReuseTenantIdPrefix,
					                                        &messages)));
					wait(delay(deterministicRandom()->random01() * 5));
				}

				wait(waitForAll(restoreFutures));

				for (auto const& f : restoreFutures) {
					if (!f.get().isError()) {
						successfulRestore = true;
					} else {
						ASSERT(f.get().getError().code() == error_code_conflicting_restore);
					}
				}

				ASSERT(successfulRestore || restoreFutures.size() > 1);
				numRestores = 1;
			}

			metacluster::DataClusterMetadata clusterMetadata =
			    wait(metacluster::getCluster(self->managementDb, clusterName));
			ASSERT_EQ(clusterMetadata.entry.clusterState, metacluster::DataClusterState::READY);
			TraceEvent("MetaclusterRestoreWorkloadRestoreComplete").detail("ClusterName", clusterName);
		}

		self->dataDbs[clusterName].restored = true;
		self->dataDbs[clusterName].restoreHasMessages = !messages.empty();

		return Void();
	}

	void removeTrackedTenant(int64_t tenantId) {
		auto itr = createdTenants.find(tenantId);
		if (itr != createdTenants.end()) {
			TraceEvent(SevDebug, "MetaclusterRestoreWorkloadRemoveTrackedTenant")
			    .detail("TenantId", tenantId)
			    .detail("TenantName", itr->second.name);
			deletedTenants.insert(tenantId);
			dataDbs[itr->second.cluster].tenants.erase(tenantId);
			if (itr->second.tenantGroup.present()) {
				tenantGroups[itr->second.tenantGroup.get()].tenants.erase(tenantId);
			}
			createdTenants.erase(itr);
		}
	}

	// A map from tenant name to a pair of IDs. The first ID is from the data cluster, and the second is from the
	// management cluster.
	using TenantCollisions = std::unordered_map<TenantName, std::pair<int64_t, int64_t>>;

	using GroupCollisions = std::unordered_set<TenantGroupName>;

	Future<Void> resolveTenantCollisions(MetaclusterRestoreWorkload* self,
	                                     ClusterName clusterName,
	                                     Database dataDb,
	                                     TenantCollisions const& tenantCollisions) {
		TraceEvent("MetaclusterRestoreWorkloadDeleteTenantCollisions")
		    .detail("FromCluster", clusterName)
		    .detail("TenantCollisions", tenantCollisions.size());
		std::vector<Future<Void>> deleteFutures;
		for (auto const& t : tenantCollisions) {
			// If the data cluster tenant is expected, then remove the management tenant
			// Note that the management tenant may also have been expected
			if (self->createdTenants.count(t.second.first)) {
				removeTrackedTenant(t.second.second);
				deleteFutures.push_back(metacluster::deleteTenant(self->managementDb, t.second.second));
			}
			// We don't expect the data cluster tenant, so delete it
			else {
				removeTrackedTenant(t.second.first);
				deleteFutures.push_back(TenantAPI::deleteTenant(dataDb.getReference(), t.first, t.second.first));
			}
		}

		return waitForAll(deleteFutures);
	}

	ACTOR template <class Transaction, class TenantTypes>
	static Future<std::unordered_set<int64_t>> getTenantsInGroup(
	    Transaction tr,
	    TenantMetadataSpecification<TenantTypes> tenantMetadata,
	    TenantGroupName tenantGroup) {
		KeyBackedRangeResult<Tuple> groupTenants =
		    wait(tenantMetadata.tenantGroupTenantIndex.getRange(tr,
		                                                        Tuple::makeTuple(tenantGroup),
		                                                        Tuple::makeTuple(keyAfter(tenantGroup)),
		                                                        CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		std::unordered_set<int64_t> tenants;
		for (auto const& tuple : groupTenants.results) {
			tenants.insert(tuple.getInt(2));
		}

		return tenants;
	}

	ACTOR Future<Void> resolveGroupCollisions(MetaclusterRestoreWorkload* self,
	                                          ClusterName clusterName,
	                                          Database dataDb,
	                                          GroupCollisions groupCollisions) {
		TraceEvent("MetaclusterRestoreWorkloadDeleteTenantGroupCollisions")
		    .detail("FromCluster", clusterName)
		    .detail("GroupCollisions", groupCollisions.size());

		state std::vector<Future<Void>> deleteFutures;

		state GroupCollisions::const_iterator collisionItr;
		for (collisionItr = groupCollisions.begin(); collisionItr != groupCollisions.end(); ++collisionItr) {
			// If the data cluster tenant group is expected, then remove the management tenant group
			// Note that the management tenant group may also have been expected
			auto itr = self->tenantGroups.find(*collisionItr);
			if (itr->second.cluster == clusterName) {
				TraceEvent(SevDebug, "MetaclusterRestoreWorkloadDeleteTenantGroupCollision")
				    .detail("From", "ManagementCluster")
				    .detail("TenantGroup", *collisionItr);
				std::unordered_set<int64_t> tenantsInGroup =
				    wait(runTransaction(self->managementDb, [collisionItr = collisionItr](Reference<ITransaction> tr) {
					    return getTenantsInGroup(
					        tr, metacluster::metadata::management::tenantMetadata(), *collisionItr);
				    }));

				for (auto const& t : tenantsInGroup) {
					self->removeTrackedTenant(t);
					deleteFutures.push_back(metacluster::deleteTenant(self->managementDb, t));
				}

			}
			// The tenant group from the management cluster is what we expect
			else {
				TraceEvent(SevDebug, "MetaclusterRestoreWorkloadDeleteTenantGroupCollision")
				    .detail("From", "DataCluster")
				    .detail("TenantGroup", *collisionItr);
				std::unordered_set<int64_t> tenantsInGroup = wait(runTransaction(
				    dataDb.getReference(), [collisionItr = collisionItr](Reference<ReadYourWritesTransaction> tr) {
					    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					    return getTenantsInGroup(tr, TenantMetadata::instance(), *collisionItr);
				    }));

				deleteFutures.push_back(runTransactionVoid(
				    dataDb.getReference(), [self = self, tenantsInGroup](Reference<ReadYourWritesTransaction> tr) {
					    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					    std::vector<Future<Void>> groupDeletions;
					    for (auto const& t : tenantsInGroup) {
						    self->removeTrackedTenant(t);
						    groupDeletions.push_back(TenantAPI::deleteTenantTransaction(tr, t));
					    }
					    return waitForAll(groupDeletions);
				    }));
			}
		}

		wait(waitForAll(deleteFutures));
		return Void();
	}

	ACTOR static Future<std::vector<std::pair<int64_t, TenantMapEntry>>> getDataClusterTenants(Database db) {
		KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants =
		    wait(runTransaction(db.getReference(), [](Reference<ReadYourWritesTransaction> tr) {
			    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			    return TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1);
		    }));

		ASSERT_LE(tenants.results.size(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		return tenants.results;
	}

	ACTOR static Future<std::pair<TenantCollisions, GroupCollisions>> getCollisions(MetaclusterRestoreWorkload* self,
	                                                                                Database db) {
		state KeyBackedRangeResult<std::pair<TenantName, int64_t>> managementTenantList;
		state KeyBackedRangeResult<std::pair<TenantGroupName, metacluster::MetaclusterTenantGroupEntry>>
		    managementGroupList;
		state KeyBackedRangeResult<std::pair<TenantName, int64_t>> dataClusterTenants;
		state KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> dataClusterGroups;

		state TenantCollisions tenantCollisions;
		state GroupCollisions groupCollisions;

		// Read the management cluster tenant map and tenant group map
		wait(runTransactionVoid(
		    self->managementDb,
		    [managementTenantList = &managementTenantList,
		     managementGroupList = &managementGroupList](Reference<ITransaction> tr) {
			    return store(*managementTenantList,
			                 metacluster::metadata::management::tenantMetadata().tenantNameIndex.getRange(
			                     tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)) &&
			           store(*managementGroupList,
			                 metacluster::metadata::management::tenantMetadata().tenantGroupMap.getRange(
			                     tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		    }));

		// Read the data cluster tenant map and tenant group map
		wait(runTransaction(db.getReference(),
		                    [dataClusterTenants = &dataClusterTenants,
		                     dataClusterGroups = &dataClusterGroups](Reference<ReadYourWritesTransaction> tr) {
			                    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			                    return store(*dataClusterTenants,
			                                 TenantMetadata::tenantNameIndex().getRange(
			                                     tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)) &&
			                           store(*dataClusterGroups,
			                                 TenantMetadata::tenantGroupMap().getRange(
			                                     tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		                    }));

		std::unordered_map<TenantName, int64_t> managementTenants(managementTenantList.results.begin(),
		                                                          managementTenantList.results.end());
		std::unordered_map<TenantGroupName, metacluster::MetaclusterTenantGroupEntry> managementGroups(
		    managementGroupList.results.begin(), managementGroupList.results.end());

		ASSERT(managementTenants.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		ASSERT(managementGroups.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		ASSERT(dataClusterTenants.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		ASSERT(dataClusterGroups.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);

		for (auto const& t : dataClusterTenants.results) {
			auto itr = managementTenants.find(t.first);
			if (itr != managementTenants.end()) {
				tenantCollisions[t.first] = std::make_pair(t.second, itr->second);
			}
		}
		for (auto const& g : dataClusterGroups.results) {
			if (managementGroups.count(g.first)) {
				groupCollisions.insert(g.first);
			}
		}

		return std::make_pair(tenantCollisions, groupCollisions);
	}

	ACTOR static Future<Void> restoreManagementCluster(MetaclusterRestoreWorkload* self) {
		TraceEvent("MetaclusterRestoreWorkloadRestoringManagementCluster");

		state int newTenantIdPrefix = self->initialTenantIdPrefix;
		if (deterministicRandom()->coinflip()) {
			while (newTenantIdPrefix == self->initialTenantIdPrefix) {
				newTenantIdPrefix = deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
				                                                     TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1);
			}
		}

		wait(success(
		    metacluster::createMetacluster(self->managementDb, "management_cluster"_sr, newTenantIdPrefix, false)));
		state std::map<ClusterName, DataClusterData>::iterator clusterItr;
		for (clusterItr = self->dataDbs.begin(); clusterItr != self->dataDbs.end(); ++clusterItr) {
			TraceEvent("MetaclusterRestoreWorkloadProcessDataCluster").detail("FromCluster", clusterItr->first);

			// Remove the data cluster from its old metacluster
			wait(success(metacluster::removeCluster(clusterItr->second.db.getReference(),
			                                        clusterItr->first,
			                                        ClusterType::METACLUSTER_DATA,
			                                        metacluster::ForceRemove::True)));
			TraceEvent("MetaclusterRestoreWorkloadForgotMetacluster").detail("ClusterName", clusterItr->first);

			state std::pair<TenantCollisions, GroupCollisions> collisions =
			    wait(getCollisions(self, clusterItr->second.db));

			state std::vector<std::string> messages;
			state bool completed = false;
			while (!completed) {
				state std::vector<std::pair<int64_t, TenantMapEntry>> dataTenantsBeforeRestore =
				    wait(getDataClusterTenants(clusterItr->second.db));

				try {
					TraceEvent("MetaclusterRestoreWorkloadRestoreManagementCluster")
					    .detail("FromCluster", clusterItr->first)
					    .detail("TenantCollisions", collisions.first.size());

					if (deterministicRandom()->coinflip()) {
						TraceEvent("MetaclusterRestoreWorkloadRestoreManagementClusterDryRun")
						    .detail("FromCluster", clusterItr->first)
						    .detail("TenantCollisions", collisions.first.size());

						state metacluster::util::MetaclusterData<IDatabase> preDryRunMetaclusterData(
						    self->managementDb);
						wait(preDryRunMetaclusterData.load());
						std::vector<Future<Void>> preDataClusterLoadFutures;
						for (auto const& [name, data] : self->dataDbs) {
							preDataClusterLoadFutures.push_back(preDryRunMetaclusterData.loadDataCluster(
							    name, data.db->getConnectionRecord()->getConnectionString()));
						}
						wait(waitForAll(preDataClusterLoadFutures));

						wait(metacluster::restoreCluster(
						    self->managementDb,
						    clusterItr->first,
						    clusterItr->second.db->getConnectionRecord()->getConnectionString(),
						    metacluster::ApplyManagementClusterUpdates::False,
						    metacluster::RestoreDryRun::True,
						    metacluster::ForceJoin(deterministicRandom()->coinflip()),
						    metacluster::ForceReuseTenantIdPrefix(newTenantIdPrefix == self->initialTenantIdPrefix),
						    &messages));

						state metacluster::util::MetaclusterData<IDatabase> postDryRunMetaclusterData(
						    self->managementDb);
						wait(postDryRunMetaclusterData.load());
						std::vector<Future<Void>> postDataClusterLoadFutures;
						for (auto const& [name, data] : self->dataDbs) {
							postDataClusterLoadFutures.push_back(postDryRunMetaclusterData.loadDataCluster(
							    name, data.db->getConnectionRecord()->getConnectionString()));
						}
						wait(waitForAll(postDataClusterLoadFutures));

						// A dry-run shouldn't change anything
						preDryRunMetaclusterData.assertEquals(postDryRunMetaclusterData);

						TraceEvent("MetaclusterRestoreWorkloadRestoreManagementClusterDryRunDone")
						    .detail("FromCluster", clusterItr->first)
						    .detail("TenantCollisions", collisions.first.size());

						messages.clear();
					}

					state int numRestores = deterministicRandom()->randomInt(1, 3);
					state bool successfulRestore = false;
					loop {
						state std::vector<std::vector<std::string>> messagesList(numRestores);
						state std::vector<Future<ErrorOr<Void>>> restoreFutures;
						for (; numRestores > 0; numRestores--) {
							restoreFutures.push_back(errorOr(metacluster::restoreCluster(
							    self->managementDb,
							    clusterItr->first,
							    clusterItr->second.db->getConnectionRecord()->getConnectionString(),
							    metacluster::ApplyManagementClusterUpdates::False,
							    metacluster::RestoreDryRun::False,
							    metacluster::ForceJoin(deterministicRandom()->coinflip()),
							    metacluster::ForceReuseTenantIdPrefix(newTenantIdPrefix == self->initialTenantIdPrefix),
							    &messagesList[restoreFutures.size()])));
							wait(delay(deterministicRandom()->random01() * 5));
						}

						wait(waitForAll(restoreFutures));

						Optional<Error> nonConflictError;
						for (int i = 0; i < restoreFutures.size(); ++i) {
							ErrorOr<Void> result = restoreFutures[i].get();
							fmt::print("Restore result for {}: {}\n",
							           printable(clusterItr->first),
							           result.isError() ? result.getError().what() : "success");
							if (!result.isError()) {
								ASSERT(!successfulRestore);
								successfulRestore = true;
								messages = messagesList[i];
							} else if (result.getError().code() != error_code_conflicting_restore &&
							           result.getError().code() != error_code_cluster_already_registered &&
							           result.getError().code() != error_code_cluster_already_exists &&
							           !nonConflictError.present()) {
								nonConflictError = result.getError().code();
								messages = messagesList[i];
							}
						}

						if (nonConflictError.present()) {
							ASSERT(!successfulRestore);
							throw nonConflictError.get();
						} else if (successfulRestore) {
							break;
						}

						ASSERT_GT(restoreFutures.size(), 1);

						numRestores = 1;
						wait(success(metacluster::removeCluster(clusterItr->second.db.getReference(),
						                                        clusterItr->first,
						                                        ClusterType::METACLUSTER_DATA,
						                                        metacluster::ForceRemove::True)));
						wait(success(metacluster::removeCluster(self->managementDb,
						                                        clusterItr->first,
						                                        ClusterType::METACLUSTER_MANAGEMENT,
						                                        metacluster::ForceRemove::True)));
						TraceEvent("MetaclusterRestoreWorkloadRemovedFailedCluster")
						    .detail("ClusterName", clusterItr->first);
					}

					metacluster::DataClusterMetadata clusterMetadata =
					    wait(metacluster::getCluster(self->managementDb, clusterItr->first));
					ASSERT_EQ(clusterMetadata.entry.clusterState, metacluster::DataClusterState::READY);

					ASSERT(collisions.first.empty() && collisions.second.empty());
					completed = true;
				} catch (Error& e) {
					bool failedDueToCollision =
					    (e.code() == error_code_tenant_already_exists && !collisions.first.empty()) ||
					    (e.code() == error_code_invalid_tenant_configuration && !collisions.second.empty());
					if (!failedDueToCollision) {
						TraceEvent(SevError, "MetaclusterRestoreWorkloadRestoreManagementClusterFailed")
						    .error(e)
						    .detail("FromCluster", clusterItr->first)
						    .detail("TenantCollisions", collisions.first.size());
						ASSERT(false);
					}

					// If the restore did not succeed, remove the partially restored cluster
					try {
						wait(success(metacluster::removeCluster(self->managementDb,
						                                        clusterItr->first,
						                                        ClusterType::METACLUSTER_MANAGEMENT,
						                                        metacluster::ForceRemove::True)));
						TraceEvent("MetaclusterRestoreWorkloadRemovedFailedCluster")
						    .detail("ClusterName", clusterItr->first);
					} catch (Error& e) {
						if (e.code() != error_code_cluster_not_found) {
							throw;
						}
					}
				}

				std::vector<std::pair<int64_t, TenantMapEntry>> dataTenantsAfterRestore =
				    wait(getDataClusterTenants(clusterItr->second.db));

				// Restoring a management cluster from data clusters should not change the data clusters at all
				fmt::print("Checking data clusters: {}\n", completed);
				ASSERT_EQ(dataTenantsBeforeRestore.size(), dataTenantsAfterRestore.size());
				for (int i = 0; i < dataTenantsBeforeRestore.size(); ++i) {
					ASSERT_EQ(dataTenantsBeforeRestore[i].first, dataTenantsAfterRestore[i].first);
					ASSERT(dataTenantsBeforeRestore[i].second == dataTenantsAfterRestore[i].second);
				}

				// If we didn't succeed, resolve tenant and group collisions and try again
				if (!completed) {
					ASSERT(messages.size() > 0);

					wait(self->resolveTenantCollisions(
					    self, clusterItr->first, clusterItr->second.db, collisions.first));
					wait(self->resolveGroupCollisions(
					    self, clusterItr->first, clusterItr->second.db, collisions.second));

					collisions.first.clear();
					collisions.second.clear();
				}
			}
			TraceEvent("MetaclusterRestoreWorkloadRestoredDataClusterToManagementCluster")
			    .detail("FromCluster", clusterItr->first);
		}

		TraceEvent("MetaclusterRestoreWorkloadRestoredManagementCluster");
		return Void();
	}

	ACTOR static Future<Void> resetManagementCluster(MetaclusterRestoreWorkload* self) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		TraceEvent("MetaclusterRestoreWorkloadEraseManagementCluster");
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->clear(""_sr, "\xff"_sr);
				metacluster::metadata::metaclusterRegistration().clear(tr);
				wait(safeThreadFutureToFuture(tr->commit()));
				TraceEvent("MetaclusterRestoreWorkloadManagementClusterErased");
				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> createTenant(MetaclusterRestoreWorkload* self, RestoreTenantData::AccessTime createTime) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(tenantName) == 0) {
				break;
			}
		}

		if (self->tenantNameIndex.count(tenantName)) {
			return Void();
		}

		loop {
			try {
				metacluster::MetaclusterTenantMapEntry tenantEntry;
				tenantEntry.tenantName = tenantName;
				tenantEntry.tenantGroup = self->chooseTenantGroup();
				wait(metacluster::createTenant(
				    self->managementDb, tenantEntry, metacluster::AssignClusterAutomatically::True));
				metacluster::MetaclusterTenantMapEntry createdEntry =
				    wait(metacluster::getTenant(self->managementDb, tenantName));
				TraceEvent(SevDebug, "MetaclusterRestoreWorkloadCreatedTenant")
				    .detail("Tenant", tenantName)
				    .detail("TenantId", createdEntry.id)
				    .detail("AccessTime", createTime);
				self->createdTenants[createdEntry.id] =
				    RestoreTenantData(tenantName, createdEntry.assignedCluster, createdEntry.tenantGroup, createTime);
				self->tenantNameIndex[tenantName] = createdEntry.id;
				auto& dataDb = self->dataDbs[createdEntry.assignedCluster];
				dataDb.tenants.insert(createdEntry.id);
				if (createdEntry.tenantGroup.present()) {
					auto& tenantGroupData = self->tenantGroups[createdEntry.tenantGroup.get()];
					tenantGroupData.cluster = createdEntry.assignedCluster;
					tenantGroupData.tenants.insert(createdEntry.id);
					dataDb.tenantGroups.insert(createdEntry.tenantGroup.get());
				}
				return Void();
			} catch (Error& e) {
				if (e.code() != error_code_metacluster_no_capacity) {
					throw;
				}

				wait(increaseMetaclusterCapacity(self));
			}
		}
	}

	ACTOR static Future<Void> deleteTenant(MetaclusterRestoreWorkload* self, RestoreTenantData::AccessTime accessTime) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(tenantName) != 0) {
				break;
			}
		}

		if (self->tenantNameIndex.count(tenantName) == 0) {
			return Void();
		}

		state int64_t tenantId = self->tenantNameIndex[tenantName];

		TraceEvent(SevDebug, "MetaclusterRestoreWorkloadDeleteTenant")
		    .detail("Tenant", tenantName)
		    .detail("TenantId", tenantId)
		    .detail("AccessTime", accessTime);
		wait(metacluster::deleteTenant(self->managementDb, tenantName));
		auto const& tenantData = self->createdTenants[tenantId];

		auto& dataDb = self->dataDbs[tenantData.cluster];
		dataDb.tenants.erase(tenantId);
		if (tenantData.tenantGroup.present()) {
			auto groupItr = self->tenantGroups.find(tenantData.tenantGroup.get());
			groupItr->second.tenants.erase(tenantId);
			if (groupItr->second.tenants.empty()) {
				self->tenantGroups.erase(groupItr);
				dataDb.tenantGroups.erase(tenantData.tenantGroup.get());
			}
		}

		self->createdTenants.erase(tenantId);
		self->tenantNameIndex.erase(tenantName);
		self->deletedTenants.insert(tenantId);

		return Void();
	}

	ACTOR static Future<Void> configureTenant(MetaclusterRestoreWorkload* self,
	                                          RestoreTenantData::AccessTime accessTime) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(tenantName) != 0) {
				break;
			}
		}

		if (self->tenantNameIndex.count(tenantName) == 0) {
			return Void();
		}

		state int64_t tenantId = self->tenantNameIndex[tenantName];

		state Optional<TenantGroupName> tenantGroup = self->chooseTenantGroup(self->createdTenants[tenantId].cluster);
		state std::map<Standalone<StringRef>, Optional<Value>> configurationParams = { { "tenant_group"_sr,
			                                                                             tenantGroup } };

		loop {
			try {
				wait(metacluster::configureTenant(
				    self->managementDb, tenantName, configurationParams, metacluster::IgnoreCapacityLimit::False));

				auto& tenantData = self->createdTenants[tenantId];

				TraceEvent(SevDebug, "MetaclusterRestoreWorkloadConfigureTenant")
				    .detail("Tenant", tenantName)
				    .detail("TenantId", tenantId)
				    .detail("OldTenantGroup", tenantData.tenantGroup)
				    .detail("NewTenantGroup", tenantGroup)
				    .detail("AccessTime", accessTime);

				if (tenantData.tenantGroup != tenantGroup) {
					auto& dataDb = self->dataDbs[tenantData.cluster];
					if (tenantData.tenantGroup.present()) {
						auto groupItr = self->tenantGroups.find(tenantData.tenantGroup.get());
						groupItr->second.tenants.erase(tenantId);
						if (groupItr->second.tenants.empty()) {
							self->tenantGroups.erase(groupItr);
							dataDb.tenantGroups.erase(tenantData.tenantGroup.get());
						}
					}

					if (tenantGroup.present()) {
						self->tenantGroups[tenantGroup.get()].tenants.insert(tenantId);
						dataDb.tenantGroups.insert(tenantGroup.get());
					}

					tenantData.tenantGroup = tenantGroup;
					tenantData.configureTime = accessTime;
				}
				return Void();
			} catch (Error& e) {
				if (e.code() != error_code_cluster_no_capacity) {
					throw;
				}

				wait(increaseMetaclusterCapacity(self));
			}
		}
	}

	ACTOR static Future<Void> renameTenant(MetaclusterRestoreWorkload* self, RestoreTenantData::AccessTime accessTime) {
		state TenantName oldTenantName;
		state TenantName newTenantName;
		for (int i = 0; i < 10; ++i) {
			oldTenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(oldTenantName) != 0) {
				break;
			}
		}
		for (int i = 0; i < 10; ++i) {
			newTenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(newTenantName) == 0) {
				break;
			}
		}

		if (self->tenantNameIndex.count(oldTenantName) == 0 || self->tenantNameIndex.count(newTenantName) != 0) {
			return Void();
		}

		state int64_t tenantId = self->tenantNameIndex[oldTenantName];

		TraceEvent(SevDebug, "MetaclusterRestoreWorkloadRenameTenant")
		    .detail("OldTenantName", oldTenantName)
		    .detail("NewTenantName", newTenantName)
		    .detail("TenantId", tenantId)
		    .detail("AccessTime", accessTime);
		wait(metacluster::renameTenant(self->managementDb, oldTenantName, newTenantName));

		RestoreTenantData& tenantData = self->createdTenants[tenantId];
		tenantData.name = newTenantName;
		tenantData.renameTime = accessTime;
		self->tenantNameIndex[newTenantName] = tenantId;
		self->tenantNameIndex.erase(oldTenantName);

		return Void();
	}

	ACTOR static Future<Void> runOperations(MetaclusterRestoreWorkload* self) {
		while (now() < self->endTime) {
			state int operation = deterministicRandom()->randomInt(0, 4);
			state RestoreTenantData::AccessTime accessTime = self->backupComplete
			                                                     ? RestoreTenantData::AccessTime::AFTER_BACKUP
			                                                     : RestoreTenantData::AccessTime::DURING_BACKUP;
			if (operation == 0) {
				wait(createTenant(self, accessTime));
			} else if (operation == 1) {
				wait(deleteTenant(self, accessTime));
			} else if (operation == 2) {
				wait(configureTenant(self, accessTime));
			} else if (operation == 3) {
				wait(renameTenant(self, accessTime));
			}
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}
	ACTOR static Future<Void> _start(Database cx, MetaclusterRestoreWorkload* self) {
		state std::set<ClusterName> clustersToRestore;

		TraceEvent("MetaclusterRestoreWorkloadStart")
		    .detail("RecoverManagementCluster", self->recoverManagementCluster)
		    .detail("RecoverDataClusters", self->recoverDataClusters);

		if (self->recoverDataClusters) {
			for (auto db : self->dataDbIndex) {
				if (deterministicRandom()->random01() < 0.1) {
					clustersToRestore.insert(db);
				}
			}

			if (clustersToRestore.empty()) {
				clustersToRestore.insert(deterministicRandom()->randomChoice(self->dataDbIndex));
			}

			for (auto c : clustersToRestore) {
				TraceEvent("MetaclusterRestoreWorkloadChoseClusterForRestore").detail("ClusterName", c);
			}
		}

		state Future<Void> opsFuture = runOperations(self);

		state std::map<ClusterName, Future<std::string>> backups;
		for (auto cluster : clustersToRestore) {
			backups[cluster] = backupCluster(cluster, self->dataDbs[cluster].db, self);
		}

		for (auto [_, f] : backups) {
			wait(success(f));
		}

		self->backupComplete = true;
		self->endTime = now() + self->postBackupDuration;

		wait(opsFuture);
		TraceEvent("MetaclusterRestoreWorkloadOperationsComplete");

		if (self->recoverManagementCluster) {
			wait(resetManagementCluster(self));
		} else {
			KeyBackedRangeResult<std::pair<int64_t, metacluster::MetaclusterTenantMapEntry>> tenants =
			    wait(runTransaction(self->managementDb, [](Reference<ITransaction> tr) {
				    return metacluster::metadata::management::tenantMetadata().tenantMap.getRange(
				        tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1);
			    }));
			ASSERT_LE(tenants.results.size(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
			self->managementTenantsBeforeRestore = tenants.results;
		}

		std::vector<Future<Void>> restores;
		for (auto [cluster, backupUrl] : backups) {
			restores.push_back(restoreDataCluster(cluster,
			                                      self->dataDbs[cluster].db,
			                                      backupUrl.get(),
			                                      !self->recoverManagementCluster,
			                                      metacluster::ForceJoin(deterministicRandom()->coinflip()),
			                                      backups.size(),
			                                      self));
		}

		wait(waitForAll(restores));

		if (self->recoverManagementCluster) {
			wait(restoreManagementCluster(self));

			if (deterministicRandom()->coinflip()) {
				std::vector<Future<Void>> secondRestores;
				for (auto [cluster, backupUrl] : backups) {
					secondRestores.push_back(restoreDataCluster(cluster,
					                                            self->dataDbs[cluster].db,
					                                            backupUrl.get(),
					                                            true,
					                                            metacluster::ForceJoin::True,
					                                            backups.size(),
					                                            self));
				}
				wait(waitForAll(secondRestores));
			}
		}

		return Void();
	}

	// Checks that the data cluster state matches our local state
	ACTOR static Future<Void> checkDataCluster(MetaclusterRestoreWorkload* self,
	                                           ClusterName clusterName,
	                                           DataClusterData clusterData) {
		state Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants;
		state Reference<ReadYourWritesTransaction> tr = clusterData.db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(
				    store(metaclusterRegistration, metacluster::metadata::metaclusterRegistration().get(tr)) &&
				    store(tenants,
				          TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
		ASSERT_LE(tenants.results.size(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);

		ASSERT(metaclusterRegistration.present() &&
		       metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);

		if (!clusterData.restored) {
			ASSERT_EQ(tenants.results.size(), clusterData.tenants.size());
			for (auto [tenantId, tenantEntry] : tenants.results) {
				ASSERT(clusterData.tenants.count(tenantId));
				auto tenantData = self->createdTenants[tenantId];
				ASSERT(tenantData.cluster == clusterName);
				ASSERT(tenantData.tenantGroup == tenantEntry.tenantGroup);
				ASSERT(tenantData.name == tenantEntry.tenantName);
			}
		} else {
			int expectedTenantCount = 0;
			std::map<int64_t, TenantMapEntry> tenantMap(tenants.results.begin(), tenants.results.end());
			for (auto tenantId : clusterData.tenants) {
				RestoreTenantData tenantData = self->createdTenants[tenantId];
				auto tenantItr = tenantMap.find(tenantId);
				if (tenantData.createTime == RestoreTenantData::AccessTime::BEFORE_BACKUP) {
					++expectedTenantCount;
					ASSERT(tenantItr != tenantMap.end());
					ASSERT(tenantData.cluster == clusterName);
					if (!self->recoverManagementCluster ||
					    tenantData.configureTime <= RestoreTenantData::AccessTime::BEFORE_BACKUP) {
						ASSERT(tenantItr->second.tenantGroup == tenantData.tenantGroup);
					}
					if (!self->recoverManagementCluster ||
					    tenantData.renameTime <= RestoreTenantData::AccessTime::BEFORE_BACKUP) {
						ASSERT(tenantItr->second.tenantName == tenantData.name);
					}
				} else if (tenantData.createTime == RestoreTenantData::AccessTime::AFTER_BACKUP) {
					ASSERT(tenantItr == tenantMap.end());
				} else if (tenantItr != tenantMap.end()) {
					++expectedTenantCount;
				}
			}

			// Check for deleted tenants that reappeared
			int unexpectedTenants = 0;
			for (auto const& [tenantId, tenantEntry] : tenantMap) {
				if (!clusterData.tenants.count(tenantId)) {
					ASSERT(self->recoverManagementCluster);
					ASSERT(self->deletedTenants.count(tenantId));
					++unexpectedTenants;
				}
			}

			ASSERT_EQ(tenantMap.size() - unexpectedTenants, expectedTenantCount);
		}

		return Void();
	}

	ACTOR static Future<Void> checkTenants(MetaclusterRestoreWorkload* self) {
		state KeyBackedRangeResult<std::pair<int64_t, metacluster::MetaclusterTenantMapEntry>> tenants =
		    wait(runTransaction(self->managementDb, [](Reference<ITransaction> tr) {
			    return metacluster::metadata::management::tenantMetadata().tenantMap.getRange(
			        tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1);
		    }));

		ASSERT_LE(tenants.results.size(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		std::map<int64_t, metacluster::MetaclusterTenantMapEntry> tenantMap(tenants.results.begin(),
		                                                                    tenants.results.end());

		// If we did not restore the management cluster, then every tenant present in the management cluster before the
		// restore should be present after the restore. All tenants in the management cluster should be unchanged except
		// for those tenants that were created after the backup and lost during the restore, which will be marked in an
		// error state.
		for (auto const& [tenantId, tenantEntry] : self->managementTenantsBeforeRestore) {
			auto itr = tenantMap.find(tenantId);
			ASSERT(itr != tenantMap.end());

			metacluster::MetaclusterTenantMapEntry postRecoveryEntry = itr->second;
			if (postRecoveryEntry.tenantState == metacluster::TenantState::ERROR) {
				ASSERT(self->dataDbs[itr->second.assignedCluster].restored);
				postRecoveryEntry.tenantState = tenantEntry.tenantState;
				postRecoveryEntry.error.clear();
			}

			ASSERT(tenantEntry == postRecoveryEntry);
		}

		if (!self->managementTenantsBeforeRestore.empty()) {
			ASSERT_EQ(self->managementTenantsBeforeRestore.size(), tenantMap.size());
		}

		for (auto const& [tenantId, tenantData] : self->createdTenants) {
			auto tenantItr = tenantMap.find(tenantId);
			if (tenantItr == tenantMap.end()) {
				// A tenant that we expected to have been created can only be missing from the management cluster if we
				// lost data in the process of recovering both the management and some data clusters
				ASSERT_NE(tenantData.createTime, RestoreTenantData::AccessTime::BEFORE_BACKUP);
				ASSERT(self->dataDbs[tenantData.cluster].restored && self->recoverManagementCluster);
			} else {
				if (tenantData.createTime != RestoreTenantData::AccessTime::BEFORE_BACKUP &&
				    self->dataDbs[tenantData.cluster].restored) {
					ASSERT(tenantItr->second.tenantState == metacluster::TenantState::ERROR ||
					       (tenantItr->second.tenantState == metacluster::TenantState::READY &&
					        tenantData.createTime == RestoreTenantData::AccessTime::DURING_BACKUP));
					if (tenantItr->second.tenantState == metacluster::TenantState::ERROR) {
						ASSERT(self->dataDbs[tenantData.cluster].restoreHasMessages);
					}
				} else {
					ASSERT_EQ(tenantItr->second.tenantState, metacluster::TenantState::READY);
				}
			}
		}

		// If we recovered both the management and some data clusters, we might undelete a tenant
		// Check that any unexpected tenants were deleted and that we had a potentially lossy recovery
		for (auto const& [tenantId, tenantEntry] : tenantMap) {
			if (!self->createdTenants.count(tenantId)) {
				ASSERT(self->deletedTenants.count(tenantId));
				ASSERT(self->recoverManagementCluster);
				ASSERT(self->recoverDataClusters);
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(this);
		} else {
			return true;
		}
	}
	ACTOR static Future<bool> _check(MetaclusterRestoreWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, metacluster::util::AllowPartialMetaclusterOperations::True);

		wait(metaclusterConsistencyCheck.run());

		std::vector<Future<Void>> dataClusterChecks;
		for (auto [clusterName, dataClusterData] : self->dataDbs) {
			dataClusterChecks.push_back(checkDataCluster(self, clusterName, dataClusterData));
		}
		wait(waitForAll(dataClusterChecks));
		wait(checkTenants(self));
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MetaclusterRestoreWorkload> MetaclusterRestoreWorkloadFactory;
