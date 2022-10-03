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
#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/MetaclusterConsistency.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterRestoreWorkload : TestWorkload {

	struct DataClusterData {
		Database db;
		std::set<TenantName> tenants;
		bool restored = false;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	struct TenantData {
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;
		bool beforeBackup = true;

		TenantData() {}
		TenantData(ClusterName cluster, Optional<TenantGroupName> tenantGroup, bool beforeBackup)
		  : cluster(cluster), tenantGroup(tenantGroup), beforeBackup(beforeBackup) {}
	};

	Reference<IDatabase> managementDb;
	std::map<ClusterName, DataClusterData> dataDbs;
	std::vector<ClusterName> dataDbIndex;

	std::map<TenantName, TenantData> createdTenants;

	int initialTenants;
	int maxTenants;
	int maxTenantGroups;
	int tenantGroupCapacity;

	MetaclusterRestoreWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		initialTenants = std::min<int>(maxTenants, getOption(options, "initialTenants"_sr, 100));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));

		tenantGroupCapacity = (initialTenants / 2 + maxTenantGroups - 1) / g_simulator->extraDatabases.size();
	}

	std::string description() const override { return "MetaclusterRestore"; }

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert("MachineAttritionWorkload");
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant%08d", deterministicRandom()->randomInt(0, maxTenants)));
		return tenant;
	}

	Optional<TenantGroupName> chooseTenantGroup() {
		Optional<TenantGroupName> tenantGroup;
		if (deterministicRandom()->coinflip()) {
			tenantGroup =
			    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
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
					DataClusterMetadata clusterMetadata =
					    wait(MetaclusterAPI::getClusterTransaction(tr, self->dataDbIndex[dbIndex]));
					DataClusterEntry updatedEntry = clusterMetadata.entry;
					updatedEntry.capacity.numTenantGroups = self->tenantGroupCapacity;
					MetaclusterAPI::updateClusterMetadata(
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
		fmt::print("Setup start\n");
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		fmt::print("Create thread safe handle\n");

		MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
		self->managementDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);
		wait(success(MetaclusterAPI::createMetacluster(self->managementDb, "management_cluster"_sr)));
		fmt::print("Create metacluster\n");

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

			DataClusterEntry clusterEntry;
			clusterEntry.capacity.numTenantGroups = self->tenantGroupCapacity;

			wait(MetaclusterAPI::registerCluster(self->managementDb, clusterName, ccs, clusterEntry));
			fmt::print("Register cluster {}\n", printable(clusterName));
		}

		while (self->createdTenants.size() < self->initialTenants) {
			wait(createTenant(self, true));
		}

		fmt::print("Setup complete\n");
		return Void();
	}

	ACTOR static Future<std::string> backupCluster(ClusterName clusterName,
	                                               Database dataDb,
	                                               MetaclusterRestoreWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<StringRef> backupContainer = "file://simfdb/backups/"_sr.withSuffix(clusterName);
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;

		addDefaultBackupRanges(backupRanges);

		fmt::print("Backup cluster start {}\n", printable(clusterName));

		try {
			wait(backupAgent.submitBackup(
			    dataDb, backupContainer, {}, 0, 0, clusterName.toString(), backupRanges, StopWhenDone::True));
		} catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		fmt::print("Backup submitted {}\n", printable(clusterName));

		state Reference<IBackupContainer> container;
		wait(success(backupAgent.waitBackup(dataDb, clusterName.toString(), StopWhenDone::True, &container)));
		fmt::print("Backup completed {} {}\n", printable(clusterName), container->getURL());
		return container->getURL();
	}

	ACTOR static Future<Void> restoreCluster(ClusterName clusterName,
	                                         Database dataDb,
	                                         std::string backupUrl,
	                                         MetaclusterRestoreWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		addDefaultBackupRanges(backupRanges);

		fmt::print("Restore cluster start {}\n", printable(clusterName));

		wait(runTransaction(dataDb.getReference(),
		                    [backupRanges = backupRanges](Reference<ReadYourWritesTransaction> tr) {
			                    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			                    for (auto range : backupRanges) {
				                    tr->clear(range);
			                    }
			                    return Future<Void>(Void());
		                    }));

		fmt::print("Restore cleared data {}\n", printable(clusterName));

		wait(success(backupAgent.restore(dataDb, dataDb, clusterName, StringRef(backupUrl), {}, backupRanges)));

		fmt::print("Restore cluster {}\n", printable(clusterName));

		wait(MetaclusterAPI::restoreCluster(self->managementDb,
		                                    clusterName,
		                                    dataDb->getConnectionRecord()->getConnectionString(),
		                                    AddNewTenants::False,
		                                    RemoveMissingTenants::True));

		self->dataDbs[clusterName].restored = true;

		fmt::print("Restore added back to metacluster {}\n", printable(clusterName));

		return Void();
	}

	ACTOR static Future<Void> createTenant(MetaclusterRestoreWorkload* self, bool beforeBackup) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->createdTenants.count(tenantName) == 0) {
				break;
			}
		}

		if (self->createdTenants.count(tenantName)) {
			return Void();
		}

		fmt::print("Create tenant {}\n", printable(tenantName));

		loop {
			try {
				TenantMapEntry tenantEntry;
				tenantEntry.tenantGroup = self->chooseTenantGroup();
				wait(MetaclusterAPI::createTenant(self->managementDb, tenantName, tenantEntry));
				fmt::print("Created tenant {}\n", printable(tenantName));
				TenantMapEntry createdEntry = wait(MetaclusterAPI::getTenant(self->managementDb, tenantName));
				self->createdTenants[tenantName] =
				    TenantData(createdEntry.assignedCluster.get(), createdEntry.tenantGroup, beforeBackup);
				self->dataDbs[createdEntry.assignedCluster.get()].tenants.insert(tenantName);
				return Void();
			} catch (Error& e) {
				fmt::print("Tenant create error {} {}\n", e.what(), printable(tenantName));
				if (e.code() != error_code_metacluster_no_capacity) {
					throw;
				}

				wait(increaseMetaclusterCapacity(self));
				fmt::print("Increased metacluster capacity {}\n", printable(tenantName));
			}
		}
	}

	ACTOR static Future<Void> deleteTenant(MetaclusterRestoreWorkload* self) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->createdTenants.count(tenantName) != 0) {
				break;
			}
		}

		if (self->createdTenants.count(tenantName) == 0) {
			return Void();
		}

		fmt::print("Delete tenant {}\n", printable(tenantName));

		loop {
			try {
				wait(MetaclusterAPI::deleteTenant(self->managementDb, tenantName));
				auto const& tenantData = self->createdTenants[tenantName];
				self->dataDbs[tenantData.cluster].tenants.erase(tenantName);
				self->createdTenants.erase(tenantName);
				return Void();
			} catch (Error& e) {
				fmt::print("Tenant create error {} {}\n", e.what(), printable(tenantName));
				if (e.code() != error_code_metacluster_no_capacity) {
					throw;
				}

				wait(increaseMetaclusterCapacity(self));
				fmt::print("Increased metacluster capacity {}\n", printable(tenantName));
			}
		}
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

		for (auto db : self->dataDbIndex) {
			if (deterministicRandom()->random01() < 0.1) {
				clustersToRestore.insert(db);
			}
		}

		if (clustersToRestore.empty()) {
			clustersToRestore.insert(deterministicRandom()->randomChoice(self->dataDbIndex));
		}

		// TODO: partially completed operations before backup

		state std::map<ClusterName, Future<std::string>> backups;
		for (auto cluster : clustersToRestore) {
			backups[cluster] = backupCluster(cluster, self->dataDbs[cluster].db, self);
		}

		for (auto [_, f] : backups) {
			wait(success(f));
		}

		// Make random tenant mutations
		state int tenantMutationNum;
		for (tenantMutationNum = 0; tenantMutationNum < 100; ++tenantMutationNum) {
			state int operation = deterministicRandom()->randomInt(0, 2);
			if (operation == 0) {
				wait(createTenant(self, false));
			} else if (operation == 1) {
				wait(deleteTenant(self));
				/*} else if (operation == 2) {
				    wait(configureTenant(self));
				} else if (operation == 3) {
				    wait(renameTenant(self));*/
			}
		}

		std::vector<Future<Void>> restores;
		for (auto [cluster, backupUrl] : backups) {
			restores.push_back(restoreCluster(cluster, self->dataDbs[cluster].db, backupUrl.get(), self));
		}

		wait(waitForAll(restores));

		return Void();
	}

	// Checks that the data cluster state matches our local state
	ACTOR static Future<Void> checkDataCluster(MetaclusterRestoreWorkload* self,
	                                           ClusterName clusterName,
	                                           DataClusterData clusterData) {
		state Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants;
		state Reference<ReadYourWritesTransaction> tr = clusterData.db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(
				    store(metaclusterRegistration,
				          MetaclusterMetadata::metaclusterRegistration().get(clusterData.db.getReference())) &&
				    store(tenants,
				          TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, clusterData.tenants.size() + 1)));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		ASSERT(metaclusterRegistration.present() &&
		       metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);

		if (!clusterData.restored) {
			ASSERT(tenants.size() == clusterData.tenants.size());
			for (auto [tenantName, tenantEntry] : tenants) {
				ASSERT(clusterData.tenants.count(tenantName));
				auto tenantData = self->createdTenants[tenantName];
				ASSERT(tenantData.cluster == clusterName);
				ASSERT(tenantData.tenantGroup == tenantEntry.tenantGroup);
			}
		} else {
			int expectedTenantCount = 0;
			std::map<TenantName, TenantMapEntry> tenantMap(tenants.begin(), tenants.end());
			for (auto tenantName : clusterData.tenants) {
				TenantData tenantData = self->createdTenants[tenantName];
				if (tenantData.beforeBackup) {
					fmt::print("Expected tenant: {}\n", printable(tenantName));
					++expectedTenantCount;
					auto tenantItr = tenantMap.find(tenantName);
					ASSERT(tenantItr != tenantMap.end());
					ASSERT(tenantData.cluster == clusterName);
					ASSERT(tenantItr->second.tenantGroup == tenantData.tenantGroup);
				} else {
					ASSERT(tenantMap.count(tenantName) == 0);
				}
			}

			fmt::print("Size check: {} {}\n", tenants.size(), expectedTenantCount);
			for (auto tenant : tenants) {
				fmt::print("Has tenant {}, {}\n",
				           printable(tenant.first),
				           clusterData.tenants.find(tenant.first) != clusterData.tenants.end());
			}
			ASSERT(tenants.size() == expectedTenantCount);
		}

		return Void();
	}

	ACTOR static Future<Void> checkTenants(MetaclusterRestoreWorkload* self) {
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants = wait(MetaclusterAPI::listTenants(
		    self->managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		ASSERT(tenants.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);

		std::map<TenantName, TenantMapEntry> tenantMap(tenants.begin(), tenants.end());
		for (auto& [tenantName, tenantData] : self->createdTenants) {
			TenantMapEntry const& entry = tenantMap[tenantName];
			if (!tenantData.beforeBackup && self->dataDbs[tenantData.cluster].restored) {
				ASSERT(entry.tenantState == TenantState::ERROR);
			} else {
				ASSERT(entry.tenantState == TenantState::READY);
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
		state MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, AllowPartialMetaclusterOperations::False);

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

WorkloadFactory<MetaclusterRestoreWorkload> MetaclusterRestoreWorkloadFactory("MetaclusterRestore");
