/*
 * MetaclusterManagementWorkload.actor.cpp
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
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/MetaclusterConsistency.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/BooleanParam.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DEFINE_BOOLEAN_PARAM(AllowPartialMetaclusterOperations);

struct MetaclusterManagementWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterManagement";

	struct TenantTestData : ReferenceCounted<TenantTestData> {
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;

		TenantTestData() {}
		TenantTestData(ClusterName cluster, Optional<TenantGroupName> tenantGroup)
		  : cluster(cluster), tenantGroup(tenantGroup) {}
	};

	struct TenantGroupData : ReferenceCounted<TenantGroupData> {
		ClusterName cluster;
		std::set<TenantName> tenants;

		TenantGroupData() {}
		TenantGroupData(ClusterName cluster) : cluster(cluster) {}
	};

	struct DataClusterData : ReferenceCounted<DataClusterData> {
		Database db;
		bool registered = false;
		bool detached = false;
		int tenantGroupCapacity = 0;

		std::map<TenantName, Reference<TenantTestData>> tenants;
		std::map<TenantGroupName, Reference<TenantGroupData>> tenantGroups;
		std::set<TenantName> ungroupedTenants;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	Reference<IDatabase> managementDb;
	std::map<ClusterName, Reference<DataClusterData>> dataDbs;
	std::map<TenantGroupName, Reference<TenantGroupData>> tenantGroups;
	std::set<TenantName> ungroupedTenants;
	std::vector<ClusterName> dataDbIndex;

	int64_t totalTenantGroupCapacity = 0;
	std::map<TenantName, Reference<TenantTestData>> createdTenants;

	int maxTenants;
	int maxTenantGroups;
	int64_t tenantIdPrefix;
	double testDuration;

	MetaclusterManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		tenantIdPrefix = getOption(options,
		                           "tenantIdPrefix"_sr,
		                           deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                                            TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1));
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			if (g_network->isSimulated() && BUGGIFY) {
				IKnobCollection::getMutableGlobalKnobCollection().setKnob(
				    "max_tenants_per_cluster", KnobValueRef::create(int{ deterministicRandom()->randomInt(20, 100) }));
			}
			return _setup(cx, this);
		} else {
			return Void();
		}
	}
	ACTOR static Future<Void> _setup(Database cx, MetaclusterManagementWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));

		MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
		self->managementDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

		ASSERT(g_simulator->extraDatabases.size() > 0);
		for (auto connectionString : g_simulator->extraDatabases) {
			ClusterConnectionString ccs(connectionString);
			self->dataDbIndex.push_back(ClusterName(format("cluster_%08d", self->dataDbs.size())));
			self->dataDbs[self->dataDbIndex.back()] = makeReference<DataClusterData>(
			    Database::createSimulatedExtraDatabase(connectionString, cx->defaultTenant));
		}
		wait(success(MetaclusterAPI::createMetacluster(
		    cx.getReference(), "management_cluster"_sr, self->tenantIdPrefix, false)));
		return Void();
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

	ACTOR static Future<Void> registerCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state bool retried = false;

		try {
			state DataClusterEntry entry;
			entry.capacity.numTenantGroups = deterministicRandom()->randomInt(0, 4);

			loop {
				try {
					Future<Void> registerFuture =
					    MetaclusterAPI::registerCluster(self->managementDb,
					                                    clusterName,
					                                    dataDb->db->getConnectionRecord()->getConnectionString(),
					                                    entry);

					Optional<Void> result = wait(timeout(registerFuture, deterministicRandom()->randomInt(1, 30)));
					if (result.present()) {
						break;
					} else {
						retried = true;
					}
				} catch (Error& e) {
					state Error registerError = e;
					if (registerError.code() == error_code_cluster_removed ||
					    registerError.code() == error_code_cluster_not_empty) {
						if (registerError.code() == error_code_cluster_removed) {
							ASSERT(retried);
						} else if (registerError.code() == error_code_cluster_not_empty) {
							ASSERT(dataDb->detached);
						}

						wait(success(errorOr(MetaclusterAPI::removeCluster(
						    self->managementDb, clusterName, ClusterType::METACLUSTER_MANAGEMENT, ForceRemove::True))));

						return Void();
					} else if (registerError.code() == error_code_cluster_already_exists && retried &&
					           !dataDb->registered) {
						Optional<DataClusterMetadata> clusterMetadata =
						    wait(MetaclusterAPI::tryGetCluster(self->managementDb, clusterName));
						ASSERT(clusterMetadata.present());
						break;
					} else {
						throw registerError;
					}
				}
			}

			ASSERT(!dataDb->registered);
			ASSERT(!dataDb->detached || dataDb->tenants.empty());

			dataDb->tenantGroupCapacity = entry.capacity.numTenantGroups;
			self->totalTenantGroupCapacity += entry.capacity.numTenantGroups;
			dataDb->registered = true;
			dataDb->detached = false;

			// Get a version to know that the cluster has recovered
			wait(success(runTransaction(dataDb->db.getReference(),
			                            [](Reference<ReadYourWritesTransaction> tr) { return tr->getReadVersion(); })));
		} catch (Error& e) {
			if (e.code() == error_code_cluster_already_exists) {
				ASSERT(dataDb->registered);
				return Void();
			} else if (e.code() == error_code_cluster_not_empty) {
				ASSERT(dataDb->detached && !dataDb->tenants.empty());
				return Void();
			}

			TraceEvent(SevError, "RegisterClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> removeCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state bool retried = false;
		state ForceRemove detachCluster = ForceRemove(deterministicRandom()->coinflip());

		try {
			loop {
				Future<bool> removeFuture = MetaclusterAPI::removeCluster(
				    self->managementDb, clusterName, ClusterType::METACLUSTER_MANAGEMENT, ForceRemove(detachCluster));
				try {
					Optional<bool> result = wait(timeout(removeFuture, deterministicRandom()->randomInt(1, 30)));
					if (result.present()) {
						ASSERT(result.get());
						break;
					} else {
						retried = true;
					}
				} catch (Error& e) {
					if (e.code() == error_code_cluster_not_found && retried && dataDb->registered) {
						Optional<DataClusterMetadata> clusterMetadata =
						    wait(MetaclusterAPI::tryGetCluster(self->managementDb, clusterName));

						ASSERT(!clusterMetadata.present());
						break;
					} else {
						throw;
					}
				}
			}

			ASSERT(dataDb->registered);
			ASSERT(detachCluster || dataDb->tenants.empty());

			self->totalTenantGroupCapacity -= std::max<int64_t>(
			    dataDb->tenantGroups.size() + dataDb->ungroupedTenants.size(), dataDb->tenantGroupCapacity);
			dataDb->tenantGroupCapacity = 0;
			dataDb->registered = false;

			if (detachCluster) {
				dataDb->detached = true;
				for (auto const& t : dataDb->ungroupedTenants) {
					self->ungroupedTenants.erase(t);
				}
				for (auto const& t : dataDb->tenantGroups) {
					self->tenantGroups.erase(t.first);
				}
				for (auto const& t : dataDb->tenants) {
					self->createdTenants.erase(t.first);
				}
			}

			// Get a version to know that the cluster has recovered
			wait(success(runTransaction(dataDb->db.getReference(),
			                            [](Reference<ReadYourWritesTransaction> tr) { return tr->getReadVersion(); })));
		} catch (Error& e) {
			if (e.code() == error_code_cluster_not_found) {
				ASSERT(!dataDb->registered);
				return Void();
			} else if (e.code() == error_code_cluster_not_empty && !detachCluster) {
				ASSERT(!dataDb->tenants.empty());
				return Void();
			}

			TraceEvent(SevError, "RemoveClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> removeFailedRestoredCluster(MetaclusterManagementWorkload* self,
	                                                      ClusterName clusterName) {
		// On retries, we need to remove the cluster if it was partially added
		try {
			wait(success(MetaclusterAPI::removeCluster(
			    self->managementDb, clusterName, ClusterType::METACLUSTER_MANAGEMENT, ForceRemove::True)));
		} catch (Error& e) {
			if (e.code() != error_code_cluster_not_found) {
				throw;
			}
		}

		return Void();
	}

	ACTOR static Future<Void> resolveCollisions(MetaclusterManagementWorkload* self,
	                                            ClusterName clusterName,
	                                            Reference<DataClusterData> dataDb) {
		state std::set<TenantName> tenantsToRemove;

		state bool foundTenantCollision = false;
		for (auto t : dataDb->tenants) {
			if (self->createdTenants.count(t.first)) {
				foundTenantCollision = true;
				tenantsToRemove.insert(t.first);
			}
		}

		state bool foundGroupCollision = false;
		for (auto t : dataDb->tenantGroups) {
			if (self->tenantGroups.count(t.first)) {
				foundGroupCollision = true;
				tenantsToRemove.insert(t.second->tenants.begin(), t.second->tenants.end());
			}
		}

		state Reference<ReadYourWritesTransaction> tr = dataDb->db->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state std::vector<Future<Optional<int64_t>>> getFutures;
				for (auto const& t : tenantsToRemove) {
					getFutures.push_back(TenantMetadata::tenantNameIndex().get(tr, t));
					auto tenantItr = dataDb->tenants.find(t);
					if (tenantItr != dataDb->tenants.end()) {
						if (tenantItr->second->tenantGroup.present()) {
							auto groupItr = dataDb->tenantGroups.find(tenantItr->second->tenantGroup.get());
							ASSERT(groupItr != dataDb->tenantGroups.end());
							groupItr->second->tenants.erase(t);
							if (groupItr->second->tenants.empty()) {
								dataDb->tenantGroups.erase(groupItr);
							}
						}
						dataDb->tenants.erase(tenantItr);
					}
					dataDb->ungroupedTenants.erase(t);
				}

				wait(waitForAll(getFutures));

				state std::vector<Future<Void>> deleteFutures;
				for (auto const& f : getFutures) {
					if (f.get().present()) {
						deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, f.get().get()));
					}
				}

				wait(waitForAll(deleteFutures));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		ASSERT(foundTenantCollision || foundGroupCollision);
		return Void();
	}

	ACTOR static Future<Void> restoreCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state bool dryRun = deterministicRandom()->coinflip();
		state bool forceJoin = deterministicRandom()->coinflip();

		state std::vector<std::string> messages;
		state bool retried = false;
		loop {
			try {
				if (dataDb->detached) {
					if (retried) {
						wait(removeFailedRestoredCluster(self, clusterName));
					} else {
						// On the first try, we need to remove the metacluster registration entry from the data
						// cluster
						wait(success(MetaclusterAPI::removeCluster(
						    dataDb->db.getReference(), clusterName, ClusterType::METACLUSTER_DATA, ForceRemove::True)));
					}
				}

				Future<Void> restoreFuture =
				    MetaclusterAPI::restoreCluster(self->managementDb,
				                                   clusterName,
				                                   dataDb->db->getConnectionRecord()->getConnectionString(),
				                                   ApplyManagementClusterUpdates(!dataDb->detached),
				                                   RestoreDryRun(dryRun),
				                                   ForceJoin(forceJoin),
				                                   &messages);
				Optional<Void> result = wait(timeout(restoreFuture, deterministicRandom()->randomInt(1, 30)));
				if (!result.present()) {
					retried = true;
					continue;
				}

				ASSERT(dataDb->registered || dataDb->detached);
				if (dataDb->detached && !dryRun) {
					dataDb->detached = false;
					dataDb->registered = true;
					for (auto const& t : dataDb->ungroupedTenants) {
						self->ungroupedTenants.insert(t);
					}
					for (auto const& t : dataDb->tenantGroups) {
						ASSERT(self->tenantGroups.try_emplace(t.first, t.second).second);
					}
					for (auto const& t : dataDb->tenants) {
						ASSERT(self->createdTenants.try_emplace(t.first, t.second).second);
						ASSERT(self->createdTenants[t.first]->cluster == clusterName);
					}

					self->totalTenantGroupCapacity += dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size();
				}

				break;
			} catch (Error& e) {
				state Error error = e;
				if (error.code() == error_code_conflicting_restore ||
				    error.code() == error_code_cluster_already_exists) {
					ASSERT(retried);
					CODE_PROBE(true, "MetaclusterManagementWorkload: timed out restore conflicts with retried restore");
					continue;
				} else if (error.code() == error_code_cluster_not_found) {
					ASSERT(!dataDb->registered);
					return Void();
				} else if (error.code() == error_code_tenant_already_exists ||
				           error.code() == error_code_invalid_tenant_configuration) {
					ASSERT(dataDb->detached);
					wait(removeFailedRestoredCluster(self, clusterName));
					wait(resolveCollisions(self, clusterName, dataDb));
					continue;
				}
				TraceEvent(SevError, "RestoreClusterFailure").error(error).detail("ClusterName", clusterName);
				ASSERT(false);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> listClusters(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName1 = self->chooseClusterName();
		state ClusterName clusterName2 = self->chooseClusterName();
		state int limit = deterministicRandom()->randomInt(1, self->dataDbs.size() + 1);

		try {
			std::map<ClusterName, DataClusterMetadata> clusterList =
			    wait(MetaclusterAPI::listClusters(self->managementDb, clusterName1, clusterName2, limit));

			ASSERT(clusterName1 <= clusterName2);

			auto resultItr = clusterList.begin();

			int count = 0;
			for (auto localItr = self->dataDbs.find(clusterName1);
			     localItr != self->dataDbs.find(clusterName2) && count < limit;
			     ++localItr) {
				if (localItr->second->registered) {
					ASSERT(resultItr != clusterList.end());
					ASSERT(resultItr->first == localItr->first);
					ASSERT(resultItr->second.connectionString ==
					       localItr->second->db->getConnectionRecord()->getConnectionString());
					++resultItr;
					++count;
				}
			}

			ASSERT(resultItr == clusterList.end());
		} catch (Error& e) {
			if (e.code() == error_code_inverted_range) {
				ASSERT(clusterName1 > clusterName2);
				return Void();
			}
			TraceEvent(SevError, "ListClustersFailure")
			    .error(e)
			    .detail("BeginClusterName", clusterName1)
			    .detail("EndClusterName", clusterName2)
			    .detail("Limit", limit);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> getCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];

		try {
			DataClusterMetadata clusterMetadata = wait(MetaclusterAPI::getCluster(self->managementDb, clusterName));
			ASSERT(dataDb->registered);
			ASSERT(dataDb->db->getConnectionRecord()->getConnectionString() == clusterMetadata.connectionString);
		} catch (Error& e) {
			if (e.code() == error_code_cluster_not_found) {
				ASSERT(!dataDb->registered);
				return Void();
			}
			TraceEvent(SevError, "GetClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Optional<DataClusterEntry>> configureImpl(MetaclusterManagementWorkload* self,
	                                                              ClusterName clusterName,
	                                                              Reference<DataClusterData> dataDb,
	                                                              Optional<int64_t> numTenantGroups,
	                                                              Optional<ClusterConnectionString> connectionString) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<DataClusterMetadata> clusterMetadata =
				    wait(MetaclusterAPI::tryGetClusterTransaction(tr, clusterName));
				state Optional<DataClusterEntry> entry;

				if (clusterMetadata.present()) {
					if (numTenantGroups.present()) {
						entry = clusterMetadata.get().entry;
						entry.get().capacity.numTenantGroups = numTenantGroups.get();
					}
					MetaclusterAPI::updateClusterMetadata(
					    tr, clusterName, clusterMetadata.get(), connectionString, entry);

					wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				}

				return entry;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> configureCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state Optional<DataClusterEntry> updatedEntry;

		state Optional<int64_t> newNumTenantGroups;
		state Optional<ClusterConnectionString> connectionString;
		if (deterministicRandom()->coinflip()) {
			newNumTenantGroups = deterministicRandom()->randomInt(0, 4);
		}
		if (deterministicRandom()->coinflip()) {
			connectionString = dataDb->db->getConnectionRecord()->getConnectionString();
		}

		try {
			loop {
				Optional<Optional<DataClusterEntry>> result =
				    wait(timeout(configureImpl(self, clusterName, dataDb, newNumTenantGroups, connectionString),
				                 deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					updatedEntry = result.get();
					break;
				}
			}

			if (updatedEntry.present()) {
				int allocatedGroups = dataDb->tenantGroups.size() + dataDb->ungroupedTenants.size();
				int64_t tenantGroupDelta =
				    std::max<int64_t>(updatedEntry.get().capacity.numTenantGroups, allocatedGroups) -
				    std::max<int64_t>(dataDb->tenantGroupCapacity, allocatedGroups);

				self->totalTenantGroupCapacity += tenantGroupDelta;
				dataDb->tenantGroupCapacity = updatedEntry.get().capacity.numTenantGroups;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "ConfigureClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> verifyListFilter(MetaclusterManagementWorkload* self,
	                                           TenantName tenant,
	                                           const char* context) {
		try {
			state MetaclusterTenantMapEntry checkEntry = wait(MetaclusterAPI::getTenant(self->managementDb, tenant));
			state MetaclusterAPI::TenantState checkState = checkEntry.tenantState;
			state std::vector<MetaclusterAPI::TenantState> filters;
			filters.push_back(checkState);

			state std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> tenantList =
			    wait(MetaclusterAPI::listTenantMetadata(self->managementDb, ""_sr, "\xff\xff"_sr, 10e6, 0, filters));
			// Possible to have changed state between now and the getTenant call above
			state MetaclusterTenantMapEntry checkEntry2 = wait(MetaclusterAPI::getTenant(self->managementDb, tenant));
			DisabledTraceEvent(SevDebug, "VerifyListFilter")
			    .detail("Context", context)
			    .detail("Tenant", tenant)
			    .detail("CheckState", (int)checkState)
			    .detail("Entry2State", (int)checkEntry2.tenantState);

			bool found = false;
			for (auto pair : tenantList) {
				ASSERT(pair.second.tenantState == checkState);
				if (pair.first == tenant) {
					found = true;
				}
			}
			ASSERT(found || checkEntry2.tenantState != checkState);
		} catch (Error& e) {
			if (e.code() != error_code_tenant_not_found) {
				TraceEvent(SevError, "VerifyListFilterFailure").error(e).detail("Tenant", tenant);
				throw;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> createTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state Optional<TenantGroupName> tenantGroup = self->chooseTenantGroup();
		state AssignClusterAutomatically assignClusterAutomatically(deterministicRandom()->coinflip());

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state bool tenantGroupExists = tenantGroup.present() && self->tenantGroups.count(tenantGroup.get());
		state bool hasCapacity = tenantGroupExists || self->ungroupedTenants.size() + self->tenantGroups.size() <
		                                                  self->totalTenantGroupCapacity;
		state bool retried = false;

		state MetaclusterTenantMapEntry tenantMapEntry;
		tenantMapEntry.tenantName = tenant;
		tenantMapEntry.tenantGroup = tenantGroup;

		// Choose between two preferred clusters because if we get a partial completion and
		// retry, we want the operation to eventually succeed instead of having a chance of
		// never re-visiting the original preferred cluster.
		state std::vector<ClusterName> preferredClusters;
		state int preferredClusterIndex = 0;
		if (!assignClusterAutomatically) {
			preferredClusters.push_back(self->chooseClusterName());
			preferredClusters.push_back(self->chooseClusterName());
			tenantMapEntry.assignedCluster = preferredClusters[preferredClusterIndex];
		}

		try {
			loop {
				try {
					if (!assignClusterAutomatically && deterministicRandom()->coinflip()) {
						preferredClusterIndex = deterministicRandom()->randomInt(0, preferredClusters.size());
						tenantMapEntry.assignedCluster = preferredClusters[preferredClusterIndex];
					}
					Future<Void> createFuture =
					    MetaclusterAPI::createTenant(self->managementDb, tenantMapEntry, assignClusterAutomatically);
					Optional<Void> result = wait(timeout(createFuture, deterministicRandom()->randomInt(1, 30)));
					if (result.present()) {
						break;
					} else {
						retried = true;
						wait(verifyListFilter(self, tenant, "createTenant"));
					}
				} catch (Error& e) {
					if (e.code() == error_code_tenant_already_exists && retried && !exists) {
						Optional<MetaclusterTenantMapEntry> entry =
						    wait(MetaclusterAPI::tryGetTenant(self->managementDb, tenant));
						ASSERT(entry.present());
						tenantMapEntry = entry.get();
						break;
					} else if (!assignClusterAutomatically && (e.code() == error_code_cluster_no_capacity ||
					                                           e.code() == error_code_cluster_not_found ||
					                                           e.code() == error_code_invalid_tenant_configuration)) {
						state Error error = e;
						Optional<MetaclusterTenantMapEntry> entry =
						    wait(MetaclusterAPI::tryGetTenant(self->managementDb, tenant));

						// When picking a different assigned cluster, it is possible to leave the
						// tenant creation in a partially completed state, which we want to avoid.
						// Continue retrying if the new preferred cluster throws errors rather than
						// exiting immediately so we can allow the operation to finish.
						if (preferredClusters.size() > 1 &&
						    (!entry.present() || entry.get().assignedCluster != tenantMapEntry.assignedCluster)) {
							preferredClusters.erase(preferredClusters.begin() + preferredClusterIndex);
							preferredClusterIndex = 0;
							tenantMapEntry.assignedCluster = preferredClusters[preferredClusterIndex];

							continue;
						}

						throw error;
					} else {
						throw;
					}
				}
			}

			MetaclusterTenantMapEntry entry = wait(MetaclusterAPI::getTenant(self->managementDb, tenant));

			ASSERT(!exists);
			ASSERT(hasCapacity);
			ASSERT(entry.tenantGroup == tenantGroup);
			ASSERT(TenantAPI::getTenantIdPrefix(entry.id) == self->tenantIdPrefix);

			Reference<TenantTestData> tenantData = makeReference<TenantTestData>(entry.assignedCluster, tenantGroup);
			self->createdTenants[tenant] = tenantData;

			auto assignedCluster = self->dataDbs.find(entry.assignedCluster);
			ASSERT(assignClusterAutomatically || tenantMapEntry.assignedCluster == assignedCluster->first);
			ASSERT(assignedCluster != self->dataDbs.end());
			ASSERT(assignedCluster->second->tenants.try_emplace(tenant, tenantData).second);

			if (tenantGroup.present()) {
				auto tenantGroupData =
				    self->tenantGroups
				        .try_emplace(tenantGroup.get(), makeReference<TenantGroupData>(entry.assignedCluster))
				        .first;
				ASSERT(tenantGroupData->second->cluster == entry.assignedCluster);
				tenantGroupData->second->tenants.insert(tenant);
				assignedCluster->second->tenantGroups[tenantGroup.get()] = tenantGroupData->second;
			} else {
				self->ungroupedTenants.insert(tenant);
				assignedCluster->second->ungroupedTenants.insert(tenant);
			}

			ASSERT(tenantGroupExists ||
			       assignedCluster->second->tenantGroupCapacity >=
			           assignedCluster->second->tenantGroups.size() + assignedCluster->second->ungroupedTenants.size());
		} catch (Error& e) {
			if (e.code() == error_code_tenant_already_exists) {
				ASSERT(exists);
				return Void();
			} else if (e.code() == error_code_metacluster_no_capacity) {
				ASSERT(!hasCapacity && !exists);
				return Void();
			} else if (e.code() == error_code_cluster_no_capacity) {
				ASSERT(!assignClusterAutomatically);
				return Void();
			} else if (e.code() == error_code_cluster_not_found) {
				ASSERT(!assignClusterAutomatically);
				return Void();
			} else if (e.code() == error_code_invalid_tenant_configuration) {
				ASSERT(tenantGroup.present());
				auto itr = self->tenantGroups.find(tenantGroup.get());
				ASSERT(itr != self->tenantGroups.end());
				ASSERT(itr->second->cluster != tenantMapEntry.assignedCluster);
				return Void();
			}

			TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state bool retried = false;

		try {
			loop {
				try {
					Future<Void> deleteFuture = MetaclusterAPI::deleteTenant(self->managementDb, tenant);
					Optional<Void> result = wait(timeout(deleteFuture, deterministicRandom()->randomInt(1, 30)));

					if (result.present()) {
						break;
					} else {
						retried = true;
						wait(verifyListFilter(self, tenant, "deleteTenant"));
					}
				} catch (Error& e) {
					if (e.code() == error_code_tenant_not_found && retried && exists) {
						Optional<MetaclusterTenantMapEntry> entry =
						    wait(MetaclusterAPI::tryGetTenant(self->managementDb, tenant));
						ASSERT(!entry.present());
						break;
					} else {
						throw;
					}
				}
			}

			ASSERT(exists);
			auto tenantData = self->createdTenants.find(tenant);
			ASSERT(tenantData != self->createdTenants.end());

			bool erasedTenantGroup = false;
			if (tenantData->second->tenantGroup.present()) {
				auto tenantGroupData = self->tenantGroups.find(tenantData->second->tenantGroup.get());
				ASSERT(tenantGroupData != self->tenantGroups.end());
				tenantGroupData->second->tenants.erase(tenant);
				if (tenantGroupData->second->tenants.empty()) {
					erasedTenantGroup = true;
					self->tenantGroups.erase(tenantData->second->tenantGroup.get());
				}
			} else {
				self->ungroupedTenants.erase(tenant);
			}

			auto dataDb = self->dataDbs[tenantData->second->cluster];
			ASSERT(dataDb->registered);
			auto tenantItr = dataDb->tenants.find(tenant);
			ASSERT(tenantItr != dataDb->tenants.end());

			bool reducedAllocatedCount = false;
			if (erasedTenantGroup) {
				reducedAllocatedCount = true;
				dataDb->tenantGroups.erase(tenantData->second->tenantGroup.get());
			} else if (!tenantData->second->tenantGroup.present()) {
				reducedAllocatedCount = true;
				dataDb->ungroupedTenants.erase(tenant);
			}

			if (reducedAllocatedCount &&
			    dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size() >= dataDb->tenantGroupCapacity) {
				--self->totalTenantGroupCapacity;
			}
			dataDb->tenants.erase(tenantItr);
			self->createdTenants.erase(tenant);
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			}

			TraceEvent(SevError, "DeleteTenantFailure").error(e).detail("TenantName", tenant);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> configureTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state Optional<TenantGroupName> newTenantGroup = self->chooseTenantGroup();
		state IgnoreCapacityLimit ignoreCapacityLimit = IgnoreCapacityLimit(deterministicRandom()->coinflip());

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state bool tenantGroupExists =
		    newTenantGroup.present() && self->tenantGroups.find(newTenantGroup.get()) != self->tenantGroups.end();

		state bool hasCapacity = false;
		state Optional<ClusterName> oldClusterName;
		if (exists) {
			auto& dataDb = self->dataDbs[itr->second->cluster];
			hasCapacity = dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size() < dataDb->tenantGroupCapacity;
			oldClusterName = itr->second->cluster;
		}

		state Optional<ClusterName> newClusterName = oldClusterName;
		if (deterministicRandom()->coinflip()) {
			newClusterName = self->chooseClusterName();
		}

		state std::map<Standalone<StringRef>, Optional<Value>> configurationParameters = {
			{ "assigned_cluster"_sr, newClusterName }, { "tenant_group"_sr, newTenantGroup }
		};

		try {
			loop {
				Future<Void> configureFuture = MetaclusterAPI::configureTenant(
				    self->managementDb, tenant, configurationParameters, ignoreCapacityLimit);
				Optional<Void> result = wait(timeout(configureFuture, deterministicRandom()->randomInt(1, 30)));

				if (result.present()) {
					break;
				}
				wait(verifyListFilter(self, tenant, "configureTenant"));
			}

			ASSERT(exists);
			auto tenantData = self->createdTenants.find(tenant);
			ASSERT(tenantData != self->createdTenants.end());

			auto& dataDb = self->dataDbs[tenantData->second->cluster];
			ASSERT(dataDb->registered);

			bool allocationRemoved = false;
			bool allocationAdded = false;
			if (tenantData->second->tenantGroup != newTenantGroup) {
				if (tenantData->second->tenantGroup.present()) {
					auto& tenantGroupData = self->tenantGroups[tenantData->second->tenantGroup.get()];
					tenantGroupData->tenants.erase(tenant);
					if (tenantGroupData->tenants.empty()) {
						allocationRemoved = true;
						self->tenantGroups.erase(tenantData->second->tenantGroup.get());
						dataDb->tenantGroups.erase(tenantData->second->tenantGroup.get());
					}
				} else {
					allocationRemoved = true;
					self->ungroupedTenants.erase(tenant);
					dataDb->ungroupedTenants.erase(tenant);
				}

				if (newTenantGroup.present()) {
					auto [tenantGroupData, inserted] = self->tenantGroups.try_emplace(
					    newTenantGroup.get(), makeReference<TenantGroupData>(tenantData->second->cluster));
					tenantGroupData->second->tenants.insert(tenant);
					if (inserted) {
						allocationAdded = true;
						ASSERT(dataDb->tenantGroups.try_emplace(newTenantGroup.get(), tenantGroupData->second).second);
					}
				} else {
					allocationAdded = true;
					self->ungroupedTenants.insert(tenant);
					dataDb->ungroupedTenants.insert(tenant);
				}

				tenantData->second->tenantGroup = newTenantGroup;

				if (allocationAdded && !allocationRemoved) {
					ASSERT(ignoreCapacityLimit || hasCapacity);
					if (!hasCapacity) {
						++self->totalTenantGroupCapacity;
					}
				} else if (allocationRemoved && !allocationAdded &&
				           dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size() >=
				               dataDb->tenantGroupCapacity) {
					--self->totalTenantGroupCapacity;
				}
			}
			ASSERT(oldClusterName == newClusterName);
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			} else if (e.code() == error_code_cluster_no_capacity) {
				ASSERT(exists && !hasCapacity && !ignoreCapacityLimit);
				return Void();
			} else if (e.code() == error_code_invalid_tenant_configuration) {
				ASSERT(exists);
				if (oldClusterName == newClusterName) {
					ASSERT(tenantGroupExists &&
					       self->createdTenants[tenant]->cluster != self->tenantGroups[newTenantGroup.get()]->cluster);
				}
				return Void();
			}

			TraceEvent(SevError, "ConfigureTenantFailure")
			    .error(e)
			    .detail("TenantName", tenant)
			    .detail("TenantGroup", newTenantGroup);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> renameTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state TenantName newTenantName = self->chooseTenantName();

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();

		itr = self->createdTenants.find(newTenantName);
		state bool newTenantExists = itr != self->createdTenants.end();

		try {
			state bool retried = false;
			loop {
				try {
					Future<Void> renameFuture = MetaclusterAPI::renameTenant(self->managementDb, tenant, newTenantName);
					Optional<Void> result = wait(timeout(renameFuture, deterministicRandom()->randomInt(1, 30)));

					if (result.present()) {
						break;
					}

					retried = true;
					wait(verifyListFilter(self, tenant, "renameTenant"));
				} catch (Error& e) {
					// If we retry the rename after it had succeeded, we will get an error that we should ignore
					if (e.code() == error_code_tenant_not_found && exists && !newTenantExists && retried) {
						break;
					}
					throw e;
				}
			}
			wait(verifyListFilter(self, newTenantName, "renameTenantNew"));

			ASSERT(exists);
			ASSERT(!newTenantExists);

			Optional<MetaclusterTenantMapEntry> oldEntry =
			    wait(MetaclusterAPI::tryGetTenant(self->managementDb, tenant));
			ASSERT(!oldEntry.present());

			MetaclusterTenantMapEntry newEntry = wait(MetaclusterAPI::getTenant(self->managementDb, newTenantName));

			auto tenantDataItr = self->createdTenants.find(tenant);
			ASSERT(tenantDataItr != self->createdTenants.end());

			Reference<TenantTestData> tenantData = tenantDataItr->second;
			ASSERT(tenantData->tenantGroup == newEntry.tenantGroup);
			ASSERT(tenantData->cluster == newEntry.assignedCluster);

			self->createdTenants[newTenantName] = tenantData;
			self->createdTenants.erase(tenantDataItr);

			auto& dataDb = self->dataDbs[newEntry.assignedCluster];
			ASSERT(dataDb->registered);

			dataDb->tenants.erase(tenant);
			dataDb->tenants[newTenantName] = tenantData;

			if (newEntry.tenantGroup.present()) {
				auto& tenantGroup = self->tenantGroups[newEntry.tenantGroup.get()];
				tenantGroup->tenants.erase(tenant);
				tenantGroup->tenants.insert(newTenantName);
			} else {
				dataDb->ungroupedTenants.erase(tenant);
				dataDb->ungroupedTenants.insert(newTenantName);
				self->ungroupedTenants.erase(tenant);
				self->ungroupedTenants.insert(newTenantName);
			}
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			} else if (e.code() == error_code_tenant_already_exists) {
				ASSERT(newTenantExists);
				return Void();
			}

			TraceEvent(SevError, "RenameTenantFailure")
			    .error(e)
			    .detail("OldTenantName", tenant)
			    .detail("NewTenantName", newTenantName);
			ASSERT(false);
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
	ACTOR static Future<Void> _start(Database cx, MetaclusterManagementWorkload* self) {
		state double start = now();

		// Run a random sequence of operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 10);
			if (operation == 0) {
				wait(registerCluster(self));
			} else if (operation == 1) {
				wait(removeCluster(self));
			} else if (operation == 2) {
				wait(listClusters(self));
			} else if (operation == 3) {
				wait(getCluster(self));
			} else if (operation == 4) {
				wait(configureCluster(self));
			} else if (operation == 5) {
				wait(createTenant(self));
			} else if (operation == 6) {
				wait(deleteTenant(self));
			} else if (operation == 7) {
				wait(configureTenant(self));
			} else if (operation == 8) {
				wait(renameTenant(self));
			} else if (operation == 9) {
				wait(restoreCluster(self));
			}
		}

		return Void();
	}

	// Checks that the data cluster state matches our local state
	ACTOR static Future<Void> checkDataCluster(MetaclusterManagementWorkload* self,
	                                           ClusterName clusterName,
	                                           Reference<DataClusterData> clusterData) {
		state Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants;
		state Reference<ReadYourWritesTransaction> tr = clusterData->db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(metaclusterRegistration, MetaclusterMetadata::metaclusterRegistration().get(tr)) &&
				     store(tenants,
				           TenantAPI::listTenantMetadataTransaction(
				               tr, ""_sr, "\xff\xff"_sr, clusterData->tenants.size() + 1)));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		if (clusterData->registered) {
			ASSERT(metaclusterRegistration.present() &&
			       metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);
		} else {
			ASSERT(!metaclusterRegistration.present());
		}

		ASSERT_EQ(tenants.size(), clusterData->tenants.size());
		for (auto [tenantName, tenantEntry] : tenants) {
			ASSERT(clusterData->tenants.count(tenantName));
			auto tenantData = clusterData->tenants.find(tenantName);
			ASSERT(tenantData != clusterData->tenants.end());
			ASSERT(tenantData->second->cluster == clusterName);
			ASSERT(tenantData->second->tenantGroup == tenantEntry.tenantGroup);

			if (!clusterData->detached) {
				auto itr = self->createdTenants.find(tenantName);
				ASSERT(itr != self->createdTenants.end());
				ASSERT(itr->second == tenantData->second);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> decommissionMetacluster(MetaclusterManagementWorkload* self) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();

		state bool deleteTenants = deterministicRandom()->coinflip();

		if (deleteTenants) {
			state std::vector<std::pair<TenantName, int64_t>> tenants =
			    wait(MetaclusterAPI::listTenants(self->managementDb, ""_sr, "\xff\xff"_sr, 10e6));

			state std::vector<Future<Void>> deleteTenantFutures;
			for (auto [tenantName, tid] : tenants) {
				deleteTenantFutures.push_back(MetaclusterAPI::deleteTenant(self->managementDb, tenantName));
			}

			wait(waitForAll(deleteTenantFutures));
		}

		state std::map<ClusterName, DataClusterMetadata> dataClusters = wait(
		    MetaclusterAPI::listClusters(self->managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS));

		std::vector<Future<Void>> removeClusterFutures;
		for (auto [clusterName, clusterMetadata] : dataClusters) {
			removeClusterFutures.push_back(success(MetaclusterAPI::removeCluster(
			    self->managementDb, clusterName, ClusterType::METACLUSTER_MANAGEMENT, ForceRemove(!deleteTenants))));
		}

		wait(waitForAll(removeClusterFutures));
		wait(MetaclusterAPI::decommissionMetacluster(self->managementDb));

		Optional<MetaclusterRegistrationEntry> entry =
		    wait(MetaclusterMetadata::metaclusterRegistration().get(self->managementDb));
		ASSERT(!entry.present());

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(this);
		} else {
			return true;
		}
	}
	ACTOR static Future<bool> _check(MetaclusterManagementWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, AllowPartialMetaclusterOperations::False);
		wait(metaclusterConsistencyCheck.run());

		std::map<ClusterName, DataClusterMetadata> dataClusters = wait(MetaclusterAPI::listClusters(
		    self->managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS + 1));

		int totalTenantGroupsAllocated = 0;
		std::vector<Future<Void>> dataClusterChecks;
		for (auto [clusterName, dataClusterData] : self->dataDbs) {
			auto dataClusterItr = dataClusters.find(clusterName);
			if (dataClusterData->registered) {
				ASSERT(dataClusterItr != dataClusters.end());
				ASSERT(dataClusterItr->second.entry.capacity.numTenantGroups == dataClusterData->tenantGroupCapacity);
				totalTenantGroupsAllocated +=
				    dataClusterData->tenantGroups.size() + dataClusterData->ungroupedTenants.size();
			} else {
				ASSERT(dataClusterItr == dataClusters.end());
			}

			dataClusterChecks.push_back(checkDataCluster(self, clusterName, dataClusterData));
		}
		auto capacityNumbers = MetaclusterAPI::metaclusterCapacity(dataClusters);
		ASSERT(capacityNumbers.first.numTenantGroups == self->totalTenantGroupCapacity);
		ASSERT(capacityNumbers.second.numTenantGroups == totalTenantGroupsAllocated);

		wait(waitForAll(dataClusterChecks));

		wait(decommissionMetacluster(self));

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MetaclusterManagementWorkload> MetaclusterManagementWorkloadFactory;
