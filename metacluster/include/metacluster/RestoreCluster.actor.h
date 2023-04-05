/*
 * RestoreCluster.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_RESTORECLUSTER_ACTOR_G_H)
#define METACLUSTER_RESTORECLUSTER_ACTOR_G_H
#include "metacluster/RestoreCluster.actor.g.h"
#elif !defined(METACLUSTER_RESTORECLUSTER_ACTOR_H)
#define METACLUSTER_RESTORECLUSTER_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/ConfigureCluster.h"
#include "metacluster/GetTenant.actor.h"
#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/MetaclusterUtil.actor.h"
#include "metacluster/UpdateTenantGroups.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

FDB_BOOLEAN_PARAM(ApplyManagementClusterUpdates);
FDB_BOOLEAN_PARAM(RestoreDryRun);
FDB_BOOLEAN_PARAM(ForceJoin);
FDB_BOOLEAN_PARAM(ForceReuseTenantIdPrefix);

namespace internal {
template <class DB>
struct RestoreClusterImpl {
	// This prefix is used during a cluster restore if the desired name is in use
	//
	// SOMEDAY: this should probably live in the `\xff` tenant namespace, but other parts of the code are not able to
	// work with `\xff` tenants yet. In the unlikely event that we have regular tenants using this prefix, we have a
	// rename cycle, and we have a collision between the names, a restore will fail with an error. This error can be
	// resolved manually using tenant rename commands.
	static inline const StringRef metaclusterTemporaryRenamePrefix = "\xfe/restoreTenant/"_sr;

	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	ClusterName clusterName;
	ClusterConnectionString connectionString;
	ApplyManagementClusterUpdates applyManagementClusterUpdates;
	RestoreDryRun restoreDryRun;
	ForceJoin forceJoin;
	ForceReuseTenantIdPrefix forceReuseTenantIdPrefix;
	std::vector<std::string>& messages;

	// Unique ID generated for this restore. Used to avoid concurrent restores
	UID restoreId = deterministicRandom()->randomUniqueID();

	// Loaded from the management cluster
	Optional<int64_t> lastManagementClusterTenantId;

	// Loaded from the data cluster
	UID dataClusterId;
	Optional<int64_t> lastDataClusterTenantId;
	Optional<int64_t> newLastDataClusterTenantId;

	// Tenant list from data and management clusters
	std::unordered_map<int64_t, TenantMapEntry> dataClusterTenantMap;
	std::unordered_set<TenantName> dataClusterTenantNames;
	std::unordered_map<int64_t, MetaclusterTenantMapEntry> mgmtClusterTenantMap;
	std::unordered_set<int64_t> mgmtClusterTenantSetForCurrentDataCluster;

	RestoreClusterImpl(Reference<DB> managementDb,
	                   ClusterName clusterName,
	                   ClusterConnectionString connectionString,
	                   ApplyManagementClusterUpdates applyManagementClusterUpdates,
	                   RestoreDryRun restoreDryRun,
	                   ForceJoin forceJoin,
	                   ForceReuseTenantIdPrefix forceReuseTenantIdPrefix,
	                   std::vector<std::string>& messages)
	  : ctx(managementDb, {}, { DataClusterState::RESTORING }), clusterName(clusterName),
	    connectionString(connectionString), applyManagementClusterUpdates(applyManagementClusterUpdates),
	    restoreDryRun(restoreDryRun), forceJoin(forceJoin), forceReuseTenantIdPrefix(forceReuseTenantIdPrefix),
	    messages(messages) {}

	ACTOR template <class Transaction>
	static Future<Void> checkRestoreId(RestoreClusterImpl* self, Transaction tr) {
		if (!self->restoreDryRun) {
			Optional<UID> activeRestoreId = wait(metadata::activeRestoreIds().get(tr, self->clusterName));
			if (!activeRestoreId.present() || activeRestoreId.get() != self->restoreId) {
				throw conflicting_restore();
			}
		}

		return Void();
	}

	// Returns true if the restore ID was erased
	ACTOR template <class Transaction>
	static Future<bool> eraseRestoreId(RestoreClusterImpl* self, Transaction tr) {
		Optional<UID> transactionId = wait(metadata::activeRestoreIds().get(tr, self->clusterName));
		if (!transactionId.present()) {
			return false;
		} else if (transactionId.get() != self->restoreId) {
			throw conflicting_restore();
		} else {
			metadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
			metadata::activeRestoreIds().erase(tr, self->clusterName);
		}

		return true;
	}

	template <class Function>
	Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runRestoreManagementTransaction(Function func) {
		return ctx.runManagementTransaction([this, func](Reference<typename DB::TransactionT> tr) {
			return joinWith(func(tr), checkRestoreId(this, tr));
		});
	}

	template <class Function>
	Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runRestoreDataClusterTransaction(
	    Function func,
	    RunOnDisconnectedCluster runOnDisconnectedCluster = RunOnDisconnectedCluster::False,
	    RunOnMismatchedCluster runOnMismatchedCluster = RunOnMismatchedCluster::False) {
		return ctx.runDataClusterTransaction(
		    [this, func](Reference<ITransaction> tr) { return joinWith(func(tr), checkRestoreId(this, tr)); },
		    runOnDisconnectedCluster,
		    runOnMismatchedCluster);
	}

	// If restoring a data cluster, verify that it has a matching registration entry
	ACTOR static Future<Void> loadDataClusterRegistration(RestoreClusterImpl* self) {
		state Reference<IDatabase> db = wait(util::openDatabase(self->connectionString));
		state Reference<ITransaction> tr = db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
				    wait(metadata::metaclusterRegistration().get(tr));

				if (!metaclusterRegistration.present()) {
					throw invalid_data_cluster();
				} else if (!metaclusterRegistration.get().matches(self->ctx.metaclusterRegistration.get())) {
					if (!self->forceJoin) {
						TraceEvent(SevWarn, "MetaclusterRestoreClusterMismatch")
						    .detail("ExistingRegistration", metaclusterRegistration.get())
						    .detail("ManagementClusterRegistration", self->ctx.metaclusterRegistration.get());
						throw cluster_already_registered();
					} else if (!self->restoreDryRun) {
						ASSERT(self->ctx.metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);
						metadata::metaclusterRegistration().set(tr, self->ctx.metaclusterRegistration.get());
					} else {
						self->messages.push_back(fmt::format("Move data cluster to new metacluster\n"
						                                     "        original: {}\n"
						                                     "        updated:  {}",
						                                     metaclusterRegistration.get().toString(),
						                                     self->ctx.metaclusterRegistration.get().toString()));
					}
				} else if (metaclusterRegistration.get().name != self->clusterName) {
					TraceEvent(SevWarn, "MetaclusterRestoreClusterNameMismatch")
					    .detail("ExistingName", metaclusterRegistration.get().name)
					    .detail("ManagementClusterRegistration", self->clusterName);
					throw cluster_already_registered();
				}

				self->dataClusterId = metaclusterRegistration.get().id;
				self->ctx.dataClusterDb = db;

				if (!self->restoreDryRun) {
					wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				}

				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	// Store the cluster entry for the restored cluster
	ACTOR static Future<Void> registerRestoringClusterInManagementCluster(RestoreClusterImpl* self,
	                                                                      Reference<typename DB::TransactionT> tr) {
		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (dataClusterMetadata.present() &&
		    (dataClusterMetadata.get().entry.clusterState != DataClusterState::RESTORING ||
		     !self->dataClusterId.isValid() || dataClusterMetadata.get().entry.id != self->dataClusterId)) {
			TraceEvent("RestoredClusterAlreadyExists").detail("ClusterName", self->clusterName);
			throw cluster_already_exists();
		} else if (!self->restoreDryRun) {
			metadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
			metadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);

			if (!dataClusterMetadata.present()) {
				self->dataClusterId = deterministicRandom()->randomUniqueID();
			}

			DataClusterEntry clusterEntry;
			clusterEntry.id = self->dataClusterId;
			clusterEntry.clusterState = DataClusterState::RESTORING;

			metadata::management::dataClusters().set(tr, self->clusterName, clusterEntry);
			metadata::management::dataClusterConnectionRecords().set(tr, self->clusterName, self->connectionString);

			TraceEvent("RegisteredRestoringDataCluster")
			    .detail("ClusterName", self->clusterName)
			    .detail("ClusterID", clusterEntry.id)
			    .detail("Capacity", clusterEntry.capacity)
			    .detail("Version", tr->getCommittedVersion())
			    .detail("ConnectionString", self->connectionString.toString());
		}

		return Void();
	}

	// If adding a data cluster to a restored management cluster, write a metacluster registration entry
	// to attach it
	ACTOR static Future<Void> writeDataClusterRegistration(RestoreClusterImpl* self) {
		state Reference<IDatabase> db = wait(util::openDatabase(self->connectionString));
		state Reference<ITransaction> tr = db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Future<bool> tombstoneFuture = metadata::registrationTombstones().exists(tr, self->dataClusterId);

				state Future<Void> lastTenantIdFuture =
				    store(self->lastDataClusterTenantId, TenantMetadata::lastTenantId().get(tr));

				state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
				    wait(metadata::metaclusterRegistration().get(tr));

				// Check if the cluster was removed concurrently
				bool tombstone = wait(tombstoneFuture);
				if (tombstone) {
					throw cluster_removed();
				}

				wait(lastTenantIdFuture);

				MetaclusterRegistrationEntry dataClusterEntry =
				    self->ctx.metaclusterRegistration.get().toDataClusterRegistration(self->clusterName,
				                                                                      self->dataClusterId);

				if (metaclusterRegistration.present()) {
					if (dataClusterEntry.matches(metaclusterRegistration.get())) {
						break;
					}

					TraceEvent(SevWarn, "MetaclusterRestoreClusterAlreadyRegistered")
					    .detail("ExistingRegistration", metaclusterRegistration.get())
					    .detail("NewRegistration", dataClusterEntry);
					throw cluster_already_registered();
				}

				if (!self->restoreDryRun) {
					metadata::metaclusterRegistration().set(tr, dataClusterEntry);
					metadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
					metadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);
					wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				}

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> markClusterRestoring(RestoreClusterImpl* self, Reference<typename DB::TransactionT> tr) {
		metadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
		metadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);
		if (self->ctx.dataClusterMetadata.get().entry.clusterState != DataClusterState::RESTORING) {
			DataClusterEntry updatedEntry = self->ctx.dataClusterMetadata.get().entry;
			updatedEntry.clusterState = DataClusterState::RESTORING;

			updateClusterMetadata(
			    tr, self->clusterName, self->ctx.dataClusterMetadata.get(), self->connectionString, updatedEntry);
			// Remove this cluster from the cluster capacity index, but leave its configured capacity intact in the
			// cluster entry. This allows us to retain the configured capacity while preventing the cluster from
			// being used to allocate new tenant groups.
			DataClusterEntry noCapacityEntry = updatedEntry;
			noCapacityEntry.capacity.numTenantGroups = 0;
			internal::updateClusterCapacityIndex(tr, self->clusterName, updatedEntry, noCapacityEntry);
		}

		wait(store(self->lastManagementClusterTenantId, metadata::management::tenantMetadata().lastTenantId.get(tr)));

		TraceEvent("MarkedDataClusterRestoring").detail("Name", self->clusterName);
		return Void();
	}

	Future<Void> markClusterAsReady(Reference<typename DB::TransactionT> tr) {
		if (ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::RESTORING) {
			DataClusterEntry updatedEntry = ctx.dataClusterMetadata.get().entry;
			updatedEntry.clusterState = DataClusterState::READY;

			updateClusterMetadata(tr, clusterName, ctx.dataClusterMetadata.get(), {}, updatedEntry);

			// Add this cluster back to the cluster capacity index so that it can be assigned to again.
			DataClusterEntry noCapacityEntry = updatedEntry;
			noCapacityEntry.capacity.numTenantGroups = 0;
			internal::updateClusterCapacityIndex(tr, clusterName, noCapacityEntry, updatedEntry);

			return success(eraseRestoreId(this, tr));
		}

		return Void();
	}

	ACTOR static Future<Void> markManagementTenantsAsError(RestoreClusterImpl* self,
	                                                       Reference<typename DB::TransactionT> tr,
	                                                       std::vector<int64_t> tenants) {
		ASSERT(!self->restoreDryRun);
		state std::vector<Future<Optional<MetaclusterTenantMapEntry>>> getFutures;
		for (auto tenantId : tenants) {
			getFutures.push_back(tryGetTenantTransaction(tr, tenantId));
		}

		wait(waitForAll(getFutures));

		for (auto const& f : getFutures) {
			if (!f.get().present()) {
				continue;
			}

			MetaclusterTenantMapEntry entry = f.get().get();
			entry.tenantState = TenantState::ERROR;
			entry.error = "The tenant is missing after restoring its data cluster";
			metadata::management::tenantMetadata().tenantMap.set(tr, entry.id, entry);
		}

		return Void();
	}

	ACTOR static Future<Void> getTenantsFromDataCluster(RestoreClusterImpl* self, Reference<ITransaction> tr) {
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants =
		    wait(TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));

		for (auto const& t : tenants.results) {
			self->dataClusterTenantMap.emplace(t.first, t.second);
			self->dataClusterTenantNames.insert(t.second.tenantName);
		}

		return Void();
	}

	ACTOR static Future<Optional<int64_t>> getTenantsFromManagementCluster(RestoreClusterImpl* self,
	                                                                       Reference<typename DB::TransactionT> tr,
	                                                                       int64_t initialTenantId) {
		state KeyBackedRangeResult<std::pair<int64_t, MetaclusterTenantMapEntry>> tenants =
		    wait(metadata::management::tenantMetadata().tenantMap.getRange(
		        tr, initialTenantId, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));

		for (auto const& t : tenants.results) {
			self->mgmtClusterTenantMap.emplace(t.first, t.second);
			if (self->clusterName == t.second.assignedCluster) {
				self->mgmtClusterTenantSetForCurrentDataCluster.emplace(t.first);
			}
		}

		return tenants.more ? Optional<int64_t>(tenants.results.rbegin()->first + 1) : Optional<int64_t>();
	}

	ACTOR static Future<Void> getAllTenantsFromManagementCluster(RestoreClusterImpl* self) {
		// get all tenants across all data clusters
		state Optional<int64_t> beginTenant = 0;
		while (beginTenant.present()) {
			wait(store(beginTenant,
			           self->runRestoreManagementTransaction(
			               [self = self, beginTenant = beginTenant](Reference<typename DB::TransactionT> tr) {
				               return getTenantsFromManagementCluster(self, tr, beginTenant.get());
			               })));
		}

		return Void();
	}

	ACTOR static Future<Void> renameTenant(RestoreClusterImpl* self,
	                                       Reference<ITransaction> tr,
	                                       int64_t tenantId,
	                                       TenantName oldTenantName,
	                                       TenantName newTenantName,
	                                       int configurationSequenceNum) {
		state Optional<TenantMapEntry> entry;
		state Optional<int64_t> newId;

		wait(store(entry, TenantAPI::tryGetTenantTransaction(tr, tenantId)) &&
		     store(newId, TenantMetadata::tenantNameIndex().get(tr, newTenantName)));

		if (entry.present()) {
			if (entry.get().tenantName == oldTenantName && !newId.present()) {
				wait(TenantAPI::renameTenantTransaction(tr,
				                                        oldTenantName,
				                                        newTenantName,
				                                        tenantId,
				                                        ClusterType::METACLUSTER_DATA,
				                                        configurationSequenceNum));
				return Void();
			} else if (entry.get().tenantName == newTenantName && newId.present() && newId.get() == tenantId) {
				// The tenant has already been renamed
				return Void();
			}
		}

		TraceEvent(SevWarnAlways, "RestoreDataClusterRenameError")
		    .detail("OldName", oldTenantName)
		    .detail("NewName", newTenantName)
		    .detail("TenantID", tenantId)
		    .detail("ActualTenantName", entry.map(&TenantMapEntry::tenantName))
		    .detail("OldEntryPresent", entry.present())
		    .detail("NewEntryPresent", newId.present());

		if (newId.present()) {
			self->messages.push_back(
			    fmt::format("Failed to rename the tenant `{}' to `{}' because the new name is already in use",
			                printable(oldTenantName),
			                printable(newTenantName)));
			throw tenant_already_exists();
		} else {
			self->messages.push_back(fmt::format(
			    "Failed to rename the tenant `{}' to `{}' because the tenant did not have the expected ID {}",
			    printable(oldTenantName),
			    printable(newTenantName),
			    tenantId));
			throw tenant_not_found();
		}
	}

	ACTOR static Future<Void> updateTenantConfiguration(RestoreClusterImpl* self,
	                                                    Reference<ITransaction> tr,
	                                                    int64_t tenantId,
	                                                    TenantMapEntry updatedEntry) {
		TenantMapEntry existingEntry = wait(TenantAPI::getTenantTransaction(tr, tenantId));

		// The tenant should have already been renamed, so in most cases its name will match.
		// If we had to break a rename cycle using temporary tenant names, use that in the updated
		// entry here since the rename will be completed later.
		if (existingEntry.tenantName != updatedEntry.tenantName) {
			if (!existingEntry.tenantName.startsWith(metaclusterTemporaryRenamePrefix)) {
				// This transaction is going to fail due to the restore ID check, so we don't need to do anything
				CODE_PROBE(true, "tenant restore rename mismatch due to concurrency", probe::decoration::rare);
				return Void();
			}
			updatedEntry.tenantName = existingEntry.tenantName;
		}

		if (existingEntry.configurationSequenceNum <= updatedEntry.configurationSequenceNum) {
			wait(TenantAPI::configureTenantTransaction(tr, existingEntry, updatedEntry));
		}

		return Void();
	}

	// Updates a tenant to match the management cluster state
	// Returns the name of the tenant after it has been reconciled
	ACTOR static Future<Optional<std::pair<TenantName, MetaclusterTenantMapEntry>>> reconcileTenant(
	    RestoreClusterImpl* self,
	    TenantMapEntry tenantEntry) {
		state std::unordered_map<int64_t, MetaclusterTenantMapEntry>::iterator managementEntry =
		    self->mgmtClusterTenantMap.find(tenantEntry.id);

		// A data cluster tenant is not present on the management cluster
		if (managementEntry == self->mgmtClusterTenantMap.end() ||
		    managementEntry->second.assignedCluster != self->clusterName ||
		    managementEntry->second.tenantState == TenantState::REMOVING) {
			if (self->restoreDryRun) {
				if (managementEntry == self->mgmtClusterTenantMap.end()) {
					self->messages.push_back(fmt::format("Delete missing tenant `{}' with ID {} on data cluster",
					                                     printable(tenantEntry.tenantName),
					                                     tenantEntry.id));
				} else if (managementEntry->second.assignedCluster != self->clusterName) {
					self->messages.push_back(fmt::format(
					    "Delete tenant `{}' with ID {} on data cluster because it is now located on the cluster `{}'",
					    printable(tenantEntry.tenantName),
					    tenantEntry.id,
					    printable(managementEntry->second.assignedCluster)));
				} else {
					self->messages.push_back(
					    fmt::format("Delete tenant `{}' with ID {} on data cluster because it is in the REMOVING state",
					                printable(tenantEntry.tenantName),
					                tenantEntry.id));
				}
			} else {
				wait(self->runRestoreDataClusterTransaction([tenantEntry = tenantEntry](Reference<ITransaction> tr) {
					return TenantAPI::deleteTenantTransaction(tr, tenantEntry.id, ClusterType::METACLUSTER_DATA);
				}));
			}

			return Optional<std::pair<TenantName, MetaclusterTenantMapEntry>>();
		} else {
			state TenantName tenantName = tenantEntry.tenantName;
			state MetaclusterTenantMapEntry managementTenant = managementEntry->second;

			// Rename
			state TenantName managementTenantName = managementTenant.tenantState != TenantState::RENAMING
			                                            ? managementTenant.tenantName
			                                            : managementTenant.renameDestination.get();
			state bool renamed = tenantName != managementTenantName;
			if (renamed) {
				state TenantName temporaryName;
				state bool usingTemporaryName = self->dataClusterTenantNames.count(managementTenantName) > 0;
				if (usingTemporaryName) {
					temporaryName = metaclusterTemporaryRenamePrefix.withSuffix(managementTenantName);
				} else {
					temporaryName = managementTenantName;
				}

				if (self->restoreDryRun) {
					self->messages.push_back(fmt::format("Rename tenant `{}' with ID {} to `{}' on data cluster{}",
					                                     printable(tenantEntry.tenantName),
					                                     tenantEntry.id,
					                                     printable(managementTenantName),
					                                     usingTemporaryName ? " via temporary name" : ""));
				} else {
					wait(self->runRestoreDataClusterTransaction(
					    [self = self,
					     tenantName = tenantName,
					     temporaryName = temporaryName,
					     tenantEntry = tenantEntry,
					     managementTenant = managementTenant](Reference<ITransaction> tr) {
						    return renameTenant(self,
						                        tr,
						                        tenantEntry.id,
						                        tenantName,
						                        temporaryName,
						                        managementTenant.configurationSequenceNum);
					    }));
					// SOMEDAY: we could mark the tenant in the management cluster as READY if it is in the RENAMING
					// state
				}
				tenantName = temporaryName;
			}

			// Update configuration
			bool configurationChanged = !managementTenant.matchesConfiguration(tenantEntry);
			if (configurationChanged ||
			    managementTenant.configurationSequenceNum != tenantEntry.configurationSequenceNum) {
				if (self->restoreDryRun) {
					// If this is an update to the internal sequence number only and we are also renaming the tenant,
					// we don't need to report anything. The internal metadata update is (at least partially) caused
					// by the rename in that case
					if (configurationChanged || !renamed) {
						self->messages.push_back(
						    fmt::format("Update tenant configuration for tenant `{}' with ID {} on data cluster{}",
						                printable(tenantEntry.tenantName),
						                tenantEntry.id,
						                configurationChanged ? "" : " (internal metadata only)"));
					}
				} else {
					wait(self->runRestoreDataClusterTransaction([self = self,
					                                             managementTenant = managementTenant,
					                                             tenantEntry = tenantEntry,
					                                             tenantName = tenantName](Reference<ITransaction> tr) {
						ASSERT_GE(managementTenant.configurationSequenceNum, tenantEntry.configurationSequenceNum);
						TenantMapEntry updatedEntry = managementTenant.toTenantMapEntry();
						updatedEntry.tenantName = tenantName;
						return updateTenantConfiguration(self, tr, managementTenant.id, updatedEntry);
					}));
					// SOMEDAY: we could mark the tenant in the management cluster as READY if it is in the
					// UPDATING_CONFIGURATION state
				}
			}

			return std::make_pair(tenantName, managementTenant);
		}
	}

	Future<Void> renameTenantBatch(std::map<TenantName, TenantMapEntry> tenantsToRename) {
		return runRestoreDataClusterTransaction([this, tenantsToRename](Reference<ITransaction> tr) {
			std::vector<Future<Void>> renameFutures;
			for (auto t : tenantsToRename) {
				renameFutures.push_back(renameTenant(
				    this, tr, t.second.id, t.first, t.second.tenantName, t.second.configurationSequenceNum));
			}
			return waitForAll(renameFutures);
		});
	}

	ACTOR static Future<Void> reconcileTenants(RestoreClusterImpl* self) {
		state std::vector<Future<Optional<std::pair<TenantName, MetaclusterTenantMapEntry>>>> reconcileFutures;
		for (auto itr = self->dataClusterTenantMap.begin(); itr != self->dataClusterTenantMap.end(); ++itr) {
			reconcileFutures.push_back(reconcileTenant(self, itr->second));
		}

		wait(waitForAll(reconcileFutures));

		if (!self->restoreDryRun) {
			state int reconcileIndex;
			state std::map<TenantName, TenantMapEntry> tenantsToRename;
			for (reconcileIndex = 0; reconcileIndex < reconcileFutures.size(); ++reconcileIndex) {
				Optional<std::pair<TenantName, MetaclusterTenantMapEntry>> const& result =
				    reconcileFutures[reconcileIndex].get();

				if (result.present() && result.get().first.startsWith(metaclusterTemporaryRenamePrefix)) {
					TenantMapEntry destinationTenant = result.get().second.toTenantMapEntry();
					if (result.get().second.renameDestination.present()) {
						destinationTenant.tenantName = result.get().second.renameDestination.get();
					}

					if (result.get().first != destinationTenant.tenantName) {
						tenantsToRename[result.get().first] = destinationTenant;

						if (tenantsToRename.size() >= CLIENT_KNOBS->METACLUSTER_RESTORE_BATCH_SIZE) {
							wait(self->renameTenantBatch(tenantsToRename));
						}
					}
				}
			}

			if (!tenantsToRename.empty()) {
				wait(self->renameTenantBatch(tenantsToRename));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> processMissingTenants(RestoreClusterImpl* self) {
		state std::unordered_set<int64_t>::iterator setItr = self->mgmtClusterTenantSetForCurrentDataCluster.begin();
		state std::vector<int64_t> missingTenants;
		state int64_t missingTenantCount = 0;
		while (setItr != self->mgmtClusterTenantSetForCurrentDataCluster.end()) {
			int64_t tenantId = *setItr;
			MetaclusterTenantMapEntry const& managementTenant = self->mgmtClusterTenantMap[tenantId];

			// If a tenant is present on the management cluster and not on the data cluster, mark it in an error
			// state unless it is already in certain states (e.g. REGISTERING, REMOVING) that allow the tenant to be
			// missing on the data cluster
			//
			// SOMEDAY: this could optionally complete the partial operations (e.g. finish creating or removing the
			// tenant)
			if (self->dataClusterTenantMap.find(tenantId) == self->dataClusterTenantMap.end() &&
			    managementTenant.tenantState != TenantState::REGISTERING &&
			    managementTenant.tenantState != TenantState::REMOVING) {
				if (self->restoreDryRun) {
					self->messages.push_back(fmt::format("The tenant `{}' with ID {} is missing on the data cluster",
					                                     printable(managementTenant.tenantName),
					                                     tenantId));
				} else {
					// Tenants in an error state that aren't on the data cluster count as missing tenants. This will
					// include tenants we previously marked as missing, and as new errors are added it could include
					// other tenants
					++missingTenantCount;
					if (managementTenant.tenantState != TenantState::ERROR) {
						missingTenants.push_back(tenantId);
						if (missingTenants.size() == CLIENT_KNOBS->METACLUSTER_RESTORE_BATCH_SIZE) {
							wait(self->runRestoreManagementTransaction([self = self, missingTenants = missingTenants](
							                                               Reference<typename DB::TransactionT> tr) {
								return markManagementTenantsAsError(self, tr, missingTenants);
							}));
							missingTenants.clear();
						}
					}
				}
			}
			++setItr;
		}

		if (!self->restoreDryRun && missingTenants.size() > 0) {
			wait(self->runRestoreManagementTransaction(
			    [self = self, missingTenants = missingTenants](Reference<typename DB::TransactionT> tr) {
				    return markManagementTenantsAsError(self, tr, missingTenants);
			    }));
		}

		if (missingTenantCount > 0) {
			self->messages.push_back(fmt::format(
			    "The metacluster has {} tenants that are missing in the restored data cluster", missingTenantCount));
		}
		return Void();
	}

	// Returns true if the group needs to be created
	ACTOR static Future<bool> addTenantToManagementCluster(RestoreClusterImpl* self,
	                                                       Reference<ITransaction> tr,
	                                                       TenantMapEntry tenantEntry) {
		state Future<Optional<MetaclusterTenantGroupEntry>> tenantGroupEntry = Optional<MetaclusterTenantGroupEntry>();
		if (tenantEntry.tenantGroup.present()) {
			tenantGroupEntry =
			    metadata::management::tenantMetadata().tenantGroupMap.get(tr, tenantEntry.tenantGroup.get());
		}

		if (self->lastDataClusterTenantId.present() &&
		    TenantAPI::getTenantIdPrefix(tenantEntry.id) ==
		        TenantAPI::getTenantIdPrefix(self->lastDataClusterTenantId.get()) &&
		    !self->restoreDryRun) {
			ASSERT_LE(tenantEntry.id, self->lastDataClusterTenantId.get());
		}

		Optional<MetaclusterTenantMapEntry> existingEntry = wait(tryGetTenantTransaction(tr, tenantEntry.tenantName));
		if (existingEntry.present()) {
			if (existingEntry.get().assignedCluster == self->clusterName) {
				if (existingEntry.get().id != tenantEntry.id ||
				    !existingEntry.get().matchesConfiguration(tenantEntry)) {
					ASSERT(self->restoreDryRun);
					self->messages.push_back(
					    fmt::format("The tenant `{}' was modified concurrently with the restore dry-run",
					                printable(tenantEntry.tenantName)));
					throw tenant_already_exists();
				}

				// This is a retry, so return success
				return false;
			} else {
				self->messages.push_back(fmt::format("The tenant `{}' already exists on cluster `{}'",
				                                     printable(tenantEntry.tenantName),
				                                     printable(existingEntry.get().assignedCluster)));
				throw tenant_already_exists();
			}
		}

		state MetaclusterTenantMapEntry managementEntry = MetaclusterTenantMapEntry::fromTenantMapEntry(tenantEntry);
		managementEntry.assignedCluster = self->clusterName;

		if (!self->restoreDryRun) {
			metadata::management::tenantMetadata().tenantMap.set(tr, managementEntry.id, managementEntry);
			metadata::management::tenantMetadata().tenantNameIndex.set(
			    tr, managementEntry.tenantName, managementEntry.id);

			metadata::management::tenantMetadata().tenantCount.atomicOp(tr, 1, MutationRef::AddValue);
			metadata::management::clusterTenantCount().atomicOp(
			    tr, managementEntry.assignedCluster, 1, MutationRef::AddValue);

			// Updated indexes to include the new tenant
			metadata::management::clusterTenantIndex().insert(
			    tr, Tuple::makeTuple(managementEntry.assignedCluster, managementEntry.tenantName, managementEntry.id));
		}

		wait(success(tenantGroupEntry));

		if (tenantGroupEntry.get().present() && tenantGroupEntry.get().get().assignedCluster != self->clusterName) {
			self->messages.push_back(
			    fmt::format("The tenant `{}' is part of a tenant group `{}' that already exists on cluster `{}'",
			                printable(managementEntry.tenantName),
			                printable(managementEntry.tenantGroup.get()),
			                printable(tenantGroupEntry.get().get().assignedCluster)));
			throw invalid_tenant_configuration();
		}

		if (!self->restoreDryRun) {
			internal::managementClusterAddTenantToGroup(tr,
			                                            managementEntry,
			                                            &self->ctx.dataClusterMetadata.get(),
			                                            GroupAlreadyExists(tenantGroupEntry.get().present()),
			                                            IgnoreCapacityLimit::True,
			                                            IsRestoring::True);
		}

		return !tenantGroupEntry.get().present();
	}

	ACTOR static Future<Void> addTenantBatchToManagementCluster(RestoreClusterImpl* self,
	                                                            Reference<typename DB::TransactionT> tr,
	                                                            std::vector<TenantMapEntry> tenants,
	                                                            int64_t tenantIdPrefix) {
		state std::vector<Future<bool>> futures;
		state int64_t maxId = -1;
		for (auto const& t : tenants) {
			if (TenantAPI::getTenantIdPrefix(t.id) == tenantIdPrefix) {
				maxId = std::max(maxId, t.id);
				self->newLastDataClusterTenantId = std::max(t.id, self->newLastDataClusterTenantId.orDefault(0));
			}
			futures.push_back(addTenantToManagementCluster(self, tr, t));
		}

		wait(waitForAll(futures));

		std::set<TenantGroupName> groupsCreated;
		state int numGroupsCreated = 0;
		for (int i = 0; i < tenants.size(); ++i) {
			if (futures[i].get()) {
				if (tenants[i].tenantGroup.present()) {
					groupsCreated.insert(tenants[i].tenantGroup.get());
				} else {
					++numGroupsCreated;
				}
			}
		}

		numGroupsCreated += groupsCreated.size();

		if (!self->restoreDryRun) {
			if (numGroupsCreated > 0) {
				DataClusterEntry updatedEntry = self->ctx.dataClusterMetadata.get().entry;
				if (updatedEntry.clusterState != DataClusterState::RESTORING) {
					throw conflicting_restore();
				}
				updatedEntry.allocated.numTenantGroups += numGroupsCreated;
				updateClusterMetadata(tr,
				                      self->clusterName,
				                      self->ctx.dataClusterMetadata.get(),
				                      Optional<ClusterConnectionString>(),
				                      updatedEntry,
				                      IsRestoring::True);
			}

			int64_t lastTenantId =
			    wait(metadata::management::tenantMetadata().lastTenantId.getD(tr, Snapshot::False, -1));

			if (maxId > lastTenantId) {
				metadata::management::tenantMetadata().lastTenantId.set(tr, maxId);
			}

			metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	ACTOR static Future<int64_t> updateLastTenantId(RestoreClusterImpl* self, Reference<typename DB::TransactionT> tr) {
		state Optional<int64_t> lastTenantId = wait(metadata::management::tenantMetadata().lastTenantId.get(tr));
		state int64_t tenantIdPrefix;
		if (!lastTenantId.present()) {
			Optional<int64_t> prefix = wait(TenantMetadata::tenantIdPrefix().get(tr));
			ASSERT(prefix.present());
			tenantIdPrefix = prefix.get();
		} else {
			tenantIdPrefix = TenantAPI::getTenantIdPrefix(lastTenantId.get());
		}

		if (self->lastDataClusterTenantId.present() &&
		    tenantIdPrefix == TenantAPI::getTenantIdPrefix(self->lastDataClusterTenantId.get())) {
			if (!self->forceReuseTenantIdPrefix) {
				self->messages.push_back(fmt::format(
				    "The data cluster being added is using the same tenant ID prefix {} as the management cluster.",
				    tenantIdPrefix));
				throw invalid_metacluster_configuration();
			} else if (!self->restoreDryRun && self->lastDataClusterTenantId.get() > lastTenantId.orDefault(-1)) {
				metadata::management::tenantMetadata().lastTenantId.set(tr, self->lastDataClusterTenantId.get());
			}

			self->newLastDataClusterTenantId = self->lastDataClusterTenantId;
		}

		return tenantIdPrefix;
	}

	ACTOR static Future<Void> addTenantsToManagementCluster(RestoreClusterImpl* self) {
		state std::unordered_map<int64_t, TenantMapEntry>::iterator itr;
		state std::vector<TenantMapEntry> tenantBatch;
		state int64_t tenantsToAdd = 0;

		state int64_t tenantIdPrefix = wait(self->runRestoreManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return updateLastTenantId(self, tr); }));

		for (itr = self->dataClusterTenantMap.begin(); itr != self->dataClusterTenantMap.end(); ++itr) {
			state std::unordered_map<int64_t, MetaclusterTenantMapEntry>::iterator managementEntry =
			    self->mgmtClusterTenantMap.find(itr->second.id);
			if (managementEntry == self->mgmtClusterTenantMap.end()) {
				++tenantsToAdd;
				tenantBatch.push_back(itr->second);
			} else if (managementEntry->second.tenantName != itr->second.tenantName ||
			           managementEntry->second.assignedCluster != self->clusterName ||
			           !managementEntry->second.matchesConfiguration(itr->second)) {
				self->messages.push_back(
				    fmt::format("The tenant `{}' has the same ID {} as an existing tenant `{}' on cluster `{}'",
				                printable(itr->second.tenantName),
				                itr->second.id,
				                printable(managementEntry->second.tenantName),
				                printable(managementEntry->second.assignedCluster)));
				throw tenant_already_exists();
			}

			if (tenantBatch.size() == CLIENT_KNOBS->METACLUSTER_RESTORE_BATCH_SIZE) {
				wait(self->runRestoreManagementTransaction(
				    [self = self, tenantBatch = tenantBatch, tenantIdPrefix = tenantIdPrefix](
				        Reference<typename DB::TransactionT> tr) {
					    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					    return addTenantBatchToManagementCluster(self, tr, tenantBatch, tenantIdPrefix);
				    }));
				tenantBatch.clear();
			}
		}

		if (!tenantBatch.empty()) {
			wait(self->runRestoreManagementTransaction(
			    [self = self, tenantBatch = tenantBatch, tenantIdPrefix = tenantIdPrefix](
			        Reference<typename DB::TransactionT> tr) {
				    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				    return addTenantBatchToManagementCluster(self, tr, tenantBatch, tenantIdPrefix);
			    }));
		}

		if (self->restoreDryRun) {
			self->messages.push_back(
			    fmt::format("Restore will add {} tenant(s) to the management cluster from the data cluster `{}'",
			                tenantsToAdd,
			                printable(self->clusterName)));
		}

		return Void();
	}

	ACTOR static Future<Void> finalizeDataClusterAfterRepopulate(RestoreClusterImpl* self, Reference<ITransaction> tr) {
		bool erased = wait(eraseRestoreId(self, tr));
		if (erased) {
			if (self->newLastDataClusterTenantId.present()) {
				TenantMetadata::lastTenantId().set(tr, self->newLastDataClusterTenantId.get());
			} else {
				TenantMetadata::lastTenantId().clear(tr);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> runDataClusterRestore(RestoreClusterImpl* self) {
		// Run a management transaction to populate the data cluster metadata
		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return self->ctx.setCluster(tr, self->clusterName);
		}));

		// Make sure that the data cluster being restored has the appropriate metacluster registration entry and
		// name
		wait(loadDataClusterRegistration(self));

		// set state to restoring
		if (!self->restoreDryRun) {
			try {
				wait(self->ctx.runManagementTransaction(
				    [self = self](Reference<typename DB::TransactionT> tr) { return markClusterRestoring(self, tr); }));
			} catch (Error& e) {
				// If the transaction retries after success or if we are trying a second time to restore the cluster, it
				// will throw an error indicating that the restore has already started
				if (e.code() != error_code_cluster_restoring) {
					throw;
				}
			}
		}

		// Set the restore ID in the data cluster and update the last tenant ID to match the management cluster
		if (!self->restoreDryRun) {
			wait(self->ctx.runDataClusterTransaction([self = self](Reference<ITransaction> tr) {
				metadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
				metadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);
				if (self->lastManagementClusterTenantId.present()) {
					TenantMetadata::lastTenantId().set(tr, self->lastManagementClusterTenantId.get());
				} else {
					TenantMetadata::lastTenantId().clear(tr);
				}
				return Future<Void>(Void());
			}));
		}

		// get all the tenants in the metacluster
		wait(getAllTenantsFromManagementCluster(self));

		// get all the tenant information from the newly registered data cluster
		wait(self->runRestoreDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getTenantsFromDataCluster(self, tr); },
		    RunOnDisconnectedCluster::False,
		    RunOnMismatchedCluster(self->restoreDryRun && self->forceJoin)));

		// Fix any differences between the data cluster and the management cluster
		wait(reconcileTenants(self));

		// Mark tenants that are missing from the data cluster in an error state on the management cluster
		wait(processMissingTenants(self));

		if (!self->restoreDryRun) {
			// Remove the active restore ID from the data cluster
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return success(eraseRestoreId(self, tr)); }));

			// set restored cluster to ready state
			wait(self->ctx.runManagementTransaction(
			    [self = self](Reference<typename DB::TransactionT> tr) { return self->markClusterAsReady(tr); }));
			TraceEvent("MetaclusterRepopulatedFromDataCluster").detail("Name", self->clusterName);
		}

		return Void();
	}

	ACTOR static Future<Void> runManagementClusterRepopulate(RestoreClusterImpl* self) {
		// Record the data cluster in the management cluster
		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return registerRestoringClusterInManagementCluster(self, tr);
		}));

		// Write a metacluster registration entry in the data cluster
		wait(writeDataClusterRegistration(self));

		if (!self->restoreDryRun) {
			wait(self->runRestoreManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return self->ctx.setCluster(tr, self->clusterName);
			}));
		}

		// get all the tenants in the metacluster
		wait(getAllTenantsFromManagementCluster(self));

		if (self->restoreDryRun) {
			wait(store(self->ctx.dataClusterDb, util::openDatabase(self->connectionString)));
		}

		// get all the tenant information from the newly registered data cluster
		wait(self->runRestoreDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getTenantsFromDataCluster(self, tr); },
		    RunOnDisconnectedCluster(self->restoreDryRun)));

		// Add all tenants from the data cluster to the management cluster
		wait(addTenantsToManagementCluster(self));

		if (!self->restoreDryRun) {
			// Remove the active restore ID from the data cluster
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return finalizeDataClusterAfterRepopulate(self, tr); }));

			// set restored cluster to ready state
			wait(self->ctx.runManagementTransaction(
			    [self = self](Reference<typename DB::TransactionT> tr) { return self->markClusterAsReady(tr); }));
			TraceEvent("DataClusterRestoredToMetacluster").detail("Name", self->clusterName);
		}

		return Void();
	}

	Future<Void> run() {
		if (applyManagementClusterUpdates) {
			return runDataClusterRestore(this);
		} else {
			return runManagementClusterRepopulate(this);
		}
	}
};
} // namespace internal

ACTOR template <class DB>
Future<Void> restoreCluster(Reference<DB> db,
                            ClusterName name,
                            ClusterConnectionString connectionString,
                            ApplyManagementClusterUpdates applyManagementClusterUpdates,
                            RestoreDryRun restoreDryRun,
                            ForceJoin forceJoin,
                            ForceReuseTenantIdPrefix forceReuseTenantIdPrefix,
                            std::vector<std::string>* messages) {
	state internal::RestoreClusterImpl<DB> impl(db,
	                                            name,
	                                            connectionString,
	                                            applyManagementClusterUpdates,
	                                            restoreDryRun,
	                                            forceJoin,
	                                            forceReuseTenantIdPrefix,
	                                            *messages);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif