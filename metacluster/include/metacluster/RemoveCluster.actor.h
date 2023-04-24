/*
 * RemoveCluster.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_REMOVECLUSTER_ACTOR_G_H)
#define METACLUSTER_REMOVECLUSTER_ACTOR_G_H
#include "metacluster/RemoveCluster.actor.g.h"
#elif !defined(METACLUSTER_REMOVECLUSTER_ACTOR_H)
#define METACLUSTER_REMOVECLUSTER_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/ConfigureCluster.h"
#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

FDB_BOOLEAN_PARAM(ForceRemove);

namespace internal {
template <class DB>
struct RemoveClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	Reference<DB> db;
	ClusterType clusterType;
	ClusterName clusterName;
	ForceRemove forceRemove;
	double dataClusterTimeout;

	// Optional parameters that are set by internal users
	Optional<UID> clusterId;
	std::set<DataClusterState> legalClusterStates;

	// Parameters set in markClusterRemoving
	Optional<int64_t> lastTenantId;

	// Output parameter indicating whether the data cluster was updated during the removal
	bool dataClusterUpdated = false;

	RemoveClusterImpl(Reference<DB> db,
	                  ClusterName clusterName,
	                  ClusterType clusterType,
	                  ForceRemove forceRemove,
	                  double dataClusterTimeout)
	  : ctx(db,
	        Optional<ClusterName>(),
	        { DataClusterState::REGISTERING, DataClusterState::REMOVING, DataClusterState::RESTORING }),
	    db(db), clusterType(clusterType), clusterName(clusterName), forceRemove(forceRemove),
	    dataClusterTimeout(dataClusterTimeout) {}

	// Returns false if the cluster is no longer present, or true if it is present and the removal should proceed.
	ACTOR static Future<Void> markClusterRemoving(RemoveClusterImpl* self, Reference<typename DB::TransactionT> tr) {
		state DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, self->clusterName));
		wait(self->ctx.setCluster(tr, self->clusterName));

		if ((self->clusterId.present() && clusterMetadata.entry.id != self->clusterId.get()) ||
		    (!self->legalClusterStates.empty() &&
		     !self->legalClusterStates.count(clusterMetadata.entry.clusterState))) {
			// The type of error is currently ignored, and this is only used to terminate the remove operation.
			// If that changes in the future, we may want to introduce a more suitable error type.
			throw operation_failed();
		}

		if (!self->forceRemove && self->ctx.dataClusterMetadata.get().entry.allocated.numTenantGroups > 0) {
			throw cluster_not_empty();
		} else if (self->ctx.dataClusterMetadata.get().entry.clusterState != DataClusterState::REMOVING) {
			// Mark the cluster in a removing state while we finish the remaining removal steps. This prevents new
			// tenants from being assigned to it.
			DataClusterEntry updatedEntry = self->ctx.dataClusterMetadata.get().entry;
			updatedEntry.clusterState = DataClusterState::REMOVING;
			updatedEntry.capacity.numTenantGroups = 0;

			metadata::activeRestoreIds().erase(tr, self->clusterName);

			updateClusterMetadata(tr,
			                      self->ctx.clusterName.get(),
			                      self->ctx.dataClusterMetadata.get(),
			                      Optional<ClusterConnectionString>(),
			                      updatedEntry);
		}

		metadata::management::clusterCapacityIndex().erase(
		    tr,
		    Tuple::makeTuple(self->ctx.dataClusterMetadata.get().entry.allocated.numTenantGroups,
		                     self->ctx.clusterName.get()));

		// Get the last allocated tenant ID to be used on the detached data cluster
		if (self->forceRemove) {
			Optional<int64_t> lastId = wait(metadata::management::tenantMetadata().lastTenantId.get(tr));
			self->lastTenantId = lastId;
		}

		TraceEvent("MarkedDataClusterRemoving").detail("Name", self->ctx.clusterName.get());
		return Void();
	}

	// Delete metacluster metadata from the data cluster
	ACTOR template <class Transaction>
	static Future<Void> updateDataCluster(RemoveClusterImpl* self, Reference<Transaction> tr, UID clusterId) {
		if (self->ctx.dataClusterIsRegistered) {
			// Delete metacluster related metadata
			metadata::metaclusterRegistration().clear(tr);
			metadata::activeRestoreIds().clear(tr);
			TenantMetadata::tenantTombstones().clear(tr);
			TenantMetadata::tombstoneCleanupData().clear(tr);

			// If we are force removing a cluster, then it will potentially contain tenants that have IDs
			// larger than the next tenant ID to be allocated on the cluster. To avoid collisions, we advance
			// the ID so that it will be the larger of the current one on the data cluster and the management
			// cluster.
			if (self->lastTenantId.present()) {
				Optional<int64_t> lastId = wait(TenantMetadata::lastTenantId().get(tr));
				if (!lastId.present() || (TenantAPI::getTenantIdPrefix(lastId.get()) ==
				                              TenantAPI::getTenantIdPrefix(self->lastTenantId.get()) &&
				                          lastId.get() < self->lastTenantId.get())) {
					TenantMetadata::lastTenantId().set(tr, self->lastTenantId.get());
				}
			}
		}

		// Insert a tombstone marking this cluster removed even if we aren't registered
		metadata::registrationTombstones().insert(tr, clusterId);

		TraceEvent("RemovedMetaclusterRegistrationOnDataCluster")
		    .detail("Name", self->clusterName)
		    .detail("WasRegistered", self->ctx.dataClusterIsRegistered);

		return Void();
	}

	// Returns a pair of bools. The first will be true if all tenants have been purged, and the second will be true if
	// any tenants have been purged
	ACTOR static Future<std::pair<bool, bool>> purgeTenants(RemoveClusterImpl* self,
	                                                        Reference<typename DB::TransactionT> tr,
	                                                        std::pair<Tuple, Tuple> clusterTupleRange) {
		ASSERT(self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING);

		// Get the list of tenants
		state Future<KeyBackedRangeResult<Tuple>> tenantEntriesFuture =
		    metadata::management::clusterTenantIndex().getRange(
		        tr, clusterTupleRange.first, clusterTupleRange.second, CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

		state KeyBackedRangeResult<Tuple> tenantEntries = wait(tenantEntriesFuture);

		// Erase each tenant from the tenant map on the management cluster
		int64_t erasedTenants = 0;
		for (Tuple entry : tenantEntries.results) {
			int64_t tenantId = entry.getInt(2);
			ASSERT(entry.getString(0) == self->ctx.clusterName.get());
			if (tenantId != TenantInfo::INVALID_TENANT) {
				++erasedTenants;
				metadata::management::tenantMetadata().tenantMap.erase(tr, tenantId);
			}
			metadata::management::tenantMetadata().tenantNameIndex.erase(tr, entry.getString(1));
			metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		// Erase all of the tenants processed in this transaction from the cluster tenant index
		if (!tenantEntries.results.empty()) {
			metadata::management::clusterTenantIndex().erase(
			    tr,
			    clusterTupleRange.first,
			    Tuple::makeTuple(self->ctx.clusterName.get(), keyAfter(tenantEntries.results.rbegin()->getString(1))));
		}

		metadata::management::tenantMetadata().tenantCount.atomicOp(tr, -erasedTenants, MutationRef::AddValue);
		metadata::management::clusterTenantCount().atomicOp(
		    tr, self->ctx.clusterName.get(), -erasedTenants, MutationRef::AddValue);

		return std::make_pair(!tenantEntries.more, !tenantEntries.results.empty());
	}

	// Returns true if all tenant groups have been purged
	ACTOR static Future<bool> purgeTenantGroups(RemoveClusterImpl* self,
	                                            Reference<typename DB::TransactionT> tr,
	                                            std::pair<Tuple, Tuple> clusterTupleRange) {
		ASSERT(self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING);

		// Get the list of tenant groups
		state Future<KeyBackedRangeResult<Tuple>> tenantGroupEntriesFuture =
		    metadata::management::clusterTenantGroupIndex().getRange(
		        tr, clusterTupleRange.first, clusterTupleRange.second, CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

		// Erase each tenant group from the tenant group map and the tenant group tenant index
		state KeyBackedRangeResult<Tuple> tenantGroupEntries = wait(tenantGroupEntriesFuture);
		for (Tuple entry : tenantGroupEntries.results) {
			ASSERT(entry.getString(0) == self->ctx.clusterName.get());
			TenantGroupName tenantGroup = entry.getString(1);
			metadata::management::tenantMetadata().tenantGroupTenantIndex.erase(
			    tr, Tuple::makeTuple(tenantGroup), Tuple::makeTuple(keyAfter(tenantGroup)));
			metadata::management::tenantMetadata().tenantGroupMap.erase(tr, tenantGroup);
		}

		if (!tenantGroupEntries.results.empty()) {
			// Erase all of the tenant groups processed in this transaction from the cluster tenant group index
			metadata::management::clusterTenantGroupIndex().erase(
			    tr,
			    clusterTupleRange.first,
			    Tuple::makeTuple(self->ctx.clusterName.get(),
			                     keyAfter(tenantGroupEntries.results.rbegin()->getString(1))));
		}

		return !tenantGroupEntries.more;
	}

	// Removes the data cluster entry from the management cluster
	void removeDataClusterEntry(Reference<typename DB::TransactionT> tr) {
		metadata::management::dataClusters().erase(tr, ctx.clusterName.get());
		metadata::management::dataClusterConnectionRecords().erase(tr, ctx.clusterName.get());
		metadata::management::clusterTenantCount().erase(tr, ctx.clusterName.get());
	}

	// Removes the next set of metadata from the management cluster; returns true when all specified
	// metadata is removed
	ACTOR static Future<bool> managementClusterPurgeSome(RemoveClusterImpl* self,
	                                                     Reference<typename DB::TransactionT> tr,
	                                                     std::pair<Tuple, Tuple> clusterTupleRange,
	                                                     bool* deleteTenants) {
		if (deleteTenants) {
			std::pair<bool, bool> deleteResult = wait(purgeTenants(self, tr, clusterTupleRange));
			// If we didn't delete everything, return and try again on the next iteration
			if (!deleteResult.first) {
				return false;
			}

			// If there was nothing to delete, then we don't have to try purging tenants again the next time
			*deleteTenants = deleteResult.second;
		}

		bool deletedAllTenantGroups = wait(purgeTenantGroups(self, tr, clusterTupleRange));
		if (!deletedAllTenantGroups) {
			return false;
		}

		return true;
	}

	// Remove all metadata associated with the data cluster from the management cluster
	ACTOR static Future<Void> managementClusterPurgeDataCluster(RemoveClusterImpl* self) {
		state std::pair<Tuple, Tuple> clusterTupleRange = std::make_pair(
		    Tuple::makeTuple(self->ctx.clusterName.get()), Tuple::makeTuple(keyAfter(self->ctx.clusterName.get())));

		state bool deleteTenants = true;

		loop {
			bool clearedAll = wait(self->ctx.runManagementTransaction(
			    [self = self, clusterTupleRange = clusterTupleRange, deleteTenants = &deleteTenants](
			        Reference<typename DB::TransactionT> tr) {
				    return managementClusterPurgeSome(self, tr, clusterTupleRange, deleteTenants);
			    }));

			if (clearedAll) {
				break;
			}
		}

		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			self->removeDataClusterEntry(tr);
			return Future<Void>(Void());
		}));

		TraceEvent("RemovedDataCluster").detail("Name", self->ctx.clusterName.get());
		return Void();
	}

	// Remove the metacluster registration entry on a data cluster without modifying the management cluster.
	// Useful when reconstructing a management cluster when the original is lost.
	ACTOR static Future<Void> dataClusterForgetMetacluster(RemoveClusterImpl* self) {
		state Reference<typename DB::TransactionT> tr = self->db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Optional<MetaclusterRegistrationEntry> metaclusterRegistrationEntry =
				    wait(metadata::metaclusterRegistration().get(tr));

				if (!metaclusterRegistrationEntry.present()) {
					return Void();
				}

				if (metaclusterRegistrationEntry.get().clusterType != ClusterType::METACLUSTER_DATA) {
					TraceEvent(SevWarn, "CannotRemoveNonDataCluster")
					    .detail("ClusterName", self->clusterName)
					    .detail("MetaclusterRegistration",
					            metaclusterRegistrationEntry.map(&MetaclusterRegistrationEntry::toString));
					throw invalid_metacluster_operation();
				}

				if (metaclusterRegistrationEntry.get().name != self->clusterName) {
					TraceEvent(SevWarn, "CannotRemoveDataClusterWithNameMismatch")
					    .detail("ExpectedName", self->clusterName)
					    .detail("MetaclusterRegistration",
					            metaclusterRegistrationEntry.map(&MetaclusterRegistrationEntry::toString));
					throw metacluster_mismatch();
				}

				wait(updateDataCluster(self, tr, metaclusterRegistrationEntry.get().id));
				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> run(RemoveClusterImpl* self) {
		// On data clusters, we forget the metacluster information without updating the management cluster
		if (self->clusterType == ClusterType::METACLUSTER_DATA) {
			if (!self->forceRemove) {
				throw invalid_metacluster_operation();
			}

			wait(dataClusterForgetMetacluster(self));
			self->dataClusterUpdated = true;
			return Void();
		}

		try {
			wait(self->ctx.runManagementTransaction(
			    [self = self](Reference<typename DB::TransactionT> tr) { return markClusterRemoving(self, tr); }));
		} catch (Error& e) {
			// If the transaction retries after success or if we are trying a second time to remove the cluster, it
			// will throw an error indicating that the removal has already started
			if (e.code() != error_code_cluster_removed) {
				throw;
			}
		}

		try {
			Future<Void> f = self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) {
				    return updateDataCluster(self, tr, self->ctx.metaclusterRegistration.get().id);
			    },
			    RunOnDisconnectedCluster::True);

			if (self->forceRemove && self->dataClusterTimeout > 0) {
				f = timeoutError(f, self->dataClusterTimeout);
			}

			wait(f);
			self->dataClusterUpdated = true;
		} catch (Error& e) {
			// If this transaction gets retried, the metacluster information may have already been erased.
			if (e.code() == error_code_cluster_removed) {
				self->dataClusterUpdated = true;
			} else if (e.code() != error_code_timed_out) {
				throw;
			}
		}

		// This runs multiple transactions, so the run transaction calls are inside the function
		try {
			wait(managementClusterPurgeDataCluster(self));
		} catch (Error& e) {
			// If this transaction gets retried, the cluster may have already been deleted.
			if (e.code() != error_code_cluster_removed) {
				throw;
			}
		}
		return Void();
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<bool> removeCluster(Reference<DB> db,
                           ClusterName name,
                           ClusterType clusterType,
                           ForceRemove forceRemove,
                           double dataClusterTimeout = 0) {
	state internal::RemoveClusterImpl<DB> impl(db, name, clusterType, forceRemove, dataClusterTimeout);
	wait(impl.run());
	return impl.dataClusterUpdated;
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif