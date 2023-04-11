/*
 * CreateTenant.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_CREATETENANT_ACTOR_G_H)
#define METACLUSTER_CREATETENANT_ACTOR_G_H
#include "metacluster/CreateTenant.actor.g.h"
#elif !defined(METACLUSTER_CREATETENANT_ACTOR_H)
#define METACLUSTER_CREATETENANT_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/MetaclusterUtil.actor.h"
#include "metacluster/UpdateTenantGroups.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

FDB_BOOLEAN_PARAM(AssignClusterAutomatically);

namespace internal {
template <class DB>
struct CreateTenantImpl {
	MetaclusterOperationContext<DB> ctx;
	AssignClusterAutomatically assignClusterAutomatically;

	// Initialization parameters
	MetaclusterTenantMapEntry tenantEntry;

	// Parameter set if tenant creation permanently fails on the data cluster
	Optional<int64_t> replaceExistingTenantId;

	CreateTenantImpl(Reference<DB> managementDb,
	                 MetaclusterTenantMapEntry tenantEntry,
	                 AssignClusterAutomatically assignClusterAutomatically)
	  : ctx(managementDb), tenantEntry(tenantEntry), assignClusterAutomatically(assignClusterAutomatically) {}

	ACTOR static Future<ClusterName> checkClusterAvailability(Reference<IDatabase> dataClusterDb,
	                                                          ClusterName clusterName) {
		state Reference<ITransaction> tr = dataClusterDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->addWriteConflictRange(KeyRangeRef("\xff/metacluster/availability_check"_sr,
				                                      "\xff/metacluster/availability_check\x00"_sr));
				wait(safeThreadFutureToFuture(tr->commit()));
				return clusterName;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	// Returns true if the tenant is already assigned and can proceed to the next step and false if it needs
	// to be created. Throws an error if the tenant already exists and cannot be created.
	ACTOR static Future<bool> checkForExistingTenant(CreateTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		// Check if the tenant already exists. If it's partially created and matches the parameters we
		// specified, continue creating it. Otherwise, fail with an error.
		state Optional<MetaclusterTenantMapEntry> existingEntry =
		    wait(tryGetTenantTransaction(tr, self->tenantEntry.tenantName));
		if (existingEntry.present()) {
			if (!existingEntry.get().matchesConfiguration(self->tenantEntry) ||
			    existingEntry.get().tenantState != TenantState::REGISTERING) {
				// The tenant already exists and is either completely created or has a different
				// configuration
				throw tenant_already_exists();
			} else if (!self->replaceExistingTenantId.present() ||
			           self->replaceExistingTenantId.get() != existingEntry.get().id) {
				// The tenant creation has already started, so resume where we left off
				if (!self->assignClusterAutomatically &&
				    existingEntry.get().assignedCluster != self->tenantEntry.assignedCluster) {
					TraceEvent("MetaclusterCreateTenantClusterMismatch")
					    .detail("Preferred", self->tenantEntry.assignedCluster)
					    .detail("Actual", existingEntry.get().assignedCluster);
					throw invalid_tenant_configuration();
				}
				self->tenantEntry = existingEntry.get();
				wait(self->ctx.setCluster(tr, existingEntry.get().assignedCluster));
				return true;
			} else {
				// The previous creation is permanently failed, so cleanup the tenant and create it again from
				// scratch. We don't need to remove it from the tenant name index because we will overwrite the
				// existing entry later in this transaction.
				metadata::management::tenantMetadata().tenantMap.erase(tr, existingEntry.get().id);
				metadata::management::tenantMetadata().tenantCount.atomicOp(tr, -1, MutationRef::AddValue);
				metadata::management::clusterTenantCount().atomicOp(
				    tr, existingEntry.get().assignedCluster, -1, MutationRef::AddValue);

				metadata::management::clusterTenantIndex().erase(tr,
				                                                 Tuple::makeTuple(existingEntry.get().assignedCluster,
				                                                                  self->tenantEntry.tenantName,
				                                                                  existingEntry.get().id));

				state DataClusterMetadata previousAssignedClusterMetadata =
				    wait(getClusterTransaction(tr, existingEntry.get().assignedCluster));

				wait(internal::managementClusterRemoveTenantFromGroup(
				    tr, existingEntry.get(), &previousAssignedClusterMetadata));
			}
		} else if (self->replaceExistingTenantId.present()) {
			throw tenant_removed();
		}

		return false;
	}

	// Returns a pair with the name of the assigned cluster and whether the group was already assigned
	ACTOR static Future<std::pair<ClusterName, bool>> assignTenant(CreateTenantImpl* self,
	                                                               Reference<typename DB::TransactionT> tr) {
		// If our tenant group is already assigned, then we just use that assignment
		state Optional<MetaclusterTenantGroupEntry> groupEntry;
		if (self->tenantEntry.tenantGroup.present()) {
			Optional<MetaclusterTenantGroupEntry> _groupEntry = wait(
			    metadata::management::tenantMetadata().tenantGroupMap.get(tr, self->tenantEntry.tenantGroup.get()));
			groupEntry = _groupEntry;

			if (groupEntry.present()) {
				if (!self->assignClusterAutomatically &&
				    groupEntry.get().assignedCluster != self->tenantEntry.assignedCluster) {
					TraceEvent("MetaclusterCreateTenantGroupClusterMismatch")
					    .detail("TenantGroupCluster", groupEntry.get().assignedCluster)
					    .detail("SpecifiedCluster", self->tenantEntry.assignedCluster);
					throw invalid_tenant_configuration();
				}
				return std::make_pair(groupEntry.get().assignedCluster, true);
			}
		}

		state std::vector<Future<Reference<IDatabase>>> dataClusterDbs;
		state std::vector<ClusterName> dataClusterNames;
		state std::vector<Future<ClusterName>> clusterAvailabilityChecks;
		// Get a set of the most full clusters that still have capacity
		// If preferred cluster is specified, look for that one.
		if (!self->assignClusterAutomatically) {
			DataClusterMetadata dataClusterMetadata =
			    wait(getClusterTransaction(tr, self->tenantEntry.assignedCluster));
			if (!dataClusterMetadata.entry.hasCapacity()) {
				throw cluster_no_capacity();
			}
			dataClusterNames.push_back(self->tenantEntry.assignedCluster);
		} else {
			state KeyBackedSet<Tuple>::RangeResultType availableClusters =
			    wait(metadata::management::clusterCapacityIndex().getRange(
			        tr,
			        {},
			        {},
			        CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_CLUSTERS_TO_CHECK,
			        Snapshot::False,
			        Reverse::True));
			if (availableClusters.results.empty()) {
				throw metacluster_no_capacity();
			}
			for (auto const& clusterTuple : availableClusters.results) {
				dataClusterNames.push_back(clusterTuple.getString(1));
			}
		}
		for (auto const& dataClusterName : dataClusterNames) {
			dataClusterDbs.push_back(util::getAndOpenDatabase(tr, dataClusterName));
		}
		wait(waitForAll(dataClusterDbs));
		// Check the availability of our set of clusters
		for (int i = 0; i < dataClusterDbs.size(); ++i) {
			clusterAvailabilityChecks.push_back(checkClusterAvailability(dataClusterDbs[i].get(), dataClusterNames[i]));
		}

		// Wait for a successful availability check from some cluster. We prefer the most full cluster, but if it
		// doesn't return quickly we may choose another.
		Optional<Void> clusterAvailabilityCheck = wait(timeout(
		    success(clusterAvailabilityChecks[0]) || (delay(CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_FIRST_CHOICE_DELAY) &&
		                                              waitForAny(clusterAvailabilityChecks)),
		    CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_AVAILABILITY_TIMEOUT));

		if (!clusterAvailabilityCheck.present()) {
			// If no clusters were available for long enough, then we throw an error and try again
			throw transaction_too_old();
		}

		// Get the first cluster that was available
		state Optional<ClusterName> chosenCluster;
		for (auto const& f : clusterAvailabilityChecks) {
			if (f.isReady()) {
				chosenCluster = f.get();
				break;
			}
		}

		ASSERT(chosenCluster.present());
		return std::make_pair(chosenCluster.get(), false);
	}

	ACTOR static Future<Void> assignTenantAndStoreInManagementCluster(CreateTenantImpl* self,
	                                                                  Reference<typename DB::TransactionT> tr) {
		// If the tenant already exists, we either throw an error from this function or move on to the next phase
		bool tenantExists = wait(checkForExistingTenant(self, tr));
		if (tenantExists) {
			return Void();
		}

		// Choose a cluster for the tenant
		state std::pair<ClusterName, bool> assignment = wait(assignTenant(self, tr));
		self->tenantEntry.assignedCluster = assignment.first;

		// Update the context with the chosen cluster
		state Future<Void> setClusterFuture = self->ctx.setCluster(tr, assignment.first);

		// Create a tenant entry in the management cluster
		state Optional<int64_t> lastId = wait(metadata::management::tenantMetadata().lastTenantId.get(tr));
		// If the last tenant id is not present fetch the prefix from system keys and make it the prefix for the
		// next allocated tenant id
		if (!lastId.present()) {
			Optional<int64_t> tenantIdPrefix = wait(TenantMetadata::tenantIdPrefix().get(tr));
			ASSERT(tenantIdPrefix.present());
			lastId = tenantIdPrefix.get() << 48;
		}
		self->tenantEntry.setId(TenantAPI::computeNextTenantId(lastId.get(), 1));
		metadata::management::tenantMetadata().lastTenantId.set(tr, self->tenantEntry.id);

		self->tenantEntry.tenantState = TenantState::REGISTERING;
		metadata::management::tenantMetadata().tenantMap.set(tr, self->tenantEntry.id, self->tenantEntry);
		metadata::management::tenantMetadata().tenantNameIndex.set(
		    tr, self->tenantEntry.tenantName, self->tenantEntry.id);
		metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		metadata::management::tenantMetadata().tenantCount.atomicOp(tr, 1, MutationRef::AddValue);
		metadata::management::clusterTenantCount().atomicOp(
		    tr, self->tenantEntry.assignedCluster, 1, MutationRef::AddValue);

		int64_t clusterTenantCount = wait(
		    metadata::management::clusterTenantCount().getD(tr, self->tenantEntry.assignedCluster, Snapshot::False, 0));

		if (clusterTenantCount > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			throw cluster_no_capacity();
		}

		// Updated indexes to include the new tenant
		metadata::management::clusterTenantIndex().insert(
		    tr,
		    Tuple::makeTuple(self->tenantEntry.assignedCluster, self->tenantEntry.tenantName, self->tenantEntry.id));

		wait(setClusterFuture);

		// If we are part of a tenant group that is assigned to a cluster being removed from the metacluster,
		// then we fail with an error.
		if (self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING) {
			throw cluster_removed();
		} else if (self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::RESTORING) {
			throw cluster_restoring();
		}

		ASSERT(self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::READY);

		internal::managementClusterAddTenantToGroup(
		    tr, self->tenantEntry, &self->ctx.dataClusterMetadata.get(), GroupAlreadyExists(assignment.second));

		return Void();
	}

	ACTOR static Future<Void> storeTenantInDataCluster(CreateTenantImpl* self, Reference<ITransaction> tr) {
		state Future<int64_t> lastTenantIdFuture = TenantMetadata::lastTenantId().getD(tr, Snapshot::False, -1);
		state std::pair<Optional<TenantMapEntry>, bool> dataClusterTenant = wait(TenantAPI::createTenantTransaction(
		    tr, self->tenantEntry.toTenantMapEntry(), ClusterType::METACLUSTER_DATA));

		int64_t lastTenantId = wait(lastTenantIdFuture);
		if (lastTenantId < self->tenantEntry.id) {
			TenantMetadata::lastTenantId().set(tr, self->tenantEntry.id);
		}

		// If the tenant map entry is empty, then we encountered a tombstone indicating that the tenant was
		// simultaneously removed.
		if (!dataClusterTenant.first.present()) {
			throw tenant_removed();
		}

		return Void();
	}

	ACTOR static Future<Void> markTenantReady(CreateTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> managementEntry =
		    wait(tryGetTenantTransaction(tr, self->tenantEntry.id));
		if (!managementEntry.present()) {
			throw tenant_removed();
		}

		if (managementEntry.get().tenantState == TenantState::REGISTERING) {
			MetaclusterTenantMapEntry updatedEntry = managementEntry.get();
			updatedEntry.tenantState = TenantState::READY;
			metadata::management::tenantMetadata().tenantMap.set(tr, updatedEntry.id, updatedEntry);
			metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	ACTOR static Future<Void> run(CreateTenantImpl* self) {
		if (self->tenantEntry.tenantName.startsWith("\xff"_sr)) {
			throw invalid_tenant_name();
		}

		loop {
			wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return assignTenantAndStoreInManagementCluster(self, tr);
			}));

			self->replaceExistingTenantId = {};
			try {
				wait(self->ctx.runDataClusterTransaction(
				    [self = self](Reference<ITransaction> tr) { return storeTenantInDataCluster(self, tr); }));

				wait(self->ctx.runManagementTransaction(
				    [self = self](Reference<typename DB::TransactionT> tr) { return markTenantReady(self, tr); }));

				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_creation_permanently_failed) {
					// If the data cluster has permanently failed to create the tenant, then we can reassign it in
					// the management cluster and start over
					self->replaceExistingTenantId = self->tenantEntry.id;
					self->ctx.clearCluster();
				} else {
					throw;
				}
			}
		}
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<Void> createTenant(Reference<DB> db,
                          MetaclusterTenantMapEntry tenantEntry,
                          AssignClusterAutomatically assignClusterAutomatically) {
	state internal::CreateTenantImpl<DB> impl(db, tenantEntry, assignClusterAutomatically);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif