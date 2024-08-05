/*
 * DeleteTenant.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_DELETETENANT_ACTOR_G_H)
#define METACLUSTER_DELETETENANT_ACTOR_G_H
#include "metacluster/DeleteTenant.actor.g.h"
#elif !defined(METACLUSTER_DELETETENANT_ACTOR_H)
#define METACLUSTER_DELETETENANT_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/UpdateTenantGroups.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

namespace internal {
template <class DB>
struct DeleteTenantImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	// Either one can be specified, and the other will be looked up
	// and filled in by reading the metacluster metadata
	Optional<TenantName> tenantName;
	int64_t tenantId = -1;

	DeleteTenantImpl(Reference<DB> managementDb, TenantName tenantName) : ctx(managementDb), tenantName(tenantName) {}
	DeleteTenantImpl(Reference<DB> managementDb, int64_t tenantId) : ctx(managementDb), tenantId(tenantId) {}

	// Loads the cluster details for the cluster where the tenant is assigned.
	// Returns true if the deletion is already in progress
	ACTOR static Future<std::pair<int64_t, bool>> getAssignedLocation(DeleteTenantImpl* self,
	                                                                  Reference<typename DB::TransactionT> tr) {
		state int64_t resolvedId = self->tenantId;
		if (self->tenantId == -1) {
			CODE_PROBE(true, "Delete tenant by name");
			ASSERT(self->tenantName.present());
			wait(store(resolvedId,
			           metadata::management::tenantMetadata().tenantNameIndex.getD(
			               tr, self->tenantName.get(), Snapshot::False, TenantInfo::INVALID_TENANT)));
		} else {
			CODE_PROBE(true, "Delete tenant by ID");
		}

		state MetaclusterTenantMapEntry tenantEntry = wait(getTenantTransaction(tr, resolvedId));

		// Disallow removing the "new" name of a renamed tenant before it completes
		if (self->tenantName.present() && tenantEntry.tenantName != self->tenantName.get()) {
			ASSERT(tenantEntry.tenantState == TenantState::RENAMING ||
			       tenantEntry.tenantState == TenantState::REMOVING);
			CODE_PROBE(true, "Delete tenant by new name during rename");
			throw tenant_not_found();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.assignedCluster));
		return std::make_pair(resolvedId, tenantEntry.tenantState == TenantState::REMOVING);
	}

	// Does an initial check if the tenant is empty. This is an optimization to prevent us marking a tenant
	// in the deleted state while it has data, but it is still possible that data gets added to it after this
	// point.
	//
	// SOMEDAY: should this also lock the tenant when locking is supported?
	ACTOR static Future<Void> checkTenantEmpty(DeleteTenantImpl* self, Reference<ITransaction> tr) {
		state Optional<TenantMapEntry> tenantEntry = wait(TenantAPI::tryGetTenantTransaction(tr, self->tenantId));
		if (!tenantEntry.present()) {
			// The tenant must have been removed simultaneously
			CODE_PROBE(true, "Tenant removed while deleting");
			return Void();
		}

		ThreadFuture<RangeResult> rangeFuture = tr->getRange(prefixRange(tenantEntry.get().prefix), 1);
		RangeResult result = wait(safeThreadFutureToFuture(rangeFuture));
		if (!result.empty()) {
			CODE_PROBE(true, "Attempt to delete non-empty tenant");
			throw tenant_not_empty();
		}

		return Void();
	}

	// Mark the tenant as being in a removing state on the management cluster
	ACTOR static Future<Void> markTenantInRemovingState(DeleteTenantImpl* self,
	                                                    Reference<typename DB::TransactionT> tr) {
		state MetaclusterTenantMapEntry tenantEntry = wait(getTenantTransaction(tr, self->tenantId));

		if (tenantEntry.tenantState != TenantState::REMOVING) {
			tenantEntry.tenantState = TenantState::REMOVING;

			metadata::management::tenantMetadata().tenantMap.set(tr, tenantEntry.id, tenantEntry);
			metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	// Delete the tenant and related metadata on the management cluster
	ACTOR static Future<Void> deleteTenantFromManagementCluster(DeleteTenantImpl* self,
	                                                            Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantId));

		if (!tenantEntry.present()) {
			CODE_PROBE(true, "Tenant removed during deletion");
			return Void();
		}

		ASSERT(tenantEntry.get().tenantState == TenantState::REMOVING);

		// Erase the tenant entry itself
		metadata::management::tenantMetadata().tenantMap.erase(tr, tenantEntry.get().id);
		metadata::management::tenantMetadata().tenantNameIndex.erase(tr, tenantEntry.get().tenantName);
		metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		// This is idempotent because this function is only called if the tenant is in the map
		metadata::management::tenantMetadata().tenantCount.atomicOp(tr, -1, MutationRef::AddValue);
		metadata::management::clusterTenantCount().atomicOp(
		    tr, tenantEntry.get().assignedCluster, -1, MutationRef::AddValue);

		// Remove the tenant from the cluster -> tenant index
		metadata::management::clusterTenantIndex().erase(
		    tr, Tuple::makeTuple(tenantEntry.get().assignedCluster, tenantEntry.get().tenantName, self->tenantId));

		if (tenantEntry.get().renameDestination.present()) {
			CODE_PROBE(true, "Remove tenant that is being renamed");
			// If renaming, remove the metadata associated with the tenant destination
			metadata::management::tenantMetadata().tenantNameIndex.erase(tr, tenantEntry.get().renameDestination.get());

			metadata::management::clusterTenantIndex().erase(tr,
			                                                 Tuple::makeTuple(tenantEntry.get().assignedCluster,
			                                                                  tenantEntry.get().renameDestination.get(),
			                                                                  TenantInfo::INVALID_TENANT));
		}

		// Remove the tenant from its tenant group
		wait(internal::managementClusterRemoveTenantFromGroup(
		    tr, tenantEntry.get(), &self->ctx.dataClusterMetadata.get()));

		return Void();
	}

	ACTOR static Future<Void> run(DeleteTenantImpl* self) {
		// Get information about the tenant and where it is assigned
		std::pair<int64_t, bool> result = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return getAssignedLocation(self, tr); }));

		if (self->tenantId == -1) {
			self->tenantId = result.first;
		} else {
			ASSERT(result.first == self->tenantId);
		}

		if (!result.second) {
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return checkTenantEmpty(self, tr); }));

			wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return markTenantInRemovingState(self, tr);
			}));
		} else {
			CODE_PROBE(true, "Resume tenant deletion");
		}

		// Delete tenant on the data cluster
		wait(self->ctx.runDataClusterTransaction([self = self](Reference<ITransaction> tr) {
			return TenantAPI::deleteTenantTransaction(tr, self->tenantId, ClusterType::METACLUSTER_DATA);
		}));
		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return deleteTenantFromManagementCluster(self, tr);
		}));

		return Void();
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, TenantName name) {
	state internal::DeleteTenantImpl<DB> impl(db, name);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, int64_t id) {
	state internal::DeleteTenantImpl<DB> impl(db, id);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif