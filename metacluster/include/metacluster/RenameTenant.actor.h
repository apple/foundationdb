/*
 * RenameTenant.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_RENAMETENANT_ACTOR_G_H)
#define METACLUSTER_RENAMETENANT_ACTOR_G_H
#include "metacluster/RenameTenant.actor.g.h"
#elif !defined(METACLUSTER_RENAMETENANT_ACTOR_H)
#define METACLUSTER_RENAMETENANT_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

namespace internal {
template <class DB>
struct RenameTenantImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantName oldName;
	TenantName newName;

	// Parameters set in markTenantsInRenamingState
	int64_t tenantId = -1;
	int64_t configurationSequenceNum = -1;

	RenameTenantImpl(Reference<DB> managementDb, TenantName oldName, TenantName newName)
	  : ctx(managementDb), oldName(oldName), newName(newName) {}

	ACTOR static Future<Void> markTenantsInRenamingState(RenameTenantImpl* self,
	                                                     Reference<typename DB::TransactionT> tr) {
		state MetaclusterTenantMapEntry tenantEntry;
		state Optional<int64_t> newNameId;
		wait(store(tenantEntry, getTenantTransaction(tr, self->oldName)) &&
		     store(newNameId, metadata::management::tenantMetadata().tenantNameIndex.get(tr, self->newName)));

		if (self->tenantId != -1 && tenantEntry.id != self->tenantId) {
			// The tenant must have been removed simultaneously
			CODE_PROBE(true, "Metacluster rename old tenant ID mismatch");
			throw tenant_removed();
		}

		self->tenantId = tenantEntry.id;

		// If marked for deletion, abort the rename
		if (tenantEntry.tenantState == TenantState::REMOVING) {
			CODE_PROBE(true, "Metacluster rename candidates marked for deletion");
			throw tenant_removed();
		}

		if (newNameId.present() && (newNameId.get() != self->tenantId || self->oldName == self->newName)) {
			CODE_PROBE(true, "Metacluster rename new name already exists");
			throw tenant_already_exists();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.assignedCluster));

		if (tenantEntry.tenantState == TenantState::RENAMING) {
			if (tenantEntry.tenantName != self->oldName) {
				CODE_PROBE(true, "Renaming a tenant that is currently the destination of another rename");
				throw tenant_not_found();
			}
			if (tenantEntry.renameDestination.get() != self->newName) {
				CODE_PROBE(true, "Metacluster concurrent rename with different name");
				throw tenant_already_exists();
			} else {
				CODE_PROBE(true, "Metacluster rename retry in progress");
				self->configurationSequenceNum = tenantEntry.configurationSequenceNum;
				return Void();
			}
		}

		if (tenantEntry.tenantState != TenantState::READY) {
			CODE_PROBE(true, "Metacluster unable to proceed with rename operation");
			throw invalid_tenant_state();
		}

		self->configurationSequenceNum = tenantEntry.configurationSequenceNum + 1;
		// Check cluster capacity. If we would exceed the amount due to temporary extra tenants
		// then we deny the rename request altogether.
		int64_t clusterTenantCount =
		    wait(metadata::management::clusterTenantCount().getD(tr, tenantEntry.assignedCluster, Snapshot::False, 0));

		if (clusterTenantCount + 1 > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			CODE_PROBE(true, "Rename failed due to cluster capacity limit");
			throw cluster_no_capacity();
		}

		MetaclusterTenantMapEntry updatedEntry = tenantEntry;
		updatedEntry.tenantState = TenantState::RENAMING;
		updatedEntry.renameDestination = self->newName;
		updatedEntry.configurationSequenceNum = self->configurationSequenceNum;

		metadata::management::tenantMetadata().tenantMap.set(tr, self->tenantId, updatedEntry);
		metadata::management::tenantMetadata().tenantNameIndex.set(tr, self->newName, self->tenantId);
		metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		// Updated indexes to include the new tenant
		metadata::management::clusterTenantIndex().insert(
		    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->newName, TenantInfo::INVALID_TENANT));

		return Void();
	}

	ACTOR static Future<Void> updateDataCluster(RenameTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		ASSERT(self->tenantId != -1);
		ASSERT(self->configurationSequenceNum != -1);
		wait(TenantAPI::renameTenantTransaction(tr,
		                                        self->oldName,
		                                        self->newName,
		                                        self->tenantId,
		                                        ClusterType::METACLUSTER_DATA,
		                                        self->configurationSequenceNum));
		return Void();
	}

	ACTOR static Future<Void> finishRenameFromManagementCluster(RenameTenantImpl* self,
	                                                            Reference<typename DB::TransactionT> tr) {
		Optional<MetaclusterTenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantId));

		// Another (or several other) operations have already removed/changed the old entry
		// Possible for the new entry to also have been tampered with,
		// so it may or may not be present with or without the same id, which are all
		// legal states. Assume the rename completed properly in this case
		if (!tenantEntry.present() || tenantEntry.get().tenantName != self->oldName ||
		    tenantEntry.get().configurationSequenceNum > self->configurationSequenceNum) {
			CODE_PROBE(true,
			           "Metacluster finished rename with missing entries, mismatched id, and/or mismatched "
			           "configuration sequence.");
			return Void();
		}
		if (tenantEntry.get().tenantState == TenantState::REMOVING) {
			CODE_PROBE(true, "Tenant removed during rename");
			throw tenant_removed();
		}

		MetaclusterTenantMapEntry updatedEntry = tenantEntry.get();

		// Only update if in the expected state
		if (updatedEntry.tenantState == TenantState::RENAMING) {
			updatedEntry.tenantName = self->newName;
			updatedEntry.tenantState = TenantState::READY;
			updatedEntry.renameDestination.reset();
			metadata::management::tenantMetadata().tenantMap.set(tr, self->tenantId, updatedEntry);
			metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

			metadata::management::tenantMetadata().tenantNameIndex.erase(tr, self->oldName);

			// Update the tenant in the tenant group -> tenant index
			if (updatedEntry.tenantGroup.present()) {
				metadata::management::tenantMetadata().tenantGroupTenantIndex.erase(
				    tr, Tuple::makeTuple(updatedEntry.tenantGroup.get(), self->oldName, self->tenantId));
				metadata::management::tenantMetadata().tenantGroupTenantIndex.insert(
				    tr, Tuple::makeTuple(updatedEntry.tenantGroup.get(), self->newName, self->tenantId));
			}

			// Update the tenant in the cluster -> tenant index
			metadata::management::clusterTenantIndex().erase(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->oldName, self->tenantId));
			metadata::management::clusterTenantIndex().erase(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->newName, TenantInfo::INVALID_TENANT));
			metadata::management::clusterTenantIndex().insert(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->newName, self->tenantId));
		}

		return Void();
	}

	ACTOR static Future<Void> run(RenameTenantImpl* self) {
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return markTenantsInRenamingState(self, tr); }));

		// Rename tenant on the data cluster
		try {
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return updateDataCluster(self, tr); }));
		} catch (Error& e) {
			// Since we track the tenant entries on the management cluster, these error codes should only appear
			// on a retry of the transaction, typically caused by commit_unknown_result.
			// Operating on the assumption that the first transaction completed successfully, we keep going
			// so we can finish the rename on the management cluster.
			if (e.code() == error_code_tenant_not_found || e.code() == error_code_tenant_already_exists) {
				CODE_PROBE(true, "Metacluster rename ran into commit_unknown_result");
			} else {
				throw e;
			}
		}

		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return finishRenameFromManagementCluster(self, tr);
		}));
		return Void();
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<Void> renameTenant(Reference<DB> db, TenantName oldName, TenantName newName) {
	state internal::RenameTenantImpl<DB> impl(db, oldName, newName);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif