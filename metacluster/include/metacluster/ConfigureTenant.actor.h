/*
 * ConfigureTenant.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_CONFIGURETENANT_ACTOR_G_H)
#define METACLUSTER_CONFIGURETENANT_ACTOR_G_H
#include "metacluster/ConfigureTenant.actor.g.h"
#elif !defined(METACLUSTER_CONFIGURETENANT_ACTOR_H)
#define METACLUSTER_CONFIGURETENANT_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/GetTenant.actor.h"
#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/UpdateTenantGroups.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

namespace internal {
template <class DB>
struct ConfigureTenantImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantName tenantName;
	std::map<Standalone<StringRef>, Optional<Value>> configurationParameters;
	IgnoreCapacityLimit ignoreCapacityLimit = IgnoreCapacityLimit::False;
	Optional<TenantAPI::TenantLockState> lockState;
	Optional<UID> lockId;

	// Parameters set in updateManagementCluster
	MetaclusterTenantMapEntry updatedEntry;

	ConfigureTenantImpl(Reference<DB> managementDb,
	                    TenantName tenantName,
	                    std::map<Standalone<StringRef>, Optional<Value>> configurationParameters,
	                    IgnoreCapacityLimit ignoreCapacityLimit)
	  : ctx(managementDb), tenantName(tenantName), configurationParameters(configurationParameters),
	    ignoreCapacityLimit(ignoreCapacityLimit) {}

	ConfigureTenantImpl(Reference<DB> managementDb,
	                    TenantName tenantName,
	                    TenantAPI::TenantLockState lockState,
	                    UID lockId)
	  : ctx(managementDb), tenantName(tenantName), lockState(lockState), lockId(lockId) {}

	// This verifies that the tenant group can be changed, and if so it updates all of the tenant group data
	// structures. It does not update the TenantMapEntry stored in the tenant map.
	ACTOR static Future<Void> updateTenantGroup(ConfigureTenantImpl* self,
	                                            Reference<typename DB::TransactionT> tr,
	                                            MetaclusterTenantMapEntry tenantEntry,
	                                            Optional<TenantGroupName> desiredGroup) {

		state MetaclusterTenantMapEntry entryWithUpdatedGroup = tenantEntry;
		entryWithUpdatedGroup.tenantGroup = desiredGroup;

		if (tenantEntry.tenantGroup == desiredGroup) {
			return Void();
		}

		// Removing a tenant group is only possible if we have capacity for more groups on the current cluster
		else if (!desiredGroup.present()) {
			if (!self->ctx.dataClusterMetadata.get().entry.hasCapacity() && !self->ignoreCapacityLimit) {
				throw cluster_no_capacity();
			}

			wait(internal::managementClusterRemoveTenantFromGroup(
			    tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			internal::managementClusterAddTenantToGroup(tr,
			                                            entryWithUpdatedGroup,
			                                            &self->ctx.dataClusterMetadata.get(),
			                                            GroupAlreadyExists::False,
			                                            self->ignoreCapacityLimit);
			return Void();
		}

		state Optional<MetaclusterTenantGroupEntry> tenantGroupEntry =
		    wait(metadata::management::tenantMetadata().tenantGroupMap.get(tr, desiredGroup.get()));

		// If we are creating a new tenant group, we need to have capacity on the current cluster
		if (!tenantGroupEntry.present()) {
			if (!self->ctx.dataClusterMetadata.get().entry.hasCapacity() && !self->ignoreCapacityLimit) {
				throw cluster_no_capacity();
			}
			wait(internal::managementClusterRemoveTenantFromGroup(
			    tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			internal::managementClusterAddTenantToGroup(tr,
			                                            entryWithUpdatedGroup,
			                                            &self->ctx.dataClusterMetadata.get(),
			                                            GroupAlreadyExists::False,
			                                            self->ignoreCapacityLimit);
			return Void();
		}

		// Moves between groups in the same cluster are freely allowed
		else if (tenantGroupEntry.get().assignedCluster == tenantEntry.assignedCluster) {
			wait(internal::managementClusterRemoveTenantFromGroup(
			    tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			internal::managementClusterAddTenantToGroup(tr,
			                                            entryWithUpdatedGroup,
			                                            &self->ctx.dataClusterMetadata.get(),
			                                            GroupAlreadyExists::True,
			                                            self->ignoreCapacityLimit);
			return Void();
		}

		// We don't currently support movement between groups on different clusters
		else {
			TraceEvent("TenantGroupChangeToDifferentCluster")
			    .detail("Tenant", self->tenantName)
			    .detail("OriginalGroup", tenantEntry.tenantGroup)
			    .detail("DesiredGroup", desiredGroup)
			    .detail("TenantAssignedCluster", tenantEntry.assignedCluster)
			    .detail("DesiredGroupAssignedCluster", tenantGroupEntry.get().assignedCluster);

			throw invalid_tenant_configuration();
		}
	}

	// Updates the configuration in the management cluster and marks it as being in the UPDATING_CONFIGURATION state
	ACTOR static Future<bool> updateManagementCluster(ConfigureTenantImpl* self,
	                                                  Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantName));

		if (!tenantEntry.present()) {
			throw tenant_not_found();
		}

		if (tenantEntry.get().tenantState != TenantState::READY &&
		    tenantEntry.get().tenantState != TenantState::UPDATING_CONFIGURATION) {
			throw invalid_tenant_state();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.get().assignedCluster));

		self->updatedEntry = tenantEntry.get();
		self->updatedEntry.tenantState = TenantState::UPDATING_CONFIGURATION;

		ASSERT_EQ(self->lockState.present(), self->lockId.present());
		ASSERT_NE(self->lockState.present(), self->configurationParameters.size() > 0);

		state std::map<Standalone<StringRef>, Optional<Value>>::iterator configItr;
		for (configItr = self->configurationParameters.begin(); configItr != self->configurationParameters.end();
		     ++configItr) {
			if (configItr->first == "tenant_group"_sr) {
				wait(updateTenantGroup(self, tr, self->updatedEntry, configItr->second));
			} else if (configItr->first == "assigned_cluster"_sr &&
			           configItr->second != tenantEntry.get().assignedCluster) {
				auto& newClusterName = configItr->second;
				TraceEvent(SevWarn, "CannotChangeAssignedCluster")
				    .detail("TenantName", tenantEntry.get().tenantName)
				    .detail("OriginalAssignedCluster", tenantEntry.get().assignedCluster)
				    .detail("NewAssignedCluster", newClusterName);
				throw invalid_tenant_configuration();
			}
			self->updatedEntry.configure(configItr->first, configItr->second);
		}

		if (self->lockState.present()) {
			TenantAPI::checkLockState(tenantEntry.get(), self->lockState.get(), self->lockId.get());
			self->updatedEntry.tenantLockState = self->lockState.get();
			if (self->updatedEntry.tenantLockState == TenantAPI::TenantLockState::UNLOCKED) {
				self->updatedEntry.tenantLockId = {};
			} else {
				self->updatedEntry.tenantLockId = self->lockId.get();
			}
		}

		if (self->updatedEntry.matchesConfiguration(tenantEntry.get()) &&
		    tenantEntry.get().tenantState == TenantState::READY) {
			return false;
		}

		++self->updatedEntry.configurationSequenceNum;
		ASSERT_EQ(self->updatedEntry.tenantLockState != TenantAPI::TenantLockState::UNLOCKED,
		          self->updatedEntry.tenantLockId.present());
		metadata::management::tenantMetadata().tenantMap.set(tr, self->updatedEntry.id, self->updatedEntry);
		metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		return true;
	}

	// Updates the configuration in the data cluster
	ACTOR static Future<Void> updateDataCluster(ConfigureTenantImpl* self, Reference<ITransaction> tr) {
		state Optional<TenantMapEntry> tenantEntry =
		    wait(TenantAPI::tryGetTenantTransaction(tr, self->updatedEntry.id));

		if (!tenantEntry.present() ||
		    tenantEntry.get().configurationSequenceNum >= self->updatedEntry.configurationSequenceNum) {
			// If the tenant isn't in the metacluster, it must have been concurrently removed
			return Void();
		}

		wait(TenantAPI::configureTenantTransaction(tr, tenantEntry.get(), self->updatedEntry.toTenantMapEntry()));
		return Void();
	}

	// Updates the tenant state in the management cluster to READY
	ACTOR static Future<Void> markManagementTenantAsReady(ConfigureTenantImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> tenantEntry =
		    wait(tryGetTenantTransaction(tr, self->updatedEntry.id));

		if (!tenantEntry.present() || tenantEntry.get().tenantState != TenantState::UPDATING_CONFIGURATION ||
		    tenantEntry.get().configurationSequenceNum > self->updatedEntry.configurationSequenceNum) {
			return Void();
		}

		tenantEntry.get().tenantState = TenantState::READY;
		metadata::management::tenantMetadata().tenantMap.set(tr, tenantEntry.get().id, tenantEntry.get());
		metadata::management::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		return Void();
	}

	ACTOR static Future<Void> run(ConfigureTenantImpl* self) {
		bool configUpdated = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return updateManagementCluster(self, tr); }));

		if (configUpdated) {
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return updateDataCluster(self, tr); }));
			wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return markManagementTenantAsReady(self, tr);
			}));
		}

		return Void();
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<Void> configureTenant(Reference<DB> db,
                             TenantName name,
                             std::map<Standalone<StringRef>, Optional<Value>> configurationParameters,
                             IgnoreCapacityLimit ignoreCapacityLimit) {
	state internal::ConfigureTenantImpl<DB> impl(db, name, configurationParameters, ignoreCapacityLimit);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> changeTenantLockState(Reference<DB> db,
                                   TenantName name,
                                   TenantAPI::TenantLockState lockState,
                                   UID lockId) {
	state internal::ConfigureTenantImpl<DB> impl(db, name, lockState, lockId);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif