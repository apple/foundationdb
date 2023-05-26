#pragma once
#include "fdbclient/IClientApi.h"
#ifndef FDB_METACLUSTER_METACLUSTERMOVE_H
#define FDB_METACLUSTER_METACLUSTERMOVE_H

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"

#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/TenantName.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "metacluster/ListTenants.actor.h"
#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterMetadata.h"

#include "flow/actorcompiler.h" // has to be last include
namespace metacluster {
namespace internal {

template <class DB>
struct StartTenantMovementImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;
	ClusterName src;
	ClusterName dst;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	StartTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        UID runID,
	                        ClusterName src,
	                        ClusterName dst)
	  : ctx(managementDb), tenantGroup(tenantGroup), runID(runID), src(src), dst(dst) {}

	ACTOR static Future<bool> storeRunId(StartTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		UID existingRunID = metadata::management::MovementMetadata::emergencyMovements().get(tr, self->tenantGroup);
		if (existingRunID != UID() && self->runID != existingRunID) {
			TraceEvent("TenantMoveAlreadyInProgress")
			    .detail("ExistingRunId", existingRunID)
			    .detail("NewRunId", self->runID);
			return false;
		}

		metadata::management::MovementMetadata::emergencyMovements().set(tr, self->tenantGroup, self->runID);
		return true;
	}

	ACTOR static Future<Void> storeVersion(StartTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Version existingVersion = metadata::management::MovementMetadata::movementVersions().get(
		    tr, std::pair(self->tenantGroup, self->runID));
		if (!existingVersion) {
			Version sourceVersion;
			// Figure out a way to read source version
			metadata::management::MovementMetadata::movementVersions().set(
			    tr, std::pair(self->tenantGroup, self->runID), sourceVersion);
		}
	}

	ACTOR static Future<Void> findTenantsInGroup(StartTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		self->tenantsInGroup =
		    listTenantGroupTenantsTransaction(tr, self->tenantGroup, TenantName(""_sr), TenantName("\xff"_sr), 10e6);
	}

	ACTOR static Future<Void> lockSourceTenants(StartTenantMovementImpl* self,
	                                            Reference<typename DB::TransactionT> tr) {
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantAPI::changeLockState(tr, tenantPair.second, TenantAPI::TenantLockState::LOCKED, self->runID);
		}
		return Void();
	}

	ACTOR static Future<Void> storeTenantSplitPoints(StartTenantMovementImpl* self,
	                                                 Reference<typename DB::TransactionT> tr,
	                                                 Tenant tenant,
	                                                 TenantName tenantName) {
		// check if self->ctx->dataClusterDb works correctly
		wait(self->ctx.setCluster(tr, self->src));
		state Reference<ReadYourWritesTransaction> ryw =
		    makeReference<ReadYourWritesTransaction>(self->ctx.dataClusterDb, tenant);
		// What should chunkSize be?
		state Standalone<VectorRef<KeyRef>> splitPoints =
		    wait(ryw->getRangeSplitPoints(KeyRangeRef(""_sr, "\xff"_sr), 1000));
		state bool first = true;
		state KeyRef lastKey;
		for (auto& key : splitPoints) {
			if (first) {
				lastKey = key;
				first = false;
				continue;
			}
			metadata::management::MovementMetadata::splitPointsMap().set(
			    tr, Tuple::makeTuple(self->tenantGroup, self->runID, tenantName, lastKey), key);
			lastKey = key;
		}
	}

	ACTOR static Future<Void> storeAllTenantsSplitPoints(StartTenantMovementImpl* self,
	                                                     Reference<typename DB::TransactionT> tr) {
		for (auto& tenantPair : self->tenantsInGroup) {
			Tenant t(tenantPair.second);
			storeTenantSplitPoints(self, tr, t, tenantPair.first);
		}
		ASSERT(self->tenantsInGroup.size());
		TenantName firstTenant = self->tenantsInGroup[0].first;

		// Set the queue head to the first tenant and an empty key
		metadata::management::MovementMetadata::movementQueue().set(std::make_pair(self->tenantGroup, self->runID),
		                                                            std::make_pair(firstTenant, Key()));
		return Void();
	}

	ACTOR static Future<Void> createLockedDestinationTenants(StartTenantMovementImpl* self) {
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantMapEntry entry(tenantPair.second, tenantPair.first, self->tenantGroup);
			entry.tenantLockState = TenantAPI::TenantLockState::LOCKED;
			entry.tenantLockId = self->runID;
			wait(success(TenantAPI::createTenant(self->ctx.dataClusterDb, tenantPair.first, entry)));
		}
	}

	ACTOR static Future<Void> setDestinationQuota(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		// ThrottleApi::setTagQuota(tr, tag, quota.reservedQuota, quota.totalQuota);
	}

	ACTOR static Future<Void> run(StartTenantMovementImpl* self) {
		bool success = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeRunId(self, tr); }));
		if (!success) {
			return Void();
		}
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeVersion(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return lockSourceTenants(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeAllTenantsSplitPoints(self, tr); }));

		wait(createLockedDestinationTenants(self));

		wait(self->ctx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return setDestinationQuota(self, tr); }));

		return Void();
	}

	Future<Void> run() { return run(this); }
};

template <class DB>
struct SwitchTenantMovementImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;
	ClusterName src;
	ClusterName dst;

	SwitchTenantMovementImpl(Reference<DB> managementDb,
	                         TenantGroupName tenantGroup,
	                         UID runID,
	                         ClusterName src,
	                         ClusterName dst)
	  : ctx(managementDb), tenantGroup(tenantGroup), runID(runID), src(src), dst(dst) {}
};

template <class DB>
struct FinishTenantMovementImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;
	ClusterName src;
	ClusterName dst;

	FinishTenantMovementImpl(Reference<DB> managementDb,
	                         TenantGroupName tenantGroup,
	                         UID runID,
	                         ClusterName src,
	                         ClusterName dst)
	  : ctx(managementDb), tenantGroup(tenantGroup), runID(runID), src(src), dst(dst) {}
};

template <class DB>
struct AbortTenantMovementImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;
	ClusterName src;
	ClusterName dst;

	AbortTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        UID runID,
	                        ClusterName src,
	                        ClusterName dst)
	  : ctx(managementDb), tenantGroup(tenantGroup), runID(runID), src(src), dst(dst) {}
};

} // namespace internal

ACTOR template <class DB>
Future<Void> startTenantMovement(Reference<DB> db,
                                 TenantGroupName tenantGroup,
                                 UID runID,
                                 ClusterName src,
                                 ClusterName dst) {
	state internal::StartTenantMovementImpl<DB> impl(db, tenantGroup, runID, src, dst);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> switchTenantMovement(Reference<DB> db,
                                  TenantGroupName tenantGroup,
                                  UID runID,
                                  ClusterName src,
                                  ClusterName dst) {
	state internal::SwitchTenantMovementImpl<DB> impl(db, tenantGroup, runID, src, dst);
	wait(impl.run());
	return Void();
}
ACTOR template <class DB>
Future<Void> finishTenantMovement(Reference<DB> db,
                                  TenantGroupName tenantGroup,
                                  UID runID,
                                  ClusterName src,
                                  ClusterName dst) {
	state internal::FinishTenantMovementImpl<DB> impl(db, tenantGroup, runID, src, dst);
	wait(impl.run());
	return Void();
}
ACTOR template <class DB>
Future<Void> abortTenantMovement(Reference<DB> db,
                                 TenantGroupName tenantGroup,
                                 UID runID,
                                 ClusterName src,
                                 ClusterName dst) {
	state internal::AbortTenantMovementImpl<DB> impl(db, tenantGroup, runID, src, dst);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#endif