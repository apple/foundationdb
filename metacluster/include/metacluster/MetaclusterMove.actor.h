#pragma once
#include "flow/genericactors.actor.h"
#ifndef FDB_METACLUSTER_METACLUSTERMOVE_H
#define FDB_METACLUSTER_METACLUSTERMOVE_H

#include "fdbclient/IClientApi.h"
#include "fdbclient/TagThrottle.actor.h"
#include "flow/ThreadHelper.actor.h"

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
	ClusterName src;
	ClusterName dst;
	UID runID;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;
	ThrottleApi::TagQuotaValue quota;

	StartTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        ClusterName src,
	                        ClusterName dst,
	                        UID runID)
	  : ctx(managementDb), tenantGroup(tenantGroup), src(src), dst(dst), runID(runID) {}

	ACTOR static Future<bool> storeRunId(StartTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		UID existingRunID =
		    wait(metadata::management::MovementMetadata::emergencyMovements().get(tr, self->tenantGroup));
		if (existingRunID != UID() && self->runID != existingRunID) {
			TraceEvent("TenantMoveStartAlreadyInProgress")
			    .detail("ExistingRunId", existingRunID)
			    .detail("NewRunId", self->runID);
			return false;
		}

		wait(metadata::management::MovementMetadata::emergencyMovements().set(tr, self->tenantGroup, self->runID));
		wait(tr->commit());
		return true;
	}

	ACTOR static Future<Void> storeVersion(StartTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		// check if self->ctx->dataClusterDb works correctly
		wait(self->ctx.setCluster(tr, self->src));
		Version existingVersion = wait(metadata::management::MovementMetadata::movementVersions().get(
		    tr, std::pair(self->tenantGroup, self->runID)));
		if (!existingVersion) {
			// Figure out a way to read source version
			Version sourceVersion = wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return tr->getReadVersion(); }));
			wait(metadata::management::MovementMetadata::movementVersions().set(
			    tr, std::pair(self->tenantGroup, self->runID), sourceVersion));
			wait(tr->commit());
		}
		return Void();
	}

	ACTOR static Future<Void> findTenantsInGroup(StartTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		wait(store(
		    self->tenantsInGroup,
		    listTenantGroupTenantsTransaction(tr, self->tenantGroup, TenantName(""_sr), TenantName("\xff"_sr), 10e6)));
		return Void();
	}

	ACTOR static Future<Void> lockSourceTenants(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		std::vector<Future<Void>> lockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			lockFutures.push_back(
			    TenantAPI::changeLockState(tr, tenantPair.second, TenantAPI::TenantLockState::LOCKED, self->runID));
		}
		wait(waitForAll(lockFutures));
		wait(safeThreadFutureToFuture(tr->commit()));
		return Void();
	}

	ACTOR static Future<Void> storeTenantSplitPoints(StartTenantMovementImpl* self,
	                                                 Reference<typename DB::TransactionT> tr,
	                                                 Tenant tenant,
	                                                 TenantName tenantName) {
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
			wait(metadata::management::MovementMetadata::splitPointsMap().set(
			    tr, Tuple::makeTuple(self->tenantGroup, self->runID, tenantName, lastKey), key));
			lastKey = key;
		}
		wait(tr->commit());
		return Void();
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
		wait(metadata::management::MovementMetadata::movementQueue().set(std::make_pair(self->tenantGroup, self->runID),
		                                                                 std::make_pair(firstTenant, Key())));
		wait(tr->commit());
		return Void();
	}

	ACTOR static Future<Void> createLockedDestinationTenants(StartTenantMovementImpl* self,
	                                                         Reference<typename DB::TransactionT> tr) {
		std::vector<Future<Optional<TenantMapEntry>>> createFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantMapEntry entry(tenantPair.second, tenantPair.first, self->tenantGroup);
			entry.tenantLockState = TenantAPI::TenantLockState::LOCKED;
			entry.tenantLockId = self->runID;
			createFutures.push_back(TenantAPI::createTenant(self->ctx.dataClusterDb, tenantPair.first, entry));
		}
		wait(success(waitForAll(createFutures)));
		return Void();
	}

	ACTOR static Future<Void> storeSourceQuota(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		state ThreadFuture<ValueReadResult> resultFuture = tr->get(ThrottleApi::getTagQuotaKey(self->tenantGroup));
		ValueReadResult v = wait(safeThreadFutureToFuture(resultFuture));
		self->quota = v.map([](Value val) { return ThrottleApi::TagQuotaValue::unpack(Tuple::unpack(val)); });

		// Set cluster to destination cluster prepare to set quota
		wait(self->ctx.clearCluster());
		wait(self->ctx.setCluster(tr, self->dst));
		return Void();
	}

	ACTOR static Future<Void> setDestinationQuota(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		ThrottleApi::setTagQuota(tr, self->tenantGroup, self->quota.reservedQuota, self->quota.totalQuota);
		wait(safeThreadFutureToFuture(tr->commit()));
		return Void();
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

		wait(self->ctx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return lockSourceTenants(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeAllTenantsSplitPoints(self, tr); }));

		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return createLockedDestinationTenants(self, tr);
		}));

		wait(self->ctx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return storeSourceQuota(self, tr); }));

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
	ClusterName src;
	ClusterName dst;

	// Parameters filled in during the run
	UID runID;
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	SwitchTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : ctx(managementDb), tenantGroup(tenantGroup), src(src), dst(dst) {}

	ACTOR static Future<bool> checkRunId(SwitchTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		UID existingRunID =
		    wait(metadata::management::MovementMetadata::emergencyMovements().get(tr, self->tenantGroup));
		if (existingRunID == UID()) {
			TraceEvent("TenantMoveSwitchNotInProgress").detail("TenantGroup", self->tenantGroup);
			return false;
		}

		self->runID = existingRunID;
		return true;
	}

	ACTOR static Future<Void> findTenantsInGroup(SwitchTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		wait(store(
		    self->tenantsInGroup,
		    listTenantGroupTenantsTransaction(tr, self->tenantGroup, TenantName(""_sr), TenantName("\xff"_sr), 10e6)));
		return Void();
	}

	ACTOR static Future<Void> applyHybridRanges(SwitchTenantMovementImpl* self,
	                                            Reference<typename DB::TransactionT> tr) {
		state int rangeLimit = 10e6;
		state KeyRange allKeys = KeyRangeRef(""_sr, "\xff"_sr);
		state DataClusterMetadata srcClusterMetadata = wait(getClusterTransaction(tr, self->src));
		state DataClusterMetadata dstClusterMetadata = wait(getClusterTransaction(tr, self->dst));
		state Reference<IDatabase> srcDb;
		state Reference<IDatabase> dstDb;
		wait(store(srcDb, util::openDatabase(srcClusterMetadata.connectionString)));
		wait(store(dstDb, util::openDatabase(dstClusterMetadata.connectionString)));

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			Reference<ITenant> srcTenant = srcDb->openTenant(tName);
			Reference<ITenant> dstTenant = dstDb->openTenant(tName);
			ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> resultFuture =
			    srcTenant->listBlobbifiedRanges(allKeys, rangeLimit);
			state Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(safeThreadFutureToFuture(resultFuture));
			for (auto& blobRange : blobRanges) {
				bool result = wait(safeThreadFutureToFuture(dstTenant->blobbifyRange(blobRange)));
				if (!result) {
					TraceEvent("TenantMoveBlobbifyFailed").detail("TenantName", tName).detail("BlobRange", blobRange);
					throw operation_failed();
				}
			}
			dstTenant->verifyBlobRange(allKeys, Version());
		}
		return Void();
	}

	ACTOR static Future<Void> switchMetadata(SwitchTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		// Data Structures to look out for:
		// clusterTenantIndex()
		// clusterTenantGroupIndex()
		// clusterCapacityIndex()
		// clusterTenantCount()
		// tenantMetadata().tenantMap
		// tenantMetadata().tenantNameIndex
		// tenantMetadata().tenantCount
		// tenantMetadata().tenantGroupTenantIndex
		// tenantMetadata().tenantGroupMap
		wait(delay(1.0));
		return Void();
	}

	ACTOR static Future<Void> run(SwitchTenantMovementImpl* self) {
		bool success = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));
		if (!success) {
			return Void();
		}

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return applyHybridRanges(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return switchMetadata(self, tr); }));

		return Void();
	}

	Future<Void> run() { return run(this); }
};

template <class DB>
struct FinishTenantMovementImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	ClusterName src;
	ClusterName dst;
	UID runID;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	FinishTenantMovementImpl(Reference<DB> managementDb,
	                         TenantGroupName tenantGroup,
	                         ClusterName src,
	                         ClusterName dst,
	                         UID runID)
	  : ctx(managementDb), tenantGroup(tenantGroup), src(src), dst(dst), runID(runID) {}

	ACTOR static Future<bool> checkRunId(FinishTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		UID existingRunID =
		    wait(metadata::management::MovementMetadata::emergencyMovements().get(tr, self->tenantGroup));
		if (existingRunID == UID()) {
			TraceEvent("TenantMoveFinishRunIDMissing")
			    .detail("ExistingRunId", existingRunID)
			    .detail("NewRunId", self->runID);
			return false;
		} else if ((self->runID != existingRunID)) {
			TraceEvent("TenantMoveFinishRunIDMismatch")
			    .detail("ExistingRunId", existingRunID)
			    .detail("NewRunId", self->runID);
			return false;
		}

		return true;
	}

	ACTOR static Future<Void> findTenantsInGroup(FinishTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		// After the "switch" command, this should be grabbing the tenants from the destination cluster
		// The results should be the same but it might be better to add some more assertions
		wait(store(
		    self->tenantsInGroup,
		    listTenantGroupTenantsTransaction(tr, self->tenantGroup, TenantName(""_sr), TenantName("\xff"_sr), 10e6)));
		return Void();
	}

	ACTOR static Future<bool> checkValidUnlock(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr) {
		// Assert the tenant we are unlocking is on the right cluster
		for (auto& tenantPair : self->tenantsInGroup) {
			// maybe read this from the clusterTenantIndex
		}
		// Assert matching tenant exists on other cluster
		// Assert other tenant is locked
		// Assert other tenant has the correct tenant group
		// Assert tenant data matches
		wait(delay(1.0));
		return true;
	}

	ACTOR static Future<Void> unlockDestinationTenants(FinishTenantMovementImpl* self, Reference<ITransaction> tr) {
		std::vector<Future<Void>> unlockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			unlockFutures.push_back(
			    TenantAPI::changeLockState(tr, tenantPair.second, TenantAPI::TenantLockState::UNLOCKED, self->runID));
		}
		wait(waitForAll(unlockFutures));
		wait(safeThreadFutureToFuture(tr->commit()));
		return Void();
	}

	ACTOR static Future<Void> run(FinishTenantMovementImpl* self) {
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		bool success = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkValidUnlock(self, tr); }));

		if (!success) {
			return Void();
		}

		wait(self->ctx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return unlockDestinationTenants(self, tr); }));

		return Void();
	}

	Future<Void> run() { return run(this); }
};

template <class DB>
struct AbortTenantMovementImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	ClusterName src;
	ClusterName dst;
	UID runID;

	AbortTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        ClusterName src,
	                        ClusterName dst,
	                        UID runID)
	  : ctx(managementDb), tenantGroup(tenantGroup), src(src), dst(dst), runID(runID) {}
};

} // namespace internal

ACTOR template <class DB>
Future<Void> startTenantMovement(Reference<DB> db,
                                 TenantGroupName tenantGroup,
                                 ClusterName src,
                                 ClusterName dst,
                                 UID runID) {
	state internal::StartTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, runID);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> switchTenantMovement(Reference<DB> db, TenantGroupName tenantGroup, ClusterName src, ClusterName dst) {
	state internal::SwitchTenantMovementImpl<DB> impl(db, tenantGroup, src, dst);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> finishTenantMovement(Reference<DB> db,
                                  TenantGroupName tenantGroup,
                                  ClusterName src,
                                  ClusterName dst,
                                  UID runID) {
	state internal::FinishTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, runID);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> abortTenantMovement(Reference<DB> db,
                                 TenantGroupName tenantGroup,
                                 ClusterName src,
                                 ClusterName dst,
                                 UID runID) {
	state internal::AbortTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, runID);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#endif