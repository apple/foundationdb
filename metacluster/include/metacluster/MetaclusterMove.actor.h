/*
 * MetaclusterMove.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_METACLUSTERMOVE_ACTOR_G_H)
#define METACLUSTER_METACLUSTERMOVE_ACTOR_G_H
#include "metacluster/MetaclusterMove.actor.g.h"
#elif !defined(METACLUSTER_METACLUSTERMOVE_H)
#define METACLUSTER_METACLUSTERMOVE_H

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/SystemData.h"
#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include <limits>
#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "fdbclient/IClientApi.h"
#include "fdbclient/TagThrottle.actor.h"
#include "flow/ThreadHelper.actor.h"

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"

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
	MetaclusterOperationContext<DB> srcCtx;
	MetaclusterOperationContext<DB> dstCtx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;
	Optional<ThrottleApi::TagQuotaValue> quota;
	Version sourceVersion;

	StartTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        ClusterName src,
	                        ClusterName dst,
	                        UID runID)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), runID(runID) {}

	ACTOR static Future<bool> storeRunId(StartTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<std::string> existingRunID =
		    wait(metadata::management::move::emergencyMovements().get(tr, self->tenantGroup));
		if (existingRunID.present() && self->runID.toString() != existingRunID.get()) {
			TraceEvent("TenantMoveStartAlreadyInProgress")
			    .detail("ExistingRunId", existingRunID)
			    .detail("NewRunId", self->runID);
			return false;
		}

		metadata::management::move::emergencyMovements().set(tr, self->tenantGroup, self->runID.toString());
		return true;
	}

	ACTOR static Future<Void> findTenantsInGroup(StartTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		wait(store(self->tenantsInGroup,
		           listTenantGroupTenantsTransaction(tr,
		                                             self->tenantGroup,
		                                             TenantName(""_sr),
		                                             TenantName("\xff"_sr),
		                                             CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER)));
		return Void();
	}

	ACTOR static Future<Void> lockSourceTenants(StartTenantMovementImpl* self) {
		std::vector<Future<Void>> lockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			lockFutures.push_back(metacluster::changeTenantLockState(
			    self->srcCtx.managementDb, tenantPair.first, TenantAPI::TenantLockState::LOCKED, self->runID));
		}
		wait(waitForAll(lockFutures));
		return Void();
	}

	ACTOR static Future<Void> getVersionFromSource(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		state ThreadFuture<Version> resultFuture = tr->getReadVersion();
		wait(store(self->sourceVersion, safeThreadFutureToFuture(resultFuture)));
		return Void();
	}

	ACTOR static Future<Void> storeVersionToManagement(StartTenantMovementImpl* self,
	                                                   Reference<typename DB::TransactionT> tr) {
		Optional<Version> existingVersion = wait(metadata::management::move::movementVersions().get(
		    tr, std::pair(self->tenantGroup, self->runID.toString())));
		if (!existingVersion.present()) {
			metadata::management::move::movementVersions().set(
			    tr, std::pair(self->tenantGroup, self->runID.toString()), self->sourceVersion);
		}
		return Void();
	}

	ACTOR static Future<Void> storeTenantSplitPoints(StartTenantMovementImpl* self,
	                                                 Reference<typename DB::TransactionT> tr,
	                                                 TenantName tenantName) {
		Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tenantName);
		Reference<ITransaction> srcTr = srcTenant->createTransaction();
		// What should chunkSize be?
		KeyRange allKeys = KeyRangeRef(""_sr, "\xff"_sr);
		int64_t chunkSize = 10000;
		ThreadFuture<Standalone<VectorRef<KeyRef>>> resultFuture = srcTr->getRangeSplitPoints(allKeys, chunkSize);
		Standalone<VectorRef<KeyRef>> splitPoints = wait(safeThreadFutureToFuture(resultFuture));
		bool first = true;
		KeyRef lastKey;
		KeyRef iterKey;
		for (auto& key : splitPoints) {
			if (first) {
				lastKey = key;
				first = false;
				continue;
			}
			metadata::management::move::splitPointsMap().set(
			    tr, Tuple::makeTuple(self->tenantGroup, self->runID.toString(), tenantName, lastKey), key);
			lastKey = key;
		}
		return Void();
	}

	ACTOR static Future<Void> storeAllTenantsSplitPoints(StartTenantMovementImpl* self,
	                                                     Reference<typename DB::TransactionT> tr) {
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			wait(storeTenantSplitPoints(self, tr, tenantPair.first));
		}
		ASSERT(self->tenantsInGroup.size());
		TenantName firstTenant = self->tenantsInGroup[0].first;

		// Set the queue head to the first tenant and an empty key
		metadata::management::move::movementQueue().set(
		    tr, std::make_pair(self->tenantGroup, self->runID.toString()), std::make_pair(firstTenant, Key()));
		return Void();
	}

	ACTOR static Future<Void> storeSourceQuota(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		state ThreadFuture<ValueReadResult> resultFuture = tr->get(ThrottleApi::getTagQuotaKey(self->tenantGroup));
		ValueReadResult v = wait(safeThreadFutureToFuture(resultFuture));
		self->quota = v.map([](Value val) { return ThrottleApi::TagQuotaValue::unpack(Tuple::unpack(val)); });
		return Void();
	}

	ACTOR static Future<Void> setDestinationQuota(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		loop {
			try {
				ASSERT(self->quota.present());
				ThrottleApi::setTagQuota(
				    tr, self->tenantGroup, self->quota.get().reservedQuota, self->quota.get().totalQuota);
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> createLockedDestinationTenants(StartTenantMovementImpl* self,
	                                                         Reference<ITransaction> tr) {
		std::vector<Future<std::pair<Optional<TenantMapEntry>, bool>>> createFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantMapEntry entry(tenantPair.second, tenantPair.first, self->tenantGroup);
			entry.tenantLockState = TenantAPI::TenantLockState::LOCKED;
			entry.tenantLockId = self->runID;
			createFutures.push_back(TenantAPI::createTenantTransaction(tr, entry, ClusterType::METACLUSTER_DATA));
		}
		wait(success(waitForAll(createFutures)));
		return Void();
	}

	ACTOR static Future<Void> run(StartTenantMovementImpl* self) {
		wait(self->srcCtx.initializeContext());
		wait(self->dstCtx.initializeContext());

		bool success = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeRunId(self, tr); }));
		if (!success) {
			return Void();
		}

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		wait(lockSourceTenants(self));

		wait(self->srcCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getVersionFromSource(self, tr); }));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeVersionToManagement(self, tr); }));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeAllTenantsSplitPoints(self, tr); }));

		wait(self->srcCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return storeSourceQuota(self, tr); }));

		wait(self->dstCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return setDestinationQuota(self, tr); }));

		wait(self->dstCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return createLockedDestinationTenants(self, tr); }));

		return Void();
	}

	Future<Void> run() { return run(this); }
};

template <class DB>
struct SwitchTenantMovementImpl {
	MetaclusterOperationContext<DB> srcCtx;
	MetaclusterOperationContext<DB> dstCtx;

	// Initialization parameters
	TenantGroupName tenantGroup;

	// Parameters filled in during the run
	UID runID;
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	SwitchTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup) {}

	ACTOR static Future<bool> checkRunId(SwitchTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<std::string> existingRunID =
		    wait(metadata::management::move::emergencyMovements().get(tr, self->tenantGroup));
		if (!existingRunID.present()) {
			TraceEvent("TenantMoveSwitchNotInProgress").detail("TenantGroup", self->tenantGroup);
			return false;
		}

		self->runID = UID::fromString(existingRunID.get());
		return true;
	}

	ACTOR static Future<Void> findTenantsInGroup(SwitchTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		wait(store(self->tenantsInGroup,
		           listTenantGroupTenantsTransaction(tr,
		                                             self->tenantGroup,
		                                             TenantName(""_sr),
		                                             TenantName("\xff"_sr),
		                                             CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER)));
		return Void();
	}

	ACTOR static Future<Void> applyHybridRanges(SwitchTenantMovementImpl* self,
	                                            Reference<typename DB::TransactionT> tr) {
		state int rangeLimit = CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER;
		state KeyRange allKeys = KeyRangeRef(""_sr, "\xff"_sr);

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tName);
			state Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);
			ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> resultFuture =
			    srcTenant->listBlobbifiedRanges(allKeys, rangeLimit);
			state Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(safeThreadFutureToFuture(resultFuture));
			for (auto& blobRange : blobRanges) {
				state ThreadFuture<bool> resultFuture = dstTenant->blobbifyRange(blobRange);
				bool result = wait(safeThreadFutureToFuture(resultFuture));
				if (!result) {
					TraceEvent("TenantMoveBlobbifyFailed").detail("TenantName", tName);
					throw operation_failed();
				}
			}
			state ThreadFuture<Version> resultFuture2 = dstTenant->verifyBlobRange(allKeys, latestVersion);
			Version v = wait(safeThreadFutureToFuture(resultFuture2));
			TraceEvent("TenantMoveBlobbifyVerified").detail("TenantName", tName).detail("VerifyVersion", v);
		}
		return Void();
	}

	ACTOR static Future<Void> switchMetadata(SwitchTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		// clusterCapacityIndex()
		// at the start, this should update the dst capacity of the cluster we're moving to by adding 1 group to it
		// remove the capacity from the old one at some point
		DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, srcName));
		DataClusterEntry currentEntry, updatedEntry = clusterMetadata.entry;
		updatedEntry.allocated.numTenantGroups--;
		internal::updateClusterCapacityIndex(tr, srcName, currentEntry, updatedEntry);

		// clusterTenantCount()
		// update at start by adding to dst as well
		int numTenants = self->tenantsInGroup.size();
		metadata::management::clusterTenantCount().atomicOp(tr, srcName, -numTenants, MutationRef::AddValue);

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state int64_t tId = tenantPair.second;

			// tenantMetadata().tenantMap
			state Optional<MetaclusterTenantMapEntry> tenantEntry;
			wait(store(tenantEntry, metadata::management::tenantMetadata().tenantMap.get(tr, tId)));
			if (!tenantEntry.present()) {
				// TODO: Throw appropriate error and log events
				throw operation_failed();
			}
			if (tenantEntry.get().assignedCluster != srcName) {
				// TODO: Throw appropriate error and log events
				throw operation_failed();
			}
			tenantEntry.get().assignedCluster = dstName;
			metadata::management::tenantMetadata().tenantMap.erase(tr, tId);
			metadata::management::tenantMetadata().tenantMap.set(tr, tId, tenantEntry.get());

			// clusterTenantIndex
			metadata::management::clusterTenantIndex().erase(tr, Tuple::makeTuple(srcName, tName, tId));
			metadata::management::clusterTenantIndex().insert(tr, Tuple::makeTuple(dstName, tName, tId));
		}
		// clusterTenantGroupIndex
		metadata::management::clusterTenantGroupIndex().erase(tr, Tuple::makeTuple(srcName, self->tenantGroup));
		metadata::management::clusterTenantGroupIndex().insert(tr, Tuple::makeTuple(dstName, self->tenantGroup));

		// tenantMetadata().tenantGroupMap
		state Optional<MetaclusterTenantGroupEntry> groupEntry;
		wait(store(groupEntry, metadata::management::tenantMetadata().tenantGroupMap.get(tr, self->tenantGroup)));
		if (!groupEntry.present()) {
			// TODO: Throw appropriate error and log events
			throw operation_failed();
		}
		if (groupEntry.get().assignedCluster != srcName) {
			// TODO: Throw appropriate error and log events
			throw operation_failed();
		}
		groupEntry.get().assignedCluster = dstName;
		metadata::management::tenantMetadata().tenantGroupMap.erase(tr, self->tenantGroup);
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		wait(safeThreadFutureToFuture(tr->commit()));
		return Void();
	}

	ACTOR static Future<Void> run(SwitchTenantMovementImpl* self) {
		wait(self->srcCtx.initializeContext());
		wait(self->dstCtx.initializeContext());
		bool success = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));
		if (!success) {
			return Void();
		}

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return applyHybridRanges(self, tr); }));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return switchMetadata(self, tr); }));

		return Void();
	}

	Future<Void> run() { return run(this); }
};

template <class DB>
struct FinishTenantMovementImpl {
	MetaclusterOperationContext<DB> srcCtx;
	MetaclusterOperationContext<DB> dstCtx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	FinishTenantMovementImpl(Reference<DB> managementDb,
	                         TenantGroupName tenantGroup,
	                         ClusterName src,
	                         ClusterName dst,
	                         UID runID)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), runID(runID) {}

	ACTOR static Future<bool> checkRunId(FinishTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<std::string> existingRunID =
		    wait(metadata::management::move::emergencyMovements().get(tr, self->tenantGroup));
		if (!existingRunID.present()) {
			TraceEvent("TenantMoveFinishRunIDMissing").detail("NewRunId", self->runID);
			return false;
		}
		UID existingID = UID::fromString(existingRunID.get());
		if (self->runID.toString() != existingRunID.get()) {
			TraceEvent("TenantMoveFinishRunIDMismatch")
			    .detail("ExistingRunId", existingID)
			    .detail("NewRunId", self->runID);
			return false;
		}

		return true;
	}

	ACTOR static Future<Void> findTenantsInGroup(FinishTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		// After the "switch" command, this should be grabbing the tenants from the destination cluster
		// The results should be the same but it might be better to add some more assertions
		wait(store(self->tenantsInGroup,
		           listTenantGroupTenantsTransaction(tr,
		                                             self->tenantGroup,
		                                             TenantName(""_sr),
		                                             TenantName("\xff"_sr),
		                                             CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER)));
		return Void();
	}

	ACTOR static Future<bool> checkValidUnlock(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr) {
		// TODO:
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state std::pair<TenantName, int64_t> stateTenantPair = tenantPair;
			// Assert the tenant we are unlocking is on the right cluster
			Tuple indexTuple =
			    Tuple::makeTuple(self->dstCtx.clusterName.get(), stateTenantPair.first, stateTenantPair.second);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				// TODO: proper error and trace
				return false;
			}
			// Assert matching tenant exists on other cluster
			state TenantMapEntry srcEntry;
			wait(store(srcEntry, TenantAPI::getTenant(self->srcCtx.dataClusterDb, stateTenantPair.first)));
			// Assert other tenant has the correct tenant group
			ASSERT(srcEntry.tenantGroup.present());
			ASSERT_EQ(srcEntry.tenantGroup.get(), self->tenantGroup);
			// Assert other tenant is locked
			ASSERT_EQ(srcEntry.tenantLockState, TenantAPI::TenantLockState::LOCKED);
			// Assert tenant data matches
		}
		return true;
	}

	ACTOR static Future<Void> unlockDestinationTenants(FinishTenantMovementImpl* self) {
		std::vector<Future<Void>> unlockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			unlockFutures.push_back(metacluster::changeTenantLockState(
			    self->srcCtx.managementDb, tenantPair.first, TenantAPI::TenantLockState::UNLOCKED, self->runID));
		}
		wait(waitForAll(unlockFutures));
		return Void();
	}

	ACTOR static Future<Void> purgeSourceBlobRanges(FinishTenantMovementImpl* self,
	                                                Reference<typename DB::TransactionT> tr) {
		state KeyRange allKeys = KeyRangeRef(""_sr, "\xff"_sr);

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tName);
			state ThreadFuture<Key> resultFuture = srcTenant->purgeBlobGranules(allKeys, latestVersion, false);
			Key purgeKey = wait(safeThreadFutureToFuture(resultFuture));
			state ThreadFuture<Void> resultFuture2 = srcTenant->waitPurgeGranulesComplete(purgeKey);
			wait(safeThreadFutureToFuture(resultFuture2));
		}
		return Void();
	}

	ACTOR static Future<bool> checkValidDelete(FinishTenantMovementImpl* self, Reference<ITransaction> tr) {
		// TODO:
		// Assert tenant exists, is locked, and is assigned to the correct cluster
		// Assert matching tenant exists on other cluster
		// Assert other tenant is unlocked
		// Assert matching tenant groups
		wait(delay(1.0));
		return false;
	}

	ACTOR static Future<Void> deleteSourceTenants(FinishTenantMovementImpl* self, Reference<ITransaction> tr) {
		std::vector<Future<Void>> deleteFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			deleteFutures.push_back(
			    TenantAPI::deleteTenantTransaction(tr, tenantPair.second, ClusterType::METACLUSTER_DATA));
		}
		wait(success(waitForAll(deleteFutures)));

		return Void();
	}

	ACTOR static Future<Void> run(FinishTenantMovementImpl* self) {
		wait(self->srcCtx.initializeContext());
		wait(self->dstCtx.initializeContext());
		bool runIDValid = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));

		if (!runIDValid) {
			return Void();
		}

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		bool unlockValid = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkValidUnlock(self, tr); }));

		if (!unlockValid) {
			return Void();
		}

		wait(unlockDestinationTenants(self));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return purgeSourceBlobRanges(self, tr); }));

		bool deleteValid = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkValidDelete(self, tr); }));

		if (!deleteValid) {
			return Void();
		}

		wait(self->dstCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return deleteSourceTenants(self, tr); }));

		return Void();
	}

	Future<Void> run() { return run(this); }
};

template <class DB>
struct AbortTenantMovementImpl {
	MetaclusterOperationContext<DB> srcCtx;
	MetaclusterOperationContext<DB> dstCtx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	UID runID;

	AbortTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        ClusterName src,
	                        ClusterName dst,
	                        UID runID)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), runID(runID) {}
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

#include "flow/unactorcompiler.h"
#endif