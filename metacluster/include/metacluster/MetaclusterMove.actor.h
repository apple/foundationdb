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
		state ClusterName srcName = self->srcCtx.clusterName.get();
		// Check that tenantGroup exists on src
		state bool exists = wait(
		    metadata::management::clusterTenantGroupIndex().exists(tr, Tuple::makeTuple(srcName, self->tenantGroup)));
		if (!exists) {
			TraceEvent("TenantMoveStartGroupNotOnSource")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ClusterName", srcName);
			throw invalid_tenant_move();
		}
		Optional<std::string> existingRunID =
		    wait(metadata::management::move::emergencyMovements().get(tr, self->tenantGroup));
		if (existingRunID.present() && self->runID.toString() != existingRunID.get()) {
			TraceEvent("TenantMoveStartAlreadyInProgress")
			    .detail("ExistingRunId", existingRunID)
			    .detail("NewRunId", self->runID);
			return false;
		}
		if (!existingRunID.present()) {
			metadata::management::move::emergencyMovements().set(tr, self->tenantGroup, self->runID.toString());

			// clusterCapacityIndex to accoommodate for capacity calculations
			state ClusterName dstName = self->dstCtx.clusterName.get();
			DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, dstName));
			DataClusterEntry currentEntry, updatedEntry = clusterMetadata.entry;
			updatedEntry.allocated.numTenantGroups++;
			internal::updateClusterCapacityIndex(tr, dstName, currentEntry, updatedEntry);

			// clusterTenantCount to accoommodate for capacity calculations
			int numTenants = self->tenantsInGroup.size();
			metadata::management::clusterTenantCount().atomicOp(tr, dstName, numTenants, MutationRef::AddValue);
		}
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
		// Is it possible to have no quota at all?
		return Void();
	}

	ACTOR static Future<Void> setDestinationQuota(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		loop {
			try {
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
			// In retry case, might be good to check for existence first
			createFutures.push_back(TenantAPI::createTenantTransaction(tr, entry, ClusterType::METACLUSTER_DATA));
		}
		wait(success(waitForAll(createFutures)));
		return Void();
	}

	ACTOR static Future<Void> run(StartTenantMovementImpl* self) {
		wait(self->srcCtx.initializeContext());
		wait(self->dstCtx.initializeContext());

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		bool success = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return storeRunId(self, tr); }));
		if (!success) {
			throw invalid_tenant_move();
		}

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
					TraceEvent("TenantMoveSwitchBlobbifyFailed").detail("TenantName", tName);
					throw invalid_tenant_move();
				}
			}
			state ThreadFuture<Version> resultFuture2 = dstTenant->verifyBlobRange(allKeys, latestVersion);
			Version v = wait(safeThreadFutureToFuture(resultFuture2));
			TraceEvent("TenantMoveSwitchBlobbifyVerified").detail("TenantName", tName).detail("VerifyVersion", v);
		}
		return Void();
	}

	ACTOR static Future<Void> switchMetadataToDestination(SwitchTenantMovementImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		// clusterCapacityIndex()
		DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, srcName));
		DataClusterEntry currentEntry, updatedEntry = clusterMetadata.entry;
		updatedEntry.allocated.numTenantGroups--;
		internal::updateClusterCapacityIndex(tr, srcName, currentEntry, updatedEntry);

		// clusterTenantCount()
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
				TraceEvent(SevError, "TenantMoveSwitchTenantEntryMissing")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId);
				throw invalid_tenant_move();
			}
			if (tenantEntry.get().assignedCluster != srcName) {
				TraceEvent(SevError, "TenantMoveSwitchTenantEntryWrongCluster")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId)
				    .detail("ExpectedCluster", srcName)
				    .detail("EntryCluster", tenantEntry.get().assignedCluster);
				throw invalid_tenant_move();
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
			TraceEvent(SevError, "TenantMoveSwitchGroupEntryMissing").detail("TenantGroup", self->tenantGroup);
			throw invalid_tenant_move();
		}
		if (groupEntry.get().assignedCluster != srcName) {
			TraceEvent(SevError, "TenantMoveSwitchGroupEntryIncorrectCluster")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ExpectedCluster", srcName)
			    .detail("GroupEntryAssignedCluster", groupEntry.get().assignedCluster);
			throw invalid_tenant_move();
		}
		groupEntry.get().assignedCluster = dstName;
		metadata::management::tenantMetadata().tenantGroupMap.erase(tr, self->tenantGroup);
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		return Void();
	}

	ACTOR static Future<Void> run(SwitchTenantMovementImpl* self) {
		wait(self->srcCtx.initializeContext());
		wait(self->dstCtx.initializeContext());
		bool success = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));
		if (!success) {
			throw invalid_tenant_move();
		}

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return applyHybridRanges(self, tr); }));

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return switchMetadataToDestination(self, tr); }));

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

	FinishTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup) {}

	ACTOR static Future<bool> checkRunId(FinishTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<std::string> existingRunID =
		    wait(metadata::management::move::emergencyMovements().get(tr, self->tenantGroup));
		if (!existingRunID.present()) {
			TraceEvent("TenantMoveFinishRunIDMissing").detail("NewRunId", self->runID);
			return false;
		}
		self->runID = UID::fromString(existingRunID.get());
		// if (self->runID.toString() != existingRunID.get()) {
		// 	TraceEvent("TenantMoveFinishRunIDMismatch")
		// 	    .detail("ExistingRunId", existingID)
		// 	    .detail("NewRunId", self->runID);
		// 	return false;
		// }

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

	ACTOR static Future<bool> checkTenantData(FinishTenantMovementImpl* self, TenantName tName) {
		state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tName);
		state Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);
		state Reference<ITransaction> srcTr = srcTenant->createTransaction();
		state Reference<ITransaction> dstTr = dstTenant->createTransaction();
		state KeyRef begin = ""_sr;
		state KeyRef end = "\xff"_sr;
		// what should limit be?
		state int64_t limit = 100000;
		state ThreadFuture<RangeResult> srcFuture;
		state ThreadFuture<RangeResult> dstFuture;
		state RangeResult srcRange;
		state RangeResult dstRange;
		loop {
			srcFuture = srcTr->getRange(KeyRangeRef(begin, end), limit);
			dstFuture = dstTr->getRange(KeyRangeRef(begin, end), limit);
			wait(store(srcRange, safeThreadFutureToFuture(srcFuture)) &&
			     store(dstRange, safeThreadFutureToFuture(dstFuture)));
			state int srcSize = srcRange.size();
			state int dstSize = dstRange.size();
			state int iterLen = std::min(srcSize, dstSize);
			state int index = 0;
			for (; index < iterLen; index++) {
				if (srcRange[index].key != dstRange[index].key || srcRange[index].value != dstRange[index].value) {
					return false;
				}
			}

			if (srcRange.more) {
				begin = srcRange.nextBeginKeySelector().getKey();
			} else {
				break;
			}

			/* Implementation if same size containers is not guaranteed:
			// Reached the end of at least one of the ranges after the for loop completes
			if (srcSize == dstSize) {
			    if (srcRange.more != dstRange.more) {
			        return false;
			    }
			    if (srcRange.more) {
			        begin = srcRange.nextBeginKeySelector().getKey();
			    } else {
			        // Neither range has any more so we're done verifying the data
			        break;
			    }
			} else if (dstSize < srcSize) {
			    begin = dstRange.nextBeginKeySelector().getKey();
			} else {
			    begin = srcRange.nextBeginKeySelector().getKey();
			}
			*/
		}
		return true;
	}

	ACTOR static Future<bool> checkValidUnlock(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr) {
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state int64_t tId = tenantPair.second;
			// Assert the tenant we are unlocking is on the right cluster
			Tuple indexTuple = Tuple::makeTuple(self->dstCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantClusterMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->dstCtx.clusterName.get());
				return false;
			}
			// Assert matching tenant exists on other cluster
			state TenantMapEntry srcEntry;
			wait(store(srcEntry, TenantAPI::getTenant(self->srcCtx.dataClusterDb, tName)));
			// Assert other tenant has the correct tenant group
			if (!srcEntry.tenantGroup.present()) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantGroupMissing")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}
			if (srcEntry.tenantGroup.get() != self->tenantGroup) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantGroupMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedGroup", self->tenantGroup)
				    .detail("SourceEntryTenantGroup", srcEntry.tenantGroup.get());
				return false;
			}
			// Assert other tenant is locked
			if (srcEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveFinishUnlockMatchingTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}
			// Assert tenant data matches
			bool dataMatch = wait(checkTenantData(self, tName));
			if (!dataMatch) {
				TraceEvent(SevError, "TenantMoveFinishUnlockDataMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}
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

	ACTOR static Future<bool> checkValidDelete(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr) {
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state int64_t tId = tenantPair.second;
			// Assert the tenant we are deleting is on the right cluster
			state TenantMapEntry srcEntry;
			wait(store(srcEntry, TenantAPI::getTenant(self->srcCtx.dataClusterDb, tName)));

			// Assert tenant is locked
			if (srcEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveFinishTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}

			// Assert matching tenant exists in metacluster metadata
			Tuple indexTuple = Tuple::makeTuple(self->dstCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishDeleteDataMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->dstCtx.clusterName.get());
				return false;
			}

			// Assert matching tenant exists on other cluster
			state TenantMapEntry dstEntry;
			wait(store(dstEntry, TenantAPI::getTenant(self->dstCtx.dataClusterDb, tName)));

			// Assert other tenant is unlocked
			if (dstEntry.tenantLockState != TenantAPI::TenantLockState::UNLOCKED) {
				TraceEvent(SevError, "TenantMoveFinishDstTenantNotUnlocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}

			// Assert matching tenant groups
			if (dstEntry.tenantGroup != srcEntry.tenantGroup) {
				TraceEvent(SevError, "TenantMoveFinishTenantGroupMismatch")
				    .detail("DestinationTenantGroup", dstEntry.tenantGroup)
				    .detail("SourceTenantGroup", srcEntry.tenantGroup);
				return false;
			}
		}
		return true;
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
		bool success = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));

		if (!success) {
			throw invalid_tenant_move();
		}

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		bool unlockValid = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkValidUnlock(self, tr); }));

		if (!unlockValid) {
			TraceEvent(SevError, "TenantMoveFinishUnlockInvalid")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("RunID", self->runID);
			throw invalid_tenant_move();
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

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	AbortTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup) {}

	ACTOR static Future<bool> checkRunId(AbortTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<std::string> existingRunID =
		    wait(metadata::management::move::emergencyMovements().get(tr, self->tenantGroup));
		if (!existingRunID.present()) {
			TraceEvent("TenantMoveAbortNotInProgress").detail("TenantGroup", self->tenantGroup);
			return false;
		}

		self->runID = UID::fromString(existingRunID.get());
		return true;
	}

	ACTOR static Future<Void> findTenantsInGroup(AbortTenantMovementImpl* self,
	                                             Reference<typename DB::TransactionT> tr) {
		wait(store(self->tenantsInGroup,
		           listTenantGroupTenantsTransaction(tr,
		                                             self->tenantGroup,
		                                             TenantName(""_sr),
		                                             TenantName("\xff"_sr),
		                                             CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER)));
		return Void();
	}

	static Future<Void> clearMovementMetadata(AbortTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		metadata::management::move::emergencyMovements().clear(tr);
		metadata::management::move::movementVersions().clear(tr);
		metadata::management::move::movementQueue().clear(tr);
		metadata::management::move::splitPointsMap().clear(tr);
		return Void();
	}

	ACTOR static Future<bool> checkValidUnlock(AbortTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state int64_t tId = tenantPair.second;
			// Assert the tenant we are unlocking is on the right cluster
			Tuple indexTuple = Tuple::makeTuple(self->srcCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveAbortUnlockTenantClusterMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->srcCtx.clusterName.get());
				return false;
			}
			// Assert matching tenant exists on other cluster
			state TenantMapEntry dstEntry;
			wait(store(dstEntry, TenantAPI::getTenant(self->dstCtx.dataClusterDb, tName)));
			// Assert other tenant has the correct tenant group
			if (!dstEntry.tenantGroup.present()) {
				TraceEvent(SevError, "TenantMoveAbortUnlockTenantGroupMissing")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}
			if (dstEntry.tenantGroup.get() != self->tenantGroup) {
				TraceEvent(SevError, "TenantMoveAbortUnlockTenantGroupMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedGroup", self->tenantGroup)
				    .detail("SourceEntryTenantGroup", dstEntry.tenantGroup.get());
				return false;
			}
			// Assert other tenant is locked
			if (dstEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveFinishUnlockMatchingTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}

			/* Probably don't need this for abort. Data on source should be unchanged because of locks
			// Assert tenant data matches
			bool dataMatch = wait(checkTenantData(self, tName));
			if (!dataMatch) {
			    TraceEvent(SevError, "TenantMoveAbortUnlockDataMismatch")
			        .detail("TenantName", tName)
			        .detail("TenantID", tId);
			    return false;
			}
			*/
		}
		return true;
	}

	ACTOR static Future<Void> unlockSourceTenants(AbortTenantMovementImpl* self,
	                                              Reference<typename DB::TransactionT> tr) {
		std::vector<Future<Void>> unlockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			unlockFutures.push_back(metacluster::changeTenantLockState(
			    self->srcCtx.managementDb, tenantPair.first, TenantAPI::TenantLockState::UNLOCKED, self->runID));
		}
		wait(waitForAll(unlockFutures));
		return Void();
	}

	ACTOR static Future<bool> checkValidDelete(AbortTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state int64_t tId = tenantPair.second;
			// Assert the tenant we are deleting is on the right cluster
			state TenantMapEntry dstEntry;
			wait(store(dstEntry, TenantAPI::getTenant(self->dstCtx.dataClusterDb, tName)));

			// Assert tenant is locked
			if (dstEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveAbortTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}

			// Assert matching tenant exists in metacluster metadata
			// Abort will have switched metadata to source by this point
			Tuple indexTuple = Tuple::makeTuple(self->srcCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishDeleteDataMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->srcCtx.clusterName.get());
				return false;
			}

			// Assert matching tenant exists on other cluster
			state TenantMapEntry srcEntry;
			wait(store(srcEntry, TenantAPI::getTenant(self->srcCtx.dataClusterDb, tName)));

			// Assert other tenant is unlocked
			if (srcEntry.tenantLockState != TenantAPI::TenantLockState::UNLOCKED) {
				TraceEvent(SevError, "TenantMoveFinishDstTenantNotUnlocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				return false;
			}

			// Assert matching tenant groups
			if (dstEntry.tenantGroup != srcEntry.tenantGroup) {
				TraceEvent(SevError, "TenantMoveFinishTenantGroupMismatch")
				    .detail("DestinationTenantGroup", dstEntry.tenantGroup)
				    .detail("SourceTenantGroup", srcEntry.tenantGroup);
				return false;
			}
		}
		return true;
	}

	static Future<Void> deleteDestinationData(AbortTenantMovementImpl* self, TenantName tName) {
		Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);
		Reference<ITransaction> dstTr = dstTenant->createTransaction();
		KeyRangeRef normalKeys(""_sr, "\xff"_sr);
		dstTr->clear(normalKeys);
		return Void();
	}

	ACTOR static Future<Void> deleteDestinationTenants(AbortTenantMovementImpl* self, Reference<ITransaction> tr) {
		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			TenantName tName = tenantPair.first;
			int64_t tId = tenantPair.second;
			deleteDestinationData(self, tName);
			wait(TenantAPI::deleteTenantTransaction(tr, tId));
		}
		return Void();
	}

	ACTOR static Future<Void> purgeDestinationBlobRanges(AbortTenantMovementImpl* self) {
		state KeyRange allKeys = KeyRangeRef(""_sr, "\xff"_sr);

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);
			state ThreadFuture<Key> resultFuture = dstTenant->purgeBlobGranules(allKeys, latestVersion, false);
			Key purgeKey = wait(safeThreadFutureToFuture(resultFuture));
			state ThreadFuture<Void> resultFuture2 = dstTenant->waitPurgeGranulesComplete(purgeKey);
			wait(safeThreadFutureToFuture(resultFuture2));
		}
		return Void();
	}

	ACTOR static Future<Void> switchMetadataToSource(AbortTenantMovementImpl* self,
	                                                 Reference<typename DB::TransactionT> tr) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		// clusterCapacityIndex()
		DataClusterMetadata srcClusterMetadata = wait(getClusterTransaction(tr, srcName));
		DataClusterEntry srcCurrentEntry, srcUpdatedEntry = srcClusterMetadata.entry;
		srcUpdatedEntry.allocated.numTenantGroups++;
		internal::updateClusterCapacityIndex(tr, srcName, srcCurrentEntry, srcUpdatedEntry);

		DataClusterMetadata dstClusterMetadata = wait(getClusterTransaction(tr, dstName));
		DataClusterEntry dstCurrentEntry, dstUpdatedEntry = dstClusterMetadata.entry;
		dstUpdatedEntry.allocated.numTenantGroups--;
		internal::updateClusterCapacityIndex(tr, dstName, dstCurrentEntry, dstUpdatedEntry);

		// clusterTenantCount()
		int numTenants = self->tenantsInGroup.size();
		metadata::management::clusterTenantCount().atomicOp(tr, srcName, numTenants, MutationRef::AddValue);
		metadata::management::clusterTenantCount().atomicOp(tr, dstName, -numTenants, MutationRef::AddValue);

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state int64_t tId = tenantPair.second;

			// tenantMetadata().tenantMap
			state Optional<MetaclusterTenantMapEntry> tenantEntry;
			wait(store(tenantEntry, metadata::management::tenantMetadata().tenantMap.get(tr, tId)));
			if (!tenantEntry.present()) {
				TraceEvent(SevError, "TenantMoveAbortSwitchTenantEntryMissing")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId);
				throw invalid_tenant_move();
			}
			if (tenantEntry.get().assignedCluster != dstName) {
				TraceEvent(SevError, "TenantMoveAbortSwitchTenantEntryWrongCluster")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId)
				    .detail("ExpectedCluster", srcName)
				    .detail("EntryCluster", tenantEntry.get().assignedCluster);
				throw invalid_tenant_move();
			}
			tenantEntry.get().assignedCluster = srcName;
			metadata::management::tenantMetadata().tenantMap.erase(tr, tId);
			metadata::management::tenantMetadata().tenantMap.set(tr, tId, tenantEntry.get());

			// clusterTenantIndex
			metadata::management::clusterTenantIndex().erase(tr, Tuple::makeTuple(dstName, tName, tId));
			metadata::management::clusterTenantIndex().insert(tr, Tuple::makeTuple(srcName, tName, tId));
		}
		// clusterTenantGroupIndex
		metadata::management::clusterTenantGroupIndex().erase(tr, Tuple::makeTuple(dstName, self->tenantGroup));
		metadata::management::clusterTenantGroupIndex().insert(tr, Tuple::makeTuple(srcName, self->tenantGroup));

		// tenantMetadata().tenantGroupMap
		state Optional<MetaclusterTenantGroupEntry> groupEntry;
		wait(store(groupEntry, metadata::management::tenantMetadata().tenantGroupMap.get(tr, self->tenantGroup)));
		if (!groupEntry.present()) {
			TraceEvent(SevError, "TenantMoveAbortSwitchGroupEntryMissing").detail("TenantGroup", self->tenantGroup);
			throw invalid_tenant_move();
		}
		if (groupEntry.get().assignedCluster != dstName) {
			TraceEvent(SevError, "TenantMoveAbortSwitchGroupEntryIncorrectCluster")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ExpectedCluster", dstName)
			    .detail("GroupEntryAssignedCluster", groupEntry.get().assignedCluster);
			throw invalid_tenant_move();
		}
		groupEntry.get().assignedCluster = srcName;
		metadata::management::tenantMetadata().tenantGroupMap.erase(tr, self->tenantGroup);
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		return Void();
	}

	ACTOR static Future<Void> run(AbortTenantMovementImpl* self) {
		wait(self->srcCtx.initializeContext());
		wait(self->dstCtx.initializeContext());
		bool success = wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return checkRunId(self, tr); }));
		if (!success) {
			throw invalid_tenant_move();
		}

		wait(self->srcCtx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return findTenantsInGroup(self, tr); }));

		// Determine how far in the move process we've progressed and begin unwinding

		return Void();
	}

	Future<Void> run() { return run(this); }
};

} // namespace internal

ACTOR template <class DB>
Future<Void> startTenantMovement(Reference<DB> db,
                                 TenantGroupName tenantGroup,
                                 ClusterName src,
                                 ClusterName dst,
                                 UID runID) {
	state internal::StartTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, runID);
	if (src == dst) {
		TraceEvent("TenantMoveStartSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> switchTenantMovement(Reference<DB> db, TenantGroupName tenantGroup, ClusterName src, ClusterName dst) {
	state internal::SwitchTenantMovementImpl<DB> impl(db, tenantGroup, src, dst);
	if (src == dst) {
		TraceEvent("TenantMoveSwitchSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> finishTenantMovement(Reference<DB> db, TenantGroupName tenantGroup, ClusterName src, ClusterName dst) {
	state internal::FinishTenantMovementImpl<DB> impl(db, tenantGroup, src, dst);
	if (src == dst) {
		TraceEvent("TenantMoveFinishSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> abortTenantMovement(Reference<DB> db, TenantGroupName tenantGroup, ClusterName src, ClusterName dst) {
	state internal::AbortTenantMovementImpl<DB> impl(db, tenantGroup, src, dst);
	if (src == dst) {
		TraceEvent("TenantMoveAbortSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif