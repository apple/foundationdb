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
#include "metacluster/ConfigureCluster.h"
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

FDB_BOOLEAN_PARAM(Aborting);

ACTOR static Future<Void> updateMoveRecordState(Reference<ITransaction> tr,
                                                metadata::management::MovementState mState,
                                                TenantGroupName tenantGroup) {
	Optional<metadata::management::MovementRecord> existingMoveRec =
	    wait(metadata::management::emergency_movement::emergencyMovements().get(tr, tenantGroup));

	auto updatedMoveRec = existingMoveRec.get();
	updatedMoveRec.mState = mState;

	metadata::management::emergency_movement::emergencyMovements().set(tr, tenantGroup, updatedMoveRec);
	return Void();
}

ACTOR template <class Transaction>
static Future<Optional<metadata::management::MovementRecord>> tryGetMovementRecord(
    Reference<Transaction> tr,
    TenantGroupName tenantGroup,
    ClusterName src,
    ClusterName dst,
    Aborting aborting,
    std::set<Optional<metadata::management::MovementState>> validMovementStates =
        std::set<Optional<metadata::management::MovementState>>()) {
	Optional<metadata::management::MovementRecord> moveRecord =
	    wait(metadata::management::emergency_movement::emergencyMovements().get(tr, tenantGroup));

	if (!validMovementStates.empty()) {
		Optional<metadata::management::MovementState> movementState =
		    moveRecord.map(&metadata::management::MovementRecord::mState);

		if (!validMovementStates.count(movementState)) {
			TraceEvent("TenantMovementInInvalidState").detail("State", movementState);
			throw invalid_tenant_move();
		}
	}

	if (moveRecord.present()) {
		if (moveRecord->srcCluster != src || moveRecord->dstCluster != dst) {
			TraceEvent("TenantMoveRecordSrcDstMismatch")
			    .detail("TenantGroup", tenantGroup)
			    .detail("ExpectedSrc", src)
			    .detail("ExpectedDst", dst)
			    .detail("RecordSrc", moveRecord->srcCluster)
			    .detail("RecordDst", moveRecord->dstCluster);
			throw invalid_tenant_move();
		}

		if (moveRecord->aborting && !aborting) {
			TraceEvent("TenantMoveRecordAborting").detail("TenantGroup", tenantGroup);
			throw invalid_tenant_move();
		}
	}

	return moveRecord;
}

ACTOR template <class DB, class Function>
Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>(),
                                         Optional<metadata::management::MovementRecord>())
                    .getValue())>
moveManagementTransactionImpl(Reference<typename DB::TransactionT> tr,
                              TenantGroupName tenantGroupName,
                              ClusterName srcCluster,
                              ClusterName dstCluster,
                              Aborting aborting,
                              std::set<Optional<metadata::management::MovementState>> validMovementStates,
                              Function func) {
	Optional<metadata::management::MovementRecord> movementRecord =
	    wait(tryGetMovementRecord(tr, tenantGroupName, srcCluster, dstCluster, aborting, validMovementStates));

	decltype(std::declval<Function>()(Reference<typename DB::TransactionT>(),
	                                  Optional<metadata::management::MovementRecord>())
	             .getValue()) result = wait(func(tr, movementRecord));

	return result;
}

template <class DB, class Function>
Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>(),
                                         Optional<metadata::management::MovementRecord>())
                    .getValue())>
runMoveManagementTransaction(TenantGroupName const& tenantGroupName,
                             MetaclusterOperationContext<DB>& srcCtx,
                             MetaclusterOperationContext<DB> const& dstCtx,
                             Aborting aborting,
                             std::set<Optional<metadata::management::MovementState>> validMovementStates,
                             Function func) {
	return srcCtx.runManagementTransaction([tenantGroupName,
	                                        srcCluster = srcCtx.clusterName.get(),
	                                        dstCluster = dstCtx.clusterName.get(),
	                                        aborting,
	                                        validMovementStates,
	                                        func](Reference<typename DB::TransactionT> tr) {
		return moveManagementTransactionImpl<DB>(
		    tr, tenantGroupName, srcCluster, dstCluster, aborting, validMovementStates, func);
	});
}

ACTOR template <class Transaction>
static Future<metadata::management::MovementRecord> getMovementRecord(
    Reference<Transaction> tr,
    TenantGroupName tenantGroup,
    ClusterName src,
    ClusterName dst,
    Aborting aborting,
    std::set<Optional<metadata::management::MovementState>> validMovementStates =
        std::set<Optional<metadata::management::MovementState>>()) {
	Optional<metadata::management::MovementRecord> moveRecord =
	    wait(tryGetMovementRecord(tr, tenantGroup, src, dst, aborting, validMovementStates));

	if (!moveRecord.present()) {
		TraceEvent("TenantMoveRecordNotPresent").detail("TenantGroup", tenantGroup);
		throw invalid_tenant_move();
	}

	return moveRecord.get();
}

ACTOR template <class Transaction>
static Future<Void> findTenantsInGroup(Reference<Transaction> tr,
                                       TenantGroupName tenantGroup,
                                       std::vector<std::pair<TenantName, int64_t>>* tenantsInGroup) {
	wait(store(*tenantsInGroup,
	           listTenantGroupTenantsTransaction(
	               tr, tenantGroup, TenantName(""_sr), TenantName("\xff"_sr), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER)));
	return Void();
}

ACTOR static Future<std::vector<TenantMapEntry>> getTenantEntries(
    std::vector<std::pair<TenantName, int64_t>> tenantsInGroup,
    Reference<ITransaction> tr) {
	state std::vector<Future<TenantMapEntry>> entryFutures;
	for (const auto& tenantPair : tenantsInGroup) {
		entryFutures.push_back(TenantAPI::getTenantTransaction(tr, tenantPair.first));
	}

	wait(waitForAll(entryFutures));

	state std::vector<TenantMapEntry> results;
	for (auto const& f : entryFutures) {
		results.push_back(f.get());
	}

	return results;
}

ACTOR static Future<Void> purgeAndVerifyTenant(Reference<IDatabase> db, TenantName tenant) {
	state Reference<ITenant> tenantObj = db->openTenant(tenant);
	state ThreadFuture<Key> resultFuture = tenantObj->purgeBlobGranules(normalKeys, latestVersion, false);
	TraceEvent("BreakpointPurge4");
	state Key purgeKey = wait(safeThreadFutureToFuture(resultFuture));
	TraceEvent("BreakpointPurge5");
	// state ThreadFuture<Void> resultFuture2 = tenantObj->waitPurgeGranulesComplete(purgeKey);
	// // wait(safeThreadFutureToFuture(tenantObj->waitPurgeGranulesComplete(purgeKey)));
	// wait(safeThreadFutureToFuture(resultFuture2));
	// TraceEvent("BreakpointPurge6");
	return Void();
}

template <class DB>
struct StartTenantMovementImpl {
	MetaclusterOperationContext<DB> srcCtx;
	MetaclusterOperationContext<DB> dstCtx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	metadata::management::MovementRecord moveRecord;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;
	Version lockedVersion;
	Optional<ThrottleApi::ThroughputQuotaValue> tagQuota;
	Optional<int64_t> storageQuota;

	StartTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup) {}

	ACTOR static Future<Void> storeMoveRecord(StartTenantMovementImpl* self,
	                                          Reference<typename DB::TransactionT> tr,
	                                          Optional<metadata::management::MovementRecord> movementRecord) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();

		// Check that tenantGroup exists on src
		state bool exists = wait(
		    metadata::management::clusterTenantGroupIndex().exists(tr, Tuple::makeTuple(srcName, self->tenantGroup)));

		if (!exists) {
			TraceEvent("TenantMoveStartGroupNotOnSource")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ClusterName", srcName);
			throw invalid_tenant_move();
		}

		if (!movementRecord.present()) {
			self->moveRecord.runId = deterministicRandom()->randomUniqueID();
			self->moveRecord.srcCluster = srcName;
			self->moveRecord.dstCluster = dstName;
			self->moveRecord.mState = metadata::management::MovementState::START_LOCK;
			self->moveRecord.version = -1;
			metadata::management::emergency_movement::emergencyMovements().set(tr, self->tenantGroup, self->moveRecord);

			// clusterCapacityIndex to accommodate for capacity calculations
			DataClusterMetadata clusterMetadata = self->dstCtx.dataClusterMetadata.get();
			DataClusterEntry updatedEntry = clusterMetadata.entry;
			updatedEntry.allocated.numTenantGroups++;
			updateClusterMetadata(tr, dstName, clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

			// clusterTenantCount to accommodate for capacity calculations
			int numTenants = self->tenantsInGroup.size();
			metadata::management::clusterTenantCount().atomicOp(tr, dstName, numTenants, MutationRef::AddValue);
		} else {
			self->moveRecord = movementRecord.get();
		}

		return Void();
	}

	ACTOR static Future<Void> initializeMove(StartTenantMovementImpl* self,
	                                         Reference<typename DB::TransactionT> tr,
	                                         Optional<metadata::management::MovementRecord> movementRecord) {
		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		wait(storeMoveRecord(self, tr, movementRecord));
		return Void();
	}

	ACTOR static Future<Void> lockSourceTenants(StartTenantMovementImpl* self) {
		std::vector<Future<Void>> lockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			lockFutures.push_back(changeTenantLockState(self->srcCtx.managementDb,
			                                            tenantPair.first,
			                                            TenantAPI::TenantLockState::LOCKED,
			                                            self->moveRecord.runId));
		}

		wait(waitForAll(lockFutures));
		return Void();
	}

	ACTOR static Future<Void> getSourceClusterMetadata(StartTenantMovementImpl* self, Reference<ITransaction> tr) {
		state ThreadFuture<Version> versionFuture = tr->getReadVersion();
		wait(store(self->lockedVersion, safeThreadFutureToFuture(versionFuture)));

		state ThreadFuture<ValueReadResult> tagQuotaFuture = tr->get(ThrottleApi::getTagQuotaKey(self->tenantGroup));

		wait(store(self->storageQuota, TenantMetadata::storageQuota().get(tr, self->tenantGroup)));

		ValueReadResult v = wait(safeThreadFutureToFuture(tagQuotaFuture));
		self->tagQuota = v.map([](Value val) { return ThrottleApi::ThroughputQuotaValue::unpack(Tuple::unpack(val)); });

		return Void();
	}

	ACTOR static Future<Standalone<VectorRef<KeyRef>>> getTenantSplitPointsFromSource(StartTenantMovementImpl* self,
	                                                                                  TenantName tenantName) {
		state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tenantName);
		state Reference<ITransaction> srcTr = srcTenant->createTransaction();
		// chunkSize = 100MB, use smaller size for simulation to avoid making every range '' - \xff
		state int64_t chunkSize = (!g_network->isSimulated()) ? 1e8 : 1e3;
		loop {
			try {
				state ThreadFuture<Standalone<VectorRef<KeyRef>>> resultFuture =
				    srcTr->getRangeSplitPoints(normalKeys, chunkSize);

				Standalone<VectorRef<KeyRef>> splitPoints = wait(safeThreadFutureToFuture(resultFuture));
				Standalone<VectorRef<KeyRef>> copy = VectorRef<KeyRef>(splitPoints);
				return copy;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(srcTr->onError(e)));
			}
		}
	}

	ACTOR static Future<std::vector<std::pair<TenantName, Standalone<VectorRef<KeyRef>>>>>
	getAllTenantSplitPointsFromSource(StartTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		state std::vector<Future<Standalone<VectorRef<KeyRef>>>> getSplitPointFutures;
		ASSERT(self->tenantsInGroup.size());
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantName tenantName = tenantPair.first;
			getSplitPointFutures.push_back(getTenantSplitPointsFromSource(self, tenantName));
		}

		wait(waitForAll(getSplitPointFutures));

		state std::vector<std::pair<TenantName, Standalone<VectorRef<KeyRef>>>> result;
		state int index = 0;
		state size_t iterLen = self->tenantsInGroup.size();
		for (; index < iterLen; index++) {
			state TenantName tName = self->tenantsInGroup[index].first;
			state Standalone<VectorRef<KeyRef>> splitPoints = getSplitPointFutures[index].get();
			auto resultPair = std::make_pair(tName, splitPoints);
			result.push_back(resultPair);
		}

		return result;
	}

	void storeTenantSplitPoints(Reference<typename DB::TransactionT> tr,
	                            TenantName tenantName,
	                            Standalone<VectorRef<KeyRef>> splitPoints) {
		bool first = true;
		KeyRef lastKey;
		TraceEvent("BreakpointSplitPointSize").detail("Size", splitPoints.size());

		for (auto& key : splitPoints) {
			if (first) {
				lastKey = key;
				first = false;
				continue;
			}

			auto tupleKey = Tuple::makeTuple(tenantGroup, moveRecord.runId.toString(), tenantName, lastKey);
			TraceEvent("BreakpointStoreSplitPoint")
			    .detail("TupleKey", Tuple::tupleToString(tupleKey))
			    .detail("NextKey", key);
			metadata::management::emergency_movement::splitPointsMap().set(tr, tupleKey, key);
			lastKey = key;
		}
	}

	void storeAllTenantsSplitPoints(Reference<typename DB::TransactionT> tr,
	                                std::vector<std::pair<TenantName, Standalone<VectorRef<KeyRef>>>> allSplitPoints) {
		for (const auto& [tName, splitPoints] : allSplitPoints) {
			storeTenantSplitPoints(tr, tName, splitPoints);
		}

		TenantName firstTenant = tenantsInGroup[0].first;

		// Set the queue head to the first tenant and an empty key
		metadata::management::emergency_movement::movementQueue().set(
		    tr, std::make_pair(tenantGroup, moveRecord.runId.toString()), std::make_pair(firstTenant, Key()));
	}

	ACTOR static Future<Void> writeMovementMetadata(StartTenantMovementImpl* self,
	                                                Reference<ITransaction> tr,
	                                                Optional<metadata::management::MovementRecord> movementRecord) {
		state Future<std::vector<std::pair<TenantName, Standalone<VectorRef<KeyRef>>>>> splitPointsFuture =
		    getAllTenantSplitPointsFromSource(self, tr);

		ASSERT(movementRecord.present());
		if (movementRecord.get().mState == metadata::management::MovementState::START_LOCK) {
			auto updatedMoveRec = movementRecord.get();
			updatedMoveRec.mState = metadata::management::MovementState::START_CREATE;
			updatedMoveRec.version = self->lockedVersion;

			metadata::management::emergency_movement::emergencyMovements().set(tr, self->tenantGroup, updatedMoveRec);
			std::vector<std::pair<TenantName, Standalone<VectorRef<KeyRef>>>> allSplitPoints = wait(splitPointsFuture);
			self->storeAllTenantsSplitPoints(tr, allSplitPoints);
		}

		return Void();
	}

	void setDestinationQuota(Reference<ITransaction> tr) {
		// If source is unset, leave the destination unset too
		if (tagQuota.present()) {
			ThrottleApi::setTagQuota(tr, tenantGroup, tagQuota.get().reservedQuota, tagQuota.get().totalQuota);
		}
		if (storageQuota.present()) {
			TenantMetadata::storageQuota().set(tr, tenantGroup, storageQuota.get());
		}
	}

	ACTOR static Future<Void> createLockedDestinationTenants(StartTenantMovementImpl* self,
	                                                         Reference<ITransaction> tr) {
		state std::vector<Future<std::pair<Optional<TenantMapEntry>, bool>>> createFutures;

		Optional<int64_t> optionalMaxId = wait(TenantMetadata::lastTenantId().get(tr));
		state int64_t maxId = optionalMaxId.present() ? optionalMaxId.get() : 0;

		for (auto& tenantPair : self->tenantsInGroup) {
			maxId = std::max(tenantPair.second, maxId);
			TenantMapEntry entry(tenantPair.second, tenantPair.first, self->tenantGroup);
			entry.tenantLockState = TenantAPI::TenantLockState::LOCKED;
			entry.tenantLockId = self->moveRecord.runId;
			createFutures.push_back(TenantAPI::createTenantTransaction(tr, entry, ClusterType::METACLUSTER_DATA));
		}

		wait(waitForAll(createFutures));
		TenantMetadata::lastTenantId().set(tr, maxId);

		// Set quotas for new tenants
		self->setDestinationQuota(tr);

		return Void();
	}

	ACTOR static Future<Void> run(StartTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());

		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::False,
		                                  { Optional<metadata::management::MovementState>(),
		                                    metadata::management::MovementState::START_LOCK,
		                                    metadata::management::MovementState::START_CREATE },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return initializeMove(self, tr, movementRecord);
		                                  }));

		if (self->moveRecord.mState < metadata::management::MovementState::START_CREATE) {
			wait(lockSourceTenants(self));
		}

		wait(self->srcCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getSourceClusterMetadata(self, tr); }));

		if (self->moveRecord.mState < metadata::management::MovementState::START_CREATE) {
			wait(runMoveManagementTransaction(
			    self->tenantGroup,
			    self->srcCtx,
			    self->dstCtx,
			    Aborting::False,
			    { metadata::management::MovementState::START_LOCK, metadata::management::MovementState::START_CREATE },
			    [self = self](Reference<typename DB::TransactionT> tr,
			                  Optional<metadata::management::MovementRecord> movementRecord) {
				    return writeMovementMetadata(self, tr, movementRecord);
			    }));
		}

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
	std::vector<std::string>& messages;

	// Parameters filled in during the run
	metadata::management::MovementRecord moveRecord;
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	SwitchTenantMovementImpl(Reference<DB> managementDb,
	                         TenantGroupName tenantGroup,
	                         ClusterName src,
	                         ClusterName dst,
	                         std::vector<std::string>& messages)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), messages(messages) {}

	ACTOR static Future<Void> initSwitch(SwitchTenantMovementImpl* self,
	                                     Reference<typename DB::TransactionT> tr,
	                                     Optional<metadata::management::MovementRecord> movementRecord) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		// Check that tenantGroup exists on src
		// If it doesn't the switch may have already completed
		state bool exists = wait(
		    metadata::management::clusterTenantGroupIndex().exists(tr, Tuple::makeTuple(srcName, self->tenantGroup)));
		if (!exists) {
			TraceEvent("TenantMoveSwitchGroupNotOnSource")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ClusterName", srcName);

			throw invalid_tenant_move();
		}

		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		if (!movementRecord.present()) {
			TraceEvent("TenantMoveSwitchNoMoveRecord")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("SrcName", srcName)
			    .detail("DstName", dstName);
			throw invalid_tenant_move();
		} else {
			self->moveRecord = movementRecord.get();
		}
		return Void();
	}

	ACTOR static Future<RangeReadResult> readTenantData(Reference<ITenant> tenant,
	                                                    KeyRef begin,
	                                                    KeyRef end,
	                                                    int64_t limit) {
		state ThreadFuture<RangeReadResult> resultFuture;
		state Reference<ITransaction> tr = tenant->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::Option::LOCK_AWARE);
				resultFuture = tr->getRange(KeyRangeRef(begin, end), limit);
				RangeReadResult result = wait(safeThreadFutureToFuture(resultFuture));
				return result;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> checkTenantData(SwitchTenantMovementImpl* self, TenantName tName) {
		state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tName);
		state Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);

		state KeyRef begin = ""_sr;
		state KeyRef end = "\xff"_sr;
		// what should limit be?
		state int64_t limit = 10000;

		state RangeReadResult srcRange;
		state RangeReadResult dstRange;

		loop {
			wait(store(srcRange, readTenantData(srcTenant, begin, end, limit)) &&
			     store(dstRange, readTenantData(dstTenant, begin, end, limit)));
			if (srcRange != dstRange) {
				TraceEvent("TenantMoveSwitchDataMismatch")
				    .detail("TenantName", tName)
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("SrcSize", srcRange.size())
				    .detail("DstSize", dstRange.size());
				throw invalid_tenant_move();
			}

			if (srcRange.more) {
				begin = srcRange.nextBeginKeySelector().getKey();
			} else {
				break;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> checkAllTenantData(SwitchTenantMovementImpl* self) {
		std::vector<Future<Void>> checkFutures;
		for (const auto& tenantPair : self->tenantsInGroup) {
			checkFutures.push_back(checkTenantData(self, tenantPair.first));
		}
		wait(waitForAll(checkFutures));
		return Void();
	}

	ACTOR static Future<Void> applyHybridRanges(SwitchTenantMovementImpl* self) {
		wait(self->srcCtx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return updateMoveRecordState(tr, metadata::management::MovementState::SWITCH_HYBRID, self->tenantGroup);
		}));
		TraceEvent("TenantMoveAfterSwitchHybrid");

		// container of range-based for with continuation must be a state variable
		state std::vector<std::pair<TenantName, int64_t>> tenantsInGroup = self->tenantsInGroup;
		for (auto& tenantPair : tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tName);
			state Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);
			state ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> resultFutureSrc =
			    srcTenant->listBlobbifiedRanges(normalKeys, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
			state Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(safeThreadFutureToFuture(resultFutureSrc));

			// Blobbifying ranges is an idempotent operation
			// If retrying, re-blobbify all ranges
			state std::vector<Future<bool>> blobFutures;
			state std::vector<Future<Version>> verifyFutures;
			for (const auto& blobRange : blobRanges) {
				state ThreadFuture<bool> resultFuture = dstTenant->blobbifyRangeBlocking(blobRange);
				blobFutures.push_back(safeThreadFutureToFuture(resultFuture));
			}

			wait(waitForAll(blobFutures));

			for (const auto& blobResult : blobFutures) {
				if (!blobResult.get()) {
					TraceEvent("TenantMoveSwitchBlobbifyFailed").detail("TenantName", tName);
					throw tenant_move_failed();
				}
			}
			TraceEvent("TenantMoveSwitchBlobRangeApplied").detail("TenantName", tName);
		}
		return Void();
	}

	ACTOR static Future<Void> switchMetadataToDestination(SwitchTenantMovementImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		TraceEvent("TenantMoveSwitchBegin");

		metadata::management::MovementRecord moveRecord =
		    wait(getMovementRecord(tr, self->tenantGroup, srcName, dstName, Aborting::False));

		// Possible Retry
		if (moveRecord.mState >= metadata::management::MovementState::SWITCH_METADATA) {
			TraceEvent("TenantMoveSwitchBegin2");
			Tuple indexEntry = Tuple::makeTuple(dstName, self->tenantGroup);
			bool exists = wait(metadata::management::clusterTenantGroupIndex().exists(tr, indexEntry));
			if (exists) {
				TraceEvent("TenantMoveSwitchBegin3");
				return Void();
			}
		} else {
			TraceEvent("TenantMoveSwitchBegin4");
			wait(updateMoveRecordState(tr, metadata::management::MovementState::SWITCH_METADATA, self->tenantGroup));
		}

		state std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> tenantMetadataList;
		wait(store(tenantMetadataList, listTenantMetadataTransaction(tr, self->tenantsInGroup)));

		for (auto& tenantPair : tenantMetadataList) {
			state TenantName tName = tenantPair.first;
			state MetaclusterTenantMapEntry tenantEntry = tenantPair.second;
			state int64_t tId = tenantEntry.id;

			// tenantMetadata().tenantMap update assigned cluster
			if (tenantEntry.assignedCluster != srcName) {
				TraceEvent(SevError, "TenantMoveSwitchTenantEntryWrongCluster")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId)
				    .detail("ExpectedCluster", srcName)
				    .detail("Src", srcName)
				    .detail("Dst", dstName)
				    .detail("EntryCluster", tenantEntry.assignedCluster);
				self->messages.push_back(fmt::format("Tenant move switch wrong assigned cluster\n"
				                                     "		expected:	{}\n"
				                                     "		actual:		{}",
				                                     srcName,
				                                     tenantEntry.assignedCluster));
				throw invalid_tenant_move();
			}
			tenantEntry.assignedCluster = dstName;
			metadata::management::tenantMetadata().tenantMap.set(tr, tId, tenantEntry);

			// clusterTenantIndex erase tenant index on src, create tenant index on dst
			metadata::management::clusterTenantIndex().erase(tr, Tuple::makeTuple(srcName, tName, tId));
			metadata::management::clusterTenantIndex().insert(tr, Tuple::makeTuple(dstName, tName, tId));
		}

		// clusterTenantGroupIndex erase group index on src, create group index on dst
		metadata::management::clusterTenantGroupIndex().erase(tr, Tuple::makeTuple(srcName, self->tenantGroup));
		metadata::management::clusterTenantGroupIndex().insert(tr, Tuple::makeTuple(dstName, self->tenantGroup));

		// tenantMetadata().tenantGroupMap update assigned cluster
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
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		return Void();
	}

	ACTOR static Future<Void> run(SwitchTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());

		TraceEvent("BreakpointSwitch1");
		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::False,
		                                  { metadata::management::MovementState::START_CREATE,
		                                    metadata::management::MovementState::SWITCH_HYBRID,
		                                    metadata::management::MovementState::SWITCH_METADATA },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return initSwitch(self, tr, movementRecord);
		                                  }));

		TraceEvent("BreakpointSwitch2");
		if (self->moveRecord.mState < metadata::management::MovementState::SWITCH_HYBRID) {
			wait(checkAllTenantData(self));
		}

		TraceEvent("BreakpointSwitch3");
		if (self->moveRecord.mState < metadata::management::MovementState::SWITCH_METADATA) {
			wait(applyHybridRanges(self));
		}

		TraceEvent("BreakpointSwitch4");
		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::False,
		                                  { metadata::management::MovementState::SWITCH_HYBRID,
		                                    metadata::management::MovementState::SWITCH_METADATA },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return switchMetadataToDestination(self, tr);
		                                  }));

		TraceEvent("BreakpointSwitch5");
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
	metadata::management::MovementRecord moveRecord;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	FinishTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup) {}

	ACTOR static Future<Void> initFinish(FinishTenantMovementImpl* self,
	                                     Reference<typename DB::TransactionT> tr,
	                                     Optional<metadata::management::MovementRecord> movementRecord) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();

		// Check that tenantGroup exists on dst
		state bool exists = wait(
		    metadata::management::clusterTenantGroupIndex().exists(tr, Tuple::makeTuple(dstName, self->tenantGroup)));

		if (!exists) {
			TraceEvent("TenantMoveFinishGroupNotOnDestination")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ClusterName", dstName);
			throw invalid_tenant_move();
		}

		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		if (!movementRecord.present()) {
			TraceEvent("TenantMoveFinishNoMoveRecord")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("SrcName", srcName)
			    .detail("DstName", dstName);
			throw invalid_tenant_move();
		} else {
			self->moveRecord = movementRecord.get();
		}

		return Void();
	}

	ACTOR static Future<Void> checkValidUnlock(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr,
	                                           std::vector<TenantMapEntry> srcEntries,
	                                           std::vector<TenantMapEntry> dstEntries) {
		state size_t iterLen = self->tenantsInGroup.size();
		ASSERT_EQ(iterLen, srcEntries.size());
		ASSERT_EQ(iterLen, dstEntries.size());
		state int index = 0;
		for (; index < iterLen; index++) {
			state TenantName tName = self->tenantsInGroup[index].first;
			state int64_t tId = self->tenantsInGroup[index].second;
			state TenantMapEntry srcEntry = srcEntries[index];
			state TenantMapEntry dstEntry = dstEntries[index];

			// Assert the tenant we are unlocking is on the right cluster
			Tuple indexTuple = Tuple::makeTuple(self->dstCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantClusterMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->dstCtx.clusterName.get());
				throw invalid_tenant_move();
			}
			// Assert src tenant has the correct tenant group
			if (!srcEntry.tenantGroup.present()) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantGroupMissing")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				throw invalid_tenant_move();
			}
			if (srcEntry.tenantGroup.get() != self->tenantGroup ||
			    srcEntry.tenantGroup.get() != dstEntry.tenantGroup.get()) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantGroupMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedGroup", self->tenantGroup)
				    .detail("SourceEntryTenantGroup", srcEntry.tenantGroup.get())
				    .detail("DestinationEntryTenantGroup", dstEntry.tenantGroup.get());
				throw invalid_tenant_move();
			}
			// Assert src tenant is locked
			if (srcEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveFinishUnlockMatchingTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				throw invalid_tenant_move();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> unlockDestinationTenants(FinishTenantMovementImpl* self) {
		wait(self->srcCtx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return updateMoveRecordState(tr, metadata::management::MovementState::FINISH_UNLOCK, self->tenantGroup);
		}));
		std::vector<Future<Void>> unlockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			unlockFutures.push_back(changeTenantLockState(self->srcCtx.managementDb,
			                                              tenantPair.first,
			                                              TenantAPI::TenantLockState::UNLOCKED,
			                                              self->moveRecord.runId));
		}
		wait(waitForAll(unlockFutures));
		return Void();
	}

	ACTOR static Future<Void> purgeSourceBlobRanges(FinishTenantMovementImpl* self) {
		state std::vector<Future<Void>> purgeFutures;
		TraceEvent("BreakpointPurge1");

		for (auto& tenantPair : self->tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			purgeFutures.push_back(purgeAndVerifyTenant(self->srcCtx.dataClusterDb, tName));
		}
		TraceEvent("BreakpointPurge2");
		wait(waitForAll(purgeFutures));
		TraceEvent("BreakpointPurge3");

		return Void();
	}

	ACTOR static Future<Void> checkValidDelete(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr,
	                                           std::vector<TenantMapEntry> srcEntries,
	                                           std::vector<TenantMapEntry> dstEntries) {
		state size_t iterLen = self->tenantsInGroup.size();
		ASSERT_EQ(iterLen, srcEntries.size());
		ASSERT_EQ(iterLen, dstEntries.size());
		state int index = 0;
		for (; index < iterLen; index++) {
			state TenantName tName = self->tenantsInGroup[index].first;
			state int64_t tId = self->tenantsInGroup[index].second;
			state TenantMapEntry srcEntry = srcEntries[index];
			state TenantMapEntry dstEntry = dstEntries[index];

			// Assert src tenant is locked
			if (srcEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveFinishTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				throw invalid_tenant_move();
			}

			// Assert dst tenant exists in metacluster metadata
			Tuple indexTuple = Tuple::makeTuple(self->dstCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishDeleteDataMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->dstCtx.clusterName.get());
				throw invalid_tenant_move();
			}

			// Assert matching tenant groups
			if (dstEntry.tenantGroup != srcEntry.tenantGroup) {
				TraceEvent(SevError, "TenantMoveFinishTenantGroupMismatch")
				    .detail("DestinationTenantGroup", dstEntry.tenantGroup)
				    .detail("SourceTenantGroup", srcEntry.tenantGroup);
				throw invalid_tenant_move();
			}
		}
		return Void();
	}

	ACTOR static Future<bool> checkDestinationVersion(FinishTenantMovementImpl* self, Reference<ITransaction> tr) {
		loop {
			try {
				state ThreadFuture<Version> resultFuture = tr->getReadVersion();
				Version version = wait(safeThreadFutureToFuture(resultFuture));
				if (version > self->moveRecord.version) {
					return true;
				}
				wait(delay(1.0));
				return false;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> deleteSourceData(FinishTenantMovementImpl* self, TenantName tName) {
		state Reference<ITenant> srcTenant = self->srcCtx.dataClusterDb->openTenant(tName);
		state Reference<ITransaction> srcTr = srcTenant->createTransaction();
		loop {
			try {
				srcTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				srcTr->clear(normalKeys);
				wait(safeThreadFutureToFuture(srcTr->commit()));
				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(srcTr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> deleteAllSourceData(FinishTenantMovementImpl* self) {
		state std::vector<Future<Void>> deleteDataFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantName tName = tenantPair.first;
			int64_t tId = tenantPair.second;
			TraceEvent("BreakpointDeleteSrcData1").detail("TenantName", tName).detail("TenantId", tId);
			deleteDataFutures.push_back(deleteSourceData(self, tName));
			TraceEvent("BreakpointDeleteSrcData2").detail("TenantName", tName).detail("TenantId", tId);
		}
		wait(waitForAll(deleteDataFutures));
		TraceEvent("BreakpointDeleteSrcData3");
		return Void();
	}

	ACTOR static Future<Void> deleteSourceTenants(FinishTenantMovementImpl* self, Reference<ITransaction> tr) {
		state std::vector<Future<Void>> deleteFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			// TenantName tName = tenantPair.first;
			int64_t tId = tenantPair.second;
			deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, tId, ClusterType::METACLUSTER_DATA));
		}

		TraceEvent("BreakpointDeleteSrcTenants1");
		wait(waitForAll(deleteFutures));
		TraceEvent("BreakpointDeleteSrcTenants2");
		return Void();
	}

	void updateCapacityMetadata(Reference<typename DB::TransactionT> tr) {
		ClusterName srcName = srcCtx.clusterName.get();

		// clusterCapacityIndex() reduce allocated capacity of source
		DataClusterMetadata clusterMetadata = srcCtx.dataClusterMetadata.get();
		DataClusterEntry updatedEntry = clusterMetadata.entry;
		updatedEntry.allocated.numTenantGroups--;
		updateClusterMetadata(tr, srcName, clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		// clusterTenantCount() reduce tenant count of source
		int numTenants = tenantsInGroup.size();
		metadata::management::clusterTenantCount().atomicOp(tr, srcName, -numTenants, MutationRef::AddValue);
	}

	void clearMovementMetadata(Reference<typename DB::TransactionT> tr) {
		UID runId = moveRecord.runId;
		metadata::management::emergency_movement::emergencyMovements().erase(tr, tenantGroup);
		metadata::management::emergency_movement::movementQueue().erase(tr,
		                                                                std::make_pair(tenantGroup, runId.toString()));
		Tuple beginTuple = Tuple::makeTuple(tenantGroup, runId.toString(), TenantName(""_sr), KeyRef(""_sr));
		Tuple endTuple = Tuple::makeTuple(tenantGroup, runId.toString(), TenantName("\xff"_sr), KeyRef("\xff"_sr));
		metadata::management::emergency_movement::splitPointsMap().erase(tr, beginTuple, endTuple);
	}

	ACTOR static Future<Void> finalizeMovement(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr) {
		Optional<metadata::management::MovementRecord> movementRecord = wait(tryGetMovementRecord(
		    tr, self->tenantGroup, self->srcCtx.clusterName.get(), self->dstCtx.clusterName.get(), Aborting::False));

		if (movementRecord.present()) {
			self->updateCapacityMetadata(tr);
			self->clearMovementMetadata(tr);
		}

		return Void();
	}

	ACTOR static Future<Void> run(FinishTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());
		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::False,
		                                  { metadata::management::MovementState::SWITCH_METADATA,
		                                    metadata::management::MovementState::FINISH_UNLOCK },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return initFinish(self, tr, movementRecord);
		                                  }));
		state metadata::management::MovementState initialMoveState = self->moveRecord.mState;
		if (initialMoveState != metadata::management::MovementState::SWITCH_METADATA &&
		    initialMoveState != metadata::management::MovementState::FINISH_UNLOCK) {
			TraceEvent("TenantMoveFinishWrongInitialMoveState")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("MoveState", initialMoveState);
			throw invalid_tenant_move();
		}
		if (initialMoveState == metadata::management::MovementState::SWITCH_METADATA) {
			TraceEvent("Breakpoint1");

			state std::vector<TenantMapEntry> srcEntries = wait(self->srcCtx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return getTenantEntries(self->tenantsInGroup, tr); }));
			TraceEvent("Breakpoint2");
			state std::vector<TenantMapEntry> dstEntries = wait(self->dstCtx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return getTenantEntries(self->tenantsInGroup, tr); }));
			TraceEvent("Breakpoint3");
			loop {
				bool versionReady = wait(self->dstCtx.runDataClusterTransaction(
				    [self = self](Reference<ITransaction> tr) { return checkDestinationVersion(self, tr); }));
				if (versionReady) {
					break;
				}
			}
			TraceEvent("Breakpoint4");

			wait(runMoveManagementTransaction(self->tenantGroup,
			                                  self->srcCtx,
			                                  self->dstCtx,
			                                  Aborting::False,
			                                  { metadata::management::MovementState::SWITCH_METADATA },
			                                  [self = self, srcEntries = srcEntries, dstEntries = dstEntries](
			                                      Reference<typename DB::TransactionT> tr,
			                                      Optional<metadata::management::MovementRecord> movementRecord) {
				                                  return checkValidUnlock(self, tr, srcEntries, dstEntries);
			                                  }));
			TraceEvent("Breakpoint5");
			wait(runMoveManagementTransaction(self->tenantGroup,
			                                  self->srcCtx,
			                                  self->dstCtx,
			                                  Aborting::False,
			                                  { metadata::management::MovementState::SWITCH_METADATA },
			                                  [self = self, srcEntries = srcEntries, dstEntries = dstEntries](
			                                      Reference<typename DB::TransactionT> tr,
			                                      Optional<metadata::management::MovementRecord> movementRecord) {
				                                  return checkValidDelete(self, tr, srcEntries, dstEntries);
			                                  }));
		}

		TraceEvent("Breakpoint6");
		wait(unlockDestinationTenants(self));

		try {
			TraceEvent("Breakpoint7");
			wait(purgeSourceBlobRanges(self));

			TraceEvent("Breakpoint8");
			wait(deleteAllSourceData(self));

			TraceEvent("Breakpoint9");
			wait(self->srcCtx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return deleteSourceTenants(self, tr); }));
		} catch (Error& e) {
			TraceEvent("TenantMoveFinishDeleteError").error(e);
			// If tenant is not found, this is a retry and the tenants have been deleted already
			if (e.code() != error_code_tenant_not_found) {
				throw e;
			}
		}

		TraceEvent("Breakpoint10");

		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::False,
		                                  { metadata::management::MovementState::FINISH_UNLOCK },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return finalizeMovement(self, tr);
		                                  }));

		TraceEvent("Breakpoint11");

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
	metadata::management::MovementRecord moveRecord;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	AbortTenantMovementImpl(Reference<DB> managementDb, TenantGroupName tenantGroup, ClusterName src, ClusterName dst)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup) {}

	ACTOR static Future<Void> initAbort(AbortTenantMovementImpl* self,
	                                    Reference<typename DB::TransactionT> tr,
	                                    Optional<metadata::management::MovementRecord> movementRecord) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		if (!movementRecord.present()) {
			TraceEvent("TenantMoveSwitchNoMoveRecord")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("SrcName", srcName)
			    .detail("DstName", dstName);
			throw invalid_tenant_move();
		} else {
			self->moveRecord = movementRecord.get();
		}
		if (self->moveRecord.mState == metadata::management::MovementState::FINISH_UNLOCK) {
			TraceEvent("TenantMoveAbortNotAllowedAfterDestUnlocked");
			throw invalid_tenant_move();
		}
		// Mark movement as aborting and write it into metadata immediately
		self->moveRecord.aborting = true;
		metadata::management::emergency_movement::emergencyMovements().set(tr, self->tenantGroup, self->moveRecord);
		return Void();
	}

	static Future<Void> clearMovementMetadata(AbortTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		UID runId = self->moveRecord.runId;
		metadata::management::emergency_movement::emergencyMovements().erase(tr, self->tenantGroup);
		metadata::management::emergency_movement::movementQueue().erase(
		    tr, std::make_pair(self->tenantGroup, runId.toString()));
		Tuple beginTuple = Tuple::makeTuple(self->tenantGroup, runId.toString(), TenantName(""_sr), KeyRef(""_sr));
		Tuple endTuple =
		    Tuple::makeTuple(self->tenantGroup, runId.toString(), TenantName("\xff"_sr), KeyRef("\xff"_sr));
		metadata::management::emergency_movement::splitPointsMap().erase(tr, beginTuple, endTuple);

		// Decrease clusterTenantCount and clusterCapacityIndex of destination here since it was
		// pre-emptively increased for capacity purposes in the Start step
		ClusterName dstName = self->dstCtx.clusterName.get();

		DataClusterMetadata clusterMetadata = self->dstCtx.dataClusterMetadata.get();
		DataClusterEntry updatedEntry = clusterMetadata.entry;
		updatedEntry.allocated.numTenantGroups--;
		updateClusterMetadata(tr, dstName, clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		int numTenants = self->tenantsInGroup.size();
		metadata::management::clusterTenantCount().atomicOp(tr, dstName, -numTenants, MutationRef::AddValue);
		return Void();
	}

	// Don't check for matching destination entries because they will have been deleted or not yet created
	ACTOR static Future<Void> checkValidUnlock(AbortTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr,
	                                           std::vector<TenantMapEntry> srcEntries) {
		state size_t iterLen = self->tenantsInGroup.size();
		ASSERT_EQ(iterLen, srcEntries.size());
		state int index = 0;
		for (; index < iterLen; index++) {
			state TenantName tName = self->tenantsInGroup[index].first;
			state int64_t tId = self->tenantsInGroup[index].second;
			state TenantMapEntry srcEntry = srcEntries[index];

			// Assert the tenant we are unlocking is on the right cluster
			Tuple indexTuple = Tuple::makeTuple(self->srcCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveAbortUnlockTenantClusterMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->srcCtx.clusterName.get());
				throw invalid_tenant_move();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> unlockSourceTenants(AbortTenantMovementImpl* self) {
		std::vector<Future<Void>> unlockFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			unlockFutures.push_back(changeTenantLockState(self->srcCtx.managementDb,
			                                              tenantPair.first,
			                                              TenantAPI::TenantLockState::UNLOCKED,
			                                              self->moveRecord.runId));
		}
		wait(waitForAll(unlockFutures));
		return Void();
	}

	ACTOR static Future<Void> checkValidDelete(AbortTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr,
	                                           std::vector<TenantMapEntry> srcEntries,
	                                           std::vector<TenantMapEntry> dstEntries) {
		state size_t iterLen = self->tenantsInGroup.size();
		ASSERT_EQ(iterLen, srcEntries.size());
		ASSERT_EQ(iterLen, dstEntries.size());
		state int index = 0;
		for (; index < iterLen; index++) {
			state TenantName tName = self->tenantsInGroup[index].first;
			state int64_t tId = self->tenantsInGroup[index].second;
			state TenantMapEntry srcEntry = srcEntries[index];
			state TenantMapEntry dstEntry = dstEntries[index];

			// Assert dst tenant is locked
			if (dstEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveAbortTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				throw invalid_tenant_move();
			}

			// Assert src tenant exists in metacluster metadata
			// Abort will have switched metadata to source by this point
			Tuple indexTuple = Tuple::makeTuple(self->srcCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishDeleteNoMatchingTenant")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->srcCtx.clusterName.get());
				throw invalid_tenant_move();
			}

			// Assert matching tenant groups
			if (dstEntry.tenantGroup != srcEntry.tenantGroup) {
				TraceEvent(SevError, "TenantMoveFinishTenantGroupMismatch")
				    .detail("DestinationTenantGroup", dstEntry.tenantGroup)
				    .detail("SourceTenantGroup", srcEntry.tenantGroup);
				throw invalid_tenant_move();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> deleteDestinationData(AbortTenantMovementImpl* self, TenantName tName) {
		state Reference<ITenant> dstTenant = self->dstCtx.dataClusterDb->openTenant(tName);
		state Reference<ITransaction> dstTr = dstTenant->createTransaction();
		loop {
			try {
				dstTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				dstTr->clear(normalKeys);
				wait(safeThreadFutureToFuture(dstTr->commit()));
				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(dstTr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> deleteAllDestinationData(AbortTenantMovementImpl* self) {
		state std::vector<Future<Void>> deleteDataFutures;
		for (auto& tenantPair : self->tenantsInGroup) {
			TenantName tName = tenantPair.first;
			deleteDataFutures.push_back(deleteDestinationData(self, tName));
		}
		wait(waitForAll(deleteDataFutures));
		return Void();
	}

	ACTOR static Future<Void> deleteDestinationTenants(AbortTenantMovementImpl* self, Reference<ITransaction> tr) {
		state std::vector<Future<Void>> deleteFutures;

		for (auto& tenantPair : self->tenantsInGroup) {
			int64_t tId = tenantPair.second;
			deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, tId, ClusterType::METACLUSTER_DATA));
		}

		wait(waitForAll(deleteFutures));
		return Void();
	}

	ACTOR static Future<Void> purgeDestinationBlobRanges(AbortTenantMovementImpl* self) {
		state std::vector<Future<Void>> purgeFutures;

		for (auto& tenantPair : self->tenantsInGroup) {
			state TenantName tName = tenantPair.first;
			purgeFutures.push_back(purgeAndVerifyTenant(self->dstCtx.dataClusterDb, tName));
		}
		wait(waitForAll(purgeFutures));

		return Void();
	}

	ACTOR static Future<Void> switchMetadataToSource(AbortTenantMovementImpl* self,
	                                                 Reference<typename DB::TransactionT> tr) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();

		state std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> tenantMetadataList;
		wait(store(tenantMetadataList, listTenantMetadataTransaction(tr, self->tenantsInGroup)));
		for (auto& tenantPair : tenantMetadataList) {
			state TenantName tName = tenantPair.first;
			state MetaclusterTenantMapEntry tenantEntry = tenantPair.second;
			state int64_t tId = tenantEntry.id;

			// tenantMetadata().tenantMap update assigned cluster
			if (tenantEntry.assignedCluster != dstName) {
				TraceEvent(SevError, "TenantMoveAbortSwitchTenantEntryWrongCluster")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId)
				    .detail("ExpectedCluster", dstName)
				    .detail("EntryCluster", tenantEntry.assignedCluster);
				throw invalid_tenant_move();
			}
			tenantEntry.assignedCluster = srcName;
			metadata::management::tenantMetadata().tenantMap.set(tr, tId, tenantEntry);

			// clusterTenantIndex erase tenant index on dst, create tenant index on src
			metadata::management::clusterTenantIndex().erase(tr, Tuple::makeTuple(dstName, tName, tId));
			metadata::management::clusterTenantIndex().insert(tr, Tuple::makeTuple(srcName, tName, tId));
		}

		// clusterTenantGroupIndex erase group index on dst, create group index on src
		metadata::management::clusterTenantGroupIndex().erase(tr, Tuple::makeTuple(dstName, self->tenantGroup));
		metadata::management::clusterTenantGroupIndex().insert(tr, Tuple::makeTuple(srcName, self->tenantGroup));

		// tenantMetadata().tenantGroupMap update assigned cluster
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
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		return Void();
	}

	ACTOR static Future<Void> abortStartLock(AbortTenantMovementImpl* self) {
		state std::vector<TenantMapEntry> srcEntries = wait(self->srcCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getTenantEntries(self->tenantsInGroup, tr); }));

		wait(runMoveManagementTransaction(
		    self->tenantGroup,
		    self->srcCtx,
		    self->dstCtx,
		    Aborting::True,
		    { metadata::management::MovementState::START_LOCK },
		    [self = self, srcEntries = srcEntries](Reference<typename DB::TransactionT> tr,
		                                           Optional<metadata::management::MovementRecord> movementRecord) {
			    return checkValidUnlock(self, tr, srcEntries);
		    }));

		wait(unlockSourceTenants(self));

		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::True,
		                                  { metadata::management::MovementState::START_LOCK },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return clearMovementMetadata(self, tr);
		                                  }));

		return Void();
	}

	ACTOR static Future<Void> abortStartCreate(AbortTenantMovementImpl* self) {
		// If no tenant entries exist on dst, they are already deleted or were never created
		state std::vector<TenantMapEntry> dstEntries;
		state bool runDelete = true;
		try {
			wait(store(dstEntries, self->dstCtx.runDataClusterTransaction([self = self](Reference<ITransaction> tr) {
				return getTenantEntries(self->tenantsInGroup, tr);
			})));
		} catch (Error& e) {
			if (e.code() != error_code_tenant_not_found) {
				throw;
			}
			runDelete = false;
		}
		if (runDelete) {
			state std::vector<TenantMapEntry> srcEntries = wait(self->srcCtx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return getTenantEntries(self->tenantsInGroup, tr); }));

			wait(runMoveManagementTransaction(self->tenantGroup,
			                                  self->srcCtx,
			                                  self->dstCtx,
			                                  Aborting::True,
			                                  { metadata::management::MovementState::START_CREATE },
			                                  [self = self, srcEntries = srcEntries, dstEntries = dstEntries](
			                                      Reference<typename DB::TransactionT> tr,
			                                      Optional<metadata::management::MovementRecord> movementRecord) {
				                                  return checkValidDelete(self, tr, srcEntries, dstEntries);
			                                  }));
			wait(deleteAllDestinationData(self));
			wait(self->dstCtx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return deleteDestinationTenants(self, tr); }));
		}

		// Update state and unwind with other steps
		wait(self->srcCtx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return updateMoveRecordState(tr, metadata::management::MovementState::START_LOCK, self->tenantGroup);
		}));
		wait(abortStartLock(self));
		return Void();
	}

	ACTOR static Future<Void> abortSwitchHybrid(AbortTenantMovementImpl* self) {
		// Okay to run even if step is uncompleted or partially completed
		wait(purgeDestinationBlobRanges(self));

		// Update state and unwind with other steps
		wait(self->srcCtx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return updateMoveRecordState(tr, metadata::management::MovementState::START_CREATE, self->tenantGroup);
		}));
		wait(abortStartCreate(self));
		return Void();
	}

	ACTOR static Future<Void> abortSwitchMetadata(AbortTenantMovementImpl* self) {
		// Check for full completion and only reverse if fully completed
		Optional<MetaclusterTenantGroupEntry> optionalGroupEntry =
		    wait(tryGetTenantGroup(self->dstCtx.managementDb, self->tenantGroup));
		ASSERT(optionalGroupEntry.present());
		if (optionalGroupEntry.get().assignedCluster == self->dstCtx.clusterName.get()) {
			wait(runMoveManagementTransaction(
			    self->tenantGroup,
			    self->srcCtx,
			    self->dstCtx,
			    Aborting::True,
			    { metadata::management::MovementState::SWITCH_METADATA },
			    [self = self](Reference<typename DB::TransactionT> tr,
			                  Optional<metadata::management::MovementRecord> movementRecord) {
				    return switchMetadataToSource(self, tr);
			    }));
		}

		// Update state and unwind with other steps
		wait(self->srcCtx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return updateMoveRecordState(tr, metadata::management::MovementState::SWITCH_HYBRID, self->tenantGroup);
		}));
		TraceEvent("TenantMoveAfterSwitchHybrid2");
		wait(abortSwitchHybrid(self));
		return Void();
	}

	ACTOR static Future<Void> run(AbortTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());
		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::True,
		                                  {},
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return initAbort(self, tr, movementRecord);
		                                  }));

		// Determine how far in the move process we've progressed and begin unwinding
		Future<Void> abortFuture;
		switch (self->moveRecord.mState) {
		case metadata::management::MovementState::START_LOCK:
			abortFuture = abortStartLock(self);
			break;
		case metadata::management::MovementState::START_CREATE:
			abortFuture = abortStartCreate(self);
			break;
		case metadata::management::MovementState::SWITCH_HYBRID:
			abortFuture = abortSwitchHybrid(self);
			break;
		case metadata::management::MovementState::SWITCH_METADATA:
			abortFuture = abortSwitchMetadata(self);
			break;
		case metadata::management::MovementState::FINISH_UNLOCK:
			TraceEvent("TenantMoveAbortNotAllowedAfterDestUnlocked");
			throw invalid_tenant_move();
		}
		wait(abortFuture);
		return Void();
	}

	Future<Void> run() { return run(this); }
};

} // namespace internal

ACTOR template <class DB>
Future<Void> startTenantMovement(Reference<DB> db, TenantGroupName tenantGroup, ClusterName src, ClusterName dst) {
	state internal::StartTenantMovementImpl<DB> impl(db, tenantGroup, src, dst);
	if (src == dst) {
		TraceEvent("TenantMoveStartSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> switchTenantMovement(Reference<DB> db,
                                  TenantGroupName tenantGroup,
                                  ClusterName src,
                                  ClusterName dst,
                                  std::vector<std::string>* messages) {
	state internal::SwitchTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, *messages);
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