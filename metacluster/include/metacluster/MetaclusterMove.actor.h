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
#include "fdbclient/TagThrottle.h"
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
			if (!moveRecord.present()) {
				TraceEvent("TenantMoveRecordNotPresent").detail("TenantGroup", tenantGroup);
				throw tenant_move_record_missing();
			} else {	
				TraceEvent("TenantMovementInInvalidState")
				    .detail("State", movementState)
				    .detail("SourceCluster", src)
				    .detail("DestinationCluster", dst)
				    .detail("TenantGroup", tenantGroup);
				throw invalid_tenant_move();
			}
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
		TraceEvent("TenantMoveRecordGetNotPresent").detail("TenantGroup", tenantGroup);
		throw tenant_move_record_missing();
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

ACTOR template <class Transaction>
static Future<std::vector<TenantMapEntry>> getTenantEntries(std::vector<std::pair<TenantName, int64_t>> tenantsInGroup,
                                                            Reference<Transaction> tr) {
	state std::vector<Future<TenantMapEntry>> entryFutures;
	for (const auto& tenantPair : tenantsInGroup) {
		entryFutures.push_back(TenantAPI::getTenantTransaction(tr, tenantPair.second));
	}

	wait(waitForAll(entryFutures));

	state std::vector<TenantMapEntry> results;
	for (auto const& f : entryFutures) {
		results.push_back(f.get());
	}

	return results;
}

ACTOR template <class Transaction>
static Future<Void> updateMoveRecordState(Reference<Transaction> tr,
                                          metadata::management::MovementRecord movementRecord,
                                          metadata::management::MovementState mState,
                                          TenantGroupName tenantGroup) {
	auto updatedMoveRec = movementRecord;
	TraceEvent("BreakpointExistingMoveRec")
	    .detail("TenantGroup", tenantGroup)
	    .detail("RunID", updatedMoveRec.runId)
	    .detail("Src", updatedMoveRec.srcCluster)
	    .detail("Dst", updatedMoveRec.dstCluster)
	    .detail("State", updatedMoveRec.mState)
	    .detail("NewState", mState)
	    .detail("Version", updatedMoveRec.version)
	    .detail("Aborting", updatedMoveRec.aborting);
	updatedMoveRec.mState = mState;

	metadata::management::emergency_movement::emergencyMovements().set(tr, tenantGroup, updatedMoveRec);
	return Void();
}

ACTOR template <class DB>
static Future<Void> purgeAndVerifyTenant(Reference<DB> db, TenantName tenant) {
	state Reference<ITenant> tenantObj = db->openTenant(tenant);
	state ThreadFuture<Key> resultFuture = tenantObj->purgeBlobGranules(normalKeys, latestVersion, false);
	state Key purgeKey = wait(safeThreadFutureToFuture(resultFuture));
	wait(safeThreadFutureToFuture(tenantObj->waitPurgeGranulesComplete(purgeKey)));
	return Void();
}

template <class DB>
struct StartTenantMovementImpl {
	MetaclusterOperationContext<DB> srcCtx;
	MetaclusterOperationContext<DB> dstCtx;

	// Initialization parameters
	TenantGroupName tenantGroup;
	metadata::management::MovementRecord moveRecord;
	std::vector<std::string>& messages;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;
	Version lockedVersion;
	Optional<ThrottleApi::ThroughputQuotaValue> tagQuota;
	Optional<int64_t> storageQuota;

	StartTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        ClusterName src,
	                        ClusterName dst,
	                        std::vector<std::string>& messages)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), messages(messages) {}

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
			self->messages.push_back(
			    fmt::format("Tenant move start: no tenant group named {} found on source cluster {}\n",
			                self->tenantGroup,
			                srcName));
			throw invalid_tenant_move();
		}

		if (!movementRecord.present()) {
			self->moveRecord.runId = deterministicRandom()->randomUniqueID();
			self->moveRecord.srcCluster = srcName;
			self->moveRecord.dstCluster = dstName;
			self->moveRecord.mState = metadata::management::MovementState::START_LOCK;
			self->moveRecord.version = invalidVersion;
			TraceEvent("BreakpointSetMove1", self->srcCtx.debugId).detail("TenantGroup", self->tenantGroup);
			metadata::management::emergency_movement::emergencyMovements().set(tr, self->tenantGroup, self->moveRecord);

			// clusterCapacityIndex to accommodate for capacity calculations
			state DataClusterMetadata clusterMetadata = self->dstCtx.dataClusterMetadata.get();
			state DataClusterEntry updatedEntry = clusterMetadata.entry;
			if (!updatedEntry.hasCapacity()) {
				TraceEvent("TenantMoveStartClusterNoCapacity").detail("DstCluster", dstName);
				self->messages.push_back(
				    fmt::format("Tenant move start: destination cluster {} has no more capacity.\n", dstName));
				throw cluster_no_capacity();
			}
			updatedEntry.allocated.numTenantGroups++;
			TraceEvent("BreakpointUpdateAllocated1", self->srcCtx.debugId).detail("DstName", dstName);
			updateClusterMetadata(tr,
			                      dstName,
			                      clusterMetadata,
			                      Optional<ClusterConnectionString>(),
			                      updatedEntry,
			                      IsRestoring::False,
			                      self->srcCtx.debugId);

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
		// state int64_t chunkSize = (!g_network->isSimulated()) ? 1e8 : 1e3;
		state int64_t chunkSize = (!g_network->isSimulated()) ? 1e8 : 1e2;
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

			TraceEvent("BreakpointSetMove2", self->srcCtx.debugId).detail("TenantGroup", self->tenantGroup);
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
		TraceEvent("BreakpointStart1");
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

		TraceEvent("BreakpointStart2");
		if (self->moveRecord.mState < metadata::management::MovementState::START_CREATE) {
			TraceEvent("BreakpointStart2.1");
			wait(lockSourceTenants(self));
		}
		TraceEvent("BreakpointStart3");

		wait(self->srcCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getSourceClusterMetadata(self, tr); }));
		TraceEvent("BreakpointStart4");

		if (self->moveRecord.mState < metadata::management::MovementState::START_CREATE) {
			TraceEvent("BreakpointStart4.1");
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
		TraceEvent("BreakpointStart5");

		wait(self->dstCtx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return createLockedDestinationTenants(self, tr); }));
		TraceEvent("BreakpointStart6");

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

	// returns true if this has already completed and this is a retry
	ACTOR static Future<bool> initSwitch(SwitchTenantMovementImpl* self,
	                                     Reference<typename DB::TransactionT> tr,
	                                     Optional<metadata::management::MovementRecord> movementRecord) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		ASSERT(movementRecord.present());
		self->moveRecord = movementRecord.get();
		// Check that tenantGroup exists on src
		// If it doesn't the switch may have already completed
		state bool srcExists = wait(
		    metadata::management::clusterTenantGroupIndex().exists(tr, Tuple::makeTuple(srcName, self->tenantGroup)));
		if (!srcExists) {
			state bool dstExists = wait(metadata::management::clusterTenantGroupIndex().exists(
			    tr, Tuple::makeTuple(dstName, self->tenantGroup)));
			if (dstExists) {
				return true;
			}
			TraceEvent("TenantMoveSwitchGroupNotOnSourceOrDest")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("SourceName", srcName)
			    .detail("DestName", dstName);
			self->messages.push_back(fmt::format("Tenant move switch: tenantGroup not found on source or destination\n"
			                                     "		group name:	{}\n",
			                                     self->tenantGroup));
			throw invalid_tenant_move();
		}

		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		return false;
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
				auto copy = result;
				return copy;
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
				self->messages.push_back(fmt::format(
				    "Tenant move switch: data mismatch in range {} - {} for tenant {}\n", begin, end, tName));
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
					self->messages.push_back(fmt::format("Tenant move switch: blobbify failed for tenant {}\n", tName));
					throw tenant_move_failed();
				}
			}
			TraceEvent("TenantMoveSwitchBlobRangeApplied").detail("TenantName", tName);
		}
		return Void();
	}

	ACTOR static Future<Void> switchMetadataToDestination(
	    SwitchTenantMovementImpl* self,
	    Reference<typename DB::TransactionT> tr,
	    Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		TraceEvent("TenantMoveSwitchBegin");

		// Possible Retry
		TraceEvent("TenantMoveSwitchBegin2");
		Tuple dstEntry = Tuple::makeTuple(dstName, self->tenantGroup);
		bool exists = wait(metadata::management::clusterTenantGroupIndex().exists(tr, dstEntry));
		if (exists) {
			TraceEvent("TenantMoveSwitchBegin2.1");
			return Void();
		}
		TraceEvent("TenantMoveSwitchBegin3");

		state std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> tenantMetadataList;
		wait(store(tenantMetadataList, listTenantMetadataTransaction(tr, self->tenantsInGroup)));

		for (auto& tenantPair : tenantMetadataList) {
			state TenantName tName = tenantPair.first;
			state MetaclusterTenantMapEntry tenantEntry = tenantPair.second;
			state int64_t tId = tenantEntry.id;

			// tenantMetadata().tenantMap update assigned cluster
			if (tenantEntry.assignedCluster != srcName) {
				// if (tenantEntry.assignedCluster == dstName) {
				// 	// possible that this is a retry
				// 	return Void();
				// }
				TraceEvent(SevError, "TenantMoveSwitchTenantEntryWrongCluster")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId)
				    .detail("ExpectedCluster", srcName)
				    .detail("Src", srcName)
				    .detail("Dst", dstName)
				    .detail("EntryCluster", tenantEntry.assignedCluster);
				self->messages.push_back(fmt::format("Tenant move switch: tenantEntry with incorrect cluster\n"
				                                     "		expected cluster:	{}\n"
				                                     "		actual cluster:		{}\n",
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
			self->messages.push_back(fmt::format("Tenant move switch: tenantGroupEntry missing\n"
			                                     "		group name:	{}\n",
			                                     self->tenantGroup));
			throw invalid_tenant_move();
		}
		if (groupEntry.get().assignedCluster != srcName) {
			TraceEvent(SevError, "TenantMoveSwitchGroupEntryIncorrectCluster")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ExpectedCluster", srcName)
			    .detail("GroupEntryAssignedCluster", groupEntry.get().assignedCluster);
			self->messages.push_back(fmt::format("Tenant move switch: tenantGroupEntry with incorrect cluster\n"
			                                     "		group name:			{}\n"
			                                     "		expected cluster:	{}\n"
			                                     "		actual cluster:	{}\n",
			                                     self->tenantGroup,
			                                     srcName,
			                                     groupEntry.get().assignedCluster));
			throw invalid_tenant_move();
		}
		groupEntry.get().assignedCluster = dstName;
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		TraceEvent("TenantMoveSwitchEnd");
		return Void();
	}

	ACTOR static Future<Void> run(SwitchTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());

		TraceEvent("BreakpointSwitch1");
		bool alreadySwitched = wait(
		    runMoveManagementTransaction(self->tenantGroup,
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
		if (alreadySwitched) {
			TraceEvent("BreakpointSwitch1.1");
			return Void();
		}

		TraceEvent("BreakpointSwitch2");
		if (self->moveRecord.mState < metadata::management::MovementState::SWITCH_HYBRID) {
			wait(checkAllTenantData(self));
		}

		try {
			wait(runMoveManagementTransaction(
			    self->tenantGroup,
			    self->srcCtx,
			    self->dstCtx,
			    Aborting::False,
			    { metadata::management::MovementState::START_CREATE,
			      metadata::management::MovementState::SWITCH_HYBRID },
			    [self = self](Reference<typename DB::TransactionT> tr,
			                  Optional<metadata::management::MovementRecord> movementRecord) {
				    return updateMoveRecordState(tr,
				                                 movementRecord.get(),
				                                 metadata::management::MovementState::SWITCH_HYBRID,
				                                 self->tenantGroup);
			    }));
			wait(applyHybridRanges(self));
		} catch (Error& e) {
			// Because of timing/retry issues, the move record can progress to SWITCH_METADATA
			// which isn't part of the valid movement states
			// If this is the case, don't reapply hybrid ranges and continue working
			// If the updateMoveRecordState threw invalid_tenant_move for another reason
			// The call to update again below will re-throw the same error
			if (e.code() != error_code_invalid_tenant_move) {
				throw e;
			}
		}

		TraceEvent("BreakpointSwitch3");

		wait(runMoveManagementTransaction(
		    self->tenantGroup,
		    self->srcCtx,
		    self->dstCtx,
		    Aborting::False,
		    { metadata::management::MovementState::SWITCH_HYBRID,
		      metadata::management::MovementState::SWITCH_METADATA },
		    [self = self](Reference<typename DB::TransactionT> tr,
		                  Optional<metadata::management::MovementRecord> movementRecord) {
			    return updateMoveRecordState(
			        tr, movementRecord.get(), metadata::management::MovementState::SWITCH_METADATA, self->tenantGroup);
		    }));

		TraceEvent("BreakpointSwitch4");
		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::False,
		                                  { metadata::management::MovementState::SWITCH_METADATA },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return switchMetadataToDestination(self, tr, movementRecord);
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
	std::vector<std::string>& messages;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	FinishTenantMovementImpl(Reference<DB> managementDb,
	                         TenantGroupName tenantGroup,
	                         ClusterName src,
	                         ClusterName dst,
	                         std::vector<std::string>& messages)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), messages(messages) {}

	ACTOR static Future<Void> initFinish(FinishTenantMovementImpl* self,
	                                     Reference<typename DB::TransactionT> tr,
	                                     Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();

		// Check that tenantGroup exists on dst
		state bool exists = wait(
		    metadata::management::clusterTenantGroupIndex().exists(tr, Tuple::makeTuple(dstName, self->tenantGroup)));

		if (!exists) {
			TraceEvent("TenantMoveFinishGroupNotOnDestination")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("DstClusterName", dstName);
			self->messages.push_back(
			    fmt::format("Tenant move finish: tenant group {} not found on destination cluster {}\n",
			                self->tenantGroup,
			                dstName));
			throw invalid_tenant_move();
		}

		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		self->moveRecord = movementRecord.get();
		TraceEvent("BreakpointInitFinishMoveRecord")
		    .detail("RunID", self->moveRecord.runId)
		    .detail("Src", self->moveRecord.srcCluster)
		    .detail("Dst", self->moveRecord.dstCluster)
		    .detail("State", self->moveRecord.mState)
		    .detail("Version", self->moveRecord.version)
		    .detail("Aborting", self->moveRecord.aborting);
		// }

		return Void();
	}

	ACTOR static Future<Void> checkValidUnlock(FinishTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr,
	                                           std::vector<TenantMapEntry> srcEntries,
	                                           std::vector<TenantMapEntry> dstEntries,
	                                           Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
		state size_t iterLen = self->tenantsInGroup.size();
		if (iterLen != srcEntries.size()) {
			TraceEvent(SevError, "TenantMoveFinishCheckUnlockSrcEntriesSizeDiff")
			    .detail("SrcEntriesSize", srcEntries.size())
			    .detail("TenantsInGroup", iterLen);
			fmt::print("Tenant move finish: The number of source tenant entries does not match the expected number of "
			           "tenants in the group {}:\n"
			           "	# Source tenant entries:				{}"
			           "	# Tenants expected in tenant group:		{}",
			           self->tenantGroup,
			           srcEntries.size(),
			           iterLen);
		}
		if (iterLen != dstEntries.size()) {
			TraceEvent(SevError, "TenantMoveFinishCheckUnlockDstEntriesSizeDiff")
			    .detail("DstEntriesSize", dstEntries.size())
			    .detail("TenantsInGroup", iterLen);
			fmt::print("Tenant move finish: The number of destination tenant entries does not match the expected "
			           "number of tenants in the group {}:\n"
			           "	# Destination tenant entries:			{}"
			           "	# Tenants expected in tenant group:		{}",
			           self->tenantGroup,
			           dstEntries.size(),
			           iterLen);
		}
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
				self->messages.push_back(
				    fmt::format("Tenant move finish: tenant {} does not exist on destination cluster {}\n",
				                tName,
				                self->dstCtx.clusterName.get()));
				throw invalid_tenant_move();
			}
			// Assert src tenant has the correct tenant group
			if (!srcEntry.tenantGroup.present()) {
				TraceEvent(SevError, "TenantMoveFinishUnlockTenantGroupMissing")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				self->messages.push_back(
				    fmt::format("Tenant move finish: tenant group missing for tenant {}\n", tName));
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
				self->messages.push_back(fmt::format(
				    "Tenant move finish: tenant group does not match between corresponding tenant entries:\n"
				    "	tenant name:						{}"
				    "	source entry tenant group:			{}"
				    "	destination entry tenant group:		{}",
				    tName,
				    srcEntry.tenantGroup.get(),
				    dstEntry.tenantGroup.get()));
				throw invalid_tenant_move();
			}
			// Assert src tenant is locked
			if (srcEntry.tenantLockState != TenantAPI::TenantLockState::LOCKED) {
				TraceEvent(SevError, "TenantMoveFinishUnlockMatchingTenantNotLocked")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId);
				self->messages.push_back(
				    fmt::format("Tenant move finish: tenant {} on source cluster is not locked\n", tName));
				throw invalid_tenant_move();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> unlockDestinationTenants(FinishTenantMovementImpl* self) {
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
	                                           std::vector<TenantMapEntry> dstEntries,
	                                           Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
		state size_t iterLen = self->tenantsInGroup.size();
		if (iterLen != srcEntries.size()) {
			TraceEvent(SevError, "TenantMoveFinishCheckUnlockSrcEntriesSizeDiff")
			    .detail("SrcEntriesSize", srcEntries.size())
			    .detail("TenantsInGroup", iterLen);
			fmt::print("Tenant move finish: The number of source tenant entries does not match the expected number of "
			           "tenants in the group {}:\n"
			           "	# Source tenant entries:				{}"
			           "	# Tenants expected in tenant group:		{}",
			           self->tenantGroup,
			           srcEntries.size(),
			           iterLen);
		}
		if (iterLen != dstEntries.size()) {
			TraceEvent(SevError, "TenantMoveFinishCheckUnlockDstEntriesSizeDiff")
			    .detail("DstEntriesSize", dstEntries.size())
			    .detail("TenantsInGroup", iterLen);
			fmt::print("Tenant move finish: The number of destination tenant entries does not match the expected "
			           "number of tenants in the group {}:\n"
			           "	# Destination tenant entries:			{}"
			           "	# Tenants expected in tenant group:		{}",
			           self->tenantGroup,
			           dstEntries.size(),
			           iterLen);
		}
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
				self->messages.push_back(
				    fmt::format("Tenant move finish: tenant {} on source cluster is not locked\n", tName));
				throw invalid_tenant_move();
			}

			// Assert dst tenant exists in metacluster metadata
			Tuple indexTuple = Tuple::makeTuple(self->dstCtx.clusterName.get(), tName, tId);
			bool result = wait(metadata::management::clusterTenantIndex().exists(tr, indexTuple));
			if (!result) {
				TraceEvent(SevError, "TenantMoveFinishDeleteDataClusterMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->dstCtx.clusterName.get());
				self->messages.push_back(
				    fmt::format("Tenant move finish: tenant {} does not exist on destination cluster {}\n",
				                tName,
				                self->dstCtx.clusterName.get()));
				throw invalid_tenant_move();
			}

			// Assert matching tenant groups
			if (dstEntry.tenantGroup != srcEntry.tenantGroup) {
				TraceEvent(SevError, "TenantMoveFinishDeleteTenantGroupMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedGroup", self->tenantGroup)
				    .detail("SourceEntryTenantGroup", srcEntry.tenantGroup.get())
				    .detail("DestinationEntryTenantGroup", dstEntry.tenantGroup.get());
				self->messages.push_back(fmt::format(
				    "Tenant move finish: tenant group does not match between corresponding tenant entries:\n"
				    "	tenant name:						{}"
				    "	source entry tenant group:			{}"
				    "	destination entry tenant group:		{}",
				    tName,
				    srcEntry.tenantGroup.get(),
				    dstEntry.tenantGroup.get()));
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

	void clearMovementMetadataFinish(Reference<typename DB::TransactionT> tr) {
		UID runId = moveRecord.runId;
		metadata::management::emergency_movement::emergencyMovements().erase(tr, tenantGroup);
		metadata::management::emergency_movement::movementQueue().erase(tr,
		                                                                std::make_pair(tenantGroup, runId.toString()));
		Tuple beginTuple = Tuple::makeTuple(tenantGroup, runId.toString(), ""_sr, ""_sr);
		Tuple endTuple = Tuple::makeTuple(tenantGroup, runId.toString(), "\xff"_sr, "\xff"_sr);
		metadata::management::emergency_movement::splitPointsMap().erase(tr, beginTuple, endTuple);
	}

	static Future<Void> finalizeMovement(FinishTenantMovementImpl* self,
	                                     Reference<typename DB::TransactionT> tr,
	                                     Optional<metadata::management::MovementRecord> movementRecord) {
		// if (movementRecord.present()) {
		ASSERT(movementRecord.present());
		self->updateCapacityMetadata(tr);
		self->clearMovementMetadataFinish(tr);
		// }

		return Void();
	}

	ACTOR static Future<Void> run(FinishTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());
		TraceEvent("Breakpoint0");
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
		try {
			if (self->moveRecord.mState == metadata::management::MovementState::SWITCH_METADATA) {
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
					                                  return checkValidUnlock(
					                                      self, tr, srcEntries, dstEntries, movementRecord);
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
					                                  return checkValidDelete(
					                                      self, tr, srcEntries, dstEntries, movementRecord);
				                                  }));
			}
		} catch (Error& e) {
			state Error err(e);
			if (err.code() == error_code_invalid_tenant_move) {
				// Possible for delayed moveRecord update on retry
				metadata::management::MovementRecord moveRecord =
				    wait(self->srcCtx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
					    return getMovementRecord(tr,
					                             self->tenantGroup,
					                             self->srcCtx.clusterName.get(),
					                             self->dstCtx.clusterName.get(),
					                             Aborting::False,
					                             { metadata::management::MovementState::SWITCH_METADATA,
					                               metadata::management::MovementState::FINISH_UNLOCK });
				    }));
				if (moveRecord.mState != metadata::management::MovementState::FINISH_UNLOCK) {
					throw err;
				}
			} else {
				throw err;
			}
		}
		wait(runMoveManagementTransaction(
		    self->tenantGroup,
		    self->srcCtx,
		    self->dstCtx,
		    Aborting::False,
		    { metadata::management::MovementState::SWITCH_METADATA,
		      metadata::management::MovementState::FINISH_UNLOCK },
		    [self = self](Reference<typename DB::TransactionT> tr,
		                  Optional<metadata::management::MovementRecord> movementRecord) {
			    return updateMoveRecordState(
			        tr, movementRecord.get(), metadata::management::MovementState::FINISH_UNLOCK, self->tenantGroup);
		    }));
		TraceEvent("Breakpoint6");
		wait(unlockDestinationTenants(self));

		TraceEvent("Breakpoint7");
		try {
			wait(purgeSourceBlobRanges(self));
		} catch (Error& e) {
			TraceEvent("TenantMoveFinishPurgeSourceBlobRangesError").error(e);
			// If tenant is not found, this is a retry and the tenants have been deleted already
			if (e.code() != error_code_tenant_not_found) {
				throw e;
			}
		}

		TraceEvent("Breakpoint8");

		try {
			wait(deleteAllSourceData(self));
		} catch (Error& e) {
			TraceEvent("TenantMoveFinishDeleteSourceDataError").error(e);
			// If tenant is not found, this is a retry and the tenants have been deleted already
			if (e.code() != error_code_tenant_not_found) {
				throw e;
			}
		}

		try {
			TraceEvent("Breakpoint9");
			wait(self->srcCtx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return deleteSourceTenants(self, tr); }));
		} catch (Error& e) {
			TraceEvent("TenantMoveFinishDeleteSourceTenantsError").error(e);
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
			                                  return finalizeMovement(self, tr, movementRecord);
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
	std::vector<std::string>& messages;

	// Parameters filled in during the run
	std::vector<std::pair<TenantName, int64_t>> tenantsInGroup;

	AbortTenantMovementImpl(Reference<DB> managementDb,
	                        TenantGroupName tenantGroup,
	                        ClusterName src,
	                        ClusterName dst,
	                        std::vector<std::string>& messages)
	  : srcCtx(managementDb, src), dstCtx(managementDb, dst), tenantGroup(tenantGroup), messages(messages) {}

	ACTOR static Future<Void> initAbort(AbortTenantMovementImpl* self,
	                                    Reference<typename DB::TransactionT> tr,
	                                    Optional<metadata::management::MovementRecord> movementRecord) {
		state ClusterName srcName = self->srcCtx.clusterName.get();
		state ClusterName dstName = self->dstCtx.clusterName.get();
		wait(findTenantsInGroup(tr, self->tenantGroup, &self->tenantsInGroup));
		if (!movementRecord.present()) {
			TraceEvent("TenantMoveAbortNoMoveRecord").detail("TenantGroup", self->tenantGroup);
			self->messages.push_back(fmt::format(
			    "Tenant move abort: no movement in progress for the tenant group: {}\n", self->tenantGroup));
			throw tenant_move_record_missing();
		} else {
			self->moveRecord = movementRecord.get();
		}
		if (self->moveRecord.mState == metadata::management::MovementState::FINISH_UNLOCK) {
			TraceEvent("TenantMoveAbortNotAllowedAfterDestUnlocked");
			self->messages.push_back(fmt::format("Tenant move abort: disallowed after tenants have been unlocked"));
			throw invalid_tenant_move();
		}
		// Mark movement as aborting and write it into metadata immediately
		self->moveRecord.aborting = true;
		metadata::management::emergency_movement::emergencyMovements().set(tr, self->tenantGroup, self->moveRecord);
		return Void();
	}

	ACTOR static Future<Void> clearMovementMetadataAbort(
	    AbortTenantMovementImpl* self,
	    Reference<typename DB::TransactionT> tr,
	    Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
		state UID runId = movementRecord.get().runId;
		metadata::management::emergency_movement::emergencyMovements().erase(tr, self->tenantGroup);
		metadata::management::emergency_movement::movementQueue().erase(
		    tr, std::make_pair(self->tenantGroup, runId.toString()));
		Tuple beginTuple = Tuple::makeTuple(self->tenantGroup, runId.toString(), TenantName(""_sr), KeyRef(""_sr));
		Tuple endTuple =
		    Tuple::makeTuple(self->tenantGroup, runId.toString(), TenantName("\xff"_sr), KeyRef("\xff"_sr));
		metadata::management::emergency_movement::splitPointsMap().erase(tr, beginTuple, endTuple);

		// Decrease clusterTenantCount and clusterCapacityIndex of destination here since it was
		// pre-emptively increased for capacity purposes in the Start step
		state ClusterName dstName = self->dstCtx.clusterName.get();

		// DataClusterMetadata clusterMetadata = self->dstCtx.dataClusterMetadata.get();
		// Protect against race conditions
		DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, self->dstCtx.clusterName.get()));
		DataClusterEntry updatedEntry = clusterMetadata.entry;
		updatedEntry.allocated.numTenantGroups--;
		TraceEvent("BreakpointUpdateAllocated2", self->srcCtx.debugId).detail("DstName", dstName);
		updateClusterMetadata(tr,
		                      dstName,
		                      clusterMetadata,
		                      Optional<ClusterConnectionString>(),
		                      updatedEntry,
		                      IsRestoring::False,
		                      self->srcCtx.debugId);

		int numTenants = self->tenantsInGroup.size();
		metadata::management::clusterTenantCount().atomicOp(tr, dstName, -numTenants, MutationRef::AddValue);
		return Void();
	}

	// Don't check for matching destination entries because they will have been deleted or not yet created
	ACTOR static Future<Void> checkValidUnlock(AbortTenantMovementImpl* self,
	                                           Reference<typename DB::TransactionT> tr,
	                                           std::vector<TenantMapEntry> srcEntries,
	                                           Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
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
				self->messages.push_back(
				    fmt::format("Tenant move abort: tenant {} does not exist on source cluster {}\n",
				                tName,
				                self->srcCtx.clusterName.get()));
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
	                                           std::vector<TenantMapEntry> dstEntries,
	                                           Optional<metadata::management::MovementRecord> movementRecord) {
		ASSERT(movementRecord.present());
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
				self->messages.push_back(
				    fmt::format("Tenant move abort: tenant {} on source cluster is not locked\n", tName));
				throw invalid_tenant_move();
			}

			// Assert src tenant exists in metacluster metadata
			// Abort will have switched metadata to source by this point
			Tuple srcTuple = Tuple::makeTuple(self->srcCtx.clusterName.get(), tName, tId);
			bool srcExists = wait(metadata::management::clusterTenantIndex().exists(tr, srcTuple));
			if (!srcExists) {
				TraceEvent("TenantMoveAbortDeleteTenantClusterMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedCluster", self->srcCtx.clusterName.get());
				self->messages.push_back(
				    fmt::format("Tenant move abort: tenant {} does not exist on source cluster {}\n",
				                tName,
				                self->srcCtx.clusterName.get()));
				throw invalid_tenant_move();
			}

			// Assert matching tenant groups
			if (dstEntry.tenantGroup != srcEntry.tenantGroup) {
				TraceEvent(SevError, "TenantMoveAbortDeleteTenantGroupMismatch")
				    .detail("TenantName", tName)
				    .detail("TenantID", tId)
				    .detail("ExpectedGroup", self->tenantGroup)
				    .detail("SourceEntryTenantGroup", srcEntry.tenantGroup.get())
				    .detail("DestinationEntryTenantGroup", dstEntry.tenantGroup.get());
				self->messages.push_back(
				    fmt::format("Tenant move abort: tenant group does not match between corresponding tenant entries:\n"
				                "	tenant name:						{}"
				                "	source entry tenant group:			{}"
				                "	destination entry tenant group:		{}",
				                tName,
				                srcEntry.tenantGroup.get(),
				                dstEntry.tenantGroup.get()));
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
				// if (tenantEntry.assignedCluster == srcName) {
				// 	// possible that this is a retry
				// 	return Void();
				// }
				TraceEvent(SevError, "TenantMoveAbortSwitchTenantEntryWrongCluster")
				    .detail("TenantName", tName)
				    .detail("TenantId", tId)
				    .detail("ExpectedCluster", dstName)
				    .detail("Src", srcName)
				    .detail("Dst", dstName)
				    .detail("EntryCluster", tenantEntry.assignedCluster);
				self->messages.push_back(fmt::format("Tenant move switch: tenantEntry with incorrect cluster\n"
				                                     "		expected cluster:	{}\n"
				                                     "		actual cluster:		{}\n",
				                                     dstName,
				                                     tenantEntry.assignedCluster));
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
			self->messages.push_back(fmt::format("Tenant move abort: tenantGroupEntry missing\n"
			                                     "		group name:	{}\n",
			                                     self->tenantGroup));
			throw invalid_tenant_move();
		}
		if (groupEntry.get().assignedCluster != dstName) {
			TraceEvent(SevError, "TenantMoveAbortSwitchGroupEntryIncorrectCluster")
			    .detail("TenantGroup", self->tenantGroup)
			    .detail("ExpectedCluster", dstName)
			    .detail("GroupEntryAssignedCluster", groupEntry.get().assignedCluster);
			self->messages.push_back(fmt::format("Tenant move abort: tenantGroupEntry with incorrect cluster\n"
			                                     "		group name:			{}\n"
			                                     "		expected cluster:	{}\n"
			                                     "		actual cluster:	{}\n",
			                                     self->tenantGroup,
			                                     dstName,
			                                     groupEntry.get().assignedCluster));
			throw invalid_tenant_move();
		}
		groupEntry.get().assignedCluster = srcName;
		metadata::management::tenantMetadata().tenantGroupMap.set(tr, self->tenantGroup, groupEntry.get());

		return Void();
	}

	ACTOR static Future<Void> abortStartLock(AbortTenantMovementImpl* self) {
		TraceEvent("BreakpointAbort4");
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
			    return checkValidUnlock(self, tr, srcEntries, movementRecord);
		    }));

		wait(unlockSourceTenants(self));

		wait(runMoveManagementTransaction(self->tenantGroup,
		                                  self->srcCtx,
		                                  self->dstCtx,
		                                  Aborting::True,
		                                  { metadata::management::MovementState::START_LOCK },
		                                  [self = self](Reference<typename DB::TransactionT> tr,
		                                                Optional<metadata::management::MovementRecord> movementRecord) {
			                                  return clearMovementMetadataAbort(self, tr, movementRecord);
		                                  }));

		return Void();
	}

	ACTOR static Future<Void> abortStartCreate(AbortTenantMovementImpl* self) {
		TraceEvent("BreakpointAbort3")
		    .detail("TenantGroup", self->tenantGroup)
		    .detail("DstName", self->dstCtx.clusterName);
		// If no tenant entries exist on dst, they are already deleted or were never created
		state std::vector<TenantMapEntry> dstEntries;
		state bool runDelete = true;
		state int tries = 0;
		loop {
			TraceEvent("BreakpointLoop1");
			try {
				wait(
				    store(dstEntries, self->dstCtx.runDataClusterTransaction([self = self](Reference<ITransaction> tr) {
					    return getTenantEntries(self->tenantsInGroup, tr);
				    })));
				break;
			} catch (Error& e) {
				state Error err(e);
				TraceEvent("TenantMoveAbortStartCreateGetTenantEntriesError").error(err);
				// Timing issues can make it so that abort doesn't see the created tenants on dst
				// Delete every tenant regardless if our initial "get" doesn't see any
				// This will succeed (no-op) or result in a conflict
				if (err.code() == error_code_tenant_not_found) {
					TraceEvent("TenantMoveAbortStartCreateDestinationEntriesNotFound")
					    .detail("TenantGroup", self->tenantGroup)
					    .detail("NumTenants", self->tenantsInGroup.size());
					try {
						wait(self->dstCtx.runDataClusterTransaction(
						    [self = self](Reference<ITransaction> tr) { return deleteDestinationTenants(self, tr); }));
						runDelete = false;
						break;
					} catch (Error& e2) {
						if (e2.code() == error_code_not_committed || e2.code() == error_code_tenant_not_empty) {
							continue;
						}
					}
				}
				throw err;
			}
		}
		TraceEvent("BreakpointAbort3.1").detail("RunDelete", runDelete);
		if (runDelete) {
			try {
				state std::vector<TenantMapEntry> srcEntries = wait(self->srcCtx.runDataClusterTransaction(
				    [self = self](Reference<ITransaction> tr) { return getTenantEntries(self->tenantsInGroup, tr); }));

				TraceEvent("BreakpointAbort3.2");
				wait(runMoveManagementTransaction(self->tenantGroup,
				                                  self->srcCtx,
				                                  self->dstCtx,
				                                  Aborting::True,
				                                  { metadata::management::MovementState::START_CREATE },
				                                  [self = self, srcEntries = srcEntries, dstEntries = dstEntries](
				                                      Reference<typename DB::TransactionT> tr,
				                                      Optional<metadata::management::MovementRecord> movementRecord) {
					                                  return checkValidDelete(
					                                      self, tr, srcEntries, dstEntries, movementRecord);
				                                  }));
				TraceEvent("BreakpointAbort3.3");
				wait(deleteAllDestinationData(self));
				TraceEvent("BreakpointAbort3.4");
				wait(self->dstCtx.runDataClusterTransaction(
				    [self = self](Reference<ITransaction> tr) { return deleteDestinationTenants(self, tr); }));
				TraceEvent("BreakpointAbort3.5");
			} catch (Error& e) {
				TraceEvent("TenantMoveAbortStartCreateDeleteError").error(e);
				if (e.code() != error_code_tenant_not_found) {
					throw e;
				}
			}
		}
		TraceEvent("BreakpointAbort3.6");

		// Update state and unwind with other steps
		wait(runMoveManagementTransaction(
		    self->tenantGroup,
		    self->srcCtx,
		    self->dstCtx,
		    Aborting::True,
		    { metadata::management::MovementState::START_CREATE, metadata::management::MovementState::START_LOCK },
		    [self = self](Reference<typename DB::TransactionT> tr,
		                  Optional<metadata::management::MovementRecord> movementRecord) {
			    return updateMoveRecordState(
			        tr, movementRecord.get(), metadata::management::MovementState::START_LOCK, self->tenantGroup);
		    }));
		wait(abortStartLock(self));
		return Void();
	}

	ACTOR static Future<Void> abortSwitchHybrid(AbortTenantMovementImpl* self) {
		TraceEvent("BreakpointAbort2");
		// Okay to run even if step is uncompleted or partially completed
		wait(purgeDestinationBlobRanges(self));

		// Update state and unwind with other steps
		wait(runMoveManagementTransaction(
		    self->tenantGroup,
		    self->srcCtx,
		    self->dstCtx,
		    Aborting::True,
		    { metadata::management::MovementState::SWITCH_HYBRID, metadata::management::MovementState::START_CREATE },
		    [self = self](Reference<typename DB::TransactionT> tr,
		                  Optional<metadata::management::MovementRecord> movementRecord) {
			    return updateMoveRecordState(
			        tr, movementRecord.get(), metadata::management::MovementState::START_CREATE, self->tenantGroup);
		    }));
		wait(abortStartCreate(self));
		return Void();
	}

	ACTOR static Future<Void> abortSwitchMetadata(AbortTenantMovementImpl* self) {
		TraceEvent("BreakpointAbort1");
		// Check for full completion and only reverse if fully completed
		Optional<MetaclusterTenantGroupEntry> optionalGroupEntry =
		    wait(tryGetTenantGroup(self->srcCtx.managementDb, self->tenantGroup));
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
		wait(runMoveManagementTransaction(
		    self->tenantGroup,
		    self->srcCtx,
		    self->dstCtx,
		    Aborting::True,
		    { metadata::management::MovementState::SWITCH_METADATA,
		      metadata::management::MovementState::SWITCH_HYBRID },
		    [self = self](Reference<typename DB::TransactionT> tr,
		                  Optional<metadata::management::MovementRecord> movementRecord) {
			    return updateMoveRecordState(
			        tr, movementRecord.get(), metadata::management::MovementState::SWITCH_HYBRID, self->tenantGroup);
		    }));
		TraceEvent("TenantMoveAfterSwitchHybrid2");
		wait(abortSwitchHybrid(self));
		return Void();
	}

	// Returns true if this can be considered a complete abort
	ACTOR static Future<bool> clearMoveRecord(AbortTenantMovementImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<metadata::management::MovementRecord> moveRecord =
		    wait(metadata::management::emergency_movement::emergencyMovements().get(tr, self->tenantGroup));
		if (!moveRecord.present()) {
			metadata::management::emergency_movement::emergencyMovements().erase(tr, self->tenantGroup);
		} else {
			return false;
		}
		return true;
	}

	ACTOR static Future<Void> run(AbortTenantMovementImpl* self) {
		wait(self->dstCtx.initializeContext());
		loop {
			try {
				wait(runMoveManagementTransaction(
				    self->tenantGroup,
				    self->srcCtx,
				    self->dstCtx,
				    Aborting::True,
				    {},
				    [self = self](Reference<typename DB::TransactionT> tr,
				                  Optional<metadata::management::MovementRecord> movementRecord) {
					    return initAbort(self, tr, movementRecord);
				    }));
				break;
			} catch (Error& e) {
				// Rare scenario where abort is called immediately after "start"
				// See no move record while the start tr is still committing
				state Error err(e);
				TraceEvent("TenantMoveInitAbortError").error(err);
				// Attempt to re-clear the move record
				// Conflict should arise if "start" transaction is still in flight
				if (err.code() == error_code_tenant_move_record_missing) {
					bool exitSignal = wait(self->srcCtx.runManagementTransaction(
					    [self = self](Reference<typename DB::TransactionT> tr) { return clearMoveRecord(self, tr); }));
					if (exitSignal) {
						return Void();
					} else {
						continue;
					}
				}
				throw err;
			}
		}

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

template <class DB>
struct MoveStatusImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantGroupName tenantGroup;

	// Result
	metadata::management::MovementRecord moveRecord;

	MoveStatusImpl(Reference<DB> managementDb, TenantGroupName tenantGroup)
	  : ctx(managementDb), tenantGroup(tenantGroup) {}

	ACTOR static Future<Void> getMoveRecord(MoveStatusImpl* self, Reference<typename DB::TransactionT> tr) {
		Optional<metadata::management::MovementRecord> moveRecord =
		    wait(metadata::management::emergency_movement::emergencyMovements().get(tr, self->tenantGroup));
		if (!moveRecord.present()) {
			TraceEvent("TenantMoveStatusRecordNotPresent").detail("TenantGroup", self->tenantGroup);
			throw tenant_move_record_missing();
		}
		self->moveRecord = moveRecord.get();
		return Void();
	}

	ACTOR static Future<Void> run(MoveStatusImpl* self) {
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return getMoveRecord(self, tr); }));
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
                                 std::vector<std::string>* messages) {
	state internal::StartTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, *messages);
	if (src == dst) {
		TraceEvent("TenantMoveStartSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		messages->push_back(fmt::format("Tenant move start: source and destination cluster must be distinct\n"));
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
		messages->push_back(fmt::format("Tenant move switch: source and destination cluster must be distinct\n"));
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> finishTenantMovement(Reference<DB> db,
                                  TenantGroupName tenantGroup,
                                  ClusterName src,
                                  ClusterName dst,
                                  std::vector<std::string>* messages) {
	state internal::FinishTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, *messages);
	if (src == dst) {
		TraceEvent("TenantMoveFinishSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		messages->push_back(fmt::format("Tenant move finish: source and destination cluster must be distinct\n"));
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> abortTenantMovement(Reference<DB> db,
                                 TenantGroupName tenantGroup,
                                 ClusterName src,
                                 ClusterName dst,
                                 std::vector<std::string>* messages) {
	state internal::AbortTenantMovementImpl<DB> impl(db, tenantGroup, src, dst, *messages);
	if (src == dst) {
		TraceEvent("TenantMoveAbortSameSrcDst").detail("TenantGroup", tenantGroup).detail("ClusterName", src);
		messages->push_back(fmt::format("Tenant move abort: source and destination cluster must be distinct\n"));
		throw invalid_tenant_move();
	}
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<metadata::management::MovementRecord> moveStatus(Reference<DB> db, TenantGroupName tenantGroup) {
	state internal::MoveStatusImpl<DB> impl(db, tenantGroup);
	wait(impl.run());
	return impl.moveRecord;
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif