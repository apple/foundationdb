/*
 * DataDistribution.actor.cpp
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

#include <set>
#include <string>

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbrpc/Replication.h"
#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/TenantCache.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/MockDataDistributor.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/BooleanParam.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static const std::string ddServerBulkDumpFolder = "ddBulkDumpFiles";

DataMoveType getDataMoveTypeFromDataMoveId(const UID& dataMoveId) {
	bool assigned, emptyRange;
	DataMoveType dataMoveType;
	DataMovementReason dataMoveReason;
	decodeDataMoveId(dataMoveId, assigned, emptyRange, dataMoveType, dataMoveReason);
	return dataMoveType;
}

void RelocateShard::setParentRange(KeyRange const& parent) {
	ASSERT(reason == RelocateReason::WRITE_SPLIT || reason == RelocateReason::SIZE_SPLIT);
	parent_range = parent;
}

Optional<KeyRange> RelocateShard::getParentRange() const {
	return parent_range;
}

ShardSizeBounds ShardSizeBounds::shardSizeBoundsBeforeTrack() {
	return ShardSizeBounds{ .max = StorageMetrics{ .bytes = -1,
		                                           .bytesWrittenPerKSecond = StorageMetrics::infinity,
		                                           .iosPerKSecond = StorageMetrics::infinity,
		                                           .bytesReadPerKSecond = StorageMetrics::infinity,
		                                           .opsReadPerKSecond = StorageMetrics::infinity },
		                    .min = StorageMetrics{ .bytes = -1,
		                                           .bytesWrittenPerKSecond = 0,
		                                           .iosPerKSecond = 0,
		                                           .bytesReadPerKSecond = 0,
		                                           .opsReadPerKSecond = 0 },
		                    .permittedError = StorageMetrics{ .bytes = -1,
		                                                      .bytesWrittenPerKSecond = StorageMetrics::infinity,
		                                                      .iosPerKSecond = StorageMetrics::infinity,
		                                                      .bytesReadPerKSecond = StorageMetrics::infinity,
		                                                      .opsReadPerKSecond = StorageMetrics::infinity } };
}

namespace {

std::set<int> const& normalDDQueueErrors() {
	static std::set<int> s{ error_code_movekeys_conflict,
		                    error_code_broken_promise,
		                    error_code_data_move_cancelled,
		                    error_code_data_move_dest_team_not_found };
	return s;
}

} // anonymous namespace

enum class DDAuditContext : uint8_t {
	INVALID = 0,
	RESUME = 1,
	LAUNCH = 2,
	RETRY = 3,
};

struct DDAudit {
	DDAudit(AuditStorageState coreState)
	  : coreState(coreState), actors(true), foundError(false), auditStorageAnyChildFailed(false), retryCount(0),
	    cancelled(false), overallCompleteDoAuditCount(0), overallIssuedDoAuditCount(0), overallSkippedDoAuditCount(0),
	    remainingBudgetForAuditTasks(SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX), context(DDAuditContext::INVALID) {}

	AuditStorageState coreState;
	ActorCollection actors;
	Future<Void> auditActor;
	bool foundError;
	int retryCount;
	bool auditStorageAnyChildFailed;
	bool cancelled; // use to cancel any actor beyond auditActor
	int64_t overallIssuedDoAuditCount;
	int64_t overallCompleteDoAuditCount;
	int64_t overallSkippedDoAuditCount;
	AsyncVar<int> remainingBudgetForAuditTasks;
	DDAuditContext context;
	std::unordered_set<UID> serversFinishedSSShardAudit; // dedicated to ssshard

	inline void setAuditRunActor(Future<Void> actor) { auditActor = actor; }
	inline Future<Void> getAuditRunActor() { return auditActor; }

	inline void setDDAuditContext(DDAuditContext context_) { this->context = context_; }
	inline DDAuditContext getDDAuditContext() const { return context; }

	// auditActor and actors are guaranteed to deliver a cancel signal
	void cancel() {
		auditActor.cancel();
		actors.clear(true);
		cancelled = true;
	}

	bool isCancelled() const { return cancelled; }
};

void DataMove::validateShard(const DDShardInfo& shard, KeyRangeRef range, int priority) {
	if (!valid) {
		if (shard.hasDest && shard.destId != anonymousShardId) {
			TraceEvent(SevError, "DataMoveValidationError")
			    .detail("Range", range)
			    .detail("Reason", "DataMoveMissing")
			    .detail("DestID", shard.destId)
			    .detail("ShardPrimaryDest", describe(shard.primaryDest))
			    .detail("ShardRemoteDest", describe(shard.remoteDest));
		}
		return;
	}

	ASSERT(!this->meta.ranges.empty() && this->meta.ranges.front().contains(range));

	if (!shard.hasDest) {
		TraceEvent(SevWarnAlways, "DataMoveValidationError")
		    .detail("Range", range)
		    .detail("Reason", "ShardMissingDest")
		    .detail("DataMoveMetaData", this->meta.toString())
		    .detail("DataMovePrimaryDest", describe(this->primaryDest))
		    .detail("DataMoveRemoteDest", describe(this->remoteDest));
		cancelled = true;
		return;
	}

	if (shard.destId != this->meta.id) {
		TraceEvent(SevWarnAlways, "DataMoveValidationError")
		    .detail("Range", range)
		    .detail("Reason", "DataMoveIDMissMatch")
		    .detail("DataMoveMetaData", this->meta.toString())
		    .detail("ShardMoveID", shard.destId);
		cancelled = true;
		return;
	}

	if (!std::equal(
	        this->primaryDest.begin(), this->primaryDest.end(), shard.primaryDest.begin(), shard.primaryDest.end()) ||
	    !std::equal(
	        this->remoteDest.begin(), this->remoteDest.end(), shard.remoteDest.begin(), shard.remoteDest.end())) {
		TraceEvent(g_network->isSimulated() ? SevWarn : SevError, "DataMoveValidationError")
		    .detail("Range", range)
		    .detail("Reason", "DataMoveDestMissMatch")
		    .detail("DataMoveMetaData", this->meta.toString())
		    .detail("DataMovePrimaryDest", describe(this->primaryDest))
		    .detail("DataMoveRemoteDest", describe(this->remoteDest))
		    .detail("ShardPrimaryDest", describe(shard.primaryDest))
		    .detail("ShardRemoteDest", describe(shard.remoteDest));
		cancelled = true;
	}
}

Future<Void> StorageWiggler::onCheck() const {
	return delay(MIN_ON_CHECK_DELAY_SEC);
}

// add server to wiggling queue
void StorageWiggler::addServer(const UID& serverId, const StorageMetadataType& metadata) {
	// std::cout << "size: " << pq_handles.size() << " add " << serverId.toString() << " DC: "
	//           << teamCollection->isPrimary() << std::endl;
	ASSERT(!pq_handles.contains(serverId));
	pq_handles[serverId] = wiggle_pq.emplace(metadata, serverId);
}

void StorageWiggler::removeServer(const UID& serverId) {
	// std::cout << "size: " << pq_handles.size() << " remove " << serverId.toString() << " DC: "
	//           << teamCollection->isPrimary() << std::endl;
	if (contains(serverId)) { // server haven't been popped
		auto handle = pq_handles.at(serverId);
		pq_handles.erase(serverId);
		wiggle_pq.erase(handle);
	}
}

void StorageWiggler::updateMetadata(const UID& serverId, const StorageMetadataType& metadata) {
	//	std::cout << "size: " << pq_handles.size() << " update " << serverId.toString()
	//	          << " DC: " << teamCollection->isPrimary() << std::endl;
	auto handle = pq_handles.at(serverId);
	if ((*handle).first == metadata) {
		return;
	}
	wiggle_pq.update(handle, std::make_pair(metadata, serverId));
}

bool StorageWiggler::necessary(const UID& serverId, const StorageMetadataType& metadata) const {
	return metadata.wrongConfigured || (now() - metadata.createdTime > SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC);
}

Optional<UID> StorageWiggler::getNextServerId(bool necessaryOnly) {
	if (!wiggle_pq.empty()) {
		auto [metadata, id] = wiggle_pq.top();
		if (necessaryOnly && !necessary(id, metadata)) {
			return {};
		}
		wiggle_pq.pop();
		pq_handles.erase(id);
		return Optional<UID>(id);
	}
	return Optional<UID>();
}

Future<Void> StorageWiggler::resetStats() {
	metrics.reset();
	return runRYWTransaction(
	    teamCollection->dbContext(), [this](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		    return wiggleData.resetStorageWiggleMetrics(tr, PrimaryRegion(teamCollection->isPrimary()), metrics);
	    });
}

Future<Void> StorageWiggler::restoreStats() {
	auto readFuture = wiggleData.storageWiggleMetrics(PrimaryRegion(teamCollection->isPrimary()))
	                      .getD(teamCollection->dbContext().getReference(), Snapshot::False, metrics);
	return store(metrics, readFuture);
}

Future<Void> StorageWiggler::startWiggle() {
	metrics.last_wiggle_start = StorageMetadataType::currentTime();
	if (shouldStartNewRound()) {
		metrics.last_round_start = metrics.last_wiggle_start;
	}
	return runRYWTransaction(
	    teamCollection->dbContext(), [this](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		    return wiggleData.updateStorageWiggleMetrics(tr, metrics, PrimaryRegion(teamCollection->isPrimary()));
	    });
}

Future<Void> StorageWiggler::finishWiggle() {
	metrics.last_wiggle_finish = StorageMetadataType::currentTime();
	metrics.finished_wiggle += 1;
	auto duration = metrics.last_wiggle_finish - metrics.last_wiggle_start;
	metrics.smoothed_wiggle_duration.setTotal((double)duration);

	if (shouldFinishRound()) {
		metrics.last_round_finish = metrics.last_wiggle_finish;
		metrics.finished_round += 1;
		duration = metrics.last_round_finish - metrics.last_round_start;
		metrics.smoothed_round_duration.setTotal((double)duration);
	}
	return runRYWTransaction(
	    teamCollection->dbContext(), [this](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		    return wiggleData.updateStorageWiggleMetrics(tr, metrics, PrimaryRegion(teamCollection->isPrimary()));
	    });
}

ACTOR Future<Void> remoteRecovered(Reference<AsyncVar<ServerDBInfo> const> db) {
	TraceEvent("DDTrackerStarting").log();
	while (db->get().recoveryState < RecoveryState::ALL_LOGS_RECRUITED) {
		TraceEvent("DDTrackerStarting").detail("RecoveryState", (int)db->get().recoveryState);
		wait(db->onChange());
	}
	return Void();
}

// Ensures that the serverKeys key space is properly coalesced
// This method is only used for testing and is not implemented in a manner that is safe for large databases
ACTOR Future<Void> debugCheckCoalescing(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			state RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

			state int i;
			for (i = 0; i < serverList.size(); i++) {
				state UID id = decodeServerListValue(serverList[i].value).id();
				RangeResult ranges = wait(krmGetRanges(&tr, serverKeysPrefixFor(id), allKeys));
				ASSERT(ranges.end()[-1].key == allKeys.end);

				for (int j = 0; j < ranges.size() - 2; j++)
					if (ranges[j].value == ranges[j + 1].value)
						TraceEvent(SevError, "UncoalescedValues", id)
						    .detail("Key1", ranges[j].key)
						    .detail("Key2", ranges[j + 1].key)
						    .detail("Value", ranges[j].value);
			}

			TraceEvent("DoneCheckingCoalescing").log();
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

struct DataDistributor;
void runAuditStorage(
    Reference<DataDistributor> self,
    AuditStorageState auditStates,
    int retryCount,
    DDAuditContext context,
    Optional<std::unordered_set<UID>> serversFinishedSSShardAudit = Optional<std::unordered_set<UID>>());
ACTOR Future<Void> auditStorageCore(Reference<DataDistributor> self,
                                    UID auditID,
                                    AuditType auditType,
                                    int currentRetryCount);
ACTOR Future<UID> launchAudit(Reference<DataDistributor> self,
                              KeyRange auditRange,
                              AuditType auditType,
                              KeyValueStoreType auditStorageEngineType);
ACTOR Future<Void> auditStorage(Reference<DataDistributor> self, TriggerAuditRequest req);
void loadAndDispatchAudit(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit);
ACTOR Future<Void> dispatchAuditStorageServerShard(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit);
ACTOR Future<Void> scheduleAuditStorageShardOnServer(Reference<DataDistributor> self,
                                                     std::shared_ptr<DDAudit> audit,
                                                     StorageServerInterface ssi);
ACTOR Future<Void> dispatchAuditStorage(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit);
ACTOR Future<Void> dispatchAuditLocationMetadata(Reference<DataDistributor> self,
                                                 std::shared_ptr<DDAudit> audit,
                                                 KeyRange range);
ACTOR Future<Void> doAuditLocationMetadata(Reference<DataDistributor> self,
                                           std::shared_ptr<DDAudit> audit,
                                           KeyRange auditRange);
ACTOR Future<Void> scheduleAuditOnRange(Reference<DataDistributor> self,
                                        std::shared_ptr<DDAudit> audit,
                                        KeyRange range);
ACTOR Future<Void> doAuditOnStorageServer(Reference<DataDistributor> self,
                                          std::shared_ptr<DDAudit> audit,
                                          StorageServerInterface ssi,
                                          AuditStorageRequest req);
ACTOR Future<Void> skipAuditOnRange(Reference<DataDistributor> self,
                                    std::shared_ptr<DDAudit> audit,
                                    KeyRange rangeToSkip);

void runBulkLoadTaskAsync(Reference<DataDistributor> self, KeyRange range, UID taskId, bool restart);
ACTOR Future<Void> scheduleBulkLoadTasks(Reference<DataDistributor> self);

struct DataDistributor : NonCopyable, ReferenceCounted<DataDistributor> {
public:
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<DDSharedContext> context;
	UID ddId;
	PromiseStream<Future<Void>> addActor;

	// State initialized when bootstrap
	Reference<IDDTxnProcessor> txnProcessor;
	MoveKeysLock& lock; // reference to context->lock
	DatabaseConfiguration& configuration; // reference to context->configuration
	std::vector<Optional<Key>> primaryDcId;
	std::vector<Optional<Key>> remoteDcIds;
	Reference<InitialDataDistribution> initData;

	Reference<EventCacheHolder> initialDDEventHolder;
	Reference<EventCacheHolder> movingDataEventHolder;
	Reference<EventCacheHolder> totalDataInFlightEventHolder;
	Reference<EventCacheHolder> totalDataInFlightRemoteEventHolder;

	// Optional components that can be set after ::init(). They're optional when test, but required for DD being
	// fully-functional.
	DDTeamCollection* teamCollection;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	// consumer is a yield stream from producer. The RelocateShard is pushed into relocationProducer and popped from
	// relocationConsumer (by DDQueue)
	PromiseStream<RelocateShard> relocationProducer, relocationConsumer;
	PromiseStream<BulkLoadShardRequest> triggerShardBulkLoading;
	Reference<PhysicalShardCollection> physicalShardCollection;
	Reference<BulkLoadTaskCollection> bulkLoadTaskCollection;

	Promise<Void> initialized;

	std::unordered_map<AuditType, std::unordered_map<UID, std::shared_ptr<DDAudit>>> audits;
	FlowLock auditStorageHaLaunchingLock;
	FlowLock auditStorageReplicaLaunchingLock;
	FlowLock auditStorageLocationMetadataLaunchingLock;
	FlowLock auditStorageSsShardLaunchingLock;
	Promise<Void> auditStorageInitialized;
	bool auditStorageInitStarted;

	Optional<Reference<TenantCache>> ddTenantCache;

	// monitor DD configuration change
	Promise<Version> configChangeWatching;
	Future<Void> onConfigChange;

	ActorCollection bulkLoadActors;
	bool bulkLoadEnabled = false;

	bool bulkDumpEnabled = false;
	KeyRangeActorMap ongoingBulkDumpActors;
	ParallelismLimitor bulkDumpParallelismLimitor;
	std::string folder;
	std::string bulkDumpFolder;

	DataDistributor(Reference<AsyncVar<ServerDBInfo> const> const& db,
	                UID id,
	                Reference<DDSharedContext> context,
	                std::string folder)
	  : dbInfo(db), context(context), ddId(id), txnProcessor(nullptr), lock(context->lock),
	    configuration(context->configuration), initialDDEventHolder(makeReference<EventCacheHolder>("InitialDD")),
	    movingDataEventHolder(makeReference<EventCacheHolder>("MovingData")),
	    totalDataInFlightEventHolder(makeReference<EventCacheHolder>("TotalDataInFlight")),
	    totalDataInFlightRemoteEventHolder(makeReference<EventCacheHolder>("TotalDataInFlightRemote")),
	    teamCollection(nullptr), bulkLoadTaskCollection(nullptr), auditStorageHaLaunchingLock(1),
	    auditStorageReplicaLaunchingLock(1), auditStorageLocationMetadataLaunchingLock(1),
	    auditStorageSsShardLaunchingLock(1), auditStorageInitStarted(false), bulkLoadActors(false),
	    bulkLoadEnabled(false), bulkDumpEnabled(false),
	    bulkDumpParallelismLimitor(SERVER_KNOBS->DD_BULKDUMP_PARALLELISM), folder(folder) {
		if (!folder.empty()) {
			bulkDumpFolder = abspath(joinPath(folder, ddServerBulkDumpFolder));
			// TODO(BulkDump): clear this folder in the presence of crash
		}
	}

	// bootstrap steps

	Future<Void> takeMoveKeysLock() { return store(lock, txnProcessor->takeMoveKeysLock(ddId)); }

	Future<Void> loadDatabaseConfiguration() { return store(configuration, txnProcessor->getDatabaseConfiguration()); }

	Future<Void> updateReplicaKeys() {
		return txnProcessor->updateReplicaKeys(primaryDcId, remoteDcIds, configuration);
	}

	Future<Void> loadInitialDataDistribution() {
		return store(initData,
		             txnProcessor->getInitialDataDistribution(
		                 ddId,
		                 lock,
		                 configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>(),
		                 context->ddEnabledState.get(),
		                 SkipDDModeCheck::False));
	}

	void initDcInfo() {
		primaryDcId.clear();
		remoteDcIds.clear();
		const std::vector<RegionInfo>& regions = configuration.regions;
		if (configuration.regions.size() > 0) {
			primaryDcId.push_back(regions[0].dcId);
		}
		if (configuration.regions.size() > 1) {
			remoteDcIds.push_back(regions[1].dcId);
		}
	}

	Future<Void> waitDataDistributorEnabled() const {
		return txnProcessor->waitForDataDistributionEnabled(context->ddEnabledState.get());
	}

	// Resume in-memory audit instances and issue background audit metadata cleanup
	void resumeAuditStorage(Reference<DataDistributor> self, std::vector<AuditStorageState> auditStates) {
		for (const auto& auditState : auditStates) {
			if (auditState.getPhase() != AuditPhase::Running) {
				TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "WrongAuditStateToResume")
				    .detail("AuditState", auditState.toString());
				return;
			}
			if (self->audits.contains(auditState.getType()) &&
			    self->audits[auditState.getType()].contains(auditState.id)) {
				// Ignore any RUNNING auditState with an alive audit
				// instance in DD audits map
				continue;
			}
			runAuditStorage(self, auditState, 0, DDAuditContext::RESUME);
			TraceEvent(SevInfo, "AuditStorageResumed", self->ddId)
			    .detail("AuditID", auditState.id)
			    .detail("AuditType", auditState.getType())
			    .detail("AuditState", auditState.toString());
		}
		return;
	}

	ACTOR static Future<Void> initAuditStorage(Reference<DataDistributor> self) {
		self->auditStorageInitStarted = true;
		MoveKeyLockInfo lockInfo;
		lockInfo.myOwner = self->lock.myOwner;
		lockInfo.prevOwner = self->lock.prevOwner;
		lockInfo.prevWrite = self->lock.prevWrite;
		std::vector<AuditStorageState> auditStatesToResume =
		    wait(initAuditMetadata(self->txnProcessor->context(),
		                           lockInfo,
		                           self->context->isDDEnabled(),
		                           self->ddId,
		                           SERVER_KNOBS->PERSIST_FINISH_AUDIT_COUNT));
		self->resumeAuditStorage(self, auditStatesToResume);
		self->auditStorageInitialized.send(Void());
		return Void();
	}

	ACTOR static Future<Void> waitUntilDataDistributorExitSecurityMode(Reference<DataDistributor> self) {
		state Transaction tr(self->txnProcessor->context());
		loop {
			wait(delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskPriority::DataDistribution));
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			try {
				Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
				if (!mode.present()) {
					return Void();
				}
				BinaryReader rd(mode.get(), Unversioned());
				int ddMode = 1;
				rd >> ddMode;
				if (ddMode != 2) {
					return Void();
				}
				wait(checkMoveKeysLockReadOnly(&tr, self->context->lock, self->context->ddEnabledState.get()));
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Initialize the required internal states of DataDistributor from system metadata. It's necessary before
	// DataDistributor start working. Doesn't include initialization of optional components, like TenantCache, DDQueue,
	// Tracker, TeamCollection. The components should call its own ::init methods.
	ACTOR static Future<Void> init(Reference<DataDistributor> self) {
		loop {
			wait(self->waitDataDistributorEnabled());
			TraceEvent("DataDistributionEnabled").log();

			TraceEvent("DDInitTakingMoveKeysLock", self->ddId).log();
			wait(self->takeMoveKeysLock());
			TraceEvent("DDInitTookMoveKeysLock", self->ddId).log();

			// AuditStorage does not rely on DatabaseConfiguration
			// AuditStorage read necessary info purely from system key space
			if (!self->auditStorageInitStarted) {
				// AuditStorage currently does not support DDMockTxnProcessor
				if (!self->txnProcessor->isMocked()) {
					// Avoid multiple initAuditStorages
					self->addActor.send(self->initAuditStorage(self));
				}
			}
			// It is possible that an audit request arrives and then DDMode
			// is set to 2 at this point
			// No polling MoveKeyLock is running
			// So, we need to check MoveKeyLock when waitUntilDataDistributorExitSecurityMode
			if (!self->txnProcessor->isMocked()) {
				// AuditStorage currently does not support DDMockTxnProcessor
				wait(waitUntilDataDistributorExitSecurityMode(self)); // Trap DDMode == 2
			}
			// It is possible DDMode begins with 2 and passes
			// waitDataDistributorEnabled and then set to 0 before
			// waitUntilDataDistributorExitSecurityMode. For this case,
			// after waitUntilDataDistributorExitSecurityMode, DDMode is 0.
			// The init loop does not break and the loop will stuct at
			// waitDataDistributorEnabled in the next iteration.
			TraceEvent("DataDistributorExitSecurityMode").log();

			wait(self->loadDatabaseConfiguration());
			self->initDcInfo();
			TraceEvent("DDInitGotConfiguration", self->ddId)
			    .setMaxFieldLength(-1)
			    .detail("Conf", self->configuration.toString());

			if (self->configuration.storageServerStoreType == KeyValueStoreType::SSD_SHARDED_ROCKSDB &&
			    !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
				TraceEvent(SevError, "PhysicalShardNotEnabledForShardedRocks", self->ddId)
				    .detail("EnableServerKnob", "SHARD_ENCODE_LOCATION_METADATA");
				throw internal_error();
			}

			wait(self->updateReplicaKeys());
			TraceEvent("DDInitUpdatedReplicaKeys", self->ddId).log();

			wait(self->loadInitialDataDistribution());

			if (self->initData->shards.size() > 1) {
				TraceEvent("DDInitGotInitialDD", self->ddId)
				    .detail("B", self->initData->shards.end()[-2].key)
				    .detail("E", self->initData->shards.end()[-1].key)
				    .detail("Src", describe(self->initData->shards.end()[-2].primarySrc))
				    .detail("Dest", describe(self->initData->shards.end()[-2].primaryDest))
				    .trackLatest(self->initialDDEventHolder->trackingKey);
			} else {
				TraceEvent("DDInitGotInitialDD", self->ddId)
				    .detail("B", "")
				    .detail("E", "")
				    .detail("Src", "[no items]")
				    .detail("Dest", "[no items]")
				    .trackLatest(self->initialDDEventHolder->trackingKey);
			}

			if (self->initData->mode == 1 && self->context->isDDEnabled()) {
				// mode may be set true by system operator using fdbcli and isEnabled() set to true
				break;
			}

			TraceEvent("DataDistributionDisabled", self->ddId).log();

			TraceEvent("MovingData", self->ddId)
			    .detail("InFlight", 0)
			    .detail("InQueue", 0)
			    .detail("AverageShardSize", -1)
			    .detail("UnhealthyRelocations", 0)
			    .detail("HighestPriority", 0)
			    .detail("BytesWritten", 0)
			    .detail("BytesWrittenAverageRate", 0)
			    .detail("PriorityRecoverMove", 0)
			    .detail("PriorityRebalanceUnderutilizedTeam", 0)
			    .detail("PriorityRebalannceOverutilizedTeam", 0)
			    .detail("PriorityTeamHealthy", 0)
			    .detail("PriorityTeamContainsUndesiredServer", 0)
			    .detail("PriorityTeamRedundant", 0)
			    .detail("PriorityMergeShard", 0)
			    .detail("PriorityTeamUnhealthy", 0)
			    .detail("PriorityTeam2Left", 0)
			    .detail("PriorityTeam1Left", 0)
			    .detail("PriorityTeam0Left", 0)
			    .detail("PrioritySplitShard", 0)
			    .trackLatest(self->movingDataEventHolder->trackingKey);

			TraceEvent("TotalDataInFlight", self->ddId)
			    .detail("Primary", true)
			    .detail("TotalBytes", 0)
			    .detail("UnhealthyServers", 0)
			    .detail("HighestPriority", 0)
			    .trackLatest(self->totalDataInFlightEventHolder->trackingKey);
			TraceEvent("TotalDataInFlight", self->ddId)
			    .detail("Primary", false)
			    .detail("TotalBytes", 0)
			    .detail("UnhealthyServers", 0)
			    .detail("HighestPriority", self->configuration.usableRegions > 1 ? 0 : -1)
			    .trackLatest(self->totalDataInFlightRemoteEventHolder->trackingKey);
		}
		return Void();
	}

	ACTOR static Future<Void> removeDataMoveTombstoneBackground(Reference<DataDistributor> self) {
		state UID currentID;
		try {
			state Database cx = openDBOnServer(self->dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
			state Transaction tr(cx);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					for (UID& dataMoveID : self->initData->toCleanDataMoveTombstone) {
						currentID = dataMoveID;
						tr.clear(dataMoveKeyFor(currentID));
						TraceEvent(SevDebug, "RemoveDataMoveTombstone", self->ddId).detail("DataMoveID", currentID);
					}
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevWarn, "RemoveDataMoveTombstoneError", self->ddId)
			    .errorUnsuppressed(e)
			    .detail("CurrentDataMoveID", currentID);
			// DD needs not restart when removing tombstone gets failed unless this actor gets cancelled
			// So, do not throw error
		}
		return Void();
	}

	ACTOR static Future<Void> resumeFromShards(Reference<DataDistributor> self, bool traceShard) {
		// All physicalShard init must be completed before issuing data move
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
			for (int i = 0; i < self->initData->shards.size() - 1; i++) {
				const DDShardInfo& iShard = self->initData->shards[i];
				KeyRangeRef keys = KeyRangeRef(iShard.key, self->initData->shards[i + 1].key);
				std::vector<ShardsAffectedByTeamFailure::Team> teams;
				teams.emplace_back(iShard.primarySrc, /*primary=*/true);
				if (self->configuration.usableRegions > 1) {
					teams.emplace_back(iShard.remoteSrc, /*primary=*/false);
				}
				self->physicalShardCollection->initPhysicalShardCollection(keys, teams, iShard.srcId.first(), 0);
			}
		}

		state std::vector<Key> customBoundaries;
		if (bulkLoadIsEnabled(self->initData->bulkLoadMode)) {
			// Bulk load does not allow boundary change
			TraceEvent(SevInfo, "DDInitCustomRangeConfigDisabledByBulkLoadMode", self->ddId);
		} else {
			for (auto it : self->initData->userRangeConfig->ranges()) {
				auto range = it->range();
				customBoundaries.push_back(range.begin);
				TraceEvent(SevDebug, "DDInitCustomRangeConfig", self->ddId)
				    .detail("Range", KeyRangeRef(range.begin, range.end))
				    .detail("Config", it->value());
			}
		}

		state int shard = 0;
		state int customBoundary = 0;
		state int overreplicatedCount = 0;
		for (; shard < self->initData->shards.size() - 1; shard++) {
			const DDShardInfo& iShard = self->initData->shards[shard];
			std::vector<KeyRangeRef> ranges;

			Key beginKey = iShard.key;
			Key endKey = self->initData->shards[shard + 1].key;
			while (customBoundary < customBoundaries.size() && customBoundaries[customBoundary] <= beginKey) {
				customBoundary++;
			}
			while (customBoundary < customBoundaries.size() && customBoundaries[customBoundary] < endKey) {
				ranges.push_back(KeyRangeRef(beginKey, customBoundaries[customBoundary]));
				beginKey = customBoundaries[customBoundary];
				customBoundary++;
			}
			ranges.push_back(KeyRangeRef(beginKey, endKey));

			std::vector<ShardsAffectedByTeamFailure::Team> teams;
			teams.push_back(ShardsAffectedByTeamFailure::Team(iShard.primarySrc, true));
			if (self->configuration.usableRegions > 1) {
				teams.push_back(ShardsAffectedByTeamFailure::Team(iShard.remoteSrc, false));
			}

			for (int r = 0; r < ranges.size(); r++) {
				auto& keys = ranges[r];
				self->shardsAffectedByTeamFailure->defineShard(keys);

				auto it = self->initData->userRangeConfig->rangeContaining(keys.begin);
				int customReplicas =
				    std::max(self->configuration.storageTeamSize, it->value().replicationFactor.orDefault(0));
				ASSERT_WE_THINK(KeyRangeRef(it->range().begin, it->range().end).contains(keys));

				bool unhealthy = iShard.primarySrc.size() != customReplicas;
				if (!unhealthy && self->configuration.usableRegions > 1) {
					unhealthy = iShard.remoteSrc.size() != customReplicas;
				}
				if (!unhealthy && iShard.primarySrc.size() > self->configuration.storageTeamSize) {
					if (++overreplicatedCount > SERVER_KNOBS->DD_MAX_SHARDS_ON_LARGE_TEAMS) {
						unhealthy = true;
					}
				}

				if (traceShard) {
					TraceEvent(SevDebug, "DDInitShard", self->ddId)
					    .detail("Keys", keys)
					    .detail("PrimarySrc", describe(iShard.primarySrc))
					    .detail("RemoteSrc", describe(iShard.remoteSrc))
					    .detail("PrimaryDest", describe(iShard.primaryDest))
					    .detail("RemoteDest", describe(iShard.remoteDest))
					    .detail("SrcID", iShard.srcId)
					    .detail("DestID", iShard.destId)
					    .detail("CustomReplicas", customReplicas)
					    .detail("StorageTeamSize", self->configuration.storageTeamSize)
					    .detail("Unhealthy", unhealthy)
					    .detail("Overreplicated", overreplicatedCount);
				}

				self->shardsAffectedByTeamFailure->moveShard(keys, teams);
				if ((ddLargeTeamEnabled() && (unhealthy || r > 0)) ||
				    (iShard.hasDest && iShard.destId == anonymousShardId)) {
					// This shard is already in flight.  Ideally we should use dest in ShardsAffectedByTeamFailure and
					// generate a dataDistributionRelocator directly in DataDistributionQueue to track it, but it's
					// easier to just (with low priority) schedule it for movement.
					DataMovementReason reason = DataMovementReason::RECOVER_MOVE;
					if (unhealthy) {
						reason = DataMovementReason::TEAM_UNHEALTHY;
					} else if (r > 0) {
						reason = DataMovementReason::SPLIT_SHARD;
					}
					self->relocationProducer.send(RelocateShard(keys, reason, RelocateReason::OTHER));
				}
			}

			wait(yield(TaskPriority::DataDistribution));
		}
		return Void();
	}

	// TODO: unit test needed
	ACTOR static Future<Void> resumeFromDataMoves(Reference<DataDistributor> self, Future<Void> readyToStart) {
		state KeyRangeMap<std::shared_ptr<DataMove>>::iterator it = self->initData->dataMoveMap.ranges().begin();

		wait(readyToStart);

		for (; it != self->initData->dataMoveMap.ranges().end(); ++it) {
			const DataMoveMetaData& meta = it.value()->meta;
			DataMoveType dataMoveType = getDataMoveTypeFromDataMoveId(meta.id);
			if (meta.ranges.empty()) {
				TraceEvent(SevInfo, "EmptyDataMoveRange", self->ddId).detail("DataMoveMetaData", meta.toString());
				continue;
			}
			if (meta.bulkLoadState.present()) {
				RelocateShard rs(meta.ranges.front(), DataMovementReason::RECOVER_MOVE, RelocateReason::OTHER);
				rs.dataMoveId = meta.id;
				rs.cancelled = true;
				self->relocationProducer.send(rs);
				// Cancel data move for old bulk loading
				// Do not assign bulk load to rs so that this is a normal data move cancellation signal
				TraceEvent("DDInitScheduledCancelOldBulkLoadDataMove", self->ddId).detail("DataMove", meta.toString());
			} else if (dataMoveType == DataMoveType::LOGICAL_BULKLOAD ||
			           dataMoveType == DataMoveType::PHYSICAL_BULKLOAD) {
				// The metadata is from the old system
				RelocateShard rs(meta.ranges.front(), DataMovementReason::RECOVER_MOVE, RelocateReason::OTHER);
				rs.dataMoveId = meta.id;
				rs.cancelled = true;
				self->relocationProducer.send(rs); // Cancal data move
				TraceEvent("DDInitScheduledCancelDataMoveForWrongType", self->ddId)
				    .detail("DataMove", meta.toString())
				    .detail("DataMoveType", dataMoveType);
			} else if (it.value()->isCancelled() ||
			           (it.value()->valid && !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA)) {
				RelocateShard rs(meta.ranges.front(), DataMovementReason::RECOVER_MOVE, RelocateReason::OTHER);
				rs.dataMoveId = meta.id;
				rs.cancelled = true;
				self->relocationProducer.send(rs);
				TraceEvent("DDInitScheduledCancelDataMove", self->ddId).detail("DataMove", meta.toString());
			} else if (it.value()->valid) {
				TraceEvent(SevDebug, "DDInitFoundDataMove", self->ddId).detail("DataMove", meta.toString());
				ASSERT(meta.ranges.front() == it.range());
				// TODO: Persist priority in DataMoveMetaData.
				RelocateShard rs(meta.ranges.front(), DataMovementReason::RECOVER_MOVE, RelocateReason::OTHER);
				rs.dataMoveId = meta.id;
				rs.dataMove = it.value();
				std::vector<ShardsAffectedByTeamFailure::Team> teams;
				teams.push_back(ShardsAffectedByTeamFailure::Team(rs.dataMove->primaryDest, true));
				if (!rs.dataMove->remoteDest.empty()) {
					teams.push_back(ShardsAffectedByTeamFailure::Team(rs.dataMove->remoteDest, false));
				}

				// Since a DataMove could cover more than one keyrange, e.g., during merge, we need to define
				// the target shard and restart the shard tracker.
				self->shardsAffectedByTeamFailure->restartShardTracker.send(rs.keys);
				self->shardsAffectedByTeamFailure->defineShard(rs.keys);

				// When restoring a DataMove, the destination team is determined, and hence we need to register
				// the data move now, so that team failures can be captured.
				self->shardsAffectedByTeamFailure->moveShard(rs.keys, teams);
				self->relocationProducer.send(rs);
				wait(yield(TaskPriority::DataDistribution));
			}
		}

		// Trigger background cleanup for datamove tombstones
		if (!self->txnProcessor->isMocked()) {
			self->addActor.send(self->removeDataMoveTombstoneBackground(self));
		}
		return Void();
	}

	// Resume inflight relocations from the previous DD
	// TODO: The initialDataDistribution is unused once resumeRelocations,
	// DataDistributionTracker::trackInitialShards, and DDTeamCollection::init are done. In the future, we can release
	// the object to save memory usage if it turns out to be a problem.
	Future<Void> resumeRelocations() {
		ASSERT(shardsAffectedByTeamFailure); // has to be allocated
		Future<Void> shardsReady = resumeFromShards(Reference<DataDistributor>::addRef(this), g_network->isSimulated());
		return resumeFromDataMoves(Reference<DataDistributor>::addRef(this), shardsReady);
	}

	Future<Void> pollMoveKeysLock() const {
		return txnProcessor->pollMoveKeysLock(lock, context->ddEnabledState.get());
	}

	Future<bool> isDataDistributionEnabled() const {
		return txnProcessor->isDataDistributionEnabled(context->ddEnabledState.get());
	}

	Future<Void> removeKeysFromFailedServer(const UID& serverID, const std::vector<UID>& teamForDroppedRange) const {
		return txnProcessor->removeKeysFromFailedServer(
		    serverID, teamForDroppedRange, lock, context->ddEnabledState.get());
	}

	Future<Void> removeStorageServer(const UID& serverID, const Optional<UID>& tssPairID = Optional<UID>()) const {
		return txnProcessor->removeStorageServer(serverID, tssPairID, lock, context->ddEnabledState.get());
	}

	Future<Void> initDDConfigWatch();

	Future<Void> initTenantCache();
};

Future<Void> DataDistributor::initDDConfigWatch() {
	if (txnProcessor->isMocked()) {
		onConfigChange = Never();
		return Void();
	}
	onConfigChange = map(DDConfiguration().trigger.onChange(
	                         SystemDBWriteLockedNow(txnProcessor->context().getReference()), {}, configChangeWatching),
	                     [](Version v) {
		                     CODE_PROBE(true, "DataDistribution change detected");
		                     TraceEvent("DataDistributionConfigChanged").detail("ChangeVersion", v);
		                     throw dd_config_changed();
		                     return Void();
	                     });

	return success(configChangeWatching.getFuture());
}

Future<Void> DataDistributor::initTenantCache() {
	// SOMEDAY: support tenant cache in MockDD
	ASSERT(!txnProcessor->isMocked());
	ddTenantCache = makeReference<TenantCache>(txnProcessor->context(), ddId);
	return ddTenantCache.get()->build();
}

inline void addAuditToAuditMap(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit) {
	AuditType auditType = audit->coreState.getType();
	UID auditID = audit->coreState.id;
	TraceEvent(SevDebug, "AuditMapOps", self->ddId)
	    .detail("Ops", "addAuditToAuditMap")
	    .detail("AuditType", auditType)
	    .detail("AuditID", auditID);
	ASSERT(!self->audits[auditType].contains(auditID));
	self->audits[auditType][auditID] = audit;
	return;
}

inline std::shared_ptr<DDAudit> getAuditFromAuditMap(Reference<DataDistributor> self,
                                                     AuditType auditType,
                                                     UID auditID) {
	TraceEvent(SevDebug, "AuditMapOps", self->ddId)
	    .detail("Ops", "getAuditFromAuditMap")
	    .detail("AuditType", auditType)
	    .detail("AuditID", auditID);
	ASSERT(self->audits.contains(auditType) && self->audits[auditType].contains(auditID));
	return self->audits[auditType][auditID];
}

inline void removeAuditFromAuditMap(Reference<DataDistributor> self, AuditType auditType, UID auditID) {
	ASSERT(self->audits.contains(auditType) && self->audits[auditType].contains(auditID));
	std::shared_ptr<DDAudit> audit = self->audits[auditType][auditID];
	audit->cancel();
	self->audits[auditType].erase(auditID);
	TraceEvent(SevDebug, "AuditMapOps", self->ddId)
	    .detail("Ops", "removeAuditFromAuditMap")
	    .detail("AuditType", auditType)
	    .detail("AuditID", auditID);
	return;
}

inline bool auditExistInAuditMap(Reference<DataDistributor> self, AuditType auditType, UID auditID) {
	return self->audits.contains(auditType) && self->audits[auditType].contains(auditID);
}

inline bool existAuditInAuditMapForType(Reference<DataDistributor> self, AuditType auditType) {
	return self->audits.contains(auditType) && !self->audits[auditType].empty();
}

inline std::unordered_map<UID, std::shared_ptr<DDAudit>> getAuditsForType(Reference<DataDistributor> self,
                                                                          AuditType auditType) {
	ASSERT(self->audits.contains(auditType));
	return self->audits[auditType];
}

// Periodically check and log the physicalShard status; clean up empty physicalShard;
ACTOR Future<Void> monitorPhysicalShardStatus(Reference<PhysicalShardCollection> self) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	loop {
		self->cleanUpPhysicalShardCollection();
		self->logPhysicalShardCollection();
		wait(delay(SERVER_KNOBS->PHYSICAL_SHARD_METRICS_DELAY));
	}
}

// This actor must be a singleton
ACTOR Future<Void> prepareDataMigration(PrepareBlobRestoreRequest req,
                                        Reference<DDSharedContext> context,
                                        Database cx) {
	try {
		// Register as a storage server, so that DataDistributor could start data movement after
		std::pair<Version, Tag> verAndTag = wait(addStorageServer(cx, req.ssi));
		TraceEvent(SevDebug, "BlobRestorePrepare", context->id())
		    .detail("State", "BMAdded")
		    .detail("ReqId", req.requesterID)
		    .detail("Version", verAndTag.first)
		    .detail("Tag", verAndTag.second);

		wait(prepareBlobRestore(
		    cx, context->lock, context->ddEnabledState.get(), context->id(), req.keys, req.ssi.id(), req.requesterID));
		req.reply.send(PrepareBlobRestoreReply(PrepareBlobRestoreReply::SUCCESS));
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw e;
		req.reply.sendError(e);
	}

	ASSERT(context->ddEnabledState->trySetEnabled(req.requesterID));
	return Void();
}

ACTOR Future<Void> serveBlobMigratorRequests(Reference<DataDistributor> self,
                                             Reference<DataDistributionTracker> tracker,
                                             Reference<DDQueue> queue) {
	wait(self->initialized.getFuture());
	loop {
		PrepareBlobRestoreRequest req = waitNext(self->context->interface.prepareBlobRestoreReq.getFuture());
		if (BlobMigratorInterface::isBlobMigrator(req.ssi.id())) {
			if (self->context->ddEnabledState->sameId(req.requesterID) &&
			    self->context->ddEnabledState->isBlobRestorePreparing()) {
				// the sender use at-least once model, so we need to guarantee the idempotence
				CODE_PROBE(true, "Receive repeated PrepareBlobRestoreRequest");
				continue;
			}
			if (self->context->ddEnabledState->trySetBlobRestorePreparing(req.requesterID)) {
				// trySetBlobRestorePreparing won't destroy DataDistributor, but will destroy tracker and queue
				self->addActor.send(prepareDataMigration(req, self->context, self->txnProcessor->context()));
				// force reloading initData and restarting DD components
				throw dd_config_changed();
			} else {
				auto reason = self->context->ddEnabledState->isBlobRestorePreparing()
				                  ? PrepareBlobRestoreReply::CONFLICT_BLOB_RESTORE
				                  : PrepareBlobRestoreReply::CONFLICT_SNAPSHOT;
				req.reply.send(PrepareBlobRestoreReply(reason));
				continue;
			}
		} else {
			req.reply.sendError(operation_failed());
		}
	}
}

// Trigger a task on range based on the current bulk load task metadata
ACTOR Future<std::pair<BulkLoadState, Version>> triggerBulkLoadTask(Reference<DataDistributor> self,
                                                                    KeyRange range,
                                                                    UID taskId,
                                                                    bool restart) {
	loop {
		Database cx = self->txnProcessor->context();
		state Transaction tr(cx);
		state BulkLoadState newBulkLoadState;
		try {
			// TODO(BulkLoad): make sure the range has been locked
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(checkMoveKeysLock(&tr, self->context->lock, self->context->ddEnabledState.get()));
			std::vector<BulkLoadPhase> phase;
			if (!restart) {
				wait(
				    store(newBulkLoadState,
				          getBulkLoadTask(&tr, range, taskId, { BulkLoadPhase::Submitted, BulkLoadPhase::Triggered })));
			} else {
				wait(store(newBulkLoadState,
				           getBulkLoadTask(&tr, range, taskId, { BulkLoadPhase::Triggered, BulkLoadPhase::Running })));
			}
			newBulkLoadState.phase = BulkLoadPhase::Triggered;
			newBulkLoadState.clearDataMoveId();
			newBulkLoadState.restartCount = newBulkLoadState.restartCount + 1;
			newBulkLoadState.triggerTime = now();
			wait(krmSetRange(&tr, bulkLoadPrefix, newBulkLoadState.getRange(), bulkLoadStateValue(newBulkLoadState)));
			wait(tr.commit());
			Version commitVersion = tr.getCommittedVersion();
			TraceEvent(SevInfo, "DDBulkLoadTaskTriggeredPersist", self->ddId)
			    .detail("CommitVersion", commitVersion)
			    .detail("BulkLoadState", newBulkLoadState.toString());
			ASSERT(commitVersion != invalidVersion);
			return std::make_pair(newBulkLoadState, commitVersion);

		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevInfo, "DDBulkLoadTaskTriggeredPersistError", self->ddId)
				    .errorUnsuppressed(e)
				    .detail("BulkLoadState", newBulkLoadState.toString());
			}
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> tryStartBulkLoadTaskUntilSucceed(Reference<DataDistributor> self) {
	loop {
		if (self->bulkLoadTaskCollection->tryStart()) {
			break;
		}
		wait(self->bulkLoadTaskCollection->waitUntilChanged());
	}
	return Void();
}

ACTOR Future<Void> waitUntilBulkLoadTaskCanStart(Reference<DataDistributor> self) {
	loop {
		if (self->bulkLoadTaskCollection->canStart()) {
			break;
		}
		wait(self->bulkLoadTaskCollection->waitUntilChanged());
	}
	return Void();
}

// A bulk load task is guaranteed to be either complete or overwritten by another task
// When a bulk load task is trigged, the range traffic is turned off atomically
// If the task completes, the task re-enables the traffic atomically
ACTOR Future<Void> doBulkLoadTask(Reference<DataDistributor> self, KeyRange range, UID taskId, bool restart) {
	state Promise<Void> completeAck;
	state BulkLoadState triggeredBulkLoadTask;
	state Version commitVersion = invalidVersion;
	TraceEvent(SevInfo, "DDBulkLoadDoBulkLoadBegin", self->ddId)
	    .detail("Range", range)
	    .detail("TaskID", taskId)
	    .detail("Restart", restart);
	wait(tryStartBulkLoadTaskUntilSucceed(self)); // increase the task counter when succeed
	try {
		// Step 1: persist bulk load task phase as triggered
		std::pair<BulkLoadState, Version> triggeredBulkLoadTask_ =
		    wait(triggerBulkLoadTask(self, range, taskId, restart));
		triggeredBulkLoadTask = triggeredBulkLoadTask_.first;
		commitVersion = triggeredBulkLoadTask_.second;
		TraceEvent(SevInfo, "DDBulkLoadDoBulkLoadTaskTriggered", self->ddId)
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("Task", triggeredBulkLoadTask.toString())
		    .detail("CommitVersion", commitVersion)
		    .detail("Restart", restart);
		ASSERT(triggeredBulkLoadTask.getRange() == range);

		// Step 2: submit the task to in-memory task map, which (1) turns off shard boundary change;
		// (2) when starting a data move on the task range, the task will be attached to the data move;
		// (3) when the data move completes, the completeAck is satisfied. So, waiting on completeAck
		// can get notified when the task is completed by a data move
		self->bulkLoadTaskCollection->publishTask(triggeredBulkLoadTask, commitVersion, completeAck);

		// Step 3: create bulk load shard and trigger data move and wait for task completion
		// The completion of the task relies on the fact that a data move on a range is either
		// completed by itself or replaced by a data move on the overlapping range
		self->triggerShardBulkLoading.send(BulkLoadShardRequest(triggeredBulkLoadTask));
		wait(completeAck.getFuture()); // proceed when a data move completes with this task
		TraceEvent(SevInfo, "DDBulkLoadDoBulkLoadTaskComplete", self->ddId)
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("Task", triggeredBulkLoadTask.toString())
		    .detail("Restart", restart);
		self->bulkLoadTaskCollection->decrementTaskCounter();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "DDBulkLoadDoBulkLoadTaskFailed", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("Range", range)
		    .detail("TaskID", taskId)
		    .detail("Restart", restart);
		if (e.code() == error_code_bulkload_task_outdated) {
			self->bulkLoadTaskCollection->decrementTaskCounter();
			// sliently exits
		} else if (e.code() == error_code_movekeys_conflict) {
			throw e;
		} else {
			// retry by spawning a new one
			runBulkLoadTaskAsync(self, range, taskId, true);
		}
	}
	return Void();
}

ACTOR Future<Void> eraseBulkLoadTask(Reference<DataDistributor> self, KeyRange range, UID taskId) {
	state Database cx = self->txnProcessor->context();
	state Transaction tr(cx);
	state BulkLoadState bulkLoadTask;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(store(bulkLoadTask, getBulkLoadTask(&tr, range, taskId, { BulkLoadPhase::Acknowledged })));
			wait(krmSetRangeCoalescing(&tr, bulkLoadPrefix, range, normalKeys, StringRef()));
			wait(tr.commit());
			self->bulkLoadTaskCollection->eraseTask(bulkLoadTask);
			break;
		} catch (Error& e) {
			if (e.code() == error_code_bulkload_task_outdated) {
				// Silently exit
				break;
			}
			wait(tr.onError(e));
		}
	}
	return Void();
}

void runBulkLoadTaskAsync(Reference<DataDistributor> self, KeyRange range, UID taskId, bool restart) {
	TraceEvent(SevInfo, "DDBulkLoadTaskRunAsync", self->ddId)
	    .detail("Range", range)
	    .detail("TaskId", taskId)
	    .detail("Restart", restart);
	self->bulkLoadActors.add(doBulkLoadTask(self, range, taskId, restart));
	return;
}

void eraseBulkLoadTaskAsync(Reference<DataDistributor> self, KeyRange range, UID taskId, bool restart) {
	TraceEvent(SevInfo, "DDBulkLoadTaskEraseAsync", self->ddId)
	    .detail("Range", range)
	    .detail("TaskId", taskId)
	    .detail("Restart", restart);
	self->bulkLoadActors.add(eraseBulkLoadTask(self, range, taskId));
	return;
}

ACTOR Future<Void> scheduleBulkLoadTasks(Reference<DataDistributor> self) {
	state Key beginKey = normalKeys.begin;
	state Key endKey = normalKeys.end;
	state KeyRange rangeToRead;
	state Database cx = self->txnProcessor->context();
	state Transaction tr(cx);
	state int i = 0;
	state BulkLoadState bulkLoadState;
	state RangeResult result;
	while (beginKey < endKey) {
		try {
			rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
			result.clear();
			wait(store(
			    result,
			    krmGetRanges(&tr, bulkLoadPrefix, rangeToRead, SERVER_KNOBS->DD_BULKLOAD_TASK_METADATA_READ_SIZE)));
			i = 0;
			for (; i < result.size() - 1; i++) {
				if (!result[i].value.empty()) {
					KeyRange range = Standalone(KeyRangeRef(result[i].key, result[i + 1].key));
					bulkLoadState = decodeBulkLoadState(result[i].value);
					if (range != bulkLoadState.getRange()) {
						// This task is outdated
						continue;
					} else if (bulkLoadState.phase == BulkLoadPhase::Submitted) {
						wait(waitUntilBulkLoadTaskCanStart(self));
						runBulkLoadTaskAsync(
						    self, bulkLoadState.getRange(), bulkLoadState.getTaskId(), /*restart=*/false);
					} else if (bulkLoadState.phase == BulkLoadPhase::Acknowledged) {
						eraseBulkLoadTaskAsync(
						    self, bulkLoadState.getRange(), bulkLoadState.getTaskId(), /*restart=*/false);
					} else {
						ASSERT(bulkLoadState.phase == BulkLoadPhase::Triggered ||
						       bulkLoadState.phase == BulkLoadPhase::Running ||
						       bulkLoadState.phase == BulkLoadPhase::Complete);
					}
				}
			}
			beginKey = result.back().key;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> bulkLoadTaskScheduler(Reference<DataDistributor> self) {
	loop {
		wait(scheduleBulkLoadTasks(self) && delay(SERVER_KNOBS->DD_BULKLOAD_SCHEDULE_MIN_INTERVAL_SEC));
	}
}

ACTOR Future<Void> resumeBulkLoadTasks(Reference<DataDistributor> self) {
	state Key beginKey = normalKeys.begin;
	state Key endKey = normalKeys.end;
	state KeyRange rangeToRead;
	while (beginKey < endKey) {
		Database cx = self->txnProcessor->context();
		state Transaction tr(cx);
		try {
			rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
			RangeResult result =
			    wait(krmGetRanges(&tr, bulkLoadPrefix, rangeToRead, SERVER_KNOBS->DD_BULKLOAD_TASK_METADATA_READ_SIZE));
			for (int i = 0; i < result.size() - 1; i++) {
				if (!result[i].value.empty()) {
					KeyRange range = Standalone(KeyRangeRef(result[i].key, result[i + 1].key));
					BulkLoadState bulkLoadState = decodeBulkLoadState(result[i].value);
					if (range != bulkLoadState.getRange()) {
						TraceEvent(SevWarn, "DDBulkLoadRestartTriggeredTaskFailed", self->ddId)
						    .detail("Reason", "Task boundary changed")
						    .detail("BulkLoadTask", bulkLoadState.toString())
						    .detail("RangeInSpace", range);
					} else if (bulkLoadState.phase == BulkLoadPhase::Triggered) {
						runBulkLoadTaskAsync(
						    self, bulkLoadState.getRange(), bulkLoadState.getTaskId(), /*restart=*/true);
					} else if (bulkLoadState.phase == BulkLoadPhase::Running) {
						runBulkLoadTaskAsync(
						    self, bulkLoadState.getRange(), bulkLoadState.getTaskId(), /*restart=*/true);
					} else if (bulkLoadState.phase == BulkLoadPhase::Acknowledged) {
						eraseBulkLoadTaskAsync(
						    self, bulkLoadState.getRange(), bulkLoadState.getTaskId(), /*restart=*/true);
					} else {
						TraceEvent(SevDebug, "DDBulkLoadRestartRangeNoTask", self->ddId).detail("RangeInSpace", range);
					}
				}
			}
			beginKey = result.back().key;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	TraceEvent(SevInfo, "DDBulkLoadRestartTriggeredTasksComplete", self->ddId);
	return Void();
}

ACTOR Future<Void> bulkLoadingCore(Reference<DataDistributor> self, Future<Void> readyToStart) {
	wait(readyToStart);
	state Database cx = self->txnProcessor->context();
	wait(registerRangeLockOwner(cx, rangeLockNameForBulkLoad, rangeLockNameForBulkLoad));
	wait(resumeBulkLoadTasks(self));
	TraceEvent(SevInfo, "DDBulkLoadCoreResumed", self->ddId);

	loop {
		try {
			self->bulkLoadActors.add(bulkLoadTaskScheduler(self));
			wait(self->bulkLoadActors.getResult());
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "DDBulkLoadCoreError", self->ddId).errorUnsuppressed(e);
			if (e.code() == error_code_movekeys_conflict) {
				throw e;
			}
		}
		self->bulkLoadActors.clear(false);
		wait(delay(SERVER_KNOBS->DD_BULKLOAD_SCHEDULE_MIN_INTERVAL_SEC));
	}
}

// The actor spawned by DD dedicated to listen on a SS bulkdump task and holding a budget of parallelismLimitor.
// The parallelismLimitor is used to limit the maximum concurrent bulkloading tasks spawned by DD.
// Each DD spawned task corresponds to an actual alive SS bulk dumping task.
// This actor silently exit if SS suceeds or fails to handle a task.
ACTOR Future<Void> doBulkDumpTask(Reference<DataDistributor> self,
                                  StorageServerInterface ssi,
                                  BulkDumpState bulkDumpState,
                                  std::vector<UID> checksumServers) {
	TraceEvent(SevInfo, "DDBulkDumpDoTaskStart", self->ddId)
	    .detail("TargetSS", ssi.id())
	    .detail("BulkDumpState", bulkDumpState.toString());
	try {
		ErrorOr<BulkDumpState> vResult =
		    wait(ssi.bulkdump.tryGetReply(BulkDumpRequest(checksumServers, bulkDumpState)));
		if (vResult.isError()) {
			throw vResult.getError();
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDBulkDumpDoTaskError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("TargetSS", ssi.id())
		    .detail("BulkDumpState", bulkDumpState.toString());
	}
	self->bulkDumpParallelismLimitor.decrementTaskCounter();
	TraceEvent(SevInfo, "DDBulkDumpDoTaskComplete", self->ddId)
	    .detail("TargetSS", ssi.id())
	    .detail("BulkDumpState", bulkDumpState.toString());
	return Void();
}

// The actor to expand the bulk dump metadata. If there is a task submitted,
// the actor will spawn a doBulkDumpTask working on it under the concurrency
// limitation controlled by bulkDumpParallelismLimitor.
// DD_BULKDUMP_PARALLELISM defines the maximum number of concurrent bulkdump
// tasks spawned by DD.
ACTOR Future<bool> scheduleBulkDumpTasks(Reference<DataDistributor> self) {
	state Database cx = self->txnProcessor->context();

	state Key beginKey = normalKeys.begin;
	state Key endKey = normalKeys.end;
	state KeyRange rangeToRead;

	state int bulkDumpResultIndex = 0;
	state BulkDumpState bulkDumpState;
	state KeyRange bulkDumpRange;
	state RangeResult bulkDumpResult;

	state int rangeLocationIndex = 0;
	state std::vector<IDDTxnProcessor::DDRangeLocations> rangeLocations;
	state KeyRange taskRange;
	state bool allComplete = true;
	state bool hasJob = false;

	while (beginKey < endKey) {
		state Transaction tr(cx);
		try {
			rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
			bulkDumpResult.clear();
			wait(store(
			    bulkDumpResult,
			    krmGetRanges(&tr, bulkDumpPrefix, rangeToRead, SERVER_KNOBS->DD_BULKDUMP_TASK_METADATA_READ_SIZE)));
			bulkDumpResultIndex = 0;
			for (; bulkDumpResultIndex < bulkDumpResult.size() - 1; bulkDumpResultIndex++) {
				if (bulkDumpResult[bulkDumpResultIndex].value.empty()) {
					continue;
				}
				hasJob = true;
				bulkDumpRange = Standalone(
				    KeyRangeRef(bulkDumpResult[bulkDumpResultIndex].key, bulkDumpResult[bulkDumpResultIndex + 1].key));
				bulkDumpState = decodeBulkDumpState(bulkDumpResult[bulkDumpResultIndex].value);
				if (bulkDumpState.getPhase() == BulkDumpPhase::Complete) {
					continue;
				}
				ASSERT_WE_THINK(bulkDumpState.getPhase() == BulkDumpPhase::Submitted);
				// Partition the job in the unit of shard
				allComplete = false;
				wait(store(rangeLocations, self->txnProcessor->getSourceServerInterfacesForRange(bulkDumpRange)));
				rangeLocationIndex = 0;
				for (; rangeLocationIndex < rangeLocations.size(); ++rangeLocationIndex) {
					// Spawn task per shard
					taskRange = rangeLocations[rangeLocationIndex].range;
					ASSERT(!taskRange.empty());
					if (!self->ongoingBulkDumpActors.liveActorAt(taskRange.begin)) {
						// Limit parallelism
						loop {
							if (self->bulkDumpParallelismLimitor.tryIncrementTaskCounter()) {
								break;
							}
							wait(self->bulkDumpParallelismLimitor.waitUntilCounterChanged());
						}
						// In case no ongoing task on the same range
						SSBulkDumpTask task = getSSBulkDumpTask(rangeLocations[rangeLocationIndex].servers,
						                                        bulkDumpState.getRangeTaskState(taskRange));
						// Issue task
						self->ongoingBulkDumpActors.insert(
						    taskRange,
						    doBulkDumpTask(self, task.targetServer, task.bulkDumpState, task.checksumServers));
					}
					beginKey = rangeLocations.back().range.end;
				}
			}
			beginKey = bulkDumpResult.back().key;
			wait(delay(0.1));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			wait(tr.onError(e));
		}
	}
	return allComplete && hasJob;
}

void bulkDumpUploadJobManifestFile(Reference<DataDistributor> self,
                                   BulkDumpTransportMethod transportMethod,
                                   const std::map<Key, BulkDumpManifest>& manifests,
                                   const std::string& remoteRoot,
                                   const UID& jobId) {
	if (self->folder.empty()) {
		return;
	}
	// Upload job manifest file
	std::string content = generateJobManifestFileContent(manifests);
	ASSERT(!content.empty() && !self->bulkDumpFolder.empty());
	std::string jobFolder = generateBulkDumpJobFolder(jobId);
	std::string jobManifestFileName = joinPath(jobFolder, getJobManifestFileName(jobId));
	std::string localJobManifestFilePath = joinPath(self->bulkDumpFolder, jobManifestFileName);
	std::string remoteJobManifestFilePath = joinPath(remoteRoot, jobManifestFileName);
	uploadBulkDumpJobManifestFile(transportMethod,
	                              content,
	                              self->bulkDumpFolder,
	                              jobFolder,
	                              localJobManifestFilePath,
	                              remoteJobManifestFilePath,
	                              self->ddId);
	return;
}

ACTOR Future<Void> finalizeBulkDumpJob(Reference<DataDistributor> self) {
	// Collect necessary info to generate job manifest file by scan the entire bulkDump key space
	state std::map<Key, BulkDumpManifest> manifests;
	state Optional<UID> jobId;
	state Optional<std::string> remoteRoot;
	state Optional<BulkDumpTransportMethod> transportMethod;

	TraceEvent(SevInfo, "DDBulkDumpJobFinalizeStart", self->ddId);
	state Database cx = self->txnProcessor->context();
	state RangeResult bulkDumpResult;
	state Key beginKey = normalKeys.begin;
	state Key endKey = normalKeys.end;
	state KeyRange rangeToRead;
	state int bulkDumpResultIndex = 0;
	while (beginKey < endKey) {
		state Transaction tr(cx);
		bulkDumpResultIndex = 0;
		bulkDumpResult.clear();
		try {
			rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
			wait(store(
			    bulkDumpResult,
			    krmGetRanges(&tr, bulkDumpPrefix, rangeToRead, SERVER_KNOBS->DD_BULKDUMP_TASK_METADATA_READ_SIZE)));
			for (; bulkDumpResultIndex < bulkDumpResult.size() - 1; bulkDumpResultIndex++) {
				if (bulkDumpResult[bulkDumpResultIndex].value.empty()) {
					continue;
				}
				BulkDumpState bulkDumpState = decodeBulkDumpState(bulkDumpResult[bulkDumpResultIndex].value);
				if (!jobId.present()) {
					jobId = bulkDumpState.getJobId();
				} else if (jobId.get() != bulkDumpState.getJobId()) {
					throw bulkdump_task_outdated();
				}
				if (!remoteRoot.present()) {
					remoteRoot = bulkDumpState.getRemoteRoot();
				} else if (remoteRoot.get() != bulkDumpState.getRemoteRoot()) {
					throw bulkdump_task_outdated();
				}
				if (!transportMethod.present()) {
					transportMethod = bulkDumpState.getTransportMethod();
				} else if (transportMethod.get() != bulkDumpState.getTransportMethod()) {
					throw bulkdump_task_outdated();
				}
				ASSERT_WE_THINK(bulkDumpState.getPhase() == BulkDumpPhase::Complete);
				if (bulkDumpState.getPhase() != BulkDumpPhase::Complete) {
					throw bulkdump_task_failed();
				}
				ASSERT(bulkDumpState.getManifest().present());
				ASSERT(bulkDumpState.getManifest().get().beginKey == bulkDumpResult[bulkDumpResultIndex].key &&
				       bulkDumpState.getManifest().get().endKey == bulkDumpResult[bulkDumpResultIndex + 1].key);
				auto res =
				    manifests.insert({ bulkDumpState.getManifest().get().beginKey, bulkDumpState.getManifest().get() });
				ASSERT(res.second);
			}
			beginKey = bulkDumpResult.back().key;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "DDBulkDumpJobFinalizeGetManifestFailed", self->ddId).errorUnsuppressed(e);
			wait(tr.onError(e));
		}
		wait(delay(0.1));
	}

	if (manifests.empty()) {
		throw bulkdump_task_outdated();
	}

	TraceEvent(SevInfo, "DDBulkDumpJobFinalizeGotManifest", self->ddId).detail("ManifestSize", manifests.size());

	// At this point, we have all manifest data to generate the job manifest file.
	// Generate the file at a local folder at first and then upload the file to the remote.
	// Finally all bulkdump metadata.
	// Any failure during this process will retry by DD.
	try {
		ASSERT(jobId.present() && remoteRoot.present() && transportMethod.present());
		// Generate the file at a local folder at first and then upload the file to the remote
		// The local file path:
		//	<self->bulkDumpFolder>/<jobId>/<jobId>-job-manifest.txt
		// The remote file path:
		//	<self->rootRemote>/<jobId>/<jobId>-job-manifest.txt
		bulkDumpUploadJobManifestFile(self, transportMethod.get(), manifests, remoteRoot.get(), jobId.get());
		TraceEvent(SevInfo, "DDBulkDumpJobFinalizeUploadManifest", self->ddId);

		// clear all bulkdump metadata
		wait(clearBulkDumpJob(cx));
		TraceEvent(SevInfo, "DDBulkDumpJobFinalizeMetadataClear", self->ddId);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDBulkDumpJobFinalizeError", self->ddId).errorUnsuppressed(e);
		throw bulkdump_task_failed();
	}
	return Void();
}

// The actor monitors whether the all tasks completed by the scheduleBulkDumpTasks.
// If not, it issue a new scheduleBulkDumpTasks to do the remaining tasks.
ACTOR Future<Void> bulkDumpTaskScheduler(Reference<DataDistributor> self) {
	state Database cx = self->txnProcessor->context();
	state bool allComplete = false;
	loop {
		try {
			wait(store(allComplete, scheduleBulkDumpTasks(self)) &&
			     delay(SERVER_KNOBS->DD_BULKDUMP_SCHEDULE_MIN_INTERVAL_SEC));
			if (allComplete) {
				wait(finalizeBulkDumpJob(self));
				TraceEvent(SevInfo, "DDBulkDumpTaskSchedulerComplete", self->ddId);
				break;
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "DDBulkDumpTaskSchedulerError", self->ddId).errorUnsuppressed(e);
		}
		wait(delay(5.0));
	}
	return Void();
}

ACTOR Future<Void> bulkDumpingCore(Reference<DataDistributor> self, Future<Void> readyToStart) {
	wait(readyToStart);
	state Database cx = self->txnProcessor->context();
	loop {
		try {
			wait(bulkDumpTaskScheduler(self));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "DDBulkDumpCoreError", self->ddId).errorUnsuppressed(e);
			if (e.code() == error_code_movekeys_conflict) {
				throw e;
			}
		}
		wait(delay(SERVER_KNOBS->DD_BULKDUMP_SCHEDULE_MIN_INTERVAL_SEC));
	}
}

// Runs the data distribution algorithm for FDB, including the DD Queue, DD tracker, and DD team collection
ACTOR Future<Void> dataDistribution(Reference<DataDistributor> self,
                                    PromiseStream<GetMetricsListRequest> getShardMetricsList,
                                    IsMocked isMocked) {

	if (!isMocked) {
		Database cx = openDBOnServer(self->dbInfo, TaskPriority::DataDistributionLaunch, LockAware::True);
		cx->locationCacheSize = SERVER_KNOBS->DD_LOCATION_CACHE_SIZE;
		// cx->setOption( FDBDatabaseOptions::LOCATION_CACHE_SIZE, StringRef((uint8_t*)
		// &SERVER_KNOBS->DD_LOCATION_CACHE_SIZE, 8) ); ASSERT( cx->locationCacheSize ==
		// SERVER_KNOBS->DD_LOCATION_CACHE_SIZE
		// );
		self->txnProcessor = Reference<IDDTxnProcessor>(new DDTxnProcessor(cx));
		// wait(debugCheckCoalescing(self->txnProcessor->context()));
	} else {
		ASSERT(self->txnProcessor.isValid() && self->txnProcessor->isMocked());
	}

	TraceEvent(SevInfo, "DataDistributionInitProgress", self->ddId).detail("Phase", "Start");

	// Make sure that the watcher has established a baseline before init() below so the watcher will
	// see any changes that occur after init() has read the config state.
	wait(self->initDDConfigWatch());

	TraceEvent(SevInfo, "DataDistributionInitProgress", self->ddId).detail("Phase", "DDConfigWatch Initialized");

	loop {
		self->context->trackerCancelled = false;
		// whether all initial shard are tracked
		self->initialized = Promise<Void>();

		// Stored outside of data distribution tracker to avoid slow tasks
		// when tracker is cancelled
		state KeyRangeMap<ShardTrackedData> shards;
		state Promise<UID> removeFailedServer;
		try {

			wait(DataDistributor::init(self));

			TraceEvent(SevInfo, "DataDistributionInitProgress", self->ddId).detail("Phase", "Metadata Initialized");

			// When/If this assertion fails, Evan owes Ben a pat on the back for his foresight
			ASSERT(self->configuration.storageTeamSize > 0);

			state PromiseStream<Promise<int64_t>> getAverageShardBytes;
			state PromiseStream<RebalanceStorageQueueRequest> triggerStorageQueueRebalance;
			state PromiseStream<Promise<int>> getUnhealthyRelocationCount;
			state PromiseStream<GetMetricsRequest> getShardMetrics;
			state PromiseStream<GetTopKMetricsRequest> getTopKShardMetrics;
			state Reference<AsyncVar<bool>> processingUnhealthy(new AsyncVar<bool>(false));
			state Reference<AsyncVar<bool>> processingWiggle(new AsyncVar<bool>(false));

			if (SERVER_KNOBS->DD_TENANT_AWARENESS_ENABLED || SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
				wait(self->initTenantCache());
			}

			self->shardsAffectedByTeamFailure = makeReference<ShardsAffectedByTeamFailure>();
			self->physicalShardCollection = makeReference<PhysicalShardCollection>(self->txnProcessor);
			self->bulkLoadTaskCollection =
			    makeReference<BulkLoadTaskCollection>(self->ddId, SERVER_KNOBS->DD_BULKLOAD_PARALLELISM);
			wait(self->resumeRelocations());

			TraceEvent(SevInfo, "DataDistributionInitProgress", self->ddId).detail("Phase", "Relocation Resumed");

			std::vector<TeamCollectionInterface> tcis; // primary and remote region interface
			Reference<AsyncVar<bool>> anyZeroHealthyTeams; // true if primary or remote has zero healthy team
			std::vector<Reference<AsyncVar<bool>>> zeroHealthyTeams; // primary and remote

			tcis.push_back(TeamCollectionInterface());
			zeroHealthyTeams.push_back(makeReference<AsyncVar<bool>>(true));
			int replicaSize = self->configuration.storageTeamSize;

			std::vector<Future<Void>> actors; // the container of ACTORs
			actors.push_back(self->onConfigChange);

			if (self->configuration.usableRegions > 1) {
				tcis.push_back(TeamCollectionInterface());
				replicaSize = 2 * self->configuration.storageTeamSize;

				zeroHealthyTeams.push_back(makeReference<AsyncVar<bool>>(true));
				anyZeroHealthyTeams = makeReference<AsyncVar<bool>>(true);
				actors.push_back(anyTrue(zeroHealthyTeams, anyZeroHealthyTeams));
			} else {
				anyZeroHealthyTeams = zeroHealthyTeams[0];
			}

			actors.push_back(self->pollMoveKeysLock());

			self->context->tracker = makeReference<DataDistributionTracker>(
			    DataDistributionTrackerInitParams{ .db = self->txnProcessor,
			                                       .distributorId = self->ddId,
			                                       .readyToStart = self->initialized,
			                                       .output = self->relocationProducer,
			                                       .shardsAffectedByTeamFailure = self->shardsAffectedByTeamFailure,
			                                       .physicalShardCollection = self->physicalShardCollection,
			                                       .bulkLoadTaskCollection = self->bulkLoadTaskCollection,
			                                       .anyZeroHealthyTeams = anyZeroHealthyTeams,
			                                       .shards = &shards,
			                                       .trackerCancelled = &self->context->trackerCancelled,
			                                       .ddTenantCache = self->ddTenantCache,
			                                       .usableRegions = self->configuration.usableRegions });
			actors.push_back(reportErrorsExcept(DataDistributionTracker::run(self->context->tracker,
			                                                                 self->initData,
			                                                                 getShardMetrics.getFuture(),
			                                                                 getTopKShardMetrics.getFuture(),
			                                                                 getShardMetricsList.getFuture(),
			                                                                 getAverageShardBytes.getFuture(),
			                                                                 triggerStorageQueueRebalance.getFuture(),
			                                                                 self->triggerShardBulkLoading.getFuture()),
			                                    "DDTracker",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));

			self->context->ddQueue = makeReference<DDQueue>(
			    DDQueueInitParams{ .id = self->ddId,
			                       .lock = self->lock,
			                       .db = self->txnProcessor,
			                       .teamCollections = tcis,
			                       .shardsAffectedByTeamFailure = self->shardsAffectedByTeamFailure,
			                       .physicalShardCollection = self->physicalShardCollection,
			                       .bulkLoadTaskCollection = self->bulkLoadTaskCollection,
			                       .getAverageShardBytes = getAverageShardBytes,
			                       .teamSize = replicaSize,
			                       .singleRegionTeamSize = self->configuration.storageTeamSize,
			                       .relocationProducer = self->relocationProducer,
			                       .relocationConsumer = self->relocationConsumer.getFuture(),
			                       .getShardMetrics = getShardMetrics,
			                       .getTopKMetrics = getTopKShardMetrics });
			actors.push_back(reportErrorsExcept(DDQueue::run(self->context->ddQueue,
			                                                 processingUnhealthy,
			                                                 processingWiggle,
			                                                 getUnhealthyRelocationCount.getFuture(),
			                                                 self->context->ddEnabledState.get()),
			                                    "DDQueue",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));

			if (self->ddTenantCache.present()) {
				actors.push_back(reportErrorsExcept(self->ddTenantCache.get()->monitorTenantMap(),
				                                    "DDTenantCacheMonitor",
				                                    self->ddId,
				                                    &normalDDQueueErrors()));
			}
			if (self->ddTenantCache.present() && SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
				actors.push_back(reportErrorsExcept(self->ddTenantCache.get()->monitorStorageQuota(),
				                                    "StorageQuotaTracker",
				                                    self->ddId,
				                                    &normalDDQueueErrors()));
				actors.push_back(reportErrorsExcept(self->ddTenantCache.get()->monitorStorageUsage(),
				                                    "StorageUsageTracker",
				                                    self->ddId,
				                                    &normalDDQueueErrors()));
			}

			std::vector<DDTeamCollection*> teamCollectionsPtrs;
			self->context->primaryTeamCollection = makeReference<DDTeamCollection>(DDTeamCollectionInitParams{
			    self->txnProcessor,
			    self->ddId,
			    self->lock,
			    self->relocationProducer,
			    self->shardsAffectedByTeamFailure,
			    self->configuration,
			    self->primaryDcId,
			    self->configuration.usableRegions > 1 ? self->remoteDcIds : std::vector<Optional<Key>>(),
			    self->initialized.getFuture(),
			    zeroHealthyTeams[0],
			    IsPrimary::True,
			    processingUnhealthy,
			    processingWiggle,
			    getShardMetrics,
			    removeFailedServer,
			    getUnhealthyRelocationCount,
			    getAverageShardBytes,
			    triggerStorageQueueRebalance });
			teamCollectionsPtrs.push_back(self->context->primaryTeamCollection.getPtr());
			Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage;
			if (!isMocked) {
				recruitStorage = IAsyncListener<RequestStream<RecruitStorageRequest>>::create(
				    self->dbInfo, [](auto const& info) { return info.clusterInterface.recruitStorage; });
			}
			if (self->configuration.usableRegions > 1) {
				self->context->remoteTeamCollection = makeReference<DDTeamCollection>(
				    DDTeamCollectionInitParams{ self->txnProcessor,
				                                self->ddId,
				                                self->lock,
				                                self->relocationProducer,
				                                self->shardsAffectedByTeamFailure,
				                                self->configuration,
				                                self->remoteDcIds,
				                                Optional<std::vector<Optional<Key>>>(),
				                                self->initialized.getFuture() && remoteRecovered(self->dbInfo),
				                                zeroHealthyTeams[1],
				                                IsPrimary::False,
				                                processingUnhealthy,
				                                processingWiggle,
				                                getShardMetrics,
				                                removeFailedServer,
				                                getUnhealthyRelocationCount,
				                                getAverageShardBytes,
				                                triggerStorageQueueRebalance });
				teamCollectionsPtrs.push_back(self->context->remoteTeamCollection.getPtr());
				self->context->remoteTeamCollection->teamCollections = teamCollectionsPtrs;
				actors.push_back(reportErrorsExcept(DDTeamCollection::run(self->context->remoteTeamCollection,
				                                                          self->initData,
				                                                          tcis[1],
				                                                          recruitStorage,
				                                                          *self->context->ddEnabledState.get()),
				                                    "DDTeamCollectionSecondary",
				                                    self->ddId,
				                                    &normalDDQueueErrors()));
				actors.push_back(DDTeamCollection::printSnapshotTeamsInfo(self->context->remoteTeamCollection));
			}
			self->context->primaryTeamCollection->teamCollections = teamCollectionsPtrs;
			self->teamCollection = self->context->primaryTeamCollection.getPtr();
			actors.push_back(reportErrorsExcept(DDTeamCollection::run(self->context->primaryTeamCollection,
			                                                          self->initData,
			                                                          tcis[0],
			                                                          recruitStorage,
			                                                          *self->context->ddEnabledState.get()),
			                                    "DDTeamCollectionPrimary",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));

			actors.push_back(DDTeamCollection::printSnapshotTeamsInfo(self->context->primaryTeamCollection));
			actors.push_back(yieldPromiseStream(self->relocationProducer.getFuture(), self->relocationConsumer));
			if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
				actors.push_back(monitorPhysicalShardStatus(self->physicalShardCollection));
			}

			actors.push_back(serveBlobMigratorRequests(self, self->context->tracker, self->context->ddQueue));
			if (bulkLoadIsEnabled(self->initData->bulkLoadMode)) {
				TraceEvent(SevInfo, "DDBulkLoadModeEnabled", self->ddId)
				    .detail("UsableRegions", self->configuration.usableRegions);
				self->bulkLoadEnabled = true;
				if (self->configuration.usableRegions > 1) {
					actors.push_back(
					    bulkLoadingCore(self, self->initialized.getFuture() && remoteRecovered(self->dbInfo)));
				} else {
					actors.push_back(bulkLoadingCore(self, self->initialized.getFuture()));
				}
			}

			if (bulkDumpIsEnabled(self->initData->bulkDumpMode)) {
				TraceEvent(SevInfo, "DDBulkDumpModeEnabled", self->ddId)
				    .detail("UsableRegions", self->configuration.usableRegions);
				self->bulkDumpEnabled = true;
				if (self->configuration.usableRegions > 1) {
					actors.push_back(
					    bulkDumpingCore(self, self->initialized.getFuture() && remoteRecovered(self->dbInfo)));
				} else {
					actors.push_back(bulkDumpingCore(self, self->initialized.getFuture()));
				}
			}

			wait(waitForAll(actors));
			ASSERT_WE_THINK(false);
			return Void();
		} catch (Error& e) {
			self->context->tracker.clear();
			self->context->ddQueue.clear();
			self->context->markTrackerCancelled();
			state Error err = e;
			TraceEvent("DataDistributorDestroyTeamCollections", self->ddId).error(e);
			state std::vector<UID> teamForDroppedRange;
			if (removeFailedServer.getFuture().isReady() && !removeFailedServer.getFuture().isError()) {
				// Choose a random healthy team to host the to-be-dropped range.
				const UID serverID = removeFailedServer.getFuture().get();
				std::vector<UID> pTeam = self->context->primaryTeamCollection->getRandomHealthyTeam(serverID);
				teamForDroppedRange.insert(teamForDroppedRange.end(), pTeam.begin(), pTeam.end());
				if (self->configuration.usableRegions > 1) {
					std::vector<UID> rTeam = self->context->remoteTeamCollection->getRandomHealthyTeam(serverID);
					teamForDroppedRange.insert(teamForDroppedRange.end(), rTeam.begin(), rTeam.end());
				}
			}
			self->teamCollection = nullptr;
			self->context->primaryTeamCollection = Reference<DDTeamCollection>();
			self->context->remoteTeamCollection = Reference<DDTeamCollection>();
			if (err.code() == error_code_actor_cancelled) {
				// When cancelled, we cannot clear asynchronously because
				// this will result in invalid memory access. This should only
				// be an issue in simulation.
				if (!g_network->isSimulated()) {
					TraceEvent(SevWarn, "DataDistributorCancelled");
				}
				shards.clear();
				throw e;
			} else {
				wait(shards.clearAsync());
			}
			TraceEvent("DataDistributorTeamCollectionsDestroyed", self->ddId).error(err);
			if (removeFailedServer.getFuture().isReady() && !removeFailedServer.getFuture().isError()) {
				TraceEvent("RemoveFailedServer", removeFailedServer.getFuture().get()).error(err);
				wait(self->removeKeysFromFailedServer(removeFailedServer.getFuture().get(), teamForDroppedRange));
				wait(self->removeStorageServer(removeFailedServer.getFuture().get()));
			} else {
				if (err.code() != error_code_movekeys_conflict && err.code() != error_code_dd_config_changed) {
					throw err;
				}

				bool ddEnabled = wait(self->isDataDistributionEnabled());
				TraceEvent("DataDistributionError", self->ddId).error(err).detail("DataDistributionEnabled", ddEnabled);
				if (ddEnabled) {
					throw err;
				}
			}
		}
	}
}

static std::set<int> const& normalDataDistributorErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_worker_removed);
		s.insert(error_code_broken_promise);
		s.insert(error_code_actor_cancelled);
		s.insert(error_code_please_reboot);
		s.insert(error_code_movekeys_conflict);
		s.insert(error_code_data_move_cancelled);
		s.insert(error_code_data_move_dest_team_not_found);
		s.insert(error_code_dd_config_changed);
		s.insert(error_code_audit_storage_failed);
	}
	return s;
}

ACTOR template <class Req>
Future<Void> sendSnapReq(RequestStream<Req> stream, Req req, Error e) {
	ErrorOr<REPLY_TYPE(Req)> reply = wait(stream.tryGetReply(req));
	if (reply.isError()) {
		TraceEvent("SnapDataDistributor_ReqError")
		    .errorUnsuppressed(reply.getError())
		    .detail("ConvertedErrorType", e.what())
		    .detail("Peer", stream.getEndpoint().getPrimaryAddress())
		    .detail("PeerAddress", stream.getEndpoint().getPrimaryAddress());
		throw e;
	}
	return Void();
}

ACTOR Future<ErrorOr<Void>> trySendSnapReq(RequestStream<WorkerSnapRequest> stream, WorkerSnapRequest req) {
	state int snapReqRetry = 0;
	state double snapRetryBackoff = FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY;
	loop {
		ErrorOr<REPLY_TYPE(WorkerSnapRequest)> reply = wait(stream.tryGetReply(req));
		if (reply.isError()) {
			TraceEvent("SnapDataDistributor_ReqError")
			    .errorUnsuppressed(reply.getError())
			    .detail("Peer", stream.getEndpoint().getPrimaryAddress())
			    .detail("PeerAddress", stream.getEndpoint().getPrimaryAddress())
			    .detail("Retry", snapReqRetry);
			if (reply.getError().code() != error_code_request_maybe_delivered ||
			    ++snapReqRetry > SERVER_KNOBS->SNAP_NETWORK_FAILURE_RETRY_LIMIT)
				return ErrorOr<Void>(reply.getError());
			else {
				// retry for network failures with same snap UID to avoid snapshot twice
				req = WorkerSnapRequest(req.snapPayload, req.snapUID, req.role);
				wait(delay(snapRetryBackoff));
				snapRetryBackoff = snapRetryBackoff * 2;
			}
		} else
			break;
	}
	return ErrorOr<Void>(Void());
}

ACTOR Future<std::map<NetworkAddress, std::pair<WorkerInterface, std::string>>> getStatefulWorkers(
    Database cx,
    Reference<AsyncVar<ServerDBInfo> const> dbInfo,
    std::vector<TLogInterface>* tlogs,
    int* storageFaultTolerance) {
	state std::map<NetworkAddress, std::pair<WorkerInterface, std::string>> result;
	state std::map<NetworkAddress, WorkerInterface> workersMap;
	state Transaction tr(cx);
	state DatabaseConfiguration configuration;
	loop {
		try {
			// necessary options
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			// get database configuration
			DatabaseConfiguration _configuration = wait(getDatabaseConfiguration(&tr));
			configuration = _configuration;

			// get storages
			RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
			state std::vector<StorageServerInterface> storageServers;
			storageServers.reserve(serverList.size());
			for (int i = 0; i < serverList.size(); i++)
				storageServers.push_back(decodeServerListValue(serverList[i].value));

			// get workers
			state std::vector<WorkerDetails> workers = wait(getWorkers(dbInfo));
			for (const auto& worker : workers) {
				workersMap[worker.interf.address()] = worker.interf;
			}

			Optional<Value> regionsValue = wait(tr.get("usable_regions"_sr.withPrefix(configKeysPrefix)));
			int usableRegions = 1;
			if (regionsValue.present()) {
				usableRegions = atoi(regionsValue.get().toString().c_str());
			}
			auto masterDcId = dbInfo->get().master.locality.dcId();
			int storageFailures = 0;
			for (const auto& server : storageServers) {
				TraceEvent(SevDebug, "StorageServerDcIdInfo")
				    .detail("Address", server.address().toString())
				    .detail("ServerLocalityID", server.locality.dcId())
				    .detail("MasterDcID", masterDcId);
				if (usableRegions == 1 || server.locality.dcId() == masterDcId) {
					auto itr = workersMap.find(server.address());
					if (itr == workersMap.end()) {
						TraceEvent(SevWarn, "GetStorageWorkers")
						    .detail("Reason", "Could not find worker for storage server")
						    .detail("SS", server.id());
						++storageFailures;
					} else {
						if (result.contains(server.address())) {
							ASSERT(itr->second.id() == result[server.address()].first.id());
							if (result[server.address()].second.find("storage") == std::string::npos)
								result[server.address()].second.append(",storage");
						} else {
							result[server.address()] = std::make_pair(itr->second, "storage");
						}
					}
				}
			}
			// calculate fault tolerance
			*storageFaultTolerance = std::min(static_cast<int>(SERVER_KNOBS->MAX_STORAGE_SNAPSHOT_FAULT_TOLERANCE),
			                                  configuration.storageTeamSize - 1) -
			                         storageFailures;
			if (*storageFaultTolerance < 0) {
				CODE_PROBE(true, "Too many failed storage servers to complete snapshot", probe::decoration::rare);
				throw snap_storage_failed();
			}
			// tlogs
			for (const auto& tlog : *tlogs) {
				TraceEvent(SevDebug, "GetStatefulWorkersTlog").detail("Addr", tlog.address());
				if (workersMap.find(tlog.address()) == workersMap.end()) {
					TraceEvent(SevWarn, "MissingTlogWorkerInterface").detail("TlogAddress", tlog.address());
					throw snap_tlog_failed();
				}
				if (result.contains(tlog.address())) {
					ASSERT(workersMap[tlog.address()].id() == result[tlog.address()].first.id());
					result[tlog.address()].second.append(",tlog");
				} else {
					result[tlog.address()] = std::make_pair(workersMap[tlog.address()], "tlog");
				}
			}

			// get coordinators
			Optional<Value> coordinators = wait(tr.get(coordinatorsKey));
			if (!coordinators.present()) {
				CODE_PROBE(true, "Failed to read the coordinatorsKey", probe::decoration::rare);
				throw operation_failed();
			}
			ClusterConnectionString ccs(coordinators.get().toString());
			std::vector<NetworkAddress> coordinatorsAddr = wait(ccs.tryResolveHostnames());
			std::set<NetworkAddress> coordinatorsAddrSet(coordinatorsAddr.begin(), coordinatorsAddr.end());
			for (const auto& worker : workers) {
				// Note : only considers second address for coordinators,
				// as we use primary addresses from storage and tlog interfaces above
				NetworkAddress primary = worker.interf.address();
				Optional<NetworkAddress> secondary = worker.interf.tLog.getEndpoint().addresses.secondaryAddress;
				if (coordinatorsAddrSet.find(primary) != coordinatorsAddrSet.end() ||
				    (secondary.present() && (coordinatorsAddrSet.find(secondary.get()) != coordinatorsAddrSet.end()))) {
					if (result.contains(primary)) {
						ASSERT(workersMap[primary].id() == result[primary].first.id());
						result[primary].second.append(",coord");
					} else {
						result[primary] = std::make_pair(workersMap[primary], "coord");
					}
				}
			}
			if (SERVER_KNOBS->SNAPSHOT_ALL_STATEFUL_PROCESSES) {
				for (const auto& worker : workers) {
					const auto& processAddress = worker.interf.address();
					// skip processes that are already included
					if (result.contains(processAddress))
						continue;
					const auto& processClassType = worker.processClass.classType();
					// coordinators are always configured to be recruited
					if (processClassType == ProcessClass::StorageClass) {
						result[processAddress] = std::make_pair(worker.interf, "storage");
						TraceEvent(SevInfo, "SnapUnRecruitedStorageProcess").detail("ProcessAddress", processAddress);
					} else if (processClassType == ProcessClass::TransactionClass ||
					           processClassType == ProcessClass::LogClass) {
						result[processAddress] = std::make_pair(worker.interf, "tlog");
						TraceEvent(SevInfo, "SnapUnRecruitedLogProcess").detail("ProcessAddress", processAddress);
					}
				}
			}
			return result;
		} catch (Error& e) {
			wait(tr.onError(e));
			result.clear();
		}
	}
}

ACTOR Future<Void> ddSnapCreateCore(DistributorSnapRequest snapReq, Reference<AsyncVar<ServerDBInfo> const> db) {
	state Database cx = openDBOnServer(db, TaskPriority::DefaultDelay, LockAware::True);

	state ReadYourWritesTransaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			TraceEvent("SnapDataDistributor_WriteFlagAttempt")
			    .detail("SnapPayload", snapReq.snapPayload)
			    .detail("SnapUID", snapReq.snapUID);
			tr.set(writeRecoveryKey, writeRecoveryKeyTrue);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent("SnapDataDistributor_WriteFlagError").error(e);
			wait(tr.onError(e));
		}
	}
	TraceEvent("SnapDataDistributor_SnapReqEnter")
	    .detail("SnapPayload", snapReq.snapPayload)
	    .detail("SnapUID", snapReq.snapUID);
	try {
		// disable tlog pop on local tlog nodes
		state std::vector<TLogInterface> tlogs = db->get().logSystemConfig.allLocalLogs(false);
		std::vector<Future<Void>> disablePops;
		disablePops.reserve(tlogs.size());
		for (const auto& tlog : tlogs) {
			disablePops.push_back(sendSnapReq(
			    tlog.disablePopRequest, TLogDisablePopRequest{ snapReq.snapUID }, snap_disable_tlog_pop_failed()));
		}
		wait(waitForAll(disablePops));

		TraceEvent("SnapDataDistributor_AfterDisableTLogPop")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);

		state int storageFaultTolerance;
		// snap stateful nodes
		state std::map<NetworkAddress, std::pair<WorkerInterface, std::string>> statefulWorkers =
		    wait(transformErrors(getStatefulWorkers(cx, db, &tlogs, &storageFaultTolerance), snap_storage_failed()));

		TraceEvent("SnapDataDistributor_GotStatefulWorkers")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID)
		    .detail("StorageFaultTolerance", storageFaultTolerance);

		// we need to snapshot storage nodes before snapshot any tlogs
		std::vector<Future<ErrorOr<Void>>> storageSnapReqs;
		for (const auto& [addr, entry] : statefulWorkers) {
			auto& [interf, role] = entry;
			if (role.find("storage") != std::string::npos)
				storageSnapReqs.push_back(trySendSnapReq(
				    interf.workerSnapReq, WorkerSnapRequest(snapReq.snapPayload, snapReq.snapUID, "storage"_sr)));
		}
		wait(waitForMost(storageSnapReqs, storageFaultTolerance, snap_storage_failed()));
		TraceEvent("SnapDataDistributor_AfterSnapStorage")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);

		std::vector<Future<ErrorOr<Void>>> tLogSnapReqs;
		tLogSnapReqs.reserve(tlogs.size());
		for (const auto& [addr, entry] : statefulWorkers) {
			auto& [interf, role] = entry;
			if (role.find("tlog") != std::string::npos)
				tLogSnapReqs.push_back(trySendSnapReq(
				    interf.workerSnapReq, WorkerSnapRequest(snapReq.snapPayload, snapReq.snapUID, "tlog"_sr)));
		}
		wait(waitForMost(tLogSnapReqs, 0, snap_tlog_failed()));

		TraceEvent("SnapDataDistributor_AfterTLogStorage")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);

		// enable tlog pop on local tlog nodes
		std::vector<Future<Void>> enablePops;
		enablePops.reserve(tlogs.size());
		for (const auto& tlog : tlogs) {
			enablePops.push_back(sendSnapReq(
			    tlog.enablePopRequest, TLogEnablePopRequest{ snapReq.snapUID }, snap_enable_tlog_pop_failed()));
		}
		wait(waitForAll(enablePops));

		TraceEvent("SnapDataDistributor_AfterEnableTLogPops")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);

		std::vector<Future<ErrorOr<Void>>> coordSnapReqs;
		for (const auto& [addr, entry] : statefulWorkers) {
			auto& [interf, role] = entry;
			if (role.find("coord") != std::string::npos)
				coordSnapReqs.push_back(trySendSnapReq(
				    interf.workerSnapReq, WorkerSnapRequest(snapReq.snapPayload, snapReq.snapUID, "coord"_sr)));
		}
		// At present, the fault injection workload doesn't respect the KNOB
		// MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE Consequently, we ignore it in simulation tests
		auto const coordFaultTolerance = std::min<int>(
		    std::max<int>(0, (coordSnapReqs.size() - 1) / 2),
		    g_network->isSimulated() ? coordSnapReqs.size() : SERVER_KNOBS->MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE);
		wait(waitForMost(coordSnapReqs, coordFaultTolerance, snap_coord_failed()));

		TraceEvent("SnapDataDistributor_AfterSnapCoords")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		tr.reset();
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				TraceEvent("SnapDataDistributor_ClearFlagAttempt")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				tr.clear(writeRecoveryKey);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				TraceEvent("SnapDataDistributor_ClearFlagError").error(e);
				wait(tr.onError(e));
			}
		}
	} catch (Error& err) {
		state Error e = err;
		TraceEvent("SnapDataDistributor_SnapReqExit")
		    .errorUnsuppressed(e)
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		if (e.code() == error_code_snap_storage_failed || e.code() == error_code_snap_tlog_failed ||
		    e.code() == error_code_operation_cancelled || e.code() == error_code_snap_disable_tlog_pop_failed) {
			// enable tlog pop on local tlog nodes
			std::vector<TLogInterface> tlogs = db->get().logSystemConfig.allLocalLogs(false);
			try {
				std::vector<Future<Void>> enablePops;
				enablePops.reserve(tlogs.size());
				for (const auto& tlog : tlogs) {
					enablePops.push_back(transformErrors(
					    throwErrorOr(tlog.enablePopRequest.tryGetReply(TLogEnablePopRequest(snapReq.snapUID))),
					    snap_enable_tlog_pop_failed()));
				}
				wait(waitForAll(enablePops));
			} catch (Error& error) {
				TraceEvent(SevDebug, "IgnoreEnableTLogPopFailure").log();
			}
		}
		throw e;
	}
	return Void();
}

ACTOR Future<Void> ddSnapCreate(
    DistributorSnapRequest snapReq,
    Reference<AsyncVar<ServerDBInfo> const> db,
    DDEnabledState* ddEnabledState,
    std::map<UID, DistributorSnapRequest>* ddSnapMap /* ongoing snapshot requests */,
    std::map<UID, ErrorOr<Void>>*
        ddSnapResultMap /* finished snapshot requests, expired in SNAP_MINIMUM_TIME_GAP seconds */) {
	state Future<Void> dbInfoChange = db->onChange();
	if (!ddEnabledState->trySetSnapshot(snapReq.snapUID)) {
		// disable DD before doing snapCreate, if previous snap req has already disabled DD then this operation
		// fails here
		TraceEvent("SnapDDSetDDEnabledFailedInMemoryCheck").detail("SnapUID", snapReq.snapUID);
		ddSnapMap->at(snapReq.snapUID).reply.sendError(operation_failed());
		ddSnapMap->erase(snapReq.snapUID);
		(*ddSnapResultMap)[snapReq.snapUID] = ErrorOr<Void>(operation_failed());
		return Void();
	}
	try {
		choose {
			when(wait(dbInfoChange)) {
				TraceEvent("SnapDDCreateDBInfoChanged")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				ddSnapMap->at(snapReq.snapUID).reply.sendError(snap_with_recovery_unsupported());
				ddSnapMap->erase(snapReq.snapUID);
				(*ddSnapResultMap)[snapReq.snapUID] = ErrorOr<Void>(snap_with_recovery_unsupported());
			}
			when(wait(ddSnapCreateCore(snapReq, db))) {
				TraceEvent("SnapDDCreateSuccess")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				ddSnapMap->at(snapReq.snapUID).reply.send(Void());
				ddSnapMap->erase(snapReq.snapUID);
				(*ddSnapResultMap)[snapReq.snapUID] = ErrorOr<Void>(Void());
			}
			when(wait(delay(SERVER_KNOBS->SNAP_CREATE_MAX_TIMEOUT))) {
				TraceEvent("SnapDDCreateTimedOut")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				ddSnapMap->at(snapReq.snapUID).reply.sendError(timed_out());
				ddSnapMap->erase(snapReq.snapUID);
				(*ddSnapResultMap)[snapReq.snapUID] = ErrorOr<Void>(timed_out());
			}
		}
	} catch (Error& e) {
		TraceEvent("SnapDDCreateError")
		    .errorUnsuppressed(e)
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		if (e.code() != error_code_operation_cancelled) {
			ddSnapMap->at(snapReq.snapUID).reply.sendError(e);
			ddSnapMap->erase(snapReq.snapUID);
			(*ddSnapResultMap)[snapReq.snapUID] = ErrorOr<Void>(e);
		} else {
			// enable DD should always succeed
			bool success = ddEnabledState->trySetEnabled(snapReq.snapUID);
			ASSERT(success);
			throw e;
		}
	}
	// enable DD should always succeed
	bool success = ddEnabledState->trySetEnabled(snapReq.snapUID);
	ASSERT(success);
	return Void();
}

ACTOR Future<Void> ddExclusionSafetyCheck(DistributorExclusionSafetyCheckRequest req,
                                          Reference<DataDistributor> self,
                                          Database cx) {
	TraceEvent("DDExclusionSafetyCheckBegin", self->ddId).log();
	std::vector<StorageServerInterface> ssis = wait(getStorageServers(cx));
	DistributorExclusionSafetyCheckReply reply(true);
	if (!self->teamCollection) {
		TraceEvent("DDExclusionSafetyCheckTeamCollectionInvalid", self->ddId).log();
		reply.safe = false;
		req.reply.send(reply);
		return Void();
	}
	// If there is only 1 team, unsafe to mark failed: team building can get stuck due to lack of servers left
	if (self->teamCollection->teams.size() <= 1) {
		TraceEvent("DDExclusionSafetyCheckNotEnoughTeams", self->ddId).log();
		reply.safe = false;
		req.reply.send(reply);
		return Void();
	}
	std::vector<UID> excludeServerIDs;
	// Go through storage server interfaces and translate Address -> server ID (UID)
	for (const AddressExclusion& excl : req.exclusions) {
		for (const auto& ssi : ssis) {
			if (excl.excludes(ssi.address()) ||
			    (ssi.secondaryAddress().present() && excl.excludes(ssi.secondaryAddress().get()))) {
				excludeServerIDs.push_back(ssi.id());
			}
		}
	}
	reply.safe = self->teamCollection->exclusionSafetyCheck(excludeServerIDs);
	TraceEvent("DDExclusionSafetyCheckFinish", self->ddId).log();
	req.reply.send(reply);
	return Void();
}

ACTOR Future<Void> waitFailCacheServer(Database* db, StorageServerInterface ssi) {
	state Transaction tr(*db);
	state Key key = storageCacheServerKey(ssi.id());
	wait(waitFailureClient(ssi.waitFailure));
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			tr.addReadConflictRange(storageCacheServerKeys);
			tr.clear(key);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> cacheServerWatcher(Database* db) {
	state Transaction tr(*db);
	state ActorCollection actors(false);
	state std::set<UID> knownCaches;
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			RangeResult range = wait(tr.getRange(storageCacheServerKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!range.more);
			std::set<UID> caches;
			for (auto& kv : range) {
				UID id;
				BinaryReader reader{ kv.key.removePrefix(storageCacheServersPrefix), Unversioned() };
				reader >> id;
				caches.insert(id);
				if (knownCaches.find(id) == knownCaches.end()) {
					StorageServerInterface ssi;
					BinaryReader reader{ kv.value, IncludeVersion() };
					reader >> ssi;
					actors.add(waitFailCacheServer(db, ssi));
				}
			}
			knownCaches = std::move(caches);
			tr.reset();
			wait(delay(5.0) || actors.getResult());
			ASSERT(!actors.getResult().isReady());
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

static int64_t getMedianShardSize(VectorRef<DDMetricsRef> metricVec) {
	std::nth_element(metricVec.begin(),
	                 metricVec.begin() + metricVec.size() / 2,
	                 metricVec.end(),
	                 [](const DDMetricsRef& d1, const DDMetricsRef& d2) { return d1.shardBytes < d2.shardBytes; });
	return metricVec[metricVec.size() / 2].shardBytes;
}

GetStorageWigglerStateReply getStorageWigglerStates(Reference<DataDistributor> self) {
	GetStorageWigglerStateReply reply;
	if (self->teamCollection) {
		std::tie(reply.primary, reply.lastStateChangePrimary) = self->teamCollection->getStorageWigglerState();
		if (self->teamCollection->teamCollections.size() > 1) {
			std::tie(reply.remote, reply.lastStateChangeRemote) =
			    self->teamCollection->teamCollections[1]->getStorageWigglerState();
		}
	}
	return reply;
}

TenantsOverStorageQuotaReply getTenantsOverStorageQuota(Reference<DataDistributor> self) {
	TenantsOverStorageQuotaReply reply;
	if (self->ddTenantCache.present() && SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
		reply.tenants = self->ddTenantCache.get()->getTenantsOverQuota();
	}
	return reply;
}

ACTOR Future<Void> ddGetMetrics(GetDataDistributorMetricsRequest req,
                                PromiseStream<GetMetricsListRequest> getShardMetricsList) {
	ErrorOr<Standalone<VectorRef<DDMetricsRef>>> result = wait(
	    errorOr(brokenPromiseToNever(getShardMetricsList.getReply(GetMetricsListRequest(req.keys, req.shardLimit)))));

	if (result.isError()) {
		req.reply.sendError(result.getError());
	} else {
		GetDataDistributorMetricsReply rep;
		if (!req.midOnly) {
			rep.storageMetricsList = result.get();
		} else {
			auto& metricVec = result.get();
			if (metricVec.empty())
				rep.midShardSize = 0;
			else {
				rep.midShardSize = getMedianShardSize(metricVec.contents());
			}
		}
		req.reply.send(rep);
	}

	return Void();
}

ACTOR Future<bool> checkAuditProgressCompleteForSSShard(Database cx, std::shared_ptr<DDAudit> audit) {
	ASSERT(audit->coreState.getType() == AuditType::ValidateStorageServerShard);
	state ActorCollection actors(true);
	state std::unordered_map<UID, bool> res;
	state std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
	state std::shared_ptr<AsyncVar<int>> remainingBudget =
	    std::make_shared<AsyncVar<int>>(SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
	state UID serverId;
	state bool allFinish = true;
	state int i = 0;
	TraceEvent(SevDebug, "CheckAuditProgressCompleteForSSShardStart")
	    .detail("TotalSS", interfs.size())
	    .detail("InitBudget", remainingBudget->get());
	for (; i < interfs.size(); i++) {
		serverId = interfs[i].uniqueID;
		if (audit->serversFinishedSSShardAudit.contains(serverId)) {
			TraceEvent(SevDebug, "CheckAuditProgressCompleteForSSShardSkipCheck").detail("ServerId", serverId);
			continue; // Skip if already complete
		}
		if (interfs[i].isTss()) {
			continue; // SSShard audit does not test TSS
		}
		ASSERT(remainingBudget->get() >= 0);
		while (remainingBudget->get() == 0) {
			wait(remainingBudget->onChange());
			ASSERT(remainingBudget->get() >= 0);
		}
		remainingBudget->set(remainingBudget->get() - 1);
		TraceEvent(SevDebug, "CheckAuditProgressCompleteForSSShardBudget")
		    .detail("RemainingBudget", remainingBudget->get())
		    .detail("ServerId", serverId);
		ASSERT(remainingBudget->get() >= 0);
		if (actors.getResult().isReady()) {
			actors.clear(true);
		}
		actors.add(store(res[serverId],
		                 checkAuditProgressCompleteByServer(
		                     cx, audit->coreState.getType(), audit->coreState.id, allKeys, serverId, remainingBudget)));
	}
	wait(actors.getResult());
	for (const auto& [serverId, finish] : res) {
		TraceEvent(SevDebug, "CheckAuditProgressCompleteForSSShardRes")
		    .detail("AuditState", audit->coreState.toString())
		    .detail("ServerId", serverId)
		    .detail("Finish", finish);
		if (finish) {
			audit->serversFinishedSSShardAudit.insert(serverId);
		} else {
			allFinish = false;
		}
	}
	return allFinish;
}

// Maintain an alive state of an audit until the audit completes
// Automatically retry until if errors of the auditing process happen
// Return if (1) audit completes; (2) retry times exceed the maximum retry times
// Throw error if this actor gets cancelled
ACTOR Future<Void> auditStorageCore(Reference<DataDistributor> self,
                                    UID auditID,
                                    AuditType auditType,
                                    int currentRetryCount) {
	ASSERT(auditID.isValid());
	state std::shared_ptr<DDAudit> audit = getAuditFromAuditMap(self, auditType, auditID);

	state MoveKeyLockInfo lockInfo;
	lockInfo.myOwner = self->lock.myOwner;
	lockInfo.prevOwner = self->lock.prevOwner;
	lockInfo.prevWrite = self->lock.prevWrite;

	try {
		ASSERT(audit != nullptr);
		ASSERT(audit->coreState.ddId == self->ddId);
		loadAndDispatchAudit(self, audit);
		TraceEvent(SevInfo, "DDAuditStorageCoreScheduled", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditID", audit->coreState.id)
		    .detail("Range", audit->coreState.range)
		    .detail("AuditType", audit->coreState.getType())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
		wait(audit->actors.getResult()); // goto exception handler if any actor is failed
		TraceEvent(SevInfo, "DDAuditStorageCoreAllActorsComplete", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("Range", audit->coreState.range)
		    .detail("AuditType", audit->coreState.getType())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount)
		    .detail("DDDoAuditTasksIssued", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTasksComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTasksSkipped", audit->overallSkippedDoAuditCount);

		if (audit->foundError) {
			audit->coreState.setPhase(AuditPhase::Error);
		} else if (audit->auditStorageAnyChildFailed) {
			audit->auditStorageAnyChildFailed = false;
			TraceEvent(SevInfo, "DDAuditStorageCoreRetry", self->ddId)
			    .detail("Reason", "AuditStorageAnyChildFailed")
			    .detail("AuditID", auditID)
			    .detail("RetryCount", audit->retryCount)
			    .detail("AuditType", auditType);
			throw retry();
		} else {
			// Check audit persist progress to double check if any range omitted to be check
			if (audit->coreState.getType() == AuditType::ValidateHA ||
			    audit->coreState.getType() == AuditType::ValidateReplica) {
				bool allFinish = wait(checkAuditProgressCompleteByRange(self->txnProcessor->context(),
				                                                        audit->coreState.getType(),
				                                                        audit->coreState.id,
				                                                        audit->coreState.range));
				if (!allFinish) {
					TraceEvent(SevInfo, "DDAuditStorageCoreRetry", self->ddId)
					    .detail("Reason", "AuditReplicaNotFinish")
					    .detail("AuditID", auditID)
					    .detail("RetryCount", audit->retryCount)
					    .detail("AuditType", auditType);
					throw retry();
				}
			} else if (audit->coreState.getType() == AuditType::ValidateLocationMetadata) {
				bool allFinish = wait(checkAuditProgressCompleteByRange(
				    self->txnProcessor->context(), audit->coreState.getType(), audit->coreState.id, allKeys));
				if (!allFinish) {
					TraceEvent(SevInfo, "DDAuditStorageCoreRetry", self->ddId)
					    .detail("Reason", "AuditLocationMetadataNotFinish")
					    .detail("AuditID", auditID)
					    .detail("RetryCount", audit->retryCount)
					    .detail("AuditType", auditType);
					throw retry();
				}
			} else {
				bool allFinish = wait(checkAuditProgressCompleteForSSShard(self->txnProcessor->context(), audit));
				if (!allFinish) {
					TraceEvent(SevInfo, "DDAuditStorageCoreRetry", self->ddId)
					    .detail("Reason", "AuditSSShardNotFinish")
					    .detail("AuditID", auditID)
					    .detail("RetryCount", audit->retryCount)
					    .detail("AuditType", auditType);
					throw retry();
				}
			}
			audit->coreState.setPhase(AuditPhase::Complete);
		}
		TraceEvent(SevVerbose, "DDAuditStorageCoreCompleteAudit", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditState", audit->coreState.toString())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
		wait(persistAuditState(self->txnProcessor->context(),
		                       audit->coreState,
		                       "AuditStorageCore",
		                       lockInfo,
		                       self->context->isDDEnabled()));
		TraceEvent(SevVerbose, "DDAuditStorageCoreSetResult", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditState", audit->coreState.toString())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
		removeAuditFromAuditMap(self, audit->coreState.getType(),
		                        audit->coreState.id); // remove audit

		TraceEvent(SevInfo, "DDAuditStorageCoreEnd", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditID", auditID)
		    .detail("AuditType", auditType)
		    .detail("Range", audit->coreState.range)
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			// If this audit is cancelled, the place where cancelling
			// this audit does removeAuditFromAuditMap
			throw e;
		}
		TraceEvent(SevInfo, "DDAuditStorageCoreError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditID", auditID)
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount)
		    .detail("AuditType", auditType)
		    .detail("Range", audit->coreState.range);
		if (e.code() == error_code_movekeys_conflict) {
			removeAuditFromAuditMap(self, audit->coreState.getType(),
			                        audit->coreState.id); // remove audit
			// Silently exit
		} else if (e.code() == error_code_audit_storage_task_outdated) {
			// Silently exit
		} else if (e.code() == error_code_audit_storage_cancelled) {
			// If this audit is cancelled, the place where cancelling
			// this audit does removeAuditFromAuditMap
		} else if (audit->retryCount < SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX && e.code() != error_code_not_implemented) {
			audit->retryCount++;
			audit->actors.clear(true);
			TraceEvent(SevVerbose, "DDAuditStorageCoreRetry", self->ddId)
			    .detail("AuditID", auditID)
			    .detail("AuditType", auditType)
			    .detail("AuditStorageCoreGeneration", currentRetryCount)
			    .detail("RetryCount", audit->retryCount)
			    .detail("Contains", self->audits.contains(auditType) && self->audits[auditType].contains(auditID));
			wait(delay(0.1));
			TraceEvent(SevVerbose, "DDAuditStorageCoreRetryAfterWait", self->ddId)
			    .detail("AuditID", auditID)
			    .detail("AuditType", auditType)
			    .detail("AuditStorageCoreGeneration", currentRetryCount)
			    .detail("RetryCount", audit->retryCount)
			    .detail("Contains", self->audits.contains(auditType) && self->audits[auditType].contains(auditID));
			// Erase the old audit from map and spawn a new audit inherit from the old audit
			removeAuditFromAuditMap(self, audit->coreState.getType(),
			                        audit->coreState.id); // remove audit
			if (audit->coreState.getType() == AuditType::ValidateStorageServerShard) {
				runAuditStorage(self,
				                audit->coreState,
				                audit->retryCount,
				                DDAuditContext::RETRY,
				                audit->serversFinishedSSShardAudit);
			} else {
				runAuditStorage(self, audit->coreState, audit->retryCount, DDAuditContext::RETRY);
			}
		} else {
			try {
				audit->coreState.setPhase(AuditPhase::Failed);
				wait(persistAuditState(self->txnProcessor->context(),
				                       audit->coreState,
				                       "AuditStorageCoreError",
				                       lockInfo,
				                       self->context->isDDEnabled()));
				TraceEvent(SevWarn, "DDAuditStorageCoreSetAuditFailed", self->ddId)
				    .detail("Context", audit->getDDAuditContext())
				    .detail("AuditID", auditID)
				    .detail("AuditType", auditType)
				    .detail("AuditStorageCoreGeneration", currentRetryCount)
				    .detail("RetryCount", audit->retryCount)
				    .detail("AuditState", audit->coreState.toString());
			} catch (Error& e) {
				TraceEvent(SevWarn, "DDAuditStorageCoreErrorWhenSetAuditFailed", self->ddId)
				    .errorUnsuppressed(e)
				    .detail("Context", audit->getDDAuditContext())
				    .detail("AuditID", auditID)
				    .detail("AuditType", auditType)
				    .detail("AuditStorageCoreGeneration", currentRetryCount)
				    .detail("RetryCount", audit->retryCount)
				    .detail("AuditState", audit->coreState.toString());
				// unexpected error when persistAuditState
				// However, we do not want any audit error kills the DD
				// So, we silently remove audit from auditMap
				// As a result, this audit can be in RUNNING state on disk but not alive
				// We call this audit a zombie audit
				// Note that a client may wait for the state on disk to proceed to "complete"
				// However, this progress can never happen to a zombie audit
				// For this case, the client should be able to be timed out
				// A zombie audit will be either: (1) resumed by the next DD; (2) removed by client
			}
			removeAuditFromAuditMap(self, audit->coreState.getType(),
			                        audit->coreState.id); // remove audit
		}
	}
	return Void();
}

// runAuditStorage is the only entry to start an Audit entity
// Three scenarios when using runAuditStorage:
// (1) When DD receives an Audit request;
// (2) When DD restarts and resume an Audit;
// (3) When an Audit gets failed and retries.
// runAuditStorage is a non-flow function which starts an audit for auditState
// with four steps (the four steps are atomic):
// (1) Validate input auditState; (2) Create audit data structure based on input auditState;
// (3) register it to dd->audits, (4) run auditStorageCore
void runAuditStorage(Reference<DataDistributor> self,
                     AuditStorageState auditState,
                     int retryCount,
                     DDAuditContext context,
                     Optional<std::unordered_set<UID>> serversFinishedSSShardAudit) {
	// Validate input auditState
	if (auditState.getType() != AuditType::ValidateHA && auditState.getType() != AuditType::ValidateReplica &&
	    auditState.getType() != AuditType::ValidateLocationMetadata &&
	    auditState.getType() != AuditType::ValidateStorageServerShard) {
		throw not_implemented();
	}
	TraceEvent(SevDebug, "DDRunAuditStorage", self->ddId)
	    .detail("AuditState", auditState.toString())
	    .detail("Context", context);
	ASSERT(auditState.id.isValid());
	ASSERT(!auditState.range.empty());
	ASSERT(auditState.getPhase() == AuditPhase::Running);
	auditState.ddId = self->ddId; // make sure any existing audit state claims the current DD
	std::shared_ptr<DDAudit> audit = std::make_shared<DDAudit>(auditState);
	audit->retryCount = retryCount;
	audit->setDDAuditContext(context);
	if (serversFinishedSSShardAudit.present()) {
		audit->serversFinishedSSShardAudit = serversFinishedSSShardAudit.get();
	}
	addAuditToAuditMap(self, audit);
	audit->setAuditRunActor(auditStorageCore(self, audit->coreState.id, audit->coreState.getType(), audit->retryCount));
	return;
}

// Get audit for auditRange and auditType, if not exist, launch a new one
ACTOR Future<UID> launchAudit(Reference<DataDistributor> self,
                              KeyRange auditRange,
                              AuditType auditType,
                              KeyValueStoreType auditStorageEngineType) {
	state MoveKeyLockInfo lockInfo;
	lockInfo.myOwner = self->lock.myOwner;
	lockInfo.prevOwner = self->lock.prevOwner;
	lockInfo.prevWrite = self->lock.prevWrite;

	state UID auditID;
	try {
		TraceEvent(SevInfo, "DDAuditStorageLaunchStarts", self->ddId)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", auditStorageEngineType)
		    .detail("RequestedRange", auditRange);
		wait(self->auditStorageInitialized.getFuture());
		// Start an audit if no audit exists
		// If existing an audit for a different purpose, send error to client
		// aka, we only allow one audit at a time for all purposes
		if (existAuditInAuditMapForType(self, auditType)) {
			std::shared_ptr<DDAudit> audit;
			// find existing audit with requested type and range
			for (auto& [id, currentAudit] : getAuditsForType(self, auditType)) {
				TraceEvent(SevInfo, "DDAuditStorageLaunchCheckExisting", self->ddId)
				    .detail("AuditID", currentAudit->coreState.id)
				    .detail("AuditType", currentAudit->coreState.getType())
				    .detail("KeyValueStoreType", auditStorageEngineType)
				    .detail("AuditPhase", currentAudit->coreState.getPhase())
				    .detail("AuditRange", currentAudit->coreState.range)
				    .detail("AuditRetryTime", currentAudit->retryCount);
				// We do not want to distinguish audit phase here
				// An audit will be gracefully removed from the map after
				// the audit enters the complete/error/failed phase
				// If an audit gets removed from the map, we think this
				// audit finishes and a new audit can be created for the
				// same time.
				if (currentAudit->coreState.range.contains(auditRange)) {
					ASSERT(auditType == currentAudit->coreState.getType());
					auditID = currentAudit->coreState.id;
					audit = currentAudit;
					break;
				}
			}
			if (audit == nullptr) { // Only one ongoing audit is allowed at a time
				throw audit_storage_exceeded_request_limit();
			}
			TraceEvent(SevInfo, "DDAuditStorageLaunchExist", self->ddId)
			    .detail("AuditType", auditType)
			    .detail("KeyValueStoreType", auditStorageEngineType)
			    .detail("AuditID", auditID)
			    .detail("RequestedRange", auditRange)
			    .detail("ExistingState", audit->coreState.toString());
		} else {
			state AuditStorageState auditState;
			auditState.setType(auditType);
			auditState.engineType = auditStorageEngineType;
			auditState.range = auditRange;
			auditState.setPhase(AuditPhase::Running);
			auditState.ddId = self->ddId; // persist ddId to new ddAudit metadata
			TraceEvent(SevVerbose, "DDAuditStorageLaunchPersistNewAuditIDBefore", self->ddId)
			    .detail("AuditType", auditType)
			    .detail("KeyValueStoreType", auditStorageEngineType)
			    .detail("Range", auditRange);
			UID auditID_ = wait(persistNewAuditState(
			    self->txnProcessor->context(), auditState, lockInfo, self->context->isDDEnabled()));
			self->addActor.send(clearAuditMetadataForType(self->txnProcessor->context(),
			                                              auditState.getType(),
			                                              auditID_,
			                                              SERVER_KNOBS->PERSIST_FINISH_AUDIT_COUNT));
			// data distribution could restart in the middle of persistNewAuditState
			// It is possible that the auditState has been written to disk before data distribution restarts,
			// hence a new audit resumption loads audits from disk and launch the audits
			// Since the resumed audit has already taken over the launchAudit job,
			// we simply retry this launchAudit, then return the audit id to client
			if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
				TraceEvent(SevDebug, "DDAuditStorageLaunchInjectActorCancelWhenPersist", self->ddId)
				    .detail("AuditID", auditID_)
				    .detail("AuditType", auditType)
				    .detail("KeyValueStoreType", auditStorageEngineType)
				    .detail("Range", auditRange);
				throw operation_failed(); // Trigger DD restart and check if resume audit is correct
			}
			TraceEvent(SevInfo, "DDAuditStorageLaunchPersistNewAuditID", self->ddId)
			    .detail("AuditID", auditID_)
			    .detail("AuditType", auditType)
			    .detail("KeyValueStoreType", auditStorageEngineType)
			    .detail("Range", auditRange);
			auditState.id = auditID_;
			auditID = auditID_;
			if (self->audits.contains(auditType) && self->audits[auditType].contains(auditID)) {
				// It is possible that the current DD is running this audit
				// Suppose DDinit re-runs right after a new audit is persisted
				// For this case, auditResume sees the new audit and resumes it
				// At this point, the new audit is already in the audit map
				return auditID;
			}
			runAuditStorage(self, auditState, 0, DDAuditContext::LAUNCH);
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDAuditStorageLaunchError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", auditStorageEngineType)
		    .detail("Range", auditRange);
		throw e;
	}
	return auditID;
}

ACTOR Future<Void> cancelAuditStorage(Reference<DataDistributor> self, TriggerAuditRequest req) {
	state FlowLock::Releaser holder;
	if (req.getType() == AuditType::ValidateHA) {
		wait(self->auditStorageHaLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageHaLaunchingLock);
	} else if (req.getType() == AuditType::ValidateReplica) {
		wait(self->auditStorageReplicaLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageReplicaLaunchingLock);
	} else if (req.getType() == AuditType::ValidateLocationMetadata) {
		wait(self->auditStorageLocationMetadataLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageLocationMetadataLaunchingLock);
	} else if (req.getType() == AuditType::ValidateStorageServerShard) {
		wait(self->auditStorageSsShardLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageSsShardLaunchingLock);
	} else {
		req.reply.sendError(not_implemented());
		return Void();
	}

	try {
		ASSERT(req.cancel);
		ASSERT(req.id.isValid());
		TraceEvent(SevDebug, "DDCancelAuditStorageStart", self->ddId)
		    .detail("AuditType", req.getType())
		    .detail("AuditID", req.id);
		wait(cancelAuditMetadata(self->txnProcessor->context(), req.getType(), req.id));
		// Once auditMetadata cancelled, any ongoing audit will stop
		// Then clear ongoing audit D/S
		if (auditExistInAuditMap(self, req.getType(), req.id)) {
			removeAuditFromAuditMap(self, req.getType(), req.id);
		}
		TraceEvent(SevInfo, "DDCancelAuditStorageComplete", self->ddId)
		    .detail("AuditType", req.getType())
		    .detail("AuditID", req.id);
		req.reply.send(req.id);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "DDCancelAuditStorageError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", req.id)
		    .detail("AuditType", req.getType());
		req.reply.sendError(cancel_audit_storage_failed());
	}
	return Void();
}

// Handling audit requests
// For each request, launch audit storage and reply to CC with following three replies:
// (1) auditID: reply auditID when the audit is successfully launch
// (2) error_code_audit_storage_exceeded_request_limit: reply this error when dd
// already has a running auditStorage
// (3) error_code_audit_storage_failed: reply this error when: 1. the retry time exceeds the maximum;
// 2. failed to persist new audit state; 3. DD is cancelled during persisting new audit state
// For 1 and 2, we believe no new audit is persisted; For 3, we do not know whether a new
// audit is persisted.
ACTOR Future<Void> auditStorage(Reference<DataDistributor> self, TriggerAuditRequest req) {
	state FlowLock::Releaser holder;
	if (req.getType() == AuditType::ValidateHA) {
		wait(self->auditStorageHaLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageHaLaunchingLock);
	} else if (req.getType() == AuditType::ValidateReplica) {
		wait(self->auditStorageReplicaLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageReplicaLaunchingLock);
	} else if (req.getType() == AuditType::ValidateLocationMetadata) {
		wait(self->auditStorageLocationMetadataLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageLocationMetadataLaunchingLock);
	} else if (req.getType() == AuditType::ValidateStorageServerShard) {
		wait(self->auditStorageSsShardLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageSsShardLaunchingLock);
	} else {
		req.reply.sendError(not_implemented());
		return Void();
	}

	if (req.range.empty()) {
		req.reply.sendError(audit_storage_failed());
		return Void();
	}

	state int retryCount = 0;
	loop {
		try {
			TraceEvent(SevDebug, "DDAuditStorageStart", self->ddId)
			    .detail("RetryCount", retryCount)
			    .detail("AuditType", req.getType())
			    .detail("KeyValueStoreType", req.engineType.toString())
			    .detail("Range", req.range);
			UID auditID = wait(launchAudit(self, req.range, req.getType(), req.engineType));
			req.reply.send(auditID);
			TraceEvent(SevVerbose, "DDAuditStorageReply", self->ddId)
			    .detail("RetryCount", retryCount)
			    .detail("AuditType", req.getType())
			    .detail("KeyValueStoreType", req.engineType.toString())
			    .detail("Range", req.range)
			    .detail("AuditID", auditID);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "DDAuditStorageError", self->ddId)
			    .errorUnsuppressed(e)
			    .detail("RetryCount", retryCount)
			    .detail("AuditType", req.getType())
			    .detail("KeyValueStoreType", req.engineType.toString())
			    .detail("Range", req.range);
			if (e.code() == error_code_operation_failed && g_network->isSimulated()) {
				throw audit_storage_failed(); // to trigger dd restart
			} else if (e.code() == error_code_audit_storage_exceeded_request_limit) {
				req.reply.sendError(audit_storage_exceeded_request_limit());
			} else if (e.code() == error_code_persist_new_audit_metadata_error) {
				req.reply.sendError(audit_storage_failed());
			} else if (retryCount < SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
				retryCount++;
				wait(delay(0.1));
				continue;
			} else {
				req.reply.sendError(audit_storage_failed());
			}
		}
		break;
	}
	return Void();
}

// The entry of starting a series of audit workers
// Decide which dispatch impl according to audit type
void loadAndDispatchAudit(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit) {
	TraceEvent(SevInfo, "DDLoadAndDispatchAudit", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditType", audit->coreState.getType())
	    .detail("AuditRange", audit->coreState.range);

	if (audit->coreState.getType() == AuditType::ValidateHA) {
		audit->actors.add(dispatchAuditStorage(self, audit));
	} else if (audit->coreState.getType() == AuditType::ValidateReplica) {
		audit->actors.add(dispatchAuditStorage(self, audit));
	} else if (audit->coreState.getType() == AuditType::ValidateLocationMetadata) {
		audit->actors.add(dispatchAuditLocationMetadata(self, audit, allKeys));
	} else if (audit->coreState.getType() == AuditType::ValidateStorageServerShard) {
		audit->actors.add(dispatchAuditStorageServerShard(self, audit));
	} else {
		UNREACHABLE();
	}
	return;
}

// This function is for locationmetadata audits
// Schedule audit task on input range
ACTOR Future<Void> dispatchAuditLocationMetadata(Reference<DataDistributor> self,
                                                 std::shared_ptr<DDAudit> audit,
                                                 KeyRange range) {
	state const AuditType auditType = audit->coreState.getType();
	ASSERT(auditType == AuditType::ValidateLocationMetadata);
	TraceEvent(SevInfo, "DDdispatchAuditLocationMetadataBegin", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditType", auditType);
	state Key begin = range.begin;
	state KeyRange currentRange = range;
	state std::vector<AuditStorageState> auditStates;
	state int64_t issueDoAuditCount = 0;

	try {
		while (begin < range.end) {
			currentRange = KeyRangeRef(begin, range.end);
			wait(store(
			    auditStates,
			    getAuditStateByRange(self->txnProcessor->context(), auditType, audit->coreState.id, currentRange)));
			ASSERT(!auditStates.empty());
			begin = auditStates.back().range.end;
			TraceEvent(SevInfo, "DDdispatchAuditLocationMetadataDispatch", self->ddId)
			    .detail("AuditID", audit->coreState.id)
			    .detail("CurrentRange", currentRange)
			    .detail("AuditType", auditType)
			    .detail("NextBegin", begin)
			    .detail("RangeEnd", range.end);
			state int i = 0;
			for (; i < auditStates.size(); i++) {
				state AuditPhase phase = auditStates[i].getPhase();
				ASSERT(phase != AuditPhase::Running && phase != AuditPhase::Failed);
				if (phase == AuditPhase::Complete) {
					continue; // pass
				} else if (phase == AuditPhase::Error) {
					audit->foundError = true;
				} else {
					ASSERT(phase == AuditPhase::Invalid);
					// Set doAuditOnStorageServer
					ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					while (audit->remainingBudgetForAuditTasks.get() == 0) {
						wait(audit->remainingBudgetForAuditTasks.onChange());
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					}
					audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() - 1);
					ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
					    .detail("Loc", "dispatchAuditLocationMetadata")
					    .detail("Ops", "Decrease")
					    .detail("Val", audit->remainingBudgetForAuditTasks.get())
					    .detail("AuditType", auditType);
					issueDoAuditCount++;
					audit->actors.add(doAuditLocationMetadata(self, audit, auditStates[i].range));
				}
			}
			wait(delay(0.1));
		}
		TraceEvent(SevInfo, "DDdispatchAuditLocationMetadataEnd", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType)
		    .detail("IssuedDoAuditCount", issueDoAuditCount);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "DDdispatchAuditLocationMetadataError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType);
		audit->auditStorageAnyChildFailed = true;
	}

	return Void();
}

// This function dedicates to audit ssshard
// For each of storage servers, audits allKeys
ACTOR Future<Void> dispatchAuditStorageServerShard(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit) {
	state const AuditType auditType = audit->coreState.getType();
	ASSERT(auditType == AuditType::ValidateStorageServerShard);
	TraceEvent(SevInfo, "DDDispatchAuditStorageServerShardBegin", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditType", auditType);
	try {
		state std::vector<StorageServerInterface> interfs = wait(getStorageServers(self->txnProcessor->context()));
		state int i = 0;
		for (; i < interfs.size(); ++i) {
			state StorageServerInterface targetServer = interfs[i];
			// Currently, Tss server may not follow the auit consistency rule
			// Thus, skip if the server is tss
			if (targetServer.isTss()) {
				continue;
			}
			ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
			while (audit->remainingBudgetForAuditTasks.get() == 0) {
				wait(audit->remainingBudgetForAuditTasks.onChange());
				ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
			}
			audit->actors.add(scheduleAuditStorageShardOnServer(self, audit, targetServer));
			wait(delay(0.1));
		}
		TraceEvent(SevInfo, "DDDispatchAuditStorageServerShardEnd", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "DDDispatchAuditStorageServerShardError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType);
		audit->auditStorageAnyChildFailed = true;
	}

	return Void();
}

// Schedule audit ssshard task on the input storage server (ssi)
// Do audit on allKeys
// Automatically retry until complete or timed out
ACTOR Future<Void> scheduleAuditStorageShardOnServer(Reference<DataDistributor> self,
                                                     std::shared_ptr<DDAudit> audit,
                                                     StorageServerInterface ssi) {
	state UID serverId = ssi.uniqueID;
	state const AuditType auditType = audit->coreState.getType();
	ASSERT(auditType == AuditType::ValidateStorageServerShard);
	TraceEvent(SevInfo, "DDScheduleAuditStorageShardOnServerBegin", self->ddId)
	    .detail("ServerID", serverId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditType", auditType);
	state Key begin = allKeys.begin;
	state KeyRange currentRange = allKeys;
	state std::vector<AuditStorageState> auditStates;
	state int64_t issueDoAuditCount = 0;

	try {
		while (begin < allKeys.end) {
			currentRange = KeyRangeRef(begin, allKeys.end);
			wait(store(auditStates,
			           getAuditStateByServer(
			               self->txnProcessor->context(), auditType, audit->coreState.id, serverId, currentRange)));
			ASSERT(!auditStates.empty());
			begin = auditStates.back().range.end;
			TraceEvent(SevInfo, "DDScheduleAuditStorageShardOnServerDispatch", self->ddId)
			    .detail("ServerID", serverId)
			    .detail("AuditID", audit->coreState.id)
			    .detail("CurrentRange", currentRange)
			    .detail("AuditType", auditType)
			    .detail("NextBegin", begin)
			    .detail("RangeEnd", allKeys.end);
			state int i = 0;
			for (; i < auditStates.size(); i++) {
				state AuditPhase phase = auditStates[i].getPhase();
				ASSERT(phase != AuditPhase::Running && phase != AuditPhase::Failed);
				if (phase == AuditPhase::Complete) {
					continue; // pass
				} else if (phase == AuditPhase::Error) {
					audit->foundError = true;
				} else {
					ASSERT(phase == AuditPhase::Invalid);
					// Set doAuditOnStorageServer
					ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					while (audit->remainingBudgetForAuditTasks.get() == 0) {
						wait(audit->remainingBudgetForAuditTasks.onChange());
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					}
					audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() - 1);
					ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
					    .detail("Loc", "scheduleAuditStorageShardOnServer")
					    .detail("Ops", "Decrease")
					    .detail("Val", audit->remainingBudgetForAuditTasks.get())
					    .detail("AuditType", auditType);
					AuditStorageRequest req(audit->coreState.id, auditStates[i].range, auditType);
					issueDoAuditCount++;
					req.ddId = self->ddId; // send this ddid to SS
					wait(doAuditOnStorageServer(self, audit, ssi, req)); // do audit one by one
				}
			}
			wait(delay(0.1));
		}

		TraceEvent(SevInfo, "DDScheduleAuditStorageShardOnServerEnd", self->ddId)
		    .detail("ServerID", serverId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType)
		    .detail("IssuedDoAuditCount", issueDoAuditCount);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDScheduleAuditStorageShardOnServerError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType)
		    .detail("IssuedDoAuditCount", issueDoAuditCount);

		if (e.code() == error_code_not_implemented || e.code() == error_code_audit_storage_cancelled) {
			throw e;
		} else if (e.code() == error_code_audit_storage_error) {
			audit->foundError = true;
		} else if (audit->retryCount >= SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
			throw audit_storage_failed();
		} else {
			if (e.code() != error_code_audit_storage_failed) {
				try {
					bool ssRemoved = wait(checkStorageServerRemoved(self->txnProcessor->context(), ssi.uniqueID));
					if (ssRemoved) {
						// It is possible that the input ss has been removed, then silently exit
						return Void();
					}
				} catch (Error& e) {
					// retry
				}
			}
			audit->retryCount++;
			audit->actors.add(scheduleAuditStorageShardOnServer(self, audit, ssi));
		}
	}

	return Void();
}

// This function is for ha/replica audits
// Schedule audit task on the input range
ACTOR Future<Void> dispatchAuditStorage(Reference<DataDistributor> self, std::shared_ptr<DDAudit> audit) {
	state const AuditType auditType = audit->coreState.getType();
	state const KeyRange range = audit->coreState.range;
	ASSERT(auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica);
	TraceEvent(SevInfo, "DDDispatchAuditStorageBegin", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("Range", range)
	    .detail("AuditType", auditType);
	state Key begin = range.begin;
	state KeyRange currentRange = range;
	state int64_t completedCount = 0;
	state int64_t totalCount = 0;
	try {
		while (begin < range.end) {
			currentRange = KeyRangeRef(begin, range.end);
			state std::vector<AuditStorageState> auditStates =
			    wait(getAuditStateByRange(self->txnProcessor->context(), auditType, audit->coreState.id, currentRange));
			ASSERT(!auditStates.empty());
			begin = auditStates.back().range.end;
			TraceEvent(SevInfo, "DDDispatchAuditStorageDispatch", self->ddId)
			    .detail("AuditID", audit->coreState.id)
			    .detail("Range", range)
			    .detail("CurrentRange", currentRange)
			    .detail("AuditType", auditType)
			    .detail("NextBegin", begin)
			    .detail("NumAuditStates", auditStates.size());
			state int i = 0;
			for (; i < auditStates.size(); i++) {
				state AuditPhase phase = auditStates[i].getPhase();
				ASSERT(phase != AuditPhase::Running && phase != AuditPhase::Failed);
				totalCount++;
				if (phase == AuditPhase::Complete) {
					completedCount++;
				} else if (phase == AuditPhase::Error) {
					completedCount++;
					audit->foundError = true;
				} else {
					ASSERT(phase == AuditPhase::Invalid);
					ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					while (audit->remainingBudgetForAuditTasks.get() == 0) {
						wait(audit->remainingBudgetForAuditTasks.onChange());
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					}
					audit->actors.add(scheduleAuditOnRange(self, audit, auditStates[i].range));
				}
			}
			wait(delay(0.1));
		}
		TraceEvent(SevInfo, "DDDispatchAuditStorageEnd", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("Range", range)
		    .detail("AuditType", auditType)
		    .detail("TotalRanges", totalCount)
		    .detail("TotalComplete", completedCount)
		    .detail("CompleteRatio", completedCount * 1.0 / totalCount);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "DDDispatchAuditStorageError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType);
		audit->auditStorageAnyChildFailed = true;
	}

	return Void();
}

ACTOR Future<std::unordered_map<UID, KeyValueStoreType>> getStorageType(
    std::vector<StorageServerInterface> storageServers) {
	state std::vector<Future<ErrorOr<KeyValueStoreType>>> storageTypeFutures;
	state std::unordered_map<UID, KeyValueStoreType> res;

	try {
		for (int i = 0; i < storageServers.size(); i++) {
			ReplyPromise<KeyValueStoreType> typeReply;
			storageTypeFutures.push_back(
			    storageServers[i].getKeyValueStoreType.getReplyUnlessFailedFor(typeReply, 2, 0));
		}
		wait(waitForAll(storageTypeFutures));

		for (int i = 0; i < storageServers.size(); i++) {
			ErrorOr<KeyValueStoreType> reply = storageTypeFutures[i].get();
			if (!reply.present()) {
				TraceEvent(SevWarn, "AuditStorageFailedToGetStorageType")
				    .error(reply.getError())
				    .detail("StorageServer", storageServers[i].id())
				    .detail("IsTSS", storageServers[i].isTss() ? "True" : "False");
			} else {
				res[storageServers[i].id()] = reply.get();
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent("AuditStorageErrorGetStorageType").errorUnsuppressed(e);
		res.clear();
	}

	return res;
}

// Partition the input range into multiple subranges according to the range ownership, and
// schedule ha/replica audit tasks of each subrange on the server which owns the subrange
// Automatically retry until complete or timed out
ACTOR Future<Void> scheduleAuditOnRange(Reference<DataDistributor> self,
                                        std::shared_ptr<DDAudit> audit,
                                        KeyRange rangeToSchedule) {
	state const AuditType auditType = audit->coreState.getType();
	ASSERT(auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica);
	TraceEvent(SevInfo, "DDScheduleAuditOnRangeBegin", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditRange", audit->coreState.range)
	    .detail("RangeToSchedule", rangeToSchedule)
	    .detail("AuditType", auditType)
	    .detail("RemainingBudget", audit->remainingBudgetForAuditTasks.get());

	state Key currentRangeToScheduleBegin = rangeToSchedule.begin;
	state KeyRange currentRangeToSchedule;
	state int64_t issueDoAuditCount = 0;
	state int64_t numSkippedShards = 0;

	try {
		while (currentRangeToScheduleBegin < rangeToSchedule.end) {
			currentRangeToSchedule = Standalone(KeyRangeRef(currentRangeToScheduleBegin, rangeToSchedule.end));
			state std::vector<IDDTxnProcessor::DDRangeLocations> rangeLocations =
			    wait(self->txnProcessor->getSourceServerInterfacesForRange(currentRangeToSchedule));
			if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
				TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRange", self->ddId)
				    .detail("AuditID", audit->coreState.id)
				    .detail("AuditType", auditType)
				    .detail("RangeToSchedule", rangeToSchedule)
				    .detail("CurrentRangeToSchedule", currentRangeToSchedule)
				    .detail("NumTaskRanges", rangeLocations.size())
				    .detail("RangeLocationsBackKey", rangeLocations.back().range.end);
			}

			// Divide the audit job in to tasks according to KeyServers system mapping
			state int assignedRangeTasks = 0;
			state int rangeLocationIndex = 0;
			for (; rangeLocationIndex < rangeLocations.size(); ++rangeLocationIndex) {
				// For each task, check the progress, and create task request for the unfinished range
				state KeyRange taskRange = rangeLocations[rangeLocationIndex].range;
				if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
					TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRangeTask", self->ddId)
					    .detail("AuditID", audit->coreState.id)
					    .detail("AuditType", auditType)
					    .detail("RangeToSchedule", rangeToSchedule)
					    .detail("CurrentRangeToSchedule", currentRangeToSchedule)
					    .detail("TaskRange", taskRange);
				}

				state Key taskRangeBegin = taskRange.begin;
				while (taskRangeBegin < taskRange.end) {
					state std::vector<AuditStorageState> auditStates =
					    wait(getAuditStateByRange(self->txnProcessor->context(),
					                              auditType,
					                              audit->coreState.id,
					                              KeyRangeRef(taskRangeBegin, taskRange.end)));
					if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
						TraceEvent(SevInfo, "DDScheduleAuditOnRangeSubTask", self->ddId)
						    .detail("AuditID", audit->coreState.id)
						    .detail("AuditType", auditType)
						    .detail("AuditRange", audit->coreState.range)
						    .detail("RangeToSchedule", rangeToSchedule)
						    .detail("CurrentRangeToSchedule", currentRangeToSchedule)
						    .detail("TaskRange", taskRange)
						    .detail("SubTaskBegin", taskRangeBegin)
						    .detail("SubTaskEnd", auditStates.back().range.end)
						    .detail("NumAuditStates", auditStates.size());
					}
					ASSERT(!auditStates.empty());

					state int auditStateIndex = 0;
					for (; auditStateIndex < auditStates.size(); ++auditStateIndex) {
						state AuditPhase phase = auditStates[auditStateIndex].getPhase();
						ASSERT(phase != AuditPhase::Running && phase != AuditPhase::Failed);
						if (phase == AuditPhase::Complete) {
							continue;
						} else if (phase == AuditPhase::Error) {
							audit->foundError = true;
							continue;
						}
						// Create audit task for the range where the phase is Invalid which indicates
						// this range has not been audited
						ASSERT(phase == AuditPhase::Invalid);
						state AuditStorageRequest req(
						    audit->coreState.id, auditStates[auditStateIndex].range, auditType);
						state StorageServerInterface targetServer;
						state std::vector<StorageServerInterface> storageServersToCheck;
						// Set req.targetServers and targetServer, which will be
						// used to doAuditOnStorageServer
						// Different audit types have different settings
						if (auditType == AuditType::ValidateHA) {
							if (rangeLocations[rangeLocationIndex].servers.size() < 2) {
								TraceEvent(SevInfo, "DDScheduleAuditOnRangeEnd", self->ddId)
								    .detail("Reason", "Single DC, ignore")
								    .detail("AuditID", audit->coreState.id)
								    .detail("AuditRange", audit->coreState.range)
								    .detail("AuditType", auditType);
								return Void();
							}
							// pick a server from primary DC
							auto it = rangeLocations[rangeLocationIndex].servers.begin();
							const int idx = deterministicRandom()->randomInt(0, it->second.size());
							targetServer = it->second[idx];
							storageServersToCheck.push_back(it->second[idx]);
							++it;
							// pick a server from each remote DC
							for (; it != rangeLocations[rangeLocationIndex].servers.end(); ++it) {
								const int idx = deterministicRandom()->randomInt(0, it->second.size());
								req.targetServers.push_back(it->second[idx].id());
								storageServersToCheck.push_back(it->second[idx]);
							}
						} else if (auditType == AuditType::ValidateReplica) {
							// select a server from primary DC to do audit
							// check all servers from each DC
							int dcid = 0;
							for (const auto& [_, dcServers] : rangeLocations[rangeLocationIndex].servers) {
								if (dcid == 0) {
									// in primary DC randomly select a server to do the audit task
									const int idx = deterministicRandom()->randomInt(0, dcServers.size());
									targetServer = dcServers[idx];
								}
								for (int i = 0; i < dcServers.size(); i++) {
									if (dcServers[i].id() == targetServer.id()) {
										ASSERT_WE_THINK(dcid == 0);
									} else {
										req.targetServers.push_back(dcServers[i].id());
									}
									storageServersToCheck.push_back(dcServers[i]);
								}
								dcid++;
							}
							if (storageServersToCheck.size() <= 1) {
								TraceEvent(SevInfo, "DDScheduleAuditOnRangeEnd", self->ddId)
								    .detail("Reason", "Single replica, ignore")
								    .detail("AuditID", audit->coreState.id)
								    .detail("AuditRange", audit->coreState.range)
								    .detail("AuditType", auditType);
								return Void();
							}
						} else {
							UNREACHABLE();
						}
						// Set doAuditOnStorageServer
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
						while (audit->remainingBudgetForAuditTasks.get() == 0) {
							wait(audit->remainingBudgetForAuditTasks.onChange());
							ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
						}
						audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() - 1);
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
						TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
						    .detail("Loc", "scheduleAuditOnRange1")
						    .detail("Ops", "Decrease")
						    .detail("Val", audit->remainingBudgetForAuditTasks.get())
						    .detail("AuditType", auditType);

						req.ddId = self->ddId; // send this ddid to SS
						// Check if the shard is in any specified storage engine
						// If yes, issue doAuditOnStorageServer
						// Otherwise, persist progress complete
						state bool anySpecifiedEngine = false;
						if (audit->coreState.engineType == KeyValueStoreType::END) {
							// Do not specify any storage engine, so check for all engine types
							anySpecifiedEngine = true;
						} else {
							try {
								std::unordered_map<UID, KeyValueStoreType> storageTypeMapping =
								    wait(getStorageType(storageServersToCheck));
								for (int j = 0; j < storageServersToCheck.size(); j++) {
									auto ssStorageType = storageTypeMapping.find(storageServersToCheck[j].id());
									if (ssStorageType != storageTypeMapping.end()) {
										if (ssStorageType->second == audit->coreState.engineType) {
											anySpecifiedEngine = true;
											break;
										}
									}
								}
							} catch (Error& e) {
								audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
								ASSERT(audit->remainingBudgetForAuditTasks.get() <=
								       SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
								TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
								    .detail("Loc", "scheduleAuditOnRange")
								    .detail("Ops", "Increase")
								    .detail("Val", audit->remainingBudgetForAuditTasks.get())
								    .detail("AuditType", auditType);
								throw e;
							}
						}
						if (!anySpecifiedEngine) {
							numSkippedShards++;
							audit->actors.add(skipAuditOnRange(self, audit, auditStates[auditStateIndex].range));
						} else {
							issueDoAuditCount++;
							audit->actors.add(doAuditOnStorageServer(self, audit, targetServer, req));
						}
					}

					taskRangeBegin = auditStates.back().range.end;
					if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
						TraceEvent(SevInfo, "DDScheduleAuditOnRangeSubTaskAssigned", self->ddId)
						    .detail("TaskRange", taskRange)
						    .detail("NextTaskRangeBegin", taskRangeBegin)
						    .detail("BreakRangeEnd", taskRange.end);
					}
				}
				if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
					TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRangeTaskAssigned", self->ddId);
				}
				++assignedRangeTasks;
				wait(delay(0.1));
			}
			// Proceed to the next range if getSourceServerInterfacesForRange is partially read
			currentRangeToScheduleBegin = rangeLocations.back().range.end;
			if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
				TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRangeAssigned", self->ddId)
				    .detail("AssignedRangeTasks", assignedRangeTasks)
				    .detail("NextCurrentRangeToScheduleBegin", currentRangeToScheduleBegin)
				    .detail("BreakRangeEnd", rangeToSchedule.end)
				    .detail("RangeToSchedule", rangeToSchedule);
			}
		}

		TraceEvent(SevInfo, "DDScheduleAuditOnRangeEnd", self->ddId)
		    .detail("Reason", "End")
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditRange", audit->coreState.range)
		    .detail("RangeToSchedule", rangeToSchedule)
		    .detail("AuditType", auditType)
		    .detail("SkippedShardsCountInThisSchedule", numSkippedShards)
		    .detail("IssuedDoAuditCountInThisSchedule", issueDoAuditCount);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDScheduleAuditOnRangeError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditRange", audit->coreState.range)
		    .detail("RangeToSchedule", rangeToSchedule)
		    .detail("AuditType", auditType)
		    .detail("SkippedShardsCountInThisSchedule", numSkippedShards)
		    .detail("IssuedDoAuditCountInThisSchedule", issueDoAuditCount);
		audit->auditStorageAnyChildFailed = true;
	}

	return Void();
}

ACTOR Future<Void> skipAuditOnRange(Reference<DataDistributor> self,
                                    std::shared_ptr<DDAudit> audit,
                                    KeyRange rangeToSkip) {
	state AuditType auditType = audit->coreState.getType();
	ASSERT(auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica);
	try {
		audit->overallIssuedDoAuditCount++;
		AuditStorageState res(audit->coreState.id, rangeToSkip, auditType);
		res.setPhase(AuditPhase::Complete);
		res.ddId = self->ddId;
		wait(persistAuditStateByRange(self->txnProcessor->context(), res));
		audit->overallSkippedDoAuditCount++;
		TraceEvent(SevInfo, "DDSkipAuditOnRangeComplete", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditRange", audit->coreState.range)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
		    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "skipAuditOnRange")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", auditType);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDSkipAuditOnRangeError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditRange", audit->coreState.range)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
		    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "skipAuditOnRange")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", auditType);
		if (e.code() == error_code_audit_storage_cancelled || e.code() == error_code_audit_storage_task_outdated) {
			throw e;
		} else if (audit->retryCount >= SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
			throw audit_storage_failed();
		} else {
			audit->actors.add(scheduleAuditOnRange(self, audit, rangeToSkip));
		}
	}
	return Void();
}

// Request SS to do the audit
// This actor is the only interface to SS to do the audit for
// all audit types
ACTOR Future<Void> doAuditOnStorageServer(Reference<DataDistributor> self,
                                          std::shared_ptr<DDAudit> audit,
                                          StorageServerInterface ssi,
                                          AuditStorageRequest req) {
	state AuditType auditType = req.getType();
	ASSERT(auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica ||
	       auditType == AuditType::ValidateStorageServerShard);
	TraceEvent(SevInfo, "DDDoAuditOnStorageServerBegin", self->ddId)
	    .detail("AuditID", req.id)
	    .detail("Range", req.range)
	    .detail("AuditType", auditType)
	    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
	    .detail("StorageServer", ssi.toString())
	    .detail("TargetServers", describe(req.targetServers))
	    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
	    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
	    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);

	try {
		audit->overallIssuedDoAuditCount++;
		ASSERT(req.ddId.isValid());
		ErrorOr<AuditStorageState> vResult = wait(ssi.auditStorage.tryGetReply(req));
		if (vResult.isError()) {
			throw vResult.getError();
		}
		audit->overallCompleteDoAuditCount++;
		TraceEvent(SevInfo, "DDDoAuditOnStorageServerResult", self->ddId)
		    .detail("AuditID", req.id)
		    .detail("Range", req.range)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
		    .detail("StorageServer", ssi.toString())
		    .detail("TargetServers", describe(req.targetServers))
		    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "doAuditOnStorageServer")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", auditType);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDDoAuditOnStorageServerError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", req.id)
		    .detail("Range", req.range)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
		    .detail("StorageServer", ssi.toString())
		    .detail("TargetServers", describe(req.targetServers))
		    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "doAuditOnStorageServerError")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", auditType);
		if (req.getType() == AuditType::ValidateStorageServerShard) {
			throw e; // handled by scheduleAuditStorageShardOnServer
		}
		if (e.code() == error_code_not_implemented || e.code() == error_code_audit_storage_exceeded_request_limit ||
		    e.code() == error_code_audit_storage_cancelled || e.code() == error_code_audit_storage_task_outdated) {
			throw e;
		} else if (e.code() == error_code_audit_storage_error) {
			audit->foundError = true;
		} else if (audit->retryCount >= SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
			throw audit_storage_failed();
		} else {
			ASSERT(req.getType() != AuditType::ValidateStorageServerShard);
			audit->retryCount++;
			audit->actors.add(scheduleAuditOnRange(self, audit, req.range));
		}
	}
	return Void();
}

// Check consistency between KeyServers and ServerKeys system key space
ACTOR Future<Void> doAuditLocationMetadata(Reference<DataDistributor> self,
                                           std::shared_ptr<DDAudit> audit,
                                           KeyRange auditRange) {
	ASSERT(audit->coreState.getType() == AuditType::ValidateLocationMetadata);
	TraceEvent(SevInfo, "DDDoAuditLocationMetadataBegin", self->ddId)
	    .detail("AuditId", audit->coreState.id)
	    .detail("AuditRange", auditRange);
	state AuditStorageState res(audit->coreState.id,
	                            audit->coreState.getType()); // we will set range of audit later
	state std::vector<Future<Void>> actors;
	state std::vector<std::string> errors;
	state AuditGetKeyServersRes keyServerRes;
	state std::unordered_map<UID, AuditGetServerKeysRes> serverKeyResMap;
	state Version readAtVersion;
	state std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServersRaw;
	// Note that since krmReadRange may not return the value of the entire range at a time
	// Given auditRange, a part of the range is returned, thus, only a part of the range is
	// able to be compared --- claimRange
	// Given claimRange, rangeToRead is decided for reading the remaining range
	// At beginning, rangeToRead is auditRange
	state KeyRange claimRange;
	state KeyRange completeRangeByKeyServer;
	// To compare
	state std::unordered_map<UID, std::vector<KeyRange>> mapFromServerKeys;
	state std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServers;

	state Transaction tr(self->txnProcessor->context());
	state Key rangeToReadBegin = auditRange.begin;
	state KeyRangeRef rangeToRead;
	state int64_t cumulatedValidatedServerKeysNum = 0;
	state int64_t cumulatedValidatedKeyServersNum = 0;
	state Reference<IRateControl> rateLimiter =
	    Reference<IRateControl>(new SpeedLimit(SERVER_KNOBS->AUDIT_STORAGE_RATE_PER_SERVER_MAX, 1));
	state int64_t remoteReadBytes = 0;
	state double lastRateLimiterWaitTime = 0;
	state double rateLimiterBeforeWaitTime = 0;
	state double rateLimiterTotalWaitTime = 0;

	try {
		loop {
			try {
				// Read
				actors.clear();
				errors.clear();
				mapFromServerKeys.clear();
				mapFromKeyServers.clear();
				serverKeyResMap.clear();
				mapFromKeyServersRaw.clear();
				remoteReadBytes = 0;

				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				// Read KeyServers
				wait(store(keyServerRes, getShardMapFromKeyServers(self->ddId, &tr, rangeToRead)));
				completeRangeByKeyServer = keyServerRes.completeRange;
				readAtVersion = keyServerRes.readAtVersion;
				mapFromKeyServersRaw = keyServerRes.rangeOwnershipMap;
				remoteReadBytes += keyServerRes.readBytes;
				// Use ssid of mapFromKeyServersRaw to read ServerKeys
				for (auto& [ssid, _] : mapFromKeyServersRaw) {
					actors.push_back(
					    store(serverKeyResMap[ssid], getThisServerKeysFromServerKeys(ssid, &tr, rangeToRead)));
				}
				wait(waitForAll(actors));

				// Decide claimRange and check readAtVersion
				claimRange = completeRangeByKeyServer;
				for (auto& [ssid, serverKeyRes] : serverKeyResMap) {
					KeyRange serverKeyCompleteRange = serverKeyRes.completeRange;
					TraceEvent(SevVerbose, "DDDoAuditLocationMetadataGetClaimRange", self->ddId)
					    .detail("ServerId", ssid)
					    .detail("ServerKeyCompleteRange", serverKeyCompleteRange)
					    .detail("CurrentClaimRange", claimRange);
					KeyRange overlappingRange = serverKeyCompleteRange & claimRange;
					if (serverKeyCompleteRange.begin != claimRange.begin || overlappingRange.empty() ||
					    readAtVersion != serverKeyRes.readAtVersion) {
						TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
						           "DDDoAuditLocationMetadataReadCheckWrong",
						           self->ddId)
						    .detail("ServerKeyCompleteRangeBegin", serverKeyCompleteRange.begin)
						    .detail("ClaimRangeBegin", claimRange.begin)
						    .detail("OverlappingRange", overlappingRange)
						    .detail("ReadAtVersion", readAtVersion)
						    .detail("ServerKeyResReadAtVersion", serverKeyRes.readAtVersion);
						throw audit_storage_cancelled();
					}
					claimRange = overlappingRange;
					remoteReadBytes += serverKeyRes.readBytes;
				}
				// Use claimRange to get mapFromServerKeys and mapFromKeyServers to compare
				int64_t numValidatedServerKeys = 0;
				for (auto& [ssid, serverKeyRes] : serverKeyResMap) {
					for (auto& range : serverKeyRes.ownRanges) {
						KeyRange overlappingRange = range & claimRange;
						if (overlappingRange.empty()) {
							continue;
						}
						TraceEvent(SevVerbose, "DDDoAuditLocationMetadataAddToServerKeyMap", self->ddId)
						    .detail("RawRange", range)
						    .detail("ClaimRange", claimRange)
						    .detail("Range", overlappingRange)
						    .detail("SSID", ssid);
						mapFromServerKeys[ssid].push_back(overlappingRange);
						numValidatedServerKeys++;
					}
				}
				cumulatedValidatedServerKeysNum = cumulatedValidatedServerKeysNum + numValidatedServerKeys;

				int64_t numValidatedKeyServers = 0;
				for (auto& [ssid, ranges] : mapFromKeyServersRaw) {
					std::vector mergedRanges = coalesceRangeList(ranges);
					for (auto& range : mergedRanges) {
						KeyRange overlappingRange = range & claimRange;
						if (overlappingRange.empty()) {
							continue;
						}
						TraceEvent(SevVerbose, "DDDoAuditLocationMetadataAddToKeyServerMap", self->ddId)
						    .detail("RawRange", range)
						    .detail("ClaimRange", claimRange)
						    .detail("Range", overlappingRange)
						    .detail("SSID", ssid);
						mapFromKeyServers[ssid].push_back(overlappingRange);
						numValidatedKeyServers++;
					}
				}
				cumulatedValidatedKeyServersNum = cumulatedValidatedKeyServersNum + numValidatedKeyServers;

				// Compare: check if mapFromKeyServers === mapFromServerKeys
				// 1. check mapFromKeyServers => mapFromServerKeys
				for (auto& [ssid, keyServerRanges] : mapFromKeyServers) {
					if (!mapFromServerKeys.contains(ssid)) {
						std::string error =
						    format("KeyServers and serverKeys mismatch: Some key in range(%s, %s) exists "
						           "on Server(%s) in KeyServers but not ServerKeys",
						           claimRange.toString().c_str(),
						           claimRange.toString().c_str(),
						           ssid.toString().c_str());
						errors.push_back(error);
						TraceEvent(SevError, "DDDoAuditLocationMetadataError", self->ddId)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", audit->coreState.id)
						    .detail("AuditRange", auditRange)
						    .detail("ClaimRange", claimRange)
						    .detail("ErrorMessage", error);
					}
					std::vector<KeyRange> serverKeyRanges = mapFromServerKeys[ssid];
					Optional<std::pair<KeyRange, KeyRange>> anyMismatch = rangesSame(keyServerRanges, serverKeyRanges);
					if (anyMismatch.present()) { // mismatch detected
						KeyRange mismatchedRangeByKeyServer = anyMismatch.get().first;
						KeyRange mismatchedRangeByServerKey = anyMismatch.get().second;
						std::string error =
						    format("KeyServers and serverKeys mismatch on Server(%s): KeyServer: %s; ServerKey: %s",
						           ssid.toString().c_str(),
						           mismatchedRangeByKeyServer.toString().c_str(),
						           mismatchedRangeByServerKey.toString().c_str());
						errors.push_back(error);
						TraceEvent(SevError, "DDDoAuditLocationMetadataError", self->ddId)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", audit->coreState.id)
						    .detail("AuditRange", auditRange)
						    .detail("ClaimRange", claimRange)
						    .detail("ErrorMessage", error)
						    .detail("MismatchedRangeByKeyServer", mismatchedRangeByKeyServer)
						    .detail("MismatchedRangeByServerKey", mismatchedRangeByServerKey);
					}
				}
				// 2. check mapFromServerKeys => mapFromKeyServers
				for (auto& [ssid, serverKeyRanges] : mapFromServerKeys) {
					if (!mapFromKeyServers.contains(ssid)) {
						std::string error =
						    format("KeyServers and serverKeys mismatch: Some key of range(%s, %s) exists "
						           "on Server(%s) in ServerKeys but not KeyServers",
						           claimRange.toString().c_str(),
						           claimRange.toString().c_str(),
						           ssid.toString().c_str());
						errors.push_back(error);
						TraceEvent(SevError, "DDDoAuditLocationMetadataError", self->ddId)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", audit->coreState.id)
						    .detail("AuditRange", auditRange)
						    .detail("ClaimRange", claimRange)
						    .detail("ErrorMessage", error);
					}
				}

				// Log statistic
				TraceEvent(SevInfo, "DDDoAuditLocationMetadataStatistic", self->ddId)
				    .suppressFor(30.0)
				    .detail("AuditType", audit->coreState.getType())
				    .detail("AuditId", audit->coreState.id)
				    .detail("AuditRange", auditRange)
				    .detail("CurrentValidatedServerKeysNum", numValidatedServerKeys)
				    .detail("CurrentValidatedKeyServersNum", numValidatedServerKeys)
				    .detail("CurrentValidatedInclusiveRange", claimRange)
				    .detail("CumulatedValidatedServerKeysNum", cumulatedValidatedServerKeysNum)
				    .detail("CumulatedValidatedKeyServersNum", cumulatedValidatedKeyServersNum)
				    .detail("CumulatedValidatedInclusiveRange", KeyRangeRef(auditRange.begin, claimRange.end));

				// Return result
				if (!errors.empty()) {
					TraceEvent(SevError, "DDDoAuditLocationMetadataError", self->ddId)
					    .setMaxFieldLength(-1)
					    .setMaxEventLength(-1)
					    .detail("AuditId", audit->coreState.id)
					    .detail("AuditRange", auditRange)
					    .detail("NumErrors", errors.size())
					    .detail("Version", readAtVersion)
					    .detail("ClaimRange", claimRange);
					res.range = claimRange;
					res.setPhase(AuditPhase::Error);
					res.ddId = self->ddId; // used to compare self->ddId with existing persisted ddId
					wait(persistAuditStateByRange(self->txnProcessor->context(), res));
					throw audit_storage_error();
				} else {
					// Expand persisted complete range
					res.range = Standalone(KeyRangeRef(auditRange.begin, claimRange.end));
					res.setPhase(AuditPhase::Complete);
					res.ddId = self->ddId; // used to compare self->ddId with existing persisted ddId
					wait(persistAuditStateByRange(self->txnProcessor->context(), res));
					if (res.range.end < auditRange.end) {
						TraceEvent(SevInfo, "DDDoAuditLocationMetadataPartialDone", self->ddId)
						    .suppressFor(10.0)
						    .detail("AuditId", audit->coreState.id)
						    .detail("AuditRange", auditRange)
						    .detail("Version", readAtVersion)
						    .detail("CompleteRange", res.range)
						    .detail("LastRateLimiterWaitTime", lastRateLimiterWaitTime)
						    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime);
						rangeToReadBegin = res.range.end;
					} else { // complete
						TraceEvent(SevInfo, "DDDoAuditLocationMetadataComplete", self->ddId)
						    .detail("AuditId", audit->coreState.id)
						    .detail("AuditRange", auditRange)
						    .detail("CompleteRange", res.range)
						    .detail("NumValidatedServerKeys", cumulatedValidatedServerKeysNum)
						    .detail("NumValidatedKeyServers", cumulatedValidatedKeyServersNum)
						    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime);
						break;
					}
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}

			rateLimiterBeforeWaitTime = now();
			wait(rateLimiter->getAllowance(remoteReadBytes)); // Rate Keeping
			lastRateLimiterWaitTime = now() - rateLimiterBeforeWaitTime;
			rateLimiterTotalWaitTime = rateLimiterTotalWaitTime + lastRateLimiterWaitTime;
		}
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "doAuditLocationMetadata")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", audit->coreState.getType());

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDDoAuditLocationMetadataFailed", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditId", audit->coreState.id)
		    .detail("AuditRange", auditRange);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "doAuditLocationMetadataFailed")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", audit->coreState.getType());
		if (e.code() == error_code_audit_storage_error || e.code() == error_code_audit_storage_cancelled ||
		    e.code() == error_code_audit_storage_task_outdated) {
			throw e;
		} else if (audit->retryCount >= SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
			throw audit_storage_failed();
		} else {
			audit->retryCount++;
			audit->actors.add(dispatchAuditLocationMetadata(self, audit, auditRange));
		}
	}

	return Void();
}

ACTOR Future<Void> dataDistributor_impl(DataDistributorInterface di,
                                        Reference<DataDistributor> self,
                                        IsMocked isMocked) {
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	state PromiseStream<GetMetricsListRequest> getShardMetricsList;
	state Database cx;
	state ActorCollection actors(false);
	state std::map<UID, DistributorSnapRequest> ddSnapReqMap;
	state std::map<UID, ErrorOr<Void>> ddSnapReqResultMap;

	TraceEvent("DataDistributorRunning", di.id()).detail("IsMocked", isMocked);
	self->addActor.send(actors.getResult());
	self->addActor.send(traceRole(Role::DATA_DISTRIBUTOR, di.id()));
	self->addActor.send(waitFailureServer(di.waitFailure.getFuture()));
	if (!isMocked) {
		cx = openDBOnServer(self->dbInfo, TaskPriority::DefaultDelay, LockAware::True);
		self->addActor.send(cacheServerWatcher(&cx));
	}

	state Future<Void> distributor = reportErrorsExcept(dataDistribution(self, getShardMetricsList, isMocked),
	                                                    "DataDistribution",
	                                                    di.id(),
	                                                    &normalDataDistributorErrors());

	try {
		loop choose {
			when(wait(distributor || collection)) {
				ASSERT(false);
				throw internal_error();
			}
			when(HaltDataDistributorRequest req = waitNext(di.haltDataDistributor.getFuture())) {
				req.reply.send(Void());
				TraceEvent("DataDistributorHalted", di.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(GetDataDistributorMetricsRequest req = waitNext(di.dataDistributorMetrics.getFuture())) {
				actors.add(ddGetMetrics(req, getShardMetricsList));
			}
			when(DistributorSnapRequest snapReq = waitNext(di.distributorSnapReq.getFuture())) {
				auto& snapUID = snapReq.snapUID;
				if (ddSnapReqResultMap.contains(snapUID)) {
					CODE_PROBE(true,
					           "Data distributor received a duplicate finished snapshot request",
					           probe::decoration::rare);
					auto result = ddSnapReqResultMap[snapUID];
					result.isError() ? snapReq.reply.sendError(result.getError()) : snapReq.reply.send(result.get());
					TraceEvent("RetryFinishedDistributorSnapRequest")
					    .detail("SnapUID", snapUID)
					    .detail("Result", result.isError() ? result.getError().code() : 0);
				} else if (ddSnapReqMap.contains(snapReq.snapUID)) {
					CODE_PROBE(true, "Data distributor received a duplicate ongoing snapshot request");
					TraceEvent("RetryOngoingDistributorSnapRequest").detail("SnapUID", snapUID);
					ASSERT(snapReq.snapPayload == ddSnapReqMap[snapUID].snapPayload);
					// Discard the old request if a duplicate new request is received
					ddSnapReqMap[snapUID].reply.sendError(duplicate_snapshot_request());
					ddSnapReqMap[snapUID] = snapReq;
				} else {
					ddSnapReqMap[snapUID] = snapReq;
					auto* ddSnapReqResultMapPtr = &ddSnapReqResultMap;
					actors.add(fmap(
					    [ddSnapReqResultMapPtr, snapUID](Void _) {
						    ddSnapReqResultMapPtr->erase(snapUID);
						    return Void();
					    },
					    delayed(ddSnapCreate(snapReq,
					                         self->dbInfo,
					                         self->context->ddEnabledState.get(),
					                         &ddSnapReqMap,
					                         &ddSnapReqResultMap),
					            SERVER_KNOBS->SNAP_MINIMUM_TIME_GAP)));
				}
			}
			when(DistributorExclusionSafetyCheckRequest exclCheckReq =
			         waitNext(di.distributorExclCheckReq.getFuture())) {
				actors.add(ddExclusionSafetyCheck(exclCheckReq, self, cx));
			}
			when(GetStorageWigglerStateRequest req = waitNext(di.storageWigglerState.getFuture())) {
				req.reply.send(getStorageWigglerStates(self));
			}
			when(TriggerAuditRequest req = waitNext(di.triggerAudit.getFuture())) {
				if (req.cancel) {
					ASSERT(req.id.isValid());
					actors.add(cancelAuditStorage(self, req));
					continue;
				}
				actors.add(auditStorage(self, req));
			}
			when(TenantsOverStorageQuotaRequest req = waitNext(di.tenantsOverStorageQuota.getFuture())) {
				req.reply.send(getTenantsOverStorageQuota(self));
			}
		}
	} catch (Error& err) {
		if (!(normalDataDistributorErrors().contains(err.code()))) {
			TraceEvent("DataDistributorError", di.id()).errorUnsuppressed(err);
			throw err;
		}
		TraceEvent("DataDistributorDied", di.id()).errorUnsuppressed(err);
	}

	return Void();
}

Future<Void> MockDataDistributor::run(Reference<DDSharedContext> context, Reference<DDMockTxnProcessor> txnProcessor) {
	Reference<DataDistributor> dd =
	    makeReference<DataDistributor>(Reference<AsyncVar<ServerDBInfo> const>(nullptr), context->ddId, context, "");
	dd->txnProcessor = txnProcessor;
	return dataDistributor_impl(context->interface, dd, IsMocked::True);
}

Future<Void> dataDistributor(DataDistributorInterface di,
                             Reference<AsyncVar<ServerDBInfo> const> db,
                             std::string folder) {
	return dataDistributor_impl(
	    di, makeReference<DataDistributor>(db, di.id(), makeReference<DDSharedContext>(di), folder), IsMocked::False);
}

namespace data_distribution_test {

inline DDShardInfo doubleToNoLocationShardInfo(double d, bool hasDest) {
	DDShardInfo res(doubleToTestKey(d), anonymousShardId, anonymousShardId);
	res.primarySrc.emplace_back((uint64_t)d, 0);
	if (hasDest) {
		res.primaryDest.emplace_back((uint64_t)d + 1, 0);
		res.hasDest = true;
	}
	return res;
}

inline int getRandomShardCount() {
#if defined(USE_SANITIZER)
	return deterministicRandom()->randomInt(1000, 24000); // 24000 * MAX_SHARD_SIZE = 12TB
#else
	return deterministicRandom()->randomInt(1000, CLIENT_KNOBS->TOO_MANY); // 2000000000; OOM
#endif
}

} // namespace data_distribution_test

TEST_CASE("/DataDistribution/StorageWiggler/Order") {
	StorageWiggler wiggler(nullptr);
	double startTime = now() - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC - 0.4;
	wiggler.addServer(UID(1, 0), StorageMetadataType(startTime, KeyValueStoreType::SSD_BTREE_V2));
	wiggler.addServer(UID(2, 0), StorageMetadataType(startTime + 0.1, KeyValueStoreType::MEMORY, true));
	wiggler.addServer(UID(3, 0), StorageMetadataType(startTime + 0.2, KeyValueStoreType::SSD_ROCKSDB_V1, true));
	wiggler.addServer(UID(4, 0), StorageMetadataType(startTime + 0.3, KeyValueStoreType::SSD_BTREE_V2));

	std::vector<UID> correctOrder{ UID(2, 0), UID(3, 0), UID(1, 0), UID(4, 0) };
	for (int i = 0; i < correctOrder.size(); ++i) {
		auto id = wiggler.getNextServerId();
		std::cout << "Get " << id.get().shortString() << "\n";
		ASSERT(id == correctOrder[i]);
	}
	ASSERT(!wiggler.getNextServerId().present());
	return Void();
}

TEST_CASE("/DataDistribution/Initialization/ResumeFromShard") {
	state Reference<DDSharedContext> context(new DDSharedContext(UID()));
	state Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	state Reference<DataDistributor> self(new DataDistributor(dbInfo, UID(), context, ""));

	self->shardsAffectedByTeamFailure = makeReference<ShardsAffectedByTeamFailure>();
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
		self->physicalShardCollection = makeReference<PhysicalShardCollection>();
	}
	self->initData = makeReference<InitialDataDistribution>();
	self->configuration.usableRegions = 1;
	self->configuration.storageTeamSize = 1;

	// add DDShardInfo
	self->shardsAffectedByTeamFailure->setCheckMode(
	    ShardsAffectedByTeamFailure::CheckMode::ForceNoCheck); // skip check when build
	int shardNum = data_distribution_test::getRandomShardCount();
	std::cout << "generating " << shardNum << " shards...\n";
	for (int i = 1; i <= SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM; ++i) {
		self->initData->shards.emplace_back(data_distribution_test::doubleToNoLocationShardInfo(i, true));
	}
	for (int i = SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM + 1; i <= shardNum; ++i) {
		self->initData->shards.emplace_back(data_distribution_test::doubleToNoLocationShardInfo(i, false));
	}
	self->initData->shards.emplace_back(DDShardInfo(allKeys.end));
	std::cout << "Start resuming...\n";
	wait(DataDistributor::resumeFromShards(self, false));
	std::cout << "Start validation...\n";
	auto relocateFuture = self->relocationProducer.getFuture();
	for (int i = 0; i < SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM; ++i) {
		ASSERT(relocateFuture.isReady());
		auto rs = relocateFuture.pop();
		ASSERT(rs.isRestore() == false);
		ASSERT(rs.cancelled == false);
		ASSERT(rs.dataMoveId == anonymousShardId);
		ASSERT(rs.priority == SERVER_KNOBS->PRIORITY_RECOVER_MOVE);
		// std::cout << rs.keys.begin.toString() << " " << self->initData->shards[i].key.toString() << " \n";
		ASSERT(rs.keys.begin.compare(self->initData->shards[i].key) == 0);
		ASSERT(rs.keys.end == self->initData->shards[i + 1].key);
	}
	self->shardsAffectedByTeamFailure->setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);
	self->shardsAffectedByTeamFailure->check();
	return Void();
}
