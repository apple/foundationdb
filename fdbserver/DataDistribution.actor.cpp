/*
 * DataDistribution.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/TenantCache.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/BooleanParam.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"

ShardSizeBounds ShardSizeBounds::shardSizeBoundsBeforeTrack() {
	return ShardSizeBounds{
		.max = StorageMetrics{ .bytes = -1,
		                       .bytesWrittenPerKSecond = StorageMetrics::infinity,
		                       .iosPerKSecond = StorageMetrics::infinity,
		                       .bytesReadPerKSecond = StorageMetrics::infinity },
		.min = StorageMetrics{ .bytes = -1, .bytesWrittenPerKSecond = 0, .iosPerKSecond = 0, .bytesReadPerKSecond = 0 },
		.permittedError = StorageMetrics{ .bytes = -1,
		                                  .bytesWrittenPerKSecond = StorageMetrics::infinity,
		                                  .iosPerKSecond = StorageMetrics::infinity,
		                                  .bytesReadPerKSecond = StorageMetrics::infinity }
	};
}

struct DDAudit {
	DDAudit(UID id, KeyRange range, AuditType type)
	  : id(id), range(range), type(type), auditMap(AuditPhase::Invalid, allKeys.end), actors(true) {}

	const UID id;
	KeyRange range;
	const AuditType type;
	KeyRangeMap<AuditPhase> auditMap;
	ActorCollection actors;
};

void DataMove::validateShard(const DDShardInfo& shard, KeyRangeRef range, int priority) {
	if (!valid) {
		if (shard.hasDest && shard.destId != anonymousShardId) {
			TraceEvent(SevError, "DataMoveValidationError")
			    .detail("Range", range)
			    .detail("Reason", "DataMoveMissing")
			    .detail("ShardPrimaryDest", describe(shard.primaryDest))
			    .detail("ShardRemoteDest", describe(shard.remoteDest));
		}
		return;
	}

	ASSERT(!this->meta.ranges.empty() && this->meta.ranges.front().contains(range));

	if (!shard.hasDest) {
		TraceEvent(SevError, "DataMoveValidationError")
		    .detail("Range", range)
		    .detail("Reason", "ShardMissingDest")
		    .detail("DataMoveMetaData", this->meta.toString())
		    .detail("DataMovePrimaryDest", describe(this->primaryDest))
		    .detail("DataMoveRemoteDest", describe(this->remoteDest));
		cancelled = true;
		return;
	}

	if (shard.destId != this->meta.id) {
		TraceEvent(SevError, "DataMoveValidationError")
		    .detail("Range", range)
		    .detail("Reason", "DataMoveIDMissMatch")
		    .detail("DataMoveMetaData", this->meta.toString())
		    .detail("ShardMoveID", shard.destId);
		cancelled = true;
		return;
	}

	if (!std::includes(
	        this->primaryDest.begin(), this->primaryDest.end(), shard.primaryDest.begin(), shard.primaryDest.end()) ||
	    !std::includes(
	        this->remoteDest.begin(), this->remoteDest.end(), shard.remoteDest.begin(), shard.remoteDest.end())) {
		TraceEvent(SevError, "DataMoveValidationError")
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
	ASSERT(!pq_handles.count(serverId));
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

static std::set<int> const& normalDDQueueErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_movekeys_conflict);
		s.insert(error_code_broken_promise);
		s.insert(error_code_data_move_cancelled);
		s.insert(error_code_data_move_dest_team_not_found);
	}
	return s;
}

struct DataDistributor : NonCopyable, ReferenceCounted<DataDistributor> {
public:
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<DDSharedContext> context;
	UID ddId;
	PromiseStream<Future<Void>> addActor;

	// State initialized when bootstrap
	Reference<IDDTxnProcessor> txnProcessor;
	MoveKeysLock lock;
	DatabaseConfiguration configuration;
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
	Reference<PhysicalShardCollection> physicalShardCollection;

	Promise<Void> initialized;

	std::unordered_map<AuditType, std::unordered_map<UID, std::shared_ptr<DDAudit>>> audits;
	Future<Void> auditInitialized;

	Optional<Reference<TenantCache>> ddTenantCache;

	DataDistributor(Reference<AsyncVar<ServerDBInfo> const> const& db, UID id, Reference<DDSharedContext> context)
	  : dbInfo(db), context(context), ddId(id), txnProcessor(nullptr),
	    initialDDEventHolder(makeReference<EventCacheHolder>("InitialDD")),
	    movingDataEventHolder(makeReference<EventCacheHolder>("MovingData")),
	    totalDataInFlightEventHolder(makeReference<EventCacheHolder>("TotalDataInFlight")),
	    totalDataInFlightRemoteEventHolder(makeReference<EventCacheHolder>("TotalDataInFlightRemote")),
	    teamCollection(nullptr) {}

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

	// Initialize the required internal states of DataDistributor from system metadata. It's necessary before
	// DataDistributor start working. Doesn't include initialization of optional components, like TenantCache, DDQueue,
	// Tracker, TeamCollection. The components should call its own ::init methods.
	ACTOR static Future<Void> init(Reference<DataDistributor> self) {
		loop {
			TraceEvent("DDInitTakingMoveKeysLock", self->ddId).log();
			wait(self->takeMoveKeysLock());
			TraceEvent("DDInitTookMoveKeysLock", self->ddId).log();

			wait(self->loadDatabaseConfiguration());
			self->initDcInfo();
			TraceEvent("DDInitGotConfiguration", self->ddId)
			    .setMaxFieldLength(-1)
			    .detail("Conf", self->configuration.toString());

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

			if (self->initData->mode && self->context->isDDEnabled()) {
				// mode may be set true by system operator using fdbcli and isDDEnabled() set to true
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

			wait(self->waitDataDistributorEnabled());
			TraceEvent("DataDistributionEnabled").log();
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

		state int shard = 0;
		for (; shard < self->initData->shards.size() - 1; shard++) {
			const DDShardInfo& iShard = self->initData->shards[shard];
			KeyRangeRef keys = KeyRangeRef(iShard.key, self->initData->shards[shard + 1].key);

			self->shardsAffectedByTeamFailure->defineShard(keys);
			std::vector<ShardsAffectedByTeamFailure::Team> teams;
			teams.push_back(ShardsAffectedByTeamFailure::Team(iShard.primarySrc, true));
			if (self->configuration.usableRegions > 1) {
				teams.push_back(ShardsAffectedByTeamFailure::Team(iShard.remoteSrc, false));
			}
			if (traceShard) {
				TraceEvent(SevDebug, "DDInitShard", self->ddId)
				    .detail("Keys", keys)
				    .detail("PrimarySrc", describe(iShard.primarySrc))
				    .detail("RemoteSrc", describe(iShard.remoteSrc))
				    .detail("PrimaryDest", describe(iShard.primaryDest))
				    .detail("RemoteDest", describe(iShard.remoteDest))
				    .detail("SrcID", iShard.srcId)
				    .detail("DestID", iShard.destId);
			}

			self->shardsAffectedByTeamFailure->moveShard(keys, teams);
			if (iShard.hasDest && iShard.destId == anonymousShardId) {
				// This shard is already in flight.  Ideally we should use dest in ShardsAffectedByTeamFailure and
				// generate a dataDistributionRelocator directly in DataDistributionQueue to track it, but it's
				// easier to just (with low priority) schedule it for movement.
				bool unhealthy = iShard.primarySrc.size() != self->configuration.storageTeamSize;
				if (!unhealthy && self->configuration.usableRegions > 1) {
					unhealthy = iShard.remoteSrc.size() != self->configuration.storageTeamSize;
				}
				self->relocationProducer.send(
				    RelocateShard(keys,
				                  unhealthy ? DataMovementReason::TEAM_UNHEALTHY : DataMovementReason::RECOVER_MOVE,
				                  RelocateReason::OTHER));
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
			if (meta.ranges.empty()) {
				TraceEvent(SevInfo, "EmptyDataMoveRange", self->ddId).detail("DataMoveMetaData", meta.toString());
				continue;
			}
			if (it.value()->isCancelled() || (it.value()->valid && !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA)) {
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

	Future<Void> pollMoveKeysLock() { return txnProcessor->pollMoveKeysLock(lock, context->ddEnabledState.get()); }

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
};

ACTOR Future<Void> resumeAuditStorage(Reference<DataDistributor> self, AuditStorageState auditStates);
ACTOR Future<Void> auditStorage(Reference<DataDistributor> self, TriggerAuditRequest req);
ACTOR Future<Void> loadAndDispatchAuditRange(Reference<DataDistributor> self,
                                             std::shared_ptr<DDAudit> audit,
                                             KeyRange range);
ACTOR Future<Void> scheduleAuditForRange(Reference<DataDistributor> self,
                                         std::shared_ptr<DDAudit> audit,
                                         KeyRange range);
ACTOR Future<Void> doAuditOnStorageServer(Reference<DataDistributor> self,
                                          std::shared_ptr<DDAudit> audit,
                                          StorageServerInterface ssi,
                                          AuditStorageRequest req);

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

// Runs the data distribution algorithm for FDB, including the DD Queue, DD tracker, and DD team collection
ACTOR Future<Void> dataDistribution(Reference<DataDistributor> self,
                                    PromiseStream<GetMetricsListRequest> getShardMetricsList) {
	// Declare at beginning because shardTracker is implicit non-copiable. It has to be initialized later
	state DataDistributionTracker shardTracker;

	state Database cx = openDBOnServer(self->dbInfo, TaskPriority::DataDistributionLaunch, LockAware::True);
	cx->locationCacheSize = SERVER_KNOBS->DD_LOCATION_CACHE_SIZE;
	self->txnProcessor = Reference<IDDTxnProcessor>(new DDTxnProcessor(cx));

	// cx->setOption( FDBDatabaseOptions::LOCATION_CACHE_SIZE, StringRef((uint8_t*)
	// &SERVER_KNOBS->DD_LOCATION_CACHE_SIZE, 8) ); ASSERT( cx->locationCacheSize ==
	// SERVER_KNOBS->DD_LOCATION_CACHE_SIZE
	// );

	// wait(debugCheckCoalescing(cx));
	// FIXME: wrap the bootstrap process into class DataDistributor
	state Reference<DDTeamCollection> primaryTeamCollection;
	state Reference<DDTeamCollection> remoteTeamCollection;
	state bool trackerCancelled;
	loop {
		trackerCancelled = false;
		self->initialized = Promise<Void>();

		// Stored outside of data distribution tracker to avoid slow tasks
		// when tracker is cancelled
		state KeyRangeMap<ShardTrackedData> shards;
		state Promise<UID> removeFailedServer;
		try {
			wait(DataDistributor::init(self));

			// When/If this assertion fails, Evan owes Ben a pat on the back for his foresight
			ASSERT(self->configuration.storageTeamSize > 0);

			for (const auto& auditState : self->initData->auditStates) {
				TraceEvent("ResumingAuditStorage", self->ddId).detail("AuditID", auditState.id);
				self->addActor.send(resumeAuditStorage(self, auditState));
			}

			state PromiseStream<Promise<int64_t>> getAverageShardBytes;
			state PromiseStream<Promise<int>> getUnhealthyRelocationCount;
			state PromiseStream<GetMetricsRequest> getShardMetrics;
			state PromiseStream<GetTopKMetricsRequest> getTopKShardMetrics;
			state Reference<AsyncVar<bool>> processingUnhealthy(new AsyncVar<bool>(false));
			state Reference<AsyncVar<bool>> processingWiggle(new AsyncVar<bool>(false));

			if (SERVER_KNOBS->DD_TENANT_AWARENESS_ENABLED || SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
				self->ddTenantCache = makeReference<TenantCache>(cx, self->ddId);
				wait(self->ddTenantCache.get()->build());
			}

			self->shardsAffectedByTeamFailure = makeReference<ShardsAffectedByTeamFailure>();
			self->physicalShardCollection = makeReference<PhysicalShardCollection>(self->txnProcessor);
			wait(self->resumeRelocations());

			std::vector<TeamCollectionInterface> tcis; // primary and remote region interface
			Reference<AsyncVar<bool>> anyZeroHealthyTeams; // true if primary or remote has zero healthy team
			std::vector<Reference<AsyncVar<bool>>> zeroHealthyTeams; // primary and remote

			tcis.push_back(TeamCollectionInterface());
			zeroHealthyTeams.push_back(makeReference<AsyncVar<bool>>(true));
			int replicaSize = self->configuration.storageTeamSize;

			std::vector<Future<Void>> actors; // the container of ACTORs
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

			shardTracker.init(
			    DataDistributionTrackerInitParams{ .db = self->txnProcessor,
			                                       .distributorId = self->ddId,
			                                       .readyToStart = self->initialized,
			                                       .output = self->relocationProducer,
			                                       .shardsAffectedByTeamFailure = self->shardsAffectedByTeamFailure,
			                                       .physicalShardCollection = self->physicalShardCollection,
			                                       .anyZeroHealthyTeams = anyZeroHealthyTeams,
			                                       .shards = &shards,
			                                       .trackerCancelled = &trackerCancelled,
			                                       .ddTenantCache = self->ddTenantCache });
			actors.push_back(reportErrorsExcept(shardTracker.run(self->initData,
			                                                     getShardMetrics.getFuture(),
			                                                     getTopKShardMetrics.getFuture(),
			                                                     getShardMetricsList.getFuture(),
			                                                     getAverageShardBytes.getFuture()),
			                                    "DDTracker",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));

			actors.push_back(reportErrorsExcept(dataDistributionQueue(self->txnProcessor,
			                                                          self->relocationProducer,
			                                                          self->relocationConsumer.getFuture(),
			                                                          getShardMetrics,
			                                                          getTopKShardMetrics,
			                                                          processingUnhealthy,
			                                                          processingWiggle,
			                                                          tcis,
			                                                          self->shardsAffectedByTeamFailure,
			                                                          self->physicalShardCollection,
			                                                          self->lock,
			                                                          getAverageShardBytes,
			                                                          getUnhealthyRelocationCount.getFuture(),
			                                                          self->ddId,
			                                                          replicaSize,
			                                                          self->configuration.storageTeamSize,
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
			primaryTeamCollection = makeReference<DDTeamCollection>(DDTeamCollectionInitParams{
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
			    getAverageShardBytes });
			teamCollectionsPtrs.push_back(primaryTeamCollection.getPtr());
			auto recruitStorage = IAsyncListener<RequestStream<RecruitStorageRequest>>::create(
			    self->dbInfo, [](auto const& info) { return info.clusterInterface.recruitStorage; });
			if (self->configuration.usableRegions > 1) {
				remoteTeamCollection = makeReference<DDTeamCollection>(
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
				                                getAverageShardBytes });
				teamCollectionsPtrs.push_back(remoteTeamCollection.getPtr());
				remoteTeamCollection->teamCollections = teamCollectionsPtrs;
				actors.push_back(reportErrorsExcept(DDTeamCollection::run(remoteTeamCollection,
				                                                          self->initData,
				                                                          tcis[1],
				                                                          recruitStorage,
				                                                          *self->context->ddEnabledState.get()),
				                                    "DDTeamCollectionSecondary",
				                                    self->ddId,
				                                    &normalDDQueueErrors()));
				actors.push_back(DDTeamCollection::printSnapshotTeamsInfo(remoteTeamCollection));
			}
			primaryTeamCollection->teamCollections = teamCollectionsPtrs;
			self->teamCollection = primaryTeamCollection.getPtr();
			actors.push_back(reportErrorsExcept(DDTeamCollection::run(primaryTeamCollection,
			                                                          self->initData,
			                                                          tcis[0],
			                                                          recruitStorage,
			                                                          *self->context->ddEnabledState.get()),
			                                    "DDTeamCollectionPrimary",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));

			actors.push_back(DDTeamCollection::printSnapshotTeamsInfo(primaryTeamCollection));
			actors.push_back(yieldPromiseStream(self->relocationProducer.getFuture(), self->relocationConsumer));
			if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
				actors.push_back(monitorPhysicalShardStatus(self->physicalShardCollection));
			}

			wait(waitForAll(actors));
			return Void();
		} catch (Error& e) {
			trackerCancelled = true;
			state Error err = e;
			TraceEvent("DataDistributorDestroyTeamCollections").error(e);
			state std::vector<UID> teamForDroppedRange;
			if (removeFailedServer.getFuture().isReady() && !removeFailedServer.getFuture().isError()) {
				// Choose a random healthy team to host the to-be-dropped range.
				const UID serverID = removeFailedServer.getFuture().get();
				std::vector<UID> pTeam = primaryTeamCollection->getRandomHealthyTeam(serverID);
				teamForDroppedRange.insert(teamForDroppedRange.end(), pTeam.begin(), pTeam.end());
				if (self->configuration.usableRegions > 1) {
					std::vector<UID> rTeam = remoteTeamCollection->getRandomHealthyTeam(serverID);
					teamForDroppedRange.insert(teamForDroppedRange.end(), rTeam.begin(), rTeam.end());
				}
			}
			self->teamCollection = nullptr;
			primaryTeamCollection = Reference<DDTeamCollection>();
			remoteTeamCollection = Reference<DDTeamCollection>();
			if (err.code() == error_code_actor_cancelled) {
				// When cancelled, we cannot clear asyncronously because
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
			TraceEvent("DataDistributorTeamCollectionsDestroyed").error(err);
			if (removeFailedServer.getFuture().isReady() && !removeFailedServer.getFuture().isError()) {
				TraceEvent("RemoveFailedServer", removeFailedServer.getFuture().get()).error(err);
				wait(self->removeKeysFromFailedServer(removeFailedServer.getFuture().get(), teamForDroppedRange));
				wait(self->removeStorageServer(removeFailedServer.getFuture().get()));
			} else {
				if (err.code() != error_code_movekeys_conflict) {
					throw err;
				}

				bool ddEnabled = wait(self->isDataDistributionEnabled());
				TraceEvent("DataDistributionMoveKeysConflict").error(err).detail("DataDistributionEnabled", ddEnabled);
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
		    .detail("Peer", stream.getEndpoint().getPrimaryAddress());
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
						if (result.count(server.address())) {
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
					TraceEvent(SevError, "MissingTlogWorkerInterface").detail("TlogAddress", tlog.address());
					throw snap_tlog_failed();
				}
				if (result.count(tlog.address())) {
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
					if (result.count(primary)) {
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
					if (result.count(processAddress))
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
		auto const coordFaultTolerance = std::min<int>(std::max<int>(0, coordSnapReqs.size() / 2 - 1),
		                                               SERVER_KNOBS->MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE);
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
	if (!ddEnabledState->setDDEnabled(false, snapReq.snapUID)) {
		// disable DD before doing snapCreate, if previous snap req has already disabled DD then this operation fails
		// here
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
			bool success = ddEnabledState->setDDEnabled(true, snapReq.snapUID);
			ASSERT(success);
			throw e;
		}
	}
	// enable DD should always succeed
	bool success = ddEnabledState->setDDEnabled(true, snapReq.snapUID);
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

ACTOR Future<Void> resumeAuditStorage(Reference<DataDistributor> self, AuditStorageState auditState) {
	if (auditState.getPhase() == AuditPhase::Complete) {
		return Void();
	}

	state std::shared_ptr<DDAudit> audit =
	    std::make_shared<DDAudit>(auditState.id, auditState.range, auditState.getType());
	self->audits[auditState.getType()][audit->id] = audit;
	audit->actors.add(loadAndDispatchAuditRange(self, audit, auditState.range));
	TraceEvent(SevDebug, "DDResumAuditStorageBegin", self->ddId)
	    .detail("AuditID", audit->id)
	    .detail("Range", auditState.range)
	    .detail("AuditType", auditState.type);

	try {
		wait(audit->actors.getResult());
		TraceEvent(SevDebug, "DDResumeAuditStorageEnd", self->ddId)
		    .detail("AuditID", audit->id)
		    .detail("Range", auditState.range)
		    .detail("AuditType", auditState.type);
		auditState.setPhase(AuditPhase::Complete);
		wait(persistAuditState(self->txnProcessor->context(), auditState));
	} catch (Error& e) {
		TraceEvent(SevInfo, "DDResumeAuditStorageOperationError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->id)
		    .detail("Range", auditState.range)
		    .detail("AuditType", auditState.type);
		if (e.code() == error_code_audit_storage_error) {
			auditState.setPhase(AuditPhase::Error);
			wait(persistAuditState(self->txnProcessor->context(), auditState));
		} else if (e.code() != error_code_actor_cancelled) {
			wait(delay(30));
			self->addActor.send(resumeAuditStorage(self, auditState));
		}
	}
	self->audits[auditState.getType()].erase(audit->id);

	return Void();
}

ACTOR Future<Void> auditStorage(Reference<DataDistributor> self, TriggerAuditRequest req) {
	state std::shared_ptr<DDAudit> audit;
	// TODO: store AuditStorageState in DDAudit.
	state AuditStorageState auditState;
	auditState.setType(req.getType());

	try {
		try {
			auto it = self->audits.find(req.getType());
			if (it != self->audits.end() && !it->second.empty()) {
				for (auto& [id, currentAudit] : it->second) {
					if (currentAudit->range.contains(req.range)) {
						auditState.id = currentAudit->id;
						auditState.range = currentAudit->range;
						auditState.setPhase(AuditPhase::Running);
						audit = currentAudit;
						break;
					}
				}
				if (audit == nullptr) {
					req.reply.sendError(audit_storage_exceeded_request_limit());
					return Void();
				}
			} else {
				auditState.range = req.range;
				auditState.setPhase(AuditPhase::Running);
				UID auditId = wait(persistNewAuditState(self->txnProcessor->context(), auditState));
				auditState.id = auditId;
				audit = std::make_shared<DDAudit>(auditId, req.range, req.getType());
				self->audits[req.getType()][audit->id] = audit;
				audit->actors.add(loadAndDispatchAuditRange(self, audit, req.range));
				// Simulate restarting.
				if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
					// throw operation_failed();
				}
			}

			ASSERT(audit != nullptr);
			TraceEvent(SevInfo, "DDAuditStorageScheduled", self->ddId)
			    .detail("AuditID", audit->id)
			    .detail("Range", req.range)
			    .detail("AuditType", req.type);

			if (req.async && !req.reply.isSet()) {
				req.reply.send(audit->id);
			}

			wait(audit->actors.getResult());
			TraceEvent(SevInfo, "DDAuditStorageSuccess", self->ddId)
			    .detail("AuditID", audit->id)
			    .detail("Range", req.range)
			    .detail("AuditType", req.type);
			auditState.setPhase(AuditPhase::Complete);
			wait(persistAuditState(self->txnProcessor->context(), auditState));
			if (!req.async && !req.reply.isSet()) {
				req.reply.send(audit->id);
			}
		} catch (Error& e) {
			TraceEvent(SevInfo, "DDAuditStorageError", self->ddId)
			    .errorUnsuppressed(e)
			    .detail("AuditID", (audit == nullptr ? UID() : audit->id))
			    .detail("Range", req.range)
			    .detail("AuditType", req.type);
			state Error err(e);
			if (e.code() == error_code_audit_storage_error) {
				auditState.setPhase(AuditPhase::Error);
				wait(persistAuditState(self->txnProcessor->context(), auditState));
			}
			throw err;
		}
	} catch (Error& e) {
		if (!req.async && !req.reply.isSet()) {
			if (e.code() == error_code_audit_storage_error) {
				req.reply.sendError(e);
			} else {
				req.reply.sendError(audit_storage_failed());
			}
		}
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
	}
	self->audits[auditState.getType()].erase(auditState.id);

	return Void();
}

ACTOR Future<Void> loadAndDispatchAuditRange(Reference<DataDistributor> self,
                                             std::shared_ptr<DDAudit> audit,
                                             KeyRange range) {
	TraceEvent(SevInfo, "DDLoadAndDispatchAuditRangeBegin", self->ddId)
	    .detail("AuditID", audit->id)
	    .detail("Range", range)
	    .detail("AuditType", audit->type);
	state Key begin = range.begin;
	state KeyRange currentRange = range;

	while (begin < range.end) {
		currentRange = KeyRangeRef(begin, range.end);
		std::vector<AuditStorageState> auditStates =
		    wait(getAuditStateForRange(self->txnProcessor->context(), audit->id, currentRange));
		ASSERT(!auditStates.empty());
		begin = auditStates.back().range.end;
		for (const auto& auditState : auditStates) {
			const AuditPhase phase = auditState.getPhase();
			audit->auditMap.insert(auditState.range, phase);
			if (phase == AuditPhase::Complete) {
				continue;
			} else if (phase == AuditPhase::Error) {
				throw audit_storage_error();
			} else {
				audit->actors.add(scheduleAuditForRange(self, audit, auditState.range));
			}
		}
		wait(delay(0.1));
	}

	return Void();
}

ACTOR Future<Void> scheduleAuditForRange(Reference<DataDistributor> self,
                                         std::shared_ptr<DDAudit> audit,
                                         KeyRange range) {
	TraceEvent(SevInfo, "DDScheduleAuditForRangeBegin", self->ddId)
	    .detail("AuditID", audit->id)
	    .detail("Range", range)
	    .detail("AuditType", audit->type);
	state Key begin = range.begin;
	state KeyRange currentRange = range;

	while (begin < range.end) {
		currentRange = KeyRangeRef(begin, range.end);
		try {
			state std::vector<IDDTxnProcessor::DDRangeLocations> rangeLocations =
			    wait(self->txnProcessor->getSourceServerInterfacesForRange(currentRange));

			state int i = 0;
			for (i = 0; i < rangeLocations.size(); ++i) {
				AuditStorageRequest req(audit->id, rangeLocations[i].range, audit->type);
				if (audit->type == AuditType::ValidateHA && rangeLocations[i].servers.size() >= 2) {
					auto it = rangeLocations[i].servers.begin();
					const int idx = deterministicRandom()->randomInt(0, it->second.size());
					StorageServerInterface& targetServer = it->second[idx];
					++it;
					for (; it != rangeLocations[i].servers.end(); ++it) {
						const int idx = deterministicRandom()->randomInt(0, it->second.size());
						req.targetServers.push_back(it->second[idx].id());
					}
					audit->actors.add(doAuditOnStorageServer(self, audit, targetServer, req));
				}
				begin = rangeLocations[i].range.end;
				wait(delay(0.01));
			}
		} catch (Error& e) {
			TraceEvent(SevInfo, "DDScheduleAuditRangeError", self->ddId)
			    .errorUnsuppressed(e)
			    .detail("AuditID", audit->id)
			    .detail("Range", range);
			throw e;
		}
	}

	return Void();
}

ACTOR Future<Void> doAuditOnStorageServer(Reference<DataDistributor> self,
                                          std::shared_ptr<DDAudit> audit,
                                          StorageServerInterface ssi,
                                          AuditStorageRequest req) {
	TraceEvent(SevDebug, "DDDoAuditOnStorageServerBegin", self->ddId)
	    .detail("AuditID", req.id)
	    .detail("Range", req.range)
	    .detail("AuditType", req.type)
	    .detail("StorageServer", ssi.toString())
	    .detail("TargetServers", describe(req.targetServers));

	try {
		audit->auditMap.insert(req.range, AuditPhase::Running);
		ErrorOr<AuditStorageState> vResult = wait(ssi.auditStorage.getReplyUnlessFailedFor(
		    req, /*sustainedFailureDuration=*/2.0, /*sustainedFailureSlope=*/0));
		if (vResult.isError()) {
			throw vResult.getError();
		}
		TraceEvent(SevDebug, "DDDoAuditOnStorageServerEnd", self->ddId)
		    .detail("AuditID", req.id)
		    .detail("Range", req.range)
		    .detail("AuditType", req.type)
		    .detail("StorageServer", ssi.toString())
		    .detail("TargetServers", describe(req.targetServers));
	} catch (Error& e) {
		TraceEvent(SevInfo, "DDDoAuditOnStorageServerError", req.id)
		    .errorUnsuppressed(e)
		    .detail("AuditID", req.id)
		    .detail("Range", req.range)
		    .detail("StorageServer", ssi.toString())
		    .detail("TargetServers", describe(req.targetServers));
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_audit_storage_error) {
			throw e;
		} else {
			audit->auditMap.insert(req.range, AuditPhase::Failed);
			audit->actors.add(loadAndDispatchAuditRange(self, audit, req.range));
		}
	}

	return Void();
}

ACTOR Future<Void> dataDistributor(DataDistributorInterface di, Reference<AsyncVar<ServerDBInfo> const> db) {
	state Reference<DDSharedContext> context(new DDSharedContext(di.id()));
	state Reference<DataDistributor> self(new DataDistributor(db, di.id(), context));
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	state PromiseStream<GetMetricsListRequest> getShardMetricsList;
	state Database cx = openDBOnServer(db, TaskPriority::DefaultDelay, LockAware::True);
	state ActorCollection actors(false);
	state std::map<UID, DistributorSnapRequest> ddSnapReqMap;
	state std::map<UID, ErrorOr<Void>> ddSnapReqResultMap;
	self->addActor.send(actors.getResult());
	self->addActor.send(traceRole(Role::DATA_DISTRIBUTOR, di.id()));

	try {
		TraceEvent("DataDistributorRunning", di.id());
		self->addActor.send(waitFailureServer(di.waitFailure.getFuture()));
		self->addActor.send(cacheServerWatcher(&cx));
		state Future<Void> distributor = reportErrorsExcept(
		    dataDistribution(self, getShardMetricsList), "DataDistribution", di.id(), &normalDataDistributorErrors());

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
				if (ddSnapReqResultMap.count(snapUID)) {
					CODE_PROBE(true,
					           "Data distributor received a duplicate finished snapshot request",
					           probe::decoration::rare);
					auto result = ddSnapReqResultMap[snapUID];
					result.isError() ? snapReq.reply.sendError(result.getError()) : snapReq.reply.send(result.get());
					TraceEvent("RetryFinishedDistributorSnapRequest")
					    .detail("SnapUID", snapUID)
					    .detail("Result", result.isError() ? result.getError().code() : 0);
				} else if (ddSnapReqMap.count(snapReq.snapUID)) {
					CODE_PROBE(true, "Data distributor received a duplicate ongoing snapshot request");
					TraceEvent("RetryOngoingDistributorSnapRequest").detail("SnapUID", snapUID);
					ASSERT(snapReq.snapPayload == ddSnapReqMap[snapUID].snapPayload);
					ddSnapReqMap[snapUID] = snapReq;
				} else {
					ddSnapReqMap[snapUID] = snapReq;
					actors.add(ddSnapCreate(
					    snapReq, db, self->context->ddEnabledState.get(), &ddSnapReqMap, &ddSnapReqResultMap));
					auto* ddSnapReqResultMapPtr = &ddSnapReqResultMap;
					actors.add(fmap(
					    [ddSnapReqResultMapPtr, snapUID](Void _) {
						    ddSnapReqResultMapPtr->erase(snapUID);
						    return Void();
					    },
					    delay(SERVER_KNOBS->SNAP_MINIMUM_TIME_GAP)));
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
				actors.add(auditStorage(self, req));
			}
			when(TenantsOverStorageQuotaRequest req = waitNext(di.tenantsOverStorageQuota.getFuture())) {
				req.reply.send(getTenantsOverStorageQuota(self));
			}
		}
	} catch (Error& err) {
		if (normalDataDistributorErrors().count(err.code()) == 0) {
			TraceEvent("DataDistributorError", di.id()).errorUnsuppressed(err);
			throw err;
		}
		TraceEvent("DataDistributorDied", di.id()).errorUnsuppressed(err);
	}

	return Void();
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
	state Reference<DataDistributor> self(new DataDistributor(dbInfo, UID(), context));

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
