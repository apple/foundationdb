/*
 * DataDistribution.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_DATA_DISTRIBUTION_ACTOR_G_H)
#define FDBSERVER_DATA_DISTRIBUTION_ACTOR_G_H
#include "fdbserver/DataDistribution.actor.g.h"
#elif !defined(FDBSERVER_DATA_DISTRIBUTION_ACTOR_H)
#define FDBSERVER_DATA_DISTRIBUTION_ACTOR_H

#include "fdbclient/BulkLoading.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/TenantCache.h"
#include "fdbserver/TCInfo.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"
#include "fdbclient/StorageWiggleMetrics.actor.h"
#include "fdbclient/DataDistributionConfig.actor.h"
#include <boost/heap/policies.hpp>
#include <boost/heap/skew_heap.hpp>
#include "flow/actorcompiler.h" // This must be the last #include.

/////////////////////////////// Data //////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Data
#endif

// SOMEDAY: whether it's possible to combine RelocateReason and DataMovementReason together?
// RelocateReason to DataMovementReason is one-to-N mapping
class RelocateReason {
public:
	enum Value : int8_t {
		OTHER = 0,
		REBALANCE_DISK,
		REBALANCE_READ,
		REBALANCE_WRITE,
		MERGE_SHARD,
		SIZE_SPLIT,
		WRITE_SPLIT,
		TENANT_SPLIT,
		__COUNT
	};
	RelocateReason(Value v) : value(v) { ASSERT(value != __COUNT); }
	explicit RelocateReason(int v) : value((Value)v) { ASSERT(value != __COUNT); }
	std::string toString() const {
		switch (value) {
		case OTHER:
			return "Other";
		case REBALANCE_DISK:
			return "RebalanceDisk";
		case REBALANCE_READ:
			return "RebalanceRead";
		case REBALANCE_WRITE:
			return "RebalanceWrite";
		case MERGE_SHARD:
			return "MergeShard";
		case SIZE_SPLIT:
			return "SizeSplit";
		case WRITE_SPLIT:
			return "WriteSplit";
		case TENANT_SPLIT:
			return "TenantSplit";
		case __COUNT:
			ASSERT(false);
		}
		return "";
	}
	operator int() const { return (int)value; }
	constexpr static int8_t typeCount() { return (int)__COUNT; }
	bool operator<(const RelocateReason& reason) { return (int)value < (int)reason.value; }

private:
	Value value;
};

extern int dataMovementPriority(DataMovementReason moveReason);
extern DataMovementReason priorityToDataMovementReason(int priority);

DataMoveType getDataMoveTypeFromDataMoveId(const UID& dataMoveId);

struct DDShardInfo;

// Represents a data move in DD.
struct DataMove {
	DataMove() : meta(DataMoveMetaData()), restore(false), valid(false), cancelled(false) {}
	explicit DataMove(DataMoveMetaData meta, bool restore)
	  : meta(std::move(meta)), restore(restore), valid(true), cancelled(meta.getPhase() == DataMoveMetaData::Deleting) {
	}

	// Checks if the DataMove is consistent with the shard.
	void validateShard(const DDShardInfo& shard, KeyRangeRef range, int priority = SERVER_KNOBS->PRIORITY_RECOVER_MOVE);

	bool isCancelled() const { return this->cancelled; }

	const DataMoveMetaData meta;
	bool restore; // The data move is scheduled by a previous DD, and is being recovered now.
	bool valid; // The data move data is integral.
	bool cancelled; // The data move has been cancelled.
	std::vector<UID> primarySrc;
	std::vector<UID> remoteSrc;
	std::vector<UID> primaryDest;
	std::vector<UID> remoteDest;
};

struct RelocateShard {
	KeyRange keys;
	int priority;
	bool cancelled; // The data move should be cancelled.
	std::shared_ptr<DataMove> dataMove; // Not null if this is a restored data move.
	UID dataMoveId;
	RelocateReason reason;
	DataMovementReason moveReason;

	UID traceId; // track the lifetime of this relocate shard

	// Initialization when define is a better practice. We should avoid assignment of member after definition.
	// static RelocateShard emptyRelocateShard() { return {}; }

	RelocateShard(KeyRange const& keys, DataMovementReason moveReason, RelocateReason reason, UID traceId = UID())
	  : keys(keys), priority(dataMovementPriority(moveReason)), cancelled(false), dataMoveId(anonymousShardId),
	    reason(reason), moveReason(moveReason), traceId(traceId) {}

	RelocateShard(KeyRange const& keys, int priority, RelocateReason reason, UID traceId = UID())
	  : keys(keys), priority(priority), cancelled(false), dataMoveId(anonymousShardId), reason(reason),
	    moveReason(priorityToDataMovementReason(priority)), traceId(traceId) {}

	bool isRestore() const { return this->dataMove != nullptr; }

	void setParentRange(KeyRange const& parent);
	Optional<KeyRange> getParentRange() const;

private:
	// If this rs comes from a splitting, parent range is the original range.
	Optional<KeyRange> parent_range;

	RelocateShard()
	  : priority(0), cancelled(false), dataMoveId(anonymousShardId), reason(RelocateReason::OTHER),
	    moveReason(DataMovementReason::INVALID) {}
};

struct GetMetricsRequest {
	KeyRange keys;
	Promise<StorageMetrics> reply;
	GetMetricsRequest() {}
	GetMetricsRequest(KeyRange const& keys) : keys(keys) {}
};

struct GetTopKMetricsReply {
	struct KeyRangeStorageMetrics {
		KeyRange range;
		StorageMetrics metrics;
		KeyRangeStorageMetrics() = default;
		KeyRangeStorageMetrics(const KeyRange& range, const StorageMetrics& s) : range(range), metrics(s) {}
	};
	std::vector<KeyRangeStorageMetrics> shardMetrics;
	double minReadLoad = -1, maxReadLoad = -1;
	GetTopKMetricsReply() {}
	GetTopKMetricsReply(std::vector<KeyRangeStorageMetrics> const& m, double minReadLoad, double maxReadLoad)
	  : shardMetrics(m), minReadLoad(minReadLoad), maxReadLoad(maxReadLoad) {}
};

struct GetTopKMetricsRequest {
private:
	int topK = 1; // default only return the top 1 shard based on the GetTopKMetricsRequest::compare function
public:
	std::vector<KeyRange> keys;
	Promise<GetTopKMetricsReply> reply; // topK storage metrics
	double maxReadLoadPerKSecond = 0, minReadLoadPerKSecond = 0; // all returned shards won't exceed this read load

	GetTopKMetricsRequest() {}
	GetTopKMetricsRequest(std::vector<KeyRange> const& keys,
	                      int topK = 1,
	                      double maxReadLoadPerKSecond = std::numeric_limits<double>::max(),
	                      double minReadLoadPerKSecond = 0)
	  : topK(topK), keys(keys), maxReadLoadPerKSecond(maxReadLoadPerKSecond),
	    minReadLoadPerKSecond(minReadLoadPerKSecond) {
		ASSERT_GE(topK, 1);
	}

	int getTopK() const { return topK; };

	// Return true if a.score > b.score, return the largest topK in keys
	static bool compare(const GetTopKMetricsReply::KeyRangeStorageMetrics& a,
	                    const GetTopKMetricsReply::KeyRangeStorageMetrics& b) {
		return compareByReadDensity(a, b);
	}

private:
	// larger read density means higher score
	static bool compareByReadDensity(const GetTopKMetricsReply::KeyRangeStorageMetrics& a,
	                                 const GetTopKMetricsReply::KeyRangeStorageMetrics& b) {
		return a.metrics.readLoadKSecond() / std::max(a.metrics.bytes * 1.0, 1.0) >
		       b.metrics.readLoadKSecond() / std::max(b.metrics.bytes * 1.0, 1.0);
	}
};

struct GetMetricsListRequest {
	KeyRange keys;
	int shardLimit;
	Promise<Standalone<VectorRef<DDMetricsRef>>> reply;

	GetMetricsListRequest() {}
	GetMetricsListRequest(KeyRange const& keys, const int shardLimit) : keys(keys), shardLimit(shardLimit) {}
};

struct BulkLoadShardRequest {
	BulkLoadState bulkLoadState;

	BulkLoadShardRequest() {}
	BulkLoadShardRequest(BulkLoadState const& bulkLoadState) : bulkLoadState(bulkLoadState) {}
};

// PhysicalShardCollection maintains physical shard concepts in data distribution
// A physical shard contains one or multiple shards (key range)
// PhysicalShardCollection is responsible for creation and maintenance of physical shards (including metrics)
// For multiple DCs, PhysicalShardCollection maintains a pair of primary team and remote team
// A primary team and a remote team shares a physical shard
// For each shard (key-range) move, PhysicalShardCollection decides which physical shard and corresponding team(s) to
// move The current design of PhysicalShardCollection assumes that there exists at most two teamCollections
// TODO: unit test needed
FDB_BOOLEAN_PARAM(InAnonymousPhysicalShard);
FDB_BOOLEAN_PARAM(PhysicalShardHasMoreThanKeyRange);
FDB_BOOLEAN_PARAM(InOverSizePhysicalShard);
FDB_BOOLEAN_PARAM(PhysicalShardAvailable);
FDB_BOOLEAN_PARAM(MoveKeyRangeOutPhysicalShard);

struct ShardMetrics {
	StorageMetrics metrics;
	double lastLowBandwidthStartTime;
	int shardCount; // number of smaller shards whose metrics are aggregated in the ShardMetrics

	bool operator==(ShardMetrics const& rhs) const {
		return metrics == rhs.metrics && lastLowBandwidthStartTime == rhs.lastLowBandwidthStartTime &&
		       shardCount == rhs.shardCount;
	}

	ShardMetrics(StorageMetrics const& metrics, double lastLowBandwidthStartTime, int shardCount)
	  : metrics(metrics), lastLowBandwidthStartTime(lastLowBandwidthStartTime), shardCount(shardCount) {}
};

struct ShardTrackedData {
	Future<Void> trackShard;
	Future<Void> trackBytes;
	Future<Void> trackUsableRegion;
	Reference<AsyncVar<Optional<ShardMetrics>>> stats;
};

class PhysicalShardCollection : public ReferenceCounted<PhysicalShardCollection> {
public:
	PhysicalShardCollection() : lastTransitionStartTime(now()), requireTransition(false) {}
	PhysicalShardCollection(Reference<IDDTxnProcessor> db)
	  : txnProcessor(db), lastTransitionStartTime(now()), requireTransition(false) {}

	enum class PhysicalShardCreationTime { DDInit, DDRelocator };

	struct PhysicalShard {
		PhysicalShard() : id(UID().first()) {}

		PhysicalShard(Reference<IDDTxnProcessor> txnProcessor,
		              uint64_t id,
		              StorageMetrics const& metrics,
		              std::vector<ShardsAffectedByTeamFailure::Team> teams,
		              PhysicalShardCreationTime whenCreated)
		  : txnProcessor(txnProcessor), id(id), metrics(metrics),
		    stats(makeReference<AsyncVar<Optional<StorageMetrics>>>()), teams(teams), whenCreated(whenCreated) {}

		// Adds `newRange` to this physical shard and starts monitoring the shard.
		void addRange(const KeyRange& newRange);

		// Removes `outRange` from this physical shard and updates monitored shards.
		void removeRange(const KeyRange& outRange);

		std::string toString() const { return fmt::format("{}", std::to_string(id)); }

		Reference<IDDTxnProcessor> txnProcessor;
		uint64_t id; // physical shard id (never changed)
		StorageMetrics metrics; // current metrics, updated by shardTracker
		// todo(zhewu): combine above metrics with stats. They are redundant.
		Reference<AsyncVar<Optional<StorageMetrics>>> stats; // Stats of this physical shard.
		std::vector<ShardsAffectedByTeamFailure::Team> teams; // which team owns this physical shard (never changed)
		PhysicalShardCreationTime whenCreated; // when this physical shard is created (never changed)

		struct RangeData {
			Future<Void> trackMetrics;
			Reference<AsyncVar<Optional<ShardMetrics>>> stats;
		};
		std::unordered_map<KeyRange, RangeData> rangeData;

	private:
		// Inserts a new key range into this physical shard. `newRange` must not exist in this shard already.
		void insertNewRangeData(const KeyRange& newRange);
	};

	// Generate a random physical shard ID, which is not UID().first() nor anonymousShardId.first()
	uint64_t generateNewPhysicalShardID(uint64_t debugID);

	// If the input team has any available physical shard, return an available physical shard from the input team and
	// not in `excludedPhysicalShards`. This method is used for two-step team selection The overall process has two
	// steps: Step 1: get a physical shard id given the input primary team Return a new physical shard id if the input
	// primary team is new or the team has no available physical shard checkPhysicalShardAvailable() defines whether a
	// physical shard is available
	Optional<uint64_t> trySelectAvailablePhysicalShardFor(ShardsAffectedByTeamFailure::Team team,
	                                                      StorageMetrics const& metrics,
	                                                      const std::unordered_set<uint64_t>& excludedPhysicalShards,
	                                                      uint64_t debugID);

	// Step 2: get a remote team which has the input physical shard.
	// Second field in the returned pair indicates whether this physical shard is available or not.
	// Return empty if no such remote team.
	// May return a problematic remote team, and re-selection is required for this case.
	std::pair<Optional<ShardsAffectedByTeamFailure::Team>, bool>
	tryGetAvailableRemoteTeamWith(uint64_t inputPhysicalShardID, StorageMetrics const& moveInMetrics, uint64_t debugID);
	// Invariant:
	// (1) If forceToUseNewPhysicalShard is set, use the bestTeams selected by getTeam(), and create a new physical
	// shard for the teams
	// (2) If forceToUseNewPhysicalShard is not set, use the primary team selected by getTeam()
	//     If there exists a remote team which has an available physical shard with the primary team
	//         Then, use the remote team. Note that the remote team may be unhealthy and the remote team
	//         may be one who issues the current data relocation.
	//         In this case, we set forceToUseNewPhysicalShard to use getTeam() to re-select the remote team
	//     Otherwise, use getTeam() to re-select the remote team

	// Create a physical shard when initializing PhysicalShardCollection
	void initPhysicalShardCollection(KeyRange keys,
	                                 std::vector<ShardsAffectedByTeamFailure::Team> selectedTeams,
	                                 uint64_t physicalShardID,
	                                 uint64_t debugID);

	// Create a physical shard when updating PhysicalShardCollection
	void updatePhysicalShardCollection(KeyRange keys,
	                                   bool isRestore,
	                                   std::vector<ShardsAffectedByTeamFailure::Team> selectedTeams,
	                                   uint64_t physicalShardID,
	                                   const StorageMetrics& metrics,
	                                   uint64_t debugID);

	// Update physicalShard metrics and return whether the keyRange needs to move out of its physical shard
	MoveKeyRangeOutPhysicalShard trackPhysicalShard(KeyRange keyRange,
	                                                StorageMetrics const& newMetrics,
	                                                StorageMetrics const& oldMetrics,
	                                                bool initWithNewMetrics);

	// Clean up empty physicalShard
	void cleanUpPhysicalShardCollection();

	// Log physicalShard
	void logPhysicalShardCollection();

	// Checks if a physical shard exists.
	bool physicalShardExists(uint64_t physicalShardID);

private:
	// Track physicalShard metrics by tracking keyRange metrics
	void updatePhysicalShardMetricsByKeyRange(KeyRange keyRange,
	                                          StorageMetrics const& newMetrics,
	                                          StorageMetrics const& oldMetrics,
	                                          bool initWithNewMetrics);

	// Check the input keyRange is in the anonymous physical shard
	InAnonymousPhysicalShard isInAnonymousPhysicalShard(KeyRange keyRange);

	// Check the input physicalShard has more keyRanges in addition to the input keyRange
	PhysicalShardHasMoreThanKeyRange whetherPhysicalShardHasMoreThanKeyRange(uint64_t physicalShardID,
	                                                                         KeyRange keyRange);

	// Check the input keyRange is in an oversize physical shard
	// This function returns true to enforce the keyRange to move out the physical shard
	// Note that if the physical shard only contains the keyRange, always return FALSE
	InOverSizePhysicalShard isInOverSizePhysicalShard(KeyRange keyRange);

	// Check whether the input physical shard is available
	// A physical shard is available if the current metric + moveInMetrics <= a threshold
	PhysicalShardAvailable checkPhysicalShardAvailable(uint64_t physicalShardID, StorageMetrics const& moveInMetrics);

	// Reduce the metrics of input physical shard by the input metrics
	void reduceMetricsForMoveOut(uint64_t physicalShardID, StorageMetrics const& metrics);

	// Add the input metrics to the metrics of input physical shard
	void increaseMetricsForMoveIn(uint64_t physicalShardID, StorageMetrics const& metrics);

	// In physicalShardCollection, add a physical shard initialized by the input parameters to the collection
	void insertPhysicalShardToCollection(uint64_t physicalShardID,
	                                     StorageMetrics const& metrics,
	                                     std::vector<ShardsAffectedByTeamFailure::Team> teams,
	                                     uint64_t debugID,
	                                     PhysicalShardCreationTime whenCreated);

	// In teamPhysicalShardIDs, add the input physical shard id to the input teams
	void updateTeamPhysicalShardIDsMap(uint64_t physicalShardID,
	                                   std::vector<ShardsAffectedByTeamFailure::Team> inputTeams,
	                                   uint64_t debugID);

	// In keyRangePhysicalShardIDMap, set the input physical shard id to the input key range
	void updatekeyRangePhysicalShardIDMap(KeyRange keyRange, uint64_t physicalShardID, uint64_t debugID);

	// Checks the consistency between the mapping of physical shards and key ranges.
	void checkKeyRangePhysicalShardMapping();

	// Return a string concatenating the input IDs interleaving with " "
	std::string convertIDsToString(std::set<uint64_t> ids);

	// Reset TransitionStartTime
	// Consider a system without concept of physicalShard
	// When restart, the system begins with a state where all keyRanges are in the anonymousShard
	// Our goal is to make all keyRanges are out of the anonymousShard
	// A keyRange moves out of the anonymousShard when the keyRange is triggered a data move
	// It is possible that a keyRange is cold and no data move is triggered on this keyRange for long time
	// In this case, we need to intentionally trigger data move on that keyRange
	// The minimal time span between two successive data move for this purpose is TransitionStartTime
	inline void resetLastTransitionStartTime() { // reset when a keyRange move is triggered for the transition
		lastTransitionStartTime = now();
		return;
	}

	// When DD restarts, it checks whether keyRange has anonymousShard
	// If yes, setTransitionCheck() is call to trigger the process of removing anonymousShard
	inline void setTransitionCheck() {
		if (requireTransition == true) {
			return;
		}
		requireTransition = true;
		TraceEvent("PhysicalShardSetTransitionCheck");
		return;
	}

	inline bool requireTransitionCheck() { return requireTransition; }

	Reference<IDDTxnProcessor> txnProcessor;

	// Core data structures
	// Physical shard instances indexed by physical shard id
	std::unordered_map<uint64_t, PhysicalShard> physicalShardInstances;
	// Indicate a key range belongs to which physical shard
	KeyRangeMap<uint64_t> keyRangePhysicalShardIDMap;
	// Indicate what physical shards owned by a team
	std::map<ShardsAffectedByTeamFailure::Team, std::set<uint64_t>> teamPhysicalShardIDs;
	bool requireTransition;
	double lastTransitionStartTime;
};

struct RebalanceStorageQueueRequest {
	UID serverId;
	std::vector<ShardsAffectedByTeamFailure::Team> teams;
	bool primary;

	RebalanceStorageQueueRequest() {}
	RebalanceStorageQueueRequest(UID serverId,
	                             const std::vector<ShardsAffectedByTeamFailure::Team>& teams,
	                             bool primary)
	  : serverId(serverId), teams(teams), primary(primary) {}
};

// DDShardInfo is so named to avoid link-time name collision with ShardInfo within the StorageServer
struct DDShardInfo {
	Key key;
	// all UID are sorted
	std::vector<UID> primarySrc;
	std::vector<UID> remoteSrc;
	std::vector<UID> primaryDest;
	std::vector<UID> remoteDest;
	bool hasDest;
	UID srcId;
	UID destId;

	explicit DDShardInfo(Key key) : key(key), hasDest(false) {}
	DDShardInfo(Key key, UID srcId, UID destId) : key(key), hasDest(false), srcId(srcId), destId(destId) {}
};

struct InitialDataDistribution : ReferenceCounted<InitialDataDistribution> {
	InitialDataDistribution()
	  : dataMoveMap(std::make_shared<DataMove>()),
	    userRangeConfig(makeReference<DDConfiguration::RangeConfigMapSnapshot>(allKeys.begin, allKeys.end)) {}

	// Read from dataDistributionModeKey. Whether DD is disabled. DD can be disabled persistently (mode = 0). Set mode
	// to 1 will enable all disabled parts
	int mode;
	int bulkLoadMode = 0;
	int bulkDumpMode = 0;
	std::vector<std::pair<StorageServerInterface, ProcessClass>> allServers;
	std::set<std::vector<UID>> primaryTeams;
	std::set<std::vector<UID>> remoteTeams;
	std::vector<DDShardInfo> shards;
	std::vector<UID> toCleanDataMoveTombstone;
	Optional<Key> initHealthyZoneValue; // set for maintenance mode
	KeyRangeMap<std::shared_ptr<DataMove>> dataMoveMap;
	std::vector<AuditStorageState> auditStates;
	Reference<DDConfiguration::RangeConfigMapSnapshot> userRangeConfig;
};

// Holds the permitted size and IO Bounds for a shard
struct ShardSizeBounds {
	StorageMetrics max;
	StorageMetrics min;
	StorageMetrics permittedError;

	bool operator==(ShardSizeBounds const& rhs) const {
		return max == rhs.max && min == rhs.min && permittedError == rhs.permittedError;
	}

	static ShardSizeBounds shardSizeBoundsBeforeTrack();
};

// Gets the permitted size and IO bounds for a shard
ShardSizeBounds getShardSizeBounds(KeyRangeRef shard, int64_t maxShardSize);

// Determines the maximum shard size based on the size of the database
int64_t getMaxShardSize(double dbSizeEstimate);

bool ddLargeTeamEnabled();

struct TeamCollectionInterface {
	PromiseStream<GetTeamRequest> getTeam;
};

struct DDBulkLoadTask {
	BulkLoadState coreState;
	Version commitVersion = invalidVersion;
	Promise<Void> completeAck; // satisfied when a data move for this task completes for the first time, where the task
	                           // metadata phase has been complete

	DDBulkLoadTask() = default;

	DDBulkLoadTask(BulkLoadState coreState, Version commitVersion, Promise<Void> completeAck)
	  : coreState(coreState), commitVersion(commitVersion), completeAck(completeAck) {}

	bool operator==(const DDBulkLoadTask& rhs) const {
		return coreState == rhs.coreState && commitVersion == rhs.commitVersion;
	}

	std::string toString() const {
		return coreState.toString() + ", [CommitVersion]: " + std::to_string(commitVersion);
	}
};

inline bool bulkLoadIsEnabled(int bulkLoadModeValue) {
	return SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && bulkLoadModeValue == 1;
}

inline bool bulkDumpIsEnabled(int bulkDumpModeValue) {
	return bulkDumpModeValue == 1;
}

class BulkLoadTaskCollection : public ReferenceCounted<BulkLoadTaskCollection> {
public:
	BulkLoadTaskCollection(UID ddId, int maxParallelism)
	  : ddId(ddId), maxParallelism(maxParallelism), numRunningTasks(0) {
		bulkLoadTaskMap.insert(allKeys, Optional<DDBulkLoadTask>());
	}

	// Return true if there exists a bulk load task
	bool overlappingTask(KeyRange range) {
		for (auto it : bulkLoadTaskMap.intersectingRanges(range)) {
			if (!it->value().present()) {
				continue;
			}
			return true;
		}
		return false;
	}

	// Return true if there exists a bulk load task since the given commit version
	bool overlappingTaskSince(KeyRange range, Version sinceCommitVersion) {
		for (auto it : bulkLoadTaskMap.intersectingRanges(range)) {
			if (!it->value().present()) {
				continue;
			}
			if (it->value().get().commitVersion > sinceCommitVersion) {
				return true;
			}
		}
		return false;
	}

	// Add a task and this task becomes visible to DDTracker and DDQueue
	// DDTracker stops any shard boundary change overlapping the task range
	// DDQueue attaches the task to following data moves until the task has been completed
	// If there are overlapped old tasks, make it outdated by sending a signal to completeAck
	void publishTask(const BulkLoadState& bulkLoadState, Version commitVersion, Promise<Void> completeAck) {
		if (overlappingTaskSince(bulkLoadState.getRange(), commitVersion)) {
			throw bulkload_task_outdated();
		}
		DDBulkLoadTask task(bulkLoadState, commitVersion, completeAck);
		TraceEvent(SevDebug, "DDBulkLoadCollectionPublishTask", ddId)
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("Range", bulkLoadState.getRange())
		    .detail("Task", task.toString());
		// For any overlapping task, make it outdated
		for (auto it : bulkLoadTaskMap.intersectingRanges(bulkLoadState.getRange())) {
			if (!it->value().present()) {
				continue;
			}
			if (it->value().get().coreState.getTaskId() == bulkLoadState.getTaskId()) {
				ASSERT(it->value().get().coreState.getRange() == bulkLoadState.getRange());
				// In case that the task has been already triggered
				// Avoid repeatedly being triggered by throwing the error
				// then the current doBulkLoadTask will sliently exit
				throw bulkload_task_outdated();
			}
			if (it->value().get().completeAck.canBeSet()) {
				it->value().get().completeAck.sendError(bulkload_task_outdated());
				TraceEvent(SevInfo, "DDBulkLoadCollectionPublishTaskOverwriteTask", ddId)
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("NewRange", bulkLoadState.getRange())
				    .detail("NewTask", task.toString())
				    .detail("OldTaskRange", it->range())
				    .detail("OldTask", it->value().get().toString());
			}
		}
		bulkLoadTaskMap.insert(bulkLoadState.getRange(), task);
		return;
	}

	// This method is called when there is a data move assigned to run the bulk load task
	void startTask(const BulkLoadState& bulkLoadState) {
		for (auto it : bulkLoadTaskMap.intersectingRanges(bulkLoadState.getRange())) {
			if (!it->value().present() || it->value().get().coreState.getTaskId() != bulkLoadState.getTaskId()) {
				throw bulkload_task_outdated();
			}
			TraceEvent(SevDebug, "DDBulkLoadCollectionStartTask", ddId)
			    .detail("Range", bulkLoadState.getRange())
			    .detail("TaskRange", it->range())
			    .detail("Task", it->value().get().toString());
		}
		return;
	}

	// Send complete signal to indicate this task has been completed
	void terminateTask(const BulkLoadState& bulkLoadState) {
		for (auto it : bulkLoadTaskMap.intersectingRanges(bulkLoadState.getRange())) {
			if (!it->value().present() || it->value().get().coreState.getTaskId() != bulkLoadState.getTaskId()) {
				throw bulkload_task_outdated();
			}
			// It is possible that the task has been completed by a past data move
			if (it->value().get().completeAck.canBeSet()) {
				it->value().get().completeAck.send(Void());
				TraceEvent(SevDebug, "DDBulkLoadCollectionTerminateTask", ddId)
				    .detail("Range", bulkLoadState.getRange())
				    .detail("TaskRange", it->range())
				    .detail("Task", it->value().get().toString());
			}
		}
		return;
	}

	// Erase any metadata on the map for the input bulkload task
	void eraseTask(const BulkLoadState& bulkLoadState) {
		std::vector<KeyRange> rangesToClear;
		for (auto it : bulkLoadTaskMap.intersectingRanges(bulkLoadState.getRange())) {
			if (!it->value().present() || it->value().get().coreState.getTaskId() != bulkLoadState.getTaskId()) {
				continue;
			}
			TraceEvent(SevDebug, "DDBulkLoadCollectionEraseTaskdata", ddId)
			    .detail("Range", bulkLoadState.getRange())
			    .detail("TaskRange", it->range())
			    .detail("Task", it->value().get().toString());
			rangesToClear.push_back(it->range());
		}
		for (const auto& rangeToClear : rangesToClear) {
			bulkLoadTaskMap.insert(rangeToClear, Optional<DDBulkLoadTask>());
		}
		bulkLoadTaskMap.coalesce(normalKeys);
		return;
	}

	// Get the task which has exactly the same range as the input range
	Optional<DDBulkLoadTask> getTaskByRange(KeyRange range) const {
		Optional<DDBulkLoadTask> res;
		for (auto it : bulkLoadTaskMap.intersectingRanges(range)) {
			if (!it->value().present()) {
				continue;
			}
			DDBulkLoadTask bulkLoadTask = it->value().get();
			TraceEvent(SevDebug, "DDBulkLoadCollectionGetPublishedTaskEach", ddId)
			    .detail("Range", range)
			    .detail("TaskRange", it->range())
			    .detail("Task", bulkLoadTask.toString());
			if (bulkLoadTask.coreState.getRange() == range) {
				ASSERT(!res.present());
				res = bulkLoadTask;
			}
		}
		TraceEvent(SevDebug, "DDBulkLoadCollectionGetPublishedTask", ddId)
		    .detail("Range", range)
		    .detail("Task", res.present() ? describe(res.get()) : "");
		return res;
	}

	inline void decrementTaskCounter() {
		ASSERT(numRunningTasks.get() <= maxParallelism);
		numRunningTasks.set(numRunningTasks.get() - 1);
		ASSERT(numRunningTasks.get() >= 0);
	}

	// return true if succeed
	inline bool tryStart() {
		if (numRunningTasks.get() < maxParallelism) {
			numRunningTasks.set(numRunningTasks.get() + 1);
			return true;
		} else {
			return false;
		}
	}

	inline bool canStart() const { return numRunningTasks.get() < maxParallelism; }
	inline Future<Void> waitUntilChanged() const { return numRunningTasks.onChange(); }

private:
	KeyRangeMap<Optional<DDBulkLoadTask>> bulkLoadTaskMap;
	UID ddId;
	AsyncVar<int> numRunningTasks;
	int maxParallelism;
};

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////////// Perpetual Storage Wiggle //////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Perpetual Storage Wiggle
#endif
struct DDTeamCollectionInitParams;
class DDTeamCollection;

struct StorageWiggler : ReferenceCounted<StorageWiggler> {
	static constexpr double MIN_ON_CHECK_DELAY_SEC = 5.0;
	enum State : uint8_t { INVALID = 0, RUN = 1, PAUSE = 2 };

	DDTeamCollection const* teamCollection;
	StorageWiggleData wiggleData; // the wiggle related data persistent in database

	StorageWiggleMetrics metrics;
	AsyncVar<bool> stopWiggleSignal;
	// data structures
	typedef std::pair<StorageMetadataType, UID> MetadataUIDP;
	// min-heap
	boost::heap::skew_heap<MetadataUIDP, boost::heap::mutable_<true>, boost::heap::compare<std::greater<MetadataUIDP>>>
	    wiggle_pq;
	std::unordered_map<UID, decltype(wiggle_pq)::handle_type> pq_handles;

	State wiggleState = State::INVALID;
	double lastStateChangeTs = 0.0; // timestamp describes when did the state change

	explicit StorageWiggler(DDTeamCollection* collection) : teamCollection(collection), stopWiggleSignal(true){};
	// wiggle related actors will quit when this signal is set to true
	void setStopSignal(bool value) { stopWiggleSignal.set(value); }
	bool isStopped() const { return stopWiggleSignal.get(); }
	// add server to wiggling queue
	void addServer(const UID& serverId, const StorageMetadataType& metadata);
	// remove server from wiggling queue
	void removeServer(const UID& serverId);
	// update metadata and adjust priority_queue
	void updateMetadata(const UID& serverId, const StorageMetadataType& metadata);
	bool contains(const UID& serverId) const { return pq_handles.contains(serverId); }
	bool empty() const { return wiggle_pq.empty(); }

	// It's guarantee that When a.metadata >= b.metadata, if !necessary(a) then !necessary(b)
	bool necessary(const UID& serverId, const StorageMetadataType& metadata) const;

	// try to return the next storage server that is necessary to wiggle
	Optional<UID> getNextServerId(bool necessaryOnly = true);
	// next check time to avoid busy loop
	Future<Void> onCheck() const;
	State getWiggleState() const { return wiggleState; }
	void setWiggleState(State s) {
		if (wiggleState != s) {
			wiggleState = s;
			lastStateChangeTs = g_network->now();
		}
	}
	static std::string getWiggleStateStr(State s) {
		switch (s) {
		case State::RUN:
			return "running";
		case State::PAUSE:
			return "paused";
		default:
			return "unknown";
		}
	}

	// -- statistic update

	// reset Statistic in database when perpetual wiggle is closed by user
	Future<Void> resetStats();
	// restore Statistic from database when the perpetual wiggle is opened
	Future<Void> restoreStats();
	// called when start wiggling a SS
	Future<Void> startWiggle();
	Future<Void> finishWiggle();
	bool shouldStartNewRound() const { return metrics.last_round_finish >= metrics.last_round_start; }
	bool shouldFinishRound() const {
		if (wiggle_pq.empty())
			return true;
		return (wiggle_pq.top().first.createdTime >= metrics.last_round_start);
	}
};

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

#include "flow/unactorcompiler.h"
#endif
