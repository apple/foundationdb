/*
 * DataDistribution.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_DATA_DISTRIBUTION_ACTOR_G_H)
#define FDBSERVER_DATA_DISTRIBUTION_ACTOR_G_H
#include "fdbserver/DataDistribution.actor.g.h"
#elif !defined(FDBSERVER_DATA_DISTRIBUTION_ACTOR_H)
#define FDBSERVER_DATA_DISTRIBUTION_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/TenantCache.h"
#include "fdbserver/TCInfo.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"
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

private:
	Value value;
};

// One-to-one relationship to the priority knobs
enum class DataMovementReason {
	INVALID,
	RECOVER_MOVE,
	REBALANCE_UNDERUTILIZED_TEAM,
	REBALANCE_OVERUTILIZED_TEAM,
	REBALANCE_READ_OVERUTIL_TEAM,
	REBALANCE_READ_UNDERUTIL_TEAM,
	PERPETUAL_STORAGE_WIGGLE,
	TEAM_HEALTHY,
	TEAM_CONTAINS_UNDESIRED_SERVER,
	TEAM_REDUNDANT,
	MERGE_SHARD,
	POPULATE_REGION,
	TEAM_UNHEALTHY,
	TEAM_2_LEFT,
	TEAM_1_LEFT,
	TEAM_FAILED,
	TEAM_0_LEFT,
	SPLIT_SHARD,
	ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD
};
extern int dataMovementPriority(DataMovementReason moveReason);
extern DataMovementReason priorityToDataMovementReason(int priority);

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

private:
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
	double maxBytesReadPerKSecond = 0, minBytesReadPerKSecond = 0; // all returned shards won't exceed this read load

	GetTopKMetricsRequest() {}
	GetTopKMetricsRequest(std::vector<KeyRange> const& keys,
	                      int topK = 1,
	                      double maxBytesReadPerKSecond = std::numeric_limits<double>::max(),
	                      double minBytesReadPerKSecond = 0)
	  : topK(topK), keys(keys), maxBytesReadPerKSecond(maxBytesReadPerKSecond),
	    minBytesReadPerKSecond(minBytesReadPerKSecond) {
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
		return a.metrics.bytesReadPerKSecond / std::max(a.metrics.bytes * 1.0, 1.0) >
		       b.metrics.bytesReadPerKSecond / std::max(b.metrics.bytes * 1.0, 1.0);
	}
};

struct GetMetricsListRequest {
	KeyRange keys;
	int shardLimit;
	Promise<Standalone<VectorRef<DDMetricsRef>>> reply;

	GetMetricsListRequest() {}
	GetMetricsListRequest(KeyRange const& keys, const int shardLimit) : keys(keys), shardLimit(shardLimit) {}
};

// PhysicalShardCollection maintains physical shard concepts in data distribution
// A physical shard contains one or multiple shards (key range)
// PhysicalShardCollection is responsible for creation and maintenance of physical shards (including metrics)
// For multiple DCs, PhysicalShardCollection maintains a pair of primary team and remote team
// A primary team and a remote team shares a physical shard
// For each shard (key-range) move, PhysicalShardCollection decides which physical shard and corresponding team(s) to
// move The current design of PhysicalShardCollection assumes that there exists at most two teamCollections
// TODO: unit test needed
FDB_DECLARE_BOOLEAN_PARAM(InAnonymousPhysicalShard);
FDB_DECLARE_BOOLEAN_PARAM(PhysicalShardHasMoreThanKeyRange);
FDB_DECLARE_BOOLEAN_PARAM(InOverSizePhysicalShard);
FDB_DECLARE_BOOLEAN_PARAM(PhysicalShardAvailable);
FDB_DECLARE_BOOLEAN_PARAM(MoveKeyRangeOutPhysicalShard);

class PhysicalShardCollection : public ReferenceCounted<PhysicalShardCollection> {
public:
	PhysicalShardCollection() : lastTransitionStartTime(now()), requireTransition(false) {}

	enum class PhysicalShardCreationTime { DDInit, DDRelocator };

	struct PhysicalShard {
		PhysicalShard() : id(UID().first()) {}

		PhysicalShard(uint64_t id,
		              StorageMetrics const& metrics,
		              std::vector<ShardsAffectedByTeamFailure::Team> teams,
		              PhysicalShardCreationTime whenCreated)
		  : id(id), metrics(metrics), teams(teams), whenCreated(whenCreated) {}

		std::string toString() const { return fmt::format("{}", std::to_string(id)); }

		uint64_t id; // physical shard id (never changed)
		StorageMetrics metrics; // current metrics, updated by shardTracker
		std::vector<ShardsAffectedByTeamFailure::Team> teams; // which team owns this physical shard (never changed)
		PhysicalShardCreationTime whenCreated; // when this physical shard is created (never changed)
	};

	// Two-step team selection
	// Usage: getting primary dest team and remote dest team in dataDistributionRelocator()
	// The overall process has two steps:
	// Step 1: get a physical shard id given the input primary team
	// Return a new physical shard id if the input primary team is new or the team has no available physical shard
	// checkPhysicalShardAvailable() defines whether a physical shard is available
	uint64_t determinePhysicalShardIDGivenPrimaryTeam(ShardsAffectedByTeamFailure::Team primaryTeam,
	                                                  StorageMetrics const& metrics,
	                                                  bool forceToUseNewPhysicalShard,
	                                                  uint64_t debugID);

	// Step 2: get a remote team which has the input physical shard
	// Return empty if no such remote team
	// May return a problematic remote team, and re-selection is required for this case
	Optional<ShardsAffectedByTeamFailure::Team> tryGetAvailableRemoteTeamWith(uint64_t inputPhysicalShardID,
	                                                                          StorageMetrics const& moveInMetrics,
	                                                                          uint64_t debugID);
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

	// Generate a random physical shard ID, which is not UID().first() nor anonymousShardId.first()
	uint64_t generateNewPhysicalShardID(uint64_t debugID);

	// Check whether the input physical shard is available
	// A physical shard is available if the current metric + moveInMetrics <= a threshold
	PhysicalShardAvailable checkPhysicalShardAvailable(uint64_t physicalShardID, StorageMetrics const& moveInMetrics);

	// If the input team has any available physical shard, return an available physical shard of the input team
	Optional<uint64_t> trySelectAvailablePhysicalShardFor(ShardsAffectedByTeamFailure::Team team,
	                                                      StorageMetrics const& metrics,
	                                                      uint64_t debugID);

	// Reduce the metics of input physical shard by the input metrics
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

	// Return a string concating the input IDs interleaving with " "
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
	InitialDataDistribution() : dataMoveMap(std::make_shared<DataMove>()) {}

	// Read from dataDistributionModeKey. Whether DD is disabled. DD can be disabled persistently (mode = 0). Set mode
	// to 1 will enable all disabled parts
	int mode;
	std::vector<std::pair<StorageServerInterface, ProcessClass>> allServers;
	std::set<std::vector<UID>> primaryTeams;
	std::set<std::vector<UID>> remoteTeams;
	std::vector<DDShardInfo> shards;
	Optional<Key> initHealthyZoneValue; // set for maintenance mode
	KeyRangeMap<std::shared_ptr<DataMove>> dataMoveMap;
};

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
	Reference<AsyncVar<Optional<ShardMetrics>>> stats;
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

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

// FIXME(xwang): Delete Old DD Actors once the refactoring is done
/////////////////////////////// Old DD Actors //////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Old DD Actors
#endif

struct TeamCollectionInterface {
	PromiseStream<GetTeamRequest> getTeam;
};

ACTOR Future<Void> dataDistributionTracker(Reference<InitialDataDistribution> initData,
                                           Reference<IDDTxnProcessor> db,
                                           PromiseStream<RelocateShard> output,
                                           Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
                                           Reference<PhysicalShardCollection> physicalShardCollection,
                                           PromiseStream<GetMetricsRequest> getShardMetrics,
                                           FutureStream<GetTopKMetricsRequest> getTopKMetrics,
                                           PromiseStream<GetMetricsListRequest> getShardMetricsList,
                                           FutureStream<Promise<int64_t>> getAverageShardBytes,
                                           Promise<Void> readyToStart,
                                           Reference<AsyncVar<bool>> zeroHealthyTeams,
                                           UID distributorId,
                                           KeyRangeMap<ShardTrackedData>* shards,
                                           bool* trackerCancelled,
                                           Optional<Reference<TenantCache>> ddTenantCache);

ACTOR Future<Void> dataDistributionQueue(Reference<IDDTxnProcessor> db,
                                         PromiseStream<RelocateShard> output,
                                         FutureStream<RelocateShard> input,
                                         PromiseStream<GetMetricsRequest> getShardMetrics,
                                         PromiseStream<GetTopKMetricsRequest> getTopKMetrics,
                                         Reference<AsyncVar<bool>> processingUnhealthy,
                                         Reference<AsyncVar<bool>> processingWiggle,
                                         std::vector<TeamCollectionInterface> teamCollections,
                                         Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
                                         Reference<PhysicalShardCollection> physicalShardCollection,
                                         MoveKeysLock lock,
                                         PromiseStream<Promise<int64_t>> getAverageShardBytes,
                                         FutureStream<Promise<int>> getUnhealthyRelocationCount,
                                         UID distributorId,
                                         int teamSize,
                                         int singleRegionTeamSize,
                                         const DDEnabledState* ddEnabledState);
#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////////// Perpetual Storage Wiggle //////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Perpetual Storage Wiggle
#endif
class DDTeamCollection;

struct StorageWiggleMetrics {
	constexpr static FileIdentifier file_identifier = 4728961;

	// round statistics
	// One StorageServer wiggle round is considered 'complete', when all StorageServers with creationTime < T are
	// wiggled
	// Start and finish are in epoch seconds
	double last_round_start = 0;
	double last_round_finish = 0;
	TimerSmoother smoothed_round_duration;
	int finished_round = 0; // finished round since storage wiggle is open

	// step statistics
	// 1 wiggle step as 1 storage server is wiggled in the current round
	// Start and finish are in epoch seconds
	double last_wiggle_start = 0;
	double last_wiggle_finish = 0;
	TimerSmoother smoothed_wiggle_duration;
	int finished_wiggle = 0; // finished step since storage wiggle is open

	StorageWiggleMetrics() : smoothed_round_duration(20.0 * 60), smoothed_wiggle_duration(10.0 * 60) {}

	template <class Ar>
	void serialize(Ar& ar) {
		double step_total, round_total;
		if (!ar.isDeserializing) {
			step_total = smoothed_wiggle_duration.getTotal();
			round_total = smoothed_round_duration.getTotal();
		}
		serializer(ar,
		           last_wiggle_start,
		           last_wiggle_finish,
		           step_total,
		           finished_wiggle,
		           last_round_start,
		           last_round_finish,
		           round_total,
		           finished_round);
		if (ar.isDeserializing) {
			smoothed_round_duration.reset(round_total);
			smoothed_wiggle_duration.reset(step_total);
		}
	}

	static Future<Void> runSetTransaction(Reference<ReadYourWritesTransaction> tr,
	                                      bool primary,
	                                      StorageWiggleMetrics metrics) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->set(perpetualStorageWiggleStatsPrefix.withSuffix(primary ? "primary"_sr : "remote"_sr),
		        ObjectWriter::toValue(metrics, IncludeVersion()));
		return Void();
	}

	static Future<Void> runSetTransaction(Database cx, bool primary, StorageWiggleMetrics metrics) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			return runSetTransaction(tr, primary, metrics);
		});
	}

	static Future<Optional<Value>> runGetTransaction(Reference<ReadYourWritesTransaction> tr, bool primary) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		return tr->get(perpetualStorageWiggleStatsPrefix.withSuffix(primary ? "primary"_sr : "remote"_sr));
	}

	static Future<Optional<Value>> runGetTransaction(Database cx, bool primary) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
			return runGetTransaction(tr, primary);
		});
	}

	StatusObject toJSON() const {
		StatusObject result;
		result["last_round_start_datetime"] = epochsToGMTString(last_round_start);
		result["last_round_finish_datetime"] = epochsToGMTString(last_round_finish);
		result["last_round_start_timestamp"] = last_round_start;
		result["last_round_finish_timestamp"] = last_round_finish;
		result["smoothed_round_seconds"] = smoothed_round_duration.smoothTotal();
		result["finished_round"] = finished_round;

		result["last_wiggle_start_datetime"] = epochsToGMTString(last_wiggle_start);
		result["last_wiggle_finish_datetime"] = epochsToGMTString(last_wiggle_finish);
		result["last_wiggle_start_timestamp"] = last_wiggle_start;
		result["last_wiggle_finish_timestamp"] = last_wiggle_finish;
		result["smoothed_wiggle_seconds"] = smoothed_wiggle_duration.smoothTotal();
		result["finished_wiggle"] = finished_wiggle;
		return result;
	}
};

struct StorageWiggler : ReferenceCounted<StorageWiggler> {
	static constexpr double MIN_ON_CHECK_DELAY_SEC = 5.0;
	enum State : uint8_t { INVALID = 0, RUN = 1, PAUSE = 2 };

	DDTeamCollection const* teamCollection;
	StorageWiggleMetrics metrics;
	// data structures
	typedef std::pair<StorageMetadataType, UID> MetadataUIDP;
	// min-heap
	boost::heap::skew_heap<MetadataUIDP, boost::heap::mutable_<true>, boost::heap::compare<std::greater<MetadataUIDP>>>
	    wiggle_pq;
	std::unordered_map<UID, decltype(wiggle_pq)::handle_type> pq_handles;

	State wiggleState = State::INVALID;
	double lastStateChangeTs = 0.0; // timestamp describes when did the state change

	explicit StorageWiggler(DDTeamCollection* collection) : teamCollection(collection){};
	// add server to wiggling queue
	void addServer(const UID& serverId, const StorageMetadataType& metadata);
	// remove server from wiggling queue
	void removeServer(const UID& serverId);
	// update metadata and adjust priority_queue
	void updateMetadata(const UID& serverId, const StorageMetadataType& metadata);
	bool contains(const UID& serverId) const { return pq_handles.count(serverId) > 0; }
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
