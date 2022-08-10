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
#include "fdbclient/RunTransaction.actor.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"
#include <boost/heap/policies.hpp>
#include <boost/heap/skew_heap.hpp>

#include "flow/actorcompiler.h" // This must be the last #include.

enum class RelocateReason { OTHER = 0, REBALANCE_DISK, REBALANCE_READ };

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
	SPLIT_SHARD
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

	// Initialization when define is a better practice. We should avoid assignment of member after definition.
	// static RelocateShard emptyRelocateShard() { return {}; }

	RelocateShard(KeyRange const& keys, DataMovementReason moveReason, RelocateReason reason)
	  : keys(keys), priority(dataMovementPriority(moveReason)), cancelled(false), dataMoveId(anonymousShardId),
	    reason(reason), moveReason(moveReason) {}

	RelocateShard(KeyRange const& keys, int priority, RelocateReason reason)
	  : keys(keys), priority(priority), cancelled(false), dataMoveId(anonymousShardId), reason(reason),
	    moveReason(priorityToDataMovementReason(priority)) {}

	bool isRestore() const { return this->dataMove != nullptr; }

private:
	RelocateShard()
	  : priority(0), cancelled(false), dataMoveId(anonymousShardId), reason(RelocateReason::OTHER),
	    moveReason(DataMovementReason::INVALID) {}
};

struct IDataDistributionTeam {
	virtual std::vector<StorageServerInterface> getLastKnownServerInterfaces() const = 0;
	virtual int size() const = 0;
	virtual std::vector<UID> const& getServerIDs() const = 0;
	virtual void addDataInFlightToTeam(int64_t delta) = 0;
	virtual void addReadInFlightToTeam(int64_t delta) = 0;
	virtual int64_t getDataInFlightToTeam() const = 0;
	virtual int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const = 0;
	virtual int64_t getReadInFlightToTeam() const = 0;
	virtual double getLoadReadBandwidth(bool includeInFlight = true, double inflightPenalty = 1.0) const = 0;
	virtual int64_t getMinAvailableSpace(bool includeInFlight = true) const = 0;
	virtual double getMinAvailableSpaceRatio(bool includeInFlight = true) const = 0;
	virtual bool hasHealthyAvailableSpace(double minRatio) const = 0;
	virtual Future<Void> updateStorageMetrics() = 0;
	virtual void addref() const = 0;
	virtual void delref() const = 0;
	virtual bool isHealthy() const = 0;
	virtual void setHealthy(bool) = 0;
	virtual int getPriority() const = 0;
	virtual void setPriority(int) = 0;
	virtual bool isOptimal() const = 0;
	virtual bool isWrongConfiguration() const = 0;
	virtual void setWrongConfiguration(bool) = 0;
	virtual void addServers(const std::vector<UID>& servers) = 0;
	virtual std::string getTeamID() const = 0;

	std::string getDesc() const {
		const auto& servers = getLastKnownServerInterfaces();
		std::string s = format("TeamID %s; ", getTeamID().c_str());
		s += format("Size %d; ", servers.size());
		for (int i = 0; i < servers.size(); i++) {
			if (i)
				s += ", ";
			s += servers[i].address().toString() + " " + servers[i].id().shortString();
		}
		return s;
	}
};

FDB_DECLARE_BOOLEAN_PARAM(WantNewServers);
FDB_DECLARE_BOOLEAN_PARAM(WantTrueBest);
FDB_DECLARE_BOOLEAN_PARAM(PreferLowerDiskUtil);
FDB_DECLARE_BOOLEAN_PARAM(TeamMustHaveShards);
FDB_DECLARE_BOOLEAN_PARAM(ForReadBalance);
FDB_DECLARE_BOOLEAN_PARAM(PreferLowerReadUtil);
FDB_DECLARE_BOOLEAN_PARAM(FindTeamByServers);

struct GetTeamRequest {
	bool wantsNewServers; // In additional to servers in completeSources, try to find teams with new server
	bool wantsTrueBest;
	bool preferLowerDiskUtil; // if true, lower utilized team has higher score
	bool teamMustHaveShards;
	bool forReadBalance;
	bool preferLowerReadUtil; // only make sense when forReadBalance is true
	double inflightPenalty;
	bool findTeamByServers;
	std::vector<UID> completeSources;
	std::vector<UID> src;
	Promise<std::pair<Optional<Reference<IDataDistributionTeam>>, bool>> reply;

	typedef Reference<IDataDistributionTeam> TeamRef;

	GetTeamRequest() {}
	GetTeamRequest(WantNewServers wantsNewServers,
	               WantTrueBest wantsTrueBest,
	               PreferLowerDiskUtil preferLowerDiskUtil,
	               TeamMustHaveShards teamMustHaveShards,
	               ForReadBalance forReadBalance = ForReadBalance::False,
	               PreferLowerReadUtil preferLowerReadUtil = PreferLowerReadUtil::False,
	               double inflightPenalty = 1.0)
	  : wantsNewServers(wantsNewServers), wantsTrueBest(wantsTrueBest), preferLowerDiskUtil(preferLowerDiskUtil),
	    teamMustHaveShards(teamMustHaveShards), forReadBalance(forReadBalance),
	    preferLowerReadUtil(preferLowerReadUtil), inflightPenalty(inflightPenalty),
	    findTeamByServers(FindTeamByServers::False) {}
	GetTeamRequest(std::vector<UID> servers)
	  : wantsNewServers(WantNewServers::False), wantsTrueBest(WantTrueBest::False),
	    preferLowerDiskUtil(PreferLowerDiskUtil::False), teamMustHaveShards(TeamMustHaveShards::False),
	    forReadBalance(ForReadBalance::False), preferLowerReadUtil(PreferLowerReadUtil::False), inflightPenalty(1.0),
	    findTeamByServers(FindTeamByServers::True), src(std::move(servers)) {}

	// return true if a.score < b.score
	[[nodiscard]] bool lessCompare(TeamRef a, TeamRef b, int64_t aLoadBytes, int64_t bLoadBytes) const {
		int res = 0;
		if (forReadBalance) {
			res = preferLowerReadUtil ? greaterReadLoad(a, b) : lessReadLoad(a, b);
		}
		return res == 0 ? lessCompareByLoad(aLoadBytes, bLoadBytes) : res < 0;
	}

	std::string getDesc() const {
		std::stringstream ss;

		ss << "WantsNewServers:" << wantsNewServers << " WantsTrueBest:" << wantsTrueBest
		   << " PreferLowerDiskUtil:" << preferLowerDiskUtil << " teamMustHaveShards:" << teamMustHaveShards
		   << "forReadBalance" << forReadBalance << " inflightPenalty:" << inflightPenalty
		   << " findTeamByServers:" << findTeamByServers << ";";
		ss << "CompleteSources:";
		for (const auto& cs : completeSources) {
			ss << cs.toString() << ",";
		}

		return std::move(ss).str();
	}

private:
	// return true if preferHigherUtil && aLoadBytes <= bLoadBytes (higher load bytes has larger score)
	// or preferLowerUtil && aLoadBytes > bLoadBytes
	bool lessCompareByLoad(int64_t aLoadBytes, int64_t bLoadBytes) const {
		bool lessLoad = aLoadBytes <= bLoadBytes;
		return preferLowerDiskUtil ? !lessLoad : lessLoad;
	}

	// return -1 if a.readload > b.readload
	static int greaterReadLoad(TeamRef a, TeamRef b) {
		auto r1 = a->getLoadReadBandwidth(true), r2 = b->getLoadReadBandwidth(true);
		return r1 == r2 ? 0 : (r1 > r2 ? -1 : 1);
	}
	// return -1 if a.readload < b.readload
	static int lessReadLoad(TeamRef a, TeamRef b) {
		auto r1 = a->getLoadReadBandwidth(false), r2 = b->getLoadReadBandwidth(false);
		return r1 == r2 ? 0 : (r1 < r2 ? -1 : 1);
	}
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
	int topK = 1; // default only return the top 1 shard based on the GetTopKMetricsRequest::compare function
	std::vector<KeyRange> keys;
	Promise<GetTopKMetricsReply> reply; // topK storage metrics
	double maxBytesReadPerKSecond = 0, minBytesReadPerKSecond = 0; // all returned shards won't exceed this read load

	GetTopKMetricsRequest() {}
	GetTopKMetricsRequest(std::vector<KeyRange> const& keys,
	                      int topK = 1,
	                      double maxBytesReadPerKSecond = std::numeric_limits<double>::max(),
	                      double minBytesReadPerKSecond = 0)
	  : topK(topK), keys(keys), maxBytesReadPerKSecond(maxBytesReadPerKSecond),
	    minBytesReadPerKSecond(minBytesReadPerKSecond) {}

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

struct TeamCollectionInterface {
	PromiseStream<GetTeamRequest> getTeam;
};

// DDShardInfo is so named to avoid link-time name collision with ShardInfo within the StorageServer
struct DDShardInfo {
	Key key;
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

ACTOR Future<Void> dataDistributionTracker(Reference<InitialDataDistribution> initData,
                                           Database cx,
                                           PromiseStream<RelocateShard> output,
                                           Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
                                           PromiseStream<GetMetricsRequest> getShardMetrics,
                                           FutureStream<GetTopKMetricsRequest> getTopKMetrics,
                                           PromiseStream<GetMetricsListRequest> getShardMetricsList,
                                           FutureStream<Promise<int64_t>> getAverageShardBytes,
                                           Promise<Void> readyToStart,
                                           Reference<AsyncVar<bool>> zeroHealthyTeams,
                                           UID distributorId,
                                           KeyRangeMap<ShardTrackedData>* shards,
                                           bool* trackerCancelled);

ACTOR Future<Void> dataDistributionQueue(Database cx,
                                         PromiseStream<RelocateShard> output,
                                         FutureStream<RelocateShard> input,
                                         PromiseStream<GetMetricsRequest> getShardMetrics,
                                         PromiseStream<GetTopKMetricsRequest> getTopKMetrics,
                                         Reference<AsyncVar<bool>> processingUnhealthy,
                                         Reference<AsyncVar<bool>> processingWiggle,
                                         std::vector<TeamCollectionInterface> teamCollection,
                                         Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
                                         MoveKeysLock lock,
                                         PromiseStream<Promise<int64_t>> getAverageShardBytes,
                                         FutureStream<Promise<int>> getUnhealthyRelocationCount,
                                         UID distributorId,
                                         int teamSize,
                                         int singleRegionTeamSize,
                                         const DDEnabledState* ddEnabledState);

// Holds the permitted size and IO Bounds for a shard
struct ShardSizeBounds {
	StorageMetrics max;
	StorageMetrics min;
	StorageMetrics permittedError;

	bool operator==(ShardSizeBounds const& rhs) const {
		return max == rhs.max && min == rhs.min && permittedError == rhs.permittedError;
	}
};

// Gets the permitted size and IO bounds for a shard
ShardSizeBounds getShardSizeBounds(KeyRangeRef shard, int64_t maxShardSize);

// Determines the maximum shard size based on the size of the database
int64_t getMaxShardSize(double dbSizeEstimate);

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

private:
	mutable Debouncer pqCanCheck{ MIN_ON_CHECK_DELAY_SEC };

public:
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

	// It's guarantee that When a.metadata >= b.metadata, if !eligible(a) then !eligible(b)
	bool eligible(const UID& serverId, const StorageMetadataType& metadata) const;

	// try to return the next storage server that is eligible for wiggle
	Optional<UID> getNextServerId();
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

#include "flow/unactorcompiler.h"
#endif
