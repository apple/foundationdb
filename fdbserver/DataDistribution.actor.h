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

#include <boost/heap/skew_heap.hpp>
#include <boost/heap/policies.hpp>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/LogSystem.h"
#include "fdbclient/RunTransaction.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RelocateShard {
	KeyRange keys;
	int priority;

	RelocateShard() : priority(0) {}
	RelocateShard(KeyRange const& keys, int priority) : keys(keys), priority(priority) {}
};

struct IDataDistributionTeam {
	virtual std::vector<StorageServerInterface> getLastKnownServerInterfaces() const = 0;
	virtual int size() const = 0;
	virtual std::vector<UID> const& getServerIDs() const = 0;
	virtual void addDataInFlightToTeam(int64_t delta) = 0;
	virtual int64_t getDataInFlightToTeam() const = 0;
	virtual int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const = 0;
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
		std::string s = format("TeamID:%s", getTeamID().c_str());
		s += format("Size %d; ", servers.size());
		for (int i = 0; i < servers.size(); i++) {
			if (i)
				s += ", ";
			s += servers[i].address().toString() + " " + servers[i].id().shortString();
		}
		return s;
	}
};

struct GetTeamRequest {
	bool wantsNewServers;
	bool wantsTrueBest;
	bool preferLowerUtilization;
	bool teamMustHaveShards;
	double inflightPenalty;
	std::vector<UID> completeSources;
	std::vector<UID> src;
	Promise<std::pair<Optional<Reference<IDataDistributionTeam>>, bool>> reply;

	GetTeamRequest() {}
	GetTeamRequest(bool wantsNewServers,
	               bool wantsTrueBest,
	               bool preferLowerUtilization,
	               bool teamMustHaveShards,
	               double inflightPenalty = 1.0)
	  : wantsNewServers(wantsNewServers), wantsTrueBest(wantsTrueBest), preferLowerUtilization(preferLowerUtilization),
	    teamMustHaveShards(teamMustHaveShards), inflightPenalty(inflightPenalty) {}

	std::string getDesc() const {
		std::stringstream ss;

		ss << "WantsNewServers:" << wantsNewServers << " WantsTrueBest:" << wantsTrueBest
		   << " PreferLowerUtilization:" << preferLowerUtilization << " teamMustHaveShards:" << teamMustHaveShards
		   << " inflightPenalty:" << inflightPenalty << ";";
		ss << "CompleteSources:";
		for (const auto& cs : completeSources) {
			ss << cs.toString() << ",";
		}

		return std::move(ss).str();
	}
};

struct GetMetricsRequest {
	KeyRange keys;
	Promise<StorageMetrics> reply;

	GetMetricsRequest() {}
	GetMetricsRequest(KeyRange const& keys) : keys(keys) {}
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

class ShardsAffectedByTeamFailure : public ReferenceCounted<ShardsAffectedByTeamFailure> {
public:
	ShardsAffectedByTeamFailure() {}

	struct Team {
		std::vector<UID> servers; // sorted
		bool primary;

		Team() : primary(true) {}
		Team(std::vector<UID> const& servers, bool primary) : servers(servers), primary(primary) {}

		bool operator<(const Team& r) const {
			if (servers == r.servers)
				return primary < r.primary;
			return servers < r.servers;
		}
		bool operator>(const Team& r) const { return r < *this; }
		bool operator<=(const Team& r) const { return !(*this > r); }
		bool operator>=(const Team& r) const { return !(*this < r); }
		bool operator==(const Team& r) const { return servers == r.servers && primary == r.primary; }
		bool operator!=(const Team& r) const { return !(*this == r); }
	};

	// This tracks the data distribution on the data distribution server so that teamTrackers can
	//   relocate the right shards when a team is degraded.

	// The following are important to make sure that failure responses don't revert splits or merges:
	//   - The shards boundaries in the two data structures reflect "queued" RelocateShard requests
	//       (i.e. reflects the desired set of shards being tracked by dataDistributionTracker,
	//       rather than the status quo).  These boundaries are modified in defineShard and the content
	//       of what servers correspond to each shard is a copy or union of the shards already there
	//   - The teams associated with each shard reflect either the sources for non-moving shards
	//       or the destination team for in-flight shards (the change is atomic with respect to team selection).
	//       moveShard() changes the servers associated with a shard and will never adjust the shard
	//       boundaries. If a move is received for a shard that has been redefined (the exact shard is
	//       no longer in the map), the servers will be set for all contained shards and added to all
	//       intersecting shards.

	int getNumberOfShards(UID ssID) const;
	std::vector<KeyRange> getShardsFor(Team team) const;
	bool hasShards(Team team) const;

	// The first element of the pair is either the source for non-moving shards or the destination team for in-flight
	// shards The second element of the pair is all previous sources for in-flight shards
	std::pair<std::vector<Team>, std::vector<Team>> getTeamsFor(KeyRangeRef keys);

	void defineShard(KeyRangeRef keys);
	void moveShard(KeyRangeRef keys, std::vector<Team> destinationTeam);
	void finishMove(KeyRangeRef keys);
	void check() const;

private:
	struct OrderByTeamKey {
		bool operator()(const std::pair<Team, KeyRange>& lhs, const std::pair<Team, KeyRange>& rhs) const {
			if (lhs.first < rhs.first)
				return true;
			if (lhs.first > rhs.first)
				return false;
			return lhs.second.begin < rhs.second.begin;
		}
	};

	KeyRangeMap<std::pair<std::vector<Team>, std::vector<Team>>>
	    shard_teams; // A shard can be affected by the failure of multiple teams if it is a queued merge, or when
	                 // usable_regions > 1
	std::set<std::pair<Team, KeyRange>, OrderByTeamKey> team_shards;
	std::map<UID, int> storageServerShards;

	void erase(Team team, KeyRange const& range);
	void insert(Team team, KeyRange const& range);
};

// DDShardInfo is so named to avoid link-time name collision with ShardInfo within the StorageServer
struct DDShardInfo {
	Key key;
	std::vector<UID> primarySrc;
	std::vector<UID> remoteSrc;
	std::vector<UID> primaryDest;
	std::vector<UID> remoteDest;
	bool hasDest;

	explicit DDShardInfo(Key key) : key(key), hasDest(false) {}
};

struct InitialDataDistribution : ReferenceCounted<InitialDataDistribution> {
	int mode;
	std::vector<std::pair<StorageServerInterface, ProcessClass>> allServers;
	std::set<std::vector<UID>> primaryTeams;
	std::set<std::vector<UID>> remoteTeams;
	std::vector<DDShardInfo> shards;
	Optional<Key> initHealthyZoneValue;
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
                                         Reference<AsyncVar<bool>> processingUnhealthy,
                                         Reference<AsyncVar<bool>> processingWiggle,
                                         std::vector<TeamCollectionInterface> teamCollection,
                                         Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
                                         MoveKeysLock lock,
                                         PromiseStream<Promise<int64_t>> getAverageShardBytes,
                                         PromiseStream<Promise<int>> getUnhealthyRelocationCount,
                                         UID distributorId,
                                         int teamSize,
                                         int singleRegionTeamSize,
                                         double* lastLimited,
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
	uint64_t last_round_start = 0; // wall timer: timer_int()
	uint64_t last_round_finish = 0;
	TimerSmoother smoothed_round_duration;
	int finished_round = 0; // finished round since storage wiggle is open

	// step statistics
	// 1 wiggle step as 1 storage server is wiggled in the current round
	uint64_t last_wiggle_start = 0; // wall timer: timer_int()
	uint64_t last_wiggle_finish = 0;
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
		result["last_round_start_datetime"] = timerIntToGmt(last_round_start);
		result["last_round_finish_datetime"] = timerIntToGmt(last_round_finish);
		result["last_round_start_timestamp"] = last_round_start;
		result["last_round_finish_timestamp"] = last_round_finish;
		result["smoothed_round_seconds"] = smoothed_round_duration.smoothTotal();
		result["finished_round"] = finished_round;

		result["last_wiggle_start_datetime"] = timerIntToGmt(last_wiggle_start);
		result["last_wiggle_finish_datetime"] = timerIntToGmt(last_wiggle_finish);
		result["last_wiggle_start_timestamp"] = last_wiggle_start;
		result["last_wiggle_finish_timestamp"] = last_wiggle_finish;
		result["smoothed_wiggle_seconds"] = smoothed_wiggle_duration.smoothTotal();
		result["finished_wiggle"] = finished_wiggle;
		return result;
	}
};

struct StorageWiggler : ReferenceCounted<StorageWiggler> {
	DDTeamCollection const* teamCollection;
	StorageWiggleMetrics metrics;

	// data structures
	typedef std::pair<StorageMetadataType, UID> MetadataUIDP;
	// sorted by (createdTime, UID), the least comes first
	struct CompPair {
		bool operator()(MetadataUIDP const& a, MetadataUIDP const& b) const {
			if (a.first.createdTime == b.first.createdTime) {
				return a.second > b.second;
			}
			// larger createdTime means the age is younger
			return a.first.createdTime > b.first.createdTime;
		}
	};
	boost::heap::skew_heap<MetadataUIDP, boost::heap::mutable_<true>, boost::heap::compare<CompPair>> wiggle_pq;
	std::unordered_map<UID, decltype(wiggle_pq)::handle_type> pq_handles;

	AsyncVar<bool> nonEmpty;

	explicit StorageWiggler(DDTeamCollection* collection) : teamCollection(collection), nonEmpty(false){};
	// add server to wiggling queue
	void addServer(const UID& serverId, const StorageMetadataType& metadata);
	// remove server from wiggling queue
	void removeServer(const UID& serverId);
	// update metadata and adjust priority_queue
	void updateMetadata(const UID& serverId, const StorageMetadataType& metadata);
	bool contains(const UID& serverId) const { return pq_handles.count(serverId) > 0; }
	bool empty() const { return wiggle_pq.empty(); }
	Optional<UID> getNextServerId();

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

ACTOR Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses(
    Transaction* tr);

#include "flow/unactorcompiler.h"
#endif
