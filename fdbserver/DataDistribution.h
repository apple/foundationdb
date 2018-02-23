/*
 * DataDistribution.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.h"
#include "ClusterRecruitmentInterface.h"
#include "MoveKeys.h"
#include "LogSystem.h"

struct RelocateShard {
	KeyRange keys;
	int priority;

	RelocateShard() {}
	RelocateShard( KeyRange const& keys, int priority ) : keys(keys), priority(priority) {}
};

// Higher priorities are executed first
// Priority/100 is the "priority group"/"superpriority".  Priority inversion
//   is possible within but not between priority groups; fewer priority groups
//   mean better worst case time bounds
enum {
	PRIORITY_REBALANCE_SHARD = 100,
	PRIORITY_RECOVER_MOVE    = 110,
	PRIORITY_REBALANCE_UNDERUTILIZED_TEAM  = 120,
	PRIORITY_REBALANCE_OVERUTILIZED_TEAM  = 121,
	PRIORITY_TEAM_HEALTHY    = 140,
	PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER = 150,

	PRIORITY_MERGE_SHARD     = 240,
	PRIORITY_SPLIT_SHARD     = 250,

	PRIORITY_TEAM_UNHEALTHY  = 800,
	PRIORITY_TEAM_2_LEFT     = 809,

	PRIORITY_TEAM_1_LEFT     = 900,

	PRIORITY_TEAM_0_LEFT     = 999
};

enum {
	SOME_SHARED = 2,
	NONE_SHARED = 3
};

struct IDataDistributionTeam {
	virtual vector<StorageServerInterface> getLastKnownServerInterfaces() = 0;
	virtual vector<UID> const& getServerIDs() = 0;
	virtual void addDataInFlightToTeam( int64_t delta ) = 0;
	virtual int64_t getDataInFlightToTeam() = 0;
	virtual int64_t getLoadBytes( bool includeInFlight = true, double inflightPenalty = 1.0 ) = 0;
	virtual int64_t getMinFreeSpace( bool includeInFlight = true ) = 0;
	virtual double getMinFreeSpaceRatio( bool includeInFlight = true ) = 0;
	virtual bool hasHealthyFreeSpace() = 0;
	virtual Future<Void> updatePhysicalMetrics() = 0;
	virtual void addref() = 0;
	virtual void delref() = 0;
	virtual bool isHealthy() = 0;
	virtual void setHealthy(bool) = 0;
	virtual int getPriority() = 0;
	virtual void setPriority(int) = 0;
	virtual bool isOptimal() = 0;
	virtual bool isWrongConfiguration() = 0;
	virtual void setWrongConfiguration(bool) = 0;

	std::string getDesc() {
		const auto& servers = getLastKnownServerInterfaces();
		std::string s = format("Size %d; ", servers.size());
		for(int i=0; i<servers.size(); i++) {
			if (i) s += ", ";
			s += servers[i].address().toString() + " " + servers[i].id().shortString();
		}
		return s;
	}
};

struct GetTeamRequest {
	bool wantsNewServers;
	bool wantsTrueBest;
	bool preferLowerUtilization;
	double inflightPenalty;
	std::vector<UID> sources;
	Promise< Optional< Reference<IDataDistributionTeam> > > reply;

	GetTeamRequest() {}
	GetTeamRequest( bool wantsNewServers, bool wantsTrueBest, bool preferLowerUtilization, double inflightPenalty = 1.0 ) : wantsNewServers( wantsNewServers ), wantsTrueBest( wantsTrueBest ), preferLowerUtilization( preferLowerUtilization ), inflightPenalty(inflightPenalty) {}
};

struct GetMetricsRequest {
	KeyRange keys;
	Promise< StorageMetrics > reply;

	GetMetricsRequest() {}
	GetMetricsRequest( KeyRange const& keys ) : keys(keys) {}
};

struct TeamCollectionInterface {
	PromiseStream< GetTeamRequest > getTeam;
};

class ShardsAffectedByTeamFailure : public ReferenceCounted<ShardsAffectedByTeamFailure> {
public:
	ShardsAffectedByTeamFailure() {}
	typedef vector<UID> Team;  // sorted
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

	int getNumberOfShards( UID ssID );
	vector<KeyRange> getShardsFor( Team team );
	vector<vector<UID>> getTeamsFor( KeyRangeRef keys );
	void defineShard( KeyRangeRef keys );
	void moveShard( KeyRangeRef keys, Team destinationTeam );
	void check();
private:
	struct OrderByTeamKey {
		bool operator()( const std::pair<Team,KeyRange>& lhs, const std::pair<Team,KeyRange>& rhs ) const {
			if (lhs.first < rhs.first) return true;
			if (lhs.first > rhs.first) return false;
			return lhs.second.begin < rhs.second.begin;
		}
	};

	KeyRangeMap< vector<Team> > shard_teams;	// A shard can be affected by the failure of multiple teams if it is a queued merge
	std::set< std::pair<Team,KeyRange>, OrderByTeamKey > team_shards;
	std::map< UID, int > storageServerShards;

	void erase(Team team, KeyRange const& range);
	void insert(Team team, KeyRange const& range);
};

struct InitialDataDistribution : ReferenceCounted<InitialDataDistribution> {
	typedef vector<UID> Team;  // sorted
	int mode;
	vector<std::pair<StorageServerInterface, ProcessClass>> allServers;
	std::set< Team > teams;
	vector<KeyRangeWith<std::pair<Team, Team>>> shards;
};

Future<Void> dataDistribution(
	Reference<AsyncVar<struct ServerDBInfo>> const& db,
	MasterInterface const& mi, DatabaseConfiguration const& configuration,
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > const& serverChanges,
	Reference<ILogSystem> const& logSystem,
	Version const& recoveryCommitVersion,
	double* const& lastLimited);

Future<Void> dataDistributionTracker(
	Reference<InitialDataDistribution> const& initData,
	Database const& cx,
	Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
	PromiseStream<RelocateShard> const& output,
	PromiseStream<GetMetricsRequest> const& getShardMetrics,
	FutureStream<Promise<int64_t>> const& getAverageShardBytes,
	Promise<Void> const& readyToStart,
	UID const& masterId);

Future<Void> dataDistributionQueue(
	Database const& cx,
	PromiseStream<RelocateShard> const& input,
	PromiseStream<GetMetricsRequest> const& getShardMetrics,
	TeamCollectionInterface const& teamCollection,
	Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
	MoveKeysLock const& lock,
	PromiseStream<Promise<int64_t>> const& getAverageShardBytes,
	MasterInterface const& mi,
	int const& teamSize,
	int const& durableStorageQuorum,
	double* const& lastLimited );

//Holds the permitted size and IO Bounds for a shard
struct ShardSizeBounds {
	StorageMetrics max;
	StorageMetrics min;
	StorageMetrics permittedError;

	bool operator == ( ShardSizeBounds const& rhs ) const {
		return max == rhs.max && min == rhs.min && permittedError == rhs.permittedError;
	}
};

//Gets the permitted size and IO bounds for a shard
ShardSizeBounds getShardSizeBounds(KeyRangeRef shard, int64_t maxShardSize);

//Determines the maximum shard size based on the size of the database
int64_t getMaxShardSize( double dbSizeEstimate );