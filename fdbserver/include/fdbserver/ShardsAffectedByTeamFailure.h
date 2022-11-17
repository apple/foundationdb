/*
 * ShardsAffectedByTeamFailure.h
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
#ifndef FOUNDATIONDB_SHARDSAFFECTEDBYTEAMFAILURE_H
#define FOUNDATIONDB_SHARDSAFFECTEDBYTEAMFAILURE_H

#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"

class ShardsAffectedByTeamFailure : public ReferenceCounted<ShardsAffectedByTeamFailure> {
public:
	ShardsAffectedByTeamFailure() {}

	enum class CheckMode { Normal = 0, ForceCheck, ForceNoCheck };

	struct Team {
		std::vector<UID> servers; // sorted
		bool primary;

		Team() : primary(true) {}
		Team(std::vector<UID> const& servers, bool primary) : servers(servers), primary(primary) {
			ASSERT(std::is_sorted(servers.begin(), servers.end()));
		}

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

		bool hasServer(const UID& id) const { return std::find(servers.begin(), servers.end(), id) != servers.end(); }

		bool removeServer(const UID& id) {
			auto oldSize = servers.size();
			servers.erase(std::remove(servers.begin(), servers.end(), id), servers.end());
			return oldSize != servers.size();
		}

		std::string toString() const { return describe(servers); };
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
	// shards. The second element of the pair is all previous sources for in-flight shards. This function only returns
	// the teams for the first shard in [keys.begin, keys.end)
	std::pair<std::vector<Team>, std::vector<Team>> getTeamsForFirstShard(KeyRangeRef keys);

	std::pair<std::vector<Team>, std::vector<Team>> getTeamsFor(KeyRef key);

	std::vector<UID> getSourceServerIdsFor(KeyRef key);

	// Shard boundaries are modified in defineShard and the content of what servers correspond to each shard is a copy
	// or union of the shards already there
	void defineShard(KeyRangeRef keys);
	// moveShard never change the shard boundary but just change the team value. Move keys to destinationTeams by
	// updating shard_teams, the old destination teams will be added to new source teams.
	void moveShard(KeyRangeRef keys, std::vector<Team> destinationTeam);
	// This function assume keys is exactly a shard in this mapping, this function set the srcTeam and destination
	// directly without retaining the old destination team info
	void rawMoveShard(KeyRangeRef keys, const std::vector<Team>& srcTeams, const std::vector<Team>& destinationTeam);
	// finishMove never change the shard boundary but just clear the old source team value
	void finishMove(KeyRangeRef keys);
	// a convenient function for (defineShard, moveShard, finishMove) pipeline
	void assignRangeToTeams(KeyRangeRef keys, const std::vector<Team>& destinationTeam);
	void check() const;
	void setCheckMode(CheckMode);

	PromiseStream<KeyRange> restartShardTracker;

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

	CheckMode checkMode = CheckMode::Normal;
	KeyRangeMap<std::pair<std::vector<Team>, std::vector<Team>>>
	    shard_teams; // A shard can be affected by the failure of multiple teams if it is a queued merge, or when
	                 // usable_regions > 1
	std::set<std::pair<Team, KeyRange>, OrderByTeamKey> team_shards;
	std::map<UID, int> storageServerShards;

	// only erase from team_shards
	void erase(Team team, KeyRange const& range);
	// only insert into team_shards
	void insert(Team team, KeyRange const& range);

	bool removeFailedServerForSingleRange(ShardsAffectedByTeamFailure::Team& team, const UID& id, KeyRangeRef keys);

public:
	// return the iterator that traversing all ranges
	auto getAllRanges() const -> decltype(shard_teams)::ConstRanges;
	auto intersectingRanges(KeyRangeRef keyRange) const -> decltype(shard_teams)::ConstRanges;
	// get total shards count
	size_t getNumberOfShards() const;
	void removeFailedServerForRange(KeyRangeRef keys, const UID& serverID);
};

#endif // FOUNDATIONDB_SHARDSAFFECTEDBYTEAMFAILURE_H
