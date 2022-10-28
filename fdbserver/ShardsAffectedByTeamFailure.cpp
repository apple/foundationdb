/*
 * ShardsAffectedByTeamFailure.cpp
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

#include "fdbserver/ShardsAffectedByTeamFailure.h"

std::vector<KeyRange> ShardsAffectedByTeamFailure::getShardsFor(Team team) const {
	std::vector<KeyRange> r;
	for (auto it = team_shards.lower_bound(std::pair<Team, KeyRange>(team, KeyRangeRef()));
	     it != team_shards.end() && it->first == team;
	     ++it)
		r.push_back(it->second);
	return r;
}

bool ShardsAffectedByTeamFailure::hasShards(Team team) const {
	auto it = team_shards.lower_bound(std::pair<Team, KeyRange>(team, KeyRangeRef()));
	return it != team_shards.end() && it->first == team;
}

int ShardsAffectedByTeamFailure::getNumberOfShards(UID ssID) const {
	auto it = storageServerShards.find(ssID);
	return it == storageServerShards.end() ? 0 : it->second;
}

std::pair<std::vector<ShardsAffectedByTeamFailure::Team>, std::vector<ShardsAffectedByTeamFailure::Team>>
ShardsAffectedByTeamFailure::getTeamsForFirstShard(KeyRangeRef keys) {
	return shard_teams[keys.begin];
}

std::pair<std::vector<ShardsAffectedByTeamFailure::Team>, std::vector<ShardsAffectedByTeamFailure::Team>>

ShardsAffectedByTeamFailure::getTeamsFor(KeyRef key) {
	return shard_teams[key];
}

void ShardsAffectedByTeamFailure::erase(Team team, KeyRange const& range) {
	DisabledTraceEvent(SevDebug, "ShardsAffectedByTeamFailureErase")
	    .detail("Range", range)
	    .detail("Team", team.toString());
	if (team_shards.erase(std::pair<Team, KeyRange>(team, range)) > 0) {
		for (auto uid = team.servers.begin(); uid != team.servers.end(); ++uid) {
			// Safeguard against going negative after eraseServer() sets value to 0
			if (storageServerShards[*uid] > 0) {
				storageServerShards[*uid]--;
			}
		}
	}
}

void ShardsAffectedByTeamFailure::insert(Team team, KeyRange const& range) {
	DisabledTraceEvent(SevDebug, "ShardsAffectedByTeamFailureInsert")
	    .detail("Range", range)
	    .detail("Team", team.toString());
	if (team_shards.insert(std::pair<Team, KeyRange>(team, range)).second) {
		for (auto uid = team.servers.begin(); uid != team.servers.end(); ++uid)
			storageServerShards[*uid]++;
	}
}

void ShardsAffectedByTeamFailure::defineShard(KeyRangeRef keys) {
	std::vector<Team> teams;
	std::vector<Team> prevTeams;
	auto rs = shard_teams.intersectingRanges(keys);
	for (auto it = rs.begin(); it != rs.end(); ++it) {
		for (auto t = it->value().first.begin(); t != it->value().first.end(); ++t) {
			teams.push_back(*t);
			erase(*t, it->range());
		}
		for (auto t = it->value().second.begin(); t != it->value().second.end(); ++t) {
			prevTeams.push_back(*t);
		}
	}
	uniquify(teams);
	uniquify(prevTeams);

	/*TraceEvent("ShardsAffectedByTeamFailureDefine")
	    .detail("KeyBegin", keys.begin)
	    .detail("KeyEnd", keys.end)
	    .detail("TeamCount", teams.size());*/

	auto affectedRanges = shard_teams.getAffectedRangesAfterInsertion(keys);
	shard_teams.insert(keys, std::make_pair(teams, prevTeams));

	for (auto r = affectedRanges.begin(); r != affectedRanges.end(); ++r) {
		auto& t = shard_teams[r->begin];
		for (auto it = t.first.begin(); it != t.first.end(); ++it) {
			insert(*it, *r);
		}
	}
	check();
}

// Move keys to destinationTeams by updating shard_teams
void ShardsAffectedByTeamFailure::moveShard(KeyRangeRef keys, std::vector<Team> destinationTeams) {
	/*TraceEvent("ShardsAffectedByTeamFailureMove")
	    .detail("KeyBegin", keys.begin)
	    .detail("KeyEnd", keys.end)
	    .detail("NewTeamSize", destinationTeam.size())
	    .detail("NewTeam", describe(destinationTeam));*/

	auto ranges = shard_teams.intersectingRanges(keys);
	std::vector<std::pair<std::pair<std::vector<Team>, std::vector<Team>>, KeyRange>> modifiedShards;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		if (keys.contains(it->range())) {
			// erase the many teams that were associated with this one shard
			for (auto t = it->value().first.begin(); t != it->value().first.end(); ++t) {
				erase(*t, it->range());
			}

			// save this modification for later insertion
			std::vector<Team> prevTeams = it->value().second;
			prevTeams.insert(prevTeams.end(), it->value().first.begin(), it->value().first.end());
			uniquify(prevTeams);

			modifiedShards.push_back(std::pair<std::pair<std::vector<Team>, std::vector<Team>>, KeyRange>(
			    std::make_pair(destinationTeams, prevTeams), it->range()));
		} else {
			// for each range that touches this move, add our team as affecting this range
			for (auto& team : destinationTeams) {
				insert(team, it->range());
			}

			// if we are not in the list of teams associated with this shard, add us in
			auto& teams = it->value();
			teams.second.insert(teams.second.end(), teams.first.begin(), teams.first.end());
			uniquify(teams.second);

			teams.first.insert(teams.first.end(), destinationTeams.begin(), destinationTeams.end());
			uniquify(teams.first);
		}
	}

	// we cannot modify the KeyRangeMap while iterating through it, so add saved modifications now
	for (int i = 0; i < modifiedShards.size(); i++) {
		for (auto& t : modifiedShards[i].first.first) {
			insert(t, modifiedShards[i].second);
		}
		shard_teams.insert(modifiedShards[i].second, modifiedShards[i].first);
	}

	check();
}

void ShardsAffectedByTeamFailure::finishMove(KeyRangeRef keys) {
	auto ranges = shard_teams.containedRanges(keys);
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		it.value().second.clear();
	}
}

void ShardsAffectedByTeamFailure::setCheckMode(CheckMode mode) {
	checkMode = mode;
}

void ShardsAffectedByTeamFailure::check() const {
	if (checkMode == CheckMode::ForceNoCheck)
		return;
	if (EXPENSIVE_VALIDATION || checkMode == CheckMode::ForceCheck) {
		for (auto t = team_shards.begin(); t != team_shards.end(); ++t) {
			auto i = shard_teams.rangeContaining(t->second.begin);
			if (i->range() != t->second || !std::count(i->value().first.begin(), i->value().first.end(), t->first)) {
				ASSERT(false);
			}
		}
		auto rs = shard_teams.ranges();
		for (auto i = rs.begin(); i != rs.end(); ++i) {
			for (auto t = i->value().first.begin(); t != i->value().first.end(); ++t) {
				if (!team_shards.count(std::make_pair(*t, i->range()))) {
					std::string teamDesc, shards;
					for (int k = 0; k < t->servers.size(); k++)
						teamDesc += format("%llx ", t->servers[k].first());
					for (auto x = team_shards.lower_bound(std::make_pair(*t, KeyRangeRef()));
					     x != team_shards.end() && x->first == *t;
					     ++x)
						shards += printable(x->second.begin) + "-" + printable(x->second.end) + ",";
					TraceEvent(SevError, "SATFInvariantError2")
					    .detail("KB", i->begin())
					    .detail("KE", i->end())
					    .detail("Team", teamDesc)
					    .detail("Shards", shards);
					ASSERT(false);
				}
			}
		}
	}
}

size_t ShardsAffectedByTeamFailure::getNumberOfShards() const {
	return shard_teams.size();
}

auto ShardsAffectedByTeamFailure::getAllRanges() const -> decltype(shard_teams)::ConstRanges {
	return shard_teams.ranges();
}

void ShardsAffectedByTeamFailure::assignRangeToTeams(KeyRangeRef keys, const std::vector<Team>& destinationTeam) {
	defineShard(keys);
	moveShard(keys, destinationTeam);
	finishMove(keys);
}

bool ShardsAffectedByTeamFailure::removeFailedServerForSingleRange(ShardsAffectedByTeamFailure::Team& team,
                                                                   const UID& id,
                                                                   KeyRangeRef keys) {
	if (team.hasServer(id)) {
		erase(team, keys);
		team.removeServer(id);
		insert(team, keys);
		return true;
	}
	return false;
}

void ShardsAffectedByTeamFailure::removeFailedServerForRange(KeyRangeRef keys, const UID& serverID) {
	auto rs = shard_teams.intersectingRanges(keys);
	for (auto it = rs.begin(); it != rs.end(); ++it) {
		// first team vector
		for (std::vector<Team>::iterator t = it->value().first.begin(); t != it->value().first.end(); ++t) {
			removeFailedServerForSingleRange(*t, serverID, it->range());
		}
		// second team vector
		for (std::vector<Team>::iterator t = it->value().second.begin(); t != it->value().second.end(); ++t) {
			removeFailedServerForSingleRange(*t, serverID, it->range());
		}
	}
	check();
}

auto ShardsAffectedByTeamFailure::intersectingRanges(KeyRangeRef keyRange) const -> decltype(shard_teams)::ConstRanges {
	return shard_teams.intersectingRanges(keyRange);
}

std::vector<UID> ShardsAffectedByTeamFailure::getSourceServerIdsFor(KeyRef key) {
	auto teamPair = getTeamsFor(key);
	std::set<UID> res;
	auto& srcTeams = teamPair.second.empty() ? teamPair.first : teamPair.second;
	for (auto& team : srcTeams) {
		res.insert(team.servers.begin(), team.servers.end());
	}
	return std::vector<UID>(res.begin(), res.end());
}
