/*
 * ShardsAffectedByTeamFailure.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "ShardsAffectedByTeamFailure.h"
#include "fdbserver/core/Knobs.h"

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

int ShardsAffectedByTeamFailure::getNumberOfShards(Team team) const {
	int shardCount = 0;
	for (auto it = team_shards.lower_bound(std::pair<Team, KeyRange>(team, KeyRangeRef()));
	     it != team_shards.end() && it->first == team;
	     ++it)
		shardCount++;
	return shardCount;
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

void ShardsAffectedByTeamFailure::splitTrackedShardsAtBoundaries(KeyRangeRef keys) {
	// Recreate only the boundary points of `keys`. defineShard() must not be used here: it would merge all
	// tracked shards inside keys into one entry, losing the distinct destination teams of overlapping newer
	// moves. team_shards is keyed by the EXACT (team, range) pair, so a tracked shard that straddles a
	// boundary has to be removed from it before the split and its pieces re-added afterwards, or the
	// team_shards <-> shard_teams invariant (see check()) breaks.
	std::vector<KeyRange> rangesToSplit;
	auto beginRange = shard_teams.rangeContaining(keys.begin);
	if (beginRange->begin() != keys.begin) {
		rangesToSplit.push_back(beginRange->range());
	}
	auto endRange = shard_teams.rangeContaining(keys.end);
	if (endRange->begin() != keys.end && (rangesToSplit.empty() || rangesToSplit.back() != endRange->range())) {
		rangesToSplit.push_back(endRange->range());
	}
	if (rangesToSplit.empty()) {
		// Both boundaries already exist; modify() would be a no-op.
		return;
	}
	for (const auto& range : rangesToSplit) {
		for (const auto& team : shard_teams.rangeContaining(range.begin)->value().first) {
			erase(team, range);
		}
	}
	shard_teams.modify(keys);
	for (const auto& range : rangesToSplit) {
		for (auto splitRange : shard_teams.containedRanges(range)) {
			for (const auto& team : splitRange.value().first) {
				insert(team, splitRange.range());
			}
		}
	}
}

void ShardsAffectedByTeamFailure::moveShard(KeyRangeRef keys, std::vector<Team> destinationTeams) {
	/*TraceEvent("ShardsAffectedByTeamFailureMove")
	    .detail("KeyBegin", keys.begin)
	    .detail("KeyEnd", keys.end)
	    .detail("NewTeamSize", destinationTeam.size())
	    .detail("NewTeam", describe(destinationTeam));*/

	// A relocation is frequently launched for a STRICT SUB-RANGE of a tracked shard, for three independent
	// reasons. (1) A relocation's keys are captured when it is created, so an intervening defineShard() --
	// notably from a shard merge, which coarsens several tracked shards into one before sending its own
	// relocation -- can widen the tracked shard underneath a request that is still queued. (2) DDQueue
	// truncates already-queued relocations against overlapping newer ones (queueRelocation). (3)
	// launchQueuedWork starts a relocator per range returned by getRangesAffectedByInsertion, which includes
	// the pieces of a live relocator straddling the new range's boundaries, i.e. strict subsets of an older
	// relocation's range. Without a boundary split such a move hits the partial-overlap branch below, which
	// appends the destination team but never erases the drained source team -- so a source server stays counted
	// in storageServerShards forever, and a gracefully excluded server is never removable because its removal
	// gate requires getNumberOfShards(server) == 0. Splitting the tracked shard at the move's boundaries first
	// makes every affected shard fully contained, so the source teams are erased for exactly the range that
	// actually moved. The extra boundaries are transient: the next defineShard() from a shard-tracker
	// split/merge over a coarser range merges them back.
	if (SERVER_KNOBS->DD_SPLIT_TRACKED_SHARDS_ON_MOVE) {
		splitTrackedShardsAtBoundaries(keys);
	}

	auto ranges = shard_teams.intersectingRanges(keys);
	std::vector<std::pair<std::pair<std::vector<Team>, std::vector<Team>>, KeyRange>> modifiedShards;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		// After splitTrackedShardsAtBoundaries() every intersecting range is contained in keys, so the
		// partial-overlap branch below is unreachable. It is retained as a safety net rather than an assert
		// because losing a destination team in production is worse than over-counting a source team.
		ASSERT_WE_THINK(!SERVER_KNOBS->DD_SPLIT_TRACKED_SHARDS_ON_MOVE || keys.contains(it->range()));
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

std::vector<KeyRange> ShardsAffectedByTeamFailure::cancelMove(KeyRangeRef keys,
                                                              const std::vector<Team>& destinationTeams,
                                                              const std::vector<Team>& sourceTeams) {
	std::vector<KeyRange> restoredRanges;
	// A later shard split or merge can leave the cancelled move range strictly inside a tracked shard, so
	// recreate the move's boundary points before removing its destinations.
	splitTrackedShardsAtBoundaries(keys);
	auto ranges = shard_teams.containedRanges(keys);
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		std::vector<Team> retainedTeams;
		for (const auto& team : it->value().first) {
			if (std::find(destinationTeams.begin(), destinationTeams.end(), team) == destinationTeams.end()) {
				retainedTeams.push_back(team);
			}
		}
		if (retainedTeams.size() == it->value().first.size()) {
			continue;
		}

		KeyRange range = it->range();
		for (const auto& team : it->value().first) {
			erase(team, range);
		}
		const auto& replacementTeams = retainedTeams.empty() ? sourceTeams : retainedTeams;
		for (const auto& team : replacementTeams) {
			insert(team, range);
		}
		it->value().first = replacementTeams;
		if (retainedTeams.empty()) {
			it->value().second.clear();
		}
		restoredRanges.push_back(range);
	}
	check();
	return restoredRanges;
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
			if (i->range() != t->second ||
			    std::find(i->value().first.begin(), i->value().first.end(), t->first) == i->value().first.end()) {
				ASSERT(false);
			}
		}
		auto rs = shard_teams.ranges();
		for (auto i = rs.begin(); i != rs.end(); ++i) {
			for (auto t = i->value().first.begin(); t != i->value().first.end(); ++t) {
				if (!team_shards.contains(std::make_pair(*t, i->range()))) {
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
		for (auto t = it->value().first.begin(); t != it->value().first.end(); ++t) {
			removeFailedServerForSingleRange(*t, serverID, it->range());
		}
		// second team vector
		for (auto t = it->value().second.begin(); t != it->value().second.end(); ++t) {
			removeFailedServerForSingleRange(*t, serverID, it->range());
		}
	}
	check();
}

ShardsAffectedByTeamFailure::ScrubResult ShardsAffectedByTeamFailure::scrubServer(const UID& serverID) {
	auto containsServer = [&serverID](const std::vector<Team>& teams) {
		return std::any_of(teams.begin(), teams.end(), [&serverID](const Team& t) { return t.hasServer(serverID); });
	};
	auto without = [&serverID](const std::vector<Team>& teams) {
		std::vector<Team> retained;
		for (const auto& t : teams) {
			if (!t.hasServer(serverID)) {
				retained.push_back(t);
			}
		}
		return retained;
	};

	ScrubResult result;
	std::vector<KeyRange> restartRanges;
	// Values are mutated in place; no shard_teams range is inserted or erased, so the iteration stays valid.
	auto rs = shard_teams.ranges();
	for (auto it = rs.begin(); it != rs.end(); ++it) {
		++result.shardsScanned;
		auto& teams = it->value();
		const bool inCurrent = containsServer(teams.first);
		if (!inCurrent && !containsServer(teams.second)) {
			continue;
		}

		const KeyRange range = it->range();
		std::vector<Team> retained = without(teams.first);
		std::vector<Team> retainedPrev = without(teams.second);

		if (inCurrent) {
			// Only the current-team list is mirrored in team_shards (and therefore in
			// storageServerShards), so it is the only one that needs the erase/insert dance.
			for (const auto& t : teams.first) {
				erase(t, range);
			}
			if (retained.empty()) {
				retained = retainedPrev;
				retainedPrev.clear();
			}
			for (const auto& t : retained) {
				insert(t, range);
			}
			teams.first = retained;
			if (retained.empty()) {
				++result.ownerlessShards;
				restartRanges.push_back(range);
			}
		}
		// getSourceServerIdsFor() prefers the previous-source list, so a drained server left there would
		// still be handed out as a relocation source.
		teams.second = retainedPrev;

		if (result.shardsRewritten == 0) {
			result.sampleRange = range;
		}
		++result.shardsRewritten;
	}
	check();

	// Sent after the map is consistent: the receiver re-enters this object via defineShard().
	for (const auto& range : restartRanges) {
		restartShardTracker.send(range);
	}
	return result;
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
