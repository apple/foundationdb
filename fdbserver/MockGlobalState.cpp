/*
 * MockGlobalState.cpp
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

#include "fdbserver/MockGlobalState.h"

bool MockStorageServer::allShardStatusEqual(KeyRangeRef range, MockShardStatus status) {
	auto ranges = serverKeys.intersectingRanges(range);
	ASSERT(!ranges.empty()); // at least the range is allKeys

	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		if (it->cvalue().status != status)
			return false;
	}
	return true;
}

void MockStorageServer::setShardStatus(KeyRangeRef range, MockShardStatus status) {
	auto ranges = serverKeys.intersectingRanges(range);
	ASSERT(!ranges.empty());
	if (ranges.begin().range().contains(range)) {
		CODE_PROBE(true, "Implicitly split single shard to 3 pieces");
		threeWayShardSplitting(ranges.begin().range(), range, ranges.begin().cvalue().shardSize);
		return;
	}
	if (ranges.begin().begin() < range.begin) {
		CODE_PROBE(true, "Implicitly split begin range to 2 pieces");
		twoWayShardSplitting(ranges.begin().range(), range.begin, ranges.begin().cvalue().shardSize);
	}
	if (ranges.end().end() > range.end) {
		CODE_PROBE(true, "Implicitly split end range to 2 pieces");
		twoWayShardSplitting(ranges.end().range(), range.end, ranges.end().cvalue().shardSize);
	}
	ranges = serverKeys.containedRanges(range);
	// now the boundary must be aligned
	ASSERT(ranges.begin().begin() == range.begin);
	ASSERT(ranges.end().end() == range.end);
	uint64_t newSize = 0;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		newSize += it->cvalue().shardSize;
	}
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		it.value() = ShardInfo{ status, newSize };
	}
	serverKeys.coalesce(range);
}

// split the out range [a, d) based on the inner range's boundary [b, c). The result would be [a,b), [b,c), [c,d). The
// size of the new shards are randomly split from old size of [a, d)
void MockStorageServer::threeWayShardSplitting(KeyRangeRef outerRange,
                                               KeyRangeRef innerRange,
                                               uint64_t outerRangeSize) {
	ASSERT(outerRange.contains(innerRange));

	Key left = outerRange.begin;
	// random generate 3 shard sizes, the caller guarantee that the min, max parameters are always valid.
	int leftSize = deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES,
	                                                outerRangeSize - 2 * SERVER_KNOBS->MIN_SHARD_BYTES + 1);
	int midSize = deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES,
	                                               outerRangeSize - leftSize - SERVER_KNOBS->MIN_SHARD_BYTES + 1);
	int rightSize = outerRangeSize - leftSize - midSize;

	serverKeys.insert(innerRange, { MockShardStatus::UNSET, (uint64_t)midSize });
	serverKeys[left].shardSize = leftSize;
	serverKeys[innerRange.end].shardSize = rightSize;
}

// split the range [a,c) with split point b. The result would be [a, b), [b, c). The
// size of the new shards are randomly split from old size of [a, c)
void MockStorageServer::twoWayShardSplitting(KeyRangeRef range, KeyRef splitPoint, uint64_t rangeSize) {
	Key left = range.begin;
	// random generate 3 shard sizes, the caller guarantee that the min, max parameters are always valid.
	int leftSize =
	    deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, rangeSize - SERVER_KNOBS->MIN_SHARD_BYTES + 1);
	int rightSize = rangeSize - leftSize;
	serverKeys.rawInsert(splitPoint, { MockShardStatus::UNSET, (uint64_t)rightSize });
	serverKeys[left].shardSize = leftSize;
}

void MockGlobalState::initializeAsEmptyDatabaseMGS(const DatabaseConfiguration& conf, uint64_t defaultDiskSpace) {
	ASSERT(conf.storageTeamSize > 0);
	configuration = conf;
	std::vector<UID> serverIds;
	for (int i = 1; i <= conf.storageTeamSize; ++i) {
		UID id = indexToUID(i);
		serverIds.push_back(id);
		allServers[id] = MockStorageServer(id, defaultDiskSpace);
		allServers[id].serverKeys.insert(allKeys, { MockShardStatus::COMPLETED, 0 });
	}
	shardMapping->assignRangeToTeams(allKeys, { Team(serverIds, true) });
}

void MockGlobalState::addStorageServer(StorageServerInterface server, uint64_t diskSpace) {
	allServers[server.id()] = MockStorageServer(server, diskSpace);
}

bool MockGlobalState::serverIsSourceForShard(const UID& serverId, KeyRangeRef shard, bool inFlightShard) {
	if (!allServers.count(serverId))
		return false;

	// check serverKeys
	auto& mss = allServers.at(serverId);
	if (!mss.allShardStatusEqual(shard, MockShardStatus::COMPLETED)) {
		return false;
	}

	// check keyServers
	auto teams = shardMapping->getTeamsFor(shard);
	if (inFlightShard) {
		return std::any_of(teams.second.begin(), teams.second.end(), [&serverId](const Team& team) {
			return team.hasServer(serverId);
		});
	}
	return std::any_of(
	    teams.first.begin(), teams.first.end(), [&serverId](const Team& team) { return team.hasServer(serverId); });
}

bool MockGlobalState::serverIsDestForShard(const UID& serverId, KeyRangeRef shard) {
	if (!allServers.count(serverId))
		return false;

	// check serverKeys
	auto& mss = allServers.at(serverId);
	if (!mss.allShardStatusEqual(shard, MockShardStatus::INFLIGHT)) {
		return false;
	}

	// check keyServers
	auto teams = shardMapping->getTeamsFor(shard);
	return !teams.second.empty() && std::any_of(teams.first.begin(), teams.first.end(), [&serverId](const Team& team) {
		return team.hasServer(serverId);
	});
}

bool MockGlobalState::allShardRemovedFromServer(const UID& serverId) {
	return allServers.count(serverId) && shardMapping->getNumberOfShards(serverId) == 0;
}

TEST_CASE("/MockGlobalState/initializeAsEmptyDatabaseMGS/SimpleThree") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 3;
	testConfig.logAntiQuorum = 0;
	DatabaseConfiguration dbConfig = generateNormalDatabaseConfiguration(testConfig);
	TraceEvent("UnitTestDbConfig").detail("Config", dbConfig.toString());

	auto mgs = std::make_shared<MockGlobalState>();
	mgs->initializeAsEmptyDatabaseMGS(dbConfig);
	for (int i = 1; i <= dbConfig.storageTeamSize; ++i) {
		auto id = MockGlobalState::indexToUID(i);
		std::cout << "Check server " << i << "\n";
		ASSERT(mgs->serverIsSourceForShard(id, allKeys));
		ASSERT(mgs->allServers.at(id).serverKeys.sumRange(allKeys.begin, allKeys.end) == 0);
	}

	return Void();
}
