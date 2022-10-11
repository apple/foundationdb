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
