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

void MockStorageServer::setShardStatus(KeyRangeRef range, MockShardStatus status, bool restrictSize) {
	auto ranges = serverKeys.intersectingRanges(range);
	ASSERT(!ranges.empty());
	if (ranges.begin().range().contains(range)) {
		CODE_PROBE(true, "Implicitly split single shard to 3 pieces");
		threeWayShardSplitting(ranges.begin().range(), range, ranges.begin().cvalue().shardSize, restrictSize);
		return;
	}
	if (ranges.begin().begin() < range.begin) {
		CODE_PROBE(true, "Implicitly split begin range to 2 pieces");
		twoWayShardSplitting(ranges.begin().range(), range.begin, ranges.begin().cvalue().shardSize, restrictSize);
	}
	if (ranges.end().end() > range.end) {
		CODE_PROBE(true, "Implicitly split end range to 2 pieces");
		twoWayShardSplitting(ranges.end().range(), range.end, ranges.end().cvalue().shardSize, restrictSize);
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
		auto oldStatus = it.value().status;
		if (isStatusTransitionValid(oldStatus, status)) {
			it.value() = ShardInfo{ status, newSize };
		} else if (oldStatus == MockShardStatus::COMPLETED && status == MockShardStatus::INFLIGHT) {
			CODE_PROBE(true, "Shard already on server");
		} else {
			TraceEvent(SevError, "MockShardStatusTransitionError")
			    .detail("From", oldStatus)
			    .detail("To", status)
			    .detail("ID", id)
			    .detail("KeyBegin", range.begin.toHexString())
			    .detail("KeyEnd", range.begin.toHexString());
		}
	}
	serverKeys.coalesce(range);
}

// split the out range [a, d) based on the inner range's boundary [b, c). The result would be [a,b), [b,c), [c,d). The
// size of the new shards are randomly split from old size of [a, d)
void MockStorageServer::threeWayShardSplitting(KeyRangeRef outerRange,
                                               KeyRangeRef innerRange,
                                               uint64_t outerRangeSize,
                                               bool restrictSize) {
	ASSERT(outerRange.contains(innerRange));

	Key left = outerRange.begin;
	// random generate 3 shard sizes, the caller guarantee that the min, max parameters are always valid.
	int leftSize = deterministicRandom()->randomInt(
	    SERVER_KNOBS->MIN_SHARD_BYTES,
	    restrictSize ? outerRangeSize - 2 * SERVER_KNOBS->MIN_SHARD_BYTES + 1 : SERVER_KNOBS->MAX_SHARD_BYTES);
	int midSize = deterministicRandom()->randomInt(
	    SERVER_KNOBS->MIN_SHARD_BYTES,
	    restrictSize ? outerRangeSize - leftSize - SERVER_KNOBS->MIN_SHARD_BYTES + 1 : SERVER_KNOBS->MAX_SHARD_BYTES);
	int rightSize =
	    restrictSize ? outerRangeSize - leftSize - midSize
	                 : deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, SERVER_KNOBS->MAX_SHARD_BYTES);

	serverKeys.insert(innerRange, { serverKeys[left].status, (uint64_t)midSize });
	serverKeys[left].shardSize = leftSize;
	serverKeys[innerRange.end].shardSize = rightSize;
}

// split the range [a,c) with split point b. The result would be [a, b), [b, c). The
// size of the new shards are randomly split from old size of [a, c)
void MockStorageServer::twoWayShardSplitting(KeyRangeRef range,
                                             KeyRef splitPoint,
                                             uint64_t rangeSize,
                                             bool restrictSize) {
	Key left = range.begin;
	// random generate 3 shard sizes, the caller guarantee that the min, max parameters are always valid.
	int leftSize = deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES,
	                                                restrictSize ? rangeSize - SERVER_KNOBS->MIN_SHARD_BYTES + 1
	                                                             : SERVER_KNOBS->MAX_SHARD_BYTES);
	int rightSize =
	    restrictSize ? rangeSize - leftSize
	                 : deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, SERVER_KNOBS->MAX_SHARD_BYTES);
	serverKeys.rawInsert(splitPoint, { serverKeys[left].status, (uint64_t)rightSize });
	serverKeys[left].shardSize = leftSize;
}

void MockStorageServer::removeShard(KeyRangeRef range) {
	auto ranges = serverKeys.containedRanges(range);
	ASSERT(ranges.begin().range() == range);
	serverKeys.rawErase(range);
}

uint64_t MockStorageServer::sumRangeSize(KeyRangeRef range) const {
	auto ranges = serverKeys.intersectingRanges(range);
	uint64_t totalSize = 0;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		totalSize += it->cvalue().shardSize;
	}
	return totalSize;
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
		ASSERT(mgs->allServers.at(id).sumRangeSize(allKeys) == 0);
	}

	return Void();
}

struct MockGlobalStateTester {

	// expectation [r0.begin, r0.end) => [r0.begin, x1), [x1, x2), [x2, r0.end)
	void testThreeWaySplitFirstRange(MockStorageServer& mss) {
		auto it = mss.serverKeys.ranges().begin();
		uint64_t oldSize =
		    deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, std::numeric_limits<int>::max());
		MockShardStatus oldStatus = it.cvalue().status;
		it->value().shardSize = oldSize;
		KeyRangeRef outerRange = it->range();
		Key x1 = keyAfter(it->range().begin);
		Key x2 = keyAfter(x1);
		std::cout << "it->range.begin: " << it->range().begin.toHexString() << " size: " << oldSize << "\n";

		mss.threeWayShardSplitting(outerRange, KeyRangeRef(x1, x2), oldSize, false);
		auto ranges = mss.serverKeys.containedRanges(outerRange);
		ASSERT(ranges.begin().range() == KeyRangeRef(outerRange.begin, x1));
		ranges.pop_front();
		ASSERT(ranges.begin().range() == KeyRangeRef(x1, x2));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.begin().range() == KeyRangeRef(x2, outerRange.end));
		ranges.pop_front();
		ASSERT(ranges.empty());
	}

	// expectation [r0.begin, r0.end) => [r0.begin, x1), [x1, r0.end)
	void testTwoWaySplitFirstRange(MockStorageServer& mss) {
		auto it = mss.serverKeys.nthRange(0);
		MockShardStatus oldStatus = it.cvalue().status;
		uint64_t oldSize =
		    deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, std::numeric_limits<int>::max());
		it->value().shardSize = oldSize;
		KeyRangeRef outerRange = it->range();
		Key x1 = keyAfter(it->range().begin);
		std::cout << "it->range.begin: " << it->range().begin.toHexString() << " size: " << oldSize << "\n";

		mss.twoWayShardSplitting(it->range(), x1, oldSize, false);
		auto ranges = mss.serverKeys.containedRanges(outerRange);
		ASSERT(ranges.begin().range() == KeyRangeRef(outerRange.begin, x1));
		ranges.pop_front();
		ASSERT(ranges.begin().range() == KeyRangeRef(x1, outerRange.end));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.empty());
	}
};

TEST_CASE("/MockGlobalState/MockStorageServer/SplittingFunctions") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	DatabaseConfiguration dbConfig = generateNormalDatabaseConfiguration(testConfig);
	TraceEvent("UnitTestDbConfig").detail("Config", dbConfig.toString());

	auto mgs = std::make_shared<MockGlobalState>();
	mgs->initializeAsEmptyDatabaseMGS(dbConfig);

	MockGlobalStateTester tester;
	auto& mss = mgs->allServers.at(MockGlobalState::indexToUID(1));
	std::cout << "Test 3-way splitting...\n";
	tester.testThreeWaySplitFirstRange(mss);
	std::cout << "Test 2-way splitting...\n";
	mss.serverKeys.insert(allKeys, { MockShardStatus::COMPLETED, 0 }); // reset to empty
	tester.testTwoWaySplitFirstRange(mss);

	return Void();
}
