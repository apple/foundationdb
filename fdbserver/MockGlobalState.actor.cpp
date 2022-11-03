/*
 * MockGlobalState.actor.cpp
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
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/DataDistribution.actor.h"
#include "flow/actorcompiler.h"

class MockGlobalStateImpl {
public:
	ACTOR static Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(MockGlobalState* mgs,
	                                                                                 KeyRange keys,
	                                                                                 StorageMetrics min,
	                                                                                 StorageMetrics max,
	                                                                                 StorageMetrics permittedError,
	                                                                                 int shardLimit,
	                                                                                 int expectedShardCount) {
		state TenantInfo tenantInfo;
		loop {
			auto locations = mgs->getKeyRangeLocations(tenantInfo,
			                                           keys,
			                                           shardLimit,
			                                           Reverse::False,
			                                           SpanContext(),
			                                           Optional<UID>(),
			                                           UseProvisionalProxies::False,
			                                           0)
			                     .get();
			TraceEvent(SevDebug, "MGSWaitStorageMetrics").detail("Phase", "GetLocation");
			// NOTE(xwang): in native API, there's code handling the non-equal situation, but I think in mock world
			// there shouldn't have any delay to update the locations.
			ASSERT_EQ(expectedShardCount, locations.size());

			Optional<StorageMetrics> res =
			    wait(::waitStorageMetricsWithLocation(tenantInfo, keys, locations, min, max, permittedError));

			if (res.present()) {
				return std::make_pair(res, -1);
			}
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
		}
	}

	// SOMEDAY: reuse the NativeAPI implementation
	ACTOR static Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(MockGlobalState* mgs,
	                                                                       KeyRange keys,
	                                                                       StorageMetrics limit,
	                                                                       StorageMetrics estimated,
	                                                                       Optional<int> minSplitBytes) {
		state TenantInfo tenantInfo;
		loop {
			state std::vector<KeyRangeLocationInfo> locations =
			    mgs->getKeyRangeLocations(tenantInfo,
			                              keys,
			                              CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT,
			                              Reverse::False,
			                              SpanContext(),
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              0)
			        .get();

			// Same solution to NativeAPI::splitStorageMetrics, wait some merge finished
			if (locations.size() == CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT) {
				wait(delay(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskPriority::DataDistribution));
			}

			Optional<Standalone<VectorRef<KeyRef>>> results =
			    wait(splitStorageMetricsWithLocations(locations, keys, limit, estimated, minSplitBytes));

			if (results.present()) {
				return results.get();
			}

			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
		}
	}
};

class MockStorageServerImpl {
public:
	ACTOR static Future<Void> waitMetricsTenantAware(MockStorageServer* self, WaitMetricsRequest req) {
		if (req.tenantInfo.present() && req.tenantInfo.get().tenantId != TenantInfo::INVALID_TENANT) {
			// TODO(xwang) add support for tenant test, search for tenant entry
			Optional<TenantMapEntry> entry;
			Optional<Key> tenantPrefix = entry.map<Key>([](TenantMapEntry e) { return e.prefix; });
			if (tenantPrefix.present()) {
				UNREACHABLE();
				// req.keys = req.keys.withPrefix(tenantPrefix.get(), req.arena);
			}
		}

		if (!self->isReadable(req.keys)) {
			self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
		} else {
			wait(self->metrics.waitMetrics(req, delayJittered(SERVER_KNOBS->STORAGE_METRIC_TIMEOUT)));
		}
		return Void();
	}
};

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

void MockStorageServer::addActor(Future<Void> future) {
	actors.add(future);
}

void MockStorageServer::getSplitPoints(const SplitRangeRequest& req) {}

Future<Void> MockStorageServer::waitMetricsTenantAware(const WaitMetricsRequest& req) {
	return MockStorageServerImpl::waitMetricsTenantAware(this, req);
}

void MockStorageServer::getStorageMetrics(const GetStorageMetricsRequest& req) {}

Future<Void> MockStorageServer::run() {
	ssi.locality = LocalityData(Optional<Standalone<StringRef>>(),
	                            Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                            Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                            Optional<Standalone<StringRef>>());
	ssi.initEndpoints();
	ssi.startAcceptingRequests();
	TraceEvent("MockStorageServerStart").detail("Address", ssi.address());
	return serveStorageMetricsRequests(this, ssi);
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
	auto teams = shardMapping->getTeamsForFirstShard(shard);
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
	auto teams = shardMapping->getTeamsForFirstShard(shard);
	return !teams.second.empty() && std::any_of(teams.first.begin(), teams.first.end(), [&serverId](const Team& team) {
		return team.hasServer(serverId);
	});
}

bool MockGlobalState::allShardsRemovedFromServer(const UID& serverId) {
	return allServers.count(serverId) && shardMapping->getNumberOfShards(serverId) == 0;
}

Future<std::pair<Optional<StorageMetrics>, int>> MockGlobalState::waitStorageMetrics(
    const KeyRange& keys,
    const StorageMetrics& min,
    const StorageMetrics& max,
    const StorageMetrics& permittedError,
    int shardLimit,
    int expectedShardCount) {
	return MockGlobalStateImpl::waitStorageMetrics(
	    this, keys, min, max, permittedError, shardLimit, expectedShardCount);
}

Reference<LocationInfo> buildLocationInfo(const std::vector<StorageServerInterface>& interfaces) {
	// construct the location info with the servers
	std::vector<Reference<ReferencedInterface<StorageServerInterface>>> serverRefs;
	serverRefs.reserve(interfaces.size());
	for (const auto& interf : interfaces) {
		serverRefs.push_back(makeReference<ReferencedInterface<StorageServerInterface>>(interf));
	}

	return makeReference<LocationInfo>(serverRefs);
}

Future<KeyRangeLocationInfo> MockGlobalState::getKeyLocation(TenantInfo tenant,
                                                             Key key,
                                                             SpanContext spanContext,
                                                             Optional<UID> debugID,
                                                             UseProvisionalProxies useProvisionalProxies,
                                                             Reverse isBackward,
                                                             Version version) {
	if (isBackward) {
		// DD never ask for backward range.
		UNREACHABLE();
	}
	ASSERT(key < allKeys.end);

	GetKeyServerLocationsReply rep;
	KeyRange single = singleKeyRange(key);
	auto teamPair = shardMapping->getTeamsForFirstShard(single);
	auto& srcTeam = teamPair.second.empty() ? teamPair.first : teamPair.second;
	ASSERT_EQ(srcTeam.size(), 1);
	rep.results.emplace_back(single, extractStorageServerInterfaces(srcTeam.front().servers));

	return KeyRangeLocationInfo(
	    rep.tenantEntry,
	    KeyRange(toPrefixRelativeRange(rep.results[0].first, rep.tenantEntry.prefix), rep.arena),
	    buildLocationInfo(rep.results[0].second));
}

Future<std::vector<KeyRangeLocationInfo>> MockGlobalState::getKeyRangeLocations(
    TenantInfo tenant,
    KeyRange keys,
    int limit,
    Reverse reverse,
    SpanContext spanContext,
    Optional<UID> debugID,
    UseProvisionalProxies useProvisionalProxies,
    Version version) {

	if (reverse) {
		// DD never ask for backward range.
		ASSERT(false);
	}
	ASSERT(keys.begin < keys.end);

	GetKeyServerLocationsReply rep;
	auto ranges = shardMapping->intersectingRanges(keys);
	auto it = ranges.begin();
	for (int count = 0; it != ranges.end() && count < limit; ++it, ++count) {
		auto teamPair = shardMapping->getTeamsFor(it->begin());
		auto& srcTeam = teamPair.second.empty() ? teamPair.first : teamPair.second;
		ASSERT_EQ(srcTeam.size(), 1);
		rep.results.emplace_back(it->range(), extractStorageServerInterfaces(srcTeam.front().servers));
	}
	CODE_PROBE(it != ranges.end(), "getKeyRangeLocations is limited", probe::decoration::rare);

	std::vector<KeyRangeLocationInfo> results;
	for (int shard = 0; shard < rep.results.size(); shard++) {
		results.emplace_back(rep.tenantEntry,
		                     (toPrefixRelativeRange(rep.results[shard].first, rep.tenantEntry.prefix) & keys),
		                     buildLocationInfo(rep.results[shard].second));
	}
	return results;
}

std::vector<StorageServerInterface> MockGlobalState::extractStorageServerInterfaces(const std::vector<UID>& ids) const {
	std::vector<StorageServerInterface> interfaces;
	for (auto& id : ids) {
		interfaces.emplace_back(allServers.at(id).ssi);
	}
	return interfaces;
}

Future<Standalone<VectorRef<KeyRef>>> MockGlobalState::splitStorageMetrics(const KeyRange& keys,
                                                                           const StorageMetrics& limit,
                                                                           const StorageMetrics& estimated,
                                                                           const Optional<int>& minSplitBytes) {
	return MockGlobalStateImpl::splitStorageMetrics(this, keys, limit, estimated, minSplitBytes);
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

	KeyRangeLocationInfo getKeyLocationInfo(KeyRef key, std::shared_ptr<MockGlobalState> mgs) {
		return mgs
		    ->getKeyLocation(
		        TenantInfo(), key, SpanContext(), Optional<UID>(), UseProvisionalProxies::False, Reverse::False, 0)
		    .get();
	}

	std::vector<KeyRangeLocationInfo> getKeyRangeLocations(KeyRangeRef keys,
	                                                       int limit,
	                                                       std::shared_ptr<MockGlobalState> mgs) {
		return mgs
		    ->getKeyRangeLocations(TenantInfo(),
		                           keys,
		                           limit,
		                           Reverse::False,
		                           SpanContext(),
		                           Optional<UID>(),
		                           UseProvisionalProxies::False,
		                           0)
		    .get();
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

namespace {
inline bool locationInfoEqualsToTeam(Reference<LocationInfo> loc, const std::vector<UID>& ids) {
	return loc->locations()->size() == ids.size() &&
	       std::all_of(ids.begin(), ids.end(), [loc](const UID& id) { return loc->locations()->hasInterface(id); });
}
}; // namespace
TEST_CASE("/MockGlobalState/MockStorageServer/GetKeyLocations") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	DatabaseConfiguration dbConfig = generateNormalDatabaseConfiguration(testConfig);
	TraceEvent("UnitTestDbConfig").detail("Config", dbConfig.toString());

	auto mgs = std::make_shared<MockGlobalState>();
	mgs->initializeAsEmptyDatabaseMGS(dbConfig);
	// add one empty server
	mgs->addStorageServer(StorageServerInterface(mgs->indexToUID(mgs->allServers.size() + 1)));

	// define 3 ranges:
	// team 1 (UID 1,2,...,n-1):[begin, 1.0), [2.0, end)
	// team 2 (UID 2,3,...n-1, n): [1.0, 2.0)
	ShardsAffectedByTeamFailure::Team team1, team2;
	for (int i = 0; i < mgs->allServers.size() - 1; ++i) {
		UID id = mgs->indexToUID(i + 1);
		team1.servers.emplace_back(id);
		id = mgs->indexToUID(i + 2);
		team2.servers.emplace_back(id);
	}
	Key one = doubleToTestKey(1.0), two = doubleToTestKey(2.0);
	std::vector<KeyRangeRef> ranges{ KeyRangeRef(allKeys.begin, one),
		                             KeyRangeRef(one, two),
		                             KeyRangeRef(two, allKeys.end) };
	mgs->shardMapping->assignRangeToTeams(ranges[0], { team1 });
	mgs->shardMapping->assignRangeToTeams(ranges[1], { team2 });
	mgs->shardMapping->assignRangeToTeams(ranges[2], { team1 });

	// query key location
	MockGlobalStateTester tester;
	// -- team 1
	Key testKey = doubleToTestKey(0.5);
	auto locInfo = tester.getKeyLocationInfo(testKey, mgs);
	ASSERT(locationInfoEqualsToTeam(locInfo.locations, team1.servers));

	// -- team 2
	testKey = doubleToTestKey(1.3);
	locInfo = tester.getKeyLocationInfo(testKey, mgs);
	ASSERT(locationInfoEqualsToTeam(locInfo.locations, team2.servers));

	// query range location
	testKey = doubleToTestKey(3.0);
	// team 1,2,1
	auto locInfos = tester.getKeyRangeLocations(KeyRangeRef(allKeys.begin, testKey), 100, mgs);
	ASSERT(locInfos.size() == 3);
	ASSERT(locInfos[0].range == ranges[0]);
	ASSERT(locationInfoEqualsToTeam(locInfos[0].locations, team1.servers));
	ASSERT(locInfos[1].range == ranges[1]);
	ASSERT(locationInfoEqualsToTeam(locInfos[1].locations, team2.servers));
	ASSERT(locInfos[2].range == KeyRangeRef(ranges[2].begin, testKey));
	ASSERT(locationInfoEqualsToTeam(locInfos[2].locations, team1.servers));

	// team 1,2
	locInfos = tester.getKeyRangeLocations(KeyRangeRef(allKeys.begin, testKey), 2, mgs);
	ASSERT(locInfos.size() == 2);
	ASSERT(locInfos[0].range == ranges[0]);
	ASSERT(locationInfoEqualsToTeam(locInfos[0].locations, team1.servers));
	ASSERT(locInfos[1].range == ranges[1]);
	ASSERT(locationInfoEqualsToTeam(locInfos[1].locations, team2.servers));

	return Void();
}

TEST_CASE("/MockGlobalState/MockStorageServer/WaitStorageMetricsRequest") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	DatabaseConfiguration dbConfig = generateNormalDatabaseConfiguration(testConfig);
	TraceEvent("WaitStorageMetricsRequestUnitTestConfig").detail("Config", dbConfig.toString());

	state std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeAsEmptyDatabaseMGS(dbConfig);
	state ActorCollection actors;

	ActorCollection* ptr = &actors; // get around ACTOR syntax restriction
	std::for_each(mgs->allServers.begin(), mgs->allServers.end(), [ptr](auto& server) {
		ptr->add(server.second.run());
		IFailureMonitor::failureMonitor().setStatus(server.second.ssi.address(), FailureStatus(false));
		server.second.metrics.byteSample.sample.insert("something"_sr, 500000);
	});

	KeyRange testRange = allKeys;
	ShardSizeBounds bounds = ShardSizeBounds::shardSizeBoundsBeforeTrack();
	std::pair<Optional<StorageMetrics>, int> res =
	    wait(mgs->waitStorageMetrics(testRange, bounds.min, bounds.max, bounds.permittedError, 1, 1));
	// std::cout << "get result " << res.second << "\n";
	// std::cout << "get byte "<< res.first.get().bytes << "\n";
	ASSERT_EQ(res.second, -1); // the valid result always return -1, strange contraction though.
	ASSERT_EQ(res.first.get().bytes, 500000);
	return Void();
}
