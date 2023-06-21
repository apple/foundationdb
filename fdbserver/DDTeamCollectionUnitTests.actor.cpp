/*
 * DDTeamCollectionUnitTests.actor.cpp
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

#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/ExclusionTracker.actor.h"
#include "fdbserver/DataDistributionTeam.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <climits>
#include <unordered_set>

class DDTeamCollectionUnitTest {
public:
	static std::unique_ptr<DDTeamCollection> testTeamCollection(int teamSize,
	                                                            Reference<IReplicationPolicy> policy,
	                                                            int processCount) {
		Database database = DatabaseContext::create(
		    makeReference<AsyncVar<ClientDBInfo>>(), Never(), LocalityData(), EnableLocalityLoadBalance::False);
		auto txnProcessor = Reference<IDDTxnProcessor>(new DDTxnProcessor(database));
		DatabaseConfiguration conf;
		conf.storageTeamSize = teamSize;
		conf.storagePolicy = policy;

		auto collection = std::unique_ptr<DDTeamCollection>(
		    new DDTeamCollection(DDTeamCollectionInitParams{ txnProcessor,
		                                                     UID(0, 0),
		                                                     MoveKeysLock(),
		                                                     PromiseStream<RelocateShard>(),
		                                                     makeReference<ShardsAffectedByTeamFailure>(),
		                                                     conf,
		                                                     {},
		                                                     {},
		                                                     Future<Void>(Void()),
		                                                     makeReference<AsyncVar<bool>>(true),
		                                                     IsPrimary::True,
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     PromiseStream<GetMetricsRequest>(),
		                                                     Promise<UID>(),
		                                                     PromiseStream<Promise<int>>(),
		                                                     PromiseStream<Promise<int64_t>>() }));

		for (int id = 1; id <= processCount; ++id) {
			UID uid(id, 0);
			StorageServerInterface interface;
			interface.uniqueID = uid;
			interface.locality.set("machineid"_sr, Standalone<StringRef>(std::to_string(id)));
			interface.locality.set("zoneid"_sr, Standalone<StringRef>(std::to_string(id % 5)));
			interface.locality.set("data_hall"_sr, Standalone<StringRef>(std::to_string(id % 3)));
			collection->server_info[uid] = makeReference<TCServerInfo>(
			    interface, collection.get(), ProcessClass(), true, collection->storageServerSet);
			collection->server_status.set(uid, ServerStatus(false, false, false, interface.locality));
			collection->checkAndCreateMachine(collection->server_info[uid]);
		}

		return collection;
	}

	static std::unique_ptr<DDTeamCollection> testMachineTeamCollection(int teamSize,
	                                                                   Reference<IReplicationPolicy> policy,
	                                                                   int processCount) {
		Database database = DatabaseContext::create(
		    makeReference<AsyncVar<ClientDBInfo>>(), Never(), LocalityData(), EnableLocalityLoadBalance::False);
		auto txnProcessor = Reference<IDDTxnProcessor>(new DDTxnProcessor(database));
		DatabaseConfiguration conf;
		conf.storageTeamSize = teamSize;
		conf.storagePolicy = policy;

		auto collection = std::unique_ptr<DDTeamCollection>(
		    new DDTeamCollection(DDTeamCollectionInitParams{ txnProcessor,
		                                                     UID(0, 0),
		                                                     MoveKeysLock(),
		                                                     PromiseStream<RelocateShard>(),
		                                                     makeReference<ShardsAffectedByTeamFailure>(),
		                                                     conf,
		                                                     {},
		                                                     {},
		                                                     Future<Void>(Void()),
		                                                     makeReference<AsyncVar<bool>>(true),
		                                                     IsPrimary::True,
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     PromiseStream<GetMetricsRequest>(),
		                                                     Promise<UID>(),
		                                                     PromiseStream<Promise<int>>(),
		                                                     PromiseStream<Promise<int64_t>>() }));

		for (int id = 1; id <= processCount; id++) {
			UID uid(id, 0);
			StorageServerInterface interface;
			interface.uniqueID = uid;
			int process_id = id;
			int dc_id = process_id / 1000;
			int data_hall_id = process_id / 100;
			int zone_id = process_id / 10;
			int machine_id = process_id / 5;

			printf("testMachineTeamCollection: process_id:%d zone_id:%d machine_id:%d ip_addr:%s\n",
			       process_id,
			       zone_id,
			       machine_id,
			       interface.address().toString().c_str());
			interface.locality.set("processid"_sr, Standalone<StringRef>(std::to_string(process_id)));
			interface.locality.set("machineid"_sr, Standalone<StringRef>(std::to_string(machine_id)));
			interface.locality.set("zoneid"_sr, Standalone<StringRef>(std::to_string(zone_id)));
			interface.locality.set("data_hall"_sr, Standalone<StringRef>(std::to_string(data_hall_id)));
			interface.locality.set("dcid"_sr, Standalone<StringRef>(std::to_string(dc_id)));
			collection->server_info[uid] = makeReference<TCServerInfo>(
			    interface, collection.get(), ProcessClass(), true, collection->storageServerSet);

			collection->server_status.set(uid, ServerStatus(false, false, false, interface.locality));
		}

		int totalServerIndex = collection->constructMachinesFromServers();
		printf("testMachineTeamCollection: construct machines for %d servers\n", totalServerIndex);

		return collection;
	}

	ACTOR static Future<Void> AddTeamsBestOf_UseMachineID() {
		wait(Future<Void>(Void()));

		int teamSize = 3; // replication size
		int processSize = 60;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

		Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
		    new PolicyAcross(teamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		state std::unique_ptr<DDTeamCollection> collection = testMachineTeamCollection(teamSize, policy, processSize);

		collection->addTeamsBestOf(30, desiredTeams, maxTeams);

		ASSERT(collection->sanityCheckTeams() == true);

		return Void();
	}

	ACTOR static Future<Void> AddTeamsBestOf_NotUseMachineID() {
		wait(Future<Void>(Void()));

		int teamSize = 3; // replication size
		int processSize = 60;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

		Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
		    new PolicyAcross(teamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		state std::unique_ptr<DDTeamCollection> collection = testMachineTeamCollection(teamSize, policy, processSize);

		if (collection == nullptr) {
			fprintf(stderr, "collection is null\n");
			return Void();
		}

		collection->addBestMachineTeams(30); // Create machine teams to help debug
		collection->addTeamsBestOf(30, desiredTeams, maxTeams);
		collection->sanityCheckTeams(); // Server team may happen to be on the same machine team, although unlikely

		return Void();
	}

	static void AddAllTeams_isExhaustive() {
		// Add some safeguards here to ensure that we are able to build 10 machine teams
		int random_teams_per_server = deterministicRandom()->randomInt(2, 10);
		auto desired_teams_per_server = KnobValueRef::create(int{ random_teams_per_server });
		auto max_teams_per_server = KnobValueRef::create(int{ random_teams_per_server * 5 });
		IKnobCollection::getMutableGlobalKnobCollection().setKnob("desired_teams_per_server", desired_teams_per_server);
		IKnobCollection::getMutableGlobalKnobCollection().setKnob("max_teams_per_server", max_teams_per_server);

		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		int processSize = 10;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
		std::unique_ptr<DDTeamCollection> collection = testTeamCollection(3, policy, processSize);
		int result = collection->addTeamsBestOf(200, desiredTeams, maxTeams);

		// The maximum number of available server teams without considering machine locality is 120
		// The maximum number of available server teams with machine locality constraint is 120 - 40, because
		// the 40 (5*4*2) server teams whose servers come from the same machine are invalid.
		// Ideally, we will reach 80, but it might not always hold true since we have lots of random in addTeamsBestOf.
		// Hence, it is much safe and less flaky to check that we have >= 70 teams.
		ASSERT(result >= 70);
	}

	static void AddAllTeams_withLimit() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		int processSize = 10;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

		std::unique_ptr<DDTeamCollection> collection = testTeamCollection(3, policy, processSize);

		int result = collection->addTeamsBestOf(10, desiredTeams, maxTeams);

		ASSERT(result >= 10);
	}

	ACTOR static Future<Void> AddTeamsBestOf_SkippingBusyServers() {
		wait(Future<Void>(Void()));
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 10;
		state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
		state int teamSize = 3;
		// state int targetTeamsPerServer = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (teamSize + 1) / 2;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(1, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);

		state int result = collection->addTeamsBestOf(8, desiredTeams, maxTeams);

		ASSERT(result >= 8);

		for (const auto& [serverID, server] : collection->server_info) {
			auto teamCount = server->getTeams().size();
			ASSERT(teamCount >= 1);
			// ASSERT(teamCount <= targetTeamsPerServer);
		}

		return Void();
	}

	// Due to the randomness in choosing the machine team and the server team from the machine team, it is possible that
	// we may not find the remaining several (e.g., 1 or 2) available teams.
	// It is hard to conclude what is the minimum number of  teams the addTeamsBestOf() should create in this situation.
	ACTOR static Future<Void> AddTeamsBestOf_NotEnoughServers() {
		wait(Future<Void>(Void()));
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(1, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);

		collection->addBestMachineTeams(10);
		int result = collection->addTeamsBestOf(10, desiredTeams, maxTeams);

		if (collection->machineTeams.size() != 10 || result != 8) {
			collection->traceAllInfo(true); // Debug message
		}

		// NOTE: Due to the pure randomness in selecting a machine for a machine team,
		// we cannot guarantee that all machine teams are created.
		// When we chnage the selectReplicas function to achieve such guarantee, we can enable the following ASSERT
		ASSERT(collection->machineTeams.size() == 10); // Should create all machine teams

		// We need to guarantee a server always have at least a team so that the server can participate in data
		// distribution
		for (const auto& [serverID, server] : collection->server_info) {
			auto teamCount = server->getTeams().size();
			ASSERT(teamCount >= 1);
		}

		// If we find all available teams, result will be 8 because we prebuild 2 teams
		ASSERT(result == 8);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_NewServersNotNeeded() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		/*
		 * Suppose  1, 2 and 3 are complete sources, i.e., they have all shards in
		 * the key range being considered for movement. If the caller says that they
		 * don't strictly need new servers and all of these servers are healthy,
		 * maintain status quo.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_COMPLETE_SRCS,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(1, 0), UID(2, 0), UID(3, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_HealthyCompleteSource() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);
		collection->server_info[UID(1, 0)]->markTeamUnhealthy(0);

		/*
		 * Suppose  1, 2, 3 and 4 are complete sources, i.e., they have all shards in
		 * the key range being considered for movement. If the caller says that they don't
		 * strictly need new servers but '1' is not healthy, see that the other team of
		 * complete sources is selected.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0), UID(4, 0) };

		state GetTeamRequest req(TeamSelect::WANT_COMPLETE_SRCS,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(2, 0), UID(3, 0), UID(4, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());

		ASSERT(expectedServers == selectedServers);
		return Void();
	}

	ACTOR static Future<Void> GetTeam_TrueBestLeastUtilized() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		/*
		 * Among server teams that have healthy space available, pick the team that is
		 * least utilized, if the caller says they preferLowerDiskUtil.
		 */

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(2, 0), UID(3, 0), UID(4, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_TrueBestMostUtilized() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		/*
		 * Among server teams that have healthy space available, pick the team that is
		 * most utilized, if the caller says they don't preferLowerDiskUtil.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::False,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(1, 0), UID(2, 0), UID(3, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_ServerUtilizationBelowCutoff() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply low_avail;
		low_avail.capacity.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 20;
		low_avail.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE / 2;
		low_avail.load.bytes = 90 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 2000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(high_avail);
		collection->server_info[UID(2, 0)]->setMetrics(low_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(low_avail);
		collection->server_info[UID(1, 0)]->markTeamUnhealthy(0);

		/*
		 * If the only available team is one where at least one server is low on
		 * space, decline to pick that team. Every server must have some minimum
		 * free space defined by the MIN_AVAILABLE_SPACE server knob.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		ASSERT(!resTeam.present());

		return Void();
	}

	ACTOR static Future<Void> GetTeam_ServerUtilizationNearCutoff() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply low_avail;
		if (SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO > 0) {
			/* Pick a capacity where MIN_AVAILABLE_SPACE_RATIO of the capacity would be higher than MIN_AVAILABLE_SPACE
			 */
			low_avail.capacity.bytes =
			    SERVER_KNOBS->MIN_AVAILABLE_SPACE * (2 / SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO);
		} else {
			low_avail.capacity.bytes = 2000 * 1024 * 1024;
		}
		low_avail.available.bytes = (SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * 1.1) * low_avail.capacity.bytes;
		low_avail.load.bytes = 90 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 2000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(3, 0), UID(4, 0), UID(5, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(high_avail);
		collection->server_info[UID(2, 0)]->setMetrics(low_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(low_avail);
		collection->server_info[UID(5, 0)]->setMetrics(high_avail);
		collection->server_info[UID(1, 0)]->markTeamUnhealthy(0);

		/*
		 * If the only available team is one where all servers are low on space,
		 * test that each server has at least MIN_AVAILABLE_SPACE_RATIO (server knob)
		 * percentage points of capacity free before picking that team.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto& [resTeam, srcTeamFound] = req.reply.getFuture().get();

		ASSERT(!resTeam.present());

		return Void();
	}

	ACTOR static Future<Void> GetTeam_TrueBestLeastReadBandwidth() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(1, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 1;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		int64_t capacity = 1000 * 1024 * 1024, available = 800 * 1024 * 1024;
		std::vector<int64_t> read_bandwidths{
			300 * 1024 * 1024, 100 * 1024 * 1024, 500 * 1024 * 1024, 100 * 1024 * 1024, 900 * 1024 * 1024
		};
		std::vector<int64_t> load_bytes{
			50 * 1024 * 1024, 300 * 1024 * 1024, 800 * 1024 * 1024, 200 * 1024 * 1024, 400 * 1024 * 1024
		};
		GetStorageMetricsReply metrics[5];
		for (int i = 0; i < 5; ++i) {
			metrics[i].capacity.bytes = capacity;
			metrics[i].available.bytes = available;
			metrics[i].load.bytesReadPerKSecond = read_bandwidths[i];
			metrics[i].load.bytes = load_bytes[i];
			collection->addTeam(std::set<UID>({ UID(i + 1, 0) }), IsInitialTeam::True);
			collection->server_info[UID(i + 1, 0)]->setMetrics(metrics[i]);
		}

		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		auto preferLowerDiskUtil = PreferLowerDiskUtil::True;
		auto teamMustHaveShards = TeamMustHaveShards::False;
		auto forReadBalance = ForReadBalance::True;
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         preferLowerDiskUtil,
		                         teamMustHaveShards,
		                         PreferLowerReadUtil::True,
		                         forReadBalance);
		req.completeSources = completeSources;

		state GetTeamRequest reqHigh(TeamSelect::WANT_TRUE_BEST,
		                             PreferLowerDiskUtil::False,
		                             teamMustHaveShards,
		                             PreferLowerReadUtil::False,
		                             forReadBalance);

		// Case 1:
		wait(collection->getTeam(req) && collection->getTeam(reqHigh));
		auto [resTeam, resTeamSrcFound] = req.reply.getFuture().get();
		auto [resTeamHigh, resTeamHighSrcFound] = reqHigh.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(4, 0) };
		std::set<UID> expectedServersHigh{ UID(5, 0) };

		ASSERT(resTeam.present());
		ASSERT(resTeamHigh.present());
		auto servers = resTeam.get()->getServerIDs(), serversHigh = resTeamHigh.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end()),
		    selectedServersHigh(serversHigh.begin(), serversHigh.end());
		ASSERT(expectedServers == selectedServers);
		ASSERT(expectedServersHigh == selectedServersHigh);

		// Case 2: resTeam1 = {2, 0}
		resTeam.get()->addReadInFlightToTeam(
		    50, std::unordered_set<UID>(req.completeSources.begin(), req.completeSources.end()));
		req.reply.reset();
		wait(collection->getTeam(req));
		auto [resTeam1, resTeam1Found] = req.reply.getFuture().get();
		std::set<UID> expectedServers1{ UID(2, 0) };
		auto servers1 = resTeam1.get()->getServerIDs();
		const std::set<UID> selectedServers1(servers1.begin(), servers1.end());
		ASSERT(expectedServers1 == selectedServers1);

		// Case 3: resTeam2 = {2, 0}
		resTeam1.get()->addReadInFlightToTeam(
		    100, std::unordered_set<UID>(req.completeSources.begin(), req.completeSources.end()));
		req.reply.reset();
		wait(collection->getTeam(req));
		auto [resTeam2, resTeam2Found] = req.reply.getFuture().get();
		std::set<UID> expectedServers2{ UID(2, 0) };
		auto servers2 = resTeam2.get()->getServerIDs();
		const std::set<UID> selectedServers2(servers2.begin(), servers2.end());
		ASSERT(expectedServers2 == selectedServers2);

		// Case 4: resTeam3 = {4, 0}
		resTeam2.get()->addReadInFlightToTeam(50, std::unordered_set<UID>{ UID(1, 0), UID(3, 0) });
		req.reply.reset();
		wait(collection->getTeam(req));
		auto [resTeam3, resTeam3Found] = req.reply.getFuture().get();
		std::set<UID> expectedServers3{ UID(4, 0) };
		auto servers3 = resTeam3.get()->getServerIDs();
		const std::set<UID> selectedServers3(servers3.begin(), servers3.end());
		ASSERT(expectedServers3 == selectedServers3);

		// Case 5: resTeam4 = {2, 0}
		resTeam3.get()->addDataInFlightToTeam(
		    600 * 1024 * 1024, std::unordered_set<UID>(req.completeSources.begin(), req.completeSources.end()));
		req.reply.reset();
		wait(collection->getTeam(req));
		auto [resTeam4, resTeam4Found] = req.reply.getFuture().get();
		std::set<UID> expectedServers4{ UID(2, 0) };
		auto servers4 = resTeam4.get()->getServerIDs();
		const std::set<UID> selectedServers4(servers4.begin(), servers4.end());
		ASSERT(expectedServers4 == selectedServers4);

		// Case 5: resTeam5 = {2, 0}
		resTeam4.get()->addDataInFlightToTeam(
		    600 * 1024 * 1024, std::unordered_set<UID>(req.completeSources.begin(), req.completeSources.end()));
		req.reply.reset();
		wait(collection->getTeam(req));
		auto [resTeam5, resTeam5Found] = req.reply.getFuture().get();
		std::set<UID> expectedServers5{ UID(2, 0) };
		auto servers5 = resTeam5.get()->getServerIDs();
		const std::set<UID> selectedServers5(servers5.begin(), servers5.end());
		ASSERT(expectedServers5 == selectedServers5);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_DeprioritizeWigglePausedTeam() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);
		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		collection->wigglingId = UID(4, 0);
		collection->pauseWiggle = makeReference<AsyncVar<bool>>(true);

		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(1, 0), UID(2, 0), UID(3, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> StorageWiggler_NextIdWithMinAge() {
		state std::unique_ptr<DDTeamCollection> collection =
		    DDTeamCollectionUnitTest::testMachineTeamCollection(1, Reference<IReplicationPolicy>(new PolicyOne()), 5);

		state Reference<StorageWiggler> wiggler = collection.get()->storageWiggler;
		state double startTime = now();
		wiggler->addServer(UID(1, 0),
		                   StorageMetadataType(startTime - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 5.0,
		                                       KeyValueStoreType::SSD_BTREE_V2));
		wiggler->addServer(UID(2, 0),
		                   StorageMetadataType(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC,
		                                       KeyValueStoreType::MEMORY,
		                                       true));
		wiggler->addServer(UID(3, 0), StorageMetadataType(startTime - 5.0, KeyValueStoreType::SSD_ROCKSDB_V1, true));
		wiggler->addServer(UID(4, 0),
		                   StorageMetadataType(startTime - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC - 1.0,
		                                       KeyValueStoreType::SSD_BTREE_V2));
		std::vector<Optional<UID>> correctResult{ UID(3, 0), UID(2, 0), UID(4, 0), Optional<UID>() };
		for (int i = 0; i < 4; ++i) {
			auto id = wiggler->getNextServerId();
			ASSERT(id == correctResult[i]);
		}

		{
			std::cout << "Finish Initial Check. Start test getNextWigglingServerID() loop...\n";
			// test the getNextWigglingServerID() loop
			UID id = wait(collection->getNextWigglingServerID());
			ASSERT(id == UID(1, 0));
		}

		std::cout << "Test after addServer() ...\n";
		state Future<UID> nextFuture = collection->getNextWigglingServerID();
		ASSERT(!nextFuture.isReady());
		startTime = now();
		StorageMetadataType metadata(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 100.0,
		                             KeyValueStoreType::SSD_BTREE_V2);
		wiggler->addServer(UID(5, 0), metadata);
		ASSERT(!nextFuture.isReady());

		std::cout << "Test after updateServer() ...\n";
		StorageWiggler* ptr = wiggler.getPtr();
		wait(trigger(
		    [ptr]() {
			    ptr->updateMetadata(UID(5, 0),
			                        StorageMetadataType(now() - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC,
			                                            KeyValueStoreType::SSD_BTREE_V2));
		    },
		    delay(5.0)));
		wait(success(nextFuture));
		ASSERT(now() - startTime < SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 100.0);
		ASSERT(nextFuture.get() == UID(5, 0));
		return Void();
	}

	ACTOR static Future<Void> StorageWiggler_NextIdWithTSS() {
		state std::unique_ptr<DDTeamCollection> collection =
		    DDTeamCollectionUnitTest::testMachineTeamCollection(1, Reference<IReplicationPolicy>(new PolicyOne()), 5);
		state Reference<StorageWiggler> wiggler = collection.get()->storageWiggler;

		std::cout << "Test when need TSS ... \n";
		collection->configuration.usableRegions = 1;
		collection->configuration.desiredTSSCount = 1;
		state double startTime = now();
		wiggler->addServer(UID(1, 0),
		                   StorageMetadataType(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 150.0,
		                                       KeyValueStoreType::SSD_BTREE_V2));
		wiggler->addServer(UID(2, 0),
		                   StorageMetadataType(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 150.0,
		                                       KeyValueStoreType::SSD_BTREE_V2));
		ASSERT(!wiggler->getNextServerId(true).present());
		ASSERT(wiggler->getNextServerId(collection->reachTSSPairTarget()) == UID(1, 0));
		UID id = wait(collection->getNextWigglingServerID());
		ASSERT(now() - startTime < SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 150.0);
		ASSERT(id == UID(2, 0));
		return Void();
	}

	// Cut off high Cpu teams
	ACTOR static Future<Void> GetTeam_CutOffByCpu() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(1, "zoneid", makeReference<PolicyOne>());
		state int processSize = 4;
		state int teamSize = 1;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);
		state GetTeamRequest bestReq(TeamSelect::WANT_TRUE_BEST,
		                             PreferLowerDiskUtil::True,
		                             TeamMustHaveShards::False,
		                             PreferLowerReadUtil::True,
		                             ForReadBalance::True);
		state GetTeamRequest randomReq(TeamSelect::ANY,
		                               PreferLowerDiskUtil::True,
		                               TeamMustHaveShards::False,
		                               PreferLowerReadUtil::True,
		                               ForReadBalance::True);
		collection->teamPivots.lastPivotsUpdate = -100;

		int64_t capacity = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 20, loadBytes = 90 * 1024 * 1024;
		GetStorageMetricsReply high_s_high_r;
		high_s_high_r.capacity.bytes = capacity;
		high_s_high_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 5;
		high_s_high_r.load.bytes = loadBytes;
		high_s_high_r.load.opsReadPerKSecond = 7000 * 1000;

		GetStorageMetricsReply high_s_low_r;
		high_s_low_r.capacity.bytes = capacity;
		high_s_low_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 5;
		high_s_low_r.load.bytes = loadBytes;
		high_s_low_r.load.opsReadPerKSecond = 100 * 1000;

		GetStorageMetricsReply low_s_low_r;
		low_s_low_r.capacity.bytes = capacity;
		low_s_low_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE / 2;
		low_s_low_r.load.bytes = loadBytes;
		low_s_low_r.load.opsReadPerKSecond = 100 * 1000;

		HealthMetrics::StorageStats low_cpu, mid_cpu, high_cpu;
		// use constant cutoff value
		bool maxCutoff = deterministicRandom()->coinflip();
		if (!maxCutoff) {
			// use pivot value as cutoff
			auto ratio = KnobValueRef::create(double{ 0.7 });
			IKnobCollection::getMutableGlobalKnobCollection().setKnob("cpu_pivot_ratio", ratio);
		}
		low_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 60;
		mid_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 40;
		high_cpu.cpuUsage = maxCutoff ? SERVER_KNOBS->MAX_DEST_CPU_PERCENT + 1 : SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 1;

		// high space, low cpu, high read (in pool)
		collection->addTeam(std::set<UID>({ UID(1, 0) }), IsInitialTeam::True);
		collection->server_info[UID(1, 0)]->setMetrics(high_s_high_r);
		collection->server_info[UID(1, 0)]->setStorageStats(low_cpu);
		// high space, mid cpu, low read (in pool)
		collection->addTeam(std::set<UID>({ UID(2, 0) }), IsInitialTeam::True);
		collection->server_info[UID(2, 0)]->setMetrics(high_s_low_r);
		collection->server_info[UID(2, 0)]->setStorageStats(mid_cpu);
		// low space, low cpu, low read (not in pool)
		collection->addTeam(std::set<UID>({ UID(3, 0) }), IsInitialTeam::True);
		collection->server_info[UID(3, 0)]->setMetrics(low_s_low_r);
		collection->server_info[UID(3, 0)]->setStorageStats(low_cpu);
		// high space, high cpu, low read (not in pool)
		collection->addTeam(std::set<UID>({ UID(4, 0) }), IsInitialTeam::True);
		collection->server_info[UID(4, 0)]->setMetrics(high_s_low_r);
		collection->server_info[UID(4, 0)]->setStorageStats(high_cpu);

		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		wait(collection->getTeam(bestReq));
		const auto [bestTeam, found1] = bestReq.reply.getFuture().get();
		fmt::print("{} {} {}\n",
		           SERVER_KNOBS->CPU_PIVOT_RATIO,
		           collection->teamPivots.pivotCPU,
		           collection->teamPivots.pivotAvailableSpaceRatio);
		ASSERT(bestTeam.present());
		ASSERT_EQ(bestTeam.get()->getServerIDs(), std::vector<UID>{ UID(2, 0) });

		wait(collection->getTeam(randomReq));
		const auto [randomTeam, found2] = randomReq.reply.getFuture().get();
		if (randomTeam.present()) {
			CODE_PROBE(true, "Unit Test Random Team Return Candidate.");
			ASSERT_NE(randomTeam.get()->getServerIDs(), std::vector<UID>{ UID(3, 0) });
			ASSERT_NE(randomTeam.get()->getServerIDs(), std::vector<UID>{ UID(4, 0) });
		}
		return Void();
	}

	// After enabling SERVER_KNOBS->DD_REEVALUATION_ENABLED=true, we would evaluate the cpu and available space of
	// source team when we are relocating a shard. If both are better than pivot values, we would return the source team
	// as new destination team.
	ACTOR static Future<Void> GetTeam_EvaluateSourceTeam() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(1, "zoneid", makeReference<PolicyOne>());
		state int processSize = 4;
		state int teamSize = 1;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		collection->teamPivots.lastPivotsUpdate = -100;

		int64_t capacity = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 20, loadBytes = 90 * 1024 * 1024;
		GetStorageMetricsReply high_s_high_r;
		high_s_high_r.capacity.bytes = capacity;
		high_s_high_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 5;
		high_s_high_r.load.bytes = loadBytes;
		high_s_high_r.load.opsReadPerKSecond = 7000 * 1000;

		GetStorageMetricsReply high_s_low_r;
		high_s_low_r.capacity.bytes = capacity;
		high_s_low_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 5;
		high_s_low_r.load.bytes = loadBytes;
		high_s_low_r.load.opsReadPerKSecond = 100 * 1000;

		GetStorageMetricsReply low_s_low_r;
		low_s_low_r.capacity.bytes = capacity;
		low_s_low_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 2;
		low_s_low_r.load.bytes = loadBytes;
		low_s_low_r.load.opsReadPerKSecond = 100 * 1000;

		IKnobCollection::getMutableGlobalKnobCollection().setKnob("dd_reevaluation_enabled",
		                                                          KnobValueRef::create(bool{ true }));
		IKnobCollection::getMutableGlobalKnobCollection().setKnob("dd_strict_cpu_pivot_ratio",
		                                                          KnobValueRef::create(double{ 0.6 }));
		IKnobCollection::getMutableGlobalKnobCollection().setKnob("dd_strict_available_space_pivot_ratio",
		                                                          KnobValueRef::create(double{ 0.5 }));
		IKnobCollection::getMutableGlobalKnobCollection().setKnob("cpu_pivot_ratio",
		                                                          KnobValueRef::create(double{ 0.9 }));

		HealthMetrics::StorageStats low_cpu, mid_cpu, high_cpu;
		low_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 60;
		mid_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 40;
		high_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT;

		// high available space, low cpu, high read (in pool)
		collection->addTeam(std::set<UID>({ UID(1, 0) }), IsInitialTeam::True);
		collection->server_info[UID(1, 0)]->setMetrics(high_s_high_r);
		collection->server_info[UID(1, 0)]->setStorageStats(low_cpu);
		// high available space, mid cpu, low read (in pool)
		collection->addTeam(std::set<UID>({ UID(2, 0) }), IsInitialTeam::True);
		collection->server_info[UID(2, 0)]->setMetrics(high_s_low_r);
		collection->server_info[UID(2, 0)]->setStorageStats(mid_cpu);
		// low available space, low cpu, low read (not in pool)
		collection->addTeam(std::set<UID>({ UID(3, 0) }), IsInitialTeam::True);
		collection->server_info[UID(3, 0)]->setStorageStats(low_cpu);
		collection->server_info[UID(3, 0)]->setMetrics(low_s_low_r);
		// high available space, high cpu, high read (not in pool)
		collection->addTeam(std::set<UID>({ UID(4, 0) }), IsInitialTeam::True);
		collection->server_info[UID(4, 0)]->setMetrics(high_s_high_r);
		collection->server_info[UID(4, 0)]->setStorageStats(high_cpu);

		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		// Case 1: Return source team even if it asks for TeamSelect::WANT_TRUE_BEST
		TeamSelect sourceSelect(TeamSelect::WANT_TRUE_BEST);
		sourceSelect.setForRelocateShard(ForRelocateShard::True);
		state GetTeamRequest sourceReq(sourceSelect,
		                               PreferLowerDiskUtil::True,
		                               TeamMustHaveShards::False,
		                               PreferLowerReadUtil::True,
		                               ForReadBalance::True);
		sourceReq.src = std::vector{ UID(1, 0) };

		wait(collection->getTeam(sourceReq));
		const auto [sourceTeam, found1] = sourceReq.reply.getFuture().get();
		fmt::print("StrictPivotRatio: {}, teamStrictPivotCPU: {}, strictPivotAvailableSpaceRatio: {}\n",
		           SERVER_KNOBS->DD_STRICT_CPU_PIVOT_RATIO,
		           collection->teamPivots.strictPivotCPU,
		           collection->teamPivots.strictPivotAvailableSpaceRatio);
		ASSERT(sourceTeam.present());
		ASSERT_EQ(sourceTeam.get()->getServerIDs(), std::vector<UID>{ UID(1, 0) });

		// CASE 2: Don't return source team because its CPU is beyond pivot values
		TeamSelect bestSelect(TeamSelect::WANT_TRUE_BEST);
		bestSelect.setForRelocateShard(ForRelocateShard::True);
		state GetTeamRequest bestReq(bestSelect,
		                             PreferLowerDiskUtil::True,
		                             TeamMustHaveShards::False,
		                             PreferLowerReadUtil::True,
		                             ForReadBalance::True);
		bestReq.src = std::vector{ UID(4, 0) };
		wait(collection->getTeam(bestReq));
		const auto [bestTeam, found2] = bestReq.reply.getFuture().get();
		ASSERT(bestTeam.present());
		ASSERT_EQ(bestTeam.get()->getServerIDs(), std::vector<UID>{ UID(2, 0) });

		// CASE 3: Don't return source team because its available space is below pivot values
		TeamSelect anySelect(TeamSelect::ANY);
		anySelect.setForRelocateShard(ForRelocateShard::True);
		state GetTeamRequest anyReq(anySelect,
		                            PreferLowerDiskUtil::True,
		                            TeamMustHaveShards::False,
		                            PreferLowerReadUtil::True,
		                            ForReadBalance::True);
		anyReq.src = std::vector{ UID(3, 0) };
		wait(collection->getTeam(anyReq));
		const auto [anyTeam, found3] = anyReq.reply.getFuture().get();
		ASSERT(anyTeam.present());
		ASSERT_NE(anyTeam.get()->getServerIDs(), std::vector<UID>{ UID(3, 0) });

		return Void();
	}
};

TEST_CASE("/DataDistribution/AddTeamsBestOf/UseMachineID") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_UseMachineID());
	return Void();
}

TEST_CASE("/DataDistribution/AddTeamsBestOf/NotUseMachineID") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_NotUseMachineID());
	return Void();
}

TEST_CASE("/DataDistribution/AddAllTeams/isExhaustive") {
	DDTeamCollectionUnitTest::AddAllTeams_isExhaustive();
	return Void();
}

TEST_CASE("/DataDistribution/AddAllTeams/withLimit") {
	DDTeamCollectionUnitTest::AddAllTeams_withLimit();
	return Void();
}

TEST_CASE("/DataDistribution/AddTeamsBestOf/SkippingBusyServers") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_SkippingBusyServers());
	return Void();
}

TEST_CASE("/DataDistribution/AddTeamsBestOf/NotEnoughServers") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_NotEnoughServers());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/NewServersNotNeeded") {
	wait(DDTeamCollectionUnitTest::GetTeam_NewServersNotNeeded());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/HealthyCompleteSource") {
	wait(DDTeamCollectionUnitTest::GetTeam_HealthyCompleteSource());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/TrueBestLeastUtilized") {
	wait(DDTeamCollectionUnitTest::GetTeam_TrueBestLeastUtilized());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/TrueBestMostUtilized") {
	wait(DDTeamCollectionUnitTest::GetTeam_TrueBestMostUtilized());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/ServerUtilizationBelowCutoff") {
	wait(DDTeamCollectionUnitTest::GetTeam_ServerUtilizationBelowCutoff());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/ServerUtilizationNearCutoff") {
	wait(DDTeamCollectionUnitTest::GetTeam_ServerUtilizationNearCutoff());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/TrueBestLeastReadBandwidth") {
	wait(DDTeamCollectionUnitTest::GetTeam_TrueBestLeastReadBandwidth());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/DeprioritizeWigglePausedTeam") {
	wait(DDTeamCollectionUnitTest::GetTeam_DeprioritizeWigglePausedTeam());
	return Void();
}

TEST_CASE("/DataDistribution/StorageWiggler/NextIdWithMinAge") {
	wait(DDTeamCollectionUnitTest::StorageWiggler_NextIdWithMinAge());
	return Void();
}

TEST_CASE("/DataDistribution/StorageWiggler/NextIdWithTSS") {
	wait(DDTeamCollectionUnitTest::StorageWiggler_NextIdWithTSS());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/CutOffByCpu") {
	wait(DDTeamCollectionUnitTest::GetTeam_CutOffByCpu());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/EvaluateSourceTeam") {
	wait(DDTeamCollectionUnitTest::GetTeam_EvaluateSourceTeam());
	return Void();
}