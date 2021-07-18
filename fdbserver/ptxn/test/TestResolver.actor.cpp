/*
 * TestResolver.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/TLogGroupVersionTracker.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/FakeResolver.actor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ResolverInterface.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

namespace {

// Returns a randomly picked subset of "teams".
std::vector<ptxn::StorageTeamID> getRandomTeams(const std::vector<ptxn::StorageTeamID>& teams) {
	std::vector<ptxn::StorageTeamID> results;
	for (const auto& team : teams) {
		if (deterministicRandom()->coinflip()) {
			results.push_back(team);
		}
	}
	return results;
}

// Makes a batch of "n" requests starting at "beginVersion", where each batch
// increases by "increment" amount.
std::vector<ResolveTransactionBatchRequest> makeTxnBatch(Version prevVersion,
                                                         Version beginVersion,
                                                         int n,
                                                         int64_t increment,
                                                         const std::vector<ptxn::StorageTeamID>& teams) {
	std::vector<ResolveTransactionBatchRequest> batch(n);

	Version current = beginVersion;
	Version pv = prevVersion;
	for (auto& r : batch) {
		r.prevVersion = pv;
		r.version = current;
		r.lastReceivedVersion = prevVersion;
		r.updatedGroups = getRandomTeams(teams);

		pv = current;
		current += increment;
	}

	return batch;
}

// Returns the index in "batches", that item's version is "v".
int findBatch(std::vector<ResolveTransactionBatchRequest> batches, Version v) {
	for (int i = 0; i < batches.size(); i++) {
		if (batches[i].version == v)
			return i;
	}
	ASSERT(false);
	return -1;
}

bool trackerTest() {
	ptxn::TLogGroupVersionTracker tracker;
	std::vector<ptxn::StorageTeamID> teams;
	const int totalTeams = 10;
	for (int i = 0; i < totalTeams; i++) {
		teams.emplace_back(0, i);
	}
	const Version initialVersion = 100;
	tracker.addGroups(teams, initialVersion);
	ASSERT_EQ(tracker.getCommitVersion({0, 1}), initialVersion);
	ASSERT_EQ(tracker.getMaxCommitVersion(), initialVersion);

	std::vector<ptxn::StorageTeamID> updatedTeams, newTeams, removedTeams;
	for (int i = 0; i < totalTeams/2; i++) {
		updatedTeams.emplace_back(0, i);
		newTeams.emplace_back(0, totalTeams + i);
		removedTeams.emplace_back(0, totalTeams - i - 1);
	}
	const Version cv1 = 200, cv2 = 200;
	tracker.updateGroups(updatedTeams, cv1);
	tracker.addGroups(newTeams, cv2);
	tracker.removeGroups(removedTeams);

	for (const auto team : updatedTeams) {
		ASSERT_EQ(tracker.getCommitVersion(team), cv1);
	}
	for (const auto team : newTeams) {
		ASSERT_EQ(tracker.getCommitVersion(team), cv2);
	}
	for (const auto team : removedTeams) {
		ASSERT_EQ(tracker.getCommitVersion(team), invalidVersion);
	}
	std::pair<ptxn::StorageTeamID, Version> p = tracker.mostLaggingTeam();
	ASSERT_EQ(p.second, cv1);

	return true;
}

} // anonymous namespace

// Creates a random set of teams, sends 10 batches in random order to Resolvers,
// and verifies the correct previous commit version (PCV) is returned back.
TEST_CASE("fdbserver/ptxn/test/resolver") {
	state const Version lastEpochEnd = 100;
	state const int totalRequests = 10;
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> context;
	state std::vector<ptxn::StorageTeamID> teams;
	state const int totalTeams = deterministicRandom()->randomInt(10, 1000);

	ptxn::test::TestDriverOptions options(params);
	ptxn::test::print::print(options);

	for (int i = 0; i < totalTeams; i++) {
		teams.emplace_back(0, i);
	}

	context = ptxn::test::initTestDriverContext(options);
	startFakeResolver(actors, context);
	std::cout << "Started " << context->numResolvers << " Resolvers with " << totalTeams << " teams\n";

	state std::vector<Future<ResolveTransactionBatchReply>> replies;
	// Imitates resolver initialization from the master server
	for (auto& r : context->resolverInterfaces) {
		ResolveTransactionBatchRequest req;
		req.prevVersion = -1;
		req.version = lastEpochEnd;
		req.lastReceivedVersion = -1;
		// triggers debugging trace events at Resolvers
		req.debugID = deterministicRandom()->randomUniqueID();
		req.newGroups = teams;

		replies.push_back(brokenPromiseToNever(r->resolve.getReply(req)));
	}

	wait(waitForAll(replies));
	for (const auto& f : replies) {
		ASSERT(f.isReady() && f.isValid());
	}
	std::cout << "Initialized " << replies.size() << " Resolvers\n";

	// Generate 10 batches, send in a random order, and verify all responses.
	state Version beginVersion = 200;
	state int64_t increment = 7;

	state std::vector<ResolveTransactionBatchRequest> batches =
	    makeTxnBatch(lastEpochEnd, beginVersion, totalRequests, increment, teams);
	std::mt19937 g(deterministicRandom()->randomUInt32());
	std::shuffle(batches.begin(), batches.end(), g);

	replies.clear();
	for (auto& req : batches) {
		replies.push_back(brokenPromiseToNever(context->resolverInterfaces[0]->resolve.getReply(req)));
	}

	// Note even though requests are processed in order at Resolver. The
	// responses could be received out of order.
	wait(waitForAll(replies));
	for (const auto& f : replies) {
		ASSERT(f.isReady() && f.isValid());
	}

	// Verify team's previous commit versions (PCVs) are correct.
	std::map<ptxn::StorageTeamID, Version> pcv; // previous commit version
	for (const auto& team : teams) {
		pcv[team] = lastEpochEnd;
	}
	for (int i = 0; i < totalRequests; i++) {
		Version v = beginVersion + i * increment;
		int idx = findBatch(batches, v); // the i'th request

		auto& pcvGot = replies[idx].get().previousCommitVersions;
		ASSERT_EQ(batches[idx].updatedGroups.size(), pcvGot.size());
		for (const auto& team : batches[idx].updatedGroups) {
			auto it = pcvGot.find(team);
			ASSERT(it != pcvGot.end());
			ASSERT_EQ(pcv[team], it->second);
			pcv[team] = batches[idx].version;
		}
	}

	ASSERT(trackerTest());

	return Void();
}
