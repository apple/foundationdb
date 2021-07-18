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

// Returns a randomly picked subset of "groups".
std::vector<ptxn::TLogGroupID> getRandomGroups(const std::vector<ptxn::TLogGroupID>& groups) {
	std::vector<ptxn::TLogGroupID> results;
	for (const auto& group : groups) {
		if (deterministicRandom()->coinflip()) {
			results.push_back(group);
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
                                                         const std::vector<ptxn::TLogGroupID>& groups) {
	std::vector<ResolveTransactionBatchRequest> batch(n);

	Version current = beginVersion;
	Version pv = prevVersion;
	for (auto& r : batch) {
		r.prevVersion = pv;
		r.version = current;
		r.lastReceivedVersion = prevVersion;
		r.updatedGroups = getRandomGroups(groups);

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
	std::vector<ptxn::TLogGroupID> groups;
	const int totalGroups = 10;
	for (int i = 0; i < totalGroups; i++) {
		groups.emplace_back(0, i);
	}
	const Version initialVersion = 100;
	tracker.addGroups(groups, initialVersion);
	ASSERT_EQ(tracker.getCommitVersion({0, 1}), initialVersion);
	ASSERT_EQ(tracker.getMaxCommitVersion(), initialVersion);

	std::vector<ptxn::TLogGroupID> updatedGroups, newGroups, removedGroups;
	for (int i = 0; i < totalGroups / 2; i++) {
		updatedGroups.emplace_back(0, i);
		newGroups.emplace_back(0, totalGroups + i);
		removedGroups.emplace_back(0, totalGroups - i - 1);
	}
	const Version cv1 = 200, cv2 = 200;
	tracker.updateGroups(updatedGroups, cv1);
	tracker.addGroups(newGroups, cv2);
	tracker.removeGroups(removedGroups);

	for (const auto group : updatedGroups) {
		ASSERT_EQ(tracker.getCommitVersion(group), cv1);
	}
	for (const auto group : newGroups) {
		ASSERT_EQ(tracker.getCommitVersion(group), cv2);
	}
	for (const auto group : removedGroups) {
		ASSERT_EQ(tracker.getCommitVersion(group), invalidVersion);
	}
	std::pair<ptxn::TLogGroupID, Version> p = tracker.mostLaggingGroup();
	ASSERT_EQ(p.second, cv1);

	return true;
}

} // anonymous namespace

// Creates a random set of groups, sends 10 batches in random order to Resolvers,
// and verifies the correct previous commit version (PCV) is returned back.
TEST_CASE("fdbserver/ptxn/test/resolver") {
	state const Version lastEpochEnd = 100;
	state const int totalRequests = 10;
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> context;
	state std::vector<ptxn::TLogGroupID> groups;
	state const int totalGroups = deterministicRandom()->randomInt(10, 1000);

	ptxn::test::TestDriverOptions options(params);
	ptxn::test::print::print(options);

	for (int i = 0; i < totalGroups; i++) {
		groups.emplace_back(0, i);
	}

	context = ptxn::test::initTestDriverContext(options);
	startFakeResolver(actors, context);
	std::cout << "Started " << context->numResolvers << " Resolvers with " << totalGroups << " groups\n";

	state std::vector<Future<ResolveTransactionBatchReply>> replies;
	// Imitates resolver initialization from the master server
	for (auto& r : context->resolverInterfaces) {
		ResolveTransactionBatchRequest req;
		req.prevVersion = -1;
		req.version = lastEpochEnd;
		req.lastReceivedVersion = -1;
		// triggers debugging trace events at Resolvers
		req.debugID = deterministicRandom()->randomUniqueID();
		req.newGroups = groups;

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
	    makeTxnBatch(lastEpochEnd, beginVersion, totalRequests, increment, groups);
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

	// Verify group's previous commit versions (PCVs) are correct.
	std::map<ptxn::TLogGroupID, Version> pcv; // previous commit version
	for (const auto& group : groups) {
		pcv[group] = lastEpochEnd;
	}
	for (int i = 0; i < totalRequests; i++) {
		Version v = beginVersion + i * increment;
		int idx = findBatch(batches, v); // the i'th request

		auto& pcvGot = replies[idx].get().previousCommitVersions;
		ASSERT_EQ(batches[idx].updatedGroups.size(), pcvGot.size());
		for (const auto& group : batches[idx].updatedGroups) {
			auto it = pcvGot.find(group);
			ASSERT(it != pcvGot.end());
			ASSERT_EQ(pcv[group], it->second);
			pcv[group] = batches[idx].version;
		}
	}

	ASSERT(trackerTest());

	return Void();
}
