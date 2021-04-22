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
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <stdint.h>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/FakeResolver.actor.h"
#include "fdbserver/ResolverInterface.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"

namespace {

// Makes a batch of "n" requests starting at "beginVersion", where each batch
// increases by "increment" amount.
std::vector<ResolveTransactionBatchRequest> makeTxnBatch(Version prevVersion, Version beginVersion, int n, int64_t increment) {
	std::vector<ResolveTransactionBatchRequest> batch(n);

	Version current = beginVersion;
	Version pv = prevVersion;
	for (auto& r : batch) {
		r.prevVersion = pv;
		r.version = current;
		r.lastReceivedVersion = prevVersion;

		pv = current;
		current += increment;
	}

	return batch;
}

} // anonymous namespace

TEST_CASE("fdbserver/ptxn/test/resolver") {
	ptxn::TestDriverOptions options(params);
	std::cout << options << std::endl;

	state const Version lastEpochEnd = 100;
	state const int totalRequests = 10;
>>>>>>> Test batches to a resolver
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::TestDriverContext> context = ptxn::initTestDriverContext();

	startFakeResolver(actors, context);
	std::cout << "Started " << context->numResolvers << " Resolvers\n";

	state std::vector<Future<ResolveTransactionBatchReply>> replies;
	// Imitates resolver initialization from the master server
	for (auto& r : context->resolverInterfaces) {
		ResolveTransactionBatchRequest req;
		req.prevVersion = -1;
		req.version = lastEpochEnd;
		req.lastReceivedVersion = -1;
		// triggers debugging trace events at Resolvers
		req.debugID = deterministicRandom()->randomUniqueID();

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

	std::vector<ResolveTransactionBatchRequest> batch = makeTxnBatch(lastEpochEnd, beginVersion, totalRequests, increment);
	std::mt19937 g(deterministicRandom()->randomUInt32());
	std::shuffle(batch.begin(), batch.end(), g);

	replies.clear();
	for (auto& req : batch) {
		replies.push_back(brokenPromiseToNever(context->resolverInterfaces[0]->resolve.getReply(req)));
	}

	// Note even though requests are processed in order at Resolver. The
	// responses could be received out of order.
	wait(waitForAll(replies));
	for (const auto& f : replies) {
		ASSERT(f.isReady() && f.isValid());
	}

	return Void();
}
