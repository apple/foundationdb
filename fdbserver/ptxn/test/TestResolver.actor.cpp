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

#include "fdbserver/ptxn/test/Driver.h"

#include <iostream>
#include <memory>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/test/FakeResolver.actor.h"
#include "fdbserver/ResolverInterface.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"

 TEST_CASE("fdbserver/ptxn/test/resolver") {
	ptxn::TestDriverOptions options(params);
	std::cout << options << std::endl;

	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::TestDriverContext> context = ptxn::initTestDriverContext();

	startFakeResolver(actors, context);
	std::cout<<"Started " << context->numResolvers << " Resolvers\n";

	const Version lastEpochEnd = 100;
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
	for (auto& f : replies) {
		ASSERT(f.isReady() && f.isValid());
	}
	std::cout << "Initialized " << replies.size() << " Resolvers\n";

	return Void();
}
