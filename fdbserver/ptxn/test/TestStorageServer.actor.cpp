/*
 * TestStorageServer.actor.cpp
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

#include "fdbserver/ptxn/test/TestStorageServer.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include

namespace ptxn::test {

// TODO: This is defined in TestTLogPeek.actor.cpp. Rethink where messageFeeder should stay in.
ACTOR Future<Void> messageFeeder();

const int TestStorageServerPullOptions::DEFAULT_TLOGS = 3;
const int TestStorageServerPullOptions::DEFAULT_STORAGE_TEAMS = 9;
const int TestStorageServerPullOptions::DEFAULT_INITIAL_VERSION = 1000;
const int TestStorageServerPullOptions::DEFAULT_NUM_VERSIONS = 10;
const int TestStorageServerPullOptions::DEFAULT_NUM_MUTATIONS_PER_VERSION = 10;

TestStorageServerPullOptions::TestStorageServerPullOptions(const UnitTestParameters& params)
  : numTLogs(params.getInt("numTLogs").orDefault(DEFAULT_TLOGS)),
    numStorageTeams(params.getInt("numStorageTeams").orDefault(DEFAULT_STORAGE_TEAMS)),
    initialVersion(params.getInt("initialVersion").orDefault(DEFAULT_INITIAL_VERSION)),
    numVersions(params.getInt("numVersions").orDefault(DEFAULT_NUM_VERSIONS)),
    numMutationsPerVersion(params.getInt("numMutationsPerVersion").orDefault(DEFAULT_NUM_MUTATIONS_PER_VERSION)) {}

} // namespace ptxn::test

TEST_CASE("fdbserver/ptxn/test/StorageServerPull") {
	state ptxn::test::TestEnvironment testEnvironment;
	state ptxn::test::TestStorageServerPullOptions options(params);
	state ptxn::test::print::PrintTiming printTiming("fdbserver/ptxn/test/StorageServerPull");

	testEnvironment
	    .initDriverContext()
	    // At this stage we set num tLogs same to numStorageTeams
	    .initTLogGroupWithPrivateMutationsFixture(options.numTLogs, options.numTLogs)
	    .initPtxnTLog(ptxn::MessageTransferModel::StorageServerActivelyPull, options.numTLogs)
	    .initServerDBInfo()
	    .initPtxnStorageServer(1)
	    .initMessagesWithPrivateMutations(
	        options.initialVersion, options.numVersions, options.numMutationsPerVersion, Optional<std::vector<UID>>());

	wait(ptxn::test::messageFeeder());

	std::vector<Future<InitializeStorageReply>> initializeStoreReplyFutures;
	std::transform(std::begin(ptxn::test::TestEnvironment::getStorageServers()->initializeStorageReplies),
	               std::end(ptxn::test::TestEnvironment::getStorageServers()->initializeStorageReplies),
	               std::back_inserter(initializeStoreReplyFutures),
	               [](ReplyPromise<InitializeStorageReply>& reply) { return reply.getFuture(); });
	wait(waitForAll(initializeStoreReplyFutures));

	printTiming << "All storage servers are ready" << std::endl;

	wait(delay(10));

	return Void();
}
