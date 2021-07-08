/*
 * TestTLogServer.actor.cpp
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

#include <memory>
#include <vector>

#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "flow/Arena.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace {

ACTOR Future<Void> startTLogServers(std::vector<Future<Void>>* actors,
                                    std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                                    std::string folder) {
	state std::vector<InitializeTLogRequest> tLogInitializations;
	state int i = 0;
	for (; i < pContext->numTLogs; i++) {
		PromiseStream<InitializeTLogRequest> initializeTLog;
		Promise<Void> recovered;
		tLogInitializations.emplace_back();
		tLogInitializations.back().isPrimary = true;
		tLogInitializations.back().tlogGroups = pContext->tLogGroups;
		UID tlogId = ptxn::test::randomUID();
		UID workerId = ptxn::test::randomUID();
		actors->push_back(ptxn::tLog(std::vector<std::pair<IKeyValueStore*, IDiskQueue*>>(),
		                             makeReference<AsyncVar<ServerDBInfo>>(),
		                             LocalityData(),
		                             initializeTLog,
		                             tlogId,
		                             workerId,
		                             false,
		                             Promise<Void>(),
		                             Promise<Void>(),
		                             folder,
		                             makeReference<AsyncVar<bool>>(false),
		                             makeReference<AsyncVar<UID>>(tlogId)));
		initializeTLog.send(tLogInitializations.back());
		std::cout << "Recruit tlog " << i << " : " << tlogId.shortString() << ", workerID: " << workerId.shortString()
		          << "\n";
	}

	// replace fake TLogInterface with recruited interface
	std::vector<Future<ptxn::TLogInterface_PassivelyPull>> interfaceFutures(pContext->numTLogs);
	for (i = 0; i < pContext->numTLogs; i++) {
		interfaceFutures[i] = tLogInitializations[i].ptxnReply.getFuture();
	}
	std::vector<ptxn::TLogInterface_PassivelyPull> interfaces = wait(getAll(interfaceFutures));
	for (i = 0; i < pContext->numTLogs; i++) {
		*(pContext->tLogInterfaces[i]) = interfaces[i];
	}
	return Void();
}

#if 0

std::pair<Standalone<StringRef>, std::vector<ptxn::Message>> generateSerializedCommitdata(
    const Version& version,
    const int numMutations,
    const ptxn::StorageTeamID& storageTeamID,
    Arena& arena) {

	std::vector<ptxn::Message> messages;
	for (int i = 0; i < numMutations; i++) {
		messages.emplace_back(MutationRef(arena,
		                                  MutationRef::SetValue,
		                                  deterministicRandom()->randomAlphaNumeric(10),
		                                  deterministicRandom()->randomAlphaNumeric(16)));
	}

	ptxn::ProxySubsequencedMessageSerializer serializer(version);
	for (const auto& message : messages)
		serializer.write(message, storageTeamID);

	return { serializer.getSerialized(storageTeamID), std::move(messages) };
}
// Randomly commit to a tlog, then peek data, and verify if the data is consistent.
ACTOR Future<Void> commitPeekAndCheck(std::shared_ptr<ptxn::test::TestDriverContext> pContext) {
	state ptxn::test::print::PrintTiming printTiming("tlog/commitPeekAndCheck");

	const ptxn::TLogGroup& group = pContext->tLogGroups[0];
	ASSERT(!group.storageTeams.empty());
	state ptxn::StorageTeamID storageTeamID = group.storageTeams.begin()->first;
	printTiming << "Storage Team ID: " << storageTeamID.toString() << std::endl;

	state std::shared_ptr<ptxn::TLogInterfaceBase> tli = pContext->getTLogInterface(storageTeamID);
	state Version prevVersion = 0; // starts from 0 for first epoch
	state Version beginVersion = 150;
	state Version endVersion(beginVersion + deterministicRandom()->randomInt(5, 20));
	state Optional<UID> debugID(ptxn::test::randomUID());

	// printTiming << "Generated " << numMutations << " mutations" << std::endl;
	Standalone<StringRef> message = serializer.getSerialized(storageTeamID);

	// Commit
	ptxn::TLogCommitRequest commitRequest(
	    ptxn::test::randomUID(), storageTeamID, message.arena(), message, prevVersion, beginVersion, 0, 0, debugID);
	ptxn::test::print::print(commitRequest);

	ptxn::TLogCommitReply commitReply = wait(tli->commit.getReply(commitRequest));
	ptxn::test::print::print(commitReply);

	// Peek
	ptxn::TLogPeekRequest peekRequest(debugID, beginVersion, endVersion, storageTeamID);
	ptxn::test::print::print(peekRequest);

	ptxn::TLogPeekReply peekReply = wait(tli->peek.getReply(peekRequest));
	ptxn::test::print::print(peekReply);

	// Verify
	ptxn::SubsequencedMessageDeserializer deserializer(peekReply.data);
	ASSERT(storageTeamID == deserializer.getStorageTeamID());
	ASSERT_EQ(beginVersion, deserializer.getFirstVersion());
	ASSERT_EQ(beginVersion, deserializer.getLastVersion());
	int i = 0;
	for (auto iter = deserializer.begin(); iter != deserializer.end(); ++iter, ++i) {
		const ptxn::VersionSubsequenceMessage& m = *iter;
		ASSERT_EQ(beginVersion, m.version);
		ASSERT_EQ(i + 1, m.subsequence); // subsequence starts from 1
		ASSERT(mutations[i] == std::get<MutationRef>(m.message));
	}
	printTiming << "Received " << i << " mutations" << std::endl;
	ASSERT_EQ(i, mutations.size());

	return Void();
}
#endif // #if 0

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/run_tlog_server") {
	ptxn::test::TestDriverOptions options(params);
	// Commit validation in real TLog is not supported for now
	options.skipCommitValidation = true;
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);

	state std::string folder = "simdb" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);
	// start a real TLog server
	wait(startTLogServers(&actors, pContext, folder));
	// TODO: start fake proxy to talk to real TLog servers.
	startFakeSequencer(actors, pContext);
	startFakeProxy(actors, pContext);
	wait(quorum(actors, 1));
	platform::eraseDirectoryRecursive(folder);
	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/peek_tlog_server") {
	state ptxn::test::TestDriverOptions options(params);
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);

	for (const auto& group : pContext->tLogGroups) {
		std::cout << "TLog Group " << group.logGroupId;
		for (const auto& [storageTeamId, tags] : group.storageTeams) {
			std::cout << ", SS team " << storageTeamId;
		}
		std::cout << "\n";
	}

	state std::string folder = "simdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);
	// start a real TLog server
	wait(startTLogServers(&actors, pContext, folder));
	// wait(commitPeekAndCheck(pContext));

	platform::eraseDirectoryRecursive(folder);
	return Void();
}

namespace {

// ACTOR Future<Void> commitInject

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/commit_peek") {
	return Void();
}
