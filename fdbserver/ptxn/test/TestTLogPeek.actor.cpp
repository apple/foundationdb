/*
 * TestTLogPeek.actor.cpp
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

#include "fdbserver/ptxn/test/TestTLogPeek.h"

#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/FakeTLog.actor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/TLogPeekCursor.actor.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace ptxn::test {

TestTLogPeekOptions::TestTLogPeekOptions(const UnitTestParameters& params)
  : numMutations(params.getInt("numMutations").orDefault(DEFAULT_NUM_MUTATIONS)),
    numStorageTeams(params.getInt("numStorageTeams").orDefault(DEFAULT_NUM_TEAMS)),
    initialVersion(params.getInt("initialVersion").orDefault(DEFAULT_INITIAL_VERSION)),
    peekTimes(params.getInt("peekTimes").orDefault(DEFAULT_PEEK_TIMES)) {}

namespace {

Future<Void> initializeTLogForPeekTest(MessageTransferModel transferModel,
                                       const int numStorageTeams,
                                       std::shared_ptr<FakeTLogContext>& pContext) {
	// We use the CommitRecord in the context -- no other parts needed, so no need to call initTestDriverContext
	if (!pContext->pTestDriverContext)
		pContext->pTestDriverContext.reset(new TestDriverContext());

	pContext->pTLogInterface = getNewTLogInterface(transferModel);
	pContext->pTLogInterface->initEndpoints();

	// We do NOT use the storageTeamID in the TestDriverContext
	for (auto _ = 0; _ < numStorageTeams; ++_) {
		pContext->storageTeamIDs.push_back(getNewStorageTeamID());
	}

	return getFakeTLogActor(transferModel, pContext);
}

void fillTLogWithRandomMutations(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                                 const Version& initialVersion,
                                 const int numMutations) {
	ASSERT(pFakeTLogContext);
	ASSERT(pFakeTLogContext->pTestDriverContext);

	print::PrintTiming printTiming(__FUNCTION__);

	Arena& arena = pFakeTLogContext->persistenceArena;
	VectorRef<MutationRef> mutationRefs;
	generateMutationRefs(numMutations, arena, mutationRefs);

	CommitRecord& commitRecord = pFakeTLogContext->pTestDriverContext->commitRecord;
	if (!commitRecord.messageArena.sameArena(arena)) {
		commitRecord.messageArena.dependsOn(arena);
	}

	auto& epochVersionMessages = pFakeTLogContext->epochVersionMessages;
	Version version = initialVersion;
	Subsequence subsequence = 0;

	pFakeTLogContext->versions.push_back(version);
	for (int i = 0; i < numMutations; ++i) {
		// 20% chance version change
		if (deterministicRandom()->randomInt(0, 10) <= 2) {
			version += deterministicRandom()->randomInt(5, 10);
			subsequence = 0;
			pFakeTLogContext->versions.push_back(version);
		}
		const StorageTeamID& storageTeamID = randomlyPick(pFakeTLogContext->storageTeamIDs);
		epochVersionMessages[version][storageTeamID].push_back(arena, { version, ++subsequence, mutationRefs[i] });
	}
}

std::vector<ptxn::VersionSubsequenceMessage> collectAllMessagesFromCommitRecord(const CommitRecord& commitRecord) {
	std::vector<ptxn::VersionSubsequenceMessage> allMessages;

	for (const auto& [version, teamedMessages] : commitRecord.messages) {
		for (const auto& [storageTeamID, subsequencedMessages] : teamedMessages) {
			for (int i = 0; i < subsequencedMessages.size(); ++i) {
				allMessages.emplace_back(version, subsequencedMessages[i].first, subsequencedMessages[i].second);
			}
		}
	}
	std::sort(std::begin(allMessages), std::end(allMessages));

	return allMessages;
}

// Randomly peek data from FakeTLog and verify if the data is consistent
ACTOR Future<Void> peekAndCheck(std::shared_ptr<FakeTLogContext> pContext) {
	state print::PrintTiming printTiming("peekAndCheck");

	state std::vector<StorageTeamID>& storageTeamIDs = pContext->storageTeamIDs;
	state std::vector<Version>& versions = pContext->versions;

	state Optional<UID> debugID(randomUID());
	state StorageTeamID storageTeamID(randomlyPick(storageTeamIDs));

	state Version beginVersion(randomlyPick(versions));
	state Version endVersion(beginVersion + deterministicRandom()->randomInt(5, 20));

	state TLogPeekRequest request(debugID, beginVersion, endVersion, false, false, storageTeamID, TLogGroupID());
	print::print(request);

	state TLogPeekReply reply = wait(pContext->pTLogInterface->peek.getReply(request));
	print::print(reply);

	return Void();
}

} // anonymous namespace

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/readFromSerialization") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state int numPeeks = 0;
	state Future<Void> tLog;

	tLog = ptxn::test::initializeTLogForPeekTest(
	    ptxn::MessageTransferModel::TLogActivelyPush, options.numStorageTeams, pContext);
	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations);

	loop {
		wait(ptxn::test::peekAndCheck(pContext));

		if (++numPeeks == options.peekTimes) {
			break;
		}
	}

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/StorageTeamPeekCursor") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state Future<Void> tLog;
	state std::shared_ptr<ptxn::StorageTeamPeekCursor> pCursor;
	state ptxn::StorageTeamID storageTeamID;
	state ptxn::test::print::PrintTiming printTiming("TestStorageTeamPeekCursor");

	// Limit the size of reply, to force multiple peeks
	pContext->maxBytesPerPeek = params.getInt("maxBytesPerPeek").orDefault(32 * 1024);
	tLog = ptxn::test::initializeTLogForPeekTest(
	    ptxn::MessageTransferModel::TLogActivelyPush, options.numStorageTeams, pContext);
	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations);

	storageTeamID = ptxn::test::randomlyPick(pContext->storageTeamIDs);
	pCursor = std::make_shared<ptxn::StorageTeamPeekCursor>(
	    options.initialVersion, storageTeamID, pContext->pTLogInterface.get());
	state size_t index = 0;
	loop {
		printTiming << "Querying team " << storageTeamID.toString() << " from version: " << pCursor->getLastVersion()
		            << std::endl;
		state bool remoteDataAvailable = wait(pCursor->remoteMoreAvailable());
		if (!remoteDataAvailable) {
			printTiming << "TLog reported no more messages available." << std::endl;
			break;
		}

		// There are two ways iterating local data in pCursor, one is using
		//    hasRemaining
		//    get
		//    next
		// e.g.
		// while (pCursor->hasRemaining()) {
		//     ASSERT(pCursor->get() == pContext->storageTeamMessages[storageTeamID][index++]);
		//     pCursor->next();
		// }
		// the other way is to use iterators. Yet the iterator here is *NOT* standard input iterator, see comments in
		// PeekCursorBase::iterator. In this implementation we use iterators since iterators are implemented using the
		// methods above.
	}

	return Void();
}
