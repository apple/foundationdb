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

Future<Void> initializeTLogForPeekTest(MessageTransferModel transferModel, std::shared_ptr<FakeTLogContext>& pContext) {
	pContext->pTLogInterface = getNewTLogInterface(transferModel);
	pContext->pTLogInterface->initEndpoints();

	return getFakeTLogActor(transferModel, pContext);
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

	state TLogPeekRequest request(debugID, beginVersion, endVersion, storageTeamID);
	print::print(request);

	state TLogPeekReply reply = wait(pContext->pTLogInterface->peek.getReply(request));
	print::print(reply);

	// Locate the messages in TLog storage
	int messagesCount = 0;
	VersionSubsequenceMessage* pOriginalMessage = pContext->storageTeamMessages[storageTeamID].begin();
	// Because the messages are *randomly* distributed over teams, and we are picking up version from *ALL* versions,
	// it is entirely possible that for one team, a version is missing. It is then necesssary to check for boundary, and
	// we check for less than beginVersion rather than equal to beginVersion.
	while (pOriginalMessage != pContext->storageTeamMessages[storageTeamID].end() &&
	       pOriginalMessage->version < beginVersion)
		++pOriginalMessage;

	SubsequencedMessageDeserializer deserializer(reply.data);
	// We *CANNOT* do a
	//   std::equal(deserializer.begin(), deserializer.end(), expectedBegin, expectedEnd)
	// where expectedBegin/expectedEnd refer to iterators to the data in FakeTLog messages. Because the FakeTLog/TLog
	// might return less data if the size of the serialized data is too big.
	for (SubsequencedMessageDeserializer::iterator iter = deserializer.begin(); iter != deserializer.end();
	     ++iter, ++pOriginalMessage, ++messagesCount) {
		ASSERT(*iter == *pOriginalMessage);
	}

	printTiming << "Checked " << messagesCount << " ." << std::endl;

	return Void();
}

} // anonymous namespace

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/readFromSerialization") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state int numPeeks = 0;
	state Future<Void> tLog;

	ptxn::test::fillTLogWithRandomMutations(
	    pContext, options.initialVersion, options.numMutations, options.numStorageTeams);
	tLog = ptxn::test::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

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
	ptxn::test::fillTLogWithRandomMutations(
	    pContext, options.initialVersion, options.numMutations, options.numStorageTeams);
	tLog = ptxn::test::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

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

		for (const ptxn::VersionSubsequenceMessage& message : *pCursor) {
			ASSERT(message == pContext->storageTeamMessages[storageTeamID][index++]);
		}
	}

	printTiming << "Checked " << index << " messages out of " << pContext->storageTeamMessages[storageTeamID].size()
	            << " messages." << std::endl;
	ASSERT_EQ(index, pContext->storageTeamMessages[storageTeamID].size());

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedPeekCursor/base") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state Future<Void> tLog;
	state std::vector<std::unique_ptr<ptxn::PeekCursorBase>> cursorPtrs;
	state ptxn::test::print::PrintTiming printTiming("TestMergedPeekCursor");

	// Limit the size of reply, to force multiple peeks
	pContext->maxBytesPerPeek = params.getInt("maxBytesPerPeek").orDefault(32 * 1024);
	ptxn::test::fillTLogWithRandomMutations(
	    pContext, options.initialVersion, options.numMutations, options.numStorageTeams);
	tLog = ptxn::test::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	for (auto storageTeamID : pContext->storageTeamIDs) {
		cursorPtrs.emplace_back(
		    new ptxn::StorageTeamPeekCursor(options.initialVersion, storageTeamID, pContext->pTLogInterface.get()));
	}

	state std::shared_ptr<ptxn::MergedPeekCursor> pMergedCursor =
	    std::make_shared<ptxn::MergedPeekCursor>(cursorPtrs.begin(), cursorPtrs.end());
	state size_t index = 0;
	loop {
		state bool remoteDataAvailable = wait(pMergedCursor->remoteMoreAvailable());
		if (!remoteDataAvailable) {
			printTiming << "TLog reported no more messages available." << std::endl;
			break;
		}

		for (const ptxn::VersionSubsequenceMessage& message : *pMergedCursor) {
			ASSERT(message == pContext->allMessages[index++]);
		}
	}

	printTiming << "Checked " << index << " messages out of " << pContext->allMessages.size() << " messages."
	            << std::endl;
	ASSERT_EQ(index, pContext->allMessages.size());

	return Void();
}

namespace ptxn::test {

namespace {

const Version TEST_INITIAL_VERSION = 1000;
const int NUM_VERSIONS = 4;
const Version STEP_VERSION = 5;
const int NUM_MUTATIONS_PER_VERSION = 8;
const int NUM_TEAMS = 4;

// Fill the TLog with the mutations:
//
// Version                1000          1005          1010          1015
// Team 0 Subseq          1, 5          1, 5          1, 5          1, 5
// Team 1 Subseq          2, 6          2, 6          2, 6          2, 6
// Team 2 Subseq          3, 7          3, 7          3, 7          3, 7
// Team 3 Subseq          4, 8          4, 8          4, 8          4, 8
//
// For each mutation, the key and value are randomized string.
Future<Void> initializeTLogForCursorTest(std::shared_ptr<FakeTLogContext>& pContext) {
	auto& storageTeamIDs = pContext->storageTeamIDs;
	auto& versions = pContext->versions;
	auto& allMessages = pContext->allMessages;
	Arena& arena = pContext->persistenceArena;

	for (int _ = 0; _ < NUM_TEAMS; ++_) {
		storageTeamIDs.push_back(getNewStorageTeamID());
	}

	Version version = TEST_INITIAL_VERSION;
	for (int _v = 0; _v < NUM_VERSIONS; ++_v, version += STEP_VERSION) {
		Subsequence subsequence = 1;
		for (int _m = 0; _m < NUM_MUTATIONS_PER_VERSION; ++_m, ++subsequence) {
			allMessages.push_back(arena,
			                      { version,
			                        subsequence,
			                        Message(std::in_place_type<MutationRef>,
			                                MutationRef::SetValue,
			                                StringRef(arena, deterministicRandom()->randomAlphaNumeric(10)),
			                                StringRef(arena, deterministicRandom()->randomAlphaNumeric(100))) });
		}
		versions.push_back(version);
	}

	int index = 0;
	for (const auto& item : pContext->allMessages) {
		const auto& storageTeamID = storageTeamIDs[index++];
		index %= NUM_TEAMS;

		pContext->storageTeamMessages[storageTeamID].emplace_back(arena, item.version, item.subsequence, item.message);
	}

	return initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);
}

// For expectedMessageIndex:
//    first: the index in the storageTeamIDs array
//    second: the index in the VectorRef<VersionSubsequenceMessage>
// The message can be accessed by:
//     messages[storageTeamIDs[first]][second]
// See comments in initializeTLogForCursorTest
ACTOR Future<Void> verifyCursorDataMatchTLogData(std::shared_ptr<FakeTLogContext> pContext,
                                                 std::shared_ptr<PeekCursorBase> pCursor,
                                                 std::vector<std::pair<int, int>> expectedMessageIndex) {

	state std::vector<StorageTeamID>& storageTeamIDs = pContext->storageTeamIDs;
	state std::unordered_map<ptxn::StorageTeamID, VectorRef<ptxn::VersionSubsequenceMessage>>& storageTeamMessages =
	    pContext->storageTeamMessages;
	state std::vector<std::pair<int, int>>::const_iterator expectedMessageIter;
	state PeekCursorBase::iterator iter = pCursor->begin();

	// We *CANNOT* initialize the expectedMessageIndex in the definition line, as `state` makes expectedMessageIndex
	// there different from expectedMessageIndex here. The expectedMessageIndex there is the vector passed in to the
	// constructor, and the expectedMessageIndex here is the local copy of the original vector.
	expectedMessageIter = expectedMessageIndex.begin();
	loop {
		bool hasMore = wait(pCursor->remoteMoreAvailable());
		ASSERT(hasMore);

		iter = pCursor->begin();
		while (iter != pCursor->end() && expectedMessageIter != expectedMessageIndex.end()) {
			if (*iter != storageTeamMessages[storageTeamIDs[expectedMessageIter->first]][expectedMessageIter->second]) {
				std::cerr
				    << "Expected: "
				    << storageTeamMessages[storageTeamIDs[expectedMessageIter->first]][expectedMessageIter->second]
				    << std::endl;
				std::cerr << "Actual:   " << *iter << std::endl;
				ASSERT(*iter ==
				       storageTeamMessages[storageTeamIDs[expectedMessageIter->first]][expectedMessageIter->second]);
			}

			++iter;
			++expectedMessageIter;
		}
		if (expectedMessageIter == expectedMessageIndex.end()) {
			break;
		}
	}

	return Void();
}

} // namespace

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedPeekCursor/addCursorOnTheFly") {
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state const std::vector<ptxn::StorageTeamID>& storageTeamIDs = pContext->storageTeamIDs;
	state Future<Void> tLog = ptxn::test::initializeTLogForCursorTest(pContext);
	state std::shared_ptr<ptxn::MergedPeekCursor> pCursor = std::make_shared<ptxn::MergedPeekCursor>();

	pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[0], pContext->pTLogInterface.get()));
	pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[1], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 2);
	wait(ptxn::test::verifyCursorDataMatchTLogData(pContext, pCursor, { { 0, 0 }, { 1, 0 }, { 0, 1 }, { 1, 1 } }));

	pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[2], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 3);
	wait(ptxn::test::verifyCursorDataMatchTLogData(
	    pContext, pCursor, { { 2, 0 }, { 2, 1 }, { 0, 2 }, { 1, 2 }, { 2, 2 } }));

	pCursor->addCursor(
	    std::make_unique<ptxn::StorageTeamPeekCursor>(ptxn::test::TEST_INITIAL_VERSION + ptxn::test::STEP_VERSION * 2,
	                                                  storageTeamIDs[3],
	                                                  pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 4);
	wait(ptxn::test::verifyCursorDataMatchTLogData(
	    pContext, pCursor, { { 0, 3 }, { 1, 3 }, { 2, 3 }, { 0, 4 }, { 1, 4 }, { 2, 4 }, { 3, 4 } }));

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedStorageTeamPeekCursor/removeCursorOnTheFly") {
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state const std::vector<ptxn::StorageTeamID>& storageTeamIDs = pContext->storageTeamIDs;
	state Future<Void> tLog = ptxn::test::initializeTLogForCursorTest(pContext);
	state std::shared_ptr<ptxn::MergedStorageTeamPeekCursor> pCursor =
	    std::make_shared<ptxn::MergedStorageTeamPeekCursor>();

	pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[0], pContext->pTLogInterface.get()));
	pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[1], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 2);
	wait(ptxn::test::verifyCursorDataMatchTLogData(pContext, pCursor, { { 0, 0 }, { 1, 0 }, { 0, 1 }, { 1, 1 } }));

	pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[2], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 3);
	pCursor->removeCursor(storageTeamIDs[1]);
	ASSERT_EQ(pCursor->getNumActiveCursors(), 2);
	wait(ptxn::test::verifyCursorDataMatchTLogData(pContext, pCursor, { { 2, 0 }, { 2, 1 }, { 0, 2 }, { 2, 2 } }));

	std::vector<ptxn::StorageTeamID> activeTeamIDs(pCursor->getCursorTeamIDs());
	ASSERT((activeTeamIDs[0] == storageTeamIDs[0] && activeTeamIDs[1] == storageTeamIDs[2]) ||
	       (activeTeamIDs[0] == storageTeamIDs[2] && activeTeamIDs[1] == storageTeamIDs[0]));

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/advanceTo") {
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state const std::vector<ptxn::StorageTeamID>& storageTeamIDs = pContext->storageTeamIDs;
	state Future<Void> tLog = ptxn::test::initializeTLogForCursorTest(pContext);
	state std::shared_ptr<ptxn::MergedStorageTeamPeekCursor> pCursor =
	    std::make_shared<ptxn::MergedStorageTeamPeekCursor>();

	for (int i = 0; i < ptxn::test::NUM_TEAMS; ++i) {
		pCursor->addCursor(std::make_unique<ptxn::StorageTeamPeekCursor>(
		    ptxn::test::TEST_INITIAL_VERSION, storageTeamIDs[i], pContext->pTLogInterface.get()));
	}
	bool hasMore = wait(pCursor->remoteMoreAvailable());
	ASSERT(hasMore);
	ASSERT(*pCursor->begin() == pContext->storageTeamMessages[storageTeamIDs[0]][0]);

	// Advance to an non-existing version: the cursor should move to the next closest version, first subsequence
	wait(advanceTo(pCursor.get(), ptxn::test::TEST_INITIAL_VERSION + (ptxn::test::STEP_VERSION - 1), 2));
	ASSERT(*pCursor->begin() == pContext->storageTeamMessages[storageTeamIDs[0]][2]);

	// Advance to a specific version/subsequence
	wait(advanceTo(pCursor.get(), ptxn::test::TEST_INITIAL_VERSION + ptxn::test::STEP_VERSION * 2, 3));
	ASSERT(*pCursor->begin() == pContext->storageTeamMessages[storageTeamIDs[2]][4]);

	// Advance to the end
	wait(advanceTo(pCursor.get(), ptxn::test::TEST_INITIAL_VERSION + ptxn::test::STEP_VERSION * 3 + 1, 0));
	ASSERT(pCursor->begin() == pCursor->end());

	return Void();
}
