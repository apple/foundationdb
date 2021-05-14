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
    numTeams(params.getInt("numTeams").orDefault(DEFAULT_NUM_TEAMS)),
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
	state std::vector<TeamID>& teamIDs = pContext->teamIDs;
	state std::vector<Version>& versions = pContext->versions;

	state Optional<UID> debugID(randomUID());
	state TeamID teamID(randomlyPick(teamIDs));

	state Version beginVersion(randomlyPick(versions));
	state Version endVersion(beginVersion + deterministicRandom()->randomInt(5, 20));

	state TLogPeekRequest request(debugID, beginVersion, endVersion, teamID);
	print::print(request);

	state TLogPeekReply reply = wait(pContext->pTLogInterface->peek.getReply(request));
	print::print(reply);

	// Locate the mutations in TLog storage
	int mutationsCount = 0;
	VersionSubsequenceMutation* pOriginalMutation = pContext->mutations[teamID].begin();
	// Because the mutations are *randomly* distributed over teams, and we are picking up version from *ALL* versions,
	// it is entirely possible that for one team, a version is missing. It is then necesssary to check for boundary, and
	// we check for less than beginVersion rather than equal to beginVersion.
	while (pOriginalMutation != pContext->mutations[teamID].end() && pOriginalMutation->version < beginVersion)
		++pOriginalMutation;

	TLogStorageServerMessageDeserializer deserializer(reply.arena, reply.data);
	// We *CANNOT* do a
	//   std::equal(deserializer.begin(), deserializer.end(), expectedBegin, expectedEnd)
	// where expectedBegin/expectedEnd refer to iterators to the data in FakeTLog mutations. Because the FakeTLog/TLog
	// might return less data if the size of the serialized data is too big.
	for (TLogStorageServerMessageDeserializer::iterator iter = deserializer.begin(); iter != deserializer.end();
	     ++iter, ++pOriginalMutation, ++mutationsCount) {
		ASSERT(*iter == *pOriginalMutation);
	}

	std::cout << __FUNCTION__ << ">> Checked " << mutationsCount << " mutations." << std::endl;

	return Void();
}

} // anonymous namespace

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/tlogPeek/readFromSerialization") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state int numPeeks = 0;
	state Future<Void> tLog;

	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations, options.numTeams);
	tLog = ptxn::test::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	loop {
		wait(ptxn::test::peekAndCheck(pContext));

		if (++numPeeks == options.peekTimes) {
			break;
		}
	}

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/TLogGroupPeekCursor") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state Future<Void> tLog;
	state std::shared_ptr<ptxn::TLogGroupPeekCursor> pCursor;
	state ptxn::TeamID teamID;

	// Limit the size of reply, to force multiple peeks
	pContext->maxBytesPerPeek = params.getInt("maxBytesPerPeek").orDefault(32 * 1024);
	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations, options.numTeams);
	tLog = ptxn::test::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	teamID = ptxn::test::randomlyPick(pContext->teamIDs);
	pCursor =
	    std::make_shared<ptxn::TLogGroupPeekCursor>(options.initialVersion, teamID, pContext->pTLogInterface.get());
	state size_t index = 0;
	loop {
		std::cout << "TestServerTeamPeekCursor"
		          << ">> Querying team " << teamID.toString() << " from version: " << pCursor->getLastVersion()
		          << std::endl;
		state bool remoteDataAvailable = wait(pCursor->remoteMoreAvailable());
		if (!remoteDataAvailable) {
			std::cout << "TestServerTeamPeekCursor"
			          << ">> TLog reported no more mutations available." << std::endl;
			break;
		}

		// There are two ways iterating local data in pCursor, one is using
		//    hasRemaining
		//    get
		//    next
		// e.g.
		// while (pCursor->hasRemaining()) {
		//     ASSERT(pCursor->get() == pContext->mutations[teamID][index++]);
		//     pCursor->next();
		// }
		// the other way is to use iterators. Yet the iterator here is *NOT* standard input iterator, see comments in
		// PeekCursorBase::iterator. In this implementation we use iterators since iterators are implemented using the
		// methods above.

		for (const ptxn::VersionSubsequenceMutation& mutation : *pCursor) {
			ASSERT(mutation == pContext->mutations[teamID][index++]);
		}
	}

	std::cout << "TestServerTeamPeekCursor"
	          << ">> Checked " << index << " mutations out of " << pContext->mutations[teamID].size() << " mutations."
	          << std::endl;
	ASSERT_EQ(index, pContext->mutations[teamID].size());

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedPeekCursor/base") {
	state ptxn::test::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state Future<Void> tLog;
	state std::vector<std::unique_ptr<ptxn::PeekCursorBase>> cursorPtrs;

	// Limit the size of reply, to force multiple peeks
	pContext->maxBytesPerPeek = params.getInt("maxBytesPerPeek").orDefault(32 * 1024);
	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations, options.numTeams);
	tLog = ptxn::test::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	for (auto teamID : pContext->teamIDs) {
		cursorPtrs.emplace_back(
		    new ptxn::TLogGroupPeekCursor(options.initialVersion, teamID, pContext->pTLogInterface.get()));
	}

	state std::shared_ptr<ptxn::MergedPeekCursor> pMergedCursor =
	    std::make_shared<ptxn::MergedPeekCursor>(cursorPtrs.begin(), cursorPtrs.end());
	state size_t index = 0;
	loop {
		state bool remoteDataAvailable = wait(pMergedCursor->remoteMoreAvailable());
		if (!remoteDataAvailable) {
			std::cout << "TestMergedPeekCursor"
			          << ">> TLog reported no more mutations available." << std::endl;
			break;
		}

		for (const ptxn::VersionSubsequenceMutation& mutation : *pMergedCursor) {
			ASSERT(mutation == pContext->allMutations[index++]);
		}
	}

	std::cout << "TestServerTeamPeekCursor"
	          << ">> Checked " << index << " mutations out of " << pContext->allMutations.size() << " mutations."
	          << std::endl;
	ASSERT_EQ(index, pContext->allMutations.size());

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
	auto& teamIDs = pContext->teamIDs;
	auto& versions = pContext->versions;
	auto& allMutations = pContext->allMutations;
	Arena& arena = pContext->persistenceArena;

	for (int _ = 0; _ < NUM_TEAMS; ++_) {
		teamIDs.push_back(getNewTeamID());
	}

	Version version = TEST_INITIAL_VERSION;
	for (int _v = 0; _v < NUM_VERSIONS; ++_v, version += STEP_VERSION) {
		Subsequence subsequence = 1;
		for (int _m = 0; _m < NUM_MUTATIONS_PER_VERSION; ++_m, ++subsequence) {
			allMutations.push_back(arena,
			                       { version,
			                         subsequence,
			                         { MutationRef::SetValue,
			                           StringRef(arena, deterministicRandom()->randomAlphaNumeric(10)),
			                           StringRef(arena, deterministicRandom()->randomAlphaNumeric(100)) } });
		}
		versions.push_back(version);
	}

	int index = 0;
	for (const auto& item : pContext->allMutations) {
		const auto& teamID = teamIDs[index++];
		index %= NUM_TEAMS;

		pContext->mutations[teamID].emplace_back(arena, item.version, item.subsequence, item.mutation);
	}

	return initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);
}

// For expectedMutationIndex:
//    first: the index in the teamIDs array
//    second: the index in the VectorRef<VersionSubsequenceMutation>
// The mutation can be accessed by:
//     mutations[teamIDs[first]][second]
// See comments in initializeTLogForCursorTest
ACTOR Future<Void> verifyCursorDataMatchTLogData(std::shared_ptr<FakeTLogContext> pContext,
                                                 std::shared_ptr<PeekCursorBase> pCursor,
                                                 std::vector<std::pair<int, int>> expectedMutationIndex) {

	state std::vector<TeamID>& teamIDs = pContext->teamIDs;
	state std::unordered_map<ptxn::TeamID, VectorRef<ptxn::VersionSubsequenceMutation>>& mutations =
	    pContext->mutations;
	state std::vector<std::pair<int, int>>::const_iterator expectedMutationIter;
	state PeekCursorBase::iterator iter = pCursor->begin();

	// We *CANNOT* initialize the expectedMutationIndex in the definition line, as `state` makes expectedMutationIndex
	// there different from expectedMutationIndex here. The expectedMutationIndex there is the vector passed in to the
	// constructor, and the expectedMutationIndex here is the local copy of the original vector.
	expectedMutationIter = expectedMutationIndex.begin();
	loop {
		bool hasMore = wait(pCursor->remoteMoreAvailable());
		ASSERT(hasMore);

		iter = pCursor->begin();
		while (iter != pCursor->end() && expectedMutationIter != expectedMutationIndex.end()) {
			if (*iter != mutations[teamIDs[expectedMutationIter->first]][expectedMutationIter->second]) {
				std::cerr << "Expected: "
				          << mutations[teamIDs[expectedMutationIter->first]][expectedMutationIter->second] << std::endl;
				std::cerr << "Actual:   " << *iter << std::endl;
				ASSERT(*iter == mutations[teamIDs[expectedMutationIter->first]][expectedMutationIter->second]);
			}

			++iter;
			++expectedMutationIter;
		}
		if (expectedMutationIter == expectedMutationIndex.end()) {
			break;
		}
	}

	return Void();
}

} // namespace

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedPeekCursor/addCursorOnTheFly") {
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state const std::vector<ptxn::TeamID>& teamIDs = pContext->teamIDs;
	state Future<Void> tLog = ptxn::test::initializeTLogForCursorTest(pContext);
	state std::shared_ptr<ptxn::MergedPeekCursor> pCursor = std::make_shared<ptxn::MergedPeekCursor>();

	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, teamIDs[0], pContext->pTLogInterface.get()));
	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, teamIDs[1], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 2);
	wait(ptxn::test::verifyCursorDataMatchTLogData(pContext, pCursor, { { 0, 0 }, { 1, 0 }, { 0, 1 }, { 1, 1 } }));

	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, teamIDs[2], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 3);
	wait(ptxn::test::verifyCursorDataMatchTLogData(
	    pContext, pCursor, { { 2, 0 }, { 2, 1 }, { 0, 2 }, { 1, 2 }, { 2, 2 } }));

	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION + ptxn::test::STEP_VERSION * 2, teamIDs[3], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 4);
	wait(ptxn::test::verifyCursorDataMatchTLogData(
	    pContext, pCursor, { { 0, 3 }, { 1, 3 }, { 2, 3 }, { 0, 4 }, { 1, 4 }, { 2, 4 }, { 3, 4 } }));

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedServerTeamPeekCursor/removeCursorOnTheFly") {
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state const std::vector<ptxn::TeamID>& teamIDs = pContext->teamIDs;
	state Future<Void> tLog = ptxn::test::initializeTLogForCursorTest(pContext);
	state std::shared_ptr<ptxn::MergedServerTeamPeekCursor> pCursor =
	    std::make_shared<ptxn::MergedServerTeamPeekCursor>();

	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, teamIDs[0], pContext->pTLogInterface.get()));
	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, teamIDs[1], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 2);
	wait(ptxn::test::verifyCursorDataMatchTLogData(pContext, pCursor, { { 0, 0 }, { 1, 0 }, { 0, 1 }, { 1, 1 } }));

	pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
	    ptxn::test::TEST_INITIAL_VERSION, teamIDs[2], pContext->pTLogInterface.get()));
	ASSERT_EQ(pCursor->getNumActiveCursors(), 3);
	pCursor->removeCursor(teamIDs[1]);
	ASSERT_EQ(pCursor->getNumActiveCursors(), 2);
	wait(ptxn::test::verifyCursorDataMatchTLogData(pContext, pCursor, { { 2, 0 }, { 2, 1 }, { 0, 2 }, { 2, 2 } }));

	std::vector<ptxn::TeamID> activeTeamIDs(pCursor->getCursorTeamIDs());
	ASSERT((activeTeamIDs[0] == teamIDs[0] && activeTeamIDs[1] == teamIDs[2]) ||
	       (activeTeamIDs[0] == teamIDs[2] && activeTeamIDs[1] == teamIDs[0]));

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/advanceTo") {
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state const std::vector<ptxn::TeamID>& teamIDs = pContext->teamIDs;
	state Future<Void> tLog = ptxn::test::initializeTLogForCursorTest(pContext);
	state std::shared_ptr<ptxn::MergedServerTeamPeekCursor> pCursor =
	    std::make_shared<ptxn::MergedServerTeamPeekCursor>();

	for (int i = 0; i < ptxn::test::NUM_TEAMS; ++i) {
		pCursor->addCursor(std::make_unique<ptxn::TLogGroupPeekCursor>(
		    ptxn::test::TEST_INITIAL_VERSION, teamIDs[i], pContext->pTLogInterface.get()));
	}
	bool hasMore = wait(pCursor->remoteMoreAvailable());
	ASSERT(hasMore);
	ASSERT(*pCursor->begin() == pContext->mutations[teamIDs[0]][0]);

	// Advance to an non-existing version: the cursor should move to the next closest version, first subsequence
	wait(advanceTo(pCursor.get(), ptxn::test::TEST_INITIAL_VERSION + (ptxn::test::STEP_VERSION - 1), 2));
	ASSERT(*pCursor->begin() == pContext->mutations[teamIDs[0]][2]);

	// Advance to a specific version/subsequence
	wait(advanceTo(pCursor.get(), ptxn::test::TEST_INITIAL_VERSION + ptxn::test::STEP_VERSION * 2, 3));
	ASSERT(*pCursor->begin() == pContext->mutations[teamIDs[2]][4]);

	// Advance to the end
	wait(advanceTo(pCursor.get(), ptxn::test::TEST_INITIAL_VERSION + ptxn::test::STEP_VERSION * 3 + 1, 0));
	ASSERT(pCursor->begin() == pCursor->end());

	return Void();
}
