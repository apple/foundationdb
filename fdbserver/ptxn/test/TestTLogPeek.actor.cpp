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

#include "fdbserver/ptxn/test/FakeTLog.actor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/TLogPeekCursor.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace ptxn::test::tLogPeek {

TestTLogPeekOptions::TestTLogPeekOptions(const UnitTestParameters& params)
  : numMutations(params.getInt("numMutations").orDefault(DEFAULT_NUM_MUTATIONS)),
    numTeams(params.getInt("numTeams").orDefault(DEFAULT_NUM_TEAMS)),
    initialVersion(params.getInt("initialVersion").orDefault(DEFAULT_INITIAL_VERSION)),
    peekTimes(params.getInt("peekTimes").orDefault(DEFAULT_PEEK_TIMES)) {}

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

	state TLogPeekRequest request;
	request.debugID = debugID;
	request.beginVersion = beginVersion;
	request.endVersion = endVersion;
	request.teamID = teamID;
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

} // namespace ptxn::test::tLogPeek

TEST_CASE("/fdbserver/ptxn/test/tlogPeek/readFromSerialization") {
	state ptxn::test::tLogPeek::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state int numPeeks = 0;
	state Future<Void> tLog;

	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations, options.numTeams);
	tLog = ptxn::test::tLogPeek::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	loop {
		wait(ptxn::test::tLogPeek::peekAndCheck(pContext));

		if (++numPeeks == options.peekTimes) {
			break;
		}
	}

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/ServerTeamPeekCursor") {
	state ptxn::test::tLogPeek::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state Future<Void> tLog;
	state std::shared_ptr<ptxn::ServerTeamPeekCursor> pCursor;
	state ptxn::TeamID teamID;

	// Limit the size of reply, to force multiple peeks
	pContext->maxBytesPerPeek = params.getInt("maxBytesPerPeek").orDefault(32 * 1024);
	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations, options.numTeams);
	tLog = ptxn::test::tLogPeek::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	teamID = ptxn::test::randomlyPick(pContext->teamIDs);
	pCursor =
	    std::make_shared<ptxn::ServerTeamPeekCursor>(options.initialVersion, teamID, pContext->pTLogInterface.get());
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

TEST_CASE("/fdbserver/ptxn/test/tLogPeek/cursor/MergedPeekCursor") {
	state ptxn::test::tLogPeek::TestTLogPeekOptions options(params);
	state std::shared_ptr<ptxn::test::FakeTLogContext> pContext = std::make_shared<ptxn::test::FakeTLogContext>();
	state Future<Void> tLog;
	state std::vector<ptxn::PeekCursorBasePtr> cursorPtrs;

	// Limit the size of reply, to force multiple peeks
	pContext->maxBytesPerPeek = params.getInt("maxBytesPerPeek").orDefault(32 * 1024);
	ptxn::test::fillTLogWithRandomMutations(pContext, options.initialVersion, options.numMutations, options.numTeams);
	tLog = ptxn::test::tLogPeek::initializeTLogForPeekTest(ptxn::MessageTransferModel::TLogActivelyPush, pContext);

	for (auto teamID : pContext->teamIDs) {
		cursorPtrs.push_back(
		    new ptxn::ServerTeamPeekCursor(options.initialVersion, teamID, pContext->pTLogInterface.get()));
	}

	state std::shared_ptr<ptxn::MergedPeekCursor> pMergedCursor =
	    std::make_shared<ptxn::MergedPeekCursor>(std::begin(cursorPtrs), std::end(cursorPtrs));
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

	for (auto pCursor : cursorPtrs) {
		delete pCursor;
	}

	return Void();
}
