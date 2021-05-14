/*
 * FakeProxy.actor.cpp
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

#include "fdbserver/ptxn/test/FakeProxy.actor.h"

#include <iostream>
#include <unordered_map>
#include <utility>

#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "flow/DeterministicRandom.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h"

namespace ptxn::test {

const double CHECK_PERSIST_INTERVAL = 0.1;
const int MAX_CHECK_TIMES = 10;

ACTOR Future<Void> fakeProxy(std::shared_ptr<FakeProxyContext> pFakeProxyContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeProxyContext->pTestDriverContext;
	state int numTeams = pFakeProxyContext->pTestDriverContext->numTeamIDs;
	state std::vector<CommitRecord>& commitRecord = pFakeProxyContext->pTestDriverContext->commitRecord;
	state Version versionGap = 10000;
	state Version version = versionGap;
	state int i = 0;
	loop {
		std::cout << "Commit " << i << std::endl;

		std::unordered_map<StorageTeamID, std::vector<MutationRef>> fakeMutations;
		ProxyTLogPushMessageSerializer serializer;

		for (int _ = 0; _ < deterministicRandom()->randomInt(1, 12); ++_) {
			StorageTeamID teamID{ pTestDriverContext->teamIDs[deterministicRandom()->randomInt(0, numTeams)] };
			MutationRef mutation(pTestDriverContext->mutationsArena,
			                     MutationRef::SetValue,
			                     StringRef(format("Key%d", deterministicRandom()->randomInt(0, 100))),
			                     StringRef(format("Value%d", deterministicRandom()->randomInt(0, 100))));
			serializer.writeMessage(mutation, teamID);

			fakeMutations[teamID].push_back(mutation);
		}

		state std::vector<Future<TLogCommitReply>> requests;
		for (auto iter = fakeMutations.begin(); iter != fakeMutations.end(); ++iter) {
			const StorageTeamID teamID = iter->first;
			auto& mutations = iter->second;

			// Here we use move semantic in order to keep the mutations in arena
			// 	pTestDriverContext->mutationsArena
			commitRecord.emplace_back(version, teamID, std::move(mutations));

			serializer.completeMessageWriting(teamID);
			Standalone<StringRef> encoded = serializer.getSerialized(teamID);
			TLogCommitRequest request(deterministicRandom()->randomUniqueID(),
			                          teamID,
			                          encoded.arena(),
			                          encoded,
			                          version - versionGap,
			                          version,
			                          0,
			                          0,
			                          Optional<UID>());
			requests.push_back(pTestDriverContext->getTLogInterface(teamID)->commit.getReply(request));
		}
		version += versionGap;

		printCommitRecord(pTestDriverContext->commitRecord);

		wait(waitForAll(requests));

		if (++i == pFakeProxyContext->numCommits) {
			break;
		}
	}

	// Wait for all commits being completed persisted/timeout
	state int numChecks = 0;
	loop {
		if (isAllRecordsValidated(pTestDriverContext->commitRecord)) {
			break;
		}
		if (++numChecks >= MAX_CHECK_TIMES) {
			throw internal_error_msg("Timeout waiting persistence");
		}
		wait(delay(CHECK_PERSIST_INTERVAL));
	}

	return Void();
}

} // namespace ptxn::test
