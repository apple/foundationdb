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

#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "flow/DeterministicRandom.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h"

namespace ptxn::test {

const double CHECK_PERSIST_INTERVAL = 0.1;
const int MAX_CHECK_TIMES = 10;

namespace {

std::vector<std::pair<StorageTeamID, Message>> prepareMessage(const std::vector<StorageTeamID>& storageTeamIDs,
                                                              Arena& mutationsArena) {
	std::vector<std::pair<StorageTeamID, Message>> messages;
	for (int _ = 0; _ < deterministicRandom()->randomInt(1, 12); ++_) {
		StorageTeamID storageTeamID = randomlyPick(storageTeamIDs);

		messages.emplace_back(randomlyPick(storageTeamIDs),
		                      Message(std::in_place_type<MutationRef>,
		                              mutationsArena,
		                              MutationRef::SetValue,
		                              StringRef(format("Key%d", deterministicRandom()->randomInt(0, 100))),
		                              StringRef(format("Value%d", deterministicRandom()->randomInt(0, 100)))));
	}

	return messages;
}

void recordMessages(const std::vector<std::pair<StorageTeamID, Message>>& messages,
                    const Version& version,
                    std::vector<CommitRecord>& commitRecord) {

	std::unordered_map<StorageTeamID, std::vector<Message>> storageTeamMessages;
	for (const auto& [storageTeamID, message] : messages) {
		storageTeamMessages[storageTeamID].push_back(message);
	}
	for (auto& [storageTeamID, messages] : storageTeamMessages) {
		commitRecord.emplace_back(version, storageTeamID, std::move(messages));
	}
}

} // anonymous namespace

ACTOR Future<Void> fakeProxy(std::shared_ptr<FakeProxyContext> pFakeProxyContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeProxyContext->pTestDriverContext;
	state std::shared_ptr<MasterInterface> pSequencerInterface =
	    pFakeProxyContext->pTestDriverContext->sequencerInterface;
	state int numStorageTeams = pFakeProxyContext->pTestDriverContext->numStorageTeamIDs;
	state std::vector<std::pair<StorageTeamID, Message>> fakeMessages;
	state std::vector<CommitRecord>& commitRecord = pFakeProxyContext->pTestDriverContext->commitRecord;
	state int version = 0;
	state int commitCount = 0;
	state std::vector<Future<TLogCommitReply>> tLogCommitReplies;
	state print::PrintTiming printTiming(concatToString("fakeProxy ", pFakeProxyContext->proxyID));

	loop {
		printTiming << "Commit " << commitCount << std::endl;

		fakeMessages = prepareMessage(pTestDriverContext->storageTeamIDs, pTestDriverContext->mutationsArena);

		// Pre-resolution: get the version of the commit
		GetCommitVersionRequest commitVersionRequest(UID(), commitCount, commitCount - 1, UID());
		state GetCommitVersionReply commitVersionReply =
		    wait(pSequencerInterface->getCommitVersion.getReply(commitVersionRequest));

		recordMessages(fakeMessages, commitVersionReply.version, commitRecord);

		// Resolve
		// FIXME use the resolver role
		/*
		ResolveTransactionBatchRequest resolveRequest;
		resolveRequest.prevVersion = commitVersionReply.prevVersion;
		resolveRequest.version = commitVersionReply.version;
		resolveRequest.lastReceivedVersion = commitVersionReply.prevVersion;
		// FIXME we do not need all teams
		resolveRequest.teams = pTestDriverContext->storageTeamIDs;
		state ResolveTransactionBatchReply resolveReply =
		    wait(randomlyPick(pTestDriverContext->resolverInterfaces)->resolve.getReply(resolveRequest));
		    */

		// TLog
		ProxySubsequencedMessageSerializer serializer(commitVersionReply.version);
		for (const auto& [storageTeamID, message] : fakeMessages) {
			serializer.write(std::get<MutationRef>(message), storageTeamID);
		}

		std::unordered_map<StorageTeamID, Standalone<StringRef>> serialized = serializer.getAllSerialized();
		for (const auto& [storageTeamID, messages] : serialized) {
			auto commitVersionPair = pTestDriverContext->getCommitVersionPair(storageTeamID, commitVersionReply.version);
			printTiming << commitVersionReply.prevVersion << '\t' << commitVersionReply.version << std::endl;
			printTiming << storageTeamID.toString() << '\t' << commitVersionPair.first << '\t'
			            << commitVersionPair.second << std::endl;
			TLogCommitRequest request(deterministicRandom()->randomUniqueID(),
			                          storageTeamID,
			                          messages.arena(),
			                          messages,
			                          commitVersionPair.first,
			                          commitVersionPair.second,
			                          // commitVersionReply.prevVersion,
			                          // commitVersionReply.version,
			                          0,
			                          0,
			                          Optional<UID>(deterministicRandom()->randomUniqueID()));
			tLogCommitReplies.push_back(pTestDriverContext->getTLogInterface(storageTeamID)->commit.getReply(request));
		}

		wait(waitForAll(tLogCommitReplies));

		if (++commitCount == pFakeProxyContext->numCommits) {
			break;
		}
	}

	if (pTestDriverContext->skipCommitValidation) {
		printTiming << "Skipping commit persistence validation" << std::endl;
		return Void();
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
