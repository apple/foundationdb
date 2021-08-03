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
#include "fdbserver/ptxn/test/CommitUtils.h"
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

const int NUM_MESSAGE_PER_COMMIT = 100;

void prepareMessageForVersion(const Version& version,
                              const int numMessages,
                              const std::vector<StorageTeamID> storageTeamIDs,
                              CommitRecord& commitRecord) {
	Arena& arena = commitRecord.messageArena;
	VectorRef<MutationRef> mutationRefs;
	generateMutationRefs(numMessages, arena, mutationRefs);
	distributeMutationRefs(mutationRefs, version, storageTeamIDs, commitRecord);
}

} // anonymous namespace

ACTOR Future<Void> fakeProxy(std::shared_ptr<FakeProxyContext> pFakeProxyContext) {
	state print::PrintTiming printTiming(concatToString("fakeProxy ", pFakeProxyContext->proxyID));

	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeProxyContext->pTestDriverContext;
	state std::shared_ptr<MasterInterface> pSequencerInterface =
	    pFakeProxyContext->pTestDriverContext->sequencerInterface;
	state std::vector<StorageTeamID>& storageTeamIDs = pFakeProxyContext->pTestDriverContext->storageTeamIDs;
	state CommitRecord& commitRecord = pFakeProxyContext->pTestDriverContext->commitRecord;

	state int version = 0;
	state int commitCount = 0;
	state std::vector<Future<TLogCommitReply>> tLogCommitReplies;

	loop {
		printTiming << "Commit " << commitCount << std::endl;

		// Pre-resolution: get the version of the commit
		GetCommitVersionRequest commitVersionRequest(UID(), commitCount, commitCount - 1, UID());
		state GetCommitVersionReply commitVersionReply =
		    wait(pSequencerInterface->getCommitVersion.getReply(commitVersionRequest));
		version = commitVersionReply.version;

		// Prepare the mutations -- since this is a test, the mutations are generated *AFTER* the version is retrieved.
		prepareMessageForVersion(version, NUM_MESSAGE_PER_COMMIT, storageTeamIDs, commitRecord);

		// Resolve
		// FIXME use the resolver role
		// ResolveTransactionBatchRequest resolveRequest;
		// resolveRequest.prevVersion = commitVersionReply.prevVersion;
		// resolveRequest.version = commitVersionReply.version;
		// resolveRequest.lastReceivedVersion = commitVersionReply.prevVersion;
		// // FIXME we do not need all teams
		// resolveRequest.teams = pTestDriverContext->storageTeamIDs;
		// state ResolveTransactionBatchReply resolveReply =
		// wait(randomlyPick(pTestDriverContext->resolverInterfaces)->resolve.getReply(resolveRequest));

		// TLog
		std::unordered_map<ptxn::TLogGroupID, std::shared_ptr<ProxySubsequencedMessageSerializer>> tLogGroupSerializers;
		prepareProxySerializedMessages(commitRecord, version, [&](StorageTeamID storageTeamId) {
			auto tLogGroup = pTestDriverContext->storageTeamIDTLogGroupIDMapper[storageTeamId];
			if (!tLogGroupSerializers.count(tLogGroup)) {
				tLogGroupSerializers.emplace(tLogGroup, std::make_shared<ProxySubsequencedMessageSerializer>(version));
			}
			return tLogGroupSerializers[tLogGroup];
		});

		for (auto& [tLogGroupID, serializer] : tLogGroupSerializers) {
			auto commitVersionPair = pTestDriverContext->getCommitVersionPair(tLogGroupID, commitVersionReply.version);
			printTiming << commitVersionReply.prevVersion << '\t' << commitVersionReply.version << std::endl;
			printTiming << tLogGroupID.toString() << '\t' << commitVersionPair.first << '\t' << commitVersionPair.second
			            << std::endl;
			auto serialized = serializer->getAllSerialized();
			TLogCommitRequest request(deterministicRandom()->randomUniqueID(),
			                          std::vector<TLogGroupID>{tLogGroupID},
			                          std::vector<Arena> {serialized.first},
			                          std::vector<std::unordered_map<ptxn::StorageTeamID, StringRef>>{serialized.second},
			                          std::vector<Version>{commitVersionPair.first},
			                          commitVersionPair.second,
			                          0,
			                          0,
			                          Optional<UID>(deterministicRandom()->randomUniqueID()));
			tLogCommitReplies.push_back(pTestDriverContext->tLogGroupLeaders[tLogGroupID]->commit.getReply(request));
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
