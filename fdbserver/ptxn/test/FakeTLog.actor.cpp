/*
 * FakeTLog.actor.cpp
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

#include "fdbserver/ptxn/test/FakeTLog.actor.h"

#include <algorithm>
#include <iostream>
#include <vector>

#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
#include "fdbserver/ptxn/StorageServerInterface.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace ptxn::test {

// Generates numMutations random mutations, store them in mutations.
void generateRandomMutations(const Version& initialVersion,
                             const int numMutations,
                             Arena& mutationRefArena,
                             VectorRef<VersionSubsequenceMutation>& mutations) {
	Version currentVersion = initialVersion;
	Subsequence currentSubsequence = 1;
	for (int i = 0; i < numMutations; ++i) {
		// Create a new version
		if (deterministicRandom()->randomInt(0, 10) == 0) {
			currentVersion += deterministicRandom()->randomInt(1, 5);
			currentSubsequence = 1;
		}

		StringRef key = StringRef(mutationRefArena, deterministicRandom()->randomAlphaNumeric(10));
		StringRef value = StringRef(
		    mutationRefArena, deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(10, 1000)));

		mutations.emplace_back(
		    mutationRefArena, currentVersion, currentSubsequence++, MutationRef(MutationRef::SetValue, key, value));
	}

	std::cout << __FUNCTION__ << ">> Generated " << numMutations << " random mutations, version range ["
	          << mutations.front().version << "," << mutations.back().version << " ]." << std::endl;
}

// Randomly distributes the mutations to teams in the same TLog server
void distributeMutations(Arena& mutationRefArena,
                         std::unordered_map<TeamID, VectorRef<VersionSubsequenceMutation>>& teamedMutations,
                         const std::vector<TeamID>& teamIDs,
                         const VectorRef<VersionSubsequenceMutation>& mutations) {
	std::cout << __FUNCTION__ << ">> Distributing " << mutations.size() << " mutations to " << teamIDs.size()
	          << "teams." << std::endl;

	for (const auto& mutation : mutations) {
		const TeamID& teamID = randomlyPick(teamIDs);
		teamedMutations[teamID].push_back(mutationRefArena, mutation);
	}

	for (const auto& teamID : teamIDs) {
		std::cout << __FUNCTION__ << ">> Team " << teamID.toString() << " has " << teamedMutations[teamID].size()
		          << " mutations." << std::endl;
	}
}

void fillTLogWithRandomMutations(std::shared_ptr<FakeTLogContext> pContext,
                                 const Version& initialVersion,
                                 const int numMutations,
                                 const int numTeams) {
	// Set up teams
	if (numTeams != 0) {
		pContext->teamIDs.resize(numTeams);
		for (auto& teamID : pContext->teamIDs) {
			teamID = getNewTeamID();
		}
	}

	// Generate mutations
	pContext->persistenceArena = Arena();
	pContext->mutations.clear();
	pContext->allMutations.resize(pContext->persistenceArena, 0);

	generateRandomMutations(initialVersion, numMutations, pContext->persistenceArena, pContext->allMutations);
	distributeMutations(pContext->persistenceArena, pContext->mutations, pContext->teamIDs, pContext->allMutations);

	// Update versions
	pContext->versions.clear();
	for (const auto& item : pContext->allMutations) {
		pContext->versions.push_back(item.version);
	}
	std::sort(std::begin(pContext->versions), std::end(pContext->versions));
	auto last = std::unique(std::begin(pContext->versions), std::end(pContext->versions));
	pContext->versions.erase(last, std::end(pContext->versions));
}

void processTLogCommitRequest(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                              const TLogCommitRequest& commitRequest,
                              ProxyTLogMessageHeader& header,
                              std::vector<SubsequenceMutationItem>& seqMutations) {
	proxyTLogPushMessageDeserializer(commitRequest.arena, commitRequest.messages, header, seqMutations);

	std::vector<MutationRef> mutations;
	std::transform(seqMutations.begin(),
	               seqMutations.end(),
	               std::back_inserter(mutations),
	               [](const SubsequenceMutationItem& item) { return item.mutation; });
	verifyMutationsInRecord(pFakeTLogContext->pTestDriverContext->commitRecord,
	                        commitRequest.version,
	                        commitRequest.teamID,
	                        mutations,
	                        [](CommitValidationRecord& record) { record.tLogValidated = true; });

	// "Persist" the data into memory
	for (auto& seqMutation : seqMutations) {
		const auto& subsequence = seqMutation.subsequence;
		const auto& mutation = seqMutation.mutation;
		pFakeTLogContext->mutations[commitRequest.teamID].push_back(
		    pFakeTLogContext->persistenceArena,
		    { commitRequest.version,
		      subsequence,
		      MutationRef(pFakeTLogContext->persistenceArena,
		                  static_cast<MutationRef::Type>(mutation.type),
		                  mutation.param1,
		                  mutation.param2) });
	}

	commitRequest.reply.send(TLogCommitReply{ 0 });
}

Future<Void> fakeTLogPeek(TLogPeekRequest request, std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	const StorageTeamID teamID = request.teamID;
	if (pFakeTLogContext->mutations.find(request.teamID) == pFakeTLogContext->mutations.end()) {
		std::cout << __FUNCTION__ << ">> Team ID " << request.teamID.toString() << " not found." << std::endl;
		request.reply.sendError(teamid_not_found());
		return Void();
	}

	const VectorRef<VersionSubsequenceMutation>& mutations = pFakeTLogContext->mutations[teamID];

	Version firstVersion = invalidVersion;
	Version lastVersion = invalidVersion;
	Version endVersion = MAX_VERSION;
	bool haveUnclosedVersionSection = false;
	TLogStorageServerMessageSerializer serializer(teamID);

	if (request.endVersion.present() && request.endVersion.get() != invalidVersion) {
		endVersion = request.endVersion.get();
	}

	for (const auto& mutation : mutations) {
		const Version currentVersion = mutation.version;

		// Have not reached the expected version
		if (currentVersion < request.beginVersion) {
			continue;
		}

		// The serialized data size is too big, cutoff here
		if (serializer.getTotalBytes() >= pFakeTLogContext->maxBytesPerPeek && lastVersion != currentVersion) {
			std::cout << __FUNCTION__ << ">> Stopped serializing due to the reply size limit: Serialized "
			          << serializer.getTotalBytes() << " Limit " << pFakeTLogContext->maxBytesPerPeek << std::endl;
			break;
		}

		// Finished the range
		if (currentVersion >= endVersion) {
			break;
		}

		if (currentVersion != lastVersion) {
			if (haveUnclosedVersionSection) {
				serializer.completeVersionWriting();
				haveUnclosedVersionSection = false;
			}

			serializer.startVersionWriting(currentVersion);
			haveUnclosedVersionSection = true;

			if (firstVersion == invalidVersion) {
				firstVersion = currentVersion;
			}
		}

		const Subsequence subsequence = mutation.subsequence;
		const MutationRef mutationRef = mutation.mutation;
		serializer.writeSubsequenceMutationRef(subsequence, mutationRef);

		lastVersion = currentVersion;
	}

	if (haveUnclosedVersionSection) {
		serializer.completeVersionWriting();
	}

	serializer.completeMessageWriting();

	Standalone<StringRef> serialized = serializer.getSerialized();
	TLogPeekReply reply{ request.debugID, serialized.arena(), serialized };
	request.reply.send(reply);

	return Void();
}

ACTOR Future<Void> fakeTLog_ActivelyPush(std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeTLogContext->pTestDriverContext;
	state std::shared_ptr<TLogInterface_ActivelyPush> pTLogInterface =
	    std::dynamic_pointer_cast<TLogInterface_ActivelyPush>(pFakeTLogContext->pTLogInterface);

	ASSERT(pTLogInterface);

	loop choose {
		when(TLogCommitRequest commitRequest = waitNext(pTLogInterface->commit.getFuture())) {
			ProxyTLogMessageHeader header;
			std::vector<SubsequenceMutationItem> seqMutations;
			processTLogCommitRequest(pFakeTLogContext, commitRequest, header, seqMutations);

			StorageServerPushRequest request;
			request.spanID = commitRequest.spanID;
			request.teamID = commitRequest.teamID;
			request.arena = Arena();
			for (auto iter = std::begin(seqMutations); iter != std::end(seqMutations); ++iter) {
				const auto& mutation = iter->mutation;
				request.mutations.emplace_back(request.arena,
				                               MutationRef(request.arena,
				                                           static_cast<MutationRef::Type>(mutation.type),
				                                           mutation.param1,
				                                           mutation.param2));
			}
			request.version = commitRequest.version;

			std::shared_ptr<StorageServerInterface_PassivelyReceive> pStorageServerInterface =
			    std::dynamic_pointer_cast<StorageServerInterface_PassivelyReceive>(
			        pTestDriverContext->getStorageServerInterface(commitRequest.teamID));
			state StorageServerPushReply reply = wait(pStorageServerInterface->pushRequests.getReply(request));
		}
		when(TLogPeekRequest peekRequest = waitNext(pTLogInterface->peek.getFuture())) {
			fakeTLogPeek(peekRequest, pFakeTLogContext);
		}
	}
}

ACTOR Future<Void> fakeTLog_PassivelyProvide(std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeTLogContext->pTestDriverContext;
	state std::shared_ptr<TLogInterface_PassivelyPull> pTLogInterface =
	    std::dynamic_pointer_cast<TLogInterface_PassivelyPull>(pFakeTLogContext->pTLogInterface);

	ASSERT(pTLogInterface);

	loop choose {
		when(TLogCommitRequest commitRequest = waitNext(pTLogInterface->commit.getFuture())) {
			state ProxyTLogMessageHeader header;
			state std::vector<SubsequenceMutationItem> seqMutations;
			processTLogCommitRequest(pFakeTLogContext, commitRequest, header, seqMutations);
		}
		when(TLogPeekRequest peekRequest = waitNext(pTLogInterface->peek.getFuture())) {
			fakeTLogPeek(peekRequest, pFakeTLogContext);
		}
	}
}

Future<Void> getFakeTLogActor(const MessageTransferModel model, std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	switch (model) {
	case MessageTransferModel::TLogActivelyPush:
		return fakeTLog_ActivelyPush(pFakeTLogContext);
	case MessageTransferModel::StorageServerActivelyPull:
		return fakeTLog_PassivelyProvide(pFakeTLogContext);
	default:
		throw internal_error_msg("Unsupported message transfer model");
	}
}

} // namespace ptxn::test
