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

#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/StorageServerInterface.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace ptxn::test {

// Generates numMutations random mutations, store them in mutations.
void generateRandomMutations(const Version& initialVersion,
                             const int numMutations,
                             Arena& mutationRefArena,
                             VectorRef<VersionSubsequenceMessage>& mutations) {
	print::PrintTiming printTiming("generateRandomMutations");
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

	printTiming << "Generated " << numMutations << " random mutations, version range [" << mutations.front().version
	            << "," << mutations.back().version << " ]." << std::endl;
}

// Randomly distributes the mutations to teams in the same TLog server
void distributeMutations(Arena& arena,
                         std::unordered_map<StorageTeamID, VectorRef<VersionSubsequenceMessage>>& storageTeamMessages,
                         const std::vector<StorageTeamID>& storageTeamIDs,
                         const VectorRef<VersionSubsequenceMessage>& messages) {
	print::PrintTiming printTiming("distributeMutations");
	printTiming << "Distributing " << messages.size() << " mutations to " << storageTeamIDs.size() << "teams."
	            << std::endl;

	for (const auto& message : messages) {
		const StorageTeamID& storageTeamID = randomlyPick(storageTeamIDs);
		storageTeamMessages[storageTeamID].push_back(arena, message);
	}

	for (const auto& storageTeamID : storageTeamIDs) {
		printTiming << "Team " << storageTeamID.toString() << " has " << storageTeamMessages[storageTeamID].size()
		            << " mutations." << std::endl;
	}
}

void fillTLogWithRandomMutations(std::shared_ptr<FakeTLogContext> pContext,
                                 const Version& initialVersion,
                                 const int numMutations,
                                 const int numStorageTeams) {
	// Set up teams
	if (numStorageTeams != 0) {
		pContext->storageTeamIDs.resize(numStorageTeams);
		for (auto& storageTeamID : pContext->storageTeamIDs) {
			storageTeamID = getNewStorageTeamID();
		}
	}

	// Generate mutations
	pContext->persistenceArena = Arena();
	pContext->storageTeamMessages.clear();
	pContext->allMessages.resize(pContext->persistenceArena, 0);

	generateRandomMutations(initialVersion, numMutations, pContext->persistenceArena, pContext->allMessages);
	distributeMutations(
	    pContext->persistenceArena, pContext->storageTeamMessages, pContext->storageTeamIDs, pContext->allMessages);

	// Update versions
	pContext->versions.clear();
	for (const auto& item : pContext->allMessages) {
		pContext->versions.push_back(item.version);
	}
	std::sort(std::begin(pContext->versions), std::end(pContext->versions));
	auto last = std::unique(std::begin(pContext->versions), std::end(pContext->versions));
	pContext->versions.erase(last, std::end(pContext->versions));
}

void processTLogCommitRequest(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                              const TLogCommitRequest& commitRequest) {
	SubsequencedMessageDeserializer deserializer(commitRequest.messages);

	verifyMessagesInRecord(pFakeTLogContext->pTestDriverContext->commitRecord,
	                       commitRequest.version,
	                       commitRequest.storageTeamID,
	                       deserializer,
	                       [](CommitValidationRecord& record) { record.tLogValidated = true; });

	// "Persist" the data into memory
	for (const auto& vsm : deserializer) {
		if (vsm.message.getType() == Message::Type::MUTATION_REF) {
			const auto& version = vsm.version;
			const auto& subsequence = vsm.subsequence;
			const auto& mutation = std::get<MutationRef>(vsm.message);
			pFakeTLogContext->storageTeamMessages[commitRequest.storageTeamID].push_back(
			    pFakeTLogContext->persistenceArena,
			    { version,
			      subsequence,
			      Message(MutationRef(pFakeTLogContext->persistenceArena,
			                          static_cast<MutationRef::Type>(mutation.type),
			                          mutation.param1,
			                          mutation.param2)) });
		} else {
			pFakeTLogContext->storageTeamMessages[commitRequest.storageTeamID].push_back(
			    pFakeTLogContext->persistenceArena, vsm);
		}
	}

	commitRequest.reply.send(TLogCommitReply{ 0 });
}

Future<Void> fakeTLogPeek(TLogPeekRequest request, std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	print::PrintTiming printTiming("FakeTLogPeek");

	const StorageTeamID storageTeamID = request.storageTeamID;
	if (pFakeTLogContext->storageTeamMessages.find(request.storageTeamID) ==
	    pFakeTLogContext->storageTeamMessages.end()) {

		printTiming << "Team ID " << request.storageTeamID.toString() << " not found." << std::endl;
		request.reply.sendError(teamid_not_found());
		return Void();
	}

	const VectorRef<VersionSubsequenceMessage>& messages = pFakeTLogContext->storageTeamMessages[storageTeamID];

	Version firstVersion = invalidVersion;
	Version lastVersion = invalidVersion;
	Version endVersion = MAX_VERSION;
	bool haveUnclosedVersionSection = false;
	SubsequencedMessageSerializer serializer(storageTeamID);

	if (request.endVersion.present() && request.endVersion.get() != invalidVersion) {
		endVersion = request.endVersion.get();
	}

	for (const auto& mutation : messages) {
		const Version currentVersion = mutation.version;

		// Have not reached the expected version
		if (currentVersion < request.beginVersion) {
			continue;
		}

		// The serialized data size is too big, cutoff here
		if (serializer.getTotalBytes() >= pFakeTLogContext->maxBytesPerPeek && lastVersion != currentVersion) {
			printTiming << "Stopped serializing due to the reply size limit: Serialized " << serializer.getTotalBytes()
			            << " Limit " << pFakeTLogContext->maxBytesPerPeek << std::endl;
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
		const auto& message = mutation.message;
		serializer.write(subsequence, message);

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

Future<StorageServerPushReply> forwardTLogCommitToStorageServer(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                                                                const TLogCommitRequest& commitRequest) {

	const StorageTeamID& storageTeamID = commitRequest.storageTeamID;
	StorageServerPushRequest request;
	request.spanID = commitRequest.spanID;
	request.storageTeamID = storageTeamID;
	request.version = commitRequest.version;
	request.arena = commitRequest.arena;
	request.messages = commitRequest.messages;

	std::shared_ptr<StorageServerInterface_PassivelyReceive> storageServerInterface =
	    std::dynamic_pointer_cast<StorageServerInterface_PassivelyReceive>(
	        pFakeTLogContext->pTestDriverContext->getStorageServerInterface(storageTeamID));
	ASSERT(storageServerInterface != nullptr);

	return storageServerInterface->pushRequests.getReply(request);
}

ACTOR Future<Void> fakeTLog_ActivelyPush(std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeTLogContext->pTestDriverContext;
	state std::shared_ptr<TLogInterface_ActivelyPush> pTLogInterface =
	    std::dynamic_pointer_cast<TLogInterface_ActivelyPush>(pFakeTLogContext->pTLogInterface);
	state print::PrintTiming printTiming("FakeTLog");

	ASSERT(pTLogInterface);

	loop choose {
		when(TLogCommitRequest commitRequest = waitNext(pTLogInterface->commit.getFuture())) {
			state StorageTeamID storageTeamID(commitRequest.storageTeamID);
			printTiming << "Received commitRequest version = " << commitRequest.version << std::endl;
			processTLogCommitRequest(pFakeTLogContext, commitRequest);

			printTiming << concatToString("Push the message to storage servers, storage team id = ", storageTeamID)
			            << std::endl;
			StorageServerPushReply reply = wait(forwardTLogCommitToStorageServer(pFakeTLogContext, commitRequest));

			printTiming << concatToString("Complete push, storage team id = ", storageTeamID) << std::endl;
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
	state print::PrintTiming printTiming("FakeTLog");

	ASSERT(pTLogInterface);

	loop choose {
		when(TLogCommitRequest commitRequest = waitNext(pTLogInterface->commit.getFuture())) {
			processTLogCommitRequest(pFakeTLogContext, commitRequest);
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
