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

#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/StorageServerInterface.h"
#include "fdbserver/ptxn/test/CommitUtils.h"
#include "fdbserver/ptxn/test/Utils.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace ptxn::test {

void processTLogCommitRequestByStorageTeam(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                                           const Version& version,
                                           const StorageTeamID& storageTeamID,
                                           const StringRef& messages) {
	SubsequencedMessageDeserializer deserializer(messages);

	// Check the committed data matches the record
	{
		int index = 0;
		for (auto vsm : deserializer) {
			const auto& message = vsm.message;
			// We skip the subsequence check, as SpanContextMessage is broadcasted to all storage teams, it will be
			// interfering the subsequence of mutationRefs.
			ASSERT(message ==
			       pFakeTLogContext->pTestDriverContext->commitRecord.messages[version][storageTeamID][index++].second);
		}
		pFakeTLogContext->pTestDriverContext->commitRecord.tags[version][storageTeamID].tLogValidated = true;
	}

	// "Persist" the data into memory
	for (const auto& vsm : deserializer) {
		if (vsm.message.getType() == Message::Type::MUTATION_REF) {
			const auto& version = vsm.version;
			const auto& subsequence = vsm.subsequence;
			const auto& mutation = std::get<MutationRef>(vsm.message);
			pFakeTLogContext->storageTeamMessages[storageTeamID].push_back(
			    pFakeTLogContext->persistenceArena,
			    { version,
			      subsequence,
			      Message(MutationRef(pFakeTLogContext->persistenceArena,
			                          static_cast<MutationRef::Type>(mutation.type),
			                          mutation.param1,
			                          mutation.param2)) });
		} else {
			pFakeTLogContext->storageTeamMessages[storageTeamID].push_back(pFakeTLogContext->persistenceArena, vsm);
		}
	}
}

void processTLogCommitRequest(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                              const TLogCommitRequest& commitRequest) {
	for (const auto& messageMap : commitRequest.messages) {
		for (const auto& message : messageMap) {
			processTLogCommitRequestByStorageTeam(
			    pFakeTLogContext, commitRequest.version, message.first, message.second);
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
		request.reply.sendError(storage_team_id_not_found());
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
                                                                const TLogCommitRequest& commitRequest,
                                                                const Arena arena,
                                                                const StorageTeamID& storageTeamID,
                                                                const StringRef messages) {

	StorageServerPushRequest request;
	request.spanID = commitRequest.spanID;
	request.storageTeamID = storageTeamID;
	request.version = commitRequest.version;
	request.arena = arena;
	request.messages = messages;

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
			printTiming << "Received commitRequest version = " << commitRequest.version << std::endl;
			processTLogCommitRequest(pFakeTLogContext, commitRequest);
			for (const auto& id : commitRequest.tLogGroupIDs) {
				printTiming << concatToString("Push the message to storage servers, log group id = ", id) << std::endl;
			}
			std::vector<Future<StorageServerPushReply>> replies;
			int i = 0;
			for (const auto& messageMap : commitRequest.messages) {
				for (const auto& message : messageMap) {
					replies.push_back(forwardTLogCommitToStorageServer(
					    pFakeTLogContext, commitRequest, commitRequest.arenas[i], message.first, message.second));
				}
				i++;
			}
			state std::vector<TLogGroupID> ids = commitRequest.tLogGroupIDs;
			wait(waitForAll(replies));

			for (const auto& id : ids) {
				printTiming << concatToString("Push completed, log group id = ", id) << std::endl;
			}
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
