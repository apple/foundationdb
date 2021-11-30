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
                                           const Version& commitVersion,
                                           const StorageTeamID& storageTeamID,
                                           const StringRef& messages) {
	print::PrintTiming printTiming("processTLogCommitRequest");
	printTiming << "Processing commit " << commitVersion << " for storage team " << storageTeamID << std::endl;
	SubsequencedMessageDeserializer deserializer(messages, true);
	auto& commitRecord = pFakeTLogContext->pTestDriverContext->commitRecord;

	// Check the committed data matches the record
	// FIXME put this part into CommitUtils
	{
		// The committer's record of data
		const auto& recordedData = commitRecord.messages.at(commitVersion);

		int index = 0;
		Version storageTeamVersion = invalidVersion;
		for (auto vsm : deserializer) {
			if (storageTeamVersion == invalidVersion) {
				storageTeamVersion = vsm.version;
			}

			if (vsm.message.getType() == Message::Type::EMPTY_VERSION_MESSAGE) {
				// Empty message is not what we created in CommitRecord, so we skip it
				continue;
			}

			// FIXME: Use TestEnvironment
			// We skip the subsequence check, as SpanContextMessage is broadcasted to all storage teams, it will be
			// interfering the subsequence of mutationRefs.
			const auto& subsequenceMessage = recordedData.at(storageTeamID)[index++];

			ASSERT_EQ(vsm.subsequence, subsequenceMessage.first);
			ASSERT(vsm.message == subsequenceMessage.second);
		}
		ASSERT_EQ(storageTeamVersion, commitRecord.commitVersionStorageTeamVersionMapper.at(commitVersion));
		commitRecord.tags[commitVersion][storageTeamID].tLogValidated = true;
	}

	// "Persist" the data into memory
	if (pFakeTLogContext->epochVersionRange.empty() ||
	    pFakeTLogContext->epochVersionRange.back().second != invalidVersion) {

		// New generation
		pFakeTLogContext->epochVersionRange.emplace_back(commitVersion, invalidVersion);
	}

	if (pFakeTLogContext->epochVersionMessages.empty() ||
	    pFakeTLogContext->epochVersionMessages.rbegin()->first != commitVersion) {

		// A new version
		pFakeTLogContext->epochVersionMessages[commitVersion] = FakeTLogContext::StorageTeamMessages();
	}

	auto& storageTeamMessages = pFakeTLogContext->epochVersionMessages.rbegin()->second;
	auto iter = std::begin(deserializer);
	auto& arena = iter.arena();
	for (; iter != std::end(deserializer); ++iter) {
		storageTeamMessages[storageTeamID].push_back(pFakeTLogContext->persistenceArena, *iter);
	}
	pFakeTLogContext->persistenceArena.dependsOn(arena);
}

ACTOR Future<Void> processTLogCommitRequest(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                                            TLogCommitRequest commitRequest) {
	wait(pFakeTLogContext->latency());
	for (const auto& [storageTeamID, message] : commitRequest.messages) {
		processTLogCommitRequestByStorageTeam(pFakeTLogContext, commitRequest.version, storageTeamID, message);
	}
	commitRequest.reply.send(TLogCommitReply{ commitRequest.version });
	return Void();
}

ACTOR Future<Void> fakeTLogPeek(TLogPeekRequest request, std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	state print::PrintTiming printTiming("FakeTLogPeek");

	const StorageTeamID storageTeamID = request.storageTeamID;
	const auto& storageTeamEpochVersionRange =
	    pFakeTLogContext->pTestDriverContext->commitRecord.storageTeamEpochVersionRange;
	const std::pair<Version, Version>& versionRange = storageTeamEpochVersionRange.at(storageTeamID);

	printTiming << "Storage Team ID = " << storageTeamID.toString() << "\tVersion range [" << versionRange.first << ", "
	            << versionRange.second << ")" << std::endl;
	printTiming << "Request begin commit version = " << request.beginVersion << std::endl;

	Version firstVersion = std::max(versionRange.first, request.beginVersion);
	Version lastVersion = invalidVersion;
	Version endVersion = versionRange.second;

	if (request.endVersion.present() && request.endVersion.get() != invalidVersion) {
		endVersion = std::min(endVersion, request.endVersion.get());
	}

	if (request.beginVersion >= endVersion) {
		printTiming << "End of stream" << std::endl;
		request.reply.sendError(end_of_stream());
		return Void();
	}

	auto epochVersionMessagesIter = pFakeTLogContext->epochVersionMessages.lower_bound(request.beginVersion);
	if (epochVersionMessagesIter != std::end(pFakeTLogContext->epochVersionMessages)) {
		firstVersion = epochVersionMessagesIter->first;
	}

	SubsequencedMessageSerializer serializer(storageTeamID);
	int numVersions = 0;
	while (epochVersionMessagesIter != std::end(pFakeTLogContext->epochVersionMessages)) {
		if (epochVersionMessagesIter->first >= endVersion) {
			break;
		}
		// No storage team written in this version
		if (epochVersionMessagesIter->second.count(storageTeamID) != 0) {
			serializer.startVersionWriting(epochVersionMessagesIter->first);
			for (auto vsm : epochVersionMessagesIter->second.at(storageTeamID)) {
				serializer.write(vsm.subsequence, vsm.message);
			}
			serializer.completeVersionWriting();

			++numVersions;
		}

		lastVersion = epochVersionMessagesIter->first;

		if (serializer.getTotalBytes() >= pFakeTLogContext->maxBytesPerPeek) {
			printTiming << "Stopped serializing due to the reply size limit: Serialized " << serializer.getTotalBytes()
			            << " Limit " << pFakeTLogContext->maxBytesPerPeek << std::endl;
			break;
		}
		if (numVersions >= pFakeTLogContext->maxVersionsPerPeek) {
			printTiming << "Stopped serializing due to the reply version limit: Serialized " << numVersions << " Limit "
			            << pFakeTLogContext->maxVersionsPerPeek << std::endl;
			break;
		}

		++epochVersionMessagesIter;
	} // while iterate over epochVersionMessages

	serializer.completeMessageWriting();

	if (epochVersionMessagesIter != std::end(pFakeTLogContext->epochVersionMessages)) {
		printTiming << "Storage Team ID = " << storageTeamID.toString() << " Last processed version = " << lastVersion
		            << std::endl;
	} else {
		printTiming << "Storage Team ID = " << storageTeamID.toString() << " All versions persisted are consumed"
		            << std::endl;
	}

	Standalone<StringRef> serialized = serializer.getSerialized();
	state TLogPeekReply reply(request.debugID, serialized.arena(), serialized);
	reply.beginVersion = firstVersion;
	reply.endVersion = lastVersion;

	wait(pFakeTLogContext->latency());
	request.reply.send(reply);

	return Void();
}

Future<StorageServerPushReply> forwardTLogCommitToStorageServer(std::shared_ptr<FakeTLogContext> pFakeTLogContext,
                                                                const TLogCommitRequest& commitRequest,
                                                                const StorageTeamID& storageTeamID,
                                                                const StringRef messages) {

	StorageServerPushRequest request;
	request.spanID = commitRequest.spanID;
	request.storageTeamID = storageTeamID;
	request.version = commitRequest.version;
	request.arena = commitRequest.arena;
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
	state print::PrintTiming printTiming("FakeTLog_ActivelyPush");
	state std::vector<Future<Void>> peekActors;

	ASSERT(pTLogInterface);

	loop choose {
		when(state TLogCommitRequest commitRequest = waitNext(pTLogInterface->commit.getFuture())) {
			state TLogGroupID tLogGroupID(commitRequest.tLogGroupID);
			printTiming << "Received commitRequest version = " << commitRequest.version << std::endl;
			wait(processTLogCommitRequest(pFakeTLogContext, commitRequest));

			printTiming << concatToString("Push the message to storage servers, log group id = ", tLogGroupID)
			            << std::endl;
			std::vector<Future<StorageServerPushReply>> replies;
			replies.reserve(commitRequest.messages.size());
			for (const auto& message : commitRequest.messages) {
				replies.push_back(
				    forwardTLogCommitToStorageServer(pFakeTLogContext, commitRequest, message.first, message.second));
			}
			wait(waitForAll(replies));

			printTiming << concatToString("Complete push, log group id = ", tLogGroupID) << std::endl;
		}
		when(TLogPeekRequest peekRequest = waitNext(pTLogInterface->peek.getFuture())) {
			peekActors.push_back(fakeTLogPeek(peekRequest, pFakeTLogContext));
		}
	}
}

ACTOR Future<Void> fakeTLog_PassivelyProvide(std::shared_ptr<FakeTLogContext> pFakeTLogContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeTLogContext->pTestDriverContext;
	state std::shared_ptr<TLogInterface_PassivelyPull> pTLogInterface =
	    std::dynamic_pointer_cast<TLogInterface_PassivelyPull>(pFakeTLogContext->pTLogInterface);
	state print::PrintTiming printTiming("FakeTLog_PassivelyProvide");
	state std::vector<Future<Void>> peekActors;

	ASSERT(pTLogInterface);

	loop choose {
		when(state TLogCommitRequest commitRequest = waitNext(pTLogInterface->commit.getFuture())) {
			wait(processTLogCommitRequest(pFakeTLogContext, commitRequest));
		}
		when(TLogPeekRequest peekRequest = waitNext(pTLogInterface->peek.getFuture())) {
			peekActors.push_back(fakeTLogPeek(peekRequest, pFakeTLogContext));
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
