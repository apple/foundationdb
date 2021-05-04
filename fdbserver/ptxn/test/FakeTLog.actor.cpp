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

#include <iomanip>
#include <iostream>
#include <vector>

#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
#include "fdbserver/ptxn/StorageServerInterface.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ptxn::test {

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
	const TeamID teamID = request.teamID;
	if (pFakeTLogContext->mutations.find(request.teamID) == pFakeTLogContext->mutations.end()) {
		std::cout << std::endl << "Team ID " << request.teamID.toString() << " not found." << std::endl;
		request.reply.sendError(teamid_not_found());
		return Void();
	}

	const VectorRef<VersionSubsequenceMutation>& mutations = pFakeTLogContext->mutations[teamID];

	std::cout << std::endl;
	std::cout << "Requested peek: " << std::endl;
	std::cout << std::setw(30)
	          << "Debug ID: " << (request.debugID.present() ? request.debugID.get().toString() : "Not present")
	          << std::endl;
	std::cout << std::setw(30) << "Team ID: " << request.teamID.toString() << std::endl;
	std::cout << std::setw(30) << "Version range: [" << request.beginVersion << ", "
	          << (request.endVersion.present() ? concatToString(request.endVersion.get()) : "-") << ")" << std::endl;
	std::cout << std::endl;

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
			std::cout << "Stopped serializing due to the reply size limit: Serialized " << serializer.getTotalBytes()
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

	std::cout << std::endl;
	std::cout << " Reply:" << std::endl;
	std::cout << std::setw(30) << "Version range: "
	          << "[" << firstVersion << ", " << lastVersion << "]" << std::endl;
	std::cout << std::setw(30) << "Serialized data length: " << serializer.getTotalBytes() << std::endl;

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
