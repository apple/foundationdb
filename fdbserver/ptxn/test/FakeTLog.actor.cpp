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

#include <vector>

#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "fdbserver/ptxn/StorageServerInterface.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ptxn {

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
	for (auto& seqMutation: seqMutations) {
		const auto& subsequence = seqMutation.subsequence;
		const auto& mutation = seqMutation.mutation;
		pFakeTLogContext->mutations.push_back(pFakeTLogContext->persistenceArena,
		                                      { commitRequest.teamID,
		                                        { commitRequest.version, subsequence },
		                                        MutationRef(pFakeTLogContext->persistenceArena,
		                                                    static_cast<MutationRef::Type>(mutation.type),
		                                                    mutation.param1,
		                                                    mutation.param2) });
	}

	commitRequest.reply.send(TLogCommitReply{ 0 });
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

} // namespace ptxn
