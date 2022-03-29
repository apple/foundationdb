/*
 * FakeStorageServer.actor.cpp
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

#include "fdbserver/ptxn/test/FakeStorageServer.actor.h"

#include <iostream>
#include <vector>

#include "fdbserver/ptxn/test/Driver.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ptxn::test {

ACTOR Future<Void> fakeStorageServer_PassivelyReceive(
    std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeStorageServerContext->pTestDriverContext;
	state std::shared_ptr<StorageServerInterface_PassivelyReceive> pStorageServerInterface =
	    std::dynamic_pointer_cast<StorageServerInterface_PassivelyReceive>(
	        pFakeStorageServerContext->pStorageServerInterface);

	ASSERT(pStorageServerInterface);

	loop choose {
		when(StorageServerPushRequest request = waitNext(pStorageServerInterface->pushRequests.getFuture())) {
			const Version version = request.version;
			const StorageTeamID storageTeamID = request.storageTeamID;
			SubsequencedMessageDeserializer deserializer(request.arena, request.messages);
			int index = 0;
			for (auto vsm : deserializer) {
				const auto& message = vsm.message;
				// We skip the subsequence check, as SpanContextMessage is broadcasted to all storage teams, it will be
				// interfering the subsequence of mutationRefs.
				ASSERT(message == pTestDriverContext->commitRecord.messages[version][storageTeamID][index++].second);
			}
			pTestDriverContext->commitRecord.tags[version][storageTeamID].storageServerValidated = true;

			request.reply.send(StorageServerPushReply());
		}
	}
}

ACTOR Future<Void> fakeStorageServer_ActivelyPull(std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeStorageServerContext->pTestDriverContext;
	state std::shared_ptr<StorageServerInterface_ActivelyPull> pStorageServerInterface =
	    std::dynamic_pointer_cast<StorageServerInterface_ActivelyPull>(
	        pFakeStorageServerContext->pStorageServerInterface);

	ASSERT(pStorageServerInterface);

	loop choose {}
}

Future<Void> getFakeStorageServerActor(const MessageTransferModel model,
                                       std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	switch (model) {
	case MessageTransferModel::TLogActivelyPush:
		return fakeStorageServer_PassivelyReceive(pFakeStorageServerContext);
	case MessageTransferModel::StorageServerActivelyPull:
		return fakeStorageServer_ActivelyPull(pFakeStorageServerContext);
	default:
		throw internal_error_msg("Unsupported message transfer model");
	}
}

} // namespace ptxn::test
