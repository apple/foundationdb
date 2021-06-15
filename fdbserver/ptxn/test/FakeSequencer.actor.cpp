/*
 * FakeSequencer.actor.cpp
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

#include "fdbserver/ptxn/test/FakeSequencer.actor.h"

#include <vector>

#include "fdbserver/ptxn/test/Utils.h"

#include "flow/actorcompiler.h" // must be the last file being included

namespace ptxn::test {

namespace {

void getVersion(std::shared_ptr<FakeSequencerContext> pContext, GetCommitVersionRequest request) {
	GetCommitVersionReply reply;
	reply.requestNum = request.requestNum;

	reply.prevVersion = pContext->version;
	pContext->version += pContext->pTestDriverContext->commitVersionGap;
	reply.version = pContext->version;

	request.reply.send(reply);
}

} // anonymous namespace

ACTOR Future<Void> fakeSequencer(std::shared_ptr<FakeSequencerContext> pContext) {
	state std::vector<Future<Void>> actors;
	state ptxn::test::print::PrintTiming printTiming("fakeSequencer");

	loop {
		choose {
			when(GetCommitVersionRequest request =
			         waitNext(pContext->pSequencerInterface->getCommitVersion.getFuture())) {
				getVersion(pContext, request);
			}
		}
	}
}

} // namespace ptxn::test