/*
 * WellKnownEndpoints.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/WellKnownEndpoints.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/UnitTest.h"

#include <utility>

TEST_CASE("/fdbclient/WellKnownEndpoints/LegacyIds") {
	constexpr std::pair<int, int> legacyEndpoints[] = {
		{ WLTOKEN_CLIENTLEADERREG_GETLEADER, 3 },
		{ WLTOKEN_CLIENTLEADERREG_OPENDATABASE, 4 },
		{ WLTOKEN_LEADERELECTIONREG_CANDIDACY, 5 },
		{ WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT, 6 },
		{ WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT, 7 },
		{ WLTOKEN_LEADERELECTIONREG_FORWARD, 8 },
		{ WLTOKEN_GENERATIONREG_READ, 9 },
		{ WLTOKEN_PROTOCOL_INFO, 10 },
		{ WLTOKEN_GENERATIONREG_WRITE, 11 },
		{ WLTOKEN_CLIENTLEADERREG_DESCRIPTOR_MUTABLE, 12 },
		{ WLTOKEN_CONFIGTXN_GETGENERATION, 13 },
		{ WLTOKEN_CONFIGTXN_GET, 14 },
		{ WLTOKEN_CONFIGTXN_GETCLASSES, 15 },
		{ WLTOKEN_CONFIGTXN_GETKNOBS, 16 },
		{ WLTOKEN_CONFIGTXN_COMMIT, 17 },
		{ WLTOKEN_CONFIGFOLLOWER_GETSNAPSHOTANDCHANGES, 18 },
		{ WLTOKEN_CONFIGFOLLOWER_GETCHANGES, 19 },
		{ WLTOKEN_CONFIGFOLLOWER_COMPACT, 20 },
		{ WLTOKEN_CONFIGFOLLOWER_ROLLFORWARD, 21 },
		{ WLTOKEN_CONFIGFOLLOWER_GETCOMMITTEDVERSION, 22 },
		{ WLTOKEN_PROCESS, 23 },
		{ WLTOKEN_CONFIGFOLLOWER_LOCK, 24 },
	};
	for (const auto& [endpointId, legacyId] : legacyEndpoints) {
		ASSERT(endpointId == legacyId);
		ASSERT(Endpoint::wellKnownToken(endpointId) == UID(-1, legacyId));
	}
	ASSERT(WLTOKEN_RESERVED_COUNT == 25);
	return Void();
}

TEST_CASE("/fdbclient/WellKnownEndpoints/ReservationBoundary") {
	ReplyPromise<Void> reply;
	ASSERT(uint32_t(reply.getEndpoint().token.second()) >= WLTOKEN_RESERVED_COUNT);

	RequestStream<ReplyPromise<Void>> first;
	RequestStream<ReplyPromise<Void>> second;
	FlowTransport::transport().addEndpoints({ first.getReceiver(), second.getReceiver() });
	ASSERT(uint32_t(first.getEndpoint().token.second()) >= WLTOKEN_RESERVED_COUNT);
	ASSERT(first.getEndpoint().getAdjustedEndpoint(1) == second.getEndpoint());
	return Void();
}

void forceLinkWellKnownEndpointsTests() {}
