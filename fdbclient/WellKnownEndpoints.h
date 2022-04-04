/*
 * WellKnownEndpoints.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_WELLKNOWNENDPOINTS_H
#define FDBCLIENT_WELLKNOWNENDPOINTS_H
#pragma once

#include <fdbrpc/fdbrpc.h>

/*
 * All well-known endpoints of FDB must be listed here to guarantee their uniqueness
 */
enum WellKnownEndpoints {
	WLTOKEN_CLIENTLEADERREG_GETLEADER = WLTOKEN_FIRST_AVAILABLE, // 2
	WLTOKEN_CLIENTLEADERREG_OPENDATABASE, // 3
	WLTOKEN_LEADERELECTIONREG_CANDIDACY, // 4
	WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT, // 5
	WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT, // 6
	WLTOKEN_LEADERELECTIONREG_FORWARD, // 7
	WLTOKEN_GENERATIONREG_READ, // 8
	WLTOKEN_GENERATIONREG_WRITE, // 9
	WLTOKEN_PROTOCOL_INFO, // 10 : the value of this endpoint should be stable and not change.
	WLTOKEN_CLIENTLEADERREG_DESCRIPTOR_MUTABLE, // 11
	WLTOKEN_CONFIGTXN_GETGENERATION, // 12
	WLTOKEN_CONFIGTXN_GET, // 13
	WLTOKEN_CONFIGTXN_GETCLASSES, // 14
	WLTOKEN_CONFIGTXN_GETKNOBS, // 15
	WLTOKEN_CONFIGTXN_COMMIT, // 16
	WLTOKEN_CONFIGFOLLOWER_GETSNAPSHOTANDCHANGES, // 17
	WLTOKEN_CONFIGFOLLOWER_GETCHANGES, // 18
	WLTOKEN_CONFIGFOLLOWER_COMPACT, // 19
	WLTOKEN_CONFIGFOLLOWER_ROLLFORWARD, // 20
	WLTOKEN_CONFIGFOLLOWER_GETCOMMITTEDVERSION, // 21
	WLTOKEN_PROCESS, // 22
	WLTOKEN_RESERVED_COUNT // 23
};

static_assert(WLTOKEN_PROTOCOL_INFO ==
              10); // Enforce that the value of this endpoint does not change per comment above.

#endif
