/*
 * dining_philosophers.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fmt/format.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/TLSConfig.actor.h"
#include <functional>
#include <unordered_map>
#include <memory>
#include <iostream>
#include "flow/actorcompiler.h"

enum DPEndpoints {
	WLTOKEN_DP_SERVER = WLTOKEN_FIRST_AVAILABLE,
	DP_ENDPOINT_COUNT,
};

struct DPServerInterface {
	constexpr static FileIdentifier file_identifier = 9957031;
	RequestStream<struct GetInterfaceRequest> getInterface;
	RequestStream<struct GetForkRequest> getFork;
	RequestStream<struct ReleaseForkRequest> releaseFork;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getInterface, getFork, releaseFork);
	}
};

struct GetInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 13789052;
	ReplyPromise<DPServerInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// This is sent in both requests and responses.
// Having a default constructor seems important for Flow RPC.
// Having a file_identifier also seems important.
struct ForkState {
	constexpr static FileIdentifier file_identifier = 998236;
	// ID [0, N) of the philospher requesting this fork.
	uint32_t clientId;
	// The number of the fork we are requesting, also [0, N).
	// Philosophers numbered i request forks i and (i + 1) % N,
	// not necessarily in that order.
	uint32_t forkNumber;
	ForkState() : clientId(0), forkNumber(0) {}
	ForkState(int c, int f) : clientId(c), forkNumber(f) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clientId, forkNumber);
	}
};

struct GetForkRequest {
	constexpr static FileIdentifier file_identifier = 14904213;

	ForkState forkState;
	ReplyPromise<ForkState> reply;

	explicit GetForkRequest(ForkState fork_state) : forkState(fork_state) {}
	GetForkRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, forkState, reply);
	}
};

struct ReleaseForkRequest {
	constexpr static FileIdentifier file_identifier = 5914324;
	ForkState forkState;
	ReplyPromise<ForkState> reply;

	ReleaseForkRequest(ForkState fork_state) : forkState(fork_state) {}
	ReleaseForkRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, forkState, reply);
	}
};

ACTOR Future<Void> dpClient(NetworkAddress serverAddress, int idnum, int numEaters) {
	std::cout << format("dpClient: starting philosopher #%d, server address [%s]\n",
						idnum, serverAddress.toString().c_str());

	state DPServerInterface server;
	server.getInterface = RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({serverAddress}, WLTOKEN_DP_SERVER));
	DPServerInterface s = wait(server.getInterface.getReply(GetInterfaceRequest()));
	server = s;

	state int firstfork;
	state int secondfork;
	state GetForkRequest gf1;
	state GetForkRequest gf2;
	state ReleaseForkRequest rf1;
	state ReleaseForkRequest rf2;

	// The deadlock we must avoid is where each eater has 1 fork and is blocked
	// trying to get one held by a person next to them.  The protocol is is that
	// odd numbered eaters get the fork to the left of them first, then the one
	// to the right.  Even numbered eaters get the fork to the right of them first,
	// then the one to the left.
	if (idnum % 2) {
		firstfork = idnum;
		secondfork = (idnum + 1) % numEaters;
	} else {
		firstfork = (idnum + 1) % numEaters;
		secondfork = idnum;
	}

	state double msec;

	try {
		loop {
			std::cout << format("dpClient: eater [%d] WANTS TO EAT...\n", idnum);

			gf1 = GetForkRequest(ForkState(idnum, firstfork));
			ForkState reply = wait(server.getFork.getReply(gf1));
			ASSERT(reply.clientId == idnum);
			ASSERT(reply.forkNumber == firstfork);

			gf2 = GetForkRequest(ForkState(idnum, secondfork));
			ForkState reply2 = wait(server.getFork.getReply(gf2));
			ASSERT(reply2.clientId == idnum);
			ASSERT(reply2.forkNumber == secondfork);

			std::cout << format("dpClient: eater [%d] NOW EATING...\n", idnum);
			// NOTE: We will probably see all eaters eating simultaneously with
			// the current timing of eating for [10,11)s and pausing for only
			// [1,2)s between eats, combined with the fact that the server
			// currently vends unlimited forks, i.e. doesn't actually enforce that
			// only one client holds a given fork at a given time.
			msec = deterministicRandom()->randomInt(0, 1000)/1000.0 + 10;
			wait(delay(msec));

			rf1 = ReleaseForkRequest(ForkState(idnum, firstfork));
			ForkState reply3 = wait(server.releaseFork.getReply(rf1));

			rf2 = ReleaseForkRequest(ForkState(idnum, secondfork));
			ForkState reply4 = wait(server.releaseFork.getReply(rf2));

			// Elvis has left the bulding.
			std::cout << format("dpClient: eater [%d] HAS RELEASED ITS FORKS\n", idnum);
			
			msec = deterministicRandom()->randomInt(0, 1000)/1000.0 + 1;
			wait(delay(msec));
		}
	} catch (Error& e) {
		std::cerr << format("dpClient: caught Error code %s, %s\n",
							e.code(), e.what());
	}
		
	std::cout << format("dpClient: philosopher #%d finished.\n", idnum);
	return Void();
}

ACTOR Future<Void> dpServerLoop() {
	state DPServerInterface dpServer;
	dpServer.getInterface.makeWellKnownEndpoint(WLTOKEN_DP_SERVER, TaskPriority::DefaultEndpoint);

	std::cout << format("dpServer: starting...\n");

	loop {
		try {
			choose {
				when(GetInterfaceRequest req = waitNext(dpServer.getInterface.getFuture())) {
					req.reply.send(dpServer);
				}
				when(GetForkRequest req = waitNext(dpServer.getFork.getFuture())) {
					// XXX implement

					// For now just immediately give the requested fork.
					// This means that multiple clients can use the same fork
					// at the same time and we're obviously not solving the
					// exclusion problem.  Come back later.
					req.reply.send(req.forkState);
				}
				when(ReleaseForkRequest req = waitNext(dpServer.releaseFork.getFuture())) {
					// XXX implement
					// Wake up anybody waiting for this fork
					req.reply.send(req.forkState);
				}
			}
		} catch (Error& e) {
			// XXX this is cargo-culted from tutorial.actor.cpp; what does this do
			if (e.code() != error_code_operation_obsolete) {
				std::cerr << format("dpServerLoop: Error %d / %s\n",
									e.code(), e.what());
				throw e;
			}
		}
	}
}

static void usage(const char *argv0) {
	std::cerr << format("Usage: %s -p portNum | -s serverAddress\n", argv0);
}

int main(int argc, char **argv) {
	// Cargo-culted from tutorial.actor.cpp.
	platformInit();
	g_network = newNet2(TLSConfig(), /*useThreadPool=*/ false, /*useMetrics=*/true);

	if (argc != 3) {
		usage(argv[0]);
		return 1;
	}

	NetworkAddress serverAddress;
	bool isServer = false;

	if (0 == strcmp(argv[1], "-p")) {
		isServer = true;
		serverAddress = NetworkAddress::parse("0.0.0.0:" + std::string(argv[2]));
	} else if (0 == strcmp(argv[1], "-s")) {
		serverAddress = NetworkAddress::parse(argv[2]);
	} else {
		usage(argv[0]);
		return 1;
	}

	FlowTransport::createInstance(!isServer, 0, DP_ENDPOINT_COUNT);

	std::vector<Future<Void>> all;
	if (isServer) {
		try {
			auto listenError = FlowTransport::transport().bind(serverAddress, serverAddress);
			if (listenError.isError()) {
				listenError.get();
			}
		} catch (Error& e) {
			std::cerr << format("Error binding to address [%s]: %d, %s\n",
								serverAddress.toString().c_str(), e.code(), e.what());
			return 2;
		}
		all.emplace_back(dpServerLoop());
	} else {
		for (int i = 0; i < 5; i++) {
			all.emplace_back(dpClient(serverAddress, i, 5));
		}
	}

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
