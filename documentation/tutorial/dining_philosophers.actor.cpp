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
	// RequestStream<struct ReleaseForkRequest> releaseFork;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getInterface, getFork);
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
struct ForkState {
	constexpr static FileIdentifier file_identifier = 998236;
	// ID [0, N) of the philospher requesting this fork.
	int clientId;
	// The number of the fork we are requesting, also [0, N).
	// Philosophers numbered i request forks i and (i + 1) % N,
	// not necessarily in that order.
	int forkNumber;
	ForkState() : clientId(0), forkNumber(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clientId, forkNumber);
	}

};

struct GetForkRequest {
	constexpr static FileIdentifier file_identifier = 14904213;

	ForkState forkState;
	ReplyPromise<ForkState> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, forkState, reply);
	}
};

// struct ReleaseForkRequest {
// 	constexpr static FileIdentifier file_identifier = 5914324;
// 	int clientId = 0;
// 	// Release fork numbered forkNumber. Must have been previously
// 	// acquired by a successful call to GetForkRequest().
// 	int forkNumber = 0;
// 	// The reply echos the fork number.
// 	// TODO: pass an error back in case of out-of-range forkNumber, or
// 	// an attempt to free a fork held by somebody else, or whatever.
// 	ReplyPromise<int> reply;
// 
// 	template <class Ar>
// 	void serialize(Ar& ar) {
// 		serializer(ar, clientId, forkNumber, reply);
// 	}
// };
// 

ACTOR Future<Void> dpClient(NetworkAddress serverAddress, int idnum) {
	std::cout << format("dpClient: starting philosopher #%d, server address [%s]\n",
						idnum, serverAddress.toString().c_str());

	wait(delay(idnum));

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
				//				when(ReleaseForkRequest req = waitNext(dpServer.releaseFork.getFuture())) {
					// XXX implement
					// Wake up anybody waiting for this fork
				//					req.reply.send(req.forkNumber);
				//				}
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
			all.emplace_back(dpClient(serverAddress, i));
		}
	}

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
