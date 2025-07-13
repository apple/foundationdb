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

// Flow solution to Dining Philosophers problem
// (https://en.wikipedia.org/wiki/Dining_philosophers_problem; or
// https://leetcode.com/problems/the-dining-philosophers/description/,
// but note that that calls for a single process/threaded solution
// and here we implement a distributed solution).
//
// This uses most of the techniques illustrated in tutorial.actor.cpp.
// A server is used to track "fork ownership".  The dining
// philosophers are modeled as clients who must request and obtain
// ownership of forks prior to eating.
//
// To do this exercise, delete the code below down to main(), then
// implement it using techniques you see in tutorial.actor.cpp.

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
// NOTE: it seems better to have reply types be structs with
// file_identifier members and serialize() overrides.
// tutorial.actor.cpp has an example where a std::string is sent
// directly.  Attempts to do similar things with base types like int
// will run into trouble.  (Caveat: I didn't try uint32_t or the like;
// maybe those work.)
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

	GetForkRequest(ForkState fork_state) : forkState(fork_state) {}
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
	std::cout << format(
	    "dpClient: starting philosopher #%d, server address [%s]\n", idnum, serverAddress.toString().c_str());

	state DPServerInterface server;
	server.getInterface = RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_DP_SERVER));
	DPServerInterface s = wait(server.getInterface.getReply(GetInterfaceRequest()));
	server = s;

	state int firstfork;
	state int secondfork;
	state GetForkRequest gf1;
	state GetForkRequest gf2;
	state ReleaseForkRequest rf1;
	state ReleaseForkRequest rf2;

	// Change this to true and it should deadlock pretty quickly.
	bool CAUSE_DEADLOCK = false;

	if (CAUSE_DEADLOCK) {
		firstfork = idnum;
		secondfork = (idnum + 1) % numEaters;
	} else {
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
	}

	state double msec;
	state int meals_eaten = 0;

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
			msec = deterministicRandom()->randomInt(0, 1000) / 1000.0 + 1;
			wait(delay(msec));
			meals_eaten++;

			rf1 = ReleaseForkRequest(ForkState(idnum, firstfork));
			ForkState reply3 = wait(server.releaseFork.getReply(rf1));

			rf2 = ReleaseForkRequest(ForkState(idnum, secondfork));
			ForkState reply4 = wait(server.releaseFork.getReply(rf2));

			// Elvis has left the bulding.
			std::cout << format("dpClient: eater [%d] HAS RELEASED ITS FORKS (%d meals eaten)\n", idnum, meals_eaten);

			msec = deterministicRandom()->randomInt(0, 1000) / 1000.0 + 1;
			wait(delay(msec));
		}
	} catch (Error& e) {
		std::cerr << format("dpClient: caught Error code %s, %s\n", e.code(), e.what());
	}

	std::cout << format("dpClient: philosopher #%d finished.\n", idnum);
	return Void();
}

ACTOR Future<Void> dpServerLoop() {
	state DPServerInterface dpServer;
	dpServer.getInterface.makeWellKnownEndpoint(WLTOKEN_DP_SERVER, TaskPriority::DefaultEndpoint);

	std::cout << format("dpServer: starting...\n");

	// Maps int fork to int eaterId who owns it.
	state std::map<int, int> forkOwners;

	// Maps to eaters who are waiting for a fork.  In a more general problem
	// of waiting for a shared resource, this might be a queue.  Because of
	// the specifics of Dining Philosophers, at most one eater is waiting for
	// an in-use fork, so it can be a singleton.
	// Maps int fork to pending reply to an eater who is waiting for it to be freed.
	state std::map<int, GetForkRequest> pending;

	loop {
		try {
			choose {
				when(GetInterfaceRequest req = waitNext(dpServer.getInterface.getFuture())) {
					req.reply.send(dpServer);
				}
				when(GetForkRequest req = waitNext(dpServer.getFork.getFuture())) {
					int clientId = req.forkState.clientId;
					int forkNo = req.forkState.forkNumber;
					auto it = forkOwners.find(forkNo);
					if (it == forkOwners.end()) {
						// Available immediately, give it.
						std::cout << format("dpServerLoop: eater %d gets fork %d\n", clientId, forkNo);
						forkOwners[forkNo] = clientId;
						req.reply.send(req.forkState);
					} else {
						auto it2 = pending.find(forkNo);
						ASSERT(it2 == pending.end());
						std::cout << format("dpServerLoop: eater %d has to wait for fork %d\n", clientId, forkNo);
						pending[forkNo] = req;
					}
				}
				when(ReleaseForkRequest req = waitNext(dpServer.releaseFork.getFuture())) {
					int clientId = req.forkState.clientId;
					int forkNo = req.forkState.forkNumber;
					auto it = forkOwners.find(forkNo);
					if (it == forkOwners.end()) {
						std::cerr << format(
						    "dpServerLoop: request from clientId %d to free fork %d which is not owned by anybody\n",
						    clientId,
						    forkNo);
					} else if (it->second != clientId) {
						std::cerr << format("dpServerLoop: request from clientId %d to free fork %d whichis owned by "
						                    "somebody else [%d]\n ",
						                    clientId,
						                    forkNo,
						                    it->second);
					} else {
						std::cout << format("dpServerLoop: eater %d is freeing fork %d\n", clientId, forkNo);
						forkOwners.erase(it);
						auto it2 = pending.find(forkNo);
						if (it2 == pending.end()) {
							std::cout << format(
							    "dpServerLoop: eater %d, fork %d: nobody is waiting on this fork\n", clientId, forkNo);
							req.reply.send(req.forkState);
						} else {
							GetForkRequest pending_req = it2->second;
							pending.erase(it2);
							int nextClient = pending_req.forkState.clientId;
							std::cout << format("dpServerLoop: eater %d fork %d: giving to waiting eater %d\n",
							                    clientId,
							                    forkNo,
							                    nextClient);
							forkOwners[forkNo] = nextClient;
							req.reply.send(req.forkState);
							pending_req.reply.send(pending_req.forkState);
						}
					}
				}
			}
		} catch (Error& e) {
			// XXX this is cargo-culted from tutorial.actor.cpp
			if (e.code() != error_code_operation_obsolete) {
				std::cerr << format("dpServerLoop: Error %d / %s\n", e.code(), e.what());
				throw e;
			}
		}
	}
}

static void usage(const char* argv0) {
	std::cerr << format("Usage: %s -p portNum | -s serverAddress\n", argv0);
}

int main(int argc, char** argv) {
	// Cargo-culted from tutorial.actor.cpp.
	platformInit();
	g_network = newNet2(TLSConfig(), /*useThreadPool=*/false, /*useMetrics=*/true);

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
			std::cerr << format(
			    "Error binding to address [%s]: %d, %s\n", serverAddress.toString().c_str(), e.code(), e.what());
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
