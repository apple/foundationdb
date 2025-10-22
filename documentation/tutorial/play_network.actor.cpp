/*
 * play_network.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "flow/Arena.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

#include <iostream>
#include <vector>

/// This file has code similar to play.actor.cpp, but for client/server actors talking over a network
/// It's also similar to tutorial.actor.cpp but only has the minimal code needed for testing client/server actor
/// interactions over the network.
/// Use this file as means to play with flow code and test network behavior
/// The goal is to give you a starting point with a boilerplate template
/// Don't expect frequent changes to this file unless we want to change the base template
/// The use-case would be for people to have ephemeral code (on top of this template) that never gets checked in
/// An example use-case is in the description of https://github.com/apple/foundationdb/pull/12484

/// Usage below

/// Start server:
/// ~/c/bin $ ./play_network -s 6666 serverActor

/// Then client:
/// ~/c/bin $ ./play_network -c 127.0.0.1:6666 clientActor
/// req msg: Hello World
/// rsp msg: dlroW olleH

/// Now server prints:
/// ~/c/bin $ ./play_network -s 6666 serverActor
/// got interface request from client
/// sent interface back to client
/// got play request with msg: Hello World
/// sending this play response back: dlroW olleH
/// sent this play response back: dlroW olleH

NetworkAddress serverAddress;

enum TutorialWellKnownEndpoints { WLTOKEN_PLAY_SERVER = WLTOKEN_FIRST_AVAILABLE, WLTOKEN_COUNT_IN_TUTORIAL };

struct PlayServerInterface {
	constexpr static FileIdentifier file_identifier = 3152015;
	RequestStream<struct GetInterfaceRequest> getInterface;
	RequestStream<struct PlayRequest> play;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getInterface, play);
	}
};

struct GetInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 12004156;
	ReplyPromise<PlayServerInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct PlayRequest {
	constexpr static FileIdentifier file_identifier = 10624019;
	std::string msg;
	// For now, returns a reverse of msg
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, msg, reply);
	}
};

ACTOR Future<Void> server() {
	// Setup
	state PlayServerInterface playServer;
	playServer.getInterface.makeWellKnownEndpoint(WLTOKEN_PLAY_SERVER, TaskPriority::DefaultEndpoint);

	// Main server
	loop {
		try {
			choose {
				when(GetInterfaceRequest req = waitNext(playServer.getInterface.getFuture())) {
					std::cout << "got interface request from client" << std::endl;
					req.reply.send(playServer);
					std::cout << "sent interface back to client" << std::endl;
				}
				when(PlayRequest req = waitNext(playServer.play.getFuture())) {
					std::cout << "got play request with msg: " << req.msg << std::endl;
					std::string rsp(req.msg.rbegin(), req.msg.rend());
					std::cout << "sending this play response back: " << rsp << std::endl;
					req.reply.send(rsp);
					std::cout << "sent this play response back: " << rsp << std::endl;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_operation_obsolete) {
				fprintf(stderr, "Error: %s\n", e.what());
				throw e;
			}
		}
	}
}

ACTOR Future<Void> client() {
	// Setup
	state PlayServerInterface server;
	server.getInterface =
	    RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_PLAY_SERVER));
	PlayServerInterface s = wait(server.getInterface.getReply(GetInterfaceRequest()));
	server = s;

	// Create and print req
	PlayRequest playReq;
	playReq.msg = "Hello World";
	std::cout << "req msg: " << playReq.msg << std::endl;

	// Send req and wait for rsp
	std::string playRsp = wait(server.play.getReply(playReq));

	// Print rsp
	std::cout << "rsp msg: " << playRsp << std::endl;

	return Void();
}

std::unordered_map<std::string, std::function<Future<Void>()>> actors = {
	{ "serverActor", &server }, // ./play_network -s 6666 serverActor
	{ "clientActor", &client }, // ./play_network -c 127.0.0.1:6666 clientActor
};

int main(int argc, char* argv[]) {
	bool isServer = false;
	std::string port;
	std::vector<std::function<Future<Void>()>> toRun;

	// parse arguments
	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (arg == "-s") {
			isServer = true;
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after -p\n";
				return 1;
			}
			port = std::string(argv[++i]);
			continue;
		} else if (arg == "-c") {
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after -s\n";
				return 1;
			}
			serverAddress = NetworkAddress::parse(argv[++i]);
			continue;
		} else {
			assert(false);
		}

		auto actor = actors.find(arg);
		if (actor == actors.end()) {
			std::cout << format("Error: actor %s does not exist\n", arg.c_str());
			return 1;
		}
		toRun.push_back(actor->second);
	}

	// platform init
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);
	FlowTransport::createInstance(!isServer, 0, WLTOKEN_COUNT_IN_TUTORIAL);
	NetworkAddress publicAddress = NetworkAddress::parse("0.0.0.0:0");
	if (isServer) {
		publicAddress = NetworkAddress::parse("0.0.0.0:" + port);
	}

	try {
		if (isServer) {
			auto listenError = FlowTransport::transport().bind(publicAddress, publicAddress);
			if (listenError.isError()) {
				listenError.get();
			}
		}
	} catch (Error& e) {
		std::cout << format("Error while binding to address (%d): %s\n", e.code(), e.what());
	}

	// now we start the actors
	std::vector<Future<Void>> all;
	all.reserve(toRun.size());
	for (auto& f : toRun) {
		all.emplace_back(f());
	}

	auto f = stopAfter(waitForAll(all));

	g_network->run();

	return 0;
}