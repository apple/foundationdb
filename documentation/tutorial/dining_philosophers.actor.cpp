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
#include "flow/actorcompiler.h"

#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

enum DPEndpoints {
	DP_SERVER = WLTOKEN_FIRST_AVAILABLE,
	DP_ENDPOINT_COUNT,
};


ACTOR Future<Void> dpServer() {
	std::cout << format("dpServer: starting...\n");
	wait(delay(1.0));

	std::cout << format("dpServer: finished.\n");
	return Void();
}

ACTOR Future<Void> dpClient(NetworkAddress serverAddress, int idnum) {
	std::cout << format("dpClient: starting philosopher #%d, server address [%s]\n",
						idnum, serverAddress.toString().c_str());

	wait(delay(idnum));

	std::cout << format("dpClient: philosopher #%d finished.\n", idnum);
	return Void();
}

static void usage(const char *argv0) {
	std::cerr << format("Usage: %s -p portnum | -s serverAddress\n", argv0);
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
	std::string port;

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
		all.emplace_back(dpServer());
	} else {
		for (int i = 0; i < 5; i++) {
			all.emplace_back(dpClient(serverAddress, i));
		}
	}

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
