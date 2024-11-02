/*
 * fdbrpc_bench.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <iostream>
#include <boost/program_options.hpp>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowTransport.h"
#include "flow/actorcompiler.h" // has to be last include

namespace fdbrpc_bench {
NetworkAddress serverAddress;

enum FdbRpcBenchWellKnownEndpoints {
	WLTOKEN_ECHO_SERVER = WLTOKEN_FIRST_AVAILABLE,
	WLTOKEN_COUNT_ENDPOINTS,
};

struct EchoServerInterface {
	constexpr static FileIdentifier file_identifier = 3152015;
	RequestStream<struct GetInterfaceRequest> getInterface;
	RequestStream<struct EchoRequest> echo;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, echo);
	}
};

struct GetInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 12004156;
	ReplyPromise<EchoServerInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct EchoRequest {
	constexpr static FileIdentifier file_identifier = 10624019;
	std::string message;
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, message, reply);
	}
};

// A sliding window counter over last `size` seconds. Internally uses a vector
// where each entry counts the number of hits each second.
class StatCounter {
public:
	StatCounter(int size = 10) : vals(size) {}

	// Returns the average number of hits per seconds over last `size` seconds.
	int avg() {
		int now_ts = this->now() / 1000; // Convert ms to second.
		int sum = 0;
		for (auto [ts, v] : vals) {
			if (ts < now_ts - vals.size()) // timestamp older than last `size` seconds.
				continue;
			sum += v;
		}
		return sum / vals.size();
	}

	// Increaments the counter by one for current time.
	void inc() {
		int ts = this->now() / 1000;
		int pos = ts % vals.size();

		auto [old_ts, v] = vals[pos];
		if (old_ts < ts) {
			// Timestamp older than last `size` second, so we reset it back.
			vals[pos] = { ts, 1 };
		} else {
			vals[pos] = { old_ts, v + 1 };
		}
	}

private:
	int64_t now() {
		auto n = std::chrono::system_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(n.time_since_epoch());
		return duration.count();
	}

	std::vector<std::pair<int64_t, int>> vals;
};

ACTOR Future<Void> echoServer() {
	state EchoServerInterface echoServer;
	state StatCounter c;
	echoServer.getInterface.makeWellKnownEndpoint(WLTOKEN_ECHO_SERVER, TaskPriority::DefaultEndpoint);
	state Future<Void> next = delay(10);
	loop {
		try {
			choose {
				when(GetInterfaceRequest req = waitNext(echoServer.getInterface.getFuture())) {
					req.reply.send(echoServer);
				}
				when(EchoRequest req = waitNext(echoServer.echo.getFuture())) {
					req.reply.send(req.message);
					c.inc();
				}
				when(wait(next)) {
					next = delay(10);
					std::cout << "Throughput: " << c.avg() << " req/sec" << std::endl;
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

int payload_size_bytes = 1024 * 10;

std::string randString(int size) {
	const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	const int charsetLength = charset.length();
	std::string result;

	// Seed the random number generator
	std::srand(static_cast<unsigned int>(std::time(nullptr)));

	for (int i = 0; i < size; ++i) {
		result += charset[std::rand() % charsetLength];
	}

	return result;
}

ACTOR Future<Void> echoClient() {
	std::cout << "Starting client. Payload size: " << payload_size_bytes << " bytes" << std::endl;
	state EchoServerInterface server;
	server.getInterface =
	    RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_ECHO_SERVER));
	EchoServerInterface s = wait(server.getInterface.getReply(GetInterfaceRequest()));
	server = s;
	state std::string payload = randString(payload_size_bytes);

	while (true) {
		state int duration_seconds = 10;
		state int request_count = 0;

		state std::chrono::time_point<std::chrono::steady_clock> start_time = std::chrono::steady_clock::now();
		state std::chrono::time_point<std::chrono::steady_clock> end_time =
		    start_time + std::chrono::seconds(duration_seconds);

		while (std::chrono::steady_clock::now() < end_time) {
			EchoRequest echoRequest;
			echoRequest.message = payload;
			std::string echoMessage = wait(server.echo.getReply(echoRequest));
			++request_count;
		}
		std::cout << "Sent " << request_count << " requests in " << request_count / duration_seconds << " /second"
		          << std::endl;
	}
}

std::unordered_map<std::string, std::function<Future<Void>()>> actors = {
	{ "server", &echoServer },
	{ "client", &echoClient },
};
} // namespace fdbrpc_bench

int main(int argc, char* argv[]) {
	using namespace fdbrpc_bench;
	namespace po = boost::program_options;

	po::options_description desc("fdbrpc_bench usage");
	// clang-format off
	desc.add_options()
		("help,h","show help message")
		("mode,m", po::value<std::string>(), "process mode [server/client]")
		("payload_size,s", po::value<int>(), "size of payload sent by client (bytes)");
	// clang-format on

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	// Check for help option
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 0;
	}

	auto errMsg = "invalid arguments provided.\n";
	if (vm.count("mode") == 0) {
		std::cerr << errMsg << desc << std::endl;
		return -1;
	}

	auto mode = vm["mode"].as<std::string>();
	if ((mode != "client" && mode != "server") || (mode == "server" && vm.count("payload_size") > 0)) {
		std::cerr << errMsg << desc << std::endl;
		return -1;
	}

	if (vm.count("payload_size") > 0) {
		payload_size_bytes = vm["payload_size"].as<int>();
	}

	bool isServer = (mode == "server");
	std::string port;
	std::vector<std::function<Future<Void>()>> toRun;
	auto actor = actors.find(mode);
	toRun.push_back(actor->second);

	platformInit();
	g_network = newNet2(TLSConfig(), false, true);
	FlowTransport::createInstance(!isServer, 0, WLTOKEN_COUNT_ENDPOINTS);

	serverAddress = NetworkAddress::parse("127.0.0.1:9001");
	NetworkAddress publicAddress = NetworkAddress::parse("127.0.0.1:9001");

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

	std::vector<Future<Void>> all;
	all.reserve(toRun.size());
	for (auto& f : toRun) {
		all.emplace_back(f());
	}

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
