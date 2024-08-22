/*
* tutorial.actor.cpp

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

NetworkAddress serverAddress;

enum TutorialWellKnownEndpoints {
	WLTOKEN_SIMPLE_KV_SERVER = WLTOKEN_FIRST_AVAILABLE,
	WLTOKEN_ECHO_SERVER,
	WLTOKEN_COUNT_IN_TUTORIAL
};

// this is a simple actor that will report how long
// it is already running once a second.
ACTOR Future<Void> simpleTimer() {
	// we need to remember the time when we first
	// started.
	// This needs to be a state-variable because
	// we will use it in different parts of the
	// actor. If you don't understand how state
	// variables work, it is a good idea to remove
	// the state keyword here and look at the
	// generated C++ code from the actor compiler.
	state double start_time = g_network->now();
	loop {
		wait(delay(1.0));
		std::cout << format("Time: %.2f\n", g_network->now() - start_time);
	}
}

// A actor that demonstrates how choose-when blocks work.
ACTOR Future<Void> someFuture(Future<int> ready) {
	// loop choose {} works as well here - the braces are optional
	loop choose {
		when(wait(delay(0.5))) {
			std::cout << "Still waiting...\n";
		}
		when(int r = wait(ready)) {
			std::cout << format("Ready %d\n", r);
			wait(delay(double(r)));
			std::cout << "Done\n";
			return Void();
		}
	}
}

ACTOR Future<Void> promiseDemo() {
	state Promise<int> promise;
	state Future<Void> f = someFuture(promise.getFuture());
	wait(delay(3.0));
	promise.send(2);
	wait(f);
	return Void();
}

ACTOR Future<Void> eventLoop(AsyncTrigger* trigger) {
	loop choose {
		when(wait(delay(0.5))) {
			std::cout << "Still waiting...\n";
		}
		when(wait(trigger->onTrigger())) {
			std::cout << "Triggered!\n";
		}
	}
}

ACTOR Future<Void> triggerDemo() {
	state int runs = 1;
	state AsyncTrigger trigger;
	state Future<Void> triggerLoop = eventLoop(&trigger);
	while (++runs < 10) {
		wait(delay(1.0));
		std::cout << "trigger..";
		trigger.trigger();
	}
	std::cout << "Done.";
	return Void();
}

struct EchoServerInterface {
	constexpr static FileIdentifier file_identifier = 3152015;
	RequestStream<struct GetInterfaceRequest> getInterface;
	RequestStream<struct EchoRequest> echo;
	RequestStream<struct ReverseRequest> reverse;
	RequestStream<struct StreamRequest> stream;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, echo, reverse, stream);
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
	// this variable has to be called reply!
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, message, reply);
	}
};

struct ReverseRequest {
	constexpr static FileIdentifier file_identifier = 10765955;
	std::string message;
	// this variable has to be called reply!
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, message, reply);
	}
};

struct StreamReply : ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 440804;

	int index = 0;
	StreamReply() = default;
	explicit StreamReply(int index) : index(index) {}

	size_t expectedSize() const { return 2e6; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ReplyPromiseStreamReply::acknowledgeToken, ReplyPromiseStreamReply::sequence, index);
	}
};

struct StreamRequest {
	constexpr static FileIdentifier file_identifier = 5410805;
	ReplyPromiseStream<StreamReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

uint64_t tokenCounter = 1;

ACTOR Future<Void> echoServer() {
	state EchoServerInterface echoServer;
	echoServer.getInterface.makeWellKnownEndpoint(WLTOKEN_ECHO_SERVER, TaskPriority::DefaultEndpoint);
	loop {
		try {
			choose {
				when(GetInterfaceRequest req = waitNext(echoServer.getInterface.getFuture())) {
					req.reply.send(echoServer);
				}
				when(EchoRequest req = waitNext(echoServer.echo.getFuture())) {
					req.reply.send(req.message);
				}
				when(ReverseRequest req = waitNext(echoServer.reverse.getFuture())) {
					req.reply.send(std::string(req.message.rbegin(), req.message.rend()));
				}
				when(state StreamRequest req = waitNext(echoServer.stream.getFuture())) {
					req.reply.setByteLimit(1024);
					state int i = 0;
					for (; i < 100; ++i) {
						wait(req.reply.onReady());
						std::cout << "Send " << i << std::endl;
						req.reply.send(StreamReply{ i });
					}
					req.reply.sendError(end_of_stream());
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

ACTOR Future<Void> echoClient() {
	state EchoServerInterface server;
	server.getInterface =
	    RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_ECHO_SERVER));
	EchoServerInterface s = wait(server.getInterface.getReply(GetInterfaceRequest()));
	server = s;
	EchoRequest echoRequest;
	echoRequest.message = "Hello World";
	std::string echoMessage = wait(server.echo.getReply(echoRequest));
	std::cout << format("Sent %s to echo, received %s\n", "Hello World", echoMessage.c_str());
	ReverseRequest reverseRequest;
	reverseRequest.message = "Hello World";
	std::string reverseString = wait(server.reverse.getReply(reverseRequest));
	std::cout << format("Sent %s to reverse, received %s\n", "Hello World", reverseString.c_str());

	state ReplyPromiseStream<StreamReply> stream = server.stream.getReplyStream(StreamRequest{});
	state int j = 0;
	try {
		loop {
			StreamReply rep = waitNext(stream.getFuture());
			std::cout << "Rep: " << rep.index << std::endl;
			ASSERT(rep.index == j++);
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream || e.code() == error_code_connection_failed);
	}
	return Void();
}

struct SimpleKeyValueStoreInterface {
	constexpr static FileIdentifier file_identifier = 8226647;
	RequestStream<struct GetKVInterface> connect;
	RequestStream<struct GetRequest> get;
	RequestStream<struct SetRequest> set;
	RequestStream<struct ClearRequest> clear;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connect, get, set, clear);
	}
};

struct GetKVInterface {
	constexpr static FileIdentifier file_identifier = 8062308;
	ReplyPromise<SimpleKeyValueStoreInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct GetRequest {
	constexpr static FileIdentifier file_identifier = 6983506;
	std::string key;
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, reply);
	}
};

struct SetRequest {
	constexpr static FileIdentifier file_identifier = 7554186;
	std::string key;
	std::string value;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value, reply);
	}
};

struct ClearRequest {
	constexpr static FileIdentifier file_identifier = 8500026;
	std::string from;
	std::string to;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, from, to, reply);
	}
};

ACTOR Future<Void> kvStoreServer() {
	state SimpleKeyValueStoreInterface inf;
	state std::map<std::string, std::string> store;
	inf.connect.makeWellKnownEndpoint(WLTOKEN_SIMPLE_KV_SERVER, TaskPriority::DefaultEndpoint);
	loop {
		choose {
			when(GetKVInterface req = waitNext(inf.connect.getFuture())) {
				std::cout << "Received connection attempt\n";
				req.reply.send(inf);
			}
			when(GetRequest req = waitNext(inf.get.getFuture())) {
				auto iter = store.find(req.key);
				if (iter == store.end()) {
					req.reply.sendError(io_error());
				} else {
					req.reply.send(iter->second);
				}
			}
			when(SetRequest req = waitNext(inf.set.getFuture())) {
				store[req.key] = req.value;
				req.reply.send(Void());
			}
			when(ClearRequest req = waitNext(inf.clear.getFuture())) {
				auto from = store.lower_bound(req.from);
				auto to = store.lower_bound(req.to);
				while (from != store.end() && from != to) {
					auto next = from;
					++next;
					store.erase(from);
					from = next;
				}
				req.reply.send(Void());
			}
		}
	}
}

ACTOR Future<SimpleKeyValueStoreInterface> connect() {
	std::cout << format("%llu: Connect...\n", uint64_t(g_network->now()));
	auto reqStream = RequestStream<GetKVInterface>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_SIMPLE_KV_SERVER));
	SimpleKeyValueStoreInterface result = wait(reqStream.getReply(GetKVInterface()));
	std::cout << format("%llu: done..\n", uint64_t(g_network->now()));
	return result;
}

ACTOR Future<Void> kvSimpleClient() {
	state SimpleKeyValueStoreInterface server = wait(connect());
	std::cout << format("Set %s -> %s\n", "foo", "bar");
	SetRequest setRequest;
	setRequest.key = "foo";
	setRequest.value = "bar";
	wait(server.set.getReply(setRequest));
	GetRequest getRequest;
	getRequest.key = "foo";
	std::string value = wait(server.get.getReply(getRequest));
	std::cout << format("get(%s) -> %s\n", "foo", value.c_str());
	return Void();
}

ACTOR Future<Void> kvClient(SimpleKeyValueStoreInterface server, std::shared_ptr<uint64_t> ops) {
	state Future<Void> timeout = delay(20);
	state int rangeSize = 2 << 12;
	loop {
		SetRequest setRequest;
		setRequest.key = std::to_string(deterministicRandom()->randomInt(0, rangeSize));
		setRequest.value = "foo";
		wait(server.set.getReply(setRequest));
		++(*ops);
		try {
			GetRequest getRequest;
			getRequest.key = std::to_string(deterministicRandom()->randomInt(0, rangeSize));
			std::string _ = wait(server.get.getReply(getRequest));
			++(*ops);
		} catch (Error& e) {
			if (e.code() != error_code_io_error) {
				throw e;
			}
		}
		int from = deterministicRandom()->randomInt(0, rangeSize);
		ClearRequest clearRequest;
		clearRequest.from = std::to_string(from);
		clearRequest.to = std::to_string(from + 100);
		wait(server.clear.getReply(clearRequest));
		++(*ops);
		if (timeout.isReady()) {
			// we are done
			return Void();
		}
	}
}

ACTOR Future<Void> throughputMeasurement(std::shared_ptr<uint64_t> operations) {
	loop {
		wait(delay(1.0));
		std::cout << format("%llu op/s\n", *operations);
		*operations = 0;
	}
}

ACTOR Future<Void> multipleClients() {
	SimpleKeyValueStoreInterface server = wait(connect());
	auto ops = std::make_shared<uint64_t>(0);
	std::vector<Future<Void>> clients(100);
	for (auto& f : clients) {
		f = kvClient(server, ops);
	}
	auto done = waitForAll(clients);
	wait(done || throughputMeasurement(ops));
	return Void();
}

std::string clusterFile = "fdb.cluster";

ACTOR Future<Void> logThroughput(int64_t* v, Key* next) {
	loop {
		state int64_t last = *v;
		wait(delay(1));
		fmt::print("throughput: {} bytes/s, next: {}\n", *v - last, printable(*next).c_str());
	}
}

ACTOR Future<Void> fdbClientStream() {
	state Database db = Database::createDatabase(clusterFile, 300);
	state Transaction tx(db);
	state Key next;
	state int64_t bytes = 0;
	state Future<Void> logFuture = logThroughput(&bytes, &next);
	loop {
		state PromiseStream<Standalone<RangeResultRef>> results;
		try {
			state Future<Void> stream = tx.getRangeStream(results,
			                                              KeySelector(firstGreaterOrEqual(next), next.arena()),
			                                              KeySelector(firstGreaterOrEqual(normalKeys.end)),
			                                              GetRangeLimits());
			loop {
				Standalone<RangeResultRef> range = waitNext(results.getFuture());
				if (range.size()) {
					bytes += range.expectedSize();
					next = keyAfter(range.back().key);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			wait(tx.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> fdbClientGetRange() {
	state Database db = Database::createDatabase(clusterFile, 300);
	state Transaction tx(db);
	state Key next;
	state int64_t bytes = 0;
	state Future<Void> logFuture = logThroughput(&bytes, &next);
	loop {
		try {
			Standalone<RangeResultRef> range =
			    wait(tx.getRange(KeySelector(firstGreaterOrEqual(next), next.arena()),
			                     KeySelector(firstGreaterOrEqual(normalKeys.end)),
			                     GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, CLIENT_KNOBS->REPLY_BYTE_LIMIT)));
			bytes += range.expectedSize();
			if (!range.more) {
				break;
			}
			next = keyAfter(range.back().key);
		} catch (Error& e) {
			wait(tx.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> fdbClient() {
	wait(delay(30));
	state Database db = Database::createDatabase(clusterFile, 300);
	state Transaction tx(db);
	state std::string keyPrefix = "/tut/";
	state Key startKey;
	state KeyRef endKey = "/tut0"_sr;
	state int beginIdx = 0;
	loop {
		try {
			tx.reset();
			// this workload is stupidly simple:
			// 1. select a random key between 1
			//    and 1e8
			// 2. select this key plus the 100
			//    next ones
			// 3. write 10 values in [k, k+100]
			beginIdx = deterministicRandom()->randomInt(0, 1e8 - 100);
			startKey = keyPrefix + std::to_string(beginIdx);
			RangeResult range = wait(tx.getRange(KeyRangeRef(startKey, endKey), 100));
			for (int i = 0; i < 10; ++i) {
				Key k = Key(keyPrefix + std::to_string(beginIdx + deterministicRandom()->randomInt(0, 100)));
				tx.set(k, "foo"_sr);
			}
			wait(tx.commit());
			std::cout << "Committed\n";
			wait(delay(2.0));
		} catch (Error& e) {
			wait(tx.onError(e));
		}
	}
}

ACTOR Future<Void> fdbStatusStresser() {
	state Database db = Database::createDatabase(clusterFile, 300);
	state ReadYourWritesTransaction tx(db);
	state Key statusJson(std::string("\xff\xff/status/json"));
	loop {
		try {
			tx.reset();
			Optional<Value> _ = wait(tx.get(statusJson));
		} catch (Error& e) {
			wait(tx.onError(e));
		}
	}
}

std::unordered_map<std::string, std::function<Future<Void>()>> actors = {
	{ "timer", &simpleTimer }, // ./tutorial timer
	{ "promiseDemo", &promiseDemo }, // ./tutorial promiseDemo
	{ "triggerDemo", &triggerDemo }, // ./tutorial triggerDemo
	{ "echoServer", &echoServer }, // ./tutorial -p 6666 echoServer
	{ "echoClient", &echoClient }, // ./tutorial -s 127.0.0.1:6666 echoClient
	{ "kvStoreServer", &kvStoreServer }, // ./tutorial -p 6666 kvStoreServer
	{ "kvSimpleClient", &kvSimpleClient }, // ./tutorial -s 127.0.0.1:6666 kvSimpleClient
	{ "multipleClients", &multipleClients }, // ./tutorial -s 127.0.0.1:6666 multipleClients
	{ "fdbClientStream", &fdbClientStream }, // ./tutorial -C $CLUSTER_FILE_PATH fdbClientStream
	{ "fdbClientGetRange", &fdbClientGetRange }, // ./tutorial -C $CLUSTER_FILE_PATH fdbClientGetRange
	{ "fdbClient", &fdbClient }, // ./tutorial -C $CLUSTER_FILE_PATH fdbClient
	{ "fdbStatusStresser", &fdbStatusStresser }
}; // ./tutorial -C $CLUSTER_FILE_PATH fdbStatusStresser

int main(int argc, char* argv[]) {
	bool isServer = false;
	std::string port;
	std::vector<std::function<Future<Void>()>> toRun;
	// parse arguments
	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (arg == "-p") {
			isServer = true;
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after -p\n";
				return 1;
			}
			port = std::string(argv[++i]);
			continue;
		} else if (arg == "-s") {
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after -s\n";
				return 1;
			}
			serverAddress = NetworkAddress::parse(argv[++i]);
			continue;
		} else if (arg == "-C") {
			clusterFile = argv[++i];
			std::cout << "Using cluster file " << clusterFile << std::endl;
			continue;
		}
		auto actor = actors.find(arg);
		if (actor == actors.end()) {
			std::cout << format("Error: actor %s does not exist\n", arg.c_str());
			return 1;
		}
		toRun.push_back(actor->second);
	}
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);
	FlowTransport::createInstance(!isServer, 0, WLTOKEN_COUNT_IN_TUTORIAL);
	NetworkAddress publicAddress = NetworkAddress::parse("0.0.0.0:0");
	if (isServer) {
		publicAddress = NetworkAddress::parse("0.0.0.0:" + port);
	}
	// openTraceFile(publicAddress, TRACE_DEFAULT_ROLL_SIZE,
	//              TRACE_DEFAULT_MAX_LOGS_SIZE);
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
