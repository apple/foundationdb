/*
 * tutorial.cpp
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
#include "fdbrpc/Net2FileSystem.h"
#include <functional>
#include <unordered_map>
#include <memory>
#include <iostream>

using namespace std::literals::string_literals;
using namespace std::literals::string_view_literals;

NetworkAddress serverAddress;

enum TutorialWellKnownEndpoints {
	WLTOKEN_SIMPLE_KV_SERVER = WLTOKEN_FIRST_AVAILABLE,
	WLTOKEN_ECHO_SERVER,
	WLTOKEN_COUNT_IN_TUTORIAL
};

// this is a simple actor that will report how long
// it is already running once a second.
Future<Void> simpleTimer() {
	// we need to remember the time when we first
	// started.
	double start_time = g_network->now();
	loop {
		co_await delay(1.0);
		std::cout << format("Time: %.2f\n", g_network->now() - start_time);
	}
}

// A actor that demonstrates how choose-when
// blocks work.
Future<Void> someFuture(Future<int> ready) {
	// loop choose {} works as well here - the braces are optional
	loop {
		co_await Choose()
		    .When(delay(0.5), [](Void const&) { std::cout << "Still waiting...\n"; })
		    .When(ready, [](int const& r) { std::cout << format("Ready %d\n", r); })
		    .run();
	}
}

Future<Void> promiseDemo() {
	Promise<int> promise;
	Future<Void> f = someFuture(promise.getFuture());
	co_await delay(3.0);
	promise.send(2);
	co_await f;
}

Future<Void> eventLoop(AsyncTrigger* trigger) {
	loop {
		co_await Choose()
		    .When(delay(0.5), [](Void const&) { std::cout << "Still waiting...\n"; })
		    .When(trigger->onTrigger(), [](Void const&) { std::cout << "Triggered!\n"; })
		    .run();
	}
}

Future<Void> triggerDemo() {
	int runs = 1;
	AsyncTrigger trigger;
	auto triggerLoop = eventLoop(&trigger);
	while (++runs < 10) {
		co_await delay(1.0);
		std::cout << "trigger..";
		trigger.trigger();
	}
	std::cout << "Done.";
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

Future<Void> echoServer() {
	EchoServerInterface echoServer;
	echoServer.getInterface.makeWellKnownEndpoint(WLTOKEN_ECHO_SERVER, TaskPriority::DefaultEndpoint);
	ActorCollection requests;
	loop {
		try {
			co_await Choose()
			    .When(requests.getResult(),
			          [](Void const&) {
				          // An actor collection with no constructor arguments or `false` as it's constructor argument
				          // will never finish. However, `getResult` will throw if any of the Futures we pass to it
				          // throw. So we have to wait on it, but we can assert that it either throws or is never ready
				          UNREACHABLE();
			          })
			    .When(echoServer.getInterface.getFuture(),
			          [&echoServer](GetInterfaceRequest const& req) { req.reply.send(echoServer); })
			    .When(echoServer.echo.getFuture(), [](EchoRequest const& req) { req.reply.send(req.message); })
			    .When(echoServer.reverse.getFuture(),
			          [](ReverseRequest const& req) {
				          req.reply.send(std::string(req.message.rbegin(), req.message.rend()));
			          })
			    .When(echoServer.stream.getFuture(),
			          [&requests](StreamRequest const& req) {
				          requests.add([](StreamRequest req) -> Future<Void> {
					          req.reply.setByteLimit(1024);
					          int i = 0;
					          for (; i < 100; ++i) {
						          co_await req.reply.onReady();
						          std::cout << "Send " << i << std::endl;
						          req.reply.send(StreamReply{ i });
					          }
					          req.reply.sendError(end_of_stream());
				          }(req));
			          })
			    .run();
		} catch (Error& e) {
			if (e.code() != error_code_operation_obsolete) {
				fprintf(stderr, "Error: %s\n", e.what());
				throw e;
			}
		}
	}
}

Future<Void> echoClient() {
	EchoServerInterface server;
	server.getInterface =
	    RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_ECHO_SERVER));
	server = co_await server.getInterface.getReply(GetInterfaceRequest());
	EchoRequest echoRequest;
	echoRequest.message = "Hello World";
	std::string echoMessage = co_await server.echo.getReply(echoRequest);
	std::cout << format("Sent %s to echo, received %s\n", "Hello World", echoMessage.c_str());
	ReverseRequest reverseRequest;
	reverseRequest.message = "Hello World";
	std::string reverseString = co_await server.reverse.getReply(reverseRequest);
	std::cout << format("Sent %s to reverse, received %s\n", "Hello World", reverseString.c_str());

	ReplyPromiseStream<StreamReply> stream = server.stream.getReplyStream(StreamRequest{});
	int j = 0;
	try {
		loop {
			StreamReply rep = co_await stream.getFuture();
			std::cout << "Rep: " << rep.index << std::endl;
			ASSERT(rep.index == j++);
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream || e.code() == error_code_connection_failed);
	}
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

Future<Void> kvStoreServer() {
	SimpleKeyValueStoreInterface inf;
	std::map<std::string, std::string> store;
	inf.connect.makeWellKnownEndpoint(WLTOKEN_SIMPLE_KV_SERVER, TaskPriority::DefaultEndpoint);
	loop {
		co_await Choose()
		    .When(inf.connect.getFuture(),
		          [&inf](GetKVInterface const& req) {
			          std::cout << "Received connection attempt\n";
			          req.reply.send(inf);
		          })
		    .When(inf.get.getFuture(),
		          [&store](GetRequest const& req) {
			          auto iter = store.find(req.key);
			          if (iter == store.end()) {
				          req.reply.sendError(io_error());
			          } else {
				          req.reply.send(iter->second);
			          }
		          })
		    .When(inf.set.getFuture(),
		          [&store](SetRequest const& req) {
			          store[req.key] = req.value;
			          req.reply.send(Void());
		          })
		    .When(inf.clear.getFuture(),
		          [&store](ClearRequest const& req) {
			          auto from = store.lower_bound(req.from);
			          auto to = store.lower_bound(req.to);
			          while (from != store.end() && from != to) {
				          auto next = from;
				          ++next;
				          store.erase(from);
				          from = next;
			          }
			          req.reply.send(Void());
		          })
		    .run();
	}
}

Future<SimpleKeyValueStoreInterface> connect() {
	std::cout << format("%llu: Connect...\n", uint64_t(g_network->now()));
	SimpleKeyValueStoreInterface c;
	c.connect = RequestStream<GetKVInterface>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_SIMPLE_KV_SERVER));
	SimpleKeyValueStoreInterface result = co_await c.connect.getReply(GetKVInterface());
	std::cout << format("%llu: done..\n", uint64_t(g_network->now()));
	co_return result;
}

Future<Void> kvSimpleClient() {
	SimpleKeyValueStoreInterface server = co_await connect();
	std::cout << format("Set %s -> %s\n", "foo", "bar");
	SetRequest setRequest;
	setRequest.key = "foo";
	setRequest.value = "bar";
	co_await server.set.getReply(setRequest);
	GetRequest getRequest;
	getRequest.key = "foo";
	std::string value = co_await server.get.getReply(getRequest);
	std::cout << format("get(%s) -> %s\n", "foo", value.c_str());
}

Future<Void> kvClient(SimpleKeyValueStoreInterface server, std::shared_ptr<uint64_t> ops) {
	auto timeout = delay(20);
	int rangeSize = 2 << 12;
	loop {
		SetRequest setRequest;
		setRequest.key = std::to_string(deterministicRandom()->randomInt(0, rangeSize));
		setRequest.value = "foo";
		co_await server.set.getReply(setRequest);
		++(*ops);
		try {
			GetRequest getRequest;
			getRequest.key = std::to_string(deterministicRandom()->randomInt(0, rangeSize));
			co_await server.get.getReply(getRequest);
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
		co_await server.clear.getReply(clearRequest);
		++(*ops);
		if (timeout.isReady()) {
			// we are done
			co_return;
		}
	}
}

Future<Void> throughputMeasurement(std::shared_ptr<uint64_t> operations) {
	loop {
		co_await delay(1.0);
		std::cout << format("%llu op/s\n", *operations);
		*operations = 0;
	}
}

Future<Void> multipleClients() {
	SimpleKeyValueStoreInterface server = co_await connect();
	auto ops = std::make_shared<uint64_t>(0);
	std::vector<Future<Void>> clients(100);
	for (auto& f : clients) {
		f = kvClient(server, ops);
	}
	auto done = waitForAll(clients);
	co_await (done || throughputMeasurement(ops));
	co_return;
}

std::string clusterFile = "fdb.cluster";

Future<Void> logThroughput(int64_t* v, Key* next) {
	loop {
		int64_t last = *v;
		co_await delay(1);
		fmt::print("throughput: {} bytes/s, next: {}\n", *v - last, printable(*next).c_str());
	}
}

Future<Void> fdbClientStream() {
	Database db = Database::createDatabase(clusterFile, 300);
	Transaction tx(db);
	Key next;
	int64_t bytes = 0;
	Future<Void> logFuture = logThroughput(&bytes, &next);
	loop {
		Future<Void> onError;
		PromiseStream<Standalone<RangeResultRef>> results;
		try {
			Future<Void> stream = tx.getRangeStream(results,
			                                        KeySelector(firstGreaterOrEqual(next), next.arena()),
			                                        KeySelector(firstGreaterOrEqual(normalKeys.end)),
			                                        GetRangeLimits());
			loop {
				Standalone<RangeResultRef> range = co_await results.getFuture();
				if (range.size()) {
					bytes += range.expectedSize();
					next = keyAfter(range.back().key);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			onError = tx.onError(e);
		}
		co_await onError;
	}
}

bool transactionDone(std::convertible_to<bool> auto v) {
	return v;
}

bool transaction_done(void) {
	return true;
}

template <class DB, class Fun>
Future<Void> runTransactionWhile(DB const& db, Fun f) {
	Transaction tr(db);
	loop {
		Future<Void> onError;
		try {
			if (transactionDone(co_await f(&tr))) {
				co_return;
			}
		} catch (Error& e) {
			onError = tr.onError(e);
		}
		co_await onError;
	}
}

template <class DB, class Fun>
Future<Void> runTransaction(DB const& db, Fun f) {
	return runTransactionWhile(db, [&f](Transaction* tr) -> Future<bool> {
		co_await f(tr);
		co_return true;
	});
}

template <class DB, class Fun>
Future<Void> runRYWTransaction(DB const& db, Fun f) {
	Future<Void> onError;
	ReadYourWritesTransaction tr(db);
	loop {
		if (onError.isValid()) {
			co_await onError;
			onError = Future<Void>();
		}
		try {
			co_await f(&tr);
			co_return;
		} catch (Error& e) {
			onError = tr.onError(e);
		}
	}
}

Future<Void> fdbClientGetRange() {
	Database db = Database::createDatabase(clusterFile, 300);
	Transaction tx(db);
	Key next;
	int64_t bytes = 0;
	Future<Void> logFuture = logThroughput(&bytes, &next);
	co_await runTransactionWhile(db, [&bytes, &next](Transaction* tr) -> Future<bool> {
		RangeResult range =
		    co_await tr->getRange(KeySelector(firstGreaterOrEqual(next), next.arena()),
		                          KeySelector(firstGreaterOrEqual(normalKeys.end)),
		                          GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, CLIENT_KNOBS->REPLY_BYTE_LIMIT));
		bytes += range.expectedSize();
		if (!range.more) {
			co_return true;
		}
		next = keyAfter(range.back().key);
		co_return false;
	});
	co_return;
}

Future<Void> fdbClient() {
	co_await delay(30);
	Database db = Database::createDatabase(clusterFile, 300);
	std::string keyPrefix = "/tut/";
	Key startKey;
	KeyRef endKey = "/tut0"_sr;
	int beginIdx = 0;
	loop {
		co_await runTransaction(db, [&](Transaction* tr) -> Future<Void> {
			// this workload is stupidly simple:
			// 1. select a random key between 1
			//    and 1e8
			// 2. select this key plus the 100
			//    next ones
			// 3. write 10 values in [k, k+100]
			beginIdx = deterministicRandom()->randomInt(0, 1e8 - 100);
			startKey = keyPrefix + std::to_string(beginIdx);
			auto range = co_await tr->getRange(KeyRangeRef(startKey, endKey), 100);
			for (int i = 0; i < 10; ++i) {
				Key k = Key(keyPrefix + std::to_string(beginIdx + deterministicRandom()->randomInt(0, 100)));
				tr->set(k, "foo"_sr);
			}
			co_await tr->commit();
			std::cout << "Committed\n";
			co_await delay(2.0);
			co_return;
		});
	}
}

Future<Void> fdbStatusStresser() {
	Database db = Database::createDatabase(clusterFile, 300);
	Key statusJson(std::string("\xff\xff/status/json"));
	loop {
		co_await runRYWTransaction(db, [&statusJson](ReadYourWritesTransaction* tr) -> Future<Void> {
			co_await tr->get(statusJson);
			co_return;
		});
	}
}

AsyncGenerator<Optional<StringRef>> readBlocks(Reference<IAsyncFile> file, int64_t blockSize) {
	auto sz = co_await file->size();
	decltype(sz) offset = 0;
	Arena arena;
	auto block = new (arena) uint8_t[blockSize];
	while (offset < sz) {
		auto read = co_await file->read(block, blockSize, offset);
		offset += read;
		co_yield StringRef(block, read);
	}
	while (true) {
		co_yield Optional<StringRef>{};
	}
}

AsyncGenerator<Optional<StringRef>> readLines(Reference<IAsyncFile> file) {
	auto blocks = readBlocks(file, 4 * 1024);
	Arena arena;
	StringRef lastLine;
	loop {
		auto optionalBlock = co_await blocks();
		if (!optionalBlock.present()) {
			if (lastLine.empty()) {
				co_yield Optional<StringRef>{};
			} else {
				co_yield lastLine;
				lastLine = {};
				arena = Arena();
				co_return;
			}
		}
		StringRef block = optionalBlock.get();
		auto endsWithNewLine = block.back() == uint8_t('\n');
		while (!block.empty()) {
			if (!lastLine.empty()) [[unlikely]] {
				concatenateStrings(arena, lastLine, block.eatAny("\n"_sr, nullptr));
				if (!block.empty() || endsWithNewLine) {
					co_yield lastLine;
					lastLine = StringRef();
					arena = Arena();
				}
			} else {
				auto line = block.eatAny("\n"_sr, nullptr);
				if (block.empty() && !endsWithNewLine) {
					lastLine = StringRef(arena, line);
				} else {
					co_yield line;
				}
			}
		}
	}
}

Future<Void> testReadLines() {
	auto path = "/etc/hosts"s;
	auto file = co_await IAsyncFileSystem::filesystem()->open(path, IAsyncFile::OPEN_READWRITE, 0640);
	auto lines = readLines(file);
	for (int i = 0; true; ++i) {
		auto line = co_await lines();
		if (!line.present()) {
			break;
		}
		fmt::print("{}: {}\n", i, line.get());
	}
}

// readLines -> Stream of lines of a text file

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
	{ "fdbStatusStresser", &fdbStatusStresser },
	{ "testReadLines", &testReadLines }
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
	Net2FileSystem::newFileSystem(-10, "");
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
