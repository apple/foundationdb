
/**
 * FlowGrpcTests.actor.cpp
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

#ifdef FLOW_GRPC_ENABLED
#include "flow/UnitTest.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/FlowGrpcTests.h"
#include "flow/flow.h"

// So that tests are not optimized out. :/
void forceLinkGrpcTests2() {}

namespace fdbrpc_test {
namespace asio = boost::asio;

TEST_CASE("/fdbrpc/grpc/basic_coro") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50500"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		EchoRequest request;
		request.set_message("Ping!");
		EchoResponse response = co_await client.call(&TestEchoService::Stub::Echo, request);
		std::cout << "Echo received: " << response.message() << std::endl;
		ASSERT_EQ(response.message(), "Echo: Ping!");
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_grpc_error);
		ASSERT(false);
	}
}

TEST_CASE("/fdbrpc/grpc/basic_stream_server") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50501"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	int count = 0;
	try {
		EchoRequest request;
		request.set_message("Ping!");
		auto stream = client.call(&TestEchoService::Stub::EchoRecvStream10, request);
		loop {
			auto response = co_await stream;
			ASSERT_EQ(response.message(), "Echo: Ping!");
			count += 1;
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			ASSERT_EQ(count, 10); // Should send 10 reponses.
			co_return;
		}
		ASSERT(false);
	}
	co_return;
}

TEST_CASE("/fdbrpc/grpc/future_destroy") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50500"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		EchoRequest request;
		request.set_message("Ping!");
		{
			auto w = client.call(&TestEchoService::Stub::Echo, request);
			// Out-of-scope
		}
	} catch (Error& e) {
		ASSERT(false);
	}
	co_await delay(1); // So that lifetime of client stays.
	co_return;
}

TEST_CASE("/fdbrpc/grpc/stream_destroy") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50501"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	int count = 0;
	try {
		EchoRequest request;
		request.set_message("Ping!");
		{
			auto stream = client.call(&TestEchoService::Stub::EchoRecvStream10, request);
			auto response = co_await stream;
			ASSERT_EQ(response.message(), "Echo: Ping!");
		}
		// TODO: Test if server cancels.
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			ASSERT_EQ(count, 10); // Should send 10 reponses.
			co_return;
		}
		ASSERT(false);
	}
	co_return;
}

// T_EST_CASE("/fdbrpc/grpc/basic_stream_client") {
// 	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50502"));
// 	GrpcServer server(addr);
// 	server.registerService(make_shared<TestEchoServiceImpl>());
// 	Future<Void> _ = server.run();

// 	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
// 	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

// 	int count = 0;
// 	try {
// 		EchoRequest request;
// 		request.set_message("Ping!");
// 		auto stream = client.call(&TestEchoService::Stub::EchoSendStream10, request);
// 		loop {
// 			auto response = co_await stream;
// 		    ASSERT_EQ(response.message(), "Echo: Ping!");
// 			count += 1;
// 		}
// 	} catch (Error& e) {
// 		if (e.code() == error_code_end_of_stream) {
// 			ASSERT_EQ(count, 10); // Should send 10 reponses.
// 			co_return;
// 		}
// 		ASSERT(false);
// 	}
// 	co_return;
// }

} // namespace fdbrpc_test

#endif