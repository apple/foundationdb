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
#include <cstdio>

#include "flow/UnitTest.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/FlowGrpcTests.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// So that tests are not optimized out. :/
void forceLinkGrpcTests() {}

namespace fdbrpc_test {
namespace asio = boost::asio;

TEST_CASE("/fdbrpc/grpc/basic_sync_client") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50001"));
	state GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	state Future<Void> server_actor = server.run();

	EchoClient client(grpc::CreateChannel(addr.toString(), grpc::InsecureChannelCredentials()));
	std::string reply = client.Echo("Ping!");
	std::cout << "Echo received: " << reply << std::endl;
	ASSERT_EQ(reply, "Echo: Ping!");

	server.shutdown();
	wait(server_actor);
	return Void();
}

TEST_CASE("/fdbrpc/grpc/basic_async_client") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50003"));
	state GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	state Future<Void> _ = server.run();

	state shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
	state AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		state EchoRequest request;
		request.set_message("Ping!");
		EchoResponse response = wait(client.call(&TestEchoService::Stub::Echo, request));
		std::cout << "Echo received: " << response.message() << std::endl;
		ASSERT_EQ(response.message(), "Echo: Ping!");
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_grpc_error);
		ASSERT(false);
	}

	return Void();
}

TEST_CASE("/fdbrpc/grpc/no_server_running") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50004"));
	state shared_ptr<asio::thread_pool> pool(make_shared<asio::thread_pool>(4));
	state AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		state EchoRequest request;
		request.set_message("Ping!");
		EchoResponse response = wait(client.call(&TestEchoService::Stub::Echo, request));
		ASSERT(false); // RPC should fail as there is no server running.;
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_grpc_error);
	}

	return Void();
}

TEST_CASE("/fdbrpc/grpc/destroy_server_without_shutdown") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50005"));
	state GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	state Future<Void> _ = server.run();
	return Void();
}

} // namespace fdbrpc_test

#endif