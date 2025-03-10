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

#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/FlowGrpcTests.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// So that tests are not optimized out. :/
void forceLinkGrpcTests() {}

namespace fdbrpc_test {

TEST_CASE("/fdbrpc/grpc/basic_sync_client") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50001"));
	state GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	state Future<Void> server_actor = server.run();
	wait(server.onRunning());

	EchoClient client(grpc::CreateChannel(addr.toString(), grpc::InsecureChannelCredentials()));
	std::string reply = client.Echo("Ping!");
	std::cout << "Echo received: " << reply << std::endl;
	ASSERT_EQ(reply, "Echo: Ping!");

	wait(server.shutdown());
	wait(server_actor);
	return Void();
}

TEST_CASE("/fdbrpc/grpc/basic_async_client") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50003"));
	state GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	state Future<Void> _ = server.run();
	wait(server.onRunning());

	state shared_ptr<AsyncTaskExecutor> pool = make_shared<AsyncTaskExecutor>(4);
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

TEST_CASE("/fdbrpc/grpc/actor_basic_stream_server") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50002"));
	state GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	state Future<Void> _ = server.run();
	wait(server.onRunning());

	state shared_ptr<AsyncTaskExecutor> pool = make_shared<AsyncTaskExecutor>(4);
	state AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	state int count = 0;
	try {
		EchoRequest request;
		request.set_message("Ping!");
		state FutureStream<EchoResponse> stream = client.call(&TestEchoService::Stub::EchoRecvStream10, request);
		while (true) {
			EchoResponse response = waitNext(stream);
			ASSERT_EQ(response.message(), "Echo: Ping!");
			count += 1;
		}
	} catch (Error& e) {
		std::cout << "Error: " << e.name() << std::endl;
		if (e.code() == error_code_end_of_stream) {
			ASSERT_EQ(count, 10); // Should send 10 reponses.
			return Void();
		}
		ASSERT(false);
	}
	return Void();
}

TEST_CASE("/fdbrpc/grpc/no_server_running") {
	state NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50004"));
	state shared_ptr<AsyncTaskExecutor> pool = make_shared<AsyncTaskExecutor>(4);
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
	wait(server.onRunning());
	return Void();
}

} // namespace fdbrpc_test

#endif
