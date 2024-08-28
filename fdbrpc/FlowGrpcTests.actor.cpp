/**
 * grpc_tests.actor.cpp
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

#include <cstdio>
#include <thread>

#include "flow/UnitTest.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/test/echo.grpc.pb.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// So that tests are not optimized out. :/
void forceLinkGrpcTests() {}

namespace fdbrpc_test {

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

using fdbrpc::test::EchoRequest;
using fdbrpc::test::EchoResponse;
using fdbrpc::test::TestEchoService;

// Service implementation
class TestEchoServiceImpl final : public TestEchoService::Service {
	Status Echo(ServerContext* context, const EchoRequest* request, EchoResponse* reply) override {
		reply->set_message("Echo: " + request->message());
		return Status::OK;
	}
};

class EchoClient {
public:
	EchoClient(std::shared_ptr<Channel> channel) : stub_(TestEchoService::NewStub(channel)) {}

	std::string Echo(const std::string& message) {
		EchoRequest request;
		request.set_message(message);

		EchoResponse reply;
		ClientContext context;

		Status status = stub_->Echo(&context, request, &reply);

		if (status.ok()) {
			return reply.message();
		} else {
			std::cout << "RPC failed" << std::endl;
			return "RPC failed";
		}
	}

private:
	std::unique_ptr<TestEchoService::Stub> stub_;
};

TEST_CASE("/fdbrpc/grpc/basic_thread") {
	state NetworkAddress addr = NetworkAddress::parse("127.0.0.1:50000");
	state std::shared_ptr<TestEchoServiceImpl> service = std::make_shared<TestEchoServiceImpl>();
	state std::shared_ptr<GrpcServer> s = std::make_shared<GrpcServer>(addr);
	s->registerService(service);
	auto server = std::thread([=] { s->runSync(); });

	EchoClient client(grpc::CreateChannel(addr.toString(), grpc::InsecureChannelCredentials()));
	std::string message = "Ping!";
	std::string reply = client.Echo(message);
	std::cout << "Echo received: " << reply << std::endl;
	ASSERT_EQ(reply, "Echo: Ping!");

	s->shutdown();
	server.join();
	return Void();
}

TEST_CASE("/fdbrpc/grpc/basic_async_server") {
	state NetworkAddress addr = NetworkAddress::parse("127.0.0.1:50001");
	state std::shared_ptr<TestEchoServiceImpl> service = std::make_shared<TestEchoServiceImpl>();
	state std::shared_ptr<GrpcServer> s = std::make_shared<GrpcServer>(addr);
	s->registerService(service);
	state Future<Void> server_future = s->run();

	EchoClient client(grpc::CreateChannel(addr.toString(), grpc::InsecureChannelCredentials()));
	std::string message = "Ping!";
	std::string reply = client.Echo(message);
	std::cout << "Echo received: " << reply << std::endl;
	ASSERT_EQ(reply, "Echo: Ping!");

	s->shutdown();
	wait(server_future);
	return Void();
}

TEST_CASE("/fdbrpc/grpc/basic_async_client") {
	state NetworkAddress addr = NetworkAddress::parse("127.0.0.1:50002");
	state std::shared_ptr<TestEchoServiceImpl> service = std::make_shared<TestEchoServiceImpl>();
	state std::shared_ptr<GrpcServer> s = std::make_shared<GrpcServer>(addr);
	s->registerService(service);
	state Future<Void> server_future = s->run();

	state std::shared_ptr<boost::asio::thread_pool> pool = std::make_shared<boost::asio::thread_pool>(4);
	state AsyncGrpcClient<TestEchoService> client(pool, addr.toString());

	state EchoRequest request;
	state EchoResponse response;
  	request.set_message("Ping!");
	grpc::Status result = wait(client.call(&TestEchoService::Stub::Echo, request, &response));
	ASSERT(result.ok());
	std::cout << "Echo received: " << response.message() << std::endl;
	ASSERT_EQ(response.message(), "Echo: Ping!");

	s->shutdown();
	wait(server_future);

	return Void();
}

// T_EST_CASE("/fdbrpc/grpc/destroy_server_without_shutdown") {
// 	using namespace fdbrpc::test;

// 	state std::shared_ptr<TestEchoServiceImpl> service = std::make_shared<TestEchoServiceImpl>();
// 	state std::shared_ptr<GrpcServer> s = std::make_shared<GrpcServer>();
// 	s->registerService(service);
// 	state Future<Void> server_future = s->run();
// 	return Void();
// }


} // namespace fdbrpc_test