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

#include "echo.pb.h"
#include "flow/UnitTest.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/test/echo.grpc.pb.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// So that tests are not optimized out. :/
void forceLinkGrpcTests() {}

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

using fdbrpc::test::TestEchoService;
using fdbrpc::test::EchoRequest;
using fdbrpc::test::EchoResponse;

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
	state std::shared_ptr<TestEchoServiceImpl> service = std::make_shared<TestEchoServiceImpl>();
	state GRPCServer s;
	s.registerService(service);
	auto server = std::thread([&] { s.runSync(); });

	EchoClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
	std::string message = "Ping!";
	std::string reply = client.Echo(message);
	std::cout << "Echo received: " << reply << std::endl;
	ASSERT_EQ(reply, "Echo: Ping!");

	s.shutdown();
	server.join();
	return Void();
}

TEST_CASE("/fdbrpc/grpc/basic_async") {
	state std::shared_ptr<TestEchoServiceImpl> service = std::make_shared<TestEchoServiceImpl>();
	state GRPCServer s;
	s.registerService(service);
	state Future<Void> r = s.run();

	EchoClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
	std::string message = "Ping!";
	std::string reply = client.Echo(message);
	std::cout << "Echo received: " << reply << std::endl;
	ASSERT_EQ(reply, "Echo: Ping!");

	s.shutdown();
	wait(r);
	return Void();
}