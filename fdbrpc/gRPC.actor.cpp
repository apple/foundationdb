/**
 * gRPC.actor.cpp
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
#include "fdbrpc/gRPC.h"
#include "fdbrpc/test/echo.grpc.pb.h"

#include "flow/actorcompiler.h" // This must be the last #include.

using test_fdbrpc::EchoRequest;
using test_fdbrpc::EchoResponse;
using test_fdbrpc::TestEchoService;
using grpc::ServerContext;
using grpc::Status;

GRPCServer::~GRPCServer() {
	server_promise_.sendError(unknown_error());
}

Future<Void> GRPCServer::run() {
	server_thread_ = std::thread([&] {
		runSync();
		server_promise_.send(Void());
	});

	return server_promise_.getFuture();
}

void GRPCServer::runSync() {
	auto cq = builder_.AddCompletionQueue();
	for (auto& service : registered_services_) {
		builder_.RegisterService(service.get());
	}
	builder_.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
	server_ = builder_.BuildAndStart();
	server_->Wait();
}

void GRPCServer::shutdown() {
	server_->Shutdown();
}

void GRPCServer::registerService(std::shared_ptr<grpc::Service> service) {
	registered_services_.push_back(service);
}

//-- Tests --

// So that tests are not optimized out. :/
void forceLinkGrpcTests() {}

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

// Service implementation
class FDBRpcServiceImpl final : public TestEchoService::Service {
	Status Echo(ServerContext* context, const EchoRequest* request, EchoResponse* reply) override {
		reply->set_message(request->message());
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

TEST_CASE("/fdbrpc/grpc/basic") {
	state std::shared_ptr<FDBRpcServiceImpl> service = std::make_shared<FDBRpcServiceImpl>();
	state GRPCServer s;
	//s.registerService(service);
	auto server = std::thread([&] { s.runSync(); });

	EchoClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
	std::string message = "Hello, gRPC!";
	std::string reply = client.Echo(message);
	std::cout << "Echo received: " << reply << std::endl;

	s.shutdown();
	server.join();
	return Void();
}