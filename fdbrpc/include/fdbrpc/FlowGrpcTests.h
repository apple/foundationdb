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
#include <thread>
#include "flow/Error.h"
#include "fdbrpc/test/echo.grpc.pb.h"

namespace fdbrpc_test {

using std::make_shared;
using std::shared_ptr;
using std::thread;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
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

	Status EchoRecvStream10(ServerContext* context,
	                        const EchoRequest* request,
	                        ServerWriter<EchoResponse>* writer) override {
		for (int ii = 0; ii < 10; ii++) {
			if (context->IsCancelled()) {
				std::cout << "Request Cancelled.\n";
				return Status::CANCELLED;
			}
			EchoResponse reply;
			reply.set_message("Echo: " + request->message());
			writer->Write(reply);
		}
		return Status::OK;
	}

	Status EchoSendStream10(ServerContext* context, ServerReader<EchoRequest>* reader, EchoResponse* reply) override {
		EchoRequest request;
		std::string res;
		int count = 0;
		while (reader->Read(&request)) {
			count++;
			res += request.message();
		}
		reply->set_message(res);
		ASSERT_EQ(count, 10);
		return Status::OK;
	}
};

class EchoClient {
public:
	EchoClient(const shared_ptr<Channel>& channel) : stub_(TestEchoService::NewStub(channel)) {}

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

} // namespace fdbrpc_test

#endif