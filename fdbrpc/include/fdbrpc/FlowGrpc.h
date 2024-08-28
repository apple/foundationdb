/**
 * FlowGrpc.h
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

#ifndef FDBRPC_FLOW_GRPC_H
#define FDBRPC_FLOW_GRPC_H

#include <memory>
#include <thread>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>

#include "flow/IThreadPool.h"
#include "flow/flow.h"

namespace {
template <typename T>
struct get_response_type_impl;

template <typename Stub, typename Request, typename Response>
struct get_response_type_impl<grpc::Status (Stub::*)(grpc::ClientContext*, const Request&, Response*)> {
	using type = Response;
};

template <typename T>
using get_response_type = typename get_response_type_impl<T>::type;
} // namespace

class GrpcServer {
public:
	GrpcServer(const NetworkAddress& addr);
	~GrpcServer();

	Future<Void> run();
	void runSync();
	void shutdown();

	void registerService(std::shared_ptr<grpc::Service> service);
	std::vector<int> listeningPort() const;

private:
	NetworkAddress address_;
	std::unique_ptr<grpc::Server> server_;
	ThreadReturnPromise<Void> server_promise_;
	std::thread server_thread_;
	std::vector<std::shared_ptr<grpc::Service>> registered_services_;
};

template <typename ServiceType>
class AsyncGrpcClient {
	using RpcStub = typename ServiceType::Stub;

	template <class RequestType, class ResponseType>
	using RpcFn = grpc::Status (RpcStub::*)(grpc::ClientContext*, const RequestType&, ResponseType*);

public:
	using Rpc = typename ServiceType::Stub;

	AsyncGrpcClient() {}
	AsyncGrpcClient(const std::string& endpoint, std::shared_ptr<boost::asio::thread_pool> pool)
	  : pool_(pool),
	    channel_(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials())),
	    stub_(ServiceType::NewStub(channel_)) {}

	template <class RequestType, class ResponseType, class RpcFn = RpcFn<RequestType, ResponseType>>
	Future<grpc::Status> call(RpcFn rpc, const RequestType& request, ResponseType* response) {
		auto promise = std::make_shared<ThreadReturnPromise<grpc::Status>>();

		boost::asio::post(*pool_, [this, promise, rpc, request, response]() {
			grpc::ClientContext context;
			auto status = (stub_.get()->*rpc)(&context, request, response);
			promise->send(status);
		});

		return promise->getFuture();
	}

	template <class RequestType, class RpcFn, class ResponseType = get_response_type<RpcFn>>
	Future<ResponseType> call(RpcFn rpc, const RequestType& request) {
		auto promise = std::make_shared<ThreadReturnPromise<ResponseType>>();

		boost::asio::post(*pool_, [this, promise, rpc, request]() {
			grpc::ClientContext context;
			ResponseType response;
			auto status = (stub_.get()->*rpc)(&context, request, &response);
			if (status.ok()) {
				promise->send(response);
			} else {
				promise->sendError(grpc_error()); // TODO (Vishesh): Propogate the gRPC error codes.
			}
		});

		return promise->getFuture();
	}

private:
	std::shared_ptr<boost::asio::thread_pool> pool_;
	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<RpcStub> stub_;
};

#endif // FDBRPC_FLOW_GRPC_H
