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
#include <type_traits>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>

#include "flow/IThreadPool.h"
#include "flow/flow.h"

namespace {
template <class T>
struct get_response_type_impl;

template <typename Stub, typename Request, typename Response>
struct get_response_type_impl<grpc::Status (Stub::*)(grpc::ClientContext*, const Request&, Response*)> {
	using type = Response;
};

template <typename Stub, typename Request, typename Response>
struct get_response_type_impl<std::unique_ptr<grpc::ClientReader<Response>> (Stub::*)(grpc::ClientContext*, const Request&)> {
	using type = Response;
};

template <typename T>
using get_response_type = typename get_response_type_impl<T>::type;

enum RpcMode {
	Unary,
	ServerStreaming,
	ClientStreaming,
};

template <class ServiceType, class RequestType, class ResponseType, RpcMode Mode>
struct _RpcFnHelper {
	using type =
	    std::conditional_t<Mode == RpcMode::Unary,
	                       grpc::Status (ServiceType::Stub::*)(grpc::ClientContext*, const RequestType&, ResponseType*),
	                       std::conditional_t<Mode == RpcMode::ServerStreaming,
	                                          std::unique_ptr<grpc::ClientReader<ResponseType>> (
	                                              ServiceType::Stub::*)(grpc::ClientContext*, const RequestType&),
	                                          void // Fallback if other modes are added but not handled
	                                          >>;

	static_assert(Mode == RpcMode::Unary || Mode == RpcMode::ServerStreaming,
	              "Unsupported RpcMode. Please provide a valid RpcMode.");
};

// Define the type alias using the helper struct
template <class ServiceType, class RequestType, class ResponseType, RpcMode Mode>
using _RpcFn = typename _RpcFnHelper<ServiceType, RequestType, ResponseType, Mode>::type;

} // namespace

class GrpcServer {
public:
	GrpcServer(const NetworkAddress& addr);
	~GrpcServer();

	Future<Void> run();
	void shutdown();

	void registerService(std::shared_ptr<grpc::Service> service);

private:
	NetworkAddress address_;
	std::unique_ptr<grpc::Server> server_;
	ThreadReturnPromise<Void> server_promise_;
	std::thread server_thread_;
	std::vector<std::shared_ptr<grpc::Service>> registered_services_;
};

template <typename ServiceType>
class AsyncGrpcClient {
public:
	using Rpc = typename ServiceType::Stub;

	AsyncGrpcClient() {}
	AsyncGrpcClient(const std::string& endpoint, std::shared_ptr<boost::asio::thread_pool> pool)
	  : pool_(pool), channel_(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials())),
	    stub_(ServiceType::NewStub(channel_)) {}

	template <class RpcFn, class RequestType, class ResponseType = get_response_type<RpcFn>>
	requires std::is_same_v<ResponseType, get_response_type<RpcFn>> &&
		     std::is_same_v<RpcFn, _RpcFn<ServiceType, RequestType, ResponseType, RpcMode::Unary>>
	Future<grpc::Status> call(RpcFn rpc, const RequestType& request, ResponseType* response) {
		auto promise = std::make_shared<ThreadReturnPromise<grpc::Status>>();

		boost::asio::post(*pool_, [this, promise, rpc, request, response]() {
			grpc::ClientContext context;
			auto status = (stub_.get()->*rpc)(&context, request, response);
			promise->send(status);
		});

		return promise->getFuture();
	}

	// Returned future can be waited safely only from the originating thread.
	template <class RpcFn, class RequestType, class ResponseType = get_response_type<RpcFn>>
		requires std::is_same_v<ResponseType, get_response_type<RpcFn>> &&
		         std::is_same_v<RpcFn, _RpcFn<ServiceType, RequestType, ResponseType, RpcMode::Unary>>
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

	template <class RpcFn, class RequestType, class ResponseType = get_response_type<RpcFn>>
	    requires std::is_same_v<ResponseType, get_response_type<RpcFn>> &&
	             std::is_same_v<RpcFn, _RpcFn<ServiceType, RequestType, ResponseType, RpcMode::ServerStreaming>>
	FutureStream<ResponseType> call(RpcFn rpc, const RequestType& request) {
		auto promise = std::make_shared<ThreadReturnPromiseStream<ResponseType>>();

		boost::asio::post(*pool_, [this, promise, rpc, request]() {
			grpc::ClientContext context;
			ResponseType response;
			auto reader = (stub_.get()->*rpc)(&context, request);
			while (reader->Read(&response)) {
				promise->send(response);
			}

			auto status = reader->Finish();
			if (status.ok()) {
				promise->sendError(end_of_stream());
			} else {
				promise->sendError(grpc_error()); // TODO (Vishesh): Propogate the gRPC error codes.
			}
		});

		return promise->getFuture();
	}

private:
	std::shared_ptr<boost::asio::thread_pool> pool_;
	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<typename ServiceType::Stub> stub_;
};

#endif // FDBRPC_FLOW_GRPC_H
