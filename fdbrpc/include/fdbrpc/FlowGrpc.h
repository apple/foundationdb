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
#ifdef FLOW_GRPC_ENABLED
#ifndef FDBRPC_FLOW_GRPC_H
#define FDBRPC_FLOW_GRPC_H

#include <memory>
#include <thread>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>

#include "flow/IThreadPool.h"
#include "flow/flow.h"

class GrpcServer {
public:
	GrpcServer(const NetworkAddress& addr);
	~GrpcServer();

	Future<Void> run();
	void shutdown();
	void registerService(std::shared_ptr<grpc::Service> service);

	static GrpcServer* initInstance(const NetworkAddress& addr) {
		GrpcServer* server = new GrpcServer(addr);
		g_network->setGlobal(INetwork::enGrpcServer, (flowGlobalType)server);
		return server;
	}

	static GrpcServer* instance() { return static_cast<GrpcServer*>((void*)g_network->global(INetwork::enGrpcServer)); }

private:
	NetworkAddress address_;
	std::unique_ptr<grpc::Server> server_;
	ThreadReturnPromise<Void> server_promise_;
	std::thread server_thread_;
	std::vector<std::shared_ptr<grpc::Service>> registered_services_;
};

template <class ServiceType>
class AsyncGrpcClient {
	template <class Request, class Response>
	using UnaryRpcFn = grpc::Status (ServiceType::Stub::*)(grpc::ClientContext*, const Request&, Response*);

	template <class Request, class Response>
	using ServerStreamingRpcFn =
	    std::unique_ptr<grpc::ClientReader<Response>> (ServiceType::Stub::*)(grpc::ClientContext*, const Request&);

	template <class Request, class Response>
	using ClientStreamingRpcFn =
	    std::unique_ptr<grpc::ClientWriter<Request>> (ServiceType::Stub::*)(grpc::ClientContext*, Response*);

public:
	using Rpc = typename ServiceType::Stub;

	AsyncGrpcClient() {}
	AsyncGrpcClient(const std::string& endpoint, std::shared_ptr<boost::asio::thread_pool> pool)
	  : pool_(pool), channel_(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials())),
	    stub_(ServiceType::NewStub(channel_)) {}

	// NOTE: Must be called from network thread. This is because the underlying primitive used
	//   is ThreadReturnPromise.
	template <class RequestType, class ResponseType>
	Future<grpc::Status> call(UnaryRpcFn<RequestType, ResponseType> rpc,
	                          const RequestType& request,
	                          ResponseType* response) {
		ASSERT_WE_THINK(g_network->isOnMainThread());
		auto promise = std::make_shared<ThreadReturnPromise<grpc::Status>>();

		boost::asio::post(*pool_, [this, promise, rpc, request, response]() {
			grpc::ClientContext context;
			auto status = (stub_.get()->*rpc)(&context, request, response);
			if (promise->getFutureReferenceCount() == 0) {
				return;
			}
			promise->send(status);
		});

		return promise->getFuture();
	}

	// NOTE: Must be called from network thread. This is because the underlying primitive used
	//   is ThreadReturnPromise.
	template <class RequestType, class ResponseType>
	Future<ResponseType> call(UnaryRpcFn<RequestType, ResponseType> rpc, const RequestType& request) {
		ASSERT_WE_THINK(g_network->isOnMainThread());
		auto promise = std::make_shared<ThreadReturnPromise<ResponseType>>();

		boost::asio::post(*pool_, [this, promise, rpc, request]() {
			if (promise->getFutureReferenceCount() == 0) {
				return;
			}

			grpc::ClientContext context;
			ResponseType response;
			auto status = (stub_.get()->*rpc)(&context, request, &response);

			if (promise->getFutureReferenceCount() == 0) {
				return;
			}

			if (status.ok()) {
				promise->send(response);
			} else {
				std::cout << "Error: " << status.error_message() << std::endl;
				promise->sendError(grpc_error()); // TODO (Vishesh): Propogate the gRPC error codes.
			}
		});

		return promise->getFuture();
	}
	// NOTE: Must be called from network thread. This is because the underlying primitive used
	//   is ThreadReturnPromise.
	template <class RequestType, class ResponseType>
	FutureStream<ResponseType> call(ServerStreamingRpcFn<RequestType, ResponseType> rpc, const RequestType& request) {
		ASSERT_WE_THINK(g_network->isOnMainThread());
		auto promise = std::make_shared<ThreadReturnPromiseStream<ResponseType>>();

		boost::asio::post(*pool_, [this, promise, rpc, request]() {
			grpc::ClientContext context;
			ResponseType response;
			auto reader = (stub_.get()->*rpc)(&context, request);
			while (reader->Read(&response)) {
				if (promise->getFutureReferenceCount() == 0) {
					std::cout << "Stream cancelled.\n";
					context.TryCancel();
					return;
				}

				promise->send(response);
			}

			auto status = reader->Finish();
			if (status.ok()) {
				promise->sendError(end_of_stream());
			} else {
				std::cout << "Error: " << status.error_message() << std::endl;
				promise->sendError(grpc_error()); // TODO (Vishesh): Propogate the gRPC error codes.
			}
		});

		return promise->getFuture();
	}

	// template <class RequestType, class ResponseType>
	// void call(ClientStreamingRpcFn<RequestType, ResponseType> rpc, const RequestType& request) {
	// 	auto promise = std::make_shared<ThreadReturnPromiseStream<ResponseType>>();

	// 	boost::asio::post(*pool_, [this, promise, rpc, request]() {
	// 		grpc::ClientContext context;
	// 		ResponseType response;
	// 		auto reader = (stub_.get()->*rpc)(&context, request);
	// 		while (reader->Read(&response)) {
	// 			promise->send(response);
	// 		}

	// 		auto status = reader->Finish();
	// 		if (status.ok()) {
	// 			promise->sendError(end_of_stream());
	// 		} else {
	// 			promise->sendError(grpc_error()); // TODO (Vishesh): Propogate the gRPC error codes.
	// 		}
	// 	});

	// 	return promise->getFuture();
	// }

private:
	std::shared_ptr<boost::asio::thread_pool> pool_;
	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<typename ServiceType::Stub> stub_;
};

#endif // FDBRPC_FLOW_GRPC_H
#endif // FLOW_GRPC_ENABLED