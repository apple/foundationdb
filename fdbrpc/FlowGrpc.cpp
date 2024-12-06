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
#include "fdbrpc/FlowGrpc.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#ifdef FLOW_GRPC_ENABLED

GrpcServer::GrpcServer(const NetworkAddress& addr) : address_(addr) {}

GrpcServer::~GrpcServer() {
	if (server_) {
		shutdown();
	}

	if (server_thread_.joinable()) {
		server_thread_.join();
	}
}

Future<Void> GrpcServer::run() {
	grpc::ServerBuilder builder_;
	builder_.AddListeningPort(address_.toString(), grpc::InsecureServerCredentials());
	for (auto& service : registered_services_) {
		builder_.RegisterService(service.get());
	}
	server_ = builder_.BuildAndStart();
	return server_promise_.getFuture();
}

void GrpcServer::shutdown() {
	server_->Shutdown(); // TODO (Vishesh): This needs to be Future.
	server_promise_.send(Void());
	server_ = nullptr;
}

void GrpcServer::registerService(std::shared_ptr<grpc::Service> service) {
	registered_services_.push_back(service);
}

#endif