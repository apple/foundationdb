/*
 * gRPC.h
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

#ifndef FDBRPC_GRPC_H
#define FDBRPC_GRPC_H

#include <grpcpp/server.h>
#include <memory>
#include <thread>

#include <grpc++/grpc++.h>
#include <vector>

#include "flow/IConnection.h"
#include "flow/IThreadPool.h"
#include "flow/NetworkAddress.h"
#include "flow/flow.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/fdbrpc.grpc.pb.h"

class GRPCServer {
public:
	~GRPCServer();

	Future<Void> run();
	void runSync();
	void shutdown();
	void registerService(std::shared_ptr<grpc::Service> service);

private:
	grpc::ServerBuilder builder_;
	std::unique_ptr<grpc::Server> server_;
	std::thread server_thread_;
	ThreadReturnPromise<Void> server_promise_;
	std::vector<std::shared_ptr<grpc::Service>> registered_services_;
};

#endif //
