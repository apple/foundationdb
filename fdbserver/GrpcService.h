/*
 * GrpcService.h 
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_GRPCSERVICE_H
#define FDBSERVER_GRPCSERVICE_H
#pragma once

#include "fdbserver/GrpcService.h"
#include "fdbserver/IRpcInterface.h"

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"

#include "fdbserver/kv_service.grpc.pb.h"
#include "fdbserver/kv_service.pb.h"

#include <grpcpp/grpcpp.h>

class GrpcService : public IRpcService, ReferenceCounted<GrpcService> {
public:
	virtual Future<Void> run(NetworkAddress address, RpcInterface const& rpcInterface);
	virtual void shutdown();

	virtual void addref() { ReferenceCounted<GrpcService>::addref(); }
	virtual void delref() { ReferenceCounted<GrpcService>::delref(); }

	void handleRequests();

private:
	void doShutdown(Optional<Error> e);

	RpcInterface rpcInterface;

	std::unique_ptr<grpc::ServerCompletionQueue> cq;
	foundationdb::FdbKvClient::AsyncService asyncGrpcService;
	std::unique_ptr<grpc::Server> server;

	std::atomic<bool> isShutdown = false;
	Promise<Void> shutdownPromise;
};

#endif
