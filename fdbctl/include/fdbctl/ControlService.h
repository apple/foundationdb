/*
 * ControlService.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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
#ifndef FDBCTL_CLI_COMMAND_SERVER_H
#define FDBCTL_CLI_COMMAND_SERVER_H

#include <grpcpp/support/status.h>

#include "fdbctl/ControlCommands.h"
#include "fdbclient/IClientApi.h"

#define DEFINE_GRPC_HANDLER(rpcName, handlerName)                                                                      \
	grpc::Status rpcName(grpc::ServerContext* context, const rpcName##Request* req, rpcName##Reply* rep) override {    \
		return handleRequestOnMainThread(&handlerName, req, rep, context);                                             \
	}

namespace fdbctl {
class ControlServiceImpl final : public fdbctl::ControlService::Service {
public:
	ControlServiceImpl(Reference<IDatabase> db);

	DEFINE_GRPC_HANDLER(GetCoordinators, getCoordinators);
	DEFINE_GRPC_HANDLER(ChangeCoordinators, changeCoordinators);
	// DEFINE_GRPC_HANDLER(Configure, configure);
	DEFINE_GRPC_HANDLER(GetStatus, getStatus);
	DEFINE_GRPC_HANDLER(GetWorkers, getWorkers);
	DEFINE_GRPC_HANDLER(Include, include);
	// DEFINE_GRPC_HANDLER(Exclude, exclude);
	// DEFINE_GRPC_HANDLER(ExcludeStatus, excludeStatus);
	DEFINE_GRPC_HANDLER(Kill, kill);

private:
	// Bridges flow with gRPC handlers. The RPC handlers are defined using `DEFINE_GRPC_HANDLER`
	// which uses this method to invoke the actual handler written in Flow and runs it on the main thread.
	template <class Handler, class Request, class Reply>
	grpc::Status handleRequestOnMainThread(Handler* h, const Request* req, Reply* rep, grpc::ServerContext* context);

private:
	Reference<IDatabase> db_;
};
} // namespace fdbctl

#endif // FDBCTL_CLI_COMMAND_SERVER_H
#endif // FLOW_GRPC_ENABLED
