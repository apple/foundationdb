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

#include <grpc/support/time.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>

#include "fdbctl/ControlCommands.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbrpc/simulator.h"
#include "flow/ThreadHelper.h"
#include "flow/genericactors.h"

#define DEFINE_GRPC_HANDLER(rpcName, handlerName)                                                                      \
	grpc::ServerUnaryReactor* rpcName(                                                                                 \
	    grpc::CallbackServerContext* context, const rpcName##Request* req, rpcName##Reply* rep) override {             \
		return handleRequestOnMainThread(&handlerName, req, rep, context);                                             \
	}

namespace fdbctl {

template <class Handler, class Request, class Reply>
Future<grpc::Status> grpcHandlerWrapper(Reference<IDatabase> db,
                                        Handler* h,
                                        const Request* req,
                                        Reply* rep,
                                        grpc::CallbackServerContext* context) {
	try {
		double timeout = CLIENT_KNOBS->GRPC_CTL_SERVICE_DEFAULT_TIMEOUT; // Default timeout when no deadline is set
		// gpr_now(), unlike the system clock, follows simulated time.
		gpr_timespec deadline = gpr_convert_clock_type(context->raw_deadline(), GPR_CLOCK_MONOTONIC);
		if (gpr_time_cmp(deadline, gpr_inf_future(GPR_CLOCK_MONOTONIC)) != 0) {
			timeout = gpr_timespec_to_micros(gpr_time_sub(deadline, gpr_now(GPR_CLOCK_MONOTONIC))) / 1e6;
		}

		if (timeout <= 0) {
			co_return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Request deadline already exceeded");
		}

		co_return co_await timeoutError((*h)(db, req, rep), timeout);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		if (e.code() == error_code_timed_out) {
			co_return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Operation timed out");
		}

		co_return grpc::Status(grpc::StatusCode::INTERNAL, fmt::format("Unknown error '{}'", e.name()));
	}
}

// The reactor must be finished even if nothing still holds the returned future. Under simulation gRPC invokes
// handlers on behalf of whichever simulated process its event engine is serving, so the Flow work first moves to
// `process`, the one hosting the service.
inline Future<Void> finishReactor(ISimulator::ProcessInfo* process,
                                  grpc::ServerUnaryReactor* reactor,
                                  std::function<Future<grpc::Status>()> handle,
                                  Uncancellable) {
	if (process) {
		co_await g_simulator->onProcess(process);
	}
	reactor->Finish(co_await handle());
}

class ControlServiceImpl final : public fdbctl::ControlService::CallbackService {
public:
	explicit ControlServiceImpl(Reference<IDatabase> db);

	DEFINE_GRPC_HANDLER(GetCoordinators, getCoordinators);
	DEFINE_GRPC_HANDLER(ChangeCoordinators, changeCoordinators);
	// DEFINE_GRPC_HANDLER(Configure, configure);
	DEFINE_GRPC_HANDLER(GetStatus, getStatus);
	DEFINE_GRPC_HANDLER(GetWorkers, getWorkers);
	DEFINE_GRPC_HANDLER(Include, include);
	DEFINE_GRPC_HANDLER(Exclude, exclude);
	DEFINE_GRPC_HANDLER(ExcludeStatus, excludeStatus);
	DEFINE_GRPC_HANDLER(Kill, kill);

private:
	// Bridges flow with gRPC handlers. The RPC handlers are defined using `DEFINE_GRPC_HANDLER`
	// which uses this method to invoke the actual handler written in Flow and runs it on the main thread.
	// Callback handlers must not block: under simulation they run on the main thread itself.
	template <class Handler, class Request, class Reply>
	grpc::ServerUnaryReactor* handleRequestOnMainThread(Handler* h,
	                                                    const Request* req,
	                                                    Reply* rep,
	                                                    grpc::CallbackServerContext* context) {
		grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
		Reference<IDatabase> db = db_;
		auto handle = [=]() { return grpcHandlerWrapper(db, h, req, rep, context); };
		if (process_) {
			finishReactor(process_, reactor, handle, Uncancellable{});
		} else {
			onMainThreadVoid([=]() { finishReactor(nullptr, reactor, handle, Uncancellable{}); });
		}
		return reactor;
	}

private:
	Reference<IDatabase> db_;
	// The simulated process hosting this service; null outside simulation.
	ISimulator::ProcessInfo* process_;
};
} // namespace fdbctl

#endif // FDBCTL_CLI_COMMAND_SERVER_H
#endif // FLOW_GRPC_ENABLED
