/*
 * ControlService.cpp
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
#include <fmt/format.h>
#include <chrono>

#include "fdbctl/ControlService.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbclient/Knobs.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/genericactors.actor.h"

namespace fdbctl {

using namespace std::chrono_literals;

ControlServiceImpl::ControlServiceImpl(Reference<IDatabase> db) : Service(), db_(db) {}

template <class Handler, class Request, class Reply>
Future<grpc::Status> grpcHandlerWrapper(Reference<IDatabase> db,
                                        Handler* h,
                                        const Request* req,
                                        Reply* rep,
                                        grpc::ServerContext* context) {
	try {
		double timeout = CLIENT_KNOBS->GRPC_CTL_SERVICE_DEFAULT_TIMEOUT; // Default timeout when no deadline is set
		auto deadline = context->deadline();
		auto now = std::chrono::system_clock::now();

		// TODO: Should use FDBs now(), but in this world we are breaking determinism anyway at this
		//   point.
		if (deadline != std::chrono::system_clock::time_point::max()) {
			auto grpc_timeout = std::chrono::duration_cast<std::chrono::seconds>(deadline - now).count();
			timeout = static_cast<double>(grpc_timeout);
		}

		if (timeout <= 0) {
			co_return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Request deadline already exceeded");
		}

		co_return co_await timeoutError((*h)(db, req, rep), timeout);
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			co_return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Operation timed out");
		}

		co_return grpc::Status(grpc::StatusCode::INTERNAL, fmt::format("Unknown error '{}'", e.name()));
	}
}

template <class Handler, class Request, class Reply>
grpc::Status ControlServiceImpl::handleRequestOnMainThread(Handler* h,
                                                           const Request* req,
                                                           Reply* rep,
                                                           grpc::ServerContext* context) {
	return onMainThread([=]() { return grpcHandlerWrapper(db_, h, req, rep, context); }).getBlocking();
}

} // namespace fdbctl
#endif // FLOW_GRPC_ENABLED
