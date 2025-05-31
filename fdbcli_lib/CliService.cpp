/*
 * CliService.cpp
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

#include "fdbcli_lib/CliService.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/ApiVersion.h"
#include "flow/ThreadHelper.actor.h"
#include <fmt/format.h>
#include <functional>
#include <grpcpp/support/status.h>
#include <thread>
#include <chrono>

namespace fdbcli_lib {

using namespace std::chrono_literals;

CliServiceImpl::CliServiceImpl(Reference<IDatabase> db) : Service(), db_(db) {}

template <class Handler, class Request, class Reply>
Future<grpc::Status> grpcHandlerWrapper(Reference<IDatabase> db, Handler* h, const Request* req, Reply* rep) {
	try {
		co_return co_await (*h)(db, req, rep);
	} catch (Error& e) {
		co_return grpc::Status(grpc::StatusCode::INTERNAL, fmt::format("ERROR: Unknown error {}", e.name()));
	}
}

template <class Handler, class Request, class Reply>
grpc::Status CliServiceImpl::handleRequestOnMainThread(Handler* h, const Request* req, Reply* rep) {
	return onMainThread([=]() { return grpcHandlerWrapper(db_, h, req, rep); }).getBlocking();
}

} // namespace fdbcli_lib
