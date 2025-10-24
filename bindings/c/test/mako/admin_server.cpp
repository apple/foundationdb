/*
 * admin_server.cpp
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

#include "admin_server.hpp"
#include "future.hpp"
#include "logger.hpp"
#include "time.hpp"
#include "utils.hpp"
#include <map>
#include <cerrno>
#include <cstring> // strerror
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <tuple>

#include <unistd.h>
#include <sys/wait.h>

#include <boost/variant/apply_visitor.hpp>

#include "rapidjson/document.h"

extern thread_local mako::Logger logr;

namespace {

fdb::Database getOrCreateDatabase(std::map<std::string, fdb::Database>& db_map, const std::string& cluster_file) {
	auto iter = db_map.find(cluster_file);
	if (iter == db_map.end()) {
		auto [inserted_iter, _] = db_map.insert({ cluster_file, fdb::Database(cluster_file) });
		return inserted_iter->second;
	} else {
		return iter->second;
	}
}

} // anonymous namespace

namespace mako::ipc {

void AdminServer::start() {
	assert(server_pid == -1);
	assert(logr.isFor(ProcKind::MAIN));
	logr.info("forking admin server process");
	auto pid = fork();
	if (pid == 0) {
		// Update thread-local logger to modify log line header
		logr = Logger(AdminProcess{}, args.verbose);
	} else if (pid > 0) {
		server_pid = pid;
		return;
	} else {
		logr.error("failed to fork admin server process: {}", std::strerror(errno));
		throw std::runtime_error("fork error");
	}
	assert(logr.isFor(ProcKind::ADMIN));
	logr.info("starting admin server process");
	boost::optional<std::string> setup_error;
	if (args.setGlobalOptions() < 0) {
		setup_error = std::string("global option setup failed");
	} else if (auto err = fdb::network::setupNothrow()) {
		setup_error = std::string(err.what());
	}
	std::map<std::string, fdb::Database> databases;
	std::optional<std::thread> network_thread;
	if (!setup_error) {
		network_thread.emplace([parent_logr = logr]() {
			logr = parent_logr;
			logr.debug("network thread started");
			if (auto err = fdb::network::run()) {
				logr.error("fdb::network::run(): {}", err.what());
			}
		});
	}
	auto network_thread_guard = ExitGuard([&network_thread]() {
		if (network_thread.has_value()) {
			logr.debug("fdb::network::stop()");
			if (auto err = fdb::network::stop()) {
				logr.error("network::stop(): {}", err.what());
			}
			logr.debug("waiting for network thread to join");
			network_thread.value().join();
		}
	});

	bool stop = false;
	while (!stop) {
		try {
			auto req = receiveObject<Request>(pipe_to_server);
			boost::apply_visitor(
			    [this, &databases, &setup_error, &stop](auto&& request) -> void {
				    using ReqType = std::decay_t<decltype(request)>;
				    if (setup_error) {
					    sendResponse<ReqType>(pipe_to_client, ReqType::ResponseType::makeError(*setup_error));
					    return;
				    }
				    if constexpr (std::is_same_v<ReqType, PingRequest>) {
					    sendResponse<ReqType>(pipe_to_client, DefaultResponse{});
				    } else if constexpr (std::is_same_v<ReqType, StopRequest>) {
					    logr.info("server was requested to stop");
					    sendResponse<ReqType>(pipe_to_client, DefaultResponse{});
					    stop = true;
				    } else {
					    logr.error("unknown request received, typename '{}'", typeid(ReqType).name());
					    sendResponse<ReqType>(pipe_to_client, ReqType::ResponseType::makeError("unknown request type"));
				    }
			    },
			    req);
		} catch (const std::exception& e) {
			logr.error("fatal exception: {}", e.what());
			return;
		}
	}
}

AdminServer::~AdminServer() {
	if (server_pid > 0) {
		// may only be called from main process
		assert(logr.isFor(ProcKind::MAIN));
		auto res = send(ipc::StopRequest{});
		if (res.error_message) {
			logr.error("failed to stop admin server: {}", *res.error_message);
		}
		logr.info("waiting for admin server to terminate");
		int status = 0;
		::waitpid(server_pid, &status, 0);
		if (status) {
			logr.warn("admin server existed with error code {}", status);
		}
	}
}

} // namespace mako::ipc
