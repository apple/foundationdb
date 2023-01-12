/*
 * admin_server.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "tenant.hpp"
#include "utils.hpp"
#include <map>
#include <cerrno>
#include <cstring> // strerror
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <unistd.h>
#include <sys/wait.h>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/variant.hpp>

#include "rapidjson/document.h"

extern thread_local mako::Logger logr;

using oarchive = boost::archive::binary_oarchive;
using iarchive = boost::archive::binary_iarchive;

namespace {

template <class T>
void sendObject(boost::process::pstream& pipe, T obj) {
	oarchive oa(pipe);
	oa << obj;
}

template <class T>
T receiveObject(boost::process::pstream& pipe) {
	iarchive ia(pipe);
	T obj;
	ia >> obj;
	return obj;
}

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

	while (true) {
		try {
			auto req = receiveObject<Request>(pipe_to_server);
			if (setup_error) {
				sendObject(pipe_to_client, Response{ setup_error });
			} else if (boost::get<PingRequest>(&req)) {
				sendObject(pipe_to_client, Response{});
			} else if (boost::get<StopRequest>(&req)) {
				logr.info("server was requested to stop");
				sendObject(pipe_to_client, Response{});
				return;
			} else if (auto p = boost::get<BatchCreateTenantRequest>(&req)) {
				logr.info("received request to batch-create tenant {}-{} in database '{}'",
				          p->id_begin,
				          p->id_end,
				          p->cluster_file);
				auto err_msg = createTenant(getOrCreateDatabase(databases, p->cluster_file), p->id_begin, p->id_end);
				sendObject(pipe_to_client, Response{ std::move(err_msg) });
			} else if (auto p = boost::get<BatchDeleteTenantRequest>(&req)) {
				logr.info("received request to batch-delete tenant {}-{} in database '{}'",
				          p->id_begin,
				          p->id_end,
				          p->cluster_file);
				auto err_msg = deleteTenant(getOrCreateDatabase(databases, p->cluster_file), p->id_begin, p->id_end);
				sendObject(pipe_to_client, Response{ std::move(err_msg) });
			} else {
				logr.info("Unknown request received");
				sendObject(pipe_to_client, Response{ std::string("unknown request type") });
			}
		} catch (const std::exception& e) {
			logr.error("fatal exception: {}", e.what());
			return;
		}
	}
}

boost::optional<std::string> AdminServer::createTenant(fdb::Database db, int id_begin, int id_end) {
	try {
		auto tx = db.createTransaction();
		logr.info("create_tenants [{}-{})", id_begin, id_end);
		while (true) {
			for (auto id = id_begin; id < id_end; id++) {
				auto tenant_name = getTenantNameByIndex(id);
				fdb::Tenant::createTenant(tx, fdb::toBytesRef(tenant_name));
			}
			auto f = tx.commit();
			const auto rc = waitAndHandleError(tx, f);
			if (rc == FutureRC::OK) {
				logr.info("create_tenant commit OK", id_begin, id_end);
				tx.reset();
				break;
			} else if (rc == FutureRC::RETRY) {
				logr.error("create_tenant retryable error: {}", f.error().what());
				continue;
			} else {
				logr.error("create_tenant unretryable error: {}", f.error().what());
				return fmt::format("create_tenant [{}:{}) failed with '{}'", id_begin, id_end, f.error().what());
			}
		}
		logr.info("create_tenants [{}-{}) OK", id_begin, id_end);
		logr.info("blobbify tenants [{}-{})", id_begin, id_end);
		for (auto id = id_begin; id < id_end; id++) {
			while (true) {
				auto tenant = db.openTenant(fdb::toBytesRef(getTenantNameByIndex(id)));
				std::string range_end = "\xff";
				auto blobbify_future = tenant.blobbifyRange(fdb::BytesRef(), fdb::toBytesRef(range_end));
				const auto rc = waitAndHandleError(tx, blobbify_future);
				if (rc == FutureRC::OK) {
					if (!blobbify_future.get()) {
						return fmt::format("failed to blobbify tenant {}", id);
					}
					break;
				} else if (rc == FutureRC::RETRY) {
					logr.info("blobbify_tenant retryable error: {}", blobbify_future.error().what());
					continue;
				} else {
					logr.error("unretryable error during blobbify: {}", blobbify_future.error().what());
					return fmt::format("critical error encountered while blobbifying tenant {}: {}",
					                   id,
					                   blobbify_future.error().what());
				}
			}
		}
		logr.info("blobbify tenants [{}-{}) OK", id_begin, id_end);
		return {};
	} catch (const std::exception& e) {
		return std::string(e.what());
	}
}

boost::optional<std::string> AdminServer::getTenantPrefixes(fdb::Transaction tx,
                                                            int id_begin,
                                                            int id_end,
                                                            std::vector<fdb::ByteString>& out_prefixes) {
	const auto num_tenants = id_end - id_begin;
	auto tenant_futures = std::vector<fdb::TypedFuture<fdb::future_var::ValueRef>>(num_tenants);
	while (true) {
		for (auto id = id_begin; id < id_end; id++) {
			tenant_futures[id - id_begin] = fdb::Tenant::getTenant(tx, fdb::toBytesRef(getTenantNameByIndex(id)));
		}
		out_prefixes.clear();
		out_prefixes.reserve(num_tenants);
		bool success = true;
		for (auto id = id_begin; id < id_end; id++) {
			auto f = tenant_futures[id - id_begin];
			const auto rc = waitAndHandleError(tx, f);
			if (rc == FutureRC::OK) {
				if (f.get().has_value()) {
					auto val = std::string(fdb::toCharsRef(f.get().value()));
					auto doc = rapidjson::Document();
					doc.Parse(val.c_str());
					if (!doc.HasParseError()) {
						// rapidjson does not decode the prefix as the same byte string that
						// was passed as input. This is because we use a non-standard encoding.
						// The encoding will likely change in the future.
						// For a workaround, we take the id and compute the prefix on our own
						auto tenant_prefix = fdb::ByteString(8, '\0');
						computeTenantPrefix(tenant_prefix, doc["id"].GetUint64());
						out_prefixes.push_back(tenant_prefix);
					}
				}
			} else if (rc == FutureRC::RETRY) {
				success = false;
				break;
			} else {
				return fmt::format("unretryable error while getting metadata for tenant {}: {}", id, f.error().what());
			}
		}
		if (success)
			return {};
	}
}

boost::optional<std::string> AdminServer::deleteTenant(fdb::Database db, int id_begin, int id_end) {
	try {
		while (true) {
			std::vector<fdb::ByteString> prefixes;
			if (auto error = getTenantPrefixes(db.createTransaction(), id_begin, id_end, prefixes)) {
				return error;
			}
			auto tx = db.createTransaction();
			tx.setOption(FDBTransactionOption::FDB_TR_OPTION_LOCK_AWARE, fdb::BytesRef());
			tx.setOption(FDBTransactionOption::FDB_TR_OPTION_RAW_ACCESS, fdb::BytesRef());
			for (const auto& tenant_prefix : prefixes) {
				tx.clearRange(tenant_prefix, fdb::strinc(tenant_prefix));
			}
			auto commit_future = tx.commit();
			auto rc = waitAndHandleError(tx, commit_future);
			if (rc == FutureRC::OK) {
				tx.reset();
				// continue on this iteration
			} else if (rc == FutureRC::RETRY) {
				logr.error("Retryable error during tenant prefix clear: {}", commit_future.error().what());
				continue;
			} else {
				return fmt::format("unretryable error while clearing key ranges for tenant [{}:{}): {}",
				                   id_begin,
				                   id_end,
				                   commit_future.error().what());
			}
			// tenant keyspaces have been cleared. now delete tenants
			for (int id = id_begin; id < id_end; id++) {
				fdb::Tenant::deleteTenant(tx, fdb::toBytesRef(getTenantNameByIndex(id)));
			}
			commit_future = tx.commit();
			rc = waitAndHandleError(tx, commit_future);
			if (rc == FutureRC::OK) {
				return {};
			} else if (rc == FutureRC::ABORT) {
				return fmt::format("unretryable error while committing delete-tenant for tenant id range [{}:{}): {}",
				                   id_begin,
				                   id_end,
				                   commit_future.error().what());
			} else {
				// try again
				logr.error("retryable error during tenant delete: {}", commit_future.error().what());
			}
		}
	} catch (const std::exception& e) {
		return std::string(e.what());
	}
}

Response AdminServer::request(Request req) {
	// should always be invoked from client side (currently just the main process)
	assert(server_pid > 0);
	assert(logr.isFor(ProcKind::MAIN));
	sendObject(pipe_to_server, std::move(req));
	return receiveObject<Response>(pipe_to_client);
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
