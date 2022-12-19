/*
 * admin_server.hpp
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

#include <string>
#include <utility>
#include <boost/process/pipe.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <unistd.h>
#include "fdb_api.hpp"
#include "logger.hpp"
#include "mako.hpp"

extern thread_local mako::Logger logr;

// IPC mechanism for executing administrative tasks (e.g. create/delete tenant) in support of benchmark.
// This is necessary because currently TLS configuration is a process-global setting.
// Therefore, order to benchmark for authorization
namespace mako::ipc {

struct Response {
	boost::optional<std::string> error_message;

	template <class Ar>
	void serialize(Ar& ar, unsigned int) {
		ar& error_message;
	}
};

struct BatchCreateTenantRequest {
	std::string cluster_file;
	int id_begin = 0;
	int id_end = 0;

	template <class Ar>
	void serialize(Ar& ar, unsigned int) {
		ar& cluster_file;
		ar& id_begin;
		ar& id_end;
	}
};

struct BatchDeleteTenantRequest {
	std::string cluster_file;
	int id_begin = 0;
	int id_end = 0;

	template <class Ar>
	void serialize(Ar& ar, unsigned int) {
		ar& id_begin;
		ar& id_end;
	}
};

struct PingRequest {
	template <class Ar>
	void serialize(Ar&, unsigned int) {}
};

struct StopRequest {
	template <class Ar>
	void serialize(Ar&, unsigned int) {}
};

using Request = boost::variant<PingRequest, StopRequest, BatchCreateTenantRequest, BatchDeleteTenantRequest>;

class AdminServer {
	const Arguments& args;
	pid_t server_pid;
	boost::process::pstream pipe_to_server;
	boost::process::pstream pipe_to_client;
	void start();
	void configure();
	Response request(Request req);
	boost::optional<std::string> getTenantPrefixes(fdb::Transaction tx,
	                                               int id_begin,
	                                               int id_end,
	                                               std::vector<fdb::ByteString>& out_prefixes);
	boost::optional<std::string> createTenant(fdb::Database db, int id_begin, int id_end);
	boost::optional<std::string> deleteTenant(fdb::Database db, int id_begin, int id_end);

public:
	AdminServer(const Arguments& args)
	  : args(args), server_pid(-1), pipe_to_server(boost::process::pipe()), pipe_to_client(boost::process::pipe()) {
		start();
	}
	~AdminServer();

	// forks a server subprocess internally

	bool isClient() const noexcept { return server_pid > 0; }
	template <class T>
	Response send(T req) {
		return request(Request(std::forward<T>(req)));
	}

	AdminServer(const AdminServer&) = delete;
	AdminServer(AdminServer&&) = delete;
	AdminServer& operator=(const AdminServer&) = delete;
	AdminServer& operator=(AdminServer&&) = delete;
};

} // namespace mako::ipc
