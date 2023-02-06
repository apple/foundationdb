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
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/variant.hpp>
#include <boost/serialization/vector.hpp>

#include <unistd.h>
#include "fdb_api.hpp"
#include "logger.hpp"
#include "mako.hpp"

extern thread_local mako::Logger logr;

// IPC mechanism for executing administrative tasks (e.g. create/delete tenant) in support of benchmark.
// This is necessary because currently TLS configuration is a process-global setting.
// Therefore, order to benchmark for authorization
namespace mako::ipc {

struct DefaultResponse {
	boost::optional<std::string> error_message;

	static DefaultResponse makeError(std::string msg) { return DefaultResponse{ msg }; }

	template <class Ar>
	void serialize(Ar& ar, unsigned int) {
		ar& error_message;
	}
};

struct TenantIdsResponse {
	boost::optional<std::string> error_message;
	std::vector<int64_t> ids;

	static TenantIdsResponse makeError(std::string msg) { return TenantIdsResponse{ msg }; }

	template <class Ar>
	void serialize(Ar& ar, unsigned int) {
		ar& error_message;
		ar& ids;
	}
};

struct BatchCreateTenantRequest {
	using ResponseType = DefaultResponse;
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
	using ResponseType = DefaultResponse;
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

struct FetchTenantIdsRequest {
	using ResponseType = TenantIdsResponse;
	std::string cluster_file;
	int id_begin;
	int id_end;

	template <class Ar>
	void serialize(Ar& ar, unsigned int) {
		ar& cluster_file;
		ar& id_begin;
		ar& id_end;
	}
};

struct PingRequest {
	using ResponseType = DefaultResponse;
	template <class Ar>
	void serialize(Ar&, unsigned int) {}
};

struct StopRequest {
	using ResponseType = DefaultResponse;
	template <class Ar>
	void serialize(Ar&, unsigned int) {}
};

using Request =
    boost::variant<PingRequest, StopRequest, BatchCreateTenantRequest, BatchDeleteTenantRequest, FetchTenantIdsRequest>;

class AdminServer {
	const Arguments& args;
	pid_t server_pid;
	boost::process::pstream pipe_to_server;
	boost::process::pstream pipe_to_client;
	void start();
	void configure();
	boost::optional<std::string> getTenantPrefixes(fdb::Transaction tx,
	                                               int id_begin,
	                                               int id_end,
	                                               std::vector<fdb::ByteString>& out_prefixes);
	boost::optional<std::string> createTenant(fdb::Database db, int id_begin, int id_end);
	boost::optional<std::string> deleteTenant(fdb::Database db, int id_begin, int id_end);
	TenantIdsResponse fetchTenantIds(fdb::Database db, int id_begin, int id_end);

	template <class T>
	static void sendObject(boost::process::pstream& pipe, T obj) {
		boost::archive::binary_oarchive oa(pipe);
		oa << obj;
	}

	template <class T>
	static T receiveObject(boost::process::pstream& pipe) {
		boost::archive::binary_iarchive ia(pipe);
		T obj;
		ia >> obj;
		return obj;
	}

	template <class RequestType>
	static void sendResponse(boost::process::pstream& pipe, typename RequestType::ResponseType obj) {
		sendObject(pipe, std::move(obj));
	}

public:
	AdminServer(const Arguments& args)
	  : args(args), server_pid(-1), pipe_to_server(boost::process::pipe()), pipe_to_client(boost::process::pipe()) {
		start();
	}
	~AdminServer();

	// forks a server subprocess internally

	bool isClient() const noexcept { return server_pid > 0; }

	template <class T>
	typename T::ResponseType send(T req) {
		// should always be invoked from client side (currently just the main process)
		assert(server_pid > 0);
		assert(logr.isFor(ProcKind::MAIN));
		sendObject(pipe_to_server, Request(std::move(req)));
		return receiveObject<typename T::ResponseType>(pipe_to_client);
	}

	AdminServer(const AdminServer&) = delete;
	AdminServer(AdminServer&&) = delete;
	AdminServer& operator=(const AdminServer&) = delete;
	AdminServer& operator=(AdminServer&&) = delete;
};

} // namespace mako::ipc
