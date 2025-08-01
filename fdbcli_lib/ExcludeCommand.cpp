/*
 * ExcludeCommand.cpp
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
#include "boost/algorithm/string.hpp"
#include "fdbcli_lib/CliCommands.h"

#include "fdbcli_lib/cli_service/cli_service.pb.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/Locality.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "fmt/format.h"
#include <boost/algorithm/string/join.hpp>
#include <iostream>
#include <grpcpp/support/status.h>

#include <map>

namespace fdbcli_lib {

namespace utils {
Future<std::vector<std::string>> getExcludedServers(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(special_keys::excludedServersSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> exclusions;
			for (const auto& i : r) {
				auto addr = i.key.removePrefix(special_keys::excludedServersSpecialKeyRange.begin).toString();
				exclusions.push_back(addr);
			}
			co_return exclusions;
		} catch (Error& e) {
			TraceEvent(SevWarn, "GetExcludedServersError").error(e);
			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<std::vector<std::string>> getFailedServers(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(special_keys::failedServersSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> exclusions;
			for (const auto& i : r) {
				auto addr = i.key.removePrefix(special_keys::failedServersSpecialKeyRange.begin).toString();
				exclusions.push_back(addr);
			}

			co_return exclusions;
		} catch (Error& e) {
			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<std::vector<std::string>> getExcludedLocalities(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(special_keys::excludedLocalitySpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> excludedLocalities;
			for (const auto& i : r) {
				auto locality = i.key.removePrefix(special_keys::excludedLocalitySpecialKeyRange.begin).toString();
				excludedLocalities.push_back(locality);
			}
			co_return excludedLocalities;
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<std::set<NetworkAddress>> getInProgressExclusion(Reference<ITransaction> tr) {
	ThreadFuture<RangeResult> resultFuture =
	    tr->getRange(fdbcli_lib::special_keys::exclusionInProgressSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
	RangeResult result = co_await safeThreadFutureToFuture(resultFuture);
	ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
	std::set<NetworkAddress> inProgressExclusion;
	for (const auto& addr : result) {
		inProgressExclusion.insert(NetworkAddress::parse(
		    addr.key.removePrefix(fdbcli_lib::special_keys::exclusionInProgressSpecialKeyRange.begin).toString()));
	}
	co_return inProgressExclusion;
}

Future<std::vector<std::string>> getFailedLocalities(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(special_keys::failedLocalitySpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> excludedLocalities;
			for (const auto& i : r) {
				auto locality = i.key.removePrefix(special_keys::failedLocalitySpecialKeyRange.begin).toString();
				excludedLocalities.push_back(locality);
			}
			co_return excludedLocalities;
		} catch (Error& e) {
			TraceEvent(SevWarn, "GetExcludedLocalitiesError").error(e);
			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

} // namespace utils

Future<grpc::Status> exclude(Reference<IDatabase> db, const ExcludeRequest* req, ExcludeReply* rep) {
	try {
		// std::set<AddressExclusion> exclusionSet;
		// std::vector<AddressExclusion> exclusionAddresses;
		// std::vector<std::string> noMatchLocalities;
		// std::vector<ProcessData> workers;
		std::map<std::string, StorageSemkkrverInterface> server_interfaces;

		GetWorkersRequest getWorkersReq;
		GetWorkersReply getWorkersRep;
		Future<grpc::Status> future_workers = co_await getWorkers(db, &getWorkersReq, &getWorkersRep);
		Future<Void> future_server_interfaces = utils::getStorageServerInterfaces(db, &server_interfaces);

		co_await success(future_workers);
		co_await future_server_interfaces;

		// bool force = req->force();
		// bool waitForAllExcluded = !req->no_wait();
		// bool markFailed = req->failed();

		co_return grpc::Status::OK;
	} catch (const Error& e) {
		co_return grpc::Status(grpc::StatusCode::INTERNAL,
		                       fmt::format("Error getting worker information: {}", e.what()));
	}
}

Future<grpc::Status> excludeStatus(Reference<IDatabase> db, const ExcludeStatusRequest* req, ExcludeStatusReply* rep) {
	try {
		std::vector<std::string> excludedAddresses = co_await fdbcli_lib::utils::getExcludedServers(db);
		std::vector<std::string> excludedLocalities = co_await fdbcli_lib::utils::getExcludedLocalities(db);
		std::vector<std::string> failedAddresses = co_await fdbcli_lib::utils::getFailedServers(db);
		std::vector<std::string> failedLocalities = co_await fdbcli_lib::utils::getFailedLocalities(db);

		for (const auto& e : excludedAddresses) {
			rep->add_excluded_addresses(e);
		}

		for (const auto& e : excludedLocalities) {
			rep->add_excluded_localities(e);
		}

		for (const auto& f : failedAddresses) {
			rep->add_failed_addresses(f);
		}

		for (const auto& f : failedLocalities) {
			rep->add_failed_localities(f);
		}

		Reference<ITransaction> tr = db->createTransaction();
		std::set<NetworkAddress> inProgressExclusion = co_await utils::getInProgressExclusion(tr);
		for (const auto& addr : inProgressExclusion) {
			rep->add_in_progress_excludes(addr.toString());
		}

		co_return grpc::Status::OK;
	} catch (const Error& e) {
		co_return grpc::Status(grpc::StatusCode::INTERNAL,
		                       fmt::format("Error getting worker information: {}", e.what()));
	}
}

} // namespace fdbcli_lib
