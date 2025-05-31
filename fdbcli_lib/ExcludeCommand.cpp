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
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/Locality.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/NetworkAddress.h"
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

Future<bool> excludeServersAndLocalities(Reference<IDatabase> db,
                                         std::vector<AddressExclusion> servers,
                                         std::unordered_set<std::string> localities,
                                         bool failed,
                                         bool force) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		try {
			if (force && servers.size())
				tr->set(failed ? special_keys::failedForceOptionSpecialKey
				               : special_keys::excludedForceOptionSpecialKey,
				        ValueRef());
			for (const auto& s : servers) {
				Key addr = failed ? special_keys::failedServersSpecialKeyRange.begin.withSuffix(s.toString())
				                  : special_keys::excludedServersSpecialKeyRange.begin.withSuffix(s.toString());
				tr->set(addr, ValueRef());
			}
			if (force && localities.size())
				tr->set(failed ? special_keys::failedLocalityForceOptionSpecialKey
				               : special_keys::excludedLocalityForceOptionSpecialKey,
				        ValueRef());
			for (const auto& l : localities) {
				Key addr = failed ? special_keys::failedLocalitySpecialKeyRange.begin.withSuffix(l)
				                  : special_keys::excludedLocalitySpecialKeyRange.begin.withSuffix(l);
				tr->set(addr, ValueRef());
			}
			co_await safeThreadFutureToFuture(tr->commit());
			co_return true;
		} catch (Error& e) {
			err = e;
		}

		if (err.code() == error_code_special_keys_api_failure) {
			std::string errorMsgStr = co_await utils::getSpecialKeysFailureErrorMessage(tr);
			// last character is \n
			auto pos = errorMsgStr.find_last_of("\n", errorMsgStr.size() - 2);
			auto last_line = errorMsgStr.substr(pos + 1);
			// customized the error message for fdbcli
			fprintf(stderr,
			        "%s\n%s\n",
			        errorMsgStr.substr(0, pos).c_str(),
			        last_line.find("free space") != std::string::npos
			            ? "Type `exclude FORCE <ADDRESS...>' to exclude without checking free space."
			            : "Type `exclude FORCE failed <ADDRESS...>' to exclude without performing safety checks.");
			co_return false;
		}
		TraceEvent(SevWarn, "ExcludeServersAndLocalitiesError").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<std::set<NetworkAddress>> checkForExcludingServers(Reference<IDatabase> db,
                                                          std::set<AddressExclusion> exclusions,
                                                          bool waitForAllExcluded) {
	std::set<NetworkAddress> inProgressExclusion;
	Reference<ITransaction> tr = db->createTransaction();
	tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		Error err;
		inProgressExclusion.clear();
		try {
			std::set<NetworkAddress> result = co_await utils::getInProgressExclusion(tr);
			if (result.empty())
				co_return inProgressExclusion;
			inProgressExclusion = result;

			// Check if all of the specified exclusions are done.
			bool allExcluded = true;
			for (const auto& inProgressAddr : inProgressExclusion) {
				if (!allExcluded) {
					break;
				}

				for (const auto& exclusion : exclusions) {
					// We found an exclusion that is still in progress
					if (exclusion.excludes(inProgressAddr)) {
						allExcluded = false;
						break;
					}
				}
			}

			if (allExcluded) {
				inProgressExclusion.clear();
				co_return inProgressExclusion;
			}

			if (!waitForAllExcluded)
				break;

			co_await delayJittered(1.0); // SOMEDAY: watches!
		} catch (Error& e) {
			TraceEvent(SevWarn, "CheckForExcludingServersError").error(e);
			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}

	co_return inProgressExclusion;
}

Future<grpc::Status> exclude(Reference<IDatabase> db, const ExcludeRequest* req, ExcludeReply* rep) {
	try {
		std::vector<ProcessData> workers;
		std::map<std::string, StorageServerInterface> server_interfaces;
		Future<bool> future_workers = utils::getWorkers(db, &workers);
		Future<Void> future_server_interfaces = utils::getStorageServerInterfaces(db, &server_interfaces);

		co_await success(future_workers);
		co_await future_server_interfaces;

		bool force = req->force();
		bool waitForAllExcluded = !req->no_wait();
		bool markFailed = req->failed();

		ASSERT(force == force && waitForAllExcluded == waitForAllExcluded && markFailed == markFailed);

		std::set<AddressExclusion> exclusionSet;
		std::unordered_set<std::string> exclusionLocalities;
		std::vector<std::string> noMatchLocalities;
		for (auto& loc : req->localities()) {
			ASSERT(loc.starts_with(LocalityData::ExcludeLocalityPrefix.toString()) &&
			       loc.find(':') != std::string::npos);
			exclusionLocalities.insert(loc);
			auto localityAddresses = getAddressesByLocality(workers, loc);
			auto localityServerAddresses = getServerAddressesByLocality(server_interfaces, loc);
			if (localityAddresses.empty() && localityServerAddresses.empty()) {
				noMatchLocalities.push_back(loc);
			}

			if (!localityAddresses.empty()) {
				exclusionSet.insert(localityAddresses.begin(), localityAddresses.end());
			}

			if (!localityServerAddresses.empty()) {
				exclusionSet.insert(localityServerAddresses.begin(), localityServerAddresses.end());
			}
		}

		std::vector<AddressExclusion> exclusionAddresses;
		for (auto& addr : req->processes()) {
			auto a = AddressExclusion::parse(addr);
			if (!a.isValid()) {
				// fprintf(stderr,
				//         "ERROR: '%s' is neither a valid network endpoint address nor a locality\n",
				//         t->toString().c_str())m;
				// if (t->toString().find(":tls") != std::string::npos)
				// 	printf("        Do not include the `:tls' suffix when naming a process\n");
				co_return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "");
			}
			exclusionSet.insert(a);
			exclusionAddresses.push_back(a);
		}

		// The validation if a locality or address has no match is done below and will result in a warning. If we abort
		// here the provided locality and/or address will not be excluded.
		if (exclusionAddresses.empty() && exclusionLocalities.empty()) {
			fprintf(stderr, "ERROR: At least one valid network endpoint address or a locality must be provided\n");
			co_return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "");
		}

		bool res = co_await excludeServersAndLocalities(db, exclusionAddresses, exclusionLocalities, markFailed, force);
		if (!res) {
			co_return grpc::Status(grpc::StatusCode::INTERNAL, "");
		}

		std::set<NetworkAddress> notExcludedServers =
		    co_await checkForExcludingServers(db, exclusionSet, waitForAllExcluded);
		std::map<IPAddress, std::set<uint16_t>> workerPorts;
		for (auto addr : workers)
			workerPorts[addr.address.ip].insert(addr.address.port);

		// Print a list of all excluded addresses that don't have a corresponding worker
		std::set<AddressExclusion> absentExclusions;
		for (const auto& addr : exclusionSet) {
			auto worker = workerPorts.find(addr.ip);
			if (worker == workerPorts.end())
				absentExclusions.insert(addr);
			else if (addr.port > 0 && worker->second.count(addr.port) == 0)
				absentExclusions.insert(addr);
		}
		
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
