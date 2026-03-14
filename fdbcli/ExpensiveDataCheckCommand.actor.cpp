/*
 * ExpensiveDataCheckCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "fmt/format.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

// The command is used to send a data check request to the specified process
// The check request is accomplished by rebooting the process

ACTOR Future<bool> expensiveDataCheckCommandActor(
    Reference<IDatabase> db,
    Reference<ITransaction> tr,
    std::vector<StringRef> tokens,
    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	state bool result = true;
	state std::string addressesStr;
	if (tokens.size() == 1) {
		// initialize worker interfaces
		address_interface->clear();
		wait(getWorkerInterfaces(tr, address_interface, true));
	}
	if (tokens.size() == 1 || tokencmp(tokens[1], "list")) {
		if (address_interface->size() == 0) {
			fmt::print("\nNo addresses can be checked.\n");
		} else if (address_interface->size() == 1) {
			fmt::print("\nThe following address can be checked:\n");
		} else {
			fmt::print("\nThe following {} addresses can be checked:\n", address_interface->size());
		}
		for (auto it : *address_interface) {
			fmt::println("{}", printable(it.first));
		}
		fmt::println("");
	} else if (tokencmp(tokens[1], "all")) {
		if (address_interface->size() == 0) {
			fmt::println(stderr,
			             "ERROR: no processes to check. You must run the `expensive_data_checkâ command before "
			             "running `expensive_data_check allâ.");
		} else {
			std::vector<std::string> addressesVec;
			for (const auto& [address, _] : *address_interface) {
				addressesVec.push_back(address.toString());
			}
			addressesStr = boost::algorithm::join(addressesVec, ",");
			// make sure we only call the interface once to send requests in parallel
			int64_t checkRequestsSent = wait(safeThreadFutureToFuture(db->rebootWorker(addressesStr, true, 0)));
			if (!checkRequestsSent) {
				result = false;
				fmt::println(stderr,
				             "ERROR: failed to send requests to check all processes, please run the "
				             "`expensive_data_checkâ command again to fetch latest addresses.");
			} else {
				fmt::println("Attempted to kill and check {} processes", address_interface->size());
			}
		}
	} else {
		state int i;
		for (i = 1; i < tokens.size(); i++) {
			if (!address_interface->count(tokens[i])) {
				fmt::println(stderr, "ERROR: process `{}' not recognized.", printable(tokens[i]));
				result = false;
				break;
			}
		}

		if (result) {
			std::vector<std::string> addressesVec;
			for (i = 1; i < tokens.size(); i++) {
				addressesVec.push_back(tokens[i].toString());
			}
			addressesStr = boost::algorithm::join(addressesVec, ",");
			int64_t checkRequestsSent = wait(safeThreadFutureToFuture(db->rebootWorker(addressesStr, true, 0)));
			if (!checkRequestsSent) {
				result = false;
				fmt::println(stderr,
				             "ERROR: failed to send requests to check processes `{}', please run the "
				             "`expensive_data_checkâ command again to fetch latest addresses.",
				             addressesStr);
			} else {
				fmt::println("Attempted to kill and check {} processes", tokens.size() - 1);
			}
		}
	}
	return result;
}
// hidden commands, no help text for now
CommandFactory expensiveDataCheckFactory("expensive_data_check");
} // namespace fdb_cli
