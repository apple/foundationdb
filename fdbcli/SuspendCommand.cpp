/*
 * SuspendCommand.cpp
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

#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
namespace fdb_cli {

Future<bool> suspendCommandActor(Reference<IDatabase> db,
                                 Reference<ITransaction> tr,
                                 std::vector<StringRef> const& tokens,
                                 std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	ASSERT(!tokens.empty());
	bool result = true;
	std::string addressesStr;
	if (tokens.size() == 1) {
		// initialize worker interfaces
		address_interface->clear();
		co_await getWorkerInterfaces(tr, address_interface, true);
		if (address_interface->empty()) {
			printf("\nNo addresses can be suspended.\n");
		} else if (address_interface->size() == 1) {
			printf("\nThe following address can be suspended:\n");
		} else {
			printf("\nThe following %zu addresses can be suspended:\n", address_interface->size());
		}
		for (const auto& it : *address_interface) {
			printf("%s\n", printable(it.first).c_str());
		}
		printf("\n");
	} else if (tokens.size() == 2) {
		printUsage(tokens[0]);
		result = false;
	} else {
		for (int i = 2; i < tokens.size(); i++) {
			if (!address_interface->count(tokens[i])) {
				fprintf(stderr, "ERROR: process `%s' not recognized.\n", printable(tokens[i]).c_str());
				result = false;
				break;
			}
		}

		if (result) {
			double seconds{ 0 };
			int n = 0;
			int i{ 0 };
			auto secondsStr = tokens[1].toString();
			if (sscanf(secondsStr.c_str(), "%lf%n", &seconds, &n) != 1 || n != secondsStr.size()) {
				printUsage(tokens[0]);
				result = false;
			} else {
				std::vector<std::string> addressesVec;
				for (i = 2; i < tokens.size(); i++) {
					addressesVec.push_back(tokens[i].toString());
				}
				addressesStr = boost::algorithm::join(addressesVec, ",");
				int64_t suspendRequestSent =
				    co_await safeThreadFutureToFuture(db->rebootWorker(addressesStr, false, static_cast<int>(seconds)));
				if (!suspendRequestSent) {
					result = false;
					fprintf(
					    stderr,
					    "ERROR: failed to send requests to suspend processes `%s', please run the `suspend’ command "
					    "to fetch latest addresses.\n",
					    addressesStr.c_str());
				} else {
					printf("Attempted to suspend %zu processes\n", tokens.size() - 2);
				}
			}
		}
	}
	co_return result;
}

CommandFactory suspendFactory(
    "suspend",
    CommandHelp(
        "suspend <SECONDS> <ADDRESS...>",
        "attempts to suspend one or more processes in the cluster",
        "If no parameters are specified, populates the list of processes which can be suspended. Processes cannot be "
        "suspended before this list has been populated.\n\nFor each IP:port pair in <ADDRESS...>, attempt to suspend "
        "the processes for the specified SECONDS after which the process will die."));
} // namespace fdb_cli
