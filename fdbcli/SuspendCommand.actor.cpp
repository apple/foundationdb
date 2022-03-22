/*
 * SuspendCommand.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> suspendCommandActor(Reference<IDatabase> db,
                                       Reference<ITransaction> tr,
                                       std::vector<StringRef> tokens,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	ASSERT(tokens.size() >= 1);
	state bool result = true;
	if (tokens.size() == 1) {
		// initialize worker interfaces
		wait(getWorkerInterfaces(tr, address_interface));
		if (address_interface->size() == 0) {
			printf("\nNo addresses can be suspended.\n");
		} else if (address_interface->size() == 1) {
			printf("\nThe following address can be suspended:\n");
		} else {
			printf("\nThe following %zu addresses can be suspended:\n", address_interface->size());
		}
		for (auto it : *address_interface) {
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
			state double seconds;
			int n = 0;
			state int i;
			auto secondsStr = tokens[1].toString();
			if (sscanf(secondsStr.c_str(), "%lf%n", &seconds, &n) != 1 || n != secondsStr.size()) {
				printUsage(tokens[0]);
				result = false;
			} else {
				int64_t timeout_ms = seconds * 1000;
				tr->setOption(FDBTransactionOptions::TIMEOUT, StringRef((uint8_t*)&timeout_ms, sizeof(int64_t)));
				for (i = 2; i < tokens.size(); i++) {
					int64_t suspendRequestSent =
					    wait(safeThreadFutureToFuture(db->rebootWorker(tokens[i], false, static_cast<int>(seconds))));
					if (!suspendRequestSent) {
						result = false;
						fprintf(stderr,
						        "ERROR: failed to send request to suspend process `%s'.\n",
						        tokens[i].toString().c_str());
					}
				}
				printf("Attempted to suspend %zu processes\n", tokens.size() - 2);
			}
		}
	}
	return result;
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
