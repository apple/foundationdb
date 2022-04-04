/*
 * KillCommand.actor.cpp
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

ACTOR Future<bool> killCommandActor(Reference<IDatabase> db,
                                    Reference<ITransaction> tr,
                                    std::vector<StringRef> tokens,
                                    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	ASSERT(tokens.size() >= 1);
	state bool result = true;
	if (tokens.size() == 1) {
		// initialize worker interfaces
		wait(getWorkerInterfaces(tr, address_interface));
	}
	if (tokens.size() == 1 || tokencmp(tokens[1], "list")) {
		if (address_interface->size() == 0) {
			printf("\nNo addresses can be killed.\n");
		} else if (address_interface->size() == 1) {
			printf("\nThe following address can be killed:\n");
		} else {
			printf("\nThe following %zu addresses can be killed:\n", address_interface->size());
		}
		for (auto it : *address_interface) {
			printf("%s\n", printable(it.first).c_str());
		}
		printf("\n");
	} else if (tokencmp(tokens[1], "all")) {
		state std::map<Key, std::pair<Value, ClientLeaderRegInterface>>::const_iterator it;
		for (it = address_interface->cbegin(); it != address_interface->cend(); it++) {
			int64_t killRequestSent = wait(safeThreadFutureToFuture(db->rebootWorker(it->first, false, 0)));
			if (!killRequestSent) {
				result = false;
				fprintf(stderr, "ERROR: failed to send request to kill process `%s'.\n", it->first.toString().c_str());
			}
		}
		if (address_interface->size() == 0) {
			result = false;
			fprintf(stderr,
			        "ERROR: no processes to kill. You must run the `kill’ command before "
			        "running `kill all’.\n");
		} else {
			printf("Attempted to kill %zu processes\n", address_interface->size());
		}
	} else {
		state int i;
		for (i = 1; i < tokens.size(); i++) {
			if (!address_interface->count(tokens[i])) {
				fprintf(stderr, "ERROR: process `%s' not recognized.\n", printable(tokens[i]).c_str());
				result = false;
				break;
			}
		}

		if (result) {
			for (i = 1; i < tokens.size(); i++) {
				int64_t killRequestSent = wait(safeThreadFutureToFuture(db->rebootWorker(tokens[i], false, 0)));
				if (!killRequestSent) {
					result = false;
					fprintf(
					    stderr, "ERROR: failed to send request to kill process `%s'.\n", tokens[i].toString().c_str());
				}
			}
			printf("Attempted to kill %zu processes\n", tokens.size() - 1);
		}
	}
	return result;
}

CommandFactory killFactory(
    "kill",
    CommandHelp(
        "kill all|list|<ADDRESS...>",
        "attempts to kill one or more processes in the cluster",
        "If no addresses are specified, populates the list of processes which can be killed. Processes cannot be "
        "killed before this list has been populated.\n\nIf `all' is specified, attempts to kill all known "
        "processes.\n\nIf `list' is specified, displays all known processes. This is only useful when the database is "
        "unresponsive.\n\nFor each IP:port pair in <ADDRESS ...>, attempt to kill the specified process."));
} // namespace fdb_cli
