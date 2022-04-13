/*
 * ExpensiveDataCheckCommand.actor.cpp
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

// The command is used to send a data check request to the specified process
// The check request is accomplished by rebooting the process

ACTOR Future<bool> expensiveDataCheckCommandActor(
    Reference<IDatabase> db,
    Reference<ITransaction> tr,
    std::vector<StringRef> tokens,
    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	state bool result = true;
	if (tokens.size() == 1) {
		// initialize worker interfaces
		wait(getWorkerInterfaces(tr, address_interface));
	}
	if (tokens.size() == 1 || tokencmp(tokens[1], "list")) {
		if (address_interface->size() == 0) {
			printf("\nNo addresses can be checked.\n");
		} else if (address_interface->size() == 1) {
			printf("\nThe following address can be checked:\n");
		} else {
			printf("\nThe following %zu addresses can be checked:\n", address_interface->size());
		}
		for (auto it : *address_interface) {
			printf("%s\n", printable(it.first).c_str());
		}
		printf("\n");
	} else if (tokencmp(tokens[1], "all")) {
		state std::map<Key, std::pair<Value, ClientLeaderRegInterface>>::const_iterator it;
		for (it = address_interface->cbegin(); it != address_interface->cend(); it++) {
			int64_t checkRequestSent = wait(safeThreadFutureToFuture(db->rebootWorker(it->first, true, 0)));
			if (!checkRequestSent) {
				result = false;
				fprintf(stderr, "ERROR: failed to send request to check process `%s'.\n", it->first.toString().c_str());
			}
		}
		if (address_interface->size() == 0) {
			fprintf(stderr,
			        "ERROR: no processes to check. You must run the `expensive_data_check’ "
			        "command before running `expensive_data_check all’.\n");
		} else {
			printf("Attempted to kill and check %zu processes\n", address_interface->size());
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
				int64_t checkRequestSent = wait(safeThreadFutureToFuture(db->rebootWorker(tokens[i], true, 0)));
				if (!checkRequestSent) {
					result = false;
					fprintf(
					    stderr, "ERROR: failed to send request to check process `%s'.\n", tokens[i].toString().c_str());
				}
			}
			printf("Attempted to kill and check %zu processes\n", tokens.size() - 1);
		}
	}
	return result;
}
// hidden commands, no help text for now
CommandFactory expensiveDataCheckFactory("expensive_data_check");
} // namespace fdb_cli
