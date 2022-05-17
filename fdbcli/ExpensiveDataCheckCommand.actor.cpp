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

ACTOR Future<bool> expensiveDataCheckCommandActor(Reference<IDatabase> db,
                                                  Reference<ITransaction> tr,
                                                  std::vector<StringRef> tokens,
                                                  std::set<std::string>* addresses) {
	state bool result = true;
	state std::vector<Future<int64_t>> futures;
	state int i;
	if (tokens.size() == 1) {
		wait(fetchAndUpdateWorkerInterfaces(db, addresses));
	}
	if (tokens.size() == 1 || tokencmp(tokens[1], "list")) {
		if (addresses->size() == 0) {
			printf("\nNo addresses can be checked.\n");
		} else if (addresses->size() == 1) {
			printf("\nThe following address can be checked:\n");
		} else {
			printf("\nThe following %zu addresses can be checked:\n", addresses->size());
		}
		for (const auto& addr : *addresses) {
			printf("%s\n", printable(addr).c_str());
		}
		printf("\n");
	} else if (tokencmp(tokens[1], "all")) {
		state std::set<std::string>::iterator it;
		for (it = addresses->begin(); it != addresses->end(); ++it) {
			futures.push_back(safeThreadFutureToFuture(db->rebootWorker(*it, true, 0)));
		}
		wait(waitForAll(futures));
		i = 0;
		it = addresses->begin();
		while (i < futures.size()) {
			if (!futures[i].get()) {
				result = false;
				fprintf(stderr, "ERROR: failed to send request to check process `%s'.\n", it->c_str());
			}
			i++;
			++it;
		}

		if (addresses->size() == 0) {
			fprintf(stderr,
			        "ERROR: no processes to check. You must run the `expensive_data_check’ "
			        "command before running `expensive_data_check all’.\n");
		} else {
			printf("Attempted to kill and check %zu processes\n", addresses->size());
		}
	} else {
		for (i = 1; i < tokens.size(); i++) {
			if (addresses->find(tokens[i].toString()) == addresses->end()) {
				fprintf(stderr, "ERROR: process `%s' not recognized.\n", printable(tokens[i]).c_str());
				result = false;
				break;
			}
		}

		if (result) {
			for (i = 1; i < tokens.size(); i++) {
				futures.push_back(safeThreadFutureToFuture(db->rebootWorker(tokens[i], true, 0)));
			}
			wait(waitForAll(futures));
			for (i = 0; i < futures.size(); i++) {
				if (!futures[i].get()) {
					result = false;
					fprintf(stderr,
					        "ERROR: failed to send request to check process `%s'.\n",
					        tokens[i + 1].toString().c_str());
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
