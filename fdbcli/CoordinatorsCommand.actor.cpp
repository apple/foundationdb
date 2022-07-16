/*
 * CoordinatorsCommand.actor.cpp
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

#include "boost/algorithm/string.hpp"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> printCoordinatorsInfo(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			// Hold the reference to the standalone's memory
			state ThreadFuture<Optional<Value>> descriptionF = tr->get(fdb_cli::clusterDescriptionSpecialKey);
			Optional<Value> description = wait(safeThreadFutureToFuture(descriptionF));
			ASSERT(description.present());
			printf("Cluster description: %s\n", description.get().toString().c_str());
			// Hold the reference to the standalone's memory
			state ThreadFuture<Optional<Value>> processesF = tr->get(fdb_cli::coordinatorsProcessSpecialKey);
			Optional<Value> processes = wait(safeThreadFutureToFuture(processesF));
			ASSERT(processes.present());
			std::vector<std::string> process_addresses;
			boost::split(process_addresses, processes.get().toString(), [](char c) { return c == ','; });
			printf("Cluster coordinators (%zu): %s\n", process_addresses.size(), processes.get().toString().c_str());
			printf("Type `help coordinators' to learn how to change this information.\n");
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> changeCoordinators(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state int retries = 0;
	state int notEnoughMachineResults = 0;
	state StringRef new_cluster_description;
	state std::string auto_coordinators_str;
	StringRef nameTokenBegin = LiteralStringRef("description=");
	for (auto tok = tokens.begin() + 1; tok != tokens.end(); ++tok) {
		if (tok->startsWith(nameTokenBegin)) {
			new_cluster_description = tok->substr(nameTokenBegin.size());
			std::copy(tok + 1, tokens.end(), tok);
			tokens.resize(tokens.size() - 1);
			break;
		}
	}

	state bool automatic = tokens.size() == 2 && tokens[1] == LiteralStringRef("auto");
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// update cluster description
			if (new_cluster_description.size()) {
				tr->set(fdb_cli::clusterDescriptionSpecialKey, new_cluster_description);
			}
			// if auto change, read the special key to retrieve the recommended config
			if (automatic) {
				// if previous read failed, retry, otherwise, use the same recommened config
				if (!auto_coordinators_str.size()) {
					// Hold the reference to the standalone's memory
					state ThreadFuture<Optional<Value>> auto_coordinatorsF =
					    tr->get(fdb_cli::coordinatorsAutoSpecialKey);
					Optional<Value> auto_coordinators = wait(safeThreadFutureToFuture(auto_coordinatorsF));
					ASSERT(auto_coordinators.present());
					auto_coordinators_str = auto_coordinators.get().toString();
				}
				tr->set(fdb_cli::coordinatorsProcessSpecialKey, auto_coordinators_str);
			} else if (tokens.size() > 1) {
				state std::set<NetworkAddress> new_coordinators_addresses;
				state std::set<Hostname> new_coordinators_hostnames;
				state std::vector<std::string> newCoordinatorslist;
				state std::vector<StringRef>::iterator t;
				for (t = tokens.begin() + 1; t != tokens.end(); ++t) {
					try {
						if (Hostname::isHostname(t->toString())) {
							// We do not resolve hostnames here. We commit them as is.
							const auto& hostname = Hostname::parse(t->toString());
							if (new_coordinators_hostnames.count(hostname)) {
								fprintf(stderr,
								        "ERROR: passed redundant coordinators: `%s'\n",
								        hostname.toString().c_str());
								return true;
							}
							new_coordinators_hostnames.insert(hostname);
							newCoordinatorslist.push_back(hostname.toString());
						} else {
							const auto& addr = NetworkAddress::parse(t->toString());
							if (new_coordinators_addresses.count(addr)) {
								fprintf(
								    stderr, "ERROR: passed redundant coordinators: `%s'\n", addr.toString().c_str());
								return true;
							}
							new_coordinators_addresses.insert(addr);
							newCoordinatorslist.push_back(addr.toString());
						}
					} catch (Error& e) {
						if (e.code() == error_code_connection_string_invalid) {
							fprintf(
							    stderr, "ERROR: '%s' is not a valid network endpoint address\n", t->toString().c_str());
							return true;
						}
						throw;
					}
				}
				std::string new_coordinators_str = boost::algorithm::join(newCoordinatorslist, ",");
				tr->set(fdb_cli::coordinatorsProcessSpecialKey, new_coordinators_str);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			// commit should always fail here
			// If the commit succeeds, the coordinators change and the commit will fail with commit_unknown_result().
			ASSERT(false);
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(fdb_cli::getSpecialKeysFailureErrorMessage(tr));
				if (errorMsgStr == ManagementAPI::generateErrorMessage(CoordinatorsResult::NOT_ENOUGH_MACHINES) &&
				    notEnoughMachineResults < 1) {
					// we could get not_enough_machines if we happen to see the database while the cluster controller is
					// updating the worker list, so make sure it happens twice before returning a failure
					notEnoughMachineResults++;
					wait(delay(1.0));
					tr->reset();
					continue;
				} else if (errorMsgStr ==
				           ManagementAPI::generateErrorMessage(CoordinatorsResult::SAME_NETWORK_ADDRESSES)) {
					if (retries)
						printf("Coordination state changed\n");
					else
						printf("No change (existing configuration satisfies request)\n");
					return true;
				} else {
					fprintf(stderr, "ERROR: %s\n", errorMsgStr.c_str());
					return false;
				}
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
			++retries;
		}
	}
}

} // namespace

namespace fdb_cli {

const KeyRef clusterDescriptionSpecialKey = LiteralStringRef("\xff\xff/configuration/coordinators/cluster_description");
const KeyRef coordinatorsAutoSpecialKey = LiteralStringRef("\xff\xff/management/auto_coordinators");
const KeyRef coordinatorsProcessSpecialKey = LiteralStringRef("\xff\xff/configuration/coordinators/processes");

ACTOR Future<bool> coordinatorsCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 2) {
		wait(printCoordinatorsInfo(db));
		return true;
	} else {
		bool result = wait(changeCoordinators(db, tokens));
		return result;
	}
}

CommandFactory coordinatorsFactory(
    "coordinators",
    CommandHelp(
        "coordinators auto|<ADDRESS>+ [description=new_cluster_description]",
        "change cluster coordinators or description",
        "If 'auto' is specified, coordinator addresses will be choosen automatically to support the configured "
        "redundancy level. (If the current set of coordinators are healthy and already support the redundancy level, "
        "nothing will be changed.)\n\nOtherwise, sets the coordinators to the list of IP:port pairs specified by "
        "<ADDRESS>+. An fdbserver process must be running on each of the specified addresses.\n\ne.g. coordinators "
        "10.0.0.1:4000 10.0.0.2:4000 10.0.0.3:4000\n\nIf 'description=desc' is specified then the description field in "
        "the cluster\nfile is changed to desc, which must match [A-Za-z0-9_]+."));
} // namespace fdb_cli
