/*
 * ExcludeCommand.actor.cpp
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// Exclude the given servers and localities
ACTOR Future<bool> excludeServersAndLocalities(Reference<IDatabase> db,
                                               std::vector<AddressExclusion> servers,
                                               std::unordered_set<std::string> localities,
                                               bool failed,
                                               bool force) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			if (force && servers.size())
				tr->set(failed ? fdb_cli::failedForceOptionSpecialKey : fdb_cli::excludedForceOptionSpecialKey,
				        ValueRef());
			for (const auto& s : servers) {
				Key addr = failed ? fdb_cli::failedServersSpecialKeyRange.begin.withSuffix(s.toString())
				                  : fdb_cli::excludedServersSpecialKeyRange.begin.withSuffix(s.toString());
				tr->set(addr, ValueRef());
			}
			if (force && localities.size())
				tr->set(failed ? fdb_cli::failedLocalityForceOptionSpecialKey
				               : fdb_cli::excludedLocalityForceOptionSpecialKey,
				        ValueRef());
			for (const auto& l : localities) {
				Key addr = failed ? fdb_cli::failedLocalitySpecialKeyRange.begin.withSuffix(l)
				                  : fdb_cli::excludedLocalitySpecialKeyRange.begin.withSuffix(l);
				tr->set(addr, ValueRef());
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return true;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(fdb_cli::getSpecialKeysFailureErrorMessage(tr));
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
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}
}

ACTOR Future<std::vector<std::string>> getExcludedServers(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::excludedServersSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			state RangeResult r = wait(safeThreadFutureToFuture(resultFuture));
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);
			state ThreadFuture<RangeResult> resultFuture2 =
			    tr->getRange(fdb_cli::failedServersSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			state RangeResult r2 = wait(safeThreadFutureToFuture(resultFuture2));
			ASSERT(!r2.more && r2.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> exclusions;
			for (const auto& i : r) {
				auto addr = i.key.removePrefix(fdb_cli::excludedServersSpecialKeyRange.begin).toString();
				exclusions.push_back(addr);
			}
			for (const auto& i : r2) {
				auto addr = i.key.removePrefix(fdb_cli::failedServersSpecialKeyRange.begin).toString();
				exclusions.push_back(addr);
			}
			return exclusions;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// Get the list of excluded localities by reading the keys.
ACTOR Future<std::vector<std::string>> getExcludedLocalities(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::excludedLocalitySpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			state RangeResult r = wait(safeThreadFutureToFuture(resultFuture));
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);
			state ThreadFuture<RangeResult> resultFuture2 =
			    tr->getRange(fdb_cli::failedLocalitySpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			state RangeResult r2 = wait(safeThreadFutureToFuture(resultFuture2));
			ASSERT(!r2.more && r2.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> excludedLocalities;
			for (const auto& i : r) {
				auto locality = i.key.removePrefix(fdb_cli::excludedLocalitySpecialKeyRange.begin).toString();
				excludedLocalities.push_back(locality);
			}
			for (const auto& i : r2) {
				auto locality = i.key.removePrefix(fdb_cli::failedLocalitySpecialKeyRange.begin).toString();
				excludedLocalities.push_back(locality);
			}
			return excludedLocalities;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<std::set<NetworkAddress>> checkForExcludingServers(Reference<IDatabase> db,
                                                                std::vector<AddressExclusion> excl,
                                                                bool waitForAllExcluded) {
	state std::set<AddressExclusion> exclusions(excl.begin(), excl.end());
	state std::set<NetworkAddress> inProgressExclusion;
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		inProgressExclusion.clear();
		try {
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::exclusionInProgressSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult exclusionInProgress = wait(safeThreadFutureToFuture(resultFuture));
			ASSERT(!exclusionInProgress.more && exclusionInProgress.size() < CLIENT_KNOBS->TOO_MANY);
			if (exclusionInProgress.empty())
				return inProgressExclusion;
			for (const auto& addr : exclusionInProgress)
				inProgressExclusion.insert(NetworkAddress::parse(
				    addr.key.removePrefix(fdb_cli::exclusionInProgressSpecialKeyRange.begin).toString()));
			if (!waitForAllExcluded)
				break;

			wait(delayJittered(1.0)); // SOMEDAY: watches!
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
	return inProgressExclusion;
}

ACTOR Future<Void> checkForCoordinators(Reference<IDatabase> db, std::vector<AddressExclusion> exclusionVector) {

	state bool foundCoordinator = false;
	state std::vector<NetworkAddress> coordinatorList;
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			// Hold the reference to the standalone's memory
			state ThreadFuture<Optional<Value>> coordinatorsF = tr->get(fdb_cli::coordinatorsProcessSpecialKey);
			Optional<Value> coordinators = wait(safeThreadFutureToFuture(coordinatorsF));
			ASSERT(coordinators.present());
			coordinatorList = NetworkAddress::parseList(coordinators.get().toString());
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
	for (const auto& c : coordinatorList) {
		if (std::count(exclusionVector.begin(), exclusionVector.end(), AddressExclusion(c.ip, c.port)) ||
		    std::count(exclusionVector.begin(), exclusionVector.end(), AddressExclusion(c.ip))) {
			fprintf(stderr, "WARNING: %s is a coordinator!\n", c.toString().c_str());
			foundCoordinator = true;
		}
	}
	if (foundCoordinator)
		printf("Type `help coordinators' for information on how to change the\n"
		       "cluster's coordination servers before removing them.\n");
	return Void();
}

} // namespace

namespace fdb_cli {

const KeyRangeRef excludedServersSpecialKeyRange(LiteralStringRef("\xff\xff/management/excluded/"),
                                                 LiteralStringRef("\xff\xff/management/excluded0"));
const KeyRangeRef failedServersSpecialKeyRange(LiteralStringRef("\xff\xff/management/failed/"),
                                               LiteralStringRef("\xff\xff/management/failed0"));
const KeyRangeRef excludedLocalitySpecialKeyRange(LiteralStringRef("\xff\xff/management/excluded_locality/"),
                                                  LiteralStringRef("\xff\xff/management/excluded_locality0"));
const KeyRangeRef failedLocalitySpecialKeyRange(LiteralStringRef("\xff\xff/management/failed_locality/"),
                                                LiteralStringRef("\xff\xff/management/failed_locality0"));
const KeyRef excludedForceOptionSpecialKey = LiteralStringRef("\xff\xff/management/options/excluded/force");
const KeyRef failedForceOptionSpecialKey = LiteralStringRef("\xff\xff/management/options/failed/force");
const KeyRef excludedLocalityForceOptionSpecialKey =
    LiteralStringRef("\xff\xff/management/options/excluded_locality/force");
const KeyRef failedLocalityForceOptionSpecialKey =
    LiteralStringRef("\xff\xff/management/options/failed_locality/force");
const KeyRangeRef exclusionInProgressSpecialKeyRange(LiteralStringRef("\xff\xff/management/in_progress_exclusion/"),
                                                     LiteralStringRef("\xff\xff/management/in_progress_exclusion0"));

ACTOR Future<bool> excludeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens, Future<Void> warn) {
	if (tokens.size() <= 1) {
		state std::vector<std::string> excludedAddresses = wait(getExcludedServers(db));
		state std::vector<std::string> excludedLocalities = wait(getExcludedLocalities(db));

		if (!excludedAddresses.size() && !excludedLocalities.size()) {
			printf("There are currently no servers or localities excluded from the database.\n"
			       "To learn how to exclude a server, type `help exclude'.\n");
			return true;
		}

		printf("There are currently %zu servers or localities being excluded from the database:\n",
		       excludedAddresses.size() + excludedLocalities.size());
		for (const auto& e : excludedAddresses)
			printf("  %s\n", e.c_str());
		for (const auto& e : excludedLocalities)
			printf("  %s\n", e.c_str());

		printf("To find out whether it is safe to remove one or more of these\n"
		       "servers from the cluster, type `exclude <addresses>'.\n"
		       "To return one of these servers to the cluster, type `include <addresses>'.\n");

		return true;
	} else {
		state std::vector<AddressExclusion> exclusionVector;
		state std::set<AddressExclusion> exclusionSet;
		state std::vector<AddressExclusion> exclusionAddresses;
		state std::unordered_set<std::string> exclusionLocalities;
		state std::vector<std::string> noMatchLocalities;
		state bool force = false;
		state bool waitForAllExcluded = true;
		state bool markFailed = false;
		state std::vector<ProcessData> workers;
		bool result = wait(fdb_cli::getWorkers(db, &workers));
		if (!result)
			return false;
		for (auto t = tokens.begin() + 1; t != tokens.end(); ++t) {
			if (*t == LiteralStringRef("FORCE")) {
				force = true;
			} else if (*t == LiteralStringRef("no_wait")) {
				waitForAllExcluded = false;
			} else if (*t == LiteralStringRef("failed")) {
				markFailed = true;
			} else if (t->startsWith(LocalityData::ExcludeLocalityPrefix) &&
			           t->toString().find(':') != std::string::npos) {
				std::set<AddressExclusion> localityAddresses = getAddressesByLocality(workers, t->toString());
				if (localityAddresses.empty()) {
					noMatchLocalities.push_back(t->toString());
				} else {
					// add all the server ipaddresses that belong to the given localities to the exclusionSet.
					exclusionVector.insert(exclusionVector.end(), localityAddresses.begin(), localityAddresses.end());
					exclusionSet.insert(localityAddresses.begin(), localityAddresses.end());
				}
				exclusionLocalities.insert(t->toString());
			} else {
				auto a = AddressExclusion::parse(*t);
				if (!a.isValid()) {
					fprintf(stderr,
					        "ERROR: '%s' is neither a valid network endpoint address nor a locality\n",
					        t->toString().c_str());
					if (t->toString().find(":tls") != std::string::npos)
						printf("        Do not include the `:tls' suffix when naming a process\n");
					return true;
				}
				exclusionVector.push_back(a);
				exclusionSet.insert(a);
				exclusionAddresses.push_back(a);
			}
		}

		if (exclusionAddresses.empty() && exclusionLocalities.empty()) {
			fprintf(stderr, "ERROR: At least one valid network endpoint address or a locality is not provided\n");
			return false;
		}

		bool res = wait(excludeServersAndLocalities(db, exclusionAddresses, exclusionLocalities, markFailed, force));
		if (!res)
			return false;

		if (waitForAllExcluded) {
			printf("Waiting for state to be removed from all excluded servers. This may take a while.\n");
			printf("(Interrupting this wait with CTRL+C will not cancel the data movement.)\n");
		}

		if (warn.isValid())
			warn.cancel();

		state std::set<NetworkAddress> notExcludedServers =
		    wait(checkForExcludingServers(db, exclusionVector, waitForAllExcluded));
		std::map<IPAddress, std::set<uint16_t>> workerPorts;
		for (auto addr : workers)
			workerPorts[addr.address.ip].insert(addr.address.port);

		// Print a list of all excluded addresses that don't have a corresponding worker
		std::set<AddressExclusion> absentExclusions;
		for (const auto& addr : exclusionVector) {
			auto worker = workerPorts.find(addr.ip);
			if (worker == workerPorts.end())
				absentExclusions.insert(addr);
			else if (addr.port > 0 && worker->second.count(addr.port) == 0)
				absentExclusions.insert(addr);
		}

		for (const auto& exclusion : exclusionVector) {
			if (absentExclusions.find(exclusion) != absentExclusions.end()) {
				if (exclusion.port == 0) {
					fprintf(stderr,
					        "  %s(Whole machine)  ---- WARNING: Missing from cluster!Be sure that you excluded the "
					        "correct machines before removing them from the cluster!\n",
					        exclusion.ip.toString().c_str());
				} else {
					fprintf(stderr,
					        "  %s  ---- WARNING: Missing from cluster! Be sure that you excluded the correct processes "
					        "before removing them from the cluster!\n",
					        exclusion.toString().c_str());
				}
			} else if (std::any_of(notExcludedServers.begin(), notExcludedServers.end(), [&](const NetworkAddress& a) {
				           return addressExcluded({ exclusion }, a);
			           })) {
				if (exclusion.port == 0) {
					fprintf(stderr,
					        "  %s(Whole machine)  ---- WARNING: Exclusion in progress! It is not safe to remove this "
					        "machine from the cluster\n",
					        exclusion.ip.toString().c_str());
				} else {
					fprintf(stderr,
					        "  %s  ---- WARNING: Exclusion in progress! It is not safe to remove this process from the "
					        "cluster\n",
					        exclusion.toString().c_str());
				}
			} else {
				if (exclusion.port == 0) {
					printf("  %s(Whole machine)  ---- Successfully excluded. It is now safe to remove this machine "
					       "from the cluster.\n",
					       exclusion.ip.toString().c_str());
				} else {
					printf(
					    "  %s  ---- Successfully excluded. It is now safe to remove this process from the cluster.\n",
					    exclusion.toString().c_str());
				}
			}
		}

		for (const auto& locality : noMatchLocalities) {
			fprintf(
			    stderr,
			    "  %s  ---- WARNING: Currently no servers found with this locality match! Be sure that you excluded "
			    "the correct locality.\n",
			    locality.c_str());
		}

		wait(checkForCoordinators(db, exclusionVector));
		return true;
	}
}

CommandFactory excludeFactory(
    "exclude",
    CommandHelp(
        "exclude [FORCE] [failed] [no_wait] [<ADDRESS...>] [locality_dcid:<excludedcid>]\n"
        "        [locality_zoneid:<excludezoneid>] [locality_machineid:<excludemachineid>]\n"
        "        [locality_processid:<excludeprocessid>] [locality_<KEY>:<localtyvalue>]",
        "exclude servers from the database by IP address or locality",
        "If no addresses or localities are specified, lists the set of excluded addresses and localities.\n"
        "\n"
        "For each IP address or IP:port pair in <ADDRESS...> and/or each locality attribute (like dcid, "
        "zoneid, machineid, processid), adds the address/locality to the set of exclusions and waits until all "
        "database state has been safely moved away from affected servers.\n"
        "\n"
        "If 'FORCE' is set, the command does not perform safety checks before excluding.\n"
        "\n"
        "If 'no_wait' is set, the command returns immediately without checking if the exclusions have completed "
        "successfully.\n"
        "\n"
        "If 'failed' is set, the cluster will immediately forget all data associated with the excluded processes. "
        "Doing so can be helpful if the process is not expected to recover, as it will allow the cluster to delete "
        "state that would be needed to catch the failed process up. Re-including a process excluded with 'failed' will "
        "result in it joining as an empty process.\n"
        "\n"
        "If a cluster has failed storage servers that result in all replicas of some data being permanently gone, "
        "'exclude failed' can be used to clean up the affected key ranges by restoring them to empty.\n"
        "\n"
        "WARNING: use of 'exclude failed' can result in data loss. If an excluded server contains the last replica of "
        "some data, then using the 'failed' option will permanently remove that data from the cluster."));
} // namespace fdb_cli
