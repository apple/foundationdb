/*
 * ExcludeCommand.cpp
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

#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace {

// Exclude the given servers and localities
Future<bool> excludeServersAndLocalities(Reference<IDatabase> db,
                                         std::vector<AddressExclusion> servers,
                                         std::unordered_set<std::string> localities,
                                         bool failed,
                                         bool force) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Error err;
		try {
			if (force && !servers.empty())
				tr->set(failed ? fdb_cli::failedForceOptionSpecialKey : fdb_cli::excludedForceOptionSpecialKey,
				        ValueRef());
			for (const auto& s : servers) {
				Key addr = failed ? fdb_cli::failedServersSpecialKeyRange.begin.withSuffix(s.toString())
				                  : fdb_cli::excludedServersSpecialKeyRange.begin.withSuffix(s.toString());
				tr->set(addr, ValueRef());
			}
			if (force && !localities.empty())
				tr->set(failed ? fdb_cli::failedLocalityForceOptionSpecialKey
				               : fdb_cli::excludedLocalityForceOptionSpecialKey,
				        ValueRef());
			for (const auto& l : localities) {
				Key addr = failed ? fdb_cli::failedLocalitySpecialKeyRange.begin.withSuffix(l)
				                  : fdb_cli::excludedLocalitySpecialKeyRange.begin.withSuffix(l);
				tr->set(addr, ValueRef());
			}
			co_await safeThreadFutureToFuture(tr->commit());
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_special_keys_api_failure) {
			std::string errorMsgStr = co_await fdb_cli::getSpecialKeysFailureErrorMessage(tr);
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

AsyncResult<std::vector<std::string>> getExcludedServers(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::excludedServersSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> exclusions;
			for (const auto& i : r) {
				auto addr = i.key.removePrefix(fdb_cli::excludedServersSpecialKeyRange.begin).toString();
				exclusions.push_back(addr);
			}
			co_return exclusions;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevWarn, "GetExcludedServersError").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

// Get the list of excluded localities by reading the keys.
AsyncResult<std::vector<std::string>> getExcludedLocalities(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::excludedLocalitySpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> excludedLocalities;
			for (const auto& i : r) {
				auto locality = i.key.removePrefix(fdb_cli::excludedLocalitySpecialKeyRange.begin).toString();
				excludedLocalities.push_back(locality);
			}
			co_return excludedLocalities;
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

AsyncResult<std::vector<std::string>> getFailedServers(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::failedServersSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> exclusions;
			for (const auto& i : r) {
				auto addr = i.key.removePrefix(fdb_cli::failedServersSpecialKeyRange.begin).toString();
				exclusions.push_back(addr);
			}
			co_return exclusions;
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

// Get the list of failed localities by reading the keys.
AsyncResult<std::vector<std::string>> getFailedLocalities(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::failedLocalitySpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult r = co_await safeThreadFutureToFuture(resultFuture);
			ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<std::string> excludedLocalities;
			for (const auto& i : r) {
				auto locality = i.key.removePrefix(fdb_cli::failedLocalitySpecialKeyRange.begin).toString();
				excludedLocalities.push_back(locality);
			}
			co_return excludedLocalities;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevWarn, "GetExcludedLocalitiesError").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<std::set<NetworkAddress>> getInProgressExclusion(Reference<ITransaction> tr) {
	ThreadFuture<RangeResult> resultFuture =
	    tr->getRange(fdb_cli::exclusionInProgressSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
	RangeResult result = co_await safeThreadFutureToFuture(resultFuture);
	ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
	std::set<NetworkAddress> inProgressExclusion;
	for (const auto& addr : result) {
		inProgressExclusion.insert(
		    NetworkAddress::parse(addr.key.removePrefix(fdb_cli::exclusionInProgressSpecialKeyRange.begin).toString()));
	}
	co_return inProgressExclusion;
}

Future<std::set<NetworkAddress>> checkForExcludingServers(Reference<IDatabase> db,
                                                          std::set<AddressExclusion> exclusions,
                                                          bool waitForAllExcluded) {
	std::set<NetworkAddress> inProgressExclusion;
	Reference<ITransaction> tr = db->createTransaction();
	tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	while (true) {
		inProgressExclusion.clear();
		Error err;
		bool hasErr = false;
		try {
			std::set<NetworkAddress> result = co_await getInProgressExclusion(tr);
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
			err = e;
			hasErr = true;
		}
		if (hasErr) {
			TraceEvent(SevWarn, "CheckForExcludingServersError").error(err);
			co_await safeThreadFutureToFuture(tr->onError(err));
		}
	}
	co_return inProgressExclusion;
}

Future<Void> checkForCoordinators(Reference<IDatabase> db, std::set<AddressExclusion> exclusions) {

	bool foundCoordinator = false;
	std::vector<NetworkAddress> coordinatorList;
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			// Hold the reference to the standalone's memory
			ThreadFuture<Optional<Value>> coordinatorsF = tr->get(fdb_cli::coordinatorsProcessSpecialKey);
			Optional<Value> coordinators = co_await safeThreadFutureToFuture(coordinatorsF);
			ASSERT(coordinators.present());
			coordinatorList = NetworkAddress::parseList(coordinators.get().toString());
			break;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevWarn, "CheckForCoordinatorsError").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}

	for (const auto& c : coordinatorList) {
		if (exclusions.find(AddressExclusion(c.ip, c.port)) != exclusions.end() ||
		    exclusions.find(AddressExclusion(c.ip)) != exclusions.end()) {
			fprintf(stderr, "WARNING: %s is a coordinator!\n", c.toString().c_str());
			foundCoordinator = true;
		}
	}
	if (foundCoordinator)
		printf("Type `help coordinators' for information on how to change the\n"
		       "cluster's coordination servers before removing them.\n");
}

} // namespace

namespace fdb_cli {

const KeyRangeRef excludedServersSpecialKeyRange("\xff\xff/management/excluded/"_sr,
                                                 "\xff\xff/management/excluded0"_sr);
const KeyRangeRef failedServersSpecialKeyRange("\xff\xff/management/failed/"_sr, "\xff\xff/management/failed0"_sr);
const KeyRangeRef excludedLocalitySpecialKeyRange("\xff\xff/management/excluded_locality/"_sr,
                                                  "\xff\xff/management/excluded_locality0"_sr);
const KeyRangeRef failedLocalitySpecialKeyRange("\xff\xff/management/failed_locality/"_sr,
                                                "\xff\xff/management/failed_locality0"_sr);
const KeyRef excludedForceOptionSpecialKey = "\xff\xff/management/options/excluded/force"_sr;
const KeyRef failedForceOptionSpecialKey = "\xff\xff/management/options/failed/force"_sr;
const KeyRef excludedLocalityForceOptionSpecialKey = "\xff\xff/management/options/excluded_locality/force"_sr;
const KeyRef failedLocalityForceOptionSpecialKey = "\xff\xff/management/options/failed_locality/force"_sr;
const KeyRangeRef exclusionInProgressSpecialKeyRange("\xff\xff/management/in_progress_exclusion/"_sr,
                                                     "\xff\xff/management/in_progress_exclusion0"_sr);

Future<bool> excludeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens, Future<Void> _warn) {
	auto warn = std::move(_warn);
	if (tokens.size() <= 1) {
		std::vector<std::string> excludedAddresses = co_await getExcludedServers(db);
		std::vector<std::string> excludedLocalities = co_await getExcludedLocalities(db);
		std::vector<std::string> failedAddresses = co_await getFailedServers(db);
		std::vector<std::string> failedLocalities = co_await getFailedLocalities(db);

		if (excludedAddresses.empty() && excludedLocalities.empty() && failedAddresses.empty() &&
		    failedLocalities.empty()) {
			printf("There are currently no servers or localities excluded from the database.\n"
			       "To learn how to exclude a server, type `help exclude'.\n");
			co_return true;
		}

		printf("There are currently %zu servers or localities being excluded from the database:\n",
		       excludedAddresses.size() + excludedLocalities.size());
		for (const auto& e : excludedAddresses)
			printf("  %s\n", e.c_str());
		for (const auto& e : excludedLocalities)
			printf("  %s\n", e.c_str());

		if (!excludedAddresses.empty() || !excludedLocalities.empty()) {
			printf("To find out whether it is safe to remove one or more of these\n"
			       "servers from the cluster, type `exclude <addresses>'.\n"
			       "To return one of these servers to the cluster, type `include <addresses>'.\n");
		}

		printf("\n");

		printf("There are currently %zu servers or localities marked as failed in the database:\n",
		       failedAddresses.size() + failedLocalities.size());
		for (const auto& f : failedAddresses)
			printf("  %s\n", f.c_str());
		for (const auto& f : failedLocalities)
			printf("  %s\n", f.c_str());

		if (!failedAddresses.empty() || !failedLocalities.empty()) {
			printf("To return one of these servers to the cluster, type `include failed <addresses>'.\n");
		}

		printf("\n");

		Reference<ITransaction> tr = db->createTransaction();
		std::set<NetworkAddress> inProgressExclusion = co_await getInProgressExclusion(tr);
		printf("There are currently %zu processes for which exclusion is in progress:\n", inProgressExclusion.size());
		for (const auto& addr : inProgressExclusion) {
			printf("%s\n", addr.toString().c_str());
		}

		co_return true;
	} else {
		std::set<AddressExclusion> exclusionSet;
		std::vector<AddressExclusion> exclusionAddresses;
		std::unordered_set<std::string> exclusionLocalities;
		std::vector<std::string> noMatchLocalities;
		bool force = false;
		bool waitForAllExcluded = true;
		bool markFailed = false;
		std::vector<ProcessData> workers;
		std::map<std::string, StorageServerInterface> server_interfaces;
		Future<bool> future_workers = fdb_cli::getWorkers(db, &workers);
		Future<Void> future_server_interfaces = fdb_cli::getStorageServerInterfaces(db, &server_interfaces);
		co_await (success(future_workers) && success(future_server_interfaces));

		for (auto t = tokens.begin() + 1; t != tokens.end(); ++t) {
			if (*t == "FORCE"_sr) {
				force = true;
			} else if (*t == "no_wait"_sr) {
				waitForAllExcluded = false;
			} else if (*t == "failed"_sr) {
				markFailed = true;
			} else if (t->startsWith(LocalityData::ExcludeLocalityPrefix) &&
			           t->toString().find(':') != std::string::npos) {
				exclusionLocalities.insert(t->toString());
				auto localityAddresses = getAddressesByLocality(workers, t->toString());
				auto localityServerAddresses = getServerAddressesByLocality(server_interfaces, t->toString());
				if (localityAddresses.empty() && localityServerAddresses.empty()) {
					noMatchLocalities.push_back(t->toString());
					continue;
				}

				if (!localityAddresses.empty()) {
					exclusionSet.insert(localityAddresses.begin(), localityAddresses.end());
				}

				if (!localityServerAddresses.empty()) {
					exclusionSet.insert(localityServerAddresses.begin(), localityServerAddresses.end());
				}
			} else {
				auto a = AddressExclusion::parse(*t);
				if (!a.isValid()) {
					fprintf(stderr,
					        "ERROR: '%s' is neither a valid network endpoint address nor a locality\n",
					        t->toString().c_str());
					if (t->toString().find(":tls") != std::string::npos)
						printf("        Do not include the `:tls' suffix when naming a process\n");
					co_return true;
				}
				exclusionSet.insert(a);
				exclusionAddresses.push_back(a);
			}
		}

		// The validation if a locality or address has no match is done below and will result in a warning. If we abort
		// here the provided locality and/or address will not be excluded.
		if (exclusionAddresses.empty() && exclusionLocalities.empty()) {
			fprintf(stderr, "ERROR: At least one valid network endpoint address or a locality must be provided\n");
			co_return false;
		}

		bool res = co_await excludeServersAndLocalities(db, exclusionAddresses, exclusionLocalities, markFailed, force);
		if (!res)
			co_return false;

		if (waitForAllExcluded) {
			printf("Waiting for state to be removed from all excluded servers. This may take a while.\n");
			printf("(Interrupting this wait with CTRL+C will not cancel the data movement.)\n");
		}

		if (warn.isValid())
			warn.cancel();

		std::set<NetworkAddress> notExcludedServers =
		    co_await checkForExcludingServers(db, exclusionSet, waitForAllExcluded);
		std::map<IPAddress, std::set<uint16_t>> workerPorts;
		for (const auto& addr : workers)
			workerPorts[addr.address.ip].insert(addr.address.port);

		// Print a list of all excluded addresses that don't have a corresponding worker
		std::set<AddressExclusion> absentExclusions;
		for (const auto& addr : exclusionSet) {
			auto worker = workerPorts.find(addr.ip);
			if (worker == workerPorts.end())
				absentExclusions.insert(addr);
			else if (addr.port > 0 && !worker->second.contains(addr.port))
				absentExclusions.insert(addr);
		}

		for (const auto& exclusion : exclusionSet) {
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

		co_await checkForCoordinators(db, exclusionSet);
		co_return true;
	}
}

CommandFactory excludeFactory(
    "exclude",
    CommandHelp(
        "exclude [FORCE] [failed] [no_wait] [<ADDRESS...>] [locality_dcid:<excludedcid>]\n"
        "        [locality_zoneid:<excludezoneid>] [locality_machineid:<excludemachineid>]\n"
        "        [locality_processid:<excludeprocessid>] [locality_<KEY>:<localtyvalue>]",
        "exclude servers from the database by IP address or locality",
        "If no addresses or localities are specified, lists the set of excluded addresses and localities in addition "
        "to addresses for which exclusion is in progress.\n"
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
