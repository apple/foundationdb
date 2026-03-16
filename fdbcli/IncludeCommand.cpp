/*
 * IncludeCommand.cpp
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

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace {

// Remove the given localities from the exclusion list.
// include localities by clearing the keys.
Future<Void> includeLocalities(Reference<IDatabase> db,
                               std::vector<std::string> localities,
                               bool failed,
                               bool includeAll) {
	std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			if (includeAll) {
				if (failed) {
					tr->clear(fdb_cli::failedLocalitySpecialKeyRange);
				} else {
					tr->clear(fdb_cli::excludedLocalitySpecialKeyRange);
				}
			} else {
				for (const auto& l : localities) {
					Key locality = failed ? fdb_cli::failedLocalitySpecialKeyRange.begin.withSuffix(l)
					                      : fdb_cli::excludedLocalitySpecialKeyRange.begin.withSuffix(l);
					tr->clear(locality);
				}
			}
			co_await safeThreadFutureToFuture(tr->commit());
			co_return;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent("IncludeLocalitiesError").errorUnsuppressed(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<Void> includeServers(Reference<IDatabase> db, std::vector<AddressExclusion> servers, bool failed) {
	std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			for (auto& s : servers) {
				// include all, just clear the whole key range
				if (!s.isValid()) {
					if (failed) {
						tr->clear(fdb_cli::failedServersSpecialKeyRange);
					} else {
						tr->clear(fdb_cli::excludedServersSpecialKeyRange);
					}
				} else {
					Key addr = failed ? fdb_cli::failedServersSpecialKeyRange.begin.withSuffix(s.toString())
					                  : fdb_cli::excludedServersSpecialKeyRange.begin.withSuffix(s.toString());
					tr->clear(addr);
					// Eliminate both any ip-level exclusion (1.2.3.4) and any
					// port-level exclusions (1.2.3.4:5)
					// The range ['IP', 'IP;'] was originally deleted. ';' is
					// char(':' + 1). This does not work, as other for all
					// x between 0 and 9, 'IPx' will also be in this range.
					//
					// This is why we now make two clears: first only of the ip
					// address, the second will delete all ports.
					if (s.isWholeMachine())
						tr->clear(KeyRangeRef(addr.withSuffix(":"_sr), addr.withSuffix(";"_sr)));
				}
			}
			co_await safeThreadFutureToFuture(tr->commit());
			co_return;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent("IncludeServersError").errorUnsuppressed(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

// Includes the servers that could be IP addresses or localities back to the cluster.
Future<bool> include(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	std::vector<AddressExclusion> addresses;
	std::vector<std::string> localities;
	bool failed = false;
	bool all = false;
	for (auto t = tokens.begin() + 1; t != tokens.end(); ++t) {
		if (*t == "all"_sr) {
			all = true;
		} else if (*t == "failed"_sr) {
			failed = true;
		} else if (t->startsWith(LocalityData::ExcludeLocalityPrefix) && t->toString().find(':') != std::string::npos) {
			// if the token starts with 'locality_' prefix.
			localities.push_back(t->toString());
		} else {
			auto a = AddressExclusion::parse(*t);
			if (!a.isValid()) {
				fprintf(stderr,
				        "ERROR: '%s' is neither a valid network endpoint address nor a locality\n",
				        t->toString().c_str());
				if (t->toString().find(":tls") != std::string::npos)
					printf("        Do not include the `:tls' suffix when naming a process\n");
				co_return false;
			}
			addresses.push_back(a);
		}
	}
	if (all) {
		std::vector<AddressExclusion> includeAll;
		includeAll.push_back(AddressExclusion());
		co_await includeServers(db, includeAll, failed);
		co_await includeLocalities(db, localities, failed, all);
	} else {
		if (!addresses.empty()) {
			co_await includeServers(db, addresses, failed);
		}
		if (!localities.empty()) {
			// include the servers that belong to given localities.
			co_await includeLocalities(db, localities, failed, all);
		}
	}
	co_return true;
};

} // namespace

namespace fdb_cli {

Future<bool> includeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 2) {
		printUsage(tokens[0]);
		co_return false;
	} else {
		bool result = co_await include(db, tokens);
		co_return result;
	}
}

CommandFactory includeFactory(
    "include",
    CommandHelp(
        "include all|[<ADDRESS...>] [locality_dcid:<excludedcid>] [locality_zoneid:<excludezoneid>] "
        "[locality_machineid:<excludemachineid>] [locality_processid:<excludeprocessid>] or any locality data",
        "permit previously-excluded servers and localities to rejoin the database",
        "If `all' is specified, the excluded servers and localities list is cleared.\n\nFor each IP address or IP:port "
        "pair in <ADDRESS...> or any LocalityData (like dcid, zoneid, machineid, processid), removes any "
        "matching exclusions from the excluded servers and localities list. "
        "(A specified IP will match all IP:* exclusion entries)"));
} // namespace fdb_cli
