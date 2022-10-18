/*
 * TenantGroupCommands.actor.cpp
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
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

// tenantgroup list command
ACTOR Future<bool> tenantGroupListCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 5) {
		fmt::print("Usage: tenantgroup list [BEGIN] [END] [LIMIT]\n\n");
		fmt::print("Lists the tenant groups in a cluster.\n");
		fmt::print("Only tenant groups in the range BEGIN - END will be printed.\n");
		fmt::print("An optional LIMIT can be specified to limit the number of results (default 100).\n");
		return false;
	}

	state StringRef beginTenantGroup = ""_sr;
	state StringRef endTenantGroup = "\xff\xff"_sr;
	state int limit = 100;

	if (tokens.size() >= 3) {
		beginTenantGroup = tokens[2];
	}
	if (tokens.size() >= 4) {
		endTenantGroup = tokens[3];
		if (endTenantGroup <= beginTenantGroup) {
			fmt::print(stderr, "ERROR: end must be larger than begin");
			return false;
		}
	}
	if (tokens.size() == 5) {
		int n = 0;
		if (sscanf(tokens[4].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[4].size() || limit <= 0) {
			fmt::print(stderr, "ERROR: invalid limit `{}'\n", tokens[4].toString());
			return false;
		}
	}

	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::vector<TenantGroupName> tenantGroupNames;
			state std::vector<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroups;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(store(tenantGroups,
				           MetaclusterAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, limit)));
			} else {
				wait(store(tenantGroups,
				           TenantAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, limit)));
			}

			if (tenantGroups.empty()) {
				if (tokens.size() == 2) {
					fmt::print("The cluster has no tenant groups\n");
				} else {
					fmt::print("The cluster has no tenant groups in the specified range\n");
				}
			}

			int index = 0;
			for (auto tenantGroup : tenantGroups) {
				fmt::print("  {}. {}\n", ++index, printable(tenantGroup.first));
			}

			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// tenantgroup get command
ACTOR Future<bool> tenantGroupGetCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 4 || (tokens.size() == 4 && tokens[3] != "JSON"_sr)) {
		fmt::print("Usage: tenantgroup get <NAME> [JSON]\n\n");
		fmt::print("Prints metadata associated with the given tenant group.\n");
		fmt::print("If JSON is specified, then the output will be in JSON format.\n");
		return false;
	}

	state bool useJson = tokens.size() == 4;
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::string tenantJson;
			state Optional<TenantGroupEntry> entry;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(store(entry, MetaclusterAPI::tryGetTenantGroupTransaction(tr, tokens[2])));
			} else {
				wait(store(entry, TenantAPI::tryGetTenantGroupTransaction(tr, tokens[2])));
				Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				// We don't store assigned clusters in the tenant group entry on data clusters, so we can instead
				// populate it from the metacluster registration
				if (entry.present() && metaclusterRegistration.present() &&
				    metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA &&
				    !entry.get().assignedCluster.present()) {
					entry.get().assignedCluster = metaclusterRegistration.get().name;
				}
			}

			if (!entry.present()) {
				throw tenant_not_found();
			}

			if (useJson) {
				json_spirit::mObject resultObj;
				resultObj["tenant_group"] = entry.get().toJson();
				resultObj["type"] = "success";
				fmt::print("{}\n",
				           json_spirit::write_string(json_spirit::mValue(resultObj), json_spirit::pretty_print));
			} else {
				if (entry.get().assignedCluster.present()) {
					fmt::print("  assigned cluster: {}\n", printable(entry.get().assignedCluster));
				} else {
					// This is a placeholder output for when a tenant group is read in a non-metacluster, where
					// it currently has no metadata. When metadata is eventually added, we can print that instead.
					fmt::print("The tenant group is present in the cluster\n");
				}
			}
			return true;
		} catch (Error& e) {
			try {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			} catch (Error& finalErr) {
				state std::string errorStr;
				if (finalErr.code() == error_code_tenant_not_found) {
					errorStr = "tenant group not found";
				} else if (useJson) {
					errorStr = finalErr.what();
				} else {
					throw finalErr;
				}

				if (useJson) {
					json_spirit::mObject resultObj;
					resultObj["type"] = "error";
					resultObj["error"] = errorStr;
					fmt::print("{}\n",
					           json_spirit::write_string(json_spirit::mValue(resultObj), json_spirit::pretty_print));
				} else {
					fmt::print(stderr, "ERROR: {}\n", errorStr);
				}

				return false;
			}
		}
	}
}

// tenantgroup command
Future<bool> tenantGroupCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return true;
	} else if (tokencmp(tokens[1], "list")) {
		return tenantGroupListCommand(db, tokens);
	} else if (tokencmp(tokens[1], "get")) {
		return tenantGroupGetCommand(db, tokens);
	} else {
		printUsage(tokens[0]);
		return true;
	}
}

void tenantGroupGenerator(const char* text,
                          const char* line,
                          std::vector<std::string>& lc,
                          std::vector<StringRef> const& tokens) {
	if (tokens.size() == 1) {
		const char* opts[] = { "list", "get", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() == 3 && tokencmp(tokens[1], "get")) {
		const char* opts[] = { "JSON", nullptr };
		arrayGenerator(text, line, opts, lc);
	}
}

std::vector<const char*> tenantGroupHintGenerator(std::vector<StringRef> const& tokens, bool inArgument) {
	if (tokens.size() == 1) {
		return { "<list|get>", "[ARGS]" };
	} else if (tokencmp(tokens[1], "list") && tokens.size() < 5) {
		static std::vector<const char*> opts = { "[BEGIN]", "[END]", "[LIMIT]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "get") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<NAME>", "[JSON]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else {
		return {};
	}
}

CommandFactory tenantGroupRegisterFactory("tenantgroup",
                                          CommandHelp("tenantgroup <list|get> [ARGS]",
                                                      "view tenant group information",
                                                      "`list' prints a list of tenant groups in the cluster.\n"
                                                      "`get' prints the metadata for a particular tenant group.\n"),
                                          &tenantGroupGenerator,
                                          &tenantGroupHintGenerator);

} // namespace fdb_cli
