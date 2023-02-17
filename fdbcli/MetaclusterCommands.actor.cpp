/*
 * MetaclusterCommands.actor.cpp
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
#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>

namespace fdb_cli {

Optional<std::pair<Optional<ClusterConnectionString>, Optional<DataClusterEntry>>> parseClusterConfiguration(
    std::vector<StringRef> const& tokens,
    DataClusterEntry const& defaults,
    int startIndex,
    int endIndex) {
	Optional<DataClusterEntry> entry;
	Optional<ClusterConnectionString> connectionString;

	std::set<std::string> usedParams;
	for (int tokenNum = startIndex; tokenNum < endIndex; ++tokenNum) {
		StringRef token = tokens[tokenNum];
		bool foundEquals;
		StringRef param = token.eat("=", &foundEquals);
		if (!foundEquals) {
			fmt::print(stderr,
			           "ERROR: invalid configuration string `{}'. String must specify a value using `='.\n",
			           param.toString().c_str());
			return {};
		}
		std::string value = token.toString();
		if (!usedParams.insert(value).second) {
			fmt::print(
			    stderr, "ERROR: configuration parameter `{}' specified more than once.\n", param.toString().c_str());
			return {};
		}
		if (tokencmp(param, "max_tenant_groups")) {
			entry = defaults;

			int n;
			if (sscanf(value.c_str(), "%d%n", &entry.get().capacity.numTenantGroups, &n) != 1 || n != value.size() ||
			    entry.get().capacity.numTenantGroups < 0) {
				fmt::print(stderr, "ERROR: invalid number of tenant groups `{}'.\n", value.c_str());
				return {};
			}
		} else if (tokencmp(param, "connection_string")) {
			connectionString = ClusterConnectionString(value);
		} else {
			fmt::print(stderr, "ERROR: unrecognized configuration parameter `{}'.\n", param.toString().c_str());
			return {};
		}
	}

	return std::make_pair(connectionString, entry);
}

void printMetaclusterConfigureOptionsUsage() {
	fmt::print("max_tenant_groups sets the maximum number of tenant groups that can be assigned\n"
	           "to the named data cluster.\n");
	fmt::print("connection_string sets the connection string for the named data cluster.\n");
}

// metacluster create command
ACTOR Future<bool> metaclusterCreateCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 4) {
		fmt::print("Usage: metacluster create_experimental <NAME> <TENANT_ID_PREFIX>\n\n");
		fmt::print("Configures the cluster to be a management cluster in a metacluster.\n");
		fmt::print("NAME is an identifier used to distinguish this metacluster from other metaclusters.\n");
		fmt::print("TENANT_ID_PREFIX is an integer in the range [0,32767] inclusive which is prepended to all tenant "
		           "ids in the metacluster.\n");
		return false;
	}

	int64_t tenantIdPrefix = std::stoi(tokens[3].toString());
	if (tenantIdPrefix < TenantAPI::TENANT_ID_PREFIX_MIN_VALUE ||
	    tenantIdPrefix > TenantAPI::TENANT_ID_PREFIX_MAX_VALUE) {
		fmt::print("TENANT_ID_PREFIX must be in the range [0,32767] inclusive\n");
		return false;
	}

	Optional<std::string> errorStr = wait(MetaclusterAPI::createMetacluster(db, tokens[2], tenantIdPrefix));
	if (errorStr.present()) {
		fmt::print("ERROR: {}.\n", errorStr.get());
	} else {
		fmt::print("The cluster has been configured as a metacluster.\n");
	}
	return true;
}

// metacluster decommission command
ACTOR Future<bool> metaclusterDecommissionCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 2) {
		fmt::print("Usage: metacluster decommission\n\n");
		fmt::print("Converts the current cluster from a metacluster management cluster back into an\n");
		fmt::print("ordinary cluster. It must be called on a cluster with no registered data clusters.\n");
		return false;
	}

	wait(MetaclusterAPI::decommissionMetacluster(db));

	fmt::print("The cluster is no longer a metacluster.\n");
	return true;
}

// metacluster register command
ACTOR Future<bool> metaclusterRegisterCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 4) {
		fmt::print("Usage: metacluster register <NAME> connection_string=<CONNECTION_STRING>\n"
		           "[max_tenant_groups=<NUM_GROUPS>]\n\n");
		fmt::print("Adds a data cluster to a metacluster.\n");
		fmt::print("NAME is used to identify the cluster in future commands.\n");
		printMetaclusterConfigureOptionsUsage();
		return false;
	}

	DataClusterEntry defaultEntry;
	auto config = parseClusterConfiguration(tokens, defaultEntry, 3, tokens.size());
	if (!config.present()) {
		return false;
	} else if (!config.get().first.present()) {
		fmt::print(stderr, "ERROR: connection_string must be configured when registering a cluster.\n");
		return false;
	}

	wait(MetaclusterAPI::registerCluster(
	    db, tokens[2], config.get().first.get(), config.get().second.orDefault(defaultEntry)));

	fmt::print("The cluster `{}' has been added\n", printable(tokens[2]).c_str());
	return true;
}

// metacluster remove command
ACTOR Future<bool> metaclusterRemoveCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 3 || tokens.size() > 4 || (tokens.size() == 4 && tokens[2] != "FORCE"_sr)) {
		fmt::print("Usage: metacluster remove [FORCE] <NAME> \n\n");
		fmt::print("Removes the specified data cluster from a metacluster.\n");
		fmt::print("If FORCE is specified, then the cluster will be detached even if it has\n"
		           "tenants assigned to it.\n");
		fmt::print("If run on a data cluster, the data cluster will remove its association\n"
		           "with the metacluster without modifying the management cluster. Doing so\n"
		           "requires the FORCE option to be set. Use of this mode is required to\n"
		           "repopulate a management cluster from a data cluster using the\n"
		           "`metacluster restore' command.\n");
		return false;
	}

	state ClusterNameRef clusterName = tokens[tokens.size() - 1];
	state bool force = tokens.size() == 4;

	state ClusterType clusterType = wait(runTransaction(db, [](Reference<ITransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		return TenantAPI::getClusterType(tr);
	}));

	if (clusterType == ClusterType::METACLUSTER_DATA && !force) {
		if (tokens[2] == "FORCE"_sr) {
			fmt::print("ERROR: a cluster name must be specified.\n");
		} else {
			fmt::print("ERROR: cannot remove a data cluster directly. To remove a data cluster,\n"
			           "use the `remove' command on the management cluster. To force a data cluster\n"
			           "to forget its metacluster association without fully removing it, use FORCE.\n");
		}

		return false;
	}

	bool updatedDataCluster =
	    wait(MetaclusterAPI::removeCluster(db, clusterName, clusterType, tokens.size() == 4, 15.0));

	if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
		fmt::print("The cluster `{}' has been removed\n", printable(clusterName).c_str());
		if (!updatedDataCluster) {
			fmt::print("WARNING: the data cluster could not be updated and may still contains its\n"
			           "metacluster registration info. To finish removing it, FORCE remove the\n"
			           "data cluster directly.\n");
		}
	} else {
		ASSERT(updatedDataCluster);
		fmt::print("The cluster `{}' has removed its association with its metacluster.\n"
		           "The metacluster has not been modified.\n",
		           printable(clusterName).c_str());
	}
	return true;
}

void printRestoreUsage() {
	fmt::print("Usage: metacluster restore <NAME> [dryrun] connection_string=<CONNECTION_STRING>\n"
	           "<restore_known_data_cluster|repopulate_from_data_cluster> [force_join_new_metacluster]\n\n");

	fmt::print("Add a restored data cluster back to a metacluster.\n\n");

	fmt::print("Use `dryrun' to report what changes a restore would make and whether any\n");
	fmt::print("failures would occur. Without `dryrun', the restore will modify the metacluster\n");
	fmt::print("with the changes required to perform the restore.\n\n");

	fmt::print("Use `restore_known_data_cluster' to add back a restored copy of a data cluster\n");
	fmt::print("that the metacluster is already tracking. This mode should be used if only data\n");
	fmt::print("clusters are being restored, and any discrepancies between the management and\n");
	fmt::print("data clusters will be resolved using the management cluster metadata.\n");
	fmt::print("If `force_join_new_metacluster' is specified, the cluster will try to restore\n");
	fmt::print("to a different metacluster than it was originally registered to.\n\n");

	fmt::print("Use `repopulate_from_data_cluster' to rebuild a lost management cluster from the\n");
	fmt::print("data clusters in a metacluster. This mode should be used if the management\n");
	fmt::print("cluster is being restored. If any data clusters are also being restored, the\n");
	fmt::print("oldest data clusters should be added first before any non-recovered data\n");
	fmt::print("clusters. Any conflicts arising between the added data cluster and existing data\n");
	fmt::print("will cause the restore to fail. Before repopulating a metacluster from a data\n");
	fmt::print("cluster, that data cluster needs to be detached from its prior metacluster using\n");
	fmt::print("the `metacluster remove' command.\n");
}

// metacluster restore command
ACTOR Future<bool> metaclusterRestoreCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 5 || tokens.size() > 7) {
		printRestoreUsage();
		return false;
	}

	state bool dryRun = tokens[3] == "dryrun"_sr;
	state bool forceJoin = tokens[tokens.size() - 1] == "force_join_new_metacluster"_sr;

	if (tokens.size() < 5 + (int)dryRun + (int)forceJoin) {
		printRestoreUsage();
		return false;
	}

	state ClusterName clusterName = tokens[2];
	state StringRef restoreType = tokens[tokens.size() - 1 - (int)forceJoin];

	// connection string
	DataClusterEntry defaultEntry;
	auto config = parseClusterConfiguration(tokens, defaultEntry, 3 + (int)dryRun, 3 + (int)dryRun + 1);
	if (!config.present()) {
		return false;
	} else if (!config.get().first.present()) {
		fmt::print(stderr, "ERROR: connection_string must be configured when registering a cluster.\n");
		return false;
	}

	state std::vector<std::string> messages;
	state bool success = true;
	state UID restoreId = deterministicRandom()->randomUniqueID();

	try {
		if (restoreType == "restore_known_data_cluster"_sr) {
			wait(MetaclusterAPI::restoreCluster(db,
			                                    clusterName,
			                                    restoreId,
			                                    config.get().first.get(),
			                                    ApplyManagementClusterUpdates::True,
			                                    RestoreDryRun(dryRun),
			                                    ForceJoinNewMetacluster(forceJoin),
			                                    &messages));
		} else if (restoreType == "repopulate_from_data_cluster"_sr) {
			wait(MetaclusterAPI::restoreCluster(db,
			                                    clusterName,
			                                    restoreId,
			                                    config.get().first.get(),
			                                    ApplyManagementClusterUpdates::False,
			                                    RestoreDryRun(dryRun),
			                                    ForceJoinNewMetacluster(forceJoin),
			                                    &messages));
		} else {
			fmt::print(stderr, "ERROR: unrecognized restore mode `{}'\n", printable(restoreType));
			success = false;
		}
	} catch (Error& e) {
		success = false;
		fmt::print(stderr, "ERROR: {} ({})\n", e.what(), e.code());
	}

	if (!messages.empty()) {
		fmt::print(success ? stdout : stderr, "\nThe restore reported the following messages:\n\n");
		for (int i = 0; i < messages.size(); ++i) {
			fmt::print(success ? stdout : stderr, "  {}. {}\n", i + 1, messages[i]);
		}

		if (success) {
			fmt::print("\n");
		}
	}

	if (success) {
		if (dryRun) {
			fmt::print("The restore dry run completed successfully. To perform the restore, run the same command\n");
			fmt::print("without the `dryrun' argument.\n");
		} else {
			fmt::print("The cluster `{}' has been restored\n", printable(clusterName).c_str());
		}
	}

	return success;
}

// metacluster configure command
ACTOR Future<bool> metaclusterConfigureCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 4) {
		fmt::print("Usage: metacluster configure <NAME> <max_tenant_groups=<NUM_GROUPS>|\n"
		           "connection_string=<CONNECTION_STRING>> ...\n\n");
		fmt::print("Updates the configuration of the metacluster.\n");
		printMetaclusterConfigureOptionsUsage();
		return false;
	}

	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			Optional<DataClusterMetadata> metadata = wait(MetaclusterAPI::tryGetClusterTransaction(tr, tokens[2]));
			if (!metadata.present()) {
				throw cluster_not_found();
			}

			auto config = parseClusterConfiguration(tokens, metadata.get().entry, 3, tokens.size());
			if (!config.present()) {
				return false;
			}

			MetaclusterAPI::updateClusterMetadata(
			    tr, tokens[2], metadata.get(), config.get().first, config.get().second);

			wait(safeThreadFutureToFuture(tr->commit()));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	return true;
}

// metacluster list command
ACTOR Future<bool> metaclusterListCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 5) {
		fmt::print("Usage: metacluster list [BEGIN] [END] [LIMIT]\n\n");
		fmt::print("Lists the data clusters in a metacluster.\n");
		fmt::print("Only cluster names in the range BEGIN - END will be printed.\n");
		fmt::print("An optional LIMIT can be specified to limit the number of results (default 100).\n");
		return false;
	}

	state ClusterNameRef begin = tokens.size() > 2 ? tokens[2] : ""_sr;
	state ClusterNameRef end = tokens.size() > 3 ? tokens[3] : "\xff"_sr;
	int limit = 100;

	if (tokens.size() > 4) {
		int n = 0;
		if (sscanf(tokens[3].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[3].size() || limit < 0) {
			fmt::print(stderr, "ERROR: invalid limit {}\n", tokens[3].toString().c_str());
			return false;
		}
	}

	std::map<ClusterName, DataClusterMetadata> clusters = wait(MetaclusterAPI::listClusters(db, begin, end, limit));
	if (clusters.empty()) {
		if (tokens.size() == 2) {
			fmt::print("The metacluster has no registered data clusters\n");
		} else {
			fmt::print("The metacluster has no registered data clusters in the specified range\n");
		}
	}

	int index = 0;
	for (auto cluster : clusters) {
		fmt::print("  {}. {}\n", ++index, printable(cluster.first).c_str());
	}

	return true;
}

// metacluster get command
ACTOR Future<bool> metaclusterGetCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 4 || (tokens.size() == 4 && tokens[3] != "JSON"_sr)) {
		fmt::print("Usage: metacluster get <NAME> [JSON]\n\n");
		fmt::print("Prints metadata associated with the given data cluster.\n");
		fmt::print("If JSON is specified, then the output will be in JSON format.\n");
		return false;
	}

	state bool useJson = tokens.size() == 4;

	try {
		DataClusterMetadata metadata = wait(MetaclusterAPI::getCluster(db, tokens[2]));

		if (useJson) {
			json_spirit::mObject obj;
			obj[msgTypeKey] = "success";
			obj[msgClusterKey] = metadata.toJson();
			fmt::print("{}\n", json_spirit::write_string(json_spirit::mValue(obj), json_spirit::pretty_print).c_str());
		} else {
			fmt::print("  id: {}\n", metadata.entry.id.toString().c_str());
			fmt::print("  connection string: {}\n", metadata.connectionString.toString().c_str());
			fmt::print("  cluster state: {}\n", DataClusterEntry::clusterStateToString(metadata.entry.clusterState));
			fmt::print("  tenant group capacity: {}\n", metadata.entry.capacity.numTenantGroups);
			fmt::print("  allocated tenant groups: {}\n", metadata.entry.allocated.numTenantGroups);
		}
	} catch (Error& e) {
		if (useJson) {
			json_spirit::mObject obj;
			obj[msgTypeKey] = "error";
			obj[msgErrorKey] = e.what();
			fmt::print("{}\n", json_spirit::write_string(json_spirit::mValue(obj), json_spirit::pretty_print).c_str());
			return false;
		} else {
			throw;
		}
	}

	return true;
}

// metacluster status command
ACTOR Future<bool> metaclusterStatusCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 2 || tokens.size() > 3) {
		fmt::print("Usage: metacluster status [JSON]\n\n");
		fmt::print("Prints metacluster metadata.\n");
		fmt::print("If JSON is specified, then the output will be in JSON format.\n");
		return false;
	}

	state bool useJson = tokens.size() == 3;

	state Optional<std::string> metaclusterName;

	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<MetaclusterRegistrationEntry> registrationEntry =
			    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));
			const ClusterType clusterType =
			    !registrationEntry.present() ? ClusterType::STANDALONE : registrationEntry.get().clusterType;
			if (ClusterType::STANDALONE == clusterType) {
				if (useJson) {
					json_spirit::mObject obj;
					obj[msgTypeKey] = "success";
					obj[msgClusterTypeKey] = clusterTypeToString(clusterType);
					fmt::print("{}\n",
					           json_spirit::write_string(json_spirit::mValue(obj), json_spirit::pretty_print).c_str());
				} else {
					fmt::print("This cluster is not part of a metacluster\n");
				}
				return true;
			} else if (ClusterType::METACLUSTER_DATA == clusterType) {
				ASSERT(registrationEntry.present());
				metaclusterName = registrationEntry.get().metaclusterName.toString();
				if (useJson) {
					json_spirit::mObject obj;
					obj[msgTypeKey] = "success";
					obj[msgClusterTypeKey] = clusterTypeToString(clusterType);
					json_spirit::mObject metaclusterObj;
					metaclusterObj[msgMetaclusterName] = metaclusterName.get();
					obj[msgMetaclusterKey] = metaclusterObj;
					fmt::print("{}\n",
					           json_spirit::write_string(json_spirit::mValue(obj), json_spirit::pretty_print).c_str());
				} else {
					fmt::print("This cluster \"{}\" is a data cluster within the metacluster named \"{}\"\n",
					           registrationEntry.get().name.toString().c_str(),
					           metaclusterName.get().c_str());
				}
				return true;
			}

			metaclusterName = registrationEntry.get().metaclusterName.toString();

			ASSERT(ClusterType::METACLUSTER_MANAGEMENT == clusterType);
			std::map<ClusterName, DataClusterMetadata> clusters =
			    wait(MetaclusterAPI::listClustersTransaction(tr, ""_sr, "\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS));
			auto capacityNumbers = MetaclusterAPI::metaclusterCapacity(clusters);
			if (useJson) {
				json_spirit::mObject obj;
				obj[msgTypeKey] = "success";
				obj[msgClusterTypeKey] = clusterTypeToString(ClusterType::METACLUSTER_MANAGEMENT);

				json_spirit::mObject metaclusterObj;
				metaclusterObj[msgMetaclusterName] = metaclusterName.get();
				metaclusterObj[msgDataClustersKey] = static_cast<int>(clusters.size());
				metaclusterObj[msgCapacityKey] = capacityNumbers.first.toJson();
				metaclusterObj[msgAllocatedKey] = capacityNumbers.second.toJson();

				obj[msgMetaclusterKey] = metaclusterObj;
				fmt::print("{}\n",
				           json_spirit::write_string(json_spirit::mValue(obj), json_spirit::pretty_print).c_str());
			} else {
				fmt::print("  number of data clusters: {}\n", clusters.size());
				fmt::print("  tenant group capacity: {}\n", capacityNumbers.first.numTenantGroups);
				fmt::print("  allocated tenant groups: {}\n", capacityNumbers.second.numTenantGroups);
			}
			return true;
		} catch (Error& e) {
			if (useJson) {
				json_spirit::mObject obj;
				obj[msgTypeKey] = "error";
				obj[msgErrorKey] = e.what();
				fmt::print("{}\n",
				           json_spirit::write_string(json_spirit::mValue(obj), json_spirit::pretty_print).c_str());
				return false;
			} else {
				throw;
			}
		}
	}
}

// metacluster command
Future<bool> metaclusterCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return true;
	} else if (tokencmp(tokens[1], "create_experimental")) {
		return metaclusterCreateCommand(db, tokens);
	} else if (tokencmp(tokens[1], "decommission")) {
		return metaclusterDecommissionCommand(db, tokens);
	} else if (tokencmp(tokens[1], "register")) {
		return metaclusterRegisterCommand(db, tokens);
	} else if (tokencmp(tokens[1], "remove")) {
		return metaclusterRemoveCommand(db, tokens);
	} else if (tokencmp(tokens[1], "restore")) {
		return metaclusterRestoreCommand(db, tokens);
	} else if (tokencmp(tokens[1], "configure")) {
		return metaclusterConfigureCommand(db, tokens);
	} else if (tokencmp(tokens[1], "list")) {
		return metaclusterListCommand(db, tokens);
	} else if (tokencmp(tokens[1], "get")) {
		return metaclusterGetCommand(db, tokens);
	} else if (tokencmp(tokens[1], "status")) {
		return metaclusterStatusCommand(db, tokens);
	} else {
		printUsage(tokens[0]);
		return true;
	}
}

void metaclusterGenerator(const char* text,
                          const char* line,
                          std::vector<std::string>& lc,
                          std::vector<StringRef> const& tokens) {
	if (tokens.size() == 1) {
		const char* opts[] = { "create_experimental", "decommission", "register", "remove", "restore",
			                   "configure",           "list",         "get",      "status", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() > 1 && (tokencmp(tokens[1], "register") || tokencmp(tokens[1], "configure"))) {
		const char* opts[] = { "max_tenant_groups=", "connection_string=", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if ((tokens.size() == 2 && tokencmp(tokens[1], "status")) ||
	           (tokens.size() == 3 && tokencmp(tokens[1], "get"))) {
		const char* opts[] = { "JSON", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "remove")) {
		const char* opts[] = { "FORCE", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() > 1 && tokencmp(tokens[1], "restore")) {
		if (tokens.size() == 3) {
			const char* opts[] = { "dryrun", "connection_string=", nullptr };
			arrayGenerator(text, line, opts, lc);
		} else {
			bool dryrun = tokens[3] == "dryrun"_sr;
			if (tokens.size() == 3 + (int)dryrun) {
				const char* opts[] = { "connection_string=", nullptr };
				arrayGenerator(text, line, opts, lc);
			} else if (tokens.size() == 4 + (int)dryrun) {
				const char* opts[] = { "restore_known_data_cluster", "repopulate_from_data_cluster", nullptr };
				arrayGenerator(text, line, opts, lc);
			} else if (tokens.size() == 5 + (int)dryrun) {
				const char* opts[] = { "force_join_new_metacluster", nullptr };
				arrayGenerator(text, line, opts, lc);
			}
		}
	}
}

std::vector<const char*> metaclusterHintGenerator(std::vector<StringRef> const& tokens, bool inArgument) {
	if (tokens.size() == 1) {
		return { "<create_experimental|decommission|register|remove|restore|configure|list|get|status>", "[ARGS]" };
	} else if (tokencmp(tokens[1], "create_experimental") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<NAME>", "<TENANT_ID_PREFIX>" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "decommission")) {
		return {};
	} else if (tokencmp(tokens[1], "register") && tokens.size() < 5) {
		static std::vector<const char*> opts = { "<NAME>",
			                                     "connection_string=<CONNECTION_STRING>",
			                                     "[max_tenant_groups=<NUM_GROUPS>]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "remove") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "[FORCE]", "<NAME>" };
		if (tokens.size() == 2) {
			return opts;
		} else if (tokens.size() == 3 && (inArgument || tokens[2].size() == "FORCE"_sr.size()) &&
		           "FORCE"_sr.startsWith(tokens[2])) {
			return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
		} else {
			return {};
		}
	} else if (tokencmp(tokens[1], "restore") && tokens.size() < 7) {
		static std::vector<const char*> opts = { "<NAME>",
			                                     "[dryrun]",
			                                     "connection_string=<CONNECTION_STRING>",
			                                     "<restore_known_data_cluster|repopulate_from_data_cluster>",
			                                     "[force_join_new_metacluster]" };
		if (tokens.size() < 4 || (tokens[3].size() <= 6 && "dryrun"_sr.startsWith(tokens[3]))) {
			return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
		} else if (tokens.size() < 6) {
			return std::vector<const char*>(opts.begin() + tokens.size() - 1, opts.end());
		} else {
			return {};
		}
	} else if (tokencmp(tokens[1], "configure")) {
		static std::vector<const char*> opts = {
			"<NAME>", "<max_tenant_groups=<NUM_GROUPS>|connection_string=<CONNECTION_STRING>>"
		};
		return std::vector<const char*>(opts.begin() + std::min<int>(1, tokens.size() - 2), opts.end());
	} else if (tokencmp(tokens[1], "list") && tokens.size() < 5) {
		static std::vector<const char*> opts = { "[BEGIN]", "[END]", "[LIMIT]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "get") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<NAME>", "[JSON]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "status") && tokens.size() == 2) {
		return { "[JSON]" };
	} else {
		return {};
	}
}

CommandFactory metaclusterRegisterFactory(
    "metacluster",
    CommandHelp(
        "metacluster <create_experimental|decommission|register|remove|restore|configure|list|get|status> [ARGS]",
        "view and manage a metacluster",
        "`create_experimental' and `decommission' set up or deconfigure a metacluster.\n"
        "`register' and `remove' add and remove data clusters from the metacluster.\n"
        "`configure' updates the configuration of a data cluster.\n"
        "`restore' is used to recover from lost management or data clusters.\n"
        "`list' prints a list of data clusters in the metacluster.\n"
        "`get' prints the metadata for a particular data cluster.\n"
        "`status' prints metacluster metadata.\n"),
    &metaclusterGenerator,
    &metaclusterHintGenerator);

} // namespace fdb_cli
