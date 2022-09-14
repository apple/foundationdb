/*
 * TenantCommands.actor.cpp
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

const KeyRangeRef tenantMapSpecialKeyRange("\xff\xff/management/tenant/map/"_sr, "\xff\xff/management/tenant/map0"_sr);
const KeyRangeRef tenantConfigSpecialKeyRange("\xff\xff/management/tenant/configure/"_sr,
                                              "\xff\xff/management/tenant/configure0"_sr);
const KeyRangeRef tenantRenameSpecialKeyRange("\xff\xff/management/tenant/rename/"_sr,
                                              "\xff\xff/management/tenant/rename0"_sr);

Optional<std::map<Standalone<StringRef>, Optional<Value>>>
parseTenantConfiguration(std::vector<StringRef> const& tokens, int startIndex, bool allowUnset) {
	std::map<Standalone<StringRef>, Optional<Value>> configParams;
	for (int tokenNum = startIndex; tokenNum < tokens.size(); ++tokenNum) {
		Optional<Value> value;

		StringRef token = tokens[tokenNum];
		StringRef param;
		if (allowUnset && token == "unset"_sr) {
			if (++tokenNum == tokens.size()) {
				fmt::print(stderr, "ERROR: `unset' specified without a configuration parameter.\n");
				return {};
			}
			param = tokens[tokenNum];
		} else {
			bool foundEquals;
			param = token.eat("=", &foundEquals);
			if (!foundEquals) {
				fmt::print(stderr,
				           "ERROR: invalid configuration string `{}'. String must specify a value using `='.\n",
				           param.toString().c_str());
				return {};
			}
			value = token;
		}

		if (configParams.count(param)) {
			fmt::print(
			    stderr, "ERROR: configuration parameter `{}' specified more than once.\n", param.toString().c_str());
			return {};
		}

		if (tokencmp(param, "tenant_group")) {
			configParams[param] = value;
		} else {
			fmt::print(stderr, "ERROR: unrecognized configuration parameter `{}'.\n", param.toString().c_str());
			return {};
		}
	}

	return configParams;
}

Key makeConfigKey(TenantNameRef tenantName, StringRef configName) {
	return tenantConfigSpecialKeyRange.begin.withSuffix(Tuple().append(tenantName).append(configName).pack());
}

void applyConfigurationToSpecialKeys(Reference<ITransaction> tr,
                                     TenantNameRef tenantName,
                                     std::map<Standalone<StringRef>, Optional<Value>> configuration) {
	for (auto [configName, value] : configuration) {
		if (value.present()) {
			tr->set(makeConfigKey(tenantName, configName), value.get());
		} else {
			tr->clear(makeConfigKey(tenantName, configName));
		}
	}
}

// createtenant command
ACTOR Future<bool> createTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 2 || tokens.size() > 3) {
		printUsage(tokens[0]);
		return false;
	}

	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[1]);
	state Reference<ITransaction> tr = db->createTransaction();
	state bool doneExistenceCheck = false;

	state Optional<std::map<Standalone<StringRef>, Optional<Value>>> configuration =
	    parseTenantConfiguration(tokens, 2, false);

	if (!configuration.present()) {
		return false;
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				TenantMapEntry tenantEntry;
				for (auto const& [name, value] : configuration.get()) {
					tenantEntry.configure(name, value);
				}
				wait(MetaclusterAPI::createTenant(db, tokens[1], tenantEntry));
			} else {
				if (!doneExistenceCheck) {
					// Hold the reference to the standalone's memory
					state ThreadFuture<Optional<Value>> existingTenantFuture = tr->get(tenantNameKey);
					Optional<Value> existingTenant = wait(safeThreadFutureToFuture(existingTenantFuture));
					if (existingTenant.present()) {
						throw tenant_already_exists();
					}
					doneExistenceCheck = true;
				}

				tr->set(tenantNameKey, ValueRef());
				applyConfigurationToSpecialKeys(tr, tokens[1], configuration.get());
				wait(safeThreadFutureToFuture(tr->commit()));
			}

			break;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fmt::print(stderr, "ERROR: {}\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}

	fmt::print("The tenant `{}' has been created\n", printable(tokens[1]).c_str());
	return true;
}

CommandFactory createTenantFactory(
    "createtenant",
    CommandHelp("createtenant <TENANT_NAME> [tenant_group=<TENANT_GROUP>]",
                "creates a new tenant in the cluster",
                "Creates a new tenant in the cluster with the specified name. An optional group can be specified"
                "that will require this tenant to be placed on the same cluster as other tenants in the same group."));

// deletetenant command
ACTOR Future<bool> deleteTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	}

	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[1]);
	state Reference<ITransaction> tr = db->createTransaction();
	state bool doneExistenceCheck = false;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(MetaclusterAPI::deleteTenant(db, tokens[1]));
			} else {
				if (!doneExistenceCheck) {
					// Hold the reference to the standalone's memory
					state ThreadFuture<Optional<Value>> existingTenantFuture = tr->get(tenantNameKey);
					Optional<Value> existingTenant = wait(safeThreadFutureToFuture(existingTenantFuture));
					if (!existingTenant.present()) {
						throw tenant_not_found();
					}
					doneExistenceCheck = true;
				}

				tr->clear(tenantNameKey);
				wait(safeThreadFutureToFuture(tr->commit()));
			}

			break;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fmt::print(stderr, "ERROR: {}\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}

	fmt::print("The tenant `{}' has been deleted\n", printable(tokens[1]).c_str());
	return true;
}

CommandFactory deleteTenantFactory(
    "deletetenant",
    CommandHelp(
        "deletetenant <TENANT_NAME>",
        "deletes a tenant from the cluster",
        "Deletes a tenant from the cluster. Deletion will be allowed only if the specified tenant contains no data."));

// listtenants command
ACTOR Future<bool> listTenantsCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 4) {
		printUsage(tokens[0]);
		return false;
	}

	state StringRef beginTenant = ""_sr;
	state StringRef endTenant = "\xff\xff"_sr;
	state int limit = 100;

	if (tokens.size() >= 2) {
		beginTenant = tokens[1];
	}
	if (tokens.size() >= 3) {
		endTenant = tokens[2];
		if (endTenant <= beginTenant) {
			fmt::print(stderr, "ERROR: end must be larger than begin");
			return false;
		}
	}
	if (tokens.size() == 4) {
		int n = 0;
		if (sscanf(tokens[3].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[3].size() || limit <= 0) {
			fmt::print(stderr, "ERROR: invalid limit `{}'\n", tokens[3].toString().c_str());
			return false;
		}
	}

	state Key beginTenantKey = tenantMapSpecialKeyRange.begin.withSuffix(beginTenant);
	state Key endTenantKey = tenantMapSpecialKeyRange.begin.withSuffix(endTenant);
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::vector<TenantNameRef> tenantNames;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
				    wait(MetaclusterAPI::listTenantsTransaction(tr, beginTenant, endTenant, limit));
				for (auto tenant : tenants) {
					tenantNames.push_back(tenant.first);
				}
			} else {
				// Hold the reference to the standalone's memory
				state ThreadFuture<RangeResult> kvsFuture =
				    tr->getRange(firstGreaterOrEqual(beginTenantKey), firstGreaterOrEqual(endTenantKey), limit);
				RangeResult tenants = wait(safeThreadFutureToFuture(kvsFuture));
				for (auto tenant : tenants) {
					tenantNames.push_back(tenant.key.removePrefix(tenantMapSpecialKeyRange.begin));
				}
			}

			if (tenantNames.empty()) {
				if (tokens.size() == 1) {
					fmt::print("The cluster has no tenants\n");
				} else {
					fmt::print("The cluster has no tenants in the specified range\n");
				}
			}

			int index = 0;
			for (auto tenantName : tenantNames) {
				fmt::print("  {}. {}\n", ++index, printable(tenantName).c_str());
			}

			return true;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fmt::print(stderr, "ERROR: {}\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}
}

CommandFactory listTenantsFactory(
    "listtenants",
    CommandHelp("listtenants [BEGIN] [END] [LIMIT]",
                "print a list of tenants in the cluster",
                "Print a list of tenants in the cluster. Only tenants in the range [BEGIN] - [END] will be printed. "
                "The number of tenants to print can be specified using the [LIMIT] parameter, which defaults to 100."));

// gettenant command
ACTOR Future<bool> getTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 2 || tokens.size() > 3 || (tokens.size() == 3 && tokens[2] != "JSON"_sr)) {
		printUsage(tokens[0]);
		return false;
	}

	state bool useJson = tokens.size() == 3;
	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[1]);
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::string tenantJson;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				TenantMapEntry entry = wait(MetaclusterAPI::getTenantTransaction(tr, tokens[1]));
				tenantJson = entry.toJson();
			} else {
				// Hold the reference to the standalone's memory
				state ThreadFuture<Optional<Value>> tenantFuture = tr->get(tenantNameKey);
				Optional<Value> tenant = wait(safeThreadFutureToFuture(tenantFuture));
				if (!tenant.present()) {
					throw tenant_not_found();
				}
				tenantJson = tenant.get().toString();
			}

			json_spirit::mValue jsonObject;
			json_spirit::read_string(tenantJson, jsonObject);

			if (useJson) {
				json_spirit::mObject resultObj;
				resultObj["tenant"] = jsonObject;
				resultObj["type"] = "success";
				fmt::print(
				    "{}\n",
				    json_spirit::write_string(json_spirit::mValue(resultObj), json_spirit::pretty_print).c_str());
			} else {
				JSONDoc doc(jsonObject);

				int64_t id;
				std::string prefix;
				std::string tenantState;
				std::string tenantGroup;
				std::string assignedCluster;

				doc.get("id", id);
				doc.get("prefix.printable", prefix);

				doc.get("tenant_state", tenantState);
				bool hasTenantGroup = doc.tryGet("tenant_group.printable", tenantGroup);
				bool hasAssignedCluster = doc.tryGet("assigned_cluster", assignedCluster);

				fmt::print("  id: {}\n", id);
				fmt::print("  prefix: {}\n", printable(prefix).c_str());
				fmt::print("  tenant state: {}\n", printable(tenantState).c_str());
				if (hasTenantGroup) {
					fmt::print("  tenant group: {}\n", tenantGroup.c_str());
				}
				if (hasAssignedCluster) {
					fmt::print("  assigned cluster: {}\n", printable(assignedCluster).c_str());
				}
			}
			return true;
		} catch (Error& e) {
			try {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			} catch (Error& finalErr) {
				state std::string errorStr;
				if (finalErr.code() == error_code_special_keys_api_failure) {
					std::string str = wait(getSpecialKeysFailureErrorMessage(tr));
					errorStr = str;
				} else if (useJson) {
					errorStr = finalErr.what();
				} else {
					throw finalErr;
				}

				if (useJson) {
					json_spirit::mObject resultObj;
					resultObj["type"] = "error";
					resultObj["error"] = errorStr;
					fmt::print(
					    "{}\n",
					    json_spirit::write_string(json_spirit::mValue(resultObj), json_spirit::pretty_print).c_str());
				} else {
					fmt::print(stderr, "ERROR: {}\n", errorStr.c_str());
				}

				return false;
			}
		}
	}
}

CommandFactory getTenantFactory(
    "gettenant",
    CommandHelp("gettenant <TENANT_NAME> [JSON]",
                "prints the metadata for a tenant",
                "Prints the metadata for a tenant. If JSON is specified, then the output will be in JSON format."));

// configuretenant command
ACTOR Future<bool> configureTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 3) {
		printUsage(tokens[0]);
		return false;
	}

	state Optional<std::map<Standalone<StringRef>, Optional<Value>>> configuration =
	    parseTenantConfiguration(tokens, 2, true);

	if (!configuration.present()) {
		return false;
	}

	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				TenantMapEntry tenantEntry;
				wait(MetaclusterAPI::configureTenant(db, tokens[1], configuration.get()));
			} else {
				applyConfigurationToSpecialKeys(tr, tokens[1], configuration.get());
				wait(safeThreadFutureToFuture(tr->commit()));
			}
			break;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fmt::print(stderr, "ERROR: {}\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}

	fmt::print("The configuration for tenant `{}' has been updated\n", printable(tokens[1]).c_str());
	return true;
}

CommandFactory configureTenantFactory(
    "configuretenant",
    CommandHelp("configuretenant <TENANT_NAME> <[unset] tenant_group[=<GROUP_NAME>]> ...",
                "updates the configuration for a tenant",
                "Updates the configuration for a tenant. Use `tenant_group=<GROUP_NAME>' to change the tenant group "
                "that a tenant is assigned to or `unset tenant_group' to remove a tenant from its tenant group."));

// Helper function to extract tenant ID from json metadata string
int64_t getTenantId(Value metadata) {
	json_spirit::mValue jsonObject;
	json_spirit::read_string(metadata.toString(), jsonObject);
	JSONDoc doc(jsonObject);
	int64_t id;
	doc.get("id", id);
	return id;
}

// renametenant command
ACTOR Future<bool> renameTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 3) {
		printUsage(tokens[0]);
		return false;
	}
	state Reference<ITransaction> tr = db->createTransaction();
	state Key tenantRenameKey = tenantRenameSpecialKeyRange.begin.withSuffix(tokens[1]);
	state Key tenantOldNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[1]);
	state Key tenantNewNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[2]);
	state bool firstTry = true;
	state int64_t id = -1;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(MetaclusterAPI::renameTenant(db, tokens[1], tokens[2]));
			} else {
				// Hold the reference to the standalone's memory
				state ThreadFuture<Optional<Value>> oldEntryFuture = tr->get(tenantOldNameKey);
				state ThreadFuture<Optional<Value>> newEntryFuture = tr->get(tenantNewNameKey);
				state Optional<Value> oldEntry = wait(safeThreadFutureToFuture(oldEntryFuture));
				state Optional<Value> newEntry = wait(safeThreadFutureToFuture(newEntryFuture));
				if (firstTry) {
					if (!oldEntry.present()) {
						throw tenant_not_found();
					}
					if (newEntry.present()) {
						throw tenant_already_exists();
					}
					// Store the id we see when first reading this key
					id = getTenantId(oldEntry.get());

					firstTry = false;
				} else {
					// If we got commit_unknown_result, the rename may have already occurred.
					if (newEntry.present()) {
						int64_t checkId = getTenantId(newEntry.get());
						if (id == checkId) {
							ASSERT(!oldEntry.present() || getTenantId(oldEntry.get()) != id);
							return true;
						}
						// If the new entry is present but does not match, then
						// the rename should fail, so we throw an error.
						throw tenant_already_exists();
					}
					if (!oldEntry.present()) {
						throw tenant_not_found();
					}
					int64_t checkId = getTenantId(oldEntry.get());
					// If the id has changed since we made our first attempt,
					// then it's possible we've already moved the tenant. Don't move it again.
					if (id != checkId) {
						throw tenant_not_found();
					}
				}
				tr->set(tenantRenameKey, tokens[2]);
				wait(safeThreadFutureToFuture(tr->commit()));
			}
			break;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fmt::print(stderr, "ERROR: {}\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}

	fmt::print(
	    "The tenant `{}' has been renamed to `{}'\n", printable(tokens[1]).c_str(), printable(tokens[2]).c_str());
	return true;
}

CommandFactory renameTenantFactory(
    "renametenant",
    CommandHelp(
        "renametenant <OLD_NAME> <NEW_NAME>",
        "renames a tenant in the cluster",
        "Renames a tenant in the cluster. The old name must exist and the new name must not exist in the cluster."));
} // namespace fdb_cli
