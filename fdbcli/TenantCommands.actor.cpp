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
		} else if (tokencmp(param, "assigned_cluster")) {
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
		if (configName == "assigned_cluster"_sr) {
			fmt::print(stderr, "ERROR: assigned_cluster is only valid in metacluster configuration.\n");
			throw invalid_tenant_configuration();
		}
		if (value.present()) {
			tr->set(makeConfigKey(tenantName, configName), value.get());
		} else {
			tr->clear(makeConfigKey(tenantName, configName));
		}
	}
}

// tenant create command
ACTOR Future<bool> tenantCreateCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 3 || tokens.size() > 5) {
		fmt::print("Usage: tenant create <NAME> [tenant_group=<TENANT_GROUP>] [assigned_cluster=<CLUSTER_NAME>]\n\n");
		fmt::print("Creates a new tenant in the cluster with the specified name.\n");
		fmt::print("An optional group can be specified that will require this tenant\n");
		fmt::print("to be placed on the same cluster as other tenants in the same group.\n");
		fmt::print("An optional cluster name can be specified that this tenant will be placed in.\n");
		return false;
	}

	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[2]);
	state Reference<ITransaction> tr = db->createTransaction();
	state bool doneExistenceCheck = false;

	state Optional<std::map<Standalone<StringRef>, Optional<Value>>> configuration =
	    parseTenantConfiguration(tokens, 3, false);

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
				wait(MetaclusterAPI::createTenant(db, tokens[2], tenantEntry));
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
				applyConfigurationToSpecialKeys(tr, tokens[2], configuration.get());
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

	fmt::print("The tenant `{}' has been created\n", printable(tokens[2]).c_str());
	return true;
}

// tenant delete command
ACTOR Future<bool> tenantDeleteCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 3) {
		fmt::print("Usage: tenant delete <NAME>\n\n");
		fmt::print("Deletes a tenant from the cluster.\n");
		fmt::print("Deletion will be allowed only if the specified tenant contains no data.\n");
		return false;
	}

	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[2]);
	state Reference<ITransaction> tr = db->createTransaction();
	state bool doneExistenceCheck = false;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(MetaclusterAPI::deleteTenant(db, tokens[2]));
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

	fmt::print("The tenant `{}' has been deleted\n", printable(tokens[2]).c_str());
	return true;
}

// tenant list command
ACTOR Future<bool> tenantListCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 5) {
		fmt::print("Usage: tenant list [BEGIN] [END] [LIMIT]\n\n");
		fmt::print("Lists the tenants in a cluster.\n");
		fmt::print("Only tenants in the range BEGIN - END will be printed.\n");
		fmt::print("An optional LIMIT can be specified to limit the number of results (default 100).\n");
		return false;
	}

	state StringRef beginTenant = ""_sr;
	state StringRef endTenant = "\xff\xff"_sr;
	state int limit = 100;

	if (tokens.size() >= 3) {
		beginTenant = tokens[2];
	}
	if (tokens.size() >= 4) {
		endTenant = tokens[3];
		if (endTenant <= beginTenant) {
			fmt::print(stderr, "ERROR: end must be larger than begin");
			return false;
		}
	}
	if (tokens.size() == 5) {
		int n = 0;
		if (sscanf(tokens[4].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[4].size() || limit <= 0) {
			fmt::print(stderr, "ERROR: invalid limit `{}'\n", tokens[4].toString().c_str());
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
			state std::vector<TenantName> tenantNames;
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
				if (tokens.size() == 2) {
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

// tenant get command
ACTOR Future<bool> tenantGetCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 3 || tokens.size() > 4 || (tokens.size() == 4 && tokens[3] != "JSON"_sr)) {
		fmt::print("Usage: tenant get <NAME> [JSON]\n\n");
		fmt::print("Prints metadata associated with the given tenant.\n");
		fmt::print("If JSON is specified, then the output will be in JSON format.\n");
		return false;
	}

	state bool useJson = tokens.size() == 4;
	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[2]);
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::string tenantJson;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				TenantMapEntry entry = wait(MetaclusterAPI::getTenantTransaction(tr, tokens[2]));
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

// tenant configure command
ACTOR Future<bool> tenantConfigureCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 4) {
		fmt::print("Usage: tenant configure <TENANT_NAME> <[unset] tenant_group[=<GROUP_NAME>]> ...\n\n");
		fmt::print("Updates the configuration for a tenant.\n");
		fmt::print("Use `tenant_group=<GROUP_NAME>' to change the tenant group that a\n");
		fmt::print("tenant is assigned to or `unset tenant_group' to remove a tenant from\n");
		fmt::print("its tenant group.");
		return false;
	}

	state Optional<std::map<Standalone<StringRef>, Optional<Value>>> configuration =
	    parseTenantConfiguration(tokens, 3, true);

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
				wait(MetaclusterAPI::configureTenant(db, tokens[2], configuration.get()));
			} else {
				applyConfigurationToSpecialKeys(tr, tokens[2], configuration.get());
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

	fmt::print("The configuration for tenant `{}' has been updated\n", printable(tokens[2]).c_str());
	return true;
}

// Helper function to extract tenant ID from json metadata string
int64_t getTenantId(Value metadata) {
	json_spirit::mValue jsonObject;
	json_spirit::read_string(metadata.toString(), jsonObject);
	JSONDoc doc(jsonObject);
	int64_t id;
	doc.get("id", id);
	return id;
}

// tenant rename command
ACTOR Future<bool> tenantRenameCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 4) {
		fmt::print("Usage: tenant rename <OLD_NAME> <NEW_NAME>\n\n");
		fmt::print("Renames a tenant in the cluster. The old name must exist and the new\n");
		fmt::print("name must not exist in the cluster.\n");
		return false;
	}
	state Reference<ITransaction> tr = db->createTransaction();
	state Key tenantRenameKey = tenantRenameSpecialKeyRange.begin.withSuffix(tokens[2]);
	state Key tenantOldNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[2]);
	state Key tenantNewNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[3]);
	state bool firstTry = true;
	state int64_t id = -1;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(MetaclusterAPI::renameTenant(db, tokens[2], tokens[3]));
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
				tr->set(tenantRenameKey, tokens[3]);
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
	    "The tenant `{}' has been renamed to `{}'\n", printable(tokens[2]).c_str(), printable(tokens[3]).c_str());
	return true;
}

// tenant command
Future<bool> tenantCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return true;
	} else if (tokencmp(tokens[1], "create")) {
		return tenantCreateCommand(db, tokens);
	} else if (tokencmp(tokens[1], "delete")) {
		return tenantDeleteCommand(db, tokens);
	} else if (tokencmp(tokens[1], "list")) {
		return tenantListCommand(db, tokens);
	} else if (tokencmp(tokens[1], "get")) {
		return tenantGetCommand(db, tokens);
	} else if (tokencmp(tokens[1], "configure")) {
		return tenantConfigureCommand(db, tokens);
	} else if (tokencmp(tokens[1], "rename")) {
		return tenantRenameCommand(db, tokens);
	} else {
		printUsage(tokens[0]);
		return true;
	}
}

Future<bool> tenantCommandForwarder(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	ASSERT(!tokens.empty() && (tokens[0].endsWith("tenant"_sr) || tokens[0].endsWith("tenants"_sr)));
	std::vector<StringRef> forwardedTokens = { "tenant"_sr,
		                                       tokens[0].endsWith("tenant"_sr) ? tokens[0].removeSuffix("tenant"_sr)
		                                                                       : tokens[0].removeSuffix("tenants"_sr) };
	for (int i = 1; i < tokens.size(); ++i) {
		forwardedTokens.push_back(tokens[i]);
	}

	return tenantCommand(db, forwardedTokens);
} // namespace fdb_cli

void tenantGenerator(const char* text,
                     const char* line,
                     std::vector<std::string>& lc,
                     std::vector<StringRef> const& tokens) {
	if (tokens.size() == 1) {
		const char* opts[] = { "create", "delete", "list", "get", "configure", "rename", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() == 3 && tokencmp(tokens[1], "create")) {
		const char* opts[] = { "tenant_group=", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() == 3 && tokencmp(tokens[1], "get")) {
		const char* opts[] = { "JSON", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokencmp(tokens[1], "configure")) {
		if (tokens.size() == 3) {
			const char* opts[] = { "tenant_group=", "unset", nullptr };
			arrayGenerator(text, line, opts, lc);
		} else if (tokens.size() == 4 && tokencmp(tokens[3], "unset")) {
			const char* opts[] = { "tenant_group", nullptr };
			arrayGenerator(text, line, opts, lc);
		}
	}
}

std::vector<const char*> tenantHintGenerator(std::vector<StringRef> const& tokens, bool inArgument) {
	if (tokens.size() == 1) {
		return { "<create|delete|list|get|configure|rename>", "[ARGS]" };
	} else if (tokencmp(tokens[1], "create") && tokens.size() < 5) {
		static std::vector<const char*> opts = { "<NAME>",
			                                     "[tenant_group=<TENANT_GROUP>]",
			                                     "[assigned_cluster=<CLUSTER_NAME>]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "delete") && tokens.size() < 3) {
		static std::vector<const char*> opts = { "<NAME>" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "list") && tokens.size() < 5) {
		static std::vector<const char*> opts = { "[BEGIN]", "[END]", "[LIMIT]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "get") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<NAME>", "[JSON]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "configure")) {
		if (tokens.size() < 4) {
			static std::vector<const char*> opts = { "<TENANT_NAME>", "<[unset] tenant_group[=<GROUP_NAME>]>" };
			return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
		} else if (tokens.size() == 4 && tokencmp(tokens[3], "unset")) {
			static std::vector<const char*> opts = { "<tenant_group[=<GROUP_NAME>]>" };
			return std::vector<const char*>(opts.begin() + tokens.size() - 4, opts.end());
		}
		return {};
	} else if (tokencmp(tokens[1], "rename") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<OLD_NAME>", "<NEW_NAME>" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else {
		return {};
	}
}

CommandFactory tenantRegisterFactory("tenant",
                                     CommandHelp("tenant <create|delete|list|get|configure|rename> [ARGS]",
                                                 "view and manage tenants in a cluster or metacluster",
                                                 "`create' and `delete' add and remove tenants from the cluster.\n"
                                                 "`list' prints a list of tenants in the cluster.\n"
                                                 "`get' prints the metadata for a particular tenant.\n"
                                                 "`configure' modifies the configuration for a tenant.\n"
                                                 "`rename' changes the name of a tenant.\n"),
                                     &tenantGenerator,
                                     &tenantHintGenerator);

// Generate hidden commands for the old versions of the tenant commands
CommandFactory createTenantFactory("createtenant");
CommandFactory deleteTenantFactory("deletetenant");
CommandFactory listTenantsFactory("listtenants");
CommandFactory getTenantFactory("gettenant");
CommandFactory configureTenantFactory("configuretenant");
CommandFactory renameTenantFactory("renametenant");

} // namespace fdb_cli
