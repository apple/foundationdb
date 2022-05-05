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
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

const KeyRangeRef tenantMapSpecialKeyRange(LiteralStringRef("\xff\xff/management/tenant/map/"),
                                           LiteralStringRef("\xff\xff/management/tenant/map0"));
const KeyRangeRef tenantConfigSpecialKeyRange(LiteralStringRef("\xff\xff/management/tenant/configure/"),
                                              LiteralStringRef("\xff\xff/management/tenant/configure0"));

std::pair<bool, TenantMapEntry> parseTenantConfiguration(std::vector<StringRef> const& tokens,
                                                         TenantMapEntry const& defaults,
                                                         int startIndex) {
	TenantMapEntry entry;

	for (int tokenNum = startIndex; tokenNum < tokens.size(); ++tokenNum) {
		StringRef token = tokens[tokenNum];
		StringRef param = token.eat("=");
		std::string value = token.toString();
		if (tokencmp(param, "tenant_group")) {
			entry = defaults;
			entry.tenantGroup = value;
		} else {
			fprintf(stderr, "ERROR: unrecognized configuration parameter %s\n", param.toString().c_str());
			return std::make_pair(false, defaults);
		}
	}

	return std::make_pair(true, entry);
}

Key makeConfigKey(StringRef configName, TenantNameRef tenantName) {
	Key configKey = makeString(tenantConfigSpecialKeyRange.begin.size() + configName.size() + 1 + tenantName.size());

	uint8_t* mutableKey = mutateString(configKey);
	mutableKey = tenantConfigSpecialKeyRange.begin.copyTo(mutableKey);
	mutableKey = configName.copyTo(mutableKey);
	*(mutableKey++) = '/';
	tenantName.copyTo(mutableKey);

	return configKey;
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

	state std::pair<bool, TenantMapEntry> configuration = parseTenantConfiguration(tokens, TenantMapEntry(), 2);
	if (!configuration.first) {
		return false;
	}

	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
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
			if (configuration.second.tenantGroup.present()) {
				tr->set(makeConfigKey("tenant_group"_sr, tokens[1]), configuration.second.tenantGroup.get());
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			break;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fprintf(stderr, "ERROR: %s\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}

	printf("The tenant `%s' has been created\n", printable(tokens[1]).c_str());
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
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
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
			break;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fprintf(stderr, "ERROR: %s\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}

	printf("The tenant `%s' has been deleted\n", printable(tokens[1]).c_str());
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

	StringRef beginTenant = ""_sr;
	StringRef endTenant = "\xff\xff"_sr;
	state int limit = 100;

	if (tokens.size() >= 2) {
		beginTenant = tokens[1];
	}
	if (tokens.size() >= 3) {
		endTenant = tokens[2];
		if (endTenant <= beginTenant) {
			fprintf(stderr, "ERROR: end must be larger than begin");
			return false;
		}
	}
	if (tokens.size() == 4) {
		int n = 0;
		if (sscanf(tokens[3].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[3].size() || limit < 0) {
			fprintf(stderr, "ERROR: invalid limit %s\n", tokens[3].toString().c_str());
			return false;
		}
	}

	state Key beginTenantKey = tenantMapSpecialKeyRange.begin.withSuffix(beginTenant);
	state Key endTenantKey = tenantMapSpecialKeyRange.begin.withSuffix(endTenant);
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			// Hold the reference to the standalone's memory
			state ThreadFuture<RangeResult> kvsFuture =
			    tr->getRange(firstGreaterOrEqual(beginTenantKey), firstGreaterOrEqual(endTenantKey), limit);
			RangeResult tenants = wait(safeThreadFutureToFuture(kvsFuture));

			if (tenants.empty()) {
				if (tokens.size() == 1) {
					printf("The cluster has no tenants\n");
				} else {
					printf("The cluster has no tenants in the specified range\n");
				}
			}

			int index = 0;
			for (auto tenant : tenants) {
				printf(
				    "  %d. %s\n", ++index, printable(tenant.key.removePrefix(tenantMapSpecialKeyRange.begin)).c_str());
			}

			return true;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fprintf(stderr, "ERROR: %s\n", errorMsgStr.c_str());
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
	if (tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	}

	state Key tenantNameKey = tenantMapSpecialKeyRange.begin.withSuffix(tokens[1]);
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			// Hold the reference to the standalone's memory
			state ThreadFuture<Optional<Value>> tenantFuture = tr->get(tenantNameKey);
			Optional<Value> tenant = wait(safeThreadFutureToFuture(tenantFuture));
			if (!tenant.present()) {
				throw tenant_not_found();
			}

			json_spirit::mValue jsonObject;
			json_spirit::read_string(tenant.get().toString(), jsonObject);
			JSONDoc doc(jsonObject);

			int64_t id;
			std::string prefix;
			std::string tenantGroup;
			doc.get("id", id);
			doc.get("prefix", prefix);
			bool hasTenantGroup = doc.tryGet("tenant_group", tenantGroup);

			printf("  id: %" PRId64 "\n", id);
			printf("  prefix: %s\n", printable(prefix).c_str());
			if (hasTenantGroup) {
				printf("  tenant group: %s\n", printable(tenantGroup).c_str());
			}
			return true;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(getSpecialKeysFailureErrorMessage(tr));
				fprintf(stderr, "ERROR: %s\n", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}
}

CommandFactory getTenantFactory("gettenant",
                                CommandHelp("gettenant <TENANT_NAME>",
                                            "prints the metadata for a tenant",
                                            "Prints the metadata for a tenant."));
} // namespace fdb_cli
