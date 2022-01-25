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

namespace {} // namespace

namespace fdb_cli {
// createtenant command
ACTOR Future<bool> createTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	}

	wait(ManagementAPI::createTenant(db, tokens[1]));
	printf("The tenant `%s' has been created.\n", printable(tokens[1]).c_str());
	return true;
}

CommandFactory createTenantFactory("createtenant",
                                   CommandHelp("createtenant <TENANT_NAME>",
                                               "creates a new tenant in the cluster",
                                               "Creates a new tenant in the cluster with the specified name."));

// deletetenant command
ACTOR Future<bool> deleteTenantCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	}

	wait(ManagementAPI::deleteTenant(db, tokens[1]));
	printf("The tenant `%s' has been deleted.\n", printable(tokens[1]).c_str());
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

	StringRef begin = ""_sr;
	StringRef end = "\xff\xff"_sr;
	state int limit = 100;

	if (tokens.size() >= 2) {
		begin = tokens[1];
	}
	if (tokens.size() >= 3) {
		end = tokens[2];
		if (end <= begin) {
			fprintf(stderr, "ERROR: end must be larger than begin");
			return false;
		}
	}
	if (tokens.size() == 4) {
		int n = 0;
		if (sscanf(tokens[3].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[4].size()) {
			fprintf(stderr, "ERROR: invalid limit %s\n", tokens[3].toString().c_str());
			return false;
		}
	}

	Standalone<VectorRef<StringRef>> tenants = wait(ManagementAPI::listTenants(db, begin, end, limit));

	if (tenants.empty()) {
		if (tokens.size() == 1) {
			printf("The cluster has no tenants.\n");
		} else {
			printf("The cluster has no tenants in the specified range.\n");
		}
	}

	int index = 0;
	for (auto tenant : tenants) {
		printf("  %d. %s\n", ++index, printable(tenant).c_str());
	}

	return true;
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

	TenantMapEntry tenant = wait(ManagementAPI::getTenant(db, tokens[1]));
	printf("  Prefix: %s\n", printable(tenant.prefix).c_str());

	return true;
}

CommandFactory getTenantFactory("gettenant",
                                CommandHelp("gettenant <TENANT_NAME>",
                                            "prints the metadata for a tenant",
                                            "Prints the metadata for a tenant."));
} // namespace fdb_cli
