/*
 * FileConfigureCommand.actor.cpp
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

#include "fdbcli/FlowLineNoise.h"
#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> fileConfigureCommandActor(Reference<IDatabase> db,
                                             std::string filePath,
                                             bool isNewDatabase,
                                             bool force) {
	std::string contents(readFileBytes(filePath, 100000));
	json_spirit::mValue config;
	if (!json_spirit::read_string(contents, config)) {
		fprintf(stderr, "ERROR: Invalid JSON\n");
		return false;
	}
	if (config.type() != json_spirit::obj_type) {
		fprintf(stderr, "ERROR: Configuration file must contain a JSON object\n");
		return false;
	}
	StatusObject configJSON = config.get_obj();

	json_spirit::mValue schema;
	if (!json_spirit::read_string(JSONSchemas::clusterConfigurationSchema.toString(), schema)) {
		ASSERT(false);
	}

	std::string errorStr;
	if (!schemaMatch(schema.get_obj(), configJSON, errorStr)) {
		printf("%s", errorStr.c_str());
		return false;
	}

	std::string configString;
	if (isNewDatabase) {
		configString = "new";
	}

	for (const auto& [name, value] : configJSON) {
		if (!configString.empty()) {
			configString += " ";
		}
		if (value.type() == json_spirit::int_type) {
			configString += name + ":=" + format("%d", value.get_int());
		} else if (value.type() == json_spirit::str_type) {
			configString += value.get_str();
		} else if (value.type() == json_spirit::array_type) {
			configString +=
			    name + "=" +
			    json_spirit::write_string(json_spirit::mValue(value.get_array()), json_spirit::Output_options::none);
		} else {
			printUsage(LiteralStringRef("fileconfigure"));
			return false;
		}
	}
	ConfigurationResult result = wait(ManagementAPI::changeConfig(db, configString, force));
	// Real errors get thrown from makeInterruptable and printed by the catch block in cli(), but
	// there are various results specific to changeConfig() that we need to report:
	bool ret = true;
	switch (result) {
	case ConfigurationResult::NO_OPTIONS_PROVIDED:
		fprintf(stderr, "ERROR: No options provided\n");
		ret = false;
		break;
	case ConfigurationResult::CONFLICTING_OPTIONS:
		fprintf(stderr, "ERROR: Conflicting options\n");
		ret = false;
		break;
	case ConfigurationResult::UNKNOWN_OPTION:
		fprintf(stderr, "ERROR: Unknown option\n"); // This should not be possible because of schema match
		ret = false;
		break;
	case ConfigurationResult::INCOMPLETE_CONFIGURATION:
		fprintf(stderr,
		        "ERROR: Must specify both a replication level and a storage engine when creating a new database\n");
		ret = false;
		break;
	case ConfigurationResult::INVALID_CONFIGURATION:
		fprintf(stderr, "ERROR: These changes would make the configuration invalid\n");
		ret = false;
		break;
	case ConfigurationResult::DATABASE_ALREADY_CREATED:
		fprintf(stderr, "ERROR: Database already exists! To change configuration, don't say `new'\n");
		ret = false;
		break;
	case ConfigurationResult::DATABASE_CREATED:
		printf("Database created\n");
		break;
	case ConfigurationResult::DATABASE_UNAVAILABLE:
		fprintf(stderr, "ERROR: The database is unavailable\n");
		printf("Type `fileconfigure FORCE <FILENAME>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::STORAGE_IN_UNKNOWN_DCID:
		fprintf(stderr, "ERROR: All storage servers must be in one of the known regions\n");
		printf("Type `fileconfigure FORCE <FILENAME>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::REGION_NOT_FULLY_REPLICATED:
		fprintf(stderr,
		        "ERROR: When usable_regions > 1, All regions with priority >= 0 must be fully replicated "
		        "before changing the configuration\n");
		printf("Type `fileconfigure FORCE <FILENAME>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::MULTIPLE_ACTIVE_REGIONS:
		fprintf(stderr, "ERROR: When changing usable_regions, only one region can have priority >= 0\n");
		printf("Type `fileconfigure FORCE <FILENAME>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::REGIONS_CHANGED:
		fprintf(stderr,
		        "ERROR: The region configuration cannot be changed while simultaneously changing usable_regions\n");
		printf("Type `fileconfigure FORCE <FILENAME>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::NOT_ENOUGH_WORKERS:
		fprintf(stderr, "ERROR: Not enough processes exist to support the specified configuration\n");
		printf("Type `fileconfigure FORCE <FILENAME>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::REGION_REPLICATION_MISMATCH:
		fprintf(stderr, "ERROR: `three_datacenter' replication is incompatible with region configuration\n");
		printf("Type `fileconfigure FORCE <TOKEN...>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::DCID_MISSING:
		fprintf(stderr, "ERROR: `No storage servers in one of the specified regions\n");
		printf("Type `fileconfigure FORCE <TOKEN...>' to configure without this check\n");
		ret = false;
		break;
	case ConfigurationResult::SUCCESS:
		printf("Configuration changed\n");
		break;
	default:
		ASSERT(false);
		ret = false;
	};
	return ret;
}

CommandFactory fileconfigureFactory(
    "fileconfigure",
    CommandHelp(
        "fileconfigure [new] <FILENAME>",
        "change the database configuration from a file",
        "The `new' option, if present, initializes a new database with the given configuration rather than changing "
        "the configuration of an existing one. Load a JSON document from the provided file, and change the database "
        "configuration to match the contents of the JSON document. The format should be the same as the value of the "
        "\"configuration\" entry in status JSON without \"excluded_servers\" or \"coordinators_count\"."));

} // namespace fdb_cli
