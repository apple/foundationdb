/*
 * FileConfigureCommand.cpp
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

#include "fdbcli/FlowLineNoise.h"
#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "fmt/format.h"
namespace fdb_cli {

Future<bool> fileConfigureCommandActor(Reference<IDatabase> db,
                                       std::string const& filePath,
                                       bool isNewDatabase,
                                       bool force) {
	ConfigurationResult result;
	std::string contents(readFileBytes(filePath, 100000));
	json_spirit::mValue config;
	if (!json_spirit::read_string(contents, config)) {
		fmt::println(stderr, "ERROR: Invalid JSON");
		co_return false;
	}
	if (config.type() != json_spirit::obj_type) {
		fmt::println(stderr, "ERROR: Configuration file must contain a JSON object");
		co_return false;
	}
	StatusObject configJSON = config.get_obj();

	json_spirit::mValue schema;
	if (!json_spirit::read_string(JSONSchemas::clusterConfigurationSchema.toString(), schema)) {
		ASSERT(false);
	}

	std::string errorStr;
	if (!schemaMatch(schema.get_obj(), configJSON, errorStr)) {
		fmt::print("{}", errorStr);
		co_return false;
	}

	std::string configString;

	try {
		configString += DatabaseConfiguration::configureStringFromJSON(configJSON);
	} catch (Error& e) {
		fmt::print("ERROR: {}", e.what());
		printUsage("fileconfigure"_sr);
		co_return false;
	}

	if (isNewDatabase) {
		configString = "new" + configString;
	} else {
		configString.erase(0, 1); // configureStringFromJSON returns a string with leading space.
	}

	// Check for backup_worker_enabled configuration and reject it.
	// This setting is now managed automatically by the backup system.
	if (configString.find(" backup_worker_enabled:=") != std::string::npos) {
		result = ConfigurationResult::BACKUP_WORKER_ENABLED_RESTRICTED;
	} else {
		ConfigurationResult r = co_await ManagementAPI::changeConfig(db, configString, force);
		result = r;
	}
	// Real errors get thrown from makeInterruptable and printed by the catch block in cli(), but
	// there are various results specific to changeConfig() that we need to report:
	bool ret = true;
	switch (result) {
	case ConfigurationResult::NO_OPTIONS_PROVIDED:
		fmt::println(stderr, "ERROR: No options provided");
		ret = false;
		break;
	case ConfigurationResult::CONFLICTING_OPTIONS:
		fmt::println(stderr, "ERROR: Conflicting options");
		ret = false;
		break;
	case ConfigurationResult::UNKNOWN_OPTION:
		fmt::println(stderr, "ERROR: Unknown option"); // This should not be possible because of schema match
		ret = false;
		break;
	case ConfigurationResult::INCOMPLETE_CONFIGURATION:
		fmt::println(stderr,
		             "ERROR: Must specify both a replication level and a storage engine when creating a new database");
		ret = false;
		break;
	case ConfigurationResult::INVALID_CONFIGURATION:
		fmt::println(stderr, "ERROR: These changes would make the configuration invalid");
		ret = false;
		break;
	case ConfigurationResult::DATABASE_ALREADY_CREATED:
		fmt::println(stderr, "ERROR: Database already exists! To change configuration, don't say `new'");
		ret = false;
		break;
	case ConfigurationResult::DATABASE_CREATED:
		fmt::println("Database created");
		break;
	case ConfigurationResult::DATABASE_UNAVAILABLE:
		fmt::println(stderr, "ERROR: The database is unavailable");
		fmt::println("Type `fileconfigure FORCE <FILENAME>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::STORAGE_IN_UNKNOWN_DCID:
		fmt::println(stderr, "ERROR: All storage servers must be in one of the known regions");
		fmt::println("Type `fileconfigure FORCE <FILENAME>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::REGION_NOT_FULLY_REPLICATED:
		fmt::println(stderr,
		             "ERROR: When usable_regions > 1, All regions with priority >= 0 must be fully replicated before "
		             "changing the configuration");
		fmt::println("Type `fileconfigure FORCE <FILENAME>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::MULTIPLE_ACTIVE_REGIONS:
		fmt::println(stderr, "ERROR: When changing usable_regions, only one region can have priority >= 0");
		fmt::println("Type `fileconfigure FORCE <FILENAME>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::REGIONS_CHANGED:
		fmt::println(stderr,
		             "ERROR: The region configuration cannot be changed while simultaneously changing usable_regions");
		fmt::println("Type `fileconfigure FORCE <FILENAME>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::NOT_ENOUGH_WORKERS:
		fmt::println(stderr, "ERROR: Not enough processes exist to support the specified configuration");
		fmt::println("Type `fileconfigure FORCE <FILENAME>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::REGION_REPLICATION_MISMATCH:
		fmt::println(stderr, "ERROR: `three_datacenter' replication is incompatible with region configuration");
		fmt::println("Type `fileconfigure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::DCID_MISSING:
		fmt::println(stderr, "ERROR: `No storage servers in one of the specified regions");
		fmt::println("Type `fileconfigure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::SUCCESS:
		fmt::println("Configuration changed");
		break;
	case ConfigurationResult::BACKUP_WORKER_ENABLED_RESTRICTED:
		fmt::println(stderr,
		             "ERROR: backup_worker_enabled configuration is restricted in fdbcli and managed automatically by "
		             "the backup system");
		ret = false;
		break;
	default:
		ASSERT(false);
		ret = false;
	};
	co_return ret;
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
