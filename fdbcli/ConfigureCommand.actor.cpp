/*
 * ConfigureCommand.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "fmt/format.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> configureCommandActor(Reference<IDatabase> db,
                                         Database localDb,
                                         std::vector<StringRef> tokens,
                                         LineNoise* linenoise,
                                         Future<Void> warn) {
	state ConfigurationResult result;
	state StatusObject s;
	state int startToken = 1;
	state bool force = false;
	if (tokens.size() < 2)
		result = ConfigurationResult::NO_OPTIONS_PROVIDED;
	else {
		if (tokens[startToken] == "FORCE"_sr) {
			force = true;
			startToken = 2;
		}

		state Optional<ConfigureAutoResult> conf;
		if (tokens[startToken] == "auto"_sr) {
			// get cluster status
			state Reference<ITransaction> tr = db->createTransaction();
			if (!tr->isValid()) {
				StatusObject _s = wait(StatusClient::statusFetcher(localDb));
				s = _s;
			} else {
				state ThreadFuture<Optional<Value>> statusValueF = tr->get("\xff\xff/status/json"_sr);
				Optional<Value> statusValue = wait(safeThreadFutureToFuture(statusValueF));
				if (!statusValue.present()) {
					fmt::println(stderr, "ERROR: Failed to get status json from the cluster");
					return false;
				}
				json_spirit::mValue mv;
				json_spirit::read_string(statusValue.get().toString(), mv);
				s = StatusObject(mv.get_obj());
			}

			if (warn.isValid())
				warn.cancel();

			conf = parseConfig(s);

			if (!conf.get().isValid()) {
				fmt::println("Unable to provide advice for the current configuration.");
				return false;
			}

			bool noChanges = conf.get().old_replication == conf.get().auto_replication &&
			                 conf.get().old_logs == conf.get().auto_logs &&
			                 conf.get().old_commit_proxies == conf.get().auto_commit_proxies &&
			                 conf.get().old_grv_proxies == conf.get().auto_grv_proxies &&
			                 conf.get().old_resolvers == conf.get().auto_resolvers &&
			                 conf.get().old_processes_with_transaction == conf.get().auto_processes_with_transaction &&
			                 conf.get().old_machines_with_transaction == conf.get().auto_machines_with_transaction;

			bool noDesiredChanges = noChanges && conf.get().old_logs == conf.get().desired_logs &&
			                        conf.get().old_commit_proxies == conf.get().desired_commit_proxies &&
			                        conf.get().old_grv_proxies == conf.get().desired_grv_proxies &&
			                        conf.get().old_resolvers == conf.get().desired_resolvers;

			std::string outputString;

			outputString += "\nYour cluster has:\n\n";
			outputString += fmt::format("  processes {}\n", conf.get().processes);
			outputString += fmt::format("  machines  {}\n", conf.get().machines);

			if (noDesiredChanges)
				outputString += "\nConfigure recommends keeping your current configuration:\n\n";
			else if (noChanges)
				outputString +=
				    "\nConfigure cannot modify the configuration because some parameters have been set manually:\n\n";
			else
				outputString += "\nConfigure recommends the following changes:\n\n";
			outputString += " ------------------------------------------------------------------- \n";
			outputString += "| parameter                   | old              | new              |\n";
			outputString += " ------------------------------------------------------------------- \n";
			outputString += fmt::format("| replication                 | {:>16} | {:>16} |\n",
			                            conf.get().old_replication,
			                            conf.get().auto_replication);
			outputString += fmt::format(
			    "| logs                        | {:16} | {:16} |", conf.get().old_logs, conf.get().auto_logs);
			outputString += conf.get().auto_logs != conf.get().desired_logs
			                    ? fmt::format(" (manually set; would be {})\n", conf.get().desired_logs)
			                    : "\n";
			outputString += fmt::format("| commit_proxies              | {:16} | {:16} |",
			                            conf.get().old_commit_proxies,
			                            conf.get().auto_commit_proxies);
			outputString += conf.get().auto_commit_proxies != conf.get().desired_commit_proxies
			                    ? fmt::format(" (manually set; would be {})\n", conf.get().desired_commit_proxies)
			                    : "\n";
			outputString += fmt::format("| grv_proxies                 | {:16} | {:16} |",
			                            conf.get().old_grv_proxies,
			                            conf.get().auto_grv_proxies);
			outputString += conf.get().auto_grv_proxies != conf.get().desired_grv_proxies
			                    ? fmt::format(" (manually set; would be {})\n", conf.get().desired_grv_proxies)
			                    : "\n";
			outputString += fmt::format(
			    "| resolvers                   | {:16} | {:16} |", conf.get().old_resolvers, conf.get().auto_resolvers);
			outputString += conf.get().auto_resolvers != conf.get().desired_resolvers
			                    ? fmt::format(" (manually set; would be {})\n", conf.get().desired_resolvers)
			                    : "\n";
			outputString += fmt::format("| transaction-class processes | {:16} | {:16} |\n",
			                            conf.get().old_processes_with_transaction,
			                            conf.get().auto_processes_with_transaction);
			outputString += fmt::format("| transaction-class machines  | {:16} | {:16} |\n",
			                            conf.get().old_machines_with_transaction,
			                            conf.get().auto_machines_with_transaction);
			outputString += " ------------------------------------------------------------------- \n\n";

			fmt::print("{}", outputString);

			if (noChanges)
				return true;

			// TODO: disable completion
			Optional<std::string> line = wait(linenoise->read("Would you like to make these changes? [y/n]> "));

			if (!line.present() || (line.get() != "y" && line.get() != "Y")) {
				return true;
			}
		}

		// Check for backup_worker_enabled configuration and reject it.
		// This setting is now managed automatically by the backup system.
		for (auto it = tokens.begin() + startToken; it != tokens.end(); ++it) {
			if (it->startsWith("backup_worker_enabled:="_sr)) {
				result = ConfigurationResult::BACKUP_WORKER_ENABLED_RESTRICTED;
				break;
			}
		}

		if (result != ConfigurationResult::BACKUP_WORKER_ENABLED_RESTRICTED) {
			ConfigurationResult r = wait(ManagementAPI::changeConfig(
			    db, std::vector<StringRef>(tokens.begin() + startToken, tokens.end()), conf, force));
			result = r;
		}
	}

	// Real errors get thrown from makeInterruptable and printed by the catch block in cli(), but
	// there are various results specific to changeConfig() that we need to report:
	bool ret = true;
	switch (result) {
	case ConfigurationResult::NO_OPTIONS_PROVIDED:
	case ConfigurationResult::CONFLICTING_OPTIONS:
	case ConfigurationResult::UNKNOWN_OPTION:
	case ConfigurationResult::INCOMPLETE_CONFIGURATION:
		printUsage("configure"_sr);
		ret = false;
		break;
	case ConfigurationResult::INVALID_CONFIGURATION:
		fmt::println(stderr, "ERROR: These changes would make the configuration invalid");
		ret = false;
		break;
	case ConfigurationResult::STORAGE_MIGRATION_DISABLED:
		fmt::println(stderr, "ERROR: Storage engine type cannot be changed because storage_migration_type=disabled.");
		fmt::println(
		    stderr,
		    "Type `configure perpetual_storage_wiggle=1 storage_migration_type=gradual' to enable gradual migration "
		    "with the perpetual wiggle, or `configure storage_migration_type=aggressive' for aggressive migration.");
		ret = false;
		break;
	case ConfigurationResult::DATABASE_ALREADY_CREATED:
		fmt::println(stderr, "ERROR: Database already exists! To change configuration, don't say `new'");
		ret = false;
		break;
	case ConfigurationResult::DATABASE_CREATED:
		fmt::println("Database created");
		break;
	case ConfigurationResult::DATABASE_CREATED_WARN_SHARDED_ROCKSDB_EXPERIMENTAL:
		fmt::println("Database created");
		fmt::println(
		    stderr,
		    "WARN: Sharded RocksDB storage engine type is still in experimental stage, not yet production tested.");
		break;
	case ConfigurationResult::DATABASE_UNAVAILABLE:
		fmt::println(stderr, "ERROR: The database is unavailable");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::STORAGE_IN_UNKNOWN_DCID:
		fmt::println(stderr, "ERROR: All storage servers must be in one of the known regions");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::REGION_NOT_FULLY_REPLICATED:
		fmt::println(stderr,
		             "ERROR: When usable_regions > 1, all regions with priority >= 0 must be fully replicated before "
		             "changing the configuration");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::MULTIPLE_ACTIVE_REGIONS:
		fmt::println(stderr, "ERROR: When changing usable_regions, only one region can have priority >= 0");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::REGIONS_CHANGED:
		fmt::println(stderr,
		             "ERROR: The region configuration cannot be changed while simultaneously changing usable_regions");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::NOT_ENOUGH_WORKERS:
		fmt::println(stderr, "ERROR: Not enough processes exist to support the specified configuration");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::REGION_REPLICATION_MISMATCH:
		fmt::println(stderr, "ERROR: `three_datacenter' replication is incompatible with region configuration");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::DCID_MISSING:
		fmt::println(stderr, "ERROR: `No storage servers in one of the specified regions");
		fmt::println(stderr, "Type `configure FORCE <TOKEN...>' to configure without this check");
		ret = false;
		break;
	case ConfigurationResult::SUCCESS:
		fmt::println("Configuration changed");
		break;
	case ConfigurationResult::LOCKED_NOT_NEW:
		fmt::println(stderr, "ERROR: `only new databases can be configured as locked`");
		ret = false;
		break;
	case ConfigurationResult::SUCCESS_WARN_PPW_GRADUAL:
		fmt::println("Configuration changed, with warnings");
		fmt::println(stderr,
		             "WARN: To make progress toward the desired storage type with storage_migration_type=gradual, the "
		             "Perpetual Wiggle must be enabled.");
		fmt::println(stderr,
		             "Type `configure perpetual_storage_wiggle=1' to enable the perpetual wiggle, or `configure "
		             "storage_migration_type=gradual' to set the gradual migration type.");
		break;
	case ConfigurationResult::SUCCESS_WARN_SHARDED_ROCKSDB_EXPERIMENTAL:
		fmt::println("Configuration changed");
		fmt::println(
		    stderr,
		    "WARN: Sharded RocksDB storage engine type is still in experimental stage, not yet production tested.");
		break;
	case ConfigurationResult::DATABASE_IS_REGISTERED:
		fmt::println(stderr,
		             "ERROR: a result of type `ConfigurationResult::DATABASE_IS_REGISTERED` was unexpectedly seen.");
		ret = false;
		break;
	case ConfigurationResult::INVALID_STORAGE_TYPE:
		fmt::println(stderr, "ERROR: Invalid storage type for storage or TLog.");
		ret = false;
		break;
	case ConfigurationResult::BACKUP_WORKER_ENABLED_RESTRICTED:
		fmt::println(stderr,
		             "ERROR: backup_worker_enabled configuration is restricted in fdbcli and managed automatically by "
		             "the backup system.");
		ret = false;
		break;
	default:
		ASSERT(false);
		ret = false;
	};
	return ret;
}

void configureGenerator(const char* text,
                        const char* line,
                        std::vector<std::string>& lc,
                        std::vector<StringRef> const& tokens) {
	const char* opts[] = { "new",
		                   "single",
		                   "double",
		                   "triple",
		                   "three_data_hall",
		                   "three_datacenter",
		                   "ssd",
		                   "ssd-1",
		                   "ssd-2",
		                   "memory",
		                   "memory-1",
		                   "memory-2",
		                   "memory-radixtree",
		                   "commit_proxies=",
		                   "grv_proxies=",
		                   "logs=",
		                   "resolvers=",
		                   "perpetual_storage_wiggle=",
		                   "perpetual_storage_wiggle_locality=",
		                   // TODO(zhewu): update fdbcli command documentation.
		                   "perpetual_storage_wiggle_engine=",
		                   "storage_migration_type=",
		                   nullptr };
	arrayGenerator(text, line, opts, lc);
}

CommandFactory configureFactory(
    "configure",
    CommandHelp(
        "configure [new|tss]"
        "<single|double|triple|three_data_hall|three_datacenter|ssd|memory|memory-radixtree|proxies=<PROXIES>|"
        "commit_proxies=<COMMIT_PROXIES>|grv_proxies=<GRV_PROXIES>|logs=<LOGS>|resolvers=<RESOLVERS>>*|"
        "count=<TSS_COUNT>|perpetual_storage_wiggle=<WIGGLE_SPEED>|perpetual_storage_wiggle_locality="
        "<<LOCALITY_KEY>:<LOCALITY_VALUE>|0>|storage_migration_type={disabled|gradual|aggressive}"
        "|exclude=<ADDRESS...>",
        "change the database configuration",
        "The `new' option, if present, initializes a new database with the given configuration rather than changing "
        "the configuration of an existing one. When used, both a redundancy mode and a storage engine must be "
        "specified.\n\ntss: when enabled, configures the testing storage server for the cluster instead."
        "When used with new to set up tss for the first time, it requires both a count and a storage engine."
        "To disable the testing storage server, run \"configure tss count=0\"\n\n"
        "Redundancy mode:\n  single - one copy of the data.  Not fault tolerant.\n  double - two copies "
        "of data (survive one failure).\n  triple - three copies of data (survive two failures).\n  three_data_hall - "
        "See the Admin Guide.\n  three_datacenter - See the Admin Guide.\n\nStorage engine:\n  ssd - B-Tree storage "
        "engine optimized for solid state disks.\n  memory - Durable in-memory storage engine for small "
        "datasets.\n\nproxies=<PROXIES>: Sets the desired number of proxies in the cluster. The proxy role is being "
        "deprecated and split into GRV proxy and Commit proxy, now prefer configure 'grv_proxies' and 'commit_proxies' "
        "separately. Generally we should follow that 'commit_proxies' is three times of 'grv_proxies' and "
        "'grv_proxies' "
        "should be not more than 4. If 'proxies' is specified, it will be converted to 'grv_proxies' and "
        "'commit_proxies'. "
        "Must be at least 2 (1 GRV proxy, 1 Commit proxy), or set to -1 which restores the number of proxies to the "
        "default value.\n\ncommit_proxies=<COMMIT_PROXIES>: Sets the desired number of commit proxies in the cluster. "
        "Must be at least 1, or set to -1 which restores the number of commit proxies to the default "
        "value.\n\ngrv_proxies=<GRV_PROXIES>: Sets the desired number of GRV proxies in the cluster. Must be at least "
        "1, or set to -1 which restores the number of GRV proxies to the default value.\n\nlogs=<LOGS>: Sets the "
        "desired number of log servers in the cluster. Must be at least 1, or set to -1 which restores the number of "
        "logs to the default value.\n\nresolvers=<RESOLVERS>: Sets the desired number of resolvers in the cluster. "
        "Must be at least 1, or set to -1 which restores the number of resolvers to the default value.\n\n"
        "perpetual_storage_wiggle=<WIGGLE_SPEED>: Set the value speed (a.k.a., the number of processes that the Data "
        "Distributor should wiggle at a time). Currently, only 0 and 1 are supported. The value 0 means to disable the "
        "perpetual storage wiggle.\n\n"
        "perpetual_storage_wiggle_locality=<<LOCALITY_KEY>:<LOCALITY_VALUE>|0>: Set the process filter for wiggling. "
        "The processes that match the given locality key and locality value are only wiggled. The value 0 will disable "
        "the locality filter and matches all the processes for wiggling.\n\n"
        "exclude=<ADDRESS...>: Sets the addresses in the format of IP1:port1,IP2:port2 pairs to be excluded during "
        "recruitment. Note this should be only used when the database is unavailable because of the faulty processes "
        "that are blocking the recovery from completion. The number of addresses should be less than the replication "
        "factor to avoid data loss.\n\n"

        "See the FoundationDB Administration Guide for more information."),
    &configureGenerator);

} // namespace fdb_cli
