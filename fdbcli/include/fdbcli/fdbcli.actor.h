/*
 * fdbcli.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated
// version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLI_FDBCLI_ACTOR_G_H)
#define FDBCLI_FDBCLI_ACTOR_G_H
#include "fdbcli/fdbcli.actor.g.h"
#elif !defined(FDBCLI_FDBCLI_ACTOR_H)
#define FDBCLI_FDBCLI_ACTOR_H

#include "fdbcli/FlowLineNoise.h"

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/Arena.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

constexpr char msgTypeKey[] = "type";
constexpr char msgClusterKey[] = "cluster";
constexpr char msgClusterTypeKey[] = "cluster_type";
constexpr char msgMetaclusterName[] = "metacluster_name";
constexpr char msgMetaclusterKey[] = "metacluster";
constexpr char msgDataClustersKey[] = "data_clusters";
constexpr char msgCapacityKey[] = "capacity";
constexpr char msgAllocatedKey[] = "allocated";
constexpr char msgTenantIdPrefixKey[] = "tenant_id_prefix";
constexpr char msgErrorKey[] = "error";

struct CommandHelp {
	std::string usage;
	std::string short_desc;
	std::string long_desc;
	CommandHelp() {}
	CommandHelp(const char* usage, const char* short_desc, const char* long_desc)
	  : usage(usage), short_desc(short_desc), long_desc(long_desc) {}
};

void arrayGenerator(const char* text, const char* line, const char** options, std::vector<std::string>& lc);

struct CommandFactory {
	typedef void (*CompletionGeneratorFunc)(const char* text,
	                                        const char* line,
	                                        std::vector<std::string>& lc,
	                                        std::vector<StringRef> const& tokens);

	typedef std::vector<const char*> (*HintGeneratorFunc)(std::vector<StringRef> const& tokens, bool inArgument);

	CommandFactory(const char* name,
	               CommandHelp help,
	               CompletionGeneratorFunc completionFunc = nullptr,
	               HintGeneratorFunc hintFunc = nullptr) {
		commands()[name] = help;
		if (completionFunc) {
			completionGenerators()[name] = completionFunc;
		}
		if (hintFunc) {
			hintGenerators()[name] = hintFunc;
		}
	}
	CommandFactory(const char* name) { hiddenCommands().insert(name); }
	static std::map<std::string, CommandHelp>& commands() {
		static std::map<std::string, CommandHelp> helpMap;
		return helpMap;
	}
	static std::set<std::string>& hiddenCommands() {
		static std::set<std::string> commands;
		return commands;
	}
	static std::map<std::string, CompletionGeneratorFunc>& completionGenerators() {
		static std::map<std::string, CompletionGeneratorFunc> completionMap;
		return completionMap;
	}
	static std::map<std::string, HintGeneratorFunc>& hintGenerators() {
		static std::map<std::string, HintGeneratorFunc> hintMap;
		return hintMap;
	}
};

// Special keys used by fdbcli commands
// advanceversion
extern const KeyRef advanceVersionSpecialKey;
// consistencycheck
extern const KeyRef consistencyCheckSpecialKey;
// coordinators
extern const KeyRef clusterDescriptionSpecialKey;
extern const KeyRef configDBSpecialKey;
extern const KeyRef coordinatorsAutoSpecialKey;
extern const KeyRef coordinatorsProcessSpecialKey;
// datadistribution
extern const KeyRef ddModeSpecialKey;
extern const KeyRef ddIgnoreRebalanceSpecialKey;
// exclude/include
extern const KeyRangeRef excludedServersSpecialKeyRange;
extern const KeyRangeRef failedServersSpecialKeyRange;
extern const KeyRangeRef excludedLocalitySpecialKeyRange;
extern const KeyRangeRef failedLocalitySpecialKeyRange;
extern const KeyRef excludedForceOptionSpecialKey;
extern const KeyRef failedForceOptionSpecialKey;
extern const KeyRef excludedLocalityForceOptionSpecialKey;
extern const KeyRef failedLocalityForceOptionSpecialKey;
extern const KeyRangeRef exclusionInProgressSpecialKeyRange;
// lock/unlock
extern const KeyRef lockSpecialKey;
// maintenance
extern const KeyRangeRef maintenanceSpecialKeyRange;
extern const KeyRef ignoreSSFailureSpecialKey;
// setclass
extern const KeyRangeRef processClassSourceSpecialKeyRange;
extern const KeyRangeRef processClassTypeSpecialKeyRange;
// Other special keys
inline const KeyRef errorMsgSpecialKey = "\xff\xff/error_message"_sr;
inline const KeyRef workerInterfacesVerifyOptionSpecialKey = "\xff\xff/management/options/worker_interfaces/verify"_sr;
// help functions (Copied from fdbcli.actor.cpp)

// get all workers' info
ACTOR Future<bool> getWorkers(Reference<IDatabase> db, std::vector<ProcessData>* workers);
// get all storages' interface
ACTOR Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                              std::map<std::string, StorageServerInterface>* interfaces);

// compare StringRef with the given c string
bool tokencmp(StringRef token, const char* command);
// print the usage of the specified command
void printUsage(StringRef command);
// Pre: tr failed with special_keys_api_failure error
// Read the error message special key and return the message
ACTOR Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr);
// Using \xff\xff/worker_interfaces/ special key, get all worker interfaces.
// A worker list will be returned from CC.
// If verify, we will try to establish connections to all workers returned.
// In particular, it will deserialize \xff\xff/worker_interfaces/<address>:=<ClientInterface> kv pairs and issue RPC
// calls, then only return interfaces(kv pairs) the client can talk to
ACTOR Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                       bool verify = false);
// print cluster status info
void printStatus(StatusObjectReader statusObj,
                 StatusClient::StatusLevel level,
                 bool displayDatabaseAvailable = true,
                 bool hideErrorMessages = false);

// All fdbcli commands (alphabetically)
// All below actors return true if the command is executed successfully
// advanceversion command
ACTOR Future<bool> advanceVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// cache_range command
ACTOR Future<bool> cacheRangeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// configure command
ACTOR Future<bool> configureCommandActor(Reference<IDatabase> db,
                                         Database localDb,
                                         std::vector<StringRef> tokens,
                                         LineNoise* linenoise,
                                         Future<Void> warn);
// consistency command
ACTOR Future<bool> consistencyCheckCommandActor(Reference<ITransaction> tr,
                                                std::vector<StringRef> tokens,
                                                bool intrans);
// consistency scan command
ACTOR Future<bool> consistencyScanCommandActor(Database localDb, std::vector<StringRef> tokens);
// coordinators command
ACTOR Future<bool> coordinatorsCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// datadistribution command
ACTOR Future<bool> dataDistributionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// exclude command
ACTOR Future<bool> excludeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens, Future<Void> warn);
// expensive_data_check command
ACTOR Future<bool> expensiveDataCheckCommandActor(
    Reference<IDatabase> db,
    Reference<ITransaction> tr,
    std::vector<StringRef> tokens,
    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// fileconfigure command
ACTOR Future<bool> fileConfigureCommandActor(Reference<IDatabase> db,
                                             std::string filePath,
                                             bool isNewDatabase,
                                             bool force);
// Trigger audit storage
ACTOR Future<UID> auditStorageCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                           std::vector<StringRef> tokens);
// Retrieve audit storage status
ACTOR Future<bool> getAuditStatusCommandActor(Database cx, std::vector<StringRef> tokens);
// Retrieve shard information command
ACTOR Future<bool> locationMetadataCommandActor(Database cx, std::vector<StringRef> tokens);
// Bulk loading command
ACTOR Future<UID> bulkLoadCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                       Database cx,
                                       std::vector<StringRef> tokens);
// Bulk dumping command
ACTOR Future<UID> bulkDumpCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                       Database cx,
                                       std::vector<StringRef> tokens);
// force_recovery_with_data_loss command
ACTOR Future<bool> forceRecoveryWithDataLossCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// include command
ACTOR Future<bool> includeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// kill command
ACTOR Future<bool> killCommandActor(Reference<IDatabase> db,
                                    Reference<ITransaction> tr,
                                    std::vector<StringRef> tokens,
                                    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// lock/unlock command
ACTOR Future<bool> lockCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
ACTOR Future<bool> unlockDatabaseActor(Reference<IDatabase> db, UID uid);

// metacluster command
Future<bool> metaclusterCommand(Reference<IDatabase> db, std::vector<StringRef> tokens);

// changefeed command
ACTOR Future<bool> changeFeedCommandActor(Database localDb,
                                          Optional<TenantMapEntry> tenantEntry,
                                          std::vector<StringRef> tokens,
                                          Future<Void> warn);
// blobrange command
ACTOR Future<bool> blobRangeCommandActor(Database localDb,
                                         Optional<TenantMapEntry> tenantEntry,
                                         std::vector<StringRef> tokens);

// blobkey command
ACTOR Future<bool> blobKeyCommandActor(Database localDb,
                                       Optional<TenantMapEntry> tenantEntry,
                                       std::vector<StringRef> tokens);
// blobrestore command
ACTOR Future<bool> blobRestoreCommandActor(Database localDb, std::vector<StringRef> tokens);
// hotrange command
ACTOR Future<bool> hotRangeCommandActor(Database localDb,
                                        Reference<IDatabase> db,
                                        std::vector<StringRef> tokens,
                                        std::map<std::string, StorageServerInterface>* storage_interface);

// maintenance command
ACTOR Future<bool> setHealthyZone(Reference<IDatabase> db, StringRef zoneId, double seconds, bool printWarning = false);
ACTOR Future<bool> clearHealthyZone(Reference<IDatabase> db,
                                    bool printWarning = false,
                                    bool clearSSFailureZoneString = false);
ACTOR Future<bool> maintenanceCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// profile command
ACTOR Future<bool> profileCommandActor(Database db,
                                       Reference<ITransaction> tr,
                                       std::vector<StringRef> tokens,
                                       bool intrans);
// quota command
ACTOR Future<bool> quotaCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// setclass command
ACTOR Future<bool> setClassCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// snapshot command
ACTOR Future<bool> snapshotCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// status command
ACTOR Future<bool> statusCommandActor(Reference<IDatabase> db,
                                      Database localDb,
                                      std::vector<StringRef> tokens,
                                      bool isExecMode = false);
// suspend command
ACTOR Future<bool> suspendCommandActor(Reference<IDatabase> db,
                                       Reference<ITransaction> tr,
                                       std::vector<StringRef> tokens,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// tenant command
Future<bool> tenantCommand(Reference<IDatabase> db, std::vector<StringRef> tokens);
// tenant command compatibility layer
Future<bool> tenantCommandForwarder(Reference<IDatabase> db, std::vector<StringRef> tokens);
// tenantgroup command
Future<bool> tenantGroupCommand(Reference<IDatabase> db, std::vector<StringRef> tokens);
// throttle command
ACTOR Future<bool> throttleCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// triggerteaminfolog command
ACTOR Future<bool> triggerddteaminfologCommandActor(Reference<IDatabase> db);
// tssq command
ACTOR Future<bool> tssqCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// versionepoch command
ACTOR Future<bool> versionEpochCommandActor(Reference<IDatabase> db, Database cx, std::vector<StringRef> tokens);
// targetversion command
ACTOR Future<bool> targetVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// idempotencyids command
ACTOR Future<bool> idempotencyIdsCommandActor(Database cx, std::vector<StringRef> tokens);

// rangeconfig command
ACTOR Future<bool> rangeConfigCommandActor(Database cx, std::vector<StringRef> tokens);

// debug commands: getlocation, getall
ACTOR Future<bool> getLocationCommandActor(Database cx, std::vector<StringRef> tokens);
ACTOR Future<bool> getallCommandActor(Database cx, std::vector<StringRef> tokens, Version version);
ACTOR Future<bool> checkallCommandActor(Database cx, std::vector<StringRef> tokens);
} // namespace fdb_cli

#include "flow/unactorcompiler.h"
#endif
