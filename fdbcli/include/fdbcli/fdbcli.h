/*
 * fdbcli.h
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

#pragma once

#include "fdbcli/FlowLineNoise.h"

#include "fdbclient/BulkLoading.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/Arena.h"

namespace fdb_cli {

constexpr char msgTypeKey[] = "type";
constexpr char msgClusterKey[] = "cluster";
constexpr char msgCapacityKey[] = "capacity";
constexpr char msgAllocatedKey[] = "allocated";
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

extern const KeyRef advanceVersionSpecialKey;
extern const KeyRef consistencyCheckSpecialKey;
extern const KeyRef clusterDescriptionSpecialKey;
extern const KeyRef coordinatorsAutoSpecialKey;
extern const KeyRef coordinatorsProcessSpecialKey;
extern const KeyRef ddModeSpecialKey;
extern const KeyRef ddIgnoreRebalanceSpecialKey;

extern const KeyRangeRef excludedServersSpecialKeyRange;
extern const KeyRangeRef failedServersSpecialKeyRange;
extern const KeyRangeRef excludedLocalitySpecialKeyRange;
extern const KeyRangeRef failedLocalitySpecialKeyRange;
extern const KeyRef excludedForceOptionSpecialKey;
extern const KeyRef failedForceOptionSpecialKey;
extern const KeyRef excludedLocalityForceOptionSpecialKey;
extern const KeyRef failedLocalityForceOptionSpecialKey;
extern const KeyRangeRef exclusionInProgressSpecialKeyRange;

extern const KeyRef lockSpecialKey;

extern const KeyRangeRef maintenanceSpecialKeyRange;
extern const KeyRef ignoreSSFailureSpecialKey;

extern const KeyRangeRef processClassSourceSpecialKeyRange;
extern const KeyRangeRef processClassTypeSpecialKeyRange;

inline const KeyRef errorMsgSpecialKey = "\xff\xff/error_message"_sr;
inline const KeyRef workerInterfacesVerifyOptionSpecialKey = "\xff\xff/management/options/worker_interfaces/verify"_sr;

Future<bool> getWorkers(Reference<IDatabase> db, std::vector<ProcessData>* workers);
Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                        std::map<std::string, StorageServerInterface>* interfaces);

bool tokencmp(StringRef token, const char* command);
void printUsage(StringRef command);
void printLongDesc(StringRef command);

// Pre: tr failed with special_keys_api_failure error
// Read the error message special key and return the message
Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr);

// ============================================================================
// BulkDump/BulkLoad CLI Utilities
// ============================================================================

// Parse and validate a job ID token from CLI input. Prints usage and throws on invalid input.
UID validateBulkJobId(StringRef token, const char* usage);

// Get owner information suffix for display (e.g., " (owned by: backup 'daily-backup')").
// Returns empty string if no owner is set.
Future<std::string> getBulkOwnerSuffix(Database cx, UID jobId, bool isDumpJob);

// Format bytes with optional total and percentage.
std::string formatBytesProgress(int64_t completedBytes, Optional<int64_t> totalBytes);

// Print throughput, ETA, and elapsed time metrics to stdout.
void printProgressMetrics(double avgBytesPerSecond, Optional<double> etaSeconds, double elapsedSeconds);

// Print task and byte completion summary (e.g., "Tasks: 5/10, Bytes: 1.2 GB / 2.4 GB").
void printProgressSummary(const char* operationName,
                          int completeTasks,
                          int totalTasks,
                          int64_t completedBytes,
                          int64_t totalBytes);

// Print detailed task state breakdown for bulkload operations.
void printTaskBreakdown(int submittedTasks, int triggeredTasks, int runningTasks, int completeTasks, int errorTasks);

// ============================================================================
// BulkDump/BulkLoad Health Analysis
// Used by "bulkdump status --analyze" and "bulkload status --analyze"
// ============================================================================

// Health assessment based on throughput, efficiency, and error rates.
// healthScore: 0-100 rating, healthStatus: "healthy"/"degraded"/"critical"
struct BulkHealthMetrics {
	double healthScore;
	std::string healthStatus;
	std::vector<std::string> recommendations;

	static BulkHealthMetrics analyze(double throughputMBps,
	                                 double efficiencyPercent,
	                                 int stalledTasks,
	                                 int errorTasks,
	                                 double elapsedMinutes);
};

// Error categorization and actionable diagnostics.
struct BulkErrorAnalysis {
	int totalErrors;
	std::map<std::string, int> errorCategories;
	std::vector<std::string> criticalIssues;
	std::vector<std::string> actionableAdvice;

	static BulkErrorAnalysis analyze(int errorTasks, int stalledTasks, const std::vector<std::string>& recentErrors);
};

// Performance and operational recommendations based on current metrics.
struct BulkOptimizationRecommendations {
	std::vector<std::string> performanceRecommendations;
	std::vector<std::string> reliabilityRecommendations;
	std::vector<std::string> operationalRecommendations;

	static BulkOptimizationRecommendations generate(double throughputMBps,
	                                                double efficiency,
	                                                int stalledTasks,
	                                                int errorTasks,
	                                                double elapsedMinutes,
	                                                int totalTasks);
};

// Print formatted health analysis to stdout.
void printBulkHealthAnalysis(const BulkHealthMetrics& health);

// Print error diagnostics and advice to stdout.
void printErrorDiagnostics(const BulkErrorAnalysis& analysis);

// Print optimization recommendations to stdout.
void printOptimizationRecommendations(const BulkOptimizationRecommendations& recs);

// Print warnings for tasks that have stalled (no progress for extended period).
void printStalledTasks(const std::vector<BulkLoadStalledTask>& stalledTasks);

// Run full analysis and print results: health metrics, stalled tasks, error diagnostics, recommendations.
void printBulkAnalysis(double avgBytesPerSecond,
                       double elapsedSeconds,
                       int completeTasks,
                       int totalTasks,
                       int errorTasks,
                       const std::vector<BulkLoadStalledTask>& stalledTasks);

// Using \xff\xff/worker_interfaces/ special key, get all worker interfaces.
// A worker list will be returned from CC.
// If verify, we will try to establish connections to all workers returned.
// In particular, it will deserialize \xff\xff/worker_interfaces/<address>:=<ClientInterface> kv pairs and issue RPC
// calls, then only return interfaces(kv pairs) the client can talk to
Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
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
Future<bool> advanceVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// configure command
Future<bool> configureCommandActor(Reference<IDatabase> db,
                                   Database localDb,
                                   std::vector<StringRef> tokens,
                                   LineNoise* linenoise,
                                   Future<Void> _warn);
// consistency command
Future<bool> consistencyCheckCommandActor(Reference<ITransaction> tr,
                                          std::vector<StringRef> const& tokens,
                                          bool intrans);
// consistency scan command
Future<bool> consistencyScanCommandActor(Database localDb, std::vector<StringRef> const& tokens);
// coordinators command
Future<bool> coordinatorsCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// datadistribution command
Future<bool> dataDistributionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// exclude command
Future<bool> excludeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens, Future<Void> _warn);
// expensive_data_check command
Future<bool> expensiveDataCheckCommandActor(
    Reference<IDatabase> db,
    Reference<ITransaction> tr,
    std::vector<StringRef> tokens,
    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// fileconfigure command
Future<bool> fileConfigureCommandActor(Reference<IDatabase> db,
                                       std::string const& filePath,
                                       bool isNewDatabase,
                                       bool force);
// Trigger audit storage
Future<UID> auditStorageCommandActor(Reference<IClusterConnectionRecord> clusterFile, std::vector<StringRef> tokens);
// Retrieve audit storage status
Future<bool> getAuditStatusCommandActor(Database cx, std::vector<StringRef> tokens);
// Retrieve shard information command
Future<bool> locationMetadataCommandActor(Database cx, std::vector<StringRef> tokens);
// Bulk loading command
Future<UID> bulkLoadCommandActor(Database cx, std::vector<StringRef> tokens);
// Bulk dumping command
Future<UID> bulkDumpCommandActor(Database cx, std::vector<StringRef> tokens);
// force_recovery_with_data_loss command
Future<bool> forceRecoveryWithDataLossCommandActor(Reference<IDatabase> db, std::vector<StringRef> const& tokens);
// include command
Future<bool> includeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// kill command
Future<bool> killCommandActor(Reference<IDatabase> db,
                              Reference<ITransaction> tr,
                              std::vector<StringRef> tokens,
                              std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// lock/unlock command
Future<bool> lockCommandActor(Reference<IDatabase> db, std::vector<StringRef> const& tokens);
Future<bool> unlockDatabaseActor(Reference<IDatabase> db, UID uid);

// hotrange command
Future<bool> hotRangeCommandActor(Database localDb,
                                  Reference<IDatabase> db,
                                  std::vector<StringRef> const& tokens,
                                  std::map<std::string, StorageServerInterface>* const& storage_interface);

// maintenance command
Future<bool> setHealthyZone(Reference<IDatabase> db, StringRef zoneId, double seconds, bool printWarning = false);
Future<bool> clearHealthyZone(Reference<IDatabase> db,
                              bool printWarning = false,
                              bool clearSSFailureZoneString = false);
Future<bool> maintenanceCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// profile command
Future<bool> profileCommandActor(Database db, Reference<ITransaction> tr, std::vector<StringRef> tokens, bool intrans);
// quota command
Future<bool> quotaCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// setclass command
Future<bool> setClassCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// snapshot command
Future<bool> snapshotCommandActor(Reference<IDatabase> db, std::vector<StringRef> const& tokens);
// status command
Future<bool> statusCommandActor(Reference<IDatabase> db,
                                Database localDb,
                                std::vector<StringRef> tokens,
                                bool isExecMode = false);
// suspend command
Future<bool> suspendCommandActor(Reference<IDatabase> db,
                                 Reference<ITransaction> tr,
                                 std::vector<StringRef> const& tokens,
                                 std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// throttle command
Future<bool> throttleCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// triggerteaminfolog command
Future<bool> triggerddteaminfologCommandActor(Reference<IDatabase> db);
// tssq command
Future<bool> tssqCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// versionepoch command
Future<bool> versionEpochCommandActor(Reference<IDatabase> db, Database cx, std::vector<StringRef> tokens);
// targetversion command
Future<bool> targetVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// idempotencyids command
Future<bool> idempotencyIdsCommandActor(Database cx, std::vector<StringRef> const& tokens);

// rangeconfig command
Future<bool> rangeConfigCommandActor(Database cx, std::vector<StringRef> tokens);

// debug commands: getlocation, getall
Future<bool> getLocationCommandActor(Database cx, std::vector<StringRef> tokens);
Future<bool> getallCommandActor(Database cx, std::vector<StringRef> tokens, Version version);
Future<bool> checkallCommandActor(Database cx, std::vector<StringRef> tokens);
} // namespace fdb_cli
