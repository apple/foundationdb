/*
 * Util.cpp
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

#include "fdbcli/fdbcli.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/Status.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "flow/Arena.h"
#include "flow/ThreadHelper.actor.h"
#include <fmt/core.h>

namespace fdb_cli {

bool tokencmp(StringRef token, const char* command) {
	if (token.size() != strlen(command))
		return false;

	return !memcmp(token.begin(), command, token.size());
}

void printUsage(StringRef command) {
	const auto& helpMap = CommandFactory::commands();
	auto i = helpMap.find(command.toString());
	if (i != helpMap.end())
		printf("Usage: %s\n", i->second.usage.c_str());
	else
		fprintf(stderr, "ERROR: Unknown command `%s'\n", command.toString().c_str());
}

void printLongDesc(StringRef command) {
	const auto& helpMap = CommandFactory::commands();
	auto i = helpMap.find(command.toString());
	if (i != helpMap.end())
		printf("%s\n", i->second.long_desc.c_str());
	else
		fprintf(stderr, "ERROR: Unknown command `%s'\n", command.toString().c_str());
}

Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr) {
	// hold the returned standalone object's memory
	ThreadFuture<Optional<Value>> errorMsgF = tr->get(fdb_cli::errorMsgSpecialKey);
	Optional<Value> errorMsg = co_await safeThreadFutureToFuture(errorMsgF);
	// Error message should be present
	ASSERT(errorMsg.present());
	// Read the json string
	auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
	// verify schema
	auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
	std::string errorStr;
	ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
	// return the error message
	co_return valueObj["message"].get_str();
}

void addInterfacesFromKVs(RangeResult& kvs,
                          std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	for (const auto& kv : kvs) {
		ClientWorkerInterface workerInterf;
		try {
			// the interface is back-ward compatible, thus if parsing failed, it needs to upgrade cli version
			workerInterf = BinaryReader::fromStringRef<ClientWorkerInterface>(kv.value, IncludeVersion());
		} catch (Error& e) {
			fprintf(stderr, "Error: %s; CLI version is too old, please update to use a newer version\n", e.what());
			return;
		}
		ClientLeaderRegInterface leaderInterf(workerInterf.address());
		StringRef ip_port = (kv.key.endsWith(":tls"_sr) ? kv.key.removeSuffix(":tls"_sr) : kv.key)
		                        .removePrefix("\xff\xff/worker_interfaces/"_sr);
		(*address_interface)[ip_port] = std::make_pair(kv.value, leaderInterf);

		if (workerInterf.reboot.getEndpoint().addresses.secondaryAddress.present()) {
			Key full_ip_port2 =
			    StringRef(workerInterf.reboot.getEndpoint().addresses.secondaryAddress.get().toString());
			StringRef ip_port2 =
			    full_ip_port2.endsWith(":tls"_sr) ? full_ip_port2.removeSuffix(":tls"_sr) : full_ip_port2;
			(*address_interface)[ip_port2] = std::make_pair(kv.value, leaderInterf);
		}
	}
}

Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                 std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                 bool verify) {
	if (verify) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->set(workerInterfacesVerifyOptionSpecialKey, ValueRef());
	}
	// Hold the reference to the standalone's memory
	ThreadFuture<RangeResult> kvsFuture = tr->getRange(
	    KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr), CLIENT_KNOBS->TOO_MANY);
	RangeResult kvs = co_await safeThreadFutureToFuture(kvsFuture);
	ASSERT(!kvs.more);
	if (verify) {
		// remove the option if set
		tr->clear(workerInterfacesVerifyOptionSpecialKey);
	}
	addInterfacesFromKVs(kvs, address_interface);
}

Future<bool> getWorkers(Reference<IDatabase> db, std::vector<ProcessData>* workers) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ThreadFuture<RangeResult> processClasses = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
			ThreadFuture<RangeResult> processData = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);

			co_await (success(safeThreadFutureToFuture(processClasses)) &&
			          success(safeThreadFutureToFuture(processData)));
			ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

			std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
			int i{ 0 };
			for (i = 0; i < processClasses.get().size(); i++) {
				try {
					id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
					    decodeProcessClassValue(processClasses.get()[i].value);
				} catch (Error& e) {
					fprintf(stderr, "Error: %s; Client version is too old, please use a newer version\n", e.what());
					co_return false;
				}
			}

			for (i = 0; i < processData.get().size(); i++) {
				ProcessData data = decodeWorkerListValue(processData.get()[i].value);
				ProcessClass processClass = id_class[data.locality.processId()];

				if (processClass.classSource() == ProcessClass::DBSource ||
				    data.processClass.classType() == ProcessClass::UnsetClass)
					data.processClass = processClass;

				if (data.processClass.classType() != ProcessClass::TesterClass)
					workers->push_back(data);
			}

			co_return true;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevWarn, "GetWorkersError").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                        std::map<std::string, StorageServerInterface>* interfaces) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		interfaces->clear();
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ThreadFuture<RangeResult> serverListF = tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			co_await safeThreadFutureToFuture(serverListF);
			ASSERT(!serverListF.get().more);
			ASSERT_LT(serverListF.get().size(), CLIENT_KNOBS->TOO_MANY);
			RangeResult serverList = serverListF.get();
			// decode server interfaces
			for (int i = 0; i < serverList.size(); i++) {
				auto ssi = decodeServerListValue(serverList[i].value);
				(*interfaces)[ssi.address().toString()] = ssi;
			}
			co_return;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevWarn, "GetStorageServerInterfacesError").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

// Shared UID validation for bulk operations (BulkDump/BulkLoad)
UID validateBulkJobId(StringRef token, const char* usage) {
	UID jobId;
	try {
		jobId = UID::fromStringThrowsOnFailure(token.toString());
	} catch (Error&) {
		fmt::println("ERROR: Invalid job id '{}' (expected 32 hex characters)", token.toString());
		fmt::println("{}", usage);
		throw operation_failed();
	}

	if (!jobId.isValid()) {
		fmt::println("ERROR: Invalid job id {}", token.toString());
		fmt::println("{}", usage);
		throw operation_failed();
	}

	return jobId;
}

Future<std::string> getBulkOwnerSuffix(Database cx, UID jobId, bool isDumpJob) {
	if (isDumpJob) {
		Optional<BulkDumpOwnerInfo> ownerInfo = co_await getBulkDumpOwner(cx, jobId);
		if (ownerInfo.present()) {
			co_return fmt::format(" (owned by {} '{}')", ownerInfo.get().ownerType, ownerInfo.get().ownerName);
		}
	} else {
		Optional<BulkDumpOwnerInfo> ownerInfo = co_await getBulkLoadOwner(cx, jobId);
		if (ownerInfo.present()) {
			co_return fmt::format(" (owned by {} '{}')", ownerInfo.get().ownerType, ownerInfo.get().ownerName);
		}
	}
	co_return std::string("");
}

// Format common progress metrics (throughput, ETA, elapsed time)
void printProgressMetrics(double avgBytesPerSecond, Optional<double> etaSeconds, double elapsedSeconds) {
	// Throughput
	if (avgBytesPerSecond > 0) {
		fmt::println(" Throughput - {:.1f} MB/s", avgBytesPerSecond / 1048576.0);
	}

	// ETA
	if (etaSeconds.present()) {
		fmt::println(" Estimated time remaining - {}", formatDurationHumanReadable((int)etaSeconds.get()));
	}

	// Elapsed time
	if (elapsedSeconds > 0) {
		fmt::println(" Elapsed time - {}", formatDurationHumanReadable((int)elapsedSeconds));
	}
}

// Print detailed task breakdown for bulk operations
void printTaskBreakdown(int submittedTasks, int triggeredTasks, int runningTasks, int completeTasks, int errorTasks) {
	int totalTasks = submittedTasks + triggeredTasks + runningTasks + completeTasks + errorTasks;
	if (totalTasks > 0) {
		fmt::println(" Task Status Breakdown:");
		if (submittedTasks > 0)
			fmt::println("   Submitted: {}", submittedTasks);
		if (triggeredTasks > 0)
			fmt::println("   Triggered: {}", triggeredTasks);
		if (runningTasks > 0)
			fmt::println("   Running: {}", runningTasks);
		if (completeTasks > 0)
			fmt::println("   Complete: {}", completeTasks);
		if (errorTasks > 0)
			fmt::println("   Error: {}", errorTasks);
	}
}

// Advanced performance monitoring for bulk operations
BulkHealthMetrics BulkHealthMetrics::analyze(double throughputMBps,
                                             double efficiencyPercent,
                                             int stalledTasks,
                                             int errorTasks,
                                             double elapsedMinutes) {
	BulkHealthMetrics metrics;

	// Calculate health score (weighted average)
	double throughputScore = std::min(100.0, throughputMBps * 2); // 50MB/s = 100 points
	double efficiencyScore = efficiencyPercent;
	double reliabilityScore = std::max(0.0, 100.0 - (stalledTasks * 10) - (errorTasks * 20));

	metrics.healthScore = (throughputScore * 0.4) + (efficiencyScore * 0.4) + (reliabilityScore * 0.2);

	// Determine status
	if (metrics.healthScore >= 90) {
		metrics.healthStatus = "Excellent";
	} else if (metrics.healthScore >= 75) {
		metrics.healthStatus = "Good";
	} else if (metrics.healthScore >= 50) {
		metrics.healthStatus = "Fair";
	} else {
		metrics.healthStatus = "Poor";
	}

	// Generate recommendations
	if (throughputMBps < 5) {
		metrics.recommendations.push_back("Consider increasing cluster resources for better throughput");
	}
	if (errorTasks > 0) {
		metrics.recommendations.push_back("Investigate error tasks for potential issues");
	}
	if (stalledTasks > 0) {
		metrics.recommendations.push_back("Monitor stalled tasks - may indicate network or storage issues");
	}
	if (elapsedMinutes > 60 && efficiencyPercent < 50) {
		metrics.recommendations.push_back("Long-running job with low efficiency - consider optimization");
	}

	return metrics;
}

void printBulkHealthAnalysis(const BulkHealthMetrics& health) {
	fmt::println("");
	fmt::println("Health Analysis:");
	fmt::println(" Overall Health: {:.1f}/100 ({})", health.healthScore, health.healthStatus);

	if (!health.recommendations.empty()) {
		fmt::println(" Recommendations");
		for (const auto& rec : health.recommendations) {
			fmt::println("   - {}", rec);
		}
	}
}

BulkErrorAnalysis BulkErrorAnalysis::analyze(int errorTasks,
                                             int stalledTasks,
                                             const std::vector<std::string>& recentErrors) {
	BulkErrorAnalysis analysis;
	analysis.totalErrors = errorTasks;

	// Categorize errors
	for (const auto& error : recentErrors) {
		if (error.find("timeout") != std::string::npos) {
			analysis.errorCategories["Timeout Issues"]++;
		} else if (error.find("network") != std::string::npos || error.find("connection") != std::string::npos) {
			analysis.errorCategories["Network Issues"]++;
		} else if (error.find("permission") != std::string::npos || error.find("access") != std::string::npos) {
			analysis.errorCategories["Access Issues"]++;
		} else if (error.find("disk") != std::string::npos || error.find("storage") != std::string::npos) {
			analysis.errorCategories["Storage Issues"]++;
		} else {
			analysis.errorCategories["Other Issues"]++;
		}
	}

	return analysis;
}

void printErrorDiagnostics(const BulkErrorAnalysis& analysis) {
	if (analysis.totalErrors > 0) {
		fmt::println("");
		fmt::println("Error Diagnostics:");

		if (!analysis.errorCategories.empty()) {
			fmt::println(" Error Categories:");
			for (const auto& [category, count] : analysis.errorCategories) {
				fmt::println("   {} - {} occurrences", category, count);
			}
		}
	}
}

BulkOptimizationRecommendations BulkOptimizationRecommendations::generate(double throughputMBps,
                                                                          double efficiency,
                                                                          int stalledTasks,
                                                                          int errorTasks,
                                                                          double elapsedMinutes,
                                                                          int totalTasks) {
	BulkOptimizationRecommendations recs;

	// Performance recommendations
	if (throughputMBps < 10 && totalTasks > 100) {
		recs.performanceRecommendations.push_back(
		    "Low throughput detected. Consider increasing storage server resources or parallelism");
	}
	if (elapsedMinutes > 30 && efficiency < 80) {
		recs.performanceRecommendations.push_back(
		    "Long-running job with low efficiency. Consider optimizing shard sizes or task distribution");
	}

	return recs;
}

void printOptimizationRecommendations(const BulkOptimizationRecommendations& recs) {
	if (!recs.performanceRecommendations.empty()) {
		fmt::println("");
		fmt::println("Optimization Recommendations:");
		fmt::println(" Performance:");
		for (const auto& rec : recs.performanceRecommendations) {
			fmt::println("   - {}", rec);
		}
	}
}

// Format bytes with progress percentage for bulk operations
std::string formatBytesProgress(int64_t completedBytes, Optional<int64_t> totalBytes) {
	std::string result = format("%.2f MB", completedBytes / 1048576.0);
	if (totalBytes.present()) {
		result += fmt::format(" / {:.2f} MB ({:.1f}%)",
		                      totalBytes.get() / 1048576.0,
		                      totalBytes.get() > 0 ? 100.0 * completedBytes / totalBytes.get() : 0.0);
	}
	return result;
}

// Print progress summary for bulk operations
void printProgressSummary(const char* operationName,
                          int completeTasks,
                          int totalTasks,
                          int64_t completedBytes,
                          int64_t totalBytes) {
	fmt::println("");
	fmt::println("{} Progress Summary:", operationName);
	fmt::println(" Tasks: {} / {} ({:.1f}%)",
	             completeTasks,
	             totalTasks,
	             totalTasks > 0 ? 100.0 * completeTasks / totalTasks : 0.0);
	fmt::println(" Data: {}", formatBytesProgress(completedBytes, totalBytes));
}

void printStalledTasks(const std::vector<BulkLoadStalledTask>& stalledTasks) {
	if (!stalledTasks.empty()) {
		fmt::println("");
		fmt::println("WARNING: {} stalled tasks (no progress > 60s):", stalledTasks.size());
		for (const auto& stalled : stalledTasks) {
			fmt::println(" Task {}: {}, stalled {:.0f}s, {} restarts",
			             stalled.taskId.shortString(),
			             stalled.range.toString(),
			             stalled.stalledSeconds,
			             stalled.restartCount);
			if (!stalled.lastError.empty()) {
				fmt::println("           Last error: {}", stalled.lastError);
			}
		}
	}
}

void printBulkAnalysis(double avgBytesPerSecond,
                       double elapsedSeconds,
                       int completeTasks,
                       int totalTasks,
                       int errorTasks,
                       const std::vector<BulkLoadStalledTask>& stalledTasks) {
	double efficiency = totalTasks > 0 ? 100.0 * completeTasks / totalTasks : 0.0;

	auto healthMetrics = BulkHealthMetrics::analyze(
	    avgBytesPerSecond / 1048576.0, efficiency, stalledTasks.size(), errorTasks, elapsedSeconds / 60.0);
	printBulkHealthAnalysis(healthMetrics);

	printStalledTasks(stalledTasks);

	std::vector<std::string> recentErrors;
	auto errorAnalysis = BulkErrorAnalysis::analyze(errorTasks, stalledTasks.size(), recentErrors);
	printErrorDiagnostics(errorAnalysis);

	auto recommendations = BulkOptimizationRecommendations::generate(
	    avgBytesPerSecond / 1048576.0, efficiency, stalledTasks.size(), errorTasks, elapsedSeconds / 60.0, totalTasks);
	printOptimizationRecommendations(recommendations);

	if (errorTasks > 0) {
		fmt::println("");
		fmt::println("WARNING: {} tasks in error state", errorTasks);
	}
}

} // namespace fdb_cli
