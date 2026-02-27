/*
 * Util.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/Status.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "flow/Arena.h"
#include "flow/ThreadHelper.actor.h"
#include <fmt/core.h>
#include "flow/actorcompiler.h" // This must be the last #include.

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

ACTOR Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr) {
	// hold the returned standalone object's memory
	state ThreadFuture<Optional<Value>> errorMsgF = tr->get(fdb_cli::errorMsgSpecialKey);
	Optional<Value> errorMsg = wait(safeThreadFutureToFuture(errorMsgF));
	// Error message should be present
	ASSERT(errorMsg.present());
	// Read the json string
	auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
	// verify schema
	auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
	std::string errorStr;
	ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
	// return the error message
	return valueObj["message"].get_str();
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

ACTOR Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                       bool verify) {
	if (verify) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->set(workerInterfacesVerifyOptionSpecialKey, ValueRef());
	}
	// Hold the reference to the standalone's memory
	state ThreadFuture<RangeResult> kvsFuture = tr->getRange(
	    KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr), CLIENT_KNOBS->TOO_MANY);
	state RangeResult kvs = wait(safeThreadFutureToFuture(kvsFuture));
	ASSERT(!kvs.more);
	if (verify) {
		// remove the option if set
		tr->clear(workerInterfacesVerifyOptionSpecialKey);
	}
	addInterfacesFromKVs(kvs, address_interface);
	return Void();
}

ACTOR Future<bool> getWorkers(Reference<IDatabase> db, std::vector<ProcessData>* workers) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state ThreadFuture<RangeResult> processClasses = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
			state ThreadFuture<RangeResult> processData = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);

			wait(success(safeThreadFutureToFuture(processClasses)) && success(safeThreadFutureToFuture(processData)));
			ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

			state std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
			state int i;
			for (i = 0; i < processClasses.get().size(); i++) {
				try {
					id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
					    decodeProcessClassValue(processClasses.get()[i].value);
				} catch (Error& e) {
					fprintf(stderr, "Error: %s; Client version is too old, please use a newer version\n", e.what());
					return false;
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

			return true;
		} catch (Error& e) {
			TraceEvent(SevWarn, "GetWorkersError").error(e);
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                              std::map<std::string, StorageServerInterface>* interfaces) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		interfaces->clear();
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state ThreadFuture<RangeResult> serverListF = tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			wait(success(safeThreadFutureToFuture(serverListF)));
			ASSERT(!serverListF.get().more);
			ASSERT_LT(serverListF.get().size(), CLIENT_KNOBS->TOO_MANY);
			RangeResult serverList = serverListF.get();
			// decode server interfaces
			for (int i = 0; i < serverList.size(); i++) {
				auto ssi = decodeServerListValue(serverList[i].value);
				(*interfaces)[ssi.address().toString()] = ssi;
			}
			return Void();
		} catch (Error& e) {
			TraceEvent(SevWarn, "GetStorageServerInterfacesError").error(e);
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
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

// Get owner info suffix for bulk operations (BulkDump/BulkLoad)
ACTOR Future<std::string> getBulkOwnerSuffix(Database cx, UID jobId, bool isDumpJob) {
	if (isDumpJob) {
		Optional<BulkDumpOwnerInfo> ownerInfo = wait(getBulkDumpOwner(cx, jobId));
		if (ownerInfo.present()) {
			return fmt::format(" (owned by {} '{}')", ownerInfo.get().ownerType, ownerInfo.get().ownerName);
		}
	} else {
		Optional<BulkDumpOwnerInfo> ownerInfo = wait(getBulkLoadOwner(cx, jobId));
		if (ownerInfo.present()) {
			return fmt::format(" (owned by {} '{}')", ownerInfo.get().ownerType, ownerInfo.get().ownerName);
		}
	}
	return std::string("");
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

// Enhanced progress summary for bulk operations
void printProgressSummary(const char* operationName,
                          int completeTasks,
                          int totalTasks,
                          int64_t completedBytes,
                          int64_t totalBytes) {
	fmt::println("Progress Summary for {}:", operationName);
	fmt::println(" Tasks - {}/{} ({:.1f}%)",
	             completeTasks,
	             totalTasks,
	             totalTasks > 0 ? 100.0 * completeTasks / totalTasks : 0.0);
	fmt::println(" Bytes - {}", formatBytesProgress(completedBytes, totalBytes));
}

// Validate bulk operation mode setting (on/off)
Future<UID> validateAndSetBulkMode(Database cx,
                                   std::vector<StringRef> tokens,
                                   const char* usage,
                                   const char* operationName,
                                   std::function<Future<int>(Database, int)> setModeFunc,
                                   std::function<Future<int>(Database)> getModeFunc) {
	if (tokens.size() == 2) {
		int mode = wait(getModeFunc(cx));
		if (mode == 0) {
			fmt::println("{} mode is disabled", operationName);
		} else if (mode == 1) {
			fmt::println("{} mode is enabled", operationName);
		} else {
			fmt::println("Invalid {} mode value {}", operationName, mode);
		}
		return UID();
	}

	if (tokens.size() != 3) {
		fmt::println("{}", usage);
		return UID();
	}

	if (tokencmp(tokens[2], "on")) {
		int old = wait(setModeFunc(cx, 1));
		TraceEvent(fmt::format("Set{}ModeCommand", operationName).c_str())
		    .detail("OldValue", old)
		    .detail("NewValue", 1);
		return UID();
	} else if (tokencmp(tokens[2], "off")) {
		int old = wait(setModeFunc(cx, 0));
		TraceEvent(fmt::format("Set{}ModeCommand", operationName).c_str())
		    .detail("OldValue", old)
		    .detail("NewValue", 0);
		return UID();
	} else {
		fmt::println("ERROR: Invalid {} mode value {}", operationName, tokens[2].toString());
		fmt::println("{}", usage);
		return UID();
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
			fmt::println("   Error: {} ⚠️", errorTasks);
	}
}

// Print stalled task warnings
template <typename StalledTaskType>
void printStalledTaskWarnings(const std::vector<StalledTaskType>& stalledTasks) {
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

// Enhanced bulk operation status display with all observability features
template <typename ProgressType>
void printEnhancedBulkStatus(const ProgressType& progress, const char* operationName, const std::string& ownerSuffix) {
	fmt::println("Running bulk {} job: {}", operationName, progress.jobId.toString());
	fmt::println(" Range: {}{}", progress.jobRange.toString(), ownerSuffix);
	fmt::println("");

	// Progress summary
	printProgressSummary(
	    operationName, progress.completeTasks, progress.totalTasks, progress.completedBytes, progress.totalBytes);

	// Bytes progress
	fmt::println(" Bytes completed - {}", formatBytesProgress(progress.completedBytes, progress.totalBytes));

	// Performance metrics
	printProgressMetrics(progress.avgBytesPerSecond(), progress.etaSeconds(), progress.elapsedSeconds);

	// Stalled tasks warning (if applicable)
	if constexpr (requires { progress.stalledTasks; }) {
		printStalledTaskWarnings(progress.stalledTasks);
	}

	// Error warnings
	if constexpr (requires { progress.errorTasks; }) {
		if (progress.errorTasks > 0) {
			fmt::println("");
			fmt::println("WARNING: {} tasks in error state", progress.errorTasks);
		}
	}
}

// Advanced performance monitoring for bulk operations
struct BulkHealthMetrics {
	double healthScore; // 0-100 overall health score
	std::string healthStatus; // "Excellent", "Good", "Fair", "Poor"
	std::vector<std::string> recommendations;

	static BulkHealthMetrics analyze(double throughputMBps,
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
			metrics.healthStatus = "🚀 Excellent";
		} else if (metrics.healthScore >= 75) {
			metrics.healthStatus = "✅ Good";
		} else if (metrics.healthScore >= 50) {
			metrics.healthStatus = "⚠️ Fair";
		} else {
			metrics.healthStatus = "❌ Poor";
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
};

void printBulkHealthAnalysis(const BulkHealthMetrics& health) {
	fmt::println("");
	fmt::println("Health Analysis:");
	fmt::println(" Overall Health: {:.1f}/100 ({})", health.healthScore, health.healthStatus);

	if (!health.recommendations.empty()) {
		fmt::println(" Recommendations:");
		for (const auto& rec : health.recommendations) {
			fmt::println("   • {}", rec);
		}
	}
}

// Real-time performance trend monitoring
struct BulkPerformanceTrend {
	std::vector<double> throughputSamples;
	std::vector<double> efficiencySamples;
	double avgThroughput;
	double throughputStdDev;
	std::string trendDirection; // "↗ Improving", "↘ Declining", "→ Stable"

	static BulkPerformanceTrend analyze(const std::vector<double>& recentThroughput,
	                                    const std::vector<double>& recentEfficiency) {
		BulkPerformanceTrend trend;
		trend.throughputSamples = recentThroughput;
		trend.efficiencySamples = recentEfficiency;

		if (recentThroughput.empty()) {
			trend.trendDirection = "→ No data";
			return trend;
		}

		// Calculate average and standard deviation
		double sum = 0;
		for (double t : recentThroughput)
			sum += t;
		trend.avgThroughput = sum / recentThroughput.size();

		double variance = 0;
		for (double t : recentThroughput) {
			variance += (t - trend.avgThroughput) * (t - trend.avgThroughput);
		}
		trend.throughputStdDev = sqrt(variance / recentThroughput.size());

		// Determine trend direction (compare first half vs second half)
		if (recentThroughput.size() >= 4) {
			size_t mid = recentThroughput.size() / 2;
			double firstHalf = 0, secondHalf = 0;

			for (size_t i = 0; i < mid; i++)
				firstHalf += recentThroughput[i];
			for (size_t i = mid; i < recentThroughput.size(); i++)
				secondHalf += recentThroughput[i];

			firstHalf /= mid;
			secondHalf /= (recentThroughput.size() - mid);

			double change = (secondHalf - firstHalf) / firstHalf * 100;

			if (change > 10) {
				trend.trendDirection = "↗ Improving";
			} else if (change < -10) {
				trend.trendDirection = "↘ Declining";
			} else {
				trend.trendDirection = "→ Stable";
			}
		} else {
			trend.trendDirection = "→ Insufficient data";
		}

		return trend;
	}
};

void printPerformanceTrends(const BulkPerformanceTrend& trend) {
	if (!trend.throughputSamples.empty()) {
		fmt::println("");
		fmt::println("Performance Trends:");
		fmt::println(" Throughput: {:.1f} ± {:.1f} MB/s ({})",
		             trend.avgThroughput,
		             trend.throughputStdDev,
		             trend.trendDirection);

		if (trend.throughputStdDev > trend.avgThroughput * 0.3) {
			fmt::println(" ⚠️ High throughput variability detected - may indicate resource contention");
		}
	}
}

// Enhanced error diagnostics for bulk operations
struct BulkErrorAnalysis {
	int totalErrors;
	std::map<std::string, int> errorCategories;
	std::vector<std::string> criticalIssues;
	std::vector<std::string> actionableAdvice;

	static BulkErrorAnalysis analyze(int errorTasks, int stalledTasks, const std::vector<std::string>& recentErrors) {
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

		// Generate critical issues
		if (errorTasks > 10) {
			analysis.criticalIssues.push_back(fmt::format("High error rate: {} failed tasks", errorTasks));
		}
		if (stalledTasks > 5) {
			analysis.criticalIssues.push_back(fmt::format("Multiple stalled tasks: {} stalled", stalledTasks));
		}

		// Generate actionable advice
		if (analysis.errorCategories["Timeout Issues"] > 0) {
			analysis.actionableAdvice.push_back("Check network latency and increase timeout settings");
		}
		if (analysis.errorCategories["Storage Issues"] > 0) {
			analysis.actionableAdvice.push_back("Monitor disk space and I/O performance");
		}
		if (stalledTasks > 0) {
			analysis.actionableAdvice.push_back("Consider restarting stalled tasks or checking cluster health");
		}

		return analysis;
	}
};

void printErrorDiagnostics(const BulkErrorAnalysis& analysis) {
	if (analysis.totalErrors > 0 || !analysis.criticalIssues.empty()) {
		fmt::println("");
		fmt::println("🔍 Error Diagnostics:");

		// Error breakdown
		if (!analysis.errorCategories.empty()) {
			fmt::println(" Error Categories:");
			for (const auto& [category, count] : analysis.errorCategories) {
				fmt::println("   {} - {} occurrences", category, count);
			}
		}

		// Critical issues
		if (!analysis.criticalIssues.empty()) {
			fmt::println(" 🚨 Critical Issues:");
			for (const auto& issue : analysis.criticalIssues) {
				fmt::println("   • {}", issue);
			}
		}

		// Actionable advice
		if (!analysis.actionableAdvice.empty()) {
			fmt::println(" 💡 Recommendations:");
			for (const auto& advice : analysis.actionableAdvice) {
				fmt::println("   • {}", advice);
			}
		}
	}
}

// Automated performance optimization recommendations
struct BulkOptimizationRecommendations {
	std::vector<std::string> performanceRecommendations;
	std::vector<std::string> reliabilityRecommendations;
	std::vector<std::string> operationalRecommendations;

	static BulkOptimizationRecommendations generate(double throughputMBps,
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

		// Reliability recommendations
		if (errorTasks > totalTasks * 0.1) {
			recs.reliabilityRecommendations.push_back(
			    "High error rate (>10%). Investigate cluster health and storage server stability");
		}
		if (stalledTasks > 0) {
			recs.reliabilityRecommendations.push_back(
			    "Stalled tasks detected. Check for resource contention or failing storage servers");
		}

		// Operational recommendations
		if (elapsedMinutes > 120) {
			recs.operationalRecommendations.push_back(
			    "Very long-running operation. Consider enabling maintenance mode for affected ranges");
		}
		if (throughputMBps > 100) {
			recs.operationalRecommendations.push_back(
			    "High throughput achieved. Monitor cluster impact and consider throttling if needed");
		}

		return recs;
	}
};

void printOptimizationRecommendations(const BulkOptimizationRecommendations& recs) {
	bool hasRecommendations = !recs.performanceRecommendations.empty() || !recs.reliabilityRecommendations.empty() ||
	                          !recs.operationalRecommendations.empty();

	if (hasRecommendations) {
		fmt::println("");
		fmt::println("🎯 Optimization Recommendations:");

		if (!recs.performanceRecommendations.empty()) {
			fmt::println(" Performance:");
			for (const auto& rec : recs.performanceRecommendations) {
				fmt::println("   • {}", rec);
			}
		}

		if (!recs.reliabilityRecommendations.empty()) {
			fmt::println(" Reliability:");
			for (const auto& rec : recs.reliabilityRecommendations) {
				fmt::println("   • {}", rec);
			}
		}

		if (!recs.operationalRecommendations.empty()) {
			fmt::println(" Operations:");
			for (const auto& rec : recs.operationalRecommendations) {
				fmt::println("   • {}", rec);
			}
		}
	}
}

} // namespace fdb_cli
