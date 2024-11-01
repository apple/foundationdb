/*
 * Status.actor.cpp
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

#include <cinttypes>
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobManagerInterface.h"
#include "flow/genericactors.actor.h"
#include "fmt/format.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/BlobRestoreCommon.h"
#include "fdbserver/Status.actor.h"
#include "flow/ITrace.h"
#include "flow/ProtocolVersion.h"
#include "flow/Trace.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/WorkerInterface.actor.h"
#include <time.h>
#include "fdbserver/ClusterRecovery.actor.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbclient/ConsistencyScanInterface.actor.h"
#include "flow/UnitTest.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/StorageWiggleMetrics.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const char* RecoveryStatus::names[] = { "reading_coordinated_state",
	                                    "locking_coordinated_state",
	                                    "locking_old_transaction_servers",
	                                    "reading_transaction_system_state",
	                                    "configuration_missing",
	                                    "configuration_never_created",
	                                    "configuration_invalid",
	                                    "recruiting_transaction_servers",
	                                    "initializing_transaction_servers",
	                                    "recovery_transaction",
	                                    "writing_coordinated_state",
	                                    "accepting_commits",
	                                    "all_logs_recruited",
	                                    "storage_recovered",
	                                    "fully_recovered" };
static_assert(sizeof(RecoveryStatus::names) == sizeof(RecoveryStatus::names[0]) * RecoveryStatus::END,
              "RecoveryStatus::names[] size");
const char* RecoveryStatus::descriptions[] = {
	// reading_coordinated_state
	"Requesting information from coordination servers. Verify that a majority of coordination server processes are "
	"active.",
	// locking_coordinated_state
	"Locking coordination state. Verify that a majority of coordination server processes are active.",
	// locking_old_transaction_servers
	"Locking old transaction servers. Verify that at least one transaction server from the previous generation is "
	"running.",
	// reading_transaction_system_state
	"Recovering transaction server state. Verify that the transaction server processes are active.",
	// configuration_missing
	"There appears to be a database, but its configuration does not appear to be initialized.",
	// configuration_never_created
	"The coordinator(s) have no record of this database. Either the coordinator addresses are incorrect, the "
	"coordination state on those machines is missing, or no database has been created.",
	// configuration_invalid
	"The database configuration is invalid. Set a new, valid configuration to recover the database.",
	// recruiting_transaction_servers
	"Recruiting new transaction servers.",
	// initializing_transaction_servers
	"Initializing new transaction servers and recovering transaction logs.",
	// recovery_transaction
	"Performing recovery transaction.",
	// writing_coordinated_state
	"Writing coordinated state. Verify that a majority of coordination server processes are active.",
	// accepting_commits
	"Accepting commits.",
	// all_logs_recruited
	"Accepting commits. All logs recruited.",
	// storage_recovered
	"Accepting commits. All storage servers are reading from the new logs.",
	// fully_recovered
	"Recovery complete."
};
static_assert(sizeof(RecoveryStatus::descriptions) == sizeof(RecoveryStatus::descriptions[0]) * RecoveryStatus::END,
              "RecoveryStatus::descriptions[] size");

// From Ratekeeper.actor.cpp
extern int limitReasonEnd;
extern const char* limitReasonName[];
extern const char* limitReasonDesc[];

typedef std::map<std::string, TraceEventFields> EventMap;

struct StorageServerStatusInfo : public StorageServerMetaInfo {
	EventMap eventMap;
	StorageServerStatusInfo(const StorageServerMetaInfo& info) : StorageServerMetaInfo(info, info.metadata) {}
};

ACTOR static Future<Optional<TraceEventFields>> latestEventOnWorker(WorkerInterface worker, std::string eventName) {
	try {
		EventLogRequest req =
		    eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
		ErrorOr<TraceEventFields> eventTrace = wait(errorOr(timeoutError(worker.eventLogRequest.getReply(req), 2.0)));

		if (eventTrace.isError()) {
			return Optional<TraceEventFields>();
		}
		return eventTrace.get();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		return Optional<TraceEventFields>();
	}
}

ACTOR Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventOnWorkers(
    std::vector<WorkerDetails> workers,
    std::string eventName) {
	try {
		state std::vector<Future<ErrorOr<TraceEventFields>>> eventTraces;
		for (int c = 0; c < workers.size(); c++) {
			EventLogRequest req =
			    eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
			eventTraces.push_back(errorOr(timeoutError(workers[c].interf.eventLogRequest.getReply(req), 2.0)));
		}

		wait(waitForAll(eventTraces));

		std::set<std::string> failed;
		WorkerEvents results;

		for (int i = 0; i < eventTraces.size(); i++) {
			const ErrorOr<TraceEventFields>& v = eventTraces[i].get();
			if (v.isError()) {
				failed.insert(workers[i].interf.address().toString());
				results[workers[i].interf.address()] = TraceEventFields();
			} else {
				results[workers[i].interf.address()] = v.get();
			}
		}

		std::pair<WorkerEvents, std::set<std::string>> val;
		val.first = results;
		val.second = failed;

		return val;
	} catch (Error& e) {
		ASSERT(e.code() ==
		       error_code_actor_cancelled); // All errors should be filtering through the errorOr actor above
		throw;
	}
}
static Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestErrorOnWorkers(
    std::vector<WorkerDetails> workers) {
	return latestEventOnWorkers(workers, "");
}

static Optional<WorkerDetails> getWorker(std::vector<WorkerDetails> const& workers, NetworkAddress const& address) {
	try {
		for (int c = 0; c < workers.size(); c++)
			if (address == workers[c].interf.address())
				return workers[c];
		return Optional<WorkerDetails>();
	} catch (Error&) {
		return Optional<WorkerDetails>();
	}
}

static Optional<WorkerDetails> getWorker(std::map<NetworkAddress, WorkerDetails> const& workersMap,
                                         NetworkAddress const& address) {
	auto itr = workersMap.find(address);
	if (itr == workersMap.end()) {
		return Optional<WorkerDetails>();
	}

	return itr->second;
}

class StatusCounter {
public:
	StatusCounter() : hz(0), roughness(0), counter(0) {}
	StatusCounter(double hz, double roughness, int64_t counter) : hz(hz), roughness(roughness), counter(counter) {}
	StatusCounter(const std::string& parsableText) { parseText(parsableText); }

	StatusCounter& parseText(const std::string& parsableText) {
		sscanf(parsableText.c_str(), "%lf %lf %" SCNd64 "", &hz, &roughness, &counter);
		return *this;
	}

	StatusCounter& updateValues(const StatusCounter& statusCounter) {
		double hzNew = hz + statusCounter.hz;
		double roughnessNew = (hz + statusCounter.hz) ? (roughness * hz + statusCounter.roughness * statusCounter.hz) /
		                                                    (hz + statusCounter.hz)
		                                              : 0.0;
		int64_t counterNew = counter + statusCounter.counter;
		hz = hzNew;
		roughness = roughnessNew;
		counter = counterNew;
		return *this;
	}

	JsonBuilderObject getStatus() const {
		JsonBuilderObject statusObject;
		statusObject["hz"] = hz;
		statusObject["roughness"] = roughness;
		statusObject["counter"] = counter;
		return statusObject;
	}

	double getHz() { return hz; }

	double getRoughness() { return roughness; }

	int64_t getCounter() { return counter; }

protected:
	double hz;
	double roughness;
	int64_t counter;
};

static JsonBuilderObject getError(const TraceEventFields& errorFields) {
	JsonBuilderObject statusObj;
	try {
		if (errorFields.size()) {
			double time = atof(errorFields.getValue("Time").c_str());
			statusObj["time"] = time;

			statusObj["raw_log_message"] = errorFields.toString();

			std::string type = errorFields.getValue("Type");
			statusObj["type"] = type;

			std::string description = type;
			std::string errorName;
			if (errorFields.tryGetValue("Error", errorName)) {
				statusObj["name"] = errorName;
				description += ": " + errorName;
			} else
				statusObj["name"] = "process_error";

			struct tm* timeinfo;
			time_t t = (time_t)time;
			timeinfo = localtime(&t);
			char buffer[128];
			strftime(buffer, 128, "%c", timeinfo);
			description += " at " + std::string(buffer);

			statusObj["description"] = description;
		}
	} catch (Error& e) {
		TraceEvent(SevError, "StatusGetErrorError").error(e).detail("RawError", errorFields.toString());
	}
	return statusObj;
}

namespace {

void reportCgroupCpuStat(JsonBuilderObject& object, const TraceEventFields& eventFields) {
	JsonBuilderObject cgroupCpuStatObj;
	std::string val;
	if (eventFields.tryGetValue("NrPeriods", val)) {
		cgroupCpuStatObj.setKeyRawNumber("nr_periods", val);
	}
	if (eventFields.tryGetValue("NrThrottled", val)) {
		cgroupCpuStatObj.setKeyRawNumber("nr_throttled", val);
	}
	if (eventFields.tryGetValue("ThrottledTime", val)) {
		cgroupCpuStatObj.setKeyRawNumber("throttled_time", val);
	}
	if (!cgroupCpuStatObj.empty()) {
		object["cgroup_cpu_stat"] = cgroupCpuStatObj;
	}
}

JsonBuilderObject machineStatusFetcher(WorkerEvents mMetrics,
                                       std::vector<WorkerDetails> workers,
                                       Optional<DatabaseConfiguration> configuration,
                                       std::set<std::string>* incomplete_reasons) {
	JsonBuilderObject machineMap;
	double metric;
	int failed = 0;

	// map from machine networkAddress to datacenter ID
	std::map<NetworkAddress, std::string> dcIds;
	std::map<NetworkAddress, LocalityData> locality;
	std::map<std::string, bool> excludedMap;
	std::map<std::string, int32_t> workerContribMap;
	std::map<std::string, JsonBuilderObject> machineJsonMap;

	for (auto const& worker : workers) {
		locality[worker.interf.address()] = worker.interf.locality;
		if (worker.interf.locality.dcId().present())
			dcIds[worker.interf.address()] = worker.interf.locality.dcId().get().printable();
	}

	for (auto it = mMetrics.begin(); it != mMetrics.end(); it++) {

		if (!it->second.size()) {
			continue;
		}

		JsonBuilderObject statusObj; // Represents the status for a machine
		const TraceEventFields& event = it->second;

		try {
			std::string address = it->first.ip.toString();
			// We will use the "physical" calculated machine ID here to limit exposure to machineID repurposing
			std::string machineId = event.getValue("MachineID");

			// If this machine ID does not already exist in the machineMap, add it
			if (!machineJsonMap.contains(machineId)) {
				statusObj["machine_id"] = machineId;

				if (dcIds.contains(it->first)) {
					statusObj["datacenter_id"] = dcIds[it->first];
				}

				if (locality.contains(it->first)) {
					statusObj["locality"] = locality[it->first].toJSON<JsonBuilderObject>();
				}

				statusObj["address"] = address;

				JsonBuilderObject memoryObj;
				memoryObj.setKeyRawNumber("total_bytes", event.getValue("TotalMemory"));
				memoryObj.setKeyRawNumber("committed_bytes", event.getValue("CommittedMemory"));
				memoryObj.setKeyRawNumber("free_bytes", event.getValue("AvailableMemory"));
				statusObj["memory"] = memoryObj;

#ifdef __linux__
				reportCgroupCpuStat(statusObj, event);
#endif // __linux__

				JsonBuilderObject cpuObj;
				double cpuSeconds = event.getDouble("CPUSeconds");
				double elapsed = event.getDouble("Elapsed");
				if (elapsed > 0) {
					cpuObj["logical_core_utilization"] = std::max(0.0, std::min(cpuSeconds / elapsed, 1.0));
				}
				statusObj["cpu"] = cpuObj;

				JsonBuilderObject networkObj;
				networkObj["megabits_sent"] = JsonBuilderObject().setKeyRawNumber("hz", event.getValue("MbpsSent"));
				networkObj["megabits_received"] =
				    JsonBuilderObject().setKeyRawNumber("hz", event.getValue("MbpsReceived"));

				metric = event.getDouble("RetransSegs");
				JsonBuilderObject retransSegsObj;
				if (elapsed > 0) {
					retransSegsObj["hz"] = metric / elapsed;
				}
				networkObj["tcp_segments_retransmitted"] = retransSegsObj;
				statusObj["network"] = networkObj;

				if (configuration.present()) {
					excludedMap[machineId] =
					    true; // Will be set to false below if this or any later process is not excluded
				}

				workerContribMap[machineId] = 0;
				machineJsonMap[machineId] = statusObj;
			}

			bool excludedServer = true;
			// If the machine is already marked as not excluded, because at least one process was found to not be
			// excluded, we can stop checking further servers on this machine.
			if (configuration.present() && excludedMap[machineId]) {
				NetworkAddressList tempList;
				tempList.address = it->first;
				// Check if the locality data is present and if so, make use of it.
				auto localityData = LocalityData();
				if (locality.contains(it->first)) {
					localityData = locality[it->first];
				}

				// The isExcludedServer method already contains a check for the excluded localities.
				excludedServer = configuration.get().isExcludedServer(tempList, localityData);
			}

			// If any server is not excluded, set the overall exclusion status of the machine to false.
			if (!excludedServer) {
				excludedMap[machineId] = false;
			}
			workerContribMap[machineId]++;
		} catch (Error&) {
			++failed;
		}
	}

	// Add the status json for each machine with tracked values
	for (auto& mapPair : machineJsonMap) {
		auto& machineId = mapPair.first;
		auto& jsonItem = machineJsonMap[machineId];
		jsonItem["excluded"] = excludedMap[machineId];
		jsonItem["contributing_workers"] = workerContribMap[machineId];
		machineMap[machineId] = jsonItem;
	}

	if (failed > 0)
		incomplete_reasons->insert("Cannot retrieve all machine status information.");

	return machineMap;
}

} // anonymous namespace

JsonBuilderObject getLagObject(int64_t versions) {
	JsonBuilderObject lag;
	lag["versions"] = versions;
	lag["seconds"] = versions / (double)SERVER_KNOBS->VERSIONS_PER_SECOND;
	return lag;
}

static JsonBuilderObject getBounceImpactInfo(int recoveryStatusCode) {
	JsonBuilderObject bounceImpact;

	if (recoveryStatusCode == RecoveryStatus::fully_recovered) {
		bounceImpact["can_clean_bounce"] = true;
	} else {
		bounceImpact["can_clean_bounce"] = false;
		bounceImpact["reason"] = "cluster hasn't fully recovered yet";
	}

	return bounceImpact;
}

struct MachineMemoryInfo {
	double memoryUsage; // virtual memory usage
	double rssUsage; // RSS memory usage
	double aggregateLimit;

	MachineMemoryInfo() : memoryUsage(0), rssUsage(0), aggregateLimit(0) {}

	bool valid() { return memoryUsage >= 0; }
	void invalidate() { memoryUsage = -1; }
};

struct RolesInfo {
	std::multimap<NetworkAddress, JsonBuilderObject> roles;

	JsonBuilderObject addLatencyStatistics(TraceEventFields const& metrics) {
		JsonBuilderObject latencyStats;
		latencyStats.setKeyRawNumber("count", metrics.getValue("Count"));
		latencyStats.setKeyRawNumber("min", metrics.getValue("Min"));
		latencyStats.setKeyRawNumber("max", metrics.getValue("Max"));
		latencyStats.setKeyRawNumber("median", metrics.getValue("Median"));
		latencyStats.setKeyRawNumber("mean", metrics.getValue("Mean"));
		latencyStats.setKeyRawNumber("p25", metrics.getValue("P25"));
		latencyStats.setKeyRawNumber("p90", metrics.getValue("P90"));
		latencyStats.setKeyRawNumber("p95", metrics.getValue("P95"));
		latencyStats.setKeyRawNumber("p99", metrics.getValue("P99"));
		latencyStats.setKeyRawNumber("p99.9", metrics.getValue("P99.9"));

		return latencyStats;
	}

	JsonBuilderObject addLatencyBandInfo(TraceEventFields const& metrics) {
		JsonBuilderObject latencyBands;
		std::map<std::string, JsonBuilderObject> bands;

		for (auto itr = metrics.begin(); itr != metrics.end(); ++itr) {
			std::string band;
			if (itr->first.substr(0, 4) == "Band") {
				band = itr->first.substr(4);
			} else if (itr->first == "Filtered") {
				band = "filtered";
			} else {
				continue;
			}

			latencyBands[band] = StatusCounter(itr->second).getCounter();
		}

		return latencyBands;
	}

	JsonBuilderObject& addRole(NetworkAddress address, std::string const& role, UID id) {
		JsonBuilderObject obj;
		obj["id"] = id.shortString();
		obj["role"] = role;
		return roles.insert(std::make_pair(address, obj))->second;
	}

	JsonBuilderObject& addRole(std::string const& role,
	                           StorageServerStatusInfo& iface,
	                           Version maxTLogVersion,
	                           double* pDataLagSeconds) {
		JsonBuilderObject obj;
		EventMap const& metrics = iface.eventMap;
		double dataLagSeconds = -1.0;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		obj["tss"] = iface.isTss();
		if (iface.metadata.present()) {
			obj["storage_metadata"] = iface.metadata.get().toJSON();
			// printf("%s\n", metadataObj.getJson().c_str());
		}

		try {
			TraceEventFields const& storageMetrics = metrics.at("StorageMetrics");

			obj.setKeyRawNumber("stored_bytes", storageMetrics.getValue("BytesStored"));
			obj.setKeyRawNumber("kvstore_used_bytes", storageMetrics.getValue("KvstoreBytesUsed"));
			obj.setKeyRawNumber("kvstore_free_bytes", storageMetrics.getValue("KvstoreBytesFree"));
			obj.setKeyRawNumber("kvstore_available_bytes", storageMetrics.getValue("KvstoreBytesAvailable"));
			obj.setKeyRawNumber("kvstore_total_bytes", storageMetrics.getValue("KvstoreBytesTotal"));
			obj.setKeyRawNumber("kvstore_total_size", storageMetrics.getValue("KvstoreSizeTotal"));
			obj.setKeyRawNumber("kvstore_total_nodes", storageMetrics.getValue("KvstoreNodeTotal"));
			obj.setKeyRawNumber("kvstore_inline_keys", storageMetrics.getValue("KvstoreInlineKey"));
			obj["input_bytes"] = StatusCounter(storageMetrics.getValue("BytesInput")).getStatus();
			obj["durable_bytes"] = StatusCounter(storageMetrics.getValue("BytesDurable")).getStatus();
			obj.setKeyRawNumber("query_queue_max", storageMetrics.getValue("QueryQueueMax"));
			obj["total_queries"] = StatusCounter(storageMetrics.getValue("QueryQueue")).getStatus();
			obj["finished_queries"] = StatusCounter(storageMetrics.getValue("FinishedQueries")).getStatus();
			obj["low_priority_queries"] = StatusCounter(storageMetrics.getValue("LowPriorityQueries")).getStatus();
			obj["bytes_queried"] = StatusCounter(storageMetrics.getValue("BytesQueried")).getStatus();
			obj["keys_queried"] = StatusCounter(storageMetrics.getValue("RowsQueried")).getStatus();
			obj["mutation_bytes"] = StatusCounter(storageMetrics.getValue("MutationBytes")).getStatus();
			obj["mutations"] = StatusCounter(storageMetrics.getValue("Mutations")).getStatus();
			obj.setKeyRawNumber("local_rate", storageMetrics.getValue("LocalRate"));
			obj["fetched_versions"] = StatusCounter(storageMetrics.getValue("FetchedVersions")).getStatus();
			obj["fetches_from_logs"] = StatusCounter(storageMetrics.getValue("FetchesFromLogs")).getStatus();

			Version version = storageMetrics.getInt64("Version");
			Version durableVersion = storageMetrics.getInt64("DurableVersion");

			obj["data_version"] = version;
			obj["durable_version"] = durableVersion;

			int64_t versionLag = storageMetrics.getInt64("VersionLag");
			if (maxTLogVersion > 0) {
				// It's possible that the storage server hasn't talked to the logs recently, in which case it may not be
				// aware of how far behind it is. To account for that, we also compute the version difference between
				// each storage server and the tlog with the largest version.
				//
				// Because this data is only logged periodically, this difference will likely be an overestimate for the
				// lag. We subtract off the logging interval in order to make this estimate a bounded underestimate
				// instead.
				versionLag = std::max<int64_t>(
				    versionLag,
				    maxTLogVersion - version - SERVER_KNOBS->STORAGE_LOGGING_DELAY * SERVER_KNOBS->VERSIONS_PER_SECOND);
			}

			TraceEventFields const& readLatencyMetrics = metrics.at("ReadLatencyMetrics");
			if (readLatencyMetrics.size()) {
				obj["read_latency_statistics"] = addLatencyStatistics(readLatencyMetrics);
			}

			TraceEventFields const& readLatencyBands = metrics.at("ReadLatencyBands");
			if (readLatencyBands.size()) {
				obj["read_latency_bands"] = addLatencyBandInfo(readLatencyBands);
			}

			obj["data_lag"] = getLagObject(versionLag);
			obj["durability_lag"] = getLagObject(version - durableVersion);
			dataLagSeconds = versionLag / (double)SERVER_KNOBS->VERSIONS_PER_SECOND;

			TraceEventFields const& busiestReadTag = metrics.at("BusiestReadTag");
			if (busiestReadTag.size()) {
				int64_t tagCost = busiestReadTag.getInt64("TagCost");
				if (tagCost > 0) {
					JsonBuilderObject busiestReadTagObj;
					busiestReadTagObj["tag"] = busiestReadTag.getValue("Tag");
					busiestReadTagObj["cost"] = tagCost;
					busiestReadTagObj["fractional_cost"] = busiestReadTag.getValue("FractionalBusyness");
					obj["busiest_read_tag"] = busiestReadTagObj;
				}
			}

			TraceEventFields const& busiestWriteTag = metrics.at("BusiestWriteTag");
			if (busiestWriteTag.size()) {
				int64_t tagCost = busiestWriteTag.getInt64("TagCost");

				if (tagCost > 0) {
					JsonBuilderObject busiestWriteTagObj;

					int64_t totalCost = busiestWriteTag.getInt64("TotalCost");
					ASSERT(totalCost > 0);

					busiestWriteTagObj["tag"] = busiestWriteTag.getValue("Tag");
					busiestWriteTagObj["fractional_cost"] = (double)tagCost / totalCost;

					double elapsed = busiestWriteTag.getDouble("Elapsed");
					if (elapsed > 0) {
						JsonBuilderObject estimatedCostObj;
						estimatedCostObj["hz"] = tagCost / elapsed;
						busiestWriteTagObj["estimated_cost"] = estimatedCostObj;
					}

					obj["busiest_write_tag"] = busiestWriteTagObj;
				}
			}

			TraceEventFields const& rocksdbMetrics = metrics.at("RocksDBMetrics");
			if (rocksdbMetrics.size()) {
				JsonBuilderObject rocksdbMetricsObj;
				rocksdbMetricsObj.setKeyRawNumber("block_cache_hits", rocksdbMetrics.getValue("BlockCacheHits"));
				rocksdbMetricsObj.setKeyRawNumber("block_cache_misses", rocksdbMetrics.getValue("BlockCacheMisses"));
				rocksdbMetricsObj.setKeyRawNumber("pending_compaction_bytes",
				                                  rocksdbMetrics.getValue("EstPendCompactBytes"));
				rocksdbMetricsObj.setKeyRawNumber("memtable_bytes", rocksdbMetrics.getValue("AllMemtablesBytes"));
				rocksdbMetricsObj.setKeyRawNumber("sst_reader_bytes",
				                                  rocksdbMetrics.getValue("EstimateSstReaderBytes"));
				rocksdbMetricsObj.setKeyRawNumber("block_cache_usage", rocksdbMetrics.getValue("BlockCacheUsage"));
				rocksdbMetricsObj.setKey("block_cache_limit", SERVER_KNOBS->ROCKSDB_BLOCK_CACHE_SIZE);
				rocksdbMetricsObj.setKeyRawNumber("throttled_commits", rocksdbMetrics.getValue("CommitDelayed"));
				rocksdbMetricsObj.setKeyRawNumber("write_stall_microseconds", rocksdbMetrics.getValue("StallMicros"));

				obj["rocksdb_metrics"] = std::move(rocksdbMetricsObj);
			}

		} catch (AttributeNotFoundError& e) {
			TraceEvent(SevWarnAlways, "StorageServerStatusJson").detail("MissingAttribute", e.getMissingAttribute());
		}
		if (pDataLagSeconds) {
			*pDataLagSeconds = dataLagSeconds;
		}

		return roles.insert(std::make_pair(iface.address(), obj))->second;
	}
	JsonBuilderObject& addRole(std::string const& role,
	                           TLogInterface& iface,
	                           EventMap const& metrics,
	                           Version* pMetricVersion) {
		JsonBuilderObject obj;
		Version metricVersion = 0;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			TraceEventFields const& tlogMetrics = metrics.at("TLogMetrics");

			obj.setKeyRawNumber("kvstore_used_bytes", tlogMetrics.getValue("KvstoreBytesUsed"));
			obj.setKeyRawNumber("kvstore_free_bytes", tlogMetrics.getValue("KvstoreBytesFree"));
			obj.setKeyRawNumber("kvstore_available_bytes", tlogMetrics.getValue("KvstoreBytesAvailable"));
			obj.setKeyRawNumber("kvstore_total_bytes", tlogMetrics.getValue("KvstoreBytesTotal"));
			obj.setKeyRawNumber("queue_disk_used_bytes", tlogMetrics.getValue("QueueDiskBytesUsed"));
			obj.setKeyRawNumber("queue_disk_free_bytes", tlogMetrics.getValue("QueueDiskBytesFree"));
			obj.setKeyRawNumber("queue_disk_available_bytes", tlogMetrics.getValue("QueueDiskBytesAvailable"));
			obj.setKeyRawNumber("queue_disk_total_bytes", tlogMetrics.getValue("QueueDiskBytesTotal"));
			obj["input_bytes"] = StatusCounter(tlogMetrics.getValue("BytesInput")).getStatus();
			obj["durable_bytes"] = StatusCounter(tlogMetrics.getValue("BytesDurable")).getStatus();
			metricVersion = tlogMetrics.getInt64("Version");
			obj["data_version"] = metricVersion;
		} catch (Error& e) {
			if (e.code() != error_code_attribute_not_found)
				throw e;
		}
		if (pMetricVersion)
			*pMetricVersion = metricVersion;
		return roles.insert(std::make_pair(iface.address(), obj))->second;
	}
	JsonBuilderObject& addRole(std::string const& role, CommitProxyInterface& iface, EventMap const& metrics) {
		JsonBuilderObject obj;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			TraceEventFields const& commitLatencyMetrics = metrics.at("CommitLatencyMetrics");
			if (commitLatencyMetrics.size()) {
				obj["commit_latency_statistics"] = addLatencyStatistics(commitLatencyMetrics);
			}

			TraceEventFields const& commitLatencyBands = metrics.at("CommitLatencyBands");
			if (commitLatencyBands.size()) {
				obj["commit_latency_bands"] = addLatencyBandInfo(commitLatencyBands);
			}

			TraceEventFields const& commitBatchingWindowSize = metrics.at("CommitBatchingWindowSize");
			if (commitBatchingWindowSize.size()) {
				obj["commit_batching_window_size"] = addLatencyStatistics(commitBatchingWindowSize);
			}
		} catch (Error& e) {
			if (e.code() != error_code_attribute_not_found) {
				throw e;
			}
		}

		return roles.insert(std::make_pair(iface.address(), obj))->second;
	}

	// Returns a json object encoding a snapshot of grv proxy statistics
	JsonBuilderObject& addRole(std::string const& role, GrvProxyInterface& iface, EventMap const& metrics) {
		JsonBuilderObject obj;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			JsonBuilderObject priorityStats;

			// GRV Latency metrics are grouped according to priority (currently batch or default).
			// Other priorities can be added in the future.
			TraceEventFields const& grvLatencyMetrics = metrics.at("GRVLatencyMetrics");
			if (grvLatencyMetrics.size()) {
				priorityStats["default"] = addLatencyStatistics(grvLatencyMetrics);
			}

			TraceEventFields const& grvBatchMetrics = metrics.at("GRVBatchLatencyMetrics");
			if (grvBatchMetrics.size()) {
				priorityStats["batch"] = addLatencyStatistics(grvBatchMetrics);
			}

			// Add GRV Latency metrics (for all priorities) to parent node.
			if (priorityStats.size()) {
				obj["grv_latency_statistics"] = priorityStats;
			}

			TraceEventFields const& grvLatencyBands = metrics.at("GRVLatencyBands");
			if (grvLatencyBands.size()) {
				obj["grv_latency_bands"] = addLatencyBandInfo(grvLatencyBands);
			}
		} catch (Error& e) {
			if (e.code() != error_code_attribute_not_found) {
				throw e;
			}
		}

		return roles.insert(std::make_pair(iface.address(), obj))->second;
	}
	template <class InterfaceType>
	JsonBuilderObject& addRole(std::string const& role, InterfaceType& iface) {
		return addRole(iface.address(), role, iface.id());
	}
	JsonBuilderObject& addCoordinatorRole(NetworkAddress addr) {
		JsonBuilderObject obj;
		obj["role"] = "coordinator";
		return roles.insert(std::make_pair(addr, obj))->second;
	}
	JsonBuilderArray getStatusForAddress(NetworkAddress a) {
		JsonBuilderArray v;
		auto it = roles.lower_bound(a);
		while (it != roles.end() && it->first == a) {
			v.push_back(it->second);
			++it;
		}
		return v;
	}
};

ACTOR static Future<JsonBuilderObject> processStatusFetcher(
    Reference<AsyncVar<ServerDBInfo>> db,
    std::vector<WorkerDetails> workers,
    WorkerEvents pMetrics,
    WorkerEvents mMetrics,
    WorkerEvents nMetrics,
    WorkerEvents errors,
    WorkerEvents traceFileOpenErrors,
    WorkerEvents programStarts,
    std::map<std::string, std::vector<JsonBuilderObject>> processIssues,
    std::vector<StorageServerStatusInfo> storageServers,
    std::vector<std::pair<TLogInterface, EventMap>> tLogs,
    std::vector<std::pair<CommitProxyInterface, EventMap>> commitProxies,
    std::vector<std::pair<GrvProxyInterface, EventMap>> grvProxies,
    std::vector<BlobWorkerInterface> blobWorkers,
    ServerCoordinators coordinators,
    std::vector<NetworkAddress> coordinatorAddresses,
    Database cx,
    Optional<DatabaseConfiguration> configuration,
    Optional<Key> healthyZone,
    std::set<std::string>* incomplete_reasons) {

	state JsonBuilderObject processMap;

	// construct a map from a process address to a status object containing a trace file open error
	// this is later added to the messages subsection
	state std::map<std::string, JsonBuilderObject> tracefileOpenErrorMap;
	state WorkerEvents::iterator traceFileErrorsItr;
	for (traceFileErrorsItr = traceFileOpenErrors.begin(); traceFileErrorsItr != traceFileOpenErrors.end();
	     ++traceFileErrorsItr) {
		wait(yield());
		if (traceFileErrorsItr->second.size()) {
			try {
				// Have event fields, parse it and turn it into a message object describing the trace file opening error
				const TraceEventFields& event = traceFileErrorsItr->second;
				std::string fileName = event.getValue("Filename");
				JsonBuilderObject msgObj = JsonString::makeMessage(
				    "file_open_error",
				    format("Could not open file '%s' (%s).", fileName.c_str(), event.getValue("Error").c_str())
				        .c_str());
				msgObj["file_name"] = fileName;

				// Map the address of the worker to the error message object
				tracefileOpenErrorMap[traceFileErrorsItr->first.toString()] = msgObj;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				incomplete_reasons->insert("file_open_error details could not be retrieved");
			}
		}
	}

	state std::map<Optional<Standalone<StringRef>>, MachineMemoryInfo> machineMemoryUsage;
	state std::vector<WorkerDetails>::iterator workerItr;
	for (workerItr = workers.begin(); workerItr != workers.end(); ++workerItr) {
		wait(yield());
		state std::map<Optional<Standalone<StringRef>>, MachineMemoryInfo>::iterator memInfo =
		    machineMemoryUsage.insert(std::make_pair(workerItr->interf.locality.machineId(), MachineMemoryInfo()))
		        .first;
		try {
			ASSERT(pMetrics.contains(workerItr->interf.address()));
			const TraceEventFields& processMetrics = pMetrics[workerItr->interf.address()];
			const TraceEventFields& programStart = programStarts[workerItr->interf.address()];

			if (memInfo->second.valid()) {
				if (processMetrics.size() > 0 && programStart.size() > 0) {
					memInfo->second.memoryUsage += processMetrics.getDouble("Memory");
					memInfo->second.rssUsage += processMetrics.getDouble("ResidentMemory");
					memInfo->second.aggregateLimit += programStart.getDouble("MemoryLimit");
				} else
					memInfo->second.invalidate();
			}
		} catch (Error& e) {
			memInfo->second.invalidate();
		}
	}

	state RolesInfo roles;

	roles.addRole("master", db->get().master);
	roles.addRole("cluster_controller", db->get().clusterInterface.clientInterface);

	if (db->get().distributor.present()) {
		roles.addRole("data_distributor", db->get().distributor.get());
	}

	if (db->get().ratekeeper.present()) {
		roles.addRole("ratekeeper", db->get().ratekeeper.get());
	}

	if (configuration.present() && configuration.get().blobGranulesEnabled && db->get().blobManager.present()) {
		roles.addRole("blob_manager", db->get().blobManager.get());
	}

	if (configuration.present() && configuration.get().blobGranulesEnabled && db->get().blobMigrator.present()) {
		roles.addRole("blob_migrator", db->get().blobMigrator.get());
	}

	if (db->get().consistencyScan.present()) {
		roles.addRole("consistency_scan", db->get().consistencyScan.get());
	}

	if (db->get().client.encryptKeyProxy.present()) {
		roles.addRole("encrypt_key_proxy", db->get().client.encryptKeyProxy.get());
	}

	for (auto& tLogSet : db->get().logSystemConfig.tLogs) {
		for (auto& it : tLogSet.logRouters) {
			if (it.present()) {
				roles.addRole("router", it.interf());
			}
		}
	}

	state std::vector<OldTLogConf>::const_iterator oldTLogIter;
	state std::vector<OldTLogConf> oldTLogs = db->get().logSystemConfig.oldTLogs;
	for (oldTLogIter = oldTLogs.begin(); oldTLogIter != oldTLogs.end(); ++oldTLogIter) {
		for (auto& tLogSet : oldTLogIter->tLogs) {
			for (auto& it : tLogSet.tLogs) {
				if (it.present()) {
					roles.addRole("log", it.interf());
				}
			}
			for (auto& it : tLogSet.logRouters) {
				if (it.present()) {
					roles.addRole("router", it.interf());
				}
			}
		}
		wait(yield());
	}

	for (const auto& coordinator : coordinatorAddresses) {
		roles.addCoordinatorRole(coordinator);
	}

	state std::vector<std::pair<CommitProxyInterface, EventMap>>::iterator commit_proxy;
	for (commit_proxy = commitProxies.begin(); commit_proxy != commitProxies.end(); ++commit_proxy) {
		roles.addRole("commit_proxy", commit_proxy->first, commit_proxy->second);
		wait(yield());
	}

	state std::vector<std::pair<GrvProxyInterface, EventMap>>::iterator grvProxy;
	for (grvProxy = grvProxies.begin(); grvProxy != grvProxies.end(); ++grvProxy) {
		roles.addRole("grv_proxy", grvProxy->first, grvProxy->second);
		wait(yield());
	}

	state std::vector<std::pair<TLogInterface, EventMap>>::iterator log;
	state Version maxTLogVersion = 0;

	// Get largest TLog version
	for (log = tLogs.begin(); log != tLogs.end(); ++log) {
		Version tLogVersion = 0;
		roles.addRole("log", log->first, log->second, &tLogVersion);
		maxTLogVersion = std::max(maxTLogVersion, tLogVersion);
		wait(yield());
	}

	state std::vector<StorageServerStatusInfo>::iterator ss;
	state std::map<NetworkAddress, double> ssLag;
	state double lagSeconds;
	for (ss = storageServers.begin(); ss != storageServers.end(); ++ss) {
		roles.addRole("storage", *ss, maxTLogVersion, &lagSeconds);
		if (lagSeconds != -1.0) {
			ssLag[ss->address()] = lagSeconds;
		}
		wait(yield());
	}

	state std::vector<ResolverInterface>::const_iterator res;
	state std::vector<ResolverInterface> resolvers = db->get().resolvers;
	for (res = resolvers.begin(); res != resolvers.end(); ++res) {
		roles.addRole("resolver", *res);
		wait(yield());
	}

	if (configuration.present() && configuration.get().blobGranulesEnabled) {
		for (auto blobWorker : blobWorkers) {
			roles.addRole("blob_worker", blobWorker);
			wait(yield());
		}
	}

	for (workerItr = workers.begin(); workerItr != workers.end(); ++workerItr) {
		wait(yield());
		state JsonBuilderObject statusObj;
		try {
			ASSERT(pMetrics.contains(workerItr->interf.address()));

			NetworkAddress address = workerItr->interf.address();
			const TraceEventFields& processMetrics = pMetrics[workerItr->interf.address()];
			statusObj["address"] = address.toString();
			JsonBuilderObject memoryObj;

			if (processMetrics.size() > 0) {
				std::string zoneID = processMetrics.getValue("ZoneID");
				statusObj["fault_domain"] = zoneID;
				if (healthyZone.present() && healthyZone == workerItr->interf.locality.zoneId()) {
					statusObj["under_maintenance"] = true;
				}

				std::string MachineID = processMetrics.getValue("MachineID");
				statusObj["machine_id"] = MachineID;

				statusObj["locality"] = workerItr->interf.locality.toJSON<JsonBuilderObject>();

				statusObj.setKeyRawNumber("uptime_seconds", processMetrics.getValue("UptimeSeconds"));

				// rates are calculated over the last elapsed seconds
				double processMetricsElapsed = processMetrics.getDouble("Elapsed");
				double cpuSeconds = processMetrics.getDouble("CPUSeconds");
				double diskIdleSeconds = processMetrics.getDouble("DiskIdleSeconds");
				double diskReads = processMetrics.getDouble("DiskReads");
				double diskWrites = processMetrics.getDouble("DiskWrites");

				JsonBuilderObject diskObj;
				if (processMetricsElapsed > 0) {
					JsonBuilderObject cpuObj;
					cpuObj["usage_cores"] = std::max(0.0, cpuSeconds / processMetricsElapsed);
					statusObj["cpu"] = cpuObj;

					diskObj["busy"] =
					    std::max(0.0, std::min((processMetricsElapsed - diskIdleSeconds) / processMetricsElapsed, 1.0));

					JsonBuilderObject readsObj;
					readsObj.setKeyRawNumber("counter", processMetrics.getValue("DiskReadsCount"));
					if (processMetricsElapsed > 0)
						readsObj["hz"] = diskReads / processMetricsElapsed;
					readsObj.setKeyRawNumber("sectors", processMetrics.getValue("DiskReadSectors"));

					JsonBuilderObject writesObj;
					writesObj.setKeyRawNumber("counter", processMetrics.getValue("DiskWritesCount"));
					if (processMetricsElapsed > 0)
						writesObj["hz"] = diskWrites / processMetricsElapsed;
					writesObj.setKeyRawNumber("sectors", processMetrics.getValue("DiskWriteSectors"));

					diskObj["reads"] = readsObj;
					diskObj["writes"] = writesObj;
				}

				diskObj.setKeyRawNumber("total_bytes", processMetrics.getValue("DiskTotalBytes"));
				diskObj.setKeyRawNumber("free_bytes", processMetrics.getValue("DiskFreeBytes"));
				statusObj["disk"] = diskObj;

				JsonBuilderObject networkObj;

				networkObj.setKeyRawNumber("current_connections", processMetrics.getValue("CurrentConnections"));
				JsonBuilderObject connections_established;
				connections_established.setKeyRawNumber("hz", processMetrics.getValue("ConnectionsEstablished"));
				networkObj["connections_established"] = connections_established;
				JsonBuilderObject connections_closed;
				connections_closed.setKeyRawNumber("hz", processMetrics.getValue("ConnectionsClosed"));
				networkObj["connections_closed"] = connections_closed;
				JsonBuilderObject connection_errors;
				connection_errors.setKeyRawNumber("hz", processMetrics.getValue("ConnectionErrors"));
				networkObj["connection_errors"] = connection_errors;

				JsonBuilderObject megabits_sent;
				megabits_sent.setKeyRawNumber("hz", processMetrics.getValue("MbpsSent"));
				networkObj["megabits_sent"] = megabits_sent;

				JsonBuilderObject megabits_received;
				megabits_received.setKeyRawNumber("hz", processMetrics.getValue("MbpsReceived"));
				networkObj["megabits_received"] = megabits_received;

				JsonBuilderObject tls_policy_failures;
				tls_policy_failures.setKeyRawNumber("hz", processMetrics.getValue("TLSPolicyFailures"));
				networkObj["tls_policy_failures"] = tls_policy_failures;

				statusObj["network"] = networkObj;

				memoryObj.setKeyRawNumber("used_bytes", processMetrics.getValue("Memory"));
				memoryObj.setKeyRawNumber("rss_bytes", processMetrics.getValue("ResidentMemory"));
				memoryObj.setKeyRawNumber("unused_allocated_memory", processMetrics.getValue("UnusedAllocatedMemory"));
			}

			int64_t memoryLimit = 0;
			if (programStarts.contains(address)) {
				auto const& programStartEvent = programStarts.at(address);

				if (programStartEvent.size() > 0) {
					memoryLimit = programStartEvent.getInt64("MemoryLimit");
					memoryObj.setKey("limit_bytes", memoryLimit);

					std::string version;
					if (programStartEvent.tryGetValue("Version", version)) {
						statusObj["version"] = version;
					}

					std::string commandLine;
					if (programStartEvent.tryGetValue("CommandLine", commandLine)) {
						statusObj["command_line"] = commandLine;
					}
				}
			}

			// if this process address is in the machine metrics
			if (mMetrics.contains(address) && mMetrics[address].size()) {
				double availableMemory;
				availableMemory = mMetrics[address].getDouble("AvailableMemory");

				auto machineMemInfo = machineMemoryUsage[workerItr->interf.locality.machineId()];
				if (machineMemInfo.valid() && memoryLimit > 0) {
					ASSERT(machineMemInfo.aggregateLimit > 0);
					int64_t memory =
					    (availableMemory + machineMemInfo.rssUsage) * memoryLimit / machineMemInfo.aggregateLimit;
					memoryObj["available_bytes"] = std::min<int64_t>(std::max<int64_t>(memory, 0), memoryLimit);
				}
			}

			statusObj["memory"] = memoryObj;

			JsonBuilderArray messages;

			if (errors.contains(address) && errors[address].size()) {
				// returns status object with type and time of error
				messages.push_back(getError(errors.at(address)));
			}

			// string of address used so that other fields of a NetworkAddress are not compared
			std::string strAddress = address.toString();

			// If this process has a process issue, identified by strAddress, then add it to messages array
			for (auto issue : processIssues[strAddress]) {
				messages.push_back(issue);
			}

			// If this process had a trace file open error, identified by strAddress, then add it to messages array
			if (tracefileOpenErrorMap.contains(strAddress)) {
				messages.push_back(tracefileOpenErrorMap[strAddress]);
			}

			if (ssLag[address] >= 60) {
				messages.push_back(JsonString::makeMessage(
				    "storage_server_lagging",
				    format("Storage server lagging by %lld seconds.", (int64_t)ssLag[address]).c_str()));
			}

			// Store the message array into the status object that represents the worker process
			statusObj["messages"] = messages;

			// Get roles for the worker's address as an array of objects
			statusObj["roles"] = roles.getStatusForAddress(address);

			if (configuration.present()) {
				statusObj["excluded"] =
				    configuration.get().isExcludedServer(workerItr->interf.addresses(), workerItr->interf.locality);
			}

			statusObj["class_type"] = workerItr->processClass.toString();
			statusObj["class_source"] = workerItr->processClass.sourceString();
			if (workerItr->degraded) {
				statusObj["degraded"] = true;
			}

			const TraceEventFields& networkMetrics = nMetrics[workerItr->interf.address()];
			double networkMetricsElapsed = networkMetrics.getDouble("Elapsed");

			try {
				double runLoopBusy = networkMetrics.getDouble("PriorityStarvedBelow1");
				statusObj["run_loop_busy"] = runLoopBusy / networkMetricsElapsed;
			} catch (Error& e) {
				// This should only happen very early in the process lifetime before priority bin info has been
				// populated
				incomplete_reasons->insert("Cannot retrieve run loop busyness.");
			}

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			// Something strange occurred, process list is incomplete but what was built so far, if anything, will be
			// returned.
			incomplete_reasons->insert("Cannot retrieve all process status information.");
		}

		processMap[printable(workerItr->interf.locality.processId())] = statusObj;
	}
	return processMap;
}

static JsonBuilderObject grayFailureStatus(const std::unordered_map<NetworkAddress, double>& excludedDegradedServers) {
	JsonBuilderObject status;
	JsonBuilderArray excludedProcesses;
	for (const auto& [excludedServer, time] : excludedDegradedServers) {
		JsonBuilderObject process;
		process["address"] = excludedServer.toString();
		process["time"] = time;
		excludedProcesses.push_back(process);
	}
	status["excluded_processes"] = excludedProcesses;
	return status;
}

static JsonBuilderObject clientStatusFetcher(
    std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>>* clientStatusMap) {
	JsonBuilderObject clientStatus;

	int64_t clientCount = 0;
	// Here we handle versions and maxSupportedProtocols, the issues will be handled in getClientIssuesAsMessages
	std::map<Standalone<ClientVersionRef>, OpenDatabaseRequest::Samples> supportedVersions;
	std::map<Key, OpenDatabaseRequest::Samples> maxSupportedProtocol;

	for (auto iter = clientStatusMap->begin(); iter != clientStatusMap->end();) {
		if (now() - iter->second.first >= 2 * SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL) {
			iter = clientStatusMap->erase(iter);
			continue;
		}

		clientCount += iter->second.second.clientCount;
		for (const auto& [version, samples] : iter->second.second.supportedVersions) {
			supportedVersions[version] += samples;
		}
		for (const auto& [protocol, samples] : iter->second.second.maxProtocolSupported) {
			maxSupportedProtocol[protocol] += samples;
		}
		++iter;
	}

	clientStatus["count"] = clientCount;

	JsonBuilderArray versionsArray = JsonBuilderArray();
	for (const auto& [clientVersionRef, samples] : supportedVersions) {
		JsonBuilderObject ver;
		ver["count"] = (int64_t)samples.count;
		ver["client_version"] = clientVersionRef.clientVersion.toString();
		ver["protocol_version"] = clientVersionRef.protocolVersion.toString();
		ver["source_version"] = clientVersionRef.sourceVersion.toString();

		JsonBuilderArray clients = JsonBuilderArray();
		for (const auto& [networkAddress, trackLogGroup] : samples.samples) {
			JsonBuilderObject cli;
			cli["address"] = networkAddress.toString();
			cli["log_group"] = trackLogGroup.toString();
			clients.push_back(cli);
		}

		auto iter = maxSupportedProtocol.find(clientVersionRef.protocolVersion);
		if (iter != std::end(maxSupportedProtocol)) {
			JsonBuilderArray maxClients = JsonBuilderArray();
			for (const auto& [networkAddress, trackLogGroup] : iter->second.samples) {
				JsonBuilderObject cli;
				cli["address"] = networkAddress.toString();
				cli["log_group"] = trackLogGroup.toString();
				maxClients.push_back(cli);
			}
			ver["max_protocol_count"] = iter->second.count;
			ver["max_protocol_clients"] = maxClients;
			maxSupportedProtocol.erase(clientVersionRef.protocolVersion);
		}

		ver["connected_clients"] = clients;
		versionsArray.push_back(ver);
	}

	if (versionsArray.size() > 0) {
		clientStatus["supported_versions"] = versionsArray;
	}

	return clientStatus;
}

ACTOR static Future<JsonBuilderObject> recoveryStateStatusFetcher(Database cx,
                                                                  WorkerDetails ccWorker,
                                                                  WorkerDetails mWorker,
                                                                  int workerCount,
                                                                  std::set<std::string>* incomplete_reasons,
                                                                  int* statusCode) {
	state JsonBuilderObject message;
	state Transaction tr(cx);
	try {
		state Future<TraceEventFields> mdActiveGensF =
		    timeoutError(ccWorker.interf.eventLogRequest.getReply(EventLogRequest(StringRef(
		                     getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_GENERATION_EVENT_NAME)))),
		                 1.0);
		state Future<TraceEventFields> mdF =
		    timeoutError(ccWorker.interf.eventLogRequest.getReply(EventLogRequest(StringRef(
		                     getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME)))),
		                 1.0);
		state Future<TraceEventFields> mDBAvailableF =
		    timeoutError(ccWorker.interf.eventLogRequest.getReply(EventLogRequest(StringRef(
		                     getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_AVAILABLE_EVENT_NAME)))),
		                 1.0);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		state Future<ErrorOr<Version>> rvF = errorOr(timeoutError(tr.getReadVersion(), 1.0));

		wait(success(mdActiveGensF) && success(mdF) && success(rvF) && success(mDBAvailableF));

		const TraceEventFields& md = mdF.get();
		int mStatusCode = md.getInt("StatusCode");
		if (mStatusCode < 0 || mStatusCode >= RecoveryStatus::END)
			throw attribute_not_found();

		message =
		    JsonString::makeMessage(RecoveryStatus::names[mStatusCode], RecoveryStatus::descriptions[mStatusCode]);
		*statusCode = mStatusCode;

		ErrorOr<Version> rv = rvF.get();
		const TraceEventFields& dbAvailableMsg = mDBAvailableF.get();
		if (dbAvailableMsg.size() > 0) {
			int64_t availableAtVersion = dbAvailableMsg.getInt64("AvailableAtVersion");
			if (!rv.isError()) {
				double lastRecoveredSecondsAgo = std::max((int64_t)0, (int64_t)(rv.get() - availableAtVersion)) /
				                                 (double)SERVER_KNOBS->VERSIONS_PER_SECOND;
				message["seconds_since_last_recovered"] = lastRecoveredSecondsAgo;
			}
		} else {
			message["seconds_since_last_recovered"] = -1;
		}

		// Add additional metadata for certain statuses
		if (mStatusCode == RecoveryStatus::recruiting_transaction_servers) {
			int requiredLogs = atoi(md.getValue("RequiredTLogs").c_str());
			int requiredCommitProxies = atoi(md.getValue("RequiredCommitProxies").c_str());
			int requiredGrvProxies = atoi(md.getValue("RequiredGrvProxies").c_str());
			int requiredResolvers = atoi(md.getValue("RequiredResolvers").c_str());
			// int requiredProcesses = std::max(requiredLogs, std::max(requiredResolvers, requiredCommitProxies));
			// int requiredMachines = std::max(requiredLogs, 1);

			message["required_logs"] = requiredLogs;
			message["required_commit_proxies"] = requiredCommitProxies;
			message["required_grv_proxies"] = requiredGrvProxies;
			message["required_resolvers"] = requiredResolvers;
		} else if (mStatusCode == RecoveryStatus::locking_old_transaction_servers) {
			message["missing_logs"] = md.getValue("MissingIDs").c_str();
		}

		// TODO:  time_in_recovery: 0.5
		//        time_in_state: 0.1

		const TraceEventFields& mdActiveGens = mdActiveGensF.get();
		if (mdActiveGens.size()) {
			int activeGenerations = mdActiveGens.getInt("ActiveGenerations");
			message["active_generations"] = activeGenerations;
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
	}

	// If recovery status name is not know, status is incomplete
	if (message.empty()) {
		incomplete_reasons->insert("Recovery Status unavailable.");
	}

	return message;
}

ACTOR static Future<double> doGrvProbe(
    Transaction* tr,
    Optional<FDBTransactionOptions::Option> priority = Optional<FDBTransactionOptions::Option>()) {
	state double start = g_network->timer_monotonic();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			if (priority.present()) {
				tr->setOption(priority.get());
			}

			wait(success(tr->getReadVersion()));
			return g_network->timer_monotonic() - start;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<double> doReadProbe(Future<double> grvProbe, Transaction* tr) {
	ErrorOr<double> grv = wait(errorOr(grvProbe));
	if (grv.isError()) {
		throw grv.getError();
	}

	state double start = g_network->timer_monotonic();

	loop {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Optional<Standalone<StringRef>> _ = wait(tr->get("\xff/StatusJsonTestKey62793"_sr));
			return g_network->timer_monotonic() - start;
		} catch (Error& e) {
			wait(tr->onError(e));
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}
	}
}

ACTOR static Future<double> doCommitProbe(Future<double> grvProbe, Transaction* sourceTr, Transaction* tr) {
	ErrorOr<double> grv = wait(errorOr(grvProbe));
	if (grv.isError()) {
		throw grv.getError();
	}

	ASSERT(sourceTr->getReadVersion().isReady());
	tr->setVersion(sourceTr->getReadVersion().get());
	tr->getDatabase()->ssVersionVectorCache = sourceTr->getDatabase()->ssVersionVectorCache;
	tr->trState->readVersionObtainedFromGrvProxy = sourceTr->trState->readVersionObtainedFromGrvProxy;

	state double start = g_network->timer_monotonic();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->makeSelfConflicting();
			wait(tr->commit());
			return g_network->timer_monotonic() - start;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> doProbe(Future<double> probe,
                                  int timeoutSeconds,
                                  const char* prefix,
                                  const char* description,
                                  JsonBuilderObject* probeObj,
                                  JsonBuilderArray* messages,
                                  std::set<std::string>* incomplete_reasons,
                                  bool* isAvailable = nullptr) {
	choose {
		when(ErrorOr<double> result = wait(errorOr(probe))) {
			if (result.isError()) {
				if (isAvailable != nullptr) {
					*isAvailable = false;
				}
				incomplete_reasons->insert(format(
				    "Unable to retrieve latency probe information (%s: %s).", description, result.getError().what()));
			} else {
				(*probeObj)[format("%s_seconds", prefix).c_str()] = result.get();
			}
		}
		when(wait(delay(timeoutSeconds))) {
			if (isAvailable != nullptr) {
				*isAvailable = false;
			}
			messages->push_back(
			    JsonString::makeMessage(format("%s_probe_timeout", prefix).c_str(),
			                            format("Unable to %s after %d seconds.", description, timeoutSeconds).c_str()));
		}
	}

	return Void();
}

ACTOR static Future<JsonBuilderObject> latencyProbeFetcher(Database cx,
                                                           JsonBuilderArray* messages,
                                                           std::set<std::string>* incomplete_reasons,
                                                           bool* isAvailable) {
	state Transaction trImmediate(cx);
	state Transaction trDefault(cx);
	state Transaction trBatch(cx);
	state Transaction trWrite(cx);

	state JsonBuilderObject statusObj;

	try {
		Future<double> immediateGrvProbe = doGrvProbe(&trImmediate, FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Future<double> defaultGrvProbe = doGrvProbe(&trDefault);
		Future<double> batchGrvProbe = doGrvProbe(&trBatch, FDBTransactionOptions::PRIORITY_BATCH);

		Future<double> readProbe = doReadProbe(immediateGrvProbe, &trImmediate);
		Future<double> commitProbe = doCommitProbe(immediateGrvProbe, &trImmediate, &trWrite);

		int timeoutSeconds = 5;

		std::vector<Future<Void>> probes;
		probes.push_back(doProbe(immediateGrvProbe,
		                         timeoutSeconds,
		                         "immediate_priority_transaction_start",
		                         "start immediate priority transaction",
		                         &statusObj,
		                         messages,
		                         incomplete_reasons,
		                         isAvailable));
		probes.push_back(doProbe(defaultGrvProbe,
		                         timeoutSeconds,
		                         "transaction_start",
		                         "start default priority transaction",
		                         &statusObj,
		                         messages,
		                         incomplete_reasons));
		probes.push_back(doProbe(batchGrvProbe,
		                         timeoutSeconds,
		                         "batch_priority_transaction_start",
		                         "start batch priority transaction",
		                         &statusObj,
		                         messages,
		                         incomplete_reasons));
		probes.push_back(
		    doProbe(readProbe, timeoutSeconds, "read", "read", &statusObj, messages, incomplete_reasons, isAvailable));
		probes.push_back(doProbe(
		    commitProbe, timeoutSeconds, "commit", "commit", &statusObj, messages, incomplete_reasons, isAvailable));

		wait(waitForAll(probes));
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		incomplete_reasons->insert(format("Unable to retrieve latency probe information (%s).", e.what()));
	}

	return statusObj;
}

ACTOR static Future<JsonBuilderObject> versionEpochStatusFetcher(Database cx,
                                                                 std::set<std::string>* incomplete_reasons) {
	state JsonBuilderObject message;
	try {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> versionEpochVal = wait(timeoutError(BUGGIFY ? Never() : tr.get(versionEpochKey), 5.0));
				message["enabled"] = versionEpochVal.present();
				if (!versionEpochVal.present()) {
					break;
				}
				int64_t versionEpoch = BinaryReader::fromStringRef<int64_t>(versionEpochVal.get(), Unversioned());
				message["epoch"] = std::to_string(versionEpoch);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		incomplete_reasons->insert(format("Unable to retrieve version epoch information (%s).", e.what()));
	}
	return message;
}

ACTOR static Future<Void> consistencyCheckStatusFetcher(Database cx,
                                                        JsonBuilderArray* messages,
                                                        std::set<std::string>* incomplete_reasons) {
	try {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> ccSuspendVal =
				    wait(timeoutError(BUGGIFY ? Never() : tr.get(fdbShouldConsistencyCheckBeSuspended), 5.0));
				bool ccSuspend = ccSuspendVal.present()
				                     ? BinaryReader::fromStringRef<bool>(ccSuspendVal.get(), Unversioned())
				                     : false;
				if (ccSuspend) {
					messages->push_back(
					    JsonString::makeMessage("consistencycheck_disabled", "Consistency checker is disabled."));
				}
				break;
			} catch (Error& e) {
				if (e.code() == error_code_timed_out) {
					messages->push_back(
					    JsonString::makeMessage("consistencycheck_suspendkey_fetch_timeout",
					                            format("Timed out trying to fetch `%s` from the database.",
					                                   printable(fdbShouldConsistencyCheckBeSuspended).c_str())
					                                .c_str()));
					break;
				}
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		incomplete_reasons->insert(format("Unable to retrieve consistency check settings (%s).", e.what()));
	}
	return Void();
}

struct LogRangeAndUID {
	KeyRange range;
	UID destID;

	LogRangeAndUID(KeyRange const& range, UID const& destID) : range(range), destID(destID) {}

	bool operator<(LogRangeAndUID const& r) const {
		if (range.begin != r.range.begin)
			return range.begin < r.range.begin;
		if (range.end != r.range.end)
			return range.end < r.range.end;
		return destID < r.destID;
	}
};

ACTOR static Future<Void> logRangeWarningFetcher(Database cx,
                                                 JsonBuilderArray* messages,
                                                 std::set<std::string>* incomplete_reasons) {
	try {
		state Transaction tr(cx);
		state Future<Void> timeoutFuture = timeoutError(Future<Void>(Never()), 5.0);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Future<RangeResult> existingDestUidValues =
				    tr.getRange(KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> existingLogRanges = tr.getRange(logRangesRange, CLIENT_KNOBS->TOO_MANY);
				wait((success(existingDestUidValues) && success(existingLogRanges)) || timeoutFuture);

				std::set<LogRangeAndUID> loggingRanges;
				for (auto& it : existingLogRanges.get()) {
					Key logDestination;
					UID logUid;
					KeyRef logRangeBegin = logRangesDecodeKey(it.key, &logUid);
					Key logRangeEnd = logRangesDecodeValue(it.value, &logDestination);
					loggingRanges.insert(LogRangeAndUID(KeyRangeRef(logRangeBegin, logRangeEnd), logUid));
				}

				std::set<std::pair<Key, Key>> existingRanges;
				for (auto& it : existingDestUidValues.get()) {
					KeyRange range = BinaryReader::fromStringRef<KeyRange>(it.key.removePrefix(destUidLookupPrefix),
					                                                       IncludeVersion());
					UID logUid = BinaryReader::fromStringRef<UID>(it.value, Unversioned());
					if (loggingRanges.contains(LogRangeAndUID(range, logUid))) {
						std::pair<Key, Key> rangePair = std::make_pair(range.begin, range.end);
						if (existingRanges.contains(rangePair)) {
							std::string rangeDescription = (range == getDefaultBackupSharedRange())
							                                   ? "the default backup set"
							                                   : format("`%s` - `%s`",
							                                            printable(range.begin).c_str(),
							                                            printable(range.end).c_str());
							messages->push_back(JsonString::makeMessage(
							    "duplicate_mutation_streams",
							    format("Backup and DR are not sharing the same stream of mutations for %s",
							           rangeDescription.c_str())
							        .c_str()));
							break;
						}
						existingRanges.insert(rangePair);
					}
				}
				wait(tr.commit() || timeoutFuture);
				break;
			} catch (Error& e) {
				if (e.code() == error_code_timed_out) {
					messages->push_back(
					    JsonString::makeMessage("duplicate_mutation_fetch_timeout",
					                            format("Timed out trying to fetch `%s` from the database.",
					                                   printable(destUidLookupPrefix).c_str())
					                                .c_str()));
					break;
				}
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		incomplete_reasons->insert(format("Unable to retrieve log ranges (%s).", e.what()));
	}
	return Void();
}

struct ProtocolVersionData {
	ProtocolVersion runningProtocolVersion;
	ProtocolVersion newestProtocolVersion;
	ProtocolVersion lowestCompatibleProtocolVersion;
	ProtocolVersionData() : runningProtocolVersion(currentProtocolVersion()) {}

	ProtocolVersionData(uint64_t newestProtocolVersionValue, uint64_t lowestCompatibleProtocolVersionValue)
	  : runningProtocolVersion(currentProtocolVersion()), newestProtocolVersion(newestProtocolVersionValue),
	    lowestCompatibleProtocolVersion(lowestCompatibleProtocolVersionValue) {}
};

ACTOR Future<ProtocolVersionData> getNewestProtocolVersion(Database cx, WorkerDetails ccWorker) {

	try {
		state Future<TraceEventFields> swVersionF = timeoutError(
		    ccWorker.interf.eventLogRequest.getReply(EventLogRequest("SWVersionCompatibilityChecked"_sr)), 1.0);

		wait(success(swVersionF));
		const TraceEventFields& swVersionTrace = swVersionF.get();
		int64_t newestProtocolVersionValue =
		    std::stoull(swVersionTrace.getValue("NewestProtocolVersion").c_str(), nullptr, 16);
		int64_t lowestCompatibleProtocolVersionValue =
		    std::stoull(swVersionTrace.getValue("LowestCompatibleProtocolVersion").c_str(), nullptr, 16);

		return ProtocolVersionData(newestProtocolVersionValue, lowestCompatibleProtocolVersionValue);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent(SevWarnAlways, "SWVersionStatusFailed").error(e);

		return ProtocolVersionData();
	}
}

struct LoadConfigurationResult {
	bool fullReplication;
	Optional<Key> healthyZone;
	double healthyZoneSeconds;
	bool rebalanceDDIgnored;
	// FIXME: possible convert it to int if upgrade value can be resolved?
	std::string rebalanceDDIgnoreHex; // any or combination of 0, 1, 2, see DDIgnore;
	bool dataDistributionDisabled;

	LoadConfigurationResult()
	  : fullReplication(true), healthyZoneSeconds(0), rebalanceDDIgnored(false), dataDistributionDisabled(false) {}
};

ACTOR static Future<std::pair<Optional<DatabaseConfiguration>, Optional<LoadConfigurationResult>>>
loadConfiguration(Database cx, JsonBuilderArray* messages, std::set<std::string>* status_incomplete_reasons) {
	state Optional<DatabaseConfiguration> result;
	state Optional<LoadConfigurationResult> loadResult;
	state Transaction tr(cx);
	state Future<Void> getConfTimeout = delay(5.0);

	loop {
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::CAUSAL_READ_RISKY);
		try {
			choose {
				when(RangeResult res = wait(tr.getRange(configKeys, SERVER_KNOBS->CONFIGURATION_ROWS_TO_FETCH))) {
					DatabaseConfiguration configuration;
					if (res.size() == SERVER_KNOBS->CONFIGURATION_ROWS_TO_FETCH) {
						status_incomplete_reasons->insert("Too many configuration parameters set.");
					} else {
						configuration.fromKeyValues((VectorRef<KeyValueRef>)res);
					}

					result = configuration;
				}
				when(wait(getConfTimeout)) {
					if (!result.present()) {
						messages->push_back(JsonString::makeMessage("unreadable_configuration",
						                                            "Unable to read database configuration."));
					} else {
						messages->push_back(
						    JsonString::makeMessage("full_replication_timeout", "Unable to read datacenter replicas."));
					}
					break;
				}
			}

			ASSERT(result.present());
			state std::vector<Future<Optional<Value>>> replicasFutures;
			for (auto& region : result.get().regions) {
				replicasFutures.push_back(tr.get(datacenterReplicasKeyFor(region.dcId)));
			}
			state Future<Optional<Value>> healthyZoneValue = tr.get(healthyZoneKey);
			state Future<Optional<Value>> rebalanceDDIgnored = tr.get(rebalanceDDIgnoreKey);
			state Future<Optional<Value>> ddModeKey = tr.get(dataDistributionModeKey);

			choose {
				when(wait(waitForAll(replicasFutures) && success(healthyZoneValue) && success(rebalanceDDIgnored) &&
				          success(ddModeKey))) {
					int unreplicated = 0;
					for (int i = 0; i < result.get().regions.size(); i++) {
						if (!replicasFutures[i].get().present() ||
						    decodeDatacenterReplicasValue(replicasFutures[i].get().get()) <
						        result.get().storageTeamSize) {
							unreplicated++;
						}
					}
					LoadConfigurationResult res;
					res.fullReplication = (!unreplicated || (result.get().usableRegions == 1 &&
					                                         unreplicated < result.get().regions.size()));
					if (healthyZoneValue.get().present()) {
						auto healthyZone = decodeHealthyZoneValue(healthyZoneValue.get().get());
						if (healthyZone.first == ignoreSSFailuresZoneString) {
							res.healthyZone = healthyZone.first;
						} else if (healthyZone.second > tr.getReadVersion().get()) {
							res.healthyZone = healthyZone.first;
							res.healthyZoneSeconds =
							    (healthyZone.second - tr.getReadVersion().get()) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
						}
					}
					res.rebalanceDDIgnored = rebalanceDDIgnored.get().present();
					if (res.rebalanceDDIgnored) {
						res.rebalanceDDIgnoreHex = rebalanceDDIgnored.get().get().toHexString();
					}
					if (ddModeKey.get().present()) {
						BinaryReader rd(ddModeKey.get().get(), Unversioned());
						int currentMode;
						rd >> currentMode;
						if (currentMode == 0) {
							res.dataDistributionDisabled = true;
						}
					}
					loadResult = res;
				}
				when(wait(getConfTimeout)) {
					messages->push_back(
					    JsonString::makeMessage("full_replication_timeout", "Unable to read datacenter replicas."));
				}
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return std::make_pair(result, loadResult);
}

static JsonBuilderObject configurationFetcher(Optional<DatabaseConfiguration> conf,
                                              ServerCoordinators coordinators,
                                              std::set<std::string>* incomplete_reasons) {
	JsonBuilderObject statusObj;
	try {
		if (conf.present()) {
			DatabaseConfiguration configuration = conf.get();
			statusObj.addContents(configuration.toJSON());

			JsonBuilderArray excludedServersArr;
			std::set<AddressExclusion> excludedServers = configuration.getExcludedServers();
			for (std::set<AddressExclusion>::iterator it = excludedServers.begin(); it != excludedServers.end(); it++) {
				JsonBuilderObject statusObj;
				statusObj["address"] = it->toString();
				excludedServersArr.push_back(statusObj);
			}
			std::set<std::string> excludedLocalities = configuration.getExcludedLocalities();
			for (const auto& it : excludedLocalities) {
				JsonBuilderObject statusObj;
				statusObj["locality"] = it;
				excludedServersArr.push_back(statusObj);
			}
			statusObj["excluded_servers"] = excludedServersArr;
		}
		int count = coordinators.clientLeaderServers.size();
		statusObj["coordinators_count"] = count;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		incomplete_reasons->insert("Could not retrieve all configuration status information.");
	}
	return statusObj;
}

ACTOR static Future<JsonBuilderObject> dataStatusFetcher(WorkerDetails ddWorker,
                                                         DatabaseConfiguration configuration,
                                                         int* minStorageReplicasRemaining) {
	state JsonBuilderObject statusObjData;

	try {
		std::vector<Future<TraceEventFields>> futures;

		// TODO:  Should this be serial?
		futures.push_back(
		    timeoutError(ddWorker.interf.eventLogRequest.getReply(EventLogRequest("DDTrackerStarting"_sr)), 1.0));
		futures.push_back(
		    timeoutError(ddWorker.interf.eventLogRequest.getReply(EventLogRequest("DDTrackerStats"_sr)), 1.0));
		futures.push_back(
		    timeoutError(ddWorker.interf.eventLogRequest.getReply(EventLogRequest("MovingData"_sr)), 1.0));
		futures.push_back(
		    timeoutError(ddWorker.interf.eventLogRequest.getReply(EventLogRequest("TotalDataInFlight"_sr)), 1.0));
		futures.push_back(
		    timeoutError(ddWorker.interf.eventLogRequest.getReply(EventLogRequest("TotalDataInFlightRemote"_sr)), 1.0));

		std::vector<TraceEventFields> dataInfo = wait(getAll(futures));

		TraceEventFields startingStats = dataInfo[0];
		TraceEventFields dataStats = dataInfo[1];

		if (startingStats.size() && startingStats.getValue("State") != "Active") {
			JsonBuilderObject stateSectionObj;
			stateSectionObj["name"] = "initializing";
			stateSectionObj["description"] = "(Re)initializing automatic data distribution";
			statusObjData["state"] = stateSectionObj;
			return statusObjData;
		}

		TraceEventFields md = dataInfo[2];

		// If we have a MovingData message, parse it.
		int64_t partitionsInFlight = 0;
		int movingHighestPriority = 1000;
		if (md.size()) {
			int64_t partitionsInQueue = md.getInt64("InQueue");
			int64_t averagePartitionSize = md.getInt64("AverageShardSize");
			partitionsInFlight = md.getInt64("InFlight");
			movingHighestPriority = md.getInt("HighestPriority");

			if (averagePartitionSize >= 0) {
				JsonBuilderObject moving_data;
				moving_data["in_queue_bytes"] = partitionsInQueue * averagePartitionSize;
				moving_data["in_flight_bytes"] = partitionsInFlight * averagePartitionSize;
				moving_data.setKeyRawNumber("total_written_bytes", md.getValue("BytesWritten"));
				moving_data["highest_priority"] = movingHighestPriority;

				// TODO: moving_data["rate_bytes"] = makeCounter(hz, c, r);
				statusObjData["moving_data"] = moving_data;
				statusObjData["average_partition_size_bytes"] = averagePartitionSize;
			}
		}

		if (dataStats.size()) {
			statusObjData.setKeyRawNumber("total_kv_size_bytes", dataStats.getValue("TotalSizeBytes"));
			statusObjData.setKeyRawNumber("system_kv_size_bytes", dataStats.getValue("SystemSizeBytes"));
			statusObjData.setKeyRawNumber("partitions_count", dataStats.getValue("Shards"));
		}

		JsonBuilderArray teamTrackers;
		for (int i = 3; i < 5; i++) {
			const TraceEventFields& inFlight = dataInfo[i];
			if (inFlight.size() == 0) {
				continue;
			}

			int replicas = configuration.storageTeamSize;
			bool primary = inFlight.getInt("Primary");
			int highestPriority = inFlight.getInt("HighestPriority");

			if (movingHighestPriority < SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT) {
				highestPriority = movingHighestPriority;
			} else if (partitionsInFlight > 0) {
				highestPriority = std::max<int>(highestPriority, SERVER_KNOBS->PRIORITY_MERGE_SHARD);
			}

			JsonBuilderObject team_tracker;
			team_tracker["primary"] = primary;
			team_tracker.setKeyRawNumber("in_flight_bytes", inFlight.getValue("TotalBytes"));
			team_tracker.setKeyRawNumber("unhealthy_servers", inFlight.getValue("UnhealthyServers"));

			JsonBuilderObject stateSectionObj;
			if (highestPriority >= SERVER_KNOBS->PRIORITY_TEAM_0_LEFT) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "missing_data";
				stateSectionObj["description"] = "No replicas remain of some data";
				replicas = 0;
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_TEAM_1_LEFT) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "healing";
				stateSectionObj["description"] = "Only one replica remains of some data";
				replicas = 1;
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_TEAM_2_LEFT) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "healing";
				stateSectionObj["description"] = "Only two replicas remain of some data";
				replicas = 2;
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "healing";
				stateSectionObj["description"] = "Restoring replication factor";
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_POPULATE_REGION) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_populating_region";
				stateSectionObj["description"] = "Populating remote region";
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_MERGE_SHARD) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_repartitioning";
				stateSectionObj["description"] = "Repartitioning";
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "optimizing_team_collections";
				stateSectionObj["description"] = "Optimizing team collections";
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_removing_server";
				stateSectionObj["description"] = "Removing storage server";
			} else if (highestPriority == SERVER_KNOBS->PRIORITY_TEAM_HEALTHY) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy";
			} else if (highestPriority == SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_perpetual_wiggle";
				stateSectionObj["description"] = "Wiggling storage server";
			} else if (highestPriority >= SERVER_KNOBS->PRIORITY_RECOVER_MOVE) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_rebalancing";
				stateSectionObj["description"] = "Rebalancing";
			} else if (highestPriority >= 0) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy";
			}

			// Track the number of min replicas the storage servers in this region has. The sum of the replicas from
			// both primary and remote region give the total number of data replicas this database currently has.
			stateSectionObj["min_replicas_remaining"] = replicas;

			if (!stateSectionObj.empty()) {
				team_tracker["state"] = stateSectionObj;
				teamTrackers.push_back(team_tracker);
				if (primary) {
					statusObjData["state"] = stateSectionObj;
				}
			}

			// Update minStorageReplicasRemaining. It is mainly used for fault tolerance computation later. Note that
			// FDB treats the entire remote region as one zone, and all the zones in the remote region are in the same
			// failure domain.
			if (primary) {
				*minStorageReplicasRemaining = std::max(*minStorageReplicasRemaining, 0) + replicas;
			} else if (replicas > 0) {
				*minStorageReplicasRemaining = std::max(*minStorageReplicasRemaining, 0) + 1;
			}
		}
		statusObjData["team_trackers"] = teamTrackers;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		// The most likely reason to be here is a timeout, either way we have no idea if the data state is healthy or
		// not from the "cluster" perspective - from the client perspective it is not but that is indicated elsewhere.
	}

	return statusObjData;
}

ACTOR template <class iface>
static Future<std::vector<std::pair<iface, EventMap>>> getServerMetrics(
    std::vector<iface> servers,
    std::unordered_map<NetworkAddress, WorkerInterface> address_workers,
    std::vector<std::string> eventNames) {
	state std::vector<Future<Optional<TraceEventFields>>> futures;
	for (auto s : servers) {
		for (auto name : eventNames) {
			futures.push_back(latestEventOnWorker(address_workers[s.address()], s.id().toString() + "/" + name));
		}
	}

	wait(waitForAll(futures));

	std::vector<std::pair<iface, EventMap>> results;
	auto futureItr = futures.begin();

	for (int i = 0; i < servers.size(); i++) {
		EventMap serverResults;
		for (auto name : eventNames) {
			ASSERT(futureItr != futures.end());
			serverResults[name] = futureItr->get().present() ? futureItr->get().get() : TraceEventFields();
			++futureItr;
		}

		results.emplace_back(servers[i], serverResults);
	}

	return results;
}

namespace {

const std::vector<std::string> STORAGE_SERVER_METRICS_LIST{ "StorageMetrics", "ReadLatencyMetrics", "ReadLatencyBands",
	                                                        "BusiestReadTag", "BusiestWriteTag",    "RocksDBMetrics" };

} // namespace

ACTOR static Future<std::vector<StorageServerStatusInfo>> getStorageServerStatusInfos(
    std::vector<StorageServerMetaInfo> storageMetadatas,
    std::unordered_map<NetworkAddress, WorkerInterface> address_workers,
    WorkerDetails rkWorker) {
	state std::vector<StorageServerStatusInfo> servers;
	servers.reserve(storageMetadatas.size());
	for (const auto& meta : storageMetadatas) {
		servers.push_back(StorageServerStatusInfo(meta));
	}
	state std::vector<std::pair<StorageServerStatusInfo, EventMap>> results;
	wait(store(results, getServerMetrics(servers, address_workers, STORAGE_SERVER_METRICS_LIST)));
	for (int i = 0; i < results.size(); ++i) {
		servers[i].eventMap = std::move(results[i].second);
	}
	return servers;
}

ACTOR static Future<std::vector<std::pair<TLogInterface, EventMap>>> getTLogsAndMetrics(
    Reference<AsyncVar<ServerDBInfo>> db,
    std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	std::vector<TLogInterface> servers = db->get().logSystemConfig.allPresentLogs();
	std::vector<std::pair<TLogInterface, EventMap>> results =
	    wait(getServerMetrics(servers, address_workers, std::vector<std::string>{ "TLogMetrics" }));

	return results;
}

// Returns list of tuples of grv proxy interfaces and their latency metrics
ACTOR static Future<std::vector<std::pair<CommitProxyInterface, EventMap>>> getCommitProxiesAndMetrics(
    Reference<AsyncVar<ServerDBInfo>> db,
    std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	std::vector<std::pair<CommitProxyInterface, EventMap>> results = wait(getServerMetrics(
	    db->get().client.commitProxies,
	    address_workers,
	    std::vector<std::string>{ "CommitLatencyMetrics", "CommitLatencyBands", "CommitBatchingWindowSize" }));

	return results;
}

ACTOR static Future<std::vector<std::pair<GrvProxyInterface, EventMap>>> getGrvProxiesAndMetrics(
    Reference<AsyncVar<ServerDBInfo>> db,
    std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	std::vector<std::pair<GrvProxyInterface, EventMap>> results = wait(
	    getServerMetrics(db->get().client.grvProxies,
	                     address_workers,
	                     std::vector<std::string>{ "GRVLatencyMetrics", "GRVLatencyBands", "GRVBatchLatencyMetrics" }));
	return results;
}

// Returns the number of zones eligible for recruiting new tLogs after zone failures, to maintain the current
// replication factor.
static int getExtraTLogEligibleZones(const std::vector<WorkerDetails>& workers,
                                     const DatabaseConfiguration& configuration) {
	std::set<StringRef> allZones;
	std::map<Key, std::set<StringRef>> dcId_zone;
	for (auto const& worker : workers) {
		if (worker.processClass.machineClassFitness(ProcessClass::TLog) < ProcessClass::NeverAssign &&
		    !configuration.isExcludedServer(worker.interf.addresses(), worker.interf.locality)) {
			allZones.insert(worker.interf.locality.zoneId().get());
			if (worker.interf.locality.dcId().present()) {
				dcId_zone[worker.interf.locality.dcId().get()].insert(worker.interf.locality.zoneId().get());
			}
		}
	}

	if (configuration.regions.size() == 0) {
		return allZones.size() - std::max(configuration.tLogReplicationFactor, configuration.storageTeamSize);
	}

	int extraTlogEligibleZones = 0;
	int regionsWithNonNegativePriority = 0;
	int maxRequiredReplicationFactor =
	    std::max(configuration.remoteTLogReplicationFactor,
	             std::max(configuration.tLogReplicationFactor, configuration.storageTeamSize));
	for (const auto& region : configuration.regions) {
		if (region.priority >= 0) {
			int eligible = dcId_zone[region.dcId].size() - maxRequiredReplicationFactor;

			// FIXME: does not take into account fallback satellite policies
			if (region.satelliteTLogReplicationFactor > 0 && configuration.usableRegions > 1) {
				int totalSatelliteEligible = 0;
				for (const auto& sat : region.satellites) {
					totalSatelliteEligible += dcId_zone[sat.dcId].size();
				}
				eligible = std::min<int>(eligible, totalSatelliteEligible - region.satelliteTLogReplicationFactor);
			}
			if (eligible >= 0) {
				regionsWithNonNegativePriority++;
			}
			extraTlogEligibleZones = std::max(extraTlogEligibleZones, eligible);
		}
	}
	if (regionsWithNonNegativePriority > 1) {
		// If the database is replicated across multiple regions, we can afford to lose one entire region without
		// losing data.
		extraTlogEligibleZones++;
	}
	return extraTlogEligibleZones;
}

JsonBuilderObject getPerfLimit(TraceEventFields const& ratekeeper, double transPerSec, double tpsLimit) {
	int reason = ratekeeper.getInt("Reason");
	JsonBuilderObject perfLimit;

	if (transPerSec > tpsLimit * 0.8) {
		// If reason is known, set qos.performance_limited_by, otherwise omit
		if (reason >= 0 && reason < limitReasonEnd) {
			perfLimit = JsonString::makeMessage(limitReasonName[reason], limitReasonDesc[reason]);
			std::string reason_server_id = ratekeeper.getValue("ReasonServerID");
			if (!reason_server_id.empty())
				perfLimit["reason_server_id"] = reason_server_id;
		}
	} else {
		perfLimit = JsonString::makeMessage("workload", "The database is not being saturated by the workload.");
	}

	if (!perfLimit.empty()) {
		perfLimit["reason_id"] = reason;
	}

	return perfLimit;
}

ACTOR static Future<JsonBuilderObject> workloadStatusFetcher(
    Reference<AsyncVar<ServerDBInfo>> db,
    std::vector<WorkerDetails> workers,
    WorkerDetails mWorker,
    WorkerDetails rkWorker,
    JsonBuilderObject* qos,
    JsonBuilderObject* data_overlay,
    JsonBuilderObject* tenants,
    std::set<std::string>* incomplete_reasons,
    Future<ErrorOr<std::vector<StorageServerStatusInfo>>> storageServerFuture) {
	state JsonBuilderObject statusObj;
	state JsonBuilderObject operationsObj;
	state JsonBuilderObject bytesObj;
	state JsonBuilderObject keysObj;

	// Writes and conflicts
	try {
		state std::vector<Future<TraceEventFields>> commitProxyStatFutures;
		state std::vector<Future<TraceEventFields>> grvProxyStatFutures;
		std::map<NetworkAddress, WorkerDetails> workersMap;
		for (auto const& w : workers) {
			workersMap[w.interf.address()] = w;
		}
		for (auto& p : db->get().client.commitProxies) {
			auto worker = getWorker(workersMap, p.address());
			if (worker.present())
				commitProxyStatFutures.push_back(timeoutError(
				    worker.get().interf.eventLogRequest.getReply(EventLogRequest("ProxyMetrics"_sr)), 1.0));
			else
				throw all_alternatives_failed(); // We need data from all proxies for this result to be trustworthy
		}
		for (auto& p : db->get().client.grvProxies) {
			auto worker = getWorker(workersMap, p.address());
			if (worker.present())
				grvProxyStatFutures.push_back(timeoutError(
				    worker.get().interf.eventLogRequest.getReply(EventLogRequest("GrvProxyMetrics"_sr)), 1.0));
			else
				throw all_alternatives_failed(); // We need data from all proxies for this result to be trustworthy
		}
		state std::vector<TraceEventFields> commitProxyStats = wait(getAll(commitProxyStatFutures));
		state std::vector<TraceEventFields> grvProxyStats = wait(getAll(grvProxyStatFutures));

		StatusCounter txnStartOut;
		StatusCounter txnSystemPriorityStartOut;
		StatusCounter txnDefaultPriorityStartOut;
		StatusCounter txnBatchPriorityStartOut;

		StatusCounter mutations;
		StatusCounter mutationBytes;
		StatusCounter txnConflicts;
		StatusCounter txnRejectedForQueuedTooLong;
		StatusCounter txnCommitOutSuccess;
		StatusCounter txnKeyLocationOut;
		StatusCounter txnMemoryErrors;

		for (auto& gps : grvProxyStats) {
			txnStartOut.updateValues(StatusCounter(gps.getValue("TxnStartOut")));
			txnSystemPriorityStartOut.updateValues(StatusCounter(gps.getValue("TxnSystemPriorityStartOut")));
			txnDefaultPriorityStartOut.updateValues(StatusCounter(gps.getValue("TxnDefaultPriorityStartOut")));
			txnBatchPriorityStartOut.updateValues(StatusCounter(gps.getValue("TxnBatchPriorityStartOut")));
			txnMemoryErrors.updateValues(StatusCounter(gps.getValue("TxnRequestErrors")));
		}

		for (auto& cps : commitProxyStats) {
			mutations.updateValues(StatusCounter(cps.getValue("Mutations")));
			mutationBytes.updateValues(StatusCounter(cps.getValue("MutationBytes")));
			txnConflicts.updateValues(StatusCounter(cps.getValue("TxnConflicts")));
			txnRejectedForQueuedTooLong.updateValues(StatusCounter(cps.getValue("TxnRejectedForQueuedTooLong")));
			txnCommitOutSuccess.updateValues(StatusCounter(cps.getValue("TxnCommitOutSuccess")));
			txnKeyLocationOut.updateValues(StatusCounter(cps.getValue("KeyServerLocationOut")));
			txnMemoryErrors.updateValues(StatusCounter(cps.getValue("KeyServerLocationErrors")));
			txnMemoryErrors.updateValues(StatusCounter(cps.getValue("TxnCommitErrors")));
		}

		operationsObj["writes"] = mutations.getStatus();
		operationsObj["location_requests"] = txnKeyLocationOut.getStatus();
		operationsObj["memory_errors"] = txnMemoryErrors.getStatus();
		bytesObj["written"] = mutationBytes.getStatus();

		JsonBuilderObject transactions;
		transactions["conflicted"] = txnConflicts.getStatus();
		transactions["started"] = txnStartOut.getStatus();
		transactions["rejected_for_queued_too_long"] = txnRejectedForQueuedTooLong.getStatus();
		transactions["started_immediate_priority"] = txnSystemPriorityStartOut.getStatus();
		transactions["started_default_priority"] = txnDefaultPriorityStartOut.getStatus();
		transactions["started_batch_priority"] = txnBatchPriorityStartOut.getStatus();
		transactions["committed"] = txnCommitOutSuccess.getStatus();

		statusObj["transactions"] = transactions;

		if (commitProxyStats.size() > 0) {
			(*tenants)["num_tenants"] = commitProxyStats[0].getUint64("NumTenants");
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown mutations, conflicts, and transactions state.");
	}

	// Transactions
	try {
		state Future<TraceEventFields> f1 =
		    timeoutError(rkWorker.interf.eventLogRequest.getReply(EventLogRequest("RkUpdate"_sr)), 1.0);
		state Future<TraceEventFields> f2 =
		    timeoutError(rkWorker.interf.eventLogRequest.getReply(EventLogRequest("RkUpdateBatch"_sr)), 1.0);
		wait(success(f1) && success(f2));
		TraceEventFields ratekeeper = f1.get();
		TraceEventFields batchRatekeeper = f2.get();

		bool autoThrottlingEnabled = ratekeeper.getInt("AutoThrottlingEnabled");
		double tpsLimit = ratekeeper.getDouble("TPSLimit");
		double batchTpsLimit = batchRatekeeper.getDouble("TPSLimit");
		double transPerSec = ratekeeper.getDouble("ReleasedTPS");
		double batchTransPerSec = ratekeeper.getDouble("ReleasedBatchTPS");
		int autoThrottledTags = ratekeeper.getInt("TagsAutoThrottled");
		int autoThrottledTagsBusyRead = ratekeeper.getInt("TagsAutoThrottledBusyRead");
		int autoThrottledTagsBusyWrite = ratekeeper.getInt("TagsAutoThrottledBusyWrite");
		int manualThrottledTags = ratekeeper.getInt("TagsManuallyThrottled");
		int ssCount = ratekeeper.getInt("StorageServers");
		int tlogCount = ratekeeper.getInt("TLogs");
		int64_t worstFreeSpaceStorageServer = ratekeeper.getInt64("WorstFreeSpaceStorageServer");
		int64_t worstFreeSpaceTLog = ratekeeper.getInt64("WorstFreeSpaceTLog");
		(*data_overlay).setKeyRawNumber("total_disk_used_bytes", ratekeeper.getValue("TotalDiskUsageBytes"));

		if (ssCount > 0) {
			(*data_overlay)["least_operating_space_bytes_storage_server"] =
			    std::max(worstFreeSpaceStorageServer, (int64_t)0);
			(*qos).setKeyRawNumber("worst_queue_bytes_storage_server", ratekeeper.getValue("WorstStorageServerQueue"));
			(*qos).setKeyRawNumber("limiting_queue_bytes_storage_server",
			                       ratekeeper.getValue("LimitingStorageServerQueue"));

			(*qos)["worst_data_lag_storage_server"] = getLagObject(ratekeeper.getInt64("WorstStorageServerVersionLag"));
			(*qos)["limiting_data_lag_storage_server"] =
			    getLagObject(ratekeeper.getInt64("LimitingStorageServerVersionLag"));
			(*qos)["worst_durability_lag_storage_server"] =
			    getLagObject(ratekeeper.getInt64("WorstStorageServerDurabilityLag"));
			(*qos)["limiting_durability_lag_storage_server"] =
			    getLagObject(ratekeeper.getInt64("LimitingStorageServerDurabilityLag"));
		}

		if (tlogCount > 0) {
			(*data_overlay)["least_operating_space_bytes_log_server"] = std::max(worstFreeSpaceTLog, (int64_t)0);
			(*qos).setKeyRawNumber("worst_queue_bytes_log_server", ratekeeper.getValue("WorstTLogQueue"));
		}

		(*qos)["transactions_per_second_limit"] = tpsLimit;
		(*qos)["batch_transactions_per_second_limit"] = batchTpsLimit;
		(*qos)["released_transactions_per_second"] = transPerSec;
		(*qos)["batch_released_transactions_per_second"] = batchTransPerSec;

		JsonBuilderObject throttledTagsObj;
		JsonBuilderObject autoThrottledTagsObj;
		autoThrottledTagsObj["count"] = autoThrottledTags;
		autoThrottledTagsObj["busy_read"] = autoThrottledTagsBusyRead;
		autoThrottledTagsObj["busy_write"] = autoThrottledTagsBusyWrite;
		if (autoThrottlingEnabled) {
			autoThrottledTagsObj["recommended_only"] = 0;
		} else {
			autoThrottledTagsObj["recommended_only"] = 1;
		}

		throttledTagsObj["auto"] = autoThrottledTagsObj;

		JsonBuilderObject manualThrottledTagsObj;
		manualThrottledTagsObj["count"] = manualThrottledTags;
		throttledTagsObj["manual"] = manualThrottledTagsObj;

		(*qos)["throttled_tags"] = throttledTagsObj;

		JsonBuilderObject perfLimit = getPerfLimit(ratekeeper, transPerSec, tpsLimit);
		if (!perfLimit.empty()) {
			(*qos)["performance_limited_by"] = perfLimit;
		}

		JsonBuilderObject batchPerfLimit = getPerfLimit(batchRatekeeper, transPerSec, batchTpsLimit);
		if (!batchPerfLimit.empty()) {
			(*qos)["batch_performance_limited_by"] = batchPerfLimit;
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown performance state.");
	}

	// Reads
	try {
		ErrorOr<std::vector<StorageServerStatusInfo>> storageServers = wait(storageServerFuture);
		if (!storageServers.present()) {
			throw storageServers.getError();
		}

		StatusCounter readRequests;
		StatusCounter reads;
		StatusCounter readKeys;
		StatusCounter readBytes;
		StatusCounter lowPriorityReads;

		for (auto& ss : storageServers.get()) {
			TraceEventFields const& storageMetrics = ss.eventMap.at("StorageMetrics");

			if (storageMetrics.size() > 0) {
				readRequests.updateValues(StatusCounter(storageMetrics.getValue("QueryQueue")));
				reads.updateValues(StatusCounter(storageMetrics.getValue("FinishedQueries")));
				readKeys.updateValues(StatusCounter(storageMetrics.getValue("RowsQueried")));
				readBytes.updateValues(StatusCounter(storageMetrics.getValue("BytesQueried")));
				lowPriorityReads.updateValues(StatusCounter(storageMetrics.getValue("LowPriorityQueries")));
			}
		}

		operationsObj["read_requests"] = readRequests.getStatus();
		operationsObj["reads"] = reads.getStatus();
		keysObj["read"] = readKeys.getStatus();
		bytesObj["read"] = readBytes.getStatus();
		operationsObj["low_priority_reads"] = lowPriorityReads.getStatus();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown read state.");
	}

	statusObj["operations"] = operationsObj;
	statusObj["keys"] = keysObj;
	statusObj["bytes"] = bytesObj;

	return statusObj;
}

ACTOR static Future<JsonBuilderObject> clusterSummaryStatisticsFetcher(
    WorkerEvents pMetrics,
    Future<ErrorOr<std::vector<StorageServerStatusInfo>>> storageServerFuture,
    Future<ErrorOr<std::vector<std::pair<TLogInterface, EventMap>>>> tlogFuture,
    std::set<std::string>* incomplete_reasons) {
	state JsonBuilderObject statusObj;
	try {
		state JsonBuilderObject cacheStatistics;

		ErrorOr<std::vector<StorageServerStatusInfo>> storageServers = wait(storageServerFuture);

		if (!storageServers.present()) {
			throw storageServers.getError();
		}

		double storageCacheHitsHz = 0;
		double storageCacheMissesHz = 0;

		for (auto& ss : storageServers.get()) {
			auto processMetrics = pMetrics.find(ss.address());
			if (processMetrics != pMetrics.end()) {
				int64_t hits = processMetrics->second.getInt64("CacheHits");
				int64_t misses = processMetrics->second.getInt64("CacheMisses");
				double elapsed = processMetrics->second.getDouble("Elapsed");
				storageCacheHitsHz += hits / elapsed;
				storageCacheMissesHz += misses / elapsed;
			}
		}

		cacheStatistics["storage_hit_rate"] =
		    (storageCacheMissesHz == 0) ? 1.0 : storageCacheHitsHz / (storageCacheHitsHz + storageCacheMissesHz);

		ErrorOr<std::vector<std::pair<TLogInterface, EventMap>>> tlogServers = wait(tlogFuture);

		if (!tlogServers.present()) {
			throw tlogServers.getError();
		}

		double logCacheHitsHz = 0;
		double logCacheMissesHz = 0;

		for (auto& log : tlogServers.get()) {
			auto processMetrics = pMetrics.find(log.first.address());
			if (processMetrics != pMetrics.end()) {
				int64_t hits = processMetrics->second.getInt64("CacheHits");
				int64_t misses = processMetrics->second.getInt64("CacheMisses");
				double elapsed = processMetrics->second.getDouble("Elapsed");
				logCacheHitsHz += hits / elapsed;
				logCacheMissesHz += misses / elapsed;
			}
		}

		cacheStatistics["log_hit_rate"] =
		    (logCacheMissesHz == 0) ? 1.0 : logCacheHitsHz / (logCacheHitsHz + logCacheMissesHz);
		statusObj["page_cache"] = cacheStatistics;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		incomplete_reasons->insert("Unknown cache statistics.");
	}

	return statusObj;
}

ACTOR static Future<JsonBuilderObject> blobGranulesStatusFetcher(
    Database cx,
    Optional<BlobManagerInterface> managerIntf,
    std::vector<BlobWorkerInterface> workers,
    std::unordered_map<NetworkAddress, WorkerInterface> addressWorkersMap,
    std::set<std::string>* incompleteReason) {

	state JsonBuilderObject statusObj;
	state std::vector<Future<Optional<TraceEventFields>>> futures;

	statusObj["number_of_blob_workers"] = static_cast<int>(workers.size());
	try {
		state BlobGranuleBackupConfig config;
		bool backupEnabled = wait(config.enabled().getD(SystemDBWriteLockedNow(cx.getReference())));
		statusObj["blob_granules_backup_enabled"] = backupEnabled;
		if (backupEnabled) {
			std::string manifestUrl = wait(config.manifestUrl().getD(SystemDBWriteLockedNow(cx.getReference())));
			statusObj["blob_granules_manifest_url"] = manifestUrl;
			std::string mlogsUrl = wait(config.mutationLogsUrl().getD(SystemDBWriteLockedNow(cx.getReference())));
			statusObj["blob_granules_mlogs_url"] = mlogsUrl;
		}
		// Blob manager status
		if (managerIntf.present()) {
			Optional<TraceEventFields> fields = wait(timeoutError(
			    latestEventOnWorker(addressWorkersMap[managerIntf.get().address()], "BlobManagerMetrics"), 2.0));
			if (fields.present()) {
				statusObj["last_flush_version"] = fields.get().getUint64("LastFlushVersion");
				statusObj["last_manifest_dump_ts"] = fields.get().getUint64("LastManifestDumpTs");
				statusObj["last_manifest_seq_no"] = fields.get().getUint64("LastManifestSeqNo");
				statusObj["last_manifest_epoch"] = fields.get().getUint64("Epoch");
				statusObj["last_manifest_size_in_bytes"] = fields.get().getUint64("ManifestSizeInBytes");
				statusObj["last_truncation_version"] = fields.get().getUint64("LastMLogTruncationVersion");
			}
		}

		// Blob workers status
		for (auto& intf : workers) {
			auto workerIntf = addressWorkersMap[intf.address()];
			futures.push_back(latestEventOnWorker(workerIntf, "BlobWorkerMetrics"));
		}

		wait(timeoutError(waitForAll(futures), 2.0));

		state int totalRanges = 0;
		for (auto future : futures) {
			if (future.get().present()) {
				auto latestTrace = future.get().get();
				int numRanges = latestTrace.getInt("NumRangesAssigned");
				totalRanges += numRanges;

				JsonBuilderObject workerStatusObj;
				workerStatusObj["number_of_key_ranges"] = numRanges;
				workerStatusObj["put_requests"] = StatusCounter(latestTrace.getValue("S3PutReqs")).getStatus();
				workerStatusObj["get_requests"] = StatusCounter(latestTrace.getValue("S3GetReqs")).getStatus();
				workerStatusObj["delete_requests"] = StatusCounter(latestTrace.getValue("S3DeleteReqs")).getStatus();
				workerStatusObj["bytes_buffered"] = latestTrace.getInt64("MutationBytesBuffered");
				workerStatusObj["compression_bytes_raw"] =
				    StatusCounter(latestTrace.getValue("CompressionBytesRaw")).getStatus();
				workerStatusObj["compression_bytes_final"] =
				    StatusCounter(latestTrace.getValue("CompressionBytesFinal")).getStatus();
				statusObj[latestTrace.getValue("ID")] = workerStatusObj;
			}
		}
		statusObj["number_of_key_ranges"] = totalRanges;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incompleteReason->insert("Unknown blob worker stats");
	}
	return statusObj;
}

ACTOR static Future<JsonBuilderObject> blobRestoreStatusFetcher(Database db, std::set<std::string>* incompleteReason) {
	state JsonBuilderObject statusObj;
	state Transaction tr(db);
	try {
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state BlobGranuleRestoreConfig config;
				state BlobRestorePhase phase =
				    wait(config.phase().getD(&tr, Snapshot::False, BlobRestorePhase::UNINIT));
				if (phase > BlobRestorePhase::UNINIT) {
					statusObj["blob_full_restore_phase"] = phase;
					int progress = wait(config.progress().getD(&tr));
					statusObj["blob_full_restore_phase_progress"] = progress;
					int64_t phaseStartTs = wait(config.phaseStartTs().getD(&tr, phase));
					statusObj["blob_full_restore_phase_start_ts"] = phaseStartTs;
					int64_t startTs = wait(config.phaseStartTs().getD(&tr, BlobRestorePhase::INIT));
					statusObj["blob_full_restore_start_ts"] = startTs;
					std::string error = wait(config.error().getD(&tr));
					statusObj["blob_full_restore_error"] = error;
					Version targetVersion = wait(config.targetVersion().getD(&tr));
					statusObj["blob_full_restore_version"] = targetVersion;
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incompleteReason->insert("Unable to query blob restore status");
	}

	return statusObj;
}

static JsonBuilderObject tlogFetcher(int* logFaultTolerance,
                                     const std::vector<TLogSet>& tLogs,
                                     std::unordered_map<NetworkAddress, WorkerInterface> const& address_workers) {
	JsonBuilderObject statusObj;
	JsonBuilderArray logsObj;
	Optional<int32_t> sat_log_replication_factor, sat_log_write_anti_quorum, sat_log_fault_tolerance,
	    log_replication_factor, log_write_anti_quorum, log_fault_tolerance, remote_log_replication_factor,
	    remote_log_fault_tolerance;

	int minFaultTolerance = 1000;
	int localSetsWithNonNegativeFaultTolerance = 0;

	for (const auto& tLogSet : tLogs) {
		if (tLogSet.tLogs.size() == 0) {
			// We can have LogSets where there are no tLogs but some LogRouters. It's the way
			// recruiting is implemented for old LogRouters in TagPartitionedLogSystem, where
			// it adds an empty LogSet for missing locality.
			continue;
		}

		int failedLogs = 0;
		for (auto& log : tLogSet.tLogs) {
			JsonBuilderObject logObj;
			bool failed = !log.present() || !address_workers.contains(log.interf().address());
			logObj["id"] = log.id().shortString();
			logObj["healthy"] = !failed;
			if (log.present()) {
				logObj["address"] = log.interf().address().toString();
			}
			logsObj.push_back(logObj);
			if (failed) {
				failedLogs++;
			}
		}

		if (tLogSet.isLocal) {
			ASSERT_WE_THINK(tLogSet.tLogReplicationFactor > 0);
			int currentFaultTolerance = tLogSet.tLogReplicationFactor - 1 - tLogSet.tLogWriteAntiQuorum - failedLogs;
			if (currentFaultTolerance >= 0) {
				localSetsWithNonNegativeFaultTolerance++;
			}

			if (tLogSet.locality == tagLocalitySatellite) {
				// FIXME: This hack to bump satellite fault tolerance, is to make it consistent
				//  with 6.2.
				minFaultTolerance = std::min(minFaultTolerance, currentFaultTolerance + 1);
			} else {
				minFaultTolerance = std::min(minFaultTolerance, currentFaultTolerance);
			}
		}

		if (tLogSet.isLocal && tLogSet.locality == tagLocalitySatellite) {
			sat_log_replication_factor = tLogSet.tLogReplicationFactor;
			sat_log_write_anti_quorum = tLogSet.tLogWriteAntiQuorum;
			sat_log_fault_tolerance = tLogSet.tLogReplicationFactor - 1 - tLogSet.tLogWriteAntiQuorum - failedLogs;
		} else if (tLogSet.isLocal) {
			log_replication_factor = tLogSet.tLogReplicationFactor;
			log_write_anti_quorum = tLogSet.tLogWriteAntiQuorum;
			log_fault_tolerance = tLogSet.tLogReplicationFactor - 1 - tLogSet.tLogWriteAntiQuorum - failedLogs;
		} else {
			remote_log_replication_factor = tLogSet.tLogReplicationFactor;
			remote_log_fault_tolerance = tLogSet.tLogReplicationFactor - 1 - failedLogs;
		}
	}
	if (minFaultTolerance == 1000) {
		// just in case we do not have any tlog sets
		minFaultTolerance = 0;
	}
	if (localSetsWithNonNegativeFaultTolerance > 1) {
		minFaultTolerance++;
	}
	*logFaultTolerance = std::min(*logFaultTolerance, minFaultTolerance);
	statusObj["log_interfaces"] = logsObj;
	// We may lose logs in this log generation, storage servers may never be able to catch up this log
	// generation.
	statusObj["possibly_losing_data"] = minFaultTolerance < 0;

	if (sat_log_replication_factor.present())
		statusObj["satellite_log_replication_factor"] = sat_log_replication_factor.get();
	if (sat_log_write_anti_quorum.present())
		statusObj["satellite_log_write_anti_quorum"] = sat_log_write_anti_quorum.get();
	if (sat_log_fault_tolerance.present())
		statusObj["satellite_log_fault_tolerance"] = sat_log_fault_tolerance.get();

	if (log_replication_factor.present())
		statusObj["log_replication_factor"] = log_replication_factor.get();
	if (log_write_anti_quorum.present())
		statusObj["log_write_anti_quorum"] = log_write_anti_quorum.get();
	if (log_fault_tolerance.present())
		statusObj["log_fault_tolerance"] = log_fault_tolerance.get();

	if (remote_log_replication_factor.present())
		statusObj["remote_log_replication_factor"] = remote_log_replication_factor.get();
	if (remote_log_fault_tolerance.present())
		statusObj["remote_log_fault_tolerance"] = remote_log_fault_tolerance.get();

	return statusObj;
}

static JsonBuilderArray tlogFetcher(int* logFaultTolerance,
                                    Reference<AsyncVar<ServerDBInfo>> db,
                                    std::unordered_map<NetworkAddress, WorkerInterface> const& address_workers) {
	JsonBuilderArray tlogsArray;
	JsonBuilderObject tlogsStatus;

	// First, fetch from the current TLog generation.
	tlogsStatus = tlogFetcher(logFaultTolerance, db->get().logSystemConfig.tLogs, address_workers);
	tlogsStatus["epoch"] = db->get().logSystemConfig.epoch;
	tlogsStatus["current"] = true;
	if (db->get().logSystemConfig.recoveredAt.present()) {
		tlogsStatus["begin_version"] = db->get().logSystemConfig.recoveredAt.get();
	}
	tlogsArray.push_back(tlogsStatus);

	// fetch all the old generations of TLogs.
	for (auto it : db->get().logSystemConfig.oldTLogs) {
		JsonBuilderObject oldTlogsStatus = tlogFetcher(logFaultTolerance, it.tLogs, address_workers);
		oldTlogsStatus["epoch"] = it.epoch;
		oldTlogsStatus["current"] = false;
		oldTlogsStatus["begin_version"] = it.epochBegin;
		oldTlogsStatus["end_version"] = it.epochEnd;
		tlogsArray.push_back(oldTlogsStatus);
	}
	return tlogsArray;
}

static JsonBuilderObject faultToleranceStatusFetcher(DatabaseConfiguration configuration,
                                                     ServerCoordinators coordinators,
                                                     const std::vector<NetworkAddress>& coordinatorAddresses,
                                                     const std::vector<WorkerDetails>& workers,
                                                     int extraTlogEligibleZones,
                                                     int minStorageReplicasRemaining,
                                                     int oldLogFaultTolerance,
                                                     int fullyReplicatedRegions,
                                                     bool underMaintenance) {
	JsonBuilderObject statusObj;

	// without losing data
	int32_t maxZoneFailures = configuration.maxZoneFailuresTolerated(fullyReplicatedRegions, false);
	if (underMaintenance) {
		maxZoneFailures--;
	}
	int maxCoordinatorFailures = (coordinators.clientLeaderServers.size() - 1) / 2;

	std::map<NetworkAddress, StringRef> workerZones;
	for (const auto& worker : workers) {
		workerZones[worker.interf.address()] = worker.interf.locality.zoneId().orDefault(""_sr);
	}
	std::map<StringRef, int> coordinatorZoneCounts;
	for (const auto& coordinator : coordinatorAddresses) {
		auto zone = workerZones[coordinator];
		coordinatorZoneCounts[zone] += 1;
	}
	std::vector<std::pair<StringRef, int>> coordinatorZones(coordinatorZoneCounts.begin(), coordinatorZoneCounts.end());
	std::sort(coordinatorZones.begin(),
	          coordinatorZones.end(),
	          [](const std::pair<StringRef, int>& lhs, const std::pair<StringRef, int>& rhs) {
		          return lhs.second > rhs.second;
	          });
	int lostCoordinators = 0;
	int maxCoordinatorZoneFailures = 0;
	for (auto zone : coordinatorZones) {
		lostCoordinators += zone.second;
		if (lostCoordinators > maxCoordinatorFailures) {
			break;
		}
		maxCoordinatorZoneFailures += 1;
	}
	// max zone failures that we can tolerate to not lose data
	int zoneFailuresWithoutLosingData = std::min(maxZoneFailures, maxCoordinatorZoneFailures);

	if (minStorageReplicasRemaining >= 0) {
		zoneFailuresWithoutLosingData = std::min(zoneFailuresWithoutLosingData, minStorageReplicasRemaining - 1);
	}

	// oldLogFaultTolerance means max failures we can tolerate to lose logs data. -1 means we lose data or
	// availability.
	zoneFailuresWithoutLosingData = std::max(std::min(zoneFailuresWithoutLosingData, oldLogFaultTolerance), -1);
	statusObj["max_zone_failures_without_losing_data"] = zoneFailuresWithoutLosingData;

	int32_t maxAvaiabilityZoneFailures = configuration.maxZoneFailuresTolerated(fullyReplicatedRegions, true);
	if (underMaintenance) {
		maxAvaiabilityZoneFailures--;
	}

	statusObj["max_zone_failures_without_losing_availability"] = std::max(
	    std::min(maxAvaiabilityZoneFailures, std::min(extraTlogEligibleZones, zoneFailuresWithoutLosingData)), -1);
	return statusObj;
}

static std::string getIssueDescription(std::string name) {
	if (name == "incorrect_cluster_file_contents") {
		return "Cluster file contents do not match current cluster connection string. Verify the cluster file and "
		       "its "
		       "parent directory are writable and that the cluster file has not been overwritten externally.";
	}

	// FIXME: name and description will be the same unless the message is 'incorrect_cluster_file_contents', which
	// is currently the only possible message
	return name;
}

static std::map<std::string, std::vector<JsonBuilderObject>> getProcessIssuesAsMessages(
    std::vector<ProcessIssues> const& issues) {
	std::map<std::string, std::vector<JsonBuilderObject>> issuesMap;

	try {
		for (auto processIssues : issues) {
			for (auto issue : processIssues.issues) {
				std::string issueStr = issue.toString();
				issuesMap[processIssues.address.toString()].push_back(
				    JsonString::makeMessage(issueStr.c_str(), getIssueDescription(issueStr).c_str()));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "ErrorParsingProcessIssues").error(e);
		// swallow
	}

	return issuesMap;
}

static JsonBuilderArray getClientIssuesAsMessages(
    std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>>* clientStatusMap) {
	JsonBuilderArray issuesList;

	try {
		std::map<std::string, std::pair<int, std::vector<std::string>>> deduplicatedIssues;

		for (auto iter = clientStatusMap->begin(); iter != clientStatusMap->end();) {
			if (now() - iter->second.first >= 2 * SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL) {
				iter = clientStatusMap->erase(iter);
				continue;
			}

			for (const auto& [issueKey, samples] : iter->second.second.issues) {
				auto& t = deduplicatedIssues[issueKey.toString()];
				t.first += samples.count;
				for (const auto& sample : samples.samples) {
					t.second.push_back(formatIpPort(sample.first.ip, sample.first.port));
				}
			}
			++iter;
		}

		// FIXME: add the log_group in addition to the network address
		for (auto i : deduplicatedIssues) {
			JsonBuilderObject message = JsonString::makeMessage(i.first.c_str(), getIssueDescription(i.first).c_str());
			JsonBuilderArray addresses;
			for (auto addr : i.second.second) {
				addresses.push_back(addr);
			}

			message["count"] = i.second.first;
			message["addresses"] = addresses;
			issuesList.push_back(message);
		}
	} catch (Error& e) {
		TraceEvent(SevError, "ErrorParsingClientIssues").error(e);
		// swallow
	}

	return issuesList;
}

ACTOR Future<JsonBuilderObject> layerStatusFetcher(Database cx,
                                                   JsonBuilderArray* messages,
                                                   std::set<std::string>* incomplete_reasons) {
	state StatusObject result;
	state JSONDoc json(result);
	state double tStart = now();

	try {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				int64_t timeout_ms = 3000;
				tr.setOption(FDBTransactionOptions::TIMEOUT, StringRef((uint8_t*)&timeout_ms, sizeof(int64_t)));

				std::string jsonPrefix = layerStatusMetaPrefixRange.begin.toString() + "json/";
				RangeResult jsonLayers = wait(tr.getRange(KeyRangeRef(jsonPrefix, strinc(jsonPrefix)), 1000));
				// TODO:  Also fetch other linked subtrees of meta keys

				state std::vector<Future<RangeResult>> docFutures;
				state int i;
				for (i = 0; i < jsonLayers.size(); ++i)
					docFutures.push_back(
					    tr.getRange(KeyRangeRef(jsonLayers[i].value, strinc(jsonLayers[i].value)), 1000));

				result.clear();
				JSONDoc::expires_reference_version = (uint64_t)tr.getReadVersion().get();

				for (i = 0; i < docFutures.size(); ++i) {
					state RangeResult docs = wait(docFutures[i]);
					state int j;
					for (j = 0; j < docs.size(); ++j) {
						state json_spirit::mValue doc;
						try {
							json_spirit::read_string(docs[j].value.toString(), doc);
							wait(yield());
							json.absorb(doc.get_obj());
							wait(yield());
						} catch (Error& e) {
							TraceEvent(SevWarn, "LayerStatusBadJSON").detail("Key", docs[j].key);
						}
					}
				}
				json.create("_valid") = true;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "LayerStatusError").error(e);
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		incomplete_reasons->insert(format("Unable to retrieve layer status (%s).", e.what()));
		json.create("_error") = format("Unable to retrieve layer status (%s).", e.what());
		json.create("_valid") = false;
	}

	json.cleanOps();
	JsonBuilderObject statusObj;
	statusObj.addContents(result);
	TraceEvent("LayerStatusFetcher")
	    .detail("Duration", now() - tStart)
	    .detail("StatusSize", statusObj.getFinalLength());
	return statusObj;
}

ACTOR Future<JsonBuilderObject> lockedStatusFetcher(Database cx,
                                                    JsonBuilderArray* messages,
                                                    std::set<std::string>* incomplete_reasons) {
	state JsonBuilderObject statusObj;

	state Transaction tr(cx);
	state int timeoutSeconds = 5;
	state Future<Void> getTimeout = delay(timeoutSeconds);

	loop {
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		try {
			choose {
				when(Optional<Value> lockUID = wait(tr.get(databaseLockedKey))) {
					if (lockUID.present()) {
						statusObj["locked"] = true;
						statusObj["lock_uid"] =
						    BinaryReader::fromStringRef<UID>(lockUID.get().substr(10), Unversioned()).toString();
					} else {
						statusObj["locked"] = false;
					}
				}
				when(wait(getTimeout)) {
					incomplete_reasons->insert(
					    format("Unable to determine if database is locked after %d seconds.", timeoutSeconds));
				}
			}
			break;
		} catch (Error& e) {
			try {
				wait(tr.onError(e));
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;

				incomplete_reasons->insert(format("Unable to determine if database is locked (%s).", e.what()));
				break;
			}
		}
	}
	return statusObj;
}

ACTOR Future<Optional<Value>> getActivePrimaryDC(Database cx, int* fullyReplicatedRegions, JsonBuilderArray* messages) {
	state ReadYourWritesTransaction tr(cx);

	state Future<Void> readTimeout = delay(5); // so that we won't loop forever
	loop {
		try {
			if (readTimeout.isReady()) {
				throw timed_out();
			}
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			state Future<RangeResult> fReplicaKeys = tr.getRange(datacenterReplicasKeys, CLIENT_KNOBS->TOO_MANY);
			state Future<Optional<Value>> fPrimaryDatacenterKey = tr.get(primaryDatacenterKey);
			wait(timeoutError(success(fPrimaryDatacenterKey) && success(fReplicaKeys), 5));

			*fullyReplicatedRegions = fReplicaKeys.get().size();

			if (!fPrimaryDatacenterKey.get().present()) {
				messages->push_back(
				    JsonString::makeMessage("primary_dc_missing", "Unable to determine primary datacenter."));
			}
			return fPrimaryDatacenterKey.get();
		} catch (Error& e) {
			if (e.code() == error_code_timed_out) {
				messages->push_back(
				    JsonString::makeMessage("fetch_primary_dc_timeout", "Fetching primary DC timed out."));
				return Optional<Value>();
			} else {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<std::pair<Optional<StorageWiggleMetrics>, Optional<StorageWiggleMetrics>>> readStorageWiggleMetrics(
    Database cx,
    bool use_system_priority) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state Optional<StorageWiggleMetrics> primaryV;
	state Optional<StorageWiggleMetrics> remoteV;
	state StorageWiggleData wiggleState;
	loop {
		try {
			if (use_system_priority) {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			}
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			wait(store(primaryV, wiggleState.storageWiggleMetrics(PrimaryRegion(true)).get(tr)) &&
			     store(remoteV, wiggleState.storageWiggleMetrics(PrimaryRegion(false)).get(tr)));
			return std::make_pair(primaryV, remoteV);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<KMSHealthStatus> getKMSHealthStatus(Reference<const AsyncVar<ServerDBInfo>> db) {
	state KMSHealthStatus unhealthy;
	unhealthy.canConnectToEKP = false;
	unhealthy.canConnectToKms = false;
	unhealthy.lastUpdatedTS = now();
	try {
		if (!db->get().client.encryptKeyProxy.present()) {
			return unhealthy;
		}
		KMSHealthStatus reply = wait(timeoutError(
		    db->get().client.encryptKeyProxy.get().getHealthStatus.getReply(EncryptKeyProxyHealthStatusRequest()),
		    FLOW_KNOBS->EKP_HEALTH_CHECK_REQUEST_TIMEOUT));
		return reply;
	} catch (Error& e) {
		if (e.code() != error_code_timed_out) {
			throw;
		}
		return unhealthy;
	}
}

// read storageWigglerStats through Read-only tx, then convert it to JSON field
ACTOR Future<JsonBuilderObject> storageWigglerStatsFetcher(Optional<DataDistributorInterface> ddWorker,
                                                           DatabaseConfiguration conf,
                                                           Database cx,
                                                           bool use_system_priority,
                                                           JsonBuilderArray* messages) {

	state Future<GetStorageWigglerStateReply> stateFut;
	state Future<std::pair<Optional<StorageWiggleMetrics>, Optional<StorageWiggleMetrics>>> wiggleMetricsFut =
	    timeoutError(readStorageWiggleMetrics(cx, use_system_priority), 2.0);
	state JsonBuilderObject res;
	if (ddWorker.present()) {
		stateFut = timeoutError(ddWorker.get().storageWigglerState.getReply(GetStorageWigglerStateRequest()), 2.0);
		wait(ready(stateFut));
	} else {
		return res;
	}

	try {
		if (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.01)) {
			throw timed_out();
		}

		wait(success(wiggleMetricsFut) && success(stateFut));
		auto [primaryV, remoteV] = wiggleMetricsFut.get();
		if (primaryV.present()) {
			auto obj = primaryV.get().toJSON();
			auto& reply = stateFut.get();
			obj["state"] = StorageWiggler::getWiggleStateStr(static_cast<StorageWiggler::State>(reply.primary));
			obj["last_state_change_timestamp"] = reply.lastStateChangePrimary;
			obj["last_state_change_datetime"] = epochsToGMTString(reply.lastStateChangePrimary);
			res["primary"] = obj;
		}
		if (conf.regions.size() > 1 && remoteV.present()) {
			auto obj = remoteV.get().toJSON();
			auto& reply = stateFut.get();
			obj["state"] = StorageWiggler::getWiggleStateStr(static_cast<StorageWiggler::State>(reply.remote));
			obj["last_state_change_timestamp"] = reply.lastStateChangeRemote;
			obj["last_state_change_datetime"] = epochsToGMTString(reply.lastStateChangeRemote);
			res["remote"] = obj;
		}
		return res;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		messages->push_back(JsonString::makeMessage("fetch_storage_wiggler_stats_timeout",
		                                            "Fetching storage wiggler stats timed out."));
	}
	return res;
}

// constructs the cluster section of the json status output
ACTOR Future<StatusReply> clusterGetStatus(
    Reference<AsyncVar<ServerDBInfo>> db,
    Database cx,
    std::vector<WorkerDetails> workers,
    std::vector<ProcessIssues> workerIssues,
    std::vector<StorageServerMetaInfo> storageMetadatas,
    std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>>* clientStatus,
    ServerCoordinators coordinators,
    std::vector<NetworkAddress> incompatibleConnections,
    Version datacenterVersionDifference,
    Version dcLogServerVersionDifference,
    Version dcStorageServerVersionDifference,
    ConfigBroadcaster const* configBroadcaster,
    Optional<UnversionedMetaclusterRegistrationEntry> metaclusterRegistration,
    metacluster::MetaclusterMetrics metaclusterMetrics,
    std::unordered_map<NetworkAddress, double> excludedDegradedServers) {

	state double tStart = timer();

	state JsonBuilderArray messages;
	state std::set<std::string> status_incomplete_reasons;
	state WorkerDetails mWorker; // Master worker
	state WorkerDetails ccWorker; // Cluster-Controller worker
	state WorkerDetails ddWorker; // DataDistributor worker
	state WorkerDetails rkWorker; // Ratekeeper worker
	state WorkerDetails csWorker; // ConsistencyScan worker

	try {

		state JsonBuilderObject statusObj;

		// Get EKP Health
		if (db->get().client.encryptKeyProxy.present()) {
			KMSHealthStatus status = wait(getKMSHealthStatus(db));

			// encryption-at-rest status
			JsonBuilderObject earStatusObj;
			earStatusObj["ekp_is_healthy"] = status.canConnectToEKP;
			statusObj["encryption_at_rest"] = earStatusObj;

			JsonBuilderObject kmsStatusObj;
			kmsStatusObj["kms_is_healthy"] = status.canConnectToKms;
			if (status.canConnectToEKP) {
				kmsStatusObj["kms_connector_type"] = status.kmsConnectorType;
				kmsStatusObj["kms_stable"] = status.kmsStable;
				JsonBuilderArray kmsUrlsArr;
				for (const auto& url : status.restKMSUrls) {
					kmsUrlsArr.push_back(url);
				}
				kmsStatusObj["kms_urls"] = kmsUrlsArr;
			}
			statusObj["kms"] = kmsStatusObj;

			// TODO: In this scenario we should see if we can fetch any status fields that don't depend on encryption
			if (!status.canConnectToKms || !status.canConnectToEKP) {
				return StatusReply(statusObj.getJson());
			}
		}

		// Get the master Worker interface
		Optional<WorkerDetails> _mWorker = getWorker(workers, db->get().master.address());
		if (_mWorker.present()) {
			mWorker = _mWorker.get();
		} else {
			messages.push_back(
			    JsonString::makeMessage("unreachable_master_worker", "Unable to locate the master worker."));
		}

		// Get the cluster-controller Worker interface
		Optional<WorkerDetails> _ccWorker = getWorker(workers, db->get().clusterInterface.address());
		if (_ccWorker.present()) {
			ccWorker = _ccWorker.get();
		} else {
			messages.push_back(JsonString::makeMessage("unreachable_cluster_controller_worker",
			                                           "Unable to locate the cluster-controller worker."));
		}

		// Get the DataDistributor worker interface
		Optional<WorkerDetails> _ddWorker;
		if (db->get().distributor.present()) {
			_ddWorker = getWorker(workers, db->get().distributor.get().address());
		}

		if (!db->get().distributor.present() || !_ddWorker.present()) {
			messages.push_back(JsonString::makeMessage("unreachable_dataDistributor_worker",
			                                           "Unable to locate the data distributor worker."));
		} else {
			ddWorker = _ddWorker.get();
		}

		// Get the Ratekeeper worker interface
		Optional<WorkerDetails> _rkWorker;
		if (db->get().ratekeeper.present()) {
			_rkWorker = getWorker(workers, db->get().ratekeeper.get().address());
		}

		if (!db->get().ratekeeper.present() || !_rkWorker.present()) {
			messages.push_back(
			    JsonString::makeMessage("unreachable_ratekeeper_worker", "Unable to locate the ratekeeper worker."));
		} else {
			rkWorker = _rkWorker.get();
		}

		// Get the ConsistencyScan worker interface
		Optional<WorkerDetails> _csWorker;
		if (db->get().consistencyScan.present()) {
			_csWorker = getWorker(workers, db->get().consistencyScan.get().address());
		}

		if (!db->get().consistencyScan.present() || !_csWorker.present()) {
			messages.push_back(JsonString::makeMessage("unreachable_consistencyScan_worker",
			                                           "Unable to locate the consistencyScan worker."));
		} else {
			csWorker = _csWorker.get();
		}

		// Get latest events for various event types from ALL workers
		// WorkerEvents is a map of worker's NetworkAddress to its event string
		// The pair represents worker responses and a set of worker NetworkAddress strings which did not respond.
		std::vector<Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>>> futures;
		futures.push_back(latestEventOnWorkers(workers, "MachineMetrics"));
		futures.push_back(latestEventOnWorkers(workers, "ProcessMetrics"));
		futures.push_back(latestEventOnWorkers(workers, "NetworkMetrics"));
		futures.push_back(latestErrorOnWorkers(workers)); // Get all latest errors.
		futures.push_back(latestEventOnWorkers(workers, "TraceFileOpenError"));
		futures.push_back(latestEventOnWorkers(workers, "ProgramStart"));

		// Wait for all response pairs.
		state std::vector<Optional<std::pair<WorkerEvents, std::set<std::string>>>> workerEventsVec =
		    wait(getAll(futures));

		// Create a unique set of all workers who were unreachable for 1 or more of the event requests above.
		// Since each event request is independent and to all workers, workers can have responded to some
		// event requests but still end up in the unreachable set.
		std::set<std::string> mergeUnreachable;

		// For each (optional) pair, if the pair is present and not empty then add the unreachable workers to the
		// set.
		for (auto pair : workerEventsVec) {
			if (pair.present() && !pair.get().second.empty())
				mergeUnreachable.insert(pair.get().second.begin(), pair.get().second.end());
		}

		// We now have a unique set of workers who were in some way unreachable.  If there is anything in that set,
		// create a message for it and include the list of unreachable processes.
		if (!mergeUnreachable.empty()) {
			JsonBuilderObject message =
			    JsonBuilder::makeMessage("unreachable_processes", "The cluster has some unreachable processes.");
			JsonBuilderArray unreachableProcs;
			for (auto m : mergeUnreachable) {
				unreachableProcs.push_back(JsonBuilderObject().setKey("address", m));
			}
			message["unreachable_processes"] = unreachableProcs;
			messages.push_back(message);
		}

		state ProtocolVersionData protocolVersion = wait(getNewestProtocolVersion(cx, ccWorker));

		// construct status information for cluster subsections
		state int statusCode = (int)RecoveryStatus::END;
		state Future<JsonBuilderObject> recoveryStateStatusFuture =
		    recoveryStateStatusFetcher(cx, ccWorker, mWorker, workers.size(), &status_incomplete_reasons, &statusCode);

		state Future<JsonBuilderObject> idmpKeyStatusFuture = getIdmpKeyStatus(cx);

		state Future<JsonBuilderObject> versionEpochStatusFuture =
		    versionEpochStatusFetcher(cx, &status_incomplete_reasons);

		wait(waitForAll<JsonBuilderObject>(
		    { recoveryStateStatusFuture, idmpKeyStatusFuture, versionEpochStatusFuture }));

		state JsonBuilderObject recoveryStateStatus = recoveryStateStatusFuture.get();
		state JsonBuilderObject idmpKeyStatus = idmpKeyStatusFuture.get();
		state JsonBuilderObject versionEpochStatus = versionEpochStatusFuture.get();

		// machine metrics
		state WorkerEvents mMetrics = workerEventsVec[0].present() ? workerEventsVec[0].get().first : WorkerEvents();
		// process metrics
		state WorkerEvents pMetrics = workerEventsVec[1].present() ? workerEventsVec[1].get().first : WorkerEvents();
		state WorkerEvents networkMetrics =
		    workerEventsVec[2].present() ? workerEventsVec[2].get().first : WorkerEvents();
		state WorkerEvents latestError = workerEventsVec[3].present() ? workerEventsVec[3].get().first : WorkerEvents();
		state WorkerEvents traceFileOpenErrors =
		    workerEventsVec[4].present() ? workerEventsVec[4].get().first : WorkerEvents();
		state WorkerEvents programStarts =
		    workerEventsVec[5].present() ? workerEventsVec[5].get().first : WorkerEvents();

		if (db->get().recoveryCount > 0) {
			statusObj["generation"] = db->get().recoveryCount;
		}

		state std::map<std::string, std::vector<JsonBuilderObject>> processIssues =
		    getProcessIssuesAsMessages(workerIssues);
		state std::vector<StorageServerStatusInfo> storageServers;
		state std::vector<std::pair<TLogInterface, EventMap>> tLogs;
		state std::vector<std::pair<CommitProxyInterface, EventMap>> commitProxies;
		state std::vector<std::pair<GrvProxyInterface, EventMap>> grvProxies;
		state std::vector<BlobWorkerInterface> blobWorkers;

		state JsonBuilderObject qos;
		state JsonBuilderObject dataOverlay;
		state JsonBuilderObject tenants;
		state JsonBuilderObject metacluster;
		state JsonBuilderObject storageWiggler;
		state std::unordered_set<UID> wiggleServers;

		statusObj["protocol_version"] = format("%" PRIx64, g_network->protocolVersion().version());
		statusObj["connection_string"] = coordinators.ccr->getConnectionString().toString();
		statusObj["bounce_impact"] = getBounceImpactInfo(statusCode);
		statusObj["newest_protocol_version"] = format("%" PRIx64, protocolVersion.newestProtocolVersion.version());
		statusObj["lowest_compatible_protocol_version"] =
		    format("%" PRIx64, protocolVersion.lowestCompatibleProtocolVersion.version());

		state Optional<DatabaseConfiguration> configuration;
		state Optional<LoadConfigurationResult> loadResult;
		state std::unordered_map<NetworkAddress, WorkerInterface> address_workers;

		if (statusCode != RecoveryStatus::configuration_missing) {
			std::pair<Optional<DatabaseConfiguration>, Optional<LoadConfigurationResult>> loadResults =
			    wait(loadConfiguration(cx, &messages, &status_incomplete_reasons));
			configuration = loadResults.first;
			loadResult = loadResults.second;
		}

		if (loadResult.present()) {
			statusObj["full_replication"] = loadResult.get().fullReplication;
			if (loadResult.get().healthyZone.present()) {
				if (loadResult.get().healthyZone.get() != ignoreSSFailuresZoneString) {
					statusObj["maintenance_zone"] = loadResult.get().healthyZone.get().printable();
					statusObj["maintenance_seconds_remaining"] = loadResult.get().healthyZoneSeconds;
				} else {
					statusObj["data_distribution_disabled_for_ss_failures"] = true;
				}
			}
			if (loadResult.get().rebalanceDDIgnored) {
				// TODO: change the hex string to human-friendly fields like "rebalance_read", "rebalance_disk"
				statusObj["data_distribution_disabled_for_rebalance"] = true;
				statusObj["data_distribution_disabled_hex"] = loadResult.get().rebalanceDDIgnoreHex;
			}
			if (loadResult.get().dataDistributionDisabled) {
				statusObj["data_distribution_disabled"] = true;
			}
		}

		statusObj["machines"] = machineStatusFetcher(mMetrics, workers, configuration, &status_incomplete_reasons);

		state std::vector<NetworkAddress> coordinatorAddresses;
		if (configuration.present()) {
			// Do the latency probe by itself to avoid interference from other status activities
			state bool isAvailable = true;
			JsonBuilderObject latencyProbeResults =
			    wait(latencyProbeFetcher(cx, &messages, &status_incomplete_reasons, &isAvailable));

			statusObj["database_available"] = isAvailable;
			if (!latencyProbeResults.empty()) {
				statusObj["latency_probe"] = latencyProbeResults;
			}

			state std::vector<Future<Void>> warningFutures;
			if (isAvailable) {
				warningFutures.push_back(consistencyCheckStatusFetcher(cx, &messages, &status_incomplete_reasons));
				if (!SERVER_KNOBS->DISABLE_DUPLICATE_LOG_WARNING) {
					warningFutures.push_back(logRangeWarningFetcher(cx, &messages, &status_incomplete_reasons));
				}
			}

			// Start getting storage servers now (using system priority) concurrently.  Using sys priority because
			// having storage servers in status output is important to give context to error messages in status that
			// reference a storage server role ID.
			for (auto const& worker : workers) {
				address_workers[worker.interf.address()] = worker.interf;
			}

			state Future<ErrorOr<std::vector<StorageServerStatusInfo>>> storageServerFuture =
			    errorOr(getStorageServerStatusInfos(storageMetadatas, address_workers, rkWorker));
			state Future<ErrorOr<std::vector<std::pair<TLogInterface, EventMap>>>> tLogFuture =
			    errorOr(getTLogsAndMetrics(db, address_workers));
			state Future<ErrorOr<std::vector<std::pair<CommitProxyInterface, EventMap>>>> commitProxyFuture =
			    errorOr(getCommitProxiesAndMetrics(db, address_workers));
			state Future<ErrorOr<std::vector<std::pair<GrvProxyInterface, EventMap>>>> grvProxyFuture =
			    errorOr(getGrvProxiesAndMetrics(db, address_workers));
			state Future<ErrorOr<std::vector<BlobWorkerInterface>>> blobWorkersFuture;

			if (configuration.present() && configuration.get().blobGranulesEnabled) {
				blobWorkersFuture = errorOr(timeoutError(getBlobWorkers(cx, true), 5.0));
			}

			state int minStorageReplicasRemaining = -1;
			state int fullyReplicatedRegions = -1;
			// NOTE: here we should start all the transaction before wait in order to overlay latency
			state Future<Optional<Value>> primaryDCFO = getActivePrimaryDC(cx, &fullyReplicatedRegions, &messages);
			state std::vector<Future<JsonBuilderObject>> futures2;
			futures2.push_back(dataStatusFetcher(ddWorker, configuration.get(), &minStorageReplicasRemaining));
			futures2.push_back(workloadStatusFetcher(db,
			                                         workers,
			                                         mWorker,
			                                         rkWorker,
			                                         &qos,
			                                         &dataOverlay,
			                                         &tenants,
			                                         &status_incomplete_reasons,
			                                         storageServerFuture));
			futures2.push_back(layerStatusFetcher(cx, &messages, &status_incomplete_reasons));
			futures2.push_back(lockedStatusFetcher(cx, &messages, &status_incomplete_reasons));
			futures2.push_back(
			    clusterSummaryStatisticsFetcher(pMetrics, storageServerFuture, tLogFuture, &status_incomplete_reasons));

			if (configuration.get().perpetualStorageWiggleSpeed > 0) {
				state Future<std::vector<std::pair<UID, StorageWiggleValue>>> primaryWiggleValues;
				state Future<std::vector<std::pair<UID, StorageWiggleValue>>> remoteWiggleValues;
				double timeout = g_network->isSimulated() && BUGGIFY_WITH_PROB(0.01) ? 0.0 : 2.0;
				primaryWiggleValues = timeoutError(readStorageWiggleValues(cx, true, true), timeout);
				remoteWiggleValues = timeoutError(readStorageWiggleValues(cx, false, true), timeout);
				wait(store(
				         storageWiggler,
				         storageWigglerStatsFetcher(db->get().distributor, configuration.get(), cx, true, &messages)) &&
				     ready(primaryWiggleValues) && ready(remoteWiggleValues));

				if (primaryWiggleValues.canGet()) {
					for (auto& p : primaryWiggleValues.get())
						wiggleServers.insert(p.first);
				} else {
					messages.push_back(
					    JsonString::makeMessage("fetch_storage_wiggler_stats_timeout",
					                            "Fetching wiggling servers in primary region timed out"));
				}
				if (remoteWiggleValues.canGet()) {
					for (auto& p : remoteWiggleValues.get())
						wiggleServers.insert(p.first);
				} else {
					messages.push_back(JsonString::makeMessage("fetch_storage_wiggler_stats_timeout",
					                                           "Fetching wiggling servers in remote region timed out"));
				}
			}

			state std::vector<JsonBuilderObject> workerStatuses = wait(getAll(futures2));
			wait(success(primaryDCFO));

			ErrorOr<std::vector<NetworkAddress>> addresses =
			    wait(errorOr(timeoutError(coordinators.ccr->getConnectionString().tryResolveHostnames(), 5.0)));
			if (addresses.present()) {
				coordinatorAddresses = std::move(addresses.get());
			} else {
				messages.push_back(
				    JsonString::makeMessage("fetch_coordinator_addresses", "Fetching coordinator addresses timed out"));
			}

			int logFaultTolerance = 100;
			if (db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
				statusObj["logs"] = tlogFetcher(&logFaultTolerance, db, address_workers);
			}

			int extraTlogEligibleZones = getExtraTLogEligibleZones(workers, configuration.get());
			statusObj["fault_tolerance"] =
			    faultToleranceStatusFetcher(configuration.get(),
			                                coordinators,
			                                coordinatorAddresses,
			                                workers,
			                                extraTlogEligibleZones,
			                                minStorageReplicasRemaining,
			                                logFaultTolerance,
			                                fullyReplicatedRegions,
			                                loadResult.present() && loadResult.get().healthyZone.present());

			state JsonBuilderObject configObj =
			    configurationFetcher(configuration, coordinators, &status_incomplete_reasons);

			if (primaryDCFO.get().present()) {
				statusObj["active_primary_dc"] = primaryDCFO.get().get();
			}
			// configArr could be empty
			if (!configObj.empty()) {
				statusObj["configuration"] = configObj;
			}

			// workloadStatusFetcher returns the workload section but also optionally writes the qos section and
			// adds to the dataOverlay object
			if (!workerStatuses[1].empty())
				statusObj["workload"] = workerStatuses[1];

			statusObj["layers"] = workerStatuses[2];
			if (configBroadcaster) {
				// TODO: Read from coordinators for more up-to-date config database status?
				statusObj["configuration_database"] = configBroadcaster->getStatus();
			}

			// Add qos section if it was populated
			if (!qos.empty())
				statusObj["qos"] = qos;

			// Metacluster metadata
			if (metaclusterRegistration.present()) {
				metacluster["cluster_type"] = clusterTypeToString(metaclusterRegistration.get().clusterType);
				metacluster["metacluster_name"] = metaclusterRegistration.get().metaclusterName;
				metacluster["metacluster_id"] = metaclusterRegistration.get().metaclusterId.toString();
				if (metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA) {
					metacluster["data_cluster_name"] = metaclusterRegistration.get().name;
					metacluster["data_cluster_id"] = metaclusterRegistration.get().id.toString();
				} else if (!metaclusterMetrics.error.present()) { // clusterType == ClusterType::METACLUSTER_MANAGEMENT
					metacluster["num_data_clusters"] = metaclusterMetrics.numDataClusters;
					tenants["num_tenants"] = metaclusterMetrics.numTenants;
					tenants["tenant_group_capacity"] = metaclusterMetrics.tenantGroupCapacity;
					tenants["tenant_groups_allocated"] = metaclusterMetrics.tenantGroupsAllocated;
				} else {
					CODE_PROBE(true, "Failed to fetch metacluster metrics", probe::decoration::rare);
					messages.push_back(JsonString::makeMessage(
					    "metacluster_metrics_missing",
					    fmt::format("Failed to fetch metacluster metrics: {}.", metaclusterMetrics.error.get())
					        .c_str()));
				}

			} else {
				metacluster["cluster_type"] = clusterTypeToString(ClusterType::STANDALONE);
			}
			statusObj["metacluster"] = metacluster;

			if (!tenants.empty())
				statusObj["tenants"] = tenants;

			statusObj["version_epoch"] = versionEpochStatus;

			// Merge dataOverlay into data
			JsonBuilderObject& clusterDataSection = workerStatuses[0];

			// TODO:  This probably is no longer possible as there is no ability to merge json objects with an
			// output-only model
			clusterDataSection.addContents(dataOverlay);

			// If data section not empty, add it to statusObj
			if (!clusterDataSection.empty())
				statusObj["data"] = clusterDataSection;

			// Insert database_lock_state section
			if (!workerStatuses[3].empty()) {
				statusObj["database_lock_state"] = workerStatuses[3];
			}

			// Insert cluster summary statistics
			if (!workerStatuses[4].empty()) {
				statusObj.addContents(workerStatuses[4]);
			}

			// Need storage servers now for processStatusFetcher() below.
			ErrorOr<std::vector<StorageServerStatusInfo>> _storageServers = wait(storageServerFuture);
			if (_storageServers.present()) {
				storageServers = _storageServers.get();
			} else {
				messages.push_back(
				    JsonBuilder::makeMessage("storage_servers_error", "Timed out trying to retrieve storage servers."));
			}

			// ...also tlogs
			ErrorOr<std::vector<std::pair<TLogInterface, EventMap>>> _tLogs = wait(tLogFuture);
			if (_tLogs.present()) {
				tLogs = _tLogs.get();
			} else {
				messages.push_back(
				    JsonBuilder::makeMessage("log_servers_error", "Timed out trying to retrieve log servers."));
			}

			// ...also commit proxies
			ErrorOr<std::vector<std::pair<CommitProxyInterface, EventMap>>> _commitProxies = wait(commitProxyFuture);
			if (_commitProxies.present()) {
				commitProxies = _commitProxies.get();
			} else {
				messages.push_back(
				    JsonBuilder::makeMessage("commit_proxies_error", "Timed out trying to retrieve commit proxies."));
			}

			// ...also grv proxies
			ErrorOr<std::vector<std::pair<GrvProxyInterface, EventMap>>> _grvProxies = wait(grvProxyFuture);
			if (_grvProxies.present()) {
				grvProxies = _grvProxies.get();
			} else {
				messages.push_back(
				    JsonBuilder::makeMessage("grv_proxies_error", "Timed out trying to retrieve grv proxies."));
			}

			// ...also blob workers
			if (configuration.present() && configuration.get().blobGranulesEnabled) {
				ErrorOr<std::vector<BlobWorkerInterface>> _blobWorkers = wait(blobWorkersFuture);
				if (_blobWorkers.present()) {
					blobWorkers = _blobWorkers.get();
				} else {
					messages.push_back(
					    JsonBuilder::makeMessage("blob_workers_error", "Timed out trying to retrieve blob workers."));
				}
			}

			wait(waitForAll(warningFutures));
		} else {
			// Set layers status to { _valid: false, error: "configurationMissing"}
			JsonBuilderObject layers;
			layers["_valid"] = false;
			layers["_error"] = "configurationMissing";
			statusObj["layers"] = layers;
		}

		JsonBuilderObject processStatus =
		    wait(processStatusFetcher(db,
		                              workers,
		                              pMetrics,
		                              mMetrics,
		                              networkMetrics,
		                              latestError,
		                              traceFileOpenErrors,
		                              programStarts,
		                              processIssues,
		                              storageServers,
		                              tLogs,
		                              commitProxies,
		                              grvProxies,
		                              blobWorkers,
		                              coordinators,
		                              coordinatorAddresses,
		                              cx,
		                              configuration,
		                              loadResult.present() ? loadResult.get().healthyZone : Optional<Key>(),
		                              &status_incomplete_reasons));
		statusObj["processes"] = processStatus;
		statusObj["clients"] = clientStatusFetcher(clientStatus);

		if (configuration.present() && configuration.get().blobGranulesEnabled) {
			ErrorOr<JsonBuilderObject> blobGranulesStatus = wait(errorOr(
			    timeoutError(blobGranulesStatusFetcher(
			                     cx, db->get().blobManager, blobWorkers, address_workers, &status_incomplete_reasons),
			                 2.0)));
			if (blobGranulesStatus.present()) {
				statusObj["blob_granules"] = blobGranulesStatus.get();
			} else {
				messages.push_back(JsonString::makeMessage("fetch_blob_granule_status_timed_out",
				                                           "Fetch BlobGranule status timed out."));
			}

			ErrorOr<JsonBuilderObject> blobRestoreStatus =
			    wait(errorOr(timeoutError(blobRestoreStatusFetcher(cx, &status_incomplete_reasons), 2.0)));
			if (blobRestoreStatus.present()) {
				statusObj["blob_restore"] = blobRestoreStatus.get();
			} else {
				messages.push_back(JsonString::makeMessage("fetch_blob_restore_status_timed_out",
				                                           "Fetch BlobRestore status timed out."));
			}
		}

		JsonBuilderArray incompatibleConnectionsArray;
		for (auto it : incompatibleConnections) {
			incompatibleConnectionsArray.push_back(it.toString());
		}
		statusObj["incompatible_connections"] = incompatibleConnectionsArray;
		statusObj["datacenter_lag"] = getLagObject(datacenterVersionDifference);
		statusObj["logserver_lag"] = getLagObject(dcLogServerVersionDifference);
		statusObj["storageserver_lag"] = getLagObject(dcStorageServerVersionDifference);

		int activeTSSCount = 0;
		JsonBuilderArray wiggleServerAddress;
		for (auto& it : storageServers) {
			if (it.isTss()) {
				activeTSSCount++;
			}
			if (wiggleServers.contains(it.id())) {
				wiggleServerAddress.push_back(it.address().toString());
			}
		}
		statusObj["active_tss_count"] = activeTSSCount;

		if (!storageWiggler.empty()) {
			JsonBuilderArray wiggleServerUID;
			for (auto& id : wiggleServers)
				wiggleServerUID.push_back(id.shortString());

			storageWiggler["wiggle_server_ids"] = wiggleServerUID;
			storageWiggler["wiggle_server_addresses"] = wiggleServerAddress;
			statusObj["storage_wiggler"] = storageWiggler;
		}

		int totalDegraded = 0;
		for (auto& it : workers) {
			if (it.degraded) {
				totalDegraded++;
			}
		}
		statusObj["degraded_processes"] = totalDegraded;

		if (!recoveryStateStatus.empty())
			statusObj["recovery_state"] = recoveryStateStatus;

		statusObj["idempotency_ids"] = idmpKeyStatus;

		// cluster messages subsection;
		JsonBuilderArray clientIssuesArr = getClientIssuesAsMessages(clientStatus);
		if (clientIssuesArr.size() > 0) {
			JsonBuilderObject clientIssueMessage =
			    JsonBuilder::makeMessage("client_issues", "Some clients of this cluster have issues.");
			clientIssueMessage["issues"] = clientIssuesArr;
			messages.push_back(clientIssueMessage);
		}

		// Fetch Consistency Scan Information
		try {
			state ConsistencyScanState cs;
			state ConsistencyScanState::Config config;
			state ConsistencyScanState::RoundStats currentRound;
			state ConsistencyScanState::LifetimeStats lifetime;
			state ConsistencyScanState::StatsHistoryMap::RangeResultType olderStats;
			auto sysDB = SystemDBWriteLockedNow(cx.getReference());

			// TODO: Use the same transaction.
			wait(timeoutError(
			    store(config, cs.config().getD(sysDB)) && store(currentRound, cs.currentRoundStats().getD(sysDB)) &&
			        store(lifetime, cs.lifetimeStats().getD(sysDB)) &&
			        store(olderStats,
			              cs.roundStatsHistory().getRange(sysDB, {}, {}, 5, Snapshot::False, Reverse::True)),
			    2.0));
			json_spirit::mObject csDoc;
			csDoc["configuration"] = config.toJSON();
			csDoc["lifetime_stats"] = lifetime.toJSON();
			csDoc["current_round"] = currentRound.toJSON();
			json_spirit::mArray olderRounds;
			for (auto const& kv : olderStats.results) {
				olderRounds.push_back(kv.second.toJSON());
			}
			csDoc["previous_rounds"] = olderRounds;

			statusObj["consistency_scan"] = csDoc;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			messages.push_back(JsonString::makeMessage("fetch_consistency_scan_status_timeout",
			                                           "Fetching consistency scan state timed out."));
		}

		// Create the status_incomplete message if there were any reasons that the status is incomplete.
		if (!status_incomplete_reasons.empty()) {
			JsonBuilderObject incomplete_message =
			    JsonBuilder::makeMessage("status_incomplete", "Unable to retrieve all status information.");
			// Make a JSON array of all of the reasons in the status_incomplete_reasons set.
			JsonBuilderArray reasons;
			for (auto i : status_incomplete_reasons) {
				reasons.push_back(JsonBuilderObject().setKey("description", i));
			}
			incomplete_message["reasons"] = reasons;
			messages.push_back(incomplete_message);
		}

		statusObj["messages"] = messages;

		int64_t clusterTime = g_network->timer();
		if (clusterTime != -1) {
			statusObj["cluster_controller_timestamp"] = clusterTime;
		}

		if (SERVER_KNOBS->CC_GRAY_FAILURE_STATUS_JSON) {
			statusObj["gray_failure"] = grayFailureStatus(excludedDegradedServers);
		}

		TraceEvent("ClusterGetStatus")
		    .detail("Duration", timer() - tStart)
		    .detail("StatusSize", statusObj.getFinalLength());

		return StatusReply(statusObj.getJson());

	} catch (Error& e) {
		TraceEvent(SevError, "StatusError").error(e);
		throw;
	}
}

StatusReply clusterGetFaultToleranceStatus(const std::string& statusStr) {
	double tStart = timer();

	try {
		json_spirit::mValue mv = readJSONStrictly(statusStr);
		JSONDoc jsonDoc(mv);

		std::string faultToleranceRelatedFields[] = {
			"fault_tolerance", "data",    "logs", "maintenance_zone", "maintenance_seconds_remaining", "qos",
			"recovery_state",  "messages"
		};

		JsonBuilderObject statusObj;
		for (std::string& field : faultToleranceRelatedFields) {
			if (jsonDoc.has(field)) {
				statusObj[field] = jsonDoc.last();
			}
		}

		TraceEvent("ClusterGetFaultToleranceStatus")
		    .detail("Duration", timer() - tStart)
		    .detail("StatusSize", statusObj.getFinalLength());

		return StatusReply(statusObj.getJson());
	} catch (Error& e) {
		TraceEvent(SevError, "StatusError").error(e);
		throw;
	}
}

bool checkAsciiNumber(const char* s) {
	JsonBuilderObject number;
	number.setKeyRawNumber("number", s);
	std::string js = number.getJson();
	printf("'%s' => %s\n", s, js.c_str());

	try {
		// Make sure it parses as JSON
		readJSONStrictly(js);
	} catch (Error& e) {
		printf("error: %s\n", e.what());
		return false;
	}

	return true;
}

bool checkJson(const JsonBuilder& j, const char* expected) {
	std::string js = j.getJson();
	printf("json:     '%s'\n", js.c_str());
	printf("expected: '%s'\n\n", expected);

	try {
		// Make sure it parses as JSON
		readJSONStrictly(js);
	} catch (Error& e) {
		printf("error: %s\n", e.what());
		return false;
	}

	return js == expected;
}

TEST_CASE("/status/json/builder") {
	JsonBuilder json;
	ASSERT(checkJson(json, "null"));

	JsonBuilderArray array;
	ASSERT(checkJson(array, "[]"));

	array.push_back(1);
	ASSERT(checkJson(array, "[1]"));

	array.push_back(2);
	ASSERT(checkJson(array, "[1,2]"));

	array.push_back("test");
	ASSERT(checkJson(array, "[1,2,\"test\"]"));

	JsonBuilderObject object;
	ASSERT(checkJson(object, "{}"));

	object.setKey("a", 5);
	ASSERT(checkJson(object, "{\"a\":5}"));

	object.setKey("b", "hi");
	ASSERT(checkJson(object, "{\"a\":5,\"b\":\"hi\"}"));

	object.setKey("c", array);
	ASSERT(checkJson(object, "{\"a\":5,\"b\":\"hi\",\"c\":[1,2,\"test\"]}"));

	JsonBuilderArray array2;

	array2.push_back(json);
	ASSERT(checkJson(array2, "[null]"));

	object.setKey("d", array2);
	ASSERT(checkJson(object, "{\"a\":5,\"b\":\"hi\",\"c\":[1,2,\"test\"],\"d\":[null]}"));

	JsonBuilderObject object2;
	object2["x"] = 1;
	object2["y"] = "why";
	object2["z"] = std::string("zee");
	ASSERT(checkJson(object2, "{\"x\":1,\"y\":\"why\",\"z\":\"zee\"}"));

	object2.addContents(object);
	ASSERT(checkJson(object2,
	                 "{\"x\":1,\"y\":\"why\",\"z\":\"zee\",\"a\":5,\"b\":\"hi\",\"c\":[1,2,\"test\"],\"d\":[null]}"));

	object2.addContents(JsonBuilderObject());
	ASSERT(checkJson(object2,
	                 "{\"x\":1,\"y\":\"why\",\"z\":\"zee\",\"a\":5,\"b\":\"hi\",\"c\":[1,2,\"test\"],\"d\":[null]}"));

	array2.addContents(array);
	ASSERT(checkJson(array2, "[null,1,2,\"test\"]"));

	array2.addContents(JsonBuilderArray());
	ASSERT(checkJson(array2, "[null,1,2,\"test\"]"));

	JsonBuilderObject object3;
	object3["infinity"] = std::numeric_limits<double>::infinity();
	object3["nan"] = std::numeric_limits<double>::quiet_NaN();
	ASSERT(checkJson(object3, "{\"infinity\":1e99,\"nan\":-999}"));

	ASSERT(checkAsciiNumber("inf"));
	ASSERT(checkAsciiNumber("infA"));
	ASSERT(checkAsciiNumber("in"));
	ASSERT(checkAsciiNumber("-inf"));
	ASSERT(checkAsciiNumber("-infA"));
	ASSERT(checkAsciiNumber("-in"));
	ASSERT(checkAsciiNumber("a"));
	ASSERT(checkAsciiNumber("-1a.0"));
	ASSERT(checkAsciiNumber("-01a.0"));
	ASSERT(checkAsciiNumber("01.0a"));
	ASSERT(checkAsciiNumber("-1.0"));
	ASSERT(checkAsciiNumber("-01.0"));
	ASSERT(checkAsciiNumber("01.0"));
	ASSERT(checkAsciiNumber("-001"));
	ASSERT(checkAsciiNumber("000."));
	ASSERT(checkAsciiNumber("-0001.e-"));
	ASSERT(checkAsciiNumber("-0001.0e-01"));
	ASSERT(checkAsciiNumber("-000123e-234"));
	ASSERT(checkAsciiNumber("-09234.12312e-132"));
	ASSERT(checkAsciiNumber("-111.e-01"));
	ASSERT(checkAsciiNumber("-00111.e-01"));
	ASSERT(checkAsciiNumber("-.e"));
	ASSERT(checkAsciiNumber("-09234.123a12e-132"));
	ASSERT(checkAsciiNumber("-11a1.e-01"));
	ASSERT(checkAsciiNumber("-00111.ae-01"));
	ASSERT(checkAsciiNumber("-.ea"));
	ASSERT(checkAsciiNumber("-.e+"));
	ASSERT(checkAsciiNumber("-.0e+1"));

	return Void();
}

JsonBuilderObject randomDocument(const std::vector<std::string>& strings, int& limit, int level);
JsonBuilderArray randomArray(const std::vector<std::string>& strings, int& limit, int level);

JsonBuilderArray randomArray(const std::vector<std::string>& strings, int& limit, int level) {
	JsonBuilderArray r;
	int size = deterministicRandom()->randomInt(0, 50);

	while (--size) {
		if (--limit <= 0)
			break;

		if (level > 0 && deterministicRandom()->coinflip()) {
			if (deterministicRandom()->coinflip())
				r.push_back(randomDocument(strings, limit, level - 1));
			else
				r.push_back(randomArray(strings, limit, level - 1));
		} else {
			switch (deterministicRandom()->randomInt(0, 3)) {
			case 0:
				r.push_back(deterministicRandom()->randomInt(0, 10000000));
			case 1:
				r.push_back(strings[deterministicRandom()->randomInt(0, strings.size())]);
			case 2:
			default:
				r.push_back(deterministicRandom()->random01());
			}
		}
	}

	return r;
}

JsonBuilderObject randomDocument(const std::vector<std::string>& strings, int& limit, int level) {
	JsonBuilderObject r;
	int size = deterministicRandom()->randomInt(0, 300);

	while (--size) {
		if (--limit <= 0)
			break;

		const std::string& key = strings[deterministicRandom()->randomInt(0, strings.size())];

		if (level > 0 && deterministicRandom()->coinflip()) {
			if (deterministicRandom()->coinflip())
				r[key] = randomDocument(strings, limit, level - 1);
			else
				r[key] = randomArray(strings, limit, level - 1);
		} else {
			switch (deterministicRandom()->randomInt(0, 3)) {
			case 0:
				r[key] = deterministicRandom()->randomInt(0, 10000000);
			case 1:
				r[key] = strings[deterministicRandom()->randomInt(0, strings.size())];
			case 2:
			default:
				r[key] = deterministicRandom()->random01();
			}
		}
	}

	return r;
}

TEST_CASE("Lstatus/json/builderPerf") {
	std::vector<std::string> strings;
	int c = 1000000;
	printf("Generating random strings\n");
	while (--c)
		strings.push_back(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(0, 50)));

	int elements = 100000;
	int level = 6;
	int iterations = 200;

	printf("Generating and serializing random document\n");

	int64_t bytes = 0;
	double generated = 0;
	double serialized = 0;
	for (int i = 0; i < iterations; i++) {
		int n = elements;
		double start;

		start = timer();
		JsonBuilderObject obj = randomDocument(strings, n, level);
		double generate = timer() - start;

		start = timer();
		std::string s = obj.getJson();
		double serialize = timer() - start;

		start = timer();
		json_spirit::mValue mv = readJSONStrictly(s);
		double jsParse = timer() - start;

		start = timer();
		std::string jsStr = json_spirit::write_string(mv);
		double jsSerialize = timer() - start;

		printf("JsonBuilder: %8lu bytes  %-7.5f gen   +  %-7.5f serialize =  %-7.5f\n",
		       s.size(),
		       generate,
		       serialize,
		       generate + serialize);
		printf("json_spirit: %8lu bytes  %-7.5f parse +  %-7.5f serialize =  %-7.5f\n",
		       jsStr.size(),
		       jsParse,
		       jsSerialize,
		       jsParse + jsSerialize);
		printf("\n");

		generated += generate;
		serialized += serialize;
		bytes += s.size();
	}

	double elapsed = generated + serialized;
	fmt::print("RESULT: {0}"
	           " bytes  {1} elements  {2} levels  {3} seconds ({4} gen, {5} serialize)  {6} MB/s  {7} items/s\n",
	           bytes,
	           iterations * elements,
	           level,
	           elapsed,
	           generated,
	           elapsed - generated,
	           bytes / elapsed / 1e6,
	           iterations * elements / elapsed);

	return Void();
}

TEST_CASE("/status/json/merging") {
	StatusObject objA, objB, objC;
	JSONDoc a(objA), b(objB), c(objC);

	a.create("int_one") = 1;
	a.create("int_unmatched") = 2;
	a.create("int_total_30.$sum") = 10;
	a.create("bool_true.$and") = true;
	a.create("string") = "test";
	a.create("subdoc.int_11") = 11;
	a.create("a") = "justA";
	a.create("subdoc.double_max_5.$max") = 2.0;
	a.create("subdoc.double_min_2.$min") = 2.0;
	a.create("subdoc.obj_count_3.$count_keys.one") = 1;
	a.create("subdoc.obj_count_3.$count_keys.two") = 2;
	a.create("expired.$expires") = "I should have expired.";
	a.create("expired.version") = 1;
	a.create("not_expired_and_merged.$expires.seven.$sum") = 1;
	a.create("not_expired_and_merged.$expires.one.$min") = 3;
	a.create("not_expired_and_merged.version") = 3;
	a.create("mixed_numeric_sum_6.$sum") = 0.5;
	a.create("mixed_numeric_min_0.$min") = 1.5;

	b.create("int_one") = 1;
	b.create("int_unmatched") = 3;
	b.create("int_total_30.$sum") = 20;
	b.create("bool_true.$and") = true;
	b.create("string") = "test";
	b.create("subdoc.int_11") = 11;
	b.create("b") = "justB";
	b.create("subdoc.double_max_5.$max") = 5.0;
	b.create("subdoc.double_min_2.$min") = 5.0;
	b.create("subdoc.obj_count_3.$count_keys.three") = 3;
	b.create("expired.$expires") = "I should have also expired.";
	b.create("expired.version") = 1;
	b.create("not_expired_and_merged.$expires.seven.$sum") = 2;
	b.create("not_expired_and_merged.$expires.one.$min") = 1;
	b.create("not_expired_and_merged.version") = 3;
	b.create("last_hello.$last") = "blah";
	b.create("latest_obj.$latest.a") = 0;
	b.create("latest_obj.$latest.b") = 0;
	b.create("latest_obj.$latest.c") = 0;
	b.create("latest_obj.timestamp") = 2;
	b.create("latest_int_5.$latest") = 7;
	b.create("latest_int_5.timestamp") = 2;
	b.create("mixed_numeric_sum_6.$sum") = 1;
	b.create("mixed_numeric_min_0.$min") = 4.5;

	c.create("int_total_30.$sum") = 0;
	c.create("not_expired.$expires") = "I am still valid";
	c.create("not_expired.version") = 3;
	c.create("not_expired_and_merged.$expires.seven.$sum") = 4;
	c.create("not_expired_and_merged.$expires.one.$min") = 2;
	c.create("not_expired_and_merged.version") = 3;
	c.create("last_hello.$last") = "hello";
	c.create("latest_obj.$latest.a.$max") = "a";
	c.create("latest_obj.$latest.b.$min") = "b";
	c.create("latest_obj.$latest.expired.$expires") = "I should not be here.";
	c.create("latest_obj.$latest.expired.version") = 1;
	c.create("latest_obj.$latest.not_expired.$expires") = "Still alive.";
	c.create("latest_obj.$latest.not_expired.version") = 3;
	c.create("latest_obj.timestamp") = 3;
	c.create("latest_int_5.$latest") = 5;
	c.create("latest_int_5.timestamp") = 3;
	c.create("mixed_numeric_sum_6.$sum") = 4.5;
	c.create("mixed_numeric_min_0.$min") = (double)0.0;

	printf("a = \n%s\n", json_spirit::write_string(json_spirit::mValue(objA), json_spirit::pretty_print).c_str());
	printf("b = \n%s\n", json_spirit::write_string(json_spirit::mValue(objB), json_spirit::pretty_print).c_str());
	printf("c = \n%s\n", json_spirit::write_string(json_spirit::mValue(objC), json_spirit::pretty_print).c_str());

	JSONDoc::expires_reference_version = 2;
	a.absorb(b);
	a.absorb(c);
	a.cleanOps();
	printf("result = \n%s\n", json_spirit::write_string(json_spirit::mValue(objA), json_spirit::pretty_print).c_str());
	std::string result = json_spirit::write_string(json_spirit::mValue(objA));
	std::string expected = "{\"a\":\"justA\",\"b\":\"justB\",\"bool_true\":true,\"expired\":null,\"int_one\":1,\"int_"
	                       "total_30\":30,\"int_unmatched\":{\"ERROR\":\"Values do not "
	                       "match.\",\"a\":2,\"b\":3},\"last_hello\":\"hello\",\"latest_int_5\":5,\"latest_obj\":{"
	                       "\"a\":\"a\",\"b\":\"b\",\"not_expired\":\"Still "
	                       "alive.\"},\"mixed_numeric_min_0\":0,\"mixed_numeric_sum_6\":6,\"not_expired\":\"I am still "
	                       "valid\",\"not_expired_and_merged\":{\"one\":1,\"seven\":7},\"string\":\"test\",\"subdoc\":{"
	                       "\"double_max_5\":5,\"double_min_2\":2,\"int_11\":11,\"obj_count_3\":3}}";

	if (result != expected) {
		printf("ERROR:  Combined doc does not match expected.\nexpected:\n\n%s\nresult:\n%s\n",
		       expected.c_str(),
		       result.c_str());
		ASSERT(false);
	}

	return Void();
}
