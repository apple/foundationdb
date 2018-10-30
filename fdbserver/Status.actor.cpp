/*
 * Status.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/Status.h"
#include "flow/Trace.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/WorkerInterface.h"
#include "fdbserver/ClusterRecruitmentInterface.h"
#include <time.h>
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/DataDistribution.h"
#include "flow/UnitTest.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/RecoveryState.h"
#include "fdbclient/JsonBuilder.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

const char* RecoveryStatus::names[] = {
	"reading_coordinated_state", "locking_coordinated_state", "locking_old_transaction_servers", "reading_transaction_system_state",
	"configuration_missing", "configuration_never_created", "configuration_invalid",
	"recruiting_transaction_servers", "initializing_transaction_servers", "recovery_transaction",
	"writing_coordinated_state", "accepting_commits", "all_logs_recruited", "storage_recovered", "fully_recovered"
};
static_assert( sizeof(RecoveryStatus::names) == sizeof(RecoveryStatus::names[0])*RecoveryStatus::END, "RecoveryStatus::names[] size" );
const char* RecoveryStatus::descriptions[] = {
	// reading_coordinated_state
	"Requesting information from coordination servers. Verify that a majority of coordination server processes are active.",
	// locking_coordinated_state
	"Locking coordination state. Verify that a majority of coordination server processes are active.",
	// locking_old_transaction_servers
	"Locking old transaction servers. Verify that at least one transaction server from the previous generation is running.",
	// reading_transaction_system_state
	"Recovering transaction server state. Verify that the transaction server processes are active.",
	// configuration_missing
	"There appears to be a database, but its configuration does not appear to be initialized.",
	// configuration_never_created
	"The coordinator(s) have no record of this database. Either the coordinator addresses are incorrect, the coordination state on those machines is missing, or no database has been created.",
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
static_assert( sizeof(RecoveryStatus::descriptions) == sizeof(RecoveryStatus::descriptions[0])*RecoveryStatus::END, "RecoveryStatus::descriptions[] size" );

// From Ratekeeper.actor.cpp
extern int limitReasonEnd;
extern const char* limitReasonName[];
extern const char* limitReasonDesc[];

struct WorkerEvents : std::map<NetworkAddress, TraceEventFields>  {};

ACTOR static Future< Optional<TraceEventFields> > latestEventOnWorker(WorkerInterface worker, std::string eventName) {
	try {
		EventLogRequest req = eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
		ErrorOr<TraceEventFields> eventTrace  = wait( errorOr(timeoutError(worker.eventLogRequest.getReply(req), 2.0)));

		if (eventTrace.isError()){
			return Optional<TraceEventFields>();
		}
		return eventTrace.get();
	}
	catch (Error &e){
		if (e.code() == error_code_actor_cancelled)
			throw;
		return Optional<TraceEventFields>();
	}
}

ACTOR static Future< Optional< std::pair<WorkerEvents, std::set<std::string>> > > latestEventOnWorkers(std::vector<std::pair<WorkerInterface, ProcessClass>> workers, std::string eventName) {
	try {
		state vector<Future<ErrorOr<TraceEventFields>>> eventTraces;
		for (int c = 0; c < workers.size(); c++) {
			EventLogRequest req = eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
			eventTraces.push_back(errorOr(timeoutError(workers[c].first.eventLogRequest.getReply(req), 2.0)));
		}

		wait(waitForAll(eventTraces));

		std::set<std::string> failed;
		WorkerEvents results;

		for (int i = 0; i < eventTraces.size(); i++) {
			const ErrorOr<TraceEventFields>& v = eventTraces[i].get();
			if (v.isError()){
				failed.insert(workers[i].first.address().toString());
				results[workers[i].first.address()] = TraceEventFields();
			}
			else {
				results[workers[i].first.address()] = v.get();
			}
		}

		std::pair<WorkerEvents, std::set<std::string>> val;
		val.first = results;
		val.second = failed;

		return val;
	}
	catch (Error &e){
		ASSERT(e.code() == error_code_actor_cancelled); // All errors should be filtering through the errorOr actor above
		throw;
	}
}
static Future< Optional< std::pair<WorkerEvents, std::set<std::string>> > > latestErrorOnWorkers(std::vector<std::pair<WorkerInterface, ProcessClass>> workers) {
	return latestEventOnWorkers( workers, "" );
}

static Optional<std::pair<WorkerInterface, ProcessClass>> getWorker(std::vector<std::pair<WorkerInterface, ProcessClass>> const& workers, NetworkAddress const& address) {
	try {
		for (int c = 0; c < workers.size(); c++)
			if (address == workers[c].first.address())
				return workers[c];
		return Optional<std::pair<WorkerInterface, ProcessClass>>();
	}
	catch (Error &e){
		return Optional<std::pair<WorkerInterface, ProcessClass>>();
	}
}

static Optional<std::pair<WorkerInterface, ProcessClass>> getWorker(std::map<NetworkAddress, std::pair<WorkerInterface, ProcessClass>> const& workersMap, NetworkAddress const& address) {
	auto itr = workersMap.find(address);
	if(itr == workersMap.end()) {
		return Optional<std::pair<WorkerInterface, ProcessClass>>();
	}

	return itr->second;
}

class StatusCounter {
	public:
		StatusCounter(double hz=0.0, double roughness=0.0, int64_t counter=0) : _hz(hz), _roughness(roughness), _counter(counter) {}
		StatusCounter(const std::string& parsableText) {
			parseText(parsableText);
		}

		StatusCounter& parseText(const std::string& parsableText) {
			sscanf(parsableText.c_str(), "%lf %lf %lld", &_hz, &_roughness, &_counter);
			return *this;
		}

		StatusCounter& updateValues(const StatusCounter& statusCounter) {
			double hzNew = _hz + statusCounter._hz;
			double roughnessNew = (_hz + statusCounter._hz) ? (_roughness*_hz + statusCounter._roughness*statusCounter._hz) / (_hz + statusCounter._hz) : 0.0;
			int64_t counterNew = _counter + statusCounter._counter;
			_hz = hzNew;
			_roughness = roughnessNew;
			_counter = counterNew;
			return *this;
		}

		JsonBuilderObject getStatus() const {
			JsonBuilderObject statusObject;
			statusObject["hz"] = _hz;
			statusObject["roughness"] = _roughness;
			statusObject["counter"] = _counter;
			return statusObject;
		}

	protected:
		double	_hz;
		double	_roughness;
		int64_t	_counter;
};

static double parseDouble(std::string const& s, bool permissive = false) {
	double d = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%lf%n", &d, &consumed);
	if (r == 1 && (consumed == s.size() || permissive))
		return d;
	throw attribute_not_found();
}

static int parseInt(std::string const& s, bool permissive = false) {
	long long int iLong = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%lld%n", &iLong, &consumed);
	if (r == 1 && (consumed == s.size() || permissive)){
		if (std::numeric_limits<int>::min() <= iLong && iLong <= std::numeric_limits<int>::max())
			return (int)iLong;  // Downcast definitely safe
		else
			throw attribute_too_large();
	}
	throw attribute_not_found();
}

static int64_t parseInt64(std::string const& s, bool permissive = false) {
	long long int i = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%lld%n", &i, &consumed);
	if (r == 1 && (consumed == s.size() || permissive))
		return i;
	throw attribute_not_found();
}

static JsonBuilderObject getLocalityInfo(const LocalityData& locality) {
	JsonBuilderObject localityObj;

	for(auto it = locality._data.begin(); it != locality._data.end(); it++) {
		if(it->second.present()) {
			localityObj[it->first] = it->second.get();
		}
		else {
			localityObj[it->first] = JsonBuilder();
		}
	}

	return localityObj;
}

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
			if(errorFields.tryGetValue("Error", errorName)) {
				statusObj["name"] = errorName;
				description += ": " + errorName;
			}
			else
				statusObj["name"] = "process_error";

			struct tm* timeinfo;
			time_t t = (time_t)time;
			timeinfo = localtime(&t);
			char buffer[128];
			strftime(buffer, 128, "%c", timeinfo);
			description += " at " + std::string(buffer);

			statusObj["description"] = description;
		}
	}
	catch (Error &e){
		TraceEvent(SevError, "StatusGetErrorError").error(e).detail("RawError", errorFields.toString());
	}
	return statusObj;
}

static JsonBuilderObject machineStatusFetcher(WorkerEvents mMetrics, vector<std::pair<WorkerInterface, ProcessClass>> workers, Optional<DatabaseConfiguration> configuration, std::set<std::string> *incomplete_reasons) {
	JsonBuilderObject machineMap;
	double metric;
	int failed = 0;

	// map from machine networkAddress to datacenter ID
	std::map<NetworkAddress, std::string> dcIds;
	std::map<NetworkAddress, LocalityData> locality;
	std::map<std::string, bool> notExcludedMap;
	std::map<std::string, int32_t> workerContribMap;
	std::map<std::string, JsonBuilderObject> machineJsonMap;

	for (auto worker : workers){
		locality[worker.first.address()] = worker.first.locality;
		if (worker.first.locality.dcId().present())
			dcIds[worker.first.address()] = worker.first.locality.dcId().get().printable();
	}

	for(auto it = mMetrics.begin(); it != mMetrics.end(); it++) {

		if (!it->second.size()){
			continue;
		}

		JsonBuilderObject statusObj;  // Represents the status for a machine
		const TraceEventFields& event = it->second;

		try {
			std::string address = toIPString(it->first.ip);
			// We will use the "physical" caluculated machine ID here to limit exposure to machineID repurposing
			std::string machineId = event.getValue("MachineID");

			// If this machine ID does not already exist in the machineMap, add it
			if (machineJsonMap.count(machineId) == 0) {
				statusObj["machine_id"] = machineId;

				if (dcIds.count(it->first)){
					statusObj["datacenter_id"] = dcIds[it->first];
				}

				if(locality.count(it->first)) {
					statusObj["locality"] = getLocalityInfo(locality[it->first]);
				}

				statusObj["address"] = address;

				JsonBuilderObject memoryObj;
				memoryObj.setKeyRawNumber("total_bytes", event.getValue("TotalMemory"));
				memoryObj.setKeyRawNumber("committed_bytes", event.getValue("CommittedMemory"));
				memoryObj.setKeyRawNumber("free_bytes", event.getValue("AvailableMemory"));
				statusObj["memory"] = memoryObj;

				JsonBuilderObject cpuObj;
				double cpu_seconds = parseDouble(event.getValue("CPUSeconds"));
				double elapsed = parseDouble(event.getValue("Elapsed"));
				if (elapsed > 0){
					cpuObj["logical_core_utilization"] = std::max(0.0, std::min(cpu_seconds / elapsed, 1.0));
				}
				statusObj["cpu"] = cpuObj;

				JsonBuilderObject networkObj;
				networkObj["megabits_sent"] = JsonBuilderObject().setKeyRawNumber("hz", event.getValue("MbpsSent"));
				networkObj["megabits_received"] = JsonBuilderObject().setKeyRawNumber("hz", event.getValue("MbpsReceived"));

				metric = parseDouble(event.getValue("RetransSegs"));
				JsonBuilderObject retransSegsObj;
				if (elapsed > 0){
					retransSegsObj["hz"] = metric / elapsed;
				}
				networkObj["tcp_segments_retransmitted"] = retransSegsObj;
				statusObj["network"] = networkObj;

				if (configuration.present()){
					notExcludedMap[machineId] = true; // Will be set to false below if this or any later process is not excluded
				}

				workerContribMap[machineId] = 0;
				machineJsonMap[machineId] = statusObj;
			}

			if (configuration.present() && !configuration.get().isExcludedServer(it->first))
				notExcludedMap[machineId] = false;
			workerContribMap[machineId] ++;
		}
		catch (Error& e) {
			++failed;
		}
	}

	// Add the status json for each machine with tracked values
	for (auto& mapPair : machineJsonMap) {
		auto& machineId = mapPair.first;
		auto& jsonItem = machineJsonMap[machineId];
		jsonItem["excluded"] = notExcludedMap[machineId];
		jsonItem["contributing_workers"] = workerContribMap[machineId];
		machineMap[machineId] = jsonItem;
	}

	if(failed > 0)
		incomplete_reasons->insert("Cannot retrieve all machine status information.");

	return machineMap;
}

struct MachineMemoryInfo {
	double memoryUsage;
	double numProcesses;

	MachineMemoryInfo() : memoryUsage(0), numProcesses(0) {}

	bool valid() { return memoryUsage >= 0; }
	void invalidate() { memoryUsage = -1; }
};

struct RolesInfo {
	std::multimap<NetworkAddress, JsonBuilderObject> roles;
	JsonBuilderObject& addRole( NetworkAddress address, std::string const& role, UID id) {
		JsonBuilderObject obj;
		obj["id"] = id.shortString();
		obj["role"] = role;
		return roles.insert( std::make_pair(address, obj ))->second;
	}
	JsonBuilderObject& addRole(std::string const& role, StorageServerInterface& iface, TraceEventFields const& metrics, Version maxTLogVersion, double* pDataLagSeconds) {
		JsonBuilderObject obj;
		double dataLagSeconds = -1.0;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			obj.setKeyRawNumber("stored_bytes", metrics.getValue("BytesStored"));
			obj.setKeyRawNumber("kvstore_used_bytes", metrics.getValue("KvstoreBytesUsed"));
			obj.setKeyRawNumber("kvstore_free_bytes", metrics.getValue("KvstoreBytesFree"));
			obj.setKeyRawNumber("kvstore_available_bytes", metrics.getValue("KvstoreBytesAvailable"));
			obj.setKeyRawNumber("kvstore_total_bytes", metrics.getValue("KvstoreBytesTotal"));
			obj["input_bytes"] = StatusCounter(metrics.getValue("BytesInput")).getStatus();
			obj["durable_bytes"] = StatusCounter(metrics.getValue("BytesDurable")).getStatus();
			obj.setKeyRawNumber("query_queue_max", metrics.getValue("QueryQueueMax"));
			obj["total_queries"] = StatusCounter(metrics.getValue("QueryQueue")).getStatus();
			obj["finished_queries"] = StatusCounter(metrics.getValue("FinishedQueries")).getStatus();
			obj["bytes_queried"] = StatusCounter(metrics.getValue("BytesQueried")).getStatus();
			obj["keys_queried"] = StatusCounter(metrics.getValue("RowsQueried")).getStatus();
			obj["mutation_bytes"] = StatusCounter(metrics.getValue("MutationBytes")).getStatus();
			obj["mutations"] = StatusCounter(metrics.getValue("Mutations")).getStatus();

			Version version = parseInt64(metrics.getValue("Version"));
			Version durableVersion = parseInt64(metrics.getValue("DurableVersion"));

			obj["data_version"] = version;
			obj["durable_version"] = durableVersion;

			int64_t versionLag = parseInt64(metrics.getValue("VersionLag"));
			if(maxTLogVersion > 0) {
				// It's possible that the storage server hasn't talked to the logs recently, in which case it may not be aware of how far behind it is.
				// To account for that, we also compute the version difference between each storage server and the tlog with the largest version.
				//
				// Because this data is only logged periodically, this difference will likely be an overestimate for the lag. We subtract off the logging interval
				// in order to make this estimate a bounded underestimate instead.
				versionLag = std::max<int64_t>(versionLag, maxTLogVersion - version - SERVER_KNOBS->STORAGE_LOGGING_DELAY * SERVER_KNOBS->VERSIONS_PER_SECOND);
			}

			JsonBuilderObject dataLag;
			dataLag["versions"] = versionLag;
			dataLagSeconds = versionLag / (double)SERVER_KNOBS->VERSIONS_PER_SECOND;
			dataLag["seconds"] = dataLagSeconds;

			JsonBuilderObject durabilityLag;
			durabilityLag["versions"] = version - durableVersion;
			durabilityLag["seconds"] = (version - durableVersion) / (double)SERVER_KNOBS->VERSIONS_PER_SECOND;

			obj["data_lag"] = dataLag;
			obj["durability_lag"] = durabilityLag;

		} catch (Error& e) {
			if(e.code() != error_code_attribute_not_found)
				throw e;
		}
		if (pDataLagSeconds)
			*pDataLagSeconds = dataLagSeconds;
		return roles.insert( std::make_pair(iface.address(), obj ))->second;
	}
	JsonBuilderObject& addRole(std::string const& role, TLogInterface& iface, TraceEventFields const& metrics, Version* pMetricVersion) {
		JsonBuilderObject obj;
		Version	metricVersion = 0;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			obj.setKeyRawNumber("kvstore_used_bytes",metrics.getValue("KvstoreBytesUsed"));
			obj.setKeyRawNumber("kvstore_free_bytes",metrics.getValue("KvstoreBytesFree"));
			obj.setKeyRawNumber("kvstore_available_bytes",metrics.getValue("KvstoreBytesAvailable"));
			obj.setKeyRawNumber("kvstore_total_bytes",metrics.getValue("KvstoreBytesTotal"));
			obj.setKeyRawNumber("queue_disk_used_bytes",metrics.getValue("QueueDiskBytesUsed"));
			obj.setKeyRawNumber("queue_disk_free_bytes",metrics.getValue("QueueDiskBytesFree"));
			obj.setKeyRawNumber("queue_disk_available_bytes",metrics.getValue("QueueDiskBytesAvailable"));
			obj.setKeyRawNumber("queue_disk_total_bytes",metrics.getValue("QueueDiskBytesTotal"));
			obj["input_bytes"] = StatusCounter(metrics.getValue("BytesInput")).getStatus();
			obj["durable_bytes"] = StatusCounter(metrics.getValue("BytesDurable")).getStatus();
			metricVersion = parseInt64(metrics.getValue("Version"));
			obj["data_version"] = metricVersion;
		} catch (Error& e) {
			if(e.code() != error_code_attribute_not_found)
				throw e;
		}
		if (pMetricVersion)
			*pMetricVersion = metricVersion;
		return roles.insert( std::make_pair(iface.address(), obj ))->second;
	}
	template <class InterfaceType>
	JsonBuilderObject& addRole(std::string const& role, InterfaceType& iface) {
		return addRole(iface.address(), role, iface.id());
	}
	JsonBuilderArray getStatusForAddress( NetworkAddress a ) {
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
		Reference<AsyncVar<struct ServerDBInfo>> db,
		std::vector<std::pair<WorkerInterface, ProcessClass>> workers,
		WorkerEvents pMetrics,
		WorkerEvents mMetrics,
		WorkerEvents errors,
		WorkerEvents traceFileOpenErrors,
		WorkerEvents programStarts,
		std::map<std::string, JsonBuilderObject> processIssues,
		vector<std::pair<StorageServerInterface, TraceEventFields>> storageServers,
		vector<std::pair<TLogInterface, TraceEventFields>> tLogs,
		Database cx,
		Optional<DatabaseConfiguration> configuration,
		std::set<std::string> *incomplete_reasons) {

	state JsonBuilderObject processMap;
	state double metric;

	// construct a map from a process address to a status object containing a trace file open error
	// this is later added to the messages subsection
	state std::map<std::string, JsonBuilderObject> tracefileOpenErrorMap;
	state WorkerEvents::iterator traceFileErrorsItr;
	for(traceFileErrorsItr = traceFileOpenErrors.begin(); traceFileErrorsItr != traceFileOpenErrors.end(); ++traceFileErrorsItr) {
		wait(yield());
		if (traceFileErrorsItr->second.size()){
			try {
				// Have event fields, parse it and turn it into a message object describing the trace file opening error
				const TraceEventFields& event = traceFileErrorsItr->second;
				std::string fileName = event.getValue("Filename");
				JsonBuilderObject msgObj = JsonString::makeMessage("file_open_error", format("Could not open file '%s' (%s).", fileName.c_str(), event.getValue("Error").c_str()).c_str());
				msgObj["file_name"] = fileName;

				// Map the address of the worker to the error message object
				tracefileOpenErrorMap[traceFileErrorsItr->first.toString()] = msgObj;
			}
			catch(Error &e) {
				incomplete_reasons->insert("file_open_error details could not be retrieved");
			}
		}
	}

	state std::map<Optional<Standalone<StringRef>>, MachineMemoryInfo> machineMemoryUsage;
	state std::vector<std::pair<WorkerInterface, ProcessClass>>::iterator workerItr;
	for(workerItr = workers.begin(); workerItr != workers.end(); ++workerItr) {
		wait(yield());
		state std::map<Optional<Standalone<StringRef>>, MachineMemoryInfo>::iterator memInfo = machineMemoryUsage.insert(std::make_pair(workerItr->first.locality.machineId(), MachineMemoryInfo())).first;
		try {
			ASSERT(pMetrics.count(workerItr->first.address()));
			const TraceEventFields& processMetrics = pMetrics[workerItr->first.address()];

			if(memInfo->second.valid()) {
				if(processMetrics.size() > 0) {
					memInfo->second.memoryUsage += parseDouble(processMetrics.getValue("Memory"));
					++memInfo->second.numProcesses;
				}
				else
					memInfo->second.invalidate();
			}
		}
		catch(Error &e) {
			memInfo->second.invalidate();
		}
	}

	state RolesInfo roles;

	roles.addRole("master", db->get().master);
	roles.addRole("cluster_controller", db->get().clusterInterface.clientInterface);

	state Reference<ProxyInfo> proxies = cx->getMasterProxies();
	if (proxies) {
		state int proxyIndex;
		for(proxyIndex = 0; proxyIndex < proxies->size(); proxyIndex++) {
			roles.addRole( "proxy", proxies->getInterface(proxyIndex) );
			wait(yield());
		}
	}

	state std::vector<std::pair<TLogInterface, TraceEventFields>>::iterator log;
	state Version maxTLogVersion = 0;

	// Get largest TLog version
	for(log = tLogs.begin(); log != tLogs.end(); ++log) {
		Version tLogVersion = 0;
		roles.addRole( "log", log->first, log->second, &tLogVersion );
		maxTLogVersion = std::max(maxTLogVersion, tLogVersion);
		wait(yield());
	}

	state std::vector<std::pair<StorageServerInterface, TraceEventFields>>::iterator ss;
	state std::map<NetworkAddress, double> ssLag;
	state double lagSeconds;
	for(ss = storageServers.begin(); ss != storageServers.end(); ++ss) {
		roles.addRole( "storage", ss->first, ss->second, maxTLogVersion, &lagSeconds );
		if (lagSeconds != -1.0) {
			ssLag[ss->first.address()] = lagSeconds;
		}
		wait(yield());
	}

	state std::vector<ResolverInterface>::const_iterator res;
	state std::vector<ResolverInterface> resolvers = db->get().resolvers;
	for(res = resolvers.begin(); res != resolvers.end(); ++res) {
		roles.addRole( "resolver", *res );
		wait(yield());
	}

	for(workerItr = workers.begin(); workerItr != workers.end(); ++workerItr) {
		wait(yield());
		state JsonBuilderObject statusObj;
		try {
			ASSERT(pMetrics.count(workerItr->first.address()));

			NetworkAddress address = workerItr->first.address();
			const TraceEventFields& event = pMetrics[workerItr->first.address()];
			statusObj["address"] = address.toString();
			JsonBuilderObject memoryObj;

			if (event.size() > 0) {
				std::string zoneID = event.getValue("ZoneID");
				statusObj["fault_domain"] = zoneID;

				std::string MachineID = event.getValue("MachineID");
				statusObj["machine_id"] = MachineID;

				statusObj["locality"] = getLocalityInfo(workerItr->first.locality);

				statusObj.setKeyRawNumber("uptime_seconds",event.getValue("UptimeSeconds"));

				// rates are calculated over the last elapsed seconds
				double elapsed = parseDouble(event.getValue("Elapsed"));;
				double cpu_seconds = parseDouble(event.getValue("CPUSeconds"));
				double diskIdleSeconds = parseDouble(event.getValue("DiskIdleSeconds"));
				double diskReads = parseDouble(event.getValue("DiskReads"));
				double diskWrites = parseDouble(event.getValue("DiskWrites"));

				JsonBuilderObject diskObj;
				if (elapsed > 0){
					JsonBuilderObject cpuObj;
					cpuObj["usage_cores"] = std::max(0.0, cpu_seconds / elapsed);
					statusObj["cpu"] = cpuObj;

					diskObj["busy"] = std::max(0.0, std::min((elapsed - diskIdleSeconds) / elapsed, 1.0));

					JsonBuilderObject readsObj;
					readsObj.setKeyRawNumber("counter",event.getValue("DiskReadsCount"));
					if (elapsed > 0)
						readsObj["hz"] = diskReads / elapsed;
					readsObj.setKeyRawNumber("sectors",event.getValue("DiskReadSectors"));

					JsonBuilderObject writesObj;
					writesObj.setKeyRawNumber("counter",event.getValue("DiskWritesCount"));
					if (elapsed > 0)
						writesObj["hz"] = diskWrites / elapsed;
					writesObj.setKeyRawNumber("sectors",event.getValue("DiskWriteSectors"));

					diskObj["reads"] = readsObj;
					diskObj["writes"] = writesObj;
				}

				diskObj.setKeyRawNumber("total_bytes",event.getValue("DiskTotalBytes"));
				diskObj.setKeyRawNumber("free_bytes",event.getValue("DiskFreeBytes"));
				statusObj["disk"] = diskObj;

				JsonBuilderObject networkObj;

				networkObj.setKeyRawNumber("current_connections",event.getValue("CurrentConnections"));
				JsonBuilderObject connections_established;
				connections_established.setKeyRawNumber("hz",event.getValue("ConnectionsEstablished"));
				networkObj["connections_established"] = connections_established;
				JsonBuilderObject connections_closed;
				connections_closed.setKeyRawNumber("hz",event.getValue("ConnectionsClosed"));
				networkObj["connections_closed"] = connections_closed;
				JsonBuilderObject connection_errors;
				connection_errors.setKeyRawNumber("hz",event.getValue("ConnectionErrors"));
				networkObj["connection_errors"] = connection_errors;

				JsonBuilderObject megabits_sent;
				megabits_sent.setKeyRawNumber("hz",event.getValue("MbpsSent"));
				networkObj["megabits_sent"] = megabits_sent;

				JsonBuilderObject megabits_received;
				megabits_received.setKeyRawNumber("hz",event.getValue("MbpsReceived"));
				networkObj["megabits_received"] = megabits_received;

				statusObj["network"] = networkObj;

				memoryObj.setKeyRawNumber("used_bytes",event.getValue("Memory"));
				memoryObj.setKeyRawNumber("unused_allocated_memory",event.getValue("UnusedAllocatedMemory"));
			}

			if (programStarts.count(address)) {
				auto const& psxml = programStarts.at(address);

				if(psxml.size() > 0) {
					memoryObj.setKeyRawNumber("limit_bytes",psxml.getValue("MemoryLimit"));

					std::string version;
					if (psxml.tryGetValue("Version", version)) {
						statusObj["version"] = version;
					}

					std::string commandLine;
					if (psxml.tryGetValue("CommandLine", commandLine)) {
						statusObj["command_line"] = commandLine;
					}
				}
			}

			// if this process address is in the machine metrics
			if (mMetrics.count(address) && mMetrics[address].size()){
				double availableMemory;
				availableMemory = parseDouble(mMetrics[address].getValue("AvailableMemory"));

				auto machineMemInfo = machineMemoryUsage[workerItr->first.locality.machineId()];
				if (machineMemInfo.valid()) {
					ASSERT(machineMemInfo.numProcesses > 0);
					int64_t memory = (availableMemory + machineMemInfo.memoryUsage) / machineMemInfo.numProcesses;
					memoryObj["available_bytes"] = std::max<int64_t>(memory, 0);
				}
			}

			statusObj["memory"] = memoryObj;

			JsonBuilderArray messages;

			if (errors.count(address) && errors[address].size()) {
				// returns status object with type and time of error
				messages.push_back(getError(errors.at(address)));
			}

			// string of address used so that other fields of a NetworkAddress are not compared
			std::string strAddress = address.toString();

			// If this process has a process issue, identified by strAddress, then add it to messages array
			if (processIssues.count(strAddress)){
				messages.push_back(processIssues[strAddress]);
			}

			// If this process had a trace file open error, identified by strAddress, then add it to messages array
			if (tracefileOpenErrorMap.count(strAddress)){
				messages.push_back(tracefileOpenErrorMap[strAddress]);
			}

			if(ssLag[address] >= 60) {
				messages.push_back(JsonString::makeMessage("storage_server_lagging", format("Storage server lagging by %ld seconds.", (int64_t)ssLag[address]).c_str()));
			}

			// Store the message array into the status object that represents the worker process
			statusObj["messages"] = messages;

			// Get roles for the worker's address as an array of objects
			statusObj["roles"] = roles.getStatusForAddress(address);

			if (configuration.present()){
				statusObj["excluded"] = configuration.get().isExcludedServer(address);
			}

			statusObj["class_type"] = workerItr->second.toString();
			statusObj["class_source"] = workerItr->second.sourceString();

		}
		catch (Error& e){
			// Something strange occurred, process list is incomplete but what was built so far, if anything, will be returned.
			incomplete_reasons->insert("Cannot retrieve all process status information.");
		}

		processMap[printable(workerItr->first.locality.processId())] = statusObj;
	}
	return processMap;
}

static JsonBuilderObject clientStatusFetcher(ClientVersionMap clientVersionMap, std::map<NetworkAddress, std::string> traceLogGroupMap) {
	JsonBuilderObject clientStatus;

	clientStatus["count"] = (int64_t)clientVersionMap.size();

	std::map<ClientVersionRef, std::set<NetworkAddress>> clientVersions;
	for(auto client : clientVersionMap) {
		for(auto ver : client.second) {
			clientVersions[ver].insert(client.first);
		}
	}

	JsonBuilderArray versionsArray = JsonBuilderArray();
	for(auto cv : clientVersions) {
		JsonBuilderObject ver;
		ver["count"] = (int64_t)cv.second.size();
		ver["client_version"] = cv.first.clientVersion.toString();
		ver["protocol_version"] = cv.first.protocolVersion.toString();
		ver["source_version"] = cv.first.sourceVersion.toString();

		JsonBuilderArray clients = JsonBuilderArray();
		for(auto client : cv.second) {
			JsonBuilderObject cli;
			cli["address"] = client.toString();
			cli["log_group"] = traceLogGroupMap[client];
			clients.push_back(cli);
		}

		ver["connected_clients"] = clients;
		versionsArray.push_back(ver);
	}

	if(versionsArray.size() > 0) {
		clientStatus["supported_versions"] = versionsArray;
	}

	return clientStatus;
}

ACTOR static Future<JsonBuilderObject> recoveryStateStatusFetcher(std::pair<WorkerInterface, ProcessClass> mWorker, int workerCount, std::set<std::string> *incomplete_reasons, int* statusCode) {
	state JsonBuilderObject message;

	try {
		TraceEventFields md = wait( timeoutError(mWorker.first.eventLogRequest.getReply( EventLogRequest( LiteralStringRef("MasterRecoveryState") ) ), 1.0) );
		state int mStatusCode = parseInt( md.getValue("StatusCode") );
		if (mStatusCode < 0 || mStatusCode >= RecoveryStatus::END)
			throw attribute_not_found();

		message = JsonString::makeMessage(RecoveryStatus::names[mStatusCode], RecoveryStatus::descriptions[mStatusCode]);
		*statusCode = mStatusCode;

		// Add additional metadata for certain statuses
		if (mStatusCode == RecoveryStatus::recruiting_transaction_servers) {
			int requiredLogs = atoi( md.getValue("RequiredTLogs").c_str() );
			int requiredProxies = atoi( md.getValue("RequiredProxies").c_str() );
			int requiredResolvers = atoi( md.getValue("RequiredResolvers").c_str() );
			//int requiredProcesses = std::max(requiredLogs, std::max(requiredResolvers, requiredProxies));
			//int requiredMachines = std::max(requiredLogs, 1);

			message["required_logs"] = requiredLogs;
			message["required_proxies"] = requiredProxies;
			message["required_resolvers"] = requiredResolvers;
		} else if (mStatusCode == RecoveryStatus::locking_old_transaction_servers) {
			message["missing_logs"] = md.getValue("MissingIDs").c_str();
		}
		// TODO:  time_in_recovery: 0.5
		//        time_in_state: 0.1

	} catch (Error &e){
		if (e.code() == error_code_actor_cancelled)
			throw;
	}

	// If recovery status name is not know, status is incomplete
	if (message.empty()) {
		incomplete_reasons->insert("Recovery Status unavailable.");
	}

	return message;
}

ACTOR static Future<double> doGrvProbe(Transaction *tr, Optional<FDBTransactionOptions::Option> priority = Optional<FDBTransactionOptions::Option>()) {
	state double start = timer_monotonic();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			if(priority.present()) {
				tr->setOption(priority.get());
			}

			Version _ = wait(tr->getReadVersion());
			return timer_monotonic() - start;
		}
		catch(Error &e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<double> doReadProbe(Future<double> grvProbe, Transaction *tr) {
	ErrorOr<double> grv = wait(errorOr(grvProbe));
	if(grv.isError()) {
		throw grv.getError();
	}

	state double start = timer_monotonic();

	loop {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Optional<Standalone<StringRef> > _ = wait(tr->get(LiteralStringRef("\xff/StatusJsonTestKey62793")));
			return timer_monotonic() - start;
		}
		catch(Error &e) {
			wait(tr->onError(e));
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}
	}
}

ACTOR static Future<double> doCommitProbe(Future<double> grvProbe, Transaction *sourceTr, Transaction *tr) {
	ErrorOr<double> grv = wait(errorOr(grvProbe));
	if(grv.isError()) {
		throw grv.getError();
	}

	ASSERT(sourceTr->getReadVersion().isReady());
	tr->setVersion(sourceTr->getReadVersion().get());

	state double start = timer_monotonic();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->makeSelfConflicting();
			wait(tr->commit());
			return timer_monotonic() - start;
		}
		catch(Error &e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> doProbe(Future<double> probe, int timeoutSeconds, const char* prefix, const char* description, JsonBuilderObject *probeObj, JsonBuilderArray *messages, std::set<std::string> *incomplete_reasons, bool *isAvailable = nullptr) {
	choose {
		when(ErrorOr<double> result = wait(errorOr(probe))) {
			if(result.isError()) {
				if(isAvailable != nullptr) {
					*isAvailable = false;
				}
				incomplete_reasons->insert(format("Unable to retrieve latency probe information (%s: %s).", description, result.getError().what()));
			}
			else {
				(*probeObj)[format("%s_seconds", prefix).c_str()] = result.get();
			}
		}
		when(wait(delay(timeoutSeconds))) {
			if(isAvailable != nullptr) {
				*isAvailable = false;
			}
			messages->push_back(JsonString::makeMessage(format("%s_probe_timeout", prefix).c_str(), format("Unable to %s after %d seconds.", description, timeoutSeconds).c_str()));
		}
	}

	return Void();
}

ACTOR static Future<JsonBuilderObject> latencyProbeFetcher(Database cx, JsonBuilderArray *messages, std::set<std::string> *incomplete_reasons, bool *isAvailable) {
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
		probes.push_back(doProbe(immediateGrvProbe, timeoutSeconds, "immediate_priority_transaction_start", "start immediate priority transaction", &statusObj, messages, incomplete_reasons, isAvailable));
		probes.push_back(doProbe(defaultGrvProbe, timeoutSeconds, "transaction_start", "start default priority transaction", &statusObj, messages, incomplete_reasons));
		probes.push_back(doProbe(batchGrvProbe, timeoutSeconds, "batch_priority_transaction_start", "start batch priority transaction", &statusObj, messages, incomplete_reasons));
		probes.push_back(doProbe(readProbe, timeoutSeconds, "read", "read", &statusObj, messages, incomplete_reasons, isAvailable));
		probes.push_back(doProbe(commitProbe, timeoutSeconds, "commit", "commit", &statusObj, messages, incomplete_reasons, isAvailable));

		wait(waitForAll(probes));
	}
	catch (Error &e) {
		incomplete_reasons->insert(format("Unable to retrieve latency probe information (%s).", e.what()));
	}

	return statusObj;
}

ACTOR static Future<std::pair<Optional<DatabaseConfiguration>,Optional<bool>>> loadConfiguration(Database cx, JsonBuilderArray *messages, std::set<std::string> *status_incomplete_reasons){
	state Optional<DatabaseConfiguration> result;
	state Optional<bool> fullReplication;
	state Transaction tr(cx);
	state Future<Void> getConfTimeout = delay(5.0);

	loop{
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::CAUSAL_READ_RISKY);
		try {
			choose{
				when(Standalone<RangeResultRef> res = wait(tr.getRange(configKeys, SERVER_KNOBS->CONFIGURATION_ROWS_TO_FETCH))) {
					DatabaseConfiguration configuration;
					if (res.size() == SERVER_KNOBS->CONFIGURATION_ROWS_TO_FETCH) {
						status_incomplete_reasons->insert("Too many configuration parameters set.");
					}
					else {
						configuration.fromKeyValues((VectorRef<KeyValueRef>)res);
					}

					result = configuration;
				}
				when(wait(getConfTimeout)) {
					if(!result.present()) {
						messages->push_back(JsonString::makeMessage("unreadable_configuration", "Unable to read database configuration."));
					} else {
						messages->push_back(JsonString::makeMessage("full_replication_timeout", "Unable to read datacenter replicas."));
					}
					break;
				}
			}

			ASSERT(result.present());
			state std::vector<Future<Optional<Value>>> replicasFutures;
			for(auto& region : result.get().regions) {
				replicasFutures.push_back(tr.get(datacenterReplicasKeyFor(region.dcId)));
			}

			choose {
				when( wait( waitForAll(replicasFutures) ) ) {
					int unreplicated = 0;
					for(int i = 0; i < result.get().regions.size(); i++) {
						if( !replicasFutures[i].get().present() || decodeDatacenterReplicasValue(replicasFutures[i].get().get()) < result.get().storageTeamSize ) {
							unreplicated++;
						}
					}

					fullReplication = (!unreplicated || (result.get().usableRegions == 1 && unreplicated < result.get().regions.size()));
				}
				when(wait(getConfTimeout)) {
					messages->push_back(JsonString::makeMessage("full_replication_timeout", "Unable to read datacenter replicas."));
				}
			}
			break;
		}
		catch (Error &e) {
			wait(tr.onError(e));
		}
	}
	return std::make_pair(result, fullReplication);
}

static JsonBuilderObject configurationFetcher(Optional<DatabaseConfiguration> conf, ServerCoordinators coordinators, std::set<std::string> *incomplete_reasons) {
	JsonBuilderObject statusObj;
	try {
		if(conf.present()) {
			DatabaseConfiguration configuration = conf.get();
			statusObj.addContents(configuration.toJSON());

			JsonBuilderArray excludedServersArr;
			std::set<AddressExclusion> excludedServers = configuration.getExcludedServers();
			for (std::set<AddressExclusion>::iterator it = excludedServers.begin(); it != excludedServers.end(); it++) {
				JsonBuilderObject statusObj;
				statusObj["address"] = it->toString();
				excludedServersArr.push_back(statusObj);
			}
			statusObj["excluded_servers"] = excludedServersArr;
		}
		vector< ClientLeaderRegInterface > coordinatorLeaderServers = coordinators.clientLeaderServers;
		int count = coordinatorLeaderServers.size();
		statusObj["coordinators_count"] = count;
	}
	catch (Error &e){
		incomplete_reasons->insert("Could not retrieve all configuration status information.");
	}
	return statusObj;
}

ACTOR static Future<JsonBuilderObject> dataStatusFetcher(std::pair<WorkerInterface, ProcessClass> mWorker, int *minReplicasRemaining) {
	state JsonBuilderObject statusObjData;

	try {
		std::vector<Future<TraceEventFields>> futures;

		// TODO:  Should this be serial?
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(LiteralStringRef("DDTrackerStarting"))), 1.0));
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(LiteralStringRef("DDTrackerStats"))), 1.0));
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(LiteralStringRef("MovingData"))), 1.0));
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(LiteralStringRef("TotalDataInFlight"))), 1.0));
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(LiteralStringRef("TotalDataInFlightRemote"))), 1.0));

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
		if (md.size())
		{
			int64_t partitionsInQueue = parseInt64(md.getValue("InQueue"));
			int64_t partitionsInFlight = parseInt64(md.getValue("InFlight"));
			int64_t averagePartitionSize = parseInt64(md.getValue("AverageShardSize"));

			if( averagePartitionSize >= 0 ) {
				JsonBuilderObject moving_data;
				moving_data["in_queue_bytes"] = partitionsInQueue * averagePartitionSize;
				moving_data["in_flight_bytes"] = partitionsInFlight * averagePartitionSize;
				moving_data.setKeyRawNumber("total_written_bytes",md.getValue("BytesWritten"));
				moving_data.setKeyRawNumber("highest_priority",md.getValue("HighestPriority"));

				// TODO: moving_data["rate_bytes"] = makeCounter(hz, c, r);
				statusObjData["moving_data"] = moving_data;
				statusObjData["average_partition_size_bytes"] = averagePartitionSize;
			}
		}

		if (dataStats.size())
		{
			statusObjData.setKeyRawNumber("total_kv_size_bytes",dataStats.getValue("TotalSizeBytes"));
			statusObjData.setKeyRawNumber("partitions_count",dataStats.getValue("Shards"));
		}

		JsonBuilderArray teamTrackers;
		for(int i = 0; i < 2; i++) {
			TraceEventFields inFlight = dataInfo[3 + i];
			if (!inFlight.size()) {
				continue;
			}

			bool primary = parseInt(inFlight.getValue("Primary"));
			int highestPriority = parseInt(inFlight.getValue("HighestPriority"));

			JsonBuilderObject team_tracker;
			team_tracker["primary"] = primary;
			team_tracker.setKeyRawNumber("in_flight_bytes",inFlight.getValue("TotalBytes"));
			team_tracker.setKeyRawNumber("unhealthy_servers",inFlight.getValue("UnhealthyServers"));

			JsonBuilderObject stateSectionObj;
			if (highestPriority >= PRIORITY_TEAM_0_LEFT) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "missing_data";
				stateSectionObj["description"] = "No replicas remain of some data";
				stateSectionObj["min_replicas_remaining"] = 0;
				if(primary) {
					*minReplicasRemaining = 0;
				}
			}
			else if (highestPriority >= PRIORITY_TEAM_1_LEFT) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "healing";
				stateSectionObj["description"] = "Only one replica remains of some data";
				stateSectionObj["min_replicas_remaining"] = 1;
				if(primary) {
					*minReplicasRemaining = 1;
				}
			}
			else if (highestPriority >= PRIORITY_TEAM_2_LEFT) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "healing";
				stateSectionObj["description"] = "Only two replicas remain of some data";
				stateSectionObj["min_replicas_remaining"] = 2;
				if(primary) {
					*minReplicasRemaining = 2;
				}
			}
			else if (highestPriority >= PRIORITY_TEAM_UNHEALTHY) {
				stateSectionObj["healthy"] = false;
				stateSectionObj["name"] = "healing";
				stateSectionObj["description"] = "Restoring replication factor";
			}
			else if (highestPriority >= PRIORITY_MERGE_SHARD) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_repartitioning";
				stateSectionObj["description"] = "Repartitioning.";
			}
			else if (highestPriority >= PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_removing_server";
				stateSectionObj["description"] = "Removing storage server";
			}
			else if (highestPriority >= PRIORITY_REBALANCE_SHARD) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy_rebalancing";
				stateSectionObj["description"] = "Rebalancing";
			}
			else if (highestPriority >= 0) {
				stateSectionObj["healthy"] = true;
				stateSectionObj["name"] = "healthy";
			}

			if(!stateSectionObj.empty()) {
				team_tracker["state"] = stateSectionObj;
				teamTrackers.push_back(team_tracker);
				if(primary) {
					statusObjData["state"] = stateSectionObj;
				}
			}
		}
		statusObjData["team_trackers"] = teamTrackers;
	}
	catch (Error &e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		// The most likely reason to be here is a timeout, either way we have no idea if the data state is healthy or not
		// from the "cluster" perspective - from the client perspective it is not but that is indicated elsewhere.
	}

	return statusObjData;
}

namespace std
{
	template <>
	struct hash<NetworkAddress>
	{
		size_t operator()(const NetworkAddress& na) const
		{
			return (na.ip << 16) + na.port;
		}
	};
}

ACTOR template <class iface>
static Future<vector<std::pair<iface, TraceEventFields>>> getServerMetrics(vector<iface> servers, std::unordered_map<NetworkAddress, WorkerInterface> address_workers, std::string suffix) {
	state vector<Future<Optional<TraceEventFields>>> futures;
	for (auto s : servers) {
		futures.push_back(latestEventOnWorker(address_workers[s.address()], s.id().toString() + suffix));
	}

	wait(waitForAll(futures));

	vector<std::pair<iface, TraceEventFields>> results;
	for (int i = 0; i < servers.size(); i++) {
		results.push_back(std::make_pair(servers[i], futures[i].get().present() ? futures[i].get().get() : TraceEventFields()));
	}
	return results;
}

ACTOR static Future<vector<std::pair<StorageServerInterface, TraceEventFields>>> getStorageServersAndMetrics(Database cx, std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	vector<StorageServerInterface> servers = wait(timeoutError(getStorageServers(cx, true), 5.0));
	vector<std::pair<StorageServerInterface, TraceEventFields>> results = wait(getServerMetrics(servers, address_workers, "/StorageMetrics"));
	return results;
}

ACTOR static Future<vector<std::pair<TLogInterface, TraceEventFields>>> getTLogsAndMetrics(Reference<AsyncVar<struct ServerDBInfo>> db, std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	vector<TLogInterface> servers = db->get().logSystemConfig.allPresentLogs();
	vector<std::pair<TLogInterface, TraceEventFields>> results = wait(getServerMetrics(servers, address_workers, "/TLogMetrics"));
	return results;
}

static int getExtraTLogEligibleMachines(vector<std::pair<WorkerInterface, ProcessClass>> workers, DatabaseConfiguration configuration) {
	std::set<StringRef> allMachines;
	std::map<Key,std::set<StringRef>> dcId_machine;
	for(auto worker : workers) {
		if(worker.second.machineClassFitness(ProcessClass::TLog) < ProcessClass::NeverAssign
			&& !configuration.isExcludedServer(worker.first.address()))
		{
			allMachines.insert(worker.first.locality.zoneId().get());
			if(worker.first.locality.dcId().present()) {
				dcId_machine[worker.first.locality.dcId().get()].insert(worker.first.locality.zoneId().get());
			}
		}
	}

	if(configuration.regions.size() == 0) {
		return allMachines.size() - std::max(configuration.tLogReplicationFactor, configuration.storageTeamSize);
	}
	int extraTlogEligibleMachines = configuration.usableRegions == 1 ? 0 : std::numeric_limits<int>::max();
	for(auto& region : configuration.regions) {
		int eligible = dcId_machine[region.dcId].size() - std::max(configuration.remoteTLogReplicationFactor, std::max(configuration.tLogReplicationFactor, configuration.storageTeamSize) );
		//FIXME: does not take into account fallback satellite policies
		if(region.satelliteTLogReplicationFactor > 0) {
			int totalSatelliteEligible = 0;
			for(auto& sat : region.satellites) {
				totalSatelliteEligible += dcId_machine[sat.dcId].size();
			}
			eligible = std::min<int>( eligible, totalSatelliteEligible - region.satelliteTLogReplicationFactor );
		}
		if( configuration.usableRegions == 1 ) {
			if( region.priority >= 0 ) {
				extraTlogEligibleMachines = std::max( extraTlogEligibleMachines, eligible );
			}
		} else {
			extraTlogEligibleMachines = std::min( extraTlogEligibleMachines, eligible );
		}
	}
	return extraTlogEligibleMachines;
}

ACTOR static Future<JsonBuilderObject> workloadStatusFetcher(Reference<AsyncVar<struct ServerDBInfo>> db, vector<std::pair<WorkerInterface, ProcessClass>> workers, std::pair<WorkerInterface, ProcessClass> mWorker,
	JsonBuilderObject *qos, JsonBuilderObject *data_overlay, std::set<std::string> *incomplete_reasons, Future<ErrorOr<vector<std::pair<StorageServerInterface, TraceEventFields>>>> storageServerFuture)
{
	state JsonBuilderObject statusObj;
	state JsonBuilderObject operationsObj;
	state JsonBuilderObject bytesObj;
	state JsonBuilderObject keysObj;

	// Writes and conflicts
	try {
		vector<Future<TraceEventFields>> proxyStatFutures;
		std::map<NetworkAddress, std::pair<WorkerInterface, ProcessClass>> workersMap;
		for (auto w : workers) {
			workersMap[w.first.address()] = w;
		}
		for (auto &p : db->get().client.proxies) {
			auto worker = getWorker(workersMap, p.address());
			if (worker.present())
				proxyStatFutures.push_back(timeoutError(worker.get().first.eventLogRequest.getReply(EventLogRequest(LiteralStringRef("ProxyMetrics"))), 1.0));
			else
				throw all_alternatives_failed();  // We need data from all proxies for this result to be trustworthy
		}
		vector<TraceEventFields> proxyStats = wait(getAll(proxyStatFutures));

		StatusCounter mutations, mutationBytes, txnConflicts, txnStartOut, txnCommitOutSuccess;

		for (auto &ps : proxyStats) {
			mutations.updateValues( StatusCounter(ps.getValue("Mutations")) );
			mutationBytes.updateValues( StatusCounter(ps.getValue("MutationBytes")) );
			txnConflicts.updateValues( StatusCounter(ps.getValue("TxnConflicts")) );
			txnStartOut.updateValues( StatusCounter(ps.getValue("TxnStartOut")) );
			txnCommitOutSuccess.updateValues( StatusCounter(ps.getValue("TxnCommitOutSuccess")) );
		}

		operationsObj["writes"] = mutations.getStatus();
		bytesObj["written"] = mutationBytes.getStatus();

		JsonBuilderObject transactions;
		transactions["conflicted"] = txnConflicts.getStatus();
		transactions["started"] = txnStartOut.getStatus();
		transactions["committed"] = txnCommitOutSuccess.getStatus();

		statusObj["transactions"] = transactions;
	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown mutations, conflicts, and transactions state.");
	}

	// Transactions
	try {
		TraceEventFields md = wait( timeoutError(mWorker.first.eventLogRequest.getReply( EventLogRequest(LiteralStringRef("RkUpdate") ) ), 1.0) );
		double tpsLimit = parseDouble(md.getValue("TPSLimit"));
		double transPerSec = parseDouble(md.getValue("ReleasedTPS"));
		int ssCount = parseInt(md.getValue("StorageServers"));
		int tlogCount = parseInt(md.getValue("TLogs"));
		int64_t worstFreeSpaceStorageServer = parseInt64(md.getValue("WorstFreeSpaceStorageServer"));
		int64_t worstFreeSpaceTLog = parseInt64(md.getValue("WorstFreeSpaceTLog"));
		(*data_overlay).setKeyRawNumber("total_disk_used_bytes",md.getValue("TotalDiskUsageBytes"));

		if(ssCount > 0) {
			(*data_overlay)["least_operating_space_bytes_storage_server"] = std::max(worstFreeSpaceStorageServer, (int64_t)0);
			(*qos).setKeyRawNumber("worst_queue_bytes_storage_server",md.getValue("WorstStorageServerQueue"));
			(*qos).setKeyRawNumber("limiting_queue_bytes_storage_server",md.getValue("LimitingStorageServerQueue"));
			(*qos).setKeyRawNumber("worst_version_lag_storage_server",md.getValue("WorstStorageServerVersionLag"));
			(*qos).setKeyRawNumber("limiting_version_lag_storage_server",md.getValue("LimitingStorageServerVersionLag"));
		}

		if(tlogCount > 0) {
			(*data_overlay)["least_operating_space_bytes_log_server"] = std::max(worstFreeSpaceTLog, (int64_t)0);
			(*qos).setKeyRawNumber("worst_queue_bytes_log_server",md.getValue("WorstTLogQueue"));
		}

		(*qos)["transactions_per_second_limit"] = tpsLimit;
		(*qos)["released_transactions_per_second"] = transPerSec;

		int reason = parseInt(md.getValue("Reason"));
		JsonBuilderObject perfLimit;
		if (transPerSec > tpsLimit * 0.8) {
			// If reason is known, set qos.performance_limited_by, otherwise omit
			if (reason >= 0 && reason < limitReasonEnd) {
				perfLimit = JsonString::makeMessage(limitReasonName[reason], limitReasonDesc[reason]);
				std::string reason_server_id = md.getValue("ReasonServerID");
				if (!reason_server_id.empty())
					perfLimit["reason_server_id"] = reason_server_id;
			}
		}
		else {
			perfLimit = JsonString::makeMessage("workload", "The database is not being saturated by the workload.");
		}

		if(!perfLimit.empty()) {
			perfLimit["reason_id"] = reason;
			(*qos)["performance_limited_by"] = perfLimit;
		}
	} catch (Error &e){
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown performance state.");
	}

	// Reads
	try {
		ErrorOr<vector<std::pair<StorageServerInterface, TraceEventFields>>> storageServers = wait(storageServerFuture);
		if(!storageServers.present()) {
			throw storageServers.getError();
		}

		StatusCounter readRequests;
		StatusCounter reads;
		StatusCounter readKeys;
		StatusCounter readBytes;

		for(auto &ss : storageServers.get()) {
			readRequests.updateValues( StatusCounter(ss.second.getValue("QueryQueue")));
			reads.updateValues( StatusCounter(ss.second.getValue("FinishedQueries")));
			readKeys.updateValues( StatusCounter(ss.second.getValue("RowsQueried")));
			readBytes.updateValues( StatusCounter(ss.second.getValue("BytesQueried")));
		}

		operationsObj["read_requests"] = readRequests.getStatus();
		operationsObj["reads"] = reads.getStatus();
		keysObj["read"] = readKeys.getStatus();
		bytesObj["read"] = readBytes.getStatus();

	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown read state.");
	}

	statusObj["operations"] = operationsObj;
	statusObj["keys"] = keysObj;
	statusObj["bytes"] = bytesObj;

	return statusObj;
}

static JsonBuilderArray oldTlogFetcher(int* oldLogFaultTolerance, Reference<AsyncVar<struct ServerDBInfo>> db, std::unordered_map<NetworkAddress, WorkerInterface> const& address_workers) {
	JsonBuilderArray oldTlogsArray;

	if(db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
		for(auto it : db->get().logSystemConfig.oldTLogs) {
			JsonBuilderObject statusObj;
			JsonBuilderArray logsObj;
			Optional<int32_t> sat_log_replication_factor, sat_log_write_anti_quorum, sat_log_fault_tolerance, log_replication_factor, log_write_anti_quorum, log_fault_tolerance, remote_log_replication_factor, remote_log_fault_tolerance;

			int maxFaultTolerance = 0;

			for(int i = 0; i < it.tLogs.size(); i++) {
				int failedLogs = 0;
				for(auto& log : it.tLogs[i].tLogs) {
					JsonBuilderObject logObj;
					bool failed = !log.present() || !address_workers.count(log.interf().address());
					logObj["id"] = log.id().shortString();
					logObj["healthy"] = !failed;
					if(log.present()) {
						logObj["address"] = log.interf().address().toString();
					}
					logsObj.push_back(logObj);
					if(failed) {
						failedLogs++;
					}
				}
				maxFaultTolerance = std::max(maxFaultTolerance, it.tLogs[i].tLogReplicationFactor - 1 - it.tLogs[i].tLogWriteAntiQuorum - failedLogs);
				if(it.tLogs[i].isLocal && it.tLogs[i].locality == tagLocalitySatellite) {
					sat_log_replication_factor = it.tLogs[i].tLogReplicationFactor;
					sat_log_write_anti_quorum = it.tLogs[i].tLogWriteAntiQuorum;
					sat_log_fault_tolerance = it.tLogs[i].tLogReplicationFactor - 1 - it.tLogs[i].tLogWriteAntiQuorum - failedLogs;
				}
				else if(it.tLogs[i].isLocal) {
					log_replication_factor = it.tLogs[i].tLogReplicationFactor;
					log_write_anti_quorum = it.tLogs[i].tLogWriteAntiQuorum;
					log_fault_tolerance = it.tLogs[i].tLogReplicationFactor - 1 - it.tLogs[i].tLogWriteAntiQuorum - failedLogs;
				}
				else {
					remote_log_replication_factor = it.tLogs[i].tLogReplicationFactor;
					remote_log_fault_tolerance = it.tLogs[i].tLogReplicationFactor - 1 - failedLogs;
				}
			}
			*oldLogFaultTolerance = std::min(*oldLogFaultTolerance, maxFaultTolerance);
			statusObj["logs"] = logsObj;

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

			oldTlogsArray.push_back(statusObj);
		}
	}

	return oldTlogsArray;
}

static JsonBuilderObject faultToleranceStatusFetcher(DatabaseConfiguration configuration, ServerCoordinators coordinators, std::vector<std::pair<WorkerInterface, ProcessClass>>& workers, int extraTlogEligibleMachines, int minReplicasRemaining) {
	JsonBuilderObject statusObj;

	// without losing data
	int32_t maxMachineFailures = configuration.maxMachineFailuresTolerated();
	int maxCoordinatorFailures = (coordinators.clientLeaderServers.size() - 1) / 2;

	std::map<NetworkAddress, StringRef> workerZones;
	for(auto& worker : workers) {
		workerZones[worker.first.address()] = worker.first.locality.zoneId().orDefault(LiteralStringRef(""));
	}
	std::map<StringRef, int> coordinatorZoneCounts;
	for(auto& coordinator : coordinators.ccf->getConnectionString().coordinators()) {
		auto zone = workerZones[coordinator];
		coordinatorZoneCounts[zone] += 1;
	}
	std::vector<std::pair<StringRef, int>> coordinatorZones(coordinatorZoneCounts.begin(), coordinatorZoneCounts.end());
	std::sort(coordinatorZones.begin(), coordinatorZones.end(), [] (const std::pair<StringRef,int>& lhs, const std::pair<StringRef,int>& rhs) {
		return lhs.second > rhs.second;
	});
	int lostCoordinators = 0;
	int maxCoordinatorZoneFailures = 0;
	for(auto zone : coordinatorZones) {
		lostCoordinators += zone.second;
		if(lostCoordinators > maxCoordinatorFailures) {
			break;
		}
		maxCoordinatorZoneFailures += 1;
	}

	int machineFailuresWithoutLosingData = std::min(maxMachineFailures, maxCoordinatorZoneFailures);

	if (minReplicasRemaining >= 0){
		machineFailuresWithoutLosingData = std::min(machineFailuresWithoutLosingData, minReplicasRemaining - 1);
	}

	statusObj["max_machine_failures_without_losing_data"] = std::max(machineFailuresWithoutLosingData, 0);

	// without losing availablity
	statusObj["max_machine_failures_without_losing_availability"] = std::max(std::min(extraTlogEligibleMachines, machineFailuresWithoutLosingData), 0);
	return statusObj;
}

static std::string getIssueDescription(std::string name) {
	if(name == "incorrect_cluster_file_contents") {
		return "Cluster file contents do not match current cluster connection string. Verify the cluster file and its parent directory are writable and that the cluster file has not been overwritten externally.";
	}

	// FIXME: name and description will be the same unless the message is 'incorrect_cluster_file_contents', which is currently the only possible message
	return name;
}

static std::map<std::string, JsonBuilderObject> getProcessIssuesAsMessages( ProcessIssuesMap const& _issues ) {
	std::map<std::string, JsonBuilderObject> issuesMap;

	try {
		ProcessIssuesMap issues = _issues;
		for (auto i : issues) {
			issuesMap[i.first.toString()] = JsonString::makeMessage(i.second.first.c_str(), getIssueDescription(i.second.first).c_str());
		}
	}
	catch (Error &e) {
		TraceEvent(SevError, "ErrorParsingProcessIssues").error(e);
		// swallow
	}

	return issuesMap;
}

static JsonBuilderArray getClientIssuesAsMessages( ProcessIssuesMap const& _issues) {
	JsonBuilderArray issuesList;

	try {
		ProcessIssuesMap issues = _issues;
		std::map<std::string, std::vector<std::string>> deduplicatedIssues;

		for(auto i : issues) {
			deduplicatedIssues[i.second.first].push_back(format("%s:%d", toIPString(i.first.ip).c_str(), i.first.port));
		}

		for (auto i : deduplicatedIssues) {
			JsonBuilderObject message = JsonString::makeMessage(i.first.c_str(), getIssueDescription(i.first).c_str());
			JsonBuilderArray addresses;
			for(auto addr : i.second) {
				addresses.push_back(addr);
			}

			message["addresses"] = addresses;
			issuesList.push_back(message);
		}
	}
	catch (Error &e) {
		TraceEvent(SevError, "ErrorParsingClientIssues").error(e);
		// swallow
	}

	return issuesList;
}

ACTOR Future<JsonBuilderObject> layerStatusFetcher(Database cx, JsonBuilderArray *messages, std::set<std::string> *incomplete_reasons) {
	state StatusObject result;
	state JSONDoc json(result);
	state double tStart = now();

	try {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				int64_t timeout_ms = 3000;
				tr.setOption(FDBTransactionOptions::TIMEOUT, StringRef((uint8_t *)&timeout_ms, sizeof(int64_t)));

				std::string jsonPrefix = layerStatusMetaPrefixRange.begin.toString() + "json/";
				Standalone<RangeResultRef> jsonLayers = wait(tr.getRange(KeyRangeRef(jsonPrefix, strinc(jsonPrefix)), 1000));
				// TODO:  Also fetch other linked subtrees of meta keys

				state std::vector<Future<Standalone<RangeResultRef>>> docFutures;
				state int i;
				for(i = 0; i < jsonLayers.size(); ++i)
					docFutures.push_back(tr.getRange(KeyRangeRef(jsonLayers[i].value, strinc(jsonLayers[i].value)), 1000));

				result.clear();
				JSONDoc::expires_reference_version = (uint64_t)tr.getReadVersion().get();

				for(i = 0; i < docFutures.size(); ++i) {
					state Standalone<RangeResultRef> docs = wait(docFutures[i]);
					state int j;
					for(j = 0; j < docs.size(); ++j) {
						state json_spirit::mValue doc;
						try {
							json_spirit::read_string(docs[j].value.toString(), doc);
							wait(yield());
							json.absorb(doc.get_obj());
							wait(yield());
						} catch(Error &e) {
							TraceEvent(SevWarn, "LayerStatusBadJSON").detail("Key", printable(docs[j].key));
						}
					}
				}
				json.create("_valid") = true;
				break;
			} catch(Error &e) {
				wait(tr.onError(e));
			}
		}
	} catch(Error &e) {
		TraceEvent(SevWarn, "LayerStatusError").error(e);
		incomplete_reasons->insert(format("Unable to retrieve layer status (%s).", e.what()));
		json.create("_error") = format("Unable to retrieve layer status (%s).", e.what());
		json.create("_valid") = false;
	}

	json.cleanOps();
	JsonBuilderObject statusObj;
	statusObj.addContents(result);
	TraceEvent("LayerStatusFetcher").detail("Duration", now()-tStart).detail("StatusSize",statusObj.getFinalLength());
	return statusObj;
}

ACTOR Future<JsonBuilderObject> lockedStatusFetcher(Reference<AsyncVar<struct ServerDBInfo>> db, JsonBuilderArray *messages, std::set<std::string> *incomplete_reasons) {
	state JsonBuilderObject statusObj;

	state Database cx = openDBOnServer(db, TaskDefaultEndpoint, true, false); // Open a new database connection that isn't lock-aware
	state Transaction tr(cx);
	state int timeoutSeconds = 5;
	state Future<Void> getTimeout = delay(timeoutSeconds);

	loop {
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		try {
			choose{
				when(Version f = wait(tr.getReadVersion())) {
					statusObj["database_locked"] = false;
				}

				when(wait(getTimeout)) {
					incomplete_reasons->insert(format("Unable to determine if database is locked after %d seconds.", timeoutSeconds));
				}
			}
			break;
		}
		catch (Error &e) {
			if (e.code() == error_code_database_locked) {
				statusObj["database_locked"] = true;
				break;
			}
			else {
				try {
					wait(tr.onError(e));
				}
				catch (Error &e) {
					incomplete_reasons->insert(format("Unable to determine if database is locked (%s).", e.what()));
					break;
				}
			}
		}
	}
	return statusObj;
}

// constructs the cluster section of the json status output
ACTOR Future<StatusReply> clusterGetStatus(
		Reference<AsyncVar<struct ServerDBInfo>> db,
		Database cx,
		vector<std::pair<WorkerInterface, ProcessClass>> workers,
		ProcessIssuesMap workerIssues,
		ProcessIssuesMap clientIssues,
		ClientVersionMap clientVersionMap,
		std::map<NetworkAddress, std::string> traceLogGroupMap,
		ServerCoordinators coordinators,
		std::vector<NetworkAddress> incompatibleConnections,
		Version datacenterVersionDifference )
{
	state double tStart = timer();

	// Check if master worker is present
	state JsonBuilderArray messages;
	state std::set<std::string> status_incomplete_reasons;
	state std::pair<WorkerInterface, ProcessClass> mWorker;

	try {
		// Get the master Worker interface
		Optional<std::pair<WorkerInterface, ProcessClass>> _mWorker = getWorker( workers, db->get().master.address() );
		if (_mWorker.present()) {
			mWorker = _mWorker.get();
		} else {
			messages.push_back(JsonString::makeMessage("unreachable_master_worker", "Unable to locate the master worker."));
		}

		// Get latest events for various event types from ALL workers
		// WorkerEvents is a map of worker's NetworkAddress to its event string
		// The pair represents worker responses and a set of worker NetworkAddress strings which did not respond
		std::vector< Future< Optional <std::pair<WorkerEvents, std::set<std::string>>> > > futures;
		futures.push_back(latestEventOnWorkers(workers, "MachineMetrics"));
		futures.push_back(latestEventOnWorkers(workers, "ProcessMetrics"));
		futures.push_back(latestErrorOnWorkers(workers));
		futures.push_back(latestEventOnWorkers(workers, "TraceFileOpenError"));
		futures.push_back(latestEventOnWorkers(workers, "ProgramStart"));

		// Wait for all response pairs.
		state std::vector< Optional <std::pair<WorkerEvents, std::set<std::string>>> > workerEventsVec = wait(getAll(futures));

		// Create a unique set of all workers who were unreachable for 1 or more of the event requests above.
		// Since each event request is independent and to all workers, workers can have responded to some
		// event requests but still end up in the unreachable set.
		std::set<std::string> mergeUnreachable;

		// For each (optional) pair, if the pair is present and not empty then add the unreachable workers to the set.
		for (auto pair : workerEventsVec)
		{
			if (pair.present() && pair.get().second.size())
				mergeUnreachable.insert(pair.get().second.begin(), pair.get().second.end());
		}

		// We now have a unique set of workers who were in some way unreachable.  If there is anything in that set, create a message
		// for it and include the list of unreachable processes.
		if (mergeUnreachable.size()){
			JsonBuilderObject message = JsonBuilder::makeMessage("unreachable_processes", "The cluster has some unreachable processes.");
			JsonBuilderArray unreachableProcs;
			for (auto m : mergeUnreachable){
				unreachableProcs.push_back(JsonBuilderObject().setKey("address", m));
			}
			message["unreachable_processes"] = unreachableProcs;
			messages.push_back(message);
		}

		// construct status information for cluster subsections
		state int statusCode = (int) RecoveryStatus::END;
		state JsonBuilderObject recoveryStateStatus = wait(recoveryStateStatusFetcher(mWorker, workers.size(), &status_incomplete_reasons, &statusCode));

		// machine metrics
		state WorkerEvents mMetrics = workerEventsVec[0].present() ? workerEventsVec[0].get().first : WorkerEvents();
		// process metrics
		state WorkerEvents pMetrics = workerEventsVec[1].present() ? workerEventsVec[1].get().first : WorkerEvents();
		state WorkerEvents latestError = workerEventsVec[2].present() ? workerEventsVec[2].get().first : WorkerEvents();
		state WorkerEvents traceFileOpenErrors = workerEventsVec[3].present() ? workerEventsVec[3].get().first : WorkerEvents();
		state WorkerEvents programStarts = workerEventsVec[4].present() ? workerEventsVec[4].get().first : WorkerEvents();

		state JsonBuilderObject statusObj;
		if(db->get().recoveryCount > 0) {
			statusObj["generation"] = db->get().recoveryCount;
		}

		state std::map<std::string, JsonBuilderObject> processIssues = getProcessIssuesAsMessages(workerIssues);
		state vector<std::pair<StorageServerInterface, TraceEventFields>> storageServers;
		state vector<std::pair<TLogInterface, TraceEventFields>> tLogs;
		state JsonBuilderObject qos;
		state JsonBuilderObject data_overlay;

		statusObj["protocol_version"] = format("%llx", currentProtocolVersion);
		statusObj["connection_string"] = coordinators.ccf->getConnectionString().toString();

		state Optional<DatabaseConfiguration> configuration;
		state Optional<bool> fullReplication;

		if(statusCode != RecoveryStatus::configuration_missing) {
			std::pair<Optional<DatabaseConfiguration>,Optional<bool>> loadResults = wait(loadConfiguration(cx, &messages, &status_incomplete_reasons));
			configuration = loadResults.first;
			fullReplication = loadResults.second;
		}

		if(fullReplication.present()) {
			statusObj["full_replication"] = fullReplication.get();
		}

		statusObj["machines"] = machineStatusFetcher(mMetrics, workers, configuration, &status_incomplete_reasons);

		if (configuration.present()){
			// Do the latency probe by itself to avoid interference from other status activities
			state bool isAvailable = true;
			JsonBuilderObject latencyProbeResults = wait(latencyProbeFetcher(cx, &messages, &status_incomplete_reasons, &isAvailable));

			statusObj["database_available"] = isAvailable;
			if (!latencyProbeResults.empty()) {
				statusObj["latency_probe"] = latencyProbeResults;
			}

			// Start getting storage servers now (using system priority) concurrently.  Using sys priority because having storage servers
			// in status output is important to give context to error messages in status that reference a storage server role ID.
			state std::unordered_map<NetworkAddress, WorkerInterface> address_workers;
			for (auto worker : workers)
				address_workers[worker.first.address()] = worker.first;
			state Future<ErrorOr<vector<std::pair<StorageServerInterface, TraceEventFields>>>> storageServerFuture = errorOr(getStorageServersAndMetrics(cx, address_workers));
			state Future<ErrorOr<vector<std::pair<TLogInterface, TraceEventFields>>>> tLogFuture = errorOr(getTLogsAndMetrics(db, address_workers));

			state int minReplicasRemaining = -1;
			std::vector<Future<JsonBuilderObject>> futures2;
			futures2.push_back(dataStatusFetcher(mWorker, &minReplicasRemaining));
			futures2.push_back(workloadStatusFetcher(db, workers, mWorker, &qos, &data_overlay, &status_incomplete_reasons, storageServerFuture));
			futures2.push_back(layerStatusFetcher(cx, &messages, &status_incomplete_reasons));
			futures2.push_back(lockedStatusFetcher(db, &messages, &status_incomplete_reasons));

			state std::vector<JsonBuilderObject> workerStatuses = wait(getAll(futures2));

			int oldLogFaultTolerance = 100;
			if(db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS && db->get().logSystemConfig.oldTLogs.size() > 0) {
				statusObj["old_logs"] = oldTlogFetcher(&oldLogFaultTolerance, db, address_workers);
			}

			if(configuration.present()) {
				int extraTlogEligibleMachines = getExtraTLogEligibleMachines(workers, configuration.get());
				statusObj["fault_tolerance"] = faultToleranceStatusFetcher(configuration.get(), coordinators, workers, extraTlogEligibleMachines, minReplicasRemaining);
			}

			JsonBuilderObject configObj = configurationFetcher(configuration, coordinators, &status_incomplete_reasons);

			// configArr could be empty
			if (!configObj.empty())
				statusObj["configuration"] = configObj;

			// workloadStatusFetcher returns the workload section but also optionally writes the qos section and adds to the data_overlay object
			if (!workerStatuses[1].empty())
				statusObj["workload"] = workerStatuses[1];

			statusObj["layers"] = workerStatuses[2];

			// Add qos section if it was populated
			if (!qos.empty())
				statusObj["qos"] = qos;

			// Merge data_overlay into data
			JsonBuilderObject &clusterDataSection = workerStatuses[0];

			// TODO:  This probably is no longer possible as there is no ability to merge json objects with an output-only model
			clusterDataSection.addContents(data_overlay);

			// If data section not empty, add it to statusObj
			if (!clusterDataSection.empty())
				statusObj["data"] = clusterDataSection;

			// Insert database_locked section
			if(!workerStatuses[3].empty()) {
				statusObj.addContents(workerStatuses[3]);
			}

			// Need storage servers now for processStatusFetcher() below.
			ErrorOr<vector<std::pair<StorageServerInterface, TraceEventFields>>> _storageServers = wait(storageServerFuture);
			if (_storageServers.present()) {
				storageServers = _storageServers.get();
			}
			else
				messages.push_back(JsonBuilder::makeMessage("storage_servers_error", "Timed out trying to retrieve storage servers."));

			// ...also tlogs
			ErrorOr<vector<std::pair<TLogInterface, TraceEventFields>>> _tLogs = wait(tLogFuture);
			if (_tLogs.present()) {
				tLogs = _tLogs.get();
			}
			else
				messages.push_back(JsonBuilder::makeMessage("log_servers_error", "Timed out trying to retrieve log servers."));
		}
		else {
			// Set layers status to { _valid: false, error: "configurationMissing"}
			JsonBuilderObject layers;
			layers["_valid"] = false;
			layers["_error"] = "configurationMissing";
			statusObj["layers"] = layers;
		}

		JsonBuilderObject processStatus = wait(processStatusFetcher(db, workers, pMetrics, mMetrics, latestError, traceFileOpenErrors, programStarts, processIssues, storageServers, tLogs, cx, configuration, &status_incomplete_reasons));
		statusObj["processes"] = processStatus;
		statusObj["clients"] = clientStatusFetcher(clientVersionMap, traceLogGroupMap);

		JsonBuilderArray incompatibleConnectionsArray;
		for(auto it : incompatibleConnections) {
			incompatibleConnectionsArray.push_back(it.toString());
		}
		statusObj["incompatible_connections"] = incompatibleConnectionsArray;
		statusObj["datacenter_version_difference"] = datacenterVersionDifference;

		if (!recoveryStateStatus.empty())
			statusObj["recovery_state"] = recoveryStateStatus;

		// cluster messages subsection;
		JsonBuilderArray clientIssuesArr = getClientIssuesAsMessages(clientIssues);
		if (clientIssuesArr.size() > 0) {
			JsonBuilderObject clientIssueMessage = JsonBuilder::makeMessage("client_issues", "Some clients of this cluster have issues.");
			clientIssueMessage["issues"] = clientIssuesArr;
			messages.push_back(clientIssueMessage);
		}

		// Create the status_incomplete message if there were any reasons that the status is incomplete.
		if (!status_incomplete_reasons.empty())
		{
			JsonBuilderObject incomplete_message = JsonBuilder::makeMessage("status_incomplete", "Unable to retrieve all status information.");
			// Make a JSON array of all of the reasons in the status_incomplete_reasons set.
			JsonBuilderArray reasons;
			for (auto i : status_incomplete_reasons) {
				reasons.push_back(JsonBuilderObject().setKey("description", i));
			}
			incomplete_message["reasons"] = reasons;
			messages.push_back(incomplete_message);
		}

		statusObj["messages"] = messages;

		int64_t clusterTime = time(0);
		if (clusterTime != -1){
			statusObj["cluster_controller_timestamp"] = clusterTime;
		}

		TraceEvent("ClusterGetStatus").detail("Duration", timer()-tStart).detail("StatusSize",statusObj.getFinalLength());

		return StatusReply(statusObj.getJson());
	} catch( Error&e ) {
		TraceEvent(SevError, "StatusError").error(e);
		throw;
	}
}

bool checkAsciiNumber(const char *s) {
	JsonBuilderObject number;
	number.setKeyRawNumber("number", s);
	std::string js = number.getJson();
	printf("'%s' => %s\n", s, js.c_str());

	try {
		// Make sure it parses as JSON
		readJSONStrictly(js);
	} catch(Error &e) {
		printf("error: %s\n", e.what());
		return false;
	}

	return true;
}

bool checkJson(const JsonBuilder &j, const char *expected) {
	std::string js = j.getJson();
	printf("json:     '%s'\n", js.c_str());
	printf("expected: '%s'\n\n", expected);

	try {
		// Make sure it parses as JSON
		readJSONStrictly(js);
	} catch(Error &e) {
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
	ASSERT(checkJson(object2, "{\"x\":1,\"y\":\"why\",\"z\":\"zee\",\"a\":5,\"b\":\"hi\",\"c\":[1,2,\"test\"],\"d\":[null]}"));

	object2.addContents(JsonBuilderObject());
	ASSERT(checkJson(object2, "{\"x\":1,\"y\":\"why\",\"z\":\"zee\",\"a\":5,\"b\":\"hi\",\"c\":[1,2,\"test\"],\"d\":[null]}"));

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

JsonBuilderObject randomDocument(const std::vector<std::string> &strings, int &limit, int level);
JsonBuilderArray randomArray(const std::vector<std::string> &strings, int &limit, int level);

JsonBuilderArray randomArray(const std::vector<std::string> &strings, int &limit, int level) {
	JsonBuilderArray r;
	int size = g_random->randomInt(0, 50);

	while(--size) {
		if(--limit <= 0)
			break;

		if(level > 0 && g_random->coinflip()) {
			if(g_random->coinflip())
				r.push_back(randomDocument(strings, limit, level - 1));
			else
				r.push_back(randomArray(strings, limit, level - 1));
		}
		else {
			switch(g_random->randomInt(0, 3)) {
				case 0:
					r.push_back(g_random->randomInt(0, 10000000));
				case 1:
					r.push_back(strings[g_random->randomInt(0, strings.size())]);
				case 2:
				default:
					r.push_back(g_random->random01());
			}
		}
	}

	return r;
}

JsonBuilderObject randomDocument(const std::vector<std::string> &strings, int &limit, int level) {
	JsonBuilderObject r;
	int size = g_random->randomInt(0, 300);

	while(--size) {
		if(--limit <= 0)
			break;

		const std::string &key = strings[g_random->randomInt(0, strings.size())];

		if(level > 0 && g_random->coinflip()) {
			if(g_random->coinflip())
				r[key] = randomDocument(strings, limit, level - 1);
			else
				r[key] = randomArray(strings, limit, level - 1);
		}
		else {
			switch(g_random->randomInt(0, 3)) {
				case 0:
					r[key] = g_random->randomInt(0, 10000000);
				case 1:
					r[key] = strings[g_random->randomInt(0, strings.size())];
				case 2:
				default:
					r[key] = g_random->random01();
			}
		}
	}

	return r;
}

TEST_CASE("/status/json/builderPerf") {
	std::vector<std::string> strings;
	int c = 1000000;
	printf("Generating random strings\n");
	while(--c)
		strings.push_back(g_random->randomAlphaNumeric(g_random->randomInt(0, 50)));

	int elements = 100000;
	int level = 6;
	int iterations = 200;

	printf("Generating and serializing random document\n");
	double start = timer();

	int64_t bytes = 0;
	double generated = 0;
	double serialized = 0;
	for(int i = 0; i < iterations; i++) {
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

		printf("JsonBuilder: %8lu bytes  %-7.5f gen   +  %-7.5f serialize =  %-7.5f\n", s.size(), generate, serialize, generate + serialize);
		printf("json_spirit: %8lu bytes  %-7.5f parse +  %-7.5f serialize =  %-7.5f\n", jsStr.size(), jsParse, jsSerialize, jsParse + jsSerialize);
		printf("\n");
		
		generated += generate;
		serialized += serialize;
		bytes += s.size();
	}

	double elapsed = generated + serialized;
	printf("RESULT: %lld bytes  %d elements  %d levels  %f seconds (%f gen, %f serialize)  %f MB/s  %f items/s\n",
		bytes, iterations*elements, level, elapsed, generated, elapsed - generated, bytes / elapsed / 1e6, iterations*elements / elapsed);

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
	std::string expected = "{\"a\":\"justA\",\"b\":\"justB\",\"bool_true\":true,\"expired\":null,\"int_one\":1,\"int_total_30\":30,\"int_unmatched\":{\"ERROR\":\"Values do not match.\",\"a\":2,\"b\":3},\"last_hello\":\"hello\",\"latest_int_5\":5,\"latest_obj\":{\"a\":\"a\",\"b\":\"b\",\"not_expired\":\"Still alive.\"},\"mixed_numeric_min_0\":0,\"mixed_numeric_sum_6\":6,\"not_expired\":\"I am still valid\",\"not_expired_and_merged\":{\"one\":1,\"seven\":7},\"string\":\"test\",\"subdoc\":{\"double_max_5\":5,\"double_min_2\":2,\"int_11\":11,\"obj_count_3\":3}}";

	if(result != expected) {
		printf("ERROR:  Combined doc does not match expected.\nexpected:\n\n%s\nresult:\n%s\n", expected.c_str(), result.c_str());
		ASSERT(false);
	}

	return Void();
}
