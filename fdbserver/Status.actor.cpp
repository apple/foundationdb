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

#include "Status.h"
#include "flow/actorcompiler.h"
#include "flow/Trace.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "WorkerInterface.h"
#include "ClusterRecruitmentInterface.h"
#include <time.h>
#include "CoordinationInterface.h"
#include "DataDistribution.h"
#include "flow/UnitTest.h"
#include "QuietDatabase.h"
#include "RecoveryState.h"

const char* RecoveryStatus::names[] = {
	"reading_coordinated_state", "locking_coordinated_state", "locking_old_transaction_servers", "reading_transaction_system_state",
	"configuration_missing", "configuration_never_created", "configuration_invalid",
	"recruiting_transaction_servers", "initializing_transaction_servers", "recovery_transaction",
	"writing_coordinated_state", "fully_recovered"
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
	// fully_recovered
	"Recovery complete."
};
static_assert( sizeof(RecoveryStatus::descriptions) == sizeof(RecoveryStatus::descriptions[0])*RecoveryStatus::END, "RecoveryStatus::descriptions[] size" );

// From Ratekeeper.actor.cpp
extern int limitReasonEnd;
extern const char* limitReasonName[];
extern const char* limitReasonDesc[];

// Returns -1 if it fails to find a quoted string at the start of xml; returns the position beyond the close quote
// If decoded is not NULL, writes the decoded attribute value there
int decodeQuotedAttributeValue( StringRef xml, std::string* decoded ) {
	if (decoded) decoded->clear();
	if (!xml.size() || xml[0] != '"') return -1;
	int pos = 1;

	loop {
		if (pos == xml.size()) return -1;  // No closing quote
		if (xml[pos]=='"') { pos++; break; } // Success

		uint8_t out = xml[pos];
		if (xml[pos] == '&') {
			if (xml.substr(pos).startsWith(LiteralStringRef("&amp;"))) { out = '&'; pos += 5; }
			else if (xml.substr(pos).startsWith(LiteralStringRef("&lt;"))) { out = '<'; pos += 4; }
			else if (xml.substr(pos).startsWith(LiteralStringRef("&quot;"))) { out = '"'; pos += 6; }
			else return -1;
		} else
			pos++;
		if (decoded) decoded->push_back(out);
	}

	return pos;
}

// return false on failure; outputs decoded attribute value to `ret`
bool tryExtractAttribute( StringRef expanded, StringRef attributeToExtract, std::string& ret ) {
	// This is only expected to parse the XML that Trace.cpp actually generates; we haven't looked at the standard to even find out what it doesn't try to do

	int pos = 0;
	// Consume '<'
	if (pos == expanded.size() || expanded[pos] != '<') return false;
	pos++;
	// Consume tag name
	while (pos != expanded.size() && expanded[pos] != ' ' && expanded[pos] != '/' && expanded[pos] != '>') pos++;

	while (pos != expanded.size() && expanded[pos] != '>' && expanded[pos] != '/') {
		// Consume whitespace
		while (pos != expanded.size() && expanded[pos] == ' ') pos++;

		// We should be looking at an attribute or the end of the string; find '=' at the end of the attribute, if any
		int eq_or_end = pos;
		while (eq_or_end != expanded.size() && expanded[eq_or_end]!='=' && expanded[eq_or_end]!='>') eq_or_end++;

		if ( expanded.substr(pos, eq_or_end-pos) == attributeToExtract ) {
			// Found the attribute we want; decode the value
			int end = decodeQuotedAttributeValue(expanded.substr(eq_or_end+1), &ret);
			if (end<0) { ret.clear(); return false; }
			return true;
		}

		// We don't want this attribute, but we need to skip over its value
		// It looks like this *could* just be a scan for '"' characters
		int end = decodeQuotedAttributeValue(expanded.substr(eq_or_end+1), NULL);
		if (end<0) return false;
		pos = (eq_or_end+1)+end;
	}
	return false;
}

// Throws attribute_not_found if the key is not found
std::string extractAttribute( StringRef expanded, StringRef attributeToExtract ) {
	std::string ret;
	if (!tryExtractAttribute(expanded, attributeToExtract, ret))
		throw attribute_not_found();
	return ret;
}
std::string extractAttribute( std::string const& expanded, std::string const& attributeToExtract ) {
	return extractAttribute(StringRef(expanded), StringRef(attributeToExtract));
}

TEST_CASE("fdbserver/Status/extractAttribute/basic") {
	std::string a;

	ASSERT( tryExtractAttribute(
		LiteralStringRef("<Foo A=\"&quot;a&quot;\" B=\"\" />"),
		LiteralStringRef("A"),
		a) && a == LiteralStringRef("\"a\""));

	ASSERT( tryExtractAttribute(
		LiteralStringRef("<Foo A=\"&quot;a&quot;\" B=\"\\\" />"),
		LiteralStringRef("B"),
		a) && a == LiteralStringRef("\\") );

	ASSERT( tryExtractAttribute(
		LiteralStringRef("<Event Severity=\"10\" Time=\"1415124565.129695\" Type=\"ProgramStart\" Machine=\"10.0.0.85:6863\" ID=\"0000000000000000\" RandomSeed=\"-2044671207\" SourceVersion=\"675cd9579467+ tip\" Version=\"3.0.0-PRERELEASE\" PackageName=\"3.0\" DataFolder=\"\" ConnectionString=\"circus:81060aa85f0a5b5b@10.0.0.5:4000,10.0.0.17:4000,10.0.0.78:4000,10.0.0.162:4000,10.0.0.182:4000\" ActualTime=\"1415124565\" CommandLine=\"fdbserver -r multitest -p auto:6863 -f /tmp/circus/testspec.txt --num_testers 24 --logdir /tmp/circus/multitest\" BuggifyEnabled=\"0\"/>"),
		LiteralStringRef("Version"),
		a) && a == LiteralStringRef("3.0.0-PRERELEASE") );

	ASSERT( !tryExtractAttribute(
		LiteralStringRef("<Event Severity=\"10\" Time=\"1415124565.129695\" Type=\"ProgramStart\" Machine=\"10.0.0.85:6863\" ID=\"0000000000000000\" RandomSeed=\"-2044671207\" SourceVersion=\"675cd9579467+ tip\" Version=\"3.0.0-PRERELEASE\" PackageName=\"3.0\" DataFolder=\"\" ConnectionString=\"circus:81060aa85f0a5b5b@10.0.0.5:4000,10.0.0.17:4000,10.0.0.78:4000,10.0.0.162:4000,10.0.0.182:4000\" ActualTime=\"1415124565\" CommandLine=\"fdbserver -r multitest -p auto:6863 -f /tmp/circus/testspec.txt --num_testers 24 --logdir /tmp/circus/multitest\" BuggifyEnabled=\"0\"/>"),
		LiteralStringRef("ersion"),
		a) );

	return Void();
}

TEST_CASE("fdbserver/Status/extractAttribute/fuzz") {
	// This is just looking for anything that crashes or infinite loops
	std::string out;
	for(int i=0; i<100000; i++)
	{
		std::string s = "<Event Severity=\"10\" Time=\"1415124565.129695\" Type=\"Program &quot;Start&quot;\" Machine=\"10.0.0.85:6863\" ID=\"0000000000000000\" RandomSeed=\"-2044671207\" SourceVersion=\"675cd9579467+ tip\" Version=\"3.0.0-PRERELEASE\" PackageName=\"3.0\" DataFolder=\"\" ConnectionString=\"circus:81060aa85f0a5b5b@10.0.0.5:4000,10.0.0.17:4000,10.0.0.78:4000,10.0.0.162:4000,10.0.0.182:4000\" ActualTime=\"1415124565\" CommandLine=\"fdbserver -r multitest -p auto:6863 -f /tmp/circus/testspec.txt --num_testers 24 --logdir /tmp/circus/multitest\" BuggifyEnabled=\"0\"/>";
		s[ g_random->randomInt(0, s.size()) ] = g_random->randomChoice(LiteralStringRef("\" =q0\\&"));
		tryExtractAttribute(s, LiteralStringRef("Version"), out);
	}
	return Void();
}

struct WorkerEvents : std::map<NetworkAddress, std::string>  {};

ACTOR static Future< Optional<std::string> > latestEventOnWorker(WorkerInterface worker, std::string eventName) {
	try {
		EventLogRequest req = eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
		ErrorOr<Standalone<StringRef>> eventTrace  = wait( errorOr(timeoutError(worker.eventLogRequest.getReply(req), 2.0)));

		if (eventTrace.isError()){
			return Optional<std::string>();
		}
		return eventTrace.get().toString();
	}
	catch (Error &e){
		if (e.code() == error_code_actor_cancelled)
			throw;
		return Optional<std::string>();
	}
}

ACTOR static Future< Optional< std::pair<WorkerEvents, std::set<std::string>> > > latestEventOnWorkers(std::vector<std::pair<WorkerInterface, ProcessClass>> workers, std::string eventName) {
	try {
		state vector<Future<ErrorOr<Standalone<StringRef>>>> eventTraces;
		for (int c = 0; c < workers.size(); c++) {
			EventLogRequest req = eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
			eventTraces.push_back(errorOr(timeoutError(workers[c].first.eventLogRequest.getReply(req), 2.0)));
		}

		Void _ = wait(waitForAll(eventTraces));

		std::set<std::string> failed;
		WorkerEvents results;

		for (int i = 0; i < eventTraces.size(); i++) {
			ErrorOr<Standalone<StringRef>> v = eventTraces[i].get();
			if (v.isError()){
				failed.insert(workers[i].first.address().toString());
				results[workers[i].first.address()] = "";
			}
			else {
				results[workers[i].first.address()] = v.get().toString();
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

static StatusObject makeCounter(double hz=0.0, double r=0.0, int64_t c=0) {
	StatusObject out;
	out["hz"] = hz;
	out["roughness"] = r;
	out["counter"] = c;
	return out;
}

static StatusObject parseCounter(std::string const& s) {
	// Parse what traceCounters() in Stats.actor.cpp formats
	double hz = 0.0, roughness = 0.0;
	long long counter = 0;
	sscanf(s.c_str(), "%lf %lf %lld", &hz, &roughness, &counter);
	return makeCounter(hz, roughness, counter);
}

static StatusObject addCounters(StatusObject c1, StatusObject c2) {
	// "add" the given counter objects.  Roughness is averaged weighted by rate.

	double c1hz = c1["hz"].get_real();
	double c2hz = c2["hz"].get_real();
	double c1r = c1["roughness"].get_real();
	double c2r = c2["roughness"].get_real();
	double c1c = c1["counter"].get_real();
	double c2c = c2["counter"].get_real();

	return makeCounter(
		c1hz+c2hz,
		(c1hz + c2hz) ? (c1r*c1hz + c2r*c2hz) / (c1hz + c2hz) : 0.0,
		c1c+c2c
		);
}

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

static StatusObject getLocalityInfo(const LocalityData& locality) {
	StatusObject localityObj;

	for(auto it = locality._data.begin(); it != locality._data.end(); it++) {
		if(it->second.present()) {
			localityObj[it->first.toString()] = it->second.get().toString();
		}
		else {
			localityObj[it->first.toString()] = json_spirit::mValue();
		}
	}

	return localityObj;
}

static StatusObject getError(std::string error) {
	StatusObject statusObj;
	try {
		if (error.size()) {
			double time = atof(extractAttribute(error, "Time").c_str());
			statusObj["time"] = time;

			statusObj["raw_log_message"] = error;

			std::string type = extractAttribute(error, "Type");
			statusObj["type"] = type;

			std::string description = type;
			std::string errorName;
			if (tryExtractAttribute(error, LiteralStringRef("Error"), errorName)) {
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
		TraceEvent(SevError, "StatusGetErrorError").error(e).detail("RawError", error);
	}
	return statusObj;
}

static StatusObject machineStatusFetcher(WorkerEvents mMetrics, vector<std::pair<WorkerInterface, ProcessClass>> workers, Optional<DatabaseConfiguration> configuration, std::set<std::string> *incomplete_reasons) {
	StatusObject machineMap;
	double metric;
	int failed = 0;

	// map from machine networkAddress to datacenter ID
	WorkerEvents dcIds;
	std::map<NetworkAddress, LocalityData> locality;

	for (auto worker : workers){
		locality[worker.first.address()] = worker.first.locality;
		if (worker.first.locality.dcId().present())
			dcIds[worker.first.address()] = worker.first.locality.dcId().get().printable();
	}

	for(auto it = mMetrics.begin(); it != mMetrics.end(); it++) {

		if (!it->second.size()){
			continue;
		}

		StatusObject statusObj;  // Represents the status for a machine
		std::string event = it->second;

		try {
			std::string address = toIPString(it->first.ip);
			// We will use the "physical" caluculated machine ID here to limit exposure to machineID repurposing
			std::string machineId = extractAttribute(event, "MachineID");

			// If this machine ID does not already exist in the machineMap, add it
			if (!machineMap.count(machineId)) {
				statusObj["machine_id"] = machineId;

				if (dcIds.count(it->first)){
					statusObj["datacenter_id"] = dcIds[it->first];
				}

				if(locality.count(it->first)) {
					statusObj["locality"] = getLocalityInfo(locality[it->first]);
				}

				statusObj["address"] = address;

				StatusObject memoryObj;

				metric = parseDouble(extractAttribute(event, "TotalMemory"));
				memoryObj["total_bytes"] = metric;

				metric = parseDouble(extractAttribute(event, "CommittedMemory"));
				memoryObj["committed_bytes"] = metric;

				metric = parseDouble(extractAttribute(event, "AvailableMemory"));
				memoryObj["free_bytes"] = metric;

				statusObj["memory"] = memoryObj;

				StatusObject cpuObj;

				metric = parseDouble(extractAttribute(event, "CPUSeconds"));
				double cpu_seconds = metric;

				metric = parseDouble(extractAttribute(event, "Elapsed"));
				double elapsed = metric;

				if (elapsed > 0){
					cpuObj["logical_core_utilization"] = std::max(0.0, std::min(cpu_seconds / elapsed, 1.0));
				}

				statusObj["cpu"] = cpuObj;

				StatusObject networkObj;

				metric = parseDouble(extractAttribute(event, "MbpsSent"));
				StatusObject megabits_sent;
				megabits_sent["hz"] = metric;
				networkObj["megabits_sent"] = megabits_sent;

				metric = parseDouble(extractAttribute(event, "MbpsReceived"));
				StatusObject megabits_received;
				megabits_received["hz"] = metric;
				networkObj["megabits_received"] = megabits_received;

				metric = parseDouble(extractAttribute(event, "RetransSegs"));
				StatusObject retransSegsObj;
				if (elapsed > 0){
					retransSegsObj["hz"] = metric / elapsed;
				}
				networkObj["tcp_segments_retransmitted"] = retransSegsObj;

				statusObj["network"] = networkObj;

				if (configuration.present()){
					statusObj["excluded"] = true; // Will be set to false below if this or any later process is not excluded
				}

				statusObj["contributing_workers"] = 0;

				machineMap[machineId] = statusObj;
			}
			if (configuration.present() && !configuration.get().isExcludedServer(it->first))
				machineMap[machineId].get_obj()["excluded"] = false;

			machineMap[machineId].get_obj()["contributing_workers"] = machineMap[machineId].get_obj()["contributing_workers"].get_int() + 1;
		}
		catch (Error& e) {
			++failed;
		}
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
	std::multimap<NetworkAddress, StatusObject> roles;
	StatusObject& addRole( NetworkAddress address, std::string const& role, UID id) {
		StatusObject obj;
		obj["id"] = id.shortString();
		obj["role"] = role;
		return roles.insert( make_pair(address, obj ))->second;
	}
	StatusObject& addRole(std::string const& role, StorageServerInterface& iface, std::string const& metrics, Version maxTLogVersion) {
		StatusObject obj;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			obj["stored_bytes"] = parseInt64(extractAttribute(metrics, "bytesStored"));
			obj["kvstore_used_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesUsed"));
			obj["kvstore_free_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesFree"));
			obj["kvstore_available_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesAvailable"));
			obj["kvstore_total_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesTotal"));
			obj["input_bytes"] = parseCounter(extractAttribute(metrics, "bytesInput"));
			obj["durable_bytes"] = parseCounter(extractAttribute(metrics, "bytesDurable"));
			obj["query_queue_max"] = parseInt(extractAttribute(metrics, "QueryQueueMax"));
			obj["finished_queries"] = parseCounter(extractAttribute(metrics, "finishedQueries"));

			Version version = parseInt64(extractAttribute(metrics, "version"));
			obj["data_version"] = version;

			if(maxTLogVersion > 0) {
				obj["data_version_lag"] = std::max<Version>(0, maxTLogVersion - version);
			}

		} catch (Error& e) {
			if(e.code() != error_code_attribute_not_found)
				throw e;
		}
		return roles.insert( make_pair(iface.address(), obj ))->second;
	}
	StatusObject& addRole(std::string const& role, TLogInterface& iface, std::string const& metrics) {
		StatusObject obj;
		obj["id"] = iface.id().shortString();
		obj["role"] = role;
		try {
			obj["kvstore_used_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesUsed"));
			obj["kvstore_free_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesFree"));
			obj["kvstore_available_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesAvailable"));
			obj["kvstore_total_bytes"] = parseInt64(extractAttribute(metrics, "kvstoreBytesTotal"));
			obj["queue_disk_used_bytes"] = parseInt64(extractAttribute(metrics, "queueDiskBytesUsed"));
			obj["queue_disk_free_bytes"] = parseInt64(extractAttribute(metrics, "queueDiskBytesFree"));
			obj["queue_disk_available_bytes"] = parseInt64(extractAttribute(metrics, "queueDiskBytesAvailable"));
			obj["queue_disk_total_bytes"] = parseInt64(extractAttribute(metrics, "queueDiskBytesTotal"));
			obj["input_bytes"] = parseCounter(extractAttribute(metrics, "bytesInput"));
			obj["durable_bytes"] = parseCounter(extractAttribute(metrics, "bytesDurable"));
			obj["data_version"] = parseInt64(extractAttribute(metrics, "version"));
		} catch (Error& e) {
			if(e.code() != error_code_attribute_not_found)
				throw e;
		}
		return roles.insert( make_pair(iface.address(), obj ))->second;
	}
	template <class InterfaceType>
	StatusObject& addRole(std::string const& role, InterfaceType& iface) {
		return addRole(iface.address(), role, iface.id());
	}
	StatusArray getStatusForAddress( NetworkAddress a ) {
		StatusArray v;
		auto it = roles.lower_bound(a);
		while (it != roles.end() && it->first == a) {
			v.push_back(it->second);
			++it;
		}
		return v;
	}
};

ACTOR static Future<StatusObject> processStatusFetcher(
		Reference<AsyncVar<struct ServerDBInfo>> db,
		std::vector<std::pair<WorkerInterface, ProcessClass>> workers,
		WorkerEvents pMetrics,
		WorkerEvents mMetrics,
		WorkerEvents errors,
		WorkerEvents traceFileOpenErrors,
		WorkerEvents programStarts,
		std::map<std::string, StatusObject> processIssues,
		vector<std::pair<StorageServerInterface, std::string>> storageServers,
		vector<std::pair<TLogInterface, std::string>> tLogs,
		Database cx,
		Optional<DatabaseConfiguration> configuration,
		std::set<std::string> *incomplete_reasons) {

	// Array to hold one entry for each process
	state StatusObject processMap;
	state double metric;

	// construct a map from a process address to a status object containing a trace file open error
	// this is later added to the messages subsection
	state std::map<std::string, StatusObject> tracefileOpenErrorMap;
	state WorkerEvents::iterator traceFileErrorsItr;
	for(traceFileErrorsItr = traceFileOpenErrors.begin(); traceFileErrorsItr != traceFileOpenErrors.end(); ++traceFileErrorsItr) {
		Void _ = wait(yield());
		if (traceFileErrorsItr->second.size()){
			try {
				// Have event string, parse it and turn it into a message object describing the trace file opening error
				std::string event = traceFileErrorsItr->second;
				std::string fileName = extractAttribute(event, "Filename");
				StatusObject msgObj = makeMessage("file_open_error", format("Could not open file '%s' (%s).", fileName.c_str(), extractAttribute(event, "Error").c_str()).c_str());
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
		Void _ = wait(yield());
		state std::map<Optional<Standalone<StringRef>>, MachineMemoryInfo>::iterator memInfo = machineMemoryUsage.insert(std::make_pair(workerItr->first.locality.machineId(), MachineMemoryInfo())).first;
		try {
			ASSERT(pMetrics.count(workerItr->first.address()));
			std::string processMetrics = pMetrics[workerItr->first.address()];

			if(memInfo->second.valid()) {
				if(processMetrics.size() > 0) {
					memInfo->second.memoryUsage += parseDouble(extractAttribute(processMetrics, "Memory"));
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
			Void _ = wait(yield());
		}
	}

	state std::vector<std::pair<TLogInterface, std::string>>::iterator log;
	state Version maxTLogVersion = 0;
	for(log = tLogs.begin(); log != tLogs.end(); ++log) {
		StatusObject const& roleStatus = roles.addRole( "log", log->first, log->second );
		if(roleStatus.count("data_version") > 0) {
			maxTLogVersion = std::max(maxTLogVersion, roleStatus.at("data_version").get_int64());
		}
		Void _ = wait(yield());
	}

	state std::vector<std::pair<StorageServerInterface, std::string>>::iterator ss;
	state std::map<NetworkAddress, int64_t> ssLag;
	for(ss = storageServers.begin(); ss != storageServers.end(); ++ss) {
		StatusObject const& roleStatus = roles.addRole( "storage", ss->first, ss->second, maxTLogVersion );
		if(roleStatus.count("data_version_lag") > 0) {
			ssLag[ss->first.address()] = roleStatus.at("data_version_lag").get_int64();
		}
		Void _ = wait(yield());
	}

	state std::vector<ResolverInterface>::const_iterator res;
	state std::vector<ResolverInterface> resolvers = db->get().resolvers;
	for(res = resolvers.begin(); res != resolvers.end(); ++res) {
		roles.addRole( "resolver", *res );
		Void _ = wait(yield());
	}

	for(workerItr = workers.begin(); workerItr != workers.end(); ++workerItr) {
		Void _ = wait(yield());
		state StatusObject statusObj;
		try {
			ASSERT(pMetrics.count(workerItr->first.address()));

			processMap[printable(workerItr->first.locality.processId())] = StatusObject();

			NetworkAddress address = workerItr->first.address();
			std::string event = pMetrics[workerItr->first.address()];
			statusObj["address"] = address.toString();
			StatusObject memoryObj;

			if (event.size() > 0) {
				std::string zoneID = extractAttribute(event, "ZoneID");
				statusObj["fault_domain"] = zoneID;

				std::string MachineID = extractAttribute(event, "MachineID");
				statusObj["machine_id"] = MachineID;

				statusObj["locality"] = getLocalityInfo(workerItr->first.locality);

				statusObj["uptime_seconds"] = parseDouble(extractAttribute(event, "UptimeSeconds"));

				metric = parseDouble(extractAttribute(event, "CPUSeconds"));
				double cpu_seconds = metric;

				// rates are calculated over the last elapsed seconds
				metric = parseDouble(extractAttribute(event, "Elapsed"));
				double elapsed = metric;

				metric = parseDouble(extractAttribute(event, "DiskIdleSeconds"));
				double diskIdleSeconds = metric;

				metric = parseDouble(extractAttribute(event, "DiskReads"));
				double diskReads = metric;

				metric = parseDouble(extractAttribute(event, "DiskWrites"));
				double diskWrites = metric;

				uint64_t diskReadsCount = parseInt64(extractAttribute(event, "DiskReadsCount"));

				uint64_t diskWritesCount = parseInt64(extractAttribute(event, "DiskWritesCount"));

				metric = parseDouble(extractAttribute(event, "DiskWriteSectors"));
				double diskWriteSectors = metric;

				metric = parseDouble(extractAttribute(event, "DiskReadSectors"));
				double diskReadSectors = metric;

				StatusObject diskObj;
				if (elapsed > 0){
					StatusObject cpuObj;
					cpuObj["usage_cores"] = std::max(0.0, cpu_seconds / elapsed);
					statusObj["cpu"] = cpuObj;

					diskObj["busy"] = std::max(0.0, std::min((elapsed - diskIdleSeconds) / elapsed, 1.0));

					StatusObject readsObj;
					readsObj["counter"] = diskReadsCount;
					if (elapsed > 0)
						readsObj["hz"] = diskReads / elapsed;
					readsObj["sectors"] = diskReadSectors;

					StatusObject writesObj;
					writesObj["counter"] = diskWritesCount;
					if (elapsed > 0)
						writesObj["hz"] = diskWrites / elapsed;
					writesObj["sectors"] = diskWriteSectors;

					diskObj["reads"] = readsObj;
					diskObj["writes"] = writesObj;
				}

				diskObj["total_bytes"] = parseInt64(extractAttribute(event, "DiskTotalBytes"));
				diskObj["free_bytes"] = parseInt64(extractAttribute(event, "DiskFreeBytes"));
				statusObj["disk"] = diskObj;

				StatusObject networkObj;

				networkObj["current_connections"] = parseInt64(extractAttribute(event, "CurrentConnections"));
				StatusObject connections_established;
				connections_established["hz"] = parseDouble(extractAttribute(event, "ConnectionsEstablished"));
				networkObj["connections_established"] = connections_established;
				StatusObject connections_closed;
				connections_closed["hz"] = parseDouble(extractAttribute(event, "ConnectionsClosed"));
				networkObj["connections_closed"] = connections_closed;
				StatusObject connection_errors;
				connection_errors["hz"] = parseDouble(extractAttribute(event, "ConnectionErrors"));
				networkObj["connection_errors"] = connection_errors;

				metric = parseDouble(extractAttribute(event, "MbpsSent"));
				StatusObject megabits_sent;
				megabits_sent["hz"] = metric;
				networkObj["megabits_sent"] = megabits_sent;

				metric = parseDouble(extractAttribute(event, "MbpsReceived"));
				StatusObject megabits_received;
				megabits_received["hz"] = metric;
				networkObj["megabits_received"] = megabits_received;

				statusObj["network"] = networkObj;

				metric = parseDouble(extractAttribute(event, "Memory"));
				memoryObj["used_bytes"] = metric;
			}

			if (programStarts.count(address)) {
				auto const& psxml = programStarts.at(address);

				if(psxml.size() > 0) {
					int64_t memLimit = parseInt64(extractAttribute(psxml, "MemoryLimit"));
					memoryObj["limit_bytes"] = memLimit;

					std::string version;
					if (tryExtractAttribute(psxml, LiteralStringRef("Version"), version)) {
						statusObj["version"] = version;
					}

					std::string commandLine;
					if (tryExtractAttribute(psxml, LiteralStringRef("CommandLine"), commandLine)) {
						statusObj["command_line"] = commandLine;
					}
				}
			}

			// if this process address is in the machine metrics
			if (mMetrics.count(address) && mMetrics[address].size()){
				double availableMemory;
				availableMemory = parseDouble(extractAttribute(mMetrics[address], "AvailableMemory"));

				auto machineMemInfo = machineMemoryUsage[workerItr->first.locality.machineId()];
				if (machineMemInfo.valid()) {
					ASSERT(machineMemInfo.numProcesses > 0);
					int64_t memory = (availableMemory + machineMemInfo.memoryUsage) / machineMemInfo.numProcesses;
					memoryObj["available_bytes"] = std::max<int64_t>(memory, 0);
				}
			}

			statusObj["memory"] = memoryObj;

			StatusArray messages;

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

			if(ssLag[address] > 60 * SERVER_KNOBS->VERSIONS_PER_SECOND) {
				messages.push_back(makeMessage("storage_server_lagging", format("Storage server lagging by %ld seconds.", ssLag[address] / SERVER_KNOBS->VERSIONS_PER_SECOND).c_str()));
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

static StatusObject clientStatusFetcher(ClientVersionMap clientVersionMap, std::map<NetworkAddress, std::string> traceLogGroupMap) {
	StatusObject clientStatus;

	clientStatus["count"] = (int64_t)clientVersionMap.size();

	std::map<ClientVersionRef, std::set<NetworkAddress>> clientVersions;
	for(auto client : clientVersionMap) {
		for(auto ver : client.second) {
			clientVersions[ver].insert(client.first);
		}
	}

	StatusArray versionsArray = StatusArray();
	for(auto cv : clientVersions) {
		StatusObject ver;
		ver["count"] = (int64_t)cv.second.size();
		ver["client_version"] = cv.first.clientVersion.toString();
		ver["protocol_version"] = cv.first.protocolVersion.toString();
		ver["source_version"] = cv.first.sourceVersion.toString();

		StatusArray clients = StatusArray();
		for(auto client : cv.second) {
			StatusObject cli;
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

ACTOR static Future<StatusObject> recoveryStateStatusFetcher(std::pair<WorkerInterface, ProcessClass> mWorker, int workerCount, std::set<std::string> *incomplete_reasons) {
	state StatusObject message;

	try {
		Standalone<StringRef> md = wait( timeoutError(mWorker.first.eventLogRequest.getReply( EventLogRequest( LiteralStringRef("MasterRecoveryState") ) ), 1.0) );
		state int mStatusCode = parseInt( extractAttribute(md, LiteralStringRef("StatusCode")) );
		if (mStatusCode < 0 || mStatusCode >= RecoveryStatus::END)
			throw attribute_not_found();

		message = makeMessage(RecoveryStatus::names[mStatusCode], RecoveryStatus::descriptions[mStatusCode]);

		// Add additional metadata for certain statuses
		if (mStatusCode == RecoveryStatus::recruiting_transaction_servers) {
			int requiredLogs = atoi( extractAttribute(md, LiteralStringRef("RequiredTLogs")).c_str() );
			int requiredProxies = atoi( extractAttribute(md, LiteralStringRef("RequiredProxies")).c_str() );
			int requiredResolvers = atoi( extractAttribute(md, LiteralStringRef("RequiredResolvers")).c_str() );
			//int requiredProcesses = std::max(requiredLogs, std::max(requiredResolvers, requiredProxies));
			//int requiredMachines = std::max(requiredLogs, 1);

			message["required_logs"] = requiredLogs;
			message["required_proxies"] = requiredProxies;
			message["required_resolvers"] = requiredResolvers;
		} else if (mStatusCode == RecoveryStatus::locking_old_transaction_servers) {
			message["missing_logs"] = extractAttribute(md, LiteralStringRef("MissingIDs")).c_str();
		}
		// TODO:  time_in_recovery: 0.5
		//        time_in_state: 0.1

	} catch (Error &e){
		if (e.code() == error_code_actor_cancelled)
			throw;
	}

	// If recovery status name is not know, status is incomplete
	if (!message.count("name"))
		incomplete_reasons->insert("Recovery Status unavailable.");

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
			Void _ = wait(tr->onError(e));
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
			Void _ = wait(tr->onError(e));
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
			Void _ = wait(tr->commit());
			return timer_monotonic() - start;
		}
		catch(Error &e) {
			Void _ = wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> doProbe(Future<double> probe, int timeoutSeconds, const char* prefix, const char* description, StatusObject *probeObj, StatusArray *messages, std::set<std::string> *incomplete_reasons) {
	choose {
		when(ErrorOr<double> result = wait(errorOr(probe))) {
			if(result.isError()) {
				incomplete_reasons->insert(format("Unable to retrieve latency probe information (%s: %s).", description, result.getError().what()));
			}
			else {
				(*probeObj)[format("%s_seconds", prefix).c_str()] = result.get();
			}
		}
		when(Void _ = wait(delay(timeoutSeconds))) {
			messages->push_back(makeMessage(format("%s_probe_timeout", prefix).c_str(), format("Unable to %s after %d seconds.", description, timeoutSeconds).c_str()));
		}
	}

	return Void();
}

ACTOR static Future<StatusObject> latencyProbeFetcher(Database cx, StatusArray *messages, std::set<std::string> *incomplete_reasons) {
	state Transaction trImmediate(cx);
	state Transaction trDefault(cx);
	state Transaction trBatch(cx);
	state Transaction trWrite(cx);

	state StatusObject statusObj;

	try {
		Future<double> immediateGrvProbe = doGrvProbe(&trImmediate, FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Future<double> defaultGrvProbe = doGrvProbe(&trDefault);
		Future<double> batchGrvProbe = doGrvProbe(&trBatch, FDBTransactionOptions::PRIORITY_BATCH);

		Future<double> readProbe = doReadProbe(immediateGrvProbe, &trImmediate);
		Future<double> commitProbe = doCommitProbe(immediateGrvProbe, &trImmediate, &trWrite);

		int timeoutSeconds = 5;

		std::vector<Future<Void>> probes;
		probes.push_back(doProbe(immediateGrvProbe, timeoutSeconds, "immediate_priority_transaction_start", "start immediate priority transaction", &statusObj, messages, incomplete_reasons));
		probes.push_back(doProbe(defaultGrvProbe, timeoutSeconds, "transaction_start", "start default priority transaction", &statusObj, messages, incomplete_reasons));
		probes.push_back(doProbe(batchGrvProbe, timeoutSeconds, "batch_priority_transaction_start", "start batch priority transaction", &statusObj, messages, incomplete_reasons));
		probes.push_back(doProbe(readProbe, timeoutSeconds, "read", "read", &statusObj, messages, incomplete_reasons));
		probes.push_back(doProbe(commitProbe, timeoutSeconds, "commit", "commit", &statusObj, messages, incomplete_reasons));

		Void _ = wait(waitForAll(probes));
	}
	catch (Error &e) {
		incomplete_reasons->insert(format("Unable to retrieve latency probe information (%s).", e.what()));
	}

	return statusObj;
}

ACTOR static Future<Optional<DatabaseConfiguration>> loadConfiguration(Database cx, StatusArray *messages, std::set<std::string> *status_incomplete_reasons){
	state Optional<DatabaseConfiguration> result;
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
				when(Void _ = wait(getConfTimeout)) {
					messages->push_back(makeMessage("unreadable_configuration", "Unable to read database configuration."));
				}
			}
			break;
		}
		catch (Error &e) {
			Void _ = wait(tr.onError(e));
		}
	}
	return result;
}

static StatusObject configurationFetcher(Optional<DatabaseConfiguration> conf, ServerCoordinators coordinators, std::set<std::string> *incomplete_reasons) {
	StatusObject statusObj;
	try {
		StatusArray coordinatorLeaderServersArr;
		vector< ClientLeaderRegInterface > coordinatorLeaderServers = coordinators.clientLeaderServers;
		int count = coordinatorLeaderServers.size();
		statusObj["coordinators_count"] = count;

		if(conf.present()) {
			DatabaseConfiguration configuration = conf.get();
			std::map<std::string, std::string> configMap = configuration.toMap();
			for (auto it = configMap.begin(); it != configMap.end(); it++) {
				if (it->first == "redundancy_mode")
				{
					StatusObject redundancyStatusObj;
					redundancyStatusObj["factor"] = it->second;
					statusObj["redundancy"] = redundancyStatusObj;
				}
				else {
					statusObj[it->first] = it->second;
				}
			}

			StatusArray excludedServersArr;
			std::set<AddressExclusion> excludedServers = configuration.getExcludedServers();
			for (std::set<AddressExclusion>::iterator it = excludedServers.begin(); it != excludedServers.end(); it++) {
				StatusObject statusObj;
				statusObj["address"] = it->toString();
				excludedServersArr.push_back(statusObj);
			}
			statusObj["excluded_servers"] = excludedServersArr;

			if (configuration.masterProxyCount != -1)
				statusObj["proxies"] = configuration.getDesiredProxies();
			else if (configuration.autoMasterProxyCount != CLIENT_KNOBS->DEFAULT_AUTO_PROXIES)
				statusObj["auto_proxies"] = configuration.autoMasterProxyCount;

			if (configuration.resolverCount != -1)
				statusObj["resolvers"] = configuration.getDesiredResolvers();
			else if (configuration.autoResolverCount != CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS)
				statusObj["auto_resolvers"] = configuration.autoResolverCount;

			if (configuration.desiredTLogCount != -1)
				statusObj["logs"] = configuration.getDesiredLogs();
			else if (configuration.autoDesiredTLogCount != CLIENT_KNOBS->DEFAULT_AUTO_LOGS)
				statusObj["auto_logs"] = configuration.autoDesiredTLogCount;

			if(configuration.storagePolicy) {
				statusObj["storage_policy"] = configuration.storagePolicy->info();
			}
			if(configuration.tLogPolicy) {
				statusObj["tlog_policy"] = configuration.tLogPolicy->info();
			}
		}
	}
	catch (Error &e){
		incomplete_reasons->insert("Could not retrieve all configuration status information.");
	}
	return statusObj;
}

ACTOR static Future<StatusObject> dataStatusFetcher(std::pair<WorkerInterface, ProcessClass> mWorker, std::string dbName, int *minReplicasRemaining) {
	state StatusObject stateSectionObj;
	state StatusObject statusObjData;

	try {
		std::vector<Future<Standalone<StringRef>>> futures;

		// TODO:  Should this be serial?
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(StringRef(dbName + "/DDTrackerStarting"))), 1.0));
		futures.push_back(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(StringRef(dbName + "/DDTrackerStats"))), 1.0));

		std::vector<Standalone<StringRef>> dataInfo = wait(getAll(futures));

		Standalone<StringRef> startingStats = dataInfo[0];
		state Standalone<StringRef> dataStats = dataInfo[1];

		if (startingStats.size() && extractAttribute(startingStats, LiteralStringRef("State")) != "Active") {
			stateSectionObj["name"] = "initializing";
			stateSectionObj["description"] = "(Re)initializing automatic data distribution";
		}
		else {
			state Standalone<StringRef> md = wait(timeoutError(mWorker.first.eventLogRequest.getReply(EventLogRequest(StringRef(dbName + "/MovingData"))), 1.0));

			// If we have a MovingData message, parse it.
			if (md.size())
			{
				int64_t partitionsInQueue = parseInt64(extractAttribute(md, LiteralStringRef("InQueue")));
				int64_t partitionsInFlight = parseInt64(extractAttribute(md, LiteralStringRef("InFlight")));
				int64_t averagePartitionSize = parseInt64(extractAttribute(md, LiteralStringRef("AverageShardSize")));
				int64_t totalBytesWritten = parseInt64(extractAttribute(md, LiteralStringRef("BytesWritten")));
				int highestPriority = parseInt(extractAttribute(md, LiteralStringRef("HighestPriority")));

				if( averagePartitionSize >= 0 ) {
					StatusObject moving_data;
					moving_data["in_queue_bytes"] = partitionsInQueue * averagePartitionSize;
					moving_data["in_flight_bytes"] = partitionsInFlight * averagePartitionSize;
					moving_data["total_written_bytes"] = totalBytesWritten;

					// TODO: moving_data["rate_bytes"] = makeCounter(hz, c, r);
					statusObjData["moving_data"] = moving_data;

					statusObjData["average_partition_size_bytes"] = averagePartitionSize;
				}

				if (highestPriority >= PRIORITY_TEAM_0_LEFT) {
					stateSectionObj["healthy"] = false;
					stateSectionObj["name"] = "missing_data";
					stateSectionObj["description"] = "No replicas remain of some data";
					stateSectionObj["min_replicas_remaining"] = 0;
					*minReplicasRemaining = 0;
				}
				else if (highestPriority >= PRIORITY_TEAM_1_LEFT) {
					stateSectionObj["healthy"] = false;
					stateSectionObj["name"] = "healing";
					stateSectionObj["description"] = "Only one replica remains of some data";
					stateSectionObj["min_replicas_remaining"] = 1;
					*minReplicasRemaining = 1;
				}
				else if (highestPriority >= PRIORITY_TEAM_2_LEFT) {
					stateSectionObj["healthy"] = false;
					stateSectionObj["name"] = "healing";
					stateSectionObj["description"] = "Only two replicas remain of some data";
					stateSectionObj["min_replicas_remaining"] = 2;
					*minReplicasRemaining = 2;
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
			}

			if (dataStats.size())
			{
				int64_t totalDBBytes = parseInt64(extractAttribute(dataStats, LiteralStringRef("TotalSizeBytes")));
				statusObjData["total_kv_size_bytes"] = totalDBBytes;
				int shards = parseInt(extractAttribute(dataStats, LiteralStringRef("Shards")));
				statusObjData["partitions_count"] = shards;
			}

		}
	}
	catch (Error &e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		// The most likely reason to be here is a timeout, either way we have no idea if the data state is healthy or not
		// from the "cluster" perspective - from the client perspective it is not but that is indicated elsewhere.
	}

	if (!stateSectionObj.empty())
		statusObjData["state"] = stateSectionObj;

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
static Future<vector<std::pair<iface, std::string>>> getServerMetrics(vector<iface> servers, std::unordered_map<NetworkAddress, WorkerInterface> address_workers, std::string suffix) {
	state vector<Future<Optional<std::string>>> futures;
	for (auto s : servers) {
		futures.push_back(latestEventOnWorker(address_workers[s.address()], s.id().toString() + suffix));
	}

	Void _ = wait(waitForAll(futures));

	vector<std::pair<iface, std::string>> results;
	for (int i = 0; i < servers.size(); i++) {
		results.push_back(std::make_pair(servers[i], futures[i].get().present() ? futures[i].get().get() : ""));
	}
	return results;
}

ACTOR static Future<vector<std::pair<StorageServerInterface, std::string>>> getStorageServersAndMetrics(Database cx, std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	vector<StorageServerInterface> servers = wait(timeoutError(getStorageServers(cx, true), 5.0));
	vector<std::pair<StorageServerInterface, std::string>> results = wait(getServerMetrics(servers, address_workers, "/StorageMetrics"));
	return results;
}

ACTOR static Future<vector<std::pair<TLogInterface, std::string>>> getTLogsAndMetrics(Reference<AsyncVar<struct ServerDBInfo>> db, std::unordered_map<NetworkAddress, WorkerInterface> address_workers) {
	vector<TLogInterface> servers = db->get().logSystemConfig.allPresentLogs();
	vector<std::pair<TLogInterface, std::string>> results = wait(getServerMetrics(servers, address_workers, "/TLogMetrics"));
	return results;
}

static std::set<StringRef> getTLogEligibleMachines(vector<std::pair<WorkerInterface, ProcessClass>> workers, DatabaseConfiguration configuration) {
	std::set<StringRef> tlogEligibleMachines;
	for(auto worker : workers) {
		if(worker.second.machineClassFitness(ProcessClass::TLog) < ProcessClass::NeverAssign
			&& !configuration.isExcludedServer(worker.first.address()))
		{
			tlogEligibleMachines.insert(worker.first.locality.zoneId().get());
		}
	}

	return tlogEligibleMachines;
}

ACTOR static Future<StatusObject> workloadStatusFetcher(Reference<AsyncVar<struct ServerDBInfo>> db, vector<std::pair<WorkerInterface, ProcessClass>> workers, std::pair<WorkerInterface, ProcessClass> mWorker, std::string dbName, StatusObject *qos, StatusObject *data_overlay, std::set<std::string> *incomplete_reasons) {
	state StatusObject statusObj;
	state StatusObject operationsObj;

	// Writes and conflicts
	try {
		vector<Future<Standalone<StringRef>>> proxyStatFutures;
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
		vector<Standalone<StringRef>> proxyStats = wait(getAll(proxyStatFutures));

		StatusObject mutations=makeCounter(), mutationBytes=makeCounter(), txnConflicts=makeCounter(), txnStartOut=makeCounter(), txnCommitOutSuccess=makeCounter();

		for (auto &ps : proxyStats) {
			mutations = addCounters( mutations, parseCounter(extractAttribute(ps, LiteralStringRef("mutations"))) );
			mutationBytes = addCounters( mutationBytes, parseCounter(extractAttribute(ps, LiteralStringRef("mutationBytes"))) );
			txnConflicts = addCounters( txnConflicts, parseCounter(extractAttribute(ps, LiteralStringRef("txnConflicts"))) );
			txnStartOut = addCounters( txnStartOut, parseCounter(extractAttribute(ps, LiteralStringRef("txnStartOut"))) );
			txnCommitOutSuccess = addCounters( txnCommitOutSuccess, parseCounter(extractAttribute(ps, LiteralStringRef("txnCommitOutSuccess"))) );
		}

		operationsObj["writes"] = mutations;

		StatusObject bytesObj;
		bytesObj["written"] = mutationBytes;
		statusObj["bytes"] = bytesObj;

		StatusObject transactions;
		transactions["conflicted"] = txnConflicts;
		transactions["started"] = txnStartOut;
		transactions["committed"] = txnCommitOutSuccess;

		statusObj["transactions"] = transactions;
	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown mutations, conflicts, and transactions state.");
	}

	// Transactions and reads
	try {
		Standalone<StringRef> md = wait( timeoutError(mWorker.first.eventLogRequest.getReply( EventLogRequest(StringRef(dbName+"/RkUpdate") ) ), 1.0) );
		double tpsLimit = parseDouble(extractAttribute(md, LiteralStringRef("TPSLimit")));
		double transPerSec = parseDouble(extractAttribute(md, LiteralStringRef("ReleasedTPS")));
		double readReplyRate = parseDouble(extractAttribute(md, LiteralStringRef("ReadReplyRate")));
		int ssCount = parseInt(extractAttribute(md, LiteralStringRef("StorageServers")));
		int tlogCount = parseInt(extractAttribute(md, LiteralStringRef("TLogs")));
		int64_t worstFreeSpaceStorageServer = parseInt64(extractAttribute(md, LiteralStringRef("WorstFreeSpaceStorageServer")));
		int64_t worstFreeSpaceTLog = parseInt64(extractAttribute(md, LiteralStringRef("WorstFreeSpaceTLog")));
		int64_t worstStorageServerQueue = parseInt64(extractAttribute(md, LiteralStringRef("WorstStorageServerQueue")));
		int64_t limitingStorageServerQueue = parseInt64(extractAttribute(md, LiteralStringRef("LimitingStorageServerQueue")));
		int64_t worstTLogQueue = parseInt64(extractAttribute(md, LiteralStringRef("WorstTLogQueue")));
		int64_t totalDiskUsageBytes = parseInt64(extractAttribute(md, LiteralStringRef("TotalDiskUsageBytes")));
		int64_t worstVersionLag = parseInt64(extractAttribute(md, LiteralStringRef("WorstStorageServerVersionLag")));
		int64_t limitingVersionLag = parseInt64(extractAttribute(md, LiteralStringRef("LimitingStorageServerVersionLag")));

		StatusObject readsObj;
		readsObj["hz"] = readReplyRate;
		operationsObj["reads"] = readsObj;

		(*data_overlay)["total_disk_used_bytes"] = totalDiskUsageBytes;

		if(ssCount > 0) {
			(*data_overlay)["least_operating_space_bytes_storage_server"] = std::max(worstFreeSpaceStorageServer, (int64_t)0);
			(*qos)["worst_queue_bytes_storage_server"] = worstStorageServerQueue;
			(*qos)["limiting_queue_bytes_storage_server"] = limitingStorageServerQueue;
			(*qos)["worst_version_lag_storage_server"] = worstVersionLag;
			(*qos)["limiting_version_lag_storage_server"] = limitingVersionLag;
		}

		if(tlogCount > 0) {
			(*data_overlay)["least_operating_space_bytes_log_server"] = std::max(worstFreeSpaceTLog, (int64_t)0);
			(*qos)["worst_queue_bytes_log_server"] = worstTLogQueue;
		}

		(*qos)["transactions_per_second_limit"] = tpsLimit;
		(*qos)["released_transactions_per_second"] = transPerSec;

		int reason = parseInt(extractAttribute(md, LiteralStringRef("Reason")));
		StatusObject perfLimit;
		if (transPerSec > tpsLimit * 0.8) {
			// If reason is known, set qos.performance_limited_by, otherwise omit
			if (reason >= 0 && reason < limitReasonEnd) {
				perfLimit = makeMessage(limitReasonName[reason], limitReasonDesc[reason]);
				std::string reason_server_id = extractAttribute(md, LiteralStringRef("ReasonServerID"));
				if (!reason_server_id.empty())
					perfLimit["reason_server_id"] = reason_server_id;
			}
		}
		else {
			perfLimit = makeMessage("workload", "The database is not being saturated by the workload.");
		}

		if(!perfLimit.empty()) {
			perfLimit["reason_id"] = reason;
			(*qos)["performance_limited_by"] = perfLimit;
		}
	} catch (Error &e){
		if (e.code() == error_code_actor_cancelled)
			throw;
		incomplete_reasons->insert("Unknown read and performance state.");
	}
	statusObj["operations"] = operationsObj;

	return statusObj;
}

static StatusArray oldTlogFetcher(int* oldLogFaultTolerance, Reference<AsyncVar<struct ServerDBInfo>> db, std::unordered_map<NetworkAddress, WorkerInterface> const& address_workers) {
	StatusArray oldTlogsArray;

	if(db->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
		for(auto it : db->get().logSystemConfig.oldTLogs) {
			StatusObject statusObj;
			int failedLogs = 0;
			StatusArray logsObj;
			for(auto log : it.tLogs) {
				StatusObject logObj;
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
			*oldLogFaultTolerance = std::min(*oldLogFaultTolerance, it.tLogReplicationFactor - 1 - it.tLogWriteAntiQuorum - failedLogs);
			statusObj["logs"] = logsObj;
			statusObj["log_replication_factor"] = it.tLogReplicationFactor;
			statusObj["log_write_anti_quorum"] = it.tLogWriteAntiQuorum;
			statusObj["log_fault_tolerance"] = it.tLogReplicationFactor - 1 - it.tLogWriteAntiQuorum - failedLogs;
			oldTlogsArray.push_back(statusObj);
		}
	}

	return oldTlogsArray;
}

/*
static StatusObject faultToleranceStatusFetcher(DatabaseConfiguration configuration, ServerCoordinators coordinators, int numTLogEligibleMachines, int minReplicasRemaining, int oldLogFaultTolerance) {
=======
static StatusObject faultToleranceStatusFetcher(DatabaseConfiguration configuration, ServerCoordinators coordinators, std::vector<std::pair<WorkerInterface, ProcessClass>>& workers, int numTLogEligibleMachines, int minReplicasRemaining) {
*/

static StatusObject faultToleranceStatusFetcher(DatabaseConfiguration configuration, ServerCoordinators coordinators, std::vector<std::pair<WorkerInterface, ProcessClass>>& workers, int numTLogEligibleMachines, int minReplicasRemaining) {
	StatusObject statusObj;

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
	// ahm
	//	machineFailuresWithoutLosingData = std::min(machineFailuresWithoutLosingData, oldLogFaultTolerance);

	statusObj["max_machine_failures_without_losing_data"] = std::max(machineFailuresWithoutLosingData, 0);

	// without losing availablity
	statusObj["max_machine_failures_without_losing_availability"] = std::max(std::min(numTLogEligibleMachines - configuration.minMachinesRequired(), machineFailuresWithoutLosingData), 0);
	return statusObj;
}

static std::string getIssueDescription(std::string name) {
	if(name == "incorrect_cluster_file_contents") {
		return "Cluster file contents do not match current cluster connection string. Verify cluster file is writable and has not been overwritten externally.";
	}

	// FIXME: name and description will be the same unless the message is 'incorrect_cluster_file_contents', which is currently the only possible message
	return name;
}

static std::map<std::string, StatusObject> getProcessIssuesAsMessages( ProcessIssuesMap const& _issues ) {
	std::map<std::string, StatusObject> issuesMap;

	try {
		ProcessIssuesMap issues = _issues;
		for (auto i : issues) {
			StatusObject message = makeMessage(i.second.first.c_str(), getIssueDescription(i.second.first).c_str());
			issuesMap[i.first.toString()] = message;
		}
	}
	catch (Error &e) {
		TraceEvent(SevError, "ErrorParsingProcessIssues").error(e);
		// swallow
	}

	return issuesMap;
}

static StatusArray getClientIssuesAsMessages( ProcessIssuesMap const& _issues) {
	StatusArray issuesList;

	try {
		ProcessIssuesMap issues = _issues;
		std::map<std::string, std::vector<std::string>> deduplicatedIssues;

		for(auto i : issues) {
			deduplicatedIssues[i.second.first].push_back(format("%s:%d", toIPString(i.first.ip).c_str(), i.first.port));
		}

		for (auto i : deduplicatedIssues) {
			StatusObject message = makeMessage(i.first.c_str(), getIssueDescription(i.first).c_str());
			StatusArray addresses;
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

ACTOR Future<StatusObject> layerStatusFetcher(Database cx, StatusArray *messages, std::set<std::string> *incomplete_reasons) {
	state StatusObject result;
	state JSONDoc json(result);

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
							Void _ = wait(yield());
							json.absorb(doc.get_obj());
							Void _ = wait(yield());
						} catch(Error &e) {
							TraceEvent(SevWarn, "LayerStatusBadJSON").detail("Key", printable(docs[j].key));
						}
					}
				}
				json.create("_valid") = true;
				break;
			} catch(Error &e) {
				Void _ = wait(tr.onError(e));
			}
		}
	} catch(Error &e) {
		TraceEvent(SevWarn, "LayerStatusError").error(e);
		incomplete_reasons->insert(format("Unable to retrieve layer status (%s).", e.what()));
		json.create("_error") = format("Unable to retrieve layer status (%s).", e.what());
		json.create("_valid") = false;
	}

	json.cleanOps();
	return result;
}

ACTOR Future<StatusObject> lockedStatusFetcher(Reference<AsyncVar<struct ServerDBInfo>> db, StatusArray *messages, std::set<std::string> *incomplete_reasons) {
	state StatusObject statusObj;

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

				when(Void _ = wait(getTimeout)) {
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
					Void _ = wait(tr.onError(e));
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
		std::vector<NetworkAddress> incompatibleConnections )
{
	// since we no longer offer multi-database support, all databases must be named DB
	state std::string dbName = "DB";

	// Check if master worker is present
	state StatusArray messages;
	state std::set<std::string> status_incomplete_reasons;
	state std::pair<WorkerInterface, ProcessClass> mWorker;

	try {
		// Get the master Worker interface
		Optional<std::pair<WorkerInterface, ProcessClass>> _mWorker = getWorker( workers, db->get().master.address() );
		if (_mWorker.present()) {
			mWorker = _mWorker.get();
		} else {
			messages.push_back(makeMessage("unreachable_master_worker", "Unable to locate the master worker."));
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
			StatusObject message = makeMessage("unreachable_processes", "The cluster has some unreachable processes.");
			StatusArray unreachableProcs;
			for (auto m : mergeUnreachable){
				unreachableProcs.push_back(StatusObject({ {"address", m} }));
			}
			message["unreachable_processes"] = unreachableProcs;
			messages.push_back(message);
		}

		// construct status information for cluster subsections
		state StatusObject recoveryStateStatus = wait(recoveryStateStatusFetcher(mWorker, workers.size(), &status_incomplete_reasons));

		// machine metrics
		state WorkerEvents mMetrics = workerEventsVec[0].present() ? workerEventsVec[0].get().first : WorkerEvents();
		// process metrics
		state WorkerEvents pMetrics = workerEventsVec[1].present() ? workerEventsVec[1].get().first : WorkerEvents();
		state WorkerEvents latestError = workerEventsVec[2].present() ? workerEventsVec[2].get().first : WorkerEvents();
		state WorkerEvents traceFileOpenErrors = workerEventsVec[3].present() ? workerEventsVec[3].get().first : WorkerEvents();
		state WorkerEvents programStarts = workerEventsVec[4].present() ? workerEventsVec[4].get().first : WorkerEvents();

		state StatusObject statusObj;
		if(db->get().recoveryCount > 0) {
			statusObj["generation"] = db->get().recoveryCount;
		}

		state std::map<std::string, StatusObject> processIssues = getProcessIssuesAsMessages(workerIssues);
		state vector<std::pair<StorageServerInterface, std::string>> storageServers;
		state vector<std::pair<TLogInterface, std::string>> tLogs;
		state StatusObject qos;
		state StatusObject data_overlay;

		statusObj["protocol_version"] = format("%llx", currentProtocolVersion);

		state Optional<DatabaseConfiguration> configuration = Optional<DatabaseConfiguration>();

		if(!(recoveryStateStatus.count("name") && recoveryStateStatus["name"] == RecoveryStatus::names[RecoveryStatus::configuration_missing])) {
			Optional<DatabaseConfiguration> _configuration = wait(loadConfiguration(cx, &messages, &status_incomplete_reasons));
			configuration = _configuration;
		}

		statusObj["machines"] = machineStatusFetcher(mMetrics, workers, configuration, &status_incomplete_reasons);

		if (configuration.present()){
			// Do the latency probe by itself to avoid interference from other status activities
			StatusObject latencyProbeResults = wait(latencyProbeFetcher(cx, &messages, &status_incomplete_reasons));

			statusObj["database_available"] = latencyProbeResults.count("immediate_priority_transaction_start_seconds") && latencyProbeResults.count("read_seconds") && latencyProbeResults.count("commit_seconds");
			if (!latencyProbeResults.empty()) {
				statusObj["latency_probe"] = latencyProbeResults;
			}

			state int minReplicasRemaining = -1;
			std::vector<Future<StatusObject>> futures2;
			futures2.push_back(dataStatusFetcher(mWorker, dbName, &minReplicasRemaining));
			futures2.push_back(workloadStatusFetcher(db, workers, mWorker, dbName, &qos, &data_overlay, &status_incomplete_reasons));
			futures2.push_back(layerStatusFetcher(cx, &messages, &status_incomplete_reasons));
			futures2.push_back(lockedStatusFetcher(db, &messages, &status_incomplete_reasons));

			// Start getting storage servers now (using system priority) concurrently.  Using sys priority because having storage servers
			// in status output is important to give context to error messages in status that reference a storage server role ID.
			state std::unordered_map<NetworkAddress, WorkerInterface> address_workers;
			for (auto worker : workers)
				address_workers[worker.first.address()] = worker.first;
			state Future<ErrorOr<vector<std::pair<StorageServerInterface, std::string>>>> storageServerFuture = errorOr(getStorageServersAndMetrics(cx, address_workers));
			state Future<ErrorOr<vector<std::pair<TLogInterface, std::string>>>> tLogFuture = errorOr(getTLogsAndMetrics(db, address_workers));

			state std::vector<StatusObject> workerStatuses = wait(getAll(futures2));

			int oldLogFaultTolerance = 100;
			if(db->get().recoveryState == RecoveryState::FULLY_RECOVERED && db->get().logSystemConfig.oldTLogs.size() > 0) {
				statusObj["old_logs"] = oldTlogFetcher(&oldLogFaultTolerance, db, address_workers);
			}

			if(configuration.present()) {
				std::set<StringRef> tlogEligibleMachines = getTLogEligibleMachines(workers, configuration.get());
				statusObj["fault_tolerance"] = faultToleranceStatusFetcher(configuration.get(), coordinators, workers, tlogEligibleMachines.size(), minReplicasRemaining);
			}

			StatusObject configObj = configurationFetcher(configuration, coordinators, &status_incomplete_reasons);

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
			StatusObject &clusterDataSection = workerStatuses[0];
			clusterDataSection.insert(data_overlay.begin(), data_overlay.end());

			// If data section not empty, add it to statusObj
			if (!clusterDataSection.empty())
				statusObj["data"] = clusterDataSection;

			// Insert database_locked section
			if(!workerStatuses[3].empty()) {
				statusObj.insert(workerStatuses[3].begin(), workerStatuses[3].end());
			}

			// Need storage servers now for processStatusFetcher() below.
			ErrorOr<vector<std::pair<StorageServerInterface, std::string>>> _storageServers = wait(storageServerFuture);
			if (_storageServers.present()) {
				storageServers = _storageServers.get();
			}
			else
				messages.push_back(makeMessage("storage_servers_error", "Timed out trying to retrieve storage servers."));

			// ...also tlogs
			ErrorOr<vector<std::pair<TLogInterface, std::string>>> _tLogs = wait(tLogFuture);
			if (_tLogs.present()) {
				tLogs = _tLogs.get();
			}
			else
				messages.push_back(makeMessage("log_servers_error", "Timed out trying to retrieve log servers."));
		}
		else {
			// Set layers status to { _valid: false, error: "configurationMissing"}
			statusObj["layers"] = json_spirit::mObject({{"_valid", false}, {"_error", "configurationMissing"}});
		}

		StatusObject processStatus = wait(processStatusFetcher(db, workers, pMetrics, mMetrics, latestError, traceFileOpenErrors, programStarts, processIssues, storageServers, tLogs, cx, configuration, &status_incomplete_reasons));
		statusObj["processes"] = processStatus;
		statusObj["clients"] = clientStatusFetcher(clientVersionMap, traceLogGroupMap);

		StatusArray incompatibleConnectionsArray;
		for(auto it : incompatibleConnections) {
			incompatibleConnectionsArray.push_back(it.toString());
		}
		statusObj["incompatible_connections"] = incompatibleConnectionsArray;

		if (!recoveryStateStatus.empty())
			statusObj["recovery_state"] = recoveryStateStatus;

		// cluster messages subsection;
		StatusArray clientIssuesArr = getClientIssuesAsMessages(clientIssues);
		if (clientIssuesArr.size() > 0) {
			StatusObject clientIssueMessage = makeMessage("client_issues", "Some clients of this cluster have issues.");
			clientIssueMessage["issues"] = clientIssuesArr;
			messages.push_back(clientIssueMessage);
		}

		// Create the status_incomplete message if there were any reasons that the status is incomplete.
		if (!status_incomplete_reasons.empty())
		{
			StatusObject incomplete_message = makeMessage("status_incomplete", "Unable to retrieve all status information.");
			// Make a JSON array of all of the reasons in the status_incomplete_reasons set.
			StatusArray reasons;
			for (auto i : status_incomplete_reasons)
				reasons.push_back(StatusObject({ { "description", i } }));
			incomplete_message["reasons"] = reasons;
			messages.push_back(incomplete_message);
		}

		statusObj["messages"] = messages;

		int64_t clusterTime = time(0);
		if (clusterTime != -1){
			statusObj["cluster_controller_timestamp"] = clusterTime;
		}

		return statusObj;
	} catch( Error&e ) {
		TraceEvent(SevError, "StatusError").error(e);
		throw;
	}
}

TEST_CASE("status/json/merging") {
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
