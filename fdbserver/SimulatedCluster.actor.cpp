/*
 * SimulatedCluster.actor.cpp
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

#include <cstdint>
#include <fstream>
#include <ostream>
#include <set>
#include <sstream>
#include <string_view>
#include <unordered_map>

#include <toml.hpp>

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/IPAllowList.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/SimpleIni.h"
#include "fdbrpc/AsyncFileNonDurable.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/versions.h"
#include "flow/IRandom.h"
#include "flow/MkCert.h"
#include "flow/ProcessEvents.h"
#include "fdbrpc/WellKnownEndpoints.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/TypeTraits.h"
#include "flow/FaultInjection.h"
#include "flow/CodeProbeUtils.h"
#include "fdbserver/SimulatedCluster.h"
#include "flow/IConnection.h"
#include "fdbserver/MockGlobalState.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#undef max
#undef min

extern "C" int g_expect_full_pointermap;
extern const char* getSourceVersion();

using namespace std::literals;

bool isSimulatorProcessUnreliable() {
	return g_network->isSimulated() && !g_simulator->getCurrentProcess()->isReliable();
}

namespace {

constexpr bool hasRocksDB =
#ifdef WITH_ROCKSDB
    true
#else
    false
#endif
    ;

} // anonymous namespace

namespace probe {

namespace assert {

struct HasRocksDB {
	constexpr static AnnotationType type = AnnotationType::Assertion;
	constexpr bool operator()(ICodeProbe const* self) const { return ::hasRocksDB; }
};

constexpr HasRocksDB hasRocksDB;

} // namespace assert

} // namespace probe

// TODO: Defining these here is just asking for ODR violations.
template <>
std::string describe(bool const& val) {
	return val ? "true" : "false";
}

template <>
std::string describe(int const& val) {
	return format("%d", val);
}

template <>
std::string describe(const SimulationStorageEngine& val) {
	return std::to_string(static_cast<uint32_t>(val));
}

namespace {

bool isValidSimulationStorageEngineValue(const std::underlying_type_t<SimulationStorageEngine>& value) {
	return value < static_cast<std::underlying_type_t<SimulationStorageEngine>>(
	                   SimulationStorageEngine::SIMULATION_STORAGE_ENGINE_INVALID_VALUE);
}

constexpr int MACHINE_REBOOT_TIME = 10;

// The max number of extra blob worker machines we might (i.e. randomly) add to the simulated cluster.
// Note that this is in addition to the two we always have.
constexpr int NUM_EXTRA_BW_MACHINES = 5;

bool destructed = false;

// Configuration details specified in workload test files that change the simulation
// environment details
class TestConfig : public BasicTestConfig {
	class ConfigBuilder {
		using value_type = toml::value;
		using base_variant = std::variant<int,
		                                  float,
		                                  double,
		                                  bool,
		                                  std::string,
		                                  std::vector<int>,
		                                  std::vector<std::string>,
		                                  ConfigDBType,
		                                  SimulationStorageEngine,
		                                  std::set<SimulationStorageEngine>>;
		using types =
		    variant_map<variant_concat<base_variant, variant_map<base_variant, Optional>>, std::add_pointer_t>;
		std::unordered_map<std::string_view, types> confMap;

		struct visitor {
			const value_type& value;
			visitor(const value_type& v) : value(v) {}
			void operator()(int* val) const { *val = value.as_integer(); }
			void operator()(Optional<int>* val) const { *val = value.as_integer(); }
			void operator()(float* val) const { *val = value.as_floating(); }
			void operator()(Optional<float>* val) const { *val = value.as_floating(); }
			void operator()(double* val) const { *val = value.as_floating(); }
			void operator()(Optional<double>* val) const { *val = value.as_floating(); }
			void operator()(bool* val) const { *val = value.as_boolean(); }
			void operator()(Optional<bool>* val) const { *val = value.as_boolean(); }
			void operator()(std::string* val) const { *val = value.as_string(); }
			void operator()(Optional<std::string>* val) const { *val = value.as_string(); }
			void operator()(std::vector<int>* val) const {
				auto arr = value.as_array();
				for (const auto& i : arr) {
					val->emplace_back(i.as_integer());
				}
			}
			void operator()(Optional<std::vector<int>>* val) const {
				std::vector<int> res;
				(*this)(&res);
				*val = std::move(res);
			}
			void operator()(ConfigDBType* val) const {
				if (value.as_string() == "random") {
					*val = deterministicRandom()->coinflip() ? ConfigDBType::SIMPLE : ConfigDBType::PAXOS;
				} else {
					*val = configDBTypeFromString(value.as_string());
				}
			}
			void operator()(Optional<ConfigDBType>* val) const {
				ConfigDBType res;
				(*this)(&res);
				*val = std::move(res);
			}
			void operator()(std::vector<std::string>* val) const {
				auto arr = value.as_array();
				for (const auto& i : arr) {
					val->emplace_back(i.as_string());
				}
			}
			void operator()(Optional<std::vector<std::string>>* val) const {
				std::vector<std::string> res;
				(*this)(&res);
				*val = std::move(res);
			}
			void operator()(std::set<SimulationStorageEngine>* val) const {
				auto arr = value.as_array();
				for (const auto& i : arr) {
					const auto intVal = static_cast<uint8_t>(i.as_integer());
					ASSERT(isValidSimulationStorageEngineValue(intVal));
					val->insert(static_cast<SimulationStorageEngine>(intVal));
				}
			}
			void operator()(Optional<std::set<SimulationStorageEngine>>* val) const {
				std::set<SimulationStorageEngine> res;
				(*this)(&res);
				*val = std::move(res);
			}
			void operator()(SimulationStorageEngine* val) const {
				uint8_t intVal = static_cast<uint8_t>(value.as_integer());
				ASSERT(isValidSimulationStorageEngineValue(intVal));
				*val = static_cast<SimulationStorageEngine>(intVal);
			}
			void operator()(Optional<SimulationStorageEngine>* val) const {
				SimulationStorageEngine v;
				(*this)(&v);
				*val = v;
			}
		};

		struct trace_visitor {
			std::string key;
			TraceEvent& evt;
			trace_visitor(std::string const& key, TraceEvent& e) : key("Key" + key), evt(e) {}
			template <class T>
			void operator()(T const* val) const {
				evt.detail(key.c_str(), *val);
			}
			void operator()(std::vector<int> const* val) const {
				if (val->empty()) {
					evt.detail(key.c_str(), "[]");
					return;
				}
				std::stringstream value;
				value << "[" << val->at(0);
				for (int i = 1; i < val->size(); ++i) {
					value << "," << val->at(i);
				}
				value << "]";
				evt.detail(key.c_str(), value.str());
			}
			void operator()(Optional<std::vector<int>> const* val) const {
				if (!val->present()) {
					evt.detail(key.c_str(), *val);
				} else {
					(*this)(&(val->get()));
				}
			}
			void operator()(std::vector<std::string> const* val) const {
				if (val->empty()) {
					evt.detail(key.c_str(), "[]");
					return;
				}
				std::stringstream value;
				value << "[" << val->at(0);
				for (int i = 1; i < val->size(); ++i) {
					value << "," << val->at(i);
				}
				value << "]";
				evt.detail(key.c_str(), value.str());
			}
			void operator()(Optional<std::vector<std::string>> const* val) const {
				if (!val->present()) {
					evt.detail(key.c_str(), *val);
				} else {
					(*this)(&(val->get()));
				}
			}
			void operator()(ConfigDBType const* val) const { evt.detail(key.c_str(), *val); }
			void operator()(Optional<ConfigDBType> const* val) const {
				Optional<std::string> optStr;
				if (val->present()) {
					optStr = configDBTypeToString(val->get());
				}
				evt.detail(key.c_str(), optStr);
			}
		};

	public:
		~ConfigBuilder() {
			TraceEvent evt("SimulatorConfigFromToml");
			for (const auto& p : confMap) {
				std::visit(trace_visitor(std::string(p.first), evt), p.second);
			}
		}

		template <class V>
		ConfigBuilder& add(std::string_view key, V value) {
			confMap.emplace(key, value);
			return *this;
		}

		void set(std::string_view key, const value_type& value) {
			auto iter = confMap.find(key);
			if (iter == confMap.end()) {
				std::cerr << "Unknown configuration attribute " << key << std::endl;
				TraceEvent("UnknownConfigurationAttribute").detail("Name", std::string(key));
				throw unknown_error();
			}
			std::visit(visitor(value), iter->second);
		}
	};

	bool isIniFile(const char* fileName) {
		std::string name = fileName;
		auto pos = name.find_last_of('.');
		ASSERT(pos != std::string::npos && pos + 1 < name.size());
		auto extension = name.substr(pos + 1);
		return extension == "txt"sv;
	}

	void loadIniFile(const char* testFile) {
		std::ifstream ifs;
		ifs.open(testFile, std::ifstream::in);
		if (!ifs.good())
			return;

		std::string cline;

		while (ifs.good()) {
			getline(ifs, cline);
			std::string line = removeWhitespace(std::string(cline));
			if (!line.size() || line.find(';') == 0)
				continue;

			size_t found = line.find('=');
			if (found == std::string::npos)
				// hmmm, not good
				continue;
			std::string attrib = removeWhitespace(line.substr(0, found));
			std::string value = removeWhitespace(line.substr(found + 1));

			if (attrib == "extraDatabaseMode") {
				extraDatabaseMode = ISimulator::stringToExtraDatabaseMode(value);
			}

			if (attrib == "extraDatabaseCount") {
				sscanf(value.c_str(), "%d", &extraDatabaseCount);
			}

			if (attrib == "minimumReplication") {
				sscanf(value.c_str(), "%d", &minimumReplication);
			}

			if (attrib == "minimumRegions") {
				sscanf(value.c_str(), "%d", &minimumRegions);
			}

			if (attrib == "configureLocked") {
				int configureLockedInt;
				sscanf(value.c_str(), "%d", &configureLockedInt);
				configureLocked = (configureLockedInt != 0);
			}

			if (attrib == "startIncompatibleProcess") {
				startIncompatibleProcess = strcmp(value.c_str(), "true") == 0;
			}

			if (attrib == "logAntiQuorum") {
				sscanf(value.c_str(), "%d", &logAntiQuorum);
			}

			if (attrib == "storageEngineExcludeTypes") {
				std::stringstream ss(value);
				for (int i; ss >> i;) {
					ASSERT(isValidSimulationStorageEngineValue(i));
					storageEngineExcludeTypes.insert(static_cast<SimulationStorageEngine>(i));
					if (ss.peek() == ',') {
						ss.ignore();
					}
				}
			}
			if (attrib == "maxTLogVersion") {
				sscanf(value.c_str(), "%d", &maxTLogVersion);
			}
			if (attrib == "disableTss") {
				disableTss = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "disableHostname") {
				disableHostname = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "disableRemoteKVS") {
				disableRemoteKVS = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "encryptModes") {
				std::stringstream ss(value);
				std::string token;
				while (std::getline(ss, token, ',')) {
					encryptModes.push_back(token);
				}
			}
			if (attrib == "restartInfoLocation") {
				isFirstTestInRestart = true;
			}
			if (attrib == "configDBType") {
				if (value == "random") {
					configDBType = deterministicRandom()->random01() < 0.1 ? ConfigDBType::SIMPLE : ConfigDBType::PAXOS;
				} else {
					configDBType = configDBTypeFromString(value);
				}
			}
			if (attrib == "randomlyRenameZoneId") {
				randomlyRenameZoneId = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "blobGranulesEnabled") {
				blobGranulesEnabled = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "simHTTPServerEnabled") {
				simHTTPServerEnabled = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "allowDefaultTenant") {
				allowDefaultTenant = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "allowCreatingTenants") {
				allowCreatingTenants = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "injectSSTargetedRestart") {
				injectTargetedSSRestart = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "tenantModes") {
				std::stringstream ss(value);
				std::string token;
				while (std::getline(ss, token, ',')) {
					tenantModes.push_back(token);
				}
			}
			if (attrib == "defaultTenant") {
				defaultTenant = value;
			}
			if (attrib == "longRunningTest") {
				longRunningTest = strcmp(value.c_str(), "true") == 0;
			}
			if (attrib == "simulationNormalRunTestsTimeoutSeconds") {
				sscanf(value.c_str(), "%d", &simulationNormalRunTestsTimeoutSeconds);
			}
			if (attrib == "simulationBuggifyRunTestsTimeoutSeconds") {
				sscanf(value.c_str(), "%d", &simulationBuggifyRunTestsTimeoutSeconds);
			}
		}

		ifs.close();
	}

	ConfigDBType configDBType{ ConfigDBType::DISABLED };

public:
	int extraDB = 0;
	ISimulator::ExtraDatabaseMode extraDatabaseMode = ISimulator::ExtraDatabaseMode::Disabled;
	// The number of extra database used if the database mode is MULTIPLE
	int extraDatabaseCount = 1;
	bool extraDatabaseBackupAgents = false;
	int minimumReplication = 0;
	int minimumRegions = 0;
	bool configureLocked = false;
	bool startIncompatibleProcess = false;
	bool isFirstTestInRestart = false;
	// 7.0 cannot be downgraded to 6.3 after enabling TSS, so disable TSS for 6.3 downgrade tests
	bool disableTss = false;
	// 7.1 cannot be downgraded to 7.0 and below after enabling hostname, so disable hostname for 7.0 downgrade tests
	bool disableHostname = false;
	// remote key value store is a child process spawned by the SS process to run the storage engine
	bool disableRemoteKVS = false;
	// By default, encryption mode is set randomly (based on the tenant mode)
	// If provided, set using EncryptionAtRestMode::fromString
	std::vector<std::string> encryptModes;
	// Storage Engine Types: Verify match with SimulationConfig::generateNormalConfig
	//	0 = "ssd"
	//	1 = "memory"
	//	2 = "memory-radixtree"
	//	3 = "ssd-redwood-1"
	//	4 = "ssd-rocksdb-v1"
	//	5 = "ssd-sharded-rocksdb"
	// Requires a comma-separated list of numbers WITHOUT whitespaces
	// See SimulationStorageEngine for more details
	std::set<SimulationStorageEngine> storageEngineExcludeTypes;
	Optional<int> datacenters, stderrSeverity, processesPerMachine;
	// Set the maximum TLog version that can be selected for a test
	// Refer to FDBTypes.h::TLogVersion. Defaults to the maximum supported version.
	int maxTLogVersion = TLogVersion::MAX_SUPPORTED;
	int extraMachineCountDC = 0;
	int extraStorageMachineCountPerDC = 0;

	Optional<bool> generateFearless, buggify;
	Optional<std::string> config;
	Optional<std::string> remoteConfig;
	bool blobGranulesEnabled = false;
	bool randomlyRenameZoneId = false;
	bool simHTTPServerEnabled = true;

	bool allowDefaultTenant = true;
	bool allowCreatingTenants = true;
	bool injectTargetedSSRestart = false;
	bool injectSSDelay = false;
	// By default, tenant mode is set randomly
	// If provided, set using TenantMode::fromString
	// Ensure no '_experimental` suffix in the mode name
	std::vector<std::string> tenantModes;
	Optional<std::string> defaultTenant;
	std::string testClass; // unused -- used in TestHarness
	float testPriority; // unused -- used in TestHarness

	bool longRunningTest = false;
	int simulationNormalRunTestsTimeoutSeconds = 5400;
	int simulationBuggifyRunTestsTimeoutSeconds = 36000;

	// Number of tlogs in the remote region
	Optional<int> remoteDesiredTLogCount;
	// Number of process classes explictly set as Stateless in all DCs
	Optional<int> statelessProcessClassesPerDC;

	ConfigDBType getConfigDBType() const { return configDBType; }

	bool tomlKeyPresent(const toml::value& data, std::string key) {
		if (data.is_table()) {
			for (const auto& [k, v] : data.as_table()) {
				if (k == key || tomlKeyPresent(v, key)) {
					return true;
				}
			}
		} else if (data.is_array()) {
			for (const auto& v : data.as_array()) {
				if (tomlKeyPresent(v, key)) {
					return true;
				}
			}
		}
		return false;
	}

	void readFromConfig(const char* testFile) {
		if (isIniFile(testFile)) {
			loadIniFile(testFile);
			return;
		}
		std::string extraDatabaseModeStr;
		ConfigBuilder builder;
		builder.add("testClass", &testClass)
		    .add("testPriority", &testPriority)
		    .add("extraDatabaseMode", &extraDatabaseModeStr)
		    .add("extraDatabaseCount", &extraDatabaseCount)
		    .add("extraDatabaseBackupAgents", &extraDatabaseBackupAgents)
		    .add("minimumReplication", &minimumReplication)
		    .add("minimumRegions", &minimumRegions)
		    .add("configureLocked", &configureLocked)
		    .add("startIncompatibleProcess", &startIncompatibleProcess)
		    .add("logAntiQuorum", &logAntiQuorum)
		    .add("storageEngineExcludeTypes", &storageEngineExcludeTypes)
		    .add("maxTLogVersion", &maxTLogVersion)
		    .add("disableTss", &disableTss)
		    .add("disableHostname", &disableHostname)
		    .add("disableRemoteKVS", &disableRemoteKVS)
		    .add("encryptModes", &encryptModes)
		    .add("simpleConfig", &simpleConfig)
		    .add("generateFearless", &generateFearless)
		    .add("datacenters", &datacenters)
		    .add("desiredTLogCount", &desiredTLogCount)
		    .add("remoteDesiredTLogCount", &remoteDesiredTLogCount)
		    .add("commitProxyCount", &commitProxyCount)
		    .add("grvProxyCount", &grvProxyCount)
		    .add("resolverCount", &resolverCount)
		    .add("storageEngineType", &storageEngineType)
		    .add("config", &config)
		    .add("remoteConfig", &remoteConfig)
		    .add("buggify", &buggify)
		    .add("StderrSeverity", &stderrSeverity)
		    .add("machineCount", &machineCount)
		    .add("asanMachineCount", &asanMachineCount)
		    .add("processesPerMachine", &processesPerMachine)
		    .add("coordinators", &coordinators)
		    .add("configDB", &configDBType)
		    .add("extraMachineCountDC", &extraMachineCountDC)
		    .add("extraStorageMachineCountPerDC", &extraStorageMachineCountPerDC)
		    .add("blobGranulesEnabled", &blobGranulesEnabled)
		    .add("simHTTPServerEnabled", &simHTTPServerEnabled)
		    .add("allowDefaultTenant", &allowDefaultTenant)
		    .add("allowCreatingTenants", &allowCreatingTenants)
		    .add("randomlyRenameZoneId", &randomlyRenameZoneId)
		    .add("injectTargetedSSRestart", &injectTargetedSSRestart)
		    .add("injectSSDelay", &injectSSDelay)
		    .add("tenantModes", &tenantModes)
		    .add("defaultTenant", &defaultTenant)
		    .add("longRunningTest", &longRunningTest)
		    .add("simulationNormalRunTestsTimeoutSeconds", &simulationNormalRunTestsTimeoutSeconds)
		    .add("simulationBuggifyRunTestsTimeoutSeconds", &simulationBuggifyRunTestsTimeoutSeconds)
		    .add("statelessProcessClassesPerDC", &statelessProcessClassesPerDC);
		try {
			auto file = toml::parse(testFile);
			if (file.contains("configuration") && toml::find(file, "configuration").is_table()) {
				auto conf = toml::find(file, "configuration").as_table();
				for (const auto& [key, value] : conf) {
					if (key == "ClientInfoLogging") {
						setNetworkOption(FDBNetworkOptions::DISABLE_CLIENT_STATISTICS_LOGGING);
					} else if (key == "restartInfoLocation") {
						isFirstTestInRestart = true;
					} else {
						builder.set(key, value);
					}
				}
				if (stderrSeverity.present()) {
					TraceEvent("StderrSeverity").detail("NewSeverity", stderrSeverity.get());
				}
			}
			// look for restartInfoLocation to mark isFirstTestInRestart
			if (!isFirstTestInRestart) {
				isFirstTestInRestart = tomlKeyPresent(file, "restartInfoLocation");
			}
			if (!extraDatabaseModeStr.empty()) {
				extraDatabaseMode = ISimulator::stringToExtraDatabaseMode(extraDatabaseModeStr);
			}
		} catch (std::exception& e) {
			std::cerr << e.what() << std::endl;
			TraceEvent("TOMLParseError").detail("Error", printable(e.what()));
			throw unknown_error();
		}
		// Verify that we can use the passed config
		if (simpleConfig) {
			if (minimumRegions > 1) {
				TraceEvent("ElapsedTime").detail("SimTime", now()).detail("RealTime", 0).detail("RandomUnseed", 0);
				flushAndExit(0);
			}
		}
	}

	bool excludedStorageEngineType(SimulationStorageEngine storageEngineType) const {
		return storageEngineExcludeTypes.contains(storageEngineType);
	}

	TestConfig() = default;
	TestConfig(const BasicTestConfig& config) : BasicTestConfig(config) {}
};

template <class T>
T simulate(const T& in) {
	BinaryWriter writer(AssumeVersion(g_network->protocolVersion()));
	writer << in;
	BinaryReader reader(writer.getData(), writer.getLength(), AssumeVersion(g_network->protocolVersion()));
	T out;
	reader >> out;
	return out;
}

ACTOR Future<Void> runBackup(Reference<IClusterConnectionRecord> connRecord) {
	state Future<Void> agentFuture;
	state FileBackupAgent fileAgent;

	while (g_simulator->backupAgents == ISimulator::BackupAgentType::WaitForType) {
		wait(delay(1.0));
	}

	if (g_simulator->backupAgents == ISimulator::BackupAgentType::BackupToFile) {
		Database cx = Database::createDatabase(connRecord, ApiVersion::LATEST_VERSION);

		TraceEvent("SimBackupAgentsStarting").log();
		agentFuture =
		    fileAgent.run(cx, 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT);

		while (g_simulator->backupAgents == ISimulator::BackupAgentType::BackupToFile) {
			wait(delay(1.0));
		}

		TraceEvent("SimBackupAgentsStopping").log();
		agentFuture.cancel();
	}

	wait(Future<Void>(Never()));
	throw internal_error();
}

ACTOR Future<Void> runDr(Reference<IClusterConnectionRecord> connRecord) {
	state std::vector<Future<Void>> agentFutures;

	while (g_simulator->drAgents == ISimulator::BackupAgentType::WaitForType) {
		wait(delay(1.0));
	}

	if (g_simulator->drAgents == ISimulator::BackupAgentType::BackupToDB) {
		ASSERT(g_simulator->extraDatabases.size() == 1);
		Database cx = Database::createDatabase(connRecord, ApiVersion::LATEST_VERSION);

		state Database drDatabase = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0]);

		TraceEvent("StartingDrAgents")
		    .detail("ConnectionString", connRecord->getConnectionString().toString())
		    .detail("ExtraString", g_simulator->extraDatabases[0]);

		state DatabaseBackupAgent dbAgent = DatabaseBackupAgent(cx);
		state DatabaseBackupAgent extraAgent = DatabaseBackupAgent(drDatabase);

		auto drPollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;

		agentFutures.push_back(extraAgent.run(cx, drPollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));
		agentFutures.push_back(dbAgent.run(drDatabase, drPollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));

		while (g_simulator->drAgents == ISimulator::BackupAgentType::BackupToDB) {
			wait(delay(1.0));
		}

		TraceEvent("StoppingDrAgents").log();

		for (auto it : agentFutures) {
			it.cancel();
		}
	}

	wait(Future<Void>(Never()));
	throw internal_error();
}

ACTOR Future<Void> runSimHTTPServer() {
	TraceEvent("SimHTTPServerStarting");
	state Reference<HTTP::SimServerContext> context = makeReference<HTTP::SimServerContext>();
	g_simulator->addSimHTTPProcess(context);

	try {
		wait(context->actors.getResult());
	} catch (Error& e) {
		TraceEvent("SimHTTPServerDied").errorUnsuppressed(e);
		context->stop();
		g_simulator->removeSimHTTPProcess();
		throw e;
	}
	throw internal_error();
}

// enum AgentMode { AgentNone = 0, AgentOnly = 1, AgentAddition = 2 };
// FIXME: could do this as bit flags of (fdbd) (backup agent) (http) etc... if the space gets more complicated
enum ProcessMode { FDBDOnly = 0, BackupAgentOnly = 1, FDBDAndBackupAgent = 2, SimHTTPServer = 3 };

bool processRunBackupAgent(ProcessMode mode) {
	return mode == BackupAgentOnly || mode == FDBDAndBackupAgent;
}

bool processRunFDBD(ProcessMode mode) {
	return mode == FDBDOnly || mode == FDBDAndBackupAgent;
}

bool processRunHTTPServer(ProcessMode mode) {
	return mode == SimHTTPServer;
}

// SOMEDAY: when a process can be rebooted in isolation from the other on that machine,
//  a loop{} will be needed around the waiting on simulatedFDBD(). For now this simply
//  takes care of house-keeping such as context switching and file closing.
ACTOR Future<ISimulator::KillType> simulatedFDBDRebooter(Reference<IClusterConnectionRecord> connRecord,
                                                         IPAddress ip,
                                                         bool sslEnabled,
                                                         uint16_t port,
                                                         uint16_t listenPerProcess,
                                                         LocalityData localities,
                                                         ProcessClass processClass,
                                                         std::string* dataFolder,
                                                         std::string* coordFolder,
                                                         std::string baseFolder,
                                                         ClusterConnectionString connStr,
                                                         ClusterConnectionString otherConnStr,
                                                         bool useSeedFile,
                                                         ProcessMode processMode,
                                                         std::string whitelistBinPaths,
                                                         ProtocolVersion protocolVersion,
                                                         ConfigDBType configDBType,
                                                         bool isDr) {
	state ISimulator::ProcessInfo* simProcess = g_simulator->getCurrentProcess();
	state UID randomId = nondeterministicRandom()->randomUniqueID();
	state int cycles = 0;
	state IPAllowList allowList;

	allowList.addTrustedSubnet("0.0.0.0/2"sv);
	allowList.addTrustedSubnet("abcd::/16"sv);

	loop {
		auto waitTime =
		    SERVER_KNOBS->MIN_REBOOT_TIME +
		    (SERVER_KNOBS->MAX_REBOOT_TIME - SERVER_KNOBS->MIN_REBOOT_TIME) * deterministicRandom()->random01();
		cycles++;
		TraceEvent("SimulatedFDBDPreWait")
		    .detail("Cycles", cycles)
		    .detail("RandomId", randomId)
		    .detail("Address", NetworkAddress(ip, port, true, false))
		    .detail("ZoneId", localities.zoneId())
		    .detail("WaitTime", waitTime)
		    .detail("Port", port)
		    .detail("IsDr", isDr);

		wait(delay(waitTime));

		state ISimulator::ProcessInfo* process = g_simulator->newProcess("Server",
		                                                                 ip,
		                                                                 port,
		                                                                 sslEnabled,
		                                                                 listenPerProcess,
		                                                                 localities,
		                                                                 processClass,
		                                                                 dataFolder->c_str(),
		                                                                 coordFolder->c_str(),
		                                                                 protocolVersion,
		                                                                 isDr);
		wait(g_simulator->onProcess(
		    process,
		    TaskPriority::DefaultYield)); // Now switch execution to the process on which we will run
		state Future<ISimulator::KillType> onShutdown = process->onShutdown();

		try {
			TraceEvent("SimulatedRebooterStarting")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("ZoneId", localities.zoneId())
			    .detail("DataHall", localities.dataHallId())
			    .detail("Address", process->address.toString())
			    .detail("Excluded", process->excluded)
			    .detail("UsingSSL", sslEnabled)
			    .detail("ProcessMode", processMode);
			TraceEvent("ProgramStart")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("SourceVersion", getSourceVersion())
			    .detail("Version", FDB_VT_VERSION)
			    .detail("PackageName", FDB_VT_PACKAGE_NAME)
			    .detail("DataFolder", *dataFolder)
			    .detail("ConnectionString", connRecord ? connRecord->getConnectionString().toString() : "")
			    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(nullptr))
			    .detail("CommandLine", "fdbserver -r simulation")
			    .detail("BuggifyEnabled", isGeneralBuggifyEnabled())
			    .detail("Simulated", true)
			    .trackLatest("ProgramStart");

			try {
				// SOMEDAY: test lower memory limits, without making them too small and causing the database to stop
				// making progress
				bool client = processClass == ProcessClass::TesterClass || processMode == BackupAgentOnly ||
				              processMode == SimHTTPServer;
				FlowTransport::createInstance(client, 1, WLTOKEN_RESERVED_COUNT, &allowList);
				for (const auto& p : g_simulator->authKeys) {
					FlowTransport::transport().addPublicKey(p.first, p.second.toPublic());
				}
				Sim2FileSystem::newFileSystem();

				std::vector<Future<Void>> futures;
				for (int listenPort = port; listenPort < port + listenPerProcess; ++listenPort) {
					NetworkAddress n(ip, listenPort, true, sslEnabled && listenPort == port);
					futures.push_back(FlowTransport::transport().bind(n, n));
				}
				if (processRunFDBD(processMode)) {
					futures.push_back(fdbd(connRecord,
					                       localities,
					                       processClass,
					                       *dataFolder,
					                       *coordFolder,
					                       500e6,
					                       "",
					                       "",
					                       -1,
					                       whitelistBinPaths,
					                       "",
					                       {},
					                       configDBType,
					                       false));
				}
				if (processRunBackupAgent(processMode)) {
					futures.push_back(runBackup(connRecord));
					futures.push_back(runDr(connRecord));
				}
				if (processRunHTTPServer(processMode)) {
					fmt::print("Process {0} run http server\n", ip.toString());
					futures.push_back(runSimHTTPServer());
				}

				futures.push_back(success(onShutdown));
				if (!g_simulator->globalHasSwitchedCluster() && g_simulator->hasSwitchedCluster(process->address)) {
					// When switching machines between clusters, a simultaneous
					// reboot followed by a reboot and switch can cause the
					// reboot and switch to be ignored. Handle this case by
					// sending the reboot and switch kill type when the process
					// comes back online.
					TraceEvent("RebootProcessAndSwitchLateReboot").detail("Address", process->address);
					g_simulator->switchCluster(process->address);
					process->shutdownSignal.send(ISimulator::KillType::RebootProcessAndSwitch);
					// Need to set the rebooting flag to true to keep the same behavior as MachineAttrition
					// Otherwise, a protected process, like a coordinator process can execute this code path but the
					// rebooting flag is still false, which breaks the assertion in g_simulator->destroyProcess
					process->rebooting = true;
				}
				wait(waitForAny(futures));
			} catch (Error& e) {
				// If in simulation, if we make it here with an error other than io_timeout but enASIOTimedOut is set
				// then somewhere an io_timeout was converted to a different error.
				if (g_network->isSimulated() && e.code() != error_code_io_timeout &&
				    (bool)g_network->global(INetwork::enASIOTimedOut))
					TraceEvent(SevError, "IOTimeoutErrorSuppressed")
					    .detail("ErrorCode", e.code())
					    .detail("RandomId", randomId)
					    .backtrace();

				if (e.code() == error_code_io_timeout && !onShutdown.isReady()) {
					onShutdown = ISimulator::KillType::RebootProcess;
				}

				if (onShutdown.isReady() && onShutdown.isError())
					throw onShutdown.getError();
				if (e.code() != error_code_actor_cancelled)
					printf("SimulatedFDBDTerminated: %s\n", e.what());
				ASSERT(destructed ||
				       g_simulator->getCurrentProcess() == process); // simulatedFDBD catch called on different process
				TraceEvent(e.code() == error_code_actor_cancelled || e.code() == error_code_file_not_found ||
				                   e.code() == error_code_incompatible_software_version || destructed
				               ? SevInfo
				               : SevError,
				           "SimulatedFDBDTerminated")
				    .errorUnsuppressed(e)
				    .detail("ZoneId", localities.zoneId());
			}

			TraceEvent("SimulatedFDBDDone")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("Address", process->address)
			    .detail("Excluded", process->excluded)
			    .detail("ZoneId", localities.zoneId())
			    .detail("KillType", onShutdown.isReady() ? onShutdown.get() : ISimulator::KillType::None);

			if (!onShutdown.isReady())
				onShutdown = ISimulator::KillType::InjectFaults;
		} catch (Error& e) {
			TraceEvent(destructed ? SevInfo : SevError, "SimulatedFDBDRebooterError")
			    .errorUnsuppressed(e)
			    .detail("ZoneId", localities.zoneId())
			    .detail("RandomId", randomId);
			onShutdown = e;
		}

		ASSERT(destructed || g_simulator->getCurrentProcess() == process);

		if (!process->shutdownSignal.isSet() && !destructed) {
			process->rebooting = true;
			process->shutdownSignal.send(ISimulator::KillType::None);
		}
		TraceEvent("SimulatedFDBDWait")
		    .detail("Cycles", cycles)
		    .detail("RandomId", randomId)
		    .detail("Address", process->address)
		    .detail("Excluded", process->excluded)
		    .detail("Rebooting", process->rebooting)
		    .detail("ZoneId", localities.zoneId());
		wait(g_simulator->onProcess(simProcess));

		wait(delay(0.00001 + FLOW_KNOBS->MAX_BUGGIFIED_DELAY)); // One last chance for the process to clean up?

		g_simulator->destroyProcess(
		    process); // Leak memory here; the process may be used in other parts of the simulation

		auto shutdownResult = onShutdown.get();
		TraceEvent("SimulatedFDBDShutdown")
		    .detail("Cycles", cycles)
		    .detail("RandomId", randomId)
		    .detail("Address", process->address)
		    .detail("Excluded", process->excluded)
		    .detail("ZoneId", localities.zoneId())
		    .detail("KillType", shutdownResult);

		if (shutdownResult < ISimulator::KillType::RebootProcessAndDelete) {
			TraceEvent("SimulatedFDBDLowerReboot")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("Address", process->address)
			    .detail("Excluded", process->excluded)
			    .detail("ZoneId", localities.zoneId())
			    .detail("KillType", shutdownResult);
			return onShutdown.get();
		}

		if (onShutdown.get() == ISimulator::KillType::RebootProcessAndDelete) {
			TraceEvent("SimulatedFDBDRebootAndDelete")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("Address", process->address)
			    .detail("ZoneId", localities.zoneId())
			    .detail("KillType", shutdownResult);
			*coordFolder = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
			*dataFolder = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
			platform::createDirectory(*dataFolder);

			if (!useSeedFile) {
				writeFile(joinPath(*dataFolder, "fdb.cluster"), connStr.toString());
				connRecord = makeReference<ClusterConnectionFile>(joinPath(*dataFolder, "fdb.cluster"));
			} else {
				connRecord =
				    makeReference<ClusterConnectionFile>(joinPath(*dataFolder, "fdb.cluster"), connStr.toString());
			}
		} else if (onShutdown.get() == ISimulator::KillType::RebootProcessAndSwitch) {
			TraceEvent("SimulatedFDBDRebootAndSwitch")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("Address", process->address)
			    .detail("ZoneId", localities.zoneId())
			    .detail("KillType", shutdownResult)
			    .detail("ConnectionString", connStr.toString())
			    .detail("OtherConnectionString", otherConnStr.toString())
			    .detail("SwitchingTo", g_simulator->hasSwitchedCluster(process->address))
			    .detail("MachineId", process->machine->machineId);

			// Handle the case where otherConnStr is '@'.
			if (otherConnStr.toString().size() > 1) {
				std::string newConnStr =
				    g_simulator->hasSwitchedCluster(process->address) ? otherConnStr.toString() : connStr.toString();
				connRecord = makeReference<ClusterConnectionFile>(joinPath(*dataFolder, "fdb.cluster"), newConnStr);
			}
		} else {
			TraceEvent("SimulatedFDBDJustRepeat")
			    .detail("Cycles", cycles)
			    .detail("RandomId", randomId)
			    .detail("Address", process->address)
			    .detail("ZoneId", localities.zoneId())
			    .detail("KillType", shutdownResult);
		}
	}
}

// Since a datacenter kill is considered to be the same as killing a machine, files cannot be swapped across datacenters
std::map<Optional<Standalone<StringRef>>, std::vector<std::vector<std::string>>> availableFolders;
// process count is no longer needed because it is now the length of the vector of ip's, because it was one ip per
// process
ACTOR Future<Void> simulatedMachine(ClusterConnectionString connStr,
                                    ClusterConnectionString otherConnStr,
                                    std::vector<IPAddress> ips,
                                    bool sslEnabled,
                                    LocalityData localities,
                                    ProcessClass processClass,
                                    std::string baseFolder,
                                    bool restarting,
                                    bool useSeedFile,
                                    ProcessMode processMode,
                                    bool sslOnly,
                                    std::string whitelistBinPaths,
                                    ProtocolVersion protocolVersion,
                                    ConfigDBType configDBType,
                                    bool isDr) {
	state int bootCount = 0;
	state std::vector<std::string> myFolders;
	state std::vector<std::string> coordFolders;
	state UID randomId = nondeterministicRandom()->randomUniqueID();
	state int listenPerProcess = (sslEnabled && !sslOnly) ? 2 : 1;

	try {
		CSimpleIni ini;
		ini.SetUnicode();
		ini.LoadFile(joinPath(baseFolder, "restartInfo.ini").c_str());

		for (int i = 0; i < ips.size(); i++) {
			if (restarting) {
				myFolders.push_back(
				    ini.GetValue(printable(localities.machineId()).c_str(),
				                 format("%d", i * listenPerProcess).c_str(),
				                 joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()).c_str()));

				if (i == 0) {
					std::string coordinationFolder =
					    ini.GetValue(printable(localities.machineId()).c_str(), "coordinationFolder", "");
					if (!coordinationFolder.size())
						coordinationFolder = ini.GetValue(
						    printable(localities.machineId()).c_str(),
						    format("c%d", i * listenPerProcess).c_str(),
						    joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()).c_str());
					coordFolders.push_back(coordinationFolder);
				} else {
					coordFolders.push_back(
					    ini.GetValue(printable(localities.machineId()).c_str(),
					                 format("c%d", i * listenPerProcess).c_str(),
					                 joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()).c_str()));
				}
			} else {
				coordFolders.push_back(joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()));
				std::string thisFolder = deterministicRandom()->randomUniqueID().toString();
				myFolders.push_back(joinPath(baseFolder, thisFolder));
				platform::createDirectory(myFolders[i]);

				if (!useSeedFile) {
					writeFile(joinPath(myFolders[i], "fdb.cluster"), connStr.toString());
				}
			}
		}

		loop {
			state std::vector<Future<ISimulator::KillType>> processes;
			for (int i = 0; i < ips.size(); i++) {
				std::string path = joinPath(myFolders[i], "fdb.cluster");
				ProcessMode ipProcessMode =
				    processMode == BackupAgentOnly ? (i == ips.size() - 1 ? BackupAgentOnly : FDBDOnly) : processMode;

				// for some reason http servers are having issues with seed files which doesn't matter because they
				// don't use the db
				Reference<IClusterConnectionRecord> clusterFile;
				if (ipProcessMode != SimHTTPServer) {
					// Fall back to use seed string if fdb.cluster not present
					// It can happen when a process failed before it persisted the connection string to disk
					clusterFile = Reference<IClusterConnectionRecord>(
					    useSeedFile || !fileExists(path) ? new ClusterConnectionFile(path, connStr.toString())
					                                     : new ClusterConnectionFile(path));
				}
				const int listenPort = i * listenPerProcess + 1;

				if (g_simulator->hasDiffProtocolProcess && !g_simulator->setDiffProtocol && ipProcessMode == FDBDOnly) {
					processes.push_back(simulatedFDBDRebooter(clusterFile,
					                                          ips[i],
					                                          sslEnabled,
					                                          listenPort,
					                                          listenPerProcess,
					                                          localities,
					                                          processClass,
					                                          &myFolders[i],
					                                          &coordFolders[i],
					                                          baseFolder,
					                                          connStr,
					                                          otherConnStr,
					                                          useSeedFile,
					                                          ipProcessMode,
					                                          whitelistBinPaths,
					                                          protocolVersion,
					                                          configDBType,
					                                          isDr));
					g_simulator->setDiffProtocol = true;
				} else {
					processes.push_back(simulatedFDBDRebooter(clusterFile,
					                                          ips[i],
					                                          sslEnabled,
					                                          listenPort,
					                                          listenPerProcess,
					                                          localities,
					                                          processClass,
					                                          &myFolders[i],
					                                          &coordFolders[i],
					                                          baseFolder,
					                                          connStr,
					                                          otherConnStr,
					                                          useSeedFile,
					                                          ipProcessMode,
					                                          whitelistBinPaths,
					                                          g_network->protocolVersion(),
					                                          configDBType,
					                                          isDr));
				}
				TraceEvent("SimulatedMachineProcess", randomId)
				    .detail("Address", NetworkAddress(ips[i], listenPort, true, false))
				    .detail("ZoneId", localities.zoneId())
				    .detail("DataHall", localities.dataHallId())
				    .detail("Folder", myFolders[i]);
			}

			CODE_PROBE(bootCount >= 1, "Simulated machine rebooted");
			CODE_PROBE(bootCount >= 2, "Simulated machine rebooted twice");
			CODE_PROBE(bootCount >= 3, "Simulated machine rebooted three times", probe::decoration::rare);
			++bootCount;

			TraceEvent("SimulatedMachineStart", randomId)
			    .detail("Folder0", myFolders[0])
			    .detail("CFolder0", coordFolders[0])
			    .detail("MachineIPs", toIPVectorString(ips))
			    .detail("SSL", sslEnabled)
			    .detail("Processes", processes.size())
			    .detail("BootCount", bootCount)
			    .detail("ProcessClass", processClass.toString())
			    .detail("Restarting", restarting)
			    .detail("UseSeedFile", useSeedFile)
			    .detail("ZoneId", localities.zoneId())
			    .detail("DataHall", localities.dataHallId())
			    .detail("Locality", localities.toString());

			wait(waitForAll(processes));

			TraceEvent("SimulatedMachineRebootStart", randomId)
			    .detail("Folder0", myFolders[0])
			    .detail("CFolder0", coordFolders[0])
			    .detail("MachineIPs", toIPVectorString(ips))
			    .detail("ZoneId", localities.zoneId())
			    .detail("DataHall", localities.dataHallId());

			{
				// Kill all open files, which may cause them to write invalid data.
				auto& machineCache = g_simulator->getMachineById(localities.machineId())->openFiles;

				// Copy the file pointers to a vector because the map may be modified while we are killing files
				std::vector<AsyncFileNonDurable*> files;
				for (auto fileItr = machineCache.begin(); fileItr != machineCache.end(); ++fileItr) {
					ASSERT(fileItr->second.get().isReady());
					files.push_back((AsyncFileNonDurable*)fileItr->second.get().get().getPtr());
				}

				std::vector<Future<Void>> killFutures;
				for (auto fileItr = files.begin(); fileItr != files.end(); ++fileItr)
					killFutures.push_back((*fileItr)->kill());

				wait(waitForAll(killFutures));
			}

			state std::set<std::string> filenames;
			state std::string closingStr;
			auto& machineCache = g_simulator->getMachineById(localities.machineId())->openFiles;
			for (auto it : machineCache) {
				filenames.insert(it.first);
				closingStr += it.first + ", ";
				ASSERT(it.second.get().canGet());
			}

			for (auto it : g_simulator->getMachineById(localities.machineId())->deletingOrClosingFiles) {
				filenames.insert(it);
				closingStr += it + ", ";
			}

			TraceEvent("SimulatedMachineRebootAfterKills", randomId)
			    .detail("Folder0", myFolders[0])
			    .detail("CFolder0", coordFolders[0])
			    .detail("MachineIPs", toIPVectorString(ips))
			    .detail("Closing", closingStr)
			    .detail("ZoneId", localities.zoneId())
			    .detail("DataHall", localities.dataHallId());

			ISimulator::MachineInfo* machine = g_simulator->getMachineById(localities.machineId());
			machine->closingFiles = filenames;
			g_simulator->getMachineById(localities.machineId())->openFiles.clear();

			// During a reboot:
			//   The process is expected to close all files and be inactive in zero time, but not necessarily
			//   without delay(0)-equivalents, so delay(0) a few times waiting for it to achieve that goal.
			// After an injected fault:
			//   The process is expected to shut down eventually, but not necessarily instantly.  Wait up to 60 seconds.
			state int shutdownDelayCount = 0;
			state double backoff = 0;
			loop {
				auto& machineCache = g_simulator->getMachineById(localities.machineId())->closingFiles;

				if (!machineCache.empty()) {
					std::string openFiles;
					int i = 0;
					for (auto it = machineCache.begin(); it != machineCache.end() && i < 5; ++it) {
						openFiles += *it + ", ";
						i++;
					}
					TraceEvent("MachineFilesOpen", randomId)
					    .detail("PAddr", toIPVectorString(ips))
					    .detail("OpenFiles", openFiles);
				} else
					break;

				if (shutdownDelayCount++ >= 50) { // Worker doesn't shut down instantly on reboot
					TraceEvent(SevError, "SimulatedFDBDFilesCheck", randomId)
					    .detail("PAddrs", toIPVectorString(ips))
					    .detail("ZoneId", localities.zoneId())
					    .detail("DataHall", localities.dataHallId());
					ASSERT(false);
				}

				wait(delay(backoff));
				backoff = std::min(backoff + 1.0, 6.0);
			}

			TraceEvent("SimulatedFDBDFilesClosed", randomId)
			    .detail("Address", toIPVectorString(ips))
			    .detail("ZoneId", localities.zoneId())
			    .detail("DataHall", localities.dataHallId());

			g_simulator->destroyMachine(localities.machineId());

			// SOMEDAY: when processes can be rebooted, this check will be needed
			// ASSERT( this machine is rebooting );

			// Since processes can end with different codes, take the highest (least severe) to detmine what to do
			state ISimulator::KillType killType = processes[0].get();
			for (int i = 1; i < ips.size(); i++)
				killType = std::max(processes[i].get(), killType);

			CODE_PROBE(true, "Simulated machine has been rebooted");

			state bool swap = killType == ISimulator::KillType::Reboot && BUGGIFY_WITH_PROB(0.75) &&
			                  g_simulator->canSwapToMachine(localities.zoneId());
			if (swap)
				availableFolders[localities.dcId()].push_back(myFolders);

			auto rebootTime = deterministicRandom()->random01() * MACHINE_REBOOT_TIME;

			TraceEvent("SimulatedMachineShutdown", randomId)
			    .detail("Swap", swap)
			    .detail("KillType", killType)
			    .detail("RebootTime", rebootTime)
			    .detail("ZoneId", localities.zoneId())
			    .detail("DataHall", localities.dataHallId())
			    .detail("MachineIPs", toIPVectorString(ips));

			wait(delay(rebootTime));

			if (swap) {
				auto& avail = availableFolders[localities.dcId()];
				int i = deterministicRandom()->randomInt(0, avail.size());
				if (i != avail.size() - 1)
					std::swap(avail[i], avail.back());
				auto toRebootFrom = avail.back();
				avail.pop_back();

				if (myFolders != toRebootFrom) {
					CODE_PROBE(true, "Simulated machine swapped data folders", probe::decoration::rare);
					TraceEvent("SimulatedMachineFolderSwap", randomId)
					    .detail("OldFolder0", myFolders[0])
					    .detail("NewFolder0", toRebootFrom[0])
					    .detail("MachineIPs", toIPVectorString(ips));
				}
				myFolders = toRebootFrom;
				if (!useSeedFile) {
					for (auto f : toRebootFrom) {
						if (!fileExists(joinPath(f, "fdb.cluster"))) {
							writeFile(joinPath(f, "fdb.cluster"), connStr.toString());
						}
					}
				}
			} else if (killType == ISimulator::KillType::RebootAndDelete) {
				for (int i = 0; i < ips.size(); i++) {
					coordFolders[i] = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
					myFolders[i] = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
					platform::createDirectory(myFolders[i]);

					if (!useSeedFile) {
						writeFile(joinPath(myFolders[i], "fdb.cluster"), connStr.toString());
					}
				}

				CODE_PROBE(true, "Simulated machine rebooted with data loss");
			}

			// this machine is rebooting = false;
		}
	} catch (Error& e) {
		g_simulator->getMachineById(localities.machineId())->openFiles.clear();
		throw;
	}
}

IPAddress makeIPAddressForSim(bool isIPv6, std::array<int, 4> parts) {
	if (isIPv6) {
		IPAddress::IPAddressStore addrStore{ 0xAB, 0xCD };
		uint16_t* ptr = (uint16_t*)addrStore.data();
		ptr[4] = (uint16_t)(parts[0] << 8);
		ptr[5] = (uint16_t)(parts[1] << 8);
		ptr[6] = (uint16_t)(parts[2] << 8);
		ptr[7] = (uint16_t)(parts[3] << 8);
		return IPAddress(addrStore);
	} else {
		return IPAddress(parts[0] << 24 | parts[1] << 16 | parts[2] << 8 | parts[3]);
	}
}

#include "fdbclient/MonitorLeader.h"

// Configures the system according to the given specifications in order to run
// simulation, but with the additional consideration that it is meant to act
// like a "rebooted" machine, mostly used for restarting tests.
ACTOR Future<Void> restartSimulatedSystem(std::vector<Future<Void>>* systemActors,
                                          std::string baseFolder,
                                          int* pTesterCount,
                                          Optional<ClusterConnectionString>* pConnString,
                                          Standalone<StringRef>* pStartingConfiguration,
                                          TestConfig* testConfig,
                                          std::string whitelistBinPaths,
                                          ProtocolVersion protocolVersion) {
	CSimpleIni ini;
	ini.SetUnicode();
	ini.LoadFile(joinPath(baseFolder, "restartInfo.ini").c_str());

	auto configDBType = testConfig->getConfigDBType();

	// Randomly change data center id names to test that localities
	// can be modified on cluster restart
	bool renameZoneIds = testConfig->randomlyRenameZoneId ? deterministicRandom()->random01() < 0.1 : false;
	CODE_PROBE(renameZoneIds, "Zone ID names altered in restart test");

	// allows multiple ipAddr entries
	ini.SetMultiKey();

	try {
		int machineCount = atoi(ini.GetValue("META", "machineCount"));
		int processesPerMachine = atoi(ini.GetValue("META", "processesPerMachine"));
		int listenersPerProcess = 1;
		auto listenersPerProcessStr = ini.GetValue("META", "listenersPerProcess");
		if (listenersPerProcessStr != nullptr) {
			listenersPerProcess = atoi(listenersPerProcessStr);
		}
		int desiredCoordinators = atoi(ini.GetValue("META", "desiredCoordinators"));
		int testerCount = atoi(ini.GetValue("META", "testerCount"));
		auto tssModeStr = ini.GetValue("META", "tssMode");
		auto tenantMode = ini.GetValue("META", "tenantMode");
		if (tenantMode != nullptr) {
			CODE_PROBE(true, "Restarting test with tenant mode set");
			testConfig->tenantModes.push_back(tenantMode);
		}
		std::string defaultTenant = ini.GetValue("META", "defaultTenant", "");
		if (!defaultTenant.empty()) {
			CODE_PROBE(true, "Restarting test with default tenant set");
			testConfig->defaultTenant = defaultTenant;
		}
		if (tssModeStr != nullptr) {
			g_simulator->tssMode = (ISimulator::TSSMode)atoi(tssModeStr);
		}
		ClusterConnectionString conn(ini.GetValue("META", "connectionString"));
		if (testConfig->extraDatabaseMode == ISimulator::ExtraDatabaseMode::Local) {
			g_simulator->extraDatabases.clear();
			g_simulator->extraDatabases.push_back(conn.toString());
		}
		if (!testConfig->disableHostname) {
			auto mockDNSStr = ini.GetValue("META", "mockDNS");
			if (mockDNSStr != nullptr) {
				INetworkConnections::net()->parseMockDNSFromString(mockDNSStr);
			}
		}
		auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
		if (testConfig->disableRemoteKVS) {
			g_knobs.setKnob("remote_kv_store", KnobValueRef::create(bool{ false }));
			TraceEvent(SevDebug, "DisableRemoteKVS");
		}
		*pConnString = conn;
		*pTesterCount = testerCount;
		bool usingSSL = conn.toString().find(":tls") != std::string::npos || listenersPerProcess > 1;
		int useSeedForMachine = deterministicRandom()->randomInt(0, machineCount);
		std::vector<std::string> dcIds;
		for (int i = 0; i < machineCount; i++) {
			Optional<Standalone<StringRef>> dcUID;
			Optional<Standalone<StringRef>> zoneId;
			std::string machineIdString = ini.GetValue("META", format("%d", i).c_str());
			Standalone<StringRef> machineId = StringRef(machineIdString);

			std::string dcUIDini = ini.GetValue(machineIdString.c_str(), "dcUID");
			if (!dcUIDini.empty()) {
				dcUID = StringRef(dcUIDini);
			}

			auto zoneIDini = ini.GetValue(machineIdString.c_str(), "zoneId");
			if (zoneIDini == nullptr) {
				zoneId = machineId;
			} else {
				auto zoneIdStr = std::string(zoneIDini);
				if (renameZoneIds) {
					zoneIdStr = "modified/" + zoneIdStr;
				}
				zoneId = Standalone<StringRef>(zoneIdStr);
			}

			ProcessClass::ClassType cType =
			    (ProcessClass::ClassType)(atoi(ini.GetValue(machineIdString.c_str(), "mClass")));
			// using specialized class types can lead to nondeterministic recruitment
			if (cType == ProcessClass::MasterClass || cType == ProcessClass::ResolutionClass) {
				cType = ProcessClass::StatelessClass;
			}
			ProcessClass processClass = ProcessClass(cType, ProcessClass::CommandLineSource);

			if (processClass != ProcessClass::TesterClass) {
				dcIds.push_back(dcUIDini);
			}

			std::vector<IPAddress> ipAddrs;
			int processes = atoi(ini.GetValue(machineIdString.c_str(), "processes"));

			auto ip = ini.GetValue(machineIdString.c_str(), "ipAddr");

			// Helper to translate the IP address stored in INI file to out IPAddress representation.
			// After IPv6 work, we store the actual string representation of IP address, however earlier, it was
			// instead the 32 bit integer value.
			auto parseIp = [](const char* ipStr) -> IPAddress {
				Optional<IPAddress> parsedIp = IPAddress::parse(ipStr);
				if (parsedIp.present()) {
					return parsedIp.get();
				} else {
					return IPAddress(strtoul(ipStr, nullptr, 10));
				}
			};

			if (ip == nullptr) {
				for (int i = 0; i < processes; i++) {
					const char* val =
					    ini.GetValue(machineIdString.c_str(), format("ipAddr%d", i * listenersPerProcess).c_str());
					ipAddrs.push_back(parseIp(val));
				}
			} else {
				// old way
				ipAddrs.push_back(parseIp(ip));

				for (int i = 1; i < processes; i++) {
					if (ipAddrs.back().isV6()) {
						IPAddress::IPAddressStore store = ipAddrs.back().toV6();
						uint16_t* ptr = (uint16_t*)store.data();
						ptr[7] += 1;
						ipAddrs.push_back(IPAddress(store));
					} else {
						ipAddrs.push_back(IPAddress(ipAddrs.back().toV4() + 1));
					}
				}
			}

			LocalityData localities(Optional<Standalone<StringRef>>(), zoneId, machineId, dcUID);
			localities.set("data_hall"_sr, dcUID);

			// SOMEDAY: parse backup agent from test file
			systemActors->push_back(reportErrors(
			    simulatedMachine(conn,
			                     ClusterConnectionString(),
			                     ipAddrs,
			                     usingSSL,
			                     localities,
			                     processClass,
			                     baseFolder,
			                     true,
			                     i == useSeedForMachine,
			                     processClass == ProcessClass::SimHTTPServerClass ? SimHTTPServer : FDBDAndBackupAgent,
			                     usingSSL && (listenersPerProcess == 1 || processClass == ProcessClass::TesterClass),
			                     whitelistBinPaths,
			                     protocolVersion,
			                     configDBType,
			                     false),
			    processClass == ProcessClass::TesterClass ? "SimulatedTesterMachine" : "SimulatedMachine"));
		}

		g_simulator->desiredCoordinators = desiredCoordinators;
		g_simulator->processesPerMachine = processesPerMachine;

		uniquify(dcIds);
		if (!BUGGIFY && dcIds.size() == 2 && dcIds[0] != "" && dcIds[1] != "") {
			StatusObject primaryObj;
			StatusObject primaryDcObj;
			primaryDcObj["id"] = dcIds[0];
			primaryDcObj["priority"] = 2;
			StatusArray primaryDcArr;
			primaryDcArr.push_back(primaryDcObj);

			StatusObject remoteObj;
			StatusObject remoteDcObj;
			remoteDcObj["id"] = dcIds[1];
			remoteDcObj["priority"] = 1;
			StatusArray remoteDcArr;
			remoteDcArr.push_back(remoteDcObj);

			primaryObj["datacenters"] = primaryDcArr;
			remoteObj["datacenters"] = remoteDcArr;

			StatusArray regionArr;
			regionArr.push_back(primaryObj);
			regionArr.push_back(remoteObj);

			*pStartingConfiguration =
			    "single usable_regions=2 regions=" +
			    json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none);
		}

		g_simulator->restarted = true;

		TraceEvent("RestartSimulatorSettings")
		    .detail("DesiredCoordinators", g_simulator->desiredCoordinators)
		    .detail("ProcessesPerMachine", g_simulator->processesPerMachine)
		    .detail("ListenersPerProcess", listenersPerProcess);
	} catch (Error& e) {
		TraceEvent(SevError, "RestartSimulationError").error(e);
	}

	wait(delay(1.0));

	return Void();
}

// Configuration details compiled in a structure used when setting up a simulated cluster
struct SimulationConfig : public BasicSimulationConfig {
	explicit SimulationConfig(const TestConfig& testConfig);
	ISimulator::ExtraDatabaseMode extraDatabaseMode;
	int extraDatabaseCount;
	bool generateFearless;

	void set_config(std::string config);

	int coordinators;

private:
	void setRandomConfig();
	void setSimpleConfig();
	void setSpecificConfig(const TestConfig& testConfig);
	void setDatacenters(const TestConfig& testConfig);
	void setTenantMode(const TestConfig& testConfig);
	void setEncryptionAtRestMode(const TestConfig& testConfig);
	void setStorageEngine(const TestConfig& testConfig);
	void setRegions(const TestConfig& testConfig);
	void setReplicationType(const TestConfig& testConfig);
	void setMachineCount(const TestConfig& testConfig);
	void setCoordinators(const TestConfig& testConfig);
	void setProcessesPerMachine(const TestConfig& testConfig);
	void setTss(const TestConfig& testConfig);
	void generateNormalConfig(const TestConfig& testConfig);
};

SimulationConfig::SimulationConfig(const TestConfig& testConfig)
  : extraDatabaseMode(testConfig.extraDatabaseMode), extraDatabaseCount(testConfig.extraDatabaseCount) {
	generateNormalConfig(testConfig);
}

void SimulationConfig::set_config(std::string config) {
	// The only mechanism we have for turning "single" into what single means
	// is buildConfiguration()... :/
	std::map<std::string, std::string> hack_map;
	const auto buildResult = buildConfiguration(config, hack_map);
	ASSERT(buildResult != ConfigurationResult::NO_OPTIONS_PROVIDED);
	for (auto kv : hack_map)
		db.set(kv.first, kv.second);
}

[[maybe_unused]] StringRef StringRefOf(const char* s) {
	return StringRef((uint8_t*)s, strlen(s));
}

// Set the randomly generated options of the config. Compiled here to easily observe and trace random options
void SimulationConfig::setRandomConfig() {
	if (deterministicRandom()->random01() < 0.25) {
		db.desiredTLogCount = deterministicRandom()->randomInt(1, 7);
	}
	if (deterministicRandom()->random01() < 0.25) {
		db.commitProxyCount = deterministicRandom()->randomInt(1, 7);
	}
	if (deterministicRandom()->random01() < 0.25) {
		db.grvProxyCount = deterministicRandom()->randomInt(1, 4);
	}
	if (deterministicRandom()->random01() < 0.25) {
		db.resolverCount = deterministicRandom()->randomInt(1, 7);
	}
	// TraceEvent("SimulatedConfigRandom")
	// 	.detail("DesiredTLogCount", db.desiredTLogCount)
	// 	.detail("CommitProxyCount", db.commitProxyCount)
	// 	.detail("GRVProxyCount", db.grvProxyCount)
	// 	.detail("ResolverCount", db.resolverCount);

	if (deterministicRandom()->random01() < 0.5) {
		// TraceEvent("SimulatedConfigRandom").detail("PerpetualWiggle", 0);
		set_config("perpetual_storage_wiggle=0");
	} else {
		// TraceEvent("SimulatedConfigRandom").detail("PerpetualWiggle", 1);
		set_config("perpetual_storage_wiggle=1");
	}

	if (deterministicRandom()->random01() < 0.5) {
		set_config("backup_worker_enabled:=1");
	}
}

// Overwrite DB with simple options, used when simpleConfig is true in the TestConfig
void SimulationConfig::setSimpleConfig() {
	db.desiredTLogCount = 1;
	db.commitProxyCount = 1;
	db.grvProxyCount = 1;
	db.resolverCount = 1;
}

// Overwrite previous options with ones specified by TestConfig
void SimulationConfig::setSpecificConfig(const TestConfig& testConfig) {
	if (testConfig.desiredTLogCount.present()) {
		db.desiredTLogCount = testConfig.desiredTLogCount.get();
	}
	if (testConfig.remoteDesiredTLogCount.present()) {
		db.remoteDesiredTLogCount = testConfig.remoteDesiredTLogCount.get();
	}
	if (testConfig.commitProxyCount.present()) {
		db.commitProxyCount = testConfig.commitProxyCount.get();
	}
	if (testConfig.grvProxyCount.present()) {
		db.grvProxyCount = testConfig.grvProxyCount.get();
	}
	if (testConfig.resolverCount.present()) {
		db.resolverCount = testConfig.resolverCount.get();
	}
	db.blobGranulesEnabled = testConfig.blobGranulesEnabled;
}

// Sets generateFearless and number of dataCenters based on testConfig details
// The number of datacenters may be overwritten in setRegions
void SimulationConfig::setDatacenters(const TestConfig& testConfig) {

#ifdef NO_MULTIREGION_TEST
	if (testConfig.minimumRegions > 1 || (testConfig.generateFearless.present() && testConfig.generateFearless.get())) {
		throw internal_error_msg("Test requires multi-region while the flag is turned off in the build process");
	}
	generateFearless = false;
#else
	generateFearless =
	    testConfig.generateFearless.present()
	        ? testConfig.generateFearless.get()
	        : (!testConfig.simpleConfig && (testConfig.minimumRegions > 1 || deterministicRandom()->random01() < 0.5));
#endif
	datacenters =
	    testConfig.simpleConfig
	        ? 1
	        : (generateFearless ? (testConfig.minimumReplication > 0 || deterministicRandom()->random01() < 0.5 ? 4 : 6)
	                            : deterministicRandom()->randomInt(1, 4));

	// Overwrite with specific option if present
	if (testConfig.datacenters.present()) {
		datacenters = testConfig.datacenters.get();
	}
}

void SimulationConfig::setTenantMode(const TestConfig& testConfig) {
	TenantMode tenantMode = TenantMode::DISABLED;
	if (testConfig.tenantModes.size() > 0) {
		tenantMode = TenantMode::fromString(deterministicRandom()->randomChoice(testConfig.tenantModes));
	} else if (testConfig.allowDefaultTenant && deterministicRandom()->coinflip()) {
		tenantMode = deterministicRandom()->random01() < 0.9 ? TenantMode::REQUIRED : TenantMode::OPTIONAL_TENANT;
	} else if (deterministicRandom()->coinflip()) {
		tenantMode = TenantMode::OPTIONAL_TENANT;
	}
	set_config("tenant_mode=" + tenantMode.toString());
}

void SimulationConfig::setEncryptionAtRestMode(const TestConfig& testConfig) {
	// Enable encryption siginificantly reduces chance that a storage engine other than redwood got selected
	// We want storage servers are selected by simulation tests with roughly equal chance
	// If encryptMode is not specified explicitly, with high probability, we disable encryption
	if (testConfig.encryptModes.empty() &&
	    deterministicRandom()->random01() < SERVER_KNOBS->DISABLED_ENCRYPTION_PROBABILITY_SIM) {
		EncryptionAtRestMode encryptionMode = EncryptionAtRestMode::DISABLED;
		TraceEvent("SimulatedClusterEncryptionMode").detail("Mode", encryptionMode.toString());
		CODE_PROBE(true, "Enforce to disable encryption in simulation", probe::decoration::rare);
		set_config("encryption_at_rest_mode=" + encryptionMode.toString());
		return;
	}

	std::vector<bool> available;
	std::vector<double> probability;
	if (!testConfig.encryptModes.empty()) {
		// If encryptModes are specified explicitly, give them equal probability to be chosen.
		available = std::vector<bool>(EncryptionAtRestMode::END, false);
		probability = std::vector<double>(EncryptionAtRestMode::END, 0);
		for (auto& mode : testConfig.encryptModes) {
			available[EncryptionAtRestMode::fromString(mode).mode] = true;
			probability[EncryptionAtRestMode::fromString(mode).mode] = 1.0 / testConfig.encryptModes.size();
		}
	} else {
		// If encryptModes are not specified, give encryption higher chance to be enabled.
		// The good thing is testing with encryption on doesn't loss test coverage for most of the other features.
		available = std::vector<bool>(EncryptionAtRestMode::END, true);
		// Enabling encryption require the use of Redwood storage engine, but we don't want to test with Redwood with
		// high probability in simulation. Setting total probability of encryption being enabled to be close to 1/6,
		// since we have 6 storage engine type currently.
		probability = { 0.85, 0.1, 0.05 };
		// Only Redwood support encryption. Disable encryption if Redwood is not available.
		if ((testConfig.storageEngineType.present() &&
		     testConfig.storageEngineType != SimulationStorageEngine::REDWOOD) ||
		    testConfig.excludedStorageEngineType(SimulationStorageEngine::REDWOOD)) {
			available[(int)EncryptionAtRestMode::DOMAIN_AWARE] = false;
			available[(int)EncryptionAtRestMode::CLUSTER_AWARE] = false;
		}
	}
	// domain_aware mode is supported only with required tenant mode.
	if (db.tenantMode != TenantMode::REQUIRED) {
		available[(int)EncryptionAtRestMode::DOMAIN_AWARE] = false;
	}
	int lastAvailableMode = EncryptionAtRestMode::END;
	double totalProbability = 0;
	for (int mode = 0; mode < (int)EncryptionAtRestMode::END; mode++) {
		if (available[mode]) {
			lastAvailableMode = mode;
			totalProbability += probability[mode];
		}
	}
	ASSERT(lastAvailableMode != EncryptionAtRestMode::END); // At least one mode available
	double r = deterministicRandom()->random01() * totalProbability;
	EncryptionAtRestMode encryptionMode;
	for (int mode = 0;; mode++) {
		if (available[mode] && (r < probability[mode] || mode == lastAvailableMode)) {
			encryptionMode = (EncryptionAtRestMode::Mode)mode;
			break;
		}
		r -= probability[mode];
	}
	TraceEvent("SimulatedClusterEncryptionMode").detail("Mode", encryptionMode.toString());
	CODE_PROBE(
	    encryptionMode == EncryptionAtRestMode::DISABLED, "Disabled encryption in simulation", probe::decoration::rare);
	CODE_PROBE(encryptionMode == EncryptionAtRestMode::CLUSTER_AWARE,
	           "Enabled cluster-aware encryption in simulation",
	           probe::decoration::rare);
	CODE_PROBE(encryptionMode == EncryptionAtRestMode::DOMAIN_AWARE,
	           "Enabled domain-aware encryption in simulation",
	           probe::decoration::rare);
	set_config("encryption_at_rest_mode=" + encryptionMode.toString());
}

namespace {

using StorageEngineConfigFunc = void (*)(SimulationConfig*);

void ssdStorageEngineConfig(SimulationConfig* simCfg) {
	CODE_PROBE(true, "Simulated cluster using ssd storage engine");
	simCfg->set_config("ssd");
}
void memoryStorageEngineConfig(SimulationConfig* simCfg) {
	CODE_PROBE(true, "Simulated cluster using default memory storage engine");
	simCfg->set_config("memory");
}

void radixTreeStorageEngineConfig(SimulationConfig* simCfg) {
	CODE_PROBE(true, "Simulated cluster using radix-tree storage engine");
	simCfg->set_config("memory-radixtree");
}

void redwoodStorageEngineConfig(SimulationConfig* simCfg) {
	CODE_PROBE(true, "Simulated cluster using redwood storage engine");
	// The experimental suffix is still supported so test it randomly
	simCfg->set_config("ssd-redwood-1");
}

void rocksdbStorageEngineConfig(SimulationConfig* simCfg) {
	CODE_PROBE(true, "Simulated cluster using RocksDB storage engine", probe::assert::hasRocksDB);
	simCfg->set_config("ssd-rocksdb-v1");
}

void shardedRocksDBStorageEngineConfig(SimulationConfig* simCfg) {
	CODE_PROBE(true, "Simulated cluster using Sharded RocksDB storage engine", probe::assert::hasRocksDB);
	simCfg->set_config("encryption_at_rest_mode=disabled");
	simCfg->set_config("ssd-sharded-rocksdb");
}

const std::unordered_map<SimulationStorageEngine, StorageEngineConfigFunc> STORAGE_ENGINE_CONFIG_MAPPER = {
	{ SimulationStorageEngine::SSD, ssdStorageEngineConfig },
	{ SimulationStorageEngine::MEMORY, memoryStorageEngineConfig },
	{ SimulationStorageEngine::RADIX_TREE, radixTreeStorageEngineConfig },
	{ SimulationStorageEngine::REDWOOD, redwoodStorageEngineConfig },
	{ SimulationStorageEngine::ROCKSDB, rocksdbStorageEngineConfig },
	{ SimulationStorageEngine::SHARDED_ROCKSDB, shardedRocksDBStorageEngineConfig }
};

// TODO: Figure out what is broken with the RocksDB engine in simulation.
const std::vector<SimulationStorageEngine> SIMULATION_STORAGE_ENGINE = {
	SimulationStorageEngine::SSD,        SimulationStorageEngine::MEMORY,
	SimulationStorageEngine::RADIX_TREE, SimulationStorageEngine::REDWOOD,
#ifdef WITH_ROCKSDB
	SimulationStorageEngine::ROCKSDB,    SimulationStorageEngine::SHARDED_ROCKSDB,
#endif
};

std::string getExcludedStorageEngineTypesInString(const std::set<SimulationStorageEngine>& excluded) {
	std::string str;
	for (const auto& e : excluded) {
		str += std::to_string(static_cast<uint32_t>(e));
		str += ',';
	}
	if (!excluded.empty())
		str.pop_back();
	return str;
}

SimulationStorageEngine chooseSimulationStorageEngine(const TestConfig& testConfig, const bool isEncryptionEnabled) {
	StringRef reason;
	SimulationStorageEngine result = SimulationStorageEngine::SIMULATION_STORAGE_ENGINE_INVALID_VALUE;

	if (isEncryptionEnabled) {
		// Only storage engine supporting encryption is Redwood.
		reason = "EncryptionEnabled"_sr;
		result = SimulationStorageEngine::REDWOOD;

	} else if (testConfig.storageEngineType.present()) {
		reason = "ConfigureSpecified"_sr;
		result = testConfig.storageEngineType.get();
		if (testConfig.excludedStorageEngineType(result) ||
		    std::find(std::begin(SIMULATION_STORAGE_ENGINE), std::end(SIMULATION_STORAGE_ENGINE), result) ==
		        std::end(SIMULATION_STORAGE_ENGINE)) {

			TraceEvent(SevError, "StorageEngineNotSupported").detail("StorageEngineType", result);
			ASSERT(false);
		}

	} else if (SERVER_KNOBS->ENFORCE_SHARDED_ROCKSDB_SIM_IF_AVALIABLE &&
	           testConfig.storageEngineExcludeTypes.find(SimulationStorageEngine::SHARDED_ROCKSDB) ==
	               testConfig.storageEngineExcludeTypes.end()) {
		reason = "ENFORCE_SHARDED_ROCKSDB_SIM_IF_AVALIABLE is enabled"_sr;
		result = SimulationStorageEngine::SHARDED_ROCKSDB;

	} else {
		std::unordered_set<SimulationStorageEngine> storageEngineAvailable;
		for (const auto& storageEngine : SIMULATION_STORAGE_ENGINE) {
			storageEngineAvailable.insert(storageEngine);
		}
		for (const auto& storageEngineExcluded : testConfig.storageEngineExcludeTypes) {
			storageEngineAvailable.erase(storageEngineExcluded);
		}
		ASSERT(storageEngineAvailable.size() > 0);
		std::vector<SimulationStorageEngine> storageEngineCandidates;
		for (const auto& storageEngine : storageEngineAvailable) {
			if (storageEngine == SimulationStorageEngine::MEMORY) {
				// Adjust the chance that Memory is selected
				storageEngineCandidates.insert(
				    storageEngineCandidates.end(), SERVER_KNOBS->PROBABILITY_FACTOR_MEMORY_SELECTED_SIM, storageEngine);
			} else if (storageEngine == SimulationStorageEngine::SHARDED_ROCKSDB) {
				// Adjust the chance that ShardedRocksDB is selected
				storageEngineCandidates.insert(storageEngineCandidates.end(),
				                               SERVER_KNOBS->PROBABILITY_FACTOR_SHARDED_ROCKSDB_ENGINE_SELECTED_SIM,
				                               storageEngine);
			} else if (storageEngine == SimulationStorageEngine::SSD) {
				// Adjust the chance that SQLite is selected
				storageEngineCandidates.insert(storageEngineCandidates.end(),
				                               SERVER_KNOBS->PROBABILITY_FACTOR_SQLITE_ENGINE_SELECTED_SIM,
				                               storageEngine);
			} else if (storageEngine == SimulationStorageEngine::ROCKSDB) {
				// Adjust the chance that RocksDB is selected
				storageEngineCandidates.insert(storageEngineCandidates.end(),
				                               SERVER_KNOBS->PROBABILITY_FACTOR_ROCKSDB_ENGINE_SELECTED_SIM,
				                               storageEngine);
			} else {
				storageEngineCandidates.push_back(storageEngine);
			}
		}
		reason = "RandomlyChosen"_sr;
		result = deterministicRandom()->randomChoice(storageEngineCandidates);
		if (result == SimulationStorageEngine::SIMULATION_STORAGE_ENGINE_INVALID_VALUE) {
			UNREACHABLE();
		}
	}

	TraceEvent(SevInfo, "SimulationStorageEngine")
	    .detail("StorageEngine", static_cast<uint8_t>(result))
	    .detail("Reason", reason)
	    .detail("Excluded", getExcludedStorageEngineTypesInString(testConfig.storageEngineExcludeTypes))
	    .detail("RocksDBEngineChoosable", hasRocksDB);

	return result;
}

} // anonymous namespace

// Sets storage engine based on testConfig details
void SimulationConfig::setStorageEngine(const TestConfig& testConfig) {
	auto storageEngineType = chooseSimulationStorageEngine(testConfig, db.encryptionAtRestMode.isEncryptionEnabled());
	STORAGE_ENGINE_CONFIG_MAPPER.at(storageEngineType)(this);
}

// Sets replication type and TLogSpillType and Version
void SimulationConfig::setReplicationType(const TestConfig& testConfig) {
	replication_type = testConfig.simpleConfig
	                       ? 1
	                       : (std::max(testConfig.minimumReplication,
	                                   datacenters > 4 ? deterministicRandom()->randomInt(1, 3)
	                                                   : std::min(deterministicRandom()->randomInt(0, 6), 3)));
	if (testConfig.config.present()) {
		set_config(testConfig.config.get());
	} else {
		switch (replication_type) {
		case 0: {
			CODE_PROBE(true, "Simulated cluster using custom redundancy mode");
			int storage_servers = deterministicRandom()->randomInt(1, generateFearless ? 4 : 5);
			// FIXME: log replicas must be more than storage replicas because otherwise better master exists will not
			// recognize it needs to change dcs
			int replication_factor = deterministicRandom()->randomInt(storage_servers, generateFearless ? 4 : 5);
			// Version vector is an experimental feature that does not support logWriteAntiQuorum
			// feature. Disable logWriteAntiQuorum (when version vector is enabled) in simulation
			// tests for now.
			// @todo extend version vector to support logWriteAntiQuorum feature.
			int anti_quorum =
			    SERVER_KNOBS->ENABLE_VERSION_VECTOR
			        ? 0
			        : (deterministicRandom()->randomInt(
			              0,
			              (replication_factor / 2) +
			                  1)); // The anti quorum cannot be more than half of the replication factor, or the
			                       // log system will continue to accept commits when a recovery is impossible
			// Go through buildConfiguration, as it sets tLogPolicy/storagePolicy.
			set_config(format("storage_replicas:=%d log_replicas:=%d log_anti_quorum:=%d "
			                  "replica_datacenters:=1 min_replica_datacenters:=1",
			                  storage_servers,
			                  replication_factor,
			                  anti_quorum));
			break;
		}
		case 1: {
			CODE_PROBE(true, "Simulated cluster running in single redundancy mode");
			set_config("single");
			break;
		}
		case 2: {
			CODE_PROBE(true, "Simulated cluster running in double redundancy mode");
			set_config("double");
			break;
		}
		case 3: {
			if (datacenters <= 2 || generateFearless) {
				CODE_PROBE(true, "Simulated cluster running in triple redundancy mode");
				set_config("triple");
			} else if (datacenters == 3) {
				CODE_PROBE(true, "Simulated cluster running in 3 data-hall mode");
				set_config("three_data_hall");
			} else {
				ASSERT(false);
			}
			break;
		}
		default:
			ASSERT(false); // Programmer forgot to adjust cases.
		}
		if (deterministicRandom()->random01() < 0.5) {
			int logSpill = deterministicRandom()->randomInt(TLogSpillType::VALUE, TLogSpillType::END);
			set_config(format("log_spill:=%d", logSpill));
			int logVersion =
			    deterministicRandom()->randomInt(TLogVersion::MIN_RECRUITABLE, testConfig.maxTLogVersion + 1);
			set_config(format("log_version:=%d", logVersion));
		} else {
			if (deterministicRandom()->random01() < 0.7)
				set_config(format("log_version:=%d", testConfig.maxTLogVersion));
			if (deterministicRandom()->random01() < 0.5)
				set_config(format("log_spill:=%d", TLogSpillType::DEFAULT));
		}
	}
}

// Set the regions of the config, including the primary and remote options
// This will also determine the replication types used for satellite and remote.
void SimulationConfig::setRegions(const TestConfig& testConfig) {
	// The kill region workload relies on the fact that all "0", "2", and "4" are all of the possible primary dcids.
	StatusObject primaryObj;
	StatusObject primaryDcObj;
	primaryDcObj["id"] = "0";
	primaryDcObj["priority"] = 2;
	StatusArray primaryDcArr;
	primaryDcArr.push_back(primaryDcObj);

	StatusObject remoteObj;
	StatusObject remoteDcObj;
	remoteDcObj["id"] = "1";
	remoteDcObj["priority"] = 1;
	StatusArray remoteDcArr;
	remoteDcArr.push_back(remoteDcObj);

	bool needsRemote = generateFearless;
	if (generateFearless) {
		if (datacenters > 4) {
			// FIXME: we cannot use one satellite replication with more than one satellite per region because
			// canKillProcesses does not respect usable_dcs
			int satellite_replication_type = deterministicRandom()->randomInt(0, 3);
			if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST && satellite_replication_type == 1) {
				// two_satellite_fast sets antiQuorum to 2, because only one out of two satellites need reply before
				// commit. disabling for version vector in simulation until quorum feature works with it.
				satellite_replication_type = 2;
			}
			switch (satellite_replication_type) {
			case 0: {
				CODE_PROBE(true, "Simulated cluster using no satellite redundancy mode (>4 datacenters)");
				break;
			}
			case 1: {
				CODE_PROBE(true, "Simulated cluster using two satellite fast redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "two_satellite_fast";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_fast";
				break;
			}
			case 2: {
				CODE_PROBE(true, "Simulated cluster using two satellite safe redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "two_satellite_safe";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_safe";
				break;
			}
			default:
				ASSERT(false); // Programmer forgot to adjust cases.
			}
		} else {
			int satellite_replication_type = deterministicRandom()->randomInt(0, 5);
			switch (satellite_replication_type) {
			case 0: {
				// FIXME: implement
				CODE_PROBE(true, "Simulated cluster using custom satellite redundancy mode");
				break;
			}
			case 1: {
				CODE_PROBE(true, "Simulated cluster using no satellite redundancy mode (<4 datacenters)");
				break;
			}
			case 2: {
				CODE_PROBE(true, "Simulated cluster using single satellite redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "one_satellite_single";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_single";
				break;
			}
			case 3: {
				CODE_PROBE(true, "Simulated cluster using double satellite redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "one_satellite_double";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_double";
				break;
			}
			case 4: {
				CODE_PROBE(true, "Simulated cluster using triple satellite redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "one_satellite_triple";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_triple";
				break;
			}
			default:
				ASSERT(false); // Programmer forgot to adjust cases.
			}
		}

		if (deterministicRandom()->random01() < 0.25)
			primaryObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
		if (deterministicRandom()->random01() < 0.25)
			remoteObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);

		// We cannot run with a remote DC when MAX_READ_TRANSACTION_LIFE_VERSIONS is too small, because the log
		// routers will not be able to keep up.
		if (testConfig.minimumRegions <= 1 &&
		    (deterministicRandom()->random01() < 0.25 ||
		     SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS < SERVER_KNOBS->VERSIONS_PER_SECOND)) {
			CODE_PROBE(true, "Simulated cluster using one region");
			needsRemote = false;
		} else {
			CODE_PROBE(true, "Simulated cluster using two regions");
			db.usableRegions = 2;
		}

		if (testConfig.remoteConfig.present()) {
			set_config(testConfig.remoteConfig.get());
		} else {
			int remote_replication_type = deterministicRandom()->randomInt(0, datacenters > 4 ? 4 : 5);
			switch (remote_replication_type) {
			case 0: {
				// FIXME: implement
				CODE_PROBE(true, "Simulated cluster using custom remote redundancy mode");
				break;
			}
			case 1: {
				CODE_PROBE(true, "Simulated cluster using default remote redundancy mode");
				break;
			}
			case 2: {
				CODE_PROBE(true, "Simulated cluster using single remote redundancy mode");
				set_config("remote_single");
				break;
			}
			case 3: {
				CODE_PROBE(true, "Simulated cluster using double remote redundancy mode");
				set_config("remote_double");
				break;
			}
			case 4: {
				CODE_PROBE(true, "Simulated cluster using triple remote redundancy mode");
				set_config("remote_triple");
				break;
			}
			default:
				ASSERT(false); // Programmer forgot to adjust cases.
			}
		}

		if (deterministicRandom()->random01() < 0.25)
			db.desiredLogRouterCount = deterministicRandom()->randomInt(1, 7);

		if (testConfig.remoteDesiredTLogCount.present()) {
			db.remoteDesiredTLogCount = testConfig.remoteDesiredTLogCount.get();
		} else if (deterministicRandom()->random01() < 0.25) {
			db.remoteDesiredTLogCount = deterministicRandom()->randomInt(1, 7);
		}

		bool useNormalDCsAsSatellites =
		    datacenters > 4 && testConfig.minimumRegions < 2 && deterministicRandom()->random01() < 0.3;
		StatusObject primarySatelliteObj;
		primarySatelliteObj["id"] = useNormalDCsAsSatellites ? "1" : "2";
		primarySatelliteObj["priority"] = 1;
		primarySatelliteObj["satellite"] = 1;
		if (deterministicRandom()->random01() < 0.25)
			primarySatelliteObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
		primaryDcArr.push_back(primarySatelliteObj);

		StatusObject remoteSatelliteObj;
		remoteSatelliteObj["id"] = useNormalDCsAsSatellites ? "0" : "3";
		remoteSatelliteObj["priority"] = 1;
		remoteSatelliteObj["satellite"] = 1;
		if (deterministicRandom()->random01() < 0.25)
			remoteSatelliteObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
		remoteDcArr.push_back(remoteSatelliteObj);

		if (datacenters > 4) {
			StatusObject primarySatelliteObjB;
			primarySatelliteObjB["id"] = useNormalDCsAsSatellites ? "2" : "4";
			primarySatelliteObjB["priority"] = 1;
			primarySatelliteObjB["satellite"] = 1;
			if (deterministicRandom()->random01() < 0.25)
				primarySatelliteObjB["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
			primaryDcArr.push_back(primarySatelliteObjB);

			StatusObject remoteSatelliteObjB;
			remoteSatelliteObjB["id"] = useNormalDCsAsSatellites ? "2" : "5";
			remoteSatelliteObjB["priority"] = 1;
			remoteSatelliteObjB["satellite"] = 1;
			if (deterministicRandom()->random01() < 0.25)
				remoteSatelliteObjB["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
			remoteDcArr.push_back(remoteSatelliteObjB);
		}
		if (useNormalDCsAsSatellites) {
			datacenters = 3;
		}
	}

	primaryObj["datacenters"] = primaryDcArr;
	remoteObj["datacenters"] = remoteDcArr;

	StatusArray regionArr;
	regionArr.push_back(primaryObj);
	if (needsRemote || deterministicRandom()->random01() < 0.5) {
		regionArr.push_back(remoteObj);
	}

	if (needsRemote) {
		g_simulator->originalRegions =
		    "regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none);

		StatusArray disablePrimary = regionArr;
		disablePrimary[0].get_obj()["datacenters"].get_array()[0].get_obj()["priority"] = -1;
		g_simulator->disablePrimary = "regions=" + json_spirit::write_string(json_spirit::mValue(disablePrimary),
		                                                                     json_spirit::Output_options::none);

		StatusArray disableRemote = regionArr;
		disableRemote[1].get_obj()["datacenters"].get_array()[0].get_obj()["priority"] = -1;
		g_simulator->disableRemote = "regions=" + json_spirit::write_string(json_spirit::mValue(disableRemote),
		                                                                    json_spirit::Output_options::none);
	} else {
		// In order to generate a starting configuration with the remote disabled, do not apply the region
		// configuration to the DatabaseConfiguration until after creating the starting conf string.
		set_config("regions=" +
		           json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none));
	}
}

// Sets the machine count based on the testConfig. May be overwritten later
// if the end result is not a viable config.
void SimulationConfig::setMachineCount(const TestConfig& testConfig) {
	if (testConfig.machineCount.present()) {
		machine_count = testConfig.machineCount.get();
#ifdef ADDRESS_SANITIZER
		if (testConfig.asanMachineCount.present()) {
			machine_count = testConfig.asanMachineCount.get();
		}
#endif
	} else if (generateFearless && testConfig.minimumReplication > 1) {
		// low latency tests in fearless configurations need 4 machines per datacenter (3 for triple replication, 1 that
		// is down during failures).
		machine_count = 16;
	} else if (generateFearless) {
		machine_count = 12;
	} else if (db.tLogPolicy && db.tLogPolicy->info() == "data_hall^2 x zoneid^2 x 1") {
		machine_count = 9;
	} else {
		// datacenters+2 so that the configure database workload can configure into three_data_hall
		machine_count = std::max(datacenters + 2,
		                         ((db.minDatacentersRequired() > 0) ? datacenters : 1) *
		                             std::max(3, db.minZonesRequiredPerDatacenter()));
		machine_count = deterministicRandom()->randomInt(
		    machine_count,
		    std::max(machine_count + 1, extraDatabaseMode == ISimulator::ExtraDatabaseMode::Disabled ? 10 : 6));
		// generateMachineTeamTestConfig set up the number of servers per machine and the number of machines such that
		// if we do not remove the surplus server and machine teams, the simulation test will report error.
		// This is needed to make sure the number of server (and machine) teams is no larger than the desired number.
		bool generateMachineTeamTestConfig = BUGGIFY_WITH_PROB(0.1) ? true : false;
		if (generateMachineTeamTestConfig) {
			// When DESIRED_TEAMS_PER_SERVER is set to 1, the desired machine team number is 5
			// while the max possible machine team number is 10.
			// If machine_count > 5, we can still test the effectivenss of machine teams
			// Note: machine_count may be much larger than 5 because we may have a big replication factor
			machine_count = std::max(machine_count,
			                         deterministicRandom()->randomInt(
			                             5, extraDatabaseMode == ISimulator::ExtraDatabaseMode::Disabled ? 10 : 6));
		}
	}
	machine_count += datacenters * (testConfig.extraMachineCountDC + testConfig.extraStorageMachineCountPerDC);
}

// Sets the coordinator count based on the testConfig. May be overwritten later
// if the end result is not a viable config.
void SimulationConfig::setCoordinators(const TestConfig& testConfig) {
	if (testConfig.coordinators.present()) {
		coordinators = testConfig.coordinators.get();
	} else {
		// because we protect a majority of coordinators from being killed, it is better to run with low numbers of
		// coordinators to prevent too many processes from being protected
		coordinators = (testConfig.minimumRegions <= 1 && BUGGIFY)
		                   ? deterministicRandom()->randomInt(1, std::max(machine_count, 2))
		                   : 1;
	}
}

// Sets the processes per machine based on the testConfig.
void SimulationConfig::setProcessesPerMachine(const TestConfig& testConfig) {
	if (testConfig.processesPerMachine.present()) {
		processes_per_machine = testConfig.processesPerMachine.get();
	} else if (generateFearless) {
		processes_per_machine = 1;
	} else {
		processes_per_machine = deterministicRandom()->randomInt(
		    1, (extraDatabaseMode == ISimulator::ExtraDatabaseMode::Disabled ? 28 : 14) / machine_count + 2);
	}
}

// Sets the TSS configuration based on the testConfig.
// Also configures the cluster behaviour through setting some flags on the simulator.
void SimulationConfig::setTss(const TestConfig& testConfig) {
	int tssCount = 0;
	// TODO: Support TSS in SHARD_ENCODE_LOCATION_METADATA mode.
	if (!testConfig.simpleConfig && !testConfig.disableTss && deterministicRandom()->random01() < 0.25) {
		// 1 or 2 tss
		tssCount = deterministicRandom()->randomInt(1, 3);
	}

	// reduce tss to half of extra non-seed servers that can be recruited in usable regions.
	tssCount =
	    std::max(0, std::min(tssCount, db.usableRegions * ((machine_count / datacenters) - db.storageTeamSize) / 2));

	if (!testConfig.config.present() && tssCount > 0) {
		std::string confStr = format("tss_count:=%d tss_storage_engine:=%d", tssCount, db.storageServerStoreType);
		set_config(confStr);
		double tssRandom = deterministicRandom()->random01();
		if (tssRandom > 0.5 || !faultInjectionActivated) {
			// normal tss mode
			g_simulator->tssMode = ISimulator::TSSMode::EnabledNormal;
		} else if (tssRandom < 0.25 && !testConfig.isFirstTestInRestart) {
			// fault injection - don't enable in first test in restart because second test won't know it intentionally
			// lost data
			g_simulator->tssMode = ISimulator::TSSMode::EnabledDropMutations;
		} else {
			// delay injection
			g_simulator->tssMode = ISimulator::TSSMode::EnabledAddDelay;
		}
		printf("enabling tss for simulation in mode %d: %s\n", g_simulator->tssMode, confStr.c_str());
	}
}

void setConfigDB(TestConfig const& testConfig) {
	if (g_simulator) {
		g_simulator->configDBType = testConfig.getConfigDBType();
	}
}

// Generates and sets an appropriate configuration for the database according to
// the provided testConfig. Some attributes are randomly generated for more coverage
// of different combinations
void SimulationConfig::generateNormalConfig(const TestConfig& testConfig) {
	set_config("new");
	// Some of these options will overwrite one another so the ordering is important.
	// This is a bit inefficient but separates the different types of option setting paths for better readability.
	setDatacenters(testConfig);

	// These 3 sets will only change the settings with trivial logic and low coupling with
	// other portions of the configuration. The parameters that are more involved and use
	// complex logic will be found in their respective "set----" methods following after.
	setRandomConfig();
	if (testConfig.simpleConfig) {
		setSimpleConfig();
	}
	setSpecificConfig(testConfig);
	setTenantMode(testConfig);
	setEncryptionAtRestMode(testConfig);
	setStorageEngine(testConfig);
	setReplicationType(testConfig);
#if (!NO_MULTIREGION_TEST)
	if (!testConfig.singleRegion &&
	    (generateFearless || (datacenters == 2 && deterministicRandom()->random01() < 0.5))) {
		setRegions(testConfig);
	}
#endif
	setMachineCount(testConfig);
	setCoordinators(testConfig);

	if (testConfig.minimumReplication > 1 && datacenters == 3) {
		// low latency tests in 3 data hall mode need 2 other data centers with 2 machines each to avoid waiting for
		// logs to recover.
		machine_count = std::max(machine_count, 6);
		coordinators = 3;
	}

	setProcessesPerMachine(testConfig);
	setTss(testConfig);
	setConfigDB(testConfig);
}

// Configures the system according to the given specifications in order to run
// simulation under the correct conditions
void setupSimulatedSystem(std::vector<Future<Void>>* systemActors,
                          std::string baseFolder,
                          int* pTesterCount,
                          Optional<ClusterConnectionString>* pConnString,
                          Standalone<StringRef>* pStartingConfiguration,
                          std::string whitelistBinPaths,
                          TestConfig testConfig,
                          ProtocolVersion protocolVersion,
                          Optional<TenantMode>* tenantMode) {
	auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
	// SOMEDAY: this does not test multi-interface configurations
	SimulationConfig simconfig(testConfig);
	*tenantMode = simconfig.db.tenantMode;

	if (testConfig.testClass == MOCK_DD_TEST_CLASS) {
		MockGlobalState::g_mockState()->initializeClusterLayout(simconfig);
	}

	if (testConfig.logAntiQuorum != -1) {
		simconfig.db.tLogWriteAntiQuorum = testConfig.logAntiQuorum;
	}

	g_simulator->blobGranulesEnabled = simconfig.db.blobGranulesEnabled;

	StatusObject startingConfigJSON = simconfig.db.toJSON(true);
	std::string startingConfigString = "new";
	if (testConfig.configureLocked) {
		startingConfigString += " locked";
	}
	if (testConfig.disableRemoteKVS) {
		g_knobs.setKnob("remote_kv_store", KnobValueRef::create(bool{ false }));
		TraceEvent(SevDebug, "DisableRemoteKVS");
	}
	auto configDBType = testConfig.getConfigDBType();
	startingConfigString += DatabaseConfiguration::configureStringFromJSON(startingConfigJSON);

	// Set a random locality for the perpetual wiggle if any locality is set in the config
	// This behavior is preserved in a refactor but it unclear why the source configuration JSON
	// is translated to a random locality instead of something matching the source.
	auto ipwLocality = startingConfigJSON.find("perpetual_storage_wiggle_locality");
	if (ipwLocality != startingConfigJSON.end()) {
		if (deterministicRandom()->random01() < 0.25) {
			int dcId = deterministicRandom()->randomInt(0, simconfig.datacenters);
			startingConfigString += " " + ipwLocality->first + "=" + "data_hall:" + std::to_string(dcId);
		}
	}

	// handle tss_storage_engine separately because the passthrough needs the enum ordinal, but it's serialized to json
	// as the string name
	if (simconfig.db.desiredTSSCount > 0) {
		startingConfigString += format(" tss_storage_engine:=%d", simconfig.db.testingStorageServerStoreType);
	}

	if (g_simulator->originalRegions != "") {
		simconfig.set_config(g_simulator->originalRegions);
		g_simulator->startingDisabledConfiguration = startingConfigString + " " + g_simulator->disableRemote;
		startingConfigString += " " + g_simulator->originalRegions;
	}

	TraceEvent("SimulatorConfig").setMaxFieldLength(10000).detail("ConfigString", StringRef(startingConfigString));

	const int dataCenters = simconfig.datacenters;
	const int machineCount = simconfig.machine_count;
	const int coordinatorCount = simconfig.coordinators;
	const int processesPerMachine = simconfig.processes_per_machine;

	// half the time, when we have more than 4 machines that are not the first in their dataCenter, assign classes
	bool assignClasses = machineCount - dataCenters > 4 && deterministicRandom()->random01() < 0.5;

	// Use SSL 5% of the time
	bool sslEnabled = deterministicRandom()->random01() < 0.10;
	bool sslOnly = sslEnabled && deterministicRandom()->coinflip();
	bool isTLS = sslEnabled && sslOnly;
	g_simulator->listenersPerProcess = sslEnabled && !sslOnly ? 2 : 1;
	CODE_PROBE(sslEnabled, "SSL enabled");
	CODE_PROBE(!sslEnabled, "SSL disabled");

	// Use IPv6 25% of the time
	bool useIPv6 = deterministicRandom()->random01() < 0.25;
	CODE_PROBE(useIPv6, "Use IPv6");
	CODE_PROBE(!useIPv6, "Use IPv4");

	// Use hostname 25% of the time, unless it is disabled
	bool useHostname = !testConfig.disableHostname && deterministicRandom()->random01() < 0.25;
	CODE_PROBE(useHostname, "Use hostname");
	CODE_PROBE(!useHostname, "Use IP address");
	NetworkAddressFromHostname fromHostname =
	    useHostname ? NetworkAddressFromHostname::True : NetworkAddressFromHostname::False;

	int extraDatabaseCount = 0;
	bool useLocalDatabase = (testConfig.extraDatabaseMode == ISimulator::ExtraDatabaseMode::LocalOrSingle && BUGGIFY) ||
	                        testConfig.extraDatabaseMode == ISimulator::ExtraDatabaseMode::Local;
	if (!useLocalDatabase && testConfig.extraDatabaseMode != ISimulator::ExtraDatabaseMode::Disabled) {
		extraDatabaseCount =
		    testConfig.extraDatabaseMode == ISimulator::ExtraDatabaseMode::Multiple && testConfig.extraDatabaseCount > 0
		        ? testConfig.extraDatabaseCount
		        : 1;
	}

	std::vector<NetworkAddress> coordinatorAddresses;
	std::vector<Hostname> coordinatorHostnames;

	// A list of coordinators for each extra database being created. The Nth vector in the outer vector
	// contains the coordinators for the Nth extra database.
	std::vector<std::vector<NetworkAddress>> extraCoordinatorAddresses(extraDatabaseCount);
	std::vector<std::vector<Hostname>> extraCoordinatorHostnames(extraDatabaseCount);

	if (testConfig.minimumRegions > 1) {
		// do not put coordinators in the primary region so that we can kill that region safely
		int nonPrimaryDcs = dataCenters / 2;
		for (int dc = 1; dc < dataCenters; dc += 2) {
			int dcCoordinators = coordinatorCount / nonPrimaryDcs + ((dc - 1) / 2 < coordinatorCount % nonPrimaryDcs);
			for (int m = 0; m < dcCoordinators; m++) {
				auto ip = makeIPAddressForSim(useIPv6, { 2, dc, 1, m });
				uint16_t port = sslEnabled && !sslOnly ? 2 : 1;
				NetworkAddress coordinator(ip, port, true, isTLS, fromHostname);
				coordinatorAddresses.push_back(coordinator);

				if (useHostname) {
					std::string hostname = "fakeCoordinatorDC" + std::to_string(dc) + "M" + std::to_string(m);
					Hostname coordinatorHostname(hostname, std::to_string(port), isTLS);
					coordinatorHostnames.push_back(coordinatorHostname);
					INetworkConnections::net()->addMockTCPEndpoint(hostname, std::to_string(port), { coordinator });
				}

				for (int edb = 0; edb < extraDatabaseCount; ++edb) {
					auto extraIp = makeIPAddressForSim(useIPv6, { 4 + edb, dc, 1, m });
					NetworkAddress extraCoordinator(extraIp, port, true, isTLS, fromHostname);
					extraCoordinatorAddresses[edb].push_back(extraCoordinator);

					if (useHostname) {
						std::string hostname = "fakeExtraCoordinatorDC" + std::to_string(dc) + "M" + std::to_string(m) +
						                       "C" + std::to_string(edb);
						Hostname extraCoordinatorHostname(hostname, std::to_string(port), isTLS);
						extraCoordinatorHostnames[edb].push_back(extraCoordinatorHostname);
						INetworkConnections::net()->addMockTCPEndpoint(
						    hostname, std::to_string(port), { extraCoordinator });
					}
				}

				TraceEvent("SelectedCoordinator")
				    .detail("Hostname", useHostname ? coordinatorHostnames.back().toString().c_str() : "N/A")
				    .detail("Address", coordinatorAddresses.back());
			}
		}
	} else {
		int assignedMachines = 0;
		int coordCount = coordinatorCount;
		if (coordinatorCount > 4) {
			++coordCount;
		}
		for (int dc = 0; dc < dataCenters; dc++) {
			int dcCoordinators = coordCount / dataCenters + (dc < coordCount % dataCenters);
			int machines = machineCount / dataCenters + (dc < machineCount % dataCenters);
			for (int m = 0; m < dcCoordinators; m++) {
				if (coordinatorCount > 4 &&
				    (assignedMachines == 4 || (m + 1 == dcCoordinators && assignedMachines < 4 &&
				                               assignedMachines + machines - dcCoordinators >= 4))) {
					auto ip = makeIPAddressForSim(useIPv6, { 2, dc, 1, m });
					TraceEvent("SkippedCoordinator")
					    .detail("Address", ip.toString())
					    .detail("M", m)
					    .detail("Machines", machines)
					    .detail("Assigned", assignedMachines)
					    .detail("DcCoord", dcCoordinators)
					    .detail("CoordinatorCount", coordinatorCount);
				} else {
					auto ip = makeIPAddressForSim(useIPv6, { 2, dc, 1, m });
					uint16_t port = sslEnabled && !sslOnly ? 2 : 1;
					NetworkAddress coordinator(ip, port, true, isTLS, fromHostname);
					coordinatorAddresses.push_back(coordinator);

					if (useHostname) {
						std::string hostname = "fakeCoordinatorDC" + std::to_string(dc) + "M" + std::to_string(m);
						Hostname coordinatorHostname(hostname, std::to_string(port), isTLS);
						coordinatorHostnames.push_back(coordinatorHostname);
						INetworkConnections::net()->addMockTCPEndpoint(hostname, std::to_string(port), { coordinator });
					}

					for (int edb = 0; edb < extraDatabaseCount; ++edb) {
						auto extraIp = makeIPAddressForSim(useIPv6, { 4 + edb, dc, 1, m });
						NetworkAddress extraCoordinator(extraIp, port, true, isTLS, fromHostname);
						extraCoordinatorAddresses[edb].push_back(extraCoordinator);
						std::string hostname = "fakeExtraCoordinatorDC" + std::to_string(dc) + "M" + std::to_string(m) +
						                       "C" + std::to_string(edb);
						Hostname extraCoordinatorHostname(hostname, std::to_string(port), isTLS);
						extraCoordinatorHostnames[edb].push_back(extraCoordinatorHostname);
						INetworkConnections::net()->addMockTCPEndpoint(
						    hostname, std::to_string(port), { extraCoordinator });
					}

					TraceEvent("SelectedCoordinator")
					    .detail("Hostname", useHostname ? coordinatorHostnames.back().toString().c_str() : "N/A")
					    .detail("Address", coordinatorAddresses.back())
					    .detail("M", m)
					    .detail("Machines", machines)
					    .detail("Assigned", assignedMachines)
					    .detail("DcCoord", dcCoordinators)
					    .detail("P1", (m + 1 == dcCoordinators))
					    .detail("P2", (assignedMachines < 4))
					    .detail("P3", (assignedMachines + machines - dcCoordinators >= 4))
					    .detail("CoordinatorCount", coordinatorCount);
				}
				assignedMachines++;
			}
			assignedMachines += machines - dcCoordinators;
		}
	}

	ASSERT(coordinatorAddresses.size() > 0);

	// Mark a random majority of the coordinators as protected, so
	// we won't accidentally kill off a quorum and render the
	// cluster unrecoverable.
	deterministicRandom()->randomShuffle(coordinatorAddresses);
	for (int i = 0; i < (coordinatorAddresses.size() / 2) + 1; i++) {
		TraceEvent("ProtectCoordinator")
		    .detail("Address", coordinatorAddresses[i])
		    .detail("Coordinators", describe(coordinatorAddresses));
		g_simulator->protectedAddresses.insert(NetworkAddress(
		    coordinatorAddresses[i].ip, coordinatorAddresses[i].port, true, coordinatorAddresses[i].isTLS()));
		if (coordinatorAddresses[i].port == 2) {
			g_simulator->protectedAddresses.insert(NetworkAddress(coordinatorAddresses[i].ip, 1, true, true));
		}
	}
	deterministicRandom()->randomShuffle(coordinatorAddresses);

	for (const auto& coordinators : extraCoordinatorAddresses) {
		for (int i = 0; i < (coordinators.size() / 2) + 1; i++) {
			TraceEvent("ProtectCoordinator")
			    .detail("Address", coordinators[i])
			    .detail("Coordinators", describe(coordinators));
			g_simulator->protectedAddresses.insert(
			    NetworkAddress(coordinators[i].ip, coordinators[i].port, true, coordinators[i].isTLS()));
			if (coordinators[i].port == 2) {
				g_simulator->protectedAddresses.insert(NetworkAddress(coordinators[i].ip, 1, true, true));
			}
		}
	}

	ASSERT_EQ(coordinatorAddresses.size(), coordinatorCount);
	ClusterConnectionString conn(coordinatorAddresses, "TestCluster:0"_sr);
	if (useHostname) {
		conn = ClusterConnectionString(coordinatorHostnames, "TestCluster:0"_sr);
	}

	if (useLocalDatabase) {
		g_simulator->extraDatabases.push_back(
		    useHostname ? ClusterConnectionString(coordinatorHostnames, "TestCluster:0"_sr).toString()
		                : ClusterConnectionString(coordinatorAddresses, "TestCluster:0"_sr).toString());
	} else if (testConfig.extraDatabaseMode != ISimulator::ExtraDatabaseMode::Disabled) {
		for (int i = 0; i < extraDatabaseCount; ++i) {
			g_simulator->extraDatabases.push_back(
			    useHostname
			        ? ClusterConnectionString(extraCoordinatorHostnames[i], StringRef(format("ExtraCluster%04d:0", i)))
			              .toString()
			        : ClusterConnectionString(extraCoordinatorAddresses[i], StringRef(format("ExtraCluster%04d:0", i)))
			              .toString());
		}
	}

	*pConnString = conn;

	TraceEvent("SimulatedConnectionString")
	    .detail("String", conn.toString())
	    .detail("ConfigString", startingConfigString);

	bool requiresExtraDBMachines = !g_simulator->extraDatabases.empty() && !useLocalDatabase;
	int assignedMachines = 0;
	bool gradualMigrationPossible = true;
	std::vector<ProcessClass::ClassType> processClassesSubSet = { ProcessClass::UnsetClass,
		                                                          ProcessClass::StatelessClass };
	for (int dc = 0; dc < dataCenters; dc++) {
		// FIXME: test unset dcID
		Optional<Standalone<StringRef>> dcUID = StringRef(format("%d", dc));
		std::vector<UID> machineIdentities;
		int machines = machineCount / dataCenters +
		               (dc < machineCount % dataCenters); // add remainder of machines to first datacenter
		int possible_ss = 0;
		int dcCoordinators = coordinatorCount / dataCenters + (dc < coordinatorCount % dataCenters);

		// FIXME: we hardcode some machines to specifically test storage cache and blob workers
		// TODO: caching disabled for this merge
		int storageCacheMachines = dc == 0 ? 1 : 0;
		int blobWorkerMachines = 0;
		int simHTTPMachines = 0;
		if (testConfig.blobGranulesEnabled) {
			int blobWorkerProcesses = 1 + deterministicRandom()->randomInt(0, NUM_EXTRA_BW_MACHINES + 1);
			blobWorkerMachines = std::max(1, blobWorkerProcesses / processesPerMachine);
		}
		if (testConfig.simHTTPServerEnabled) {
			simHTTPMachines = deterministicRandom()->randomInt(1, 4);
			fmt::print("sim http machines = {0}\n", simHTTPMachines);
		}
		int extraStorageMachineCount = testConfig.extraStorageMachineCountPerDC;

		int totalMachines =
		    machines + storageCacheMachines + blobWorkerMachines + simHTTPMachines + extraStorageMachineCount;

		printf("Datacenter %d: %d/%d machines, %d/%d coordinators, %d other machines\n",
		       dc,
		       machines,
		       machineCount,
		       dcCoordinators,
		       coordinatorCount,
		       totalMachines - machines);
		ASSERT_LE(dcCoordinators, machines);

		int useSeedForMachine = deterministicRandom()->randomInt(0, totalMachines);
		Standalone<StringRef> zoneId;
		Standalone<StringRef> newZoneId;

		Optional<int> desiredStatelessClasses;
		int actualStatelessClasses = 0;
		if (testConfig.statelessProcessClassesPerDC.present()) {
			desiredStatelessClasses = testConfig.statelessProcessClassesPerDC.get();
		}

		for (int machine = 0; machine < totalMachines; machine++) {
			Standalone<StringRef> machineId(deterministicRandom()->randomUniqueID().toString());
			if (machine == 0 || machineCount - dataCenters <= 4 || assignedMachines != 4 ||
			    simconfig.db.regions.size() || deterministicRandom()->random01() < 0.5) {
				zoneId = deterministicRandom()->randomUniqueID().toString();
				newZoneId = deterministicRandom()->randomUniqueID().toString();
			}

			// Choose a machine class
			ProcessClass processClass = ProcessClass(ProcessClass::UnsetClass, ProcessClass::CommandLineSource);
			if (assignClasses) {
				if (assignedMachines < 4)
					processClass = ProcessClass((ProcessClass::ClassType)deterministicRandom()->randomInt(0, 2),
					                            ProcessClass::CommandLineSource); // Unset or Storage
				else if (assignedMachines == 4 && !simconfig.db.regions.size())
					processClass = ProcessClass(
					    processClassesSubSet[deterministicRandom()->randomInt(0, processClassesSubSet.size())],
					    ProcessClass::CommandLineSource); // Unset or Stateless
				else
					processClass = ProcessClass((ProcessClass::ClassType)deterministicRandom()->randomInt(0, 3),
					                            ProcessClass::CommandLineSource); // Unset, Storage, or Transaction

				if (processClass == ProcessClass::UnsetClass || processClass == ProcessClass::StorageClass) {
					possible_ss++;
				}
			}

			if (desiredStatelessClasses.present() && actualStatelessClasses < desiredStatelessClasses.get()) {
				processClass = ProcessClass(ProcessClass::StatelessClass, ProcessClass::CommandLineSource);
				actualStatelessClasses++;
			}

			// FIXME: hack to add machines specifically to test storage cache and blob workers and http server
			// `machines` here is the normal (non-temporary) machines that totalMachines comprises of
			int processCount = processesPerMachine;
			ProcessMode processMode = requiresExtraDBMachines ? BackupAgentOnly : FDBDAndBackupAgent;
			if (machine >= machines) {
				if (extraStorageMachineCount > 0) {
					processClass = ProcessClass(ProcessClass::ClassType::StorageClass,
					                            ProcessClass::CommandLineSource); // Storage
					extraStorageMachineCount--;
					possible_ss++;
				} else if (storageCacheMachines > 0 && dc == 0) {
					processClass = ProcessClass(ProcessClass::StorageCacheClass, ProcessClass::CommandLineSource);
					storageCacheMachines--;
				} else if (blobWorkerMachines > 0) { // add blob workers to every DC
					processClass = ProcessClass(ProcessClass::BlobWorkerClass, ProcessClass::CommandLineSource);
					blobWorkerMachines--;
				} else if (simHTTPMachines > 0) {
					processClass = ProcessClass(ProcessClass::SimHTTPServerClass, ProcessClass::CommandLineSource);
					processCount = 1;
					processMode = SimHTTPServer;
					simHTTPMachines--;
				}
			}

			std::vector<IPAddress> ips;
			ips.reserve(processesPerMachine);
			for (int i = 0; i < processCount; i++) {
				ips.push_back(
				    makeIPAddressForSim(useIPv6, { 2, dc, deterministicRandom()->randomInt(1, i + 2), machine }));
			}
			if (requiresExtraDBMachines) {
				ips.push_back(makeIPAddressForSim(useIPv6, { 2, dc, 1, machine }));
			}

			// check the sslEnablementMap using only one ip
			LocalityData localities(Optional<Standalone<StringRef>>(), zoneId, machineId, dcUID);
			localities.set("data_hall"_sr, dcUID);
			systemActors->push_back(reportErrors(
			    simulatedMachine(conn,
			                     requiresExtraDBMachines ? ClusterConnectionString(g_simulator->extraDatabases.at(0))
			                                             : ClusterConnectionString(),
			                     ips,
			                     sslEnabled,
			                     localities,
			                     processClass,
			                     baseFolder,
			                     false,
			                     machine == useSeedForMachine,
			                     processMode,
			                     sslOnly,
			                     whitelistBinPaths,
			                     protocolVersion,
			                     configDBType,
			                     false),
			    "SimulatedMachine"));

			if (requiresExtraDBMachines) {
				int cluster = 4;
				for (auto extraDatabase : g_simulator->extraDatabases) {
					std::vector<IPAddress> extraIps;
					extraIps.reserve(processesPerMachine);
					for (int i = 0; i < processesPerMachine; i++) {
						extraIps.push_back(makeIPAddressForSim(
						    useIPv6, { cluster, dc, deterministicRandom()->randomInt(1, i + 2), machine }));
					}

					Standalone<StringRef> newMachineId(deterministicRandom()->randomUniqueID().toString());

					LocalityData localities(Optional<Standalone<StringRef>>(), newZoneId, newMachineId, dcUID);
					localities.set("data_hall"_sr, dcUID);
					systemActors->push_back(reportErrors(
					    simulatedMachine(ClusterConnectionString(extraDatabase),
					                     conn,
					                     extraIps,
					                     sslEnabled,
					                     localities,
					                     processClass,
					                     baseFolder,
					                     false,
					                     machine == useSeedForMachine,
					                     testConfig.extraDatabaseBackupAgents ? FDBDAndBackupAgent : FDBDOnly,
					                     sslOnly,
					                     whitelistBinPaths,
					                     protocolVersion,
					                     configDBType,
					                     true),
					    "SimulatedMachine"));
					++cluster;
				}
			}

			assignedMachines++;
		}

		if (desiredStatelessClasses.present()) {
			// If this assertion fails, that measn that there were not enough machines in the DC (primary or remote)
			// to match desired stateless classes
			ASSERT(actualStatelessClasses == desiredStatelessClasses.get());
		}

		if (possible_ss - simconfig.db.desiredTSSCount / simconfig.db.usableRegions <= simconfig.db.storageTeamSize) {
			gradualMigrationPossible = false;
		}

		TraceEvent("SimulatedClusterAssignMachineToDC")
		    .detail("DC", dc)
		    .detail("PossibleSS", possible_ss)
		    .detail("Machines", machines)
		    .detail("DcCoordinators", dcCoordinators);
	}

	g_simulator->desiredCoordinators = coordinatorCount;
	g_simulator->physicalDatacenters = dataCenters;
	g_simulator->processesPerMachine = processesPerMachine;

	TraceEvent("SetupSimulatorSettings")
	    .detail("DesiredCoordinators", g_simulator->desiredCoordinators)
	    .detail("PhysicalDatacenters", g_simulator->physicalDatacenters)
	    .detail("ProcessesPerMachine", g_simulator->processesPerMachine);

	// SOMEDAY: add locality for testers to simulate network topology
	// FIXME: Start workers with tester class instead, at least sometimes run tests with the testers-only flag
	int testerCount = *pTesterCount = deterministicRandom()->randomInt(4, 9);
	int useSeedForMachine = deterministicRandom()->randomInt(0, testerCount);
	for (int i = 0; i < testerCount; i++) {
		std::vector<IPAddress> ips;
		ips.push_back(makeIPAddressForSim(useIPv6, { 3, 4, 3, i + 1 }));
		Standalone<StringRef> newZoneId = Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString());
		LocalityData localities(
		    Optional<Standalone<StringRef>>(), newZoneId, newZoneId, Optional<Standalone<StringRef>>());
		systemActors->push_back(
		    reportErrors(simulatedMachine(conn,
		                                  ClusterConnectionString(),
		                                  ips,
		                                  sslEnabled,
		                                  localities,
		                                  ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource),
		                                  baseFolder,
		                                  false,
		                                  i == useSeedForMachine,
		                                  FDBDOnly,
		                                  sslOnly,
		                                  whitelistBinPaths,
		                                  protocolVersion,
		                                  configDBType,
		                                  false),
		                 "SimulatedTesterMachine"));
	}

	if (g_simulator->setDiffProtocol) {
		--(*pTesterCount);
	}

	*pStartingConfiguration = startingConfigString;

	// save some state that we only need when restarting the simulator.
	g_simulator->connectionString = conn.toString();
	g_simulator->testerCount = testerCount;
	g_simulator->allowStorageMigrationTypeChange = gradualMigrationPossible;
	g_simulator->willRestart = testConfig.isFirstTestInRestart;

	TraceEvent("SimulatedClusterStarted")
	    .detail("DataCenters", dataCenters)
	    .detail("ServerMachineCount", machineCount)
	    .detail("ProcessesPerServer", processesPerMachine)
	    .detail("SSLEnabled", sslEnabled)
	    .detail("SSLOnly", sslOnly)
	    .detail("ClassesAssigned", assignClasses)
	    .detail("GradualMigrationPossible", gradualMigrationPossible)
	    .detail("StartingConfiguration", pStartingConfiguration->toString());
}

using namespace std::literals;

// Populates the TestConfig fields according to what is found in the test file.
[[maybe_unused]] void checkTestConf(const char* testFile, TestConfig* testConfig) {}

} // namespace

ACTOR void simulationSetupAndRun(std::string dataFolder,
                                 const char* testFile,
                                 bool rebooting,
                                 bool restoring,
                                 std::string whitelistBinPaths) {
	state std::vector<Future<Void>> systemActors;
	state Optional<ClusterConnectionString> connectionString;
	state Standalone<StringRef> startingConfiguration;
	state int testerCount = 1;
	state TestConfig testConfig;
	state IPAllowList allowList;
	testConfig.readFromConfig(testFile);
	g_simulator->hasDiffProtocolProcess = testConfig.startIncompatibleProcess;
	g_simulator->setDiffProtocol = false;
	if (testConfig.injectTargetedSSRestart && deterministicRandom()->random01() < 0.25) {
		g_simulator->injectTargetedSSRestartTime = 60.0 + 340.0 * deterministicRandom()->random01();
	}

	if (testConfig.injectSSDelay && deterministicRandom()->random01() < 0.25) {
		g_simulator->injectSSDelayTime = 60.0 + 240.0 * deterministicRandom()->random01();
	}

	if (deterministicRandom()->random01() < 0.25) {
		g_simulator->injectTargetedBMRestartTime = 60.0 + 340.0 * deterministicRandom()->random01();
	}

	if (deterministicRandom()->random01() < 0.25) {
		g_simulator->injectTargetedBWRestartTime = 60.0 + 340.0 * deterministicRandom()->random01();
	}

	// Build simulator allow list
	allowList.addTrustedSubnet("0.0.0.0/2"sv);
	allowList.addTrustedSubnet("abcd::/16"sv);
	state bool allowDefaultTenant = testConfig.allowDefaultTenant;
	state bool allowCreatingTenants = testConfig.allowCreatingTenants;

	if (!SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA &&
	    // NOTE: PhysicalShardMove and BulkLoading and Bulkdumping are required to have SHARDED_ROCKSDB storage engine
	    // working. Inside the TOML file, the SHARD_ENCODE_LOCATION_METADATA is overridden, however, the override will
	    // not take effect until the test starts. Here, we do an additional check for this special simulation test.
	    (std::string_view(testFile).find("PhysicalShardMove") == std::string_view::npos &&
	     std::string_view(testFile).find("BulkLoading") == std::string_view::npos &&
	     std::string_view(testFile).find("BulkDumping") == std::string_view::npos &&
	     std::string_view(testFile).find("ShardedRocksNondeterministicTest") == std::string_view::npos)) {
		testConfig.storageEngineExcludeTypes.insert(SimulationStorageEngine::SHARDED_ROCKSDB);
	}

	if (std::string_view(testFile).find("BlobGranule") != std::string_view::npos) {
		testConfig.storageEngineExcludeTypes.insert(SimulationStorageEngine::SHARDED_ROCKSDB);
	}

	if (std::string_view(testFile).find("BlobRestore") != std::string_view::npos) {
		testConfig.storageEngineExcludeTypes.insert(SimulationStorageEngine::SHARDED_ROCKSDB);
	}

	if (std::string_view(testFile).find("ChangeFeed") != std::string_view::npos) {
		testConfig.storageEngineExcludeTypes.insert(SimulationStorageEngine::SHARDED_ROCKSDB);
	}

	state ProtocolVersion protocolVersion = currentProtocolVersion();
	if (testConfig.startIncompatibleProcess) {
		// isolates right most 1 bit of compatibleProtocolVersionMask to make this protocolVersion incompatible
		uint64_t minAddToMakeIncompatible =
		    ProtocolVersion::compatibleProtocolVersionMask & ~(ProtocolVersion::compatibleProtocolVersionMask - 1);
		protocolVersion = ProtocolVersion(currentProtocolVersion().version() + minAddToMakeIncompatible);
	}

	// TODO (IPv6) Use IPv6?
	auto testSystem =
	    g_simulator->newProcess("TestSystem",
	                            IPAddress(0x01010101),
	                            1,
	                            false,
	                            1,
	                            LocalityData(Optional<Standalone<StringRef>>(),
	                                         Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                                         Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                                         Optional<Standalone<StringRef>>()),
	                            ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource),
	                            "",
	                            "",
	                            currentProtocolVersion(),
	                            false);
	testSystem->excludeFromRestarts = true;
	wait(g_simulator->onProcess(testSystem, TaskPriority::DefaultYield));
	Sim2FileSystem::newFileSystem();
	FlowTransport::createInstance(true, 1, WLTOKEN_RESERVED_COUNT, &allowList);
	CODE_PROBE(true, "Simulation start");

	state Optional<TenantName> defaultTenant;
	state Standalone<VectorRef<TenantNameRef>> tenantsToCreate;
	state Optional<TenantMode> tenantMode;

	try {
		// systemActors.push_back( startSystemMonitor(dataFolder) );
		if (rebooting) {
			wait(timeoutError(restartSimulatedSystem(&systemActors,
			                                         dataFolder,
			                                         &testerCount,
			                                         &connectionString,
			                                         &startingConfiguration,
			                                         &testConfig,
			                                         whitelistBinPaths,
			                                         protocolVersion),
			                  100.0));
			// FIXME: snapshot restore does not support multi-region restore, hence restore it as single region always
			if (restoring) {
				startingConfiguration = "usable_regions=1"_sr;
			}
		} else {
			g_expect_full_pointermap = 1;
			setupSimulatedSystem(&systemActors,
			                     dataFolder,
			                     &testerCount,
			                     &connectionString,
			                     &startingConfiguration,
			                     whitelistBinPaths,
			                     testConfig,
			                     protocolVersion,
			                     &tenantMode);
			wait(delay(1.0)); // FIXME: WHY!!!  //wait for machines to boot
		}

		// restartSimulatedSystem can adjust some testConfig params related to tenants
		// so set/overwrite those options if necessary here
		if (rebooting) {
			if (testConfig.tenantModes.size()) {
				tenantMode = TenantMode::fromString(testConfig.tenantModes[0]);
			} else {
				tenantMode = TenantMode::DISABLED;
			}
		}
		// setupSimulatedSystem/restartSimulatedSystem should fill tenantMode with valid value.
		ASSERT(tenantMode.present());
		if (tenantMode != TenantMode::DISABLED && allowDefaultTenant) {
			// Default tenant set by testConfig or restarting data in restartInfo.ini
			if (testConfig.defaultTenant.present()) {
				defaultTenant = testConfig.defaultTenant.get();
			} else if (!rebooting && (tenantMode == TenantMode::REQUIRED || deterministicRandom()->coinflip())) {
				defaultTenant = "SimulatedDefaultTenant"_sr;
			}
		}
		if (!rebooting) {
			if (defaultTenant.present() && allowDefaultTenant) {
				tenantsToCreate.push_back_deep(tenantsToCreate.arena(), defaultTenant.get());
			}
			if (allowCreatingTenants && tenantMode != TenantMode::DISABLED && deterministicRandom()->coinflip()) {
				int numTenants = deterministicRandom()->randomInt(1, 6);
				for (int i = 0; i < numTenants; ++i) {
					tenantsToCreate.push_back_deep(tenantsToCreate.arena(),
					                               TenantNameRef(format("SimulatedExtraTenant%04d", i)));
				}
			}
		}

		wait(HTTP::registerAlwaysFailHTTPHandler());

		TraceEvent("SimulatedClusterTenantMode")
		    .detail("UsingTenant", defaultTenant)
		    .detail("TenantMode", tenantMode.get().toString())
		    .detail("TotalTenants", tenantsToCreate.size());
		std::string clusterFileDir = joinPath(dataFolder, deterministicRandom()->randomUniqueID().toString());
		platform::createDirectory(clusterFileDir);
		writeFile(joinPath(clusterFileDir, "fdb.cluster"), connectionString.get().toString());
		state Reference<ClusterConnectionFile> connFile =
		    makeReference<ClusterConnectionFile>(joinPath(clusterFileDir, "fdb.cluster"));
		if (rebooting) {
			// protect coordinators for restarting tests
			std::vector<NetworkAddress> coordinatorAddresses =
			    wait(connFile->getConnectionString().tryResolveHostnames());
			ASSERT(coordinatorAddresses.size() > 0);
			for (int i = 0; i < (coordinatorAddresses.size() / 2) + 1; i++) {
				TraceEvent("ProtectCoordinator")
				    .detail("Address", coordinatorAddresses[i])
				    .detail("Coordinators", describe(coordinatorAddresses));
				g_simulator->protectedAddresses.insert(NetworkAddress(
				    coordinatorAddresses[i].ip, coordinatorAddresses[i].port, true, coordinatorAddresses[i].isTLS()));
				if (coordinatorAddresses[i].port == 2) {
					g_simulator->protectedAddresses.insert(NetworkAddress(coordinatorAddresses[i].ip, 1, true, true));
				}
			}
		}
		Future<Void> runTestsF = runTests(connFile,
		                                  TEST_TYPE_FROM_FILE,
		                                  TEST_ON_TESTERS,
		                                  testerCount,
		                                  testFile,
		                                  startingConfiguration,
		                                  LocalityData(),
		                                  UnitTestParameters(),
		                                  defaultTenant,
		                                  tenantsToCreate,
		                                  rebooting);
		wait(testConfig.longRunningTest
		         ? runTestsF
		         : timeoutError(runTestsF,
		                        isGeneralBuggifyEnabled() ? testConfig.simulationBuggifyRunTestsTimeoutSeconds
		                                                  : testConfig.simulationNormalRunTestsTimeoutSeconds));
	} catch (Error& e) {
		auto timeoutVal = isGeneralBuggifyEnabled() ? testConfig.simulationBuggifyRunTestsTimeoutSeconds
		                                            : testConfig.simulationNormalRunTestsTimeoutSeconds;
		auto msg = fmt::format("Timeout after {} simulated seconds", timeoutVal);
		ProcessEvents::trigger("Timeout"_sr, StringRef(msg), e);
		TraceEvent(SevError, "SetupAndRunError").error(e);
	}

	TraceEvent("TracingMissingCodeProbes").log();
	probe::traceMissedProbes(probe::ExecutionContext::Simulation);
	TraceEvent("SimulatedSystemDestruct").log();
	g_simulator->stop();
	destructed = true;
	wait(Never());
	ASSERT(false);
}

BasicSimulationConfig generateBasicSimulationConfig(const BasicTestConfig& testConfig) {
	TestConfig config(testConfig);
	return SimulationConfig(config);
}
