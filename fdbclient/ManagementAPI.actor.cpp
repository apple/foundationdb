/*
 * ManagementAPI.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include <string>
#include <vector>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbclient/Knobs.h"
#include "flow/Arena.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "fdbclient/SystemData.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/StatusClient.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Replication.h"
#include "fdbclient/Schemas.h"

#include "flow/actorcompiler.h" // This must be the last #include.

bool isInteger(const std::string& s) {
	if (s.empty())
		return false;
	char* p;
	strtol(s.c_str(), &p, 10);
	return (*p == 0);
}

// Defines the mapping between configuration names (as exposed by fdbcli, buildConfiguration()) and actual configuration
// parameters
std::map<std::string, std::string> configForToken(std::string const& mode) {
	std::map<std::string, std::string> out;
	std::string p = configKeysPrefix.toString();

	if (mode == "new") {
		out[p + "initialized"] = "1";
		return out;
	}

	if (mode == "tss") {
		// Set temporary marker in config map to mark that this is a tss configuration and not a normal storage/log
		// configuration. A bit of a hack but reuses the parsing code nicely.
		out[p + "istss"] = "1";
		return out;
	}

	if (mode == "locked") {
		// Setting this key is interpreted as an instruction to use the normal version-stamp-based mechanism for locking
		// the database.
		out[databaseLockedKey.toString()] = deterministicRandom()->randomUniqueID().toString();
		return out;
	}

	size_t pos;

	// key:=value is unvalidated and unchecked
	pos = mode.find(":=");
	if (pos != std::string::npos) {
		out[p + mode.substr(0, pos)] = mode.substr(pos + 2);
		return out;
	}

	// key=value is constrained to a limited set of options and basic validation is performed
	pos = mode.find("=");
	if (pos != std::string::npos) {
		std::string key = mode.substr(0, pos);
		std::string value = mode.substr(pos + 1);

		if (key == "proxies" && isInteger(value)) {
			printf("Warning: Proxy role is being split into GRV Proxy and Commit Proxy, now prefer configuring "
			       "'grv_proxies' and 'commit_proxies' separately. Generally we should follow that 'commit_proxies'"
			       " is three times of 'grv_proxies' count and 'grv_proxies' should be not more than 4.\n");
			int proxiesCount = atoi(value.c_str());
			if (proxiesCount == -1) {
				proxiesCount = CLIENT_KNOBS->DEFAULT_AUTO_GRV_PROXIES + CLIENT_KNOBS->DEFAULT_AUTO_COMMIT_PROXIES;
				ASSERT_WE_THINK(proxiesCount >= 2);
			}

			if (proxiesCount < 2) {
				printf("Error: At least 2 proxies (1 GRV proxy and 1 Commit proxy) are required.\n");
				return out;
			}

			int grvProxyCount = std::max(1,
			                             std::min(CLIENT_KNOBS->DEFAULT_MAX_GRV_PROXIES,
			                                      proxiesCount / (CLIENT_KNOBS->DEFAULT_COMMIT_GRV_PROXIES_RATIO + 1)));
			int commitProxyCount = proxiesCount - grvProxyCount;
			ASSERT_WE_THINK(grvProxyCount >= 1 && commitProxyCount >= 1);

			out[p + "grv_proxies"] = std::to_string(grvProxyCount);
			out[p + "commit_proxies"] = std::to_string(commitProxyCount);
			printf("%d proxies are automatically converted into %d GRV proxies and %d Commit proxies.\n",
			       proxiesCount,
			       grvProxyCount,
			       commitProxyCount);

			TraceEvent("DatabaseConfigurationProxiesSpecified")
			    .detail("SpecifiedProxies", atoi(value.c_str()))
			    .detail("EffectiveSpecifiedProxies", proxiesCount)
			    .detail("ConvertedGrvProxies", grvProxyCount)
			    .detail("ConvertedCommitProxies", commitProxyCount);
		}

		if ((key == "logs" || key == "commit_proxies" || key == "grv_proxies" || key == "resolvers" ||
		     key == "remote_logs" || key == "log_routers" || key == "usable_regions" ||
		     key == "repopulate_anti_quorum" || key == "count") &&
		    isInteger(value)) {
			out[p + key] = value;
		}

		if (key == "regions") {
			json_spirit::mValue mv;
			json_spirit::read_string(value, mv);

			StatusObject regionObj;
			regionObj["regions"] = mv;
			out[p + key] =
			    BinaryWriter::toValue(regionObj, IncludeVersion(ProtocolVersion::withRegionConfiguration())).toString();
		}

		if (key == "perpetual_storage_wiggle" && isInteger(value)) {
			int ppWiggle = std::stoi(value);
			if (ppWiggle >= 2 || ppWiggle < 0) {
				printf("Error: Only 0 and 1 are valid values of perpetual_storage_wiggle at present.\n");
				return out;
			}
			out[p + key] = value;
		}
		if (key == "perpetual_storage_wiggle_locality") {
			if (!isValidPerpetualStorageWiggleLocality(value)) {
				printf("Error: perpetual_storage_wiggle_locality should be in <locality_key>:<locality_value> "
				       "format or enter 0 to disable the locality match for wiggling.\n");
				return out;
			}
			out[p + key] = value;
		}
		if (key == "storage_migration_type") {
			StorageMigrationType type;
			if (value == "disabled") {
				type = StorageMigrationType::DISABLED;
			} else if (value == "aggressive") {
				type = StorageMigrationType::AGGRESSIVE;
			} else if (value == "gradual") {
				type = StorageMigrationType::GRADUAL;
			} else {
				printf("Error: Only disabled|aggressive|gradual are valid for storage_migration_type.\n");
				return out;
			}
			out[p + key] = format("%d", type);
		}

		if (key == "blob_granules_enabled") {
			int enabled = std::stoi(value);
			if (enabled != 0 && enabled != 1) {
				printf("Error: Only 0 or 1 are valid values for blob_granules_enabled. "
				       "1 enables blob granules and 0 disables them.\n");
				return out;
			}
			out[p + key] = value;
		}

		if (key == "tenant_mode") {
			TenantMode tenantMode;
			if (value == "disabled") {
				tenantMode = TenantMode::DISABLED;
			} else if (value == "optional_experimental") {
				tenantMode = TenantMode::OPTIONAL_TENANT;
			} else if (value == "required_experimental") {
				tenantMode = TenantMode::REQUIRED;
			} else {
				printf("Error: Only disabled|optional_experimental|required_experimental are valid for tenant_mode.\n");
				return out;
			}
			out[p + key] = format("%d", tenantMode);
		}
		return out;
	}

	Optional<KeyValueStoreType> logType;
	Optional<KeyValueStoreType> storeType;
	if (mode == "ssd-1") {
		logType = KeyValueStoreType::SSD_BTREE_V1;
		storeType = KeyValueStoreType::SSD_BTREE_V1;
	} else if (mode == "ssd" || mode == "ssd-2") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_BTREE_V2;
	} else if (mode == "ssd-redwood-1-experimental") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_REDWOOD_V1;
	} else if (mode == "ssd-rocksdb-v1") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_ROCKSDB_V1;
	} else if (mode == "memory" || mode == "memory-2") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::MEMORY;
	} else if (mode == "memory-1") {
		logType = KeyValueStoreType::MEMORY;
		storeType = KeyValueStoreType::MEMORY;
	} else if (mode == "memory-radixtree-beta") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::MEMORY_RADIXTREE;
	}
	// Add any new store types to fdbserver/workloads/ConfigureDatabase, too

	if (storeType.present()) {
		out[p + "log_engine"] = format("%d", logType.get().storeType());
		out[p + "storage_engine"] = format("%d", KeyValueStoreType::StoreType(storeType.get()));
		return out;
	}

	std::string redundancy, log_replicas;
	Reference<IReplicationPolicy> storagePolicy;
	Reference<IReplicationPolicy> tLogPolicy;

	bool redundancySpecified = true;
	if (mode == "single") {
		redundancy = "1";
		log_replicas = "1";
		storagePolicy = tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());

	} else if (mode == "double" || mode == "fast_recovery_double") {
		redundancy = "2";
		log_replicas = "2";
		storagePolicy = tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	} else if (mode == "triple" || mode == "fast_recovery_triple") {
		redundancy = "3";
		log_replicas = "3";
		storagePolicy = tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	} else if (mode == "three_datacenter" || mode == "multi_dc") {
		redundancy = "6";
		log_replicas = "4";
		storagePolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(3,
		                     "dcid",
		                     Reference<IReplicationPolicy>(
		                         new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
		tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2,
		                     "dcid",
		                     Reference<IReplicationPolicy>(
		                         new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
	} else if (mode == "three_datacenter_fallback") {
		redundancy = "4";
		log_replicas = "4";
		storagePolicy = tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2,
		                     "dcid",
		                     Reference<IReplicationPolicy>(
		                         new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
	} else if (mode == "three_data_hall") {
		redundancy = "3";
		log_replicas = "4";
		storagePolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(3, "data_hall", Reference<IReplicationPolicy>(new PolicyOne())));
		tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2,
		                     "data_hall",
		                     Reference<IReplicationPolicy>(
		                         new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
	} else if (mode == "three_data_hall_fallback") {
		redundancy = "2";
		log_replicas = "4";
		storagePolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2, "data_hall", Reference<IReplicationPolicy>(new PolicyOne())));
		tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2,
		                     "data_hall",
		                     Reference<IReplicationPolicy>(
		                         new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
	} else
		redundancySpecified = false;
	if (redundancySpecified) {
		out[p + "storage_replicas"] = redundancy;
		out[p + "log_replicas"] = log_replicas;
		out[p + "log_anti_quorum"] = "0";

		BinaryWriter policyWriter(IncludeVersion(ProtocolVersion::withReplicationPolicy()));
		serializeReplicationPolicy(policyWriter, storagePolicy);
		out[p + "storage_replication_policy"] = policyWriter.toValue().toString();

		policyWriter = BinaryWriter(IncludeVersion(ProtocolVersion::withReplicationPolicy()));
		serializeReplicationPolicy(policyWriter, tLogPolicy);
		out[p + "log_replication_policy"] = policyWriter.toValue().toString();
		return out;
	}

	std::string remote_redundancy, remote_log_replicas;
	Reference<IReplicationPolicy> remoteTLogPolicy;
	bool remoteRedundancySpecified = true;
	if (mode == "remote_default") {
		remote_redundancy = "0";
		remote_log_replicas = "0";
		remoteTLogPolicy = Reference<IReplicationPolicy>();
	} else if (mode == "remote_single") {
		remote_redundancy = "1";
		remote_log_replicas = "1";
		remoteTLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
	} else if (mode == "remote_double") {
		remote_redundancy = "2";
		remote_log_replicas = "2";
		remoteTLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	} else if (mode == "remote_triple") {
		remote_redundancy = "3";
		remote_log_replicas = "3";
		remoteTLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	} else if (mode == "remote_three_data_hall") { // FIXME: not tested in simulation
		remote_redundancy = "3";
		remote_log_replicas = "4";
		remoteTLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(2,
		                     "data_hall",
		                     Reference<IReplicationPolicy>(
		                         new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
	} else
		remoteRedundancySpecified = false;
	if (remoteRedundancySpecified) {
		out[p + "remote_log_replicas"] = remote_log_replicas;

		BinaryWriter policyWriter(IncludeVersion(ProtocolVersion::withReplicationPolicy()));
		serializeReplicationPolicy(policyWriter, remoteTLogPolicy);
		out[p + "remote_log_policy"] = policyWriter.toValue().toString();
		return out;
	}

	return out;
}

ConfigurationResult buildConfiguration(std::vector<StringRef> const& modeTokens,
                                       std::map<std::string, std::string>& outConf) {
	for (auto it : modeTokens) {
		std::string mode = it.toString();
		auto m = configForToken(mode);
		if (!m.size()) {
			TraceEvent(SevWarnAlways, "UnknownOption").detail("Option", mode);
			return ConfigurationResult::UNKNOWN_OPTION;
		}

		for (auto t = m.begin(); t != m.end(); ++t) {
			if (outConf.count(t->first)) {
				TraceEvent(SevWarnAlways, "ConflictingOption").detail("Option", t->first);
				return ConfigurationResult::CONFLICTING_OPTIONS;
			}
			outConf[t->first] = t->second;
		}
	}
	auto p = configKeysPrefix.toString();
	if (!outConf.count(p + "storage_replication_policy") && outConf.count(p + "storage_replicas")) {
		int storageCount = stoi(outConf[p + "storage_replicas"]);
		Reference<IReplicationPolicy> storagePolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(storageCount, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		BinaryWriter policyWriter(IncludeVersion(ProtocolVersion::withReplicationPolicy()));
		serializeReplicationPolicy(policyWriter, storagePolicy);
		outConf[p + "storage_replication_policy"] = policyWriter.toValue().toString();
	}

	if (!outConf.count(p + "log_replication_policy") && outConf.count(p + "log_replicas")) {
		int logCount = stoi(outConf[p + "log_replicas"]);
		Reference<IReplicationPolicy> logPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(logCount, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		BinaryWriter policyWriter(IncludeVersion(ProtocolVersion::withReplicationPolicy()));
		serializeReplicationPolicy(policyWriter, logPolicy);
		outConf[p + "log_replication_policy"] = policyWriter.toValue().toString();
	}
	if (outConf.count(p + "istss")) {
		// redo config parameters to be tss config instead of normal config

		// save param values from parsing as a normal config
		bool isNew = outConf.count(p + "initialized");
		Optional<std::string> count;
		Optional<std::string> storageEngine;
		if (outConf.count(p + "count")) {
			count = Optional<std::string>(outConf[p + "count"]);
		}
		if (outConf.count(p + "storage_engine")) {
			storageEngine = Optional<std::string>(outConf[p + "storage_engine"]);
		}

		// A new tss setup must have count + storage engine. An adjustment must have at least one.
		if ((isNew && (!count.present() || !storageEngine.present())) ||
		    (!isNew && !count.present() && !storageEngine.present())) {
			return ConfigurationResult::INCOMPLETE_CONFIGURATION;
		}

		// clear map and only reset tss parameters
		outConf.clear();
		if (count.present()) {
			outConf[p + "tss_count"] = count.get();
		}
		if (storageEngine.present()) {
			outConf[p + "tss_storage_engine"] = storageEngine.get();
		}
	}
	return ConfigurationResult::SUCCESS;
}

ConfigurationResult buildConfiguration(std::string const& configMode, std::map<std::string, std::string>& outConf) {
	std::vector<StringRef> modes;

	int p = 0;
	while (p < configMode.size()) {
		int end = configMode.find_first_of(' ', p);
		if (end == configMode.npos)
			end = configMode.size();
		modes.push_back(StringRef(configMode).substr(p, end - p));
		p = end + 1;
	}

	return buildConfiguration(modes, outConf);
}

bool isCompleteConfiguration(std::map<std::string, std::string> const& options) {
	std::string p = configKeysPrefix.toString();

	return options.count(p + "log_replicas") == 1 && options.count(p + "log_anti_quorum") == 1 &&
	       options.count(p + "storage_replicas") == 1 && options.count(p + "log_engine") == 1 &&
	       options.count(p + "storage_engine") == 1;
}

ACTOR Future<DatabaseConfiguration> getDatabaseConfiguration(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult res = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(res.size() < CLIENT_KNOBS->TOO_MANY);
			DatabaseConfiguration config;
			config.fromKeyValues((VectorRef<KeyValueRef>)res);
			return config;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ConfigureAutoResult parseConfig(StatusObject const& status) {
	ConfigureAutoResult result;
	StatusObjectReader statusObj(status);

	StatusObjectReader statusObjCluster;
	if (!statusObj.get("cluster", statusObjCluster))
		return ConfigureAutoResult();

	StatusObjectReader statusObjConfig;
	if (!statusObjCluster.get("configuration", statusObjConfig))
		return ConfigureAutoResult();

	if (!statusObjConfig.get("redundancy.factor", result.old_replication))
		return ConfigureAutoResult();

	result.auto_replication = result.old_replication;

	[[maybe_unused]] int storage_replication;
	int log_replication;
	if (result.old_replication == "single") {
		result.auto_replication = "double";
		storage_replication = 2;
		log_replication = 2;
	} else if (result.old_replication == "double" || result.old_replication == "fast_recovery_double") {
		storage_replication = 2;
		log_replication = 2;
	} else if (result.old_replication == "triple" || result.old_replication == "fast_recovery_triple") {
		storage_replication = 3;
		log_replication = 3;
	} else if (result.old_replication == "three_datacenter") {
		storage_replication = 6;
		log_replication = 4;
	} else if (result.old_replication == "three_datacenter_fallback") {
		storage_replication = 4;
		log_replication = 4;
	} else if (result.old_replication == "three_data_hall") {
		storage_replication = 3;
		log_replication = 4;
	} else if (result.old_replication == "three_data_hall_fallback") {
		storage_replication = 2;
		log_replication = 4;
	} else
		return ConfigureAutoResult();

	StatusObjectReader machinesMap;
	if (!statusObjCluster.get("machines", machinesMap))
		return ConfigureAutoResult();

	std::map<std::string, std::string> machineid_dcid;
	std::set<std::string> datacenters;
	int machineCount = 0;
	for (auto mach : machinesMap.obj()) {
		StatusObjectReader machine(mach.second);
		std::string dcId;
		if (machine.get("datacenter_id", dcId)) {
			machineid_dcid[mach.first] = dcId;
			datacenters.insert(dcId);
		}
		machineCount++;
	}

	result.machines = machineCount;

	if (datacenters.size() > 1)
		return ConfigureAutoResult();

	StatusObjectReader processesMap;
	if (!statusObjCluster.get("processes", processesMap))
		return ConfigureAutoResult();

	std::set<std::string> oldMachinesWithTransaction;
	int oldTransactionProcesses = 0;
	std::map<std::string, std::vector<std::pair<NetworkAddress, ProcessClass>>> machine_processes;
	int processCount = 0;
	for (auto proc : processesMap.obj()) {
		StatusObjectReader process(proc.second);
		if (!process.has("excluded") || !process.last().get_bool()) {
			std::string addrStr;
			if (!process.get("address", addrStr))
				return ConfigureAutoResult();
			std::string class_source;
			if (!process.get("class_source", class_source))
				return ConfigureAutoResult();
			std::string class_type;
			if (!process.get("class_type", class_type))
				return ConfigureAutoResult();
			std::string machineId;
			if (!process.get("machine_id", machineId))
				return ConfigureAutoResult();

			NetworkAddress addr = NetworkAddress::parse(addrStr);
			ProcessClass processClass(class_type, class_source);

			if (processClass.classType() == ProcessClass::TransactionClass ||
			    processClass.classType() == ProcessClass::LogClass) {
				oldMachinesWithTransaction.insert(machineId);
			}

			if (processClass.classType() == ProcessClass::TransactionClass ||
			    processClass.classType() == ProcessClass::CommitProxyClass ||
			    processClass.classType() == ProcessClass::GrvProxyClass ||
			    processClass.classType() == ProcessClass::ResolutionClass ||
			    processClass.classType() == ProcessClass::StatelessClass ||
			    processClass.classType() == ProcessClass::LogClass) {
				oldTransactionProcesses++;
			}

			if (processClass.classSource() == ProcessClass::AutoSource) {
				processClass = ProcessClass(ProcessClass::UnsetClass, ProcessClass::CommandLineSource);
				result.address_class[addr] = processClass;
			}

			if (processClass.classType() != ProcessClass::TesterClass) {
				machine_processes[machineId].emplace_back(addr, processClass);
				processCount++;
			}
		}
	}

	result.processes = processCount;
	result.old_processes_with_transaction = oldTransactionProcesses;
	result.old_machines_with_transaction = oldMachinesWithTransaction.size();

	std::map<std::pair<int, std::string>, std::vector<std::pair<NetworkAddress, ProcessClass>>> count_processes;
	for (auto& it : machine_processes) {
		count_processes[std::make_pair(it.second.size(), it.first)] = it.second;
	}

	std::set<std::string> machinesWithTransaction;
	std::set<std::string> machinesWithStorage;
	int totalTransactionProcesses = 0;
	int existingProxyCount = 0;
	int existingGrvProxyCount = 0;
	int existingResolverCount = 0;
	int existingStatelessCount = 0;
	for (auto& it : machine_processes) {
		for (auto& proc : it.second) {
			if (proc.second == ProcessClass::TransactionClass || proc.second == ProcessClass::LogClass) {
				totalTransactionProcesses++;
				machinesWithTransaction.insert(it.first);
			}
			if (proc.second == ProcessClass::StatelessClass) {
				existingStatelessCount++;
			}
			if (proc.second == ProcessClass::CommitProxyClass) {
				existingProxyCount++;
			}
			if (proc.second == ProcessClass::GrvProxyClass) {
				existingGrvProxyCount++;
			}
			if (proc.second == ProcessClass::ResolutionClass) {
				existingResolverCount++;
			}
			if (proc.second == ProcessClass::StorageClass) {
				machinesWithStorage.insert(it.first);
			}
			if (proc.second == ProcessClass::UnsetClass && proc.second.classSource() == ProcessClass::DBSource) {
				machinesWithStorage.insert(it.first);
			}
		}
	}

	if (processCount < 10)
		return ConfigureAutoResult();

	result.desired_resolvers = 1;
	int resolverCount;
	if (!statusObjConfig.get("resolvers", result.old_resolvers)) {
		result.old_resolvers = CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS;
		statusObjConfig.get("auto_resolvers", result.old_resolvers);
		result.auto_resolvers = result.desired_resolvers;
		resolverCount = result.auto_resolvers;
	} else {
		result.auto_resolvers = result.old_resolvers;
		resolverCount = result.old_resolvers;
	}

	result.desired_commit_proxies = std::max(std::min(12, processCount / 15), 1);
	int proxyCount;
	if (!statusObjConfig.get("commit_proxies", result.old_commit_proxies)) {
		result.old_commit_proxies = CLIENT_KNOBS->DEFAULT_AUTO_COMMIT_PROXIES;
		statusObjConfig.get("auto_commit_proxies", result.old_commit_proxies);
		result.auto_commit_proxies = result.desired_commit_proxies;
		proxyCount = result.auto_commit_proxies;
	} else {
		result.auto_commit_proxies = result.old_commit_proxies;
		proxyCount = result.old_commit_proxies;
	}

	result.desired_grv_proxies = std::max(std::min(4, processCount / 20), 1);
	int grvProxyCount;
	if (!statusObjConfig.get("grv_proxies", result.old_grv_proxies)) {
		result.old_grv_proxies = CLIENT_KNOBS->DEFAULT_AUTO_GRV_PROXIES;
		statusObjConfig.get("auto_grv_proxies", result.old_grv_proxies);
		result.auto_grv_proxies = result.desired_grv_proxies;
		grvProxyCount = result.auto_grv_proxies;
	} else {
		result.auto_grv_proxies = result.old_grv_proxies;
		grvProxyCount = result.old_grv_proxies;
	}

	result.desired_logs = std::min(12, processCount / 20);
	result.desired_logs = std::max(result.desired_logs, log_replication + 1);
	result.desired_logs = std::min<int>(result.desired_logs, machine_processes.size());
	int logCount;
	if (!statusObjConfig.get("logs", result.old_logs)) {
		result.old_logs = CLIENT_KNOBS->DEFAULT_AUTO_LOGS;
		statusObjConfig.get("auto_logs", result.old_logs);
		result.auto_logs = result.desired_logs;
		logCount = result.auto_logs;
	} else {
		result.auto_logs = result.old_logs;
		logCount = result.old_logs;
	}

	logCount = std::max(logCount, log_replication);

	totalTransactionProcesses += std::min(existingProxyCount, proxyCount);
	totalTransactionProcesses += std::min(existingGrvProxyCount, grvProxyCount);
	totalTransactionProcesses += std::min(existingResolverCount, resolverCount);
	totalTransactionProcesses += existingStatelessCount;

	// if one process on a machine is transaction class, make them all transaction class
	for (auto& it : count_processes) {
		if (machinesWithTransaction.count(it.first.second) && !machinesWithStorage.count(it.first.second)) {
			for (auto& proc : it.second) {
				if (proc.second == ProcessClass::UnsetClass &&
				    proc.second.classSource() == ProcessClass::CommandLineSource) {
					result.address_class[proc.first] =
					    ProcessClass(ProcessClass::TransactionClass, ProcessClass::AutoSource);
					totalTransactionProcesses++;
				}
			}
		}
	}

	int desiredTotalTransactionProcesses = logCount + resolverCount + proxyCount + grvProxyCount;

	// add machines with all transaction class until we have enough processes and enough machines
	for (auto& it : count_processes) {
		if (machinesWithTransaction.size() >= logCount && totalTransactionProcesses >= desiredTotalTransactionProcesses)
			break;

		if (!machinesWithTransaction.count(it.first.second) && !machinesWithStorage.count(it.first.second)) {
			for (auto& proc : it.second) {
				if (proc.second == ProcessClass::UnsetClass &&
				    proc.second.classSource() == ProcessClass::CommandLineSource) {
					ASSERT(proc.second != ProcessClass::TransactionClass);
					result.address_class[proc.first] =
					    ProcessClass(ProcessClass::TransactionClass, ProcessClass::AutoSource);
					totalTransactionProcesses++;
					machinesWithTransaction.insert(it.first.second);
				}
			}
		}
	}

	if (machinesWithTransaction.size() < logCount || totalTransactionProcesses < desiredTotalTransactionProcesses)
		return ConfigureAutoResult();

	result.auto_processes_with_transaction = totalTransactionProcesses;
	result.auto_machines_with_transaction = machinesWithTransaction.size();

	if (3 * totalTransactionProcesses > processCount)
		return ConfigureAutoResult();

	return result;
}

ACTOR Future<std::vector<ProcessData>> getWorkers(Transaction* tr) {
	state Future<RangeResult> processClasses = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
	state Future<RangeResult> processData = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);

	wait(success(processClasses) && success(processData));
	ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
	ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

	std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
	for (int i = 0; i < processClasses.get().size(); i++) {
		id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
		    decodeProcessClassValue(processClasses.get()[i].value);
	}

	std::vector<ProcessData> results;

	for (int i = 0; i < processData.get().size(); i++) {
		ProcessData data = decodeWorkerListValue(processData.get()[i].value);
		ProcessClass processClass = id_class[data.locality.processId()];

		if (processClass.classSource() == ProcessClass::DBSource ||
		    data.processClass.classType() == ProcessClass::UnsetClass)
			data.processClass = processClass;

		if (data.processClass.classType() != ProcessClass::TesterClass)
			results.push_back(data);
	}

	return results;
}

ACTOR Future<std::vector<ProcessData>> getWorkers(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // necessary?
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<ProcessData> workers = wait(getWorkers(&tr));
			return workers;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<NetworkAddress>> getCoordinators(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> currentKey = wait(tr.get(coordinatorsKey));
			if (!currentKey.present())
				return std::vector<NetworkAddress>();

			return ClusterConnectionString(currentKey.get().toString()).coordinators();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Optional<CoordinatorsResult>> changeQuorumChecker(Transaction* tr,
                                                               Reference<IQuorumChange> change,
                                                               ClusterConnectionString* conn) {
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	Optional<Value> currentKey = wait(tr->get(coordinatorsKey));

	if (!currentKey.present())
		return CoordinatorsResult::BAD_DATABASE_STATE; // Someone deleted this key entirely?

	state ClusterConnectionString old(currentKey.get().toString());
	wait(old.resolveHostnames());
	if (tr->getDatabase()->getConnectionRecord() &&
	    old.clusterKeyName().toString() !=
	        tr->getDatabase()->getConnectionRecord()->getConnectionString().clusterKeyName())
		return CoordinatorsResult::BAD_DATABASE_STATE; // Someone changed the "name" of the database??

	state CoordinatorsResult result = CoordinatorsResult::SUCCESS;
	if (!conn->coords.size()) {
		std::vector<NetworkAddress> desiredCoordinatorAddresses = wait(change->getDesiredCoordinators(
		    tr,
		    old.coordinators(),
		    Reference<ClusterConnectionMemoryRecord>(new ClusterConnectionMemoryRecord(old)),
		    result));
		conn->coords = desiredCoordinatorAddresses;
	}

	if (result != CoordinatorsResult::SUCCESS)
		return result;

	if (!conn->coordinators().size())
		return CoordinatorsResult::INVALID_NETWORK_ADDRESSES;

	std::sort(conn->coords.begin(), conn->coords.end());
	std::sort(conn->hostnames.begin(), conn->hostnames.end());

	std::string newName = change->getDesiredClusterKeyName();
	if (newName.empty())
		newName = old.clusterKeyName().toString();

	if (old.coordinators() == conn->coordinators() && old.clusterKeyName() == newName)
		return CoordinatorsResult::SAME_NETWORK_ADDRESSES;

	std::string key(newName + ':' + deterministicRandom()->randomAlphaNumeric(32));
	conn->parseKey(key);
	conn->resetConnectionString();

	if (g_network->isSimulated()) {
		int i = 0;
		int protectedCount = 0;
		while ((protectedCount < ((conn->coordinators().size() / 2) + 1)) && (i < conn->coordinators().size())) {
			auto process = g_simulator.getProcessByAddress(conn->coordinators()[i]);
			auto addresses = process->addresses;

			if (!process->isReliable()) {
				i++;
				continue;
			}

			g_simulator.protectedAddresses.insert(process->addresses.address);
			if (addresses.secondaryAddress.present()) {
				g_simulator.protectedAddresses.insert(process->addresses.secondaryAddress.get());
			}
			TraceEvent("ProtectCoordinator").detail("Address", conn->coordinators()[i]).backtrace();
			protectedCount++;
			i++;
		}
	}

	std::vector<Future<Optional<LeaderInfo>>> leaderServers;
	ClientCoordinators coord(Reference<ClusterConnectionMemoryRecord>(new ClusterConnectionMemoryRecord(*conn)));

	leaderServers.reserve(coord.clientLeaderServers.size());
	for (int i = 0; i < coord.clientLeaderServers.size(); i++)
		leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
		                                           GetLeaderRequest(coord.clusterKey, UID()),
		                                           TaskPriority::CoordinationReply));

	choose {
		when(wait(waitForAll(leaderServers))) {}
		when(wait(delay(5.0))) { return CoordinatorsResult::COORDINATOR_UNREACHABLE; }
	}
	tr->set(coordinatorsKey, conn->toString());
	return Optional<CoordinatorsResult>();
}

ACTOR Future<CoordinatorsResult> changeQuorum(Database cx, Reference<IQuorumChange> change) {
	state Transaction tr(cx);
	state int retries = 0;
	state std::vector<NetworkAddress> desiredCoordinators;
	state int notEnoughMachineResults = 0;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> currentKey = wait(tr.get(coordinatorsKey));

			if (!currentKey.present())
				return CoordinatorsResult::BAD_DATABASE_STATE; // Someone deleted this key entirely?

			state ClusterConnectionString old(currentKey.get().toString());
			if (cx->getConnectionRecord() &&
			    old.clusterKeyName().toString() != cx->getConnectionRecord()->getConnectionString().clusterKeyName())
				return CoordinatorsResult::BAD_DATABASE_STATE; // Someone changed the "name" of the database??

			state CoordinatorsResult result = CoordinatorsResult::SUCCESS;
			if (!desiredCoordinators.size()) {
				std::vector<NetworkAddress> _desiredCoordinators = wait(change->getDesiredCoordinators(
				    &tr,
				    old.coordinators(),
				    Reference<ClusterConnectionMemoryRecord>(new ClusterConnectionMemoryRecord(old)),
				    result));
				desiredCoordinators = _desiredCoordinators;
			}

			if (result == CoordinatorsResult::NOT_ENOUGH_MACHINES && notEnoughMachineResults < 1) {
				// we could get not_enough_machines if we happen to see the database while the cluster controller is
				// updating the worker list, so make sure it happens twice before returning a failure
				notEnoughMachineResults++;
				wait(delay(1.0));
				tr.reset();
				continue;
			}
			if (result != CoordinatorsResult::SUCCESS)
				return result;
			if (!desiredCoordinators.size())
				return CoordinatorsResult::INVALID_NETWORK_ADDRESSES;
			std::sort(desiredCoordinators.begin(), desiredCoordinators.end());

			std::string newName = change->getDesiredClusterKeyName();
			if (newName.empty())
				newName = old.clusterKeyName().toString();

			if (old.coordinators() == desiredCoordinators && old.clusterKeyName() == newName)
				return retries ? CoordinatorsResult::SUCCESS : CoordinatorsResult::SAME_NETWORK_ADDRESSES;

			state ClusterConnectionString conn(
			    desiredCoordinators, StringRef(newName + ':' + deterministicRandom()->randomAlphaNumeric(32)));

			if (g_network->isSimulated()) {
				for (int i = 0; i < (desiredCoordinators.size() / 2) + 1; i++) {
					auto process = g_simulator.getProcessByAddress(desiredCoordinators[i]);
					ASSERT(process->isReliable() || process->rebooting);

					g_simulator.protectedAddresses.insert(process->addresses.address);
					if (process->addresses.secondaryAddress.present()) {
						g_simulator.protectedAddresses.insert(process->addresses.secondaryAddress.get());
					}
					TraceEvent("ProtectCoordinator").detail("Address", desiredCoordinators[i]).backtrace();
				}
			}

			TraceEvent("AttemptingQuorumChange").detail("FromCS", old.toString()).detail("ToCS", conn.toString());
			TEST(old.clusterKeyName() != conn.clusterKeyName()); // Quorum change with new name
			TEST(old.clusterKeyName() == conn.clusterKeyName()); // Quorum change with unchanged name

			state std::vector<Future<Optional<LeaderInfo>>> leaderServers;
			state ClientCoordinators coord(
			    Reference<ClusterConnectionMemoryRecord>(new ClusterConnectionMemoryRecord(conn)));
			// check if allowed to modify the cluster descriptor
			if (!change->getDesiredClusterKeyName().empty()) {
				CheckDescriptorMutableReply mutabilityReply =
				    wait(coord.clientLeaderServers[0].checkDescriptorMutable.getReply(CheckDescriptorMutableRequest()));
				if (!mutabilityReply.isMutable)
					return CoordinatorsResult::BAD_DATABASE_STATE;
			}
			leaderServers.reserve(coord.clientLeaderServers.size());
			for (int i = 0; i < coord.clientLeaderServers.size(); i++)
				leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
				                                           GetLeaderRequest(coord.clusterKey, UID()),
				                                           TaskPriority::CoordinationReply));
			choose {
				when(wait(waitForAll(leaderServers))) {}
				when(wait(delay(5.0))) { return CoordinatorsResult::COORDINATOR_UNREACHABLE; }
			}

			tr.set(coordinatorsKey, conn.toString());

			wait(tr.commit());
			ASSERT(false); // commit should fail, but the value has changed
		} catch (Error& e) {
			TraceEvent("RetryQuorumChange").error(e).detail("Retries", retries);
			wait(tr.onError(e));
			++retries;
		}
	}
}

struct SpecifiedQuorumChange final : IQuorumChange {
	std::vector<NetworkAddress> desired;
	explicit SpecifiedQuorumChange(std::vector<NetworkAddress> const& desired) : desired(desired) {}
	Future<std::vector<NetworkAddress>> getDesiredCoordinators(Transaction* tr,
	                                                           std::vector<NetworkAddress> oldCoordinators,
	                                                           Reference<IClusterConnectionRecord>,
	                                                           CoordinatorsResult&) override {
		return desired;
	}
};
Reference<IQuorumChange> specifiedQuorumChange(std::vector<NetworkAddress> const& addresses) {
	return Reference<IQuorumChange>(new SpecifiedQuorumChange(addresses));
}

struct NoQuorumChange final : IQuorumChange {
	Future<std::vector<NetworkAddress>> getDesiredCoordinators(Transaction* tr,
	                                                           std::vector<NetworkAddress> oldCoordinators,
	                                                           Reference<IClusterConnectionRecord>,
	                                                           CoordinatorsResult&) override {
		return oldCoordinators;
	}
};
Reference<IQuorumChange> noQuorumChange() {
	return Reference<IQuorumChange>(new NoQuorumChange);
}

struct NameQuorumChange final : IQuorumChange {
	std::string newName;
	Reference<IQuorumChange> otherChange;
	explicit NameQuorumChange(std::string const& newName, Reference<IQuorumChange> const& otherChange)
	  : newName(newName), otherChange(otherChange) {}
	Future<std::vector<NetworkAddress>> getDesiredCoordinators(Transaction* tr,
	                                                           std::vector<NetworkAddress> oldCoordinators,
	                                                           Reference<IClusterConnectionRecord> ccr,
	                                                           CoordinatorsResult& t) override {
		return otherChange->getDesiredCoordinators(tr, oldCoordinators, ccr, t);
	}
	std::string getDesiredClusterKeyName() const override { return newName; }
};
Reference<IQuorumChange> nameQuorumChange(std::string const& name, Reference<IQuorumChange> const& other) {
	return Reference<IQuorumChange>(new NameQuorumChange(name, other));
}

struct AutoQuorumChange final : IQuorumChange {
	int desired;
	explicit AutoQuorumChange(int desired) : desired(desired) {}

	Future<std::vector<NetworkAddress>> getDesiredCoordinators(Transaction* tr,
	                                                           std::vector<NetworkAddress> oldCoordinators,
	                                                           Reference<IClusterConnectionRecord> ccr,
	                                                           CoordinatorsResult& err) override {
		return getDesired(Reference<AutoQuorumChange>::addRef(this), tr, oldCoordinators, ccr, &err);
	}

	ACTOR static Future<int> getRedundancy(AutoQuorumChange* self, Transaction* tr) {
		state Future<Optional<Value>> fStorageReplicas =
		    tr->get(LiteralStringRef("storage_replicas").withPrefix(configKeysPrefix));
		state Future<Optional<Value>> fLogReplicas =
		    tr->get(LiteralStringRef("log_replicas").withPrefix(configKeysPrefix));
		wait(success(fStorageReplicas) && success(fLogReplicas));
		int redundancy = std::min(atoi(fStorageReplicas.get().get().toString().c_str()),
		                          atoi(fLogReplicas.get().get().toString().c_str()));

		return redundancy;
	}

	ACTOR static Future<bool> isAcceptable(AutoQuorumChange* self,
	                                       Transaction* tr,
	                                       std::vector<NetworkAddress> oldCoordinators,
	                                       Reference<IClusterConnectionRecord> ccr,
	                                       int desiredCount,
	                                       std::set<AddressExclusion>* excluded) {
		// Are there enough coordinators for the redundancy level?
		if (oldCoordinators.size() < desiredCount)
			return false;
		if (oldCoordinators.size() % 2 != 1)
			return false;

		// Check availability
		ClientCoordinators coord(ccr);
		std::vector<Future<Optional<LeaderInfo>>> leaderServers;
		leaderServers.reserve(coord.clientLeaderServers.size());
		for (int i = 0; i < coord.clientLeaderServers.size(); i++) {
			leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
			                                           GetLeaderRequest(coord.clusterKey, UID()),
			                                           TaskPriority::CoordinationReply));
		}
		Optional<std::vector<Optional<LeaderInfo>>> results =
		    wait(timeout(getAll(leaderServers), CLIENT_KNOBS->IS_ACCEPTABLE_DELAY));
		if (!results.present()) {
			return false;
		} // Not all responded
		for (auto& r : results.get()) {
			if (!r.present()) {
				return false; // Coordinator doesn't know about this database?
			}
		}

		// Check exclusions
		for (auto& c : oldCoordinators) {
			if (addressExcluded(*excluded, c))
				return false;
		}

		// Check locality
		// FIXME: Actual locality!
		std::sort(oldCoordinators.begin(), oldCoordinators.end());
		for (int i = 1; i < oldCoordinators.size(); i++)
			if (oldCoordinators[i - 1].ip == oldCoordinators[i].ip)
				return false; // Multiple coordinators share an IP

		return true; // The status quo seems fine
	}

	ACTOR static Future<std::vector<NetworkAddress>> getDesired(Reference<AutoQuorumChange> self,
	                                                            Transaction* tr,
	                                                            std::vector<NetworkAddress> oldCoordinators,
	                                                            Reference<IClusterConnectionRecord> ccr,
	                                                            CoordinatorsResult* err) {
		state int desiredCount = self->desired;

		if (desiredCount == -1) {
			int redundancy = wait(getRedundancy(self.getPtr(), tr));
			desiredCount = redundancy * 2 - 1;
		}

		std::vector<AddressExclusion> excl = wait(getExcludedServers(tr));
		state std::set<AddressExclusion> excluded(excl.begin(), excl.end());

		std::vector<ProcessData> _workers = wait(getWorkers(tr));
		state std::vector<ProcessData> workers = _workers;

		std::map<NetworkAddress, LocalityData> addr_locality;
		for (auto w : workers)
			addr_locality[w.address] = w.locality;

		// since we don't have the locality data for oldCoordinators:
		//     check if every old coordinator is in the workers vector and
		//     check if multiple old coordinators map to the same locality data (same machine)
		bool checkAcceptable = true;
		std::set<Optional<Standalone<StringRef>>> checkDuplicates;
		for (auto addr : oldCoordinators) {
			auto findResult = addr_locality.find(addr);
			if (findResult == addr_locality.end() || checkDuplicates.count(findResult->second.zoneId())) {
				checkAcceptable = false;
				break;
			}
			checkDuplicates.insert(findResult->second.zoneId());
		}

		if (checkAcceptable) {
			bool ok = wait(isAcceptable(self.getPtr(), tr, oldCoordinators, ccr, desiredCount, &excluded));
			if (ok)
				return oldCoordinators;
		}

		std::vector<NetworkAddress> chosen;
		self->addDesiredWorkers(chosen, workers, desiredCount, excluded);

		if (chosen.size() < desiredCount) {
			if (chosen.empty() || chosen.size() < oldCoordinators.size()) {
				TraceEvent("NotEnoughMachinesForCoordinators")
				    .detail("EligibleWorkers", workers.size())
				    .detail("ChosenWorkers", chosen.size())
				    .detail("DesiredCoordinators", desiredCount)
				    .detail("CurrentCoordinators", oldCoordinators.size());
				*err = CoordinatorsResult::NOT_ENOUGH_MACHINES;
				return std::vector<NetworkAddress>();
			}
			chosen.resize((chosen.size() - 1) | 1);
		}

		return chosen;
	}

	// Select a desired set of workers such that
	// (1) the number of workers at each locality type (e.g., dcid) <= desiredCount; and
	// (2) prefer workers at a locality where less workers has been chosen than other localities: evenly distribute
	// workers.
	void addDesiredWorkers(std::vector<NetworkAddress>& chosen,
	                       const std::vector<ProcessData>& workers,
	                       int desiredCount,
	                       const std::set<AddressExclusion>& excluded) {
		std::vector<ProcessData> remainingWorkers(workers);
		deterministicRandom()->randomShuffle(remainingWorkers);

		std::partition(remainingWorkers.begin(), remainingWorkers.end(), [](const ProcessData& data) {
			return (data.processClass == ProcessClass::CoordinatorClass);
		});

		TraceEvent(SevDebug, "AutoSelectCoordinators").detail("CandidateWorkers", remainingWorkers.size());
		for (auto worker = remainingWorkers.begin(); worker != remainingWorkers.end(); worker++) {
			TraceEvent(SevDebug, "AutoSelectCoordinators")
			    .detail("Worker", worker->processClass.toString())
			    .detail("Address", worker->address.toString())
			    .detail("Locality", worker->locality.toString());
		}
		TraceEvent(SevDebug, "AutoSelectCoordinators").detail("ExcludedAddress", excluded.size());
		for (auto& excludedAddr : excluded) {
			TraceEvent(SevDebug, "AutoSelectCoordinators").detail("ExcludedAddress", excludedAddr.toString());
		}

		std::map<StringRef, int> maxCounts;
		std::map<StringRef, std::map<StringRef, int>> currentCounts;
		std::map<StringRef, int> hardLimits;

		std::vector<StringRef> fields({ LiteralStringRef("dcid"),
		                                LiteralStringRef("data_hall"),
		                                LiteralStringRef("zoneid"),
		                                LiteralStringRef("machineid") });

		for (auto field = fields.begin(); field != fields.end(); field++) {
			if (field->toString() == "zoneid") {
				hardLimits[*field] = 1;
			} else {
				hardLimits[*field] = desiredCount;
			}
		}

		while (chosen.size() < desiredCount) {
			bool found = false;
			for (auto worker = remainingWorkers.begin(); worker != remainingWorkers.end(); worker++) {
				if (addressExcluded(excluded, worker->address)) {
					continue;
				}
				// Exclude faulty node due to machine assassination
				if (g_network->isSimulated() && !g_simulator.getProcessByAddress(worker->address)->isReliable()) {
					TraceEvent("AutoSelectCoordinators").detail("SkipUnreliableWorker", worker->address.toString());
					continue;
				}
				bool valid = true;
				for (auto field = fields.begin(); field != fields.end(); field++) {
					if (maxCounts[*field] == 0) {
						maxCounts[*field] = 1;
					}
					auto value = worker->locality.get(*field).orDefault(LiteralStringRef(""));
					auto currentCount = currentCounts[*field][value];
					if (currentCount >= maxCounts[*field]) {
						valid = false;
						break;
					}
				}
				if (valid) {
					for (auto field = fields.begin(); field != fields.end(); field++) {
						auto value = worker->locality.get(*field).orDefault(LiteralStringRef(""));
						currentCounts[*field][value] += 1;
					}
					chosen.push_back(worker->address);
					remainingWorkers.erase(worker);
					found = true;
					break;
				}
			}
			if (!found) {
				bool canIncrement = false;
				for (auto field = fields.begin(); field != fields.end(); field++) {
					if (maxCounts[*field] < hardLimits[*field]) {
						maxCounts[*field] += 1;
						canIncrement = true;
						break;
					}
				}
				if (!canIncrement) {
					break;
				}
			}
		}
	}
};
Reference<IQuorumChange> autoQuorumChange(int desired) {
	return Reference<IQuorumChange>(new AutoQuorumChange(desired));
}

void excludeServers(Transaction& tr, std::vector<AddressExclusion>& servers, bool failed) {
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	std::string excludeVersionKey = deterministicRandom()->randomUniqueID().toString();
	auto serversVersionKey = failed ? failedServersVersionKey : excludedServersVersionKey;
	tr.addReadConflictRange(singleKeyRange(serversVersionKey)); // To conflict with parallel includeServers
	tr.set(serversVersionKey, excludeVersionKey);
	for (auto& s : servers) {
		if (failed) {
			tr.set(encodeFailedServersKey(s), StringRef());
		} else {
			tr.set(encodeExcludedServersKey(s), StringRef());
		}
	}
	TraceEvent("ExcludeServersCommit").detail("Servers", describe(servers)).detail("ExcludeFailed", failed);
}

ACTOR Future<Void> excludeServers(Database cx, std::vector<AddressExclusion> servers, bool failed) {
	if (cx->apiVersionAtLeast(700)) {
		state ReadYourWritesTransaction ryw(cx);
		loop {
			try {
				ryw.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				ryw.set(
				    SpecialKeySpace::getManagementApiCommandOptionSpecialKey(failed ? "failed" : "excluded", "force"),
				    ValueRef());
				for (auto& s : servers) {
					Key addr = failed
					               ? SpecialKeySpace::getManagementApiCommandPrefix("failed").withSuffix(s.toString())
					               : SpecialKeySpace::getManagementApiCommandPrefix("exclude").withSuffix(s.toString());
					ryw.set(addr, ValueRef());
				}
				TraceEvent("ExcludeServersSpecialKeySpaceCommit")
				    .detail("Servers", describe(servers))
				    .detail("ExcludeFailed", failed);
				wait(ryw.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("ExcludeServersError").errorUnsuppressed(e);
				wait(ryw.onError(e));
			}
		}
	} else {
		state Transaction tr(cx);
		loop {
			try {
				excludeServers(tr, servers, failed);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("ExcludeServersError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}
	}
}

// excludes localities by setting the keys in api version below 7.0
void excludeLocalities(Transaction& tr, std::unordered_set<std::string> localities, bool failed) {
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	std::string excludeVersionKey = deterministicRandom()->randomUniqueID().toString();
	auto localityVersionKey = failed ? failedLocalityVersionKey : excludedLocalityVersionKey;
	tr.addReadConflictRange(singleKeyRange(localityVersionKey)); // To conflict with parallel includeLocalities
	tr.set(localityVersionKey, excludeVersionKey);
	for (const auto& l : localities) {
		if (failed) {
			tr.set(encodeFailedLocalityKey(l), StringRef());
		} else {
			tr.set(encodeExcludedLocalityKey(l), StringRef());
		}
	}
	TraceEvent("ExcludeLocalitiesCommit").detail("Localities", describe(localities)).detail("ExcludeFailed", failed);
}

// Exclude the servers matching the given set of localities from use as state servers.
// excludes localities by setting the keys.
ACTOR Future<Void> excludeLocalities(Database cx, std::unordered_set<std::string> localities, bool failed) {
	if (cx->apiVersionAtLeast(700)) {
		state ReadYourWritesTransaction ryw(cx);
		loop {
			try {
				ryw.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				ryw.set(SpecialKeySpace::getManagementApiCommandOptionSpecialKey(
				            failed ? "failed_locality" : "excluded_locality", "force"),
				        ValueRef());
				for (const auto& l : localities) {
					Key addr = failed
					               ? SpecialKeySpace::getManagementApiCommandPrefix("failedlocality").withSuffix(l)
					               : SpecialKeySpace::getManagementApiCommandPrefix("excludedlocality").withSuffix(l);
					ryw.set(addr, ValueRef());
				}
				TraceEvent("ExcludeLocalitiesSpecialKeySpaceCommit")
				    .detail("Localities", describe(localities))
				    .detail("ExcludeFailed", failed);

				wait(ryw.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("ExcludeLocalitiesError").errorUnsuppressed(e);
				wait(ryw.onError(e));
			}
		}
	} else {
		state Transaction tr(cx);
		loop {
			try {
				excludeLocalities(tr, localities, failed);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("ExcludeLocalitiesError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> includeServers(Database cx, std::vector<AddressExclusion> servers, bool failed) {
	state std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	if (cx->apiVersionAtLeast(700)) {
		state ReadYourWritesTransaction ryw(cx);
		loop {
			try {
				ryw.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				for (auto& s : servers) {
					if (!s.isValid()) {
						if (failed) {
							ryw.clear(SpecialKeySpace::getManagementApiCommandRange("failed"));
						} else {
							ryw.clear(SpecialKeySpace::getManagementApiCommandRange("exclude"));
						}
					} else {
						Key addr =
						    failed ? SpecialKeySpace::getManagementApiCommandPrefix("failed").withSuffix(s.toString())
						           : SpecialKeySpace::getManagementApiCommandPrefix("exclude").withSuffix(s.toString());
						ryw.clear(addr);
						// Eliminate both any ip-level exclusion (1.2.3.4) and any
						// port-level exclusions (1.2.3.4:5)
						// The range ['IP', 'IP;'] was originally deleted. ';' is
						// char(':' + 1). This does not work, as other for all
						// x between 0 and 9, 'IPx' will also be in this range.
						//
						// This is why we now make two clears: first only of the ip
						// address, the second will delete all ports.
						if (s.isWholeMachine())
							ryw.clear(KeyRangeRef(addr.withSuffix(LiteralStringRef(":")),
							                      addr.withSuffix(LiteralStringRef(";"))));
					}
				}
				TraceEvent("IncludeServersCommit").detail("Servers", describe(servers)).detail("Failed", failed);

				wait(ryw.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("IncludeServersError").errorUnsuppressed(e);
				wait(ryw.onError(e));
			}
		}
	} else {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);

				// includeServers might be used in an emergency transaction, so make sure it is retry-self-conflicting
				// and CAUSAL_WRITE_RISKY
				tr.setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
				if (failed) {
					tr.addReadConflictRange(singleKeyRange(failedServersVersionKey));
					tr.set(failedServersVersionKey, versionKey);
				} else {
					tr.addReadConflictRange(singleKeyRange(excludedServersVersionKey));
					tr.set(excludedServersVersionKey, versionKey);
				}

				for (auto& s : servers) {
					if (!s.isValid()) {
						if (failed) {
							tr.clear(failedServersKeys);
						} else {
							tr.clear(excludedServersKeys);
						}
					} else if (s.isWholeMachine()) {
						// Eliminate both any ip-level exclusion (1.2.3.4) and any
						// port-level exclusions (1.2.3.4:5)
						// The range ['IP', 'IP;'] was originally deleted. ';' is
						// char(':' + 1). This does not work, as other for all
						// x between 0 and 9, 'IPx' will also be in this range.
						//
						// This is why we now make two clears: first only of the ip
						// address, the second will delete all ports.
						auto addr = failed ? encodeFailedServersKey(s) : encodeExcludedServersKey(s);
						tr.clear(singleKeyRange(addr));
						tr.clear(KeyRangeRef(addr + ':', addr + char(':' + 1)));
					} else {
						if (failed) {
							tr.clear(encodeFailedServersKey(s));
						} else {
							tr.clear(encodeExcludedServersKey(s));
						}
					}
				}

				TraceEvent("IncludeServersCommit").detail("Servers", describe(servers)).detail("Failed", failed);

				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("IncludeServersError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}
	}
}

// Remove the given localities from the exclusion list.
// include localities by clearing the keys.
ACTOR Future<Void> includeLocalities(Database cx, std::vector<std::string> localities, bool failed, bool includeAll) {
	state std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	if (cx->apiVersionAtLeast(700)) {
		state ReadYourWritesTransaction ryw(cx);
		loop {
			try {
				ryw.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				if (includeAll) {
					if (failed) {
						ryw.clear(SpecialKeySpace::getManagementApiCommandRange("failedlocality"));
					} else {
						ryw.clear(SpecialKeySpace::getManagementApiCommandRange("excludedlocality"));
					}
				} else {
					for (const auto& l : localities) {
						Key locality =
						    failed ? SpecialKeySpace::getManagementApiCommandPrefix("failedlocality").withSuffix(l)
						           : SpecialKeySpace::getManagementApiCommandPrefix("excludedlocality").withSuffix(l);
						ryw.clear(locality);
					}
				}
				TraceEvent("IncludeLocalitiesCommit")
				    .detail("Localities", describe(localities))
				    .detail("Failed", failed)
				    .detail("IncludeAll", includeAll);

				wait(ryw.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("IncludeLocalitiesError").errorUnsuppressed(e);
				wait(ryw.onError(e));
			}
		}
	} else {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);

				// includeLocalities might be used in an emergency transaction, so make sure it is
				// retry-self-conflicting and CAUSAL_WRITE_RISKY
				tr.setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
				if (failed) {
					tr.addReadConflictRange(singleKeyRange(failedLocalityVersionKey));
					tr.set(failedLocalityVersionKey, versionKey);
				} else {
					tr.addReadConflictRange(singleKeyRange(excludedLocalityVersionKey));
					tr.set(excludedLocalityVersionKey, versionKey);
				}

				if (includeAll) {
					if (failed) {
						tr.clear(failedLocalityKeys);
					} else {
						tr.clear(excludedLocalityKeys);
					}
				} else {
					for (const auto& l : localities) {
						if (failed) {
							tr.clear(encodeFailedLocalityKey(l));
						} else {
							tr.clear(encodeExcludedLocalityKey(l));
						}
					}
				}

				TraceEvent("IncludeLocalitiesCommit")
				    .detail("Localities", describe(localities))
				    .detail("Failed", failed)
				    .detail("IncludeAll", includeAll);

				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("IncludeLocalitiesError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> setClass(Database cx, AddressExclusion server, ProcessClass processClass) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);

			std::vector<ProcessData> workers = wait(getWorkers(&tr));

			bool foundChange = false;
			for (int i = 0; i < workers.size(); i++) {
				if (server.excludes(workers[i].address)) {
					if (processClass.classType() != ProcessClass::InvalidClass)
						tr.set(processClassKeyFor(workers[i].locality.processId().get()),
						       processClassValue(processClass));
					else
						tr.clear(processClassKeyFor(workers[i].locality.processId().get()));
					foundChange = true;
				}
			}

			if (foundChange)
				tr.set(processClassChangeKey, deterministicRandom()->randomUniqueID().toString());

			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<AddressExclusion>> getExcludedServers(Transaction* tr) {
	state RangeResult r = wait(tr->getRange(excludedServersKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);
	state RangeResult r2 = wait(tr->getRange(failedServersKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r2.more && r2.size() < CLIENT_KNOBS->TOO_MANY);

	std::vector<AddressExclusion> exclusions;
	for (auto i = r.begin(); i != r.end(); ++i) {
		auto a = decodeExcludedServersKey(i->key);
		if (a.isValid())
			exclusions.push_back(a);
	}
	for (auto i = r2.begin(); i != r2.end(); ++i) {
		auto a = decodeFailedServersKey(i->key);
		if (a.isValid())
			exclusions.push_back(a);
	}
	uniquify(exclusions);
	return exclusions;
}

ACTOR Future<std::vector<AddressExclusion>> getExcludedServers(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // necessary?
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<AddressExclusion> exclusions = wait(getExcludedServers(&tr));
			return exclusions;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Get the current list of excluded localities by reading the keys.
ACTOR Future<std::vector<std::string>> getExcludedLocalities(Transaction* tr) {
	state RangeResult r = wait(tr->getRange(excludedLocalityKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);
	state RangeResult r2 = wait(tr->getRange(failedLocalityKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r2.more && r2.size() < CLIENT_KNOBS->TOO_MANY);

	std::vector<std::string> excludedLocalities;
	for (const auto& i : r) {
		auto a = decodeExcludedLocalityKey(i.key);
		excludedLocalities.push_back(a);
	}
	for (const auto& i : r2) {
		auto a = decodeFailedLocalityKey(i.key);
		excludedLocalities.push_back(a);
	}
	uniquify(excludedLocalities);
	return excludedLocalities;
}

// Get the list of excluded localities by reading the keys.
ACTOR Future<std::vector<std::string>> getExcludedLocalities(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<std::string> exclusions = wait(getExcludedLocalities(&tr));
			return exclusions;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Decodes the locality string to a pair of locality prefix and its value.
// The prefix could be dcid, processid, machineid, processid.
std::pair<std::string, std::string> decodeLocality(const std::string& locality) {
	StringRef localityRef((const uint8_t*)(locality.c_str()), locality.size());

	std::string localityKeyValue = localityRef.removePrefix(LocalityData::ExcludeLocalityPrefix).toString();
	int split = localityKeyValue.find(':');
	if (split != std::string::npos) {
		return std::make_pair(localityKeyValue.substr(0, split), localityKeyValue.substr(split + 1));
	}

	return std::make_pair("", "");
}

// Returns the list of IPAddresses of the workers that match the given locality.
// Example: locality="dcid:primary" returns all the ip addresses of the workers in the primary dc.
std::set<AddressExclusion> getAddressesByLocality(const std::vector<ProcessData>& workers,
                                                  const std::string& locality) {
	std::pair<std::string, std::string> localityKeyValue = decodeLocality(locality);

	std::set<AddressExclusion> localityAddresses;
	for (int i = 0; i < workers.size(); i++) {
		if (workers[i].locality.isPresent(localityKeyValue.first) &&
		    workers[i].locality.get(localityKeyValue.first) == localityKeyValue.second) {
			localityAddresses.insert(AddressExclusion(workers[i].address.ip, workers[i].address.port));
		}
	}

	return localityAddresses;
}

ACTOR Future<Void> printHealthyZone(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> val = wait(tr.get(healthyZoneKey));
			if (val.present() && decodeHealthyZoneValue(val.get()).first == ignoreSSFailuresZoneString) {
				printf("Data distribution has been disabled for all storage server failures in this cluster and thus "
				       "maintenance mode is not active.\n");
			} else if (!val.present() || decodeHealthyZoneValue(val.get()).second <= tr.getReadVersion().get()) {
				printf("No ongoing maintenance.\n");
			} else {
				auto healthyZone = decodeHealthyZoneValue(val.get());
				fmt::print("Maintenance for zone {0} will continue for {1} seconds.\n",
				           healthyZone.first.toString(),
				           (healthyZone.second - tr.getReadVersion().get()) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
			}
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> clearHealthyZone(Database cx, bool printWarning, bool clearSSFailureZoneString) {
	state Transaction tr(cx);
	TraceEvent("ClearHealthyZone").detail("ClearSSFailureZoneString", clearSSFailureZoneString);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> val = wait(tr.get(healthyZoneKey));
			if (!clearSSFailureZoneString && val.present() &&
			    decodeHealthyZoneValue(val.get()).first == ignoreSSFailuresZoneString) {
				if (printWarning) {
					printf("ERROR: Maintenance mode cannot be used while data distribution is disabled for storage "
					       "server failures. Use 'datadistribution on' to reenable data distribution.\n");
				}
				return false;
			}

			tr.clear(healthyZoneKey);
			wait(tr.commit());
			return true;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> setHealthyZone(Database cx, StringRef zoneId, double seconds, bool printWarning) {
	state Transaction tr(cx);
	TraceEvent("SetHealthyZone").detail("Zone", zoneId).detail("DurationSeconds", seconds);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> val = wait(tr.get(healthyZoneKey));
			if (val.present() && decodeHealthyZoneValue(val.get()).first == ignoreSSFailuresZoneString) {
				if (printWarning) {
					printf("ERROR: Maintenance mode cannot be used while data distribution is disabled for storage "
					       "server failures. Use 'datadistribution on' to reenable data distribution.\n");
				}
				return false;
			}
			Version readVersion = wait(tr.getReadVersion());
			tr.set(healthyZoneKey,
			       healthyZoneValue(zoneId, readVersion + (seconds * CLIENT_KNOBS->CORE_VERSIONSPERSECOND)));
			wait(tr.commit());
			return true;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> setDDIgnoreRebalanceSwitch(Database cx, bool ignoreRebalance) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (ignoreRebalance) {
				tr.set(rebalanceDDIgnoreKey, LiteralStringRef("on"));
			} else {
				tr.clear(rebalanceDDIgnoreKey);
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<int> setDDMode(Database cx, int mode) {
	state Transaction tr(cx);
	state int oldMode = -1;
	state BinaryWriter wr(Unversioned());
	wr << mode;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> old = wait(tr.get(dataDistributionModeKey));
			if (oldMode < 0) {
				oldMode = 1;
				if (old.present()) {
					BinaryReader rd(old.get(), Unversioned());
					rd >> oldMode;
				}
			}
			BinaryWriter wrMyOwner(Unversioned());
			wrMyOwner << dataDistributionModeLock;
			tr.set(moveKeysLockOwnerKey, wrMyOwner.toValue());
			BinaryWriter wrLastWrite(Unversioned());
			wrLastWrite << deterministicRandom()->randomUniqueID();
			tr.set(moveKeysLockWriteKey, wrLastWrite.toValue());

			tr.set(dataDistributionModeKey, wr.toValue());
			if (mode) {
				// set DDMode to 1 will enable all disabled parts, for instance the SS failure monitors.
				Optional<Value> currentHealthyZoneValue = wait(tr.get(healthyZoneKey));
				if (currentHealthyZoneValue.present() &&
				    decodeHealthyZoneValue(currentHealthyZoneValue.get()).first == ignoreSSFailuresZoneString) {
					// only clear the key if it is currently being used to disable all SS failure data movement
					tr.clear(healthyZoneKey);
				}
				tr.clear(rebalanceDDIgnoreKey);
			}
			wait(tr.commit());
			return oldMode;
		} catch (Error& e) {
			TraceEvent("SetDDModeRetrying").error(e);
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> checkForExcludingServersTxActor(ReadYourWritesTransaction* tr,
                                                   std::set<AddressExclusion>* exclusions,
                                                   std::set<NetworkAddress>* inProgressExclusion) {
	// TODO : replace using ExclusionInProgressRangeImpl in special key space
	ASSERT(inProgressExclusion->size() == 0); //  Make sure every time it is cleared beforehand
	if (!exclusions->size())
		return true;

	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // necessary?
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	// Just getting a consistent read version proves that a set of tlogs satisfying the exclusions has completed
	// recovery

	// Check that there aren't any storage servers with addresses violating the exclusions
	RangeResult serverList = wait(tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

	state bool ok = true;
	for (auto& s : serverList) {
		auto addresses = decodeServerListValue(s.value).getKeyValues.getEndpoint().addresses;
		if (addressExcluded(*exclusions, addresses.address)) {
			ok = false;
			inProgressExclusion->insert(addresses.address);
		}
		if (addresses.secondaryAddress.present() && addressExcluded(*exclusions, addresses.secondaryAddress.get())) {
			ok = false;
			inProgressExclusion->insert(addresses.secondaryAddress.get());
		}
	}

	if (ok) {
		Optional<Standalone<StringRef>> value = wait(tr->get(logsKey));
		ASSERT(value.present());
		auto logs = decodeLogsValue(value.get());
		for (auto const& log : logs.first) {
			if (log.second == NetworkAddress() || addressExcluded(*exclusions, log.second)) {
				ok = false;
				inProgressExclusion->insert(log.second);
			}
		}
		for (auto const& log : logs.second) {
			if (log.second == NetworkAddress() || addressExcluded(*exclusions, log.second)) {
				ok = false;
				inProgressExclusion->insert(log.second);
			}
		}
	}

	return ok;
}

ACTOR Future<std::set<NetworkAddress>> checkForExcludingServers(Database cx,
                                                                std::vector<AddressExclusion> excl,
                                                                bool waitForAllExcluded) {
	state std::set<AddressExclusion> exclusions(excl.begin(), excl.end());
	state std::set<NetworkAddress> inProgressExclusion;

	loop {
		state ReadYourWritesTransaction tr(cx);
		inProgressExclusion.clear();
		try {
			bool ok = wait(checkForExcludingServersTxActor(&tr, &exclusions, &inProgressExclusion));
			if (ok)
				return inProgressExclusion;
			if (!waitForAllExcluded)
				break;

			wait(delayJittered(1.0)); // SOMEDAY: watches!
		} catch (Error& e) {
			TraceEvent("CheckForExcludingServersError").error(e);
			wait(tr.onError(e));
		}
	}
	return inProgressExclusion;
}

ACTOR Future<Void> mgmtSnapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID) {
	try {
		wait(snapCreate(cx, snapCmd, snapUID));
		TraceEvent("SnapCreateSucceeded").detail("snapUID", snapUID);
		return Void();
	} catch (Error& e) {
		TraceEvent(SevWarn, "SnapCreateFailed").error(e).detail("snapUID", snapUID);
		throw;
	}
}

ACTOR Future<Void> waitForFullReplication(Database cx) {
	state ReadYourWritesTransaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			RangeResult confResults = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!confResults.more && confResults.size() < CLIENT_KNOBS->TOO_MANY);
			state DatabaseConfiguration config;
			config.fromKeyValues((VectorRef<KeyValueRef>)confResults);

			state std::vector<Future<Optional<Value>>> replicasFutures;
			for (auto& region : config.regions) {
				replicasFutures.push_back(tr.get(datacenterReplicasKeyFor(region.dcId)));
			}
			wait(waitForAll(replicasFutures));

			state std::vector<Future<Void>> watchFutures;
			for (int i = 0; i < config.regions.size(); i++) {
				if (!replicasFutures[i].get().present() ||
				    decodeDatacenterReplicasValue(replicasFutures[i].get().get()) < config.storageTeamSize) {
					watchFutures.push_back(tr.watch(datacenterReplicasKeyFor(config.regions[i].dcId)));
				}
			}

			if (!watchFutures.size() || (config.usableRegions == 1 && watchFutures.size() < config.regions.size())) {
				return Void();
			}

			wait(tr.commit());
			wait(waitForAny(watchFutures));
			tr.reset();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> timeKeeperSetDisable(Database cx) {
	loop {
		state Transaction tr(cx);
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.set(timeKeeperDisableKey, StringRef());
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> lockDatabase(Transaction* tr, UID id) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(tr->get(databaseLockedKey));

	if (val.present()) {
		if (BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) == id) {
			return Void();
		} else {
			//TraceEvent("DBA_LockLocked").detail("Expecting", id).detail("Lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
			throw database_locked();
		}
	}

	tr->atomicOp(databaseLockedKey,
	             BinaryWriter::toValue(id, Unversioned())
	                 .withPrefix(LiteralStringRef("0123456789"))
	                 .withSuffix(LiteralStringRef("\x00\x00\x00\x00")),
	             MutationRef::SetVersionstampedValue);
	tr->addWriteConflictRange(normalKeys);
	return Void();
}

ACTOR Future<Void> lockDatabase(Reference<ReadYourWritesTransaction> tr, UID id) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(tr->get(databaseLockedKey));

	if (val.present()) {
		if (BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) == id) {
			return Void();
		} else {
			//TraceEvent("DBA_LockLocked").detail("Expecting", id).detail("Lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
			throw database_locked();
		}
	}

	tr->atomicOp(databaseLockedKey,
	             BinaryWriter::toValue(id, Unversioned())
	                 .withPrefix(LiteralStringRef("0123456789"))
	                 .withSuffix(LiteralStringRef("\x00\x00\x00\x00")),
	             MutationRef::SetVersionstampedValue);
	tr->addWriteConflictRange(normalKeys);
	return Void();
}

ACTOR Future<Void> lockDatabase(Database cx, UID id) {
	state Transaction tr(cx);
	loop {
		try {
			wait(lockDatabase(&tr, id));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked)
				throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> unlockDatabase(Transaction* tr, UID id) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(tr->get(databaseLockedKey));

	if (!val.present())
		return Void();

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_UnlockLocked").detail("Expecting", id).detail("Lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
		throw database_locked();
	}

	tr->clear(singleKeyRange(databaseLockedKey));
	return Void();
}

ACTOR Future<Void> unlockDatabase(Reference<ReadYourWritesTransaction> tr, UID id) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(tr->get(databaseLockedKey));

	if (!val.present())
		return Void();

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_UnlockLocked").detail("Expecting", id).detail("Lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
		throw database_locked();
	}

	tr->clear(singleKeyRange(databaseLockedKey));
	return Void();
}

ACTOR Future<Void> unlockDatabase(Database cx, UID id) {
	state Transaction tr(cx);
	loop {
		try {
			wait(unlockDatabase(&tr, id));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked)
				throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> checkDatabaseLock(Transaction* tr, UID id) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(tr->get(databaseLockedKey));

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_CheckLocked").detail("Expecting", id).detail("Lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned())).backtrace();
		throw database_locked();
	}

	return Void();
}

ACTOR Future<Void> checkDatabaseLock(Reference<ReadYourWritesTransaction> tr, UID id) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(tr->get(databaseLockedKey));

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_CheckLocked").detail("Expecting", id).detail("Lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned())).backtrace();
		throw database_locked();
	}

	return Void();
}

ACTOR Future<Void> updateChangeFeed(Transaction* tr, Key rangeID, ChangeFeedStatus status, KeyRange range) {
	state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	Optional<Value> val = wait(tr->get(rangeIDKey));
	if (status == ChangeFeedStatus::CHANGE_FEED_CREATE) {
		if (!val.present()) {
			tr->set(rangeIDKey, changeFeedValue(range, invalidVersion, status));
		} else if (std::get<0>(decodeChangeFeedValue(val.get())) != range) {
			throw unsupported_operation();
		}
	} else if (status == ChangeFeedStatus::CHANGE_FEED_STOP) {
		if (val.present()) {
			tr->set(rangeIDKey,
			        changeFeedValue(std::get<0>(decodeChangeFeedValue(val.get())),
			                        std::get<1>(decodeChangeFeedValue(val.get())),
			                        status));
		} else {
			throw unsupported_operation();
		}
	} else if (status == ChangeFeedStatus::CHANGE_FEED_DESTROY) {
		if (val.present()) {
			tr->set(rangeIDKey,
			        changeFeedValue(std::get<0>(decodeChangeFeedValue(val.get())),
			                        std::get<1>(decodeChangeFeedValue(val.get())),
			                        status));
			tr->clear(rangeIDKey);
		} else {
			throw unsupported_operation();
		}
	}
	return Void();
}

ACTOR Future<Void> updateChangeFeed(Reference<ReadYourWritesTransaction> tr,
                                    Key rangeID,
                                    ChangeFeedStatus status,
                                    KeyRange range) {
	state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	Optional<Value> val = wait(tr->get(rangeIDKey));
	if (status == ChangeFeedStatus::CHANGE_FEED_CREATE) {
		if (!val.present()) {
			tr->set(rangeIDKey, changeFeedValue(range, invalidVersion, status));
		} else if (std::get<0>(decodeChangeFeedValue(val.get())) != range) {
			throw unsupported_operation();
		}
	} else if (status == ChangeFeedStatus::CHANGE_FEED_STOP) {
		if (val.present()) {
			tr->set(rangeIDKey,
			        changeFeedValue(std::get<0>(decodeChangeFeedValue(val.get())),
			                        std::get<1>(decodeChangeFeedValue(val.get())),
			                        status));
		} else {
			throw unsupported_operation();
		}
	} else if (status == ChangeFeedStatus::CHANGE_FEED_DESTROY) {
		if (val.present()) {
			tr->set(rangeIDKey,
			        changeFeedValue(std::get<0>(decodeChangeFeedValue(val.get())),
			                        std::get<1>(decodeChangeFeedValue(val.get())),
			                        status));
			tr->clear(rangeIDKey);
		} else {
			throw unsupported_operation();
		}
	}
	return Void();
}

ACTOR Future<Void> updateChangeFeed(Database cx, Key rangeID, ChangeFeedStatus status, KeyRange range) {
	state Transaction tr(cx);
	loop {
		try {
			wait(updateChangeFeed(&tr, rangeID, status, range));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> advanceVersion(Database cx, Version v) {
	state Transaction tr(cx);
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Version rv = wait(tr.getReadVersion());
			if (rv <= v) {
				tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(v + 1, Unversioned()));
				wait(tr.commit());
			} else {
				fmt::print("Current read version is {}\n", rv);
				return Void();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> forceRecovery(Reference<IClusterConnectionRecord> clusterFile, Key dcId) {
	state Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface(new AsyncVar<Optional<ClusterInterface>>);
	state Future<Void> leaderMon = monitorLeader<ClusterInterface>(clusterFile, clusterInterface);

	loop {
		choose {
			when(wait(clusterInterface->get().present()
			              ? brokenPromiseToNever(
			                    clusterInterface->get().get().forceRecovery.getReply(ForceRecoveryRequest(dcId)))
			              : Never())) {
				return Void();
			}
			when(wait(clusterInterface->onChange())) {}
		}
	}
}

ACTOR Future<Void> waitForPrimaryDC(Database cx, StringRef dcId) {
	state ReadYourWritesTransaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> res = wait(tr.get(primaryDatacenterKey));
			if (res.present() && res.get() == dcId) {
				return Void();
			}

			state Future<Void> watchFuture = tr.watch(primaryDatacenterKey);
			wait(tr.commit());
			wait(watchFuture);
			tr.reset();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

json_spirit::Value_type normJSONType(json_spirit::Value_type type) {
	if (type == json_spirit::int_type)
		return json_spirit::real_type;
	return type;
}

void schemaCoverage(std::string const& spath, bool covered) {
	static std::map<bool, std::set<std::string>> coveredSchemaPaths;

	if (coveredSchemaPaths[covered].insert(spath).second) {
		TraceEvent ev(SevInfo, "CodeCoverage");
		ev.detail("File", "documentation/StatusSchema.json/" + spath).detail("Line", 0);
		if (!covered)
			ev.detail("Covered", 0);
	}
}

bool schemaMatch(json_spirit::mValue const& schemaValue,
                 json_spirit::mValue const& resultValue,
                 std::string& errorStr,
                 Severity sev,
                 bool checkCoverage,
                 std::string path,
                 std::string schemaPath) {
	// Returns true if everything in `result` is permitted by `schema`
	bool ok = true;

	try {
		if (normJSONType(schemaValue.type()) != normJSONType(resultValue.type())) {
			errorStr += format("ERROR: Incorrect value type for key `%s'\n", path.c_str());
			TraceEvent(sev, "SchemaMismatch")
			    .detail("Path", path)
			    .detail("SchemaType", schemaValue.type())
			    .detail("ValueType", resultValue.type());
			return false;
		}

		if (resultValue.type() == json_spirit::obj_type) {
			auto& result = resultValue.get_obj();
			auto& schema = schemaValue.get_obj();

			for (auto& rkv : result) {
				auto& key = rkv.first;
				auto& rv = rkv.second;
				std::string kpath = path + "." + key;
				std::string spath = schemaPath + "." + key;

				if (checkCoverage) {
					schemaCoverage(spath);
				}

				if (!schema.count(key)) {
					errorStr += format("ERROR: Unknown key `%s'\n", kpath.c_str());
					TraceEvent(sev, "SchemaMismatch").detail("Path", kpath).detail("SchemaPath", spath);
					ok = false;
					continue;
				}
				auto& sv = schema.at(key);

				if (sv.type() == json_spirit::obj_type && sv.get_obj().count("$enum")) {
					auto& enum_values = sv.get_obj().at("$enum").get_array();

					bool any_match = false;
					for (auto& enum_item : enum_values)
						if (enum_item == rv) {
							any_match = true;
							if (checkCoverage) {
								schemaCoverage(spath + ".$enum." + enum_item.get_str());
							}
							break;
						}
					if (!any_match) {
						errorStr += format("ERROR: Unknown value `%s' for key `%s'\n",
						                   json_spirit::write_string(rv).c_str(),
						                   kpath.c_str());
						TraceEvent(sev, "SchemaMismatch")
						    .detail("Path", kpath)
						    .detail("SchemaEnumItems", enum_values.size())
						    .detail("Value", json_spirit::write_string(rv));
						if (checkCoverage) {
							schemaCoverage(spath + ".$enum." + json_spirit::write_string(rv));
						}
						ok = false;
					}
				} else if (sv.type() == json_spirit::obj_type && sv.get_obj().count("$map")) {
					if (rv.type() != json_spirit::obj_type) {
						errorStr += format("ERROR: Expected an object as the value for key `%s'\n", kpath.c_str());
						TraceEvent(sev, "SchemaMismatch")
						    .detail("Path", kpath)
						    .detail("SchemaType", sv.type())
						    .detail("ValueType", rv.type());
						ok = false;
						continue;
					}
					if (sv.get_obj().at("$map").type() != json_spirit::obj_type) {
						continue;
					}
					auto& schemaVal = sv.get_obj().at("$map");
					auto& valueObj = rv.get_obj();

					if (checkCoverage) {
						schemaCoverage(spath + ".$map");
					}

					for (auto& valuePair : valueObj) {
						auto vpath = kpath + "[" + valuePair.first + "]";
						auto upath = spath + ".$map";
						if (valuePair.second.type() != json_spirit::obj_type) {
							errorStr += format("ERROR: Expected an object for `%s'\n", vpath.c_str());
							TraceEvent(sev, "SchemaMismatch")
							    .detail("Path", vpath)
							    .detail("ValueType", valuePair.second.type());
							ok = false;
							continue;
						}
						if (!schemaMatch(schemaVal, valuePair.second, errorStr, sev, checkCoverage, vpath, upath)) {
							ok = false;
						}
					}
				} else {
					if (!schemaMatch(sv, rv, errorStr, sev, checkCoverage, kpath, spath)) {
						ok = false;
					}
				}
			}
		} else if (resultValue.type() == json_spirit::array_type) {
			auto& valueArray = resultValue.get_array();
			auto& schemaArray = schemaValue.get_array();
			if (!schemaArray.size()) {
				// An empty schema array means that the value array is required to be empty
				if (valueArray.size()) {
					errorStr += format("ERROR: Expected an empty array for key `%s'\n", path.c_str());
					TraceEvent(sev, "SchemaMismatch")
					    .detail("Path", path)
					    .detail("SchemaSize", schemaArray.size())
					    .detail("ValueSize", valueArray.size());
					return false;
				}
			} else if (schemaArray.size() == 1) {
				// A one item schema array means that all items in the value must match the first item in the schema
				int index = 0;
				for (auto& valueItem : valueArray) {
					if (!schemaMatch(schemaArray[0],
					                 valueItem,
					                 errorStr,
					                 sev,
					                 checkCoverage,
					                 path + format("[%d]", index),
					                 schemaPath + "[0]")) {
						ok = false;
					}
					index++;
				}
			} else {
				ASSERT(false); // Schema doesn't make sense
			}
		}
		return ok;
	} catch (std::exception& e) {
		TraceEvent(SevError, "SchemaMatchException")
		    .detail("What", e.what())
		    .detail("Path", path)
		    .detail("SchemaPath", schemaPath);
		throw unknown_error();
	}
}

std::string ManagementAPI::generateErrorMessage(const CoordinatorsResult& res) {
	// Note: the error message here should not be changed if possible
	// If you do change the message here,
	// please update the corresponding fdbcli code to support both the old and the new message

	std::string msg;
	switch (res) {
	case CoordinatorsResult::INVALID_NETWORK_ADDRESSES:
		msg = "The specified network addresses are invalid";
		break;
	case CoordinatorsResult::SAME_NETWORK_ADDRESSES:
		msg = "No change (existing configuration satisfies request)";
		break;
	case CoordinatorsResult::NOT_COORDINATORS:
		msg = "Coordination servers are not running on the specified network addresses";
		break;
	case CoordinatorsResult::DATABASE_UNREACHABLE:
		msg = "Database unreachable";
		break;
	case CoordinatorsResult::BAD_DATABASE_STATE:
		msg = "The database is in an unexpected state from which changing coordinators might be unsafe";
		break;
	case CoordinatorsResult::COORDINATOR_UNREACHABLE:
		msg = "One of the specified coordinators is unreachable";
		break;
	case CoordinatorsResult::NOT_ENOUGH_MACHINES:
		msg = "Too few fdbserver machines to provide coordination at the current redundancy level";
		break;
	default:
		break;
	}
	return msg;
}

TEST_CASE("/ManagementAPI/AutoQuorumChange/checkLocality") {
	wait(Future<Void>(Void()));

	std::vector<ProcessData> workers;
	std::vector<NetworkAddress> chosen;
	std::set<AddressExclusion> excluded;
	AutoQuorumChange change(5);

	for (int i = 0; i < 10; i++) {
		ProcessData data;
		auto dataCenter = std::to_string(i / 4 % 2);
		auto dataHall = dataCenter + std::to_string(i / 2 % 2);
		auto rack = dataHall + std::to_string(i % 2);
		auto machineId = rack + std::to_string(i);
		data.locality.set(LiteralStringRef("dcid"), StringRef(dataCenter));
		data.locality.set(LiteralStringRef("data_hall"), StringRef(dataHall));
		data.locality.set(LiteralStringRef("rack"), StringRef(rack));
		data.locality.set(LiteralStringRef("zoneid"), StringRef(rack));
		data.locality.set(LiteralStringRef("machineid"), StringRef(machineId));
		data.address.ip = IPAddress(i);

		if (g_network->isSimulated()) {
			g_simulator.newProcess("TestCoordinator",
			                       data.address.ip,
			                       data.address.port,
			                       false,
			                       1,
			                       data.locality,
			                       ProcessClass(ProcessClass::CoordinatorClass, ProcessClass::CommandLineSource),
			                       "",
			                       "",
			                       currentProtocolVersion);
		}

		workers.push_back(data);
	}

	auto noAssignIndex = deterministicRandom()->randomInt(0, workers.size());
	workers[noAssignIndex].processClass._class = ProcessClass::CoordinatorClass;

	change.addDesiredWorkers(chosen, workers, 5, excluded);
	std::map<StringRef, std::set<StringRef>> chosenValues;

	ASSERT(chosen.size() == 5);
	std::vector<StringRef> fields({ LiteralStringRef("dcid"),
	                                LiteralStringRef("data_hall"),
	                                LiteralStringRef("zoneid"),
	                                LiteralStringRef("machineid") });
	for (auto worker = chosen.begin(); worker != chosen.end(); worker++) {
		ASSERT(worker->ip.toV4() < workers.size());
		LocalityData data = workers[worker->ip.toV4()].locality;
		for (auto field = fields.begin(); field != fields.end(); field++) {
			chosenValues[*field].insert(data.get(*field).get());
		}
	}

	ASSERT(chosenValues[LiteralStringRef("dcid")].size() == 2);
	ASSERT(chosenValues[LiteralStringRef("data_hall")].size() == 4);
	ASSERT(chosenValues[LiteralStringRef("zoneid")].size() == 5);
	ASSERT(chosenValues[LiteralStringRef("machineid")].size() == 5);
	ASSERT(std::find(chosen.begin(), chosen.end(), workers[noAssignIndex].address) != chosen.end());

	return Void();
}
