/*
 * ManagementAPI.actor.cpp
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
#include <string>
#include <vector>

#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/RangeLock.h"
#include "fmt/format.h"
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
#include "fdbrpc/SimulatorProcessInfo.h"

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

		if (key == "encryption_at_rest_mode") {
			EncryptionAtRestMode mode;
			if (value == "disabled") {
				mode = EncryptionAtRestMode::DISABLED;
			} else if (value == "domain_aware") {
				mode = EncryptionAtRestMode::DOMAIN_AWARE;
			} else if (value == "cluster_aware") {
				mode = EncryptionAtRestMode::CLUSTER_AWARE;
			} else {
				printf("Error: Only disabled|domain_aware|cluster_aware are valid for encryption_at_rest_mode.\n");
				return out;
			}
			out[p + key] = format("%d", mode);
		}

		if (key == "exclude") {
			int p = 0;
			while (p < value.size()) {
				int end = value.find_first_of(',', p);
				if (end == value.npos) {
					end = value.size();
				}
				auto addrRef = StringRef(value).substr(p, end - p);
				AddressExclusion addr = AddressExclusion::parse(addrRef);
				if (addr.isValid()) {
					out[encodeExcludedServersKey(addr)] = "";
				} else {
					printf("Error: invalid address format: %s\n", addrRef.toString().c_str());
				}
				p = end + 1;
			}
		}

		if (key == "storage_engine" || key == "log_engine" || key == "perpetual_storage_wiggle_engine") {
			StringRef s = value;

			// Parse as engine_name[:p=v]... to handle future storage engine params
			Value engine = s.eat(":");
			std::map<Key, Value> params;
			while (!s.empty()) {
				params[s.eat("=")] = s.eat(":");
			}

			try {
				out[p + key] = format("%d", KeyValueStoreType::fromString(engine.toString()).storeType());
			} catch (Error& e) {
				printf("Error: Invalid value for %s (%s): %s\n", key.c_str(), value.c_str(), e.what());
			}
			return out;
		}

		return out;
	}

	Optional<KeyValueStoreType> logType;
	Optional<KeyValueStoreType> storeType;

	// These are legacy shorthand commands to set a specific log engine and storage engine
	// based only on the storage engine name.  Most of them assume SQLite should be the
	// log engine.
	if (mode == "ssd-1") {
		logType = KeyValueStoreType::SSD_BTREE_V1;
		storeType = KeyValueStoreType::SSD_BTREE_V1;
	} else if (mode == "ssd" || mode == "ssd-2") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_BTREE_V2;
	} else if (mode == "ssd-redwood-1") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_REDWOOD_V1;
	} else if (mode == "ssd-rocksdb-v1") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_ROCKSDB_V1;
	} else if (mode == "ssd-sharded-rocksdb") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::SSD_SHARDED_ROCKSDB;
	} else if (mode == "memory" || mode == "memory-2") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::MEMORY;
	} else if (mode == "memory-1") {
		logType = KeyValueStoreType::MEMORY;
		storeType = KeyValueStoreType::MEMORY;
	} else if (mode == "memory-radixtree" || mode == "memory-radixtree-beta") {
		logType = KeyValueStoreType::SSD_BTREE_V2;
		storeType = KeyValueStoreType::MEMORY_RADIXTREE;
	}
	// Add any new store types to fdbserver/workloads/ConfigureDatabase, too

	if (storeType.present()) {
		out[p + "log_engine"] = format("%d", logType.get().storeType());
		out[p + "storage_engine"] = format("%d", storeType.get().storeType());
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
				TraceEvent(SevWarnAlways, "ConflictingOption")
				    .detail("Option", t->first)
				    .detail("Value", t->second)
				    .detail("ExistingValue", outConf[t->first]);
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

/*
    - Validates encryption and tenant mode configurations
    - During cluster creation (configure new) we allow the following:
        - If encryption mode is disabled/cluster_aware then any tenant mode is allowed
        - If the encryption mode is domain_aware then the only allowed tenant mode is required
    - During cluster configuration changes the following is allowed:
        - Encryption mode cannot be changed (can only be set during creation)
        - If the encryption mode is disabled/cluster_aware then any tenant mode changes are allowed
        - If the encryption mode is domain_aware then tenant mode changes are not allowed (as the only supported mode is
          required)
*/
bool isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration> oldConfiguration,
                                       std::map<std::string, std::string> newConfig,
                                       bool creating) {
	EncryptionAtRestMode encryptMode;
	TenantMode tenantMode;
	if (creating) {
		if (newConfig.count(encryptionAtRestModeConfKey.toString()) != 0) {
			encryptMode = EncryptionAtRestMode::fromValueRef(
			    ValueRef(newConfig.find(encryptionAtRestModeConfKey.toString())->second));
			// check if the tenant mode is being set during configure new (otherwise assume tenants are disabled)
			if (newConfig.count(tenantModeConfKey.toString()) != 0) {
				tenantMode = TenantMode::fromValue(ValueRef(newConfig.find(tenantModeConfKey.toString())->second));
			}
		}
	} else {
		ASSERT(oldConfiguration.present());
		encryptMode = oldConfiguration.get().encryptionAtRestMode;
		if (newConfig.count(tenantModeConfKey.toString()) != 0) {
			tenantMode = TenantMode::fromValue(ValueRef(newConfig.find(tenantModeConfKey.toString())->second));
		} else {
			// Tenant mode and encryption mode didn't change
			return true;
		}
	}
	TraceEvent(SevDebug, "EncryptAndTenantModes")
	    .detail("EncryptMode", encryptMode.toString())
	    .detail("TenantMode", tenantMode.toString());

	if (encryptMode.mode == EncryptionAtRestMode::DOMAIN_AWARE && tenantMode != TenantMode::REQUIRED) {
		// For domain aware encryption only the required tenant mode is currently supported
		TraceEvent(SevWarnAlways, "InvalidEncryptAndTenantConfiguration")
		    .detail("EncryptMode", encryptMode.toString())
		    .detail("TenantMode", tenantMode.toString());
		return false;
	}

	return true;
}

bool isTenantModeModeConfigValid(DatabaseConfiguration oldConfiguration, DatabaseConfiguration newConfiguration) {
	TenantMode oldTenantMode = oldConfiguration.tenantMode;
	TenantMode newTenantMode = newConfiguration.tenantMode;
	TraceEvent(SevDebug, "TenantModes")
	    .detail("OldTenantMode", oldTenantMode.toString())
	    .detail("NewTenantMode", newTenantMode.toString());
	if (oldTenantMode != TenantMode::REQUIRED && newTenantMode == TenantMode::REQUIRED) {
		// TODO: Changing from optional/disabled to required tenant mode should be allowed if there is no non-tenant
		// data present
		TraceEvent(SevWarnAlways, "InvalidTenantConfiguration")
		    .detail("OldTenantMode", oldTenantMode.toString())
		    .detail("NewTenantMode", newTenantMode.toString());
		return false;
	}
	return true;
}

TEST_CASE("/ManagementAPI/ChangeConfig/TenantMode") {
	DatabaseConfiguration oldConfig;
	DatabaseConfiguration newConfig;
	std::vector<TenantMode> tenantModes = { TenantMode::DISABLED, TenantMode::OPTIONAL_TENANT, TenantMode::REQUIRED };
	// required tenant mode can change to any other tenant mode
	oldConfig.tenantMode = TenantMode::REQUIRED;
	newConfig.tenantMode = deterministicRandom()->randomChoice(tenantModes);
	ASSERT(isTenantModeModeConfigValid(oldConfig, newConfig));
	// optional/disabled tenant mode can switch to optional/disabled tenant mode
	oldConfig.tenantMode = deterministicRandom()->coinflip() ? TenantMode::DISABLED : TenantMode::OPTIONAL_TENANT;
	newConfig.tenantMode = deterministicRandom()->coinflip() ? TenantMode::DISABLED : TenantMode::OPTIONAL_TENANT;
	ASSERT(isTenantModeModeConfigValid(oldConfig, newConfig));
	// optional/disabled tenant mode CANNOT switch to required tenant mode
	oldConfig.tenantMode = deterministicRandom()->coinflip() ? TenantMode::DISABLED : TenantMode::OPTIONAL_TENANT;
	newConfig.tenantMode = TenantMode::REQUIRED;
	ASSERT(!isTenantModeModeConfigValid(oldConfig, newConfig));

	return Void();
}

// unit test for changing encryption/tenant mode config options
TEST_CASE("/ManagementAPI/ChangeConfig/TenantAndEncryptMode") {
	std::map<std::string, std::string> newConfig;
	std::string encryptModeKey = encryptionAtRestModeConfKey.toString();
	std::string tenantModeKey = tenantModeConfKey.toString();
	std::vector<TenantMode> tenantModes = { TenantMode::DISABLED, TenantMode::OPTIONAL_TENANT, TenantMode::REQUIRED };
	std::vector<EncryptionAtRestMode> encryptionModes = { EncryptionAtRestMode::DISABLED,
		                                                  EncryptionAtRestMode::CLUSTER_AWARE,
		                                                  EncryptionAtRestMode::DOMAIN_AWARE };
	// configure new test cases

	// encryption disabled checks
	newConfig[encryptModeKey] = std::to_string(EncryptionAtRestMode::DISABLED);
	newConfig[tenantModeKey] = std::to_string(deterministicRandom()->randomChoice(tenantModes));
	ASSERT(isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));

	// cluster aware encryption checks
	newConfig[encryptModeKey] = std::to_string(EncryptionAtRestMode::CLUSTER_AWARE);
	newConfig[tenantModeKey] = std::to_string(deterministicRandom()->randomChoice(tenantModes));
	ASSERT(isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));

	// domain aware encryption checks
	newConfig[encryptModeKey] = std::to_string(EncryptionAtRestMode::DOMAIN_AWARE);
	newConfig[tenantModeKey] =
	    std::to_string(deterministicRandom()->coinflip() ? TenantMode::DISABLED : TenantMode::OPTIONAL_TENANT);
	ASSERT(!isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));
	newConfig[tenantModeKey] = std::to_string(TenantMode::REQUIRED);
	ASSERT(isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));

	// no encrypt mode present
	newConfig.erase(encryptModeKey);
	newConfig[tenantModeKey] = std::to_string(deterministicRandom()->randomChoice(tenantModes));
	ASSERT(isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));

	// no tenant mode present
	newConfig.erase(tenantModeKey);
	newConfig[encryptModeKey] = std::to_string(EncryptionAtRestMode::DOMAIN_AWARE);
	ASSERT(!isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));
	newConfig[encryptModeKey] = std::to_string(EncryptionAtRestMode::CLUSTER_AWARE);
	ASSERT(isEncryptionAtRestModeConfigValid(Optional<DatabaseConfiguration>(), newConfig, true));

	// change config test cases
	DatabaseConfiguration oldConfig;

	// encryption disabled checks
	oldConfig.encryptionAtRestMode = EncryptionAtRestMode::DISABLED;
	oldConfig.tenantMode = deterministicRandom()->randomChoice(tenantModes);
	newConfig[tenantModeKey] = std::to_string(deterministicRandom()->randomChoice(tenantModes));
	ASSERT(isEncryptionAtRestModeConfigValid(oldConfig, newConfig, false));

	// domain aware encryption checks
	oldConfig.encryptionAtRestMode = EncryptionAtRestMode::DOMAIN_AWARE;
	oldConfig.tenantMode = TenantMode::REQUIRED;
	newConfig[tenantModeKey] =
	    std::to_string(deterministicRandom()->coinflip() ? TenantMode::DISABLED : TenantMode::OPTIONAL_TENANT);
	ASSERT(!isEncryptionAtRestModeConfigValid(oldConfig, newConfig, false));
	newConfig[tenantModeKey] = std::to_string(TenantMode::REQUIRED);
	ASSERT(isEncryptionAtRestModeConfigValid(oldConfig, newConfig, false));

	// cluster aware encryption checks
	oldConfig.encryptionAtRestMode = EncryptionAtRestMode::CLUSTER_AWARE;
	// required tenant mode can switch to any other tenant mode with cluster aware encryption
	oldConfig.tenantMode = deterministicRandom()->randomChoice(tenantModes);
	newConfig[tenantModeKey] = std::to_string(deterministicRandom()->randomChoice(tenantModes));
	ASSERT(isEncryptionAtRestModeConfigValid(oldConfig, newConfig, false));

	// no tenant mode present
	newConfig.erase(tenantModeKey);
	oldConfig.tenantMode = deterministicRandom()->randomChoice(tenantModes);
	oldConfig.encryptionAtRestMode = deterministicRandom()->randomChoice(encryptionModes);
	ASSERT(isEncryptionAtRestModeConfigValid(oldConfig, newConfig, false));

	return Void();
}

ACTOR Future<DatabaseConfiguration> getDatabaseConfiguration(Transaction* tr, bool useSystemPriority) {
	if (useSystemPriority) {
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	}
	tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	RangeResult res = wait(tr->getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(res.size() < CLIENT_KNOBS->TOO_MANY);
	DatabaseConfiguration config;
	config.fromKeyValues((VectorRef<KeyValueRef>)res);
	return config;
}

ACTOR Future<DatabaseConfiguration> getDatabaseConfiguration(Database cx, bool useSystemPriority) {
	state Transaction tr(cx);
	loop {
		try {
			DatabaseConfiguration config = wait(getDatabaseConfiguration(&tr, useSystemPriority));
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

ACTOR Future<Optional<ClusterConnectionString>> getConnectionString(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> currentKey = wait(tr.get(coordinatorsKey));
			if (!currentKey.present())
				return Optional<ClusterConnectionString>();
			return ClusterConnectionString(currentKey.get().toString());
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

static std::vector<std::string> connectionStrings;

namespace {

ACTOR Future<Optional<ClusterConnectionString>> getClusterConnectionStringFromStorageServer(Transaction* tr) {

	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	state int retryTimes = 0;
	loop {
		if (retryTimes >= CLIENT_KNOBS->CHANGE_QUORUM_BAD_STATE_RETRY_TIMES) {
			return Optional<ClusterConnectionString>();
		}

		state Optional<Value> currentKey = wait(tr->get(coordinatorsKey));
		if (g_network->isSimulated() && currentKey.present()) {
			// If the change coordinators request succeeded, the coordinators
			// should have changed to the connection string of the most
			// recently issued request. If instead the connection string is
			// equal to one of the previously issued requests, there is a bug
			// and we are breaking the promises we make with
			// commit_unknown_result (the transaction must no longer be in
			// progress when receiving commit_unknown_result).
			int n = connectionStrings.size() > 0 ? connectionStrings.size() - 1 : 0; // avoid underflow
			for (int i = 0; i < n; ++i) {
				ASSERT(currentKey.get() != connectionStrings.at(i));
			}
		}

		if (!currentKey.present()) {
			// Someone deleted this key entirely?
			++retryTimes;
			wait(delay(CLIENT_KNOBS->CHANGE_QUORUM_BAD_STATE_RETRY_DELAY));
			continue;
		}

		state ClusterConnectionString clusterConnectionString(currentKey.get().toString());
		if (tr->getDatabase()->getConnectionRecord() &&
		    clusterConnectionString.clusterKeyName().toString() !=
		        tr->getDatabase()->getConnectionRecord()->getConnectionString().clusterKeyName()) {
			// Someone changed the "name" of the database??
			++retryTimes;
			wait(delay(CLIENT_KNOBS->CHANGE_QUORUM_BAD_STATE_RETRY_DELAY));
			continue;
		}

		return clusterConnectionString;
	}
}

ACTOR Future<Void> verifyConfigurationDatabaseAlive(Database cx) {
	state Backoff backoff;
	state Reference<ISingleThreadTransaction> configTr;
	loop {
		try {
			// Attempt to read a random value from the configuration
			// database to make sure it is online.
			configTr = ISingleThreadTransaction::create(ISingleThreadTransaction::Type::PAXOS_CONFIG, cx);
			Tuple tuple;
			tuple.appendNull(); // config class
			tuple << "test"_sr;
			Optional<Value> serializedValue = wait(configTr->get(tuple.pack()));
			TraceEvent("ChangeQuorumCheckerNewCoordinatorsOnline").log();
			return Void();
		} catch (Error& e) {
			TraceEvent("ChangeQuorumCheckerNewCoordinatorsError").error(e);
			if (e.code() == error_code_coordinators_changed) {
				wait(backoff.onError());
				configTr->reset();
			} else {
				wait(configTr->onError(e));
			}
		}
	}
}

ACTOR Future<Void> resetPreviousCoordinatorsKey(Database cx) {
	loop {
		// When the change coordinators transaction succeeds, it uses the
		// special key space error message to return a message to the client.
		// This causes the underlying transaction to not be committed. In order
		// to make sure we clear the previous coordinators key, we have to use
		// a new transaction here.
		state Reference<ISingleThreadTransaction> clearTr =
		    ISingleThreadTransaction::create(ISingleThreadTransaction::Type::RYW, cx);
		try {
			clearTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			clearTr->clear(previousCoordinatorsKey);
			wait(clearTr->commit());
			return Void();
		} catch (Error& e2) {
			wait(clearTr->onError(e2));
		}
	}
}

} // namespace

ACTOR Future<Optional<CoordinatorsResult>> changeQuorumChecker(Transaction* tr,
                                                               ClusterConnectionString* conn,
                                                               std::string newName,
                                                               bool disableConfigDB) {
	TraceEvent("ChangeQuorumCheckerStart").detail("NewConnectionString", conn->toString());
	state Optional<ClusterConnectionString> clusterConnectionStringOptional =
	    wait(getClusterConnectionStringFromStorageServer(tr));

	if (!clusterConnectionStringOptional.present()) {
		return CoordinatorsResult::BAD_DATABASE_STATE;
	}

	// The cluster connection string stored in the storage server
	state ClusterConnectionString old = clusterConnectionStringOptional.get();

	if (conn->hostnames.size() + conn->coords.size() == 0) {
		conn->hostnames = old.hostnames;
		conn->coords = old.coords;
	}
	state std::vector<NetworkAddress> desiredCoordinators = wait(conn->tryResolveHostnames());
	if (desiredCoordinators.size() != conn->hostnames.size() + conn->coords.size()) {
		TraceEvent("ChangeQuorumCheckerEarlyTermination")
		    .detail("Reason", "One or more hostnames are unresolvable")
		    .backtrace();
		return CoordinatorsResult::COORDINATOR_UNREACHABLE;
	}

	if (newName.empty()) {
		newName = old.clusterKeyName().toString();
	}
	std::sort(conn->hostnames.begin(), conn->hostnames.end());
	std::sort(conn->coords.begin(), conn->coords.end());
	std::sort(old.hostnames.begin(), old.hostnames.end());
	std::sort(old.coords.begin(), old.coords.end());
	if (conn->hostnames == old.hostnames && conn->coords == old.coords && old.clusterKeyName() == newName) {
		connectionStrings.clear();
		if (g_network->isSimulated() && g_simulator->configDBType == ConfigDBType::DISABLED) {
			disableConfigDB = true;
		}
		if (!disableConfigDB) {
			wait(verifyConfigurationDatabaseAlive(tr->getDatabase()));
		}
		if (BUGGIFY_WITH_PROB(0.1)) {
			// Introduce a random delay in simulation to allow processes to be
			// killed before previousCoordinatorKeys has been reset. This will
			// help test scenarios where the previous configuration database
			// state has been transferred to the new coordinators but the
			// broadcaster thinks it has not been transferred.
			wait(delay(deterministicRandom()->random01() * 10));
		}
		wait(resetPreviousCoordinatorsKey(tr->getDatabase()));
		return CoordinatorsResult::SAME_NETWORK_ADDRESSES;
	}

	conn->parseKey(newName + ':' + deterministicRandom()->randomAlphaNumeric(32));
	connectionStrings.push_back(conn->toString());

	if (g_network->isSimulated()) {
		int i = 0;
		int protectedCount = 0;
		int minimumCoordinators = (desiredCoordinators.size() / 2) + 1;
		while (protectedCount < minimumCoordinators && i < desiredCoordinators.size()) {
			auto process = g_simulator->getProcessByAddress(desiredCoordinators[i]);
			auto addresses = process->addresses;

			if (!process->isReliable()) {
				i++;
				continue;
			}

			g_simulator->protectedAddresses.insert(process->addresses.address);
			if (addresses.secondaryAddress.present()) {
				g_simulator->protectedAddresses.insert(process->addresses.secondaryAddress.get());
			}
			TraceEvent("ProtectCoordinator").detail("Address", desiredCoordinators[i]).backtrace();
			protectedCount++;
			i++;
		}

		if (protectedCount < minimumCoordinators) {
			TraceEvent("NotEnoughReliableCoordinators")
			    .detail("NumReliable", protectedCount)
			    .detail("MinimumRequired", minimumCoordinators)
			    .detail("ConnectionString", conn->toString());

			return CoordinatorsResult::COORDINATOR_UNREACHABLE;
		}
	}

	std::vector<Future<Optional<LeaderInfo>>> leaderServers;
	ClientCoordinators coord(Reference<ClusterConnectionMemoryRecord>(new ClusterConnectionMemoryRecord(*conn)));

	leaderServers.reserve(coord.clientLeaderServers.size());
	for (int i = 0; i < coord.clientLeaderServers.size(); i++) {
		if (coord.clientLeaderServers[i].hostname.present()) {
			leaderServers.push_back(retryGetReplyFromHostname(GetLeaderRequest(coord.clusterKey, UID()),
			                                                  coord.clientLeaderServers[i].hostname.get(),
			                                                  WLTOKEN_CLIENTLEADERREG_GETLEADER,
			                                                  TaskPriority::CoordinationReply));
		} else {
			leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
			                                           GetLeaderRequest(coord.clusterKey, UID()),
			                                           TaskPriority::CoordinationReply));
		}
	}

	choose {
		when(wait(waitForAll(leaderServers))) {}
		when(wait(delay(5.0))) {
			return CoordinatorsResult::COORDINATOR_UNREACHABLE;
		}
	}
	TraceEvent("ChangeQuorumCheckerSetCoordinatorsKey")
	    .detail("CurrentCoordinators", old.toString())
	    .detail("NewCoordinators", conn->toString());
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
			state Optional<ClusterConnectionString> clusterConnectionStringOptional =
			    wait(getClusterConnectionStringFromStorageServer(&tr));

			if (!clusterConnectionStringOptional.present()) {
				return CoordinatorsResult::BAD_DATABASE_STATE;
			}

			// The cluster connection string stored in the storage server
			state ClusterConnectionString oldClusterConnectionString = clusterConnectionStringOptional.get();
			state Key oldClusterKeyName = oldClusterConnectionString.clusterKeyName();

			state std::vector<NetworkAddress> oldCoordinators = wait(oldClusterConnectionString.tryResolveHostnames());
			state CoordinatorsResult result = CoordinatorsResult::SUCCESS;
			if (!desiredCoordinators.size()) {
				std::vector<NetworkAddress> _desiredCoordinators = wait(
				    change->getDesiredCoordinators(&tr,
				                                   oldCoordinators,
				                                   Reference<ClusterConnectionMemoryRecord>(
				                                       new ClusterConnectionMemoryRecord(oldClusterConnectionString)),
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
				newName = oldClusterKeyName.toString();

			if (oldCoordinators == desiredCoordinators && oldClusterKeyName == newName)
				return retries ? CoordinatorsResult::SUCCESS : CoordinatorsResult::SAME_NETWORK_ADDRESSES;

			state ClusterConnectionString newClusterConnectionString(
			    desiredCoordinators, StringRef(newName + ':' + deterministicRandom()->randomAlphaNumeric(32)));
			state Key newClusterKeyName = newClusterConnectionString.clusterKeyName();

			if (g_network->isSimulated()) {
				for (int i = 0; i < (desiredCoordinators.size() / 2) + 1; i++) {
					auto process = g_simulator->getProcessByAddress(desiredCoordinators[i]);
					ASSERT(process->isReliable() || process->rebooting);

					g_simulator->protectedAddresses.insert(process->addresses.address);
					if (process->addresses.secondaryAddress.present()) {
						g_simulator->protectedAddresses.insert(process->addresses.secondaryAddress.get());
					}
					TraceEvent("ProtectCoordinator").detail("Address", desiredCoordinators[i]).backtrace();
				}
			}

			TraceEvent("AttemptingQuorumChange")
			    .detail("FromCS", oldClusterConnectionString.toString())
			    .detail("ToCS", newClusterConnectionString.toString());
			CODE_PROBE(oldClusterKeyName != newClusterKeyName, "Quorum change with new name");
			CODE_PROBE(oldClusterKeyName == newClusterKeyName, "Quorum change with unchanged name");

			state std::vector<Future<Optional<LeaderInfo>>> leaderServers;
			state ClientCoordinators coord(Reference<ClusterConnectionMemoryRecord>(
			    new ClusterConnectionMemoryRecord(newClusterConnectionString)));
			// check if allowed to modify the cluster descriptor
			if (!change->getDesiredClusterKeyName().empty()) {
				CheckDescriptorMutableReply mutabilityReply =
				    wait(coord.clientLeaderServers[0].checkDescriptorMutable.getReply(CheckDescriptorMutableRequest()));
				if (!mutabilityReply.isMutable) {
					return CoordinatorsResult::BAD_DATABASE_STATE;
				}
			}
			leaderServers.reserve(coord.clientLeaderServers.size());
			for (int i = 0; i < coord.clientLeaderServers.size(); i++)
				leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
				                                           GetLeaderRequest(coord.clusterKey, UID()),
				                                           TaskPriority::CoordinationReply));
			choose {
				when(wait(waitForAll(leaderServers))) {}
				when(wait(delay(5.0))) {
					return CoordinatorsResult::COORDINATOR_UNREACHABLE;
				}
			}

			tr.set(coordinatorsKey, newClusterConnectionString.toString());

			wait(tr.commit());
			ASSERT(false); // commit should fail, but the value has changed
		} catch (Error& e) {
			TraceEvent("RetryQuorumChange").error(e).detail("Retries", retries);
			wait(tr.onError(e));
			++retries;
		}
	}
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
		state Future<Optional<Value>> fStorageReplicas = tr->get("storage_replicas"_sr.withPrefix(configKeysPrefix));
		state Future<Optional<Value>> fLogReplicas = tr->get("log_replicas"_sr.withPrefix(configKeysPrefix));
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
		ClusterConnectionString cs = ccr->getConnectionString();
		if (oldCoordinators.size() != cs.hostnames.size() + cs.coords.size()) {
			return false;
		}

		// Are there enough coordinators for the redundancy level?
		if (oldCoordinators.size() < desiredCount)
			return false;
		if (oldCoordinators.size() % 2 != 1)
			return false;

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

		// Check availability
		ClientCoordinators coord(ccr);
		std::vector<Future<Optional<LeaderInfo>>> leaderServers;
		leaderServers.reserve(coord.clientLeaderServers.size());
		for (int i = 0; i < coord.clientLeaderServers.size(); i++) {
			if (coord.clientLeaderServers[i].hostname.present()) {
				leaderServers.push_back(retryGetReplyFromHostname(GetLeaderRequest(coord.clusterKey, UID()),
				                                                  coord.clientLeaderServers[i].hostname.get(),
				                                                  WLTOKEN_CLIENTLEADERREG_GETLEADER,
				                                                  TaskPriority::CoordinationReply));
			} else {
				leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
				                                           GetLeaderRequest(coord.clusterKey, UID()),
				                                           TaskPriority::CoordinationReply));
			}
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

		std::vector<AddressExclusion> excl = wait(getAllExcludedServers(tr));
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
			if (ok) {
				*err = CoordinatorsResult::SAME_NETWORK_ADDRESSES;
				return oldCoordinators;
			}
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

		std::vector<StringRef> fields({ "dcid"_sr, "data_hall"_sr, "zoneid"_sr, "machineid"_sr });

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
				if (g_network->isSimulated() && !g_simulator->getProcessByAddress(worker->address)->isReliable()) {
					TraceEvent("AutoSelectCoordinators").detail("SkipUnreliableWorker", worker->address.toString());
					continue;
				}
				bool valid = true;
				for (auto field = fields.begin(); field != fields.end(); field++) {
					if (maxCounts[*field] == 0) {
						maxCounts[*field] = 1;
					}
					auto value = worker->locality.get(*field).orDefault(""_sr);
					auto currentCount = currentCounts[*field][value];
					if (currentCount >= maxCounts[*field]) {
						valid = false;
						break;
					}
				}
				if (valid) {
					for (auto field = fields.begin(); field != fields.end(); field++) {
						auto value = worker->locality.get(*field).orDefault(""_sr);
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

ACTOR Future<Void> excludeServers(Transaction* tr, std::vector<AddressExclusion> servers, bool failed) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	std::vector<AddressExclusion> excl = wait(failed ? getExcludedFailedServerList(tr) : getExcludedServerList(tr));
	std::set<AddressExclusion> exclusions(excl.begin(), excl.end());
	bool containNewExclusion = false;
	for (auto& s : servers) {
		if (exclusions.find(s) != exclusions.end()) {
			continue;
		}
		containNewExclusion = true;
		if (failed) {
			tr->set(encodeFailedServersKey(s), StringRef());
		} else {
			tr->set(encodeExcludedServersKey(s), StringRef());
		}
	}

	if (containNewExclusion) {
		std::string excludeVersionKey = deterministicRandom()->randomUniqueID().toString();
		auto serversVersionKey = failed ? failedServersVersionKey : excludedServersVersionKey;
		tr->addReadConflictRange(singleKeyRange(serversVersionKey)); // To conflict with parallel includeServers
		tr->set(serversVersionKey, excludeVersionKey);
	}
	TraceEvent("ExcludeServersCommit")
	    .detail("Servers", describe(servers))
	    .detail("ExcludeFailed", failed)
	    .detail("ExclusionUpdated", containNewExclusion);
	return Void();
}

ACTOR Future<Void> excludeServers(Database cx, std::vector<AddressExclusion> servers, bool failed) {
	if (cx->apiVersionAtLeast(700)) {
		state ReadYourWritesTransaction ryw(cx);
		loop {
			try {
				ryw.setOption(FDBTransactionOptions::RAW_ACCESS);
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
				wait(excludeServers(&tr, servers, failed));
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
ACTOR Future<Void> excludeLocalities(Transaction* tr, std::unordered_set<std::string> localities, bool failed) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	std::vector<std::string> excl = wait(failed ? getExcludedFailedLocalityList(tr) : getExcludedLocalityList(tr));
	std::set<std::string> exclusion(excl.begin(), excl.end());
	bool containNewExclusion = false;
	for (const auto& l : localities) {
		if (exclusion.find(l) != exclusion.end()) {
			continue;
		}
		containNewExclusion = true;
		if (failed) {
			tr->set(encodeFailedLocalityKey(l), StringRef());
		} else {
			tr->set(encodeExcludedLocalityKey(l), StringRef());
		}
	}
	if (containNewExclusion) {
		std::string excludeVersionKey = deterministicRandom()->randomUniqueID().toString();
		auto localityVersionKey = failed ? failedLocalityVersionKey : excludedLocalityVersionKey;
		tr->addReadConflictRange(singleKeyRange(localityVersionKey)); // To conflict with parallel includeLocalities
		tr->set(localityVersionKey, excludeVersionKey);
	}
	TraceEvent("ExcludeLocalitiesCommit")
	    .detail("Localities", describe(localities))
	    .detail("ExcludeFailed", failed)
	    .detail("ExclusionUpdated", containNewExclusion);
	return Void();
}

// Exclude the servers matching the given set of localities from use as state servers.
// excludes localities by setting the keys.
ACTOR Future<Void> excludeLocalities(Database cx, std::unordered_set<std::string> localities, bool failed) {
	if (cx->apiVersionAtLeast(700)) {
		state ReadYourWritesTransaction ryw(cx);
		loop {
			try {
				ryw.setOption(FDBTransactionOptions::RAW_ACCESS);
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
				wait(excludeLocalities(&tr, localities, failed));
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
				ryw.setOption(FDBTransactionOptions::RAW_ACCESS);
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
							ryw.clear(KeyRangeRef(addr.withSuffix(":"_sr), addr.withSuffix(";"_sr)));
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
				ryw.setOption(FDBTransactionOptions::RAW_ACCESS);
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

ACTOR Future<std::vector<AddressExclusion>> getExcludedServerList(Transaction* tr) {
	state RangeResult r = wait(tr->getRange(excludedServersKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

	std::vector<AddressExclusion> exclusions;
	for (auto i = r.begin(); i != r.end(); ++i) {
		auto a = decodeExcludedServersKey(i->key);
		if (a.isValid())
			exclusions.push_back(a);
	}
	uniquify(exclusions);
	return exclusions;
}

ACTOR Future<std::vector<AddressExclusion>> getExcludedFailedServerList(Transaction* tr) {
	state RangeResult r = wait(tr->getRange(failedServersKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

	std::vector<AddressExclusion> exclusions;
	for (auto i = r.begin(); i != r.end(); ++i) {
		auto a = decodeFailedServersKey(i->key);
		if (a.isValid())
			exclusions.push_back(a);
	}
	uniquify(exclusions);
	return exclusions;
}

ACTOR Future<std::vector<AddressExclusion>> getAllExcludedServers(Transaction* tr) {
	state std::vector<AddressExclusion> exclusions;
	// Request all exclusion based information concurrently.
	state Future<std::vector<AddressExclusion>> fExcludedServers = getExcludedServerList(tr);
	state Future<std::vector<AddressExclusion>> fExcludedFailed = getExcludedFailedServerList(tr);
	state Future<std::vector<std::string>> fExcludedLocalities = getAllExcludedLocalities(tr);
	state Future<std::vector<ProcessData>> fWorkers = getWorkers(tr);

	// Wait until all data is gathered, we are not waiting here for the workers future to return
	// instead we wait for the worker future only if we need the data.
	wait(success(fExcludedServers) && success(fExcludedFailed) && success(fExcludedLocalities));
	// Update the exclusions vector with all excluded servers.
	auto excludedServers = fExcludedServers.get();
	exclusions.insert(exclusions.end(), excludedServers.begin(), excludedServers.end());
	auto excludedFailed = fExcludedFailed.get();
	exclusions.insert(exclusions.end(), excludedFailed.begin(), excludedFailed.end());

	// We have to return all servers that are excluded, this includes servers that are excluded
	// based on the locality. Otherwise those excluded servers might be used, even if they shouldn't.
	state std::vector<std::string> excludedLocalities = fExcludedLocalities.get();

	// Only if at least one locality was found we have to perform this check.
	if (!excludedLocalities.empty()) {
		// First we have to fetch all workers to match the localities of each worker against the excluded localities.
		wait(success(fWorkers));
		state std::vector<ProcessData> workers = fWorkers.get();

		for (const auto& locality : excludedLocalities) {
			std::set<AddressExclusion> localityAddresses = getAddressesByLocality(workers, locality);
			if (!localityAddresses.empty()) {
				// Add all the server ipaddresses that belong to the given localities to the exclusionSet.
				exclusions.insert(exclusions.end(), localityAddresses.begin(), localityAddresses.end());
			}
		}
	}

	uniquify(exclusions);
	return exclusions;
}

ACTOR Future<std::vector<AddressExclusion>> getAllExcludedServers(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // necessary?
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<AddressExclusion> exclusions = wait(getAllExcludedServers(&tr));
			return exclusions;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<std::string>> getExcludedLocalityList(Transaction* tr) {
	state RangeResult r = wait(tr->getRange(excludedLocalityKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

	std::vector<std::string> excludedLocalities;
	for (const auto& i : r) {
		auto a = decodeExcludedLocalityKey(i.key);
		excludedLocalities.push_back(a);
	}
	uniquify(excludedLocalities);
	return excludedLocalities;
}

ACTOR Future<std::vector<std::string>> getExcludedFailedLocalityList(Transaction* tr) {
	state RangeResult r = wait(tr->getRange(failedLocalityKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!r.more && r.size() < CLIENT_KNOBS->TOO_MANY);

	std::vector<std::string> excludedLocalities;
	for (const auto& i : r) {
		auto a = decodeFailedLocalityKey(i.key);
		excludedLocalities.push_back(a);
	}
	uniquify(excludedLocalities);
	return excludedLocalities;
}

ACTOR Future<std::vector<std::string>> getAllExcludedLocalities(Transaction* tr) {
	state std::vector<std::string> exclusions;
	state Future<std::vector<std::string>> fExcludedLocalities = getExcludedLocalityList(tr);
	state Future<std::vector<std::string>> fFailedLocalities = getExcludedFailedLocalityList(tr);

	// Wait until all data is gathered.
	wait(success(fExcludedLocalities) && success(fFailedLocalities));

	auto excludedLocalities = fExcludedLocalities.get();
	exclusions.insert(exclusions.end(), excludedLocalities.begin(), excludedLocalities.end());
	auto failedLocalities = fFailedLocalities.get();
	exclusions.insert(exclusions.end(), failedLocalities.begin(), failedLocalities.end());

	uniquify(exclusions);
	return exclusions;
}

// Get the list of excluded localities by reading the keys.
ACTOR Future<std::vector<std::string>> getAllExcludedLocalities(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<std::string> exclusions = wait(getAllExcludedLocalities(&tr));
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

// Returns the list of IPAddresses of the servers that match the given locality.
// Example: locality="dcid:primary" returns all the ip addresses of the servers in the primary dc.
std::set<AddressExclusion> getServerAddressesByLocality(
    const std::map<std::string, StorageServerInterface> server_interfaces,
    const std::string& locality) {
	std::pair<std::string, std::string> locality_key_value = decodeLocality(locality);
	std::set<AddressExclusion> locality_addresses;

	for (auto& server : server_interfaces) {
		auto locality_value = server.second.locality.get(locality_key_value.first);
		if (!locality_value.present()) {
			continue;
		}

		if (locality_value.get() != locality_key_value.second) {
			continue;
		}

		auto primary_address = server.second.address();
		locality_addresses.insert(AddressExclusion(primary_address.ip, primary_address.port));
		if (server.second.secondaryAddress().present()) {
			auto secondary_address = server.second.secondaryAddress().get();
			locality_addresses.insert(AddressExclusion(secondary_address.ip, secondary_address.port));
		}
	}

	return locality_addresses;
}

// Returns the list of IPAddresses of the workers that match the given locality.
// Example: locality="locality_dcid:primary" returns all the ip addresses of the workers in the primary dc.
std::set<AddressExclusion> getAddressesByLocality(const std::vector<ProcessData>& workers,
                                                  const std::string& locality) {
	std::pair<std::string, std::string> locality_key_value = decodeLocality(locality);
	std::set<AddressExclusion> locality_addresses;

	for (int i = 0; i < workers.size(); i++) {
		auto locality_value = workers[i].locality.get(locality_key_value.first);
		if (!locality_value.present()) {
			continue;
		}

		if (locality_value.get() != locality_key_value.second) {
			continue;
		}

		locality_addresses.insert(AddressExclusion(workers[i].address.ip, workers[i].address.port));
	}

	return locality_addresses;
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

ACTOR Future<int> setDDMode(Database cx, int mode) {
	state Transaction tr(cx);
	state int oldMode = -1;
	state BinaryWriter wr(Unversioned());
	wr << mode;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
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
				// set DDMode to 2 is a security mode which disables data moves but allows auditStorage part
				// DDMode=2 is set when shard location metadata inconsistency is detected
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
	             BinaryWriter::toValue(id, Unversioned()).withPrefix("0123456789"_sr).withSuffix("\x00\x00\x00\x00"_sr),
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
	             BinaryWriter::toValue(id, Unversioned()).withPrefix("0123456789"_sr).withSuffix("\x00\x00\x00\x00"_sr),
	             MutationRef::SetVersionstampedValue);
	tr->addWriteConflictRange(normalKeys);
	return Void();
}

ACTOR Future<Void> lockDatabase(Database cx, UID id) {
	state Transaction tr(cx);
	UID debugID = deterministicRandom()->randomUniqueID();
	TraceEvent("LockDatabaseTransaction", debugID).log();
	tr.debugTransaction(debugID);
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
			if (g_network->isSimulated()) {
				g_simulator->validationData.allDestroyedChangeFeedIDs.insert(rangeID.toString());
			}
			tr->set(rangeIDKey,
			        changeFeedValue(std::get<0>(decodeChangeFeedValue(val.get())),
			                        std::get<1>(decodeChangeFeedValue(val.get())),
			                        status));
			tr->clear(rangeIDKey);
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
			if (g_network->isSimulated()) {
				g_simulator->validationData.allDestroyedChangeFeedIDs.insert(rangeID.toString());
			}
			tr->set(rangeIDKey,
			        changeFeedValue(std::get<0>(decodeChangeFeedValue(val.get())),
			                        std::get<1>(decodeChangeFeedValue(val.get())),
			                        status));
			tr->clear(rangeIDKey);
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

ACTOR Future<UID> auditStorage(Reference<IClusterConnectionRecord> clusterFile,
                               KeyRange range,
                               AuditType type,
                               KeyValueStoreType engineType,
                               double timeoutSeconds) {
	state Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface(new AsyncVar<Optional<ClusterInterface>>);
	state Future<Void> leaderMon = monitorLeader<ClusterInterface>(clusterFile, clusterInterface);
	TraceEvent(SevVerbose, "ManagementAPIAuditStorageTrigger").detail("AuditType", type).detail("Range", range);
	state UID auditId;
	try {
		while (!clusterInterface->get().present()) {
			wait(clusterInterface->onChange());
		}
		TraceEvent(SevVerbose, "ManagementAPIAuditStorageBegin").detail("AuditType", type).detail("Range", range);
		TriggerAuditRequest req(type, range, engineType);
		UID auditId_ = wait(timeoutError(clusterInterface->get().get().triggerAudit.getReply(req), timeoutSeconds));
		auditId = auditId_;
		TraceEvent(SevVerbose, "ManagementAPIAuditStorageEnd")
		    .detail("AuditType", type)
		    .detail("Range", range)
		    .detail("AuditID", auditId);
	} catch (Error& e) {
		TraceEvent(SevInfo, "ManagementAPIAuditStorageError")
		    .errorUnsuppressed(e)
		    .detail("AuditType", type)
		    .detail("Range", range)
		    .detail("AuditID", auditId);
		throw e;
	}

	return auditId;
}

ACTOR Future<UID> cancelAuditStorage(Reference<IClusterConnectionRecord> clusterFile,
                                     AuditType type,
                                     UID auditId,
                                     double timeoutSeconds) {
	state Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface(new AsyncVar<Optional<ClusterInterface>>);
	state Future<Void> leaderMon = monitorLeader<ClusterInterface>(clusterFile, clusterInterface);
	TraceEvent(SevVerbose, "ManagementAPICancelAuditStorageTrigger")
	    .detail("AuditType", type)
	    .detail("AuditId", auditId);
	try {
		while (!clusterInterface->get().present()) {
			wait(clusterInterface->onChange());
		}
		TraceEvent(SevVerbose, "ManagementAPICancelAuditStorageBegin")
		    .detail("AuditType", type)
		    .detail("AuditId", auditId);
		TriggerAuditRequest req(type, auditId);
		UID auditId_ = wait(timeoutError(clusterInterface->get().get().triggerAudit.getReply(req), timeoutSeconds));
		ASSERT(auditId_ == auditId);
		TraceEvent(SevVerbose, "ManagementAPICancelAuditStorageEnd")
		    .detail("AuditType", type)
		    .detail("AuditID", auditId);
	} catch (Error& e) {
		TraceEvent(SevInfo, "ManagementAPICancelAuditStorageError")
		    .errorUnsuppressed(e)
		    .detail("AuditType", type)
		    .detail("AuditID", auditId);
		throw e;
	}

	return auditId;
}

ACTOR Future<int> setBulkLoadMode(Database cx, int mode) {
	state Transaction tr(cx);
	state BinaryWriter wr(Unversioned());
	wr << mode;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			state int oldMode = 0;
			Optional<Value> oldModeValue = wait(tr.get(bulkLoadModeKey));
			if (oldModeValue.present()) {
				BinaryReader rd(oldModeValue.get(), Unversioned());
				rd >> oldMode;
			}
			if (oldMode != mode) {
				BinaryWriter wrMyOwner(Unversioned());
				wrMyOwner << dataDistributionModeLock;
				tr.set(moveKeysLockOwnerKey, wrMyOwner.toValue());
				BinaryWriter wrLastWrite(Unversioned());
				wrLastWrite << deterministicRandom()->randomUniqueID(); // triger DD restarts
				tr.set(moveKeysLockWriteKey, wrLastWrite.toValue());
				tr.set(bulkLoadModeKey, wr.toValue());
				wait(tr.commit());
				TraceEvent("DDBulkLoadModeKeyChanged").detail("NewMode", mode).detail("OldMode", oldMode);
			}
			return oldMode;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<BulkLoadState>> getValidBulkLoadTasksWithinRange(
    Database cx,
    KeyRange rangeToRead,
    size_t limit = 10,
    Optional<BulkLoadPhase> phase = Optional<BulkLoadPhase>()) {
	state Transaction tr(cx);
	state Key readBegin = rangeToRead.begin;
	state Key readEnd = rangeToRead.end;
	state RangeResult rangeResult;
	state std::vector<BulkLoadState> res;
	while (readBegin < readEnd) {
		state int retryCount = 0;
		loop {
			try {
				rangeResult.clear();
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(store(rangeResult,
				           krmGetRanges(&tr,
				                        bulkLoadPrefix,
				                        KeyRangeRef(readBegin, readEnd),
				                        CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                        CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES)));
				break;
			} catch (Error& e) {
				if (retryCount > 30) {
					throw timed_out();
				}
				wait(tr.onError(e));
				retryCount++;
			}
		}
		for (int i = 0; i < rangeResult.size() - 1; ++i) {
			if (rangeResult[i].value.empty()) {
				continue;
			}
			BulkLoadState bulkLoadState = decodeBulkLoadState(rangeResult[i].value);
			KeyRange range = Standalone(KeyRangeRef(rangeResult[i].key, rangeResult[i + 1].key));
			if (range != bulkLoadState.getRange()) {
				ASSERT(bulkLoadState.getRange().contains(range));
				continue;
			}
			if (!phase.present() || phase.get() == bulkLoadState.phase) {
				res.push_back(bulkLoadState);
			}
			if (res.size() >= limit) {
				return res;
			}
		}
		readBegin = rangeResult.back().key;
	}

	return res;
}

// Submit bulkload task and overwrite any existing task and lock range
ACTOR Future<Void> submitBulkLoadTask(Database cx, BulkLoadState bulkLoadTask) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (bulkLoadTask.phase != BulkLoadPhase::Submitted) {
				TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "SubmitBulkLoadTaskError")
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("Reason", "WrongPhase")
				    .detail("Task", bulkLoadTask.toString());
				throw bulkload_task_failed();
			}
			if (!normalKeys.contains(bulkLoadTask.getRange())) {
				TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "SubmitBulkLoadTaskError")
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("Reason", "RangeOutOfScope")
				    .detail("Task", bulkLoadTask.toString());
				throw bulkload_task_failed();
			}
			wait(turnOffUserWriteTrafficForBulkLoad(&tr, bulkLoadTask.getRange()));
			bulkLoadTask.submitTime = now();
			wait(krmSetRange(&tr, bulkLoadPrefix, bulkLoadTask.getRange(), bulkLoadStateValue(bulkLoadTask)));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

// Get bulk load task metadata with range and taskId and phase selector
// Throw error if the task is outdated or the task is not in any input phase at the tr read version
ACTOR Future<BulkLoadState> getBulkLoadTask(Transaction* tr,
                                            KeyRange range,
                                            UID taskId,
                                            std::vector<BulkLoadPhase> phases) {
	state BulkLoadState bulkLoadState;
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	RangeResult result = wait(krmGetRanges(tr, bulkLoadPrefix, range));
	if (result.size() > 2) {
		throw bulkload_task_outdated();
	} else if (result[0].value.empty()) {
		throw bulkload_task_outdated();
	}
	ASSERT(result.size() == 2);
	bulkLoadState = decodeBulkLoadState(result[0].value);
	ASSERT(bulkLoadState.getTaskId().isValid());
	if (taskId != bulkLoadState.getTaskId()) {
		// This task is overwritten by a newer task
		throw bulkload_task_outdated();
	}
	KeyRange currentRange = KeyRangeRef(result[0].key, result[1].key);
	if (bulkLoadState.getRange() != currentRange) {
		// This task is partially overwritten by a newer task
		ASSERT(bulkLoadState.getRange().contains(currentRange));
		throw bulkload_task_outdated();
	}
	if (phases.size() > 0 && !bulkLoadState.onAnyPhase(phases)) {
		throw bulkload_task_outdated();
	}
	return bulkLoadState;
}

// Update bulkload task to acknowledge state and unlock the range
ACTOR Future<Void> acknowledgeBulkLoadTask(Database cx, KeyRange range, UID taskId) {
	state Transaction tr(cx);
	loop {
		state BulkLoadState bulkLoadState;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(store(bulkLoadState,
			           getBulkLoadTask(&tr, range, taskId, { BulkLoadPhase::Complete, BulkLoadPhase::Acknowledged })));
			bulkLoadState.phase = BulkLoadPhase::Acknowledged;
			ASSERT(range == bulkLoadState.getRange() && taskId == bulkLoadState.getTaskId());
			ASSERT(normalKeys.contains(range));
			wait(krmSetRange(&tr, bulkLoadPrefix, bulkLoadState.getRange(), bulkLoadStateValue(bulkLoadState)));
			wait(turnOnUserWriteTrafficForBulkLoad(&tr, bulkLoadState.getRange()));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<int> setBulkDumpMode(Database cx, int mode) {
	state Transaction tr(cx);
	state BinaryWriter wr(Unversioned());
	wr << mode;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			state int oldMode = 0;
			Optional<Value> oldModeValue = wait(tr.get(bulkDumpModeKey));
			if (oldModeValue.present()) {
				BinaryReader rd(oldModeValue.get(), Unversioned());
				rd >> oldMode;
			}
			if (oldMode != mode) {
				BinaryWriter wrMyOwner(Unversioned());
				wrMyOwner << dataDistributionModeLock;
				tr.set(moveKeysLockOwnerKey, wrMyOwner.toValue());
				BinaryWriter wrLastWrite(Unversioned());
				wrLastWrite << deterministicRandom()->randomUniqueID(); // triger DD restarts
				tr.set(moveKeysLockWriteKey, wrLastWrite.toValue());
				tr.set(bulkDumpModeKey, wr.toValue());
				wait(tr.commit());
				TraceEvent("DDBulkDumpModeKeyChanged").detail("NewMode", mode).detail("OldMode", oldMode);
			}
			return oldMode;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Submit bulkdump task and overwrite any existing task
ACTOR Future<Void> submitBulkDumpTask(Database cx, BulkDumpState bulkDumpTask) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (bulkDumpTask.getPhase() != BulkDumpPhase::Submitted) {
				TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "SubmitBulkDumpTaskError")
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("Reason", "WrongPhase")
				    .detail("Task", bulkDumpTask.toString());
				throw bulkdump_task_failed();
			}
			if (!normalKeys.contains(bulkDumpTask.getRange())) {
				TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "SubmitBulkLoadTaskError")
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("Reason", "RangeOutOfScope")
				    .detail("Task", bulkDumpTask.toString());
				throw bulkdump_task_failed();
			}
			wait(krmSetRange(&tr, bulkDumpPrefix, bulkDumpTask.getRange(), bulkDumpStateValue(bulkDumpTask)));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<std::vector<BulkDumpState>> getValidBulkDumpTasksWithinRange(Database cx,
                                                                          KeyRange rangeToRead,
                                                                          Optional<size_t> limit,
                                                                          Optional<BulkDumpPhase> phase) {
	state Transaction tr(cx);
	state Key readBegin = rangeToRead.begin;
	state Key readEnd = rangeToRead.end;
	state RangeResult rangeResult;
	state std::vector<BulkDumpState> res;
	while (readBegin < readEnd) {
		state int retryCount = 0;
		loop {
			try {
				rangeResult.clear();
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(store(rangeResult,
				           krmGetRanges(&tr,
				                        bulkDumpPrefix,
				                        KeyRangeRef(readBegin, readEnd),
				                        CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                        CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES)));
				break;
			} catch (Error& e) {
				if (retryCount > 30) {
					throw timed_out();
				}
				wait(tr.onError(e));
				retryCount++;
			}
		}
		for (int i = 0; i < rangeResult.size() - 1; ++i) {
			if (rangeResult[i].value.empty()) {
				continue;
			}
			BulkDumpState bulkDumpState = decodeBulkDumpState(rangeResult[i].value);
			KeyRange range = Standalone(KeyRangeRef(rangeResult[i].key, rangeResult[i + 1].key));
			if (range != bulkDumpState.getRange()) {
				ASSERT(bulkDumpState.getRange().contains(range));
				continue;
			}
			if (!phase.present() || phase.get() == bulkDumpState.getPhase()) {
				res.push_back(bulkDumpState);
			}
			if (limit.present() && res.size() >= limit.get()) {
				return res;
			}
		}
		readBegin = rangeResult.back().key;
	}

	return res;
}

// Persist a new owner if input uniqueId is not existing; Update description if input uniqueId exists
ACTOR Future<Void> registerRangeLockOwner(Database cx, std::string uniqueId, std::string description) {
	if (uniqueId.empty() || description.empty()) {
		throw range_lock_failed();
	}
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> res = wait(tr.get(rangeLockOwnerKeyFor(uniqueId)));
			RangeLockOwner owner;
			if (res.present()) {
				owner = decodeRangeLockOwner(res.get());
				ASSERT(owner.isValid());
				if (owner.getDescription() == description) {
					return Void();
				}
				owner.setDescription(description);
			} else {
				owner = RangeLockOwner(uniqueId, description);
			}
			tr.set(rangeLockOwnerKeyFor(uniqueId), rangeLockOwnerValue(owner));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> removeRangeLockOwner(Database cx, std::string uniqueId) {
	if (uniqueId.empty()) {
		throw range_lock_failed();
	}
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> res = wait(tr.get(rangeLockOwnerKeyFor(uniqueId)));
			if (!res.present()) {
				return Void();
			}
			RangeLockOwner owner = decodeRangeLockOwner(res.get());
			ASSERT(owner.isValid());
			tr.clear(rangeLockOwnerKeyFor(uniqueId));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, std::string uniqueId) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> res = wait(tr.get(rangeLockOwnerKeyFor(uniqueId)));
			if (!res.present()) {
				return Optional<RangeLockOwner>();
			}
			RangeLockOwner owner = decodeRangeLockOwner(res.get());
			ASSERT(owner.isValid());
			return owner;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx) {
	state std::vector<RangeLockOwner> res;
	state Key beginKey = rangeLockOwnerKeys.begin;
	state Key endKey = rangeLockOwnerKeys.end;
	state Transaction tr(cx);
	loop {
		state KeyRange rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult result = wait(tr.getRange(rangeToRead, CLIENT_KNOBS->TOO_MANY));
			for (const auto& kv : result) {
				RangeLockOwner owner = decodeRangeLockOwner(kv.value);
				ASSERT(owner.isValid());
				RangeLockOwnerName uidFromKey = decodeRangeLockOwnerKey(kv.key);
				ASSERT(owner.getUniqueId() == uidFromKey);
				res.push_back(owner);
			}
			if (result[result.size() - 1].key == endKey) {
				return res;
			} else {
				beginKey = result[result.size() - 1].key;
				tr.reset();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Not transactional
ACTOR Future<std::vector<KeyRange>> getReadLockOnRange(Database cx, KeyRange range) {
	if (range.end > normalKeys.end) {
		throw range_lock_failed();
	}
	state std::vector<KeyRange> lockedRanges;
	state Key beginKey = range.begin;
	state Key endKey = range.end;
	state Transaction tr(cx);
	loop {
		state KeyRange rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult result = wait(krmGetRanges(&tr, rangeLockPrefix, rangeToRead));
			for (int i = 0; i < result.size() - 1; i++) {
				if (result[i].value.empty()) {
					continue;
				}
				RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(result[i].value);
				ASSERT(rangeLockStateSet.isValid());
				if (rangeLockStateSet.isLockedFor(RangeLockType::ReadLockOnRange)) {
					lockedRanges.push_back(Standalone(KeyRangeRef(result[i].key, result[i + 1].key)));
				}
			}
			if (result[result.size() - 1].key == range.end) {
				break;
			} else {
				beginKey = result[result.size() - 1].key;
				tr.reset();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return lockedRanges;
}

// Transactional
ACTOR Future<Void> turnOffUserWriteTrafficForBulkLoad(Transaction* tr, KeyRange range) {
	if (range.end > normalKeys.end) {
		throw bulkload_task_failed();
	}
	tr->addWriteConflictRange(normalKeys);
	// Validate
	state Key beginKey = range.begin;
	state Key endKey = range.end;
	state KeyRange rangeToRead;
	while (beginKey < endKey) {
		rangeToRead = KeyRangeRef(beginKey, endKey);
		RangeResult res = wait(krmGetRanges(tr, rangeLockPrefix, rangeToRead));
		for (int i = 0; i < res.size() - 1; i++) {
			if (res[i].value.empty()) {
				continue;
			}
			RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(res[i].value);
			ASSERT(rangeLockStateSet.isValid());
			for (const auto& [lockId, lock] : rangeLockStateSet.getLocks()) {
				if (lock == RangeLockState(RangeLockType::ReadLockOnRange, rangeLockNameForBulkLoad)) {
					continue;
				}
				// TODO(BulkLoad): should cancel this task
				TraceEvent(SevError, "DDBulkLoadSeeUnexpectedRangeLock")
				    .detail("Lock", lock.toString())
				    .detail("Range", rangeToRead);
				throw not_implemented();
			}
		}
		beginKey = res[res.size() - 1].key;
	}
	// Lock range exclusively by overwiting the range
	RangeLockStateSet rangeLockStateSet;
	rangeLockStateSet.insertIfNotExist(RangeLockState(RangeLockType::ReadLockOnRange, rangeLockNameForBulkLoad));
	wait(krmSetRangeCoalescing(tr, rangeLockPrefix, range, normalKeys, rangeLockStateSetValue(rangeLockStateSet)));
	TraceEvent(SevInfo, "DDBulkLoadTurnOffWriteTraffic").detail("Range", range);
	return Void();
}

// Transactional
ACTOR Future<Void> turnOnUserWriteTrafficForBulkLoad(Transaction* tr, KeyRange range) {
	if (range.end > normalKeys.end) {
		throw bulkload_task_failed();
	}
	// Validate
	state Key beginKey = range.begin;
	state Key endKey = range.end;
	state KeyRange rangeToRead;
	while (beginKey < endKey) {
		rangeToRead = KeyRangeRef(beginKey, endKey);
		RangeResult res = wait(krmGetRanges(tr, rangeLockPrefix, rangeToRead));
		for (int i = 0; i < res.size() - 1; i++) {
			if (res[i].value.empty()) {
				continue;
			}
			RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(res[i].value);
			ASSERT(rangeLockStateSet.isValid());
			for (const auto& [lockId, lock] : rangeLockStateSet.getLocks()) {
				if (lock != RangeLockState(RangeLockType::ReadLockOnRange, rangeLockNameForBulkLoad)) {
					TraceEvent(SevError, "DDBulkLoadSeeUnexpectedRangeLock")
					    .detail("Lock", lock.toString())
					    .detail("Range", rangeToRead);
					ASSERT_WE_THINK(false);
					// TODO(BulkLoad): make lock exclusive to other applications
					// This is unexpected because others should see the exclusive lock of bulk load
					// and give up locking the range
				}
			}
		}
		beginKey = res[res.size() - 1].key;
	}
	// Unlock exclusively by overwiting the range
	// TODO(BulkLoad): use exclusive write lock for bulk load in the future
	wait(krmSetRangeCoalescing(tr, rangeLockPrefix, range, normalKeys, StringRef()));
	TraceEvent(SevInfo, "DDBulkLoadTurnOnWriteTraffic").detail("Range", range);
	return Void();
}

// Not transactional
ACTOR Future<Void> takeReadLockOnRange(Database cx, KeyRange range, std::string ownerUniqueID) {
	if (range.end > normalKeys.end) {
		throw range_lock_failed();
	}
	state Key beginKey = range.begin;
	state Key endKey = range.end;
	state Transaction tr(cx);
	loop {
		state KeyRange rangeToLock = Standalone(KeyRangeRef(beginKey, endKey));
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.addWriteConflictRange(normalKeys);

			// Step 1: Check owner
			state Optional<Value> ownerValue = wait(tr.get(rangeLockOwnerKeyFor(ownerUniqueID)));
			if (!ownerValue.present()) {
				throw range_lock_failed();
			}
			state RangeLockOwner owner = decodeRangeLockOwner(ownerValue.get());
			ASSERT(owner.isValid());

			// Step 2: Get all locks on the range and add the new lock
			state RangeResult result = wait(krmGetRanges(&tr, rangeLockPrefix, rangeToLock));
			state int i = 0;
			for (; i < result.size() - 1; i++) {
				KeyRange lockRange = Standalone(KeyRangeRef(result[i].key, result[i + 1].key));
				RangeLockStateSet rangeLockStateSet;
				if (!result[i].value.empty()) {
					rangeLockStateSet = decodeRangeLockStateSet(result[i].value);
				}
				rangeLockStateSet.insertIfNotExist(RangeLockState(RangeLockType::ReadLockOnRange, owner.getUniqueId()));
				ASSERT(rangeLockStateSet.isValid());
				wait(krmSetRangeCoalescing(
				    &tr, rangeLockPrefix, lockRange, normalKeys, rangeLockStateSetValue(rangeLockStateSet)));
				wait(tr.commit());
				tr.reset();
				beginKey = result[i + 1].key;
				break;
			}

			// Step 3: Exit if all ranges have been locked
			if (beginKey == range.end) {
				break;
			}

			wait(delay(0.1));

		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

// Not transactional
ACTOR Future<Void> releaseReadLockOnRange(Database cx, KeyRange range, std::string ownerUniqueID) {
	if (range.end > normalKeys.end) {
		throw range_lock_failed();
	}
	state Key beginKey = range.begin;
	state Key endKey = range.end;
	state Transaction tr(cx);
	loop {
		state KeyRange rangeToLock = Standalone(KeyRangeRef(beginKey, endKey));
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			// Step 1: Check owner
			state Optional<Value> ownerValue = wait(tr.get(rangeLockOwnerKeyFor(ownerUniqueID)));
			if (!ownerValue.present()) {
				throw range_lock_failed();
			}
			state RangeLockOwner owner = decodeRangeLockOwner(ownerValue.get());
			ASSERT(owner.isValid());

			// Step 2: Get all locks on the range and remove the lock
			state RangeResult result = wait(krmGetRanges(&tr, rangeLockPrefix, rangeToLock));
			state int i = 0;
			for (; i < result.size() - 1; i++) {
				KeyRange lockRange = Standalone(KeyRangeRef(result[i].key, result[i + 1].key));
				if (result[i].value.empty()) {
					beginKey = result[i + 1].key;
					continue;
				}
				RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(result[i].value);
				rangeLockStateSet.remove(RangeLockState(RangeLockType::ReadLockOnRange, owner.getUniqueId()));
				ASSERT(rangeLockStateSet.isValid());
				wait(krmSetRangeCoalescing(
				    &tr, rangeLockPrefix, lockRange, normalKeys, rangeLockStateSetValue(rangeLockStateSet)));
				wait(tr.commit());
				tr.reset();
				beginKey = result[i + 1].key;
				break;
			}

			// Step 3: Exit if all ranges have been unlocked
			if (beginKey == range.end) {
				break;
			}

			wait(delay(0.1));

		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
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
		data.locality.set("dcid"_sr, StringRef(dataCenter));
		data.locality.set("data_hall"_sr, StringRef(dataHall));
		data.locality.set("rack"_sr, StringRef(rack));
		data.locality.set("zoneid"_sr, StringRef(rack));
		data.locality.set("machineid"_sr, StringRef(machineId));
		data.address.ip = IPAddress(i);

		if (g_network->isSimulated()) {
			g_simulator->newProcess("TestCoordinator",
			                        data.address.ip,
			                        data.address.port,
			                        false,
			                        1,
			                        data.locality,
			                        ProcessClass(ProcessClass::CoordinatorClass, ProcessClass::CommandLineSource),
			                        "",
			                        "",
			                        currentProtocolVersion(),
			                        false);
		}

		workers.push_back(data);
	}

	auto noAssignIndex = deterministicRandom()->randomInt(0, workers.size());
	workers[noAssignIndex].processClass._class = ProcessClass::CoordinatorClass;

	change.addDesiredWorkers(chosen, workers, 5, excluded);
	std::map<StringRef, std::set<StringRef>> chosenValues;

	ASSERT(chosen.size() == 5);
	std::vector<StringRef> fields({ "dcid"_sr, "data_hall"_sr, "zoneid"_sr, "machineid"_sr });
	for (auto worker = chosen.begin(); worker != chosen.end(); worker++) {
		ASSERT(worker->ip.toV4() < workers.size());
		LocalityData data = workers[worker->ip.toV4()].locality;
		for (auto field = fields.begin(); field != fields.end(); field++) {
			chosenValues[*field].insert(data.get(*field).get());
		}
	}

	ASSERT(chosenValues["dcid"_sr].size() == 2);
	ASSERT(chosenValues["data_hall"_sr].size() == 4);
	ASSERT(chosenValues["zoneid"_sr].size() == 5);
	ASSERT(chosenValues["machineid"_sr].size() == 5);
	ASSERT(std::find(chosen.begin(), chosen.end(), workers[noAssignIndex].address) != chosen.end());

	return Void();
}
