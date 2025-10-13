/*
 * DatabaseConfiguration.cpp
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

#include <cstdio>
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "flow/ITrace.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"

DatabaseConfiguration::DatabaseConfiguration() {
	resetInternal();
}

void DatabaseConfiguration::resetInternal() {
	// does NOT reset rawConfiguration
	initialized = false;
	commitProxyCount = grvProxyCount = resolverCount = desiredTLogCount = tLogWriteAntiQuorum = tLogReplicationFactor =
	    storageTeamSize = desiredLogRouterCount = -1;
	tLogVersion = TLogVersion::DEFAULT;
	tLogDataStoreType = storageServerStoreType = testingStorageServerStoreType = KeyValueStoreType::END;
	perpetualStoreType = KeyValueStoreType::NONE;
	desiredTSSCount = 0;
	tLogSpillType = TLogSpillType::DEFAULT;
	autoCommitProxyCount = CLIENT_KNOBS->DEFAULT_AUTO_COMMIT_PROXIES;
	autoGrvProxyCount = CLIENT_KNOBS->DEFAULT_AUTO_GRV_PROXIES;
	autoResolverCount = CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS;
	autoDesiredTLogCount = CLIENT_KNOBS->DEFAULT_AUTO_LOGS;
	usableRegions = 1;
	regions.clear();
	tLogPolicy = storagePolicy = remoteTLogPolicy = Reference<IReplicationPolicy>();
	remoteDesiredTLogCount = -1;
	remoteTLogReplicationFactor = repopulateRegionAntiQuorum = 0;
	backupWorkerEnabled = false;
	perpetualStorageWiggleSpeed = 0;
	perpetualStorageWiggleLocality = "0";
	storageMigrationType = StorageMigrationType::DEFAULT;
	tenantMode = TenantMode::DISABLED;
	encryptionAtRestMode = EncryptionAtRestMode::DISABLED;
}

int toInt(ValueRef const& v) {
	return atoi(v.toString().c_str());
}

void parse(int* i, ValueRef const& v) {
	// FIXME: Sanity checking
	*i = atoi(v.toString().c_str());
}

void parse(int64_t* i, ValueRef const& v) {
	// FIXME: Sanity checking
	*i = atoll(v.toString().c_str());
}

void parse(double* i, ValueRef const& v) {
	// FIXME: Sanity checking
	*i = atof(v.toString().c_str());
}

void parseReplicationPolicy(Reference<IReplicationPolicy>* policy, ValueRef const& v) {
	BinaryReader reader(v, IncludeVersion());
	serializeReplicationPolicy(reader, *policy);
}

void parse(std::vector<RegionInfo>* regions, ValueRef const& v) {
	try {
		StatusObject statusObj = BinaryReader::fromStringRef<StatusObject>(v, IncludeVersion());
		regions->clear();
		if (statusObj["regions"].type() != json_spirit::array_type) {
			return;
		}
		StatusArray regionArray = statusObj["regions"].get_array();
		for (StatusObjectReader dc : regionArray) {
			RegionInfo info;
			json_spirit::mArray datacenters;
			dc.get("datacenters", datacenters);
			bool foundNonSatelliteDatacenter = false;
			for (StatusObjectReader s : datacenters) {
				std::string idStr;
				if (s.has("satellite") && s.last().get_int() == 1) {
					SatelliteInfo satInfo;
					s.get("id", idStr);
					satInfo.dcId = idStr;
					s.get("priority", satInfo.priority);
					s.tryGet("satellite_logs", satInfo.satelliteDesiredTLogCount);
					info.satellites.push_back(satInfo);
				} else {
					if (foundNonSatelliteDatacenter)
						throw invalid_option();
					foundNonSatelliteDatacenter = true;
					s.get("id", idStr);
					info.dcId = idStr;
					s.get("priority", info.priority);
				}
			}
			std::sort(info.satellites.begin(), info.satellites.end(), SatelliteInfo::sort_by_priority());
			if (!foundNonSatelliteDatacenter)
				throw invalid_option();
			dc.tryGet("satellite_logs", info.satelliteDesiredTLogCount);
			std::string satelliteReplication;
			if (dc.tryGet("satellite_redundancy_mode", satelliteReplication)) {
				if (satelliteReplication == "one_satellite_single") {
					info.satelliteTLogReplicationFactor = 1;
					info.satelliteTLogUsableDcs = 1;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
				} else if (satelliteReplication == "one_satellite_double") {
					info.satelliteTLogReplicationFactor = 2;
					info.satelliteTLogUsableDcs = 1;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = Reference<IReplicationPolicy>(
					    new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
				} else if (satelliteReplication == "one_satellite_triple") {
					info.satelliteTLogReplicationFactor = 3;
					info.satelliteTLogUsableDcs = 1;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = Reference<IReplicationPolicy>(
					    new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
				} else if (satelliteReplication == "two_satellite_safe") {
					info.satelliteTLogReplicationFactor = 4;
					info.satelliteTLogUsableDcs = 2;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = Reference<IReplicationPolicy>(
					    new PolicyAcross(2,
					                     "dcid",
					                     Reference<IReplicationPolicy>(new PolicyAcross(
					                         2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
					info.satelliteTLogReplicationFactorFallback = 2;
					info.satelliteTLogUsableDcsFallback = 1;
					info.satelliteTLogWriteAntiQuorumFallback = 0;
					info.satelliteTLogPolicyFallback = Reference<IReplicationPolicy>(
					    new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
				} else if (satelliteReplication == "two_satellite_fast") {
					info.satelliteTLogReplicationFactor = 4;
					info.satelliteTLogUsableDcs = 2;
					info.satelliteTLogWriteAntiQuorum = 2;
					info.satelliteTLogPolicy = Reference<IReplicationPolicy>(
					    new PolicyAcross(2,
					                     "dcid",
					                     Reference<IReplicationPolicy>(new PolicyAcross(
					                         2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())))));
					info.satelliteTLogReplicationFactorFallback = 2;
					info.satelliteTLogUsableDcsFallback = 1;
					info.satelliteTLogWriteAntiQuorumFallback = 0;
					info.satelliteTLogPolicyFallback = Reference<IReplicationPolicy>(
					    new PolicyAcross(2, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
				} else {
					throw invalid_option();
				}
			}
			dc.tryGet("satellite_log_replicas", info.satelliteTLogReplicationFactor);
			dc.tryGet("satellite_usable_dcs", info.satelliteTLogUsableDcs);
			dc.tryGet("satellite_anti_quorum", info.satelliteTLogWriteAntiQuorum);
			dc.tryGet("satellite_log_replicas_fallback", info.satelliteTLogReplicationFactorFallback);
			dc.tryGet("satellite_usable_dcs_fallback", info.satelliteTLogUsableDcsFallback);
			dc.tryGet("satellite_anti_quorum_fallback", info.satelliteTLogWriteAntiQuorumFallback);
			regions->push_back(info);
		}
		std::sort(regions->begin(), regions->end(), RegionInfo::sort_by_priority());
	} catch (Error&) {
		regions->clear();
		return;
	}
}

void DatabaseConfiguration::setDefaultReplicationPolicy() {
	if (!storagePolicy) {
		storagePolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(storageTeamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	}
	if (!tLogPolicy) {
		tLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(tLogReplicationFactor, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	}
	if (remoteTLogReplicationFactor > 0 && !remoteTLogPolicy) {
		remoteTLogPolicy = Reference<IReplicationPolicy>(
		    new PolicyAcross(remoteTLogReplicationFactor, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	}
	for (auto& r : regions) {
		if (r.satelliteTLogReplicationFactor > 0 && !r.satelliteTLogPolicy) {
			r.satelliteTLogPolicy = Reference<IReplicationPolicy>(new PolicyAcross(
			    r.satelliteTLogReplicationFactor, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		}
		if (r.satelliteTLogReplicationFactorFallback > 0 && !r.satelliteTLogPolicyFallback) {
			r.satelliteTLogPolicyFallback = Reference<IReplicationPolicy>(new PolicyAcross(
			    r.satelliteTLogReplicationFactorFallback, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		}
	}
}

bool DatabaseConfiguration::isValid() const {
	// enable this via `fdbcli --knob_cli_print_invalid_configuration=1` command line parameter
	auto log_test = [](const char* text, bool val) {
		if (!val && CLIENT_KNOBS->CLI_PRINT_INVALID_CONFIGURATION) {
			fprintf(stderr, "%s: false\n", text);
		}
		return val;
	};
// LOG_TEST(expr) takes an expression that returns a boolean.  If the boolean == false, the
// expression and it's boolean return value will be printed.
#define LOG_TEST(expr) log_test(#expr, (expr))
	if (!(LOG_TEST(initialized) && LOG_TEST(tLogWriteAntiQuorum >= 0) &&
	      LOG_TEST(tLogWriteAntiQuorum <= tLogReplicationFactor / 2) && LOG_TEST(tLogReplicationFactor >= 1) &&
	      LOG_TEST(storageTeamSize >= 1) && LOG_TEST(getDesiredCommitProxies() >= 1) &&
	      LOG_TEST(getDesiredGrvProxies() >= 1) && LOG_TEST(getDesiredLogs() >= 1) &&
	      LOG_TEST(getDesiredResolvers() >= 1) && LOG_TEST(tLogVersion != TLogVersion::UNSET) &&
	      LOG_TEST(tLogVersion >= TLogVersion::MIN_RECRUITABLE) &&
	      LOG_TEST(tLogVersion <= TLogVersion::MAX_SUPPORTED) &&
	      LOG_TEST(tLogDataStoreType != KeyValueStoreType::END) && LOG_TEST(tLogSpillType != TLogSpillType::UNSET) &&
	      LOG_TEST(!(tLogSpillType == TLogSpillType::REFERENCE && tLogVersion < TLogVersion::V3)) &&
	      LOG_TEST(storageServerStoreType != KeyValueStoreType::END) && LOG_TEST(autoCommitProxyCount >= 1) &&
	      LOG_TEST(autoGrvProxyCount >= 1) && LOG_TEST(autoResolverCount >= 1) && LOG_TEST(autoDesiredTLogCount >= 1) &&
	      LOG_TEST(!!storagePolicy) && LOG_TEST(!!tLogPolicy) && LOG_TEST(getDesiredRemoteLogs() >= 1) &&
	      LOG_TEST(remoteTLogReplicationFactor >= 0) && LOG_TEST(repopulateRegionAntiQuorum >= 0) &&
	      LOG_TEST(repopulateRegionAntiQuorum <= 1) && LOG_TEST(usableRegions >= 1) && LOG_TEST(usableRegions <= 2) &&
	      LOG_TEST(regions.size() <= 2) && LOG_TEST((usableRegions == 1 || regions.size() == 2)) &&
	      LOG_TEST((regions.size() == 0 || regions[0].priority >= 0)) &&
	      LOG_TEST((regions.size() == 0 || tLogPolicy->info() != "dcid^2 x zoneid^2 x 1")) &&
	      // We cannot specify regions with three_datacenter replication
	      LOG_TEST((perpetualStorageWiggleSpeed == 0 || perpetualStorageWiggleSpeed == 1)) &&
	      LOG_TEST(isValidPerpetualStorageWiggleLocality(perpetualStorageWiggleLocality)) &&
	      LOG_TEST(storageMigrationType != StorageMigrationType::UNSET) &&
	      LOG_TEST(tenantMode >= TenantMode::DISABLED) && LOG_TEST(tenantMode < TenantMode::END) &&
	      LOG_TEST(encryptionAtRestMode >= EncryptionAtRestMode::DISABLED) &&
	      LOG_TEST(encryptionAtRestMode < EncryptionAtRestMode::END))) {
		return false;
	}
#undef LOG_TEST
	std::set<Key> dcIds;
	dcIds.insert(Key());
	for (auto& r : regions) {
		if (!(!dcIds.count(r.dcId) && r.satelliteTLogReplicationFactor >= 0 && r.satelliteTLogWriteAntiQuorum >= 0 &&
		      r.satelliteTLogUsableDcs >= 1 &&
		      (r.satelliteTLogReplicationFactor == 0 || (r.satelliteTLogPolicy && r.satellites.size())) &&
		      (r.satelliteTLogUsableDcsFallback == 0 ||
		       (r.satelliteTLogReplicationFactor > 0 && r.satelliteTLogReplicationFactorFallback > 0)))) {
			return false;
		}
		dcIds.insert(r.dcId);
		std::set<Key> satelliteDcIds;
		satelliteDcIds.insert(Key());
		satelliteDcIds.insert(r.dcId);
		for (auto& s : r.satellites) {
			if (satelliteDcIds.count(s.dcId)) {
				return false;
			}
			satelliteDcIds.insert(s.dcId);
		}
	}

	return true;
}

StatusObject DatabaseConfiguration::toJSON(bool noPolicies) const {
	StatusObject result;

	if (!initialized) {
		return result;
	}

	std::string tlogInfo = tLogPolicy->info();
	std::string storageInfo = storagePolicy->info();
	bool customRedundancy = false;
	if (tLogWriteAntiQuorum == 0) {
		if (tLogReplicationFactor == 1 && storageTeamSize == 1) {
			result["redundancy_mode"] = "single";
		} else if (tLogReplicationFactor == 2 && storageTeamSize == 2) {
			result["redundancy_mode"] = "double";
		} else if (tLogReplicationFactor == 4 && storageTeamSize == 6 && tlogInfo == "dcid^2 x zoneid^2 x 1" &&
		           storageInfo == "dcid^3 x zoneid^2 x 1") {
			result["redundancy_mode"] = "three_datacenter";
		} else if (tLogReplicationFactor == 4 && storageTeamSize == 4 && tlogInfo == "dcid^2 x zoneid^2 x 1" &&
		           storageInfo == "dcid^2 x zoneid^2 x 1") {
			result["redundancy_mode"] = "three_datacenter_fallback";
		} else if (tLogReplicationFactor == 3 && storageTeamSize == 3) {
			result["redundancy_mode"] = "triple";
		} else if (tLogReplicationFactor == 4 && storageTeamSize == 3 && tlogInfo == "data_hall^2 x zoneid^2 x 1" &&
		           storageInfo == "data_hall^3 x 1") {
			result["redundancy_mode"] = "three_data_hall";
		} else if (tLogReplicationFactor == 4 && storageTeamSize == 2 && tlogInfo == "data_hall^2 x zoneid^2 x 1" &&
		           storageInfo == "data_hall^2 x 1") {
			result["redundancy_mode"] = "three_data_hall_fallback";
		} else {
			customRedundancy = true;
		}
	} else {
		customRedundancy = true;
	}

	if (customRedundancy) {
		result["storage_replicas"] = storageTeamSize;
		result["log_replicas"] = tLogReplicationFactor;
		result["log_anti_quorum"] = tLogWriteAntiQuorum;
		if (!noPolicies)
			result["storage_replication_policy"] = storagePolicy->info();
		if (!noPolicies)
			result["log_replication_policy"] = tLogPolicy->info();
	}

	if (tLogVersion > TLogVersion::DEFAULT || isOverridden("log_version")) {
		result["log_version"] = (int)tLogVersion;
	}

	result["log_engine"] = tLogDataStoreType.toString();
	result["storage_engine"] = storageServerStoreType.toString();

	if (desiredTSSCount > 0) {
		result["tss_count"] = desiredTSSCount;
		result["tss_storage_engine"] = testingStorageServerStoreType.toString();
	}

	result["log_spill"] = (int)tLogSpillType;

	if (remoteTLogReplicationFactor == 1) {
		result["remote_redundancy_mode"] = "remote_single";
	} else if (remoteTLogReplicationFactor == 2) {
		result["remote_redundancy_mode"] = "remote_double";
	} else if (remoteTLogReplicationFactor == 3) {
		result["remote_redundancy_mode"] = "remote_triple";
	} else if (remoteTLogReplicationFactor > 3) {
		result["remote_log_replicas"] = remoteTLogReplicationFactor;
		if (noPolicies && remoteTLogPolicy)
			result["remote_log_policy"] = remoteTLogPolicy->info();
	}
	result["usable_regions"] = usableRegions;

	if (regions.size()) {
		result["regions"] = getRegionJSON();
	}

	// Add to the `proxies` count for backwards compatibility with tools built before 7.0.
	int32_t proxyCount = -1;
	if (desiredTLogCount != -1 || isOverridden("logs")) {
		result["logs"] = desiredTLogCount;
	}
	if (commitProxyCount != -1 || isOverridden("commit_proxies")) {
		result["commit_proxies"] = commitProxyCount;
		if (proxyCount != -1) {
			proxyCount += commitProxyCount;
		} else {
			proxyCount = commitProxyCount;
		}
	}
	if (grvProxyCount != -1 || isOverridden("grv_proxies")) {
		result["grv_proxies"] = grvProxyCount;
		if (proxyCount != -1) {
			proxyCount += grvProxyCount;
		} else {
			proxyCount = grvProxyCount;
		}
	}
	if (resolverCount != -1 || isOverridden("resolvers")) {
		result["resolvers"] = resolverCount;
	}
	if (desiredLogRouterCount != -1 || isOverridden("log_routers")) {
		result["log_routers"] = desiredLogRouterCount;
	}
	if (remoteDesiredTLogCount != -1 || isOverridden("remote_logs")) {
		result["remote_logs"] = remoteDesiredTLogCount;
	}
	if (repopulateRegionAntiQuorum != 0 || isOverridden("repopulate_anti_quorum")) {
		result["repopulate_anti_quorum"] = repopulateRegionAntiQuorum;
	}
	if (autoCommitProxyCount != CLIENT_KNOBS->DEFAULT_AUTO_COMMIT_PROXIES || isOverridden("auto_commit_proxies")) {
		result["auto_commit_proxies"] = autoCommitProxyCount;
	}
	if (autoGrvProxyCount != CLIENT_KNOBS->DEFAULT_AUTO_GRV_PROXIES || isOverridden("auto_grv_proxies")) {
		result["auto_grv_proxies"] = autoGrvProxyCount;
	}
	if (autoResolverCount != CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS || isOverridden("auto_resolvers")) {
		result["auto_resolvers"] = autoResolverCount;
	}
	if (autoDesiredTLogCount != CLIENT_KNOBS->DEFAULT_AUTO_LOGS || isOverridden("auto_logs")) {
		result["auto_logs"] = autoDesiredTLogCount;
	}
	if (proxyCount != -1) {
		result["proxies"] = proxyCount;
	}

	result["backup_worker_enabled"] = (int32_t)backupWorkerEnabled;
	result["perpetual_storage_wiggle"] = perpetualStorageWiggleSpeed;
	result["perpetual_storage_wiggle_locality"] = perpetualStorageWiggleLocality;
	if (perpetualStoreType.storeType() != KeyValueStoreType::END) {
		result["perpetual_storage_wiggle_engine"] = perpetualStoreType.toString();
	}
	result["storage_migration_type"] = storageMigrationType.toString();
	result["tenant_mode"] = tenantMode.toString();
	result["encryption_at_rest_mode"] = encryptionAtRestMode.toString();
	return result;
}

std::string DatabaseConfiguration::configureStringFromJSON(const StatusObject& json) {
	std::string result;

	for (auto kv : json) {
		// These JSON properties are ignored for some reason.  This behavior is being maintained in a refactor
		// of this code and the old code gave no reasoning.
		static std::set<std::string> ignore = { "tss_storage_engine", "perpetual_storage_wiggle_locality" };
		if (ignore.contains(kv.first)) {
			continue;
		}

		result += " ";
		// All integers are assumed to be actual DatabaseConfig keys and are set with
		// the hidden "<name>:=<intValue>" syntax of the configure command.
		if (kv.second.type() == json_spirit::int_type) {
			result += kv.first + ":=" + format("%d", kv.second.get_int());
		} else if (kv.second.type() == json_spirit::str_type) {
			// For string values, some properties can set with a "<name>=<value>" syntax in "configure"
			// Such properties are listed here:
			static std::set<std::string> directSet = {
				"storage_migration_type", "tenant_mode", "encryption_at_rest_mode",
				"storage_engine",         "log_engine",  "perpetual_storage_wiggle_engine"
			};

			if (directSet.contains(kv.first)) {
				result += kv.first + "=" + kv.second.get_str();
			} else {
				// For the rest, it is assumed that the property name is meaningless and the value string
				// is a standalone 'configure' command which has the identical effect.
				// TODO:  Fix this terrible legacy behavior which probably isn't compatible with
				// some of the more recently added configuration and options.
				result += kv.second.get_str();
			}
		} else if (kv.second.type() == json_spirit::array_type) {
			// Array properties convert to <name>=<json_array>
			result += kv.first + "=" +
			          json_spirit::write_string(json_spirit::mValue(kv.second.get_array()),
			                                    json_spirit::Output_options::none);
		} else {
			throw invalid_config_db_key();
		}
	}

	// The log_engine setting requires some special handling because it was not included in the JSON form of a
	// DatabaseConfiguration until FDB 7.3.  This means that configuring a new database using a JSON config object from
	// an older version will now fail because it lacks an explicit log_engine setting.  Previously, the log_engine would
	// be set indirectly because the "storage_engine=<engine_name>" property from JSON would convert to a standalone
	// "<engine_name>" command in the output, and each engine name exists as a command which sets both the
	// log and storage engines, with the log engine normally being ssd-2.
	// The storage_engine and log_engine JSON properties now explicitly indicate their engine types and map to configure
	// commands of the same name.  So, to support configuring a new database with an older JSON config without an
	// explicit log_engine we simply add " log_engine=ssd-2" to the output string if the input JSON did not contain a
	// log_engine.
	if (!json.contains("log_engine")) {
		result += " log_engine=ssd-2";
	}

	return result;
}

StatusArray DatabaseConfiguration::getRegionJSON() const {
	StatusArray regionArr;
	for (auto& r : regions) {
		StatusObject regionObj;
		StatusArray dcArr;
		StatusObject dcObj;
		dcObj["id"] = r.dcId.toString();
		dcObj["priority"] = r.priority;
		dcArr.push_back(dcObj);

		if (r.satelliteTLogReplicationFactor == 1 && r.satelliteTLogUsableDcs == 1 &&
		    r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 0) {
			regionObj["satellite_redundancy_mode"] = "one_satellite_single";
		} else if (r.satelliteTLogReplicationFactor == 2 && r.satelliteTLogUsableDcs == 1 &&
		           r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 0) {
			regionObj["satellite_redundancy_mode"] = "one_satellite_double";
		} else if (r.satelliteTLogReplicationFactor == 3 && r.satelliteTLogUsableDcs == 1 &&
		           r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 0) {
			regionObj["satellite_redundancy_mode"] = "one_satellite_triple";
		} else if (r.satelliteTLogReplicationFactor == 4 && r.satelliteTLogUsableDcs == 2 &&
		           r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 1 &&
		           r.satelliteTLogReplicationFactorFallback == 2 && r.satelliteTLogWriteAntiQuorumFallback == 0) {
			regionObj["satellite_redundancy_mode"] = "two_satellite_safe";
		} else if (r.satelliteTLogReplicationFactor == 4 && r.satelliteTLogUsableDcs == 2 &&
		           r.satelliteTLogWriteAntiQuorum == 2 && r.satelliteTLogUsableDcsFallback == 1 &&
		           r.satelliteTLogReplicationFactorFallback == 2 && r.satelliteTLogWriteAntiQuorumFallback == 0) {
			regionObj["satellite_redundancy_mode"] = "two_satellite_fast";
		} else if (r.satelliteTLogReplicationFactor != 0) {
			regionObj["satellite_log_replicas"] = r.satelliteTLogReplicationFactor;
			regionObj["satellite_usable_dcs"] = r.satelliteTLogUsableDcs;
			regionObj["satellite_anti_quorum"] = r.satelliteTLogWriteAntiQuorum;
			if (r.satelliteTLogPolicy)
				regionObj["satellite_log_policy"] = r.satelliteTLogPolicy->info();
			regionObj["satellite_log_replicas_fallback"] = r.satelliteTLogReplicationFactorFallback;
			regionObj["satellite_usable_dcs_fallback"] = r.satelliteTLogUsableDcsFallback;
			regionObj["satellite_anti_quorum_fallback"] = r.satelliteTLogWriteAntiQuorumFallback;
			if (r.satelliteTLogPolicyFallback)
				regionObj["satellite_log_policy_fallback"] = r.satelliteTLogPolicyFallback->info();
		}

		if (r.satelliteDesiredTLogCount != -1) {
			regionObj["satellite_logs"] = r.satelliteDesiredTLogCount;
		}

		if (r.satellites.size()) {
			for (auto& s : r.satellites) {
				StatusObject satObj;
				satObj["id"] = s.dcId.toString();
				satObj["priority"] = s.priority;
				satObj["satellite"] = 1;
				if (s.satelliteDesiredTLogCount != -1) {
					satObj["satellite_logs"] = s.satelliteDesiredTLogCount;
				}

				dcArr.push_back(satObj);
			}
		}

		regionObj["datacenters"] = dcArr;
		regionArr.push_back(regionObj);
	}
	return regionArr;
}

std::string DatabaseConfiguration::toString() const {
	return json_spirit::write_string(json_spirit::mValue(toJSON()), json_spirit::Output_options::none);
}

Key getKeyWithPrefix(std::string const& k) {
	return StringRef(k).withPrefix(configKeysPrefix);
}

void DatabaseConfiguration::overwriteProxiesCount() {
	Key commitProxiesKey = getKeyWithPrefix("commit_proxies");
	Key grvProxiesKey = getKeyWithPrefix("grv_proxies");
	Key proxiesKey = getKeyWithPrefix("proxies");
	Optional<ValueRef> optCommitProxies = DatabaseConfiguration::get(commitProxiesKey);
	Optional<ValueRef> optGrvProxies = DatabaseConfiguration::get(grvProxiesKey);
	Optional<ValueRef> optProxies = DatabaseConfiguration::get(proxiesKey);

	const int mutableGrvProxyCount = optGrvProxies.present() ? toInt(optGrvProxies.get()) : -1;
	const int mutableCommitProxyCount = optCommitProxies.present() ? toInt(optCommitProxies.get()) : -1;
	const int mutableProxiesCount = optProxies.present() ? toInt(optProxies.get()) : -1;

	if (mutableProxiesCount > 1) {
		TraceEvent(SevDebug, "OverwriteProxiesCount")
		    .detail("CPCount", commitProxyCount)
		    .detail("MutableCPCount", mutableCommitProxyCount)
		    .detail("GrvCount", grvProxyCount)
		    .detail("MutableGrvCPCount", mutableGrvProxyCount)
		    .detail("MutableProxiesCount", mutableProxiesCount);

		if (mutableGrvProxyCount == -1 && mutableCommitProxyCount > 0) {
			if (mutableProxiesCount > mutableCommitProxyCount) {
				grvProxyCount = mutableProxiesCount - mutableCommitProxyCount;
			} else {
				// invalid configuration; provision min GrvProxies
				grvProxyCount = 1;
				commitProxyCount = mutableProxiesCount - 1;
			}
		} else if (mutableGrvProxyCount > 0 && mutableCommitProxyCount == -1) {
			if (mutableProxiesCount > mutableGrvProxyCount) {
				commitProxyCount = mutableProxiesCount - grvProxyCount;
			} else {
				// invalid configuration; provision min CommitProxies
				commitProxyCount = 1;
				grvProxyCount = mutableProxiesCount - 1;
			}
		} else if (mutableGrvProxyCount == -1 && mutableCommitProxyCount == -1) {
			// Use DEFAULT_COMMIT_GRV_PROXIES_RATIO to split proxies between Grv & Commit proxies
			const int derivedGrvProxyCount =
			    std::max(1,
			             std::min(CLIENT_KNOBS->DEFAULT_MAX_GRV_PROXIES,
			                      mutableProxiesCount / (CLIENT_KNOBS->DEFAULT_COMMIT_GRV_PROXIES_RATIO + 1)));

			grvProxyCount = derivedGrvProxyCount;
			commitProxyCount = mutableProxiesCount - grvProxyCount;
		}

		TraceEvent(SevDebug, "OverwriteProxiesCountResult")
		    .detail("CommitProxyCount", commitProxyCount)
		    .detail("GrvProxyCount", grvProxyCount)
		    .detail("ProxyCount", mutableProxiesCount);
	}
}

bool DatabaseConfiguration::setInternal(KeyRef key, ValueRef value) {
	KeyRef ck = key.removePrefix(configKeysPrefix);
	int type;

	if (ck == "initialized"_sr) {
		initialized = true;
	} else if (ck == "commit_proxies"_sr) {
		commitProxyCount = toInt(value);
		if (commitProxyCount == -1)
			overwriteProxiesCount();
	} else if (ck == "grv_proxies"_sr) {
		grvProxyCount = toInt(value);
		if (grvProxyCount == -1)
			overwriteProxiesCount();
	} else if (ck == "resolvers"_sr) {
		parse(&resolverCount, value);
	} else if (ck == "logs"_sr) {
		parse(&desiredTLogCount, value);
	} else if (ck == "log_replicas"_sr) {
		parse(&tLogReplicationFactor, value);
		tLogWriteAntiQuorum = std::min(tLogWriteAntiQuorum, tLogReplicationFactor / 2);
	} else if (ck == "log_anti_quorum"_sr) {
		parse(&tLogWriteAntiQuorum, value);
		if (tLogReplicationFactor > 0) {
			tLogWriteAntiQuorum = std::min(tLogWriteAntiQuorum, tLogReplicationFactor / 2);
		}
	} else if (ck == "storage_replicas"_sr) {
		parse(&storageTeamSize, value);
	} else if (ck == "tss_count"_sr) {
		parse(&desiredTSSCount, value);
	} else if (ck == "log_version"_sr) {
		parse((&type), value);
		type = std::max((int)TLogVersion::MIN_RECRUITABLE, type);
		type = std::min((int)TLogVersion::MAX_SUPPORTED, type);
		tLogVersion = (TLogVersion::Version)type;
	} else if (ck == "log_engine"_sr) {
		parse((&type), value);
		tLogDataStoreType = (KeyValueStoreType::StoreType)type;
		// It makes no sense to use a memory based engine to spill data that doesn't fit in memory
		// so change these to an ssd-2
		if (tLogDataStoreType == KeyValueStoreType::MEMORY ||
		    tLogDataStoreType == KeyValueStoreType::MEMORY_RADIXTREE) {
			tLogDataStoreType = KeyValueStoreType::SSD_BTREE_V2;
		}
	} else if (ck == "log_spill"_sr) {
		parse((&type), value);
		tLogSpillType = (TLogSpillType::SpillType)type;
	} else if (ck == "storage_engine"_sr) {
		parse((&type), value);
		storageServerStoreType = (KeyValueStoreType::StoreType)type;
	} else if (ck == "tss_storage_engine"_sr) {
		parse((&type), value);
		testingStorageServerStoreType = (KeyValueStoreType::StoreType)type;
	} else if (ck == "auto_commit_proxies"_sr) {
		parse(&autoCommitProxyCount, value);
	} else if (ck == "auto_grv_proxies"_sr) {
		parse(&autoGrvProxyCount, value);
	} else if (ck == "auto_resolvers"_sr) {
		parse(&autoResolverCount, value);
	} else if (ck == "auto_logs"_sr) {
		parse(&autoDesiredTLogCount, value);
	} else if (ck == "storage_replication_policy"_sr) {
		parseReplicationPolicy(&storagePolicy, value);
	} else if (ck == "log_replication_policy"_sr) {
		parseReplicationPolicy(&tLogPolicy, value);
	} else if (ck == "log_routers"_sr) {
		parse(&desiredLogRouterCount, value);
	} else if (ck == "remote_logs"_sr) {
		parse(&remoteDesiredTLogCount, value);
	} else if (ck == "remote_log_replicas"_sr) {
		parse(&remoteTLogReplicationFactor, value);
	} else if (ck == "remote_log_policy"_sr) {
		parseReplicationPolicy(&remoteTLogPolicy, value);
	} else if (ck == "backup_worker_enabled"_sr) {
		parse((&type), value);
		backupWorkerEnabled = (type != 0);
	} else if (ck == "usable_regions"_sr) {
		parse(&usableRegions, value);
	} else if (ck == "repopulate_anti_quorum"_sr) {
		parse(&repopulateRegionAntiQuorum, value);
	} else if (ck == "regions"_sr) {
		parse(&regions, value);
	} else if (ck == "perpetual_storage_wiggle"_sr) {
		parse(&perpetualStorageWiggleSpeed, value);
	} else if (ck == "perpetual_storage_wiggle_locality"_sr) {
		if (!isValidPerpetualStorageWiggleLocality(value.toString())) {
			return false;
		}
		perpetualStorageWiggleLocality = value.toString();
	} else if (ck == "perpetual_storage_wiggle_engine"_sr) {
		parse((&type), value);
		perpetualStoreType = (KeyValueStoreType::StoreType)type;
	} else if (ck == "storage_migration_type"_sr) {
		parse((&type), value);
		storageMigrationType = (StorageMigrationType::MigrationType)type;
	} else if (ck == "tenant_mode"_sr) {
		tenantMode = TenantMode::fromValue(value);
	} else if (ck == "proxies"_sr) {
		overwriteProxiesCount();
	} else if (ck == "encryption_at_rest_mode"_sr) {
		encryptionAtRestMode = EncryptionAtRestMode::fromValueRef(Optional<ValueRef>(value));
	} else if (ck.startsWith("excluded/"_sr)) {
		// excluded servers: don't keep the state internally
	} else {
		return false;
	}
	return true; // All of the above options currently require recovery to take effect
}

static KeyValueRef const* lower_bound(VectorRef<KeyValueRef> const& config, KeyRef const& key) {
	return std::lower_bound(config.begin(), config.end(), KeyValueRef(key, ValueRef()), KeyValueRef::OrderByKey());
}

void DatabaseConfiguration::applyMutation(MutationRef m) {
	if (m.type == MutationRef::SetValue && m.param1.startsWith(configKeysPrefix)) {
		set(m.param1, m.param2);
	} else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef range(m.param1, m.param2);
		if (range.intersects(configKeys)) {
			clear(range & configKeys);
		}
	}
}

bool DatabaseConfiguration::involveMutation(MutationRef m) {
	return (m.type == MutationRef::SetValue && m.param1.startsWith(configKeysPrefix)) ||
	       (m.type == MutationRef::ClearRange && KeyRangeRef(m.param1, m.param2).intersects(configKeys));
}

bool DatabaseConfiguration::set(KeyRef key, ValueRef value) {
	makeConfigurationMutable();
	mutableConfiguration.get()[key.toString()] = value.toString();
	return setInternal(key, value);
}

bool DatabaseConfiguration::clear(KeyRangeRef keys) {
	makeConfigurationMutable();
	auto& mc = mutableConfiguration.get();
	mc.erase(mc.lower_bound(keys.begin.toString()), mc.lower_bound(keys.end.toString()));

	// FIXME: More efficient
	bool wasValid = isValid();
	resetInternal();
	for (auto c = mc.begin(); c != mc.end(); ++c)
		setInternal(c->first, c->second);
	return wasValid && !isValid();
}

Optional<ValueRef> DatabaseConfiguration::get(KeyRef key) const {
	if (mutableConfiguration.present()) {
		auto i = mutableConfiguration.get().find(key.toString());
		if (i == mutableConfiguration.get().end())
			return Optional<ValueRef>();
		return ValueRef(i->second);
	} else {
		auto i = lower_bound(rawConfiguration, key);
		if (i == rawConfiguration.end() || i->key != key)
			return Optional<ValueRef>();
		return i->value;
	}
}

bool DatabaseConfiguration::isExcludedServer(NetworkAddressList a, const LocalityData& locality) const {
	return get(encodeExcludedServersKey(AddressExclusion(a.address.ip, a.address.port))).present() ||
	       get(encodeExcludedServersKey(AddressExclusion(a.address.ip))).present() ||
	       get(encodeFailedServersKey(AddressExclusion(a.address.ip, a.address.port))).present() ||
	       get(encodeFailedServersKey(AddressExclusion(a.address.ip))).present() ||
	       (a.secondaryAddress.present() &&
	        (get(encodeExcludedServersKey(AddressExclusion(a.secondaryAddress.get().ip, a.secondaryAddress.get().port)))
	             .present() ||
	         get(encodeExcludedServersKey(AddressExclusion(a.secondaryAddress.get().ip))).present() ||
	         get(encodeFailedServersKey(AddressExclusion(a.secondaryAddress.get().ip, a.secondaryAddress.get().port)))
	             .present() ||
	         get(encodeFailedServersKey(AddressExclusion(a.secondaryAddress.get().ip))).present())) ||
	       isExcludedLocality(locality);
}
std::set<AddressExclusion> DatabaseConfiguration::getExcludedServers() const {
	const_cast<DatabaseConfiguration*>(this)->makeConfigurationImmutable();
	std::set<AddressExclusion> addrs;
	for (auto i = lower_bound(rawConfiguration, excludedServersKeys.begin);
	     i != rawConfiguration.end() && i->key < excludedServersKeys.end;
	     ++i) {
		AddressExclusion a = decodeExcludedServersKey(i->key);
		if (a.isValid())
			addrs.insert(a);
	}
	for (auto i = lower_bound(rawConfiguration, failedServersKeys.begin);
	     i != rawConfiguration.end() && i->key < failedServersKeys.end;
	     ++i) {
		AddressExclusion a = decodeFailedServersKey(i->key);
		if (a.isValid())
			addrs.insert(a);
	}
	return addrs;
}

// checks if the locality is excluded or not by checking if the key is present.
bool DatabaseConfiguration::isExcludedLocality(const LocalityData& locality) const {
	std::map<std::string, std::string> localityData = locality.getAllData();
	for (const auto& l : localityData) {
		if (get(StringRef(encodeExcludedLocalityKey(LocalityData::ExcludeLocalityPrefix.toString() + l.first + ":" +
		                                            l.second)))
		        .present() ||
		    get(StringRef(
		            encodeFailedLocalityKey(LocalityData::ExcludeLocalityPrefix.toString() + l.first + ":" + l.second)))
		        .present()) {
			return true;
		}
	}

	return false;
}

// Gets the list of already excluded localities (with failed option)
std::set<std::string> DatabaseConfiguration::getExcludedLocalities() const {
	// TODO: revisit all const_cast usages
	const_cast<DatabaseConfiguration*>(this)->makeConfigurationImmutable();
	std::set<std::string> localities;
	for (auto i = lower_bound(rawConfiguration, excludedLocalityKeys.begin);
	     i != rawConfiguration.end() && i->key < excludedLocalityKeys.end;
	     ++i) {
		std::string l = decodeExcludedLocalityKey(i->key);
		localities.insert(l);
	}
	for (auto i = lower_bound(rawConfiguration, failedLocalityKeys.begin);
	     i != rawConfiguration.end() && i->key < failedLocalityKeys.end;
	     ++i) {
		std::string l = decodeFailedLocalityKey(i->key);
		localities.insert(l);
	}
	return localities;
}

void DatabaseConfiguration::makeConfigurationMutable() {
	if (mutableConfiguration.present())
		return;
	mutableConfiguration = std::map<std::string, std::string>();
	auto& mc = mutableConfiguration.get();
	for (auto r = rawConfiguration.begin(); r != rawConfiguration.end(); ++r)
		mc[r->key.toString()] = r->value.toString();
	rawConfiguration = Standalone<VectorRef<KeyValueRef>>();
}

void DatabaseConfiguration::makeConfigurationImmutable() {
	if (!mutableConfiguration.present())
		return;
	auto& mc = mutableConfiguration.get();
	rawConfiguration = Standalone<VectorRef<KeyValueRef>>();
	rawConfiguration.resize(rawConfiguration.arena(), mc.size());
	int i = 0;
	for (auto r = mc.begin(); r != mc.end(); ++r)
		rawConfiguration[i++] = KeyValueRef(rawConfiguration.arena(), KeyValueRef(r->first, r->second));
	mutableConfiguration = Optional<std::map<std::string, std::string>>();
}

void DatabaseConfiguration::fromKeyValues(Standalone<VectorRef<KeyValueRef>> rawConfig) {
	resetInternal();
	this->rawConfiguration = rawConfig;
	for (auto c = rawConfiguration.begin(); c != rawConfiguration.end(); ++c) {
		setInternal(c->key, c->value);
	}
	setDefaultReplicationPolicy();
}

bool DatabaseConfiguration::isOverridden(std::string key) const {
	key = configKeysPrefix.toString() + std::move(key);

	if (mutableConfiguration.present()) {
		return mutableConfiguration.get().find(key) != mutableConfiguration.get().end();
	}

	const int keyLen = key.size();
	for (auto iter = rawConfiguration.begin(); iter != rawConfiguration.end(); ++iter) {
		const auto& rawConfKey = iter->key;
		if (keyLen == rawConfKey.size() &&
		    strncmp(key.c_str(), reinterpret_cast<const char*>(rawConfKey.begin()), keyLen) == 0) {
			return true;
		}
	}

	return false;
}

TEST_CASE("/fdbclient/databaseConfiguration/overwriteCommitProxy") {
	DatabaseConfiguration conf1;
	conf1.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "5"_sr));
	conf1.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/proxies"_sr, "10"_sr));
	conf1.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "-1"_sr));
	conf1.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "-1"_sr));

	DatabaseConfiguration conf2;
	conf2.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/proxies"_sr, "10"_sr));
	conf2.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "-1"_sr));
	conf2.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "-1"_sr));

	ASSERT(conf1 == conf2);
	ASSERT(conf1.getDesiredCommitProxies() == conf2.getDesiredCommitProxies());

	return Void();
}
