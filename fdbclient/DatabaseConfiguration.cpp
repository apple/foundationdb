/*
 * DatabaseConfiguration.cpp
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

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/SystemData.h"
#include "flow/ITrace.h"
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
	blobGranulesEnabled = false;
	tenantMode = TenantMode::DISABLED;
}

int toInt(ValueRef const& v) {
	return atoi(v.toString().c_str());
}

void parse(int* i, ValueRef const& v) {
	// FIXME: Sanity checking
	*i = atoi(v.toString().c_str());
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
	if (!(initialized && tLogWriteAntiQuorum >= 0 && tLogWriteAntiQuorum <= tLogReplicationFactor / 2 &&
	      tLogReplicationFactor >= 1 && storageTeamSize >= 1 && getDesiredCommitProxies() >= 1 &&
	      getDesiredGrvProxies() >= 1 && getDesiredLogs() >= 1 && getDesiredResolvers() >= 1 &&
	      tLogVersion != TLogVersion::UNSET && tLogVersion >= TLogVersion::MIN_RECRUITABLE &&
	      tLogVersion <= TLogVersion::MAX_SUPPORTED && tLogDataStoreType != KeyValueStoreType::END &&
	      tLogSpillType != TLogSpillType::UNSET &&
	      !(tLogSpillType == TLogSpillType::REFERENCE && tLogVersion < TLogVersion::V3) &&
	      storageServerStoreType != KeyValueStoreType::END && autoCommitProxyCount >= 1 && autoGrvProxyCount >= 1 &&
	      autoResolverCount >= 1 && autoDesiredTLogCount >= 1 && storagePolicy && tLogPolicy &&
	      getDesiredRemoteLogs() >= 1 && remoteTLogReplicationFactor >= 0 && repopulateRegionAntiQuorum >= 0 &&
	      repopulateRegionAntiQuorum <= 1 && usableRegions >= 1 && usableRegions <= 2 && regions.size() <= 2 &&
	      (usableRegions == 1 || regions.size() == 2) && (regions.size() == 0 || regions[0].priority >= 0) &&
	      (regions.size() == 0 || tLogPolicy->info() != "dcid^2 x zoneid^2 x 1") &&
	      // We cannot specify regions with three_datacenter replication
	      (perpetualStorageWiggleSpeed == 0 || perpetualStorageWiggleSpeed == 1) &&
	      isValidPerpetualStorageWiggleLocality(perpetualStorageWiggleLocality) &&
	      storageMigrationType != StorageMigrationType::UNSET && tenantMode >= TenantMode::DISABLED &&
	      tenantMode < TenantMode::END)) {
		return false;
	}
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

	if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V1 &&
	    storageServerStoreType == KeyValueStoreType::SSD_BTREE_V1) {
		result["storage_engine"] = "ssd-1";
	} else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 &&
	           storageServerStoreType == KeyValueStoreType::SSD_BTREE_V2) {
		result["storage_engine"] = "ssd-2";
	} else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 &&
	           storageServerStoreType == KeyValueStoreType::SSD_REDWOOD_V1) {
		result["storage_engine"] = "ssd-redwood-1-experimental";
	} else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 &&
	           storageServerStoreType == KeyValueStoreType::SSD_ROCKSDB_V1) {
		result["storage_engine"] = "ssd-rocksdb-v1";
	} else if (tLogDataStoreType == KeyValueStoreType::MEMORY && storageServerStoreType == KeyValueStoreType::MEMORY) {
		result["storage_engine"] = "memory-1";
	} else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 &&
	           storageServerStoreType == KeyValueStoreType::MEMORY_RADIXTREE) {
		result["storage_engine"] = "memory-radixtree-beta";
	} else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 &&
	           storageServerStoreType == KeyValueStoreType::MEMORY) {
		result["storage_engine"] = "memory-2";
	} else {
		result["storage_engine"] = "custom";
	}

	if (desiredTSSCount > 0) {
		result["tss_count"] = desiredTSSCount;
		if (testingStorageServerStoreType == KeyValueStoreType::SSD_BTREE_V1) {
			result["tss_storage_engine"] = "ssd-1";
		} else if (testingStorageServerStoreType == KeyValueStoreType::SSD_BTREE_V2) {
			result["tss_storage_engine"] = "ssd-2";
		} else if (testingStorageServerStoreType == KeyValueStoreType::SSD_REDWOOD_V1) {
			result["tss_storage_engine"] = "ssd-redwood-1-experimental";
		} else if (testingStorageServerStoreType == KeyValueStoreType::SSD_ROCKSDB_V1) {
			result["tss_storage_engine"] = "ssd-rocksdb-v1";
		} else if (testingStorageServerStoreType == KeyValueStoreType::MEMORY_RADIXTREE) {
			result["tss_storage_engine"] = "memory-radixtree-beta";
		} else if (testingStorageServerStoreType == KeyValueStoreType::MEMORY) {
			result["tss_storage_engine"] = "memory-2";
		} else {
			result["tss_storage_engine"] = "custom";
		}
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
	result["storage_migration_type"] = storageMigrationType.toString();
	result["blob_granules_enabled"] = (int32_t)blobGranulesEnabled;
	result["tenant_mode"] = tenantMode.toString();
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

	if (ck == LiteralStringRef("initialized")) {
		initialized = true;
	} else if (ck == LiteralStringRef("commit_proxies")) {
		commitProxyCount = toInt(value);
		if (commitProxyCount == -1)
			overwriteProxiesCount();
	} else if (ck == LiteralStringRef("grv_proxies")) {
		grvProxyCount = toInt(value);
		if (grvProxyCount == -1)
			overwriteProxiesCount();
	} else if (ck == LiteralStringRef("resolvers")) {
		parse(&resolverCount, value);
	} else if (ck == LiteralStringRef("logs")) {
		parse(&desiredTLogCount, value);
	} else if (ck == LiteralStringRef("log_replicas")) {
		parse(&tLogReplicationFactor, value);
		tLogWriteAntiQuorum = std::min(tLogWriteAntiQuorum, tLogReplicationFactor / 2);
	} else if (ck == LiteralStringRef("log_anti_quorum")) {
		parse(&tLogWriteAntiQuorum, value);
		if (tLogReplicationFactor > 0) {
			tLogWriteAntiQuorum = std::min(tLogWriteAntiQuorum, tLogReplicationFactor / 2);
		}
	} else if (ck == LiteralStringRef("storage_replicas")) {
		parse(&storageTeamSize, value);
	} else if (ck == LiteralStringRef("tss_count")) {
		parse(&desiredTSSCount, value);
	} else if (ck == LiteralStringRef("log_version")) {
		parse((&type), value);
		type = std::max((int)TLogVersion::MIN_RECRUITABLE, type);
		type = std::min((int)TLogVersion::MAX_SUPPORTED, type);
		tLogVersion = (TLogVersion::Version)type;
	} else if (ck == LiteralStringRef("log_engine")) {
		parse((&type), value);
		tLogDataStoreType = (KeyValueStoreType::StoreType)type;
		// TODO:  Remove this once Redwood works as a log engine
		if (tLogDataStoreType == KeyValueStoreType::SSD_REDWOOD_V1) {
			tLogDataStoreType = KeyValueStoreType::SSD_BTREE_V2;
		}
		// TODO:  Remove this once memroy radix tree works as a log engine
		if (tLogDataStoreType == KeyValueStoreType::MEMORY_RADIXTREE) {
			tLogDataStoreType = KeyValueStoreType::SSD_BTREE_V2;
		}
	} else if (ck == LiteralStringRef("log_spill")) {
		parse((&type), value);
		tLogSpillType = (TLogSpillType::SpillType)type;
	} else if (ck == LiteralStringRef("storage_engine")) {
		parse((&type), value);
		storageServerStoreType = (KeyValueStoreType::StoreType)type;
	} else if (ck == LiteralStringRef("tss_storage_engine")) {
		parse((&type), value);
		testingStorageServerStoreType = (KeyValueStoreType::StoreType)type;
	} else if (ck == LiteralStringRef("auto_commit_proxies")) {
		parse(&autoCommitProxyCount, value);
	} else if (ck == LiteralStringRef("auto_grv_proxies")) {
		parse(&autoGrvProxyCount, value);
	} else if (ck == LiteralStringRef("auto_resolvers")) {
		parse(&autoResolverCount, value);
	} else if (ck == LiteralStringRef("auto_logs")) {
		parse(&autoDesiredTLogCount, value);
	} else if (ck == LiteralStringRef("storage_replication_policy")) {
		parseReplicationPolicy(&storagePolicy, value);
	} else if (ck == LiteralStringRef("log_replication_policy")) {
		parseReplicationPolicy(&tLogPolicy, value);
	} else if (ck == LiteralStringRef("log_routers")) {
		parse(&desiredLogRouterCount, value);
	} else if (ck == LiteralStringRef("remote_logs")) {
		parse(&remoteDesiredTLogCount, value);
	} else if (ck == LiteralStringRef("remote_log_replicas")) {
		parse(&remoteTLogReplicationFactor, value);
	} else if (ck == LiteralStringRef("remote_log_policy")) {
		parseReplicationPolicy(&remoteTLogPolicy, value);
	} else if (ck == LiteralStringRef("backup_worker_enabled")) {
		parse((&type), value);
		backupWorkerEnabled = (type != 0);
	} else if (ck == LiteralStringRef("usable_regions")) {
		parse(&usableRegions, value);
	} else if (ck == LiteralStringRef("repopulate_anti_quorum")) {
		parse(&repopulateRegionAntiQuorum, value);
	} else if (ck == LiteralStringRef("regions")) {
		parse(&regions, value);
	} else if (ck == LiteralStringRef("perpetual_storage_wiggle")) {
		parse(&perpetualStorageWiggleSpeed, value);
	} else if (ck == LiteralStringRef("perpetual_storage_wiggle_locality")) {
		if (!isValidPerpetualStorageWiggleLocality(value.toString())) {
			return false;
		}
		perpetualStorageWiggleLocality = value.toString();
	} else if (ck == LiteralStringRef("storage_migration_type")) {
		parse((&type), value);
		storageMigrationType = (StorageMigrationType::MigrationType)type;
	} else if (ck == LiteralStringRef("tenant_mode")) {
		parse((&type), value);
		tenantMode = (TenantMode::Mode)type;
	} else if (ck == LiteralStringRef("proxies")) {
		overwriteProxiesCount();
	} else if (ck == LiteralStringRef("blob_granules_enabled")) {
		parse((&type), value);
		blobGranulesEnabled = (type != 0);
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

bool DatabaseConfiguration::isExcludedServer(NetworkAddressList a) const {
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
	         get(encodeFailedServersKey(AddressExclusion(a.secondaryAddress.get().ip))).present()));
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

// checks if this machineid of given locality is excluded.
bool DatabaseConfiguration::isMachineExcluded(const LocalityData& locality) const {
	if (locality.machineId().present()) {
		return get(encodeExcludedLocalityKey(LocalityData::ExcludeLocalityKeyMachineIdPrefix.toString() +
		                                     locality.machineId().get().toString()))
		           .present() ||
		       get(encodeFailedLocalityKey(LocalityData::ExcludeLocalityKeyMachineIdPrefix.toString() +
		                                   locality.machineId().get().toString()))
		           .present();
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