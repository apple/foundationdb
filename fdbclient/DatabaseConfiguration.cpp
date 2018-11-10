/*
 * DatabaseConfiguration.cpp
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

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/SystemData.h"

DatabaseConfiguration::DatabaseConfiguration()
{
	resetInternal();
}

void DatabaseConfiguration::resetInternal() {
	// does NOT reset rawConfiguration
	initialized = false;
	masterProxyCount = resolverCount = desiredTLogCount = tLogWriteAntiQuorum = tLogReplicationFactor = storageTeamSize = desiredLogRouterCount = -1;
	tLogDataStoreType = storageServerStoreType = KeyValueStoreType::END;
	autoMasterProxyCount = CLIENT_KNOBS->DEFAULT_AUTO_PROXIES;
	autoResolverCount = CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS;
	autoDesiredTLogCount = CLIENT_KNOBS->DEFAULT_AUTO_LOGS;
	usableRegions = 1;
	regions.clear();
	tLogPolicy = storagePolicy = remoteTLogPolicy = IRepPolicyRef();
	remoteDesiredTLogCount = -1;
	remoteTLogReplicationFactor = repopulateRegionAntiQuorum = 0;
}

void parse( int* i, ValueRef const& v ) {
	// FIXME: Sanity checking
	*i = atoi(v.toString().c_str());
}

void parseReplicationPolicy(IRepPolicyRef* policy, ValueRef const& v) {
	BinaryReader reader(v, IncludeVersion());
	serializeReplicationPolicy(reader, *policy);
}

void parse( std::vector<RegionInfo>* regions, ValueRef const& v ) {
	try {
		StatusObject statusObj = BinaryReader::fromStringRef<StatusObject>(v, IncludeVersion());
		regions->clear();
		if(statusObj["regions"].type() != json_spirit::array_type) {
			return;
		}
		StatusArray regionArray = statusObj["regions"].get_array();
		for (StatusObjectReader dc : regionArray) {
			RegionInfo info;
			json_spirit::mArray datacenters;
			dc.get("datacenters", datacenters);
			bool nonSatelliteDatacenters = 0;
			for (StatusObjectReader s : datacenters) {
				std::string idStr;
				if (s.has("satellite") && s.last().get_int() == 1) {
					SatelliteInfo satInfo;
					s.get("id", idStr);
					satInfo.dcId = idStr;
					s.get("priority", satInfo.priority);
					info.satellites.push_back(satInfo);
				} else {
					if (nonSatelliteDatacenters > 0) throw invalid_option();
					nonSatelliteDatacenters++;
					s.get("id", idStr);
					info.dcId = idStr;
					s.get("priority", info.priority);
				}
			}
			std::sort(info.satellites.begin(), info.satellites.end(), SatelliteInfo::sort_by_priority() );
			if (nonSatelliteDatacenters != 1) throw invalid_option();
			dc.tryGet("satellite_logs", info.satelliteDesiredTLogCount);
			std::string satelliteReplication;
			if(dc.tryGet("satellite_redundancy_mode", satelliteReplication)) {
				if(satelliteReplication == "one_satellite_single") {
					info.satelliteTLogReplicationFactor = 1;
					info.satelliteTLogUsableDcs = 1;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = IRepPolicyRef(new PolicyOne());
				} else if(satelliteReplication == "one_satellite_double") {
					info.satelliteTLogReplicationFactor = 2;
					info.satelliteTLogUsableDcs = 1;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())));
				} else if(satelliteReplication == "one_satellite_triple") {
					info.satelliteTLogReplicationFactor = 3;
					info.satelliteTLogUsableDcs = 1;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = IRepPolicyRef(new PolicyAcross(3, "zoneid", IRepPolicyRef(new PolicyOne())));
				} else if(satelliteReplication == "two_satellite_safe") {
					info.satelliteTLogReplicationFactor = 4;
					info.satelliteTLogUsableDcs = 2;
					info.satelliteTLogWriteAntiQuorum = 0;
					info.satelliteTLogPolicy = IRepPolicyRef(new PolicyAcross(2, "dcid", IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())))));
					info.satelliteTLogReplicationFactorFallback = 2;
					info.satelliteTLogUsableDcsFallback = 1;
					info.satelliteTLogWriteAntiQuorumFallback = 0;
					info.satelliteTLogPolicyFallback = IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())));
				} else if(satelliteReplication == "two_satellite_fast") {
					info.satelliteTLogReplicationFactor = 4;
					info.satelliteTLogUsableDcs = 2;
					info.satelliteTLogWriteAntiQuorum = 2;
					info.satelliteTLogPolicy = IRepPolicyRef(new PolicyAcross(2, "dcid", IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())))));
					info.satelliteTLogReplicationFactorFallback = 2;
					info.satelliteTLogUsableDcsFallback = 1;
					info.satelliteTLogWriteAntiQuorumFallback = 0;
					info.satelliteTLogPolicyFallback = IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())));
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
		std::sort(regions->begin(), regions->end(), RegionInfo::sort_by_priority() );
	} catch( Error &e ) {
		regions->clear();
		return;
	}
}

void DatabaseConfiguration::setDefaultReplicationPolicy() {
	if(!storagePolicy) {
		storagePolicy = IRepPolicyRef(new PolicyAcross(storageTeamSize, "zoneid", IRepPolicyRef(new PolicyOne())));
	}
	if(!tLogPolicy) {
		tLogPolicy = IRepPolicyRef(new PolicyAcross(tLogReplicationFactor, "zoneid", IRepPolicyRef(new PolicyOne())));
	}
	if(remoteTLogReplicationFactor > 0 && !remoteTLogPolicy) {
		remoteTLogPolicy = IRepPolicyRef(new PolicyAcross(remoteTLogReplicationFactor, "zoneid", IRepPolicyRef(new PolicyOne())));
	}
	for(auto& r : regions) {
		if(r.satelliteTLogReplicationFactor > 0 && !r.satelliteTLogPolicy) {
			r.satelliteTLogPolicy = IRepPolicyRef(new PolicyAcross(r.satelliteTLogReplicationFactor, "zoneid", IRepPolicyRef(new PolicyOne())));
		}
		if(r.satelliteTLogReplicationFactorFallback > 0 && !r.satelliteTLogPolicyFallback) {
			r.satelliteTLogPolicyFallback = IRepPolicyRef(new PolicyAcross(r.satelliteTLogReplicationFactorFallback, "zoneid", IRepPolicyRef(new PolicyOne())));
		}
	}
}

bool DatabaseConfiguration::isValid() const {
	if( !(initialized &&
		tLogWriteAntiQuorum >= 0 &&
		tLogReplicationFactor >= 1 &&
		storageTeamSize >= 1 &&
		getDesiredProxies() >= 1 &&
		getDesiredLogs() >= 1 &&
		getDesiredResolvers() >= 1 &&
		tLogDataStoreType != KeyValueStoreType::END &&
		storageServerStoreType != KeyValueStoreType::END &&
		autoMasterProxyCount >= 1 &&
		autoResolverCount >= 1 &&
		autoDesiredTLogCount >= 1 &&
		storagePolicy &&
		tLogPolicy &&
		getDesiredRemoteLogs() >= 1 &&
		remoteTLogReplicationFactor >= 0 &&
		repopulateRegionAntiQuorum >= 0 &&
		repopulateRegionAntiQuorum <= 1 &&
		usableRegions >= 1 &&
		usableRegions <= 2 &&
		regions.size() <= 2 &&
		( usableRegions == 1 || regions.size() == 2 ) &&
		( regions.size() == 0 || regions[0].priority >= 0 ) &&
		( regions.size() == 0 || tLogPolicy->info() != "dcid^2 x zoneid^2 x 1") ) ) { //We cannot specify regions with three_datacenter replication
		return false;
	}

	std::set<Key> dcIds;
	dcIds.insert(Key());
	for(auto& r : regions) {
		if( !(!dcIds.count(r.dcId) &&
			r.satelliteTLogReplicationFactor >= 0 &&
			r.satelliteTLogWriteAntiQuorum >= 0 &&
			r.satelliteTLogUsableDcs >= 1 &&
			( r.satelliteTLogReplicationFactor == 0 || ( r.satelliteTLogPolicy && r.satellites.size() ) ) &&
			( r.satelliteTLogUsableDcsFallback == 0 || ( r.satelliteTLogReplicationFactor > 0 && r.satelliteTLogReplicationFactorFallback > 0 ) ) ) ) {
			return false;
		}
		dcIds.insert(r.dcId);
		for(auto& s : r.satellites) {
			if(dcIds.count(s.dcId)) {
				return false;
			}
			dcIds.insert(s.dcId);
		}
	}

	return true;
}

StatusObject DatabaseConfiguration::toJSON(bool noPolicies) const {
	StatusObject result;

	if( initialized ) {
		std::string tlogInfo = tLogPolicy->info();
		std::string storageInfo = storagePolicy->info();
		bool customRedundancy = false;
		if( tLogWriteAntiQuorum == 0 ) {
			if( tLogReplicationFactor == 1 && storageTeamSize == 1 ) {
				result["redundancy_mode"] = "single";
			} else if( tLogReplicationFactor == 2 && storageTeamSize == 2 ) {
				result["redundancy_mode"] = "double";
			} else if( tLogReplicationFactor == 4 && storageTeamSize == 6 && tlogInfo == "dcid^2 x zoneid^2 x 1" && storageInfo == "dcid^3 x zoneid^2 x 1" ) {
				result["redundancy_mode"] = "three_datacenter";
			} else if( tLogReplicationFactor == 4 && storageTeamSize == 4 && tlogInfo == "dcid^2 x zoneid^2 x 1" && storageInfo == "dcid^2 x zoneid^2 x 1" ) {
				result["redundancy_mode"] = "three_datacenter_fallback";
			} else if( tLogReplicationFactor == 3 && storageTeamSize == 3 ) {
				result["redundancy_mode"] = "triple";
			} else if( tLogReplicationFactor == 4 && storageTeamSize == 3 && tlogInfo == "data_hall^2 x zoneid^2 x 1" && storageInfo == "data_hall^3 x 1" ) {
				result["redundancy_mode"] = "three_data_hall";
			} else {
				customRedundancy = true;
			}
		} else {
			customRedundancy = true;
		}

		if(customRedundancy) {
			result["storage_replicas"] = storageTeamSize;
			result["log_replicas"] = tLogReplicationFactor;
			result["log_anti_quorum"] = tLogWriteAntiQuorum;
			if(!noPolicies) result["storage_replication_policy"] = storagePolicy->info();
			if(!noPolicies)  result["log_replication_policy"] = tLogPolicy->info();
		}

		if( tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V1 && storageServerStoreType == KeyValueStoreType::SSD_BTREE_V1) {
			result["storage_engine"] = "ssd-1";
		} else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 && storageServerStoreType == KeyValueStoreType::SSD_BTREE_V2) {
			result["storage_engine"] = "ssd-2";
		} else if( tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 && storageServerStoreType == KeyValueStoreType::SSD_REDWOOD_V1 ) {
			result["storage_engine"] = "ssd-redwood-experimental";
		} else if( tLogDataStoreType == KeyValueStoreType::MEMORY && storageServerStoreType == KeyValueStoreType::MEMORY ) {
			result["storage_engine"] = "memory";
		} else {
			result["storage_engine"] = "custom";
		}

		if( remoteTLogReplicationFactor == 1 ) {
			result["remote_redundancy_mode"] = "remote_single";
		} else if( remoteTLogReplicationFactor == 2 ) {
			result["remote_redundancy_mode"] = "remote_double";
		} else if( remoteTLogReplicationFactor == 3 ) {
			result["remote_redundancy_mode"] = "remote_triple";
		} else if( remoteTLogReplicationFactor > 3 ) {
			result["remote_log_replicas"] = remoteTLogReplicationFactor;
			if(noPolicies && remoteTLogPolicy) result["remote_log_policy"] = remoteTLogPolicy->info();
		}
		result["usable_regions"] = usableRegions;

		if(regions.size()) {
			StatusArray regionArr;
			for(auto& r : regions) {
				StatusObject regionObj;
				StatusArray dcArr;
				StatusObject dcObj;
				dcObj["id"] = r.dcId.toString();
				dcObj["priority"] = r.priority;
				dcArr.push_back(dcObj);

				if(r.satelliteTLogReplicationFactor == 1 && r.satelliteTLogUsableDcs == 1 && r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 0) {
					regionObj["satellite_redundancy_mode"] = "one_satellite_single";
				} else if(r.satelliteTLogReplicationFactor == 2 && r.satelliteTLogUsableDcs == 1 && r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 0) {
					regionObj["satellite_redundancy_mode"] = "one_satellite_double";
				} else if(r.satelliteTLogReplicationFactor == 3 && r.satelliteTLogUsableDcs == 1 && r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 0) {
					regionObj["satellite_redundancy_mode"] = "one_satellite_triple";
				} else if(r.satelliteTLogReplicationFactor == 4 && r.satelliteTLogUsableDcs == 2 && r.satelliteTLogWriteAntiQuorum == 0 && r.satelliteTLogUsableDcsFallback == 1 && r.satelliteTLogReplicationFactorFallback == 2 && r.satelliteTLogWriteAntiQuorumFallback == 0) {
					regionObj["satellite_redundancy_mode"] = "two_satellite_safe";
				} else if(r.satelliteTLogReplicationFactor == 4 && r.satelliteTLogUsableDcs == 2 && r.satelliteTLogWriteAntiQuorum == 2 && r.satelliteTLogUsableDcsFallback == 1 && r.satelliteTLogReplicationFactorFallback == 2 && r.satelliteTLogWriteAntiQuorumFallback == 0) {
					regionObj["satellite_redundancy_mode"] = "two_satellite_fast";
				} else if(r.satelliteTLogReplicationFactor != 0) {
					regionObj["satellite_log_replicas"] = r.satelliteTLogReplicationFactor;
					regionObj["satellite_usable_dcs"] = r.satelliteTLogUsableDcs;
					regionObj["satellite_anti_quorum"] = r.satelliteTLogWriteAntiQuorum;
					if(r.satelliteTLogPolicy) regionObj["satellite_log_policy"] = r.satelliteTLogPolicy->info();
					regionObj["satellite_log_replicas_fallback"] = r.satelliteTLogReplicationFactorFallback;
					regionObj["satellite_usable_dcs_fallback"] = r.satelliteTLogUsableDcsFallback;
					regionObj["satellite_anti_quorum_fallback"] = r.satelliteTLogWriteAntiQuorumFallback;
					if(r.satelliteTLogPolicyFallback) regionObj["satellite_log_policy_fallback"] = r.satelliteTLogPolicyFallback->info();
				}

				if( r.satelliteDesiredTLogCount != -1 ) {
					regionObj["satellite_logs"] = r.satelliteDesiredTLogCount;
				}

				if(r.satellites.size()) {
					for(auto& s : r.satellites) {
						StatusObject satObj;
						satObj["id"] = s.dcId.toString();
						satObj["priority"] = s.priority;
						satObj["satellite"] = 1;

						dcArr.push_back(satObj);
					}
				}

				regionObj["datacenters"] = dcArr;
				regionArr.push_back(regionObj);
			}
			result["regions"] = regionArr;
		}

		if( desiredTLogCount != -1 ) {
			result["logs"] = desiredTLogCount;
		}
		if( masterProxyCount != -1 ) {
			result["proxies"] = masterProxyCount;
		}
		if( resolverCount != -1 ) {
			result["resolvers"] = resolverCount;
		}
		if( desiredLogRouterCount != -1 ) {
			result["log_routers"] = desiredLogRouterCount;
		}
		if( remoteDesiredTLogCount != -1 ) {
			result["remote_logs"] = remoteDesiredTLogCount;
		}
		if( repopulateRegionAntiQuorum != 0 ) {
			result["repopulate_anti_quorum"] = repopulateRegionAntiQuorum;
		}
		if( autoMasterProxyCount != CLIENT_KNOBS->DEFAULT_AUTO_PROXIES ) {
			result["auto_proxies"] = autoMasterProxyCount;
		}
		if (autoResolverCount != CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS) {
			result["auto_resolvers"] = autoResolverCount;
		}
		if (autoDesiredTLogCount != CLIENT_KNOBS->DEFAULT_AUTO_LOGS) {
			result["auto_logs"] = autoDesiredTLogCount;
		}
	}

	return result;
}

std::string DatabaseConfiguration::toString() const {
	return json_spirit::write_string(json_spirit::mValue(toJSON()), json_spirit::Output_options::none);
}

bool DatabaseConfiguration::setInternal(KeyRef key, ValueRef value) {
	KeyRef ck = key.removePrefix( configKeysPrefix );
	int type;

	if (ck == LiteralStringRef("initialized")) initialized = true;
	else if (ck == LiteralStringRef("proxies")) parse(&masterProxyCount, value);
	else if (ck == LiteralStringRef("resolvers")) parse(&resolverCount, value);
	else if (ck == LiteralStringRef("logs")) parse(&desiredTLogCount, value);
	else if (ck == LiteralStringRef("log_replicas")) parse(&tLogReplicationFactor, value);
	else if (ck == LiteralStringRef("log_anti_quorum")) parse(&tLogWriteAntiQuorum, value);
	else if (ck == LiteralStringRef("storage_replicas")) parse(&storageTeamSize, value);
	else if (ck == LiteralStringRef("log_engine")) { parse((&type), value); tLogDataStoreType = (KeyValueStoreType::StoreType)type; 
		// TODO:  Remove this once Redwood works as a log engine
		if(tLogDataStoreType == KeyValueStoreType::SSD_REDWOOD_V1)
			tLogDataStoreType = KeyValueStoreType::SSD_BTREE_V2;
	}
	else if (ck == LiteralStringRef("storage_engine")) { parse((&type), value); storageServerStoreType = (KeyValueStoreType::StoreType)type; }
	else if (ck == LiteralStringRef("auto_proxies")) parse(&autoMasterProxyCount, value);
	else if (ck == LiteralStringRef("auto_resolvers")) parse(&autoResolverCount, value);
	else if (ck == LiteralStringRef("auto_logs")) parse(&autoDesiredTLogCount, value);
	else if (ck == LiteralStringRef("storage_replication_policy")) parseReplicationPolicy(&storagePolicy, value);
	else if (ck == LiteralStringRef("log_replication_policy")) parseReplicationPolicy(&tLogPolicy, value);
	else if (ck == LiteralStringRef("log_routers")) parse(&desiredLogRouterCount, value);
	else if (ck == LiteralStringRef("remote_logs")) parse(&remoteDesiredTLogCount, value);
	else if (ck == LiteralStringRef("remote_log_replicas")) parse(&remoteTLogReplicationFactor, value);
	else if (ck == LiteralStringRef("remote_log_policy")) parseReplicationPolicy(&remoteTLogPolicy, value);
	else if (ck == LiteralStringRef("usable_regions")) parse(&usableRegions, value);
	else if (ck == LiteralStringRef("repopulate_anti_quorum")) parse(&repopulateRegionAntiQuorum, value);
	else if (ck == LiteralStringRef("regions")) parse(&regions, value);
	else return false;
	return true;  // All of the above options currently require recovery to take effect
}

inline static KeyValueRef * lower_bound( VectorRef<KeyValueRef> & config, KeyRef const& key ) {
	return std::lower_bound( config.begin(), config.end(), KeyValueRef(key, ValueRef()), KeyValueRef::OrderByKey() );
}
inline static KeyValueRef const* lower_bound( VectorRef<KeyValueRef> const& config, KeyRef const& key ) {
	return lower_bound( const_cast<VectorRef<KeyValueRef> &>(config), key );
}

void DatabaseConfiguration::applyMutation( MutationRef m ) {
	if( m.type == MutationRef::SetValue && m.param1.startsWith(configKeysPrefix) ) {
		set(m.param1, m.param2);
	} else if( m.type == MutationRef::ClearRange ) {
		KeyRangeRef range(m.param1, m.param2);
		if( range.intersects( configKeys ) ) {
			clear(range & configKeys);
		}
	}
}

bool DatabaseConfiguration::set(KeyRef key, ValueRef value) {
	makeConfigurationMutable();
	mutableConfiguration.get()[ key.toString() ] = value.toString();
	return setInternal(key,value);
}

bool DatabaseConfiguration::clear( KeyRangeRef keys ) {
	makeConfigurationMutable();
	auto& mc = mutableConfiguration.get();
	mc.erase( mc.lower_bound( keys.begin.toString() ), mc.lower_bound( keys.end.toString() ) );

	// FIXME: More efficient
	bool wasValid = isValid();
	resetInternal();
	for(auto c = mc.begin(); c != mc.end(); ++c)
		setInternal(c->first, c->second);
	return wasValid && !isValid();
}

Optional<ValueRef> DatabaseConfiguration::get( KeyRef key ) const {
	if (mutableConfiguration.present()) {
		auto i = mutableConfiguration.get().find(key.toString());
		if (i == mutableConfiguration.get().end()) return Optional<ValueRef>();
		return ValueRef(i->second);
	} else {
		auto i = lower_bound(rawConfiguration, key);
		if (i == rawConfiguration.end() || i->key != key) return Optional<ValueRef>();
		return i->value;
	}
}

bool DatabaseConfiguration::isExcludedServer( NetworkAddress a ) const {
	return get( encodeExcludedServersKey( AddressExclusion(a.ip, a.port) ) ).present() ||
		get( encodeExcludedServersKey( AddressExclusion(a.ip) ) ).present();
}
std::set<AddressExclusion> DatabaseConfiguration::getExcludedServers() const {
	const_cast<DatabaseConfiguration*>(this)->makeConfigurationImmutable();
	std::set<AddressExclusion> addrs;
	for( auto i = lower_bound(rawConfiguration, excludedServersKeys.begin); i != rawConfiguration.end() && i->key < excludedServersKeys.end; ++i ) {
		AddressExclusion a = decodeExcludedServersKey( i->key );
		if (a.isValid()) addrs.insert(a);
	}
	return addrs;
}

void DatabaseConfiguration::makeConfigurationMutable() {
	if (mutableConfiguration.present()) return;
	mutableConfiguration = std::map<std::string,std::string>();
	auto& mc = mutableConfiguration.get();
	for(auto r = rawConfiguration.begin(); r != rawConfiguration.end(); ++r)
		mc[ r->key.toString() ] = r->value.toString();
	rawConfiguration = Standalone<VectorRef<KeyValueRef>>();
}

void DatabaseConfiguration::makeConfigurationImmutable() {
	if (!mutableConfiguration.present()) return;
	auto & mc = mutableConfiguration.get();
	rawConfiguration = Standalone<VectorRef<KeyValueRef>>();
	rawConfiguration.resize( rawConfiguration.arena(), mc.size() );
	int i = 0;
	for(auto r = mc.begin(); r != mc.end(); ++r)
		rawConfiguration[i++] = KeyValueRef( rawConfiguration.arena(), KeyValueRef( r->first, r->second ) );
	mutableConfiguration = Optional<std::map<std::string,std::string>>();
}
