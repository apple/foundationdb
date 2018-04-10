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

#include "DatabaseConfiguration.h"
#include "fdbclient/SystemData.h"

DatabaseConfiguration::DatabaseConfiguration()
{
	resetInternal();
}

void DatabaseConfiguration::resetInternal() {
	// does NOT reset rawConfiguration
	initialized = false;
	masterProxyCount = resolverCount = desiredTLogCount = tLogWriteAntiQuorum = tLogReplicationFactor = durableStorageQuorum = storageTeamSize = -1;
	tLogDataStoreType = storageServerStoreType = KeyValueStoreType::END;
	autoMasterProxyCount = CLIENT_KNOBS->DEFAULT_AUTO_PROXIES;
	autoResolverCount = CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS;
	autoDesiredTLogCount = CLIENT_KNOBS->DEFAULT_AUTO_LOGS;
	storagePolicy = IRepPolicyRef();
	tLogPolicy = IRepPolicyRef();
}

void parse( int* i, ValueRef const& v ) {
	// FIXME: Sanity checking
	*i = atoi(v.toString().c_str());
}

void parseReplicationPolicy(IRepPolicyRef* policy, ValueRef const& v) {
	BinaryReader reader(v, IncludeVersion());
	serializeReplicationPolicy(reader, *policy);
}

void DatabaseConfiguration::setDefaultReplicationPolicy() {
	storagePolicy = IRepPolicyRef(new PolicyAcross(storageTeamSize, "zoneid", IRepPolicyRef(new PolicyOne())));
	tLogPolicy = IRepPolicyRef(new PolicyAcross(tLogReplicationFactor, "zoneid", IRepPolicyRef(new PolicyOne())));
}

bool DatabaseConfiguration::isValid() const {
	return initialized &&
		tLogWriteAntiQuorum >= 0 &&
		tLogReplicationFactor >= 1 &&
		durableStorageQuorum >= 1 &&
		storageTeamSize >= 1 &&
		getDesiredProxies() >= 1 &&
		getDesiredLogs() >= 1 &&
		getDesiredResolvers() >= 1 &&
		durableStorageQuorum <= storageTeamSize &&
		tLogDataStoreType != KeyValueStoreType::END &&
		storageServerStoreType != KeyValueStoreType::END &&
		autoMasterProxyCount >= 1 &&
		autoResolverCount >= 1 &&
		autoDesiredTLogCount >= 1 &&
		storagePolicy &&
		tLogPolicy
		;
}

std::map<std::string, std::string> DatabaseConfiguration::toMap() const {
	std::map<std::string, std::string> result;

	if( initialized ) {
		std::string tlogInfo = tLogPolicy->info();
		std::string storageInfo = storagePolicy->info();
		if( durableStorageQuorum == storageTeamSize &&
			tLogWriteAntiQuorum == 0 ) {
			if( tLogReplicationFactor == 1 && durableStorageQuorum == 1 )
				result["redundancy_mode"] = "single";
			else if( tLogReplicationFactor == 2 && durableStorageQuorum == 2 )
				result["redundancy_mode"] = "double";
			else if( tLogReplicationFactor == 4 && durableStorageQuorum == 6 && tlogInfo == "dcid^2 x zoneid^2 x 1" && storageInfo == "dcid^3 x zoneid^2 x 1" )
				result["redundancy_mode"] = "three_datacenter";
			else if( tLogReplicationFactor == 3 && durableStorageQuorum == 3 )
				result["redundancy_mode"] = "triple";
			else if( tLogReplicationFactor == 4 && durableStorageQuorum == 3 && tlogInfo == "data_hall^2 x zoneid^2 x 1" && storageInfo == "data_hall^3 x 1" )
				result["redundancy_mode"] = "three_data_hall";
			else
				result["redundancy_mode"] = "custom";
		} else
			result["redundancy_mode"] = "custom";

		if( tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V1 && storageServerStoreType == KeyValueStoreType::SSD_BTREE_V1)
			result["storage_engine"] = "ssd-1";
		else if (tLogDataStoreType == KeyValueStoreType::SSD_BTREE_V2 && storageServerStoreType == KeyValueStoreType::SSD_BTREE_V2)
			result["storage_engine"] = "ssd-2";
		else if( tLogDataStoreType == KeyValueStoreType::MEMORY && storageServerStoreType == KeyValueStoreType::MEMORY )
			result["storage_engine"] = "memory";
		else
			result["storage_engine"] = "custom";

		if( desiredTLogCount != -1 )
			result["logs"] = format("%d", desiredTLogCount);

		if( masterProxyCount != -1 )
			result["proxies"] = format("%d", masterProxyCount);

		if( resolverCount != -1 )
			result["resolvers"] = format("%d", resolverCount);
	}

	return result;
}

std::string DatabaseConfiguration::toString() const {
	std::string result;
	std::map<std::string, std::string> config = toMap();

	for(auto itr : config) {
		result += itr.first + "=" + itr.second;
		result += ";";
	}

	return result.substr(0, result.length()-1);
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
	else if (ck == LiteralStringRef("storage_quorum")) parse(&durableStorageQuorum, value);
	else if (ck == LiteralStringRef("storage_replicas")) parse(&storageTeamSize, value);
	else if (ck == LiteralStringRef("log_engine")) { parse((&type), value); tLogDataStoreType = (KeyValueStoreType::StoreType)type; }
	else if (ck == LiteralStringRef("storage_engine")) { parse((&type), value); storageServerStoreType = (KeyValueStoreType::StoreType)type; }
	else if (ck == LiteralStringRef("auto_proxies")) parse(&autoMasterProxyCount, value);
	else if (ck == LiteralStringRef("auto_resolvers")) parse(&autoResolverCount, value);
	else if (ck == LiteralStringRef("auto_logs")) parse(&autoDesiredTLogCount, value);
	else if (ck == LiteralStringRef("storage_replication_policy")) parseReplicationPolicy(&storagePolicy, value);
	else if (ck == LiteralStringRef("log_replication_policy")) parseReplicationPolicy(&tLogPolicy, value);
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
