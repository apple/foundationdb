/*
 * SystemData.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/Arena.h"
#include "flow/TDMetric.actor.h"
#include "flow/serialize.h"

const KeyRef systemKeysPrefix = LiteralStringRef("\xff");
const KeyRangeRef normalKeys(KeyRef(), systemKeysPrefix);
const KeyRangeRef systemKeys(systemKeysPrefix, LiteralStringRef("\xff\xff") );
const KeyRangeRef nonMetadataSystemKeys(LiteralStringRef("\xff\x02"), LiteralStringRef("\xff\x03"));
const KeyRangeRef allKeys = KeyRangeRef(normalKeys.begin, systemKeys.end);
const KeyRef afterAllKeys = LiteralStringRef("\xff\xff\x00");
const KeyRangeRef specialKeys = KeyRangeRef(LiteralStringRef("\xff\xff"), LiteralStringRef("\xff\xff\xff"));

// keyServersKeys.contains(k) iff k.startsWith(keyServersPrefix)
const KeyRangeRef keyServersKeys( LiteralStringRef("\xff/keyServers/"), LiteralStringRef("\xff/keyServers0") );
const KeyRef keyServersPrefix = keyServersKeys.begin;
const KeyRef keyServersEnd = keyServersKeys.end;
const KeyRangeRef keyServersKeyServersKeys ( LiteralStringRef("\xff/keyServers/\xff/keyServers/"), LiteralStringRef("\xff/keyServers/\xff/keyServers0"));
const KeyRef keyServersKeyServersKey = keyServersKeyServersKeys.begin;

const Key keyServersKey( const KeyRef& k ) {
	return k.withPrefix( keyServersPrefix );
}
const KeyRef keyServersKey( const KeyRef& k, Arena& arena ) {
	return k.withPrefix( keyServersPrefix, arena );
}
const Value keyServersValue( Standalone<RangeResultRef> result, const std::vector<UID>& src, const std::vector<UID>& dest ) {
	if(!CLIENT_KNOBS->TAG_ENCODE_KEY_SERVERS) {
		BinaryWriter wr(IncludeVersion(ProtocolVersion::withKeyServerValue())); wr << src << dest;
		return wr.toValue();
	}
	
	std::vector<Tag> srcTag;
	std::vector<Tag> destTag;

	bool foundOldLocality = false;
	for (const KeyValueRef kv : result) {
		UID uid = decodeServerTagKey(kv.key);
		if (std::find(src.begin(), src.end(), uid) != src.end()) {
			srcTag.push_back( decodeServerTagValue(kv.value) );
			if(srcTag.back().locality == tagLocalityUpgraded) {
				foundOldLocality = true;
				break;
			}
		}
		if (std::find(dest.begin(), dest.end(), uid) != dest.end()) {
			destTag.push_back( decodeServerTagValue(kv.value) );
			if(destTag.back().locality == tagLocalityUpgraded) {
				foundOldLocality = true;
				break;
			}
		}
	}

	if(foundOldLocality || src.size() != srcTag.size() || dest.size() != destTag.size()) {
		ASSERT_WE_THINK(foundOldLocality);
		BinaryWriter wr(IncludeVersion(ProtocolVersion::withKeyServerValue())); wr << src << dest;
		return wr.toValue();
	}

	return keyServersValue(srcTag, destTag);
}
const Value keyServersValue( const std::vector<Tag>& srcTag, const std::vector<Tag>& destTag ) {
	// src and dest are expected to be sorted
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withKeyServerValueV2())); wr << srcTag << destTag;
	return wr.toValue();
}

void decodeKeyServersValue( Standalone<RangeResultRef> result, const ValueRef& value,
                            std::vector<UID>& src, std::vector<UID>& dest, bool missingIsError ) {
	if (value.size() == 0) {
		src.clear();
		dest.clear();
		return;
	}

	BinaryReader rd(value, IncludeVersion());
	if(!rd.protocolVersion().hasKeyServerValueV2()) {
		rd >> src >> dest;
		return;
	}
	
	std::vector<Tag> srcTag, destTag;
	rd >> srcTag >> destTag;

	src.clear();
	dest.clear();

	for (const KeyValueRef kv : result) {
		Tag tag = decodeServerTagValue(kv.value);
		if (std::find(srcTag.begin(), srcTag.end(), tag) != srcTag.end()) {
			src.push_back( decodeServerTagKey(kv.key) );
		}
		if (std::find(destTag.begin(), destTag.end(), tag) != destTag.end()) {
			dest.push_back( decodeServerTagKey(kv.key) );
		}
	}
	std::sort(src.begin(), src.end());
	std::sort(dest.begin(), dest.end());
	if(missingIsError && (src.size() != srcTag.size() || dest.size() != destTag.size())) {
		TraceEvent(SevError, "AttemptedToDecodeMissingTag");
		for (const KeyValueRef kv : result) {
			Tag tag = decodeServerTagValue(kv.value);
			UID serverID = decodeServerTagKey(kv.key);
			TraceEvent("TagUIDMap").detail("Tag", tag.toString()).detail("UID", serverID.toString());
		}
		for(auto& it : srcTag) {
			TraceEvent("SrcTag").detail("Tag", it.toString());
		}
		for(auto& it : destTag) {
			TraceEvent("DestTag").detail("Tag", it.toString());
		}
		ASSERT(false);
	}
}

void decodeKeyServersValue( std::map<Tag, UID> const& tag_uid, const ValueRef& value,
                            std::vector<UID>& src, std::vector<UID>& dest ) {
	static std::vector<Tag> srcTag, destTag;
	src.clear();
	dest.clear();
	if (value.size() == 0) {
		return;
	}

	BinaryReader rd(value, IncludeVersion());
	rd.checkpoint();
	int srcLen, destLen;
	rd >> srcLen;
	rd.readBytes(srcLen * sizeof(Tag));
	rd >> destLen;
	rd.rewind();

	if (value.size() != sizeof(ProtocolVersion) + sizeof(int) + srcLen * sizeof(Tag) + sizeof(int) + destLen * sizeof(Tag)) {
		rd >> src >> dest;
		rd.assertEnd();
		return;
	}

	srcTag.clear();
	destTag.clear();
	rd >> srcTag >> destTag;

	for(auto t : srcTag) {
		auto itr = tag_uid.find(t);
		if(itr != tag_uid.end()) {
			src.push_back(itr->second);
		} else {
			TraceEvent(SevError, "AttemptedToDecodeMissingSrcTag").detail("Tag", t.toString());
			ASSERT(false);
		}
	}

	for(auto t : destTag) {
		auto itr = tag_uid.find(t);
		if(itr != tag_uid.end()) {
			dest.push_back(itr->second);
		} else {
			TraceEvent(SevError, "AttemptedToDecodeMissingDestTag").detail("Tag", t.toString());
			ASSERT(false);
		}
	}

	std::sort(src.begin(), src.end());
	std::sort(dest.begin(), dest.end());
}

const KeyRangeRef conflictingKeysRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/conflicting_keys/"),
                LiteralStringRef("\xff\xff/transaction/conflicting_keys/\xff\xff"));
const ValueRef conflictingKeysTrue = LiteralStringRef("1");
const ValueRef conflictingKeysFalse = LiteralStringRef("0");

const KeyRangeRef readConflictRangeKeysRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/read_conflict_range/"),
                LiteralStringRef("\xff\xff/transaction/read_conflict_range/\xff\xff"));

const KeyRangeRef writeConflictRangeKeysRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/write_conflict_range/"),
                LiteralStringRef("\xff\xff/transaction/write_conflict_range/\xff\xff"));

// "\xff/cacheServer/[[UID]] := StorageServerInterface"
// This will be added by the cache server on initialization and removed by DD
// TODO[mpilman]: We will need a way to map uint16_t ids to UIDs in a future
//                versions. For now caches simply cache everything so the ids
//                are not yet meaningful.
const KeyRangeRef storageCacheServerKeys(LiteralStringRef("\xff/cacheServer/"),
                                         LiteralStringRef("\xff/cacheServer0"));
const KeyRef storageCacheServersPrefix = storageCacheServerKeys.begin;
const KeyRef storageCacheServersEnd = storageCacheServerKeys.end;

const Key storageCacheServerKey(UID id) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(storageCacheServersPrefix);
	wr << id;
	return wr.toValue();
}

const Value storageCacheServerValue(const StorageServerInterface& ssi) {
	BinaryWriter wr(IncludeVersion());
	wr << ssi;
	return wr.toValue();
}

const KeyRangeRef ddStatsRange = KeyRangeRef(LiteralStringRef("\xff\xff/metrics/data_distribution_stats/"),
                                             LiteralStringRef("\xff\xff/metrics/data_distribution_stats/\xff\xff"));

//    "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
const KeyRangeRef storageCacheKeys( LiteralStringRef("\xff/storageCache/"), LiteralStringRef("\xff/storageCache0") );
const KeyRef storageCachePrefix = storageCacheKeys.begin;

const Key storageCacheKey( const KeyRef& k ) {
	return k.withPrefix( storageCachePrefix );
}

const Value storageCacheValue( const vector<uint16_t>& serverIndices ) {
	BinaryWriter wr((IncludeVersion(ProtocolVersion::withStorageCacheValue()))); 
	wr << serverIndices;
	return wr.toValue();
}

void decodeStorageCacheValue( const ValueRef& value, vector<uint16_t>& serverIndices ) {
	serverIndices.clear();
	if (value.size()) {
		BinaryReader rd(value, IncludeVersion());
		rd >> serverIndices;
	}
}

const Value logsValue( const vector<std::pair<UID, NetworkAddress>>& logs, const vector<std::pair<UID, NetworkAddress>>& oldLogs ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLogsValue()));
	wr << logs;
	wr << oldLogs;
	return wr.toValue();
}
std::pair<vector<std::pair<UID, NetworkAddress>>,vector<std::pair<UID, NetworkAddress>>> decodeLogsValue( const ValueRef& value ) {
	vector<std::pair<UID, NetworkAddress>> logs;
	vector<std::pair<UID, NetworkAddress>> oldLogs;
	BinaryReader reader( value, IncludeVersion() );
	reader >> logs;
	reader >> oldLogs;
	return std::make_pair(logs, oldLogs);
}

const KeyRef serverKeysPrefix = LiteralStringRef("\xff/serverKeys/");
const ValueRef serverKeysTrue = LiteralStringRef("1"), // compatible with what was serverKeysTrue
			   serverKeysFalse;

const Key serverKeysKey( UID serverID, const KeyRef& key ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverKeysPrefix );
	wr << serverID;
	wr.serializeBytes( LiteralStringRef("/") );
	wr.serializeBytes( key );
	return wr.toValue();
}
const Key serverKeysPrefixFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverKeysPrefix );
	wr << serverID;
	wr.serializeBytes( LiteralStringRef("/") );
	return wr.toValue();
}
UID serverKeysDecodeServer( const KeyRef& key ) {
	UID server_id;
	BinaryReader rd( key.removePrefix(serverKeysPrefix), Unversioned() );
	rd >> server_id;
	return server_id;
}
bool serverHasKey( ValueRef storedValue ) {
	return storedValue == serverKeysTrue;
}

const KeyRef cacheKeysPrefix = LiteralStringRef("\xff\x02/cacheKeys/");

const Key cacheKeysKey( uint16_t idx, const KeyRef& key ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( cacheKeysPrefix );
	wr << idx;
	wr.serializeBytes( LiteralStringRef("/") );
	wr.serializeBytes( key );
	return wr.toValue();
}
const Key cacheKeysPrefixFor( uint16_t idx ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( cacheKeysPrefix );
	wr << idx;
	wr.serializeBytes( LiteralStringRef("/") );
	return wr.toValue();
}
uint16_t cacheKeysDecodeIndex( const KeyRef& key ) {
	uint16_t idx;
	BinaryReader rd( key.removePrefix(cacheKeysPrefix), Unversioned() );
	rd >> idx;
	return idx;
}
KeyRef cacheKeysDecodeKey( const KeyRef& key ) {
	return key.substr( cacheKeysPrefix.size() + sizeof(uint16_t) + 1);
}

const KeyRef cacheChangeKey = LiteralStringRef("\xff\x02/cacheChangeKey");
const KeyRangeRef cacheChangeKeys( LiteralStringRef("\xff\x02/cacheChangeKeys/"), LiteralStringRef("\xff\x02/cacheChangeKeys0") );
const KeyRef cacheChangePrefix = cacheChangeKeys.begin;
const Key cacheChangeKeyFor( uint16_t idx ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( cacheChangePrefix );
	wr << idx;
	return wr.toValue();
}
uint16_t cacheChangeKeyDecodeIndex( const KeyRef& key ) {
	uint16_t idx;
	BinaryReader rd( key.removePrefix(cacheChangePrefix), Unversioned() );
	rd >> idx;
	return idx;
}

const KeyRangeRef serverTagKeys(
	LiteralStringRef("\xff/serverTag/"),
	LiteralStringRef("\xff/serverTag0") );
const KeyRef serverTagPrefix = serverTagKeys.begin;
const KeyRangeRef serverTagConflictKeys(
	LiteralStringRef("\xff/serverTagConflict/"),
	LiteralStringRef("\xff/serverTagConflict0") );
const KeyRef serverTagConflictPrefix = serverTagConflictKeys.begin;
// serverTagHistoryKeys is the old tag a storage server uses before it is migrated to a different location.
// For example, we can copy a SS file to a remote DC and start the SS there;
//   The new SS will need to consume the last bits of data from the old tag it is responsible for.
const KeyRangeRef serverTagHistoryKeys(
	LiteralStringRef("\xff/serverTagHistory/"),
	LiteralStringRef("\xff/serverTagHistory0") );
const KeyRef serverTagHistoryPrefix = serverTagHistoryKeys.begin;

const Key serverTagKeyFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagKeys.begin );
	wr << serverID;
	return wr.toValue();
}

const Key serverTagHistoryKeyFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagHistoryKeys.begin );
	wr << serverID;
	return addVersionStampAtEnd(wr.toValue());
}

const KeyRange serverTagHistoryRangeFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagHistoryKeys.begin );
	wr << serverID;
	return prefixRange(wr.toValue());
}

const KeyRange serverTagHistoryRangeBefore( UID serverID, Version version ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagHistoryKeys.begin );
	wr << serverID;
	version = bigEndian64(version);

	Key versionStr = makeString( 8 );
	uint8_t* data = mutateString( versionStr );
	memcpy(data, &version, 8);

	return KeyRangeRef( wr.toValue(), versionStr.withPrefix(wr.toValue()) );
}

const Value serverTagValue( Tag tag ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withServerTagValue()));
	wr << tag;
	return wr.toValue();
}

UID decodeServerTagKey( KeyRef const& key ) {
	UID serverID;
	BinaryReader rd( key.removePrefix(serverTagKeys.begin), Unversioned() );
	rd >> serverID;
	return serverID;
}

Version decodeServerTagHistoryKey( KeyRef const& key ) {
	Version parsedVersion;
	memcpy(&parsedVersion, key.substr(key.size()-10).begin(), sizeof(Version));
	parsedVersion = bigEndian64(parsedVersion);
	return parsedVersion;
}

Tag decodeServerTagValue( ValueRef const& value ) {
	Tag s;
	BinaryReader reader( value, IncludeVersion() );
	if(!reader.protocolVersion().hasTagLocality()) {
		int16_t id;
		reader >> id;
		if(id == invalidTagOld) {
			s = invalidTag;
		} else if(id == txsTagOld) {
			s = txsTag;
		} else {
			ASSERT(id >= 0);
			s.id = id;
			s.locality = tagLocalityUpgraded;
		}
	} else {
		reader >> s;
	}
	return s;
}

const Key serverTagConflictKeyFor( Tag tag ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagConflictKeys.begin );
	wr << tag;
	return wr.toValue();
}

const KeyRangeRef tagLocalityListKeys(
	LiteralStringRef("\xff/tagLocalityList/"),
	LiteralStringRef("\xff/tagLocalityList0") );
const KeyRef tagLocalityListPrefix = tagLocalityListKeys.begin;

const Key tagLocalityListKeyFor( Optional<Value> dcID ) {
	BinaryWriter wr(AssumeVersion(currentProtocolVersion));
	wr.serializeBytes( tagLocalityListKeys.begin );
	wr << dcID;
	return wr.toValue();
}

const Value tagLocalityListValue( int8_t const& tagLocality ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagLocalityListValue()));
	wr << tagLocality;
	return wr.toValue();
}
Optional<Value> decodeTagLocalityListKey( KeyRef const& key ) {
	Optional<Value> dcID;
	BinaryReader rd( key.removePrefix(tagLocalityListKeys.begin), AssumeVersion(currentProtocolVersion) );
	rd >> dcID;
	return dcID;
}
int8_t decodeTagLocalityListValue( ValueRef const& value ) {
	int8_t s;
	BinaryReader reader( value, IncludeVersion() );
	reader >> s;
	return s;
}

const KeyRangeRef datacenterReplicasKeys(
	LiteralStringRef("\xff\x02/datacenterReplicas/"),
	LiteralStringRef("\xff\x02/datacenterReplicas0") );
const KeyRef datacenterReplicasPrefix = datacenterReplicasKeys.begin;

const Key datacenterReplicasKeyFor( Optional<Value> dcID ) {
	BinaryWriter wr(AssumeVersion(currentProtocolVersion));
	wr.serializeBytes( datacenterReplicasKeys.begin );
	wr << dcID;
	return wr.toValue();
}

const Value datacenterReplicasValue( int const& replicas ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withDatacenterReplicasValue()));
	wr << replicas;
	return wr.toValue();
}
Optional<Value> decodeDatacenterReplicasKey( KeyRef const& key ) {
	Optional<Value> dcID;
	BinaryReader rd( key.removePrefix(datacenterReplicasKeys.begin), AssumeVersion(currentProtocolVersion) );
	rd >> dcID;
	return dcID;
}
int decodeDatacenterReplicasValue( ValueRef const& value ) {
	int s;
	BinaryReader reader( value, IncludeVersion() );
	reader >> s;
	return s;
}

//    "\xff\x02/tLogDatacenters/[[datacenterID]]"
extern const KeyRangeRef tLogDatacentersKeys;
extern const KeyRef tLogDatacentersPrefix;
const Key tLogDatacentersKeyFor( Optional<Value> dcID );

const KeyRangeRef tLogDatacentersKeys(
	LiteralStringRef("\xff\x02/tLogDatacenters/"),
	LiteralStringRef("\xff\x02/tLogDatacenters0") );
const KeyRef tLogDatacentersPrefix = tLogDatacentersKeys.begin;

const Key tLogDatacentersKeyFor( Optional<Value> dcID ) {
	BinaryWriter wr(AssumeVersion(currentProtocolVersion));
	wr.serializeBytes( tLogDatacentersKeys.begin );
	wr << dcID;
	return wr.toValue();
}
Optional<Value> decodeTLogDatacentersKey( KeyRef const& key ) {
	Optional<Value> dcID;
	BinaryReader rd( key.removePrefix(tLogDatacentersKeys.begin), AssumeVersion(currentProtocolVersion) );
	rd >> dcID;
	return dcID;
}

const KeyRef primaryDatacenterKey = LiteralStringRef("\xff/primaryDatacenter");

// serverListKeys.contains(k) iff k.startsWith( serverListKeys.begin ) because '/'+1 == '0'
const KeyRangeRef serverListKeys(
	LiteralStringRef("\xff/serverList/"),
	LiteralStringRef("\xff/serverList0") );
const KeyRef serverListPrefix = serverListKeys.begin;

const Key serverListKeyFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverListKeys.begin );
	wr << serverID;
	return wr.toValue();
}

const Value serverListValue( StorageServerInterface const& server ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withServerListValue()));
	wr << server;
	return wr.toValue();
}
UID decodeServerListKey( KeyRef const& key ) {
	UID serverID;
	BinaryReader rd( key.removePrefix(serverListKeys.begin), Unversioned() );
	rd >> serverID;
	return serverID;
}
StorageServerInterface decodeServerListValue( ValueRef const& value ) {
	StorageServerInterface s;
	BinaryReader reader( value, IncludeVersion() );
	reader >> s;
	return s;
}


// processClassKeys.contains(k) iff k.startsWith( processClassKeys.begin ) because '/'+1 == '0'
const KeyRangeRef processClassKeys(
	LiteralStringRef("\xff/processClass/"),
	LiteralStringRef("\xff/processClass0") );
const KeyRef processClassPrefix = processClassKeys.begin;
const KeyRef processClassChangeKey = LiteralStringRef("\xff/processClassChanges");
const KeyRef processClassVersionKey = LiteralStringRef("\xff/processClassChangesVersion");
const ValueRef processClassVersionValue = LiteralStringRef("1");

const Key processClassKeyFor(StringRef processID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( processClassKeys.begin );
	wr << processID;
	return wr.toValue();
}

const Value processClassValue( ProcessClass const& processClass ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withProcessClassValue()));
	wr << processClass;
	return wr.toValue();
}

Key decodeProcessClassKey( KeyRef const& key ) {
	StringRef processID;
	BinaryReader rd( key.removePrefix(processClassKeys.begin), Unversioned() );
	rd >> processID;
	return processID;
}

UID decodeProcessClassKeyOld( KeyRef const& key ) {
	UID processID;
	BinaryReader rd( key.removePrefix(processClassKeys.begin), Unversioned() );
	rd >> processID;
	return processID;
}

ProcessClass decodeProcessClassValue( ValueRef const& value ) {
	ProcessClass s;
	BinaryReader reader( value, IncludeVersion() );
	reader >> s;
	return s;
}

const KeyRangeRef configKeys( LiteralStringRef("\xff/conf/"), LiteralStringRef("\xff/conf0") );
const KeyRef configKeysPrefix = configKeys.begin;

const KeyRangeRef excludedServersKeys( LiteralStringRef("\xff/conf/excluded/"), LiteralStringRef("\xff/conf/excluded0") );
const KeyRef excludedServersPrefix = excludedServersKeys.begin;
const KeyRef excludedServersVersionKey = LiteralStringRef("\xff/conf/excluded");
const AddressExclusion decodeExcludedServersKey( KeyRef const& key ) {
	ASSERT( key.startsWith( excludedServersPrefix ) );
	// Returns an invalid NetworkAddress if given an invalid key (within the prefix)
	// Excluded servers have IP in x.x.x.x format, port optional, and no SSL suffix
	// Returns a valid, public NetworkAddress with a port of 0 if the key represents an IP address alone (meaning all ports)
	// Returns a valid, public NetworkAddress with nonzero port if the key represents an IP:PORT combination

	return AddressExclusion::parse(key.removePrefix( excludedServersPrefix ));
}
std::string encodeExcludedServersKey( AddressExclusion const& addr ) {
	//FIXME: make sure what's persisted here is not affected by innocent changes elsewhere
	return excludedServersPrefix.toString() + addr.toString();
}

const KeyRangeRef failedServersKeys( LiteralStringRef("\xff/conf/failed/"), LiteralStringRef("\xff/conf/failed0") );
const KeyRef failedServersPrefix = failedServersKeys.begin;
const KeyRef failedServersVersionKey = LiteralStringRef("\xff/conf/failed");
const AddressExclusion decodeFailedServersKey( KeyRef const& key ) {
	ASSERT( key.startsWith( failedServersPrefix ) );
	// Returns an invalid NetworkAddress if given an invalid key (within the prefix)
	// Excluded servers have IP in x.x.x.x format, port optional, and no SSL suffix
	// Returns a valid, public NetworkAddress with a port of 0 if the key represents an IP address alone (meaning all ports)
	// Returns a valid, public NetworkAddress with nonzero port if the key represents an IP:PORT combination

	return AddressExclusion::parse(key.removePrefix( failedServersPrefix ));
}
std::string encodeFailedServersKey( AddressExclusion const& addr ) {
	//FIXME: make sure what's persisted here is not affected by innocent changes elsewhere
	return failedServersPrefix.toString() + addr.toString();
}

// TODO: Add writeTxnLifetimeKey
const KeyRef readTxnLifetimeKey = LiteralStringRef("\xff/conf/readTxnLifetime");
const Version decodeReadTxnLifetime(ValueRef const& value) {
	Version lifetime;
	BinaryReader reader(value, Unversioned());
	reader >> lifetime;
	return lifetime;
}
const Value encodeReadTxnLifetime(Version lifetime) {
	BinaryWriter wr(Unversioned());
	wr << lifetime;
	return wr.toValue();
}

const KeyRangeRef workerListKeys( LiteralStringRef("\xff/worker/"), LiteralStringRef("\xff/worker0") );
const KeyRef workerListPrefix = workerListKeys.begin;

const Key workerListKeyFor( StringRef processID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( workerListKeys.begin );
	wr << processID;
	return wr.toValue();
}

const Value workerListValue( ProcessData const& processData ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withWorkerListValue()));
	wr << processData;
	return wr.toValue();
}

Key decodeWorkerListKey(KeyRef const& key) {
	StringRef processID;
	BinaryReader rd( key.removePrefix(workerListKeys.begin), Unversioned() );
	rd >> processID;
	return processID;
}

ProcessData decodeWorkerListValue( ValueRef const& value ) {
	ProcessData s;
	BinaryReader reader( value, IncludeVersion() );
	reader >> s;
	return s;
}

const KeyRangeRef backupProgressKeys(LiteralStringRef("\xff\x02/backupProgress/"),
                                     LiteralStringRef("\xff\x02/backupProgress0"));
const KeyRef backupProgressPrefix = backupProgressKeys.begin;
const KeyRef backupStartedKey = LiteralStringRef("\xff\x02/backupStarted");
extern const KeyRef backupPausedKey = LiteralStringRef("\xff\x02/backupPaused");

const Key backupProgressKeyFor(UID workerID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(backupProgressPrefix);
	wr << workerID;
	return wr.toValue();
}

const Value backupProgressValue(const WorkerBackupStatus& status) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBackupProgressValue()));
	wr << status;
	return wr.toValue();
}

UID decodeBackupProgressKey(const KeyRef& key) {
	UID serverID;
	BinaryReader rd(key.removePrefix(backupProgressPrefix), Unversioned());
	rd >> serverID;
	return serverID;
}

WorkerBackupStatus decodeBackupProgressValue(const ValueRef& value) {
	WorkerBackupStatus status;
	BinaryReader reader(value, IncludeVersion());
	reader >> status;
	return status;
}

Value encodeBackupStartedValue(const std::vector<std::pair<UID, Version>>& ids) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBackupStartValue()));
	wr << ids;
	return wr.toValue();
}

std::vector<std::pair<UID, Version>> decodeBackupStartedValue(const ValueRef& value) {
	std::vector<std::pair<UID, Version>> ids;
	BinaryReader reader(value, IncludeVersion());
	if (value.size() > 0) reader >> ids;
	return ids;
}

const KeyRef coordinatorsKey = LiteralStringRef("\xff/coordinators");
const KeyRef logsKey = LiteralStringRef("\xff/logs");
const KeyRef minRequiredCommitVersionKey = LiteralStringRef("\xff/minRequiredCommitVersion");

const KeyRef globalKeysPrefix = LiteralStringRef("\xff/globals");
const KeyRef lastEpochEndKey = LiteralStringRef("\xff/globals/lastEpochEnd");
const KeyRef lastEpochEndPrivateKey = LiteralStringRef("\xff\xff/globals/lastEpochEnd");
const KeyRef killStorageKey = LiteralStringRef("\xff/globals/killStorage");
const KeyRef killStoragePrivateKey = LiteralStringRef("\xff\xff/globals/killStorage");
const KeyRef rebootWhenDurableKey = LiteralStringRef("\xff/globals/rebootWhenDurable");
const KeyRef rebootWhenDurablePrivateKey = LiteralStringRef("\xff\xff/globals/rebootWhenDurable");
const KeyRef primaryLocalityKey = LiteralStringRef("\xff/globals/primaryLocality");
const KeyRef primaryLocalityPrivateKey = LiteralStringRef("\xff\xff/globals/primaryLocality");
const KeyRef fastLoggingEnabled = LiteralStringRef("\xff/globals/fastLoggingEnabled");
const KeyRef fastLoggingEnabledPrivateKey = LiteralStringRef("\xff\xff/globals/fastLoggingEnabled");

// Whenever configuration changes or DD related system keyspace is changed(e.g.., serverList),
// actor must grab the moveKeysLockOwnerKey and update moveKeysLockWriteKey.
// This prevents concurrent write to the same system keyspace.
// When the owner of the DD related system keyspace changes, DD will reboot
const KeyRef moveKeysLockOwnerKey = LiteralStringRef("\xff/moveKeysLock/Owner");
const KeyRef moveKeysLockWriteKey = LiteralStringRef("\xff/moveKeysLock/Write");

const KeyRef dataDistributionModeKey = LiteralStringRef("\xff/dataDistributionMode");
const UID dataDistributionModeLock = UID(6345,3425);

// Keys to view and control tag throttling
const KeyRangeRef tagThrottleKeys = KeyRangeRef(
	LiteralStringRef("\xff\x02/throttledTags/tag/"),
	LiteralStringRef("\xff\x02/throttledTags/tag0"));
const KeyRef tagThrottleKeysPrefix = tagThrottleKeys.begin;
const KeyRef tagThrottleAutoKeysPrefix = LiteralStringRef("\xff\x02/throttledTags/tag/\x01");
const KeyRef tagThrottleSignalKey = LiteralStringRef("\xff\x02/throttledTags/signal");
const KeyRef tagThrottleAutoEnabledKey = LiteralStringRef("\xff\x02/throttledTags/autoThrottlingEnabled");
const KeyRef tagThrottleLimitKey = LiteralStringRef("\xff\x02/throttledTags/manualThrottleLimit");
const KeyRef tagThrottleCountKey = LiteralStringRef("\xff\x02/throttledTags/manualThrottleCount");

// Client status info prefix
const KeyRangeRef fdbClientInfoPrefixRange(LiteralStringRef("\xff\x02/fdbClientInfo/"), LiteralStringRef("\xff\x02/fdbClientInfo0"));
const KeyRef fdbClientInfoTxnSampleRate = LiteralStringRef("\xff\x02/fdbClientInfo/client_txn_sample_rate/");
const KeyRef fdbClientInfoTxnSizeLimit = LiteralStringRef("\xff\x02/fdbClientInfo/client_txn_size_limit/");

// ConsistencyCheck settings
const KeyRef fdbShouldConsistencyCheckBeSuspended = LiteralStringRef("\xff\x02/ConsistencyCheck/Suspend");

// Request latency measurement key
const KeyRef latencyBandConfigKey = LiteralStringRef("\xff\x02/latencyBandConfig");

// Keyspace to maintain wall clock to version map
const KeyRangeRef timeKeeperPrefixRange(LiteralStringRef("\xff\x02/timeKeeper/map/"), LiteralStringRef("\xff\x02/timeKeeper/map0"));
const KeyRef timeKeeperVersionKey = LiteralStringRef("\xff\x02/timeKeeper/version");
const KeyRef timeKeeperDisableKey = LiteralStringRef("\xff\x02/timeKeeper/disable");

// Backup Log Mutation constant variables
const KeyRef backupEnabledKey = LiteralStringRef("\xff/backupEnabled");
const KeyRangeRef backupLogKeys(LiteralStringRef("\xff\x02/blog/"), LiteralStringRef("\xff\x02/blog0"));
const KeyRangeRef applyLogKeys(LiteralStringRef("\xff\x02/alog/"), LiteralStringRef("\xff\x02/alog0"));
//static_assert( backupLogKeys.begin.size() == backupLogPrefixBytes, "backupLogPrefixBytes incorrect" );
const KeyRef backupVersionKey = LiteralStringRef("\xff/backupDataFormat");
const ValueRef backupVersionValue = LiteralStringRef("4");
const int backupVersion = 4;

// Log Range constant variables
// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]), IncludeVersion() )
const KeyRangeRef logRangesRange(LiteralStringRef("\xff/logRanges/"), LiteralStringRef("\xff/logRanges0"));

// Layer status metadata prefix
const KeyRangeRef layerStatusMetaPrefixRange(LiteralStringRef("\xff\x02/status/"), LiteralStringRef("\xff\x02/status0"));

// Backup agent status root
const KeyRangeRef backupStatusPrefixRange(LiteralStringRef("\xff\x02/backupstatus/"), LiteralStringRef("\xff\x02/backupstatus0"));

// Restore configuration constant variables
const KeyRangeRef fileRestorePrefixRange(LiteralStringRef("\xff\x02/restore-agent/"), LiteralStringRef("\xff\x02/restore-agent0"));

// Backup Agent configuration constant variables
const KeyRangeRef fileBackupPrefixRange(LiteralStringRef("\xff\x02/backup-agent/"), LiteralStringRef("\xff\x02/backup-agent0"));

// DR Agent configuration constant variables
const KeyRangeRef databaseBackupPrefixRange(LiteralStringRef("\xff\x02/db-backup-agent/"), LiteralStringRef("\xff\x02/db-backup-agent0"));

// \xff\x02/sharedLogRangesConfig/destUidLookup/[keyRange]
const KeyRef destUidLookupPrefix = LiteralStringRef("\xff\x02/sharedLogRangesConfig/destUidLookup/");
// \xff\x02/sharedLogRangesConfig/backuplatestVersions/[destUid]/[logUid]
const KeyRef backupLatestVersionsPrefix = LiteralStringRef("\xff\x02/sharedLogRangesConfig/backupLatestVersions/");

// Returns the encoded key comprised of begin key and log uid
Key logRangesEncodeKey(KeyRef keyBegin, UID logUid) {
	return keyBegin.withPrefix(uidPrefixKey(logRangesRange.begin, logUid));
}

// Returns the start key and optionally the logRange Uid
KeyRef logRangesDecodeKey(KeyRef key, UID* logUid) {
	if (key.size() < logRangesRange.begin.size() + sizeof(UID)) {
		TraceEvent(SevError, "InvalidDecodeKey").detail("Key", key);
		ASSERT(false);
	}

	if (logUid)	{
		*logUid = BinaryReader::fromStringRef<UID>(key.removePrefix(logRangesRange.begin), Unversioned());
	}

	return key.substr(logRangesRange.begin.size() + sizeof(UID));
}

// Returns the encoded key value comprised of the end key and destination path
Key logRangesEncodeValue(KeyRef keyEnd, KeyRef destPath) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLogRangeEncodeValue()));
	wr << std::make_pair(keyEnd, destPath);
	return wr.toValue();
}

// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]), IncludeVersion() )
Key logRangesDecodeValue(KeyRef keyValue, Key* destKeyPrefix) {
	std::pair<KeyRef,KeyRef> endPrefixCombo;

	BinaryReader rd( keyValue, IncludeVersion() );
	rd >> endPrefixCombo;

	if (destKeyPrefix) {
		*destKeyPrefix = endPrefixCombo.second;
	}

	return endPrefixCombo.first;
}

// Returns a key prefixed with the specified key with
// the uid encoded at the end
Key uidPrefixKey(KeyRef keyPrefix, UID logUid) {
	BinaryWriter bw(Unversioned());
	bw.serializeBytes(keyPrefix);
	bw << logUid;
	return bw.toValue();
}

// Apply mutations constant variables
// \xff/applyMutationsEnd/[16-byte UID] := serialize( endVersion, Unversioned() )
// This indicates what is the highest version the mutation log can be applied
const KeyRangeRef applyMutationsEndRange(LiteralStringRef("\xff/applyMutationsEnd/"), LiteralStringRef("\xff/applyMutationsEnd0"));

// \xff/applyMutationsBegin/[16-byte UID] := serialize( beginVersion, Unversioned() )
const KeyRangeRef applyMutationsBeginRange(LiteralStringRef("\xff/applyMutationsBegin/"), LiteralStringRef("\xff/applyMutationsBegin0"));

// \xff/applyMutationsAddPrefix/[16-byte UID] := addPrefix
const KeyRangeRef applyMutationsAddPrefixRange(LiteralStringRef("\xff/applyMutationsAddPrefix/"), LiteralStringRef("\xff/applyMutationsAddPrefix0"));

// \xff/applyMutationsRemovePrefix/[16-byte UID] := removePrefix
const KeyRangeRef applyMutationsRemovePrefixRange(LiteralStringRef("\xff/applyMutationsRemovePrefix/"), LiteralStringRef("\xff/applyMutationsRemovePrefix0"));

const KeyRangeRef applyMutationsKeyVersionMapRange(LiteralStringRef("\xff/applyMutationsKeyVersionMap/"), LiteralStringRef("\xff/applyMutationsKeyVersionMap0"));
const KeyRangeRef applyMutationsKeyVersionCountRange(LiteralStringRef("\xff\x02/applyMutationsKeyVersionCount/"), LiteralStringRef("\xff\x02/applyMutationsKeyVersionCount0"));

const KeyRef systemTuplesPrefix = LiteralStringRef("\xff/a/");
const KeyRef metricConfChangeKey = LiteralStringRef("\x01TDMetricConfChanges\x00");

const KeyRangeRef metricConfKeys( LiteralStringRef("\x01TDMetricConf\x00\x01"), LiteralStringRef("\x01TDMetricConf\x00\x02") );
const KeyRef metricConfPrefix = metricConfKeys.begin;

/*
const Key metricConfKey( KeyRef const& prefix, MetricNameRef const& name, KeyRef const& key  ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( prefix );
	wr.serializeBytes( metricConfPrefix );
	wr.serializeBytes( name.type );
	wr.serializeBytes( LiteralStringRef("\x00\x01") );
	wr.serializeBytes( name.name );
	wr.serializeBytes( LiteralStringRef("\x00\x01") );
	wr.serializeBytes( name.address );
	wr.serializeBytes( LiteralStringRef("\x00\x01") );
	wr.serializeBytes( name.id );
	wr.serializeBytes( LiteralStringRef("\x00\x01") );
	wr.serializeBytes( key );
	wr.serializeBytes( LiteralStringRef("\x00") );
	return wr.toValue();
}

std::pair<MetricNameRef, KeyRef> decodeMetricConfKey( KeyRef const& prefix, KeyRef const& key ) {
	MetricNameRef result;
	KeyRef withoutPrefix = key.removePrefix( prefix );
	withoutPrefix = withoutPrefix.removePrefix( metricConfPrefix );
	int pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
	result.type = withoutPrefix.substr(0,pos);
	withoutPrefix = withoutPrefix.substr(pos+2);
	pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
	result.name = withoutPrefix.substr(0,pos);
	withoutPrefix = withoutPrefix.substr(pos+2);
	pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
	result.address = withoutPrefix.substr(0,pos);
	withoutPrefix = withoutPrefix.substr(pos+2);
	pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
	result.id = withoutPrefix.substr(0,pos);
	return std::make_pair( result, withoutPrefix.substr(pos+2,withoutPrefix.size()-pos-3) );
}
*/

const KeyRef maxUIDKey = LiteralStringRef("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

const KeyRef databaseLockedKey = LiteralStringRef("\xff/dbLocked");
const KeyRef metadataVersionKey = LiteralStringRef("\xff/metadataVersion");
const KeyRef metadataVersionKeyEnd = LiteralStringRef("\xff/metadataVersion\x00");
const KeyRef metadataVersionRequiredValue = LiteralStringRef("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
const KeyRef mustContainSystemMutationsKey = LiteralStringRef("\xff/mustContainSystemMutations");

const KeyRangeRef monitorConfKeys(
	LiteralStringRef("\xff\x02/monitorConf/"),
	LiteralStringRef("\xff\x02/monitorConf0")
);

const KeyRef restoreLeaderKey = LiteralStringRef("\xff\x02/restoreLeader");
const KeyRangeRef restoreWorkersKeys(
	LiteralStringRef("\xff\x02/restoreWorkers/"),
	LiteralStringRef("\xff\x02/restoreWorkers0")
);
const KeyRef restoreStatusKey = LiteralStringRef("\xff\x02/restoreStatus/");

const KeyRef restoreRequestTriggerKey = LiteralStringRef("\xff\x02/restoreRequestTrigger");
const KeyRef restoreRequestDoneKey = LiteralStringRef("\xff\x02/restoreRequestDone");
const KeyRangeRef restoreRequestKeys(LiteralStringRef("\xff\x02/restoreRequests/"),
                                     LiteralStringRef("\xff\x02/restoreRequests0"));

const KeyRangeRef restoreApplierKeys(LiteralStringRef("\xff\x02/restoreApplier/"),
                                     LiteralStringRef("\xff\x02/restoreApplier0"));
const KeyRef restoreApplierTxnValue = LiteralStringRef("1");

// restoreApplierKeys: track atomic transaction progress to ensure applying atomicOp exactly once
// Version and batchIndex are passed in as LittleEndian,
// they must be converted to BigEndian to maintain ordering in lexical order
const Key restoreApplierKeyFor(UID const& applierID, int64_t batchIndex, Version version) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreApplierKeys.begin);
	wr << applierID << bigEndian64(batchIndex) << bigEndian64(version);
	return wr.toValue();
}

std::tuple<UID, int64_t, Version> decodeRestoreApplierKey(ValueRef const& key) {
	BinaryReader rd(key, Unversioned());
	UID applierID;
	int64_t batchIndex;
	Version version;
	rd >> applierID >> batchIndex >> version;
	return std::make_tuple(applierID, bigEndian64(batchIndex), bigEndian64(version));
}

// Encode restore worker key for workerID
const Key restoreWorkerKeyFor(UID const& workerID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( restoreWorkersKeys.begin );
	wr << workerID;
	return wr.toValue();
}

// Encode restore agent value
const Value restoreWorkerInterfaceValue(RestoreWorkerInterface const& cmdInterf) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreWorkerInterfaceValue()));
	wr << cmdInterf;
	return wr.toValue();
}

RestoreWorkerInterface decodeRestoreWorkerInterfaceValue(ValueRef const& value) {
	RestoreWorkerInterface s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

// Encode and decode restore request value
// restoreRequestTrigger key
const Value restoreRequestTriggerValue(UID randomID, int const numRequests) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreRequestTriggerValue()));
	wr << numRequests;
	wr << randomID;
	return wr.toValue();
}
int decodeRestoreRequestTriggerValue(ValueRef const& value) {
	int s;
	UID randomID;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	reader >> randomID;
	return s;
}

// restoreRequestDone key
const Value restoreRequestDoneVersionValue(Version readVersion) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreRequestDoneVersionValue()));
	wr << readVersion;
	return wr.toValue();
}
Version decodeRestoreRequestDoneVersionValue(ValueRef const& value) {
	Version v;
	BinaryReader reader(value, IncludeVersion());
	reader >> v;
	return v;
}

const Key restoreRequestKeyFor(int const& index) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreRequestKeys.begin);
	wr << index;
	return wr.toValue();
}

const Value restoreRequestValue(RestoreRequest const& request) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreRequestValue()));
	wr << request;
	return wr.toValue();
}

RestoreRequest decodeRestoreRequestValue(ValueRef const& value) {
	RestoreRequest s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

// TODO: Register restore performance data to restoreStatus key
const Key restoreStatusKeyFor(StringRef statusType) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreStatusKey);
	wr << statusType;
	return wr.toValue();
}

const Value restoreStatusValue(double val) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreStatusValue()));
	wr << StringRef(std::to_string(val));
	return wr.toValue();
}
const KeyRef healthyZoneKey = LiteralStringRef("\xff\x02/healthyZone");
const StringRef ignoreSSFailuresZoneString = LiteralStringRef("IgnoreSSFailures");
const KeyRef rebalanceDDIgnoreKey = LiteralStringRef("\xff\x02/rebalanceDDIgnored");

const Value healthyZoneValue( StringRef const& zoneId, Version version ) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withHealthyZoneValue()));
	wr << zoneId;
	wr << version;
	return wr.toValue();
}
std::pair<Key,Version> decodeHealthyZoneValue( ValueRef const& value) {
	Key zoneId;
	Version version;
	BinaryReader reader( value, IncludeVersion() );
	reader >> zoneId;
	reader >> version;
	return std::make_pair(zoneId, version);
}

const KeyRangeRef testOnlyTxnStateStorePrefixRange(LiteralStringRef("\xff/TESTONLYtxnStateStore/"),
                                                   LiteralStringRef("\xff/TESTONLYtxnStateStore0"));