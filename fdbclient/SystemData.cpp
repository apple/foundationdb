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

#include "SystemData.h"
#include "StorageServerInterface.h"
#include "flow/TDMetric.actor.h"

const KeyRef systemKeysPrefix = LiteralStringRef("\xff");
const KeyRangeRef normalKeys(KeyRef(), systemKeysPrefix);
const KeyRangeRef systemKeys(systemKeysPrefix, LiteralStringRef("\xff\xff") );
const KeyRangeRef nonMetadataSystemKeys(LiteralStringRef("\xff\x02"), LiteralStringRef("\xff\x03"));
const KeyRangeRef allKeys = KeyRangeRef(normalKeys.begin, systemKeys.end);
const KeyRef afterAllKeys = LiteralStringRef("\xff\xff\x00");

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
const Value keyServersValue( const vector<UID>& src, const vector<UID>& dest ) {
	// src and dest are expected to be sorted
	ASSERT( std::is_sorted(src.begin(), src.end()) && std::is_sorted(dest.begin(), dest.end()) );
	BinaryWriter wr((IncludeVersion())); wr << src << dest;
	return wr.toStringRef();
}
void decodeKeyServersValue( const ValueRef& value, vector<UID>& src, vector<UID>& dest ) {
	if (value.size()) {
		BinaryReader rd(value, IncludeVersion());
		rd >> src >> dest;
	} else {
		src.clear();
		dest.clear();
	}
}

const Value logsValue( const vector<std::pair<UID, NetworkAddress>>& logs, const vector<std::pair<UID, NetworkAddress>>& oldLogs ) {
	BinaryWriter wr(IncludeVersion());
	wr << logs;
	wr << oldLogs;
	return wr.toStringRef();
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
	return wr.toStringRef();
}
const Key serverKeysPrefixFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverKeysPrefix );
	wr << serverID;
	wr.serializeBytes( LiteralStringRef("/") );
	return wr.toStringRef();
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

const KeyRangeRef serverTagKeys(
	LiteralStringRef("\xff/serverTag/"),
	LiteralStringRef("\xff/serverTag0") );
const KeyRef serverTagPrefix = serverTagKeys.begin;
const KeyRef serverTagMaxKey = LiteralStringRef("\xff/serverTagMax");
const KeyRangeRef serverTagConflictKeys(
	LiteralStringRef("\xff/serverTagConflict/"),
	LiteralStringRef("\xff/serverTagConflict0") );
const KeyRef serverTagConflictPrefix = serverTagConflictKeys.begin;

const Key serverTagKeyFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagKeys.begin );
	wr << serverID;
	return wr.toStringRef();
}

const Value serverTagValue( Tag tag ) {
	BinaryWriter wr(IncludeVersion());
	wr << tag;
	return wr.toStringRef();
}

UID decodeServerTagKey( KeyRef const& key ) {
	UID serverID;
	BinaryReader rd( key.removePrefix(serverTagKeys.begin), Unversioned() );
	rd >> serverID;
	return serverID;
}

Tag decodeServerTagValue( ValueRef const& value ) {
	Tag s;
	BinaryReader reader( value, IncludeVersion() );
	reader >> s;
	return s;
}

const Key serverTagConflictKeyFor( Tag tag ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverTagConflictKeys.begin );
	wr << tag;
	return wr.toStringRef();
}

const Value serverTagMaxValue( Tag tag ) {
	BinaryWriter wr(Unversioned());
	wr << tag;
	return wr.toStringRef();
}

Tag decodeServerTagMaxValue( ValueRef const& value ) {
	Tag s;
	BinaryReader reader( value, Unversioned() );
	reader >> s;
	return s;
}

// serverListKeys.contains(k) iff k.startsWith( serverListKeys.begin ) because '/'+1 == '0'
const KeyRangeRef serverListKeys(
	LiteralStringRef("\xff/serverList/"),
	LiteralStringRef("\xff/serverList0") );
const KeyRef serverListPrefix = serverListKeys.begin;

const Key serverListKeyFor( UID serverID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( serverListKeys.begin );
	wr << serverID;
	return wr.toStringRef();
}

const Value serverListValue( StorageServerInterface const& server ) {
	BinaryWriter wr(IncludeVersion());
	wr << server;
	return wr.toStringRef();
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
	return wr.toStringRef();
}

const Value processClassValue( ProcessClass const& processClass ) {
	BinaryWriter wr(IncludeVersion());
	wr << processClass;
	return wr.toStringRef();
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
	std::string as = format( "%d.%d.%d.%d", (addr.ip>>24)&0xff, (addr.ip>>16)&0xff, (addr.ip>>8)&0xff, addr.ip&0xff );
	//ASSERT( StringRef(as).endsWith(LiteralStringRef(":0")) == (addr.port == 0) );
	if (!addr.isWholeMachine())
		as += format(":%d", addr.port);
	return excludedServersPrefix.toString() + as;
}

const KeyRangeRef workerListKeys( LiteralStringRef("\xff/worker/"), LiteralStringRef("\xff/worker0") );
const KeyRef workerListPrefix = workerListKeys.begin;

const Key workerListKeyFor( StringRef processID ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( workerListKeys.begin );
	wr << processID;
	return wr.toStringRef();
}

const Value workerListValue( ProcessData const& processData ) {
	BinaryWriter wr(IncludeVersion());
	wr << processData;
	return wr.toStringRef();
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

const KeyRef coordinatorsKey = LiteralStringRef("\xff/coordinators");
const KeyRef logsKey = LiteralStringRef("\xff/logs");
const KeyRef minRequiredCommitVersionKey = LiteralStringRef("\xff/minRequiredCommitVersion");

const KeyRef globalKeysPrefix = LiteralStringRef("\xff/globals");
const KeyRef lastEpochEndKey = LiteralStringRef("\xff/globals/lastEpochEnd");
const KeyRef lastEpochEndPrivateKey = LiteralStringRef("\xff\xff/globals/lastEpochEnd");
const KeyRef fastLoggingEnabled = LiteralStringRef("\xff/globals/fastLoggingEnabled");
const KeyRef fastLoggingEnabledPrivateKey = LiteralStringRef("\xff\xff/globals/fastLoggingEnabled");

const KeyRef moveKeysLockOwnerKey = LiteralStringRef("\xff/moveKeysLock/Owner");
const KeyRef moveKeysLockWriteKey = LiteralStringRef("\xff/moveKeysLock/Write");

const KeyRef dataDistributionModeKey = LiteralStringRef("\xff/dataDistributionMode");
const UID dataDistributionModeLock = UID(6345,3425);

// Client status info prefix
const KeyRangeRef fdbClientInfoPrefixRange(LiteralStringRef("\xff\x02/fdbClientInfo/"), LiteralStringRef("\xff\x02/fdbClientInfo0"));
const KeyRef fdbClientInfoTxnSampleRate = LiteralStringRef("\xff\x02/fdbClientInfo/client_txn_sample_rate/");
const KeyRef fdbClientInfoTxnSizeLimit = LiteralStringRef("\xff\x02/fdbClientInfo/client_txn_size_limit/");

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
		TraceEvent(SevError, "InvalidDecodeKey").detail("key", printable(key));
		ASSERT(false);
	}

	if (logUid)	{
		*logUid = BinaryReader::fromStringRef<UID>(key.removePrefix(logRangesRange.begin), Unversioned());
	}

	return key.substr(logRangesRange.begin.size() + sizeof(UID));
}

// Returns the encoded key value comprised of the end key and destination path
Key logRangesEncodeValue(KeyRef keyEnd, KeyRef destPath) {
	BinaryWriter wr(IncludeVersion());
	wr << std::make_pair(keyEnd, destPath);
	return wr.toStringRef();
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
	return bw.toStringRef();
}

// Apply mutations constant variables
// \xff/applyMutationsEnd/[16-byte UID] := serialize( endVersion, Unversioned() )
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
	return wr.toStringRef();
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
