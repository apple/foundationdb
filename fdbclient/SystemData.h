/*
 * SystemData.h
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

#ifndef FDBCLIENT_SYSTEMDATA_H
#define FDBCLIENT_SYSTEMDATA_H
#pragma once

// Functions and constants documenting the organization of the reserved keyspace in the database beginning with "\xFF"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"

// Don't warn on constants being defined in this file.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"

struct RestoreLoaderInterface;
struct RestoreApplierInterface;
struct RestoreMasterInterface;

extern const KeyRangeRef normalKeys; // '' to systemKeys.begin
extern const KeyRangeRef systemKeys;  // [FF] to [FF][FF]
extern const KeyRangeRef nonMetadataSystemKeys; // [FF][00] to [FF][01]
extern const KeyRangeRef allKeys; // '' to systemKeys.end
extern const KeyRangeRef specialKeys; // [FF][FF] to [FF][FF][FF], some client functions are exposed through FDB calls
                                      // using these special keys, see pr#2662
extern const KeyRef afterAllKeys;

//    "\xff/keyServers/[[begin]]" := "[[vector<serverID>, vector<serverID>]|[vector<Tag>, vector<Tag>]]"
extern const KeyRangeRef keyServersKeys, keyServersKeyServersKeys;
extern const KeyRef keyServersPrefix, keyServersEnd, keyServersKeyServersKey;
const Key keyServersKey( const KeyRef& k );
const KeyRef keyServersKey( const KeyRef& k, Arena& arena );
const Value keyServersValue(
	Standalone<RangeResultRef> result,
	const std::vector<UID>& src,
	const std::vector<UID>& dest = std::vector<UID>() );
const Value keyServersValue(
	const std::vector<Tag>& srcTag,
	const std::vector<Tag>& destTag = std::vector<Tag>());
// `result` must be the full result of getting serverTagKeys
void decodeKeyServersValue( Standalone<RangeResultRef> result, const ValueRef& value,
	std::vector<UID>& src, std::vector<UID>& dest, bool missingIsError = true );
void decodeKeyServersValue( std::map<Tag, UID> const& tag_uid, const ValueRef& value,
                            std::vector<UID>& src, std::vector<UID>& dest );

// "\xff/storageCacheServer/[[UID]] := StorageServerInterface"
extern const KeyRangeRef storageCacheServerKeys;
extern const KeyRef storageCacheServersPrefix, storageCacheServersEnd;
const Key storageCacheServerKey(UID id);
const Value storageCacheServerValue(const StorageServerInterface& ssi);

//    "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
extern const KeyRangeRef storageCacheKeys;
extern const KeyRef storageCachePrefix;
const Key storageCacheKey( const KeyRef& k );
const Value storageCacheValue( const std::vector<uint16_t>& serverIndices );
void decodeStorageCacheValue( const ValueRef& value, std::vector<uint16_t>& serverIndices );

//    "\xff/serverKeys/[[serverID]]/[[begin]]" := "" | "1" | "2"
extern const KeyRef serverKeysPrefix;
extern const ValueRef serverKeysTrue, serverKeysFalse;
const Key serverKeysKey( UID serverID, const KeyRef& keys );
const Key serverKeysPrefixFor( UID serverID );
UID serverKeysDecodeServer( const KeyRef& key );
bool serverHasKey( ValueRef storedValue );

extern const KeyRangeRef conflictingKeysRange;
extern const ValueRef conflictingKeysTrue, conflictingKeysFalse;
extern const KeyRangeRef writeConflictRangeKeysRange;
extern const KeyRangeRef readConflictRangeKeysRange;
extern const KeyRangeRef ddStatsRange;

extern const KeyRef cacheKeysPrefix;

const Key cacheKeysKey( uint16_t idx, const KeyRef& key );
const Key cacheKeysPrefixFor( uint16_t idx );
uint16_t cacheKeysDecodeIndex( const KeyRef& key );
KeyRef cacheKeysDecodeKey( const KeyRef& key );

extern const KeyRef cacheChangeKey;
extern const KeyRangeRef cacheChangeKeys;
extern const KeyRef cacheChangePrefix;
const Key cacheChangeKeyFor( uint16_t idx );
uint16_t cacheChangeKeyDecodeIndex( const KeyRef& key );

// "\xff/serverTag/[[serverID]]" = "[[Tag]]"
extern const KeyRangeRef serverTagKeys;
extern const KeyRef serverTagPrefix;
extern const KeyRangeRef serverTagMaxKeys;
extern const KeyRangeRef serverTagConflictKeys;
extern const KeyRef serverTagConflictPrefix;
extern const KeyRangeRef serverTagHistoryKeys;
extern const KeyRef serverTagHistoryPrefix;

const Key serverTagKeyFor( UID serverID );
const Key serverTagHistoryKeyFor( UID serverID );
const KeyRange serverTagHistoryRangeFor( UID serverID );
const KeyRange serverTagHistoryRangeBefore( UID serverID, Version version );
const Value serverTagValue( Tag );
UID decodeServerTagKey( KeyRef const& );
Version decodeServerTagHistoryKey( KeyRef const& );
Tag decodeServerTagValue( ValueRef const& );
const Key serverTagConflictKeyFor( Tag );

//    "\xff/tagLocalityList/[[datacenterID]]" := "[[tagLocality]]"
extern const KeyRangeRef tagLocalityListKeys;
extern const KeyRef tagLocalityListPrefix;
const Key tagLocalityListKeyFor( Optional<Value> dcID );
const Value tagLocalityListValue( int8_t const& );
Optional<Value> decodeTagLocalityListKey( KeyRef const& );
int8_t decodeTagLocalityListValue( ValueRef const& );

//    "\xff\x02/datacenterReplicas/[[datacenterID]]" := "[[replicas]]"
extern const KeyRangeRef datacenterReplicasKeys;
extern const KeyRef datacenterReplicasPrefix;
const Key datacenterReplicasKeyFor( Optional<Value> dcID );
const Value datacenterReplicasValue( int const& );
Optional<Value> decodeDatacenterReplicasKey( KeyRef const& );
int decodeDatacenterReplicasValue( ValueRef const& );

//    "\xff\x02/tLogDatacenters/[[datacenterID]]"
extern const KeyRangeRef tLogDatacentersKeys;
extern const KeyRef tLogDatacentersPrefix;
const Key tLogDatacentersKeyFor( Optional<Value> dcID );
Optional<Value> decodeTLogDatacentersKey( KeyRef const& );

extern const KeyRef primaryDatacenterKey;

//    "\xff/serverList/[[serverID]]" := "[[StorageServerInterface]]"
// Storage servers are listed here when they are recruited - always before assigning them keys
// Storage servers removed from here are never replaced.  The same fdbserver, if re-recruited, will always
//    have a new ID.  When removed from here, a storage server may release all resources and destroy itself.
extern const KeyRangeRef serverListKeys;
extern const KeyRef serverListPrefix;
const Key serverListKeyFor( UID serverID );
const Value serverListValue( StorageServerInterface const& );
UID decodeServerListKey( KeyRef const& );
StorageServerInterface decodeServerListValue( ValueRef const& );

//    "\xff/processClass/[[processID]]" := "[[ProcessClass]]"
// Contains a mapping from processID to processClass
extern const KeyRangeRef processClassKeys;
extern const KeyRef processClassPrefix;
extern const KeyRef processClassChangeKey;
extern const KeyRef processClassVersionKey;
extern const ValueRef processClassVersionValue;
const Key processClassKeyFor(StringRef processID );
const Value processClassValue( ProcessClass const& );
Key decodeProcessClassKey( KeyRef const& );
ProcessClass decodeProcessClassValue( ValueRef const& );
UID decodeProcessClassKeyOld( KeyRef const& key );

//   "\xff/conf/[[option]]" := "value"
extern const KeyRangeRef configKeys;
extern const KeyRef configKeysPrefix;

//   "\xff/conf/excluded/1.2.3.4" := ""
//   "\xff/conf/excluded/1.2.3.4:4000" := ""
//   These are inside configKeysPrefix since they represent a form of configuration and they are convenient
//   to track in the same way by the tlog and recovery process, but they are ignored by the DatabaseConfiguration
//   class.
extern const KeyRef excludedServersPrefix;
extern const KeyRangeRef excludedServersKeys;
extern const KeyRef excludedServersVersionKey;  // The value of this key shall be changed by any transaction that modifies the excluded servers list
const AddressExclusion decodeExcludedServersKey( KeyRef const& key ); // where key.startsWith(excludedServersPrefix)
std::string encodeExcludedServersKey( AddressExclusion const& );

extern const KeyRef failedServersPrefix;
extern const KeyRangeRef failedServersKeys;
extern const KeyRef failedServersVersionKey;  // The value of this key shall be changed by any transaction that modifies the failed servers list
const AddressExclusion decodeFailedServersKey( KeyRef const& key ); // where key.startsWith(failedServersPrefix)
std::string encodeFailedServersKey( AddressExclusion const& );

extern const KeyRef readTxnLifetimeKey;
const Version decodeReadTxnLifetime(ValueRef const& value);
const Value encodeReadTxnLifetime(Version lifetime);

//    "\xff/workers/[[processID]]" := ""
//    Asynchronously updated by the cluster controller, this is a list of fdbserver processes that have joined the cluster
//    and are currently (recently) available
extern const KeyRangeRef workerListKeys;
extern const KeyRef workerListPrefix;
const Key workerListKeyFor(StringRef processID );
const Value workerListValue( ProcessData const& );
Key decodeWorkerListKey( KeyRef const& );
ProcessData decodeWorkerListValue( ValueRef const& );

//    "\xff\x02/backupProgress/[[workerID]]" := "[[WorkerBackupStatus]]"
extern const KeyRangeRef backupProgressKeys;
extern const KeyRef backupProgressPrefix;
const Key backupProgressKeyFor(UID workerID);
const Value backupProgressValue(const WorkerBackupStatus& status);
UID decodeBackupProgressKey(const KeyRef& key);
WorkerBackupStatus decodeBackupProgressValue(const ValueRef& value);

// The key to signal backup workers a new backup job is submitted.
//    "\xff\x02/backupStarted" := "[[vector<UID,Version1>]]"
extern const KeyRef backupStartedKey;
Value encodeBackupStartedValue(const std::vector<std::pair<UID, Version>>& ids);
std::vector<std::pair<UID, Version>> decodeBackupStartedValue(const ValueRef& value);

// The key to signal backup workers that they should pause or resume.
//    "\xff\x02/backupPaused" := "[[0|1]]"
extern const KeyRef backupPausedKey;

extern const KeyRef coordinatorsKey;
extern const KeyRef logsKey;
extern const KeyRef minRequiredCommitVersionKey;

const Value logsValue( const vector<std::pair<UID, NetworkAddress>>& logs, const vector<std::pair<UID, NetworkAddress>>& oldLogs );
std::pair<vector<std::pair<UID, NetworkAddress>>,vector<std::pair<UID, NetworkAddress>>> decodeLogsValue( const ValueRef& value );

// The "global keys" are send to each storage server any time they are changed
extern const KeyRef globalKeysPrefix;
extern const KeyRef lastEpochEndKey;
extern const KeyRef lastEpochEndPrivateKey;
extern const KeyRef killStorageKey;
extern const KeyRef killStoragePrivateKey;
extern const KeyRef rebootWhenDurableKey;
extern const KeyRef rebootWhenDurablePrivateKey;
extern const KeyRef primaryLocalityKey;
extern const KeyRef primaryLocalityPrivateKey;
extern const KeyRef fastLoggingEnabled;
extern const KeyRef fastLoggingEnabledPrivateKey;

extern const KeyRef moveKeysLockOwnerKey, moveKeysLockWriteKey;

extern const KeyRef dataDistributionModeKey;
extern const UID dataDistributionModeLock;

// Keys to view and control tag throttling
extern const KeyRangeRef tagThrottleKeys;
extern const KeyRef tagThrottleKeysPrefix;
extern const KeyRef tagThrottleAutoKeysPrefix;
extern const KeyRef tagThrottleSignalKey;
extern const KeyRef tagThrottleAutoEnabledKey;
extern const KeyRef tagThrottleLimitKey;
extern const KeyRef tagThrottleCountKey;

// Log Range constant variables
// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]), IncludeVersion() )
extern const KeyRangeRef logRangesRange;

// Returns the encoded key comprised of begin key and log uid
Key logRangesEncodeKey(KeyRef keyBegin, UID logUid);

// Returns the start key and optionally the logRange Uid
KeyRef logRangesDecodeKey(KeyRef key, UID* logUid = NULL);

// Returns the end key and optionally the key prefix
Key logRangesDecodeValue(KeyRef keyValue, Key* destKeyPrefix = NULL);

// Returns the encoded key value comprised of the end key and destination prefix
Key logRangesEncodeValue(KeyRef keyEnd, KeyRef destPath);

// Returns a key prefixed with the specified key with
// the given uid encoded at the end
Key uidPrefixKey(KeyRef keyPrefix, UID logUid);

/// Apply mutations constant variables

// applyMutationsEndRange.end defines the highest version for which we have mutations that we can
// apply to our database as part of a DR/restore operation.
// \xff/applyMutationsEnd/[16-byte UID] := serialize( endVersion, Unversioned() )
extern const KeyRangeRef applyMutationsEndRange;

// applyMutationsBeginRange.begin defines the highest version of what has already been applied by a
// DR/restore to the database, and thus also what version is of the next mutation that needs to be
// applied to the database.
// \xff/applyMutationsBegin/[16-byte UID] := serialize( beginVersion, Unversioned() )
extern const KeyRangeRef applyMutationsBeginRange;

// \xff/applyMutationsAddPrefix/[16-byte UID] := addPrefix
extern const KeyRangeRef applyMutationsAddPrefixRange;

// \xff/applyMutationsRemovePrefix/[16-byte UID] := removePrefix
extern const KeyRangeRef applyMutationsRemovePrefixRange;

extern const KeyRangeRef applyMutationsKeyVersionMapRange;
extern const KeyRangeRef applyMutationsKeyVersionCountRange;

// FdbClient Info prefix
extern const KeyRangeRef fdbClientInfoPrefixRange;
extern const KeyRef fdbClientInfoTxnSampleRate;
extern const KeyRef fdbClientInfoTxnSizeLimit;

// Consistency Check settings
extern const KeyRef fdbShouldConsistencyCheckBeSuspended;

// Request latency measurement key
extern const KeyRef latencyBandConfigKey;

// Keyspace to maintain wall clock to version map
extern const KeyRangeRef timeKeeperPrefixRange;
extern const KeyRef timeKeeperVersionKey;
extern const KeyRef timeKeeperDisableKey;

// Layer status metadata prefix
extern const KeyRangeRef layerStatusMetaPrefixRange;

// Backup agent status root
extern const KeyRangeRef backupStatusPrefixRange;

// Key range reserved by file backup agent to storing configuration and state information
extern const KeyRangeRef fileBackupPrefixRange;

// Key range reserved by file restore agent (currently part of backup agent functionally separate) for storing configuration and state information
extern const KeyRangeRef fileRestorePrefixRange;

// Key range reserved by database backup agent to storing configuration and state information
extern const KeyRangeRef databaseBackupPrefixRange;

extern const KeyRef destUidLookupPrefix;
extern const KeyRef backupLatestVersionsPrefix;

// Key range reserved by backup agent to storing mutations
extern const KeyRangeRef backupLogKeys;
extern const KeyRangeRef applyLogKeys;

extern const KeyRef backupVersionKey;
extern const ValueRef backupVersionValue;
extern const int backupVersion;
static const int backupLogPrefixBytes = 8;

// Use for legacy system support (pre 300)
extern const KeyRef backupEnabledKey;

extern const KeyRef systemTuplesPrefix;
extern const KeyRef metricConfChangeKey;

extern const KeyRangeRef metricConfKeys;
extern const KeyRef metricConfPrefix;
//const Key metricConfKey( KeyRef const& prefix, struct MetricNameRef const& name, KeyRef const& key );
//std::pair<struct MetricNameRef, KeyRef> decodeMetricConfKey( KeyRef const& prefix, KeyRef const& key );

extern const KeyRef maxUIDKey;

extern const KeyRef databaseLockedKey;
extern const KeyRef metadataVersionKey;
extern const KeyRef metadataVersionKeyEnd;
extern const KeyRef metadataVersionRequiredValue;
extern const KeyRef mustContainSystemMutationsKey;

// Key range reserved for storing changes to monitor conf files
extern const KeyRangeRef monitorConfKeys;

// Fast restore
extern const KeyRef restoreLeaderKey;
extern const KeyRangeRef restoreWorkersKeys;
extern const KeyRef restoreStatusKey; // To be used when we measure fast restore performance
extern const KeyRef restoreRequestTriggerKey;
extern const KeyRef restoreRequestDoneKey;
extern const KeyRangeRef restoreRequestKeys;
extern const KeyRangeRef restoreApplierKeys;
extern const KeyRef restoreApplierTxnValue;

const Key restoreApplierKeyFor(UID const& applierID, int64_t batchIndex, Version version);
std::tuple<UID, int64_t, Version> decodeRestoreApplierKey(ValueRef const& key);
const Key restoreWorkerKeyFor(UID const& workerID);
const Value restoreWorkerInterfaceValue(RestoreWorkerInterface const& server);
RestoreWorkerInterface decodeRestoreWorkerInterfaceValue(ValueRef const& value);
const Value restoreRequestTriggerValue(UID randomUID, int const numRequests);
int decodeRestoreRequestTriggerValue(ValueRef const& value);
const Value restoreRequestDoneVersionValue(Version readVersion);
Version decodeRestoreRequestDoneVersionValue(ValueRef const& value);
const Key restoreRequestKeyFor(int const& index);
const Value restoreRequestValue(RestoreRequest const& server);
RestoreRequest decodeRestoreRequestValue(ValueRef const& value);
const Key restoreStatusKeyFor(StringRef statusType);
const Value restoreStatusValue(double val);

extern const KeyRef healthyZoneKey;
extern const StringRef ignoreSSFailuresZoneString;
extern const KeyRef rebalanceDDIgnoreKey;

const Value healthyZoneValue( StringRef const& zoneId, Version version );
std::pair<Key,Version> decodeHealthyZoneValue( ValueRef const& );

// All mutations done to this range are blindly copied into txnStateStore.
// Used to create artificially large txnStateStore instances in testing.
extern const KeyRangeRef testOnlyTxnStateStorePrefixRange;

#pragma clang diagnostic pop

#endif
