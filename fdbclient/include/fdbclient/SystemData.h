/*
 * SystemData.h
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

#ifndef FDBCLIENT_SYSTEMDATA_H
#define FDBCLIENT_SYSTEMDATA_H
#pragma once

// Functions and constants documenting the organization of the reserved keyspace in the database beginning with "\xFF"

#include "fdbclient/AccumulativeChecksum.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/BlobWorkerInterface.h" // TODO move the functions that depend on this out of here and into BlobWorkerInterface.h to remove this dependency
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/Tenant.h"

// Don't warn on constants being defined in this file.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"

FDB_BOOLEAN_PARAM(AssignEmptyRange);
FDB_BOOLEAN_PARAM(UnassignShard);
FDB_BOOLEAN_PARAM(EnablePhysicalShardMove);

enum class DataMoveType : uint8_t {
	LOGICAL = 0,
	PHYSICAL = 1,
	PHYSICAL_EXP = 2,
	LOGICAL_BULKLOAD = 3,
	PHYSICAL_BULKLOAD = 4,
	NUMBER_OF_TYPES = 5,
};

// One-to-one relationship to the priority knobs
enum class DataMovementReason : uint8_t {
	INVALID = 0,
	RECOVER_MOVE = 1,
	REBALANCE_UNDERUTILIZED_TEAM = 2,
	REBALANCE_OVERUTILIZED_TEAM = 3,
	REBALANCE_READ_OVERUTIL_TEAM = 4,
	REBALANCE_READ_UNDERUTIL_TEAM = 5,
	PERPETUAL_STORAGE_WIGGLE = 6,
	TEAM_HEALTHY = 7,
	TEAM_CONTAINS_UNDESIRED_SERVER = 8,
	TEAM_REDUNDANT = 9,
	MERGE_SHARD = 10,
	POPULATE_REGION = 11,
	TEAM_UNHEALTHY = 12,
	TEAM_2_LEFT = 13,
	TEAM_1_LEFT = 14,
	TEAM_FAILED = 15,
	TEAM_0_LEFT = 16,
	SPLIT_SHARD = 17,
	ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD = 18,
	REBALANCE_STORAGE_QUEUE = 19,
	ASSIGN_EMPTY_RANGE = 20, // dummy reason, no corresponding data move priority
	SEED_SHARD_SERVER = 21, // dummy reason, no corresponding data move priority
	NUMBER_OF_REASONS = 22, // dummy reason, no corresponding data move priority
};

// SystemKey is just a Key but with a special type so that instances of it can be found easily throughput the code base
// and in simulation constructions will verify that no SystemKey is a direct prefix of any other.
struct SystemKey : Key {
	SystemKey(Key const& k);
};

struct RestoreLoaderInterface;
struct RestoreApplierInterface;
struct RestoreMasterInterface;

extern const KeyRangeRef normalKeys; // '' to systemKeys.begin
extern const KeyRangeRef systemKeys; // [FF] to [FF][FF]
extern const KeyRangeRef nonMetadataSystemKeys; // [FF][00] to [FF][01]
extern const KeyRangeRef allKeys; // '' to systemKeys.end
extern const KeyRangeRef specialKeys; // [FF][FF] to [FF][FF][FF], some client functions are exposed through FDB calls
                                      // using these special keys, see pr#2662
extern const KeyRef afterAllKeys;

//    "\xff/keyServers/[[begin]]" := "[[vector<serverID>, std::vector<serverID>]|[vector<Tag>, std::vector<Tag>]]"
//	An internal mapping of where shards are located in the database. [[begin]] is the start of the shard range
//	and the result is a list of serverIDs or Tags where these shards are located. These values can be changed
//	as data movement occurs.
// With ShardEncodeLocationMetaData, the encoding format is:
//    "\xff/keyServers/[[begin]]" := "[[std::vector<serverID>, std::vector<serverID>], srcID, destID]", where srcID
//  and destID are the source and destination `shard id`, respectively.
extern const KeyRangeRef keyServersKeys, keyServersKeyServersKeys;
extern const KeyRef keyServersPrefix, keyServersEnd, keyServersKeyServersKey;

// Used during the transition to the new location metadata format with shard IDs.
// If `SHARD_ENCODE_LOCATION_METADATA` is enabled, any shard that doesn't have a shard ID will be assigned this
// temporary ID, until a permanent ID is assigned to it.
extern const UID anonymousShardId;
extern const uint64_t assignedEmptyShardId;
const Key keyServersKey(const KeyRef& k);
const KeyRef keyServersKey(const KeyRef& k, Arena& arena);
const Value keyServersValue(RangeResult result,
                            const std::vector<UID>& src,
                            const std::vector<UID>& dest = std::vector<UID>());
const Value keyServersValue(const std::vector<Tag>& srcTag, const std::vector<Tag>& destTag = std::vector<Tag>());
const Value keyServersValue(const std::vector<UID>& src,
                            const std::vector<UID>& dest,
                            const UID& srcID,
                            const UID& destID);
// `result` must be the full result of getting serverTagKeys
void decodeKeyServersValue(RangeResult result,
                           const ValueRef& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest,
                           bool missingIsError = true);
void decodeKeyServersValue(std::map<Tag, UID> const& tag_uid,
                           const ValueRef& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest);
void decodeKeyServersValue(RangeResult result,
                           const ValueRef& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest,
                           UID& srcID,
                           UID& destID,
                           bool missingIsError = true);
bool isSystemKey(KeyRef key);

extern const KeyRef accumulativeChecksumKey;
const Value accumulativeChecksumValue(const AccumulativeChecksumState& acsState);
AccumulativeChecksumState decodeAccumulativeChecksum(const ValueRef& value);

extern const KeyRangeRef auditKeys;
extern const KeyRef auditPrefix;
extern const KeyRangeRef auditRanges;
extern const KeyRef auditRangePrefix;

// Key for a particular audit
const Key auditKey(const AuditType type, const UID& auditId);
// KeyRange for whole audit
const KeyRange auditKeyRange(const AuditType type);
// Prefix for audit work progress by range
const Key auditRangeBasedProgressPrefixFor(const AuditType type, const UID& auditId);
// Range for audit work progress by range
const KeyRange auditRangeBasedProgressRangeFor(const AuditType type, const UID& auditId);
const KeyRange auditRangeBasedProgressRangeFor(const AuditType type);
// Prefix for audit work progress by server
const Key auditServerBasedProgressPrefixFor(const AuditType type, const UID& auditId, const UID& serverId);
// Range for audit work progress by server
const KeyRange auditServerBasedProgressRangeFor(const AuditType type, const UID& auditId);
const KeyRange auditServerBasedProgressRangeFor(const AuditType type);

const Value auditStorageStateValue(const AuditStorageState& auditStorageState);
AuditStorageState decodeAuditStorageState(const ValueRef& value);

// "\xff/checkpoint/[[UID]] := [[CheckpointMetaData]]"
extern const KeyRef checkpointPrefix;
const Key checkpointKeyFor(UID checkpointID);
const Value checkpointValue(const CheckpointMetaData& checkpoint);
UID decodeCheckpointKey(const KeyRef& key);
CheckpointMetaData decodeCheckpointValue(const ValueRef& value);

// "\xff/dataMoves/[[UID]] := [[DataMoveMetaData]]"
extern const KeyRangeRef dataMoveKeys;
const Key dataMoveKeyFor(UID dataMoveId);
const Value dataMoveValue(const DataMoveMetaData& dataMove);
UID decodeDataMoveKey(const KeyRef& key);
DataMoveMetaData decodeDataMoveValue(const ValueRef& value);

// "\xff/storageCacheServer/[[UID]] := StorageServerInterface"
// This will be added by the cache server on initialization and removed by DD
// TODO[mpilman]: We will need a way to map uint16_t ids to UIDs in a future
//                versions. For now caches simply cache everything so the ids
//                are not yet meaningful.
extern const KeyRangeRef storageCacheServerKeys;
extern const KeyRef storageCacheServersPrefix, storageCacheServersEnd;
const Key storageCacheServerKey(UID id);
const Value storageCacheServerValue(const StorageServerInterface& ssi);

//    "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
extern const KeyRangeRef storageCacheKeys;
extern const KeyRef storageCachePrefix;
const Key storageCacheKey(const KeyRef& k);
const Value storageCacheValue(const std::vector<uint16_t>& serverIndices);
void decodeStorageCacheValue(const ValueRef& value, std::vector<uint16_t>& serverIndices);

//    "\xff/serverKeys/[[serverID]]/[[begin]]" := "[[serverKeysTrue]]" |" [[serverKeysFalse]]"
//	An internal mapping of what shards any given server currently has ownership of
//	Using the serverID as a prefix, then followed by the beginning of the shard range
//	as the key, the value indicates whether the shard does or does not exist on the server.
//	These values can be changed as data movement occurs.
extern const KeyRangeRef serverKeysRange;
extern const KeyRef serverKeysPrefix;
extern const ValueRef serverKeysTrue, serverKeysTrueEmptyRange, serverKeysFalse;
const UID newDataMoveId(const uint64_t physicalShardId,
                        AssignEmptyRange assignEmptyRange,
                        const DataMoveType type,
                        const DataMovementReason reason,
                        UnassignShard unassignShard = UnassignShard::False);
const Key serverKeysKey(UID serverID, const KeyRef& keys);
const Key serverKeysPrefixFor(UID serverID);
UID serverKeysDecodeServer(const KeyRef& key);
std::pair<UID, Key> serverKeysDecodeServerBegin(const KeyRef& key);
bool serverHasKey(ValueRef storedValue);
const Value serverKeysValue(const UID& id);
void decodeDataMoveId(const UID& id,
                      bool& assigned,
                      bool& emptyRange,
                      DataMoveType& dataMoveType,
                      DataMovementReason& dataMoveReason);
void decodeServerKeysValue(const ValueRef& value,
                           bool& assigned,
                           bool& emptyRange,
                           DataMoveType& dataMoveType,
                           UID& id,
                           DataMovementReason& dataMoveReason);

extern const KeyRangeRef conflictingKeysRange;
extern const ValueRef conflictingKeysTrue, conflictingKeysFalse;
extern const KeyRangeRef writeConflictRangeKeysRange;
extern const KeyRangeRef readConflictRangeKeysRange;
extern const KeyRangeRef ddStatsRange;

extern const KeyRef cacheKeysPrefix;

const Key cacheKeysKey(uint16_t idx, const KeyRef& key);
const Key cacheKeysPrefixFor(uint16_t idx);
uint16_t cacheKeysDecodeIndex(const KeyRef& key);
KeyRef cacheKeysDecodeKey(const KeyRef& key);

extern const KeyRef cacheChangeKey;
extern const KeyRangeRef cacheChangeKeys;
extern const KeyRef cacheChangePrefix;
const Key cacheChangeKeyFor(uint16_t idx);
uint16_t cacheChangeKeyDecodeIndex(const KeyRef& key);

// "\xff/tss/[[serverId]]" := "[[tssId]]"
extern const KeyRangeRef tssMappingKeys;

// "\xff/tssQ/[[serverId]]" := ""
// For quarantining a misbehaving TSS.
extern const KeyRangeRef tssQuarantineKeys;

const Key tssQuarantineKeyFor(UID serverID);
UID decodeTssQuarantineKey(KeyRef const&);

// \xff/tssMismatch/[[Tuple<TSSStorageUID, timestamp, mismatchUID>]] := [[TraceEventString]]
// For recording tss mismatch details in the system keyspace
extern const KeyRangeRef tssMismatchKeys;

// \xff/serverMetadata/[[storageInterfaceUID]] = [[StorageMetadataType]]
// Note: storageInterfaceUID is the one stated in the file name
extern const KeyRangeRef serverMetadataKeys;

// Any update to serverMetadataKeys will update this key to a random UID.
extern const KeyRef serverMetadataChangeKey;

UID decodeServerMetadataKey(const KeyRef&);
StorageMetadataType decodeServerMetadataValue(const KeyRef&);

// "\xff/serverTag/[[serverID]]" = "[[Tag]]"
//	Provides the Tag for the given serverID. Used to access a
//	storage server's corresponding TLog in order to apply mutations.
extern const KeyRangeRef serverTagKeys;
extern const KeyRef serverTagPrefix;
extern const KeyRangeRef serverTagMaxKeys;
extern const KeyRangeRef serverTagConflictKeys;
extern const KeyRef serverTagConflictPrefix;
extern const KeyRangeRef serverTagHistoryKeys;
extern const KeyRef serverTagHistoryPrefix;

const Key serverTagKeyFor(UID serverID);
const Key serverTagHistoryKeyFor(UID serverID);
const KeyRange serverTagHistoryRangeFor(UID serverID);
const KeyRange serverTagHistoryRangeBefore(UID serverID, Version version);
const Value serverTagValue(Tag);
UID decodeServerTagKey(KeyRef const&);
Version decodeServerTagHistoryKey(KeyRef const&);
Tag decodeServerTagValue(ValueRef const&);
const Key serverTagConflictKeyFor(Tag);

//    "\xff/tagLocalityList/[[datacenterID]]" := "[[tagLocality]]"
//	Provides the tagLocality for the given datacenterID
//	See "FDBTypes.h" struct Tag for more details on tagLocality
extern const KeyRangeRef tagLocalityListKeys;
extern const KeyRef tagLocalityListPrefix;
const Key tagLocalityListKeyFor(Optional<Value> dcID);
const Value tagLocalityListValue(int8_t const&);
Optional<Value> decodeTagLocalityListKey(KeyRef const&);
int8_t decodeTagLocalityListValue(ValueRef const&);

//    "\xff\x02/datacenterReplicas/[[datacenterID]]" := "[[replicas]]"
//	Provides the number of replicas for the given datacenterID.
//	Used in the initialization of the Data Distributor.
extern const KeyRangeRef datacenterReplicasKeys;
extern const KeyRef datacenterReplicasPrefix;
const Key datacenterReplicasKeyFor(Optional<Value> dcID);
const Value datacenterReplicasValue(int const&);
Optional<Value> decodeDatacenterReplicasKey(KeyRef const&);
int decodeDatacenterReplicasValue(ValueRef const&);

//    "\xff\x02/tLogDatacenters/[[datacenterID]]"
//	The existence of an empty string as a value signifies that the datacenterID is valid
//	(as opposed to having no value at all)
extern const KeyRangeRef tLogDatacentersKeys;
extern const KeyRef tLogDatacentersPrefix;
const Key tLogDatacentersKeyFor(Optional<Value> dcID);
Optional<Value> decodeTLogDatacentersKey(KeyRef const&);

extern const KeyRef primaryDatacenterKey;

//    "\xff/serverList/[[serverID]]" := "[[StorageServerInterface]]"
// Storage servers are listed here when they are recruited - always before assigning them keys
// Storage servers removed from here are never replaced.  The same fdbserver, if re-recruited, will always
//    have a new ID.  When removed from here, a storage server may release all resources and destroy itself.
extern const KeyRangeRef serverListKeys;
extern const KeyRef serverListPrefix;
const Key serverListKeyFor(UID serverID);
const Value serverListValue(StorageServerInterface const&);
UID decodeServerListKey(KeyRef const&);
StorageServerInterface decodeServerListValue(ValueRef const&);

Value swVersionValue(SWVersion const& swversion);
SWVersion decodeSWVersionValue(ValueRef const&);

//    "\xff/processClass/[[processID]]" := "[[ProcessClass]]"
// Contains a mapping from processID to processClass
extern const KeyRangeRef processClassKeys;
extern const KeyRef processClassPrefix;
extern const KeyRef processClassChangeKey;
extern const KeyRef processClassVersionKey;
extern const ValueRef processClassVersionValue;
const Key processClassKeyFor(StringRef processID);
const Value processClassValue(ProcessClass const&);
Key decodeProcessClassKey(KeyRef const&);
ProcessClass decodeProcessClassValue(ValueRef const&);
UID decodeProcessClassKeyOld(KeyRef const& key);

//   "\xff/conf/[[option]]" := "value"
//	An umbrella prefix for options mostly used by the DatabaseConfiguration class.
//	See DatabaseConfiguration.cpp ::setInternal for more examples.
extern const KeyRangeRef configKeys;
extern const KeyRef configKeysPrefix;

extern const KeyRef perpetualStorageWiggleKey;
extern const KeyRef perpetualStorageWiggleLocalityKey;
extern const KeyRef perpetualStorageWiggleIDPrefix;
extern const KeyRef perpetualStorageWiggleStatsPrefix;
extern const KeyRef perpetualStorageWigglePrefix;

// Change the value of this key to anything and that will trigger detailed data distribution team info log.
extern const KeyRef triggerDDTeamInfoPrintKey;

// Encryption data at-rest config key
extern const KeyRef encryptionAtRestModeConfKey;

// Tenant mode config key
extern const KeyRef tenantModeConfKey;

//	The differences between excluded and failed can be found in "command-line-interface.rst"
//	and in the help message of the fdbcli command "exclude".

//   "\xff/conf/excluded/1.2.3.4" := ""
//   "\xff/conf/excluded/1.2.3.4:4000" := ""
//   These are inside configKeysPrefix since they represent a form of configuration and they are convenient
//   to track in the same way by the tlog and recovery process, but they are ignored by the DatabaseConfiguration
//   class.
//	 The existence of an empty string as a value signifies that the provided IP has been excluded.
//	 (as opposed to having no value at all)
extern const KeyRef excludedServersPrefix;
extern const KeyRangeRef excludedServersKeys;
extern const KeyRef excludedServersVersionKey; // The value of this key shall be changed by any transaction that
                                               // modifies the excluded servers list
AddressExclusion decodeExcludedServersKey(KeyRef const& key); // where key.startsWith(excludedServersPrefix)
std::string encodeExcludedServersKey(AddressExclusion const&);

extern const KeyRef excludedLocalityPrefix;
extern const KeyRangeRef excludedLocalityKeys;
extern const KeyRef excludedLocalityVersionKey; // The value of this key shall be changed by any transaction that
                                                // modifies the excluded localities list
std::string decodeExcludedLocalityKey(KeyRef const& key); // where key.startsWith(excludedLocalityPrefix)
std::string encodeExcludedLocalityKey(std::string const&);

//   "\xff/conf/failed/1.2.3.4" := ""
//   "\xff/conf/failed/1.2.3.4:4000" := ""
//   These are inside configKeysPrefix since they represent a form of configuration and they are convenient
//   to track in the same way by the tlog and recovery process, but they are ignored by the DatabaseConfiguration
//   class.
//	 The existence of an empty string as a value signifies that the provided IP has been marked as failed.
//	 (as opposed to having no value at all)
extern const KeyRef failedServersPrefix;
extern const KeyRangeRef failedServersKeys;
extern const KeyRef failedServersVersionKey; // The value of this key shall be changed by any transaction that modifies
                                             // the failed servers list
AddressExclusion decodeFailedServersKey(KeyRef const& key); // where key.startsWith(failedServersPrefix)
std::string encodeFailedServersKey(AddressExclusion const&);

extern const KeyRef failedLocalityPrefix;
extern const KeyRangeRef failedLocalityKeys;
extern const KeyRef failedLocalityVersionKey; // The value of this key shall be changed by any transaction that modifies
                                              // the failed localities list
std::string decodeFailedLocalityKey(KeyRef const& key); // where key.startsWith(failedLocalityPrefix)
std::string encodeFailedLocalityKey(std::string const&);

//   "\xff/globalConfig/[[option]]" := "value"
//	 An umbrella prefix for global configuration data synchronized to all nodes.
// extern const KeyRangeRef globalConfigData;
// extern const KeyRef globalConfigDataPrefix;

//   "\xff/globalConfig/k/[[key]]" := "value"
//	 Key-value pairs that have been set. The range this keyspace represents
//	 contains all globally configured options.
extern const KeyRangeRef globalConfigDataKeys;
extern const KeyRef globalConfigKeysPrefix;

//   "\xff/globalConfig/h/[[version]]" := "value"
//   Maps a commit version to a list of mutations made to the global
//   configuration at that commit. Shipped to nodes periodically. In general,
//   clients should not write to keys in this keyspace; it will be written
//   automatically when updating global configuration keys.
extern const KeyRangeRef globalConfigHistoryKeys;
extern const KeyRef globalConfigHistoryPrefix;

//   "\xff/globalConfig/v" := "version"
//   Read-only key which returns the commit version of the most recent mutation
//   made to the global configuration keyspace.
extern const KeyRef globalConfigVersionKey;

//	"\xff/workers/[[processID]]" := ""
//	Asynchronously updated by the cluster controller, this is a list of fdbserver processes that have joined the cluster
//	and are currently (recently) available
extern const KeyRangeRef workerListKeys;
extern const KeyRef workerListPrefix;
const Key workerListKeyFor(StringRef processID);
const Value workerListValue(ProcessData const&);
Key decodeWorkerListKey(KeyRef const&);
ProcessData decodeWorkerListValue(ValueRef const&);

//	"\xff\x02/backupProgress/[[workerID]]" := "[[WorkerBackupStatus]]"
//	Provides the progress for the given backup worker.
//	See "FDBTypes.h" struct WorkerBackupStatus for more details on the return type value.
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

// The key to signal backup workers that they should resume or pause.
//    "\xff\x02/backupPaused" := "[[0|1]]"
// 0 = Send a signal to resume/already resumed.
// 1 = Send a signal to pause/already paused.
extern const KeyRef backupPausedKey;

//	"\xff/previousCoordinators" = "[[ClusterConnectionString]]"
//	Set to the encoded structure of the cluster's previous set of coordinators.
//	Changed when performing quorumChange.
//	See "CoordinationInterface.h" struct ClusterConnectionString for more details
extern const KeyRef previousCoordinatorsKey;

//	"\xff/coordinators" = "[[ClusterConnectionString]]"
//	Set to the encoded structure of the cluster's current set of coordinators.
//	Changed when performing quorumChange.
//	See "CoordinationInterface.h" struct ClusterConnectionString for more details
extern const KeyRef coordinatorsKey;

//	"\xff/logs" = "[[LogsValue]]"
//	Used during cluster recovery in order to communicate
//	and store info about the logs system.
extern const KeyRef logsKey;

//	"\xff/minRequiredCommitVersion" = "[[Version]]"
//	Used during backup/recovery to restrict version requirements
extern const KeyRef minRequiredCommitVersionKey;

//	"\xff/versionEpochKey" = "[[uint64_t]]"
//	Defines the base epoch representing version 0. The value itself is the
//	number of microseconds since the Unix epoch.
extern const KeyRef versionEpochKey;

const Value logsValue(const std::vector<std::pair<UID, NetworkAddress>>& logs,
                      const std::vector<std::pair<UID, NetworkAddress>>& oldLogs);
std::pair<std::vector<std::pair<UID, NetworkAddress>>, std::vector<std::pair<UID, NetworkAddress>>> decodeLogsValue(
    const ValueRef& value);

// The "global keys" are sent to each storage server any time they are changed
extern const KeyRef globalKeysPrefix;
extern const KeyRef lastEpochEndKey;
extern const KeyRef lastEpochEndPrivateKey;
// Checks whether the mutation "m" is a SetValue for the key
bool mutationForKey(const MutationRef& m, const KeyRef& key);
extern const KeyRef killStorageKey;
extern const KeyRef killStoragePrivateKey;
extern const KeyRef rebootWhenDurableKey;
extern const KeyRef rebootWhenDurablePrivateKey;
extern const KeyRef primaryLocalityKey;
extern const KeyRef primaryLocalityPrivateKey;
extern const KeyRef fastLoggingEnabled;
extern const KeyRef fastLoggingEnabledPrivateKey;
extern const KeyRef constructDataKey;

extern const KeyRef moveKeysLockOwnerKey, moveKeysLockWriteKey;

extern const KeyRef dataDistributionModeKey;
extern const UID dataDistributionModeLock;

extern const KeyRef bulkLoadModeKey;
extern const KeyRangeRef bulkLoadKeys;
extern const KeyRef bulkLoadPrefix;
const Value bulkLoadStateValue(const BulkLoadState& bulkLoadState);
BulkLoadState decodeBulkLoadState(const ValueRef& value);

extern const KeyRangeRef bulkDumpKeys;
extern const KeyRef bulkDumpPrefix;
const Value bulkDumpStateValue(const BulkDumpState& bulkDumpState);
BulkDumpState decodeBulkDumpState(const ValueRef& value);

extern const std::string rangeLockNameForBulkLoad;
extern const KeyRangeRef rangeLockKeys;
extern const KeyRef rangeLockPrefix;
const Value rangeLockStateSetValue(const RangeLockStateSet& rangeLockStateSet);
RangeLockStateSet decodeRangeLockStateSet(const ValueRef& value);

extern const KeyRangeRef rangeLockOwnerKeys;
extern const KeyRef rangeLockOwnerPrefix;
const Key rangeLockOwnerKeyFor(const RangeLockOwnerName& ownerUniqueID);
const RangeLockOwnerName decodeRangeLockOwnerKey(const KeyRef& key);
const Value rangeLockOwnerValue(const RangeLockOwner& rangeLockOwner);
RangeLockOwner decodeRangeLockOwner(const ValueRef& value);

// Keys to view and control tag throttling
extern const KeyRangeRef tagThrottleKeys;
extern const KeyRef tagThrottleKeysPrefix;
extern const KeyRef tagThrottleAutoKeysPrefix;
extern const KeyRef tagThrottleSignalKey;
extern const KeyRef tagThrottleAutoEnabledKey;
extern const KeyRef tagThrottleLimitKey;
extern const KeyRef tagThrottleCountKey;
extern const KeyRangeRef tagQuotaKeys;
extern const KeyRef tagQuotaPrefix;

// Log Range constant variables
// Used in the backup pipeline to track mutations
// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]),
// IncludeVersion() )
extern const KeyRangeRef logRangesRange;

// Returns the encoded key comprised of begin key and log uid
Key logRangesEncodeKey(KeyRef keyBegin, UID logUid);

// Returns the start key and optionally the logRange Uid
KeyRef logRangesDecodeKey(KeyRef key, UID* logUid = nullptr);

// Returns the end key and optionally the key prefix
Key logRangesDecodeValue(KeyRef keyValue, Key* destKeyPrefix = nullptr);

// Returns the encoded key value comprised of the end key and destination prefix
Key logRangesEncodeValue(KeyRef keyEnd, KeyRef destPath);

// Returns a key prefixed with the specified key with
// the given uid encoded at the end
Key uidPrefixKey(KeyRef keyPrefix, UID logUid);

extern std::tuple<Standalone<StringRef>, uint64_t, uint64_t, uint64_t> decodeConstructKeys(ValueRef value);
extern Value encodeConstructValue(StringRef keyStart, uint64_t valSize, uint64_t keyCount, uint64_t seed);

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

// Consistency Check settings
extern const KeyRef fdbShouldConsistencyCheckBeSuspended;

// Request latency measurement key
extern const KeyRef latencyBandConfigKey;

// Keyspace to maintain wall clock to version map
extern const KeyRangeRef timeKeeperPrefixRange;
extern const KeyRef timeKeeperVersionKey;
extern const KeyRef timeKeeperDisableKey;

// Durable cluster ID key
extern const KeyRef clusterIdKey;

// Layer status metadata prefix
extern const KeyRangeRef layerStatusMetaPrefixRange;

// Backup agent status root
extern const KeyRangeRef backupStatusPrefixRange;

// Key range reserved by file backup agent to storing configuration and state information
extern const KeyRangeRef fileBackupPrefixRange;

// Key range reserved by file restore agent (currently part of backup agent functionally separate) for storing
// configuration and state information
extern const KeyRangeRef fileRestorePrefixRange;

// Key range reserved by database backup agent to storing configuration and state information
extern const KeyRangeRef databaseBackupPrefixRange;

extern const KeyRef destUidLookupPrefix;
extern const KeyRef backupLatestVersionsPrefix;

// Key range reserved by backup agent to storing mutations
extern const KeyRangeRef backupLogKeys;
extern const KeyRangeRef applyLogKeys;
// Returns true if m is a blog (backup log) or alog (apply log) mutation
bool isBackupLogMutation(const MutationRef& m);

// Returns true if m is an acs mutation: a mutation carrying accumulative checksum value
bool isAccumulativeChecksumMutation(const MutationRef& m);

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
// const Key metricConfKey( KeyRef const& prefix, struct MetricNameRef const& name, KeyRef const& key );
// std::pair<struct MetricNameRef, KeyRef> decodeMetricConfKey( KeyRef const& prefix, KeyRef const& key );

extern const KeyRef maxUIDKey;

extern const KeyRef databaseLockedKey;
extern const KeyRef databaseLockedKeyEnd;
extern const KeyRef metadataVersionKey;
extern const KeyRef metadataVersionKeyEnd;
extern const KeyRef metadataVersionRequiredValue;
extern const KeyRef mustContainSystemMutationsKey;

// Key range reserved for storing changes to monitor conf files
extern const KeyRangeRef monitorConfKeys;

extern const KeyRef healthyZoneKey;
extern const StringRef ignoreSSFailuresZoneString;
extern const KeyRef rebalanceDDIgnoreKey;
namespace DDIgnore {
enum IgnoreType : uint8_t { NONE = 0, REBALANCE_DISK = 1, REBALANCE_READ = 2, ALL = 3 };
}

const Value healthyZoneValue(StringRef const& zoneId, Version version);
std::pair<Key, Version> decodeHealthyZoneValue(ValueRef const&);

// All mutations done to this range are blindly copied into txnStateStore.
// Used to create artificially large txnStateStore instances in testing.
extern const KeyRangeRef testOnlyTxnStateStorePrefixRange;

// Snapshot + Incremental Restore

//	"\xff/writeRecovery" = "[[writeRecoveryKeyTrue]]"
//	Flag used for the snapshot-restore pipeline in order to avoid
//	anomalous behaviour with multiple recoveries.
extern const KeyRef writeRecoveryKey;
extern const ValueRef writeRecoveryKeyTrue;

//	"\xff/snapshotEndVersion" = "[[Version]]"
//	Written by master server during recovery if recovering from a snapshot.
//	Allows incremental restore to read and set starting version for consistency.
extern const KeyRef snapshotEndVersionKey;

extern const KeyRangeRef changeFeedKeys;
enum class ChangeFeedStatus { CHANGE_FEED_CREATE = 0, CHANGE_FEED_STOP = 1, CHANGE_FEED_DESTROY = 2 };
const Value changeFeedValue(KeyRangeRef const& range, Version popVersion, ChangeFeedStatus status);
std::tuple<KeyRange, Version, ChangeFeedStatus> decodeChangeFeedValue(ValueRef const& value);
extern const KeyRef changeFeedPrefix;
extern const KeyRef changeFeedPrivatePrefix;

extern const KeyRangeRef changeFeedDurableKeys;
extern const KeyRef changeFeedDurablePrefix;

const Value changeFeedDurableKey(Key const& feed, Version version);
std::pair<Key, Version> decodeChangeFeedDurableKey(ValueRef const& key);
const Value changeFeedDurableValue(Standalone<VectorRef<MutationRef>> const& mutations, Version knownCommittedVersion);
std::pair<Standalone<VectorRef<MutationRef>>, Version> decodeChangeFeedDurableValue(ValueRef const& value);

extern const KeyRangeRef changeFeedCacheKeys;
extern const KeyRef changeFeedCachePrefix;

const Value changeFeedCacheKey(Key const& prefix, Key const& feed, KeyRange const& range, Version version);
std::tuple<Key, KeyRange, Version> decodeChangeFeedCacheKey(Key const& prefix, ValueRef const& key);
const Value changeFeedCacheValue(Standalone<VectorRef<MutationsAndVersionRef>> const& mutations);
Standalone<VectorRef<MutationsAndVersionRef>> decodeChangeFeedCacheValue(ValueRef const& value);

extern const KeyRangeRef changeFeedCacheFeedKeys;
extern const KeyRef changeFeedCacheFeedPrefix;

const Value changeFeedCacheFeedKey(Key const& prefix, Key const& feed, KeyRange const& range);
std::tuple<Key, Key, KeyRange> decodeChangeFeedCacheFeedKey(ValueRef const& key);
const Value changeFeedCacheFeedValue(Version const& version, Version const& popped);
std::pair<Version, Version> decodeChangeFeedCacheFeedValue(ValueRef const& value);

// Configuration database special keys
extern const KeyRef configTransactionDescriptionKey;
extern const KeyRange globalConfigKnobKeys;
extern const KeyRangeRef configKnobKeys;
extern const KeyRangeRef configClassKeys;

// blob range special keys
extern const KeyRef blobRangeChangeKey;
extern const KeyRangeRef blobRangeKeys;
extern const KeyRangeRef blobRangeChangeLogKeys;
extern const KeyRef blobManagerEpochKey;

const Value blobManagerEpochValueFor(int64_t epoch);
int64_t decodeBlobManagerEpochValue(ValueRef const& value);

// blob granule keys
extern const StringRef blobRangeActive;
extern const StringRef blobRangeInactive;

bool isBlobRangeActive(const ValueRef& blobRangeValue);

const Key blobRangeChangeLogReadKeyFor(Version version);
const Value blobRangeChangeLogValueFor(const Standalone<BlobRangeChangeLogRef>& value);
Standalone<BlobRangeChangeLogRef> decodeBlobRangeChangeLogValue(ValueRef const& value);

extern const uint8_t BG_FILE_TYPE_DELTA;
extern const uint8_t BG_FILE_TYPE_SNAPSHOT;

// FIXME: flip order of {filetype, version}
// \xff\x02/bgf/(granuleUID, {snapshot|delta}, fileVersion) = [[filename]]
extern const KeyRangeRef blobGranuleFileKeys;
// \xff\x02/bgm/[[beginKey]] = [[BlobWorkerUID]]
extern const KeyRangeRef blobGranuleMappingKeys;

// \xff\x02/bgl/(beginKey,endKey) = (epoch, seqno, granuleUID)
extern const KeyRangeRef blobGranuleLockKeys;

// \xff\x02/bgs/(parentGranuleUID, granuleUID) = [[BlobGranuleSplitState]]
extern const KeyRangeRef blobGranuleSplitKeys;

// \xff\x02/bgmerge/mergeGranuleId = [[BlobGranuleMergeState]]
extern const KeyRangeRef blobGranuleMergeKeys;

// \xff\x02/bgmergebounds/beginkey = [[BlobGranuleMergeBoundary]]
extern const KeyRangeRef blobGranuleMergeBoundaryKeys;

// \xff\x02/bgh/(beginKey,endKey,startVersion) = { granuleUID, [parentGranuleHistoryKeys] }
extern const KeyRangeRef blobGranuleHistoryKeys;

// \xff\x02/bgp/(start,end) = (version, force)
extern const KeyRangeRef blobGranulePurgeKeys;
// \xff\x02/bgpforce/(start) = {1|0} (key range map)
extern const KeyRangeRef blobGranuleForcePurgedKeys;
extern const KeyRef blobGranulePurgeChangeKey;

const Key blobGranuleFileKeyFor(UID granuleID, Version fileVersion, uint8_t fileType);
std::tuple<UID, Version, uint8_t> decodeBlobGranuleFileKey(KeyRef const& key);
const KeyRange blobGranuleFileKeyRangeFor(UID granuleID);

const Value blobGranuleFileValueFor(
    StringRef const& filename,
    int64_t offset,
    int64_t length,
    int64_t fullFileLength,
    int64_t logicalSize,
    Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta = Optional<BlobGranuleCipherKeysMeta>());
std::tuple<Standalone<StringRef>, int64_t, int64_t, int64_t, int64_t, Optional<BlobGranuleCipherKeysMeta>>
decodeBlobGranuleFileValue(ValueRef const& value);

const Value blobGranulePurgeValueFor(Version version, KeyRange range, bool force);
std::tuple<Version, KeyRange, bool> decodeBlobGranulePurgeValue(ValueRef const& value);

const Value blobGranuleMappingValueFor(UID const& workerID);
UID decodeBlobGranuleMappingValue(ValueRef const& value);

const Key blobGranuleLockKeyFor(KeyRangeRef const& granuleRange);

const Value blobGranuleLockValueFor(int64_t epochNum, int64_t sequenceNum, UID changeFeedId);
std::tuple<int64_t, int64_t, UID> decodeBlobGranuleLockValue(ValueRef const& value);

const Key blobGranuleSplitKeyFor(UID const& parentGranuleID, UID const& granuleID);
std::pair<UID, UID> decodeBlobGranuleSplitKey(KeyRef const& key);
const KeyRange blobGranuleSplitKeyRangeFor(UID const& parentGranuleID);

const Key blobGranuleMergeKeyFor(UID const& mergeGranuleID);
UID decodeBlobGranuleMergeKey(KeyRef const& key);

// these are versionstamped
const Value blobGranuleSplitValueFor(BlobGranuleSplitState st);
std::pair<BlobGranuleSplitState, Version> decodeBlobGranuleSplitValue(ValueRef const& value);

const Value blobGranuleMergeValueFor(KeyRange mergeKeyRange,
                                     std::vector<UID> parentGranuleIDs,
                                     std::vector<Key> parentGranuleRanges,
                                     std::vector<Version> parentGranuleStartVersions);
// FIXME: probably just define object type for this?
std::tuple<KeyRange, Version, std::vector<UID>, std::vector<Key>, std::vector<Version>> decodeBlobGranuleMergeValue(
    ValueRef const& value);

// BlobGranuleMergeBoundary.
const Key blobGranuleMergeBoundaryKeyFor(const KeyRef& key);
const Value blobGranuleMergeBoundaryValueFor(BlobGranuleMergeBoundary const& boundary);
Standalone<BlobGranuleMergeBoundary> decodeBlobGranuleMergeBoundaryValue(const ValueRef& value);

const Key blobGranuleHistoryKeyFor(KeyRangeRef const& range, Version version);
std::pair<KeyRange, Version> decodeBlobGranuleHistoryKey(KeyRef const& key);
const KeyRange blobGranuleHistoryKeyRangeFor(KeyRangeRef const& range);

const Value blobGranuleHistoryValueFor(Standalone<BlobGranuleHistoryValue> const& historyValue);
Standalone<BlobGranuleHistoryValue> decodeBlobGranuleHistoryValue(ValueRef const& value);

// \xff/bwl/[[BlobWorkerID]] = [[BlobWorkerInterface]]
extern const KeyRangeRef blobWorkerListKeys;

const Key blobWorkerListKeyFor(UID workerID);
UID decodeBlobWorkerListKey(KeyRef const& key);
const Value blobWorkerListValue(BlobWorkerInterface const& interface);
BlobWorkerInterface decodeBlobWorkerListValue(ValueRef const& value);

// \xff/bwa/[[BlobWorkerID]] = [[UID]]
extern const KeyRangeRef blobWorkerAffinityKeys;

const Key blobWorkerAffinityKeyFor(UID workerID);
UID decodeBlobWorkerAffinityKey(KeyRef const& key);
const Value blobWorkerAffinityValue(UID const& id);
UID decodeBlobWorkerAffinityValue(ValueRef const& value);

extern const Key blobManifestVersionKey;

extern const KeyRangeRef idempotencyIdKeys;
extern const KeyRef idempotencyIdsExpiredVersion;

#pragma clang diagnostic pop

#endif
