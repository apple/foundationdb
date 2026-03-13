/*
 * SystemData.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/StorageServerInterface.h"

// Don't warn on constants being defined in this file.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"

FDB_BOOLEAN_PARAM(AssignEmptyRange);
FDB_BOOLEAN_PARAM(UnassignShard);
FDB_BOOLEAN_PARAM(EnablePhysicalShardMove);
FDB_BOOLEAN_PARAM(ConductBulkLoad);

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

extern KeyRangeRef const normalKeys; // '' to systemKeys.begin
extern KeyRangeRef const systemKeys; // [FF] to [FF][FF]
extern KeyRangeRef const nonMetadataSystemKeys; // [FF][00] to [FF][01]
extern KeyRangeRef const allKeys; // '' to systemKeys.end
extern KeyRangeRef const specialKeys; // [FF][FF] to [FF][FF][FF], some client functions are exposed through FDB calls
                                      // using these special keys, see pr#2662
extern KeyRef const afterAllKeys;

//    "\xff/keyServers/[[begin]]" := "[[vector<serverID>, std::vector<serverID>]|[vector<Tag>, std::vector<Tag>]]"
//	An internal mapping of where shards are located in the database. [[begin]] is the start of the shard range
//	and the result is a list of serverIDs or Tags where these shards are located. These values can be changed
//	as data movement occurs.
// With ShardEncodeLocationMetaData, the encoding format is:
//    "\xff/keyServers/[[begin]]" := "[[std::vector<serverID>, std::vector<serverID>], srcID, destID]", where srcID
//  and destID are the source and destination `shard id`, respectively.
extern KeyRangeRef const keyServersKeys, keyServersKeyServersKeys;
extern KeyRef const keyServersPrefix, keyServersEnd, keyServersKeyServersKey;

// Used during the transition to the new location metadata format with shard IDs.
// If `SHARD_ENCODE_LOCATION_METADATA` is enabled, any shard that doesn't have a shard ID will be assigned this
// temporary ID, until a permanent ID is assigned to it.
extern UID const anonymousShardId;
extern uint64_t const assignedEmptyShardId;
Key keyServersKey(KeyRef const& k);
KeyRef keyServersKey(KeyRef const& k, Arena& arena);
Value keyServersValue(RangeResult result,
                      std::vector<UID> const& src,
                      std::vector<UID> const& dest = std::vector<UID>());
Value keyServersValue(std::vector<Tag> const& srcTag, std::vector<Tag> const& destTag = std::vector<Tag>());
Value keyServersValue(std::vector<UID> const& src, std::vector<UID> const& dest, UID const& srcID, UID const& destID);
// `result` must be the full result of getting serverTagKeys
void decodeKeyServersValue(RangeResult result,
                           ValueRef const& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest,
                           bool missingIsError = true);
void decodeKeyServersValue(std::map<Tag, UID> const& tag_uid,
                           ValueRef const& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest);
void decodeKeyServersValue(RangeResult result,
                           ValueRef const& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest,
                           UID& srcID,
                           UID& destID,
                           bool missingIsError = true);
bool isSystemKey(KeyRef key);

extern KeyRef const accumulativeChecksumKey;
Value accumulativeChecksumValue(AccumulativeChecksumState const& acsState);
AccumulativeChecksumState decodeAccumulativeChecksum(ValueRef const& value);

extern KeyRangeRef const auditKeys;
extern KeyRef const auditPrefix;
extern KeyRangeRef const auditRanges;
extern KeyRef const auditRangePrefix;

// Key for a particular audit
Key auditKey(AuditType const type, UID const& auditId);
// KeyRange for whole audit
KeyRange auditKeyRange(AuditType const type);
// Prefix for audit work progress by range
Key auditRangeBasedProgressPrefixFor(AuditType const type, UID const& auditId);
// Range for audit work progress by range
KeyRange auditRangeBasedProgressRangeFor(AuditType const type, UID const& auditId);
KeyRange auditRangeBasedProgressRangeFor(AuditType const type);
// Prefix for audit work progress by server
Key auditServerBasedProgressPrefixFor(AuditType const type, UID const& auditId, UID const& serverId);
// Range for audit work progress by server
KeyRange auditServerBasedProgressRangeFor(AuditType const type, UID const& auditId);
KeyRange auditServerBasedProgressRangeFor(AuditType const type);

Value auditStorageStateValue(AuditStorageState const& auditStorageState);
AuditStorageState decodeAuditStorageState(ValueRef const& value);

// "\xff/checkpoint/[[UID]] := [[CheckpointMetaData]]"
extern KeyRef const checkpointPrefix;
Key checkpointKeyFor(UID checkpointID);
Value checkpointValue(CheckpointMetaData const& checkpoint);
UID decodeCheckpointKey(KeyRef const& key);
CheckpointMetaData decodeCheckpointValue(ValueRef const& value);

// "\xff/dataMoves/[[UID]] := [[DataMoveMetaData]]"
extern KeyRangeRef const dataMoveKeys;
Key dataMoveKeyFor(UID dataMoveId);
Value dataMoveValue(DataMoveMetaData const& dataMove);
UID decodeDataMoveKey(KeyRef const& key);
DataMoveMetaData decodeDataMoveValue(ValueRef const& value);

//    "\xff/serverKeys/[[serverID]]/[[begin]]" := "[[serverKeysTrue]]" |" [[serverKeysFalse]]"
//	An internal mapping of what shards any given server currently has ownership of
//	Using the serverID as a prefix, then followed by the beginning of the shard range
//	as the key, the value indicates whether the shard does or does not exist on the server.
//	These values can be changed as data movement occurs.
extern KeyRangeRef const serverKeysRange;
extern KeyRef const serverKeysPrefix;
extern ValueRef const serverKeysTrue, serverKeysTrueEmptyRange, serverKeysFalse;
UID newDataMoveId(uint64_t const physicalShardId,
                  AssignEmptyRange assignEmptyRange,
                  DataMoveType const type,
                  DataMovementReason const reason,
                  UnassignShard unassignShard = UnassignShard::False);
Key serverKeysKey(UID serverID, KeyRef const& keys);
Key serverKeysPrefixFor(UID serverID);
UID serverKeysDecodeServer(KeyRef const& key);
std::pair<UID, Key> serverKeysDecodeServerBegin(KeyRef const& key);
bool serverHasKey(ValueRef storedValue);
Value serverKeysValue(UID const& id);
void decodeDataMoveId(UID const& id,
                      bool& assigned,
                      bool& emptyRange,
                      DataMoveType& dataMoveType,
                      DataMovementReason& dataMoveReason);
void decodeServerKeysValue(ValueRef const& value,
                           bool& assigned,
                           bool& emptyRange,
                           DataMoveType& dataMoveType,
                           UID& id,
                           DataMovementReason& dataMoveReason);

extern KeyRangeRef const conflictingKeysRange;
extern ValueRef const conflictingKeysTrue, conflictingKeysFalse;
extern KeyRangeRef const writeConflictRangeKeysRange;
extern KeyRangeRef const readConflictRangeKeysRange;
extern KeyRangeRef const ddStatsRange;

// "\xff/tss/[[serverId]]" := "[[tssId]]"
extern KeyRangeRef const tssMappingKeys;

// "\xff/tssQ/[[serverId]]" := ""
// For quarantining a misbehaving TSS.
extern KeyRangeRef const tssQuarantineKeys;

Key tssQuarantineKeyFor(UID serverID);
UID decodeTssQuarantineKey(KeyRef const&);

// \xff/tssMismatch/[[Tuple<TSSStorageUID, timestamp, mismatchUID>]] := [[TraceEventString]]
// For recording tss mismatch details in the system keyspace
extern KeyRangeRef const tssMismatchKeys;

// \xff/serverMetadata/[[storageInterfaceUID]] = [[StorageMetadataType]]
// Note: storageInterfaceUID is the one stated in the file name
extern KeyRangeRef const serverMetadataKeys;

// Any update to serverMetadataKeys will update this key to a random UID.
extern KeyRef const serverMetadataChangeKey;

UID decodeServerMetadataKey(KeyRef const&);
StorageMetadataType decodeServerMetadataValue(KeyRef const&);

// "\xff/serverTag/[[serverID]]" = "[[Tag]]"
//	Provides the Tag for the given serverID. Used to access a
//	storage server's corresponding TLog in order to apply mutations.
extern KeyRangeRef const serverTagKeys;
extern KeyRef const serverTagPrefix;
extern KeyRangeRef const serverTagMaxKeys;
extern KeyRangeRef const serverTagConflictKeys;
extern KeyRef const serverTagConflictPrefix;
extern KeyRangeRef const serverTagHistoryKeys;
extern KeyRef const serverTagHistoryPrefix;

Key serverTagKeyFor(UID serverID);
Key serverTagHistoryKeyFor(UID serverID);
KeyRange serverTagHistoryRangeFor(UID serverID);
KeyRange serverTagHistoryRangeBefore(UID serverID, Version version);
Value serverTagValue(Tag);
UID decodeServerTagKey(KeyRef const&);
Version decodeServerTagHistoryKey(KeyRef const&);
Tag decodeServerTagValue(ValueRef const&);
Key serverTagConflictKeyFor(Tag);

//    "\xff/tagLocalityList/[[datacenterID]]" := "[[tagLocality]]"
//	Provides the tagLocality for the given datacenterID
//	See "FDBTypes.h" struct Tag for more details on tagLocality
extern KeyRangeRef const tagLocalityListKeys;
extern KeyRef const tagLocalityListPrefix;
Key tagLocalityListKeyFor(Optional<Value> dcID);
Value tagLocalityListValue(int8_t const&);
Optional<Value> decodeTagLocalityListKey(KeyRef const&);
int8_t decodeTagLocalityListValue(ValueRef const&);

//    "\xff\x02/datacenterReplicas/[[datacenterID]]" := "[[replicas]]"
//	Provides the number of replicas for the given datacenterID.
//	Used in the initialization of the Data Distributor.
extern KeyRangeRef const datacenterReplicasKeys;
extern KeyRef const datacenterReplicasPrefix;
Key datacenterReplicasKeyFor(Optional<Value> dcID);
Value datacenterReplicasValue(int const&);
Optional<Value> decodeDatacenterReplicasKey(KeyRef const&);
int decodeDatacenterReplicasValue(ValueRef const&);

//    "\xff\x02/tLogDatacenters/[[datacenterID]]"
//	The existence of an empty string as a value signifies that the datacenterID is valid
//	(as opposed to having no value at all)
extern KeyRangeRef const tLogDatacentersKeys;
extern KeyRef const tLogDatacentersPrefix;
Key tLogDatacentersKeyFor(Optional<Value> dcID);
Optional<Value> decodeTLogDatacentersKey(KeyRef const&);

extern KeyRef const primaryDatacenterKey;

//    "\xff/serverList/[[serverID]]" := "[[StorageServerInterface]]"
// Storage servers are listed here when they are recruited - always before assigning them keys
// Storage servers removed from here are never replaced.  The same fdbserver, if re-recruited, will always
//    have a new ID.  When removed from here, a storage server may release all resources and destroy itself.
extern KeyRangeRef const serverListKeys;
extern KeyRef const serverListPrefix;
Key serverListKeyFor(UID serverID);
Value serverListValue(StorageServerInterface const&);
UID decodeServerListKey(KeyRef const&);
StorageServerInterface decodeServerListValue(ValueRef const&);

Value swVersionValue(SWVersion const& swversion);
SWVersion decodeSWVersionValue(ValueRef const&);

//    "\xff/processClass/[[processID]]" := "[[ProcessClass]]"
// Contains a mapping from processID to processClass
extern KeyRangeRef const processClassKeys;
extern KeyRef const processClassPrefix;
extern KeyRef const processClassChangeKey;
extern KeyRef const processClassVersionKey;
extern ValueRef const processClassVersionValue;
Key processClassKeyFor(StringRef processID);
Value processClassValue(ProcessClass const&);
Key decodeProcessClassKey(KeyRef const&);
ProcessClass decodeProcessClassValue(ValueRef const&);
UID decodeProcessClassKeyOld(KeyRef const& key);

//   "\xff/conf/[[option]]" := "value"
//	An umbrella prefix for options mostly used by the DatabaseConfiguration class.
//	See DatabaseConfiguration.cpp ::setInternal for more examples.
extern KeyRangeRef const configKeys;
extern KeyRef const configKeysPrefix;

extern KeyRef const backupWorkerEnabledKey;
extern KeyRef const perpetualStorageWiggleKey;
extern KeyRef const perpetualStorageWiggleLocalityKey;
extern KeyRef const perpetualStorageWiggleIDPrefix;
extern KeyRef const perpetualStorageWiggleStatsPrefix;
extern KeyRef const perpetualStorageWigglePrefix;

// Change the value of this key to anything and that will trigger detailed data distribution team info log.
extern KeyRef const triggerDDTeamInfoPrintKey;

// Encryption data at-rest config key
extern KeyRef const encryptionAtRestModeConfKey;

//	The differences between excluded and failed can be found in "command-line-interface.rst"
//	and in the help message of the fdbcli command "exclude".

//   "\xff/conf/excluded/1.2.3.4" := ""
//   "\xff/conf/excluded/1.2.3.4:4000" := ""
//   These are inside configKeysPrefix since they represent a form of configuration and they are convenient
//   to track in the same way by the tlog and recovery process, but they are ignored by the DatabaseConfiguration
//   class.
//	 The existence of an empty string as a value signifies that the provided IP has been excluded.
//	 (as opposed to having no value at all)
extern KeyRef const excludedServersPrefix;
extern KeyRangeRef const excludedServersKeys;
extern KeyRef const excludedServersVersionKey; // The value of this key shall be changed by any transaction that
                                               // modifies the excluded servers list
AddressExclusion decodeExcludedServersKey(KeyRef const& key); // where key.startsWith(excludedServersPrefix)
std::string encodeExcludedServersKey(AddressExclusion const&);

extern KeyRef const excludedLocalityPrefix;
extern KeyRangeRef const excludedLocalityKeys;
extern KeyRef const excludedLocalityVersionKey; // The value of this key shall be changed by any transaction that
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
extern KeyRef const failedServersPrefix;
extern KeyRangeRef const failedServersKeys;
extern KeyRef const failedServersVersionKey; // The value of this key shall be changed by any transaction that modifies
                                             // the failed servers list
AddressExclusion decodeFailedServersKey(KeyRef const& key); // where key.startsWith(failedServersPrefix)
std::string encodeFailedServersKey(AddressExclusion const&);

extern KeyRef const failedLocalityPrefix;
extern KeyRangeRef const failedLocalityKeys;
extern KeyRef const failedLocalityVersionKey; // The value of this key shall be changed by any transaction that modifies
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
extern KeyRangeRef const globalConfigDataKeys;
extern KeyRef const globalConfigKeysPrefix;

//   "\xff/globalConfig/h/[[version]]" := "value"
//   Maps a commit version to a list of mutations made to the global
//   configuration at that commit. Shipped to nodes periodically. In general,
//   clients should not write to keys in this keyspace; it will be written
//   automatically when updating global configuration keys.
extern KeyRangeRef const globalConfigHistoryKeys;
extern KeyRef const globalConfigHistoryPrefix;

//   "\xff/globalConfig/v" := "version"
//   Read-only key which returns the commit version of the most recent mutation
//   made to the global configuration keyspace.
extern KeyRef const globalConfigVersionKey;

//	"\xff/workers/[[processID]]" := ""
//	Asynchronously updated by the cluster controller, this is a list of fdbserver processes that have joined the cluster
//	and are currently (recently) available
extern KeyRangeRef const workerListKeys;
extern KeyRef const workerListPrefix;
Key workerListKeyFor(StringRef processID);
Value workerListValue(ProcessData const&);
Key decodeWorkerListKey(KeyRef const&);
ProcessData decodeWorkerListValue(ValueRef const&);

//	"\xff\x02/backupProgress/[[workerID]]" := "[[WorkerBackupStatus]]"
//	Provides the progress for the given backup worker.
//	See "FDBTypes.h" struct WorkerBackupStatus for more details on the return type value.
extern KeyRangeRef const backupProgressKeys;
extern KeyRef const backupProgressPrefix;
Key backupProgressKeyFor(UID workerID);
Value backupProgressValue(WorkerBackupStatus const& status);
UID decodeBackupProgressKey(KeyRef const& key);
WorkerBackupStatus decodeBackupProgressValue(ValueRef const& value);

// The key to signal when partition map has been uploaded for a given version.
//    "\xff\x02/backupRangePartitionedMapUploaded/<version>" := "1"
extern KeyRef const backupRangePartitionedMapUploadedPrefix;
Key backupRangePartitionedMapUploadedKeyFor(Version v);

// The key to signal backup workers a new backup job is submitted.
//    "\xff\x02/backupStarted" := "[[vector<UID,Version1>]]"
extern KeyRef const backupStartedKey;
Value encodeBackupStartedValue(std::vector<std::pair<UID, Version>> const& ids);
std::vector<std::pair<UID, Version>> decodeBackupStartedValue(ValueRef const& value);

// The key to signal backup workers that they should resume or pause.
//    "\xff\x02/backupPaused" := "[[0|1]]"
// 0 = Send a signal to resume/already resumed.
// 1 = Send a signal to pause/already paused.
extern KeyRef const backupPausedKey;

//	"\xff/previousCoordinators" = "[[ClusterConnectionString]]"
//	Set to the encoded structure of the cluster's previous set of coordinators.
//	Changed when performing quorumChange.
//	See "CoordinationInterface.h" struct ClusterConnectionString for more details
extern KeyRef const previousCoordinatorsKey;

//	"\xff/coordinators" = "[[ClusterConnectionString]]"
//	Set to the encoded structure of the cluster's current set of coordinators.
//	Changed when performing quorumChange.
//	See "CoordinationInterface.h" struct ClusterConnectionString for more details
extern KeyRef const coordinatorsKey;

//	"\xff/logs" = "[[LogsValue]]"
//	Used during cluster recovery in order to communicate
//	and store info about the logs system.
extern KeyRef const logsKey;

//	"\xff/minRequiredCommitVersion" = "[[Version]]"
//	Used during backup/recovery to restrict version requirements
extern KeyRef const minRequiredCommitVersionKey;

//	"\xff/versionEpochKey" = "[[uint64_t]]"
//	Defines the base epoch representing version 0. The value itself is the
//	number of microseconds since the Unix epoch.
extern KeyRef const versionEpochKey;

Value logsValue(std::vector<std::pair<UID, NetworkAddress>> const& logs,
                std::vector<std::pair<UID, NetworkAddress>> const& oldLogs);
std::pair<std::vector<std::pair<UID, NetworkAddress>>, std::vector<std::pair<UID, NetworkAddress>>> decodeLogsValue(
    ValueRef const& value);

// The "global keys" are sent to each storage server any time they are changed
extern KeyRef const globalKeysPrefix;
extern KeyRef const lastEpochEndKey;
extern KeyRef const lastEpochEndPrivateKey;
// Checks whether the mutation "m" is a SetValue for the key
bool mutationForKey(MutationRef const& m, KeyRef const& key);
extern KeyRef const killStorageKey;
extern KeyRef const killStoragePrivateKey;
extern KeyRef const rebootWhenDurableKey;
extern KeyRef const rebootWhenDurablePrivateKey;
extern KeyRef const primaryLocalityKey;
extern KeyRef const primaryLocalityPrivateKey;
extern KeyRef const fastLoggingEnabled;
extern KeyRef const fastLoggingEnabledPrivateKey;

extern KeyRef const moveKeysLockOwnerKey, moveKeysLockWriteKey;

extern KeyRef const dataDistributionModeKey;
extern UID const dataDistributionModeLock;

extern KeyRef const bulkLoadModeKey;
extern KeyRangeRef const bulkLoadTaskKeys;
extern KeyRef const bulkLoadTaskPrefix;
Value bulkLoadTaskStateValue(BulkLoadTaskState const& bulkLoadTaskState);
BulkLoadTaskState decodeBulkLoadTaskState(ValueRef const& value);

Value ssBulkLoadMetadataValue(SSBulkLoadMetadata const& ssBulkLoadMetadata);
SSBulkLoadMetadata decodeSSBulkLoadMetadata(ValueRef const& value);

extern KeyRangeRef const bulkLoadJobKeys;
extern KeyRef const bulkLoadJobPrefix;
Value bulkLoadJobValue(BulkLoadJobState const& bulkLoadJobState);
BulkLoadJobState decodeBulkLoadJobState(ValueRef const& value);

extern KeyRangeRef const bulkLoadJobHistoryKeys;
extern KeyRef const bulkLoadJobHistoryPrefix;
Key bulkLoadJobHistoryKeyFor(UID const& jobId);

extern KeyRef const bulkDumpModeKey;
extern KeyRangeRef const bulkDumpKeys;
extern KeyRef const bulkDumpPrefix;
Value bulkDumpStateValue(BulkDumpState const& bulkDumpState);
BulkDumpState decodeBulkDumpState(ValueRef const& value);

extern std::string const rangeLockNameForBulkLoad;
extern KeyRangeRef const rangeLockKeys;
extern KeyRef const rangeLockPrefix;
Value rangeLockStateSetValue(RangeLockStateSet const& rangeLockStateSet);
RangeLockStateSet decodeRangeLockStateSet(ValueRef const& value);

extern KeyRangeRef const rangeLockOwnerKeys;
extern KeyRef const rangeLockOwnerPrefix;
Key rangeLockOwnerKeyFor(RangeLockOwnerName const& ownerUniqueID);
Value rangeLockOwnerValue(RangeLockOwner const& rangeLockOwner);
RangeLockOwner decodeRangeLockOwner(ValueRef const& value);

// Keys to view and control tag throttling
extern KeyRangeRef const tagThrottleKeys;
extern KeyRef const tagThrottleKeysPrefix;
extern KeyRef const tagThrottleAutoKeysPrefix;
extern KeyRef const tagThrottleSignalKey;
extern KeyRef const tagThrottleAutoEnabledKey;
extern KeyRef const tagThrottleLimitKey;
extern KeyRef const tagThrottleCountKey;
extern KeyRangeRef const tagQuotaKeys;
extern KeyRef const tagQuotaPrefix;

// Log Range constant variables
// Used in the backup pipeline to track mutations
// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]),
// IncludeVersion() )
extern KeyRangeRef const logRangesRange;

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
extern KeyRangeRef const applyMutationsEndRange;

// applyMutationsBeginRange.begin defines the highest version of what has already been applied by a
// DR/restore to the database, and thus also what version is of the next mutation that needs to be
// applied to the database.
// \xff/applyMutationsBegin/[16-byte UID] := serialize( beginVersion, Unversioned() )
extern KeyRangeRef const applyMutationsBeginRange;

// \xff/applyMutationsAddPrefix/[16-byte UID] := addPrefix
extern KeyRangeRef const applyMutationsAddPrefixRange;

// \xff/applyMutationsRemovePrefix/[16-byte UID] := removePrefix
extern KeyRangeRef const applyMutationsRemovePrefixRange;

extern KeyRangeRef const applyMutationsKeyVersionMapRange;
extern KeyRangeRef const applyMutationsKeyVersionCountRange;

// FdbClient Info prefix
extern KeyRangeRef const fdbClientInfoPrefixRange;

// Consistency Check settings
extern KeyRef const fdbShouldConsistencyCheckBeSuspended;

// Request latency measurement key
extern KeyRef const latencyBandConfigKey;

// Keyspace to maintain wall clock to version map
extern KeyRangeRef const timeKeeperPrefixRange;
extern KeyRef const timeKeeperVersionKey;
extern KeyRef const timeKeeperDisableKey;

// Durable cluster ID key
extern KeyRef const clusterIdKey;

// Layer status metadata prefix
extern KeyRangeRef const layerStatusMetaPrefixRange;

// Backup agent status root
extern KeyRangeRef const backupStatusPrefixRange;

// Key range reserved by file backup agent to storing configuration and state information
extern KeyRangeRef const fileBackupPrefixRange;

// Key range reserved by file restore agent (currently part of backup agent functionally separate) for storing
// configuration and state information
extern KeyRangeRef const fileRestorePrefixRange;

// Key range reserved by database backup agent to storing configuration and state information
extern KeyRangeRef const databaseBackupPrefixRange;

extern KeyRef const destUidLookupPrefix;
extern KeyRef const backupLatestVersionsPrefix;

// Key range reserved by backup agent to storing mutations
extern KeyRangeRef const backupLogKeys;
extern KeyRangeRef const applyLogKeys;
// Key range reserved for restore validation data storage (system key space)
extern KeyRangeRef const validateRestoreLogKeys;
// Returns true if m is a blog (backup log) or alog (apply log) mutation
bool isBackupLogMutation(MutationRef const& m);

// Returns true if m is an acs mutation: a mutation carrying accumulative checksum value
bool isAccumulativeChecksumMutation(MutationRef const& m);

extern KeyRef const backupVersionKey;
extern ValueRef const backupVersionValue;
extern int const backupVersion;
static int const backupLogPrefixBytes = 8;

// Use for legacy system support (pre 300)
extern KeyRef const backupEnabledKey;

extern KeyRef const systemTuplesPrefix;
extern KeyRef const metricConfChangeKey;

extern KeyRangeRef const metricConfKeys;
extern KeyRef const metricConfPrefix;
// const Key metricConfKey( KeyRef const& prefix, struct MetricNameRef const& name, KeyRef const& key );
// std::pair<struct MetricNameRef, KeyRef> decodeMetricConfKey( KeyRef const& prefix, KeyRef const& key );

extern KeyRef const maxUIDKey;

extern KeyRef const databaseLockedKey;
extern KeyRef const databaseLockedKeyEnd;
extern KeyRef const metadataVersionKey;
extern KeyRef const metadataVersionKeyEnd;
extern KeyRef const metadataVersionRequiredValue;
extern KeyRef const mustContainSystemMutationsKey;

// Key range reserved for storing changes to monitor conf files
extern KeyRangeRef const monitorConfKeys;

extern KeyRef const healthyZoneKey;
extern StringRef const ignoreSSFailuresZoneString;
extern KeyRef const rebalanceDDIgnoreKey;
namespace DDIgnore {
enum IgnoreType : uint8_t { NONE = 0, REBALANCE_DISK = 1, REBALANCE_READ = 2, ALL = 3 };
}

Value healthyZoneValue(StringRef const& zoneId, Version version);
std::pair<Key, Version> decodeHealthyZoneValue(ValueRef const&);

// All mutations done to this range are blindly copied into txnStateStore.
// Used to create artificially large txnStateStore instances in testing.
extern KeyRangeRef const testOnlyTxnStateStorePrefixRange;

// Snapshot + Incremental Restore

//	"\xff/writeRecovery" = "[[writeRecoveryKeyTrue]]"
//	Flag used for the snapshot-restore pipeline in order to avoid
//	anomalous behaviour with multiple recoveries.
extern KeyRef const writeRecoveryKey;
extern ValueRef const writeRecoveryKeyTrue;

//	"\xff/snapshotEndVersion" = "[[Version]]"
//	Written by master server during recovery if recovering from a snapshot.
//	Allows incremental restore to read and set starting version for consistency.
extern KeyRef const snapshotEndVersionKey;

extern KeyRangeRef const idempotencyIdKeys;
extern KeyRef const idempotencyIdsExpiredVersion;

#pragma clang diagnostic pop

#endif
