/*
 * ManagementAPI.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_MANAGEMENT_API_ACTOR_G_H)
#define FDBCLIENT_MANAGEMENT_API_ACTOR_G_H
#include "fdbclient/ManagementAPI.actor.g.h"
#elif !defined(FDBCLIENT_MANAGEMENT_API_ACTOR_H)
#define FDBCLIENT_MANAGEMENT_API_ACTOR_H

/* This file defines "management" interfaces for configuration, coordination changes, and
the inclusion and exclusion of servers. It is used to implement fdbcli management commands
and by test workloads that simulate such. It isn't exposed to C clients or anywhere outside
our code base and doesn't need to be versioned. It doesn't do anything you can't do with the
standard API and some knowledge of the contents of the system key space.
*/

#include <string>
#include <map>
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<DatabaseConfiguration> getDatabaseConfiguration(Transaction* tr, bool useSystemPriority = false);
ACTOR Future<DatabaseConfiguration> getDatabaseConfiguration(Database cx, bool useSystemPriority = false);
ACTOR Future<Void> waitForFullReplication(Database cx);

struct IQuorumChange : ReferenceCounted<IQuorumChange> {
	virtual ~IQuorumChange() {}
	virtual Future<std::vector<NetworkAddress>> getDesiredCoordinators(Transaction* tr,
	                                                                   std::vector<NetworkAddress> oldCoordinators,
	                                                                   Reference<IClusterConnectionRecord>,
	                                                                   CoordinatorsResult&) = 0;
	virtual std::string getDesiredClusterKeyName() const { return std::string(); }
};

// Change to use the given set of coordination servers
ACTOR Future<Optional<CoordinatorsResult>> changeQuorumChecker(Transaction* tr,
                                                               ClusterConnectionString* conn,
                                                               std::string newName,
                                                               bool disableConfigDB);
ACTOR Future<CoordinatorsResult> changeQuorum(Database cx, Reference<IQuorumChange> change);
Reference<IQuorumChange> autoQuorumChange(int desired = -1);
Reference<IQuorumChange> nameQuorumChange(std::string const& name, Reference<IQuorumChange> const& other);

// Exclude the given set of servers from use as state servers.  Returns as soon as the change is durable, without
// necessarily waiting for the servers to be evacuated.  A NetworkAddress with a port of 0 means all servers on the
// given IP.
ACTOR Future<Void> excludeServers(Database cx, std::vector<AddressExclusion> servers, bool failed = false);
ACTOR Future<Void> excludeServers(Transaction* tr, std::vector<AddressExclusion> servers, bool failed = false);

// Exclude the servers matching the given set of localities from use as state servers.  Returns as soon as the change
// is durable, without necessarily waiting for the servers to be evacuated.
ACTOR Future<Void> excludeLocalities(Database cx, std::unordered_set<std::string> localities, bool failed = false);
ACTOR Future<Void> excludeLocalities(Transaction* tr, std::unordered_set<std::string> localities, bool failed = false);

// Remove the given servers from the exclusion list.  A NetworkAddress with a port of 0 means all servers on the given
// IP.  A NetworkAddress() means all servers (don't exclude anything)
ACTOR Future<Void> includeServers(Database cx, std::vector<AddressExclusion> servers, bool failed = false);

// Remove the given localities from the exclusion list.
ACTOR Future<Void> includeLocalities(Database cx,
                                     std::vector<std::string> localities,
                                     bool failed = false,
                                     bool includeAll = false);

// Set the process class of processes with the given address.  A NetworkAddress with a port of 0 means all servers on
// the given IP.
ACTOR Future<Void> setClass(Database cx, AddressExclusion server, ProcessClass processClass);

// Get the current list of excluded servers including both "exclude" and "failed".
ACTOR Future<std::vector<AddressExclusion>> getAllExcludedServers(Database cx);
ACTOR Future<std::vector<AddressExclusion>> getAllExcludedServers(Transaction* tr);

// Get the current list of excluded servers.
ACTOR Future<std::vector<AddressExclusion>> getExcludedServerList(Transaction* tr);

// Get the current list of failed servers.
ACTOR Future<std::vector<AddressExclusion>> getExcludedFailedServerList(Transaction* tr);

// Get the current list of excluded localities
ACTOR Future<std::vector<std::string>> getAllExcludedLocalities(Database cx);
ACTOR Future<std::vector<std::string>> getAllExcludedLocalities(Transaction* tr);

// Get the current list of excluded localities.
ACTOR Future<std::vector<std::string>> getExcludedLocalityList(Transaction* tr);

// Get the current list of failed localities.
ACTOR Future<std::vector<std::string>> getExcludedFailedLocalityList(Transaction* tr);

// Decodes the locality string to a pair of locality prefix and its value.
// The prefix could be dcid, processid, machineid, processid.
std::pair<std::string, std::string> decodeLocality(const std::string& locality);
std::set<AddressExclusion> getServerAddressesByLocality(
    const std::map<std::string, StorageServerInterface> server_interfaces,
    const std::string& locality);
std::set<AddressExclusion> getAddressesByLocality(const std::vector<ProcessData>& workers, const std::string& locality);

// Check for the given, previously excluded servers to be evacuated (no longer used for state).  If waitForExclusion
// is true, this actor returns once it is safe to shut down all such machines without impacting fault tolerance,
// until and unless any of them are explicitly included with includeServers()
ACTOR Future<std::set<NetworkAddress>> checkForExcludingServers(Database cx,
                                                                std::vector<AddressExclusion> servers,
                                                                bool waitForAllExcluded);
ACTOR Future<bool> checkForExcludingServersTxActor(ReadYourWritesTransaction* tr,
                                                   std::set<AddressExclusion>* exclusions,
                                                   std::set<NetworkAddress>* inProgressExclusion);

// Gets a list of all workers in the cluster (excluding testers)
ACTOR Future<std::vector<ProcessData>> getWorkers(Database cx);
ACTOR Future<std::vector<ProcessData>> getWorkers(Transaction* tr);

ACTOR Future<Void> timeKeeperSetDisable(Database cx);

ACTOR Future<Void> lockDatabase(Transaction* tr, UID id);
ACTOR Future<Void> lockDatabase(Reference<ReadYourWritesTransaction> tr, UID id);
ACTOR Future<Void> lockDatabase(Database cx, UID id);

ACTOR Future<Void> unlockDatabase(Transaction* tr, UID id);
ACTOR Future<Void> unlockDatabase(Reference<ReadYourWritesTransaction> tr, UID id);
ACTOR Future<Void> unlockDatabase(Database cx, UID id);

ACTOR Future<Void> checkDatabaseLock(Transaction* tr, UID id);
ACTOR Future<Void> checkDatabaseLock(Reference<ReadYourWritesTransaction> tr, UID id);

ACTOR Future<Void> updateChangeFeed(Transaction* tr, Key rangeID, ChangeFeedStatus status, KeyRange range = KeyRange());
ACTOR Future<Void> updateChangeFeed(Reference<ReadYourWritesTransaction> tr,
                                    Key rangeID,
                                    ChangeFeedStatus status,
                                    KeyRange range = KeyRange());
ACTOR Future<Void> updateChangeFeed(Database cx, Key rangeID, ChangeFeedStatus status, KeyRange range = KeyRange());

ACTOR Future<Void> advanceVersion(Database cx, Version v);

ACTOR Future<int> setDDMode(Database cx, int mode);

ACTOR Future<Void> forceRecovery(Reference<IClusterConnectionRecord> clusterFile, Standalone<StringRef> dcId);

// Start an audit on range of the specific type.
ACTOR Future<UID> auditStorage(Reference<IClusterConnectionRecord> clusterFile,
                               KeyRange range,
                               AuditType type,
                               KeyValueStoreType engineType,
                               double timeoutSeconds);
// Cancel an audit given type and id
ACTOR Future<UID> cancelAuditStorage(Reference<IClusterConnectionRecord> clusterFile,
                                     AuditType type,
                                     UID auditId,
                                     double timeoutSeconds);

// Set bulk load mode
// When the mode is on, DD will periodically check if there is any bulkload task to do by scaning the metadata.
ACTOR Future<int> setBulkLoadMode(Database cx, int mode);

// Get bulk load tasks which range is fully contained by the input range.
// If phase is provided, then return the task with the input phase.
ACTOR Future<std::vector<BulkLoadTaskState>> getBulkLoadTasksWithinRange(
    Database cx,
    KeyRange rangeToRead,
    size_t limit = 10,
    Optional<BulkLoadPhase> phase = Optional<BulkLoadPhase>());

// Create a bulkload task submission transaction without commit
// Used by ManagementAPI and bulkdumpRestore at DD
ACTOR Future<Void> setBulkLoadSubmissionTransaction(Transaction* tr, BulkLoadTaskState bulkLoadTask);

// Submit a bulk load task
ACTOR Future<Void> submitBulkLoadTask(Database cx, BulkLoadTaskState bulkLoadTask);

// Create an bulkload task acknowledge transaction without commit
// Used by ManagementAPI and bulkdumpRestore at DD
ACTOR Future<Void> setBulkLoadFinalizeTransaction(Transaction* tr, KeyRange range, UID taskId);

// Finalize a bulk load task if it has been completed
ACTOR Future<Void> finalizeBulkLoadTask(Database cx, KeyRange range, UID taskId);

// Get bulk load task for the input range and taskId
ACTOR Future<BulkLoadTaskState> getBulkLoadTask(Transaction* tr,
                                                KeyRange range,
                                                UID taskId,
                                                std::vector<BulkLoadPhase> phases);

// Submit a BulkLoad job: loading data from a remote folder using bulkloading mechanism.
// There is at most one BulkLoad or one BulkDump job at a time.
// If there is any existing BulkLoad or BulkDump job, reject the new job.
ACTOR Future<Void> submitBulkLoadJob(Database cx, BulkLoadJobState jobState);

// Create a transaction for updating bulkload metadata
// Return true if did the update, otherwise, return false
ACTOR Future<Void> updateBulkLoadJobMetadataTransaction(Transaction* tr, BulkLoadJobState jobState);

// TODO(BulkLoad): Cancel or clear the BulkLoad job
ACTOR Future<Void> clearBulkLoadJob(Database cx, UID jobId);

// Get alive bulkload job
ACTOR Future<Optional<BulkLoadJobState>> getAliveBulkLoadJob(Transaction* tr);

ACTOR Future<Optional<BulkLoadJobState>> getAliveBulkLoadJob(Database cx);

// Set bulk dump mode. When the mode is on, DD will periodically check if there is any bulkdump task to do by scaning
// the metadata.
ACTOR Future<int> setBulkDumpMode(Database cx, int mode);

// Get bulk dump mode value.
ACTOR Future<int> getBulkDumpMode(Database cx);

// TODO(BulkDump): Cancel or clear the BulkDump job
ACTOR Future<Void> clearBulkDumpJob(Database cx, UID jobId);

// Check if the input bulkLoad job is in complete state. Throw bulkload_task_outdated if the job has been cleared.
// A job is complete if and only if all tasks have been completed.
ACTOR Future<bool> checkBulkLoadJobComplete(Database cx, UID jobId);

// Submit a bulkdump job: dumping data to a remote folder by storage servers.
// There is at most one BulkLoad or one BulkDump job at a time.
// If there is any existing BulkLoad or BulkDump job, reject the new job.
ACTOR Future<Void> submitBulkDumpJob(Database cx, BulkDumpState bulkDumpJob);

// Return the existing Job ID
ACTOR Future<Optional<UID>> getAliveBulkDumpJob(Transaction* tr);

// Get total number of completed tasks within the input range
ACTOR Future<size_t> getBulkDumpCompleteTaskCount(Database cx, KeyRange rangeToRead);

// Persist a rangeLock owner to database metadata
// A range can only be locked by a registered owner
ACTOR Future<Void> registerRangeLockOwner(Database cx, std::string uniqueId, std::string description);

// Remove an owner form the database metadata
ACTOR Future<Void> removeRangeLockOwner(Database cx, std::string uniqueId);

// Get all registered rangeLock owner
ACTOR Future<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx);

ACTOR Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, std::string uniqueId);

// Turn off user traffic for bulk load based on range lock
ACTOR Future<Void> turnOffUserWriteTrafficForBulkLoad(Transaction* tr, KeyRange range);

// Turn on user traffic for bulk load based on range lock
ACTOR Future<Void> turnOnUserWriteTrafficForBulkLoad(Transaction* tr, KeyRange range);

// Lock a user range (the input range must be within normalKeys)
ACTOR Future<Void> takeReadLockOnRange(Database cx, KeyRange range, std::string ownerUniqueID);

// Unlock a user range (the input range must be within normalKeys)
ACTOR Future<Void> releaseReadLockOnRange(Database cx, KeyRange range, std::string ownerUniqueID);

// Get locked ranges within the input range (the input range must be within normalKeys)
ACTOR Future<std::vector<KeyRange>> getReadLockOnRange(Database cx, KeyRange range);

ACTOR Future<Void> printHealthyZone(Database cx);
ACTOR Future<bool> clearHealthyZone(Database cx, bool printWarning = false, bool clearSSFailureZoneString = false);
ACTOR Future<bool> setHealthyZone(Database cx, StringRef zoneId, double seconds, bool printWarning = false);

ACTOR Future<Void> waitForPrimaryDC(Database cx, StringRef dcId);

// Gets the cluster connection string
ACTOR Future<Optional<ClusterConnectionString>> getConnectionString(Database cx);

void schemaCoverage(std::string const& spath, bool covered = true);
bool schemaMatch(json_spirit::mValue const& schema,
                 json_spirit::mValue const& result,
                 std::string& errorStr,
                 Severity sev = SevError,
                 bool checkCoverage = false,
                 std::string path = std::string(),
                 std::string schema_path = std::string());

// execute payload in 'snapCmd' on all the coordinators, TLogs and
// storage nodes
ACTOR Future<Void> mgmtSnapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID);

#include "flow/unactorcompiler.h"
#endif
