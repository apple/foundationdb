/*
 * ManagementAPI.actor.h
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
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Status.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/actorcompiler.h" // has to be last include

// ConfigurationResult enumerates normal outcomes of changeConfig() and various error
// conditions specific to it.  changeConfig may also throw an Error to report other problems.
enum class ConfigurationResult {
	NO_OPTIONS_PROVIDED,
	CONFLICTING_OPTIONS,
	UNKNOWN_OPTION,
	INCOMPLETE_CONFIGURATION,
	INVALID_CONFIGURATION,
	DATABASE_ALREADY_CREATED,
	DATABASE_CREATED,
	DATABASE_UNAVAILABLE,
	STORAGE_IN_UNKNOWN_DCID,
	REGION_NOT_FULLY_REPLICATED,
	MULTIPLE_ACTIVE_REGIONS,
	REGIONS_CHANGED,
	NOT_ENOUGH_WORKERS,
	REGION_REPLICATION_MISMATCH,
	DCID_MISSING,
	LOCKED_NOT_NEW,
	SUCCESS_WARN_PPW_GRADUAL,
	SUCCESS_WARN_CHANGE_STORAGE_NOMIGRATE,
	SUCCESS,
};

enum class CoordinatorsResult {
	INVALID_NETWORK_ADDRESSES,
	SAME_NETWORK_ADDRESSES,
	NOT_COORDINATORS, // FIXME: not detected
	DATABASE_UNREACHABLE, // FIXME: not detected
	BAD_DATABASE_STATE,
	COORDINATOR_UNREACHABLE,
	NOT_ENOUGH_MACHINES,
	SUCCESS
};

struct ConfigureAutoResult {
	std::map<NetworkAddress, ProcessClass> address_class;
	int32_t processes;
	int32_t machines;

	std::string old_replication;
	int32_t old_commit_proxies;
	int32_t old_grv_proxies;
	int32_t old_resolvers;
	int32_t old_logs;
	int32_t old_processes_with_transaction;
	int32_t old_machines_with_transaction;

	std::string auto_replication;
	int32_t auto_commit_proxies;
	int32_t auto_grv_proxies;
	int32_t auto_resolvers;
	int32_t auto_logs;
	int32_t auto_processes_with_transaction;
	int32_t auto_machines_with_transaction;

	int32_t desired_commit_proxies;
	int32_t desired_grv_proxies;
	int32_t desired_resolvers;
	int32_t desired_logs;

	ConfigureAutoResult()
	  : processes(-1), machines(-1), old_commit_proxies(-1), old_grv_proxies(-1), old_resolvers(-1), old_logs(-1),
	    old_processes_with_transaction(-1), old_machines_with_transaction(-1), auto_commit_proxies(-1),
	    auto_grv_proxies(-1), auto_resolvers(-1), auto_logs(-1), auto_processes_with_transaction(-1),
	    auto_machines_with_transaction(-1), desired_commit_proxies(-1), desired_grv_proxies(-1), desired_resolvers(-1),
	    desired_logs(-1) {}

	bool isValid() const { return processes != -1; }
};

ConfigurationResult buildConfiguration(
    std::vector<StringRef> const& modeTokens,
    std::map<std::string, std::string>& outConf); // Accepts a vector of configuration tokens
ConfigurationResult buildConfiguration(
    std::string const& modeString,
    std::map<std::string, std::string>& outConf); // Accepts tokens separated by spaces in a single string

bool isCompleteConfiguration(std::map<std::string, std::string> const& options);

// All versions of changeConfig apply the given set of configuration tokens to the database, and return a
// ConfigurationResult (or error).
Future<ConfigurationResult> changeConfig(Database const& cx,
                                         std::string const& configMode,
                                         bool force); // Accepts tokens separated by spaces in a single string

ConfigureAutoResult parseConfig(StatusObject const& status);
Future<ConfigurationResult> changeConfig(Database const& cx,
                                         std::vector<StringRef> const& modes,
                                         Optional<ConfigureAutoResult> const& conf,
                                         bool force); // Accepts a vector of configuration tokens
ACTOR Future<ConfigurationResult> changeConfig(
    Database cx,
    std::map<std::string, std::string> m,
    bool force); // Accepts a full configuration in key/value format (from buildConfiguration)

ACTOR Future<DatabaseConfiguration> getDatabaseConfiguration(Database cx);
ACTOR Future<Void> waitForFullReplication(Database cx);

struct IQuorumChange : ReferenceCounted<IQuorumChange> {
	virtual ~IQuorumChange() {}
	virtual Future<std::vector<NetworkAddress>> getDesiredCoordinators(Transaction* tr,
	                                                                   std::vector<NetworkAddress> oldCoordinators,
	                                                                   Reference<ClusterConnectionFile>,
	                                                                   CoordinatorsResult&) = 0;
	virtual std::string getDesiredClusterKeyName() const { return std::string(); }
};

// Change to use the given set of coordination servers
ACTOR Future<Optional<CoordinatorsResult>> changeQuorumChecker(Transaction* tr,
                                                               Reference<IQuorumChange> change,
                                                               std::vector<NetworkAddress>* desiredCoordinators);
ACTOR Future<CoordinatorsResult> changeQuorum(Database cx, Reference<IQuorumChange> change);
Reference<IQuorumChange> autoQuorumChange(int desired = -1);
Reference<IQuorumChange> noQuorumChange();
Reference<IQuorumChange> specifiedQuorumChange(std::vector<NetworkAddress> const&);
Reference<IQuorumChange> nameQuorumChange(std::string const& name, Reference<IQuorumChange> const& other);

// Exclude the given set of servers from use as state servers.  Returns as soon as the change is durable, without
// necessarily waiting for the servers to be evacuated.  A NetworkAddress with a port of 0 means all servers on the
// given IP.
ACTOR Future<Void> excludeServers(Database cx, std::vector<AddressExclusion> servers, bool failed = false);
void excludeServers(Transaction& tr, std::vector<AddressExclusion>& servers, bool failed = false);

// Exclude the servers matching the given set of localities from use as state servers.  Returns as soon as the change
// is durable, without necessarily waiting for the servers to be evacuated.
ACTOR Future<Void> excludeLocalities(Database cx, std::unordered_set<std::string> localities, bool failed = false);
void excludeLocalities(Transaction& tr, std::unordered_set<std::string> localities, bool failed = false);

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

// Get the current list of excluded servers
ACTOR Future<std::vector<AddressExclusion>> getExcludedServers(Database cx);
ACTOR Future<std::vector<AddressExclusion>> getExcludedServers(Transaction* tr);

// Get the current list of excluded localities
ACTOR Future<std::vector<std::string>> getExcludedLocalities(Database cx);
ACTOR Future<std::vector<std::string>> getExcludedLocalities(Transaction* tr);

std::set<AddressExclusion> getAddressesByLocality(const std::vector<ProcessData>& workers, const std::string& locality);

// Check for the given, previously excluded servers to be evacuated (no longer used for state).  If waitForExclusion is
// true, this actor returns once it is safe to shut down all such machines without impacting fault tolerance, until and
// unless any of them are explicitly included with includeServers()
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

ACTOR Future<Void> advanceVersion(Database cx, Version v);

ACTOR Future<int> setDDMode(Database cx, int mode);

ACTOR Future<Void> forceRecovery(Reference<ClusterConnectionFile> clusterFile, Standalone<StringRef> dcId);

ACTOR Future<Void> printHealthyZone(Database cx);
ACTOR Future<Void> setDDIgnoreRebalanceSwitch(Database cx, bool ignoreRebalance);
ACTOR Future<bool> clearHealthyZone(Database cx, bool printWarning = false, bool clearSSFailureZoneString = false);
ACTOR Future<bool> setHealthyZone(Database cx, StringRef zoneId, double seconds, bool printWarning = false);

ACTOR Future<Void> waitForPrimaryDC(Database cx, StringRef dcId);

// Gets the cluster connection string
ACTOR Future<std::vector<NetworkAddress>> getCoordinators(Database cx);

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

// Management API written in template code to support both IClientAPI and NativeAPI
namespace ManagementAPI {

ACTOR template <class DB>
Future<Void> changeCachedRange(Reference<DB> db, KeyRangeRef range, bool add) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state KeyRange sysRange = KeyRangeRef(storageCacheKey(range.begin), storageCacheKey(range.end));
	state KeyRange sysRangeClear = KeyRangeRef(storageCacheKey(range.begin), keyAfter(storageCacheKey(range.end)));
	state KeyRange privateRange = KeyRangeRef(cacheKeysKey(0, range.begin), cacheKeysKey(0, range.end));
	state Value trueValue = storageCacheValue(std::vector<uint16_t>{ 0 });
	state Value falseValue = storageCacheValue(std::vector<uint16_t>{});
	loop {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			tr->clear(sysRangeClear);
			tr->clear(privateRange);
			tr->addReadConflictRange(privateRange);
			// hold the returned standalone object's memory
			state typename DB::TransactionT::template FutureT<RangeResult> previousFuture =
			    tr->getRange(KeyRangeRef(storageCachePrefix, sysRange.begin), 1, Snapshot::False, Reverse::True);
			RangeResult previous = wait(safeThreadFutureToFuture(previousFuture));
			bool prevIsCached = false;
			if (!previous.empty()) {
				std::vector<uint16_t> prevVal;
				decodeStorageCacheValue(previous[0].value, prevVal);
				prevIsCached = !prevVal.empty();
			}
			if (prevIsCached && !add) {
				// we need to uncache from here
				tr->set(sysRange.begin, falseValue);
				tr->set(privateRange.begin, serverKeysFalse);
			} else if (!prevIsCached && add) {
				// we need to cache, starting from here
				tr->set(sysRange.begin, trueValue);
				tr->set(privateRange.begin, serverKeysTrue);
			}
			// hold the returned standalone object's memory
			state typename DB::TransactionT::template FutureT<RangeResult> afterFuture =
			    tr->getRange(KeyRangeRef(sysRange.end, storageCacheKeys.end), 1, Snapshot::False, Reverse::False);
			RangeResult after = wait(safeThreadFutureToFuture(afterFuture));
			bool afterIsCached = false;
			if (!after.empty()) {
				std::vector<uint16_t> afterVal;
				decodeStorageCacheValue(after[0].value, afterVal);
				afterIsCached = afterVal.empty();
			}
			if (afterIsCached && !add) {
				tr->set(sysRange.end, trueValue);
				tr->set(privateRange.end, serverKeysTrue);
			} else if (!afterIsCached && add) {
				tr->set(sysRange.end, falseValue);
				tr->set(privateRange.end, serverKeysFalse);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			state Error err = e;
			wait(safeThreadFutureToFuture(tr->onError(e)));
			TraceEvent(SevDebug, "ChangeCachedRangeError").error(err);
		}
	}
}

template <class DB>
Future<Void> addCachedRange(Reference<DB> db, KeyRangeRef range) {
	return changeCachedRange(db, range, true);
}

template <class DB>
Future<Void> removeCachedRange(Reference<DB> db, KeyRangeRef range) {
	return changeCachedRange(db, range, false);
}

// return the corresponding error message for the CoordinatorsResult
// used by special keys and fdbcli
std::string generateErrorMessage(const CoordinatorsResult& res);

} // namespace ManagementAPI

#include "flow/unactorcompiler.h"
#endif
