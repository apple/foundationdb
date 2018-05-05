/*
 * ManagementAPI.h
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

#ifndef FDBCLIENT_MANAGEMENTAPI_H
#define FDBCLIENT_MANAGEMENTAPI_H
#pragma once

/* This file defines "management" interfaces for configuration, coordination changes, and
the inclusion and exclusion of servers. It is used to implement fdbcli management commands
and by test workloads that simulate such. It isn't exposed to C clients or anywhere outside
our code base and doesn't need to be versioned. It doesn't do anything you can't do with the
standard API and some knowledge of the contents of the system key space.
*/

#include <string>
#include <map>
#include "NativeAPI.h"
#include "Status.h"
#include "ReadYourWrites.h"

// ConfigurationResult enumerates normal outcomes of changeConfig() and various error
// conditions specific to it.  changeConfig may also throw an Error to report other problems.
class ConfigurationResult {
public:
	enum Type {
		NO_OPTIONS_PROVIDED,
		CONFLICTING_OPTIONS,
		UNKNOWN_OPTION,
		INCOMPLETE_CONFIGURATION,
		DATABASE_ALREADY_CREATED,
		DATABASE_CREATED,
		SUCCESS
	};
};

class CoordinatorsResult {
public:
	enum Type {
		INVALID_NETWORK_ADDRESSES,
		SAME_NETWORK_ADDRESSES,
		NOT_COORDINATORS, //FIXME: not detected
		DATABASE_UNREACHABLE, //FIXME: not detected
		BAD_DATABASE_STATE,
		COORDINATOR_UNREACHABLE,
		NOT_ENOUGH_MACHINES,
		SUCCESS
	};
};

struct ConfigureAutoResult {
	std::map<NetworkAddress, ProcessClass> address_class;
	int32_t processes;
	int32_t machines;

	std::string old_replication;
	int32_t old_proxies;
	int32_t old_resolvers;
	int32_t old_logs;
	int32_t old_processes_with_transaction;
	int32_t old_machines_with_transaction;

	std::string auto_replication;
	int32_t auto_proxies;
	int32_t auto_resolvers;
	int32_t auto_logs;
	int32_t auto_processes_with_transaction;
	int32_t auto_machines_with_transaction;

	int32_t desired_proxies;
	int32_t desired_resolvers;
	int32_t desired_logs;

	ConfigureAutoResult() : processes(-1), machines(-1),
		old_proxies(-1), old_resolvers(-1), old_logs(-1), old_processes_with_transaction(-1), old_machines_with_transaction(-1),
		auto_proxies(-1), auto_resolvers(-1), auto_logs(-1), auto_processes_with_transaction(-1), auto_machines_with_transaction(-1),
		desired_proxies(-1), desired_resolvers(-1), desired_logs(-1) {}

	bool isValid() const { return processes != -1; }
};

ConfigurationResult::Type buildConfiguration( std::vector<StringRef> const& modeTokens, std::map<std::string, std::string>& outConf );  // Accepts a vector of configuration tokens
ConfigurationResult::Type buildConfiguration( std::string const& modeString, std::map<std::string, std::string>& outConf );				// Accepts tokens separated by spaces in a single string

bool isCompleteConfiguration( std::map<std::string, std::string> const& options );

// All versions of changeConfig apply the given set of configuration tokens to the database, and return a ConfigurationResult (or error).
Future<ConfigurationResult::Type> changeConfig( Database const& cx, std::string const& configMode );  // Accepts tokens separated by spaces in a single string

ConfigureAutoResult parseConfig( StatusObject const& status );
Future<ConfigurationResult::Type> changeConfig( Database const& cx, std::vector<StringRef> const& modes, Optional<ConfigureAutoResult> const& conf );  // Accepts a vector of configuration tokens
Future<ConfigurationResult::Type> changeConfig( Database const& cx, std::map<std::string, std::string> const& m );  // Accepts a full configuration in key/value format (from buildConfiguration)

struct IQuorumChange : ReferenceCounted<IQuorumChange> {
	virtual ~IQuorumChange() {}
	virtual Future<vector<NetworkAddress>> getDesiredCoordinators( Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile>, CoordinatorsResult::Type& ) = 0;
	virtual std::string getDesiredClusterKeyName() { return std::string(); }
};

// Change to use the given set of coordination servers
Future<CoordinatorsResult::Type> changeQuorum( Database const& cx, Reference<IQuorumChange> const& change );
Reference<IQuorumChange> autoQuorumChange(int desired = -1);
Reference<IQuorumChange> noQuorumChange();
Reference<IQuorumChange> specifiedQuorumChange(vector<NetworkAddress> const&);
Reference<IQuorumChange> nameQuorumChange(std::string const& name, Reference<IQuorumChange> const& other);

// Exclude the given set of servers from use as state servers.  Returns as soon as the change is durable, without necessarily waiting for
// the servers to be evacuated.  A NetworkAddress with a port of 0 means all servers on the given IP.
Future<Void> excludeServers( Database const& cx, vector<AddressExclusion> const& servers );

// Remove the given servers from the exclusion list.  A NetworkAddress with a port of 0 means all servers on the given IP.  A NetworkAddress() means
// all servers (don't exclude anything)
Future<Void> includeServers( Database const& cx, vector<AddressExclusion> const& servers );

// Set the process class of processes with the given address.  A NetworkAddress with a port of 0 means all servers on the given IP.
Future<Void> setClass( Database const& cx, AddressExclusion const& server, ProcessClass const& processClass );

// Get the current list of excluded servers
Future<vector<AddressExclusion>> getExcludedServers( Database const& cx );

// Wait for the given, previously excluded servers to be evacuated (no longer used for state).  Once this returns it is safe to shut down all such
// machines without impacting fault tolerance, until and unless any of them are explicitly included with includeServers()
Future<Void> waitForExcludedServers( Database const& cx, vector<AddressExclusion> const& servers );

// Gets a list of all workers in the cluster (excluding testers)
Future<vector<ProcessData>> getWorkers( Database const& cx );
Future<vector<ProcessData>> getWorkers( Transaction* const& tr );

Future<Void> timeKeeperSetDisable(Database const& cx);

Future<Void> lockDatabase( Transaction* const& tr, UID const& id );
Future<Void> lockDatabase( Reference<ReadYourWritesTransaction> const& tr, UID const& id );
Future<Void> lockDatabase( Database const& cx, UID const& id );

Future<Void> unlockDatabase( Transaction* const& tr, UID const& id );
Future<Void> unlockDatabase( Reference<ReadYourWritesTransaction> const& tr, UID const& id );
Future<Void> unlockDatabase( Database const& cx, UID const& id );

Future<Void> checkDatabaseLock( Transaction* const& tr, UID const& id );
Future<Void> checkDatabaseLock( Reference<ReadYourWritesTransaction> const& tr, UID const& id );

Future<int> setDDMode( Database const& cx, int const& mode );

// Gets the cluster connection string
Future<std::vector<NetworkAddress>> getCoordinators( Database const& cx );
#endif