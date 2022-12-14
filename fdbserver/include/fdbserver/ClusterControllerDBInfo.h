/*
 * ClusterControllerDBInfo.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/FastRef.h"

#include <map>

class ClusterControllerDBInfo {
	friend class ClusterControllerDBInfoImpl;

	// Map of addresses of incompatible connections to the expiration times.
	// Once expiration time is hit, if this entry has not been refreshed,
	// the connection will be removed (i.e. marked no longer incompatible)
	std::map<NetworkAddress, double> incompatibleConnections;
	AsyncTrigger forceMasterFailureTrigger;
	int64_t dbInfoCount;
	Future<Void> clientCounter;
	Future<Void> countClients();
	int clientCount;

public:
	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Reference<AsyncVar<ServerDBInfo>> serverInfo;
	int64_t masterRegistrationCount;
	bool recoveryStalled;
	bool forceRecovery;
	DatabaseConfiguration config; // Asynchronously updated via master registration
	DatabaseConfiguration fullyRecoveredConfig;
	Database db;
	int unfinishedRecoveries;
	int logGenerations;
	std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>> clientStatus;
	AsyncVar<bool> blobGranulesEnabled;
	AsyncVar<bool> blobRestoreEnabled;
	ClusterType clusterType = ClusterType::STANDALONE;
	Optional<ClusterName> metaclusterName;
	Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
	MetaclusterMetrics metaclusterMetrics;

	ClusterControllerDBInfo();
	void setDistributor(const DataDistributorInterface& interf);
	void setRatekeeper(const RatekeeperInterface& interf);
	void setBlobManager(const BlobManagerInterface& interf);
	void setBlobMigrator(const BlobMigratorInterface& interf);
	void setEncryptKeyProxy(const EncryptKeyProxyInterface& interf);
	void setConsistencyScan(const ConsistencyScanInterface& interf);
	void clearInterf(ProcessClass::ClassType t);
	Future<Void> clusterOpenDatabase(OpenDatabaseRequest);
	Future<Void> clusterGetServerInfo(UID knownServerInfoID, ReplyPromise<ServerDBInfo> reply);

	// Monitors the global configuration version key for changes. When changes are
	// made, the global configuration history is read and any updates are sent to
	// all processes in the system by updating the ClientDBInfo object. The
	// GlobalConfig actor class contains the functionality to read the latest
	// history and update the processes local view.
	Future<Void> monitorGlobalConfig();

	// Either mark the connection to the address as newly incompatible,
	// or refresh the expiration time
	void markConnectionIncompatible(NetworkAddress address);

	Future<Void> monitorServerInfoConfig();

	// Refreshes incompatibleConnections map and returns vector of currently
	// incompatible connection addresses
	std::vector<NetworkAddress> getIncompatibleConnections();

	void forceMasterFailure() { forceMasterFailureTrigger.trigger(); }

	Future<Void> onMasterFailureForced() const { return forceMasterFailureTrigger.onTrigger(); }

	int64_t incrementAndGetDbInfoCount() { return ++dbInfoCount; }

	int getClientCount() const { return clientCount; }
};
