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

public:
	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Reference<AsyncVar<ServerDBInfo>> serverInfo;
	std::map<NetworkAddress, double> incompatibleConnections;
	AsyncTrigger forceMasterFailure;
	int64_t masterRegistrationCount;
	int64_t dbInfoCount;
	bool recoveryStalled;
	bool forceRecovery;
	DatabaseConfiguration config; // Asynchronously updated via master registration
	DatabaseConfiguration fullyRecoveredConfig;
	Database db;
	int unfinishedRecoveries;
	int logGenerations;
	bool cachePopulated;
	std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>> clientStatus;
	Future<Void> clientCounter;
	int clientCount;
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
	Future<Void> countClients();
	Future<Void> monitorServerInfoConfig();
	Future<Void> clusterOpenDatabase(OpenDatabaseRequest);
	Future<Void> clusterGetServerInfo(UID knownServerInfoID, ReplyPromise<ServerDBInfo> reply);

	// Monitors the global configuration version key for changes. When changes are
	// made, the global configuration history is read and any updates are sent to
	// all processes in the system by updating the ClientDBInfo object. The
	// GlobalConfig actor class contains the functionality to read the latest
	// history and update the processes local view.
	Future<Void> monitorGlobalConfig();
};
