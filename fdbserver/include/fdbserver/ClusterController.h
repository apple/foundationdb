/*
 * ClusterController.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/BlobMigratorInterface.h"
#include <utility>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/Metacluster.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/ClusterControllerDBInfo.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RoleFitness.h"
#include "fdbserver/UpdateWorkerList.h"
#include "fdbserver/WorkerInfo.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/SystemMonitor.h"

class ClusterController {
	friend class ClusterControllerImpl;
	friend class ClusterControllerTesting;

	std::map<Optional<Standalone<StringRef>>, ProcessClass>
	    id_class; // contains the mapping from process id to process class from the database
	RangeResult lastProcessClasses;
	bool gotProcessClasses;
	bool gotFullyRecoveredConfig;
	Optional<Standalone<StringRef>> masterProcessId;
	Optional<Standalone<StringRef>> clusterControllerProcessId;
	AsyncVar<Optional<std::vector<Optional<Key>>>> desiredDcIds; // desired DC priorities
	AsyncVar<std::pair<bool, Optional<std::vector<Optional<Key>>>>>
	    changingDcIds; // current DC priorities to change first, and whether that is the cluster controller
	AsyncVar<std::pair<bool, Optional<std::vector<Optional<Key>>>>>
	    changedDcIds; // current DC priorities to change second, and whether the cluster controller has been changed
	UID id;
	Reference<AsyncVar<Optional<UID>>> clusterId;
	std::vector<Reference<RecruitWorkersInfo>> outstandingRecruitmentRequests;
	std::vector<Reference<RecruitRemoteWorkersInfo>> outstandingRemoteRecruitmentRequests;
	std::vector<std::pair<RecruitStorageRequest, double>> outstandingStorageRequests;
	std::vector<std::pair<RecruitBlobWorkerRequest, double>> outstandingBlobWorkerRequests;
	ActorCollection ac;
	UpdateWorkerList updateWorkerList;
	Future<Void> outstandingRequestChecker;
	Future<Void> outstandingRemoteRequestChecker;
	AsyncTrigger updateDBInfo;
	std::set<Endpoint> updateDBInfoEndpoints;
	std::set<Endpoint> removedDBInfoEndpoints;

	Database cx;
	double startTime;
	Future<Void> goodRecruitmentTime;
	Future<Void> goodRemoteRecruitmentTime;
	Version datacenterVersionDifference;
	PromiseStream<Future<Void>> addActor;
	bool versionDifferenceUpdated;

	bool remoteDCMonitorStarted;
	bool remoteTransactionSystemDegraded;

	// Stores the health information from a particular worker's perspective.
	struct WorkerHealth {
		struct DegradedTimes {
			double startTime = 0;
			double lastRefreshTime = 0;
		};
		std::unordered_map<NetworkAddress, DegradedTimes> degradedPeers;
		std::unordered_map<NetworkAddress, DegradedTimes> disconnectedPeers;

		// TODO(zhewu): Include disk and CPU signals.
	};
	std::unordered_map<NetworkAddress, WorkerHealth> workerHealth;
	DegradationInfo degradationInfo;
	std::unordered_set<NetworkAddress>
	    excludedDegradedServers; // The degraded servers to be excluded when assigning workers to roles.
	std::queue<double> recentHealthTriggeredRecoveryTime;

	CounterCollection clusterControllerMetrics;

	Counter openDatabaseRequests;
	Counter registerWorkerRequests;
	Counter getWorkersRequests;
	Counter getClientWorkersRequests;
	Counter registerMasterRequests;
	Counter statusRequests;

	Reference<EventCacheHolder> recruitedMasterWorkerEventHolder;

	bool workerAvailable(WorkerInfo const& worker, bool checkStable) const;
	bool isLongLivedStateless(Optional<Key> const& processId) const;
	WorkerDetails getStorageWorker(RecruitStorageRequest const& req);

	// Returns a worker that can be used by a blob worker
	// Note: we restrict the set of possible workers to those in the same DC as the BM/CC
	WorkerDetails getBlobWorker(RecruitBlobWorkerRequest const& req);

	std::vector<WorkerDetails> getWorkersForSeedServers(
	    DatabaseConfiguration const& conf,
	    Reference<IReplicationPolicy> const& policy,
	    Optional<Optional<Standalone<StringRef>>> const& dcId = Optional<Optional<Standalone<StringRef>>>());

	// Adds workers to the result such that each field is used in the result set as evenly as possible,
	// with a secondary criteria of minimizing the reuse of zoneIds
	// only add workers which have a field which is already in the result set
	void addWorkersByLowestField(StringRef field,
	                             int desired,
	                             const std::vector<WorkerDetails>& workers,
	                             std::set<WorkerDetails>& resultSet);

	// Adds workers to the result which minimize the reuse of zoneIds
	void addWorkersByLowestZone(int desired,
	                            const std::vector<WorkerDetails>& workers,
	                            std::set<WorkerDetails>& resultSet);

	// Log the reason why the worker is considered as unavailable.
	void logWorkerUnavailable(const Severity severity,
	                          const UID& id,
	                          const std::string& method,
	                          const std::string& reason,
	                          const WorkerDetails& details,
	                          const ProcessClass::Fitness& fitness,
	                          const std::set<Optional<Key>>& dcIds);

	// A TLog recruitment method specialized for three_data_hall and three_datacenter configurations
	// It attempts to evenly recruit processes from across data_halls or datacenters
	std::vector<WorkerDetails> getWorkersForTlogsComplex(DatabaseConfiguration const& conf,
	                                                     int32_t desired,
	                                                     std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                     StringRef field,
	                                                     int minFields,
	                                                     int minPerField,
	                                                     bool allowDegraded,
	                                                     bool checkStable,
	                                                     const std::set<Optional<Key>>& dcIds,
	                                                     const std::vector<UID>& exclusionWorkerIds);

	// Attempt to recruit TLogs without degraded processes and see if it improves the configuration
	std::vector<WorkerDetails> getWorkersForTlogsComplex(DatabaseConfiguration const& conf,
	                                                     int32_t desired,
	                                                     std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                     StringRef field,
	                                                     int minFields,
	                                                     int minPerField,
	                                                     bool checkStable,
	                                                     const std::set<Optional<Key>>& dcIds,
	                                                     const std::vector<UID>& exclusionWorkerIds);

	// A TLog recruitment method specialized for single, double, and triple configurations
	// It recruits processes from with unique zoneIds until it reaches the desired amount
	std::vector<WorkerDetails> getWorkersForTlogsSimple(DatabaseConfiguration const& conf,
	                                                    int32_t required,
	                                                    int32_t desired,
	                                                    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                    bool checkStable,
	                                                    const std::set<Optional<Key>>& dcIds,
	                                                    const std::vector<UID>& exclusionWorkerIds);

	// A backup method for TLog recruitment that is used for custom policies, but does a worse job
	// selecting the best workers.
	//   conf:        the database configuration.
	//   required:    the required number of TLog workers to select.
	//   desired:     the desired number of TLog workers to select.
	//   policy:      the TLog replication policy the selection needs to satisfy.
	//   id_used:     keep track of process IDs of selected workers.
	//   checkStable: when true, only select from workers that are considered as stable worker (not rebooted more than
	//                twice recently).
	//   dcIds:       the target data centers the workers are in. The selected workers must all be from these
	//                data centers:
	//   exclusionWorkerIds: the workers to be excluded from the selection.
	std::vector<WorkerDetails> getWorkersForTlogsBackup(
	    DatabaseConfiguration const& conf,
	    int32_t required,
	    int32_t desired,
	    Reference<IReplicationPolicy> const& policy,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    bool checkStable = false,
	    const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	    const std::vector<UID>& exclusionWorkerIds = {});

	// Selects the best method for TLog recruitment based on the specified policy
	std::vector<WorkerDetails> getWorkersForTlogs(DatabaseConfiguration const& conf,
	                                              int32_t required,
	                                              int32_t desired,
	                                              Reference<IReplicationPolicy> const& policy,
	                                              std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                              bool checkStable = false,
	                                              const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	                                              const std::vector<UID>& exclusionWorkerIds = {});

	// FIXME: This logic will fallback unnecessarily when usable dcs > 1 because it does not check all combinations of
	// potential satellite locations
	std::vector<WorkerDetails> getWorkersForSatelliteLogs(const DatabaseConfiguration& conf,
	                                                      const RegionInfo& region,
	                                                      const RegionInfo& remoteRegion,
	                                                      std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                      bool& satelliteFallback,
	                                                      bool checkStable = false);

	ProcessClass::Fitness getBestFitnessForRoleInDatacenter(ProcessClass::ClusterRole role);
	WorkerFitnessInfo getWorkerForRoleInDatacenter(Optional<Standalone<StringRef>> const& dcId,
	                                               ProcessClass::ClusterRole role,
	                                               ProcessClass::Fitness unacceptableFitness,
	                                               DatabaseConfiguration const& conf,
	                                               std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                               std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	                                               bool checkStable = false);
	std::vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    int amount,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	    Optional<WorkerFitnessInfo> minWorker = Optional<WorkerFitnessInfo>(),
	    bool checkStable = false);

	Future<Optional<Value>> getPreviousCoordinators();
	Future<Void> clusterWatchDatabase(ClusterControllerDBInfo* db,
	                                  ServerCoordinators coordinators,
	                                  Future<Void> recoveredDiskFiles);
	void checkOutstandingRecruitmentRequests();
	void checkOutstandingRemoteRecruitmentRequests();
	void checkOutstandingStorageRequests();
	void checkOutstandingBlobWorkerRequests();

	// Finds and returns a new process for role
	WorkerDetails findNewProcessForSingleton(ProcessClass::ClusterRole,
	                                         std::map<Optional<Standalone<StringRef>>, int>& id_used);
	// Return best possible fitness for singleton. Note that lower fitness is better.
	ProcessClass::Fitness findBestFitnessForSingleton(const WorkerDetails&, const ProcessClass::ClusterRole&);

	void checkBetterSingletons();
	Future<Void> doCheckOutstandingRequests();
	Future<Void> doCheckOutstandingRemoteRequests();
	void checkOutstandingRequests();
	Future<Void> workerAvailabilityWatch(WorkerInterface worker, ProcessClass startingClass);
	void clusterRecruitStorage(RecruitStorageRequest);
	void clusterRecruitBlobWorker(RecruitBlobWorkerRequest);
	void clusterRegisterMaster(RegisterMasterRequest const&);
	Future<Void> registerWorker(RegisterWorkerRequest, ClusterConnectionString, class ConfigBroadcaster*);
	Future<Void> timeKeeperSetVersion();

	// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid
	// running out of space, it limits the max number of entries and clears old entries on each update. This mapping is
	// used from backup and restore to get the version information for a timestamp.
	Future<Void> timeKeeper();

	Future<Void> statusServer(FutureStream<StatusRequest> requests,
	                          ServerCoordinators coordinators,
	                          ConfigBroadcaster const* configBroadcaster);
	Future<Void> monitorProcessClasses();
	Future<Void> updatedChangingDatacenters();
	Future<Void> updatedChangedDatacenters();
	Future<Void> updateDatacenterVersionDifference();

	// A background actor that periodically checks remote DC health, and `checkOutstandingRequests` if remote DC
	// recovers.
	Future<Void> updateRemoteDCHealth();

	Future<Void> handleForcedRecoveries(ClusterControllerFullInterface);
	Future<Void> triggerAuditStorage(TriggerAuditRequest req);
	Future<Void> handleTriggerAuditStorage(ClusterControllerFullInterface);
	Future<Void> startDataDistributor(double waitTime);
	Future<Void> monitorDataDistributor();
	Future<Void> startRatekeeper(double waitTime);
	Future<Void> monitorRatekeeper();
	Future<Void> startConsistencyScan();
	Future<Void> monitorConsistencyScan();
	Future<Void> startEncryptKeyProxy(double waitTime);
	Future<Void> monitorEncryptKeyProxy();
	Future<int64_t> getNextBMEpoch();
	Future<Void> watchBlobRestoreCommand();
	Future<Void> startBlobMigrator(double waitTime);
	Future<Void> monitorBlobMigrator();
	Future<Void> startBlobManager(double waitTime);
	Future<Void> monitorBlobManager();
	Future<Void> watchBlobGranulesConfigKey();
	Future<Void> dbInfoUpdater();
	Future<Void> workerHealthMonitor();
	Future<Void> metaclusterMetricsUpdater();
	Future<Void> updateClusterId();
	Future<Void> handleGetEncryptionAtRestMode(ClusterControllerFullInterface ccInterf);
	Future<Void> recruitNewMaster(ClusterControllerDBInfo* db, MasterInterface* newMaster);
	std::set<Optional<Standalone<StringRef>>> getDatacenters(DatabaseConfiguration const& conf,
	                                                         bool checkStable = false);
	void updateKnownIds(std::map<Optional<Standalone<StringRef>>, int>* id_used);
	RecruitRemoteFromConfigurationReply findRemoteWorkersForConfiguration(
	    RecruitRemoteFromConfigurationRequest const& req);

	// Given datacenter ID, returns the primary and remote regions.
	std::pair<RegionInfo, RegionInfo> getPrimaryAndRemoteRegion(const std::vector<RegionInfo>& regions, Key dcId);

	ErrorOr<RecruitFromConfigurationReply> findWorkersForConfigurationFromDC(RecruitFromConfigurationRequest const& req,
	                                                                         Optional<Key> dcId,
	                                                                         bool checkGoodRecruitment);

	RecruitFromConfigurationReply findWorkersForConfigurationDispatch(RecruitFromConfigurationRequest const& req,
	                                                                  bool checkGoodRecruitment);

	void updateIdUsed(const std::vector<WorkerInterface>& workers,
	                  std::map<Optional<Standalone<StringRef>>, int>& id_used);

	void compareWorkers(const DatabaseConfiguration& conf,
	                    const std::vector<WorkerInterface>& first,
	                    const std::map<Optional<Standalone<StringRef>>, int>& firstUsed,
	                    const std::vector<WorkerInterface>& second,
	                    const std::map<Optional<Standalone<StringRef>>, int>& secondUsed,
	                    ProcessClass::ClusterRole role,
	                    std::string description);

	RecruitFromConfigurationReply findWorkersForConfiguration(RecruitFromConfigurationRequest const& req);

	// Check if txn system is recruited successfully in each region
	void checkRegions(const std::vector<RegionInfo>& regions);

	void checkRecoveryStalled();

	void updateIdUsed(const std::vector<WorkerDetails>& workers,
	                  std::map<Optional<Standalone<StringRef>>, int>& id_used);

	// This function returns true when the cluster controller determines it is worth forcing
	// a cluster recovery in order to change the recruited processes in the transaction subsystem.
	bool betterMasterExists();

	// Returns true iff processId is currently being used
	// for any non-singleton role other than master
	bool isUsedNotMaster(Optional<Key> processId) const;

	// Returns true iff
	// - role is master, or
	// - role is a singleton AND worker's pid is being used for any non-singleton role
	bool onMasterIsBetter(const WorkerDetails& worker, ProcessClass::ClusterRole role) const;

	// Returns a map of <pid, numRolesUsingPid> for all non-singleton roles
	std::map<Optional<Standalone<StringRef>>, int> getUsedIds();

	// Updates work health signals in `workerHealth` based on `req`.
	void updateWorkerHealth(const UpdateWorkerHealthRequest& req);

	// Checks that if any worker or their degraded peers have recovered. If so, remove them from `workerHealth`.
	void updateRecoveredWorkers();

	// Returns a list of servers who are experiencing degraded links. These are candidates to perform exclusion. Note
	// that only one endpoint of a bad link will be included in this list.
	DegradationInfo getDegradationInfo();

	// Whether the transaction system (in primary DC if in HA setting) contains degraded servers.
	bool transactionSystemContainsDegradedServers();

	// Whether transaction system in the remote DC, e.g. log router and tlogs in the remote DC, contains degraded
	// servers.
	bool remoteTransactionSystemContainsDegradedServers();

	// Returns true if remote DC is healthy and can failover to.
	bool remoteDCIsHealthy();

	// Returns true when the cluster controller should trigger a recovery due to degraded servers used in the
	// transaction system in the primary data center.
	bool shouldTriggerRecoveryDueToDegradedServers();

	// Returns true when the cluster controller should trigger a failover due to degraded servers used in the
	// transaction system in the primary data center, and no degradation in the remote data center.
	bool shouldTriggerFailoverDueToDegradedServers();

	int recentRecoveryCountDueToHealth();
	bool isExcludedDegradedServer(const NetworkAddressList& a) const;

	// Halts the registering (i.e. requesting) singleton if one is already in the process of being recruited
	// or, halts the existing singleton in favour of the requesting one
	template <class SingletonClass>
	void haltRegisteringOrCurrentSingleton(const WorkerInterface& worker,
	                                       const SingletonClass& currSingleton,
	                                       const SingletonClass& registeringSingleton,
	                                       const Optional<UID> recruitingID) {
		ASSERT(currSingleton.getRole() == registeringSingleton.getRole());
		const UID registeringID = registeringSingleton.getInterface().id();
		const std::string roleName = currSingleton.getRole().roleName;
		const std::string roleAbbr = currSingleton.getRole().abbreviation;

		// halt the requesting singleton if it isn't the one currently being recruited
		if ((recruitingID.present() && recruitingID.get() != registeringID) ||
		    clusterControllerDcId != worker.locality.dcId()) {
			TraceEvent(("CCHaltRegistering" + roleName).c_str(), id)
			    .detail(roleAbbr + "ID", registeringID)
			    .detail("DcID", printable(clusterControllerDcId))
			    .detail("ReqDcID", printable(worker.locality.dcId()))
			    .detail("Recruiting" + roleAbbr + "ID", recruitingID.present() ? recruitingID.get() : UID());
			registeringSingleton.halt(*this, worker.locality.processId());
		} else if (!recruitingID.present()) {
			// if not currently recruiting, then halt previous one in favour of requesting one
			TraceEvent(("CCRegister" + roleName).c_str(), id).detail(roleAbbr + "ID", registeringID);
			if (currSingleton.isPresent() && currSingleton.getInterface().id() != registeringID &&
			    id_worker.count(currSingleton.getInterface().locality.processId())) {
				TraceEvent(("CCHaltPrevious" + roleName).c_str(), id)
				    .detail(roleAbbr + "ID", currSingleton.getInterface().id())
				    .detail("DcID", printable(clusterControllerDcId))
				    .detail("ReqDcID", printable(worker.locality.dcId()))
				    .detail("Recruiting" + roleAbbr + "ID", recruitingID.present() ? recruitingID.get() : UID());
				currSingleton.halt(*this, currSingleton.getInterface().locality.processId());
			}
			// set the curr singleton if it doesn't exist or its different from the requesting one
			if (!currSingleton.isPresent() || currSingleton.getInterface().id() != registeringID) {
				registeringSingleton.setInterfaceToDbInfo(*this);
			}
		}
	}

	// Returns true iff the singleton is healthy. "Healthy" here means that
	// the singleton is stable (see below) and doesn't need to be rerecruited.
	// Side effects: (possibly) initiates recruitment
	template <class SingletonClass>
	bool isHealthySingleton(const WorkerDetails& newWorker,
	                        const SingletonClass& singleton,
	                        const ProcessClass::Fitness& bestFitness,
	                        const Optional<UID> recruitingID) {
		// A singleton is stable if it exists in cluster, has not been killed off of proc and is not being recruited
		bool isStableSingleton = singleton.isPresent() &&
		                         id_worker.count(singleton.getInterface().locality.processId()) &&
		                         (!recruitingID.present() || (recruitingID.get() == singleton.getInterface().id()));

		if (!isStableSingleton) {
			return false; // not healthy because unstable
		}

		auto& currWorker = id_worker[singleton.getInterface().locality.processId()];
		auto currFitness = currWorker.details.processClass.machineClassFitness(singleton.getClusterRole());
		if (currWorker.priorityInfo.isExcluded) {
			currFitness = ProcessClass::ExcludeFit;
		}
		// If any of the following conditions are met, we will switch the singleton's process:
		// - if the current proc is used by some non-master, non-singleton role
		// - if the current fitness is less than optimal (lower fitness is better)
		// - if currently at peak fitness but on same process as master, and the new worker is on different process
		bool shouldRerecruit =
		    isUsedNotMaster(currWorker.details.interf.locality.processId()) || bestFitness < currFitness ||
		    (currFitness == bestFitness && currWorker.details.interf.locality.processId() == masterProcessId &&
		     newWorker.interf.locality.processId() != masterProcessId);
		if (shouldRerecruit) {
			std::string roleAbbr = singleton.getRole().abbreviation;
			TraceEvent(("CCHalt" + roleAbbr).c_str(), id)
			    .detail(roleAbbr + "ID", singleton.getInterface().id())
			    .detail("Excluded", currWorker.priorityInfo.isExcluded)
			    .detail("Fitness", currFitness)
			    .detail("BestFitness", bestFitness);
			singleton.recruit(*this); // SIDE EFFECT: initiating recruitment
			return false; // not healthy since needed to be rerecruited
		} else {
			return true; // healthy because doesn't need to be rerecruited
		}
	}

public:
	static Future<Void> run(ClusterControllerFullInterface interf,
	                        Future<Void> leaderFail,
	                        ServerCoordinators coordinators,
	                        LocalityData locality,
	                        ConfigDBType configDBType,
	                        Future<Void> recoveredDiskFiles,
	                        Reference<AsyncVar<Optional<UID>>> clusterId);

	Future<Void> clusterRecruitFromConfiguration(Reference<RecruitWorkersInfo> req);
	Future<RecruitRemoteFromConfigurationReply> clusterRecruitRemoteFromConfiguration(
	    Reference<RecruitRemoteWorkersInfo> req);

	ClusterController(ClusterControllerFullInterface const& ccInterface,
	                  LocalityData const& locality,
	                  ServerCoordinators const& coordinators,
	                  Reference<AsyncVar<Optional<UID>>> clusterId);
	~ClusterController();
	UID getId() const { return id; }

	std::map<Optional<Standalone<StringRef>>, WorkerInfo> id_worker;

	// recruitX is used to signal when role X needs to be (re)recruited.
	// recruitingXID is used to track the ID of X's interface which is being recruited.
	// We use AsyncVars to kill (i.e. halt) singletons that have been replaced.
	double lastRecruitTime = 0;
	AsyncVar<bool> recruitDistributor;
	Optional<UID> recruitingDistributorID;
	AsyncVar<bool> recruitRatekeeper;
	Optional<UID> recruitingRatekeeperID;
	AsyncVar<bool> recruitBlobManager;
	Optional<UID> recruitingBlobManagerID;
	AsyncVar<bool> recruitBlobMigrator;
	Optional<UID> recruitingBlobMigratorID;
	AsyncVar<bool> recruitEncryptKeyProxy;
	Optional<UID> recruitingEncryptKeyProxyID;
	AsyncVar<bool> recruitConsistencyScan;
	Optional<UID> recruitingConsistencyScanID;

	bool shouldCommitSuicide;
	Optional<Standalone<StringRef>> clusterControllerDcId;
	ClusterControllerDBInfo db;

	// Capture cluster's Encryption data at-rest mode; the status is set 'only' at the time of cluster creation.
	// The promise gets set as part of cluster recovery process and is used by recovering encryption participant
	// stateful processes (such as TLog) to ensure the stateful process on-disk encryption status matches with cluster's
	// encryption status.
	Promise<EncryptionAtRestMode> encryptionAtRestMode;
};
