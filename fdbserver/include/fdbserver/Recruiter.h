/*
 * Recruiter.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include <vector>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WorkerInfo.h"

// Forward declare types from other modules to avoid a circular dependency.
class ClusterControllerData;
struct ClusterRecoveryData;

//
// Handles recruitment for all roles. Recruitment consists of two main phases:
//
//   1. Find an optimal assignment of roles to workers
//   2. Send recruitment messages to the assigned workers
//
class Recruiter {
	friend class RecruiterImpl;

	// Stores a copy of the cluster controllers' interface ID.
	const UID id;
	// Stores the time recruitment begins. Used to check whether worker
	// processes are still available for recruitment or if they have
	// potentially failed.
	const double startTime;

public:
	explicit Recruiter(UID const& id);

	// Returns a recruitment of workers that are suitable to run a complete
	// transaction subsystem for the given database configuration. If no valid
	// recruitment is possible, will throw `no_more_servers`. If
	// `checkGoodRecruitment` is true, will throw an error if the recruitment
	// is non-ideal.
	//
	// Since recruitment may fail, it is up to the caller to retry.
	WorkerRecruitment findWorkers(ClusterControllerData* clusterControllerData,
	                              RecruitmentInfo const& info,
	                              bool checkGoodRecruitment);

	// TODO: The return value is a little funny here - it returns a list of
	// transactions that need to be run on the new system. I think this should
	// instead return Future<Void>, and perhaps use an output parameter to
	// return the configuration change list (or just run the change itself?)
	Future<std::vector<Standalone<CommitTransactionRef>>> recruitWorkers(
	    Reference<ClusterRecoveryData> clusterRecoveryData,
	    WorkerRecruitment const& recruitment,
	    std::vector<StorageServerInterface>* seedServers,
	    Reference<ILogSystem> oldLogSystem);

	// Returns a worker that is suitable to run as a storage server. If no
	// valid worker can be found, throws `no_more_servers`.
	WorkerDetails findStorage(RecruitStorageRequest const& req,
	                          std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker) const;

	// Returns a worker that is suitable to run as a blob worker. Only workers
	// in the same datacenter as the cluster controller (and therefore the blob
	// manager) will be considered. If no valid worker can be found, throws
	// `no_more_servers`.
	WorkerDetails findBlobWorker(RecruitBlobWorkerRequest const& req,
	                             std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker,
	                             Optional<Standalone<StringRef>> const& clusterControllerDcId) const;

	// Check if txn system is recruited successfully in each region.
	void checkRegions(ClusterControllerData* clusterControllerData, const std::vector<RegionInfo>& regions);

	// TODO: Move functions in ClusterController.actor.cpp that recruit special
	// roles like EKP into this class. Then, this function can be made private.
	static void updateKnownIds(ClusterControllerData const* clusterControllerData,
	                           std::map<Optional<Standalone<StringRef>>, int>* id_used);

	// TODO: Make these functions private after rewriting betterMasterExists
	WorkerFitnessInfo getWorkerForRoleInDatacenter(
	    ClusterControllerData const* clusterControllerData,
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    ProcessClass::Fitness unacceptableFitness,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> const& preferredSharing = {},
	    bool checkStable = false);

	std::vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    ClusterControllerData const* clusterControllerData,
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    int amount,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> const& preferredSharing = {},
	    Optional<WorkerFitnessInfo> const& minWorker = Optional<WorkerFitnessInfo>(),
	    bool checkStable = false);

	// Selects the best method for TLog recruitment based on the specified policy
	std::vector<WorkerDetails> getWorkersForTLogs(ClusterControllerData const* clusterControllerData,
	                                              DatabaseConfiguration const& conf,
	                                              int32_t required,
	                                              int32_t desired,
	                                              Reference<IReplicationPolicy> const& policy,
	                                              std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                              bool checkStable = false,
	                                              const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	                                              const std::vector<UID>& exclusionWorkerIds = {});

	std::vector<WorkerDetails> getWorkersForSatelliteLogs(ClusterControllerData const* clusterControllerData,
	                                                      const DatabaseConfiguration& conf,
	                                                      const RegionInfo& region,
	                                                      const RegionInfo& remoteRegion,
	                                                      std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                      bool& satelliteFallback,
	                                                      bool checkStable = false);

	// TODO: Move this function to a superclass
	// Get the encryption at rest mode from the database configuration.
	static EncryptionAtRestMode getEncryptionAtRest(DatabaseConfiguration config);
};
