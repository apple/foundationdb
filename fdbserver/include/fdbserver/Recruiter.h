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
// Handles recruitment for stateless roles plus the transaction logs.
// Recruitment consists of two main phases:
//
//   1. Find an optimal assignment of roles to workers
//   2. Send recruitment messages to the assigned workers
//
class Recruiter {
	friend class RecruiterImpl;

public:
	// Create assignments for each worker and recruit commit proxies, GRV
	// proxies, resolvers, and transaction logs.
	static Future<std::vector<Standalone<CommitTransactionRef>>> recruitEverything(
	    Reference<ClusterRecoveryData> clusterRecoveryData,
	    std::vector<StorageServerInterface>* seedServers,
	    Reference<ILogSystem> oldLogSystem);

	// TODO: Move functions in ClusterController.actor.cpp that recruit special
	// roles like EKP into this class. Then, this function can be made private.
	static void updateKnownIds(ClusterControllerData const* clusterControllerData,
	                           std::map<Optional<Standalone<StringRef>>, int>* id_used);

	static RecruitFromConfigurationReply findWorkersForConfiguration(ClusterControllerData* clusterControllerData,
	                                                                 RecruitFromConfigurationRequest const& req);

	static RecruitRemoteFromConfigurationReply findRemoteWorkersForConfiguration(
	    ClusterControllerData const* clusterControllerData,
	    RecruitRemoteFromConfigurationRequest const& req);

	// TODO: Make these functions private after rewriting betterMasterExists
	static WorkerFitnessInfo getWorkerForRoleInDatacenter(
	    ClusterControllerData const* clusterControllerData,
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    ProcessClass::Fitness unacceptableFitness,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	    bool checkStable = false);

	static std::vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    ClusterControllerData const* clusterControllerData,
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    int amount,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	    Optional<WorkerFitnessInfo> minWorker = Optional<WorkerFitnessInfo>(),
	    bool checkStable = false);

	// Selects the best method for TLog recruitment based on the specified policy
	static std::vector<WorkerDetails> getWorkersForTlogs(
	    ClusterControllerData const* clusterControllerData,
	    DatabaseConfiguration const& conf,
	    int32_t required,
	    int32_t desired,
	    Reference<IReplicationPolicy> const& policy,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    bool checkStable = false,
	    const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	    const std::vector<UID>& exclusionWorkerIds = {});

	static std::vector<WorkerDetails> getWorkersForSatelliteLogs(
	    ClusterControllerData const* clusterControllerData,
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
