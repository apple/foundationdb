/*
 * MetaclusterUtil.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_METACLUSTERUTIL_ACTOR_G_H)
#define METACLUSTER_METACLUSTERUTIL_ACTOR_G_H
#include "metacluster/MetaclusterUtil.actor.g.h"
#elif !defined(METACLUSTER_METACLUSTERUTIL_ACTOR_H)
#define METACLUSTER_METACLUSTERUTIL_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"

#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

// This provide metacluster utility functions that may be used both internally and externally to the metacluster project
namespace metacluster::util {

FDB_BOOLEAN_PARAM(SkipMetaclusterCreation);

// Helper function to compute metacluster capacity by passing the result of metacluster::listClusters
std::pair<ClusterUsage, ClusterUsage> metaclusterCapacity(std::map<ClusterName, DataClusterMetadata> const& clusters);

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString);

ACTOR template <class Transaction>
Future<Reference<IDatabase>> getAndOpenDatabase(Transaction managementTr, ClusterName clusterName) {
	DataClusterMetadata clusterMetadata = wait(getClusterTransaction(managementTr, clusterName));
	Reference<IDatabase> db = wait(openDatabase(clusterMetadata.connectionString));
	return db;
}

struct SimulatedMetacluster {
	Reference<IDatabase> managementDb;
	std::map<ClusterName, Database> dataDbs;
};

ACTOR Future<SimulatedMetacluster> createSimulatedMetacluster(
    Database db,
    Optional<int64_t> tenantIdPrefix = Optional<int64_t>(),
    Optional<DataClusterEntry> dataClusterConfig = DataClusterEntry(),
    SkipMetaclusterCreation skipMetaclusterCreation = SkipMetaclusterCreation::False);

} // namespace metacluster::util

#include "flow/unactorcompiler.h"
#endif