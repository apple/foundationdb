/*
 * Status.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_ACTOR_STATUS_G_H)
#define FDBSERVER_ACTOR_STATUS_G_H
#include "fdbserver/Status.actor.g.h"
#elif !defined(FDBSERVER_ACTOR_STATUS_H)
#define FDBSERVER_ACTOR_STATUS_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/MasterInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/MetaclusterRegistration.h"

#include "metacluster/MetaclusterMetrics.h"

#include "flow/actorcompiler.h" // has to be last include

struct ProcessIssues {
	NetworkAddress address;
	Standalone<VectorRef<StringRef>> issues;

	ProcessIssues(NetworkAddress address, Standalone<VectorRef<StringRef>> issues) : address(address), issues(issues) {}
};

Future<StatusReply> clusterGetStatus(
    Reference<AsyncVar<struct ServerDBInfo>> const& db,
    Database const& cx,
    std::vector<WorkerDetails> const& workers,
    std::vector<ProcessIssues> const& workerIssues,
    std::vector<StorageServerMetaInfo> const& storageMetadatas,
    std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>>* const& clientStatus,
    ServerCoordinators const& coordinators,
    std::vector<NetworkAddress> const& incompatibleConnections,
    Version const& datacenterVersionDifference,
    Version const& dcLogServerVersionDifference,
    Version const& dcStorageServerVersionDifference,
    ConfigBroadcaster const* const& conifgBroadcaster,
    Optional<UnversionedMetaclusterRegistrationEntry> const& metaclusterRegistration,
    metacluster::MetaclusterMetrics const& metaclusterMetrics,
    std::unordered_set<NetworkAddress> const& excludedDegradedServers);

StatusReply clusterGetFaultToleranceStatus(const std::string& statusString);

struct WorkerEvents : std::map<NetworkAddress, TraceEventFields> {};
ACTOR Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventOnWorkers(
    std::vector<WorkerDetails> workers,
    std::string eventName);

ACTOR Future<KMSHealthStatus> getKMSHealthStatus(Reference<const AsyncVar<ServerDBInfo>> db);

#include "flow/unactorcompiler.h"
#endif
