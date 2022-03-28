/*
 * Status.h
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

#ifndef FDBSERVER_STATUS_H
#define FDBSERVER_STATUS_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/MasterInterface.h"
#include "fdbclient/ClusterInterface.h"

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
    std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>>* const& clientStatus,
    ServerCoordinators const& coordinators,
    std::vector<NetworkAddress> const& incompatibleConnections,
    Version const& datacenterVersionDifference,
    ConfigBroadcaster const* const& conifgBroadcaster);

struct WorkerEvents : std::map<NetworkAddress, TraceEventFields> {};
Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventOnWorkers(
    std::vector<WorkerDetails> const& workers,
    std::string const& eventName);
#endif
