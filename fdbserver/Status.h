/*
 * Status.h
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

#ifndef FDBSERVER_STATUS_H
#define FDBSERVER_STATUS_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/MasterInterface.h"
#include "fdbclient/ClusterInterface.h"

typedef Standalone<VectorRef<StringRef>> ProcessIssues;
typedef std::map<NetworkAddress, std::pair<ProcessIssues, UID>> ProcessIssuesMap;
typedef std::map< NetworkAddress, Standalone<VectorRef<ClientVersionRef>> > ClientVersionMap;

struct ClientStatusInfo {
	std::string traceLogGroup;
	int connectedCoordinatorsNum;

	ClientStatusInfo() : connectedCoordinatorsNum(0) {}
	ClientStatusInfo(std::string const& traceLogGroup, int const connectedCoordinatorsNum) : traceLogGroup(traceLogGroup), connectedCoordinatorsNum(connectedCoordinatorsNum) {}
};

Future<StatusReply> clusterGetStatus( Reference<AsyncVar<struct ServerDBInfo>> const& db, Database const& cx, vector<WorkerDetails> const& workers,
	ProcessIssuesMap const& workerIssues, ProcessIssuesMap const& clientIssues, ClientVersionMap const& clientVersionMap, std::map<NetworkAddress, struct ClientStatusInfo> const& clientStatusInfoMap,
	ServerCoordinators const& coordinators, std::vector<NetworkAddress> const& incompatibleConnections, Version const& datacenterVersionDifference );

#endif
