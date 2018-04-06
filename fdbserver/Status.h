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
#include "WorkerInterface.h"
#include "MasterInterface.h"
#include "fdbclient/ClusterInterface.h"

typedef std::map< NetworkAddress, std::pair<std::string,UID> > ProcessIssuesMap;
typedef std::map< NetworkAddress, Standalone<VectorRef<ClientVersionRef>> > ClientVersionMap;

std::string extractAttribute( std::string const& expanded, std::string const& attributeToExtract );
Future<StatusReply> clusterGetStatus( Reference<AsyncVar<struct ServerDBInfo>> const& db, Database const& cx, vector<std::pair<WorkerInterface, ProcessClass>> const& workers,
	ProcessIssuesMap const& workerIssues, ProcessIssuesMap const& clientIssues, ClientVersionMap const& clientVersionMap, std::map<NetworkAddress, std::string> const& traceLogGroupMap, ServerCoordinators const& coordinators, std::vector<NetworkAddress> const& incompatibleConnections );

#endif
