/*
 * ControlCommands.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#ifdef FLOW_GRPC_ENABLED
#ifndef FDB_CTL_LIB_FDB_CONTROL_COMMANDS_H
#define FDB_CTL_LIB_FDB_CONTROL_COMMANDS_H

#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbctl/control_service/control_service.pb.h"
#include "fdbctl/control_service/control_service.grpc.pb.h"

#include <grpcpp/support/status.h>
#include <map>
#include <vector>
#include <string>

namespace fdbctl {

//-- Coordinators ----
Future<grpc::Status> getCoordinators(Reference<IDatabase> db,
                                     const GetCoordinatorsRequest* req,
                                     GetCoordinatorsReply* rep);
Future<grpc::Status> changeCoordinators(Reference<IDatabase> db,
                                        const ChangeCoordinatorsRequest* req,
                                        ChangeCoordinatorsReply* rep);

//-- Database Configure ----
Future<grpc::Status> configure(Reference<IDatabase> db, const ConfigureRequest* req, ConfigureReply* rep);

//-- Status ----
Future<grpc::Status> getStatus(Reference<IDatabase> db, const GetStatusRequest* req, GetStatusReply* rep);

//-- Workers ----
Future<grpc::Status> getWorkers(Reference<IDatabase> db, const GetWorkersRequest* req, GetWorkersReply* rep);
Future<grpc::Status> include(Reference<IDatabase> db, const IncludeRequest* req, IncludeReply* rep);
Future<grpc::Status> exclude(Reference<IDatabase> db, const ExcludeRequest* req, ExcludeReply* rep);
Future<grpc::Status> excludeStatus(Reference<IDatabase> db, const ExcludeStatusRequest* req, ExcludeStatusReply* rep);
Future<grpc::Status> kill(Reference<IDatabase> db, const KillRequest* req, KillReply* rep);

namespace utils {
Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr);

// Returns addresses of excluded/failed/in-progress processes.
Future<std::vector<std::string>> getExcludedServers(Reference<IDatabase> db);
Future<std::vector<std::string>> getFailedServers(Reference<IDatabase> db);
Future<std::vector<std::string>> getExcludedLocalities(Reference<IDatabase> db);
Future<std::vector<std::string>> getFailedLocalities(Reference<IDatabase> db);
Future<std::set<NetworkAddress>> getInProgressExclusion(Reference<ITransaction> tr);

Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                        std::map<std::string, StorageServerInterface>* interfaces);
Future<bool> getWorkersProcessData(Reference<IDatabase> db, std::vector<ProcessData>* workers);
} // namespace utils

namespace special_keys {

const KeyRef clusterDescriptionSpecialKey = "\xff\xff/configuration/coordinators/cluster_description"_sr;
const KeyRef coordinatorsAutoSpecialKey = "\xff\xff/management/auto_coordinators"_sr;
const KeyRef coordinatorsProcessSpecialKey = "\xff\xff/configuration/coordinators/processes"_sr;

using management_api::excludedForceOptionSpecialKey;
using management_api::excludedLocalityForceOptionSpecialKey;
using management_api::excludedLocalitySpecialKeyRange;
using management_api::excludedServersSpecialKeyRange;
using management_api::exclusionInProgressSpecialKeyRange;
using management_api::failedForceOptionSpecialKey;
using management_api::failedLocalityForceOptionSpecialKey;
using management_api::failedLocalitySpecialKeyRange;
using management_api::failedServersSpecialKeyRange;

const KeyRef workerInterfacesVerifyOptionSpecialKey = "\xff\xff/management/options/worker_interfaces/verify"_sr;
} // namespace special_keys
} // namespace fdbctl

#endif
#endif // FLOW_GRPC_ENABLED
