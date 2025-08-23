/*
 * FdbCommands.h
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

#ifndef FDB_CLI_LIB_FDB_COMMANDS_H
#define FDB_CLI_LIB_FDB_COMMANDS_H

#include "fdbclient/IClientApi.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/Locality.h"
#include "flow/FastRef.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbcli_lib/cli_service/cli_service.pb.h"
#include "fdbcli_lib/cli_service/cli_service.grpc.pb.h"

#include <grpcpp/support/status.h>
#include <map>
#include <vector>
#include <string>

namespace fdbcli_lib {

//-- Coordinators ----
Future<grpc::Status> getCoordinators(Reference<IDatabase> db,
                                     const GetCoordinatorsRequest* req,
                                     GetCoordinatorsReply* rep);
Future<grpc::Status> changeCoordinators(Reference<IDatabase> db,
                                        const ChangeCoordinatorsRequest* req,
                                        ChangeCoordinatorsReply* rep);

//-- Database Configure ----
Future<grpc::Status> configure(Reference<IDatabase> db, const ConfigureRequest* req, ConfigureReply* rep);
Future<grpc::Status> getOption(Reference<IDatabase> db, const GetOptionsRequest* req, GetOptionsReply* rep);
Future<grpc::Status> setOption(Reference<IDatabase> db, const SetOptionsRequest* req, SetOptionsReply* rep);
Future<grpc::Status> getReadVersion(Reference<IDatabase> db,
                                    const GetReadVersionRequest* req,
                                    GetReadVersionReply* rep);

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

Future<std::vector<std::string>> getExcludedServers(Reference<IDatabase> db);
Future<std::vector<std::string>> getFailedServers(Reference<IDatabase> db);
Future<std::vector<std::string>> getExcludedLocalities(Reference<IDatabase> db);
Future<std::vector<std::string>> getFailedLocalities(Reference<IDatabase> db);
Future<std::set<NetworkAddress>> getInProgressExclusion(Reference<ITransaction> tr);

Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                        std::map<std::string, StorageServerInterface>* interfaces);
Future<bool> getWorkers(Reference<IDatabase> db, std::vector<ProcessData>* workers);
} // namespace utils

namespace special_keys {

// TODO: Point fdbcli ones to this.
const KeyRef clusterDescriptionSpecialKey = "\xff\xff/configuration/coordinators/cluster_description"_sr;
const KeyRef configDBSpecialKey = "\xff\xff/configuration/coordinators/config_db"_sr;
const KeyRef coordinatorsAutoSpecialKey = "\xff\xff/management/auto_coordinators"_sr;
const KeyRef coordinatorsProcessSpecialKey = "\xff\xff/configuration/coordinators/processes"_sr;

// Special key ranges for include/exclude functionality
const KeyRange excludedServersSpecialKeyRange =
    KeyRangeRef("\xff\xff/management/excluded/"_sr, "\xff\xff/management/excluded0"_sr);
const KeyRange failedServersSpecialKeyRange =
    KeyRangeRef("\xff\xff/management/failed/"_sr, "\xff\xff/management/failed0"_sr);
const KeyRange excludedLocalitySpecialKeyRange =
    KeyRangeRef("\xff\xff/management/excluded_locality/"_sr, "\xff\xff/management/excluded_locality0"_sr);
const KeyRange failedLocalitySpecialKeyRange =
    KeyRangeRef("\xff\xff/management/failed_locality/"_sr, "\xff\xff/management/failed_locality0"_sr);
const KeyRef excludedForceOptionSpecialKey = "\xff\xff/management/options/excluded/force"_sr;
const KeyRef failedForceOptionSpecialKey = "\xff\xff/management/options/failed/force"_sr;
const KeyRef excludedLocalityForceOptionSpecialKey = "\xff\xff/management/options/excluded_locality/force"_sr;
const KeyRef failedLocalityForceOptionSpecialKey = "\xff\xff/management/options/failed_locality/force"_sr;
const KeyRangeRef exclusionInProgressSpecialKeyRange("\xff\xff/management/in_progress_exclusion/"_sr,
                                                     "\xff\xff/management/in_progress_exclusion0"_sr);

const KeyRef workerInterfacesVerifyOptionSpecialKey = "\xff\xff/management/options/worker_interfaces/verify"_sr;
} // namespace special_keys
} // namespace fdbcli_lib

#endif
