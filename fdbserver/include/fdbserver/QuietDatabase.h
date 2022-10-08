/*
 * QuietDatabase.h
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

#ifndef FDBSERVER_QUIETDATABASE_H
#define FDBSERVER_QUIETDATABASE_H
#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/DatabaseContext.h" // for clone()
#include "fdbclient/FDBTypes.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"

Future<int64_t> getDataInFlight(Database const& cx, Reference<AsyncVar<struct ServerDBInfo> const> const&);
Future<std::pair<int64_t, int64_t>> getTLogQueueInfo(Database const& cx,
                                                     Reference<AsyncVar<struct ServerDBInfo> const> const&);
Future<int64_t> getMaxStorageServerQueueSize(Database const& cx,
                                             Reference<AsyncVar<struct ServerDBInfo> const> const&,
                                             Version const& version);
Future<int64_t> getDataDistributionQueueSize(Database const& cx,
                                             Reference<AsyncVar<struct ServerDBInfo> const> const&,
                                             bool const& reportInFlight);
Future<bool> getTeamCollectionValid(Database const& cx, WorkerInterface const&);
Future<bool> getTeamCollectionValid(Database const& cx, Reference<AsyncVar<struct ServerDBInfo> const> const&);
Future<std::vector<StorageServerInterface>> getStorageServers(Database const& cx,
                                                              bool const& use_system_priority = false);
Future<std::vector<BlobWorkerInterface>> getBlobWorkers(Database const& cx,
                                                        bool const& use_system_priority = false,
                                                        Version* const& grv = nullptr);
Future<std::vector<WorkerDetails>> getWorkers(Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
                                              int const& flags = 0);
Future<WorkerInterface> getMasterWorker(Database const& cx, Reference<AsyncVar<ServerDBInfo> const> const& dbInfo);
Future<Void> repairDeadDatacenter(Database const& cx,
                                  Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
                                  std::string const& context);
Future<Void> reconfigureAfter(Database const& cx,
                              double const& time,
                              Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
                              std::string const& context);

// Returns list of worker interfaces for available storage servers and the number of unavailable
// storage servers
Future<std::pair<std::vector<WorkerInterface>, int>>
getStorageWorkers(Database const& cx, Reference<AsyncVar<ServerDBInfo> const> const& dbInfo, bool const& localOnly);
Future<std::vector<WorkerInterface>> getCoordWorkers(Database const& cx,
                                                     Reference<AsyncVar<ServerDBInfo> const> const& dbInfo);

#endif
