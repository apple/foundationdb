/*
 * QuietDatabase.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"

Future<int64_t> getDataInFlight(Database cx, Reference<AsyncVar<struct ServerDBInfo> const> dbInfo);
Future<std::pair<int64_t, int64_t>> getTLogQueueInfo(Database cx,
                                                     Reference<AsyncVar<struct ServerDBInfo> const> dbInfo);
Future<int64_t> getMaxStorageServerQueueSize(Database cx,
                                             Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                             Version version);
Future<int64_t> getDataDistributionQueueSize(Database cx,
                                             Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                             bool reportInFlight);
Future<bool> getTeamCollectionValid(Database cx, WorkerInterface distributorWorker);
Future<bool> getTeamCollectionValid(Database cx, Reference<AsyncVar<struct ServerDBInfo> const> dbInfo);
Future<std::vector<StorageServerInterface>> getStorageServers(Database cx, bool use_system_priority = false);
Future<std::vector<WorkerDetails>> getWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo, int flags = 0);
Future<WorkerInterface> getMasterWorker(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
Future<Void> repairDeadDatacenter(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo, std::string context);
Future<Void> reconfigureAfter(Database cx,
                              double time,
                              Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                              std::string context);

Future<std::pair<std::vector<WorkerInterface>, int>> getStorageWorkers(Database cx,
                                                                       Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                                                       bool localOnly);
Future<std::vector<WorkerInterface>> getCoordWorkers(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo);

Future<Void> enableConsistencyScanInSim(Database db);
Future<Void> disableConsistencyScanInSim(Database db, bool waitForCompletion);

// Permanently disables DD pipeline control so that all blocked relocations pass through.
// For use by the test harness to allow DD to quiesce after tests complete.
// Uses a plain boolean (not AsyncVar) to avoid cross-process callback issues in simulation.
void disableDDPipelineControl();
bool isDDPipelineControlEnabled();
