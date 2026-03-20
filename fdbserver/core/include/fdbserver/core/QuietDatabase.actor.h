/*
 * QuietDatabase.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_CORE_QUIETDATABASE_ACTOR_G_H)
#define FDBSERVER_CORE_QUIETDATABASE_ACTOR_G_H
#include "fdbserver/core/QuietDatabase.actor.g.h"
#elif !defined(FDBSERVER_CORE_QUIETDATABASE_ACTOR_H)
#define FDBSERVER_CORE_QUIETDATABASE_ACTOR_H

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<int64_t> getDataInFlight(Database cx, Reference<AsyncVar<struct ServerDBInfo> const> dbInfo);
ACTOR Future<std::pair<int64_t, int64_t>> getTLogQueueInfo(Database cx,
                                                           Reference<AsyncVar<struct ServerDBInfo> const> dbInfo);
ACTOR Future<int64_t> getMaxStorageServerQueueSize(Database cx,
                                                   Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                                   Version version);
ACTOR Future<int64_t> getDataDistributionQueueSize(Database cx,
                                                   Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                                   bool reportInFlight);
ACTOR Future<bool> getTeamCollectionValid(Database cx, WorkerInterface distributorWorker);
ACTOR Future<bool> getTeamCollectionValid(Database cx, Reference<AsyncVar<struct ServerDBInfo> const> dbInfo);
ACTOR Future<std::vector<StorageServerInterface>> getStorageServers(Database cx, bool use_system_priority = false);
ACTOR Future<std::vector<WorkerDetails>> getWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo, int flags = 0);
ACTOR Future<WorkerInterface> getMasterWorker(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
ACTOR Future<Void> repairDeadDatacenter(Database cx,
                                        Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                        std::string context);
ACTOR Future<Void> reconfigureAfter(Database cx,
                                    double time,
                                    Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                    std::string context);

ACTOR Future<std::pair<std::vector<WorkerInterface>, int>>
getStorageWorkers(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo, bool localOnly);
ACTOR Future<std::vector<WorkerInterface>> getCoordWorkers(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo);

ACTOR Future<Void> enableConsistencyScanInSim(Database db);
ACTOR Future<Void> disableConsistencyScanInSim(Database db, bool waitForCompletion);

#include "flow/unactorcompiler.h"

#endif
