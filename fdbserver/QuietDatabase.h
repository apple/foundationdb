/*
 * QuietDatabase.h
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

#ifndef FDBSERVER_QUIETDATABASE_H
#define FDBSERVER_QUIETDATABASE_H
#pragma once

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/DatabaseContext.h" // for clone()
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.h"

Future<int64_t> getDataInFlight( Database const& cx, Reference<AsyncVar<struct ServerDBInfo>> const& );
Future<int64_t> getMaxTLogQueueSize( Database const& cx, Reference<AsyncVar<struct ServerDBInfo>> const& );
Future<int64_t> getMaxStorageServerQueueSize( Database const& cx, Reference<AsyncVar<struct ServerDBInfo>> const& );
Future<int64_t> getDataDistributionQueueSize( Database const &cx, Reference<AsyncVar<struct ServerDBInfo>> const&, bool const& reportInFlight );
Future<vector<StorageServerInterface>> getStorageServers( Database const& cx, bool const &use_system_priority = false);
Future<vector<std::pair<WorkerInterface, ProcessClass>>> getWorkers( Reference<AsyncVar<ServerDBInfo>> const& dbInfo, int const& flags = 0 );
Future<WorkerInterface> getMasterWorker( Database const& cx, Reference<AsyncVar<ServerDBInfo>> const& dbInfo );

//Waits for f to complete. If simulated, disables connection failures after waiting a specified amount of time
Future<Void> disableConnectionFailuresAfter( Future<Void> const& f, double const& disableTime, std::string const& context );

#endif