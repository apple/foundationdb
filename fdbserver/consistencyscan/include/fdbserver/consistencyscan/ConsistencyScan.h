/*
 * ConsistencyScan.h
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

#include "fdbclient/ConsistencyScanInterface.actor.h"
#include "flow/flow.h"

struct ServerDBInfo;

Future<Void> consistencyScan(ConsistencyScanInterface csInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);

// These helpers are used by the ConsistencyCheck workload and implemented in
// fdbserver/consistencyscan/ConsistencyScan.cpp.
Future<Version> getVersion(Database cx);
Future<bool> getKeyServers(
    Database cx,
    Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServersPromise,
    KeyRangeRef kr,
    bool performQuiescentChecks,
    bool failureIsError,
    bool* success);
Future<bool> getKeyLocations(Database cx,
                             std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> shards,
                             Promise<Standalone<VectorRef<KeyValueRef>>> keyLocationPromise,
                             bool performQuiescentChecks,
                             bool* success);
Future<Void> checkDataConsistency(Database cx,
                                  VectorRef<KeyValueRef> keyLocations,
                                  DatabaseConfiguration configuration,
                                  std::map<UID, StorageServerInterface> tssMapping,
                                  bool performQuiescentChecks,
                                  bool performTSSCheck,
                                  bool firstClient,
                                  bool failureIsError,
                                  int clientId,
                                  int clientCount,
                                  bool distributed,
                                  bool shuffleShards,
                                  int shardSampleFactor,
                                  int64_t sharedRandomNumber,
                                  int64_t repetitions,
                                  int64_t* bytesReadInPreviousRound,
                                  int restart,
                                  int64_t maxRate,
                                  int64_t targetInterval,
                                  bool* success);
