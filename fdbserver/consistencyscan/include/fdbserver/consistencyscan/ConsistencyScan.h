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

#include "fdbclient/ConsistencyScanInterface.h"
#include "fdbclient/StorageServerInterface.h"
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
// Shared by callers (currently ConsistencyCheckUrgent) that need to read a range from a fixed
// set of storage servers and compare the replies themselves, outside of the checkDataConsistency/
// consistencyCheckReadData path above that consistencyScanCore also uses.
struct RangeConsistencyResult {
	int firstValidServer;
	std::vector<int64_t> uniqueRefKeys;
	std::vector<int64_t> uniqueCmpKeys;
	std::vector<int64_t> mismatchedValues;
	Optional<KeyRef> nextKey;
	Optional<KeyRef> lastReadKey;
	int64_t totalReadAmount;
	bool success;
	// Set (along with success = false) when the disagreement looks like it's due to a storage server that
	// isn't actually alive (e.g. mid forced-recovery), rather than a genuine data inconsistency. Callers
	// should treat this as a signal to retry rather than as a real consistency failure.
	bool isFailed;

	explicit RangeConsistencyResult(const size_t serverCount)
	  : firstValidServer(-1), uniqueRefKeys(serverCount), uniqueCmpKeys(serverCount), mismatchedValues(serverCount),
	    totalReadAmount(0), success(true), isFailed(false) {}

	explicit RangeConsistencyResult() : RangeConsistencyResult(0) {}
};
inline bool isSuccessReply(const ErrorOr<GetKeyValuesReply>& reply) {
	return reply.present() && !reply.get().error.present();
}
Future<std::vector<ErrorOr<GetKeyValuesReply>>> readFromAllStorageServers(
    Database cx,
    std::vector<StorageServerInterface> storageServerInterfaces,
    KeyRangeRef range,
    KeySelector begin);
RangeConsistencyResult checkRangeReplies(const std::vector<StorageServerInterface>& storageServerInterfaces,
                                         const std::vector<ErrorOr<GetKeyValuesReply>>& readReplies,
                                         KeyRangeRef range,
                                         KeySelector begin,
                                         bool performQuiescentChecks);
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
