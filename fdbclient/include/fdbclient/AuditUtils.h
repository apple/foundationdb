/*
 * AuditUtils.h
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

#ifndef FDBCLIENT_AUDITUTILS_H
#define FDBCLIENT_AUDITUTILS_H

#include "fdbclient/Audit.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/fdbrpc.h"

struct MoveKeyLockInfo {
	UID prevOwner, myOwner, prevWrite;
};

// Cancel an in-progress audit by setting its persisted state to Failed and clearing progress metadata.
Future<Void> cancelAuditMetadata(Database cx, AuditType auditType, UID auditId);

// Persist a new audit's initial state to the database. Returns the assigned audit ID.
Future<UID> persistNewAuditState(Database cx, AuditStorageState auditState, MoveKeyLockInfo lock, bool ddEnabled);

// Update an existing audit's persisted state (e.g. phase transitions).
Future<Void> persistAuditState(Database cx,
                               AuditStorageState auditState,
                               std::string context,
                               MoveKeyLockInfo lock,
                               bool ddEnabled);

// Read a single audit's state by type and ID.
Future<AuditStorageState> getAuditState(Database cx, AuditType type, UID id);

// List audit states for a given type, optionally filtered by phase, ordered by ID.
AsyncResult<std::vector<AuditStorageState>> getAuditStates(Database cx,
                                                           AuditType auditType,
                                                           bool newFirst,
                                                           Optional<int> num = Optional<int>(),
                                                           Optional<AuditPhase> phase = Optional<AuditPhase>());

// Persist per-range audit progress (which sub-ranges have been validated).
Future<Void> persistAuditStateByRange(Database cx, AuditStorageState auditState);

// Read per-range audit progress for the given audit within a key range.
Future<std::vector<AuditStorageState>> getAuditStateByRange(Database cx, AuditType type, UID auditId, KeyRange range);

// Persist per-server audit progress.
Future<Void> persistAuditStateByServer(Database cx, AuditStorageState auditState);

// Read per-server audit progress for a specific server within a key range.
Future<std::vector<AuditStorageState>> getAuditStateByServer(Database cx,
                                                             AuditType type,
                                                             UID auditId,
                                                             UID auditServerId,
                                                             KeyRange range);

// Clear persisted audit metadata for completed/failed audits older than maxAuditIdToClear,
// keeping the most recent numFinishAuditToKeep finished audits.
Future<Void> clearAuditMetadataForType(Database cx,
                                       AuditType auditType,
                                       UID maxAuditIdToClear,
                                       int numFinishAuditToKeep);

// Check whether a storage server has been removed from the server list.
Future<bool> checkStorageServerRemoved(Database cx, UID ssid);

// Parse a human-readable audit phase string (e.g. "running", "complete") into an AuditPhase enum.
AuditPhase stringToAuditPhase(std::string auditPhaseStr);

// Check whether all sub-ranges of auditRange have completed range-based audit progress.
Future<bool> checkAuditProgressCompleteByRange(Database cx, AuditType auditType, UID auditId, KeyRange auditRange);

// Check whether a specific server's audit progress is complete for the given range.
Future<bool> checkAuditProgressCompleteByServer(Database cx,
                                                AuditType auditType,
                                                UID auditId,
                                                KeyRange auditRange,
                                                UID serverId,
                                                std::shared_ptr<AsyncVar<int>> checkProgressBudget);

// Initialize audit metadata on DD startup: resume incomplete audits and clean up old ones.
Future<std::vector<AuditStorageState>> initAuditMetadata(Database cx,
                                                         MoveKeyLockInfo lock,
                                                         bool ddEnabled,
                                                         UID dataDistributorId,
                                                         int persistFinishAuditCount);

// Result of reading a single server's owned ranges from the ServerKeys system key space.
struct AuditGetServerKeysRes {
	KeyRange completeRange; // the sub-range that was fully read in this batch
	Version readAtVersion;
	UID serverId;
	std::vector<KeyRange> ownRanges; // ranges this server owns according to ServerKeys
	int64_t readBytes;
	AuditGetServerKeysRes() = default;
	AuditGetServerKeysRes(KeyRange completeRange,
	                      Version readAtVersion,
	                      UID serverId,
	                      std::vector<KeyRange> ownRanges,
	                      int64_t readBytes)
	  : completeRange(completeRange), readAtVersion(readAtVersion), serverId(serverId), ownRanges(ownRanges),
	    readBytes(readBytes) {}
};

// Result of reading the KeyServers system key space: maps each server to the ranges it owns.
struct AuditGetKeyServersRes {
	KeyRange completeRange; // the sub-range that was fully read in this batch
	Version readAtVersion;
	int64_t readBytes;
	std::unordered_map<UID, std::vector<KeyRange>> rangeOwnershipMap;
	AuditGetKeyServersRes() = default;
	AuditGetKeyServersRes(KeyRange completeRange,
	                      Version readAtVersion,
	                      std::unordered_map<UID, std::vector<KeyRange>> rangeOwnershipMap,
	                      int64_t readBytes)
	  : completeRange(completeRange), readAtVersion(readAtVersion), rangeOwnershipMap(rangeOwnershipMap),
	    readBytes(readBytes) {}
};

// Merge overlapping or adjacent ranges in the input list into a minimal set of non-overlapping
// ranges covering the same key space.
std::vector<KeyRange> coalesceRangeList(std::vector<KeyRange> ranges);

// Compare two sorted, non-overlapping range lists for equivalence. Handles different split
// points that cover the same total range. Returns the first mismatched pair, or empty if equal.
Optional<std::pair<KeyRange, KeyRange>> rangesSame(std::vector<KeyRange> rangesA, std::vector<KeyRange> rangesB);

// A single consistency error found by checkLocationMetadataConsistency.
struct LocationMetadataError {
	std::string message;
	UID serverId;
	Optional<KeyRange> mismatchedRangeByKeyServer;
	Optional<KeyRange> mismatchedRangeByServerKey;
};

// Bidirectional consistency check between the KeyServers and ServerKeys views of shard
// ownership. Returns an error for each server that is missing from one side or has
// mismatched range assignments. An empty result means the two views are consistent.
std::vector<LocationMetadataError> checkLocationMetadataConsistency(
    const std::unordered_map<UID, std::vector<KeyRange>>& mapFromKeyServers,
    const std::unordered_map<UID, std::vector<KeyRange>>& mapFromServerKeys,
    KeyRange claimRange);

// Output of buildLocationMetadataMaps: the processed per-server range maps ready for
// consistency checking, plus counts of validated entries.
struct LocationMetadataMaps {
	std::unordered_map<UID, std::vector<KeyRange>> fromKeyServers;
	std::unordered_map<UID, std::vector<KeyRange>> fromServerKeys;
	int64_t numValidatedKeyServers = 0;
	int64_t numValidatedServerKeys = 0;
};

// Process raw per-server ownership data into maps suitable for checkLocationMetadataConsistency.
// For KeyServers data: coalesces overlapping ranges then intersects with claimRange.
// For ServerKeys data: intersects each range with claimRange.
// If traceId is provided, emits SevVerbose trace events for each range added.
LocationMetadataMaps buildLocationMetadataMaps(
    const std::unordered_map<UID, std::vector<KeyRange>>& mapFromKeyServersRaw,
    const std::unordered_map<UID, std::vector<KeyRange>>& serverOwnRangesMap,
    KeyRange claimRange,
    Optional<UID> traceId = Optional<UID>());

// Parse a ServerKeys KRM result into the list of ranges where serverHasKey() is true.
std::vector<KeyRange> buildOwnRangesFromServerKeysResult(const RangeResult& krmResult);

// Output of buildOwnershipMapFromKeyServersResult: per-server range ownership plus
// shard statistics from the KeyServers system key space.
struct KeyServersOwnershipMap {
	std::unordered_map<UID, std::vector<KeyRange>> rangeOwnershipMap;
	int64_t totalShardsCount = 0;
	int64_t shardsInAnonymousPhysicalShardCount = 0;
};

// Parse a KeyServers KRM result into a per-server ownership map. Decodes each entry's
// src and dest server lists via decodeKeyServersValue and merges them. Also counts total
// shards and shards using the anonymous physical shard ID.
KeyServersOwnershipMap buildOwnershipMapFromKeyServersResult(const RangeResult& krmResult,
                                                             const RangeResult& uidToTagMap);

// Read a single server's owned ranges from the ServerKeys system key space via transaction.
Future<AuditGetServerKeysRes> getThisServerKeysFromServerKeys(UID serverID, Transaction* tr, KeyRange range);

// Read the KeyServers system key space via transaction, producing a per-server ownership map.
Future<AuditGetKeyServersRes> getShardMapFromKeyServers(UID auditServerId, Transaction* tr, KeyRange range);

#endif
