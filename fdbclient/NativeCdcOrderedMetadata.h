/*
 * NativeCdcOrderedMetadata.h
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

#ifndef FDBCLIENT_NATIVECDCORDEREDMETADATA_H
#define FDBCLIENT_NATIVECDCORDEREDMETADATA_H
#pragma once

#include <cstddef>
#include <vector>

#include "fdbclient/NativeCdcClient.h"

inline constexpr size_t maxNativeCdcOrderedPartitions = NATIVE_CDC_MAX_ORDERED_PARTITIONS;
inline constexpr size_t maxNativeCdcOrderedRanges = NATIVE_CDC_MAX_RANGES;

// Ranges must already be a canonical union. Split points are strictly increasing and interior to its envelope.
// A split may lie in a gap or at a range boundary, but every resulting partition must contain selected keys.
// Empty split points produce one partition. Invalid input throws client_invalid_operation.
std::vector<std::vector<KeyRange>> nativeCdcOrderedPartitionRanges(const std::vector<KeyRange>& canonicalRanges,
                                                                   const std::vector<Key>& splitPoints);

class NativeCdcOrderedMetadata {
public:
	NativeCdcOrderedMetadata(std::vector<KeyRange> ranges,
	                         std::vector<Key> splitPoints,
	                         std::vector<CDCStreamId> partitions);

	bool operator==(const NativeCdcOrderedMetadata&) const = default;

	const std::vector<KeyRange>& ranges() const { return registeredRanges; }
	const std::vector<Key>& splitPoints() const { return partitionSplitPoints; }
	const std::vector<CDCStreamId>& partitions() const { return partitionStreamIds; }

private:
	std::vector<KeyRange> registeredRanges;
	std::vector<Key> partitionSplitPoints;
	std::vector<CDCStreamId> partitionStreamIds;
};

extern const KeyRangeRef cdcOrderedStreamKeys;
extern const KeyRangeRef cdcOrderedParentKeys;

Key cdcOrderedStreamKeyFor(CDCStreamId logicalId);
Key cdcOrderedParentKeyFor(CDCStreamId childId);
CDCStreamId decodeCDCOrderedStreamKey(KeyRef key);
CDCStreamId decodeCDCOrderedParentKey(KeyRef key);

// These storage-backed values have an explicit schema version independent of the native CDC wire protocol.
// Decoders reject unknown schemas, malformed values, and trailing bytes with serialization_failed.
Value cdcOrderedStreamValue(const NativeCdcOrderedMetadata& metadata);
NativeCdcOrderedMetadata decodeCDCOrderedStreamValue(ValueRef value);
Value cdcOrderedParentValue(CDCStreamId logicalId);
CDCStreamId decodeCDCOrderedParentValue(ValueRef value);

class Database;
class Transaction;

// Validates one physical partition and the common durable acknowledgement. Missing values are empty refs.
Version validateNativeCdcOrderedPartition(CDCStreamId logicalId,
                                          CDCStreamId childId,
                                          const std::vector<KeyRange>& expectedRanges,
                                          ValueRef ranges,
                                          ValueRef parent,
                                          ValueRef minimum,
                                          Version commonMinimum);

struct NativeCdcOrderedSnapshot {
	NativeCdcOrderedMetadata metadata;
	Version minVersion;
};

// The transaction overload neither sets options nor retries. Its caller enables system-key and lock-aware reads.
// An absent group returns none; inconsistent child metadata or unequal child watermarks fails with
// serialization_failed.
Future<Optional<NativeCdcOrderedSnapshot>> readNativeCdcOrderedSnapshot(Transaction* tr, CDCStreamId logicalId);
Future<Optional<NativeCdcOrderedSnapshot>> readNativeCdcOrderedSnapshot(Database cx, CDCStreamId logicalId);

// knownAvailableThrough must be proven across every partition of expectedMetadata. All child watermarks advance in
// one transaction; calling the physical-stream acknowledgement helper for individual children violates this invariant.
Future<Version> acknowledgeNativeCdcOrderedStream(Database cx,
                                                  CDCStreamId logicalId,
                                                  NativeCdcOrderedMetadata expectedMetadata,
                                                  Version consumedThrough,
                                                  Version knownAvailableThrough = invalidVersion);

void forceLinkNativeCdcOrderedMetadataTests();

#endif // FDBCLIENT_NATIVECDCORDEREDMETADATA_H
