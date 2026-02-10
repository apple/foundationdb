/*
 * BackupPartitionMap.cpp
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

#include "fdbserver/BackupPartitionMap.h"
#include "fdbclient/SystemData.h"
#include "flow/Trace.h"
#include <algorithm>

// System keys for backup partitioning
const KeyRef backupPartitionRequiredKey = "\xff\x02/backupPartitionRequired"_sr;
const KeyRangeRef backupPartitionMapKeys("\xff\x02/backupPartitionMap/"_sr, "\xff\x02/backupPartitionMap0"_sr);
const KeyRef backupPartitionMapPrefix = backupPartitionMapKeys.begin;

void BackupPartitionMap::addPartition(const BackupPartition& partition) {
	partitions.push_back(partition);
	rebuildLookup();
}

Optional<BackupPartition> BackupPartitionMap::getPartitionForKey(const KeyRef& key) const {
	// Binary search for the partition containing this key
	for (const auto& partition : partitions) {
		if (partition.keyRange.contains(key)) {
			return partition;
		}
	}
	return Optional<BackupPartition>();
}

std::vector<BackupPartition> BackupPartitionMap::getPartitionsForTag(const Tag& tag) const {
	std::vector<BackupPartition> result;
	for (const auto& partition : partitions) {
		if (partition.backupTag == tag) {
			result.push_back(partition);
		}
	}
	return result;
}

void BackupPartitionMap::clear() {
	partitions.clear();
	version = invalidVersion;
}

bool BackupPartitionMap::validate() const {
	if (partitions.empty()) {
		return true; // Empty map is valid
	}

	// Sort partitions by begin key for validation
	std::vector<BackupPartition> sortedPartitions = partitions;
	std::sort(sortedPartitions.begin(), sortedPartitions.end(), 
		[](const BackupPartition& a, const BackupPartition& b) {
			return a.keyRange.begin < b.keyRange.begin;
		});

	// Check for gaps and overlaps
	for (size_t i = 0; i < sortedPartitions.size(); i++) {
		const auto& current = sortedPartitions[i];
		
		// Check if partition is valid
		if (current.keyRange.begin >= current.keyRange.end) {
			TraceEvent(SevError, "BackupPartitionMapInvalidRange")
				.detail("PartitionId", current.partitionId)
				.detail("Begin", current.keyRange.begin)
				.detail("End", current.keyRange.end);
			return false;
		}

		// Check for overlap with next partition
		if (i + 1 < sortedPartitions.size()) {
			const auto& next = sortedPartitions[i + 1];
			if (current.keyRange.end != next.keyRange.begin) {
				TraceEvent(SevError, "BackupPartitionMapGapOrOverlap")
					.detail("CurrentPartitionId", current.partitionId)
					.detail("CurrentEnd", current.keyRange.end)
					.detail("NextPartitionId", next.partitionId)
					.detail("NextBegin", next.keyRange.begin);
				return false;
			}
		}
	}

	// Check if partitions cover the entire normal key space
	if (!sortedPartitions.empty()) {
		if (sortedPartitions.front().keyRange.begin != normalKeys.begin ||
		    sortedPartitions.back().keyRange.end != normalKeys.end) {
			TraceEvent(SevError, "BackupPartitionMapIncompleteKeySpaceCoverage")
				.detail("FirstBegin", sortedPartitions.front().keyRange.begin)
				.detail("LastEnd", sortedPartitions.back().keyRange.end)
				.detail("ExpectedBegin", normalKeys.begin)
				.detail("ExpectedEnd", normalKeys.end);
			return false;
		}
	}

	return true;
}

void BackupPartitionMap::rebuildLookup() {
	// Sort partitions by begin key for efficient lookup
	std::sort(partitions.begin(), partitions.end(), 
		[](const BackupPartition& a, const BackupPartition& b) {
			return a.keyRange.begin < b.keyRange.begin;
		});
}

Key backupPartitionMapKeyFor(Version version) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(backupPartitionMapPrefix);
	wr << version;
	return wr.toValue();
}

Value backupPartitionMapValue(const BackupPartitionMap& partitionMap) {
	BinaryWriter wr(IncludeVersion());
	wr << partitionMap;
	return wr.toValue();
}

BackupPartitionMap decodeBackupPartitionMap(const ValueRef& value) {
	BackupPartitionMap partitionMap;
	BinaryReader rd(value, IncludeVersion());
	rd >> partitionMap;
	return partitionMap;
}

BackupPartitionMap createPartitionMap(int numPartitions, const std::vector<Tag>& backupTags, Version version) {
	ASSERT(numPartitions > 0);
	ASSERT(!backupTags.empty());

	BackupPartitionMap partitionMap(version);
	
	// Calculate partition boundaries by dividing the key space
	std::vector<Key> boundaries;
	boundaries.push_back(normalKeys.begin);
	
	// Create evenly distributed partition boundaries
	for (int i = 1; i < numPartitions; i++) {
		// Simple approach: divide key space by partition count
		// In practice, this could be more sophisticated based on data distribution
		double fraction = double(i) / double(numPartitions);
		
		// Create a boundary key based on the fraction
		// This is a simplified approach - real implementation might use data distribution info
		std::string boundaryStr = normalKeys.begin.toString();
		if (fraction < 1.0) {
			// Create intermediate keys by interpolating
			uint8_t boundaryByte = uint8_t(fraction * 256);
			boundaryStr += char(boundaryByte);
		}
		boundaries.push_back(Key(boundaryStr));
	}
	boundaries.push_back(normalKeys.end);

	// Create partitions and assign them to BackupWorker tags in round-robin fashion
	for (int i = 0; i < numPartitions; i++) {
		KeyRange range(KeyRangeRef(boundaries[i], boundaries[i + 1]));
		Tag assignedTag = backupTags[i % backupTags.size()];
		BackupPartition partition(range, assignedTag, i);
		partitionMap.addPartition(partition);
	}

	if (!partitionMap.validate()) {
		TraceEvent(SevError, "BackupPartitionMapCreationFailed")
			.detail("NumPartitions", numPartitions)
			.detail("NumBackupTags", backupTags.size());
		throw internal_error();
	}

	TraceEvent("BackupPartitionMapCreated")
		.detail("Version", version)
		.detail("NumPartitions", numPartitions)
		.detail("NumBackupTags", backupTags.size());

	return partitionMap;
}