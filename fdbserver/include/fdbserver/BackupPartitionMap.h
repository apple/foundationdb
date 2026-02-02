/*
 * BackupPartitionMap.h
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

#ifndef FDBSERVER_BACKUPPARTITIONMAP_H
#define FDBSERVER_BACKUPPARTITIONMAP_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "flow/Arena.h"
#include <map>
#include <vector>

// Represents a single backup partition with its key range and assigned BackupWorker tag
struct BackupPartition {
	KeyRange keyRange;
	Tag backupTag; // BackupWorker tag assigned to this partition
	int partitionId; // Unique partition identifier

	BackupPartition() : partitionId(-1) {}
	BackupPartition(KeyRange range, Tag tag, int id) : keyRange(range), backupTag(tag), partitionId(id) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, backupTag, partitionId);
	}

	bool operator==(const BackupPartition& other) const {
		return keyRange == other.keyRange && backupTag == other.backupTag && partitionId == other.partitionId;
	}
};

// Maps BackupWorker tags to their assigned partitions and provides key-to-partition lookup
class BackupPartitionMap {
public:
	BackupPartitionMap() : version(invalidVersion) {}
	BackupPartitionMap(Version v) : version(v) {}

	// Add a partition to the map
	void addPartition(const BackupPartition& partition);

	// Get the partition that contains the given key
	Optional<BackupPartition> getPartitionForKey(const KeyRef& key) const;

	// Get all partitions assigned to a specific BackupWorker tag
	std::vector<BackupPartition> getPartitionsForTag(const Tag& tag) const;

	// Get all partitions in the map
	const std::vector<BackupPartition>& getAllPartitions() const { return partitions; }

	// Get the version when this partition map was created
	Version getVersion() const { return version; }

	// Set the version for this partition map
	void setVersion(Version v) { version = v; }

	// Check if the map is empty
	bool empty() const { return partitions.empty(); }

	// Clear all partitions
	void clear();

	// Get the number of partitions
	size_t size() const { return partitions.size(); }

	// Validate that partitions cover the entire key space without gaps or overlaps
	bool validate() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, partitions);
	}

private:
	Version version;
	std::vector<BackupPartition> partitions;
	
	// Rebuild internal lookup structures after modifications
	void rebuildLookup();
};

// System keys for backup partitioning
extern const KeyRef backupPartitionRequiredKey;
extern const KeyRangeRef backupPartitionMapKeys;
extern const KeyRef backupPartitionMapPrefix;

// Helper functions for partition map persistence
Key backupPartitionMapKeyFor(Version version);
Value backupPartitionMapValue(const BackupPartitionMap& partitionMap);
BackupPartitionMap decodeBackupPartitionMap(const ValueRef& value);

// Create a partition map by dividing the key space into the specified number of partitions
BackupPartitionMap createPartitionMap(int numPartitions, const std::vector<Tag>& backupTags, Version version);

#endif // FDBSERVER_BACKUPPARTITIONMAP_H