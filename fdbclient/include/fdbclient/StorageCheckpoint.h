/*
 * StorageCheckpoint.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_STORAGCHECKPOINT_H
#define FDBCLIENT_STORAGCHECKPOINT_H
#pragma once

#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"

const std::string checkpointBytesSampleFileName = "metadata_bytes.sst";
const std::string emptySstFilePath = "Dummy Empty SST File Path";

// FDB storage checkpoint format.
enum CheckpointFormat {
	InvalidFormat = 0,
	// For RocksDB, checkpoint generated via rocksdb::Checkpoint::ExportColumnFamily().
	DataMoveRocksCF = 1,
	// For RocksDB, checkpoint generated via rocksdb::Checkpoint::CreateCheckpoint().
	RocksDB = 2,
	// Checkpoint fetched as key-value pairs.
	RocksDBKeyValues = 3,
};

// Metadata of a FDB checkpoint.
struct CheckpointMetaData {
	enum CheckpointState {
		InvalidState = 0,
		Pending = 1, // Checkpoint creation pending.
		Complete = 2, // Checkpoint is created and ready to be read.
		Deleting = 3, // Checkpoint deletion requested.
		Fail = 4,
	};

	constexpr static FileIdentifier file_identifier = 13804342;
	Version version;
	std::vector<KeyRange> ranges;
	int16_t format; // CheckpointFormat.
	std::vector<UID> src; // Storage server(s) on which this checkpoint is created.
	UID checkpointID; // A unique id for this checkpoint.
	int16_t state; // CheckpointState.
	Optional<std::string> bytesSampleFile;

	// A serialized metadata associated with format, this data can be understood by the corresponding KVS.
	Standalone<StringRef> serializedCheckpoint;

	Optional<UID> actionId; // Unique ID defined by the application.

	std::string dir;

	CheckpointMetaData() = default;
	CheckpointMetaData(const std::vector<KeyRange>& ranges,
	                   CheckpointFormat format,
	                   const std::vector<UID>& src,
	                   UID const& checkpointID,
	                   UID const& actionId)
	  : version(invalidVersion), ranges(ranges), format(format), src(src), checkpointID(checkpointID), state(Pending),
	    actionId(actionId) {}
	CheckpointMetaData(const std::vector<KeyRange>& ranges,
	                   Version version,
	                   CheckpointFormat format,
	                   UID const& checkpointID)
	  : version(version), ranges(ranges), format(format), checkpointID(checkpointID), state(Pending) {}
	CheckpointMetaData(Version version, CheckpointFormat format, UID checkpointID)
	  : version(version), format(format), checkpointID(checkpointID), state(Pending) {}

	CheckpointState getState() const { return static_cast<CheckpointState>(state); }

	void setState(CheckpointState state) { this->state = static_cast<int16_t>(state); }

	CheckpointFormat getFormat() const { return static_cast<CheckpointFormat>(format); }

	void setFormat(CheckpointFormat format) { this->format = static_cast<int16_t>(format); }

	bool hasRange(const KeyRangeRef range) const {
		for (const auto& checkpointRange : ranges) {
			if (checkpointRange.contains(range)) {
				return true;
			}
		}
		return false;
	}

	bool hasRanges(const std::vector<KeyRange>& ranges) const {
		for (const auto& range : ranges) {
			if (!this->hasRange(range)) {
				return false;
			}
		}
		return true;
	}

	bool containsKey(const KeyRef key) const {
		for (const auto& range : ranges) {
			if (range.contains(key)) {
				return true;
			}
		}

		return false;
	}

	bool operator==(const CheckpointMetaData& r) const { return checkpointID == r.checkpointID; }

	std::string toString() const {
		std::string res = "Checkpoint MetaData: [Ranges]: " + describe(ranges) +
		                  " [Version]: " + std::to_string(version) + " [Format]: " + std::to_string(format) +
		                  " [Checkpoint Dir:] " + dir + " [Server]: " + describe(src) +
		                  " [ID]: " + checkpointID.toString() + " [State]: " + std::to_string(static_cast<int>(state)) +
		                  (actionId.present() ? (" [Action ID]: " + actionId.get().toString()) : "") +
		                  (bytesSampleFile.present() ? " [bytesSampleFile]: " + bytesSampleFile.get() : "");
		;
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           version,
		           ranges,
		           format,
		           state,
		           checkpointID,
		           src,
		           serializedCheckpoint,
		           actionId,
		           bytesSampleFile,
		           dir);
	}
};

namespace std {
template <>
class hash<CheckpointMetaData> {
public:
	size_t operator()(CheckpointMetaData const& checkpoint) const { return checkpoint.checkpointID.hash(); }
};
} // namespace std

// A DataMoveMetaData object corresponds to a single data move.
struct DataMoveMetaData {
	enum Phase {
		InvalidPhase = 0,
		Prepare = 1, // System keyspace is being modified.
		Running = 2, // System keyspace has been modified, data move in action.
		Completing = 3, // Data transfer has finished, finalizing system keyspace.
		Deleting = 4, // Data move is cancelled.
	};

	constexpr static FileIdentifier file_identifier = 13804362;
	UID id; // A unique id for this data move.
	Version version;
	std::vector<KeyRange> ranges;
	int priority;
	std::set<UID> src;
	std::set<UID> dest;
	std::set<UID> checkpoints;
	int16_t phase; // DataMoveMetaData::Phase.
	int8_t mode;
	Optional<BulkLoadState> bulkLoadState; // set if the data move is a bulk load data move

	DataMoveMetaData() = default;
	DataMoveMetaData(UID id, Version version, KeyRange range) : id(id), version(version), priority(0), mode(0) {
		this->ranges.push_back(range);
	}
	DataMoveMetaData(UID id, KeyRange range) : id(id), version(invalidVersion), priority(0), mode(0) {
		this->ranges.push_back(range);
	}
	DataMoveMetaData(UID id) : id(id), version(invalidVersion), priority(0), mode(0) {}

	Phase getPhase() const { return static_cast<Phase>(phase); }

	void setPhase(Phase phase) { this->phase = static_cast<int16_t>(phase); }

	std::string toString() const {
		std::string res = "DataMoveMetaData: [ID]: " + id.shortString() + ", [Range]: " + describe(ranges) +
		                  ", [Phase]: " + std::to_string(static_cast<int>(phase)) +
		                  ", [Source Servers]: " + describe(src) + ", [Destination Servers]: " + describe(dest) +
		                  ", [Checkpoints]: " + describe(checkpoints);
		if (bulkLoadState.present()) {
			res = res + ", [BulkLoadState]: " + bulkLoadState.get().toString();
		}
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, version, ranges, priority, src, dest, checkpoints, phase, mode, bulkLoadState);
	}
};

#endif
