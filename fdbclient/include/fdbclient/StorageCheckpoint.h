/*
 * StorageCheckpoint.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBTypes.h"

// FDB storage checkpoint format.
enum CheckpointFormat {
	InvalidFormat = 0,
	// For RocksDB, checkpoint generated via rocksdb::Checkpoint::ExportColumnFamily().
	RocksDBColumnFamily = 1,
	// For RocksDB, checkpoint generated via rocksdb::Checkpoint::CreateCheckpoint().
	RocksDB = 2,
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
	UID ssID; // Storage server ID on which this checkpoint is created.
	UID checkpointID; // A unique id for this checkpoint.
	int16_t state; // CheckpointState.
	int referenceCount; // A reference count on the checkpoint, it can only be deleted when this is 0.
	int64_t gcTime; // Time to delete this checkpoint, a Unix timestamp in seconds.

	// A serialized metadata associated with format, this data can be understood by the corresponding KVS.
	Standalone<StringRef> serializedCheckpoint;

	UID dataMoveId;

	CheckpointMetaData() = default;
	CheckpointMetaData(KeyRange const& range, CheckpointFormat format, UID const& ssID, UID const& checkpointID)
	  : version(invalidVersion), format(format), ssID(ssID), checkpointID(checkpointID), state(Pending),
	    referenceCount(0), gcTime(0) {
		this->ranges.push_back(range);
	}
	CheckpointMetaData(Version version, KeyRange const& range, CheckpointFormat format, UID checkpointID)
	  : version(version), format(format), ssID(UID()), checkpointID(checkpointID), state(Pending), referenceCount(0),
	    gcTime(0) {
		this->ranges.push_back(range);
	}
	CheckpointMetaData(Version version, CheckpointFormat format, UID checkpointID)
	  : version(version), format(format), ssID(UID()), checkpointID(checkpointID), state(Pending), referenceCount(0),
	    gcTime(0) {}

	CheckpointState getState() const { return static_cast<CheckpointState>(state); }

	void setState(CheckpointState state) { this->state = static_cast<int16_t>(state); }

	CheckpointFormat getFormat() const { return static_cast<CheckpointFormat>(format); }

	void setFormat(CheckpointFormat format) { this->format = static_cast<int16_t>(format); }

	std::string toString() const {
		std::string res = "Checkpoint MetaData: [Ranges]: " + describe(ranges) +
		                  " [Version]: " + std::to_string(version) + " [Format]: " + std::to_string(format) +
		                  " [Server]: " + ssID.toString() + " [ID]: " + checkpointID.toString() +
		                  " [State]: " + std::to_string(static_cast<int>(state)) +
		                  " [DataMove ID]: " + dataMoveId.toString();
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, ranges, format, state, checkpointID, ssID, gcTime, serializedCheckpoint, dataMoveId);
	}
};

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

	DataMoveMetaData() = default;
	DataMoveMetaData(UID id, Version version, KeyRange range) : id(id), version(version), priority(0), mode(0) {
		this->ranges.push_back(range);
	}
	DataMoveMetaData(UID id, KeyRange range) : id(id), version(invalidVersion), priority(0), mode(0) {
		this->ranges.push_back(range);
	}

	Phase getPhase() const { return static_cast<Phase>(phase); }

	void setPhase(Phase phase) { this->phase = static_cast<int16_t>(phase); }

	std::string toString() const {
		std::string res = "DataMoveMetaData: [ID]: " + id.shortString() + " [Range]: " + describe(ranges) +
		                  " [Phase]: " + std::to_string(static_cast<int>(phase)) +
		                  " [Source Servers]: " + describe(src) + " [Destination Servers]: " + describe(dest);
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, version, ranges, priority, src, dest, checkpoints, phase, mode);
	}
};

#endif