/*
 * StorageServerShard.h
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

#ifndef FDBCLIENT_STORAGESERVERSHARD_H
#define FDBCLIENT_STORAGESERVERSHARD_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"

// Represents a data shard on a storage server hosting a continuous keyrange.
struct StorageServerShard {
	constexpr static FileIdentifier file_identifier = 4028358;

	enum ShardState {
		NotAssigned = 0,
		Adding = 1,
		ReadWritePending = 2,
		ReadWrite = 3,
		MovingIn = 4,
		Error = 5,
	};

	StorageServerShard() = default;
	StorageServerShard(KeyRange range,
	                   Version version,
	                   const uint64_t id,
	                   const uint64_t desiredId,
	                   ShardState shardState,
	                   Optional<UID> moveInShardId)
	  : range(range), version(version), id(id), desiredId(desiredId), shardState(shardState),
	    moveInShardId(moveInShardId) {}
	StorageServerShard(KeyRange range,
	                   Version version,
	                   const uint64_t id,
	                   const uint64_t desiredId,
	                   ShardState shardState)
	  : range(range), version(version), id(id), desiredId(desiredId), shardState(shardState) {}

	static StorageServerShard notAssigned(KeyRange range, Version version = 0) {
		return StorageServerShard(range, version, 0, 0, NotAssigned);
	}

	ShardState getShardState() const { return static_cast<ShardState>(this->shardState); };

	void setShardState(const ShardState shardState) { this->shardState = static_cast<int8_t>(shardState); }

	std::string getShardStateString() const {
		const ShardState ss = getShardState();
		switch (ss) {
		case NotAssigned:
			return "NotAssigned";
		case Adding:
			return "Adding";
		case ReadWritePending:
			return "ReadWritePending";
		case ReadWrite:
			return "ReadWrite";
		case MovingIn:
			return "MovingIn";
		case Error:
			return "Error";
		}

		return "InvalidState";
	}

	std::string toString() const {
		return "StorageServerShard: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		       " [Shard ID]: " + format("%016llx", this->id) + " [Version]: " + std::to_string(version) +
		       " [State]: " + getShardStateString() + " [Desired Shard ID]: " + format("%016llx", this->desiredId);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, version, id, desiredId, shardState, moveInShardId);
	}

	KeyRange range;
	Version version; // Shard creation version.
	uint64_t id; // The actual shard ID.
	uint64_t desiredId; // The intended shard ID.
	int8_t shardState;
	Optional<UID> moveInShardId; // If present, it is the associated MoveInShardMetaData.
};

#endif
