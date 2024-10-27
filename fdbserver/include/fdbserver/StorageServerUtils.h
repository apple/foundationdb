/*
 * StorageServerutils.h
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

#ifndef FDBSERVER_STORAGESERVERUTILS_H
#define FDBSERVER_STORAGESERVERUTILS_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"
#include "fdbclient/StorageCheckpoint.h"

enum class MoveInPhase : std::int8_t {
	Pending = 0,
	Fetching = 1,
	Ingesting = 2,
	ApplyingUpdates = 3,
	ReadWritePending = 4,
	Complete = 5,
	Cancel = 6,
	Error = 7,
};

// MoveInShardMetaData contains all the necessary information to start/resume fetching a physical
// shard by a destination storage server.
struct MoveInShardMetaData {
	constexpr static FileIdentifier file_identifier = 3804366;

	UID id;
	UID dataMoveId;
	std::vector<KeyRange> ranges; // The key ranges to be fetched.
	Version createVersion = invalidVersion;
	Version highWatermark = invalidVersion; // The highest version that has been applied to the MoveInShard.
	int8_t phase = static_cast<int8_t>(MoveInPhase::Error); // MoveInPhase.
	std::vector<CheckpointMetaData> checkpoints; // All related checkpoints, they should cover `ranges`.
	Optional<std::string> error;
	double startTime = 0.0;
	bool conductBulkLoad = false;

	MoveInShardMetaData() = default;
	MoveInShardMetaData(const UID& id,
	                    const UID& dataMoveId,
	                    std::vector<KeyRange> ranges,
	                    const Version version,
	                    MoveInPhase phase,
	                    bool conductBulkLoad)
	  : id(id), dataMoveId(dataMoveId), ranges(ranges), createVersion(version), highWatermark(version),
	    phase(static_cast<int8_t>(phase)), startTime(now()), conductBulkLoad(conductBulkLoad) {}
	MoveInShardMetaData(const UID& id,
	                    const UID& dataMoveId,
	                    std::vector<KeyRange> ranges,
	                    const Version version,
	                    bool conductBulkLoad)
	  : MoveInShardMetaData(id, dataMoveId, ranges, version, MoveInPhase::Fetching, conductBulkLoad) {}
	MoveInShardMetaData(const UID& dataMoveId,
	                    std::vector<KeyRange> ranges,
	                    const Version version,
	                    bool conductBulkLoad)
	  : MoveInShardMetaData(deterministicRandom()->randomUniqueID(),
	                        dataMoveId,
	                        ranges,
	                        version,
	                        MoveInPhase::Fetching,
	                        conductBulkLoad) {}

	bool operator<(const MoveInShardMetaData& rhs) const {
		return this->ranges.front().begin < rhs.ranges.front().begin;
	}

	MoveInPhase getPhase() const { return static_cast<MoveInPhase>(this->phase); }

	void setPhase(MoveInPhase phase) { this->phase = static_cast<int8_t>(phase); }

	bool doBulkLoading() const { return this->conductBulkLoad; }

	uint64_t destShardId() const { return this->dataMoveId.first(); }
	std::string destShardIdString() const { return format("%016llx", this->dataMoveId.first()); }

	std::string toString() const {
		return "MoveInShardMetaData: [Range]: " + describe(this->ranges) +
		       " [DataMoveID]: " + this->dataMoveId.toString() +
		       " [ShardCreateVersion]: " + std::to_string(this->createVersion) + " [ID]: " + this->id.toString() +
		       " [State]: " + std::to_string(static_cast<int>(this->phase)) +
		       " [HighWatermark]: " + std::to_string(this->highWatermark) +
		       " [ConductBulkLoad]: " + std::to_string(this->conductBulkLoad);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, dataMoveId, ranges, createVersion, highWatermark, phase, checkpoints, conductBulkLoad);
	}
};

class ThroughputLimiter {
public:
	ThroughputLimiter(int64_t cap);

	Future<Void> ready();
	void addBytes(int64_t bytes);
	void settle();

private:
	int64_t cap;
	int64_t bytes;
	double lastSettleSec;
	double nextAvailableSec;
	Future<Void> readyFuture;
};

KeyRange persistMoveInShardsKeyRange();

KeyRange persistUpdatesKeyRange(const UID& id);

Key persistUpdatesKey(const UID& id, const Version version);

Version decodePersistUpdateVersion(KeyRef versionKey);

Key persistMoveInShardKey(const UID& id);

UID decodeMoveInShardKey(const KeyRef& key);

Value moveInShardValue(const MoveInShardMetaData& meta);

MoveInShardMetaData decodeMoveInShardValue(const ValueRef& value);

#endif
