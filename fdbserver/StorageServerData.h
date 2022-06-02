/*
 * StorageServerData.h
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

#ifndef FDBSERVER_STORAGESERVERDATA_H
#define FDBSERVER_STORAGESERVERDATA_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/flow.h"

struct StorageServerShard {
	constexpr static FileIdentifier file_identifier = 4028358;

	enum ShardState {
		NotAssigned = 0,
		MovingIn = 1,
		ReadWrite = 2,
	};

	StorageServerShard() = default;
	StorageServerShard(KeyRange range,
	                   Version version,
	                   const uint64_t id,
	                   const uint64_t desiredId,
	                   ShardState shardState)
	  : range(range), version(version), id(id), desiredId(desiredId), shardState(shardState) {}

	static StorageServerShard notAssigned(KeyRange range, Version version = 0) {
		return StorageServerShard(range, version, 0, 0, NotAssigned);
	}

	static StorageServerShard anonymousNotAssigned(KeyRange range) {
		return StorageServerShard(range, 0, anonymousShardId.first(), anonymousShardId.first(), NotAssigned);
	}

	static StorageServerShard anonymousShard(KeyRange range, Version version, ShardState shardState) {
		return StorageServerShard(range, version, anonymousShardId.first(), anonymousShardId.first(), shardState);
	}

	static StorageServerShard anonymousMoveIn(KeyRange range, Version version) {
		return StorageServerShard(range, version, anonymousShardId.first(), anonymousShardId.first(), MovingIn);
	}

	static StorageServerShard anonymousReadWrite(KeyRange range, Version version) {
		return StorageServerShard(range, version, anonymousShardId.first(), anonymousShardId.first(), MovingIn);
	}

	bool isAnonymous() const {
		return this->id == anonymousShardId.first() && this->desiredId == anonymousShardId.first();
	}

	ShardState getShardState() const { return static_cast<ShardState>(this->shardState); };
	void setShardState(const ShardState shardState) { this->shardState = static_cast<int8_t>(shardState); }
	std::string getShardStateString() const {
		const ShardState ss = getShardState();
		switch (ss) {
		case NotAssigned:
			return "NotAssigned";
		case MovingIn:
			return "MovingIn";
		case ReadWrite:
			return "ReadWrite";
		}
		return "InvalidState";
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, version, id, desiredId, shardState);
	}

	std::string toString() const {
		return "StorageServerShard:\nRange: " + Traceable<KeyRangeRef>::toString(range) +
		       "\nShard ID: " + format("%016llx", this->id) + "\nState: " + getShardStateString() +
		       "\nDesired Shard ID: " + format("%016llx", this->desiredId) + "\n";
	}

	KeyRange range;
	Version version;
	uint64_t id;
	uint64_t desiredId;
	int8_t shardState;
};

#endif
