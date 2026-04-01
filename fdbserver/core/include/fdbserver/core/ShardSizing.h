/*
 * ShardSizing.h
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"

// Holds the permitted size and IO bounds for a shard.
struct ShardSizeBounds {
	StorageMetrics max;
	StorageMetrics min;
	StorageMetrics permittedError;

	bool operator==(ShardSizeBounds const& rhs) const {
		return max == rhs.max && min == rhs.min && permittedError == rhs.permittedError;
	}

	static ShardSizeBounds shardSizeBoundsBeforeTrack();
};

ShardSizeBounds getShardSizeBounds(KeyRangeRef shard, int64_t maxShardSize);
int64_t getMaxShardSize(double dbSizeEstimate);
bool ddLargeTeamEnabled();
