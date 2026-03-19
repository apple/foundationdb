/*
 * ShardSizing.cpp
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

#include "fdbserver/core/ShardSizing.h"

#include <algorithm>
#include <cmath>

#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "flow/Trace.h"

ShardSizeBounds ShardSizeBounds::shardSizeBoundsBeforeTrack() {
	return ShardSizeBounds{ .max = StorageMetrics{ .bytes = -1,
		                                           .bytesWrittenPerKSecond = StorageMetrics::infinity,
		                                           .iosPerKSecond = StorageMetrics::infinity,
		                                           .bytesReadPerKSecond = StorageMetrics::infinity,
		                                           .opsReadPerKSecond = StorageMetrics::infinity },
		                    .min = StorageMetrics{ .bytes = -1,
		                                           .bytesWrittenPerKSecond = 0,
		                                           .iosPerKSecond = 0,
		                                           .bytesReadPerKSecond = 0,
		                                           .opsReadPerKSecond = 0 },
		                    .permittedError = StorageMetrics{ .bytes = -1,
		                                                      .bytesWrittenPerKSecond = StorageMetrics::infinity,
		                                                      .iosPerKSecond = StorageMetrics::infinity,
		                                                      .bytesReadPerKSecond = StorageMetrics::infinity,
		                                                      .opsReadPerKSecond = StorageMetrics::infinity } };
}

ShardSizeBounds getShardSizeBounds(KeyRangeRef shard, int64_t maxShardSize) {
	ShardSizeBounds bounds;

	if (shard.begin >= keyServersKeys.begin) {
		bounds.max.bytes = SERVER_KNOBS->KEY_SERVER_SHARD_BYTES;
	} else {
		bounds.max.bytes = maxShardSize;
	}

	bounds.max.bytesWrittenPerKSecond = bounds.max.infinity;
	bounds.max.iosPerKSecond = bounds.max.infinity;
	bounds.max.bytesReadPerKSecond = bounds.max.infinity;
	bounds.max.opsReadPerKSecond = bounds.max.infinity;

	if (shard.begin == allKeys.begin) {
		bounds.min.bytes = 0;
	} else {
		bounds.min.bytes = maxShardSize / SERVER_KNOBS->SHARD_BYTES_RATIO;
	}

	bounds.min.bytesWrittenPerKSecond = 0;
	bounds.min.iosPerKSecond = 0;
	bounds.min.bytesReadPerKSecond = 0;
	bounds.min.opsReadPerKSecond = 0;

	bounds.permittedError.bytes = bounds.max.bytes / SERVER_KNOBS->SHARD_BYTES_RATIO / 3;
	bounds.permittedError.bytesWrittenPerKSecond = bounds.permittedError.infinity;
	bounds.permittedError.iosPerKSecond = bounds.permittedError.infinity;
	bounds.permittedError.bytesReadPerKSecond = bounds.permittedError.infinity;
	bounds.permittedError.opsReadPerKSecond = bounds.permittedError.infinity;

	return bounds;
}

int64_t getMaxShardSize(double dbSizeEstimate) {
	int64_t size = std::min((SERVER_KNOBS->MIN_SHARD_BYTES + (int64_t)std::sqrt(std::max<double>(dbSizeEstimate, 0)) *
	                                                             SERVER_KNOBS->SHARD_BYTES_PER_SQRT_BYTES) *
	                            SERVER_KNOBS->SHARD_BYTES_RATIO,
	                        (int64_t)SERVER_KNOBS->MAX_SHARD_BYTES);
	if (SERVER_KNOBS->ALLOW_LARGE_SHARD) {
		size = std::max(size, static_cast<int64_t>(SERVER_KNOBS->MAX_LARGE_SHARD_BYTES));
	}

	TraceEvent("MaxShardSize")
	    .suppressFor(60.0)
	    .detail("Bytes", size)
	    .detail("EstimatedDbSize", dbSizeEstimate)
	    .detail("SqrtBytes", SERVER_KNOBS->SHARD_BYTES_PER_SQRT_BYTES)
	    .detail("AllowLargeShard", SERVER_KNOBS->ALLOW_LARGE_SHARD);
	return size;
}

bool ddLargeTeamEnabled() {
	return SERVER_KNOBS->DD_MAX_SHARDS_ON_LARGE_TEAMS > 0 && !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA;
}
