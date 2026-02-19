/*
 * BackupPartitionMap.actor.cpp
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

#include "fdbserver/DDShardTracker.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// KeyRangeMap guarantees that key ranges are contiguous with no gaps in shards.
ACTOR Future<std::vector<KeyRange>> calculateBackupPartitions(KeyRangeMap<ShardTrackedData>* shards) {
	// TODO akanksha: Hardcoded for now.
	state const int NUM_PARTITIONS = 100;
	state std::vector<std::pair<KeyRange, int64_t>> userShards; // Pair of shard key range and shard size in bytes.
	state int64_t totalBytes = 0;

	// Step 1: Collect shard sizes
	loop {
		state bool needWait = false;
		state Future<Void> onChange;
		userShards.clear();
		totalBytes = 0;
		for (auto it : shards->intersectingRanges(normalKeys)) {
			// Await trackShardMetrics to populate stats in cache (waits for notification from background actor, no
			// RPC).
			if (!it->value().stats->get().present()) {
				onChange = it->value().stats->onChange();
				needWait = true;
				TraceEvent("BackupPartitionShardMetricsWait")
				    .detail("ShardBegin", it->range().begin)
				    .detail("ShardEnd", it->range().end);
				break;
			}
			totalBytes += it->value().stats->get().get().metrics.bytes;
			userShards.push_back(std::make_pair(it.range(), it->value().stats->get().get().metrics.bytes));
		}
		if (!needWait) {
			break;
		}
		wait(onChange);
	}

	// Step 2: Partition the shards
	// Integer division is acceptable here as any rounding remainder is added to the last partition.
	int64_t targetBytesPerPartition = totalBytes / NUM_PARTITIONS;
	state std::vector<KeyRange> partitions;
	state int64_t currentPartitionBytes = 0;
	state Key partitionStart = normalKeys.begin;

	for (int i = 0; i < userShards.size(); i++) {
		currentPartitionBytes += userShards[i].second;
		// Checks if new partition should be started.
		if ((currentPartitionBytes >= targetBytesPerPartition) || (i == userShards.size() - 1)) {
			partitions.push_back(KeyRangeRef(partitionStart, userShards[i].first.end));
			partitionStart = userShards[i].first.end;
			currentPartitionBytes = 0;
		}
	}
	return partitions;
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitions/NoUserShards") {
	ShardTrackedData defaultData;
	StorageMetrics zeroMetrics;
	zeroMetrics.bytes = 0;
	zeroMetrics.bytesWrittenPerKSecond = 0;
	zeroMetrics.bytesReadPerKSecond = 0;
	zeroMetrics.iosPerKSecond = 0;
	zeroMetrics.opsReadPerKSecond = 0;
	ShardMetrics zeroShard(zeroMetrics, 0.0, 1);
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(zeroShard);

	KeyRangeMap<ShardTrackedData> shards(defaultData);
	ShardTrackedData systemData;
	systemData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(zeroShard);
	shards.insert(systemKeys, systemData);

	std::vector<KeyRange> partitions = wait(calculateBackupPartitions(&shards));

	ASSERT(partitions.size() == 1);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == normalKeys.end);
	return Void();
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitions/SingleShard") {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	ShardTrackedData data;
	StorageMetrics metrics;
	metrics.bytes = 1000000;

	ShardMetrics shardMetrics(metrics, 0.0, 1);
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
	shards.insert(normalKeys, data);

	std::vector<KeyRange> partitions = wait(calculateBackupPartitions(&shards));
	ASSERT(partitions.size() == 1);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == normalKeys.end);
	return Void();
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitions/VaryingSizes") {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	state Key key1 = "a"_sr;
	state Key key2 = "b"_sr;
	state Key key3 = "c"_sr;
	state Key key4 = normalKeys.end;
	std::vector<std::pair<KeyRange, int64_t>> testShards = { { KeyRangeRef(normalKeys.begin, key1), 50000 },
		                                                     { KeyRangeRef(key1, key2), 200000 },
		                                                     { KeyRangeRef(key2, key3), 10000 },
		                                                     { KeyRangeRef(key3, key4), 90000 } };
	for (const auto& shard : testShards) {
		ShardTrackedData data;
		StorageMetrics metrics;
		metrics.bytes = shard.second;
		ShardMetrics shardMetrics(metrics, 0.0, 1);
		data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
		shards.insert(shard.first, data);
	}
	std::vector<KeyRange> partitions = wait(calculateBackupPartitions(&shards));

	ASSERT(partitions.size() == 4);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == key1);
	ASSERT(partitions[1].begin == key1);
	ASSERT(partitions[1].end == key2);
	ASSERT(partitions[2].begin == key2);
	ASSERT(partitions[2].end == key3);
	ASSERT(partitions[3].begin == key3);
	ASSERT(partitions[3].end == key4);
	return Void();
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitions/ZeroSizeShards") {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	state Key key1 = "a"_sr;
	state Key key2 = "b"_sr;
	state Key key3 = normalKeys.end;

	std::vector<std::pair<KeyRange, int64_t>> testShards = { { KeyRangeRef(normalKeys.begin, key1), 0 },
		                                                     { KeyRangeRef(key1, key2), 1000000 },
		                                                     { KeyRangeRef(key2, key3), 0 } };

	for (const auto& shard : testShards) {
		ShardTrackedData data;
		StorageMetrics metrics;
		metrics.bytes = shard.second;
		ShardMetrics shardMetrics(metrics, 0.0, 1);
		data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
		shards.insert(shard.first, data);
	}
	std::vector<KeyRange> partitions = wait(calculateBackupPartitions(&shards));
	ASSERT(partitions.size() == 2);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == key2);
	ASSERT(partitions[1].begin == key2);
	ASSERT(partitions[1].end == normalKeys.end);
	return Void();
}

ACTOR Future<Void> testAsyncMetricsUpdate() {
	state ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	state KeyRangeMap<ShardTrackedData> shards(defaultData);
	state Key splitKey = "split"_sr;
	ShardTrackedData emptyData;
	emptyData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	shards.insert(KeyRangeRef(normalKeys.begin, splitKey), emptyData);

	ShardTrackedData dataWithMetrics;
	StorageMetrics metrics;
	metrics.bytes = 100000;
	ShardMetrics shardMetrics(metrics, 0.0, 1);
	dataWithMetrics.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
	shards.insert(KeyRangeRef(splitKey, normalKeys.end), dataWithMetrics);

	state Future<std::vector<KeyRange>> resultFuture = calculateBackupPartitions(&shards);

	wait(delay(0.1));
	ASSERT(!resultFuture.isReady());
	StorageMetrics newMetrics;
	newMetrics.bytes = 50000;
	ShardMetrics newShardMetrics(newMetrics, 0.0, 1);
	shards.rangeContaining(normalKeys.begin)->value().stats->set(newShardMetrics);
	std::vector<KeyRange> partitions = wait(resultFuture);
	ASSERT(partitions.size() == 2);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == splitKey);
	ASSERT(partitions[1].begin == splitKey);
	ASSERT(partitions[1].end == normalKeys.end);
	return Void();
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitions/WaitForAsyncMetrics") {
	wait(testAsyncMetricsUpdate());
	return Void();
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitions/MultipleSmallShards") {
	ShardTrackedData defaultData;
	StorageMetrics defaultMetrics;
	defaultMetrics.bytes = 0;
	ShardMetrics defaultShardMetrics(defaultMetrics, 0.0, 0);
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(defaultShardMetrics);
	KeyRangeMap<ShardTrackedData> shards(defaultData);

	for (int i = 0; i < 1000; i++) {
		Key start = Key(format("shard%04d", i));
		Key end = (i == 999) ? normalKeys.end : Key(format("shard%04d", i + 1));

		ShardTrackedData data;
		StorageMetrics metrics;
		metrics.bytes = 1000;
		ShardMetrics shardMetrics(metrics, 0.0, 1);
		data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
		shards.insert(KeyRangeRef(start, end), data);
	}

	std::vector<KeyRange> partitions = wait(calculateBackupPartitions(&shards));

	ASSERT(partitions.size() == 100);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[partitions.size() - 1].end == normalKeys.end);
	for (int i = 1; i < partitions.size(); i++) {
		ASSERT(partitions[i - 1].end == partitions[i].begin);
	}
	return Void();
}