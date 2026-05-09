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

#include "BackupPartitionMap.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"

std::string serializePartitionListJSON(PartitionMap const& partitionMap) {
	JsonBuilderObject root;
	JsonBuilderArray partitionsArray;
	for (const auto& [tag, partitionList] : partitionMap) {
		for (const auto& partition : partitionList) {
			JsonBuilderObject partitionObj;
			partitionObj["partitionId"] = partition.partitionId;
			partitionObj["beginKey"] = partition.ranges.begin.printable();
			partitionObj["endKey"] = partition.ranges.end.printable();
			partitionsArray.push_back(partitionObj);
		}
	}
	root["partitions"] = partitionsArray;
	root["totalPartitions"] = partitionsArray.size();
	return root.getJson();
}

// KeyRangeMap guarantees that key ranges are contiguous with no gaps in shards.
Future<std::vector<KeyRange>> calculateBackupPartitionKeyRanges(KeyRangeMap<ShardTrackedData>* shards) {
	// TODO akanksha: Hardcoded for now.
	const int NUM_PARTITIONS = 100;
	std::vector<std::pair<KeyRange, int64_t>> userShards; // Pair of shard key range and shard size in bytes.
	int64_t totalBytes = 0;

	// Step 1: Collect shard sizes
	while (true) {
		bool needWait = false;
		Future<Void> onChange;
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
		co_await onChange;
	}

	// Step 2: Partition the shards
	// Integer division is acceptable here as any rounding remainder is added to the last partition.
	int64_t targetBytesPerPartition = totalBytes / NUM_PARTITIONS;
	std::vector<KeyRange> partitionKeyRanges;
	int64_t currentPartitionBytes = 0;
	Key partitionStart = normalKeys.begin;

	for (int i = 0; i < userShards.size(); i++) {
		currentPartitionBytes += userShards[i].second;
		// Checks if new partition should be started.
		if ((currentPartitionBytes >= targetBytesPerPartition) || (i == userShards.size() - 1)) {
			partitionKeyRanges.push_back(KeyRangeRef(partitionStart, userShards[i].first.end));
			partitionStart = userShards[i].first.end;
			currentPartitionBytes = 0;
		}
	}
	co_return partitionKeyRanges;
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitionKeyRanges/NoUserShards") {
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

	std::vector<KeyRange> partitions = co_await calculateBackupPartitionKeyRanges(&shards);

	ASSERT(partitions.size() == 1);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == normalKeys.end);
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitionKeyRanges/SingleShard") {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	ShardTrackedData data;
	StorageMetrics metrics;
	metrics.bytes = 1000000;

	ShardMetrics shardMetrics(metrics, 0.0, 1);
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
	shards.insert(normalKeys, data);

	std::vector<KeyRange> partitions = co_await calculateBackupPartitionKeyRanges(&shards);
	ASSERT(partitions.size() == 1);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == normalKeys.end);
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitionKeyRanges/VaryingSizes") {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	Key key1 = "a"_sr;
	Key key2 = "b"_sr;
	Key key3 = "c"_sr;
	Key key4 = normalKeys.end;
	std::vector<std::pair<KeyRange, int64_t>> testShards = { { KeyRangeRef(normalKeys.begin, key1), 50000 },
		                                                     { KeyRangeRef(key1, key2), 200000 },
		                                                     { KeyRangeRef(key2, key3), 10000 },
		                                                     { KeyRangeRef(key3, key4), 90000 } };
	for (const auto& [range, bytes] : testShards) {
		ShardTrackedData data;
		StorageMetrics metrics;
		metrics.bytes = bytes;
		ShardMetrics shardMetrics(metrics, 0.0, 1);
		data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
		shards.insert(range, data);
	}
	std::vector<KeyRange> partitions = co_await calculateBackupPartitionKeyRanges(&shards);

	ASSERT(partitions.size() == 4);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == key1);
	ASSERT(partitions[1].begin == key1);
	ASSERT(partitions[1].end == key2);
	ASSERT(partitions[2].begin == key2);
	ASSERT(partitions[2].end == key3);
	ASSERT(partitions[3].begin == key3);
	ASSERT(partitions[3].end == key4);
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitionKeyRanges/ZeroSizeShards") {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	Key key1 = "a"_sr;
	Key key2 = "b"_sr;
	Key key3 = normalKeys.end;

	std::vector<std::pair<KeyRange, int64_t>> testShards = { { KeyRangeRef(normalKeys.begin, key1), 0 },
		                                                     { KeyRangeRef(key1, key2), 1000000 },
		                                                     { KeyRangeRef(key2, key3), 0 } };

	for (const auto& [range, bytes] : testShards) {
		ShardTrackedData data;
		StorageMetrics metrics;
		metrics.bytes = bytes;
		ShardMetrics shardMetrics(metrics, 0.0, 1);
		data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
		shards.insert(range, data);
	}
	std::vector<KeyRange> partitions = co_await calculateBackupPartitionKeyRanges(&shards);
	ASSERT(partitions.size() == 2);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == key2);
	ASSERT(partitions[1].begin == key2);
	ASSERT(partitions[1].end == normalKeys.end);
}

Future<Void> testAsyncMetricsUpdate() {
	ShardTrackedData defaultData;
	defaultData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	KeyRangeMap<ShardTrackedData> shards(defaultData);
	Key splitKey = "split"_sr;
	ShardTrackedData emptyData;
	emptyData.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	shards.insert(KeyRangeRef(normalKeys.begin, splitKey), emptyData);

	ShardTrackedData dataWithMetrics;
	StorageMetrics metrics;
	metrics.bytes = 100000;
	ShardMetrics shardMetrics(metrics, 0.0, 1);
	dataWithMetrics.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>(shardMetrics);
	shards.insert(KeyRangeRef(splitKey, normalKeys.end), dataWithMetrics);

	Future<std::vector<KeyRange>> resultFuture = calculateBackupPartitionKeyRanges(&shards);

	co_await delay(0.1);
	ASSERT(!resultFuture.isReady());
	StorageMetrics newMetrics;
	newMetrics.bytes = 50000;
	ShardMetrics newShardMetrics(newMetrics, 0.0, 1);
	shards.rangeContaining(normalKeys.begin)->value().stats->set(newShardMetrics);
	std::vector<KeyRange> partitions = co_await resultFuture;
	ASSERT(partitions.size() == 2);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[0].end == splitKey);
	ASSERT(partitions[1].begin == splitKey);
	ASSERT(partitions[1].end == normalKeys.end);
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitionKeyRanges/WaitForAsyncMetrics") {
	co_await testAsyncMetricsUpdate();
}

TEST_CASE("/BackupPartitionMap/calculateBackupPartitionKeyRanges/MultipleSmallShards") {
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

	std::vector<KeyRange> partitions = co_await calculateBackupPartitionKeyRanges(&shards);

	ASSERT(partitions.size() == 100);
	ASSERT(partitions[0].begin == normalKeys.begin);
	ASSERT(partitions[partitions.size() - 1].end == normalKeys.end);
	for (int i = 1; i < partitions.size(); i++) {
		ASSERT(partitions[i - 1].end == partitions[i].begin);
	}
}
