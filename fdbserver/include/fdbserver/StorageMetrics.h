/*
 * StorageMetrics.h
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

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/Knobs.h"

const StringRef STORAGESERVER_HISTOGRAM_GROUP = "StorageServer"_sr;
const StringRef FETCH_KEYS_LATENCY_HISTOGRAM = "FetchKeysLatency"_sr;
const StringRef FETCH_KEYS_BYTES_HISTOGRAM = "FetchKeysSize"_sr;
const StringRef FETCH_KEYS_BYTES_PER_SECOND_HISTOGRAM = "FetchKeysBandwidth"_sr;
const StringRef TLOG_CURSOR_READS_LATENCY_HISTOGRAM = "TLogCursorReadsLatency"_sr;
const StringRef SS_VERSION_LOCK_LATENCY_HISTOGRAM = "SSVersionLockLatency"_sr;
const StringRef EAGER_READS_LATENCY_HISTOGRAM = "EagerReadsLatency"_sr;
const StringRef FETCH_KEYS_PTREE_UPDATES_LATENCY_HISTOGRAM = "FetchKeysPTreeUpdatesLatency"_sr;
const StringRef TLOG_MSGS_PTREE_UPDATES_LATENCY_HISTOGRAM = "TLogMsgsPTreeUpdatesLatency"_sr;
const StringRef STORAGE_UPDATES_DURABLE_LATENCY_HISTOGRAM = "StorageUpdatesDurableLatency"_sr;
const StringRef STORAGE_COMMIT_LATENCY_HISTOGRAM = "StorageCommitLatency"_sr;
const StringRef SS_DURABLE_VERSION_UPDATE_LATENCY_HISTOGRAM = "SSDurableVersionUpdateLatency"_sr;
const StringRef SS_READ_RANGE_BYTES_RETURNED_HISTOGRAM = "SSReadRangeBytesReturned"_sr;
const StringRef SS_READ_RANGE_BYTES_LIMIT_HISTOGRAM = "SSReadRangeBytesLimit"_sr;

struct StorageMetricSample {
	IndexedSet<Key, int64_t> sample;
	int64_t metricUnitsPerSample;

	explicit StorageMetricSample(int64_t metricUnitsPerSample) : metricUnitsPerSample(metricUnitsPerSample) {}

	int64_t getEstimate(KeyRangeRef keys) const;
	KeyRef splitEstimate(KeyRangeRef range, int64_t offset, bool front = true) const;
};

struct TransientStorageMetricSample : StorageMetricSample {
	Deque<std::pair<double, std::pair<Key, int64_t>>> queue;

	explicit TransientStorageMetricSample(int64_t metricUnitsPerSample) : StorageMetricSample(metricUnitsPerSample) {}

	int64_t addAndExpire(KeyRef key, int64_t metric, double expiration);

	int64_t erase(KeyRef key);
	void erase(KeyRangeRef keys);

	void poll(KeyRangeMap<std::vector<PromiseStream<StorageMetrics>>>& waitMap, StorageMetrics m);

	void poll();

private:
	bool roll(KeyRef key, int64_t metric) const;
	int64_t add(KeyRef key, int64_t metric);
};

struct StorageServerMetrics {
	KeyRangeMap<std::vector<PromiseStream<StorageMetrics>>> waitMetricsMap;
	StorageMetricSample byteSample;
	TransientStorageMetricSample iopsSample,
	    bandwidthSample; // FIXME: iops and bandwidth calculations are not effectively tested, since they aren't
	                     // currently used by data distribution
	TransientStorageMetricSample bytesReadSample;

	StorageServerMetrics()
	  : byteSample(0), iopsSample(SERVER_KNOBS->IOPS_UNITS_PER_SAMPLE),
	    bandwidthSample(SERVER_KNOBS->BANDWIDTH_UNITS_PER_SAMPLE),
	    bytesReadSample(SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE) {}

	StorageMetrics getMetrics(KeyRangeRef const& keys) const;

	void notify(KeyRef key, StorageMetrics& metrics);

	void notifyBytesReadPerKSecond(KeyRef key, int64_t in);

	void notifyBytes(RangeMap<Key, std::vector<PromiseStream<StorageMetrics>>, KeyRangeRef>::iterator shard,
	                 int64_t bytes);

	void notifyBytes(KeyRef key, int64_t bytes);

	void notifyNotReadable(KeyRangeRef keys);

	void poll();

	// static void waitMetrics( StorageServerMetrics* const& self, WaitMetricsRequest const& req );

	KeyRef getSplitKey(int64_t remaining,
	                   int64_t estimated,
	                   int64_t limits,
	                   int64_t used,
	                   int64_t infinity,
	                   bool isLastShard,
	                   const StorageMetricSample& sample,
	                   double divisor,
	                   KeyRef const& lastKey,
	                   KeyRef const& key,
	                   bool hasUsed) const;

	void splitMetrics(SplitMetricsRequest req) const;

	void getStorageMetrics(GetStorageMetricsRequest req,
	                       StorageBytes sb,
	                       double bytesInputRate,
	                       int64_t versionLag,
	                       double lastUpdate) const;

	Future<Void> waitMetrics(WaitMetricsRequest req, Future<Void> delay);

	std::vector<ReadHotRangeWithMetrics> getReadHotRanges(KeyRangeRef shard,
	                                                      double readDensityRatio,
	                                                      int64_t baseChunkSize,
	                                                      int64_t minShardReadBandwidthPerKSeconds) const;

	void getReadHotRanges(ReadHotSubRangeRequest req) const;

	std::vector<KeyRef> getSplitPoints(KeyRangeRef range, int64_t chunkSize, Optional<Key> prefixToRemove) const;

	void getSplitPoints(SplitRangeRequest req, Optional<Key> prefix) const;

private:
	static void collapse(KeyRangeMap<int>& map, KeyRef const& key);
	static void add(KeyRangeMap<int>& map, KeyRangeRef const& keys, int delta);
};

// Contains information about whether or not a key-value pair should be included in a byte sample
// Also contains size information about the byte sample
struct ByteSampleInfo {
	bool inSample;

	// Actual size of the key value pair
	int64_t size;

	// The recorded size of the sample (max of bytesPerSample, size)
	int64_t sampledSize;
};

// Determines whether a key-value pair should be included in a byte sample
// Also returns size information about the sample
ByteSampleInfo isKeyValueInSample(KeyValueRef keyValue);
