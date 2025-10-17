/*
 * StorageMetrics.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_STORAGEMETRICS_G_H)
#define FDBSERVER_STORAGEMETRICS_G_H
#include "fdbserver/StorageMetrics.actor.g.h"
#elif !defined(FDBSERVER_STORAGEMETRICS_H)
#define FDBSERVER_STORAGEMETRICS_H
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h"

const StringRef STORAGESERVER_HISTOGRAM_GROUP = "StorageServer"_sr;
const StringRef FETCH_KEYS_LATENCY_HISTOGRAM = "FetchKeysLatency"_sr;
const StringRef FETCH_KEYS_BYTES_HISTOGRAM = "FetchKeysSize"_sr;
const StringRef FETCH_KEYS_BYTES_PER_SECOND_HISTOGRAM = "FetchKeysBandwidth"_sr;
const StringRef FETCH_KEYS_BYTES_PER_COMMIT_HISTOGRAM = "FetchKeysBytesPerCommit"_sr;
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
const StringRef SS_READ_RANGE_KV_PAIRS_RETURNED_HISTOGRAM = "SSReadRangeKVPairsReturned"_sr;

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

	int64_t addAndExpire(const Key& key, int64_t metric, double expiration);

	int64_t erase(KeyRef key);
	void erase(KeyRangeRef keys);

	void poll(KeyRangeMap<std::vector<PromiseStream<StorageMetrics>>>& waitMap, StorageMetrics m);

	void poll();

private:
	bool roll(int64_t metric) const;

	// return the sampled metric delta
	int64_t add(const Key& key, int64_t metric);
};

struct StorageServerMetrics {
	KeyRangeMap<std::vector<PromiseStream<StorageMetrics>>> waitMetricsMap;
	StorageMetricSample byteSample;

	// FIXME: iops is not effectively tested, and is not used by data distribution
	TransientStorageMetricSample iopsSample, bytesWriteSample;
	TransientStorageMetricSample bytesReadSample;
	TransientStorageMetricSample opsReadSample;

	StorageServerMetrics()
	  : byteSample(0), iopsSample(SERVER_KNOBS->IOPS_UNITS_PER_SAMPLE),
	    bytesWriteSample(SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE),
	    bytesReadSample(SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE),
	    opsReadSample(SERVER_KNOBS->OPS_READ_UNITS_PER_SAMPLE) {}

	StorageMetrics getMetrics(KeyRangeRef const& keys) const;

	void notify(const Key& key, StorageMetrics& metrics);

	void notifyBytesReadPerKSecond(const Key& key, int64_t in);

	void notifyBytes(RangeMap<Key, std::vector<PromiseStream<StorageMetrics>>, KeyRangeRef>::iterator shard,
	                 int64_t bytes);

	void notifyBytes(const KeyRef& key, int64_t bytes);

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
	                       double lastUpdate,
	                       int64_t bytesDurable,
	                       int64_t bytesInput,
	                       int ongoingBulkLoadTaskCount) const;

	Future<Void> waitMetrics(WaitMetricsRequest req, Future<Void> delay);

	std::vector<ReadHotRangeWithMetrics> getReadHotRanges(KeyRangeRef shard, int chunkCount, uint8_t splitType) const;

	void getReadHotRanges(ReadHotSubRangeRequest req) const;

	int64_t getHotShards(const KeyRange& range) const;

	std::vector<KeyRef> getSplitPoints(KeyRangeRef range, int64_t chunkSize, Optional<KeyRef> prefixToRemove) const;

	void getSplitPoints(SplitRangeRequest req, Optional<KeyRef> prefix) const;

	[[maybe_unused]] std::vector<ReadHotRangeWithMetrics> _getReadHotRanges(
	    KeyRangeRef shard,
	    double readDensityRatio,
	    int64_t baseChunkSize,
	    int64_t minShardReadBandwidthPerKSeconds) const;

private:
	static void collapse(KeyRangeMap<int>& map, KeyRef const& key);
	static void add(KeyRangeMap<int>& map, KeyRangeRef const& keys, int delta);
};

// Contains information about whether or not a key-value pair should be included in a byte sample.
//
// The invariant holds:
//    probability * sampledSize == size
struct ByteSampleInfo {
	// True if we've decided to sample this key-value pair.
	bool inSample;

	// Actual size of the key value pair.
	int64_t size;

	// Probability that the key-value pair will be sampled.
	// This is a function of key and value sizes.
	// The goal is to sample ~1/BYTE_SAMPLING_FACTOR of the key-value space,
	// which by default is 1/250th.
	double probability;

	// The recorded size of the sample (max of bytesPerSample, size).
	int64_t sampledSize;
};

// Determines whether a key-value pair should be included in a byte sample
// Also returns size information about the sample
ByteSampleInfo isKeyValueInSample(KeyRef key, int64_t totalKvSize);
inline ByteSampleInfo isKeyValueInSample(KeyValueRef keyValue) {
	return isKeyValueInSample(keyValue.key, keyValue.key.size() + keyValue.value.size());
}

struct CommonStorageCounters {
	CounterCollection cc;
	// read ops
	Counter finishedQueries, bytesQueried;

	// write ops
	// Bytes of the mutations that have been added to the memory of the storage server. When the data is durable
	// and cleared from the memory, we do not subtract it but add it to bytesDurable.
	Counter bytesInput;
	// Like bytesInput but without MVCC accounting. The size is counted as how much it takes when serialized. It
	// is basically the size of both parameters of the mutation and a 12 bytes overhead that keeps mutation type
	// and the lengths of both parameters.
	Counter mutationBytes;
	Counter mutations, setMutations, clearRangeMutations;

	// Bytes fetched by fetchKeys() for data movements. The size is counted as a collection of KeyValueRef.
	Counter bytesFetched;
	// The number of key-value pairs fetched by fetchKeys()
	Counter kvFetched;

	// Bytes replied for fetchKeys
	Counter kvFetchBytesServed;

	// The number of key-value pairs replied for fetchKeys
	Counter kvFetchServed;

	// The number of fetchKeys errors
	Counter fetchKeyErrors;

	// name and id are the inputs to CounterCollection initialization. If metrics provided, the caller should guarantee
	// the lifetime of metrics is longer than this counter
	CommonStorageCounters(const std::string& name,
	                      const std::string& id,
	                      const StorageServerMetrics* metrics = nullptr);
};

class IStorageMetricsService {
public:
	StorageServerMetrics metrics;

	// penalty used by loadBalance() to balance requests among service instances
	virtual double getPenalty() const { return 1; }

	virtual bool isReadable(KeyRangeRef const& keys) const { return true; }

	virtual void addActor(Future<Void> future) = 0;

	virtual void getSplitPoints(SplitRangeRequest const& req) = 0;

	virtual Future<Void> waitMetricsTenantAware(const WaitMetricsRequest& req) = 0;

	virtual void getStorageMetrics(const GetStorageMetricsRequest& req) = 0;

	virtual void getSplitMetrics(const SplitMetricsRequest& req) = 0;

	virtual void getHotRangeMetrics(const ReadHotSubRangeRequest& req) = 0;

	virtual int64_t getHotShardsMetrics(const KeyRange& range) = 0;

	// NOTE: also need to have this function but template can't be a virtual so...
	// template <class Reply>
	// void sendErrorWithPenalty(const ReplyPromise<Reply>& promise, const Error& err, double penalty);
};

ACTOR template <class ServiceType>
Future<Void> serveStorageMetricsRequests(ServiceType* self, StorageServerInterface ssi) {
	state Future<Void> doPollMetrics = Void();
	loop {
		choose {
			when(state WaitMetricsRequest req = waitNext(ssi.waitMetrics.getFuture())) {
				if (!req.tenantInfo.hasTenant() && !self->isReadable(req.keys)) {
					CODE_PROBE(true, "waitMetrics immediate wrong_shard_server()");
					self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
				} else {
					self->addActor(self->waitMetricsTenantAware(req));
				}
			}
			when(SplitMetricsRequest req = waitNext(ssi.splitMetrics.getFuture())) {
				if (!self->isReadable(req.keys)) {
					CODE_PROBE(true, "splitMetrics immediate wrong_shard_server()");
					self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
				} else {
					self->getSplitMetrics(req);
				}
			}
			when(GetStorageMetricsRequest req = waitNext(ssi.getStorageMetrics.getFuture())) {
				self->getStorageMetrics(req);
			}
			when(ReadHotSubRangeRequest req = waitNext(ssi.getReadHotRanges.getFuture())) {
				self->getHotRangeMetrics(req);
			}
			when(SplitRangeRequest req = waitNext(ssi.getRangeSplitPoints.getFuture())) {
				if ((!req.tenantInfo.hasTenant() && !self->isReadable(req.keys)) ||
				    (req.tenantInfo.hasTenant() &&
				     !self->isReadable(req.keys.withPrefix(req.tenantInfo.prefix.get())))) {
					CODE_PROBE(true, "getSplitPoints immediate wrong_shard_server()");
					self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
				} else {
					self->getSplitPoints(req);
				}
			}
			when(wait(doPollMetrics)) {
				self->metrics.poll();
				doPollMetrics = delay(SERVER_KNOBS->STORAGE_SERVER_POLL_METRICS_DELAY);
			}
		}
	}
}
#include "flow/unactorcompiler.h"
#endif // FDBSERVER_STORAGEMETRICS_H
