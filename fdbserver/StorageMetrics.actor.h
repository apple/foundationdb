/*
 * StorageMetrics.actor.h
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

// Included via StorageMetrics.h
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const StringRef STORAGESERVER_HISTOGRAM_GROUP = LiteralStringRef("StorageServer");
const StringRef FETCH_KEYS_LATENCY_HISTOGRAM = LiteralStringRef("FetchKeysLatency");
const StringRef FETCH_KEYS_BYTES_HISTOGRAM = LiteralStringRef("FetchKeysSize");
const StringRef FETCH_KEYS_BYTES_PER_SECOND_HISTOGRAM = LiteralStringRef("FetchKeysBandwidth");
const StringRef TLOG_CURSOR_READS_LATENCY_HISTOGRAM = LiteralStringRef("TLogCursorReadsLatency");
const StringRef SS_VERSION_LOCK_LATENCY_HISTOGRAM = LiteralStringRef("SSVersionLockLatency");
const StringRef EAGER_READS_LATENCY_HISTOGRAM = LiteralStringRef("EagerReadsLatency");
const StringRef FETCH_KEYS_PTREE_UPDATES_LATENCY_HISTOGRAM = LiteralStringRef("FetchKeysPTreeUpdatesLatency");
const StringRef TLOG_MSGS_PTREE_UPDATES_LATENCY_HISTOGRAM = LiteralStringRef("TLogMsgsPTreeUpdatesLatency");
const StringRef STORAGE_UPDATES_DURABLE_LATENCY_HISTOGRAM = LiteralStringRef("StorageUpdatesDurableLatency");
const StringRef STORAGE_COMMIT_LATENCY_HISTOGRAM = LiteralStringRef("StorageCommitLatency");
const StringRef SS_DURABLE_VERSION_UPDATE_LATENCY_HISTOGRAM = LiteralStringRef("SSDurableVersionUpdateLatency");

struct StorageMetricSample {
	IndexedSet<Key, int64_t> sample;
	int64_t metricUnitsPerSample;

	StorageMetricSample(int64_t metricUnitsPerSample) : metricUnitsPerSample(metricUnitsPerSample) {}

	int64_t getEstimate(KeyRangeRef keys) const { return sample.sumRange(keys.begin, keys.end); }
	KeyRef splitEstimate(KeyRangeRef range, int64_t offset, bool front = true) const {
		auto fwd_split = sample.index(front ? sample.sumTo(sample.lower_bound(range.begin)) + offset
		                                    : sample.sumTo(sample.lower_bound(range.end)) - offset);

		if (fwd_split == sample.end() || *fwd_split >= range.end)
			return range.end;

		if (!front && *fwd_split <= range.begin)
			return range.begin;

		auto bck_split = fwd_split;

		// Butterfly search - start at midpoint then go in both directions.
		while ((fwd_split != sample.end() && *fwd_split < range.end) ||
		       (bck_split != sample.begin() && *bck_split > range.begin)) {
			if (bck_split != sample.begin() && *bck_split > range.begin) {
				auto it = bck_split;
				bck_split.decrementNonEnd();

				KeyRef split = keyBetween(KeyRangeRef(
				    bck_split != sample.begin() ? std::max<KeyRef>(*bck_split, range.begin) : range.begin, *it));
				if (!front || (getEstimate(KeyRangeRef(range.begin, split)) > 0 &&
				               split.size() <= CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT))
					return split;
			}

			if (fwd_split != sample.end() && *fwd_split < range.end) {
				auto it = fwd_split;
				++it;

				KeyRef split = keyBetween(
				    KeyRangeRef(*fwd_split, it != sample.end() ? std::min<KeyRef>(*it, range.end) : range.end));
				if (front || (getEstimate(KeyRangeRef(split, range.end)) > 0 &&
				              split.size() <= CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT))
					return split;

				fwd_split = it;
			}
		}

		// If we didn't return above, we didn't find anything.
		TraceEvent(SevWarn, "CannotSplitLastSampleKey").detail("Range", range).detail("Offset", offset);
		return front ? range.end : range.begin;
	}
};

TEST_CASE("/fdbserver/StorageMetricSample/simple") {
	StorageMetricSample s(1000);
	s.sample.insert(LiteralStringRef("Apple"), 1000);
	s.sample.insert(LiteralStringRef("Banana"), 2000);
	s.sample.insert(LiteralStringRef("Cat"), 1000);
	s.sample.insert(LiteralStringRef("Cathode"), 1000);
	s.sample.insert(LiteralStringRef("Dog"), 1000);

	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D"))) == 5000);
	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("E"))) == 6000);
	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("B"), LiteralStringRef("C"))) == 2000);

	// ASSERT(s.splitEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 3500) ==
	// LiteralStringRef("Cat"));

	return Void();
}

struct TransientStorageMetricSample : StorageMetricSample {
	Deque<std::pair<double, std::pair<Key, int64_t>>> queue;

	TransientStorageMetricSample(int64_t metricUnitsPerSample) : StorageMetricSample(metricUnitsPerSample) {}

	// Returns the sampled metric value (possibly 0, possibly increased by the sampling factor)
	int64_t addAndExpire(KeyRef key, int64_t metric, double expiration) {
		int64_t x = add(key, metric);
		if (x)
			queue.emplace_back(expiration, std::make_pair(*sample.find(key), -x));
		return x;
	}

	// FIXME: both versions of erase are broken, because they do not remove items in the queue with will subtract a
	// metric from the value sometime in the future
	int64_t erase(KeyRef key) {
		auto it = sample.find(key);
		if (it == sample.end())
			return 0;
		int64_t x = sample.getMetric(it);
		sample.erase(it);
		return x;
	}
	void erase(KeyRangeRef keys) { sample.erase(keys.begin, keys.end); }

	void poll(KeyRangeMap<std::vector<PromiseStream<StorageMetrics>>>& waitMap, StorageMetrics m) {
		double now = ::now();
		while (queue.size() && queue.front().first <= now) {
			KeyRef key = queue.front().second.first;
			int64_t delta = queue.front().second.second;
			ASSERT(delta != 0);

			if (sample.addMetric(key, delta) == 0)
				sample.erase(key);

			StorageMetrics deltaM = m * delta;
			auto v = waitMap[key];
			for (int i = 0; i < v.size(); i++) {
				TEST(true); // TransientStorageMetricSample poll update
				v[i].send(deltaM);
			}

			queue.pop_front();
		}
	}

	void poll() {
		double now = ::now();
		while (queue.size() && queue.front().first <= now) {
			KeyRef key = queue.front().second.first;
			int64_t delta = queue.front().second.second;
			ASSERT(delta != 0);

			if (sample.addMetric(key, delta) == 0)
				sample.erase(key);

			queue.pop_front();
		}
	}

private:
	bool roll(KeyRef key, int64_t metric) const {
		return deterministicRandom()->random01() <
		       (double)metric / metricUnitsPerSample; //< SOMEDAY: Better randomInt64?
	}

	int64_t add(KeyRef key, int64_t metric) {
		if (!metric)
			return 0;
		int64_t mag = metric < 0 ? -metric : metric;

		if (mag < metricUnitsPerSample) {
			if (!roll(key, mag))
				return 0;
			metric = metric < 0 ? -metricUnitsPerSample : metricUnitsPerSample;
		}

		if (sample.addMetric(key, metric) == 0)
			sample.erase(key);

		return metric;
	}
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

	// Get the current estimated metrics for the given keys
	StorageMetrics getMetrics(KeyRangeRef const& keys) const {
		StorageMetrics result;
		result.bytes = byteSample.getEstimate(keys);
		result.bytesPerKSecond =
		    bandwidthSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		result.iosPerKSecond =
		    iopsSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		result.bytesReadPerKSecond =
		    bytesReadSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		return result;
	}

	// Called when metrics should change (IO for a given key)
	// Notifies waiting WaitMetricsRequests through waitMetricsMap, and updates metricsAverageQueue and metricsSampleMap
	void notify(KeyRef key, StorageMetrics& metrics) {
		ASSERT(metrics.bytes == 0); // ShardNotifyMetrics
		if (g_network->isSimulated()) {
			TEST(metrics.bytesPerKSecond != 0); // ShardNotifyMetrics bytes
			TEST(metrics.iosPerKSecond != 0); // ShardNotifyMetrics ios
			TEST(metrics.bytesReadPerKSecond != 0); // ShardNotifyMetrics bytesRead
		}

		double expire = now() + SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL;

		StorageMetrics notifyMetrics;

		if (metrics.bytesPerKSecond)
			notifyMetrics.bytesPerKSecond = bandwidthSample.addAndExpire(key, metrics.bytesPerKSecond, expire) *
			                                SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		if (metrics.iosPerKSecond)
			notifyMetrics.iosPerKSecond = iopsSample.addAndExpire(key, metrics.iosPerKSecond, expire) *
			                              SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		if (metrics.bytesReadPerKSecond)
			notifyMetrics.bytesReadPerKSecond = bytesReadSample.addAndExpire(key, metrics.bytesReadPerKSecond, expire) *
			                                    SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		if (!notifyMetrics.allZero()) {
			auto& v = waitMetricsMap[key];
			for (int i = 0; i < v.size(); i++) {
				if (g_network->isSimulated()) {
					TEST(true); // shard notify metrics
				}
				// ShardNotifyMetrics
				v[i].send(notifyMetrics);
			}
		}
	}

	// Due to the fact that read sampling will be called on all reads, use this specialized function to avoid overhead
	// around branch misses and unnecessary stack allocation which eventually addes up under heavy load.
	void notifyBytesReadPerKSecond(KeyRef key, int64_t in) {
		double expire = now() + SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL;
		int64_t bytesReadPerKSecond =
		    bytesReadSample.addAndExpire(key, in, expire) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		if (bytesReadPerKSecond > 0) {
			StorageMetrics notifyMetrics;
			notifyMetrics.bytesReadPerKSecond = bytesReadPerKSecond;
			auto& v = waitMetricsMap[key];
			for (int i = 0; i < v.size(); i++) {
				TEST(true); // ShardNotifyMetrics
				v[i].send(notifyMetrics);
			}
		}
	}

	// Called by StorageServerDisk when the size of a key in byteSample changes, to notify WaitMetricsRequest
	// Should not be called for keys past allKeys.end
	void notifyBytes(RangeMap<Key, std::vector<PromiseStream<StorageMetrics>>, KeyRangeRef>::iterator shard,
	                 int64_t bytes) {
		ASSERT(shard.end() <= allKeys.end);

		StorageMetrics notifyMetrics;
		notifyMetrics.bytes = bytes;
		for (int i = 0; i < shard.value().size(); i++) {
			TEST(true); // notifyBytes
			shard.value()[i].send(notifyMetrics);
		}
	}

	// Called by StorageServerDisk when the size of a key in byteSample changes, to notify WaitMetricsRequest
	void notifyBytes(KeyRef key, int64_t bytes) {
		if (key >= allKeys.end) // Do not notify on changes to internal storage server state
			return;

		notifyBytes(waitMetricsMap.rangeContaining(key), bytes);
	}

	// Called when a range of keys becomes unassigned (and therefore not readable), to notify waiting
	// WaitMetricsRequests (also other types of wait
	//   requests in the future?)
	void notifyNotReadable(KeyRangeRef keys) {
		auto rs = waitMetricsMap.intersectingRanges(keys);
		for (auto r = rs.begin(); r != rs.end(); ++r) {
			auto& v = r->value();
			TEST(v.size()); // notifyNotReadable() sending errors to intersecting ranges
			for (int n = 0; n < v.size(); n++)
				v[n].sendError(wrong_shard_server());
		}
	}

	// Called periodically (~1 sec intervals) to remove older IOs from the averages
	// Removes old entries from metricsAverageQueue, updates metricsSampleMap accordingly, and notifies
	//   WaitMetricsRequests through waitMetricsMap.
	void poll() {
		{
			StorageMetrics m;
			m.bytesPerKSecond = SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
			bandwidthSample.poll(waitMetricsMap, m);
		}
		{
			StorageMetrics m;
			m.iosPerKSecond = SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
			iopsSample.poll(waitMetricsMap, m);
		}
		{
			StorageMetrics m;
			m.bytesReadPerKSecond = SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
			bytesReadSample.poll(waitMetricsMap, m);
		}
		// bytesSample doesn't need polling because we never call addExpire() on it
	}

	// static void waitMetrics( StorageServerMetrics* const& self, WaitMetricsRequest const& req );

	// This function can run on untrusted user data.  We must validate all divisions carefully.
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
	                   bool hasUsed) const {
		ASSERT(remaining >= 0);
		ASSERT(limits > 0);
		ASSERT(divisor > 0);

		if (limits < infinity / 2) {
			int64_t expectedSize;
			if (isLastShard || remaining > estimated) {
				double remaining_divisor = (double(remaining) / limits) + 0.5;
				expectedSize = remaining / remaining_divisor;
			} else {
				// If we are here, then estimated >= remaining >= 0
				double estimated_divisor = (double(estimated) / limits) + 0.5;
				expectedSize = remaining / estimated_divisor;
			}

			if (remaining > expectedSize) {
				// This does the conversion from native units to bytes using the divisor.
				double offset = (expectedSize - used) / divisor;
				if (offset <= 0)
					return hasUsed ? lastKey : key;
				return sample.splitEstimate(
				    KeyRangeRef(lastKey, key),
				    offset * ((1.0 - SERVER_KNOBS->SPLIT_JITTER_AMOUNT) +
				              2 * deterministicRandom()->random01() * SERVER_KNOBS->SPLIT_JITTER_AMOUNT));
			}
		}

		return key;
	}

	void splitMetrics(SplitMetricsRequest req) const {
		try {
			SplitMetricsReply reply;
			KeyRef lastKey = req.keys.begin;
			StorageMetrics used = req.used;
			StorageMetrics estimated = req.estimated;
			StorageMetrics remaining = getMetrics(req.keys) + used;

			//TraceEvent("SplitMetrics").detail("Begin", req.keys.begin).detail("End", req.keys.end).detail("Remaining", remaining.bytes).detail("Used", used.bytes);

			while (true) {
				if (remaining.bytes < 2 * SERVER_KNOBS->MIN_SHARD_BYTES)
					break;
				KeyRef key = req.keys.end;
				bool hasUsed = used.bytes != 0 || used.bytesPerKSecond != 0 || used.iosPerKSecond != 0;
				key = getSplitKey(remaining.bytes,
				                  estimated.bytes,
				                  req.limits.bytes,
				                  used.bytes,
				                  req.limits.infinity,
				                  req.isLastShard,
				                  byteSample,
				                  1,
				                  lastKey,
				                  key,
				                  hasUsed);
				if (used.bytes < SERVER_KNOBS->MIN_SHARD_BYTES)
					key = std::max(key,
					               byteSample.splitEstimate(KeyRangeRef(lastKey, req.keys.end),
					                                        SERVER_KNOBS->MIN_SHARD_BYTES - used.bytes));
				key = getSplitKey(remaining.iosPerKSecond,
				                  estimated.iosPerKSecond,
				                  req.limits.iosPerKSecond,
				                  used.iosPerKSecond,
				                  req.limits.infinity,
				                  req.isLastShard,
				                  iopsSample,
				                  SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS,
				                  lastKey,
				                  key,
				                  hasUsed);
				key = getSplitKey(remaining.bytesPerKSecond,
				                  estimated.bytesPerKSecond,
				                  req.limits.bytesPerKSecond,
				                  used.bytesPerKSecond,
				                  req.limits.infinity,
				                  req.isLastShard,
				                  bandwidthSample,
				                  SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS,
				                  lastKey,
				                  key,
				                  hasUsed);
				ASSERT(key != lastKey || hasUsed);
				if (key == req.keys.end)
					break;
				reply.splits.push_back_deep(reply.splits.arena(), key);

				StorageMetrics diff = (getMetrics(KeyRangeRef(lastKey, key)) + used);
				remaining -= diff;
				estimated -= diff;

				used = StorageMetrics();
				lastKey = key;
			}

			reply.used = getMetrics(KeyRangeRef(lastKey, req.keys.end)) + used;
			req.reply.send(reply);
		} catch (Error& e) {
			req.reply.sendError(e);
		}
	}

	void getStorageMetrics(GetStorageMetricsRequest req,
	                       StorageBytes sb,
	                       double bytesInputRate,
	                       int64_t versionLag,
	                       double lastUpdate) const {
		GetStorageMetricsReply rep;

		// SOMEDAY: make bytes dynamic with hard disk space
		rep.load = getMetrics(allKeys);

		if (sb.free < 1e9) {
			TraceEvent(SevWarn, "PhysicalDiskMetrics")
			    .suppressFor(60.0)
			    .detail("Free", sb.free)
			    .detail("Total", sb.total)
			    .detail("Available", sb.available)
			    .detail("Load", rep.load.bytes);
		}

		rep.available.bytes = sb.available;
		rep.available.iosPerKSecond = 10e6;
		rep.available.bytesPerKSecond = 100e9;
		rep.available.bytesReadPerKSecond = 100e9;

		rep.capacity.bytes = sb.total;
		rep.capacity.iosPerKSecond = 10e6;
		rep.capacity.bytesPerKSecond = 100e9;
		rep.capacity.bytesReadPerKSecond = 100e9;

		rep.bytesInputRate = bytesInputRate;

		rep.versionLag = versionLag;
		rep.lastUpdate = lastUpdate;

		req.reply.send(rep);
	}

	Future<Void> waitMetrics(WaitMetricsRequest req, Future<Void> delay);

	// Given a read hot shard, this function will divide the shard into chunks and find those chunks whose
	// readBytes/sizeBytes exceeds the `readDensityRatio`. Please make sure to run unit tests
	// `StorageMetricsSampleTests.txt` after change made.
	std::vector<ReadHotRangeWithMetrics> getReadHotRanges(KeyRangeRef shard,
	                                                      double readDensityRatio,
	                                                      int64_t baseChunkSize,
	                                                      int64_t minShardReadBandwidthPerKSeconds) const {
		std::vector<ReadHotRangeWithMetrics> toReturn;

		double shardSize = (double)byteSample.getEstimate(shard);
		int64_t shardReadBandwidth = bytesReadSample.getEstimate(shard);
		if (shardReadBandwidth * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS <=
		    minShardReadBandwidthPerKSeconds) {
			return toReturn;
		}
		if (shardSize <= baseChunkSize) {
			// Shard is small, use it as is
			if (bytesReadSample.getEstimate(shard) > (readDensityRatio * shardSize)) {
				toReturn.emplace_back(shard,
				                      bytesReadSample.getEstimate(shard) / shardSize,
				                      bytesReadSample.getEstimate(shard) /
				                          SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
			}
			return toReturn;
		}
		KeyRef beginKey = shard.begin;
		auto endKey =
		    byteSample.sample.index(byteSample.sample.sumTo(byteSample.sample.lower_bound(beginKey)) + baseChunkSize);
		while (endKey != byteSample.sample.end()) {
			if (*endKey > shard.end) {
				endKey = byteSample.sample.lower_bound(shard.end);
				if (*endKey == beginKey) {
					// No need to increment endKey since otherwise it would stuck here forever.
					break;
				}
			}
			if (*endKey == beginKey) {
				++endKey;
				continue;
			}
			if (bytesReadSample.getEstimate(KeyRangeRef(beginKey, *endKey)) >
			    (readDensityRatio * std::max(baseChunkSize, byteSample.getEstimate(KeyRangeRef(beginKey, *endKey))))) {
				auto range = KeyRangeRef(beginKey, *endKey);
				if (!toReturn.empty() && toReturn.back().keys.end == range.begin) {
					// in case two consecutive chunks both are over the ratio, merge them.
					range = KeyRangeRef(toReturn.back().keys.begin, *endKey);
					toReturn.pop_back();
				}
				toReturn.emplace_back(
				    range,
				    (double)bytesReadSample.getEstimate(range) / std::max(baseChunkSize, byteSample.getEstimate(range)),
				    bytesReadSample.getEstimate(range) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
			}
			beginKey = *endKey;
			endKey = byteSample.sample.index(byteSample.sample.sumTo(byteSample.sample.lower_bound(beginKey)) +
			                                 baseChunkSize);
		}
		return toReturn;
	}

	void getReadHotRanges(ReadHotSubRangeRequest req) const {
		ReadHotSubRangeReply reply;
		auto _ranges = getReadHotRanges(req.keys,
		                                SERVER_KNOBS->SHARD_MAX_READ_DENSITY_RATIO,
		                                SERVER_KNOBS->READ_HOT_SUB_RANGE_CHUNK_SIZE,
		                                SERVER_KNOBS->SHARD_READ_HOT_BANDWITH_MIN_PER_KSECONDS);
		reply.readHotRanges = VectorRef(_ranges.data(), _ranges.size());
		req.reply.send(reply);
	}

	std::vector<KeyRef> getSplitPoints(KeyRangeRef range, int64_t chunkSize, Optional<Key> prefixToRemove) {
		std::vector<KeyRef> toReturn;
		KeyRef beginKey = range.begin;
		IndexedSet<Key, int64_t>::iterator endKey =
		    byteSample.sample.index(byteSample.sample.sumTo(byteSample.sample.lower_bound(beginKey)) + chunkSize);
		while (endKey != byteSample.sample.end()) {
			if (*endKey > range.end) {
				break;
			}
			if (*endKey == beginKey) {
				++endKey;
				continue;
			}
			KeyRef splitPoint = *endKey;
			if (prefixToRemove.present()) {
				splitPoint = splitPoint.removePrefix(prefixToRemove.get());
			}
			toReturn.push_back(splitPoint);
			beginKey = *endKey;
			endKey =
			    byteSample.sample.index(byteSample.sample.sumTo(byteSample.sample.lower_bound(beginKey)) + chunkSize);
		}
		return toReturn;
	}

	void getSplitPoints(SplitRangeRequest req, Optional<Key> prefix) {
		SplitRangeReply reply;
		KeyRangeRef range = req.keys;
		if (prefix.present()) {
			range = range.withPrefix(prefix.get(), req.arena);
		}
		std::vector<KeyRef> points = getSplitPoints(range, req.chunkSize, prefix);

		reply.splitPoints.append_deep(reply.splitPoints.arena(), points.data(), points.size());
		req.reply.send(reply);
	}

private:
	static void collapse(KeyRangeMap<int>& map, KeyRef const& key) {
		auto range = map.rangeContaining(key);
		if (range == map.ranges().begin() || range == map.ranges().end())
			return;
		int value = range->value();
		auto prev = range;
		--prev;
		if (prev->value() != value)
			return;
		KeyRange keys = KeyRangeRef(prev->begin(), range->end());
		map.insert(keys, value);
	}

	static void add(KeyRangeMap<int>& map, KeyRangeRef const& keys, int delta) {
		auto rs = map.modify(keys);
		for (auto r = rs.begin(); r != rs.end(); ++r)
			r->value() += delta;
		collapse(map, keys.begin);
		collapse(map, keys.end);
	}
};

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/simple") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 800 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 2000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 1 && t[0] == LiteralStringRef("Bah"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/multipleReturnedPoints") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 800 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 600 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 3 && t[0] == LiteralStringRef("Absolute") && t[1] == LiteralStringRef("Apple") &&
	       t[2] == LiteralStringRef("Bah"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/noneSplitable") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 800 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 10000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 0);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/chunkTooLarge") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 10 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 10 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 30 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 1000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 0);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/simple") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Banana"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cat"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cathode"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Dog"), 1000 * sampleUnit);

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 1 && (*t.begin()).keys.begin == LiteralStringRef("Bah") &&
	       (*t.begin()).keys.end == LiteralStringRef("Bob"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/moreThanOneRange") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Banana"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cat"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cathode"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Dog"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Final"), 2000 * sampleUnit);

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Dah"), 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == LiteralStringRef("Bah") &&
	       (*t.begin()).keys.end == LiteralStringRef("Bob"));
	ASSERT(t.at(1).keys.begin == LiteralStringRef("Cat") && t.at(1).keys.end == LiteralStringRef("Dah"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/consecutiveRanges") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Banana"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Bucket"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cat"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cathode"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Dog"), 5000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Final"), 2000 * sampleUnit);

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Dah"), 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == LiteralStringRef("Bah") &&
	       (*t.begin()).keys.end == LiteralStringRef("But"));
	ASSERT(t.at(1).keys.begin == LiteralStringRef("Cat") && t.at(1).keys.end == LiteralStringRef("Dah"));

	return Void();
}

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

#include "flow/unactorcompiler.h"
