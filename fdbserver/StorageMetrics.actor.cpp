/*
 * StorageMetrics.actor.cpp
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

#include "flow/UnitTest.h"
#include "fdbserver/StorageMetrics.h"
#include "flow/actorcompiler.h" // This must be the last #include.

int64_t StorageMetricSample::getEstimate(KeyRangeRef keys) const {
	return sample.sumRange(keys.begin, keys.end);
}

KeyRef StorageMetricSample::splitEstimate(KeyRangeRef range, int64_t offset, bool front) const {
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

			KeyRef split =
			    keyBetween(KeyRangeRef(*fwd_split, it != sample.end() ? std::min<KeyRef>(*it, range.end) : range.end));
			if (front ||
			    (getEstimate(KeyRangeRef(split, range.end)) > 0 && split.size() <= CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT))
				return split;

			fwd_split = it;
		}
	}

	// If we didn't return above, we didn't find anything.
	TraceEvent(SevWarn, "CannotSplitLastSampleKey").detail("Range", range).detail("Offset", offset);
	return front ? range.end : range.begin;
}

// Get the current estimated metrics for the given keys
StorageMetrics StorageServerMetrics::getMetrics(KeyRangeRef const& keys) const {
	StorageMetrics result;
	result.bytes = byteSample.getEstimate(keys);
	result.bytesPerKSecond =
	    bandwidthSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	result.iosPerKSecond = iopsSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	result.bytesReadPerKSecond =
	    bytesReadSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	return result;
}

// Called when metrics should change (IO for a given key)
// Notifies waiting WaitMetricsRequests through waitMetricsMap, and updates metricsAverageQueue and metricsSampleMap
void StorageServerMetrics::notify(KeyRef key, StorageMetrics& metrics) {
	ASSERT(metrics.bytes == 0); // ShardNotifyMetrics
	if (g_network->isSimulated()) {
		CODE_PROBE(metrics.bytesPerKSecond != 0, "ShardNotifyMetrics bytes");
		CODE_PROBE(metrics.iosPerKSecond != 0, "ShardNotifyMetrics ios");
		CODE_PROBE(metrics.bytesReadPerKSecond != 0, "ShardNotifyMetrics bytesRead", probe::decoration::rare);
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
				CODE_PROBE(true, "shard notify metrics");
			}
			// ShardNotifyMetrics
			v[i].send(notifyMetrics);
		}
	}
}

// Due to the fact that read sampling will be called on all reads, use this specialized function to avoid overhead
// around branch misses and unnecessary stack allocation which eventually addes up under heavy load.
void StorageServerMetrics::notifyBytesReadPerKSecond(KeyRef key, int64_t in) {
	double expire = now() + SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL;
	int64_t bytesReadPerKSecond =
	    bytesReadSample.addAndExpire(key, in, expire) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	if (bytesReadPerKSecond > 0) {
		StorageMetrics notifyMetrics;
		notifyMetrics.bytesReadPerKSecond = bytesReadPerKSecond;
		auto& v = waitMetricsMap[key];
		for (int i = 0; i < v.size(); i++) {
			CODE_PROBE(true, "ShardNotifyMetrics");
			v[i].send(notifyMetrics);
		}
	}
}

// Called by StorageServerDisk when the size of a key in byteSample changes, to notify WaitMetricsRequest
// Should not be called for keys past allKeys.end
void StorageServerMetrics::notifyBytes(
    RangeMap<Key, std::vector<PromiseStream<StorageMetrics>>, KeyRangeRef>::iterator shard,
    int64_t bytes) {
	ASSERT(shard.end() <= allKeys.end);

	StorageMetrics notifyMetrics;
	notifyMetrics.bytes = bytes;
	for (int i = 0; i < shard.value().size(); i++) {
		CODE_PROBE(true, "notifyBytes");
		shard.value()[i].send(notifyMetrics);
	}
}

// Called by StorageServerDisk when the size of a key in byteSample changes, to notify WaitMetricsRequest
void StorageServerMetrics::notifyBytes(KeyRef key, int64_t bytes) {
	if (key >= allKeys.end) // Do not notify on changes to internal storage server state
		return;

	notifyBytes(waitMetricsMap.rangeContaining(key), bytes);
}

// Called when a range of keys becomes unassigned (and therefore not readable), to notify waiting
// WaitMetricsRequests (also other types of wait
//   requests in the future?)
void StorageServerMetrics::notifyNotReadable(KeyRangeRef keys) {
	auto rs = waitMetricsMap.intersectingRanges(keys);
	for (auto r = rs.begin(); r != rs.end(); ++r) {
		auto& v = r->value();
		CODE_PROBE(v.size(), "notifyNotReadable() sending errors to intersecting ranges");
		for (int n = 0; n < v.size(); n++)
			v[n].sendError(wrong_shard_server());
	}
}

// Called periodically (~1 sec intervals) to remove older IOs from the averages
// Removes old entries from metricsAverageQueue, updates metricsSampleMap accordingly, and notifies
//   WaitMetricsRequests through waitMetricsMap.
void StorageServerMetrics::poll() {
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

// This function can run on untrusted user data.  We must validate all divisions carefully.
KeyRef StorageServerMetrics::getSplitKey(int64_t remaining,
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

void StorageServerMetrics::splitMetrics(SplitMetricsRequest req) const {
	int minSplitBytes = req.minSplitBytes.present() ? req.minSplitBytes.get() : SERVER_KNOBS->MIN_SHARD_BYTES;
	try {
		SplitMetricsReply reply;
		KeyRef lastKey = req.keys.begin;
		StorageMetrics used = req.used;
		StorageMetrics estimated = req.estimated;
		StorageMetrics remaining = getMetrics(req.keys) + used;

		//TraceEvent("SplitMetrics").detail("Begin", req.keys.begin).detail("End", req.keys.end).detail("Remaining", remaining.bytes).detail("Used", used.bytes).detail("MinSplitBytes", minSplitBytes);

		while (true) {
			if (remaining.bytes < 2 * minSplitBytes)
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
			if (used.bytes < minSplitBytes)
				key = std::max(
				    key, byteSample.splitEstimate(KeyRangeRef(lastKey, req.keys.end), minSplitBytes - used.bytes));
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

void StorageServerMetrics::getStorageMetrics(GetStorageMetricsRequest req,
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

// Given a read hot shard, this function will divide the shard into chunks and find those chunks whose
// readBytes/sizeBytes exceeds the `readDensityRatio`. Please make sure to run unit tests
// `StorageMetricsSampleTests.txt` after change made.
std::vector<ReadHotRangeWithMetrics> StorageServerMetrics::getReadHotRanges(
    KeyRangeRef shard,
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
			                      bytesReadSample.getEstimate(shard) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
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
			toReturn.emplace_back(range,
			                      (double)bytesReadSample.getEstimate(range) /
			                          std::max(baseChunkSize, byteSample.getEstimate(range)),
			                      bytesReadSample.getEstimate(range) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
		}
		beginKey = *endKey;
		endKey =
		    byteSample.sample.index(byteSample.sample.sumTo(byteSample.sample.lower_bound(beginKey)) + baseChunkSize);
	}
	return toReturn;
}

void StorageServerMetrics::getReadHotRanges(ReadHotSubRangeRequest req) const {
	ReadHotSubRangeReply reply;
	auto _ranges = getReadHotRanges(req.keys,
	                                SERVER_KNOBS->SHARD_MAX_READ_DENSITY_RATIO,
	                                SERVER_KNOBS->READ_HOT_SUB_RANGE_CHUNK_SIZE,
	                                SERVER_KNOBS->SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS);
	reply.readHotRanges = VectorRef(_ranges.data(), _ranges.size());
	req.reply.send(reply);
}

void StorageServerMetrics::getSplitPoints(SplitRangeRequest req, Optional<Key> prefix) const {
	SplitRangeReply reply;
	KeyRangeRef range = req.keys;
	if (prefix.present()) {
		range = range.withPrefix(prefix.get(), req.arena);
	}
	std::vector<KeyRef> points = getSplitPoints(range, req.chunkSize, prefix);

	reply.splitPoints.append_deep(reply.splitPoints.arena(), points.data(), points.size());
	req.reply.send(reply);
}

std::vector<KeyRef> StorageServerMetrics::getSplitPoints(KeyRangeRef range,
                                                         int64_t chunkSize,
                                                         Optional<Key> prefixToRemove) const {
	std::vector<KeyRef> toReturn;
	KeyRef beginKey = range.begin;
	IndexedSet<Key, int64_t>::const_iterator endKey =
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
		endKey = byteSample.sample.index(byteSample.sample.sumTo(byteSample.sample.lower_bound(beginKey)) + chunkSize);
	}
	return toReturn;
}

void StorageServerMetrics::collapse(KeyRangeMap<int>& map, KeyRef const& key) {
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

void StorageServerMetrics::add(KeyRangeMap<int>& map, KeyRangeRef const& keys, int delta) {
	auto rs = map.modify(keys);
	for (auto r = rs.begin(); r != rs.end(); ++r)
		r->value() += delta;
	collapse(map, keys.begin);
	collapse(map, keys.end);
}

// Returns the sampled metric value (possibly 0, possibly increased by the sampling factor)
int64_t TransientStorageMetricSample::addAndExpire(KeyRef key, int64_t metric, double expiration) {
	int64_t x = add(key, metric);
	if (x)
		queue.emplace_back(expiration, std::make_pair(*sample.find(key), -x));
	return x;
}

// FIXME: both versions of erase are broken, because they do not remove items in the queue with will subtract a
// metric from the value sometime in the future
int64_t TransientStorageMetricSample::erase(KeyRef key) {
	auto it = sample.find(key);
	if (it == sample.end())
		return 0;
	int64_t x = sample.getMetric(it);
	sample.erase(it);
	return x;
}

void TransientStorageMetricSample::erase(KeyRangeRef keys) {
	sample.erase(keys.begin, keys.end);
}

bool TransientStorageMetricSample::roll(KeyRef key, int64_t metric) const {
	return deterministicRandom()->random01() < (double)metric / metricUnitsPerSample; //< SOMEDAY: Better randomInt64?
}

void TransientStorageMetricSample::poll(KeyRangeMap<std::vector<PromiseStream<StorageMetrics>>>& waitMap,
                                        StorageMetrics m) {
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
			CODE_PROBE(true, "TransientStorageMetricSample poll update");
			v[i].send(deltaM);
		}

		queue.pop_front();
	}
}

void TransientStorageMetricSample::poll() {
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

int64_t TransientStorageMetricSample::add(KeyRef key, int64_t metric) {
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

TEST_CASE("/fdbserver/StorageMetricSample/simple") {
	StorageMetricSample s(1000);
	s.sample.insert("Apple"_sr, 1000);
	s.sample.insert("Banana"_sr, 2000);
	s.sample.insert("Cat"_sr, 1000);
	s.sample.insert("Cathode"_sr, 1000);
	s.sample.insert("Dog"_sr, 1000);

	ASSERT(s.getEstimate(KeyRangeRef("A"_sr, "D"_sr)) == 5000);
	ASSERT(s.getEstimate(KeyRangeRef("A"_sr, "E"_sr)) == 6000);
	ASSERT(s.getEstimate(KeyRangeRef("B"_sr, "C"_sr)) == 2000);

	// ASSERT(s.splitEstimate(KeyRangeRef("A"_sr, "D"_sr), 3500) ==
	// "Cat"_sr);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/simple") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert("A"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 800 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 100 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 2000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 1 && t[0] == "Bah"_sr);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/multipleReturnedPoints") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert("A"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 800 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 100 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 600 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 3 && t[0] == "Absolute"_sr && t[1] == "Apple"_sr && t[2] == "Bah"_sr);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/noneSplitable") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert("A"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 800 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 100 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 10000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 0);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/chunkTooLarge") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert("A"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 10 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 10 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 30 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 1000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 0);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/simple") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Banana"_sr, 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cat"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cathode"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Dog"_sr, 1000 * sampleUnit);

	ssm.byteSample.sample.insert("A"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 100 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef("A"_sr, "C"_sr), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 1 && (*t.begin()).keys.begin == "Bah"_sr && (*t.begin()).keys.end == "Bob"_sr);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/moreThanOneRange") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Banana"_sr, 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cat"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cathode"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Dog"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Final"_sr, 2000 * sampleUnit);

	ssm.byteSample.sample.insert("A"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 100 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 300 * sampleUnit);
	ssm.byteSample.sample.insert("Dah"_sr, 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef("A"_sr, "D"_sr), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == "Bah"_sr && (*t.begin()).keys.end == "Bob"_sr);
	ASSERT(t.at(1).keys.begin == "Cat"_sr && t.at(1).keys.end == "Dah"_sr);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/consecutiveRanges") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Banana"_sr, 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Bucket"_sr, 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cat"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cathode"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Dog"_sr, 5000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Final"_sr, 2000 * sampleUnit);

	ssm.byteSample.sample.insert("A"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Absolute"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.byteSample.sample.insert("Bah"_sr, 20 * sampleUnit);
	ssm.byteSample.sample.insert("Banana"_sr, 80 * sampleUnit);
	ssm.byteSample.sample.insert("Bob"_sr, 200 * sampleUnit);
	ssm.byteSample.sample.insert("But"_sr, 100 * sampleUnit);
	ssm.byteSample.sample.insert("Cat"_sr, 300 * sampleUnit);
	ssm.byteSample.sample.insert("Dah"_sr, 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef("A"_sr, "D"_sr), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == "Bah"_sr && (*t.begin()).keys.end == "But"_sr);
	ASSERT(t.at(1).keys.begin == "Cat"_sr && t.at(1).keys.end == "Dah"_sr);

	return Void();
}
