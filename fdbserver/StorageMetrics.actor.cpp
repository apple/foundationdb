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
#include "fdbserver/StorageMetrics.actor.h"
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
	result.bytesWrittenPerKSecond =
	    bytesWriteSample.getEstimate(keys) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
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
		CODE_PROBE(metrics.bytesWrittenPerKSecond != 0, "ShardNotifyMetrics bytes");
		CODE_PROBE(metrics.iosPerKSecond != 0, "ShardNotifyMetrics ios");
		CODE_PROBE(metrics.bytesReadPerKSecond != 0, "ShardNotifyMetrics bytesRead", probe::decoration::rare);
	}

	double expire = now() + SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL;

	StorageMetrics notifyMetrics;

	if (metrics.bytesWrittenPerKSecond)
		notifyMetrics.bytesWrittenPerKSecond =
		    bytesWriteSample.addAndExpire(key, metrics.bytesWrittenPerKSecond, expire) *
		    SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	if (metrics.iosPerKSecond)
		notifyMetrics.iosPerKSecond = iopsSample.addAndExpire(key, metrics.iosPerKSecond, expire) *
		                              SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	if (metrics.bytesReadPerKSecond)
		notifyMetrics.bytesReadPerKSecond = bytesReadSample.addAndExpire(key, metrics.bytesReadPerKSecond, expire) *
		                                    SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	if (metrics.opsReadPerKSecond) {
		notifyMetrics.opsReadPerKSecond = opsReadSample.addAndExpire(key, metrics.opsReadPerKSecond, expire) *
		                                  SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	}

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
	int64_t opsReadPerKSecond =
	    opsReadSample.addAndExpire(key, 1, expire) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;

	if (bytesReadPerKSecond > 0 || opsReadPerKSecond > 0) {
		StorageMetrics notifyMetrics;
		notifyMetrics.bytesReadPerKSecond = bytesReadPerKSecond;
		notifyMetrics.opsReadPerKSecond = opsReadPerKSecond;
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
		m.bytesWrittenPerKSecond = SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		bytesWriteSample.poll(waitMetricsMap, m);
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
			bool hasUsed = used.bytes != 0 || used.bytesWrittenPerKSecond != 0 || used.iosPerKSecond != 0;
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
			key = getSplitKey(remaining.bytesWrittenPerKSecond,
			                  estimated.bytesWrittenPerKSecond,
			                  req.limits.bytesWrittenPerKSecond,
			                  used.bytesWrittenPerKSecond,
			                  req.limits.infinity,
			                  req.isLastShard,
			                  bytesWriteSample,
			                  SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS,
			                  lastKey,
			                  key,
			                  hasUsed);
			ASSERT(key != lastKey || hasUsed);
			if (key == req.keys.end)
				break;
			reply.splits.push_back_deep(reply.splits.arena(), key);
			if (reply.splits.size() > SERVER_KNOBS->SPLIT_METRICS_MAX_ROWS) {
				reply.more = true;
				break;
			}

			StorageMetrics diff = (getMetrics(KeyRangeRef(lastKey, key)) + used);
			remaining -= diff;
			estimated -= diff;

			used = StorageMetrics();
			lastKey = key;
		}

		reply.used = reply.more ? StorageMetrics() : getMetrics(KeyRangeRef(lastKey, req.keys.end)) + used;
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
	rep.available.bytesWrittenPerKSecond = 100e9;
	rep.available.bytesReadPerKSecond = 100e9;

	rep.capacity.bytes = sb.total;
	rep.capacity.iosPerKSecond = 10e6;
	rep.capacity.bytesWrittenPerKSecond = 100e9;
	rep.capacity.bytesReadPerKSecond = 100e9;

	rep.bytesInputRate = bytesInputRate;

	rep.versionLag = versionLag;
	rep.lastUpdate = lastUpdate;

	req.reply.send(rep);
}

// Equally split the metrics (specified by splitType) of parentRange into splitCount and return all the sampled metrics
// (bytes, readBytes and readOps) of each chunk
// NOTE: update unit test "equalDivide" after change
std::vector<ReadHotRangeWithMetrics> StorageServerMetrics::getReadHotRanges(KeyRangeRef parentRange,
                                                                            int splitCount,
                                                                            uint8_t splitType) const {
	const StorageMetricSample* sampler = nullptr;
	switch (splitType) {
	case ReadHotSubRangeRequest::SplitType::BYTES:
		sampler = &byteSample;
		break;
	case ReadHotSubRangeRequest::SplitType::READ_BYTES:
		sampler = &bytesReadSample;
		break;
	case ReadHotSubRangeRequest::SplitType::READ_OPS:
		sampler = &opsReadSample;
		break;
	default:
		ASSERT(false);
	}

	std::vector<ReadHotRangeWithMetrics> toReturn;
	if (sampler->sample.empty()) {
		return toReturn;
	}

	double total = sampler->getEstimate(parentRange);
	double splitChunk = total / splitCount;

	KeyRef beginKey = parentRange.begin;
	while (true) {
		auto beginIt = sampler->sample.lower_bound(beginKey);
		if (beginIt == sampler->sample.end()) {
			break;
		}
		auto endIt = sampler->sample.index(sampler->sample.sumTo(beginIt) + splitChunk - 1);
		// because index return x where sumTo(x+1) (that including sample at x) > metrics, we have to forward endIt here
		if (endIt != sampler->sample.end())
			++endIt;

		if (endIt == sampler->sample.end()) {
			KeyRangeRef lastRange(beginKey, parentRange.end);
			toReturn.emplace_back(
			    lastRange,
			    byteSample.getEstimate(lastRange),
			    (double)bytesReadSample.getEstimate(lastRange) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL,
			    (double)opsReadSample.getEstimate(lastRange) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
			break;
		}

		KeyRangeRef range(beginKey, *endIt);
		toReturn.emplace_back(
		    range,
		    byteSample.getEstimate(range),
		    (double)bytesReadSample.getEstimate(range) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL,
		    (double)opsReadSample.getEstimate(range) / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);

		beginKey = *endIt;
	}
	return toReturn;
}

// Given a read hot shard, this function will divide the shard into chunks and find those chunks whose
// readBytes/sizeBytes exceeds the `readDensityRatio`. Please make sure to run unit tests
// `StorageMetricsSampleTests.txt` after change made.
std::vector<ReadHotRangeWithMetrics> StorageServerMetrics::_getReadHotRanges(
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
	auto _ranges = getReadHotRanges(req.keys, req.splitCount, req.type);
	reply.readHotRanges = VectorRef(_ranges.data(), _ranges.size());
	req.reply.send(reply);
}

void StorageServerMetrics::getSplitPoints(SplitRangeRequest req, Optional<KeyRef> prefix) const {
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
                                                         Optional<KeyRef> prefixToRemove) const {
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

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 2000 * sampleUnit, {});

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

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 600 * sampleUnit, {});

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

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 10000 * sampleUnit, {});

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

	std::vector<KeyRef> t = ssm.getSplitPoints(KeyRangeRef("A"_sr, "C"_sr), 1000 * sampleUnit, {});

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
	    ssm._getReadHotRanges(KeyRangeRef("A"_sr, "C"_sr), 2.0, 200 * sampleUnit, 0);

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
	    ssm._getReadHotRanges(KeyRangeRef("A"_sr, "D"_sr), 2.0, 200 * sampleUnit, 0);

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
	    ssm._getReadHotRanges(KeyRangeRef("A"_sr, "D"_sr), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == "Bah"_sr && (*t.begin()).keys.end == "But"_sr);
	ASSERT(t.at(1).keys.begin == "Cat"_sr && t.at(1).keys.end == "Dah"_sr);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/equalDivide") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	// 14000 / 7 = 2000 each chunk
	// chunk 0
	ssm.bytesReadSample.sample.insert("Apple"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Banana"_sr, 2000 * sampleUnit);
	// chunk 1
	ssm.bytesReadSample.sample.insert("Bucket"_sr, 2000 * sampleUnit);
	// chunk 2
	ssm.bytesReadSample.sample.insert("Cat"_sr, 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert("Cathode"_sr, 1000 * sampleUnit);
	// chunk 3
	ssm.bytesReadSample.sample.insert("Dog"_sr, 5000 * sampleUnit);
	// chunk 4
	ssm.bytesReadSample.sample.insert("Final"_sr, 2000 * sampleUnit);

	// chunk 0
	ssm.byteSample.sample.insert("A"_sr, 20);
	ssm.byteSample.sample.insert("Absolute"_sr, 80);
	ssm.byteSample.sample.insert("Apple"_sr, 1000);
	ssm.byteSample.sample.insert("Bah"_sr, 20);
	ssm.byteSample.sample.insert("Banana"_sr, 80);
	ssm.byteSample.sample.insert("Bob"_sr, 200);
	// chunk 1
	ssm.byteSample.sample.insert("But"_sr, 100);
	// chunk 2
	ssm.byteSample.sample.insert("Cat"_sr, 300);
	ssm.byteSample.sample.insert("Dah"_sr, 300);

	// edge case: no overlap
	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef("Y"_sr, "Z"_sr), 7, ReadHotSubRangeRequest::SplitType::READ_BYTES);
	ASSERT_EQ(t.size(), 0);

	// divide all keys
	t = ssm.getReadHotRanges(KeyRangeRef(""_sr, "\xff"_sr), 7, ReadHotSubRangeRequest::SplitType::READ_BYTES);
	ASSERT_EQ(t.size(), 5);
	//	for(int i = 0; i < t.size(); ++ i) {
	//		fmt::print("{} {}\n", t[i].keys.begin.toString(), t[i].readBandwidthSec);
	//	}
	ASSERT_EQ((*t.begin()).keys.begin,
	          ""_sr); // Note since difference sampler is not aligned, so "A" is not the first key
	ASSERT_EQ((*t.begin()).keys.end, "Bucket"_sr);
	ASSERT_EQ(t[0].bytes, 1400);

	ASSERT_EQ(t.at(1).keys.begin, "Bucket"_sr);
	ASSERT_EQ(t.at(1).keys.end, "Cat"_sr);

	ASSERT_EQ(t.at(2).bytes, 600);
	ASSERT_EQ(t.at(3).readBandwidthSec, 5000 * sampleUnit / SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
	ASSERT_EQ(t.at(3).bytes, 0);
	return Void();
}
