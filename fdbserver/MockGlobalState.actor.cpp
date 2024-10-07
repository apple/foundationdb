/*
 * MockGlobalState.actor.cpp
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

#include "fdbserver/MockGlobalState.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbclient/FDBTypes.h"
#include "flow/actorcompiler.h"

class MockGlobalStateImpl {
public:
	ACTOR static Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(MockGlobalState* mgs,
	                                                                                 KeyRange keys,
	                                                                                 StorageMetrics min,
	                                                                                 StorageMetrics max,
	                                                                                 StorageMetrics permittedError,
	                                                                                 int shardLimit,
	                                                                                 int expectedShardCount) {
		state TenantInfo tenantInfo;
		state Version version = 0;
		loop {
			auto locations = mgs->getKeyRangeLocations(tenantInfo,
			                                           keys,
			                                           shardLimit,
			                                           Reverse::False,
			                                           SpanContext(),
			                                           Optional<UID>(),
			                                           UseProvisionalProxies::False,
			                                           version)
			                     .get();
			TraceEvent(SevDebug, "MGSWaitStorageMetrics")
			    .detail("Phase", "GetLocation")
			    .detail("KeyRange", keys.toString())
			    .detail("LocationsCount", locations.size())
			    .detail("ExpectedShardCount", expectedShardCount);

			// NOTE(xwang): in native API, there's code handling the non-equal situation, but in mock world it's
			// possible for split shards stay in the same location
			CODE_PROBE(expectedShardCount >= 0 && locations.size() != expectedShardCount,
			           "Some shard is in the same location.",
			           probe::decoration::rare);

			try {
				Optional<StorageMetrics> res = wait(
				    ::waitStorageMetricsWithLocation(tenantInfo, version, keys, locations, min, max, permittedError));

				TraceEvent(SevDebug, "MGSWaitStorageMetrics")
				    .detail("Phase", "GetStorageMetrics")
				    .detail("KeyRange", keys.toString())
				    .detail("Present", res.present());

				if (res.present()) {
					return std::make_pair(res, -1);
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "MGSWaitStorageMetricsHandleError").error(e);
				if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
					wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
				} else if (e.code() == error_code_future_version) {
					wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, TaskPriority::DataDistribution));
				} else {
					bool ok = e.code() == error_code_tenant_not_found;
					TraceEvent(ok ? SevInfo : SevError, "MGSWaitStorageMetricsError").error(e);
					throw;
				}
			}
			// Avoid busy spin
			wait(delay(0.1, TaskPriority::DataDistribution));
		}
	}

	// SOMEDAY: reuse the NativeAPI implementation
	ACTOR static Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(MockGlobalState* mgs,
	                                                                       KeyRange keys,
	                                                                       StorageMetrics limit,
	                                                                       StorageMetrics estimated,
	                                                                       Optional<int> minSplitBytes) {
		state TenantInfo tenantInfo;
		loop {
			state std::vector<KeyRangeLocationInfo> locations =
			    mgs->getKeyRangeLocations(tenantInfo,
			                              keys,
			                              CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT,
			                              Reverse::False,
			                              SpanContext(),
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              0)
			        .get();

			// Same solution to NativeAPI::splitStorageMetrics, wait some merge finished
			if (locations.size() == CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT) {
				wait(delay(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskPriority::DataDistribution));
			}

			Optional<Standalone<VectorRef<KeyRef>>> results =
			    wait(splitStorageMetricsWithLocations(locations, keys, limit, estimated, minSplitBytes));

			if (results.present()) {
				return results.get();
			}

			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
		}
	}
};

class MockStorageServerImpl {
public:
	ACTOR static Future<Void> waitMetricsTenantAware(MockStorageServer* self, WaitMetricsRequest req) {
		if (req.tenantInfo.hasTenant()) {
			// TODO(xwang) add support for tenant test, search for tenant entry
			Optional<TenantMapEntry> entry;
			Optional<Key> tenantPrefix = entry.map(&TenantMapEntry::prefix);
			if (tenantPrefix.present()) {
				UNREACHABLE();
				// req.keys = req.keys.withPrefix(tenantPrefix.get(), req.arena);
			}
		}

		if (!self->isReadable(req.keys)) {
			self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
		} else {
			wait(self->metrics.waitMetrics(req, delayJittered(SERVER_KNOBS->STORAGE_METRIC_TIMEOUT)));
		}
		return Void();
	}

	// Randomly generate keys and kv size between the fetch range, updating the byte sample.
	// Once the fetchKeys return, the shard status will become FETCHED.
	ACTOR static Future<Void> waitFetchKeysFinish(MockStorageServer* self, MockStorageServer::FetchKeysParams params) {
		state TraceInterval interval("MockFetchKeys");
		// between each chunk delay for random time, and finally set the fetchComplete signal.
		ASSERT(params.totalRangeBytes > 0);
		state int chunkCount = std::ceil(params.totalRangeBytes * 1.0 / SERVER_KNOBS->FETCH_BLOCK_BYTES);
		state int64_t currentTotal = 0;
		state Key lastKey = params.keys.begin;

		TraceEvent(SevDebug, interval.begin(), self->id)
		    .detail("Range", params.keys)
		    .detail("ChunkCount", chunkCount)
		    .detail("TotalBytes", params.totalRangeBytes);

		state int i = 0;
		for (; i < chunkCount && currentTotal < params.totalRangeBytes; ++i) {
			wait(delayJittered(0.1, TaskPriority::FetchKeys));

			int remainedBytes = (chunkCount == 1 ? params.totalRangeBytes : SERVER_KNOBS->FETCH_BLOCK_BYTES);

			while (remainedBytes >= lastKey.size()) {
				Key nextKey;
				// try 10 times
				for (int j = 0; j < 10; j++) {
					nextKey = randomKeyBetween(KeyRangeRef(lastKey, params.keys.end));
					if (nextKey < params.keys.end)
						break;
				}

				++self->counters.kvFetched;

				// NOTE: in this case, we accumulate the bytes on lastKey on purpose (shall we?)
				if (nextKey == params.keys.end) {
					auto bytes = params.totalRangeBytes - currentTotal;
					self->counters.bytesFetched += bytes;
					self->byteSampleApplySet(lastKey, bytes);
					self->usedDiskSpace += bytes;
					currentTotal = params.totalRangeBytes;
					TraceEvent(SevWarn, "MockFetchKeysInaccurateSample", self->id)
					    .detail("PairId", interval.pairID)
					    .detail("LastKey", lastKey)
					    .detail("Size", bytes);
					break; // break the most outside loop
				}

				int maxSize = std::min(remainedBytes, 130000) + 1;
				int randomSize = deterministicRandom()->randomInt(lastKey.size(), maxSize);
				self->counters.bytesFetched += randomSize;
				self->usedDiskSpace += randomSize;
				currentTotal += randomSize;

				self->byteSampleApplySet(lastKey, randomSize);
				remainedBytes -= randomSize;
				lastKey = nextKey;
				DisabledTraceEvent(SevDebug, "MockFetchKeys_SingleKey", self->id)
				    .detail("LastKey", lastKey)
				    .detail("RemainedBytes", remainedBytes);
			}
		}

		self->setShardStatus(params.keys, MockShardStatus::FETCHED);
		TraceEvent(SevDebug, interval.end(), self->id).log();
		return Void();
	}
};

bool MockStorageServer::allShardStatusEqual(const KeyRangeRef& range, MockShardStatus status) const {
	auto ranges = serverKeys.intersectingRanges(range);
	ASSERT(!ranges.empty()); // at least the range is allKeys

	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		if (it->cvalue().status != status)
			return false;
	}
	return true;
}

bool MockStorageServer::allShardStatusIn(const KeyRangeRef& range, const std::set<MockShardStatus>& status) const {
	auto ranges = serverKeys.intersectingRanges(range);
	ASSERT(!ranges.empty()); // at least the range is allKeys

	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		// fmt::print("allShardStatusIn: {}: {} \n", id.toString(), it->range().toString());
		if (!status.contains(it->cvalue().status))
			return false;
	}
	return true;
}

void MockStorageServer::setShardStatus(const KeyRangeRef& range, MockShardStatus status) {
	auto ranges = serverKeys.intersectingRanges(range);
	// ranges at least has allKeys
	ASSERT(!ranges.empty());

	DisabledTraceEvent(SevDebug, "SetShardStatus", ssi.id()).detail("Range", range);

	// change the shard boundary if the status will change
	if (ranges.begin().begin() < range.begin && ranges.begin().end() > range.end &&
	    ranges.begin()->cvalue().status != status) {
		CODE_PROBE(true, "Implicitly split single shard to 3 pieces");
		threeWayShardSplitting(ranges.begin().range(), range, ranges.begin().cvalue().shardSize);
	} else {
		if (ranges.begin().begin() < range.begin && ranges.begin()->cvalue().status != status) {
			CODE_PROBE(true, "Implicitly split begin range to 2 pieces");
			twoWayShardSplitting(ranges.begin().range(), range.begin, ranges.begin().cvalue().shardSize);
		}
		if (ranges.end().begin() > range.end) {
			auto lastRange = ranges.end();
			--lastRange;
			if (lastRange->cvalue().status != status) {
				CODE_PROBE(true, "Implicitly split end range to 2 pieces");
				twoWayShardSplitting(lastRange.range(), range.end, lastRange.cvalue().shardSize);
			}
		}
	}
	ranges = serverKeys.containedRanges(range);

	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		auto oldStatus = it->cvalue().status;
		if (isStatusTransitionValid(oldStatus, status)) {
			it->value().status = status;
		} else if ((oldStatus == MockShardStatus::COMPLETED || oldStatus == MockShardStatus::FETCHED) &&
		           (status == MockShardStatus::INFLIGHT || status == MockShardStatus::FETCHED)) {
			CODE_PROBE(true, "Shard already on server");
		} else {
			TraceEvent(SevError, "MockShardStatusTransitionError", id)
			    .detail("From", oldStatus)
			    .detail("To", status)
			    .detail("KeyBegin", range.begin)
			    .detail("KeyEnd", range.begin);
			ASSERT(false);
		}
	}
}

void MockStorageServer::coalesceCompletedRange(const KeyRangeRef& range) {
	auto ranges = serverKeys.intersectingRanges(range);
	// ranges at least has allKeys
	ASSERT(!ranges.empty());
	auto allRanges = serverKeys.ranges();
	auto left = ranges.begin(), right = ranges.end();
	while (true) {
		if (left->cvalue().status != MockShardStatus::COMPLETED) {
			ASSERT(left != ranges.begin());
			++left;
			break;
		}
		if (left == allRanges.begin())
			break;
		--left;
	}

	while (right != allRanges.end() && right->cvalue().status == MockShardStatus::COMPLETED) {
		++right;
	}

	int newSize = 0;
	for (auto it = left; it != right; ++it) {
		ASSERT(it->cvalue().status == MockShardStatus::COMPLETED);
		newSize += it->cvalue().shardSize;
		it->value().shardSize = 0;
	}
	auto beginKey = left.begin(), endKey = right.begin();
	serverKeys.coalesce(KeyRangeRef(beginKey, endKey));
	serverKeys[beginKey].shardSize = newSize;
}

// split the out range [a, d) based on the inner range's boundary [b, c). The result would be [a,b), [b,c), [c,d). The
// size of the new shards are randomly split from old size of [a, d)
void MockStorageServer::threeWayShardSplitting(const KeyRangeRef& outerRange,
                                               const KeyRangeRef& innerRange,
                                               uint64_t outerRangeSize) {
	ASSERT(outerRange.contains(innerRange));
	if (outerRange == innerRange) {
		return;
	}

	Key left = outerRange.begin;

	// assume the split are even
	int leftSize = outerRangeSize / 3;
	int rightSize = leftSize;
	int midSize = outerRangeSize - leftSize - rightSize;

	serverKeys.insert(innerRange, { serverKeys[left].status, (uint64_t)midSize });
	serverKeys[left].shardSize = leftSize;
	serverKeys[innerRange.end].shardSize = rightSize;
}

// split the range [a,c) with split point b. The result would be [a, b), [b, c). The
// size of the new shards are randomly split from old size of [a, c)
void MockStorageServer::twoWayShardSplitting(const KeyRangeRef& range, const KeyRef& splitPoint, uint64_t rangeSize) {
	if (splitPoint == range.begin || !range.contains(splitPoint)) {
		return;
	}
	Key left = range.begin;
	DisabledTraceEvent(SevDebug, "TwoWayShardSplitting")
	    .detail("Range", range)
	    .detail("SplitPoint", splitPoint)
	    .detail("RangeSize", rangeSize);
	// Assume equally split the old range
	int leftSize = rangeSize / 2, rightSize = rangeSize - leftSize;
	serverKeys.rawInsert(splitPoint, { serverKeys[left].status, (uint64_t)rightSize });
	serverKeys[left].shardSize = leftSize;
}

void MockStorageServer::removeShard(const KeyRangeRef& range) {
	auto rangeSize = sumRangeSize(range);
	usedDiskSpace -= rangeSize;
	serverKeys.insert(range, MockStorageServer::ShardInfo{ MockShardStatus::UNSET, 0 });
	serverKeys.coalesce(range);
	byteSampleApplyClear(range);
	metrics.notifyNotReadable(range);
}

uint64_t MockStorageServer::sumRangeSize(const KeyRangeRef& range) const {
	auto ranges = serverKeys.intersectingRanges(range);
	uint64_t totalSize = 0;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		totalSize += it->cvalue().shardSize;
	}
	return totalSize;
}

void MockStorageServer::addActor(Future<Void> future) {
	actors.add(future);
}

void MockStorageServer::getSplitPoints(const SplitRangeRequest& req) {}

Future<Void> MockStorageServer::waitMetricsTenantAware(const WaitMetricsRequest& req) {
	return MockStorageServerImpl::waitMetricsTenantAware(this, req);
}

void MockStorageServer::getStorageMetrics(const GetStorageMetricsRequest& req) {
	StorageBytes storageBytes(
	    totalDiskSpace - usedDiskSpace, totalDiskSpace, usedDiskSpace, totalDiskSpace - usedDiskSpace);
	metrics.getStorageMetrics(
	    req, storageBytes, counters.bytesInput.getRate(), 0, now(), 0, counters.bytesInput.getValue());
	// FIXME: MockStorageServer does not support bytesDurable yet
}

void MockStorageServer::getSplitMetrics(const SplitMetricsRequest& req) {
	this->metrics.splitMetrics(req);
}

void MockStorageServer::getHotRangeMetrics(const ReadHotSubRangeRequest& req) {
	this->metrics.getReadHotRanges(req);
}

int64_t MockStorageServer::getHotShardsMetrics(const KeyRange& range) {
	return 0;
}

Future<Void> MockStorageServer::run() {
	ssi.initEndpoints();
	ssi.startAcceptingRequests();
	IFailureMonitor::failureMonitor().setStatus(ssi.address(), FailureStatus(false));

	TraceEvent("MockStorageServerStart", ssi.id()).detail("Address", ssi.address());
	auto& recruited = ssi;
	DUMPTOKEN(recruited.getStorageMetrics);
	addActor(serveStorageMetricsRequests(this, ssi));
	addActor(counters.cc.traceCounters("MockStorageMetrics",
	                                   ssi.id(),
	                                   SERVER_KNOBS->STORAGE_LOGGING_DELAY,
	                                   std::string(),
	                                   [self = this](TraceEvent& te) {
		                                   te.detail("CpuUsage", self->calculateCpuUsage());
		                                   te.detail("DiskUsedBytes", self->usedDiskSpace);
		                                   te.detail("BytesStored", self->metrics.byteSample.getEstimate(allKeys));
	                                   }));
	return actors.getResult();
}

void MockStorageServer::set(KeyRef const& key, int64_t bytes, int64_t oldBytes) {
	++counters.mutations;
	++counters.setMutations;
	counters.mutationBytes += bytes;
	counters.bytesInput += mvccStorageBytes(bytes);

	notifyWriteMetrics(key, bytes);
	byteSampleApplySet(key, bytes);
	auto delta = bytes - oldBytes;
	usedDiskSpace += delta;
	serverKeys[key].shardSize += delta;
}

void MockStorageServer::clear(KeyRef const& key, int64_t bytes) {
	++counters.mutations;
	++counters.clearRangeMutations;
	counters.mutationBytes += key.size();
	counters.bytesInput += mvccStorageBytes(key.size());

	notifyWriteMetrics(key, bytes);
	KeyRange sr = singleKeyRange(key);
	byteSampleApplyClear(sr);
	usedDiskSpace -= bytes;
	serverKeys[key].shardSize -= bytes;
}

int64_t MockStorageServer::clearRange(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes) {
	++counters.mutations;
	++counters.clearRangeMutations;
	counters.mutationBytes += range.expectedSize();
	counters.bytesInput += mvccStorageBytes(range.expectedSize());

	notifyWriteMetrics(range.begin, range.begin.size() + range.end.size());
	byteSampleApplyClear(range);
	auto totalByteSize = estimateRangeTotalBytes(range, beginShardBytes, endShardBytes);
	usedDiskSpace -= totalByteSize;
	clearRangeTotalBytes(range, beginShardBytes, endShardBytes);
	return totalByteSize;
}

void MockStorageServer::get(KeyRef const& key, int64_t bytes) {
	++counters.finishedQueries;
	counters.bytesQueried += bytes;

	// If the read yields no value, randomly sample the empty read.
	int64_t bytesReadPerKSecond = std::max(bytes, SERVER_KNOBS->EMPTY_READ_PENALTY);
	metrics.notifyBytesReadPerKSecond(key, bytesReadPerKSecond);
}

int64_t MockStorageServer::getRange(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes) {
	++counters.finishedQueries;

	int64_t totalByteSize = estimateRangeTotalBytes(range, beginShardBytes, endShardBytes);
	counters.bytesQueried += totalByteSize;
	// For performance concerns, the cost of a range read is billed to the start key and end key of the
	// range.
	if (totalByteSize > 0) {
		int64_t bytesReadPerKSecond = std::max(totalByteSize, SERVER_KNOBS->EMPTY_READ_PENALTY) / 2;
		metrics.notifyBytesReadPerKSecond(range.begin, bytesReadPerKSecond);
		metrics.notifyBytesReadPerKSecond(range.end, bytesReadPerKSecond);
	}
	return totalByteSize;
}

int64_t MockStorageServer::estimateRangeTotalBytes(KeyRangeRef const& range,
                                                   int64_t beginShardBytes,
                                                   int64_t endShardBytes) {
	int64_t totalByteSize = 0;
	auto ranges = serverKeys.intersectingRanges(range);

	// use the beginShardBytes as partial size
	if (ranges.begin().begin() < range.begin) {
		ranges.pop_front();
		totalByteSize += beginShardBytes;
	}
	// use the endShardBytes as partial size
	if (ranges.end().begin() < range.end) {
		totalByteSize += endShardBytes;
	}
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		totalByteSize += it->cvalue().shardSize;
	}
	return totalByteSize;
}

void MockStorageServer::clearRangeTotalBytes(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes) {
	auto ranges = serverKeys.intersectingRanges(range);

	// use the beginShardBytes as partial size
	if (ranges.begin().begin() < range.begin) {
		auto delta = std::min(ranges.begin().value().shardSize, (uint64_t)beginShardBytes);
		ranges.begin().value().shardSize -= delta;
		ranges.pop_front();
	}
	// use the endShardBytes as partial size
	if (ranges.end().begin() < range.end) {
		auto delta = std::min(ranges.end().value().shardSize, (uint64_t)endShardBytes);
		ranges.end().value().shardSize -= delta;
	}
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		it->value().shardSize = 0;
	}
}

void MockStorageServer::notifyWriteMetrics(KeyRef const& key, int64_t size) {
	// update write bandwidth and iops as mock the cost of writing a mutation
	StorageMetrics s;
	// FIXME: remove the / 2 and double the related knobs.
	s.bytesWrittenPerKSecond = mvccStorageBytes(size) / 2;
	s.iosPerKSecond = 1;
	metrics.notify(key, s);
}

void MockStorageServer::signalFetchKeys(const KeyRangeRef& range, int64_t rangeTotalBytes) {
	if (!allShardStatusEqual(range, MockShardStatus::COMPLETED)) {
		actors.add(MockStorageServerImpl::waitFetchKeysFinish(this, { range, rangeTotalBytes }));
	}
}

HealthMetrics::StorageStats MockStorageServer::getStorageStats() const {
	HealthMetrics::StorageStats res;
	res.diskUsage = usedDiskSpace * 100.0 / totalDiskSpace;
	res.cpuUsage = calculateCpuUsage();
	return res;
}

void MockStorageServer::byteSampleApplySet(KeyRef const& key, int64_t kvSize) {
	// Update byteSample in memory and notify waiting metrics
	ByteSampleInfo sampleInfo = isKeyValueInSample(key, kvSize);
	auto& byteSample = metrics.byteSample.sample;

	int64_t delta = 0;
	auto old = byteSample.find(key);
	if (old != byteSample.end())
		delta = -byteSample.getMetric(old);

	if (sampleInfo.inSample) {
		delta += sampleInfo.sampledSize;
		byteSample.insert(key, sampleInfo.sampledSize);
	} else if (old != byteSample.end()) {
		byteSample.erase(old);
	}

	if (delta)
		metrics.notifyBytes(key, delta);
}

void MockStorageServer::byteSampleApplyClear(KeyRangeRef const& range) {
	// Update byteSample and notify waiting metrics

	auto& byteSample = metrics.byteSample.sample;
	bool any = false;

	if (range.begin < allKeys.end) {
		// NotifyBytes should not be called for keys past allKeys.end
		KeyRangeRef searchRange = KeyRangeRef(range.begin, std::min(range.end, allKeys.end));

		auto r = metrics.waitMetricsMap.intersectingRanges(searchRange);
		for (auto shard = r.begin(); shard != r.end(); ++shard) {
			KeyRangeRef intersectingRange = shard.range() & range;
			int64_t bytes = byteSample.sumRange(intersectingRange.begin, intersectingRange.end);
			metrics.notifyBytes(shard, -bytes);
			any = any || bytes > 0;
		}
	}

	if (range.end > allKeys.end && byteSample.sumRange(std::max(allKeys.end, range.begin), range.end) > 0)
		any = true;

	if (any) {
		byteSample.eraseAsync(range.begin, range.end);
	}
}

double MockStorageServer::calculateCpuUsage() const {
	double res = counters.mutations.getRate() * write_op_cpu_multiplier +
	             counters.finishedQueries.getRate() * read_op_cpu_multiplier +
	             counters.mutationBytes.getRate() * write_byte_cpu_multiplier +
	             counters.bytesQueried.getRate() * read_byte_cpu_multiplier;
	return std::min(100.0, res);
}

std::shared_ptr<MockGlobalState>& MockGlobalState::g_mockState() {
	static std::shared_ptr<MockGlobalState> res(new MockGlobalState);
	return res;
}

void MockGlobalState::initializeClusterLayout(const BasicSimulationConfig& conf) {
	fmt::print("MGS Cluster Layout: {} dc, {} machines, {} processes per machine.\n",
	           conf.datacenters,
	           conf.machine_count,
	           conf.processes_per_machine);

	int mod = conf.machine_count % conf.datacenters;
	for (int i = 0; i < conf.datacenters; ++i) {
		Standalone<StringRef> dcId(StringRef(fmt::format("data_hall_{}", i)));
		clusterLayout.emplace_back(new mock::TopologyObject(mock::TopologyObject::DATA_HALL, dcId));

		auto& dc = clusterLayout.back();
		int machineCount = conf.machine_count / conf.datacenters + int(i < mod);
		for (int j = 0; j < machineCount; ++j) {
			Standalone<StringRef> mcId(StringRef(fmt::format("machine_{}_{}", i, j)));
			dc->children.emplace_back(new mock::TopologyObject(mock::TopologyObject::MACHINE, mcId, dc));

			auto& machine = dc->children.back();
			for (int k = 0; k < conf.processes_per_machine; ++k) {
				Standalone<StringRef> pid(StringRef(fmt::format("process_{}_{}_{}", i, j, k)));
				LocalityData localityData(pid, mcId, mcId, dcId);
				processes.emplace_back(new mock::Process(localityData, pid, machine));
				machine->children.emplace_back(processes.back());

				if (seedProcesses.size() < conf.db.storageTeamSize && j == 0 && k == 0) {
					seedProcesses.emplace_back(processes.back());
					fmt::print("(seed) ");
				}
				fmt::print("Mock Process: {}\n", processes.back()->locality.toString());
			}
		}
	}
}

void MockGlobalState::initializeAsEmptyDatabaseMGS(const DatabaseConfiguration& conf, uint64_t defaultDiskSpace) {
	ASSERT(conf.storageTeamSize > 0);
	ASSERT(!seedProcesses.empty());
	configuration = conf;
	std::vector<UID> serverIds;
	fmt::print("Initial Team Size: {}, initial server Ids: ", conf.storageTeamSize);
	for (int i = 1; i <= conf.storageTeamSize; ++i) {
		UID id = indexToUID(i);
		serverIds.push_back(id);

		// select seed Storage Server
		StorageServerInterface ssi(id);
		auto& process = seedProcesses[(i - 1) % seedProcesses.size()];
		ssi.locality = process->locality;
		process->ssInterfaces.push_back(ssi);
		fmt::print("{}, ", id.toString());

		allServers[id] = makeReference<MockStorageServer>(ssi, defaultDiskSpace);
		allServers[id]->serverKeys.insert(allKeys, { MockShardStatus::COMPLETED, 0 });
	}
	fmt::print("\n");
	shardMapping->assignRangeToTeams(allKeys, { Team(serverIds, true) });
}

void MockGlobalState::addStorageServer(StorageServerInterface server, uint64_t diskSpace) {
	allServers[server.id()] = makeReference<MockStorageServer>(server, diskSpace);
}

void MockGlobalState::addStoragePerProcess(uint64_t defaultDiskSpace) {
	for (auto p : processes) {
		if (p->ssInterfaces.empty()) {
			p->ssInterfaces.emplace_back(deterministicRandom()->randomUniqueID());
			p->ssInterfaces.back().locality = p->locality;
			addStorageServer(p->ssInterfaces.back(), defaultDiskSpace);
		}
	}
}

bool MockGlobalState::serverIsSourceForShard(const UID& serverId, KeyRangeRef shard, bool inFlightShard) {
	if (!allServers.contains(serverId))
		return false;

	// check serverKeys
	auto& mss = allServers.at(serverId);
	if (!mss->allShardStatusEqual(shard, MockShardStatus::COMPLETED)) {
		return false;
	}

	// check keyServers
	auto teams = shardMapping->getTeamsForFirstShard(shard);
	if (inFlightShard) {
		return std::any_of(teams.second.begin(), teams.second.end(), [&serverId](const Team& team) {
			return team.hasServer(serverId);
		});
	}
	return std::any_of(
	    teams.first.begin(), teams.first.end(), [&serverId](const Team& team) { return team.hasServer(serverId); });
}

bool MockGlobalState::serverIsDestForShard(const UID& serverId, KeyRangeRef shard) {
	TraceEvent(SevDebug, "ServerIsDestForShard")
	    .detail("ServerId", serverId)
	    .detail("Keys", shard)
	    .detail("Contains", allServers.contains(serverId));

	if (!allServers.contains(serverId))
		return false;

	// check serverKeys
	auto& mss = allServers.at(serverId);
	if (!mss->allShardStatusIn(shard,
	                           { MockShardStatus::INFLIGHT, MockShardStatus::COMPLETED, MockShardStatus::FETCHED })) {
		return false;
	}

	// check keyServers
	auto teams = shardMapping->getTeamsForFirstShard(shard);
	return !teams.second.empty() && std::any_of(teams.first.begin(), teams.first.end(), [&serverId](const Team& team) {
		return team.hasServer(serverId);
	});
}

bool MockGlobalState::allShardsRemovedFromServer(const UID& serverId) {
	return allServers.contains(serverId) && shardMapping->getNumberOfShards(serverId) == 0;
}

Future<std::pair<Optional<StorageMetrics>, int>> MockGlobalState::waitStorageMetrics(
    const KeyRange& keys,
    const StorageMetrics& min,
    const StorageMetrics& max,
    const StorageMetrics& permittedError,
    int shardLimit,
    int expectedShardCount) {
	return MockGlobalStateImpl::waitStorageMetrics(
	    this, keys, min, max, permittedError, shardLimit, expectedShardCount);
}

Reference<LocationInfo> buildLocationInfo(const std::vector<StorageServerInterface>& interfaces) {
	// construct the location info with the servers
	std::vector<Reference<ReferencedInterface<StorageServerInterface>>> serverRefs;
	serverRefs.reserve(interfaces.size());
	for (const auto& interf : interfaces) {
		serverRefs.push_back(makeReference<ReferencedInterface<StorageServerInterface>>(interf));
	}

	return makeReference<LocationInfo>(serverRefs);
}

Future<KeyRangeLocationInfo> MockGlobalState::getKeyLocation(TenantInfo tenant,
                                                             Key key,
                                                             SpanContext spanContext,
                                                             Optional<UID> debugID,
                                                             UseProvisionalProxies useProvisionalProxies,
                                                             Reverse isBackward,
                                                             Version version) {
	if (isBackward) {
		// DD never ask for backward range.
		UNREACHABLE();
	}
	ASSERT(key < allKeys.end);

	GetKeyServerLocationsReply rep;
	KeyRange single = singleKeyRange(key);
	auto teamPair = shardMapping->getTeamsForFirstShard(single);
	auto& srcTeam = teamPair.second.empty() ? teamPair.first : teamPair.second;
	ASSERT_EQ(srcTeam.size(), 1);
	rep.results.emplace_back(single, extractStorageServerInterfaces(srcTeam.front().servers));

	return KeyRangeLocationInfo(KeyRange(toPrefixRelativeRange(rep.results[0].first, tenant.prefix), rep.arena),
	                            buildLocationInfo(rep.results[0].second));
}

Future<std::vector<KeyRangeLocationInfo>> MockGlobalState::getKeyRangeLocations(
    TenantInfo tenant,
    KeyRange keys,
    int limit,
    Reverse reverse,
    SpanContext spanContext,
    Optional<UID> debugID,
    UseProvisionalProxies useProvisionalProxies,
    Version version) {

	if (reverse) {
		// DD never ask for backward range.
		ASSERT(false);
	}
	ASSERT(keys.begin < keys.end);

	GetKeyServerLocationsReply rep;
	auto ranges = shardMapping->intersectingRanges(keys);
	auto it = ranges.begin();
	for (int count = 0; it != ranges.end() && count < limit; ++it, ++count) {
		auto teamPair = shardMapping->getTeamsFor(it->begin());
		auto& srcTeam = teamPair.second.empty() ? teamPair.first : teamPair.second;
		ASSERT_EQ(srcTeam.size(), 1);
		rep.results.emplace_back(it->range(), extractStorageServerInterfaces(srcTeam.front().servers));
	}
	CODE_PROBE(it != ranges.end(), "getKeyRangeLocations is limited");

	std::vector<KeyRangeLocationInfo> results;
	for (int shard = 0; shard < rep.results.size(); shard++) {
		results.emplace_back((toPrefixRelativeRange(rep.results[shard].first, tenant.prefix) & keys),
		                     buildLocationInfo(rep.results[shard].second));
	}
	return results;
}

std::vector<StorageServerInterface> MockGlobalState::extractStorageServerInterfaces(const std::vector<UID>& ids) const {
	std::vector<StorageServerInterface> interfaces;
	for (auto& id : ids) {
		interfaces.emplace_back(allServers.at(id)->ssi);
	}
	return interfaces;
}

Future<Standalone<VectorRef<KeyRef>>> MockGlobalState::splitStorageMetrics(const KeyRange& keys,
                                                                           const StorageMetrics& limit,
                                                                           const StorageMetrics& estimated,
                                                                           const Optional<int>& minSplitBytes) {
	return MockGlobalStateImpl::splitStorageMetrics(this, keys, limit, estimated, minSplitBytes);
}

std::vector<Future<Void>> MockGlobalState::runAllMockServers() {
	std::vector<Future<Void>> futures;
	futures.reserve(allServers.size());
	for (auto& [id, _] : allServers) {
		futures.emplace_back(runMockServer(id));
	}
	return futures;
}
Future<Void> MockGlobalState::runMockServer(const UID& id) {
	return allServers.at(id)->run();
}

int MockGlobalState::getRangeSize(KeyRangeRef const& range) {
	// FIXME: return realistic number
	return SERVER_KNOBS->MIN_SHARD_BYTES;
}

int64_t MockGlobalState::get(KeyRef const& key) {
	auto ids = shardMapping->getSourceServerIdsFor(key);
	int64_t randomBytes = 0;
	if (deterministicRandom()->random01() > emptyProb) {
		randomBytes = deterministicRandom()->randomInt64(minByteSize, maxByteSize + 1);
	}
	// randomly choose 1 server
	auto id = deterministicRandom()->randomChoice(ids);
	allServers.at(id)->get(key, randomBytes);
	return randomBytes;
}

int64_t MockGlobalState::getRange(KeyRangeRef const& range) {
	auto ranges = shardMapping->intersectingRanges(range);
	int64_t totalSize = 0;
	KeyRef begin, end;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		auto ids = shardMapping->getSourceServerIdsFor(it->begin());
		if (range.begin > it->begin()) {
			begin = range.begin;
		}
		if (range.end < it->end()) {
			end = range.end;
		}

		// randomly choose 1 server
		auto id = deterministicRandom()->randomChoice(ids);
		int64_t beginSize = deterministicRandom()->randomInt64(0, SERVER_KNOBS->MIN_SHARD_BYTES),
		        endSize = deterministicRandom()->randomInt64(0, SERVER_KNOBS->MIN_SHARD_BYTES);
		totalSize += allServers.at(id)->getRange(KeyRangeRef(begin, end), beginSize, endSize);
	}
	return totalSize;
}

int64_t MockGlobalState::set(KeyRef const& key, int valueSize, bool insert) {
	auto ids = shardMapping->getSourceServerIdsFor(key);
	int64_t oldKvBytes = 0;
	insert |= (deterministicRandom()->random01() < emptyProb);

	if (!insert) {
		oldKvBytes = key.size() + deterministicRandom()->randomInt64(minByteSize, maxByteSize + 1);
	}

	for (auto& id : ids) {
		allServers.at(id)->set(key, valueSize + key.size(), oldKvBytes);
	}
	return oldKvBytes;
}

int64_t MockGlobalState::clear(KeyRef const& key) {
	auto ids = shardMapping->getSourceServerIdsFor(key);
	int64_t randomBytes = 0;
	if (deterministicRandom()->random01() > emptyProb) {
		randomBytes = deterministicRandom()->randomInt64(minByteSize, maxByteSize + 1) + key.size();
	}

	for (auto& id : ids) {
		allServers.at(id)->clear(key, randomBytes);
	}
	return randomBytes;
}

int64_t MockGlobalState::clearRange(KeyRangeRef const& range) {
	auto ranges = shardMapping->intersectingRanges(range);
	int64_t totalSize = 0;
	KeyRef begin, end;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		auto ids = shardMapping->getSourceServerIdsFor(it->begin());
		if (range.begin > it->begin()) {
			begin = range.begin;
		}
		if (range.end < it->end()) {
			end = range.end;
		}

		int64_t beginSize = deterministicRandom()->randomInt64(0, SERVER_KNOBS->MIN_SHARD_BYTES),
		        endSize = deterministicRandom()->randomInt64(0, SERVER_KNOBS->MIN_SHARD_BYTES);
		int64_t lastSize = -1;
		for (auto& id : ids) {
			int64_t size = allServers.at(id)->clearRange(KeyRangeRef(begin, end), beginSize, endSize);
			ASSERT(lastSize == size || lastSize == -1); // every server should return the same result
		}
		totalSize += lastSize;
	}
	return totalSize;
}

TEST_CASE("/MockGlobalState/initializeAsEmptyDatabaseMGS/SimpleThree") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 3;
	testConfig.logAntiQuorum = 0;

	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);
	TraceEvent("UnitTestDBConfig").detail("Config", dbConfig.db.toString());
	std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);

	for (int i = 1; i <= dbConfig.db.storageTeamSize; ++i) {
		auto id = MockGlobalState::indexToUID(i);
		std::cout << "Check server " << i << "\n";
		ASSERT(mgs->serverIsSourceForShard(id, allKeys));
		ASSERT(mgs->allServers.at(id)->sumRangeSize(allKeys) == 0);
	}

	return Void();
}

struct MockGlobalStateTester {

	// expectation [r0.begin, r0.end) => [r0.begin, x1), [x1, x2), [x2, r0.end)
	void testThreeWaySplitFirstRange(MockStorageServer& mss) {
		auto it = mss.serverKeys.ranges().begin();
		uint64_t oldSize =
		    deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, std::numeric_limits<int>::max());
		MockShardStatus oldStatus = it.cvalue().status;
		it->value().shardSize = oldSize;
		KeyRangeRef outerRange = it->range();
		Key x1 = keyAfter(it->range().begin);
		Key x2 = keyAfter(x1);
		std::cout << "it->range.begin: " << it->range().begin.toHexString() << " size: " << oldSize << "\n";

		mss.threeWayShardSplitting(outerRange, KeyRangeRef(x1, x2), oldSize);
		auto ranges = mss.serverKeys.containedRanges(outerRange);
		ASSERT(ranges.begin().range() == KeyRangeRef(outerRange.begin, x1));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.begin().range() == KeyRangeRef(x1, x2));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.begin().range() == KeyRangeRef(x2, outerRange.end));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.empty());
	}

	// expectation [r0.begin, r0.end) => [r0.begin, x1), [x1, r0.end)
	void testTwoWaySplitFirstRange(MockStorageServer& mss) {
		auto it = mss.serverKeys.nthRange(0);
		MockShardStatus oldStatus = it.cvalue().status;
		uint64_t oldSize =
		    deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, std::numeric_limits<int>::max());
		it->value().shardSize = oldSize;
		KeyRangeRef outerRange = it->range();
		Key x1 = keyAfter(it->range().begin);
		std::cout << "it->range.begin: " << it->range().begin.toHexString() << " size: " << oldSize << "\n";

		mss.twoWayShardSplitting(it->range(), x1, oldSize);
		auto ranges = mss.serverKeys.containedRanges(outerRange);
		ASSERT(ranges.begin().range() == KeyRangeRef(outerRange.begin, x1));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.begin().range() == KeyRangeRef(x1, outerRange.end));
		ASSERT(ranges.begin().cvalue().status == oldStatus);
		ranges.pop_front();
		ASSERT(ranges.empty());
	}

	KeyRangeLocationInfo getKeyLocationInfo(KeyRef key, std::shared_ptr<MockGlobalState> mgs) {
		return mgs
		    ->getKeyLocation(
		        TenantInfo(), key, SpanContext(), Optional<UID>(), UseProvisionalProxies::False, Reverse::False, 0)
		    .get();
	}

	std::vector<KeyRangeLocationInfo> getKeyRangeLocations(KeyRangeRef keys,
	                                                       int limit,
	                                                       std::shared_ptr<MockGlobalState> mgs) {
		return mgs
		    ->getKeyRangeLocations(TenantInfo(),
		                           keys,
		                           limit,
		                           Reverse::False,
		                           SpanContext(),
		                           Optional<UID>(),
		                           UseProvisionalProxies::False,
		                           0)
		    .get();
	}
};

TEST_CASE("/MockGlobalState/MockStorageServer/SplittingFunctions") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);
	TraceEvent("UnitTestDBConfig").detail("Config", dbConfig.db.toString());
	std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);

	MockGlobalStateTester tester;
	auto& mss = mgs->allServers.at(MockGlobalState::indexToUID(1));
	std::cout << "Test 3-way splitting...\n";
	tester.testThreeWaySplitFirstRange(*mss);
	std::cout << "Test 2-way splitting...\n";
	mss->serverKeys.insert(allKeys, { MockShardStatus::COMPLETED, 0 }); // reset to empty
	tester.testTwoWaySplitFirstRange(*mss);

	return Void();
}

TEST_CASE("/MockGlobalState/MockStorageServer/SetShardStatus") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);
	TraceEvent("UnitTestDBConfig").detail("Config", dbConfig.db.toString());
	state std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);

	auto& mss = mgs->allServers.at(MockGlobalState::indexToUID(1));
	mss->serverKeys.insert(allKeys, { MockShardStatus::UNSET, 1400 }); // manually reset status

	// split to 3 shards [allKeys.begin, a, b, allKeys.end]
	KeyRange testRange(KeyRangeRef("a"_sr, "b"_sr));
	mss->setShardStatus(testRange, MockShardStatus::INFLIGHT);
	ASSERT(mss->allShardStatusEqual(testRange, MockShardStatus::INFLIGHT));
	ASSERT_EQ(mss->sumRangeSize(allKeys), 1400);
	ASSERT_EQ(mss->serverKeys.size(), 3);

	// [allKeys.begin, a, b, bc, allKeys.end]
	testRange = KeyRangeRef("ac"_sr, "bc"_sr);
	mss->setShardStatus(testRange, MockShardStatus::INFLIGHT);
	ASSERT(mss->allShardStatusEqual(testRange, MockShardStatus::INFLIGHT));
	ASSERT_EQ(mss->sumRangeSize(allKeys), 1400);
	ASSERT_EQ(mss->serverKeys.size(), 4);
	testRange = KeyRangeRef("ab"_sr, "bb"_sr);
	mss->setShardStatus(testRange, MockShardStatus::INFLIGHT);
	ASSERT_EQ(mss->serverKeys.size(), 4);

	testRange = KeyRangeRef("b"_sr, "bc"_sr);
	// [allKeys.begin, a, b, bc, allKeys.end]
	mss->setShardStatus(testRange, MockShardStatus::FETCHED);
	ASSERT(mss->allShardStatusEqual(testRange, MockShardStatus::FETCHED));
	mss->setShardStatus(testRange, MockShardStatus::COMPLETED);
	ASSERT(mss->allShardStatusEqual(testRange, MockShardStatus::COMPLETED));
	mss->setShardStatus(testRange, MockShardStatus::FETCHED);
	ASSERT(mss->allShardStatusEqual(testRange, MockShardStatus::COMPLETED));
	ASSERT_EQ(mss->sumRangeSize(allKeys), 1400);
	ASSERT_EQ(mss->serverKeys.size(), 4);

	testRange = KeyRangeRef("ac"_sr, allKeys.end);
	// [allKeys.begin, a, ac, b, bc, allKeys.end]
	mss->setShardStatus(testRange, MockShardStatus::FETCHED);
	ASSERT_EQ(mss->sumRangeSize(allKeys), 1400);
	ASSERT_EQ(mss->serverKeys.size(), 5);
	ASSERT(mss->allShardStatusEqual(KeyRangeRef("ac"_sr, "b"_sr), MockShardStatus::FETCHED));
	ASSERT(mss->allShardStatusEqual(KeyRangeRef("b"_sr, "bc"_sr), MockShardStatus::COMPLETED));
	ASSERT(mss->allShardStatusEqual(KeyRangeRef("bc"_sr, allKeys.end), MockShardStatus::FETCHED));

	mss->setShardStatus(allKeys, MockShardStatus::INFLIGHT);
	mss->setShardStatus(allKeys, MockShardStatus::FETCHED);
	mss->setShardStatus(allKeys, MockShardStatus::COMPLETED);
	mss->coalesceCompletedRange(KeyRangeRef("a"_sr, "b"_sr));
	ASSERT_EQ(mss->sumRangeSize(allKeys), 1400);
	ASSERT_EQ(mss->serverKeys.size(), 1);
	return Void();
}

namespace {
inline bool locationInfoEqualsToTeam(Reference<LocationInfo> loc, const std::vector<UID>& ids) {
	return loc->locations()->size() == ids.size() &&
	       std::all_of(ids.begin(), ids.end(), [loc](const UID& id) { return loc->locations()->hasInterface(id); });
}
}; // namespace
TEST_CASE("/MockGlobalState/MockStorageServer/GetKeyLocations") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);
	TraceEvent("UnitTestDBConfig").detail("Config", dbConfig.db.toString());
	state std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);
	// add one empty server
	mgs->addStorageServer(StorageServerInterface(mgs->indexToUID(mgs->allServers.size() + 1)));

	// define 3 ranges:
	// team 1 (UID 1,2,...,n-1):[begin, 1.0), [2.0, end)
	// team 2 (UID 2,3,...n-1, n): [1.0, 2.0)
	ShardsAffectedByTeamFailure::Team team1, team2;
	for (int i = 0; i < mgs->allServers.size() - 1; ++i) {
		UID id = mgs->indexToUID(i + 1);
		team1.servers.emplace_back(id);
		id = mgs->indexToUID(i + 2);
		team2.servers.emplace_back(id);
	}
	Key one = doubleToTestKey(1.0), two = doubleToTestKey(2.0);
	std::vector<KeyRangeRef> ranges{ KeyRangeRef(allKeys.begin, one),
		                             KeyRangeRef(one, two),
		                             KeyRangeRef(two, allKeys.end) };
	mgs->shardMapping->assignRangeToTeams(ranges[0], { team1 });
	mgs->shardMapping->assignRangeToTeams(ranges[1], { team2 });
	mgs->shardMapping->assignRangeToTeams(ranges[2], { team1 });

	// query key location
	MockGlobalStateTester tester;
	// -- team 1
	Key testKey = doubleToTestKey(0.5);
	auto locInfo = tester.getKeyLocationInfo(testKey, mgs);
	ASSERT(locationInfoEqualsToTeam(locInfo.locations, team1.servers));

	// -- team 2
	testKey = doubleToTestKey(1.3);
	locInfo = tester.getKeyLocationInfo(testKey, mgs);
	ASSERT(locationInfoEqualsToTeam(locInfo.locations, team2.servers));

	// query range location
	testKey = doubleToTestKey(3.0);
	// team 1,2,1
	auto locInfos = tester.getKeyRangeLocations(KeyRangeRef(allKeys.begin, testKey), 100, mgs);
	ASSERT(locInfos.size() == 3);
	ASSERT(locInfos[0].range == ranges[0]);
	ASSERT(locationInfoEqualsToTeam(locInfos[0].locations, team1.servers));
	ASSERT(locInfos[1].range == ranges[1]);
	ASSERT(locationInfoEqualsToTeam(locInfos[1].locations, team2.servers));
	ASSERT(locInfos[2].range == KeyRangeRef(ranges[2].begin, testKey));
	ASSERT(locationInfoEqualsToTeam(locInfos[2].locations, team1.servers));

	// team 1,2
	locInfos = tester.getKeyRangeLocations(KeyRangeRef(allKeys.begin, testKey), 2, mgs);
	ASSERT(locInfos.size() == 2);
	ASSERT(locInfos[0].range == ranges[0]);
	ASSERT(locationInfoEqualsToTeam(locInfos[0].locations, team1.servers));
	ASSERT(locInfos[1].range == ranges[1]);
	ASSERT(locationInfoEqualsToTeam(locInfos[1].locations, team2.servers));

	return Void();
}

TEST_CASE("/MockGlobalState/MockStorageServer/WaitStorageMetricsRequest") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;

	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);
	TraceEvent("UnitTestDBConfig").detail("Config", dbConfig.db.toString());
	state std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);

	std::for_each(mgs->allServers.begin(), mgs->allServers.end(), [](auto& server) {
		server.second->metrics.byteSample.sample.insert("something"_sr, 500000);
	});

	state Future<Void> allServerFutures = waitForAll(mgs->runAllMockServers());

	KeyRange testRange = allKeys;
	ShardSizeBounds bounds = ShardSizeBounds::shardSizeBoundsBeforeTrack();
	std::pair<Optional<StorageMetrics>, int> res =
	    wait(mgs->waitStorageMetrics(testRange, bounds.min, bounds.max, bounds.permittedError, 1, 1));
	// std::cout << "get result " << res.second << "\n";
	// std::cout << "get byte "<< res.first.get().bytes << "\n";
	ASSERT_EQ(res.second, -1); // the valid result always return -1, strange contraction though.
	ASSERT_EQ(res.first.get().bytes, 500000);
	return Void();
}

TEST_CASE("/MockGlobalState/MockStorageServer/DataOpsSet") {
	BasicTestConfig testConfig;
	testConfig.simpleConfig = true;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;

	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);
	TraceEvent("UnitTestDBConfig").detail("Config", dbConfig.db.toString());
	state std::shared_ptr<MockGlobalState> mgs = std::make_shared<MockGlobalState>();
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);

	state Future<Void> allServerFutures = waitForAll(mgs->runAllMockServers());

	// insert
	{
		mgs->set("a"_sr, 1 * SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE, true);
		mgs->set("b"_sr, 2 * SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE, true);
		mgs->set("c"_sr, 3 * SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE, true);
		for (auto& server : mgs->allServers) {
			ASSERT_EQ(server.second->usedDiskSpace, 3 + 6 * SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE);
			ASSERT_EQ(server.second->serverKeys[""_sr].shardSize, 3 + 6 * SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE);
		}
		ShardSizeBounds bounds = ShardSizeBounds::shardSizeBoundsBeforeTrack();
		std::pair<Optional<StorageMetrics>, int> res = wait(
		    mgs->waitStorageMetrics(KeyRangeRef("a"_sr, "bc"_sr), bounds.min, bounds.max, bounds.permittedError, 1, 1));

		int64_t testSize = 2 + 3 * SERVER_KNOBS->BYTES_WRITTEN_UNITS_PER_SAMPLE;
		// SOMEDAY: how to integrate with isKeyValueInSample() better?
		if (res.first.get().bytes > 0) {
			// If sampled
			ASSERT_EQ(res.first.get().bytes, testSize);
			ASSERT_GT(res.first.get().bytesWrittenPerKSecond, 0);
		}
	}
	return Void();
}
