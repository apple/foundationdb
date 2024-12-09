/*
 * DataDistributionTracker.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/TenantCache.h"
#include "fdbserver/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/CodeProbe.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "fdbserver/DDShardTracker.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// The used bandwidth of a shard. The higher the value is, the busier the shard is.
enum BandwidthStatus { BandwidthStatusLow, BandwidthStatusNormal, BandwidthStatusHigh };

enum ReadBandwidthStatus { ReadBandwidthStatusNormal, ReadBandwidthStatusHigh };

BandwidthStatus getBandwidthStatus(StorageMetrics const& metrics) {
	if (metrics.bytesWrittenPerKSecond > SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC)
		return BandwidthStatusHigh;
	else if (metrics.bytesWrittenPerKSecond < SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC)
		return BandwidthStatusLow;

	return BandwidthStatusNormal;
}

ReadBandwidthStatus getReadBandwidthStatus(StorageMetrics const& metrics) {
	if (metrics.bytesReadPerKSecond <= SERVER_KNOBS->SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS ||
	    metrics.bytesReadPerKSecond <= SERVER_KNOBS->SHARD_MAX_READ_DENSITY_RATIO * metrics.bytes *
	                                       SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS) {
		return ReadBandwidthStatusNormal;
	} else {
		return ReadBandwidthStatusHigh;
	}
}

ACTOR Future<Void> updateMaxShardSize(Reference<AsyncVar<int64_t>> dbSizeEstimate,
                                      Reference<AsyncVar<Optional<int64_t>>> maxShardSize) {
	state int64_t lastDbSize = 0;
	state int64_t granularity = g_network->isSimulated() ? SERVER_KNOBS->DD_SHARD_SIZE_GRANULARITY_SIM
	                                                     : SERVER_KNOBS->DD_SHARD_SIZE_GRANULARITY;
	loop {
		auto sizeDelta = std::abs(dbSizeEstimate->get() - lastDbSize);
		if (sizeDelta > granularity || !maxShardSize->get().present()) {
			auto v = getMaxShardSize(dbSizeEstimate->get());
			maxShardSize->set(v);
			lastDbSize = dbSizeEstimate->get();
		}
		wait(dbSizeEstimate->onChange());
	}
}

void restartShardTrackers(DataDistributionTracker* self,
                          KeyRangeRef keys,
                          Optional<ShardMetrics> startingMetrics = Optional<ShardMetrics>(),
                          bool whenDDInit = false);

// Gets the permitted size and IO bounds for a shard. A shard that starts at allKeys.begin
//  (i.e. '') will have a permitted size of 0, since the database can contain no data.
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

	// The first shard can have arbitrarily small size
	if (shard.begin == allKeys.begin) {
		bounds.min.bytes = 0;
	} else {
		bounds.min.bytes = maxShardSize / SERVER_KNOBS->SHARD_BYTES_RATIO;
	}

	bounds.min.bytesWrittenPerKSecond = 0;
	bounds.min.iosPerKSecond = 0;
	bounds.min.bytesReadPerKSecond = 0;
	bounds.min.opsReadPerKSecond = 0;

	// The permitted error is 1/3 of the general-case minimum bytes (even in the special case where this is the last
	// shard)
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

// Returns the shard size bounds as well as whether `keys` a read hot shard.
std::pair<ShardSizeBounds, bool> calculateShardSizeBounds(
    const KeyRange& keys,
    const Reference<AsyncVar<Optional<ShardMetrics>>>& shardMetrics,
    const BandwidthStatus& bandwidthStatus) {
	ShardSizeBounds bounds = ShardSizeBounds::shardSizeBoundsBeforeTrack();
	bool readHotShard = false;
	if (shardMetrics->get().present()) {
		auto bytes = shardMetrics->get().get().metrics.bytes;
		auto readBandwidthStatus = getReadBandwidthStatus(shardMetrics->get().get().metrics);

		// 1. bytes bound
		bounds.max.bytes = std::max(int64_t(bytes * 1.1), (int64_t)SERVER_KNOBS->MIN_SHARD_BYTES);
		bounds.min.bytes = std::min(int64_t(bytes * 0.9),
		                            std::max(int64_t(bytes - (SERVER_KNOBS->MIN_SHARD_BYTES * 0.1)), (int64_t)0));
		bounds.permittedError.bytes = bytes * 0.1;

		// 2. bytes written bound
		if (bandwidthStatus == BandwidthStatusNormal) { // Not high or low
			bounds.max.bytesWrittenPerKSecond = SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC;
			bounds.min.bytesWrittenPerKSecond = SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC;
			bounds.permittedError.bytesWrittenPerKSecond = bounds.min.bytesWrittenPerKSecond / 4;
		} else if (bandwidthStatus == BandwidthStatusHigh) { // > 10MB/sec for 100MB shard, proportionally lower
			                                                 // for smaller shard, > 200KB/sec no matter what
			bounds.max.bytesWrittenPerKSecond = bounds.max.infinity;
			bounds.min.bytesWrittenPerKSecond = SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC;
			bounds.permittedError.bytesWrittenPerKSecond = bounds.min.bytesWrittenPerKSecond / 4;
		} else if (bandwidthStatus == BandwidthStatusLow) { // < 10KB/sec
			bounds.max.bytesWrittenPerKSecond = SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC;
			bounds.min.bytesWrittenPerKSecond = 0;
			bounds.permittedError.bytesWrittenPerKSecond = bounds.max.bytesWrittenPerKSecond / 4;
		} else {
			ASSERT(false);
		}

		// 3. read bandwidth bound
		if (readBandwidthStatus == ReadBandwidthStatusNormal) {
			bounds.max.bytesReadPerKSecond =
			    std::max((int64_t)(SERVER_KNOBS->SHARD_MAX_READ_DENSITY_RATIO * bytes *
			                       SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS *
			                       (1.0 + SERVER_KNOBS->SHARD_MAX_BYTES_READ_PER_KSEC_JITTER)),
			             SERVER_KNOBS->SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS);
			bounds.min.bytesReadPerKSecond = 0;
			bounds.permittedError.bytesReadPerKSecond = bounds.min.bytesReadPerKSecond / 4;
		} else if (readBandwidthStatus == ReadBandwidthStatusHigh) {
			bounds.max.bytesReadPerKSecond = bounds.max.infinity;
			bounds.min.bytesReadPerKSecond = SERVER_KNOBS->SHARD_MAX_READ_DENSITY_RATIO * bytes *
			                                 SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS *
			                                 (1.0 - SERVER_KNOBS->SHARD_MAX_BYTES_READ_PER_KSEC_JITTER);
			bounds.permittedError.bytesReadPerKSecond = bounds.min.bytesReadPerKSecond / 4;

			readHotShard = true;
		} else {
			ASSERT(false);
		}

		// 4. read ops bound
		if (shardMetrics->get()->metrics.opsReadPerKSecond > SERVER_KNOBS->SHARD_MAX_READ_OPS_PER_KSEC) {
			readHotShard = true;
		}
		// update when the read ops changed drastically
		int64_t currentReadOps = shardMetrics->get()->metrics.opsReadPerKSecond;
		bounds.max.opsReadPerKSecond = currentReadOps + SERVER_KNOBS->SHARD_READ_OPS_CHANGE_THRESHOLD;
		bounds.min.opsReadPerKSecond =
		    std::max((int64_t)0, currentReadOps - SERVER_KNOBS->SHARD_READ_OPS_CHANGE_THRESHOLD);
		bounds.permittedError.opsReadPerKSecond = currentReadOps * 0.25;
	}
	return { bounds, readHotShard };
}

ACTOR Future<Void> shardUsableRegions(DataDistributionTracker::SafeAccessor self, KeyRange keys) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->DD_SHARD_USABLE_REGION_CHECK_RATE > 0);
	wait(yieldedFuture(self()->readyToStart.getFuture()));
	double expectedCompletionSeconds = self()->shards->size() * 1.0 / SERVER_KNOBS->DD_SHARD_USABLE_REGION_CHECK_RATE;
	double delayTime = deterministicRandom()->random01() * expectedCompletionSeconds;
	wait(delayJittered(delayTime));
	auto [newTeam, previousTeam] = self()->shardsAffectedByTeamFailure->getTeamsForFirstShard(keys);
	if (newTeam.size() < self()->usableRegions) {
		TraceEvent(SevWarn, "ShardUsableRegionMismatch", self()->distributorId)
		    .suppressFor(5.0)
		    .detail("NewTeamSize", newTeam.size())
		    .detail("PreviousTeamSize", previousTeam.size())
		    .detail("NewServers", describe(newTeam))
		    .detail("PreviousServers", describe(previousTeam))
		    .detail("UsableRegion", self()->usableRegions)
		    .detail("Shard", keys);
		RelocateShard rs(keys, DataMovementReason::POPULATE_REGION, RelocateReason::OTHER);
		self()->output.send(rs);
	}
	return Void();
}

ACTOR Future<Void> trackShardMetrics(DataDistributionTracker::SafeAccessor self,
                                     KeyRange keys,
                                     Reference<AsyncVar<Optional<ShardMetrics>>> shardMetrics,
                                     bool whenDDInit) {
	state BandwidthStatus bandwidthStatus =
	    shardMetrics->get().present() ? getBandwidthStatus(shardMetrics->get().get().metrics) : BandwidthStatusNormal;
	state double lastLowBandwidthStartTime =
	    shardMetrics->get().present() ? shardMetrics->get().get().lastLowBandwidthStartTime : now();
	state int shardCount = shardMetrics->get().present() ? shardMetrics->get().get().shardCount : 1;
	state bool initWithNewMetrics = whenDDInit;
	wait(delay(0, TaskPriority::DataDistribution));

	DisabledTraceEvent(SevDebug, "TrackShardMetricsStarting", self()->distributorId)
	    .detail("Keys", keys)
	    .detail("TrackedBytesInitiallyPresent", shardMetrics->get().present())
	    .detail("StartingMetrics", shardMetrics->get().present() ? shardMetrics->get().get().metrics.bytes : 0);

	try {
		loop {
			state ShardSizeBounds bounds;
			bool readHotShard;
			std::tie(bounds, readHotShard) = calculateShardSizeBounds(keys, shardMetrics, bandwidthStatus);

			if (readHotShard) {
				// TraceEvent("RHDTriggerReadHotLoggingForShard")
				//     .detail("ShardBegin", keys.begin.printable().c_str())
				//     .detail("ShardEnd", keys.end.printable().c_str());
				self()->readHotShard.send(keys);
			}

			loop {
				// metrics.second is the number of key-ranges (i.e., shards) in the 'keys' key-range
				std::pair<Optional<StorageMetrics>, int> metrics =
				    wait(self()->db->waitStorageMetrics(keys,
				                                        bounds.min,
				                                        bounds.max,
				                                        bounds.permittedError,
				                                        CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT,
				                                        shardCount));
				if (metrics.first.present()) {
					BandwidthStatus newBandwidthStatus = getBandwidthStatus(metrics.first.get());
					if (newBandwidthStatus == BandwidthStatusLow && bandwidthStatus != BandwidthStatusLow) {
						lastLowBandwidthStartTime = now();
					}
					bandwidthStatus = newBandwidthStatus;

					DisabledTraceEvent("ShardSizeUpdate", self()->distributorId)
					    .detail("Keys", keys)
					    .detail("UpdatedSize", metrics.first.get().bytes)
					    .detail("WriteBandwidth", metrics.first.get().bytesWrittenPerKSecond)
					    .detail("BandwidthStatus", bandwidthStatus)
					    .detail("ReadBandWidth", metrics.first.get().bytesReadPerKSecond)
					    .detail("ReadOps", metrics.first.get().opsReadPerKSecond)
					    .detail("BytesLower", bounds.min.bytes)
					    .detail("BytesUpper", bounds.max.bytes)
					    .detail("WriteBandwidthLower", bounds.min.bytesWrittenPerKSecond)
					    .detail("WriteBandwidthUpper", bounds.max.bytesWrittenPerKSecond)
					    .detail("ShardSizePresent", shardMetrics->get().present())
					    .detail("OldShardSize",
					            shardMetrics->get().present() ? shardMetrics->get().get().metrics.bytes : 0);

					if (shardMetrics->get().present()) {
						DisabledTraceEvent("TrackerChangeSizes")
						    .detail("Context", "trackShardMetrics")
						    .detail("Keys", keys)
						    .detail("TotalSizeEstimate", self()->dbSizeEstimate->get())
						    .detail("EndSizeOfOldShards", shardMetrics->get().get().metrics.bytes)
						    .detail("StartingSizeOfNewShards", metrics.first.get().bytes);
						self()->dbSizeEstimate->set(self()->dbSizeEstimate->get() + metrics.first.get().bytes -
						                            shardMetrics->get().get().metrics.bytes);
						if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
							// update physicalShard metrics and return whether the keys needs to move out of
							// physicalShard
							const MoveKeyRangeOutPhysicalShard needToMove =
							    self()->physicalShardCollection->trackPhysicalShard(
							        keys, metrics.first.get(), shardMetrics->get().get().metrics, initWithNewMetrics);
							if (needToMove) {
								// Do we need to update shardsAffectedByTeamFailure here?
								// TODO(zhewu): move this to physical shard tracker that does shard split based on size.
								self()->output.send(
								    RelocateShard(keys,
								                  DataMovementReason::ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD,
								                  RelocateReason::OTHER));
							}
							if (initWithNewMetrics) {
								initWithNewMetrics = false;
							}
						}
						if (keys.begin >= systemKeys.begin) {
							self()->systemSizeEstimate +=
							    metrics.first.get().bytes - shardMetrics->get().get().metrics.bytes;
						}
					}

					shardMetrics->set(ShardMetrics(metrics.first.get(), lastLowBandwidthStartTime, shardCount));
					break;
				} else {
					shardCount = metrics.second;
					if (shardMetrics->get().present()) {
						auto newShardMetrics = shardMetrics->get().get();
						newShardMetrics.shardCount = shardCount;
						shardMetrics->set(newShardMetrics);
					}
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && e.code() != error_code_dd_tracker_cancelled) {
			DisabledTraceEvent(SevDebug, "TrackShardError", self()->distributorId).detail("Keys", keys);
			// The above loop use Database cx, but those error should only be thrown in a code using transaction.
			ASSERT(!transactionRetryableErrors.contains(e.code()));
			self()->output.sendError(e); // Propagate failure to dataDistributionTracker
		}
		throw e;
	}
}

ACTOR Future<Void> readHotDetector(DataDistributionTracker* self) {
	try {
		loop {
			state KeyRange keys = waitNext(self->readHotShard.getFuture());
			Standalone<VectorRef<ReadHotRangeWithMetrics>> readHotRanges = wait(self->db->getReadHotRanges(keys));

			for (const auto& keyRange : readHotRanges) {
				TraceEvent("ReadHotRangeLog")
				    .detail("ReadDensity", keyRange.density)
				    .detail("ReadBandwidth", keyRange.readBandwidthSec)
				    .detail("ReadDensityThreshold", SERVER_KNOBS->SHARD_MAX_READ_DENSITY_RATIO)
				    .detail("KeyRangeBegin", keyRange.keys.begin)
				    .detail("KeyRangeEnd", keyRange.keys.end);
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			// Those error should only be thrown in a code using transaction.
			ASSERT(!transactionRetryableErrors.contains(e.code()));
			self->output.sendError(e); // Propagate failure to dataDistributionTracker
		}
		throw e;
	}
}

/*
ACTOR Future<Void> extrapolateShardBytes( Reference<AsyncVar<Optional<int64_t>>> inBytes,
Reference<AsyncVar<Optional<int64_t>>> outBytes ) { state std::deque< std::pair<double,int64_t> > past; loop { wait(
inBytes->onChange() ); if( inBytes->get().present() ) { past.emplace_back(now(),inBytes->get().get()); if
(past.size() < 2) outBytes->set( inBytes->get() ); else { while (past.size() > 1 && past.end()[-1].first -
past.begin()[1].first > 1.0) past.pop_front(); double rate = std::max(0.0,
double(past.end()[-1].second-past.begin()[0].second)/(past.end()[-1].first - past.begin()[0].first)); outBytes->set(
inBytes->get().get() + rate * 10.0 );
            }
        }
    }
}*/

ACTOR Future<int64_t> getFirstSize(Reference<AsyncVar<Optional<ShardMetrics>>> stats) {
	loop {
		if (stats->get().present())
			return stats->get().get().metrics.bytes;
		wait(stats->onChange());
	}
}

ACTOR Future<Void> changeSizes(DataDistributionTracker* self,
                               KeyRange keys,
                               int64_t oldShardsEndingSize,
                               std::string context) {
	state std::vector<Future<int64_t>> sizes;
	state std::vector<Future<int64_t>> systemSizes;
	for (auto it : self->shards->intersectingRanges(keys)) {
		Future<int64_t> thisSize = getFirstSize(it->value().stats);
		sizes.push_back(thisSize);
		if (it->range().begin >= systemKeys.begin) {
			systemSizes.push_back(thisSize);
		}
	}

	wait(waitForAll(sizes));
	wait(yield(TaskPriority::DataDistribution));

	int64_t newShardsStartingSize = 0;
	for (const auto& size : sizes) {
		newShardsStartingSize += size.get();
	}

	int64_t newSystemShardsStartingSize = 0;
	for (const auto& systemSize : systemSizes) {
		newSystemShardsStartingSize += systemSize.get();
	}

	int64_t totalSizeEstimate = self->dbSizeEstimate->get();
	DisabledTraceEvent("TrackerChangeSizes")
	    .detail("Context", "changeSizes when " + context)
	    .detail("Keys", keys)
	    .detail("TotalSizeEstimate", totalSizeEstimate)
	    .detail("EndSizeOfOldShards", oldShardsEndingSize)
	    .detail("StartingSizeOfNewShards", newShardsStartingSize);
	self->dbSizeEstimate->set(totalSizeEstimate + newShardsStartingSize - oldShardsEndingSize);
	self->systemSizeEstimate += newSystemShardsStartingSize;
	if (keys.begin >= systemKeys.begin) {
		self->systemSizeEstimate -= oldShardsEndingSize;
	}
	return Void();
}

struct HasBeenTrueFor : ReferenceCounted<HasBeenTrueFor> {
	explicit HasBeenTrueFor(const Optional<ShardMetrics>& value) {
		if (value.present()) {
			lowBandwidthStartTime = value.get().lastLowBandwidthStartTime;
			trigger =
			    delayJittered(std::max(0.0, SERVER_KNOBS->DD_MERGE_COALESCE_DELAY + lowBandwidthStartTime - now()),
			                  TaskPriority::DataDistributionLow) ||
			    cleared.getFuture();
		}
	}

	Future<Void> set(double lastLowBandwidthStartTime) {
		if (!trigger.isValid() || lowBandwidthStartTime != lastLowBandwidthStartTime) {
			cleared = Promise<Void>();
			trigger =
			    delayJittered(SERVER_KNOBS->DD_MERGE_COALESCE_DELAY + std::max(lastLowBandwidthStartTime - now(), 0.0),
			                  TaskPriority::DataDistributionLow) ||
			    cleared.getFuture();

			lowBandwidthStartTime = lastLowBandwidthStartTime;
		}
		return trigger;
	}
	void clear() {
		if (!trigger.isValid()) {
			return;
		}
		trigger = Future<Void>();
		cleared.send(Void());
		lowBandwidthStartTime = 0;
	}

	// True if this->value is true and has been true for this->seconds
	bool hasBeenTrueForLongEnough() const { return trigger.isValid() && trigger.isReady(); }

private:
	double lowBandwidthStartTime = 0;
	Future<Void> trigger;
	Promise<Void> cleared;
};

std::string describeSplit(KeyRange keys, Standalone<VectorRef<KeyRef>>& splitKeys) {
	std::string s;
	s += "[" + keys.begin.toString() + ", " + keys.end.toString() + ") -> ";

	for (auto& sk : splitKeys) {
		s += sk.printable() + " ";
	}

	return s;
}
void traceSplit(KeyRange keys, Standalone<VectorRef<KeyRef>>& splitKeys) {
	auto s = describeSplit(keys, splitKeys);
	TraceEvent(SevInfo, "ExecutingShardSplit").detail("AtKeys", s);
}

void executeShardSplit(DataDistributionTracker* self,
                       KeyRange keys,
                       Standalone<VectorRef<KeyRef>> splitKeys,
                       Reference<AsyncVar<Optional<ShardMetrics>>> shardSize,
                       bool relocate,
                       RelocateReason reason) {

	int numShards = splitKeys.size() - 1;
	ASSERT(numShards > 1);

	int skipRange = deterministicRandom()->randomInt(0, numShards);

	auto s = describeSplit(keys, splitKeys);
	TraceEvent(SevInfo, "ExecutingShardSplit").suppressFor(0.5).detail("Splitting", s).detail("NumShards", numShards);

	// The queue can't deal with RelocateShard requests which split an existing shard into three pieces, so
	// we have to send the unskipped ranges in this order (nibbling in from the edges of the old range)
	for (int i = 0; i < skipRange; i++)
		restartShardTrackers(self, KeyRangeRef(splitKeys[i], splitKeys[i + 1]));
	restartShardTrackers(self, KeyRangeRef(splitKeys[skipRange], splitKeys[skipRange + 1]));
	for (int i = numShards - 1; i > skipRange; i--)
		restartShardTrackers(self, KeyRangeRef(splitKeys[i], splitKeys[i + 1]));

	for (int i = 0; i < skipRange; i++) {
		KeyRangeRef r(splitKeys[i], splitKeys[i + 1]);
		self->shardsAffectedByTeamFailure->defineShard(r);
		if (relocate) {
			RelocateShard rs(r, DataMovementReason::SPLIT_SHARD, reason);
			rs.setParentRange(keys);
			self->output.send(rs);
		}
	}
	for (int i = numShards - 1; i > skipRange; i--) {
		KeyRangeRef r(splitKeys[i], splitKeys[i + 1]);
		self->shardsAffectedByTeamFailure->defineShard(r);
		if (relocate) {
			RelocateShard rs(r, DataMovementReason::SPLIT_SHARD, reason);
			rs.setParentRange(keys);
			self->output.send(rs);
		}
	}

	self->actors.add(changeSizes(self, keys, shardSize->get().get().metrics.bytes, "ShardSplit"));
}

struct RangeToSplit {
	RangeMap<Standalone<StringRef>, ShardTrackedData, KeyRangeRef>::iterator shard;
	Standalone<VectorRef<KeyRef>> faultLines;

	RangeToSplit(RangeMap<Standalone<StringRef>, ShardTrackedData, KeyRangeRef>::iterator shard,
	             Standalone<VectorRef<KeyRef>> faultLines)
	  : shard(shard), faultLines(faultLines) {}
};

Standalone<VectorRef<KeyRef>> findShardFaultLines(KeyRef shardBegin,
                                                  KeyRef shardEnd,
                                                  KeyRef tenantBegin,
                                                  KeyRef tenantEnd) {
	Standalone<VectorRef<KeyRef>> faultLines;

	ASSERT((shardBegin < tenantBegin && shardEnd > tenantBegin) || (shardBegin < tenantEnd && shardEnd > tenantEnd));

	faultLines.push_back_deep(faultLines.arena(), shardBegin);
	if (shardBegin < tenantBegin && shardEnd > tenantBegin) {
		faultLines.push_back_deep(faultLines.arena(), tenantBegin);
	}
	if (shardBegin < tenantEnd && shardEnd > tenantEnd) {
		faultLines.push_back_deep(faultLines.arena(), tenantEnd);
	}
	faultLines.push_back_deep(faultLines.arena(), shardEnd);

	return faultLines;
}

std::vector<RangeToSplit> findTenantShardBoundaries(KeyRangeMap<ShardTrackedData>* shards, KeyRange tenantKeys) {

	std::vector<RangeToSplit> result;
	auto shardContainingTenantStart = shards->rangeContaining(tenantKeys.begin);
	auto shardContainingTenantEnd = shards->rangeContainingKeyBefore(tenantKeys.end);

	// same shard
	if (shardContainingTenantStart == shardContainingTenantEnd) {
		// If shard boundaries are not aligned with tenantKeys
		if (shardContainingTenantStart.begin() != tenantKeys.begin ||
		    shardContainingTenantStart.end() != tenantKeys.end) {

			CODE_PROBE(true, "Splitting a shard that contains complete tenant key range");

			auto startShardSize = shardContainingTenantStart->value().stats;

			if (startShardSize->get().present()) {
				auto faultLines = findShardFaultLines(shardContainingTenantStart->begin(),
				                                      shardContainingTenantStart->end(),
				                                      tenantKeys.begin,
				                                      tenantKeys.end);
				result.emplace_back(shardContainingTenantStart, faultLines);
			} else {
				CODE_PROBE(true,
				           "Shard that contains complete tenant key range not split since shard stats are unavailable");
			}
		}
	} else {
		auto startShardSize = shardContainingTenantStart->value().stats;
		auto endShardSize = shardContainingTenantEnd->value().stats;

		CODE_PROBE(true, "Splitting multiple shards that a tenant key range straddles");

		if (startShardSize->get().present() && endShardSize->get().present()) {
			if (shardContainingTenantStart->begin() != tenantKeys.begin) {
				auto faultLines = findShardFaultLines(shardContainingTenantStart->begin(),
				                                      shardContainingTenantStart->end(),
				                                      tenantKeys.begin,
				                                      tenantKeys.end);
				result.emplace_back(shardContainingTenantStart, faultLines);
			}

			if (shardContainingTenantEnd->end() != tenantKeys.end) {
				auto faultLines = findShardFaultLines(shardContainingTenantEnd->begin(),
				                                      shardContainingTenantEnd->end(),
				                                      tenantKeys.begin,
				                                      tenantKeys.end);
				result.emplace_back(shardContainingTenantEnd, faultLines);
			}
		} else {
			CODE_PROBE(true,
			           "Shards that contain tenant key range not split since shard stats are unavailable",
			           probe::decoration::rare);
		}
	}

	return result;
}

bool faultLinesMatch(std::vector<RangeToSplit>& ranges, std::vector<std::vector<KeyRef>>& expectedFaultLines) {
	if (ranges.size() != expectedFaultLines.size()) {
		return false;
	}

	for (auto& range : ranges) {
		KeyRangeRef keys = KeyRangeRef(range.shard->begin(), range.shard->end());
		traceSplit(keys, range.faultLines);
	}

	for (int r = 0; r < ranges.size(); r++) {
		if (ranges[r].faultLines.size() != expectedFaultLines[r].size()) {
			return false;
		}
		for (int fl = 0; fl < ranges[r].faultLines.size(); fl++) {
			if (ranges[r].faultLines[fl] != expectedFaultLines[r][fl]) {
				return false;
			}
		}
	}

	return true;
}

TEST_CASE("/DataDistribution/Tenant/SingleShardSplit") {
	wait(Future<Void>(Void()));
	ShardTrackedData data;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin = "a"_sr, end = "f"_sr;
	KeyRangeRef k(begin, end);
	shards.insert(k, data);

	KeyRangeRef tenantKeys("b"_sr, "c"_sr);

	data.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	std::vector<std::vector<KeyRef>> expectedFaultLines = { { "a"_sr, "b"_sr, "c"_sr, "f"_sr } };
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

TEST_CASE("/DataDistribution/Tenant/SingleShardTenantAligned") {
	wait(Future<Void>(Void()));
	ShardTrackedData data;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin = "a"_sr, end = "f"_sr;
	KeyRangeRef k(begin, end);
	shards.insert(k, data);

	KeyRangeRef tenantKeys("a"_sr, "f"_sr);

	data.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	std::vector<std::vector<KeyRef>> expectedFaultLines = {};
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

TEST_CASE("/DataDistribution/Tenant/SingleShardTenantAlignedAtStart") {
	wait(Future<Void>(Void()));
	ShardTrackedData data;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin = "a"_sr, end = "f"_sr;
	KeyRangeRef k(begin, end);
	shards.insert(k, data);

	KeyRangeRef tenantKeys("a"_sr, "d"_sr);

	data.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	std::vector<std::vector<KeyRef>> expectedFaultLines = { { "a"_sr, "d"_sr, "f"_sr } };
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

TEST_CASE("/DataDistribution/Tenant/SingleShardTenantAlignedAtEnd") {
	wait(Future<Void>(Void()));
	ShardTrackedData data;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin = "a"_sr, end = "f"_sr;
	KeyRangeRef k(begin, end);
	shards.insert(k, data);

	KeyRangeRef tenantKeys("b"_sr, "f"_sr);

	data.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	std::vector<std::vector<KeyRef>> expectedFaultLines = { { "a"_sr, "b"_sr, "f"_sr } };
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

TEST_CASE("/DataDistribution/Tenant/DoubleShardSplit") {
	wait(Future<Void>(Void()));
	ShardTrackedData data1, data2;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data1.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	data2.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin1 = "a"_sr, end1 = "c"_sr;
	KeyRef begin2 = "d"_sr, end2 = "f"_sr;
	KeyRangeRef k1(begin1, end1);
	KeyRangeRef k2(begin2, end2);

	shards.insert(k1, data1);
	shards.insert(k2, data2);

	KeyRangeRef tenantKeys("b"_sr, "e"_sr);

	data1.stats->set(sm);
	data2.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	for (auto& range : result) {
		KeyRangeRef keys = KeyRangeRef(range.shard->begin(), range.shard->end());
		traceSplit(keys, range.faultLines);
	}

	std::vector<std::vector<KeyRef>> expectedFaultLines = { { "a"_sr, "b"_sr, "c"_sr }, { "d"_sr, "e"_sr, "f"_sr } };
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

TEST_CASE("/DataDistribution/Tenant/DoubleShardTenantAlignedAtStart") {
	wait(Future<Void>(Void()));
	ShardTrackedData data1, data2;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data1.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	data2.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin1 = "a"_sr, end1 = "c"_sr;
	KeyRef begin2 = "d"_sr, end2 = "f"_sr;
	KeyRangeRef k1(begin1, end1);
	KeyRangeRef k2(begin2, end2);

	shards.insert(k1, data1);
	shards.insert(k2, data2);

	KeyRangeRef tenantKeys("a"_sr, "e"_sr);

	data1.stats->set(sm);
	data2.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	std::vector<std::vector<KeyRef>> expectedFaultLines = { { "d"_sr, "e"_sr, "f"_sr } };
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

TEST_CASE("/DataDistribution/Tenant/DoubleShardTenantAlignedAtEnd") {
	wait(Future<Void>(Void()));
	ShardTrackedData data1, data2;
	ShardMetrics sm(StorageMetrics(), now(), 1);
	data1.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	data2.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();

	KeyRangeMap<ShardTrackedData> shards;

	KeyRef begin1 = "a"_sr, end1 = "c"_sr;
	KeyRef begin2 = "d"_sr, end2 = "f"_sr;
	KeyRangeRef k1(begin1, end1);
	KeyRangeRef k2(begin2, end2);

	shards.insert(k1, data1);
	shards.insert(k2, data2);

	KeyRangeRef tenantKeys("b"_sr, "f"_sr);

	data1.stats->set(sm);
	data2.stats->set(sm);

	std::vector<RangeToSplit> result = findTenantShardBoundaries(&shards, tenantKeys);

	std::vector<std::vector<KeyRef>> expectedFaultLines = { { "a"_sr, "b"_sr, "c"_sr } };
	ASSERT(faultLinesMatch(result, expectedFaultLines));

	return Void();
}

ACTOR Future<Void> tenantShardSplitter(DataDistributionTracker* self, KeyRange tenantKeys) {
	wait(Future<Void>(Void()));
	std::vector<RangeToSplit> rangesToSplit = findTenantShardBoundaries(self->shards, tenantKeys);

	for (auto& range : rangesToSplit) {
		KeyRangeRef keys = KeyRangeRef(range.shard->begin(), range.shard->end());
		traceSplit(keys, range.faultLines);
		executeShardSplit(self, keys, range.faultLines, range.shard->value().stats, true, RelocateReason::TENANT_SPLIT);
	}

	return Void();
}

ACTOR Future<Void> tenantCreationHandling(DataDistributionTracker* self, TenantCacheTenantCreated req) {
	TraceEvent(SevInfo, "TenantCacheTenantCreated").detail("Begin", req.keys.begin).detail("End", req.keys.end);

	wait(tenantShardSplitter(self, req.keys));
	req.reply.send(true);
	return Void();
}

ACTOR Future<Void> shardSplitter(DataDistributionTracker* self,
                                 KeyRange keys,
                                 Reference<AsyncVar<Optional<ShardMetrics>>> shardSize,
                                 ShardSizeBounds shardBounds,
                                 RelocateReason reason) {
	state StorageMetrics metrics = shardSize->get().get().metrics;
	state BandwidthStatus bandwidthStatus = getBandwidthStatus(metrics);

	// Split
	CODE_PROBE(true, "shard to be split");

	StorageMetrics splitMetrics;
	splitMetrics.bytes = shardBounds.max.bytes / 2;
	splitMetrics.bytesWrittenPerKSecond =
	    keys.begin >= keyServersKeys.begin ? splitMetrics.infinity : SERVER_KNOBS->SHARD_SPLIT_BYTES_PER_KSEC;
	splitMetrics.iosPerKSecond = splitMetrics.infinity;
	splitMetrics.bytesReadPerKSecond = splitMetrics.infinity; // Don't split by readBandwidthSec

	state Standalone<VectorRef<KeyRef>> splitKeys =
	    wait(self->db->splitStorageMetrics(keys, splitMetrics, metrics, SERVER_KNOBS->MIN_SHARD_BYTES));
	// fprintf(stderr, "split keys:\n");
	// for( int i = 0; i < splitKeys.size(); i++ ) {
	//	fprintf(stderr, "   %s\n", printable(splitKeys[i]).c_str());
	//}
	int numShards = splitKeys.size() - 1;

	TraceEvent("RelocateShardStartSplit", self->distributorId)
	    .suppressFor(1.0)
	    .detail("Begin", keys.begin)
	    .detail("End", keys.end)
	    .detail("MaxBytes", shardBounds.max.bytes)
	    .detail("MetricsBytes", metrics.bytes)
	    .detail("Bandwidth",
	            bandwidthStatus == BandwidthStatusHigh     ? "High"
	            : bandwidthStatus == BandwidthStatusNormal ? "Normal"
	                                                       : "Low")
	    .detail("BytesWrittenPerKSec", metrics.bytesWrittenPerKSecond)
	    .detail("NumShards", numShards);

	if (numShards > 1) {
		executeShardSplit(self, keys, splitKeys, shardSize, true, reason);
	} else {
		wait(delay(1.0, TaskPriority::DataDistribution)); // In case the reason the split point was off was due to a
		                                                  // discrepancy between storage servers
	}
	return Void();
}

ACTOR Future<Void> brokenPromiseToReady(Future<Void> f) {
	try {
		wait(f);
	} catch (Error& e) {
		if (e.code() != error_code_broken_promise) {
			throw;
		}
	}
	return Void();
}

static bool shardMergeFeasible(DataDistributionTracker* self, KeyRange const& keys, KeyRangeRef adjRange) {
	if (!SERVER_KNOBS->DD_TENANT_AWARENESS_ENABLED) {
		return true;
	}

	ASSERT(self->ddTenantCache.present());

	Optional<Reference<TCTenantInfo>> tenantOwningRange = {};
	Optional<Reference<TCTenantInfo>> tenantOwningAdjRange = {};

	tenantOwningRange = self->ddTenantCache.get()->tenantOwning(keys.begin);
	tenantOwningAdjRange = self->ddTenantCache.get()->tenantOwning(adjRange.begin);

	if ((tenantOwningRange.present() != tenantOwningAdjRange.present()) ||
	    (tenantOwningRange.present() && (tenantOwningRange != tenantOwningAdjRange))) {
		return false;
	}

	return true;
}

static bool shardForwardMergeFeasible(DataDistributionTracker* self, KeyRange const& keys, KeyRangeRef nextRange) {
	if (keys.end == allKeys.end) {
		return false;
	}

	if (self->userRangeConfig->rangeContaining(keys.begin)->range().end < nextRange.end) {
		return false;
	}

	if (self->bulkLoadEnabled && self->bulkLoadTaskCollection->overlappingTask(nextRange)) {
		TraceEvent(SevWarn, "ShardCanForwardMergeButUnderBulkLoading", self->distributorId)
		    .suppressFor(5.0)
		    .detail("ShardMerging", keys)
		    .detail("NextShard", nextRange);
		return false;
	}

	return shardMergeFeasible(self, keys, nextRange);
}

static bool shardBackwardMergeFeasible(DataDistributionTracker* self, KeyRange const& keys, KeyRangeRef prevRange) {
	if (keys.begin == allKeys.begin) {
		return false;
	}

	if (self->userRangeConfig->rangeContaining(keys.begin)->range().begin > prevRange.begin) {
		return false;
	}

	if (self->bulkLoadEnabled && self->bulkLoadTaskCollection->overlappingTask(prevRange)) {
		TraceEvent(SevWarn, "ShardCanBackwardMergeButUnderBulkLoading", self->distributorId)
		    .suppressFor(5.0)
		    .detail("ShardMerging", keys)
		    .detail("PrevShard", prevRange);
		return false;
	}

	return shardMergeFeasible(self, keys, prevRange);
}

// Must be atomic
void createShardToBulkLoad(DataDistributionTracker* self, BulkLoadState bulkLoadState) {
	KeyRange keys = bulkLoadState.getRange();
	ASSERT(!keys.empty());
	TraceEvent e(SevInfo, "DDBulkLoadCreateShardToBulkLoad", self->distributorId);
	e.detail("TaskId", bulkLoadState.getTaskId());
	e.detail("BulkLoadRange", keys);
	// Create shards at the two ends and do not data move for those shards
	// Create a new shard and trigger data move for bulk loading on the new shard
	// Step 1: split left without data move nor updating dbEstimate size (will be rebuilt after DD restarts)
	for (auto it : self->shards->intersectingRanges(keys)) {
		if (it->range().begin < keys.begin) {
			KeyRange leftRange = Standalone(KeyRangeRef(it->range().begin, keys.begin));
			e.detail("FirstSplitShard", it->range());
			restartShardTrackers(self, leftRange);
		}
		break;
	}

	// Step 2: split right without data move nor updating dbEstimate size (will be rebuilt after DD restarts)
	for (auto it : self->shards->intersectingRanges(keys)) {
		if (it->range().end > keys.end) {
			KeyRange rightRange = Standalone(KeyRangeRef(keys.end, it->range().end));
			e.detail("LastSplitShard", it->range());
			restartShardTrackers(self, rightRange);
			break;
		}
	}

	// Step 3: merge with new data move
	StorageMetrics oldStats;
	int shardCount = 0;
	for (auto it : self->shards->intersectingRanges(keys)) {
		Reference<AsyncVar<Optional<ShardMetrics>>> stats;
		if (it->value().stats->get().present()) {
			oldStats = oldStats + it->value().stats->get().get().metrics;
			shardCount = shardCount + it->value().stats->get().get().shardCount;
		}
	}
	restartShardTrackers(self, keys, ShardMetrics(oldStats, now(), shardCount));
	self->shardsAffectedByTeamFailure->defineShard(keys);
	self->output.send(
	    RelocateShard(keys, DataMovementReason::TEAM_HEALTHY, RelocateReason::OTHER, bulkLoadState.getTaskId()));
	e.detail("NewShardToLoad", keys);
	return;
}

Future<Void> shardMerger(DataDistributionTracker* self,
                         KeyRange const& keys,
                         Reference<AsyncVar<Optional<ShardMetrics>>> shardSize) {
	const UID actionId = deterministicRandom()->randomUniqueID();
	const Severity stSev = static_cast<Severity>(SERVER_KNOBS->DD_SHARD_TRACKING_LOG_SEVERITY);
	int64_t maxShardSize = self->maxShardSize->get().get();

	auto prevIter = self->shards->rangeContaining(keys.begin);
	auto nextIter = self->shards->rangeContaining(keys.begin);

	CODE_PROBE(true, "shard to be merged");
	ASSERT(keys.begin > allKeys.begin);

	// This will merge shards both before and after "this" shard in keyspace.
	int shardsMerged = 1;
	bool forwardComplete = false;
	KeyRangeRef merged;

	StorageMetrics endingStats = shardSize->get().get().metrics;
	int shardCount = shardSize->get().get().shardCount;
	double lastLowBandwidthStartTime = shardSize->get().get().lastLowBandwidthStartTime;
	if (FLOW_KNOBS->DELAY_JITTER_OFFSET * SERVER_KNOBS->DD_MERGE_COALESCE_DELAY >
	        SERVER_KNOBS->DD_LOW_BANDWIDTH_DELAY &&
	    now() - lastLowBandwidthStartTime < SERVER_KNOBS->DD_LOW_BANDWIDTH_DELAY) {
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "ShardMergeTooSoon", self->distributorId)
		    .detail("Keys", keys)
		    .detail("LastLowBandwidthStartTime", lastLowBandwidthStartTime);
	}

	int64_t systemBytes = keys.begin >= systemKeys.begin ? shardSize->get().get().metrics.bytes : 0;

	loop {
		Optional<ShardMetrics> newMetrics;
		if (!forwardComplete) {
			if (nextIter->range().end == allKeys.end) {
				forwardComplete = true;
				continue;
			}

			++nextIter;
			if (!shardForwardMergeFeasible(self, keys, nextIter->range())) {
				--nextIter;
				forwardComplete = true;
				continue;
			}

			newMetrics = nextIter->value().stats->get();

			// If going forward, give up when the next shard's stats are not yet present, or if the
			// the shard is already over the merge bounds.
			const int newCount = newMetrics.present() ? (shardCount + newMetrics.get().shardCount) : shardCount;
			const int64_t newSize =
			    newMetrics.present() ? (endingStats.bytes + newMetrics.get().metrics.bytes) : endingStats.bytes;
			if (!newMetrics.present() || newCount >= CLIENT_KNOBS->SHARD_COUNT_LIMIT || newSize > maxShardSize) {
				if (shardsMerged == 1) {
					TraceEvent(stSev, "ShardMergeStopForward", self->distributorId)
					    .detail("ActionID", actionId)
					    .detail("Keys", keys)
					    .detail("MetricsPresent", newMetrics.present())
					    .detail("ShardCount", newCount)
					    .detail("ShardSize", newSize)
					    .detail("MaxShardSize", maxShardSize);
				}
				--nextIter;
				forwardComplete = true;
				continue;
			}
		} else {
			--prevIter;
			newMetrics = prevIter->value().stats->get();

			if (!shardBackwardMergeFeasible(self, keys, prevIter->range())) {
				++prevIter;
				break;
			}

			// If going backward, stop when the stats are not present or if the shard is already over the merge
			//  bounds. If this check triggers right away (if we have not merged anything) then return a trigger
			//  on the previous shard changing "size".
			const int newCount = newMetrics.present() ? (shardCount + newMetrics.get().shardCount) : shardCount;
			const int64_t newSize =
			    newMetrics.present() ? (endingStats.bytes + newMetrics.get().metrics.bytes) : endingStats.bytes;
			if (!newMetrics.present() || newCount >= CLIENT_KNOBS->SHARD_COUNT_LIMIT || newSize > maxShardSize) {
				if (shardsMerged == 1) {
					TraceEvent(stSev, "ShardMergeStopBackward", self->distributorId)
					    .detail("ActionID", actionId)
					    .detail("Keys", keys)
					    .detail("MetricsPresent", newMetrics.present())
					    .detail("ShardCount", newCount)
					    .detail("ShardSize", newSize)
					    .detail("MaxShardSize", maxShardSize);
					CODE_PROBE(true, "shardMerger cannot merge anything");
					return brokenPromiseToReady(prevIter->value().stats->onChange());
				}

				++prevIter;
				break;
			}
		}

		merged = KeyRangeRef(prevIter->range().begin, nextIter->range().end);
		endingStats += newMetrics.get().metrics;
		shardCount += newMetrics.get().shardCount;
		lastLowBandwidthStartTime = newMetrics.get().lastLowBandwidthStartTime;
		if ((forwardComplete ? prevIter->range().begin : nextIter->range().begin) >= systemKeys.begin) {
			systemBytes += newMetrics.get().metrics.bytes;
		}
		shardsMerged++;

		auto shardBounds = getShardSizeBounds(merged, maxShardSize);
		// If we just recently get the current shard's metrics (i.e., less than DD_LOW_BANDWIDTH_DELAY ago), it
		// means the shard's metric may not be stable yet. So we cannot continue merging in this direction.
		if (endingStats.bytes >= shardBounds.min.bytes || getBandwidthStatus(endingStats) != BandwidthStatusLow ||
		    now() - lastLowBandwidthStartTime < SERVER_KNOBS->DD_LOW_BANDWIDTH_DELAY ||
		    shardsMerged >= SERVER_KNOBS->DD_MERGE_LIMIT) {
			// The merged range is larger than the min bounds so we cannot continue merging in this direction.
			//  This means that:
			//  1. If we were going forwards (the starting direction), we roll back the last speculative merge.
			//      In this direction we do not want to go above this boundary since we will merge at least one in
			//      the other direction, even when that goes over the bounds.
			//  2. If we were going backwards we always want to merge one more shard on (to make sure we go over
			//      the shard min bounds) so we "break" without resetting the merged range.
			if (forwardComplete)
				break;

			shardsMerged--;
			if (shardsMerged == 1) {
				TraceEvent(stSev, "ShardMergeUndoForward", self->distributorId)
				    .detail("ActionID", actionId)
				    .detail("Keys", keys)
				    .detail("ShardCount", shardCount)
				    .detail("EndingMetrics", endingStats.toString())
				    .detail("MaxShardSize", maxShardSize);
			}
			// If going forward, remove most recently added range
			endingStats -= newMetrics.get().metrics;
			shardCount -= newMetrics.get().shardCount;
			if (nextIter->range().begin >= systemKeys.begin) {
				systemBytes -= newMetrics.get().metrics.bytes;
			}
			--nextIter;
			merged = KeyRangeRef(prevIter->range().begin, nextIter->range().end);
			forwardComplete = true;
		}
	}

	if (shardsMerged == 1) {
		return brokenPromiseToReady(nextIter->value().stats->onChange());
	}

	// restarting shard tracker will dereference values in the shard map, so make a copy
	KeyRange mergeRange = merged;

	// OldKeys: Shards in the key range are merged as one shard defined by NewKeys;
	// NewKeys: New key range after shards are merged;
	// EndingSize: The new merged shard size in bytes;
	// BatchedMerges: The number of shards merged. Each shard is defined in self->shards;
	// LastLowBandwidthStartTime: When does a shard's bandwidth status becomes BandwidthStatusLow. If a shard's
	// status
	//   becomes BandwidthStatusLow less than DD_LOW_BANDWIDTH_DELAY ago, the merging logic will stop at the shard;
	// ShardCount: The number of non-splittable shards that are merged. Each shard is defined in self->shards may
	// have
	//   more than 1 shards.
	TraceEvent("RelocateShardMergeMetrics", self->distributorId)
	    .detail("OldKeys", keys)
	    .detail("NewKeys", mergeRange)
	    .detail("EndingSize", endingStats.bytes)
	    .detail("BatchedMerges", shardsMerged)
	    .detail("LastLowBandwidthStartTime", lastLowBandwidthStartTime)
	    .detail("ShardCount", shardCount);

	if (mergeRange.begin < systemKeys.begin) {
		self->systemSizeEstimate -= systemBytes;
	}
	restartShardTrackers(self, mergeRange, ShardMetrics(endingStats, lastLowBandwidthStartTime, shardCount));
	self->shardsAffectedByTeamFailure->defineShard(mergeRange);
	self->output.send(RelocateShard(mergeRange, DataMovementReason::MERGE_SHARD, RelocateReason::MERGE_SHARD));

	// We are about to be cancelled by the call to restartShardTrackers
	return Void();
}

ACTOR Future<Void> shardEvaluator(DataDistributionTracker* self,
                                  KeyRange keys,
                                  Reference<AsyncVar<Optional<ShardMetrics>>> shardSize,
                                  Reference<HasBeenTrueFor> wantsToMerge) {
	Future<Void> onChange = shardSize->onChange() || yieldedFuture(self->maxShardSize->onChange());

	// There are the bounds inside of which we are happy with the shard size.
	// getShardSizeBounds() will always have shardBounds.min.bytes == 0 for shards that start at allKeys.begin,
	//  so will will never attempt to merge that shard with the one previous.
	ShardSizeBounds shardBounds = getShardSizeBounds(keys, self->maxShardSize->get().get());
	StorageMetrics const& stats = shardSize->get().get().metrics;
	auto bandwidthStatus = getBandwidthStatus(stats);

	bool sizeSplit = stats.bytes > shardBounds.max.bytes,
	     writeSplit = bandwidthStatus == BandwidthStatusHigh && keys.begin < keyServersKeys.begin;
	bool shouldSplit = sizeSplit || writeSplit;
	bool onBulkLoading = self->bulkLoadEnabled && self->bulkLoadTaskCollection->overlappingTask(keys);
	if (onBulkLoading && shouldSplit) {
		TraceEvent(SevWarn, "ShardWantToSplitButUnderBulkLoading", self->distributorId)
		    .suppressFor(5.0)
		    .detail("KeyRange", keys);
		shouldSplit = false;
		// Bulk loading will delay shard boundary change until the loading completes
		onChange = onChange || delay(SERVER_KNOBS->DD_BULKLOAD_SHARD_BOUNDARY_CHANGE_DELAY_SEC);
	}

	auto prevIter = self->shards->rangeContaining(keys.begin);
	if (keys.begin > allKeys.begin)
		--prevIter;

	auto nextIter = self->shards->rangeContaining(keys.begin);
	if (keys.end < allKeys.end)
		++nextIter;

	bool shouldMerge = stats.bytes < shardBounds.min.bytes && bandwidthStatus == BandwidthStatusLow &&
	                   (shardForwardMergeFeasible(self, keys, nextIter.range()) ||
	                    shardBackwardMergeFeasible(self, keys, prevIter.range()));
	if (onBulkLoading && shouldMerge) {
		TraceEvent(SevWarn, "ShardWantToMergeButUnderBulkLoading", self->distributorId)
		    .suppressFor(5.0)
		    .detail("KeyRange", keys);
		shouldMerge = false;
		// Bulk loading will delay shard boundary change until the loading completes
		onChange = onChange || delay(SERVER_KNOBS->DD_BULKLOAD_SHARD_BOUNDARY_CHANGE_DELAY_SEC);
	}

	// Every invocation must set this or clear it
	if (shouldMerge && !self->anyZeroHealthyTeams->get()) {
		auto whenLongEnough = wantsToMerge->set(shardSize->get().get().lastLowBandwidthStartTime);
		if (!wantsToMerge->hasBeenTrueForLongEnough()) {
			onChange = onChange || whenLongEnough;
		}
	} else {
		wantsToMerge->clear();
		if (shouldMerge) {
			onChange = onChange || self->anyZeroHealthyTeams->onChange();
		}
	}

	// TraceEvent("EdgeCaseTraceShardEvaluator", self->distributorId)
	//     .detail("BeginKey", keys.begin.printable())
	//     .detail("EndKey", keys.end.printable())
	//     .detail("ShouldSplit", shouldSplit)
	//     .detail("ShouldMerge", shouldMerge)
	//     .detail("HasBeenTrueLongEnough", wantsToMerge->hasBeenTrueForLongEnough())
	//     .detail("CurrentMetrics", stats.toString())
	//     .detail("ShardBoundsMaxBytes", shardBounds.max.bytes)
	//     .detail("ShardBoundsMinBytes", shardBounds.min.bytes)
	//     .detail("WriteBandwitdhStatus", bandwidthStatus)
	//     .detail("SplitBecauseHighWriteBandWidth", writeSplit ? "Yes" : "No");

	if (!self->anyZeroHealthyTeams->get() && wantsToMerge->hasBeenTrueForLongEnough()) {
		onChange = onChange || shardMerger(self, keys, shardSize);
	}
	if (shouldSplit) {
		RelocateReason reason = sizeSplit ? RelocateReason::SIZE_SPLIT : RelocateReason::WRITE_SPLIT;
		onChange = onChange || shardSplitter(self, keys, shardSize, shardBounds, reason);
	}

	wait(onChange);
	return Void();
}

ACTOR Future<Void> shardTracker(DataDistributionTracker::SafeAccessor self,
                                KeyRange keys,
                                Reference<AsyncVar<Optional<ShardMetrics>>> shardSize) {
	wait(yieldedFuture(self()->readyToStart.getFuture()));

	if (!shardSize->get().present())
		wait(shardSize->onChange());

	if (!self()->maxShardSize->get().present())
		wait(yieldedFuture(self()->maxShardSize->onChange()));

	// Since maxShardSize will become present for all shards at once, avoid slow tasks with a short delay
	wait(delay(0, TaskPriority::DataDistribution));

	// Survives multiple calls to shardEvaluator and keeps merges from happening too quickly.
	state Reference<HasBeenTrueFor> wantsToMerge(new HasBeenTrueFor(shardSize->get()));

	/*TraceEvent("ShardTracker", self()->distributorId)
	    .detail("Begin", keys.begin)
	    .detail("End", keys.end)
	    .detail("TrackerID", trackerID)
	    .detail("MaxBytes", self()->maxShardSize->get().get())
	    .detail("ShardSize", shardSize->get().get().bytes)
	    .detail("BytesPerKSec", shardSize->get().get().bytesWrittenPerKSecond);*/

	try {
		loop {
			// Use the current known size to check for (and start) splits and merges.
			wait(shardEvaluator(self(), keys, shardSize, wantsToMerge));

			// We could have a lot of actors being released from the previous wait at the same time. Immediately
			// calling delay(0) mitigates the resulting SlowTask
			wait(delay(0, TaskPriority::DataDistribution));
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && e.code() != error_code_dd_tracker_cancelled) {
			self()->output.sendError(e); // Propagate failure to dataDistributionTracker
		}
		throw e;
	}
}

void restartShardTrackers(DataDistributionTracker* self,
                          KeyRangeRef keys,
                          Optional<ShardMetrics> startingMetrics,
                          bool whenDDInit) {
	auto ranges = self->shards->getAffectedRangesAfterInsertion(keys, ShardTrackedData());
	for (int i = 0; i < ranges.size(); i++) {
		if (!ranges[i].value.trackShard.isValid() && ranges[i].begin != keys.begin) {
			// When starting, key space will be full of "dummy" default constructed entries.
			// This should happen when called from trackInitialShards()
			ASSERT(!self->readyToStart.isSet());
			continue;
		}

		auto shardMetrics = makeReference<AsyncVar<Optional<ShardMetrics>>>();

		// For the case where the new tracker will take over at the boundaries of current shard(s)
		//  we can use the old size if it is available. This will be the case when merging shards.
		if (startingMetrics.present()) {
			ASSERT(ranges.size() == 1);
			/*TraceEvent("ShardTrackerSizePreset", self->distributorId)
			    .detail("Keys", keys)
			    .detail("Size", startingMetrics.get().metrics.bytes)
			    .detail("Merges", startingMetrics.get().merges);*/
			CODE_PROBE(true, "shardTracker started with trackedBytes already set");
			shardMetrics->set(startingMetrics);
		}

		ShardTrackedData data;
		data.stats = shardMetrics;
		data.trackShard = shardTracker(DataDistributionTracker::SafeAccessor(self), ranges[i], shardMetrics);
		data.trackBytes =
		    trackShardMetrics(DataDistributionTracker::SafeAccessor(self), ranges[i], shardMetrics, whenDDInit);
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->DD_SHARD_USABLE_REGION_CHECK_RATE > 0 &&
		    self->usableRegions != -1) {
			data.trackUsableRegion = shardUsableRegions(DataDistributionTracker::SafeAccessor(self), ranges[i]);
		}
		self->shards->insert(ranges[i], data);
	}
}

ACTOR Future<Void> trackInitialShards(DataDistributionTracker* self, Reference<InitialDataDistribution> initData) {
	TraceEvent("TrackInitialShards", self->distributorId).detail("InitialShardCount", initData->shards.size());

	// This line reduces the priority of shard initialization to prevent interference with failure monitoring.
	// SOMEDAY: Figure out what this priority should actually be
	wait(delay(0.0, TaskPriority::DataDistribution));

	state std::vector<Key> customBoundaries;
	for (auto it : self->userRangeConfig->ranges()) {
		customBoundaries.push_back(it->range().begin);
	}

	state int s;
	state int customBoundary = 0;
	for (s = 0; s < initData->shards.size() - 1; s++) {
		Key beginKey = initData->shards[s].key;
		Key endKey = initData->shards[s + 1].key;
		while (customBoundary < customBoundaries.size() && customBoundaries[customBoundary] <= beginKey) {
			customBoundary++;
		}
		while (customBoundary < customBoundaries.size() && customBoundaries[customBoundary] < endKey) {
			restartShardTrackers(
			    self, KeyRangeRef(beginKey, customBoundaries[customBoundary]), Optional<ShardMetrics>(), true);
			beginKey = customBoundaries[customBoundary];
			customBoundary++;
		}
		restartShardTrackers(self, KeyRangeRef(beginKey, endKey), Optional<ShardMetrics>(), true);
		wait(yield(TaskPriority::DataDistribution));
	}

	Future<Void> initialSize = changeSizes(self, KeyRangeRef(allKeys.begin, allKeys.end), 0, "ShardInit");
	self->readyToStart.send(Void());
	wait(initialSize);
	self->maxShardSizeUpdater = updateMaxShardSize(self->dbSizeEstimate, self->maxShardSize);

	return Void();
}

ACTOR Future<Void> fetchTopKShardMetrics_impl(DataDistributionTracker* self, GetTopKMetricsRequest req) {
	state Future<Void> onChange;
	state std::vector<GetTopKMetricsReply::KeyRangeStorageMetrics> returnMetrics;
	// random pick a portion of shard
	if (req.keys.size() > SERVER_KNOBS->DD_SHARD_COMPARE_LIMIT) {
		deterministicRandom()->randomShuffle(req.keys, 0, SERVER_KNOBS->DD_SHARD_COMPARE_LIMIT);
	}
	try {
		loop {
			onChange = Future<Void>();
			returnMetrics.clear();
			state int64_t minReadLoad = -1;
			state int64_t maxReadLoad = -1;
			state int i;
			for (i = 0; i < SERVER_KNOBS->DD_SHARD_COMPARE_LIMIT && i < req.keys.size(); ++i) {
				auto range = req.keys[i];
				StorageMetrics metrics;
				for (auto t : self->shards->intersectingRanges(range)) {
					auto& stats = t.value().stats;
					if (!stats->get().present()) {
						onChange = stats->onChange();
						break;
					}
					metrics += t.value().stats->get().get().metrics;
				}

				// skip if current stats is invalid
				if (onChange.isValid()) {
					break;
				}

				auto readLoad = metrics.readLoadKSecond();
				if (readLoad > 0) {
					minReadLoad = std::min(readLoad, minReadLoad);
					maxReadLoad = std::max(readLoad, maxReadLoad);
					if (req.minReadLoadPerKSecond <= readLoad && readLoad <= req.maxReadLoadPerKSecond) {
						returnMetrics.emplace_back(range, metrics);
					}
				}

				wait(yield());
			}
			// FIXME(xwang): Do we need to track slow task here?
			if (!onChange.isValid()) {
				if (req.getTopK() >= returnMetrics.size())
					req.reply.send(GetTopKMetricsReply(returnMetrics, minReadLoad, maxReadLoad));
				else {
					std::nth_element(returnMetrics.begin(),
					                 returnMetrics.begin() + req.getTopK() - 1,
					                 returnMetrics.end(),
					                 GetTopKMetricsRequest::compare);
					req.reply.send(
					    GetTopKMetricsReply(std::vector<GetTopKMetricsReply::KeyRangeStorageMetrics>(
					                            returnMetrics.begin(), returnMetrics.begin() + req.getTopK()),
					                        minReadLoad,
					                        maxReadLoad));
				}
				return Void();
			}
			wait(onChange);
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !req.reply.isSet())
			req.reply.sendError(e);
		throw;
	}
}

ACTOR Future<Void> fetchTopKShardMetrics(DataDistributionTracker* self, GetTopKMetricsRequest req) {
	choose {
		// simulate time_out
		when(wait(g_network->isSimulated() && BUGGIFY_WITH_PROB(0.01) ? Never()
		                                                              : fetchTopKShardMetrics_impl(self, req))) {}
		when(wait(delay(SERVER_KNOBS->DD_SHARD_METRICS_TIMEOUT))) {
			CODE_PROBE(true, "TopK DD_SHARD_METRICS_TIMEOUT");
			req.reply.send(GetTopKMetricsReply());
		}
	}
	return Void();
}

ACTOR Future<Void> fetchShardMetrics_impl(DataDistributionTracker* self, GetMetricsRequest req) {
	try {
		loop {
			Future<Void> onChange;
			StorageMetrics returnMetrics;
			for (auto t : self->shards->intersectingRanges(req.keys)) {
				auto& stats = t.value().stats;
				if (!stats->get().present()) {
					onChange = stats->onChange();
					break;
				}
				returnMetrics += t.value().stats->get().get().metrics;
			}

			if (!onChange.isValid()) {
				req.reply.send(returnMetrics);
				return Void();
			}

			wait(onChange);
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !req.reply.isSet())
			req.reply.sendError(e);
		throw;
	}
}

ACTOR Future<Void> fetchShardMetrics(DataDistributionTracker* self, GetMetricsRequest req) {
	choose {
		when(wait(fetchShardMetrics_impl(self, req))) {}
		when(wait(delay(SERVER_KNOBS->DD_SHARD_METRICS_TIMEOUT, TaskPriority::DataDistribution))) {
			CODE_PROBE(true, "DD_SHARD_METRICS_TIMEOUT");
			StorageMetrics largeMetrics;
			largeMetrics.bytes = getMaxShardSize(self->dbSizeEstimate->get());
			req.reply.send(largeMetrics);
		}
	}
	return Void();
}

ACTOR Future<Void> fetchShardMetricsList_impl(DataDistributionTracker* self, GetMetricsListRequest req) {
	try {
		loop {
			// used to control shard limit
			int shardNum = 0;
			// list of metrics, regenerate on loop when full range unsuccessful
			Standalone<VectorRef<DDMetricsRef>> result;
			Future<Void> onChange;
			auto beginIter = self->shards->containedRanges(req.keys).begin();
			auto endIter = self->shards->intersectingRanges(req.keys).end();
			for (auto t = beginIter; t != endIter; ++t) {
				auto& stats = t.value().stats;
				if (!stats->get().present()) {
					onChange = stats->onChange();
					break;
				}
				result.push_back_deep(result.arena(),
				                      DDMetricsRef(stats->get().get().metrics.bytes,
				                                   stats->get().get().metrics.bytesWrittenPerKSecond,
				                                   KeyRef(t.begin().toString())));
				++shardNum;
				if (shardNum >= req.shardLimit) {
					break;
				}
			}

			if (!onChange.isValid()) {
				req.reply.send(result);
				return Void();
			}

			wait(onChange);
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !req.reply.isSet())
			req.reply.sendError(e);
		throw;
	}
}

ACTOR Future<Void> fetchShardMetricsList(DataDistributionTracker* self, GetMetricsListRequest req) {
	choose {
		when(wait(fetchShardMetricsList_impl(self, req))) {}
		when(wait(delay(SERVER_KNOBS->DD_SHARD_METRICS_TIMEOUT))) {
			req.reply.sendError(timed_out());
		}
	}
	return Void();
}

void triggerStorageQueueRebalance(DataDistributionTracker* self, RebalanceStorageQueueRequest req) {
	TraceEvent e("TriggerDataMoveStorageQueueRebalance", self->distributorId);
	e.detail("Server", req.serverId);
	e.detail("Teams", req.teams.size());
	int64_t maxShardWriteTraffic = 0;
	KeyRange shardToMove;
	ShardsAffectedByTeamFailure::Team selectedTeam;
	for (const auto& team : req.teams) {
		for (auto const& shard : self->shardsAffectedByTeamFailure->getShardsFor(team)) {
			for (auto it : self->shards->intersectingRanges(shard)) {
				if (it->value().stats->get().present()) {
					int64_t shardWriteTraffic = it->value().stats->get().get().metrics.bytesWrittenPerKSecond;
					if (shardWriteTraffic > maxShardWriteTraffic &&
					    (SERVER_KNOBS->DD_ENABLE_REBALANCE_STORAGE_QUEUE_WITH_LIGHT_WRITE_SHARD ||
					     shardWriteTraffic > SERVER_KNOBS->REBALANCE_STORAGE_QUEUE_SHARD_PER_KSEC_MIN)) {
						shardToMove = it->range();
						maxShardWriteTraffic = shardWriteTraffic;
					}
				}
			}
		}
	}
	if (!shardToMove.empty()) {
		e.detail("TeamSelected", selectedTeam.servers);
		e.detail("ShardSelected", shardToMove);
		e.detail("ShardWriteBytesPerKSec", maxShardWriteTraffic);
		RelocateShard rs(shardToMove, DataMovementReason::REBALANCE_STORAGE_QUEUE, RelocateReason::REBALANCE_WRITE);
		self->output.send(rs);
		TraceEvent("SendRelocateToDDQueue", self->distributorId)
		    .detail("ServerPrimary", req.primary)
		    .detail("ServerTeam", selectedTeam.servers)
		    .detail("KeyBegin", rs.keys.begin)
		    .detail("KeyEnd", rs.keys.end)
		    .detail("Priority", rs.priority);
	}
	return;
}

DataDistributionTracker::DataDistributionTracker(DataDistributionTrackerInitParams const& params)
  : IDDShardTracker(), db(params.db), distributorId(params.distributorId), shards(params.shards), actors(false),
    systemSizeEstimate(0), dbSizeEstimate(new AsyncVar<int64_t>()), maxShardSize(new AsyncVar<Optional<int64_t>>()),
    output(params.output), shardsAffectedByTeamFailure(params.shardsAffectedByTeamFailure),
    physicalShardCollection(params.physicalShardCollection), bulkLoadTaskCollection(params.bulkLoadTaskCollection),
    readyToStart(params.readyToStart), anyZeroHealthyTeams(params.anyZeroHealthyTeams),
    trackerCancelled(params.trackerCancelled), ddTenantCache(params.ddTenantCache),
    usableRegions(params.usableRegions) {}

DataDistributionTracker::~DataDistributionTracker() {
	if (trackerCancelled) {
		*trackerCancelled = true;
	}
	// Cancel all actors so they aren't waiting on sizeChanged broken promise
	actors.clear(false);
}

struct DataDistributionTrackerImpl {
	ACTOR static Future<Void> run(DataDistributionTracker* self, Reference<InitialDataDistribution> initData) {
		state Future<Void> loggingTrigger = Void();
		state Future<Void> readHotDetect = readHotDetector(self);
		state Reference<EventCacheHolder> ddTrackerStatsEventHolder = makeReference<EventCacheHolder>("DDTrackerStats");

		try {
			wait(trackInitialShards(self, initData));
			initData.clear(); // Release reference count.

			state PromiseStream<TenantCacheTenantCreated> tenantCreationSignal;
			if (SERVER_KNOBS->DD_TENANT_AWARENESS_ENABLED) {
				ASSERT(self->ddTenantCache.present());
				tenantCreationSignal = self->ddTenantCache.get()->tenantCreationSignal;
			}

			loop choose {
				when(Promise<int64_t> req = waitNext(self->averageShardBytes)) {
					req.send(self->getAverageShardBytes());
				}
				when(wait(loggingTrigger)) {
					TraceEvent("DDTrackerStats", self->distributorId)
					    .detail("Shards", self->shards->size())
					    .detail("TotalSizeBytes", self->dbSizeEstimate->get())
					    .detail("SystemSizeBytes", self->systemSizeEstimate)
					    .trackLatest(ddTrackerStatsEventHolder->trackingKey);

					loggingTrigger = delay(SERVER_KNOBS->DATA_DISTRIBUTION_LOGGING_INTERVAL, TaskPriority::FlushTrace);
				}
				when(GetMetricsRequest req = waitNext(self->getShardMetrics)) {
					self->actors.add(fetchShardMetrics(self, req));
				}
				when(GetTopKMetricsRequest req = waitNext(self->getTopKMetrics)) {
					self->actors.add(fetchTopKShardMetrics(self, req));
				}
				when(GetMetricsListRequest req = waitNext(self->getShardMetricsList)) {
					self->actors.add(fetchShardMetricsList(self, req));
				}
				when(RebalanceStorageQueueRequest req = waitNext(self->triggerStorageQueueRebalance)) {
					triggerStorageQueueRebalance(self, req);
				}
				when(BulkLoadShardRequest req = waitNext(self->triggerShardBulkLoading)) {
					createShardToBulkLoad(self, req.bulkLoadState);
				}
				when(wait(self->actors.getResult())) {}
				when(TenantCacheTenantCreated newTenant = waitNext(tenantCreationSignal.getFuture())) {
					self->actors.add(tenantCreationHandling(self, newTenant));
				}
				when(KeyRange req = waitNext(self->shardsAffectedByTeamFailure->restartShardTracker.getFuture())) {
					restartShardTrackers(self, req);
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise) {
				TraceEvent(SevError, "DataDistributionTrackerError", self->distributorId)
				    .error(e); // FIXME: get rid of broken_promise
			} else {
				TraceEvent(SevWarn, "DataDistributionTrackerError", self->distributorId).error(e);
			}
			throw e;
		}
	}
};

Future<Void> DataDistributionTracker::run(
    Reference<DataDistributionTracker> self,
    const Reference<InitialDataDistribution>& initData,
    const FutureStream<GetMetricsRequest>& getShardMetrics,
    const FutureStream<GetTopKMetricsRequest>& getTopKMetrics,
    const FutureStream<GetMetricsListRequest>& getShardMetricsList,
    const FutureStream<Promise<int64_t>>& getAverageShardBytes,
    const FutureStream<RebalanceStorageQueueRequest>& triggerStorageQueueRebalance,
    const FutureStream<BulkLoadShardRequest>& triggerShardBulkLoading) {
	self->getShardMetrics = getShardMetrics;
	self->getTopKMetrics = getTopKMetrics;
	self->getShardMetricsList = getShardMetricsList;
	self->averageShardBytes = getAverageShardBytes;
	self->triggerStorageQueueRebalance = triggerStorageQueueRebalance;
	self->triggerShardBulkLoading = triggerShardBulkLoading;
	self->userRangeConfig = initData->userRangeConfig;
	self->bulkLoadEnabled = bulkLoadIsEnabled(initData->bulkLoadMode);
	return holdWhile(self, DataDistributionTrackerImpl::run(self.getPtr(), initData));
}

// Tracks storage metrics for `keys` and updates `physicalShardStats` which is the stats for the physical shard owning
// this key range. This function is similar to `trackShardMetrics()` and altered for physical shard. This meant to be
// temporary. Eventually, we want a new interface to track physical shard metrics more efficiently.
ACTOR Future<Void> trackKeyRangeInPhysicalShardMetrics(
    Reference<IDDTxnProcessor> db,
    KeyRange keys,
    Reference<AsyncVar<Optional<ShardMetrics>>> shardMetrics,
    Reference<AsyncVar<Optional<StorageMetrics>>> physicalShardStats) {
	state BandwidthStatus bandwidthStatus =
	    shardMetrics->get().present() ? getBandwidthStatus(shardMetrics->get().get().metrics) : BandwidthStatusNormal;
	state double lastLowBandwidthStartTime =
	    shardMetrics->get().present() ? shardMetrics->get().get().lastLowBandwidthStartTime : now();
	state int shardCount = shardMetrics->get().present() ? shardMetrics->get().get().shardCount : 1;
	wait(delay(0, TaskPriority::DataDistribution));

	/*TraceEvent("trackKeyRangeInPhysicalShardMetricsStarting")
	    .detail("Keys", keys)
	    .detail("TrackedBytesInitiallyPresent", shardMetrics->get().present())
	    .detail("StartingMetrics", shardMetrics->get().present() ? shardMetrics->get().get().metrics.bytes : 0)
	    .detail("StartingMerges", shardMetrics->get().present() ? shardMetrics->get().get().merges : 0);*/

	loop {
		state ShardSizeBounds bounds;
		bool readHotShard;
		std::tie(bounds, readHotShard) = calculateShardSizeBounds(keys, shardMetrics, bandwidthStatus);

		loop {
			// metrics.second is the number of key-ranges (i.e., shards) in the 'keys' key-range
			std::pair<Optional<StorageMetrics>, int> metrics =
			    wait(db->waitStorageMetrics(keys,
			                                bounds.min,
			                                bounds.max,
			                                bounds.permittedError,
			                                CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT,
			                                shardCount));
			if (metrics.first.present()) {
				BandwidthStatus newBandwidthStatus = getBandwidthStatus(metrics.first.get());
				if (newBandwidthStatus == BandwidthStatusLow && bandwidthStatus != BandwidthStatusLow) {
					lastLowBandwidthStartTime = now();
				}
				bandwidthStatus = newBandwidthStatus;

				// Update current physical shard aggregated stats;
				if (!physicalShardStats->get().present()) {
					physicalShardStats->set(metrics.first.get());
				} else {
					if (!shardMetrics->get().present()) {
						// We collect key range stats for the first time.
						physicalShardStats->set(physicalShardStats->get().get() + metrics.first.get());
					} else {
						physicalShardStats->set(physicalShardStats->get().get() - shardMetrics->get().get().metrics +
						                        metrics.first.get());
					}
				}

				shardMetrics->set(ShardMetrics(metrics.first.get(), lastLowBandwidthStartTime, shardCount));
				break;
			} else {
				shardCount = metrics.second;
				if (shardMetrics->get().present()) {
					auto newShardMetrics = shardMetrics->get().get();
					newShardMetrics.shardCount = shardCount;
					shardMetrics->set(newShardMetrics);
				}
			}
		}
	}
}

void PhysicalShardCollection::PhysicalShard::insertNewRangeData(const KeyRange& newRange) {
	RangeData data;
	data.stats = makeReference<AsyncVar<Optional<ShardMetrics>>>();
	data.trackMetrics = trackKeyRangeInPhysicalShardMetrics(txnProcessor, newRange, data.stats, stats);
	auto it = rangeData.emplace(newRange, data);
	ASSERT(it.second);
}

void PhysicalShardCollection::PhysicalShard::addRange(const KeyRange& newRange) {
	if (g_network->isSimulated()) {
		// Test that new range must not overlap with any existing range in this shard.
		for (const auto& [range, data] : rangeData) {
			ASSERT(!range.intersects(newRange));
		}
	}

	insertNewRangeData(newRange);
}

void PhysicalShardCollection::PhysicalShard::removeRange(const KeyRange& outRange) {
	std::vector<KeyRangeRef> updateRanges;
	for (auto& [range, data] : rangeData) {
		if (range.intersects(outRange)) {
			updateRanges.push_back(range);
		}
	}

	for (auto& range : updateRanges) {
		std::vector<KeyRangeRef> remainingRanges = range - outRange;
		for (auto& r : remainingRanges) {
			ASSERT(r != range);
			insertNewRangeData(r);
		}
		// Must erase last since `remainingRanges` uses data in `range`.
		rangeData.erase(range);
	}
}

// Decide whether a physical shard is available at the moment when the func is calling
PhysicalShardAvailable PhysicalShardCollection::checkPhysicalShardAvailable(uint64_t physicalShardID,
                                                                            StorageMetrics const& moveInMetrics) {
	ASSERT(physicalShardID != UID().first() && physicalShardID != anonymousShardId.first());
	ASSERT(physicalShardInstances.contains(physicalShardID));
	if (physicalShardInstances[physicalShardID].metrics.bytes + moveInMetrics.bytes >
	    SERVER_KNOBS->MAX_PHYSICAL_SHARD_BYTES) {
		return PhysicalShardAvailable::False;
	}
	return PhysicalShardAvailable::True;
}

std::string PhysicalShardCollection::convertIDsToString(std::set<uint64_t> ids) {
	std::string r = "";
	for (auto id : ids) {
		r = r + std::to_string(id) + " ";
	}
	return r;
}

void PhysicalShardCollection::updateTeamPhysicalShardIDsMap(uint64_t inputPhysicalShardID,
                                                            std::vector<ShardsAffectedByTeamFailure::Team> inputTeams,
                                                            uint64_t debugID) {
	ASSERT(inputTeams.size() <= 2);
	ASSERT(inputPhysicalShardID != anonymousShardId.first() && inputPhysicalShardID != UID().first());
	for (auto inputTeam : inputTeams) {
		if (!teamPhysicalShardIDs.contains(inputTeam)) {
			std::set<uint64_t> physicalShardIDSet;
			physicalShardIDSet.insert(inputPhysicalShardID);
			teamPhysicalShardIDs.insert(std::make_pair(inputTeam, physicalShardIDSet));
		} else {
			teamPhysicalShardIDs[inputTeam].insert(inputPhysicalShardID);
		}
	}
	return;
}

void PhysicalShardCollection::insertPhysicalShardToCollection(uint64_t physicalShardID,
                                                              StorageMetrics const& metrics,
                                                              std::vector<ShardsAffectedByTeamFailure::Team> teams,
                                                              uint64_t debugID,
                                                              PhysicalShardCreationTime whenCreated) {
	ASSERT(physicalShardID != anonymousShardId.first() && physicalShardID != UID().first());
	ASSERT(!physicalShardInstances.contains(physicalShardID));
	physicalShardInstances.insert(
	    std::make_pair(physicalShardID, PhysicalShard(txnProcessor, physicalShardID, metrics, teams, whenCreated)));
	return;
}

// This method maintains the consistency between keyRangePhysicalShardIDMap and the RangeData in physicalShardInstances.
// They are all updated in this method.
void PhysicalShardCollection::updatekeyRangePhysicalShardIDMap(KeyRange keyRange,
                                                               uint64_t physicalShardID,
                                                               uint64_t debugID) {
	ASSERT(physicalShardID != UID().first());
	auto ranges = keyRangePhysicalShardIDMap.intersectingRanges(keyRange);
	std::set<uint64_t> physicalShardIDSet;

	// If there are any existing physical shards own `keyRange`, remove the overlapping ranges from existing physical
	// shards.
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		uint64_t shardID = it->value();
		if (shardID == UID().first()) {
			continue;
		}
		ASSERT(physicalShardInstances.find(shardID) != physicalShardInstances.end());
		physicalShardInstances[shardID].removeRange(keyRange);
	}

	// Insert `keyRange` to the new physical shard.
	keyRangePhysicalShardIDMap.insert(keyRange, physicalShardID);
	ASSERT(physicalShardInstances.find(physicalShardID) != physicalShardInstances.end());
	physicalShardInstances[physicalShardID].addRange(keyRange);

	// KeyRange to physical shard mapping consistency sanity check.
	checkKeyRangePhysicalShardMapping();
}

void PhysicalShardCollection::checkKeyRangePhysicalShardMapping() {
	if (!g_network->isSimulated()) {
		return;
	}

	// Check the invariant that keyRangePhysicalShardIDMap and physicalShardInstances should be consistent.
	KeyRangeMap<uint64_t>::Ranges keyRangePhysicalShardIDRanges = keyRangePhysicalShardIDMap.ranges();
	KeyRangeMap<uint64_t>::iterator it = keyRangePhysicalShardIDRanges.begin();
	for (; it != keyRangePhysicalShardIDRanges.end(); ++it) {
		uint64_t shardID = it->value();
		if (shardID == UID().first()) {
			continue;
		}
		auto keyRangePiece = KeyRangeRef(it->range().begin, it->range().end);
		ASSERT(physicalShardInstances.find(shardID) != physicalShardInstances.end());
		bool exist = false;
		for (const auto& [range, data] : physicalShardInstances[shardID].rangeData) {
			if (range == keyRangePiece) {
				exist = true;
				break;
			}
		}
		ASSERT(exist);
	}

	for (auto& [shardID, physicalShard] : physicalShardInstances) {
		for (auto& [range, data] : physicalShard.rangeData) {
			ASSERT(keyRangePhysicalShardIDMap[range.begin] == shardID);
		}
	}
}

// At beginning of the transition from the initial state without physical shard notion
// to the physical shard aware state, the physicalShard set only contains one element which is anonymousShardId[0]
// After a period in the transition, the physicalShard set of the team contains some meaningful physicalShardIDs
Optional<uint64_t> PhysicalShardCollection::trySelectAvailablePhysicalShardFor(
    ShardsAffectedByTeamFailure::Team team,
    StorageMetrics const& moveInMetrics,
    const std::unordered_set<uint64_t>& excludedPhysicalShards,
    uint64_t debugID) {
	ASSERT(team.servers.size() > 0);
	// Case: The team is not tracked in the mapping (teamPhysicalShardIDs)
	if (!teamPhysicalShardIDs.contains(team)) {
		return Optional<uint64_t>();
	}
	ASSERT(teamPhysicalShardIDs[team].size() >= 1);
	// Case: The team is tracked in the mapping and the system already has physical shard notion
	// 		and the number of physicalShard is large
	std::vector<uint64_t> availablePhysicalShardIDs;
	for (auto physicalShardID : teamPhysicalShardIDs[team]) {
		if (physicalShardID == anonymousShardId.first() || physicalShardID == UID().first()) {
			ASSERT(false);
		}
		ASSERT(physicalShardInstances.contains(physicalShardID));
		/*TraceEvent("TryGetPhysicalShardIDCandidates")
		    .detail("PhysicalShardID", physicalShardID)
		    .detail("Bytes", physicalShardInstances[physicalShardID].metrics.bytes)
		    .detail("BelongTeam", team.toString())
		    .detail("DebugID", debugID);*/
		if (excludedPhysicalShards.find(physicalShardID) != excludedPhysicalShards.end()) {
			continue;
		}
		if (!checkPhysicalShardAvailable(physicalShardID, moveInMetrics)) {
			continue;
		}
		availablePhysicalShardIDs.push_back(physicalShardID);
	}
	if (availablePhysicalShardIDs.size() == 0) {
		/*TraceEvent("TryGetPhysicalShardIDResultFailed")
		    .detail("Reason", "no valid physicalShard")
		    .detail("MoveInBytes", moveInMetrics.bytes)
		    .detail("MaxPhysicalShardBytes", SERVER_KNOBS->MAX_PHYSICAL_SHARD_BYTES)
		    .detail("DebugID", debugID);*/
		return Optional<uint64_t>();
	}
	return deterministicRandom()->randomChoice(availablePhysicalShardIDs);
}

uint64_t PhysicalShardCollection::generateNewPhysicalShardID(uint64_t debugID) {
	uint64_t physicalShardID = UID().first();
	int stuckCount = 0;
	while (physicalShardID == UID().first() || physicalShardID == anonymousShardId.first()) {
		physicalShardID = deterministicRandom()->randomUInt64();
		stuckCount = stuckCount + 1;
		if (stuckCount > 50) {
			ASSERT(false);
		}
	}
	ASSERT(physicalShardID != UID().first() && physicalShardID != anonymousShardId.first());
	//TraceEvent("GenerateNewPhysicalShardID").detail("PhysicalShardID", physicalShardID).detail("DebugID", debugID);
	return physicalShardID;
}

void PhysicalShardCollection::reduceMetricsForMoveOut(uint64_t physicalShardID, StorageMetrics const& moveOutMetrics) {
	ASSERT(physicalShardInstances.contains(physicalShardID));
	ASSERT(physicalShardID != UID().first() && physicalShardID != anonymousShardId.first());
	physicalShardInstances[physicalShardID].metrics = physicalShardInstances[physicalShardID].metrics - moveOutMetrics;
	return;
}

void PhysicalShardCollection::increaseMetricsForMoveIn(uint64_t physicalShardID, StorageMetrics const& moveInMetrics) {
	ASSERT(physicalShardInstances.contains(physicalShardID));
	ASSERT(physicalShardID != UID().first() && physicalShardID != anonymousShardId.first());
	physicalShardInstances[physicalShardID].metrics = physicalShardInstances[physicalShardID].metrics + moveInMetrics;
	return;
}

void PhysicalShardCollection::updatePhysicalShardMetricsByKeyRange(KeyRange keyRange,
                                                                   StorageMetrics const& newMetrics,
                                                                   StorageMetrics const& oldMetrics,
                                                                   bool initWithNewMetrics) {
	auto ranges = keyRangePhysicalShardIDMap.intersectingRanges(keyRange);
	std::set<uint64_t> physicalShardIDSet;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		physicalShardIDSet.insert(it->value());
	}
	StorageMetrics delta;
	if (initWithNewMetrics) {
		delta = newMetrics;
	} else {
		delta = newMetrics - oldMetrics;
	}
	for (auto physicalShardID : physicalShardIDSet) {
		ASSERT(physicalShardID != UID().first());
		if (physicalShardID == anonymousShardId.first()) {
			continue; // we ignore anonymousShard when updating physicalShard metrics
		}
		increaseMetricsForMoveIn(physicalShardID, (delta * (1.0 / physicalShardIDSet.size())));
	}
	return;
}

InAnonymousPhysicalShard PhysicalShardCollection::isInAnonymousPhysicalShard(KeyRange keyRange) {
	InAnonymousPhysicalShard res = InAnonymousPhysicalShard::True;
	auto ranges = keyRangePhysicalShardIDMap.intersectingRanges(keyRange);
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		uint64_t physicalShardID = it->value();
		if (physicalShardID != anonymousShardId.first()) {
			// res = false if exists a part of keyRange belongs to a non-anonymous physicalShard
			// exist a case where some keyRange of anonymousShard is decided to move
			// to a non-anonymous physicalShard but not completes
			res = InAnonymousPhysicalShard::False;
		}
	}
	return res;
}

// TODO: require optimize
// It is slow to go through the keyRangePhysicalShardIDRanges for each time
// Do we need a D/S to store the keyRange for each physicalShard?
PhysicalShardHasMoreThanKeyRange PhysicalShardCollection::whetherPhysicalShardHasMoreThanKeyRange(
    uint64_t physicalShardID,
    KeyRange keyRange) {
	KeyRangeMap<uint64_t>::Ranges keyRangePhysicalShardIDRanges = keyRangePhysicalShardIDMap.ranges();
	KeyRangeMap<uint64_t>::iterator it = keyRangePhysicalShardIDRanges.begin();
	for (; it != keyRangePhysicalShardIDRanges.end(); ++it) {
		if (it->value() != physicalShardID) {
			continue;
		}
		auto keyRangePiece = KeyRangeRef(it->range().begin, it->range().end);
		if (!keyRange.intersects(keyRangePiece)) {
			return PhysicalShardHasMoreThanKeyRange::True;
		}
		// if keyRange and keyRangePiece have intersection
		if (!keyRange.contains(keyRangePiece)) {
			return PhysicalShardHasMoreThanKeyRange::True;
		}
	}
	return PhysicalShardHasMoreThanKeyRange::False;
}

InOverSizePhysicalShard PhysicalShardCollection::isInOverSizePhysicalShard(KeyRange keyRange) {
	auto ranges = keyRangePhysicalShardIDMap.intersectingRanges(keyRange);
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		uint64_t physicalShardID = it->value();
		ASSERT(physicalShardID != UID().first());
		if (physicalShardID == anonymousShardId.first()) {
			continue;
		}
		if (checkPhysicalShardAvailable(physicalShardID, StorageMetrics())) {
			continue;
		}
		if (!whetherPhysicalShardHasMoreThanKeyRange(physicalShardID, keyRange)) {
			continue;
		}
		return InOverSizePhysicalShard::True;
	}
	return InOverSizePhysicalShard::False;
}

// May return a problematic remote team
std::pair<Optional<ShardsAffectedByTeamFailure::Team>, bool> PhysicalShardCollection::tryGetAvailableRemoteTeamWith(
    uint64_t inputPhysicalShardID,
    StorageMetrics const& moveInMetrics,
    uint64_t debugID) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	ASSERT(inputPhysicalShardID != anonymousShardId.first() && inputPhysicalShardID != UID().first());
	if (!physicalShardInstances.contains(inputPhysicalShardID)) {
		return { Optional<ShardsAffectedByTeamFailure::Team>(), true };
	}
	if (!checkPhysicalShardAvailable(inputPhysicalShardID, moveInMetrics)) {
		return { Optional<ShardsAffectedByTeamFailure::Team>(), false };
	}
	for (auto team : physicalShardInstances[inputPhysicalShardID].teams) {
		if (team.primary == false) {
			/*TraceEvent("TryGetRemoteTeamWith")
			    .detail("PhysicalShardID", inputPhysicalShardID)
			    .detail("Team", team.toString())
			    .detail("TeamSize", team.servers.size())
			    .detail("PhysicalShardsOfTeam", convertIDsToString(teamPhysicalShardIDs[team]))
			    .detail("DebugID", debugID);*/
			return { team, true };
		}
	}
	// In this case, the physical shard may not be populated in the remote region yet, e.g., we are making a
	// configuration change to turn a single region cluster into HA mode.
	return { Optional<ShardsAffectedByTeamFailure::Team>(), true };
}

// The update of PhysicalShardToTeams, Collection, keyRangePhysicalShardIDMap should be atomic
void PhysicalShardCollection::initPhysicalShardCollection(KeyRange keys,
                                                          std::vector<ShardsAffectedByTeamFailure::Team> selectedTeams,
                                                          uint64_t physicalShardID,
                                                          uint64_t debugID) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	ASSERT(physicalShardID != UID().first());
	if (physicalShardID != anonymousShardId.first()) {
		updateTeamPhysicalShardIDsMap(physicalShardID, selectedTeams, debugID);
		if (!physicalShardInstances.contains(physicalShardID)) {
			insertPhysicalShardToCollection(
			    physicalShardID, StorageMetrics(), selectedTeams, debugID, PhysicalShardCreationTime::DDInit);
		} else {
			// This assertion will be broken if we enable the optimization of data move traffic between DCs
			ASSERT(physicalShardInstances[physicalShardID].teams == selectedTeams);
		}
	} else {
		// If any physicalShard restored when DD init is the anonymousShard,
		// Then DD enters Transition state where DD graduatelly moves Shard (or KeyRange)
		// out of the anonymousShard
		setTransitionCheck();
	}
	updatekeyRangePhysicalShardIDMap(keys, physicalShardID, debugID);
	return;
}

// The update of PhysicalShardToTeams, Collection, keyRangePhysicalShardIDMap should be atomic
void PhysicalShardCollection::updatePhysicalShardCollection(
    KeyRange keys,
    bool isRestore,
    std::vector<ShardsAffectedByTeamFailure::Team> selectedTeams,
    uint64_t physicalShardID,
    const StorageMetrics& metrics,
    uint64_t debugID) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	ASSERT(physicalShardID != UID().first());
	/*TraceEvent e("UpdatePhysicalShard");
	e.detail("DebugID", debugID);
	e.detail("KeyRange", keys);
	e.detail("IsRestore", isRestore);*/
	// When updates metrics in physicalShard collection, we assume:
	// It is impossible to move a keyRange from anonymousShard to a valid physicalShard
	// Thus, we ignore anonymousShard when updating metrics
	if (physicalShardID != anonymousShardId.first()) {
		updateTeamPhysicalShardIDsMap(physicalShardID, selectedTeams, debugID);
		// Update physicalShardInstances
		// Add the metrics to in-physicalShard
		// e.detail("PhysicalShardIDIn", physicalShardID);
		if (!physicalShardInstances.contains(physicalShardID)) {
			// e.detail("Op", "Insert");
			insertPhysicalShardToCollection(
			    physicalShardID, metrics, selectedTeams, debugID, PhysicalShardCreationTime::DDRelocator);
		} else {
			// e.detail("Op", "Update");
			//  This assertion is true since we disable the optimization of data move traffic between DCs
			ASSERT(physicalShardInstances[physicalShardID].teams == selectedTeams);
			increaseMetricsForMoveIn(physicalShardID, metrics);
		}
	}
	// Minus the metrics from the existing (multiple) out-physicalShard(s)
	auto ranges = keyRangePhysicalShardIDMap.intersectingRanges(keys);
	std::set<uint64_t> physicalShardIDSet;
	for (auto it = ranges.begin(); it != ranges.end(); ++it) {
		physicalShardIDSet.insert(it->value());
	}
	/*std::string physicalShardIDOut = "";
	for (auto id : physicalShardIDSet) {
	    physicalShardIDOut = physicalShardIDOut + std::to_string(id) + " ";
	}*/
	// e.detail("PhysicalShardIDOut", physicalShardIDOut);
	for (auto physicalShardID : physicalShardIDSet) { // imprecise: evenly move out bytes
		if (physicalShardID == anonymousShardId.first()) {
			continue; // we ignore anonymousShard when updating physicalShard metrics
		}
		StorageMetrics toReduceMetrics = metrics * (1.0 / physicalShardIDSet.size());
		reduceMetricsForMoveOut(physicalShardID, toReduceMetrics);
	}
	// keyRangePhysicalShardIDMap must be update after updating the metrics of physicalShardInstances
	updatekeyRangePhysicalShardIDMap(keys, physicalShardID, debugID);
	return;
}

// return false if no need to move keyRange out of current physical shard
MoveKeyRangeOutPhysicalShard PhysicalShardCollection::trackPhysicalShard(KeyRange keyRange,
                                                                         StorageMetrics const& newMetrics,
                                                                         StorageMetrics const& oldMetrics,
                                                                         bool initWithNewMetrics) {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	updatePhysicalShardMetricsByKeyRange(keyRange, newMetrics, oldMetrics, initWithNewMetrics);
	if (requireTransitionCheck() &&
	    now() - lastTransitionStartTime > SERVER_KNOBS->ANONYMOUS_PHYSICAL_SHARD_TRANSITION_TIME) {
		if (isInAnonymousPhysicalShard(keyRange)) {
			// Currently, whenever a shard updates metrics, it checks whether is in AnonymousPhysicalShard
			// If yes, and if the shard has been created for long time, then triggers a data move on the shard.
			resetLastTransitionStartTime();
			TraceEvent("PhysicalShardTiggerTransitionMove")
			    .detail("KeyRange", keyRange)
			    .detail("TransitionCoolDownTime", SERVER_KNOBS->ANONYMOUS_PHYSICAL_SHARD_TRANSITION_TIME);
			return MoveKeyRangeOutPhysicalShard::True;
		}
	}
	if (isInOverSizePhysicalShard(keyRange)) {
		return MoveKeyRangeOutPhysicalShard::True;
	}
	return MoveKeyRangeOutPhysicalShard::False;
}

// The update of PhysicalShardToTeams, PhysicalShardInstances, KeyRangePhysicalShardIDMap should be atomic
void PhysicalShardCollection::cleanUpPhysicalShardCollection() {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	std::set<uint64_t> physicalShardsInUse;
	std::map<uint64_t, StorageMetrics> metricsReplies;
	KeyRangeMap<uint64_t>::Ranges keyRangePhysicalShardIDRanges = keyRangePhysicalShardIDMap.ranges();
	KeyRangeMap<uint64_t>::iterator it = keyRangePhysicalShardIDRanges.begin();
	// Assume that once a physical shard is disappear in keyRangePhysicalShardIDMap,
	// the physical shard (with the deleted id) should be deprecated.
	// This function aims at clean up those deprecated physical shards in PhysicalShardCollection
	// This function collects the physicalShard usage info from KeyRangePhysicalShardIDMap,
	// then based on the info to update PhysicalShardToTeams and PhysicalShardInstances

	// keyRangePhysicalShardIDMap indicates which physicalShard actually has data
	// Step 1: Clear unused physicalShard in physicalShardInstances based on keyRangePhysicalShardIDMap
	for (; it != keyRangePhysicalShardIDRanges.end(); ++it) {
		uint64_t physicalShardID = it->value();
		if (physicalShardID == anonymousShardId.first()) {
			continue;
		}
		physicalShardsInUse.insert(physicalShardID);
	}
	for (auto it = physicalShardInstances.begin(); it != physicalShardInstances.end();) {
		uint64_t physicalShardID = it->first;
		ASSERT(physicalShardInstances.contains(physicalShardID));
		if (!physicalShardsInUse.contains(physicalShardID)) {
			/*TraceEvent("PhysicalShardisEmpty")
			    .detail("PhysicalShard", physicalShardID)
			    .detail("RemainBytes", physicalShardInstances[physicalShardID].metrics.bytes);*/
			// "RemainBytes" indicates the deviation of current physical shard metric update
			it = physicalShardInstances.erase(it);
		} else {
			it++;
		}
	}
	// Step 2: Clean up teamPhysicalShardIDs
	std::set<ShardsAffectedByTeamFailure::Team> toRemoveTeams;
	for (auto [team, _] : teamPhysicalShardIDs) {
		for (auto it = teamPhysicalShardIDs[team].begin(); it != teamPhysicalShardIDs[team].end();) {
			uint64_t physicalShardID = *it;
			if (!physicalShardInstances.contains(physicalShardID)) {
				// physicalShardID has been removed from physicalShardInstances (see step 1)
				// So, remove the physicalShard from teamPhysicalShardID[team]
				it = teamPhysicalShardIDs[team].erase(it);
			} else {
				it++;
			}
		}
		if (teamPhysicalShardIDs[team].size() == 0) {
			// If a team has no physicalShard, remove the team from teamPhysicalShardID
			toRemoveTeams.insert(team);
		}
	}
	for (auto team : toRemoveTeams) {
		teamPhysicalShardIDs.erase(team);
	}
}

void PhysicalShardCollection::logPhysicalShardCollection() {
	ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	ASSERT(SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD);
	// Step 1: Logging non-empty physicalShard
	for (auto [physicalShardID, physicalShard] : physicalShardInstances) {
		ASSERT(physicalShardID == physicalShard.id);
		TraceEvent e("PhysicalShardStatus");
		e.detail("PhysicalShardID", physicalShardID);
		e.detail("TotalBytes", physicalShard.metrics.bytes);
	}
	// Step 2: Logging TeamPhysicalShardStatus
	for (auto [team, physicalShardIDs] : teamPhysicalShardIDs) {
		TraceEvent e("TeamPhysicalShardStatus");
		e.detail("Team", team.toString());
		// std::string metricsStr = "";
		int64_t counter = 0;
		int64_t totalBytes = 0;
		int64_t maxPhysicalShardBytes = -1;
		int64_t minPhysicalShardBytes = StorageMetrics::infinity;
		uint64_t maxPhysicalShardID = 0;
		uint64_t minPhysicalShardID = 0;
		for (auto physicalShardID : physicalShardIDs) {
			ASSERT(physicalShardInstances.contains(physicalShardID));
			uint64_t id = physicalShardInstances[physicalShardID].id;
			int64_t bytes = physicalShardInstances[physicalShardID].metrics.bytes;
			if (bytes > maxPhysicalShardBytes) {
				maxPhysicalShardBytes = bytes;
				maxPhysicalShardID = id;
			}
			if (bytes < minPhysicalShardBytes) {
				minPhysicalShardBytes = bytes;
				minPhysicalShardID = id;
			}
			totalBytes = totalBytes + bytes;
			/* metricsStr = metricsStr + std::to_string(id) + ":" + std::to_string(bytes);
			if (counter < physicalShardIDs.size() - 1) {
			    metricsStr = metricsStr + ",";
			} */
			counter = counter + 1;
		}
		// e.detail("Metrics", metricsStr);
		e.detail("TotalBytes", totalBytes);
		e.detail("NumPhysicalShards", counter);
		e.detail("MaxPhysicalShard", std::to_string(maxPhysicalShardID) + ":" + std::to_string(maxPhysicalShardBytes));
		e.detail("MinPhysicalShard", std::to_string(minPhysicalShardID) + ":" + std::to_string(minPhysicalShardBytes));
	}
	// Step 3: Logging StorageServerPhysicalShardStatus
	std::map<UID, std::map<uint64_t, int64_t>> storageServerPhysicalShardStatus;
	for (auto [team, _] : teamPhysicalShardIDs) {
		for (auto ssid : team.servers) {
			for (auto it = teamPhysicalShardIDs[team].begin(); it != teamPhysicalShardIDs[team].end();) {
				uint64_t physicalShardID = *it;
				if (storageServerPhysicalShardStatus.contains(ssid)) {
					if (!storageServerPhysicalShardStatus[ssid].contains(physicalShardID)) {
						ASSERT(physicalShardInstances.contains(physicalShardID));
						storageServerPhysicalShardStatus[ssid].insert(
						    std::make_pair(physicalShardID, physicalShardInstances[physicalShardID].metrics.bytes));
					}
				} else {
					ASSERT(physicalShardInstances.contains(physicalShardID));
					std::map<uint64_t, int64_t> tmp;
					tmp.insert(std::make_pair(physicalShardID, physicalShardInstances[physicalShardID].metrics.bytes));
					storageServerPhysicalShardStatus.insert(std::make_pair(ssid, tmp));
				}
				it++;
			}
		}
	}
	for (auto [serverID, physicalShardMetrics] : storageServerPhysicalShardStatus) {
		TraceEvent e("ServerPhysicalShardStatus");
		e.detail("Server", serverID);
		e.detail("NumPhysicalShards", physicalShardMetrics.size());
		int64_t totalBytes = 0;
		int64_t maxPhysicalShardBytes = -1;
		int64_t minPhysicalShardBytes = StorageMetrics::infinity;
		uint64_t maxPhysicalShardID = 0;
		uint64_t minPhysicalShardID = 0;
		// std::string metricsStr = "";
		// int64_t counter = 0;
		for (auto [physicalShardID, bytes] : physicalShardMetrics) {
			totalBytes = totalBytes + bytes;
			if (bytes > maxPhysicalShardBytes) {
				maxPhysicalShardBytes = bytes;
				maxPhysicalShardID = physicalShardID;
			}
			if (bytes < minPhysicalShardBytes) {
				minPhysicalShardBytes = bytes;
				minPhysicalShardID = physicalShardID;
			}
			/* metricsStr = metricsStr + std::to_string(physicalShardID) + ":" + std::to_string(bytes);
			if (counter < physicalShardMetrics.size() - 1) {
			        metricsStr = metricsStr + ",";
			}
			counter = counter + 1; */
		}
		e.detail("TotalBytes", totalBytes);
		e.detail("MaxPhysicalShard", std::to_string(maxPhysicalShardID) + ":" + std::to_string(maxPhysicalShardBytes));
		e.detail("MinPhysicalShard", std::to_string(minPhysicalShardID) + ":" + std::to_string(minPhysicalShardBytes));
	}
}

bool PhysicalShardCollection::physicalShardExists(uint64_t physicalShardID) {
	return physicalShardInstances.find(physicalShardID) != physicalShardInstances.end();
}

// FIXME: complete this test with non-empty range
TEST_CASE("/DataDistributor/Tracker/FetchTopK") {
	state DataDistributionTracker self;
	state std::vector<KeyRange> ranges;
	// for (int i = 1; i <= 10; i += 2) {
	//     ranges.emplace_back(KeyRangeRef(doubleToTestKey(i), doubleToTestKey(i + 2)));
	//     std::cout << "add range: " << ranges.back().begin.toString() << "\n";
	// }
	state GetTopKMetricsRequest req(ranges, 3, 1000, 100000);

	// double targetDensities[10] = { 2, 1, 3, 5, 4, 10, 6, 8, 7, 0 };

	wait(fetchTopKShardMetrics(&self, req));
	auto& reply = req.reply.getFuture().get();
	ASSERT(reply.shardMetrics.empty());
	ASSERT(reply.maxReadLoad == -1);
	ASSERT(reply.minReadLoad == -1);

	return Void();
}
