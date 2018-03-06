/*
 * DataDistributionTracker.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/SystemData.h"
#include "DataDistribution.h"
#include "Knobs.h"
#include "fdbclient/DatabaseContext.h"
#include "flow/ActorCollection.h"

enum BandwidthStatus {
	BandwidthStatusLow,
	BandwidthStatusNormal,
	BandwidthStatusHigh
};

BandwidthStatus getBandwidthStatus( StorageMetrics const& metrics ) {
	if( metrics.bytesPerKSecond > SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC )
		return BandwidthStatusHigh;
	else if( metrics.bytesPerKSecond < SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC )
		return BandwidthStatusLow;

	return BandwidthStatusNormal;
}

ACTOR Future<Void> updateMaxShardSize( Standalone<StringRef> dbName, Reference<AsyncVar<int64_t>> dbSizeEstimate, Reference<AsyncVar<Optional<int64_t>>> maxShardSize ) {
	state int64_t lastDbSize = 0;
	state int64_t granularity = g_network->isSimulated() ?
		SERVER_KNOBS->DD_SHARD_SIZE_GRANULARITY_SIM : SERVER_KNOBS->DD_SHARD_SIZE_GRANULARITY;
	loop {
		auto sizeDelta = std::abs(dbSizeEstimate->get() - lastDbSize);
		if( sizeDelta > granularity || !maxShardSize->get().present() ) {
			auto v = getMaxShardSize( dbSizeEstimate->get() );
			maxShardSize->set( v );
			lastDbSize = dbSizeEstimate->get();
		}
		Void _ = wait( dbSizeEstimate->onChange() );
	}
}

struct ShardTrackedData {
	Future<Void> trackShard;
	Future<Void> trackBytes;
	Reference<AsyncVar<Optional<StorageMetrics>>> stats;
};

struct DataDistributionTracker {
	Database cx;
	UID masterId;
	KeyRangeMap< ShardTrackedData > shards;
	ActorCollection sizeChanges;

	Reference<AsyncVar<int64_t>> dbSizeEstimate;
	Reference<AsyncVar<Optional<int64_t>>> maxShardSize;
	Future<Void> maxShardSizeUpdater;

	// CapacityTracker
	PromiseStream<RelocateShard> output;

	Promise<Void> readyToStart;

	DataDistributionTracker(Database cx, UID masterId, Promise<Void> const& readyToStart, PromiseStream<RelocateShard> const& output)
		: cx(cx), masterId( masterId ), dbSizeEstimate( new AsyncVar<int64_t>() ),
			maxShardSize( new AsyncVar<Optional<int64_t>>() ),
			sizeChanges(false), readyToStart(readyToStart), output( output ) {}

	~DataDistributionTracker()
	{
		//Cancel all actors so they aren't waiting on sizeChanged broken promise
		sizeChanges.clear(false);
		shards.insert( allKeys, ShardTrackedData() );
	}
};

void restartShardTrackers(
	DataDistributionTracker* self,
	KeyRangeRef keys,
	Optional<StorageMetrics> startingSize = Optional<StorageMetrics>());

// Gets the permitted size and IO bounds for a shard. A shard that starts at allKeys.begin
//  (i.e. '') will have a permitted size of 0, since the database can contain no data.
ShardSizeBounds getShardSizeBounds(KeyRangeRef shard, int64_t maxShardSize) {
	ShardSizeBounds bounds;

	if(shard.begin >= keyServersKeys.begin) {
		bounds.max.bytes = SERVER_KNOBS->KEY_SERVER_SHARD_BYTES;
	} else {
		bounds.max.bytes = maxShardSize;
	}

	bounds.max.bytesPerKSecond = bounds.max.infinity;
	bounds.max.iosPerKSecond = bounds.max.infinity;

	//The first shard can have arbitrarily small size
	if(shard.begin == allKeys.begin) {
		bounds.min.bytes = 0;
	} else {
		bounds.min.bytes = maxShardSize / SERVER_KNOBS->SHARD_BYTES_RATIO;
	}

	bounds.min.bytesPerKSecond = 0;
	bounds.min.iosPerKSecond = 0;

	//The permitted error is 1/3 of the general-case minimum bytes (even in the special case where this is the last shard)
	bounds.permittedError.bytes = bounds.max.bytes / SERVER_KNOBS->SHARD_BYTES_RATIO / 3;
	bounds.permittedError.bytesPerKSecond = bounds.permittedError.infinity;
	bounds.permittedError.iosPerKSecond = bounds.permittedError.infinity;

	return bounds;
}

int64_t getMaxShardSize( double dbSizeEstimate ) {
	return std::min((SERVER_KNOBS->MIN_SHARD_BYTES + (int64_t)std::sqrt( dbSizeEstimate )*SERVER_KNOBS->SHARD_BYTES_PER_SQRT_BYTES) * SERVER_KNOBS->SHARD_BYTES_RATIO,
		(int64_t)SERVER_KNOBS->MAX_SHARD_BYTES);
}

ACTOR Future<Void> trackShardBytes(
		DataDistributionTracker* self,
		KeyRange keys,
		Reference<AsyncVar<Optional<StorageMetrics>>> shardSize,
		UID trackerID,
		bool addToSizeEstimate = true)
{
	state Transaction tr(self->cx);

	Void _ = wait( delay( 0, TaskDataDistribution ) );

	/*TraceEvent("TrackShardBytesStarting")
		.detail("TrackerID", trackerID)
		.detail("Keys", printable(keys))
		.detail("TrackedBytesInitiallyPresent", shardSize->get().present())
		.detail("StartingSize", shardSize->get().present() ? shardSize->get().get().metrics.bytes : 0)
		.detail("StartingMerges", shardSize->get().present() ? shardSize->get().get().merges : 0);*/

	try {
		loop {
			try {
				state ShardSizeBounds bounds;
				if( shardSize->get().present() ) {
					auto bytes = shardSize->get().get().bytes;
					auto bandwidthStatus = getBandwidthStatus( shardSize->get().get() );
					bounds.max.bytes = std::max( int64_t(bytes * 1.1), (int64_t)SERVER_KNOBS->MIN_SHARD_BYTES );
					bounds.min.bytes = std::min( int64_t(bytes * 0.9), std::max(int64_t(bytes - (SERVER_KNOBS->MIN_SHARD_BYTES * 0.1)), (int64_t)0) );
					bounds.permittedError.bytes = bytes * 0.1;
					if( bandwidthStatus == BandwidthStatusNormal ) {   // Not high or low
						bounds.max.bytesPerKSecond = SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC;
						bounds.min.bytesPerKSecond = SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC;
						bounds.permittedError.bytesPerKSecond = bounds.min.bytesPerKSecond / 4;
					} else if( bandwidthStatus == BandwidthStatusHigh ) { // > 10MB/sec for 100MB shard, proportionally lower for smaller shard, > 200KB/sec no matter what
						bounds.max.bytesPerKSecond = bounds.max.infinity;
						bounds.min.bytesPerKSecond = SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC;
						bounds.permittedError.bytesPerKSecond = bounds.min.bytesPerKSecond / 4;
					} else if( bandwidthStatus == BandwidthStatusLow ) {  // < 10KB/sec
						bounds.max.bytesPerKSecond = SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC;
						bounds.min.bytesPerKSecond = 0;
						bounds.permittedError.bytesPerKSecond = bounds.max.bytesPerKSecond / 4;
					} else
						ASSERT( false );

				} else {
					bounds.max.bytes = -1;
					bounds.min.bytes = -1;
					bounds.permittedError.bytes = -1;
					bounds.max.bytesPerKSecond = bounds.max.infinity;
					bounds.min.bytesPerKSecond = 0;
					bounds.permittedError.bytesPerKSecond = bounds.permittedError.infinity;
				}

				bounds.max.iosPerKSecond = bounds.max.infinity;
				bounds.min.iosPerKSecond = 0;
				bounds.permittedError.iosPerKSecond = bounds.permittedError.infinity;

				StorageMetrics metrics = wait( tr.waitStorageMetrics( keys, bounds.min, bounds.max, bounds.permittedError, CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT ) );

				/*TraceEvent("ShardSizeUpdate")
					.detail("Keys", printable(keys))
					.detail("UpdatedSize", metrics.metrics.bytes)
					.detail("Bandwidth", metrics.metrics.bytesPerKSecond)
					.detail("BandwithStatus", getBandwidthStatus(metrics))
					.detail("BytesLower", bounds.min.bytes)
					.detail("BytesUpper", bounds.max.bytes)
					.detail("BandwidthLower", bounds.min.bytesPerKSecond)
					.detail("BandwidthUpper", bounds.max.bytesPerKSecond)
					.detail("ShardSizePresent", shardSize->get().present())
					.detail("OldShardSize", shardSize->get().present() ? shardSize->get().get().metrics.bytes : 0 )
					.detail("TrackerID", trackerID);*/

				if( shardSize->get().present() && addToSizeEstimate )
					self->dbSizeEstimate->set( self->dbSizeEstimate->get() + metrics.bytes - shardSize->get().get().bytes );

				shardSize->set( metrics );
			} catch( Error &e ) {
				//TraceEvent("ShardSizeUpdateError").detail("Begin", printable(keys.begin)).detail("End", printable(keys.end)).detail("TrackerID", trackerID).error(e, true);
				Void _ = wait( tr.onError(e) );
			}
		}
	} catch( Error &e ) {
		if (e.code() != error_code_actor_cancelled)
			self->output.sendError(e);		// Propagate failure to dataDistributionTracker
		throw e;
	}
}

/*
ACTOR Future<Void> extrapolateShardBytes( Reference<AsyncVar<Optional<int64_t>>> inBytes, Reference<AsyncVar<Optional<int64_t>>> outBytes ) {
	state std::deque< std::pair<double,int64_t> > past;
	loop {
		Void _ = wait( inBytes->onChange() );
		if( inBytes->get().present() ) {
			past.push_back( std::make_pair(now(),inBytes->get().get()) );
			if (past.size() < 2)
				outBytes->set( inBytes->get() );
			else {
				while (past.size() > 1 && past.end()[-1].first - past.begin()[1].first > 1.0)
					past.pop_front();
				double rate = std::max(0.0, double(past.end()[-1].second-past.begin()[0].second)/(past.end()[-1].first - past.begin()[0].first));
				outBytes->set( inBytes->get().get() + rate * 10.0 );
			}
		}
	}
}*/

ACTOR Future<Standalone<VectorRef<KeyRef>>> getSplitKeys( DataDistributionTracker* self, KeyRange splitRange, StorageMetrics splitMetrics, StorageMetrics estimated ) {
	loop {
		state Transaction tr(self->cx);
		try {
			Standalone<VectorRef<KeyRef>> keys = wait( tr.splitStorageMetrics( splitRange, splitMetrics, estimated ) );
			return keys;
		} catch( Error &e ) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<int64_t> getFirstSize( Reference<AsyncVar<Optional<StorageMetrics>>> stats ) {
	loop {
		if(stats->get().present())
			return stats->get().get().bytes;
		Void _ = wait( stats->onChange() );
	}
}

ACTOR Future<Void> changeSizes( DataDistributionTracker* self, KeyRangeRef keys, int64_t oldShardsEndingSize ) {
	state vector<Future<int64_t>> sizes;
	for (auto it : self->shards.intersectingRanges(keys) ) {
		sizes.push_back( getFirstSize( it->value().stats ) );
	}

	Void _ = wait( waitForAll( sizes ) );
	Void _ = wait( yield(TaskDataDistribution) );

	int64_t newShardsStartingSize = 0;
	for ( int i = 0; i < sizes.size(); i++ )
		newShardsStartingSize += sizes[i].get();

	int64_t totalSizeEstimate = self->dbSizeEstimate->get();
	/*TraceEvent("TrackerChangeSizes")
		.detail("TotalSizeEstimate", totalSizeEstimate)
		.detail("EndSizeOfOldShards", oldShardsEndingSize)
		.detail("StartingSizeOfNewShards", newShardsStartingSize);*/
	self->dbSizeEstimate->set( totalSizeEstimate + newShardsStartingSize - oldShardsEndingSize );
	return Void();
}

struct HasBeenTrueFor : NonCopyable {
	explicit HasBeenTrueFor( double seconds, bool value ) : enough( seconds ), trigger( value ? Void() : Future<Void>() ) {}

	Future<Void> set() {
		if( !trigger.isValid() ) {
			cleared = Promise<Void>();
			trigger = delay( enough, TaskDataDistribution - 1 ) || cleared.getFuture();
		}
		return trigger;
	}
	void clear() {
		if( !trigger.isValid() ) {
			return;
		}
		trigger = Future<Void>();
		cleared.send( Void() );
	}

	// True if this->value is true and has been true for this->seconds
	bool hasBeenTrueForLongEnough() const {
		return trigger.isValid() && trigger.isReady();
	}

private:
	Future<Void> trigger;
	Promise<Void> cleared;
	const double enough;
};

ACTOR Future<Void> shardSplitter(
	DataDistributionTracker* self,
	UID trackerId,
	KeyRange keys,
	Reference<AsyncVar<Optional<StorageMetrics>>> shardSize,
	ShardSizeBounds shardBounds )
{
	state StorageMetrics metrics = shardSize->get().get();
	state BandwidthStatus bandwidthStatus = getBandwidthStatus( shardSize->get().get() );

	//Split
	TEST(true);  // shard to be split

	StorageMetrics splitMetrics;
	splitMetrics.bytes = shardBounds.max.bytes / 2;
	splitMetrics.bytesPerKSecond = keys.begin >= keyServersKeys.begin ? splitMetrics.infinity : SERVER_KNOBS->SHARD_SPLIT_BYTES_PER_KSEC;
	splitMetrics.iosPerKSecond = splitMetrics.infinity;

	state Standalone<VectorRef<KeyRef>> splitKeys = wait( getSplitKeys(self, keys, splitMetrics, metrics ) );
	//fprintf(stderr, "split keys:\n");
	//for( int i = 0; i < splitKeys.size(); i++ ) {
	//	fprintf(stderr, "   %s\n", printable(splitKeys[i]).c_str());
	//}
	int numShards = splitKeys.size() - 1;

	if( g_random->random01() < 0.01 ) {
		TraceEvent("RelocateShardStartSplitx100", self->masterId)
			.detail("Begin", printable(keys.begin))
			.detail("End", printable(keys.end))
			.detail("TrackerID", trackerId)
			.detail("MaxBytes", shardBounds.max.bytes)
			.detail("MetricsBytes", metrics.bytes)
			.detail("Bandwidth", bandwidthStatus == BandwidthStatusHigh ? "High" : bandwidthStatus == BandwidthStatusNormal ? "Normal" : "Low")
			.detail("BytesPerKSec", metrics.bytesPerKSecond)
			.detail("numShards", numShards);
	}

	if( numShards > 1 ) {
		int skipRange = g_random->randomInt(0, numShards);
		// The queue can't deal with RelocateShard requests which split an existing shard into three pieces, so
		// we have to send the unskipped ranges in this order (nibbling in from the edges of the old range)
		for( int i = 0; i < skipRange; i++ )
			restartShardTrackers( self, KeyRangeRef(splitKeys[i], splitKeys[i+1]) );
		restartShardTrackers( self, KeyRangeRef( splitKeys[skipRange], splitKeys[skipRange+1] ) );
		for( int i = numShards-1; i > skipRange; i-- )
			restartShardTrackers( self, KeyRangeRef(splitKeys[i], splitKeys[i+1]) );

		for( int i = 0; i < skipRange; i++ )
			self->output.send( RelocateShard( KeyRangeRef(splitKeys[i], splitKeys[i+1]), PRIORITY_SPLIT_SHARD) );
		for( int i = numShards-1; i > skipRange; i-- )
			self->output.send( RelocateShard(  KeyRangeRef(splitKeys[i], splitKeys[i+1]), PRIORITY_SPLIT_SHARD) );

		self->sizeChanges.add( changeSizes( self, keys, shardSize->get().get().bytes ) );
	} else {
		Void _ = wait( delay(1.0, TaskDataDistribution) ); //In case the reason the split point was off was due to a discrepancy between storage servers
	}
	return Void();
}

Future<Void> shardMerger(
	DataDistributionTracker* self,
	UID trackerId,
	KeyRange const& keys,
	Reference<AsyncVar<Optional<StorageMetrics>>> shardSize )
{
	int64_t maxShardSize = self->maxShardSize->get().get();

	auto prevIter = self->shards.rangeContaining(keys.begin);
	auto nextIter = self->shards.rangeContaining(keys.begin);

	TEST(true);  // shard to be merged
	ASSERT( keys.begin > allKeys.begin );

	// This will merge shards both before and after "this" shard in keyspace.
	int shardsMerged = 1;
	bool forwardComplete = false;
	KeyRangeRef merged;
	StorageMetrics endingStats = shardSize->get().get();

	loop {
		Optional<StorageMetrics> newMetrics;
		if( !forwardComplete ) {
			if( nextIter->range().end == allKeys.end ) {
				forwardComplete = true;
				continue;
			}
			++nextIter;
			newMetrics = nextIter->value().stats->get();

			// If going forward, give up when the next shard's stats are not yet present.
			if( !newMetrics.present() ) {
				--nextIter;
				forwardComplete = true;
				continue;
			}
		} else {
			--prevIter;
			newMetrics = prevIter->value().stats->get();

			// If going backward, stop when the stats are not present or if the shard is already over the merge
			//  bounds. If this check triggers right away (if we have not merged anything) then return a trigger
			//  on the previous shard changing "size".
			if( !newMetrics.present() ) {
				if( shardsMerged == 1 ) {
					TEST( true ); // shardMerger cannot merge anything
					return prevIter->value().stats->onChange();
				}

				++prevIter;
				break;
			}
		}

		merged = KeyRangeRef( prevIter->range().begin, nextIter->range().end );
		endingStats += newMetrics.get();
		shardsMerged++;

		auto shardBounds = getShardSizeBounds( merged, maxShardSize );
		if( endingStats.bytes >= shardBounds.min.bytes ||
				getBandwidthStatus( endingStats ) != BandwidthStatusLow ||
				shardsMerged >= SERVER_KNOBS->DD_MERGE_LIMIT ) {
			// The merged range is larger than the min bounds se we cannot continue merging in this direction.
			//  This means that:
			//  1. If we were going forwards (the starting direction), we roll back the last speculative merge.
			//      In this direction we do not want to go above this boundary since we will merge at least one in
			//      the other direction, even when that goes over the bounds.
			//  2. If we were going backwards we always want to merge one more shard on (to make sure we go over
			//      the shard min bounds) so we "break" without resetting the merged range.
			if( forwardComplete )
				break;

			// If going forward, remove most recently added range
			endingStats -= newMetrics.get();
			shardsMerged--;
			--nextIter;
			merged = KeyRangeRef( prevIter->range().begin, nextIter->range().end );
			forwardComplete = true;
		}
	}

	//restarting shard tracker will derefenced values in the shard map, so make a copy
	KeyRange mergeRange = merged;

	TraceEvent("RelocateShardMergeMetrics", self->masterId)
		.detail("OldKeys", printable(keys))
		.detail("NewKeys", printable(mergeRange))
		.detail("EndingSize", endingStats.bytes)
		.detail("BatchedMerges", shardsMerged)
		.detail("TrackerID", trackerId);

	restartShardTrackers( self, mergeRange, endingStats );
	self->output.send( RelocateShard( mergeRange, PRIORITY_MERGE_SHARD ) );

	// We are about to be cancelled by the call to restartShardTrackers
	return Void();
}

ACTOR Future<Void> shardEvaluator(
	DataDistributionTracker* self,
	KeyRange keys,
	Reference<AsyncVar<Optional<StorageMetrics>>> shardSize,
	HasBeenTrueFor *wantsToMerge,
	UID trackerID)
{
	Future<Void> onChange = shardSize->onChange() || yieldedFuture(self->maxShardSize->onChange());

	// There are the bounds inside of which we are happy with the shard size.
	// getShardSizeBounds() will allways have shardBounds.min.bytes == 0 for shards that start at allKeys.begin,
	//  so will will never attempt to merge that shard with the one previous.
	ShardSizeBounds shardBounds = getShardSizeBounds(keys, self->maxShardSize->get().get());
	StorageMetrics const& stats = shardSize->get().get();

	bool shouldSplit = stats.bytes > shardBounds.max.bytes ||
							( getBandwidthStatus( stats ) == BandwidthStatusHigh && keys.begin < keyServersKeys.begin );
	bool shouldMerge = stats.bytes < shardBounds.min.bytes &&
							getBandwidthStatus( stats ) == BandwidthStatusLow;

	// Every invocation must set this or clear it
	if (shouldMerge) {
		auto whenLongEnough = wantsToMerge->set();
		if( !wantsToMerge->hasBeenTrueForLongEnough() ) {
			onChange = onChange || whenLongEnough;
		}
	}
	else
		wantsToMerge->clear();

	/*TraceEvent("ShardEvaluator", self->masterId)
		.detail("TrackerId", trackerID)
		.detail("ShouldSplit", shouldSplit)
		.detail("ShouldMerge", shouldMerge)
		.detail("HasBeenTrueLongEnough", wantsToMerge->hasBeenTrueForLongEnough());*/

	if(wantsToMerge->hasBeenTrueForLongEnough()) {
		onChange = onChange || shardMerger( self, trackerID, keys, shardSize );
	}
	if( shouldSplit ) {
		onChange = onChange || shardSplitter( self, trackerID, keys, shardSize, shardBounds );
	}

	Void _ = wait( onChange );
	return Void();
}

ACTOR Future<Void> shardTracker(
		DataDistributionTracker* self,
		KeyRange keys,
		Reference<AsyncVar<Optional<StorageMetrics>>> shardSize,
		UID trackerID )
{
	// Survives multiple calls to shardEvaluator and keeps merges from happening too quickly.
	state HasBeenTrueFor wantsToMerge( SERVER_KNOBS->DD_MERGE_COALESCE_DELAY, shardSize->get().present() );

	Void _ = wait( yieldedFuture(self->readyToStart.getFuture()) );

	if( !shardSize->get().present() )
		Void _ = wait( shardSize->onChange() );

	if( !self->maxShardSize->get().present() )
		Void _ = wait( yieldedFuture(self->maxShardSize->onChange()) );

	// Since maxShardSize will become present for all shards at once, avoid slow tasks with a short delay
	Void _ = wait( delay( 0, TaskDataDistribution ) );

	/*TraceEvent("ShardTracker", self->masterId)
		.detail("Begin", printable(keys.begin))
		.detail("End", printable(keys.end))
		.detail("TrackerID", trackerID)
		.detail("MaxBytes", self->maxShardSize->get().get())
		.detail("ShardSize", shardSize->get().get().bytes)
		.detail("BytesPerKSec", shardSize->get().get().bytesPerKSecond);*/

	try {
		loop {
			// Use the current known size to check for (and start) splits and merges.
			Void _ = wait( shardEvaluator( self, keys, shardSize, &wantsToMerge, trackerID ) );

			// We could have a lot of actors being released from the previous wait at the same time. Immediately calling
			// delay(0) mitigates the resulting SlowTask
			Void _ = wait( delay(0, TaskDataDistribution) );
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "ShardTrackerError", self->masterId)
			.detail("TrackerID", trackerID)
			.detail("Keys", printable(keys))
			.error(e);
		if (e.code() != error_code_actor_cancelled)
			self->output.sendError(e);		// Propagate failure to dataDistributionTracker
		throw e;
	}
}

void restartShardTrackers( DataDistributionTracker* self, KeyRangeRef keys, Optional<StorageMetrics> startingSize ) {
	auto ranges = self->shards.getAffectedRangesAfterInsertion( keys, ShardTrackedData() );
	for(int i=0; i<ranges.size(); i++) {
		if( !ranges[i].value.trackShard.isValid() && ranges[i].begin != keys.begin ) {
			// When starting, key space will be full of "dummy" default contructed entries.
			// This should happen when called from trackInitialShards()
			ASSERT( !self->readyToStart.isSet() );
			continue;
		}

		Reference<AsyncVar<Optional<StorageMetrics>>> shardSize( new AsyncVar<Optional<StorageMetrics>>() );

		// For the case where the new tracker will take over at the boundaries of current shard(s)
		//  we can use the old size if it is available. This will be the case when merging shards.
		if( startingSize.present() ) {
			ASSERT( ranges.size() == 1 );
			/*TraceEvent("ShardTrackerSizePreset", self->masterId)
				.detail("Keys", printable(keys))
				.detail("Size", startingSize.get().metrics.bytes)
				.detail("Merges", startingSize.get().merges);*/
			TEST( true ); // shardTracker started with trackedBytes already set
			shardSize->set( startingSize );
		}

		UID trackerID = g_random->randomUniqueID();
		ShardTrackedData data;
		data.stats = shardSize;
		data.trackShard = shardTracker( self, ranges[i], shardSize, trackerID );
		data.trackBytes = trackShardBytes( self, keys, shardSize, trackerID );
		self->shards.insert( ranges[i], data );
	}
}

ACTOR Future<Void> trackInitialShards(DataDistributionTracker *self,
									  Reference<InitialDataDistribution> initData,
									  Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure)
{
	TraceEvent("TrackInitialShards", self->masterId).detail("InitialShardCount", initData->shards.size());

	//This line reduces the priority of shard initialization to prevent interference with failure monitoring.
	//SOMEDAY: Figure out what this priority should actually be
	Void _ = wait( delay( 0.0, TaskDataDistribution ) );

	state int lastBegin = -1;
	state vector<UID> last;

	state int s;
	for(s=0; s<initData->shards.size(); s++) {
		state InitialDataDistribution::Team src = initData->shards[s].value.first;
		auto& dest = initData->shards[s].value.second;
		if (dest.size()) {
			// This shard is already in flight.  Ideally we should use dest in sABTF and generate a dataDistributionRelocator directly in
			// DataDistributionQueue to track it, but it's easier to just (with low priority) schedule it for movement.
			self->output.send( RelocateShard( initData->shards[s], PRIORITY_RECOVER_MOVE ) );
		}

		// The following clause was here for no remembered reason.  It was removed, however, because on resumption of stopped
		//  clusters (of size 3) it was grouping all the the shards in the system into one, and then splitting them all back out,
		//  causing unecessary data distribution.
		//if (s==0 || s+1==initData.shards.size() || lastBegin<0 || src != last || initData.shards[s].begin == keyServersPrefix) {
			// end current run, start a new shardTracker
			// relies on the dummy shard at allkeysend

			if (lastBegin >= 0) {
				state KeyRangeRef keys( initData->shards[lastBegin].begin, initData->shards[s].begin );
				restartShardTrackers( self, keys );
				shardsAffectedByTeamFailure->defineShard( keys );
				shardsAffectedByTeamFailure->moveShard( keys, last );
			}
			lastBegin = s;
			last = src;
		//}
		Void _ = wait( yield( TaskDataDistribution ) );
	}

	Future<Void> initialSize = changeSizes( self, KeyRangeRef(allKeys.begin, allKeys.end), 0 );
	self->readyToStart.send(Void());
	Void _ = wait( initialSize );
	self->maxShardSizeUpdater = updateMaxShardSize( self->cx->dbName, self->dbSizeEstimate, self->maxShardSize );

	return Void();
}

ACTOR Future<Void> fetchShardMetrics_impl( DataDistributionTracker* self, GetMetricsRequest req ) {
	try {
		loop {
			Future<Void> onChange;
			StorageMetrics returnMetrics;
			for( auto t : self->shards.intersectingRanges( req.keys ) ) {
				auto &stats = t.value().stats;
				if( !stats->get().present() ) {
					onChange = stats->onChange();
					break;
				}
				returnMetrics += t.value().stats->get().get();
			}

			if( !onChange.isValid() ) {
				req.reply.send( returnMetrics );
				return Void();
			}

			Void _ = wait( onChange );
		}
	} catch( Error &e ) {
		if( e.code() != error_code_actor_cancelled && !req.reply.isSet() )
			req.reply.sendError(e);
		throw;
	}
}

ACTOR Future<Void> fetchShardMetrics( DataDistributionTracker* self, GetMetricsRequest req ) {
	choose {
		when( Void _ = wait( fetchShardMetrics_impl( self, req ) ) ) {}
		when( Void _ = wait( delay( SERVER_KNOBS->DD_SHARD_METRICS_TIMEOUT ) ) ) {
			TEST(true); // DD_SHARD_METRICS_TIMEOUT
			StorageMetrics largeMetrics;
			largeMetrics.bytes = SERVER_KNOBS->MAX_SHARD_BYTES;
			req.reply.send( largeMetrics );
		}
	}
	return Void();
}

ACTOR Future<Void> dataDistributionTracker(
	Reference<InitialDataDistribution> initData,
	Database cx,
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
	PromiseStream<RelocateShard> output,
	PromiseStream<GetMetricsRequest> getShardMetrics,
	FutureStream<Promise<int64_t>> getAverageShardBytes,
	Promise<Void> readyToStart,
	UID masterId)
{
	state DataDistributionTracker self(cx, masterId, readyToStart, output);
	state Future<Void> loggingTrigger = Void();
	try {
		Void _ = wait( trackInitialShards( &self, initData, shardsAffectedByTeamFailure ) );
		initData = Reference<InitialDataDistribution>();

		loop choose {
			when( Promise<int64_t> req = waitNext( getAverageShardBytes ) ) {
				req.send( self.maxShardSize->get().get() / 2 );
			}
			when( Void _ = wait( loggingTrigger ) ) {
				TraceEvent("DDTrackerStats", self.masterId)
					.detail("Shards", self.shards.size())
					.detail("TotalSizeBytes", self.dbSizeEstimate->get())
					.trackLatest( format("%s/DDTrackerStats", printable(cx->dbName).c_str() ).c_str() );

				loggingTrigger = delay(SERVER_KNOBS->DATA_DISTRIBUTION_LOGGING_INTERVAL);
			}
			when( GetMetricsRequest req = waitNext( getShardMetrics.getFuture() ) ) {
				self.sizeChanges.add( fetchShardMetrics( &self, req ) );
			}
			when( Void _ = wait( self.sizeChanges.getResult() ) ) {}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "dataDistributionTrackerError", self.masterId).error(e);
		throw e;
	}
}

vector<KeyRange> ShardsAffectedByTeamFailure::getShardsFor( Team team ) {
	vector<KeyRange> r;
	for(auto it = team_shards.lower_bound( std::pair<Team,KeyRange>( team, KeyRangeRef() ) );
			it != team_shards.end() && it->first == team;
			++it)
		r.push_back( it->second );
	return r;
}

int ShardsAffectedByTeamFailure::getNumberOfShards( UID ssID ) {
	return storageServerShards[ssID];
}

vector<vector<UID>> ShardsAffectedByTeamFailure::getTeamsFor( KeyRangeRef keys ) {
	return shard_teams[keys.begin];
}

void ShardsAffectedByTeamFailure::erase(Team team, KeyRange const& range) {
	if(team_shards.erase( std::pair<Team,KeyRange>(team, range) ) > 0) {
		for(auto uid = team.begin(); uid != team.end(); ++uid)
			storageServerShards[*uid]--;
	}
}

void ShardsAffectedByTeamFailure::insert(Team team, KeyRange const& range) {
	if(team_shards.insert( std::pair<Team,KeyRange>( team, range ) ).second) {
		for(auto uid = team.begin(); uid != team.end(); ++uid)
			storageServerShards[*uid]++;
	}
}

void ShardsAffectedByTeamFailure::defineShard( KeyRangeRef keys ) {
	std::set<Team> teams;
	auto rs = shard_teams.intersectingRanges(keys);
	for(auto it = rs.begin(); it != rs.end(); ++it) {
		for(auto t=it->value().begin(); t!=it->value().end(); ++t) {
			teams.insert( *t );
			erase(*t, it->range());
		}
	}

	/*TraceEvent("ShardsAffectedByTeamFailureDefine")
		.detail("KeyBegin", printable(keys.begin))
		.detail("KeyEnd", printable(keys.end))
		.detail("TeamCount", teams.size()); */

	auto affectedRanges = shard_teams.getAffectedRangesAfterInsertion(keys);
	shard_teams.insert( keys, vector<Team>(teams.begin(), teams.end()) );

	for(auto r=affectedRanges.begin(); r != affectedRanges.end(); ++r) {
		auto& teams = shard_teams[r->begin];
		for(auto t=teams.begin(); t!=teams.end(); ++t) {
			insert(*t, *r);
		}
	}
	check();
}

void ShardsAffectedByTeamFailure::moveShard( KeyRangeRef keys, Team destinationTeam ) {
	/*TraceEvent("ShardsAffectedByTeamFailureMove")
		.detail("KeyBegin", printable(keys.begin))
		.detail("KeyEnd", printable(keys.end))
		.detail("NewTeamSize", destinationTeam.size())
		.detail("NewTeam", describe(destinationTeam));*/

	auto ranges = shard_teams.intersectingRanges( keys );
	std::vector< std::pair<Team,KeyRange> > modifiedShards;
	for(auto it = ranges.begin(); it != ranges.end(); ++it) {
		if( keys.contains( it->range() ) ) {
			// erase the many teams that were assiciated with this one shard
			for(auto t = it->value().begin(); t != it->value().end(); ++t) {
				erase(*t, it->range());
			}

			// save this modification for later insertion
			modifiedShards.push_back( std::pair<Team,KeyRange>( destinationTeam, it->range() ) );
		} else {
			// for each range that touches this move, add our team as affecting this range
			insert(destinationTeam, it->range());

			// if we are not in the list of teams associated with this shard, add us in
			auto& teams = it->value();
			if( std::find( teams.begin(), teams.end(), destinationTeam ) == teams.end() )
				teams.push_back( destinationTeam );
		}
	}

	// we cannot modify the KeyRangeMap while iterating through it, so add saved modifications now
	for( int i = 0; i < modifiedShards.size(); i++ ) {
		insert(modifiedShards[i].first, modifiedShards[i].second);
		shard_teams.insert( modifiedShards[i].second, vector<Team>( 1, modifiedShards[i].first ) );
	}

	check();
}

void ShardsAffectedByTeamFailure::check() {
	if (EXPENSIVE_VALIDATION) {
		for(auto t = team_shards.begin(); t != team_shards.end(); ++t) {
			auto i = shard_teams.rangeContaining(t->second.begin);
			if (i->range() != t->second ||
				!std::count(i->value().begin(), i->value().end(), t->first))
			{
				ASSERT(false);
			}
		}
		auto rs = shard_teams.ranges();
		for(auto i = rs.begin(); i != rs.end(); ++i)
			for(vector<Team>::iterator t = i->value().begin(); t != i->value().end(); ++t)
				if (!team_shards.count( make_pair( *t, i->range() ) )) {
					std::string teamDesc, shards;
					for(int k=0; k<t->size(); k++)
						teamDesc += format("%llx ", (*t)[k].first());
					for(auto x = team_shards.lower_bound( make_pair( *t, KeyRangeRef() ) ); x != team_shards.end() && x->first == *t; ++x)
						shards += printable(x->second.begin) + "-" + printable(x->second.end) + ",";
					TraceEvent(SevError,"SATFInvariantError2")
						.detail("KB", printable(i->begin()))
						.detail("KE", printable(i->end()))
						.detail("Team", teamDesc)
						.detail("Shards", shards);
					ASSERT(false);
				}
	}
}
