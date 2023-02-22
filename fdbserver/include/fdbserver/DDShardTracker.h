/*
 * DDShardTracker.h
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
#ifndef FOUNDATIONDB_DDSHARDTRACKER_H
#define FOUNDATIONDB_DDSHARDTRACKER_H
#include "fdbserver/DataDistribution.actor.h"

// send request/signal to DDTracker through interface
// call synchronous method from components outside DDShardTracker
struct IDDShardTracker {
	// FIXME: the streams are not used yet
	Promise<Void> readyToStart;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<GetTopKMetricsRequest> getTopKMetrics;
	PromiseStream<GetMetricsListRequest> getShardMetricsList;
	PromiseStream<KeyRange> restartShardTracker;

	// PromiseStream<Promise<int64_t>> averageShardBytes; // FIXME(xwang): change it to a synchronous call

	virtual double getAverageShardBytes() = 0;
	virtual ~IDDShardTracker() = default;
};

struct DataDistributionTrackerInitParams {
	Reference<IDDTxnProcessor> db;
	UID const& distributorId;
	Promise<Void> const& readyToStart;
	PromiseStream<RelocateShard> const& output;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	Reference<PhysicalShardCollection> physicalShardCollection;
	Reference<AsyncVar<bool>> anyZeroHealthyTeams;
	KeyRangeMap<ShardTrackedData>* shards;
	bool* trackerCancelled;
	Optional<Reference<TenantCache>> ddTenantCache;
};

struct DataDistributionTracker : public IDDShardTracker {
	Reference<IDDTxnProcessor> db;
	UID distributorId;

	// At now, the lifetime of shards is guaranteed longer than DataDistributionTracker.
	KeyRangeMap<ShardTrackedData>* shards = nullptr;
	ActorCollection sizeChanges;

	int64_t systemSizeEstimate = 0;
	Reference<AsyncVar<int64_t>> dbSizeEstimate;
	Reference<AsyncVar<Optional<int64_t>>> maxShardSize;
	Future<Void> maxShardSizeUpdater;

	// CapacityTracker
	PromiseStream<RelocateShard> output;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;

	// PhysicalShard Tracker
	Reference<PhysicalShardCollection> physicalShardCollection;

	Promise<Void> readyToStart;
	Reference<AsyncVar<bool>> anyZeroHealthyTeams;

	// Read hot detection
	PromiseStream<KeyRange> readHotShard;

	// The reference to trackerCancelled must be extracted by actors,
	// because by the time (trackerCancelled == true) this memory cannot
	// be accessed
	bool* trackerCancelled = nullptr;

	// This class extracts the trackerCancelled reference from a DataDistributionTracker object
	// Because some actors spawned by the dataDistributionTracker outlive the DataDistributionTracker
	// object, we must guard against memory errors by using a GetTracker functor to access
	// the DataDistributionTracker object.
	class SafeAccessor {
		bool const& trackerCancelled;
		DataDistributionTracker& tracker;

	public:
		SafeAccessor(DataDistributionTracker* tracker)
		  : trackerCancelled(*tracker->trackerCancelled), tracker(*tracker) {
			ASSERT(!trackerCancelled);
		}

		DataDistributionTracker* operator()() {
			if (trackerCancelled) {
				CODE_PROBE(true, "Trying to access DataDistributionTracker after tracker has been cancelled");
				throw dd_tracker_cancelled();
			}
			return &tracker;
		}
	};

	Optional<Reference<TenantCache>> ddTenantCache;

	DataDistributionTracker() = default;

	DataDistributionTracker(DataDistributionTrackerInitParams const& params);

	~DataDistributionTracker() override;

	double getAverageShardBytes() override { return maxShardSize->get().get() / 2.0; }
};

#endif // FOUNDATIONDB_DDSHARDTRACKER_H
