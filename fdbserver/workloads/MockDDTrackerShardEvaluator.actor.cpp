/*
 * MockDDTrackerShardEvaluator.actor.cpp
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

#include "fdbserver/workloads/MockDDTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class MockDDTrackerShardEvaluatorWorkload : public MockDDTestWorkload {
public:
	static constexpr auto NAME = "MockDDTrackerShardEvaluator";
	DDSharedContext ddcx;
	Reference<DDMockTxnProcessor> mock;

	PromiseStream<RelocateShard> output;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<GetTopKMetricsRequest> getTopKMetrics;
	PromiseStream<GetMetricsListRequest> getShardMetricsList;
	PromiseStream<Promise<int64_t>> getAverageShardBytes;

	KeyRangeMap<ShardTrackedData> shards;

	ActorCollection actors;

	std::map<RelocateReason, int> rsReasonCounts;

	Reference<DataDistributionTracker> shardTracker;

	// --- test configs ---

	// check threshold
	int checkMinShardCount = 1;
	int checkMinSizeSplit = 0;
	int checkMinWriteSplit = 0;

	MockDDTrackerShardEvaluatorWorkload(WorkloadContext const& wcx)
	  : MockDDTestWorkload(wcx), ddcx(deterministicRandom()->randomUniqueID()) {
		checkMinShardCount = getOption(options, "checkMinShardCount"_sr, checkMinShardCount);
		checkMinSizeSplit = getOption(options, "checkMinSizeSplit"_sr, checkMinSizeSplit);
		checkMinWriteSplit = getOption(options, "checkMinWriteSplit"_sr, checkMinWriteSplit);
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		MockDDTestWorkload::setup(cx);
		// populate sharedMgs before run tracker
		populateMgs();
		mock = makeReference<DDMockTxnProcessor>(sharedMgs);
		return Void();
	}

	ACTOR static Future<Void> relocateShardReporter(MockDDTrackerShardEvaluatorWorkload* self,
	                                                FutureStream<RelocateShard> input) {
		loop {
			try {
				choose {
					when(RelocateShard rs = waitNext(input)) {
						++self->rsReasonCounts[rs.reason];
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server)
					throw e;
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY));
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();

		// start mock servers
		actors.add(waitForAll(sharedMgs->runAllMockServers()));

		// start tracker
		Reference<InitialDataDistribution> initData =
		    mock->getInitialDataDistribution(ddcx.id(), ddcx.lock, {}, ddcx.ddEnabledState.get(), SkipDDModeCheck::True)
		        .get();
		Reference<PhysicalShardCollection> physicalShardCollection = makeReference<PhysicalShardCollection>();
		Reference<PriorityBasedAudit> priorityBasedAudit = makeReference<PriorityBasedAudit>(
		    SERVER_KNOBS->PRIORITY_BASED_AUDIT_QUEUE_MAX_SIZE, SERVER_KNOBS->PRIORITY_BASED_AUDIT_RANGE_BATCH_SIZE);
		Reference<AsyncVar<bool>> zeroHealthyTeams = makeReference<AsyncVar<bool>>(false);

		shardTracker = makeReference<DataDistributionTracker>(
		    DataDistributionTrackerInitParams{ .db = mock,
		                                       .distributorId = ddcx.id(),
		                                       .readyToStart = Promise<Void>(),
		                                       .output = output,
		                                       .shardsAffectedByTeamFailure = ddcx.shardsAffectedByTeamFailure,
		                                       .physicalShardCollection = physicalShardCollection,
		                                       .priorityBasedAudit = priorityBasedAudit,
		                                       .anyZeroHealthyTeams = zeroHealthyTeams,
		                                       .shards = &shards,
		                                       .trackerCancelled = &ddcx.trackerCancelled,
		                                       .ddTenantCache = {} });
		actors.add(DataDistributionTracker::run(shardTracker,
		                                        initData,
		                                        getShardMetrics.getFuture(),
		                                        getTopKMetrics.getFuture(),
		                                        getShardMetricsList.getFuture(),
		                                        getAverageShardBytes.getFuture()));

		actors.add(relocateShardReporter(this, output.getFuture()));

		return timeout(reportErrors(actors.getResult(), "MockDDTrackerShardEvaluatorWorkload"), testDuration, Void());
	}

	Future<bool> check(Database const& cx) override {
		if (!enabled)
			return true;

		fmt::print("Check phase shards count: {}\n", shards.size());
		ASSERT_GE(shards.size(), checkMinShardCount);
		for (auto& [r, c] : rsReasonCounts) {
			fmt::print("{}: {}\n", r.toString(), c);
		}
		ASSERT_GE(rsReasonCounts[RelocateReason::SIZE_SPLIT], checkMinSizeSplit);
		ASSERT_GE(rsReasonCounts[RelocateReason::WRITE_SPLIT], checkMinWriteSplit);

		actors.clear(true);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		for (const auto& [reason, count] : rsReasonCounts) {
			m.push_back(PerfMetric(RelocateReason(reason).toString(), count, Averaged::False));
		}
	}
};

WorkloadFactory<MockDDTrackerShardEvaluatorWorkload> MockDDTrackerShardEvaluatorWorkload;