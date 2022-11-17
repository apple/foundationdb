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

struct MockDDTrackerShardEvaluatorWorkload : public MockDDTestWorkload {
	static constexpr auto NAME = "MockDDTrackerShardEvaluator";
	DDSharedContext ddcx;

	PromiseStream<RelocateShard> output;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<GetTopKMetricsRequest> getTopKMetrics;
	PromiseStream<GetMetricsListRequest> getShardMetricsList;
	PromiseStream<Promise<int64_t>> getAverageShardBytes;

	KeyRangeMap<ShardTrackedData> shards;

	ActorCollection actors;
	uint64_t mockDbSize = 0;
	const int keySize = 16;

	std::map<RelocateReason, int> rsReasonCounts;

	// --- test configs ---

	// Each key space is convert from an int N. [N, N+1) represent a key space. So at most we have 2G key spaces
	int keySpaceCount = 0;
	// 1. fixed -- each key space has fixed size. The size of each key space is calculated as minSpaceKeyCount *
	// (minByteSize + 16) ;
	// 2. linear -- from 0 to keySpaceCount the size of key space increase by size linearStride, from
	// linearStartSize. Each value is fixed to minByteSize;
	// 3. random -- each key space can has [minSpaceKeyCount,
	// maxSpaceKeyCount] pairs and the size of value varies from [minByteSize, maxByteSize];
	Value keySpaceStrategy = "fixed"_sr;
	int minSpaceKeyCount = 1000, maxSpaceKeyCount = 1000;
	int linearStride = 10 * (1 << 20), linearStartSize = 10 * (1 << 20);

	MockDDTrackerShardEvaluatorWorkload(WorkloadContext const& wcx)
	  : MockDDTestWorkload(wcx), ddcx(deterministicRandom()->randomUniqueID()) {
		keySpaceCount = getOption(options, "keySpaceCount"_sr, keySpaceCount);
		keySpaceStrategy = getOption(options, "keySpaceStrategy"_sr, keySpaceStrategy);
		minSpaceKeyCount = getOption(options, "minSpaceKeyCount"_sr, minSpaceKeyCount);
		maxSpaceKeyCount = getOption(options, "maxSpaceKeyCount"_sr, maxSpaceKeyCount);
		linearStride = getOption(options, "linearStride"_sr, linearStride);
		linearStartSize = getOption(options, "linearStartSize"_sr, linearStartSize);
	}

	void populateRandomStrategy() {
		mockDbSize = 0;
		for (int i = 0; i < keySpaceCount; ++i) {
			int kCount = deterministicRandom()->randomInt(minSpaceKeyCount, maxSpaceKeyCount);
			for (int j = 0; j < kCount; ++j) {
				Key k = doubleToTestKey(i + deterministicRandom()->random01());
				auto vSize = deterministicRandom()->randomInt(minByteSize, maxByteSize + 1);
				mgs->set(k, vSize, true);
				mockDbSize += vSize + k.size();
			}
		}
	}

	void populateLinearStrategy() {
		mockDbSize = 0;
		auto pSize = minByteSize + keySize;
		for (int i = 0; i < keySpaceCount; ++i) {
			int kCount = std::ceil((linearStride * i + linearStartSize) * 1.0 / pSize);
			for (int j = 0; j < kCount; ++j) {
				Key k = doubleToTestKey(i + deterministicRandom()->random01());
				mgs->set(k, minByteSize, true);
			}
			mockDbSize += pSize * kCount;
		}
	}

	void populateFixedStrategy() {
		auto pSize = minByteSize + keySize;
		for (int i = 0; i < keySpaceCount; ++i) {
			for (int j = 0; j < minSpaceKeyCount; ++j) {
				Key k = doubleToTestKey(i + deterministicRandom()->random01());
				mgs->set(k, minByteSize, true);
			}
		}
		mockDbSize = keySpaceCount * minSpaceKeyCount * pSize;
	}

	void populateMgs() {
		// Will the sampling structure become too large?
		std::cout << "MGS Populating ...\n";
		if (keySpaceStrategy == "linear") {
			populateLinearStrategy();
		} else if (keySpaceStrategy == "fixed") {
			populateFixedStrategy();
		} else if (keySpaceStrategy == "random") {
			populateRandomStrategy();
		}
		uint64_t totalSize = 0;
		for (auto& server : mgs->allServers) {
			totalSize = server.second.sumRangeSize(allKeys);
		}
		TraceEvent("PopulateMockGlobalState")
		    .detail("Strategy", keySpaceStrategy)
		    .detail("EstimatedDbSize", mockDbSize)
		    .detail("MGSReportedTotalSize", totalSize);
		std::cout << "MGS Populated.\n";
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		MockDDTestWorkload::setup(cx);
		// populate mgs before run tracker
		populateMgs();
		return Void();
	}

	ACTOR static Future<Void> relocateShardReporter(MockDDTrackerShardEvaluatorWorkload* self,
	                                                FutureStream<RelocateShard> input) {
		loop choose {
			when(RelocateShard rs = waitNext(input)) { ++self->rsReasonCounts[rs.reason]; }
		}
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();

		// start mock servers
		actors.add(waitForAll(mgs->runAllMockServers()));

		// start tracker
		Reference<InitialDataDistribution> initData =
		    mock->getInitialDataDistribution(ddcx.id(), ddcx.lock, {}, ddcx.ddEnabledState.get(), SkipDDModeCheck::True)
		        .get();
		Reference<PhysicalShardCollection> physicalShardCollection = makeReference<PhysicalShardCollection>();
		Reference<AsyncVar<bool>> zeroHealthyTeams = makeReference<AsyncVar<bool>>(false);
		actors.add(dataDistributionTracker(initData,
		                                   mock,
		                                   output,
		                                   ddcx.shardsAffectedByTeamFailure,
		                                   physicalShardCollection,
		                                   getShardMetrics,
		                                   getTopKMetrics.getFuture(),
		                                   getShardMetricsList,
		                                   getAverageShardBytes.getFuture(),
		                                   Promise<Void>(),
		                                   zeroHealthyTeams,
		                                   ddcx.id(),
		                                   &shards,
		                                   &ddcx.trackerCancelled,
		                                   {}));
		actors.add(relocateShardReporter(this, output.getFuture()));

		return timeout(reportErrors(actors.getResult(), "MockDDTrackerShardEvaluatorWorkload"), testDuration, Void());
	}

	Future<bool> check(Database const& cx) override {
		std::cout << "Check phase shards count: " << shards.size() << "\n";
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