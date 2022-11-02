/*
 * MockDDTest.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MockDDTestWorkload : public TestWorkload {
	bool enabled;
	bool simpleConfig;
	double testDuration;
	double meanDelay = 0.05;
	double maxKeyspace = 0.1; // range space
	int maxByteSize = 1024, minByteSize = 32; // single point value size. The Key size is fixed to 16 bytes

	std::shared_ptr<MockGlobalState> mgs;
	Reference<DDMockTxnProcessor> mock;

	KeyRange getRandomRange(double offset) const {
		double len = deterministicRandom()->random01() * this->maxKeyspace;
		double pos = offset + deterministicRandom()->random01() * (1.0 - len);
		return KeyRangeRef(doubleToTestKey(pos), doubleToTestKey(pos + len));
	}

	MockDDTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
		simpleConfig = getOption(options, "simpleConfig"_sr, true);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		meanDelay = getOption(options, "meanDelay"_sr, meanDelay);
		maxKeyspace = getOption(options, "maxKeyspace"_sr, maxKeyspace);
		maxByteSize = getOption(options, "maxByteSize"_sr, maxByteSize);
		minByteSize = getOption(options, "minByteSize"_sr, minByteSize);
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		// initialize configuration
		BasicTestConfig testConfig;
		testConfig.simpleConfig = simpleConfig;
		testConfig.minimumReplication = 1;
		testConfig.logAntiQuorum = 0;
		DatabaseConfiguration dbConfig = generateNormalDatabaseConfiguration(testConfig);

		// initialize mgs
		mgs = std::make_shared<MockGlobalState>();
		mgs->maxByteSize = maxByteSize;
		mgs->minByteSize = minByteSize;
		mgs->initializeAsEmptyDatabaseMGS(dbConfig);
		mock = makeReference<DDMockTxnProcessor>(mgs);

		return Void();
	}
};

struct MockDDTrackerShardEvaluatorWorkload : public MockDDTestWorkload {

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
		if (keySpaceStrategy == "linear") {
			populateLinearStrategy();
		} else if (keySpaceStrategy == "fixed") {
			populateFixedStrategy();
		} else if (keySpaceStrategy == "random") {
			populateRandomStrategy();
		}
		TraceEvent("PopulateMockGlobalState")
		    .detail("Strategy", keySpaceStrategy)
		    .detail("EstimatedDbSize", mockDbSize);
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		MockDDTestWorkload::setup(cx);
		// populate mgs before run tracker
		populateMgs();
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

		return timeout(reportErrors(actors.getResult(), "MockDDTrackerShardEvaluatorWorkload"), testDuration, Void());
	}
};