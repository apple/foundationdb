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

#include "fdbserver/workloads/MockDDTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

KeyRange MockDDTestWorkload::getRandomRange(double offset) const {
	double len = deterministicRandom()->random01() * this->maxKeyspace;
	double pos = offset + deterministicRandom()->random01() * (1.0 - len);
	return KeyRangeRef(doubleToTestKey(pos), doubleToTestKey(pos + len));
}

MockDDTestWorkload::MockDDTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
	enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
	simpleConfig = getOption(options, "simpleConfig"_sr, simpleConfig);
	testDuration = getOption(options, "testDuration"_sr, testDuration);
	meanDelay = getOption(options, "meanDelay"_sr, meanDelay);

	// mock data population setting
	maxKeyspace = getOption(options, "maxKeyspace"_sr, maxKeyspace);
	maxByteSize = getOption(options, "maxByteSize"_sr, maxByteSize);
	minByteSize = getOption(options, "minByteSize"_sr, minByteSize);
	keySize = getOption(options, "keySize"_sr, keySize);
	keySpaceCount = getOption(options, "keySpaceCount"_sr, keySpaceCount);
	keySpaceStrategy = getOption(options, "keySpaceStrategy"_sr, keySpaceStrategy);
	minSpaceKeyCount = getOption(options, "minSpaceKeyCount"_sr, minSpaceKeyCount);
	maxSpaceKeyCount = getOption(options, "maxSpaceKeyCount"_sr, maxSpaceKeyCount);
	linearStride = getOption(options, "linearStride"_sr, linearStride);
	linearStartSize = getOption(options, "linearStartSize"_sr, linearStartSize);
}

void MockDDTestWorkload::populateRandomStrategy() {
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

void MockDDTestWorkload::populateLinearStrategy() {
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

void MockDDTestWorkload::populateFixedStrategy() {
	auto pSize = minByteSize + keySize;
	for (int i = 0; i < keySpaceCount; ++i) {
		for (int j = 0; j < minSpaceKeyCount; ++j) {
			Key k = doubleToTestKey(i + deterministicRandom()->random01());
			mgs->set(k, minByteSize, true);
		}
	}
	mockDbSize = keySpaceCount * minSpaceKeyCount * pSize;
}

void MockDDTestWorkload::populateMgs() {
	// Will the sampling structure become too large?
	fmt::print("MGS Populating ...\n");
	if (keySpaceStrategy == "linear") {
		populateLinearStrategy();
	} else if (keySpaceStrategy == "fixed") {
		populateFixedStrategy();
	} else if (keySpaceStrategy == "random") {
		populateRandomStrategy();
	}
	uint64_t totalSize = 0;
	for (auto& server : mgs->allServers) {
		totalSize = server.second->sumRangeSize(allKeys);
	}
	TraceEvent("PopulateMockGlobalState")
	    .detail("Strategy", keySpaceStrategy)
	    .detail("EstimatedDbSize", mockDbSize)
	    .detail("MGSReportedTotalSize", totalSize);
	fmt::print("MGS Populated.\n");
}

Future<Void> MockDDTestWorkload::setup(Database const& cx) {
	if (!enabled)
		return Void();
	// initialize configuration
	BasicTestConfig testConfig;
	testConfig.simpleConfig = simpleConfig;
	testConfig.minimumReplication = 1;
	testConfig.logAntiQuorum = 0;
	BasicSimulationConfig dbConfig = generateBasicSimulationConfig(testConfig);

	// initialize mgs
	mgs = std::make_shared<MockGlobalState>();
	mgs->maxByteSize = maxByteSize;
	mgs->minByteSize = minByteSize;
	mgs->initializeClusterLayout(dbConfig);
	mgs->initializeAsEmptyDatabaseMGS(dbConfig.db);
	mock = makeReference<DDMockTxnProcessor>(mgs);

	return Void();
}