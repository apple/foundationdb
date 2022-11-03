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
	simpleConfig = getOption(options, "simpleConfig"_sr, true);
	testDuration = getOption(options, "testDuration"_sr, 10.0);
	meanDelay = getOption(options, "meanDelay"_sr, meanDelay);
	maxKeyspace = getOption(options, "maxKeyspace"_sr, maxKeyspace);
	maxByteSize = getOption(options, "maxByteSize"_sr, maxByteSize);
	minByteSize = getOption(options, "minByteSize"_sr, minByteSize);
}

Future<Void> MockDDTestWorkload::setup(Database const& cx) {
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