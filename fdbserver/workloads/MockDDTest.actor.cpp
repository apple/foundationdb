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
	double maxKeyspace = 0.1;

	std::shared_ptr<MockGlobalState> mgs;
	std::shared_ptr<DDMockTxnProcessor> mock;

	MockDDTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
		simpleConfig = getOption(options, "simpleConfig"_sr, true);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		meanDelay = getOption(options, "meanDelay"_sr, meanDelay);
		maxKeyspace = getOption(options, "maxKeyspace"_sr, maxKeyspace);
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
		mgs->initializeAsEmptyDatabaseMGS(dbConfig);
		mock = std::make_shared<DDMockTxnProcessor>(mgs);

		return Void();
	}
};