/*
 * WorkloadUtils.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_TESTER_WORKLOADUTILS_H
#define FDBSERVER_TESTER_WORKLOADUTILS_H
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/KnobProtectiveGroups.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbrpc/simulator.h"

struct DistributedTestResults {
	std::vector<PerfMetric> metrics;
	int successes, failures;

	DistributedTestResults() {}

	DistributedTestResults(std::vector<PerfMetric> const& metrics, int successes, int failures)
	  : metrics(metrics), successes(successes), failures(failures) {}

	bool ok() const { return successes && !failures; }
};

class TestSpec {
public:
	TestSpec() {
		title = StringRef();
		dumpAfterTest = false;
		clearAfterTest = g_network->isSimulated();
		useDB = true;
		startDelay = 30.0;
		phases = TestWorkload::SETUP | TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
		timeout = g_network->isSimulated() ? 15000 : 1500;
		databasePingDelay = g_network->isSimulated() ? 0.0 : 15.0;
		runConsistencyCheck = g_network->isSimulated();
		runConsistencyCheckOnCache = false;
		runConsistencyCheckOnTSS = true;
		waitForQuiescenceBegin = true;
		waitForQuiescenceEnd = true;
		simCheckRelocationDuration = false;
		simConnectionFailuresDisableDuration = 0;
		simBackupAgents = ISimulator::BackupAgentType::NoBackupAgents;
		simDrAgents = ISimulator::BackupAgentType::NoBackupAgents;
		restorePerpetualWiggleSetting = true;
	}
	TestSpec(StringRef title,
	         bool dump,
	         bool clear,
	         double startDelay = 30.0,
	         bool useDB = true,
	         double databasePingDelay = -1.0)
	  : title(title), dumpAfterTest(dump), clearAfterTest(clear), useDB(useDB), startDelay(startDelay), timeout(600),
	    databasePingDelay(databasePingDelay), runConsistencyCheck(g_network->isSimulated()),
	    runConsistencyCheckOnCache(false), runConsistencyCheckOnTSS(false), waitForQuiescenceBegin(true),
	    waitForQuiescenceEnd(true), restorePerpetualWiggleSetting(true), simCheckRelocationDuration(false),
	    simConnectionFailuresDisableDuration(0), simBackupAgents(ISimulator::BackupAgentType::NoBackupAgents),
	    simDrAgents(ISimulator::BackupAgentType::NoBackupAgents) {
		phases = TestWorkload::SETUP | TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
		if (databasePingDelay < 0)
			databasePingDelay = g_network->isSimulated() ? 0.0 : 15.0;
	}

	Standalone<StringRef> title;
	bool dumpAfterTest;
	bool clearAfterTest;
	bool useDB;
	bool runFailureWorkloads = true;
	double startDelay;
	int phases;
	Standalone<VectorRef<VectorRef<KeyValueRef>>> options;
	int timeout;
	double databasePingDelay;
	double maxDDRunTime = 0;
	bool runConsistencyCheck;
	bool runConsistencyCheckOnCache;
	bool runConsistencyCheckOnTSS;
	bool waitForQuiescenceBegin;
	bool waitForQuiescenceEnd;
	bool restorePerpetualWiggleSetting;
	bool simCheckRelocationDuration;
	double simConnectionFailuresDisableDuration;
	ISimulator::BackupAgentType simBackupAgents;
	ISimulator::BackupAgentType simDrAgents;

	KnobKeyValuePairs overrideKnobs;
	std::vector<std::string> disabledFailureInjectionWorkloads;
};

Future<DistributedTestResults> runWorkload(Database const& cx,
                                           std::vector<TesterInterface> const& testers,
                                           TestSpec const& spec);
void logMetrics(std::vector<PerfMetric> metrics);
Future<Void> databaseWarmer(Database cx);

Future<Void> testExpectedError(Future<Void> test,
                               const char* testDescr,
                               Optional<Error> expectedError = Optional<Error>(),
                               Optional<bool*> successFlag = Optional<bool*>(),
                               std::map<std::string, std::string> details = {},
                               Optional<Error> throwOnError = Optional<Error>(),
                               UID id = UID());

#endif
