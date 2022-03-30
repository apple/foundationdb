/*
 * UnitTests.actor.cpp
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
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

void forceLinkIndexedSetTests();
void forceLinkDequeTests();
void forceLinkFlowTests();
void forceLinkVersionedMapTests();
void forceLinkMemcpyTests();
void forceLinkMemcpyPerfTests();
#if (!defined(TLS_DISABLED) && !defined(_WIN32))
void forceLinkStreamCipherTests();
void forceLinkBLockCiherTests();
#endif
void forceLinkParallelStreamTests();
void forceLinkSimExternalConnectionTests();
void forceLinkMutationLogReaderTests();
void forceLinkSimEncryptVaultProxyTests();
void forceLinkIThreadPoolTests();

struct UnitTestWorkload : TestWorkload {
	bool enabled;
	std::string testPattern;
	int testRunLimit;
	UnitTestParameters testParams;
	bool cleanupAfterTests;

	PerfIntCounter testsAvailable, testsExecuted, testsFailed;
	PerfDoubleCounter totalWallTime, totalSimTime;

	UnitTestWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), testsAvailable("Test Cases Available"), testsExecuted("Test Cases Executed"),
	    testsFailed("Test Cases Failed"), totalWallTime("Total wall clock time (s)"),
	    totalSimTime("Total flow time (s)") {
		enabled = !clientId; // only do this on the "first" client
		testPattern = getOption(options, "testsMatching"_sr, Value()).toString();
		testRunLimit = getOption(options, "maxTestCases"_sr, -1);
		if (g_network->isSimulated()) {
			testParams.setDataDir(getOption(options, "dataDir"_sr, "simfdb/unittests/"_sr).toString());
		} else {
			testParams.setDataDir(getOption(options, "dataDir"_sr, "unittests/"_sr).toString());
		}
		cleanupAfterTests = getOption(options, "cleanupAfterTests"_sr, true);

		// Consume all remaining options as testParams which the unit test can access
		for (auto& kv : options) {
			if (kv.value.size() != 0) {
				testParams.set(kv.key.toString(), getOption(options, kv.key, StringRef()).toString());
			}
		}

		forceLinkIndexedSetTests();
		forceLinkDequeTests();
		forceLinkFlowTests();
		forceLinkVersionedMapTests();
		forceLinkMemcpyTests();
		forceLinkMemcpyPerfTests();
#if (!defined(TLS_DISABLED) && !defined(_WIN32))
		forceLinkStreamCipherTests();
		void forceLinkBlobCipherTests();
#endif
		forceLinkParallelStreamTests();
		forceLinkSimExternalConnectionTests();
		forceLinkMutationLogReaderTests();
		forceLinkSimEncryptVaultProxyTests();
		forceLinkIThreadPoolTests();
	}

	std::string description() const override { return "UnitTests"; }
	Future<Void> setup(Database const& cx) override {
		platform::eraseDirectoryRecursive(testParams.getDataDir());
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (enabled)
			return runUnitTests(this);
		return Void();
	}
	Future<bool> check(Database const& cx) override { return testsFailed.getValue() == 0; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(testsAvailable.getMetric());
		m.push_back(testsExecuted.getMetric());
		m.push_back(testsFailed.getMetric());
		m.push_back(totalWallTime.getMetric());
		m.push_back(totalSimTime.getMetric());
	}

	ACTOR static Future<Void> runUnitTests(UnitTestWorkload* self) {
		state std::vector<UnitTest*> tests;

		for (auto test = g_unittests.tests; test != nullptr; test = test->next) {
			if (StringRef(test->name).startsWith(self->testPattern)) {
				++self->testsAvailable;
				tests.push_back(test);
			}
		}

		fprintf(stdout, "Found %zu tests\n", tests.size());

		if (tests.size() == 0) {
			TraceEvent(SevError, "NoMatchingUnitTests").detail("TestPattern", self->testPattern);
			++self->testsFailed;
			return Void();
		}

		deterministicRandom()->randomShuffle(tests);
		if (self->testRunLimit > 0 && tests.size() > self->testRunLimit)
			tests.resize(self->testRunLimit);

		state std::vector<UnitTest*>::iterator t;
		for (t = tests.begin(); t != tests.end(); ++t) {
			state UnitTest* test = *t;
			printf("Testing %s\n", test->name);

			state Error result = success();
			state double start_now = now();
			state double start_timer = timer();

			platform::createDirectory(self->testParams.getDataDir());
			try {
				wait(test->func(self->testParams));
			} catch (Error& e) {
				++self->testsFailed;
				result = e;
			}
			if (self->cleanupAfterTests) {
				platform::eraseDirectoryRecursive(self->testParams.getDataDir());
			}
			++self->testsExecuted;
			double wallTime = timer() - start_timer;
			double simTime = now() - start_now;

			self->totalWallTime += wallTime;
			self->totalSimTime += simTime;
			TraceEvent(result.code() != error_code_success ? SevError : SevInfo, "UnitTest")
			    .errorUnsuppressed(result)
			    .detail("Name", test->name)
			    .detail("File", test->file)
			    .detail("Line", test->line)
			    .detail("WallTime", wallTime)
			    .detail("FlowTime", simTime);
		}

		return Void();
	}
};

WorkloadFactory<UnitTestWorkload> UnitTestWorkloadFactory("UnitTests");

TEST_CASE("/fdbserver/UnitTestWorkload/long delay") {
	wait(delay(60));
	return Void();
}
