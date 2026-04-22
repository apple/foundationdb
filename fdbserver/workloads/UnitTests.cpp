/*
 * UnitTests.cpp
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

#include "fdbserver/tester/workloads.h"
#include "flow/UnitTest.h"

void forceLinkIndexedSetTests();
void forceLinkDequeTests();
void forceLinkFlowTests();
void forceLinkCoroTests();
void forceLinkMemcpyTests();
void forceLinkMemcpyPerfTests();
void forceLinkStreamCipherTests();
void forceLinkSimExternalConnectionTests();
void forceLinkMutationLogReaderTests();
void forceLinkIThreadPoolTests();
void forceLinkJsonWebKeySetTests();
void forceLinkVersionVectorTests();
void forceLinkRESTClientTests();
void forceLinkRESTUtilsTests();
void forceLinkCompressedIntTests();
void forceLinkCompressionUtilsTest();
void forceLinkAtomicTests();
void forceLinkIdempotencyIdTests();
void forceLinkActorCollectionTests();
void forceLinkDDSketchTests();
void forceLinkCommitProxyTests();
void forceLinkWipedStringTests();
void forceLinkRandomKeyValueUtilsTests();
void forceLinkActorFuzzUnitTests();
void forceLinkGrpcTests();
void forceLinkGrpcTests2();
void forceLinkSimpleCounterTests();
void forceLinkLogSystemRecoveryTests();
void forceLinkIPagerTests();
void forceLinkMockS3ServerTests();
void forceLinkClusterHealthMonitorTests();

struct UnitTestWorkload : TestWorkload {
	static constexpr auto NAME = "UnitTests";

	bool enabled;
	std::string testPattern;
	std::vector<std::string> testsIgnored;
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
		if (hasOption(options, "testsIgnored"_sr)) {
			auto ignored = getOption(options, "testsIgnored"_sr, Value());
			for (auto s : ignored.splitAny(";"_sr)) {
				auto str = s.toString();
				str.erase(remove_if(str.begin(), str.end(), isspace), str.end());
				testsIgnored.push_back(str);
			}
		}
		testRunLimit = getOption(options, "maxTestCases"_sr, -1);
		if (g_network->isSimulated()) {
			testParams.setDataDir(getOption(options, "dataDir"_sr, "simfdb/unittests/"_sr).toString());
		} else {
			testParams.setDataDir(getOption(options, "dataDir"_sr, "unittests/"_sr).toString());
		}
		cleanupAfterTests = getOption(options, "cleanupAfterTests"_sr, true);

		// Consume all remaining options as testParams which the unit test can access
		for (auto& kv : options) {
			if (!kv.value.empty()) {
				testParams.set(kv.key.toString(), getOption(options, kv.key, StringRef()).toString());
			}
		}

		forceLinkIndexedSetTests();
		forceLinkDequeTests();
		forceLinkFlowTests();
		forceLinkCoroTests();
		forceLinkMemcpyTests();
		forceLinkMemcpyPerfTests();
		forceLinkStreamCipherTests();
		forceLinkSimExternalConnectionTests();
		forceLinkMutationLogReaderTests();
		forceLinkIThreadPoolTests();
		forceLinkJsonWebKeySetTests();
		forceLinkVersionVectorTests();
		forceLinkRESTClientTests();
		forceLinkRESTUtilsTests();
		forceLinkCompressedIntTests();
		forceLinkCompressionUtilsTest();
		forceLinkAtomicTests();
		forceLinkIdempotencyIdTests();
		forceLinkActorCollectionTests();
		forceLinkDDSketchTests();
		forceLinkWipedStringTests();
		forceLinkRandomKeyValueUtilsTests();
		forceLinkActorFuzzUnitTests();
		forceLinkSimpleCounterTests();
		forceLinkLogSystemRecoveryTests();
		forceLinkIPagerTests();
		forceLinkMockS3ServerTests();
		forceLinkClusterHealthMonitorTests();

#ifdef FLOW_GRPC_ENABLED
		forceLinkGrpcTests();
		forceLinkGrpcTests2();
#endif
	}

	Future<Void> setup(Database const& cx) override {
		platform::eraseDirectoryRecursive(testParams.getDataDir());
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (enabled)
			return runUnitTests();
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

	bool testMatched(std::string const& testName) const {
		if (!StringRef(testName).startsWith(testPattern)) {
			return false;
		}

		for (const auto& ignorePatt : testsIgnored) {
			if (StringRef(testName).startsWith(ignorePatt)) {
				return false;
			}
		}

		return true;
	}

	Future<Void> runUnitTests() {
		std::vector<UnitTest*> tests;

		for (auto test = g_unittests.tests; test != nullptr; test = test->next) {
			if (testMatched(test->name)) {
				++testsAvailable;
				tests.push_back(test);
			}
		}

		std::sort(tests.begin(), tests.end(), [](auto lhs, auto rhs) {
			return std::string_view(lhs->name) < std::string_view(rhs->name);
		});

		fprintf(stdout, "Found %zu tests\n", tests.size());

		if (tests.empty()) {
			TraceEvent(SevError, "NoMatchingUnitTests")
			    .detail("TestPattern", testPattern)
			    .detail("TestsIgnored", testsIgnored);
			++testsFailed;
			co_return;
		}

		deterministicRandom()->randomShuffle(tests);
		if (testRunLimit > 0 && tests.size() > testRunLimit)
			tests.resize(testRunLimit);

		std::vector<UnitTest*>::iterator t;
		for (t = tests.begin(); t != tests.end(); ++t) {
			UnitTest* test = *t;
			printf("Testing %s\n", test->name);

			TraceEvent(SevInfo, "RunningUnitTest")
			    .detail("Name", test->name)
			    .detail("File", test->file)
			    .detail("Line", test->line)
			    .detail("Rand", deterministicRandom()->randomInt(0, 100001));

			Error result = success();
			double start_now = now();
			double start_timer = timer();

			platform::createDirectory(testParams.getDataDir());
			try {
				co_await test->func(testParams);
			} catch (Error& e) {
				++testsFailed;
				result = e;
			}
			if (cleanupAfterTests) {
				platform::eraseDirectoryRecursive(testParams.getDataDir());
			}
			++testsExecuted;
			double wallTime = timer() - start_timer;
			double simTime = now() - start_now;

			totalWallTime += wallTime;
			totalSimTime += simTime;
			TraceEvent(result.code() != error_code_success ? SevError : SevInfo, "UnitTest")
			    .errorUnsuppressed(result)
			    .detail("Name", test->name)
			    .detail("File", test->file)
			    .detail("Line", test->line)
			    .detail("WallTime", wallTime)
			    .detail("FlowTime", simTime);
		}
	}
};

WorkloadFactory<UnitTestWorkload> UnitTestWorkloadFactory;

TEST_CASE("/fdbserver/UnitTestWorkload/long delay") {
	co_await delay(60);
	co_return;
}
