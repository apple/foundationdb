/*
 * FlowTest.cpp
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

#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/network.h"

#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace {

struct FlowTestOptions {
	std::string testPattern;
	std::vector<std::string> testsIgnored;
	std::string dataDir = "flow_test_data";
	uint32_t randomSeed = 0;
	int maxTestCases = -1;
	bool cleanupAfterTests = true;
	bool listTests = false;
	bool showHelp = false;
};

struct FlowTestResult {
	int testsAvailable = 0;
	int testsExecuted = 0;
	int testsFailed = 0;
};

void printUsage(const char* program) {
	fprintf(stderr,
	        "Usage: %s [OPTIONS]\n"
	        "\n"
	        "Run TEST_CASEs linked into the flow library.\n"
	        "\n"
	        "Options:\n"
	        "  -f, --filter PREFIX       Run tests whose names start with PREFIX\n"
	        "      --ignore PREFIX       Skip tests whose names start with PREFIX\n"
	        "      --data-dir DIR        Per-test data directory (default: flow_test_data)\n"
	        "      --seed N              Deterministic random seed (default: random)\n"
	        "      --max-test-cases N    Stop after N matching tests\n"
	        "      --no-cleanup          Keep the data directory after each test\n"
	        "      --list                Print matching test names without running them\n"
	        "  -h, --help                Show this help\n",
	        program);
}

bool parseInt(const char* text, int* value) {
	char* end = nullptr;
	long parsed = strtol(text, &end, 10);
	if (*text == '\0' || *end != '\0') {
		return false;
	}
	*value = static_cast<int>(parsed);
	return true;
}

bool parseUInt32(const char* text, uint32_t* value) {
	char* end = nullptr;
	unsigned long parsed = strtoul(text, &end, 10);
	if (*text == '\0' || *end != '\0' || parsed > UINT32_MAX) {
		return false;
	}
	*value = static_cast<uint32_t>(parsed);
	return true;
}

bool parseArgs(int argc, char** argv, FlowTestOptions* options) {
	for (int i = 1; i < argc; ++i) {
		auto nextArg = [&](const char* name) -> const char* {
			if (i + 1 >= argc) {
				fprintf(stderr, "ERROR: %s requires an argument\n", name);
				return nullptr;
			}
			return argv[++i];
		};

		if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
			options->showHelp = true;
			return true;
		} else if (!strcmp(argv[i], "-f") || !strcmp(argv[i], "--filter")) {
			const char* value = nextArg(argv[i]);
			if (!value) {
				return false;
			}
			options->testPattern = value;
		} else if (!strcmp(argv[i], "--ignore")) {
			const char* value = nextArg(argv[i]);
			if (!value) {
				return false;
			}
			options->testsIgnored.emplace_back(value);
		} else if (!strcmp(argv[i], "--data-dir")) {
			const char* value = nextArg(argv[i]);
			if (!value) {
				return false;
			}
			options->dataDir = value;
		} else if (!strcmp(argv[i], "--seed")) {
			const char* value = nextArg(argv[i]);
			if (!value || !parseUInt32(value, &options->randomSeed)) {
				fprintf(stderr, "ERROR: --seed requires a uint32 value\n");
				return false;
			}
		} else if (!strcmp(argv[i], "--max-test-cases")) {
			const char* value = nextArg(argv[i]);
			if (!value || !parseInt(value, &options->maxTestCases)) {
				fprintf(stderr, "ERROR: --max-test-cases requires an integer value\n");
				return false;
			}
		} else if (!strcmp(argv[i], "--no-cleanup")) {
			options->cleanupAfterTests = false;
		} else if (!strcmp(argv[i], "--list")) {
			options->listTests = true;
		} else {
			fprintf(stderr, "ERROR: Unknown option `%s'\n", argv[i]);
			return false;
		}
	}
	return true;
}

bool startsWith(std::string_view value, std::string_view prefix) {
	return value.size() >= prefix.size() && value.substr(0, prefix.size()) == prefix;
}

bool isFlowSource(std::string_view file) {
	return startsWith(file, "flow/") || file.find("/flow/") != std::string_view::npos;
}

bool testMatched(const FlowTestOptions& options, std::string_view testName) {
	if (!startsWith(testName, options.testPattern)) {
		return false;
	}

	for (const auto& ignorePattern : options.testsIgnored) {
		if (startsWith(testName, ignorePattern)) {
			return false;
		}
	}

	return true;
}

std::vector<UnitTest*> collectTests(const FlowTestOptions& options) {
	std::vector<UnitTest*> tests;
	for (auto test = g_unittests.tests; test != nullptr; test = test->next) {
		if (isFlowSource(test->file) && testMatched(options, test->name)) {
			tests.push_back(test);
		}
	}

	std::sort(tests.begin(), tests.end(), [](auto lhs, auto rhs) {
		return std::string_view(lhs->name) < std::string_view(rhs->name);
	});
	return tests;
}

Future<Void> runFlowTests(const FlowTestOptions& options, FlowTestResult* result) {
	std::vector<UnitTest*> tests = collectTests(options);
	result->testsAvailable = tests.size();

	fprintf(stdout, "Found %zu flow tests\n", tests.size());

	if (options.listTests) {
		for (auto test : tests) {
			fprintf(stdout, "%s\n", test->name);
		}
		co_return;
	}

	if (tests.empty()) {
		TraceEvent(SevError, "NoMatchingFlowTests").detail("TestPattern", options.testPattern);
		++result->testsFailed;
		co_return;
	}

	if (options.maxTestCases > 0 && tests.size() > static_cast<size_t>(options.maxTestCases)) {
		tests.resize(options.maxTestCases);
	}

	UnitTestParameters testParams;
	testParams.setDataDir(options.dataDir);

	for (auto test : tests) {
		fprintf(stdout, "Testing %s\n", test->name);

		TraceEvent(SevInfo, "RunningFlowTest")
		    .detail("Name", test->name)
		    .detail("File", test->file)
		    .detail("Line", test->line)
		    .detail("Rand", deterministicRandom()->randomInt(0, 100001));

		Error resultCode = success();
		double startNow = now();
		double startTimer = timer();

		platform::createDirectory(testParams.getDataDir());
		try {
			co_await test->func(testParams);
		} catch (Error& e) {
			resultCode = e;
			++result->testsFailed;
		}
		if (options.cleanupAfterTests) {
			platform::eraseDirectoryRecursive(testParams.getDataDir());
		}
		++result->testsExecuted;

		double wallTime = timer() - startTimer;
		double flowTime = now() - startNow;
		TraceEvent(resultCode.code() != error_code_success ? SevError : SevInfo, "FlowTest")
		    .errorUnsuppressed(resultCode)
		    .detail("Name", test->name)
		    .detail("File", test->file)
		    .detail("Line", test->line)
		    .detail("WallTime", wallTime)
		    .detail("FlowTime", flowTime);

		if (resultCode.code() != error_code_success) {
			fprintf(stderr, "Test failed: %s: %s\n", test->name, resultCode.what());
		}
	}
}

Future<Void> stopNetworkAfter(Future<Void> what, int* exitCode) {
	try {
		co_await what;
	} catch (Error& e) {
		fprintf(stderr, "Unexpected flow_test error: %s\n", e.what());
		*exitCode = 1;
	} catch (std::exception& e) {
		fprintf(stderr, "Unexpected flow_test exception: %s\n", e.what());
		*exitCode = 1;
	} catch (...) {
		fprintf(stderr, "Unexpected flow_test exception\n");
		*exitCode = 1;
	}
	g_network->stop();
}

} // namespace

int main(int argc, char** argv) {
	platformInit();
	Error::init();
	setvbuf(stdout, nullptr, _IOLBF, BUFSIZ);
	setvbuf(stderr, nullptr, _IOLBF, BUFSIZ);

	FlowTestOptions options;
	if (!parseArgs(argc, argv, &options)) {
		printUsage(argv[0]);
		return 1;
	}
	if (options.showHelp) {
		printUsage(argv[0]);
		return 0;
	}

	if (options.randomSeed == 0) {
		options.randomSeed = platform::getRandomSeed();
	}
	setThreadLocalDeterministicRandomSeed(options.randomSeed);
	fprintf(stdout, "Random seed is %u\n", options.randomSeed);

	std::string originalWorkingDirectory = platform::getWorkingDirectory();
	std::string runDirectory = joinPath("/tmp", format("flow_test.%d.%u", ::getpid(), options.randomSeed));
	platform::createDirectory(runDirectory);
	if (::chdir(runDirectory.c_str()) != 0) {
		fprintf(stderr, "ERROR: Could not chdir to %s\n", runDirectory.c_str());
		return 1;
	}

	g_network = newNet2(TLSConfig());
	openTraceFile({}, 10 << 20, 10 << 20, ".", "flow_test");

	int exitCode = 0;
	FlowTestResult result;
	Future<Void> done = stopNetworkAfter(runFlowTests(options, &result), &exitCode);
	g_network->run();
	flushTraceFileVoid();

	fprintf(
	    stdout, "\n%d tests passed; %d tests failed.\n", result.testsExecuted - result.testsFailed, result.testsFailed);

	if (result.testsFailed != 0) {
		exitCode = 1;
	}

	if (::chdir(originalWorkingDirectory.c_str()) == 0) {
		platform::eraseDirectoryRecursive(runDirectory);
	}
	return exitCode;
}
