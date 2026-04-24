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
#include "SimpleOpt/SimpleOpt.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
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

enum FlowTestOption {
	OPT_HELP,
	OPT_FILTER,
	OPT_IGNORE,
	OPT_DATA_DIR,
	OPT_SEED,
	OPT_MAX_TEST_CASES,
	OPT_NO_CLEANUP,
	OPT_LIST,
};

CSimpleOpt::SOption flowTestOptions[] = { { OPT_HELP, "-h", SO_NONE },
	                                      { OPT_HELP, "--help", SO_NONE },
	                                      { OPT_FILTER, "-f", SO_REQ_SEP },
	                                      { OPT_FILTER, "--filter", SO_REQ_SEP },
	                                      { OPT_IGNORE, "--ignore", SO_REQ_SEP },
	                                      { OPT_DATA_DIR, "--data-dir", SO_REQ_SEP },
	                                      { OPT_SEED, "--seed", SO_REQ_SEP },
	                                      { OPT_MAX_TEST_CASES, "--max-test-cases", SO_REQ_SEP },
	                                      { OPT_NO_CLEANUP, "--no-cleanup", SO_NONE },
	                                      { OPT_LIST, "--list", SO_NONE },
	                                      SO_END_OF_OPTIONS };

void printUsage(const char* program) {
	fmt::print(stderr,
	           "Usage: {} [OPTIONS]\n"
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
	CSimpleOpt args(argc, argv, flowTestOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
	while (args.Next()) {
		if (auto err = args.LastError()) {
			switch (err) {
			case SO_ARG_INVALID_DATA:
				fmt::print(stderr, "ERROR: Invalid argument to option `{}`\n", args.OptionText());
				break;
			case SO_ARG_INVALID:
				fmt::print(stderr, "ERROR: Argument given to no-argument option `{}`\n", args.OptionText());
				break;
			case SO_ARG_MISSING:
				fmt::print(stderr, "ERROR: Argument missing for option `{}`\n", args.OptionText());
				break;
			case SO_OPT_INVALID:
				fmt::print(stderr, "ERROR: Unknown option `{}`\n", args.OptionText());
				break;
			default:
				fmt::print(
				    stderr, "ERROR: Unknown error {} with option `{}`\n", static_cast<int>(err), args.OptionText());
				break;
			}
			return false;
		}

		switch (args.OptionId()) {
		case OPT_HELP:
			options->showHelp = true;
			return true;
		case OPT_FILTER:
			options->testPattern = args.OptionArg();
			break;
		case OPT_IGNORE:
			options->testsIgnored.emplace_back(args.OptionArg());
			break;
		case OPT_DATA_DIR:
			options->dataDir = args.OptionArg();
			break;
		case OPT_SEED:
			if (!parseUInt32(args.OptionArg(), &options->randomSeed)) {
				fmt::print(stderr, "ERROR: --seed requires a uint32 value\n");
				return false;
			}
			break;
		case OPT_MAX_TEST_CASES:
			if (!parseInt(args.OptionArg(), &options->maxTestCases)) {
				fmt::print(stderr, "ERROR: --max-test-cases requires an integer value\n");
				return false;
			}
			break;
		case OPT_NO_CLEANUP:
			options->cleanupAfterTests = false;
			break;
		case OPT_LIST:
			options->listTests = true;
			break;
		default:
			fmt::print(stderr, "ERROR: Unknown option id {}\n", args.OptionId());
			return false;
		}
	}

	if (args.FileCount() > 0) {
		fmt::print(stderr, "ERROR: Unexpected argument `{}`\n", args.File(0));
		return false;
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

	fmt::print(stdout, "Found {} flow tests\n", tests.size());

	if (options.listTests) {
		for (auto test : tests) {
			fmt::print(stdout, "{}\n", test->name);
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
		fmt::print(stdout, "Testing {}\n", test->name);

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
			fmt::print(stderr, "Test failed: {}: {}\n", test->name, resultCode.what());
		}
	}
}

Future<Void> stopNetworkAfter(Future<Void> what, int* exitCode) {
	try {
		co_await what;
	} catch (Error& e) {
		fmt::print(stderr, "Unexpected flow_test error: {}\n", e.what());
		*exitCode = 1;
	} catch (std::exception& e) {
		fmt::print(stderr, "Unexpected flow_test exception: {}\n", e.what());
		*exitCode = 1;
	} catch (...) {
		fmt::print(stderr, "Unexpected flow_test exception\n");
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
	fmt::print(stdout, "Random seed is {}\n", options.randomSeed);

	std::string originalWorkingDirectory = platform::getWorkingDirectory();
	std::string runDirectory = joinPath("/tmp", format("flow_test.%d.%u", ::getpid(), options.randomSeed));
	platform::createDirectory(runDirectory);
	if (::chdir(runDirectory.c_str()) != 0) {
		fmt::print(stderr, "ERROR: Could not chdir to {}\n", runDirectory);
		return 1;
	}

	g_network = newNet2(TLSConfig());
	openTraceFile({}, 10 << 20, 10 << 20, ".", "flow_test");

	int exitCode = 0;
	FlowTestResult result;
	Future<Void> done = stopNetworkAfter(runFlowTests(options, &result), &exitCode);
	g_network->run();
	flushTraceFileVoid();

	fmt::print(
	    stdout, "\n{} tests passed; {} tests failed.\n", result.testsExecuted - result.testsFailed, result.testsFailed);

	if (result.testsFailed != 0) {
		exitCode = 1;
	}

	if (::chdir(originalWorkingDirectory.c_str()) == 0) {
		platform::eraseDirectoryRecursive(runDirectory);
	}
	return exitCode;
}
