/*
 * fdb_c_api_tester.cpp
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

#include "TesterOptions.h"
#include "TesterWorkload.h"
#include "TesterScheduler.h"
#include "TesterTransactionExecutor.h"
#include "TesterTestSpec.h"
#include "TesterUtil.h"
#include "flow/SimpleOpt.h"
#include "bindings/c/foundationdb/fdb_c.h"

#include <memory>
#include <stdexcept>
#include <thread>
#include <fmt/format.h>

namespace FdbApiTester {

namespace {

enum TesterOptionId {
	OPT_CONNFILE,
	OPT_HELP,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_LOGGROUP,
	OPT_TRACE_FORMAT,
	OPT_KNOB,
	OPT_EXTERNAL_CLIENT_LIBRARY,
	OPT_TEST_FILE
};

CSimpleOpt::SOption TesterOptionDefs[] = //
    { { OPT_CONNFILE, "-C", SO_REQ_SEP },
	  { OPT_CONNFILE, "--cluster-file", SO_REQ_SEP },
	  { OPT_TRACE, "--log", SO_NONE },
	  { OPT_TRACE_DIR, "--log-dir", SO_REQ_SEP },
	  { OPT_LOGGROUP, "--log-group", SO_REQ_SEP },
	  { OPT_HELP, "-h", SO_NONE },
	  { OPT_HELP, "--help", SO_NONE },
	  { OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	  { OPT_KNOB, "--knob-", SO_REQ_SEP },
	  { OPT_EXTERNAL_CLIENT_LIBRARY, "--external-client-library", SO_REQ_SEP },
	  { OPT_TEST_FILE, "-f", SO_REQ_SEP },
	  { OPT_TEST_FILE, "--test-file", SO_REQ_SEP },
	  SO_END_OF_OPTIONS };

void printProgramUsage(const char* execName) {
	printf("usage: %s [OPTIONS]\n"
	       "\n",
	       execName);
	printf("  -C, --cluster-file FILE\n"
	       "                 The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is `fdb.cluster'\n"
	       "  --log          Enables trace file logging for the CLI session.\n"
	       "  --log-dir PATH Specifes the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n"
	       "  --log-group LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n"
	       "  --trace-format FORMAT\n"
	       "                 Select the format of the log files. xml (the default) and json\n"
	       "                 are supported. Has no effect unless --log is specified.\n"
	       "  --knob-KNOBNAME KNOBVALUE\n"
	       "                 Changes a knob option. KNOBNAME should be lowercase.\n"
	       "  --external-client-library FILE\n"
	       "                 Path to the external client library.\n"
	       "  -f, --test-file FILE\n"
	       "                 Test file to run.\n"
	       "  -h, --help     Display this help and exit.\n");
}

// Extracts the key for command line arguments that are specified with a prefix (e.g. --knob-).
// This function converts any hyphens in the extracted key to underscores.
bool extractPrefixedArgument(std::string prefix, const std::string& arg, std::string& res) {
	if (arg.size() <= prefix.size() || arg.find(prefix) != 0 ||
	    (arg[prefix.size()] != '-' && arg[prefix.size()] != '_')) {
		return false;
	}

	res = arg.substr(prefix.size() + 1);
	std::transform(res.begin(), res.end(), res.begin(), [](int c) { return c == '-' ? '_' : c; });
	return true;
}

bool validateTraceFormat(std::string_view format) {
	return format == "xml" || format == "json";
}

bool processArg(TesterOptions& options, const CSimpleOpt& args) {
	switch (args.OptionId()) {
	case OPT_CONNFILE:
		options.clusterFile = args.OptionArg();
		break;
	case OPT_TRACE:
		options.trace = true;
		break;
	case OPT_TRACE_DIR:
		options.traceDir = args.OptionArg();
		break;
	case OPT_LOGGROUP:
		options.logGroup = args.OptionArg();
		break;
	case OPT_TRACE_FORMAT:
		if (!validateTraceFormat(args.OptionArg())) {
			fmt::print(stderr, "ERROR: Unrecognized trace format `{}'\n", args.OptionArg());
			return false;
		}
		options.traceFormat = args.OptionArg();
		break;
	case OPT_KNOB: {
		std::string knobName;
		if (!extractPrefixedArgument("--knob", args.OptionSyntax(), knobName)) {
			fmt::print(stderr, "ERROR: unable to parse knob option '{}'\n", args.OptionSyntax());
			return false;
		}
		options.knobs.emplace_back(knobName, args.OptionArg());
		break;
	}
	case OPT_EXTERNAL_CLIENT_LIBRARY:
		options.externalClientLibrary = args.OptionArg();
		break;

	case OPT_TEST_FILE:
		options.testFile = args.OptionArg();
		options.testSpec = readTomlTestSpec(options.testFile);
		break;
	}
	return true;
}

bool parseArgs(TesterOptions& options, int argc, char** argv) {
	// declare our options parser, pass in the arguments from main
	// as well as our array of valid options.
	CSimpleOpt args(argc, argv, TesterOptionDefs);

	// while there are arguments left to process
	while (args.Next()) {
		if (args.LastError() == SO_SUCCESS) {
			if (args.OptionId() == OPT_HELP) {
				printProgramUsage(argv[0]);
				return false;
			}
			if (!processArg(options, args)) {
				return false;
			}
		} else {
			fmt::print(stderr, "ERROR: Invalid argument: {}\n", args.OptionText());
			printProgramUsage(argv[0]);
			return false;
		}
	}
	return true;
}

void fdb_check(fdb_error_t e) {
	if (e) {
		fmt::print(stderr, "Unexpected FDB error: {}({})\n", e, fdb_get_error(e));
		std::abort();
	}
}

void applyNetworkOptions(TesterOptions& options) {
	if (!options.externalClientLibrary.empty()) {
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT));
		fdb_check(
		    FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY, options.externalClientLibrary));
	}

	if (options.testSpec.multiThreaded) {
		fdb_check(
		    FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, options.numFdbThreads));
	}

	if (options.testSpec.fdbCallbacksOnExternalThreads) {
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_CALLBACKS_ON_EXTERNAL_THREADS));
	}

	if (options.testSpec.buggify) {
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE));
	}

	if (options.trace) {
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_ENABLE, options.traceDir));
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_FORMAT, options.traceFormat));
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_LOG_GROUP, options.logGroup));
	}

	for (auto knob : options.knobs) {
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_KNOB,
		                            fmt::format("{}={}", knob.first.c_str(), knob.second.c_str())));
	}
}

void randomizeOptions(TesterOptions& options) {
	Random& random = Random::get();
	options.numFdbThreads = random.randomInt(options.testSpec.minFdbThreads, options.testSpec.maxFdbThreads);
	options.numClientThreads = random.randomInt(options.testSpec.minClientThreads, options.testSpec.maxClientThreads);
	options.numDatabases = random.randomInt(options.testSpec.minDatabases, options.testSpec.maxDatabases);
	options.numClients = random.randomInt(options.testSpec.minClients, options.testSpec.maxClients);
}

bool runWorkloads(TesterOptions& options) {
	TransactionExecutorOptions txExecOptions;
	txExecOptions.blockOnFutures = options.testSpec.blockOnFutures;
	txExecOptions.numDatabases = options.numDatabases;
	txExecOptions.databasePerTransaction = options.testSpec.databasePerTransaction;

	std::unique_ptr<IScheduler> scheduler = createScheduler(options.numClientThreads);
	std::unique_ptr<ITransactionExecutor> txExecutor = createTransactionExecutor(txExecOptions);
	scheduler->start();
	txExecutor->init(scheduler.get(), options.clusterFile.c_str());

	WorkloadManager workloadMgr(txExecutor.get(), scheduler.get());
	for (const auto& workloadSpec : options.testSpec.workloads) {
		for (int i = 0; i < options.numClients; i++) {
			WorkloadConfig config;
			config.name = workloadSpec.name;
			config.options = workloadSpec.options;
			config.clientId = i;
			config.numClients = options.numClients;
			std::shared_ptr<IWorkload> workload = IWorkloadFactory::create(workloadSpec.name, config);
			if (!workload) {
				throw TesterError(fmt::format("Unknown workload '{}'", workloadSpec.name));
			}
			workloadMgr.add(workload);
		}
	}

	workloadMgr.run();
	return !workloadMgr.failed();
}

} // namespace
} // namespace FdbApiTester

using namespace FdbApiTester;

int main(int argc, char** argv) {
	int retCode = 0;
	try {
		TesterOptions options;
		if (!parseArgs(options, argc, argv)) {
			return 1;
		}
		randomizeOptions(options);

		fdb_check(fdb_select_api_version(options.testSpec.apiVersion));
		applyNetworkOptions(options);
		fdb_check(fdb_setup_network());

		std::thread network_thread{ &fdb_run_network };

		if (!runWorkloads(options)) {
			retCode = 1;
		}

		fdb_check(fdb_stop_network());
		network_thread.join();
	} catch (const std::runtime_error& err) {
		fmt::print(stderr, "ERROR: {}\n", err.what());
		retCode = 1;
	}
	return retCode;
}
