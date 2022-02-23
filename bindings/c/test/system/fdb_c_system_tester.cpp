/*
 * fdb_c_system_tester.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "SysTestOptions.h"
#include "SysTestWorkload.h"
#include "SysTestScheduler.h"
#include "SysTestTransactionExecutor.h"
#include <iostream>
#include <thread>
#include "flow/SimpleOpt.h"
#include "bindings/c/foundationdb/fdb_c.h"

namespace FDBSystemTester {

namespace {

enum TesterOptionId {
	OPT_CONNFILE,
	OPT_HELP,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_LOGGROUP,
	OPT_TRACE_FORMAT,
	OPT_KNOB,
	OPT_API_VERSION,
	OPT_BLOCK_ON_FUTURES,
	OPT_NUM_CLIENT_THREADS,
	OPT_NUM_DATABASES,
	OPT_EXTERNAL_CLIENT_LIBRARY,
	OPT_NUM_FDB_THREADS
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
	  { OPT_API_VERSION, "--api-version", SO_REQ_SEP },
	  { OPT_BLOCK_ON_FUTURES, "--block-on-futures", SO_NONE },
	  { OPT_NUM_CLIENT_THREADS, "--num-client-threads", SO_REQ_SEP },
	  { OPT_NUM_DATABASES, "--num-databases", SO_REQ_SEP },
	  { OPT_EXTERNAL_CLIENT_LIBRARY, "--external-client-library", SO_REQ_SEP },
	  { OPT_NUM_FDB_THREADS, "--num-fdb-threads", SO_REQ_SEP } };

void printProgramUsage(const char* execName) {
	printf("usage: %s [OPTIONS]\n"
	       "\n",
	       execName);
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is `fdb.cluster',\n"
	       "                 then `%s'.\n",
	       "fdb.cluster");
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --log-dir PATH Specifes the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n"
	       "  --log-group LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n"
	       "  --trace-format FORMAT\n"
	       "                 Select the format of the log files. xml (the default) and json\n"
	       "                 are supported. Has no effect unless --log is specified.\n"
	       "  --api-version  APIVERSION\n"
	       "                 Specifies the version of the API for the CLI to use.\n"
	       "  --knob-KNOBNAME KNOBVALUE\n"
	       "                 Changes a knob option. KNOBNAME should be lowercase.\n"
	       "  --block-on-futures\n"
	       "                 Use blocking waits on futures instead of scheduling callbacks.\n"
	       "  --num-client-threads NUMBER\n"
	       "                 Number of threads to be used for execution of client workloads.\n"
	       "  --num-databases NUMBER\n"
	       "                 Number of database connections to be used concurrently.\n"
	       "  --external-client-library FILE_PATH\n"
	       "                 Path to the external client library.\n"
	       "  --num-fdb-threads NUMBER\n"
	       "                 Number of FDB client threads to be created.\n"
	       "  -h, --help     Display this help and exit.\n");
}

bool processIntArg(const CSimpleOpt& args, int& res, int minVal, int maxVal) {
	char* endptr;
	res = strtol(args.OptionArg(), &endptr, 10);
	if (*endptr != '\0') {
		fprintf(stderr, "ERROR: invalid value %s for %s\n", args.OptionArg(), args.OptionText());
		return false;
	}
	if (res < minVal || res > maxVal) {
		fprintf(stderr, "ERROR: value for %s must be between %d and %d\n", args.OptionText(), minVal, maxVal);
		return false;
	}
	return true;
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
	case OPT_API_VERSION: {
		// multi-version fdbcli only available after 7.0
		processIntArg(args, options.api_version, 700, FDB_API_VERSION);
		break;
	}
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
			fprintf(stderr, "WARNING: Unrecognized trace format `%s'\n", args.OptionArg());
		}
		options.traceFormat = args.OptionArg();
		break;
	case OPT_KNOB: {
		std::string knobName;
		if (!extractPrefixedArgument("--knob", args.OptionSyntax(), knobName)) {
			fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", args.OptionSyntax());
			return false;
		}
		options.knobs.emplace_back(knobName, args.OptionArg());
		break;
	}
	case OPT_BLOCK_ON_FUTURES:
		options.blockOnFutures = true;
		break;

	case OPT_NUM_CLIENT_THREADS:
		processIntArg(args, options.numClientThreads, 1, 1000);
		break;

	case OPT_NUM_DATABASES:
		processIntArg(args, options.numDatabases, 1, 1000);
		break;

	case OPT_EXTERNAL_CLIENT_LIBRARY:
		options.externalClientLibrary = args.OptionArg();
		break;

	case OPT_NUM_FDB_THREADS:
		processIntArg(args, options.numFdbThreads, 1, 1000);
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
			printf("Invalid argument: %s\n", args.OptionText());
			printProgramUsage(argv[0]);
			return false;
		}
	}
	return true;
}

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}
} // namespace

IWorkload* createApiCorrectnessWorkload();

} // namespace FDBSystemTester

using namespace FDBSystemTester;

void applyNetworkOptions(TesterOptions& options) {
	if (!options.externalClientLibrary.empty()) {
		fdb_check(FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT));
		fdb_check(
		    FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY, options.externalClientLibrary));
	}

	if (options.numFdbThreads > 1) {
		fdb_check(
		    FdbApi::setOption(FDBNetworkOption::FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, options.numFdbThreads));
	}
}

void runApiCorrectness(TesterOptions& options) {
	TransactionExecutorOptions txExecOptions;
	txExecOptions.blockOnFutures = options.blockOnFutures;
	txExecOptions.numDatabases = options.numDatabases;

	IScheduler* scheduler = createScheduler(options.numClientThreads);
	ITransactionExecutor* txExecutor = createTransactionExecutor();
	scheduler->start();
	txExecutor->init(scheduler, options.clusterFile.c_str(), txExecOptions);
	IWorkload* workload = createApiCorrectnessWorkload();
	workload->init(txExecutor, scheduler, [scheduler]() { scheduler->stop(); });
	workload->start();
	scheduler->join();

	delete workload;
	delete txExecutor;
	delete scheduler;
}

int main(int argc, char** argv) {
	TesterOptions options;
	if (!parseArgs(options, argc, argv)) {
		return 1;
	}

	fdb_check(fdb_select_api_version(options.api_version));
	applyNetworkOptions(options);
	fdb_check(fdb_setup_network());

	std::thread network_thread{ &fdb_run_network };

	runApiCorrectness(options);

	fdb_check(fdb_stop_network());
	network_thread.join();
	return 0;
}
