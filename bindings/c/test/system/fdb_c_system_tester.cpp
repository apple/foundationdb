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
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/ArgParseUtil.h"
#include "test/system/SysTestScheduler.h"
#include "test/system/SysTestTransactionExecutor.h"
#include <iostream>
#include <thread>

#define FDB_API_VERSION 710
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
	OPT_NUM_DATABASES
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
	  { OPT_NUM_DATABASES, "--num-databases", SO_REQ_SEP } };

} // namespace

void TesterOptions::printProgramUsage(const char* execName) {
	printf("usage: %s [OPTIONS]\n"
	       "\n",
	       execName);
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is first the value of the\n"
	       "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
	       "                 then `%s'.\n",
	       platform::getDefaultClusterFilePath().c_str());
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
	       "  --num-client-threads NUM_THREADS\n"
	       "                 Number of threads to be used for execution of client workloads.\n"
	       "  --num-databases NUM_DB\n"
	       "                 Number of database connections to be used concurrently.\n"
	       "  -h, --help     Display this help and exit.\n");
}

bool TesterOptions::parseArgs(int argc, char** argv) {
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
			if (!processArg(args)) {
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

namespace {

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

} // namespace

bool TesterOptions::processArg(const CSimpleOpt& args) {
	switch (args.OptionId()) {
	case OPT_CONNFILE:
		clusterFile = args.OptionArg();
		break;
	case OPT_API_VERSION: {
		// multi-version fdbcli only available after 7.0
		processIntArg(args, api_version, 700, FDB_API_VERSION);
		break;
	}
	case OPT_TRACE:
		trace = true;
		break;
	case OPT_TRACE_DIR:
		traceDir = args.OptionArg();
		break;
	case OPT_LOGGROUP:
		logGroup = args.OptionArg();
		break;
	case OPT_TRACE_FORMAT:
		if (!validateTraceFormat(args.OptionArg())) {
			fprintf(stderr, "WARNING: Unrecognized trace format `%s'\n", args.OptionArg());
		}
		traceFormat = args.OptionArg();
		break;
	case OPT_KNOB: {
		Optional<std::string> knobName = extractPrefixedArgument("--knob", args.OptionSyntax());
		if (!knobName.present()) {
			fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", args.OptionSyntax());
			return false;
		}
		knobs.emplace_back(knobName.get(), args.OptionArg());
		break;
	}
	case OPT_BLOCK_ON_FUTURES:
		blockOnFutures = true;
		break;

	case OPT_NUM_CLIENT_THREADS:
		processIntArg(args, numClientThreads, 1, 1000);
		break;

	case OPT_NUM_DATABASES:
		processIntArg(args, numDatabases, 1, 1000);
		break;
	}
	return true;
}

namespace {
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
	if (!options.parseArgs(argc, argv)) {
		return 1;
	}

	fdb_check(fdb_select_api_version(options.api_version));
	fdb_check(fdb_setup_network());

	std::thread network_thread{ &fdb_run_network };

	runApiCorrectness(options);

	fdb_check(fdb_stop_network());
	network_thread.join();
	return 0;
}
