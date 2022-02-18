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

#include "TesterOptions.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/ArgParseUtil.h"

#define FDB_API_VERSION 710
#include "bindings/c/foundationdb/fdb_c.h"

namespace FDBSystemTester {

const CSimpleOpt::SOption TesterOptions::optionDefs[] = //
    { { OPT_CONNFILE, "-C", SO_REQ_SEP },
	  { OPT_CONNFILE, "--cluster-file", SO_REQ_SEP },
	  { OPT_TRACE, "--log", SO_NONE },
	  { OPT_TRACE_DIR, "--log-dir", SO_REQ_SEP },
	  { OPT_LOGGROUP, "--log-group", SO_REQ_SEP },
	  { OPT_HELP, "-h", SO_NONE },
	  { OPT_HELP, "--help", SO_NONE },
	  { OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	  { OPT_KNOB, "--knob-", SO_REQ_SEP },
	  { OPT_API_VERSION, "--api-version", SO_REQ_SEP } };

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
	       "  -h, --help     Display this help and exit.\n");
}

bool TesterOptions::parseArgs(int argc, char** argv) {
	// declare our options parser, pass in the arguments from main
	// as well as our array of valid options.
	CSimpleOpt args(argc, argv, optionDefs);

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

bool TesterOptions::processArg(const CSimpleOpt& args) {
	switch (args.OptionId()) {
	case OPT_CONNFILE:
		clusterFile = args.OptionArg();
		break;
	case OPT_API_VERSION: {
		char* endptr;
		api_version = strtoul((char*)args.OptionArg(), &endptr, 10);
		if (*endptr != '\0') {
			fprintf(stderr, "ERROR: invalid client version %s\n", args.OptionArg());
			return 1;
		} else if (api_version < 700 || api_version > FDB_API_VERSION) {
			// multi-version fdbcli only available after 7.0
			fprintf(stderr,
			        "ERROR: api version %s is not supported. (Min: 700, Max: %d)\n",
			        args.OptionArg(),
			        FDB_API_VERSION);
			return 1;
		}
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
			return FDB_EXIT_ERROR;
		}
		knobs.emplace_back(knobName.get(), args.OptionArg());
		break;
	}
	}
	return true;
}

} // namespace FDBSystemTester

using namespace FDBSystemTester;

int main(int argc, char** argv) {
	TesterOptions options;
	if (!options.parseArgs(argc, argv)) {
		return 1;
	}

	return 0;
}
