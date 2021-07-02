/*
 * backup.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbbackup/AgentDriver.h"
#include "fdbbackup/BackupRestoreCommon.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h" // this must be the last include

namespace {

ACTOR Future<Void> runDBAgent(Database src, Database dest) {
	state double pollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
	std::string id = nondeterministicRandom()->randomUniqueID().toString();
	state Future<Void> status = statusUpdateActor(src, "dr_backup", AgentType::DB, pollDelay, dest, id);
	state Future<Void> status_other = statusUpdateActor(dest, "dr_backup_dest", AgentType::DB, pollDelay, dest, id);

	state DatabaseBackupAgent backupAgent(src);

	loop {
		try {
			wait(backupAgent.run(dest, &pollDelay, CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT));
			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				throw;

			TraceEvent(SevError, "DA_runAgent").error(e);
			fprintf(stderr, "ERROR: DR agent encountered fatal error `%s'\n", e.what());

			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
		}
	}

	return Void();
}

class DrAgentDriver : public Driver<DrAgentDriver> {
	static CSimpleOpt::SOption const rgOptions[];
	std::string destClusterFile;
	Database destDB;
	std::string sourceClusterFile;
	Database sourceDB;
	LocalityData localities;

public:
	static void printUsage(bool devhelp) {
		printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
		printf("Usage: %s [OPTIONS]\n\n", getProgramName().c_str());
		printf("  -d CONNFILE    The path of a file containing the connection string for the\n"
		       "                 destination FoundationDB cluster.\n");
		printf("  -s CONNFILE    The path of a file containing the connection string for the\n"
		       "                 source FoundationDB cluster.\n");
		printf("  --log          Enables trace file logging for the CLI session.\n"
		       "  --logdir PATH  Specifes the output directory for trace files. If\n"
		       "                 unspecified, defaults to the current directory. Has\n"
		       "                 no effect unless --log is specified.\n");
		printf("  --loggroup LOG_GROUP\n"
		       "                 Sets the LogGroup field with the specified value for all\n"
		       "                 events in the trace output (defaults to `default').\n");
		printf("  --trace_format FORMAT\n"
		       "                 Select the void name()ormat of the trace files. xml (the default) and json are "
		       "supported.\n"
		       "                 Has no effect unless --log is specified.\n");
		printf("  -m SIZE, --memory SIZE\n"
		       "                 Memory limit. The default value is 8GiB. When specified\n"
		       "                 without a unit, MiB is assumed.\n");
#ifndef TLS_DISABLED
		printf(TLS_HELP);
#endif
		printf("  --build_flags  Print build information and exit.\n");
		printf("  -v, --version  Print version information and exit.\n");
		printf("  -h, --help     Display this help and exit.\n");
		if (devhelp) {
#ifdef _WIN32
			printf("  -n             Create a new console.\n");
			printf("  -q             Disable error dialog on crash.\n");
			printf("  --parentpid PID\n");
			printf("                 Specify a process after whose termination to exit.\n");
#endif
		}
	}

	void processArg(CSimpleOpt const& args) {
		// TODO: Implement
		auto optId = args.OptionId();
		switch (optId) {
		case OPT_DEST_CLUSTER:
			destClusterFile = args.OptionArg();
			break;
		case OPT_LOCALITY:
			processLocalityArg(args, localities);
			break;
		case OPT_SOURCE_CLUSTER:
			sourceClusterFile = args.OptionArg();
			break;
		default:
			break;
		}
	}

	void parseCommandLineArgs(int argc, char** argv) {
		CSimpleOpt args(argc, argv, rgOptions, SO_O_EXACT);
		processArgs(args);
	}

	bool setup() {
		auto _sourceDB = initCluster(sourceClusterFile, localities, quietDisplay);
		if (!_sourceDB.present()) {
			return false;
		} else {
			sourceDB = _sourceDB.get();
		}
		auto _destDB = initCluster(sourceClusterFile, localities, quietDisplay);
		if (!_destDB.present()) {
			return false;
		} else {
			destDB = _destDB.get();
		}
		return true;
	}

	Future<Optional<Void>> run() { return stopAfter(runDBAgent(sourceDB, destDB)); }

	static std::string getProgramName() { return "dr_agent"; }
};

} // namespace

int main(int argc, char** argv) {
	return commonMain<DrAgentDriver>(argc, argv);
}

CSimpleOpt::SOption const DrAgentDriver::rgOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_VERSION, "--version", SO_NONE },
	{ OPT_VERSION, "-v", SO_NONE },
	{ OPT_BUILD_FLAGS, "--build_flags", SO_NONE },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_LOCALITY, "--locality_", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};
