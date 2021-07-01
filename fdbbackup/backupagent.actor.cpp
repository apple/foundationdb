/*
 * backupagent.actor.cpp
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

extern const char* getSourceVersion();

namespace {

ACTOR Future<Void> runAgent(Database db) {
	state double pollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
	state Future<Void> status = statusUpdateActor(db, "backup", AgentType::FILE, pollDelay);

	state FileBackupAgent backupAgent;

	loop {
		try {
			wait(backupAgent.run(db, &pollDelay, CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT));
			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				throw;

			TraceEvent(SevError, "BA_runAgent").error(e);
			fprintf(stderr, "ERROR: backup agent encountered fatal error `%s'\n", e.what());

			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
		}
	}

	return Void();
}

class BackupAgentDriver : public Driver<BackupAgentDriver> {
	static CSimpleOpt::SOption const rgOptions[];
	std::string clusterFile;
	Database db;
	LocalityData localities;

public:
	void processArg(CSimpleOpt const& args) {
		auto optId = args.OptionId();
		switch (optId) {
		case OPT_CLUSTERFILE:
			clusterFile = args.OptionArg();
			break;
		case OPT_LOCALITY:
			processLocalityArg(args, localities);
			break;
		case OPT_BLOB_CREDENTIALS:
			addBlobCredentials(args.OptionArg());
			break;
		default:
			break;
		}
	}

	void parseCommandLineArgs(int argc, char** argv) {
		auto args = std::make_unique<CSimpleOpt>(argc, argv, rgOptions, SO_O_EXACT);
		processArgs(*args);
	}

	bool setup() {
		auto _db = initCluster(clusterFile, localities, quietDisplay);
		if (!_db.present()) {
			return false;
		}
		db = _db.get();
		return true;
	}

	Future<Optional<Void>> run() { return stopAfter(runAgent(db)); }

	static std::string getProgramName() { return "backup_agent"; }
};

} // namespace

int main(int argc, char** argv) {
	return commonMain<BackupAgentDriver>(argc, argv);
}

CSimpleOpt::SOption const BackupAgentDriver::rgOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
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
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};
