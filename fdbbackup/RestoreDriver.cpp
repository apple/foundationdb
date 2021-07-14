/*
 * RestoreCommon.cpp
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

#include "fdbbackup/BackupRestoreCommon.h"
#include "fdbbackup/RestoreDriver.h"
#include "fdbclient/versions.h"
#include "flow/TLSConfig.actor.h"

#include <string>

void printRestoreUsage(std::string const& programName, bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [TOP_LEVEL_OPTIONS] (start | status | abort | wait) [OPTIONS]\n\n", programName.c_str());

	printf(" TOP LEVEL OPTIONS:\n");
	printf("  --build_flags  Print build information and exit.\n");
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");
	printf("\n");

	printf(" ACTION OPTIONS:\n");
	// printf("  FOLDERS        Paths to folders containing the backup files.\n");
	printf("  Options for all commands:\n\n");
	printf("  --dest_cluster_file CONNFILE\n");
	printf("                 The cluster file to restore data into.\n");
	printf("  -t, --tagname TAGNAME\n");
	printf("                 The restore tag to act on.  Default is 'default'\n");
	printf("\n");
	printf("  Options for start:\n\n");
	printf("  -r URL         The Backup URL for the restore to read from.\n");
	printBackupContainerInfo();
	printf("  -w, --waitfordone\n");
	printf("                 Wait for the restore to complete before exiting.  Prints progress updates.\n");
	printf("  -k KEYS        List of key ranges from the backup to restore.\n");
	printf("  --remove_prefix PREFIX\n");
	printf("                 Prefix to remove from the restored keys.\n");
	printf("  --add_prefix PREFIX\n");
	printf("                 Prefix to add to the restored keys\n");
	printf("  -n, --dryrun   Perform a trial run with no changes made.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH  Specifies the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
	       "                 Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                 Has no effect unless --log is specified.\n");
	printf("  --incremental\n"
	       "                 Performs incremental restore without the base backup.\n"
	       "                 This tells the backup agent to only replay the log files from the backup source.\n"
	       "                 This also allows a restore to be performed into a non-empty destination database.\n");
	printf("  --begin_version\n"
	       "                 To be used in conjunction with incremental restore.\n"
	       "                 Indicates to the backup agent to only begin replaying log files from a certain version, "
	       "instead of the entire set.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
	printf("  -v DBVERSION   The version at which the database will be restored.\n");
	printf("  --timestamp    Instead of a numeric version, use this to specify a timestamp in %s\n",
	       BackupAgentBase::timeFormat().c_str());
	printf("                 and it will be converted to a version from that time using metadata in "
	       "orig_cluster_file.\n");
	printf("  --orig_cluster_file CONNFILE\n");
	printf("                 The cluster file for the original database from which the backup was created.  The "
	       "original database\n");
	printf("                 is only needed to convert a --timestamp argument to a database version.\n");

	if (devhelp) {
#ifdef _WIN32
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
	}

	printf("\n"
	       "  KEYS FORMAT:   \"<BEGINKEY> <ENDKEY>\" [...]\n");
	printf("\n");
	puts(BlobCredentialInfo);
}

RestoreType getRestoreType(std::string const& name) {
	if (name == "start")
		return RestoreType::START;
	if (name == "abort")
		return RestoreType::ABORT;
	if (name == "status")
		return RestoreType::STATUS;
	if (name == "wait")
		return RestoreType::WAIT;
	return RestoreType::UNKNOWN;
}

CSimpleOpt::SOption const rgRestoreOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_RESTORE_TIMESTAMP, "--timestamp", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_RESTORE_CLUSTERFILE_DEST, "--dest_cluster_file", SO_REQ_SEP },
	{ OPT_RESTORECONTAINER, "-r", SO_REQ_SEP },
	{ OPT_PREFIX_ADD, "--add_prefix", SO_REQ_SEP },
	{ OPT_PREFIX_REMOVE, "--remove_prefix", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
	{ OPT_WAITFORDONE, "-w", SO_NONE },
	{ OPT_WAITFORDONE, "--waitfordone", SO_NONE },
	{ OPT_RESTORE_VERSION, "--version", SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "-v", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_DRYRUN, "-n", SO_NONE },
	{ OPT_DRYRUN, "--dryrun", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_INCREMENTALONLY, "--incremental", SO_NONE },
	{ OPT_RESTORE_BEGIN_VERSION, "--begin_version", SO_REQ_SEP },
	{ OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY, "--inconsistent_snapshot_only", SO_NONE },
	{ OPT_ENCRYPTION_KEY_FILE, "--encryption_key_file", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

void RestoreDriverState::processArg(std::string const& programName, CSimpleOpt const& args) {
	int optId = args.OptionId();
	switch (optId) {
	case OPT_RESTORE_TIMESTAMP:
		targetTimestamp = args.OptionArg();
		break;
	case OPT_RESTORE_CLUSTERFILE_DEST:
		restoreClusterFileDest = args.OptionArg();
		break;
	case OPT_RESTORE_CLUSTERFILE_ORIG:
		restoreClusterFileOrig = args.OptionArg();
		break;
	case OPT_RESTORECONTAINER:
		restoreContainer = args.OptionArg();
		// If the url starts with '/' then prepend "file://" for backwards compatibility
		if (StringRef(restoreContainer).startsWith(LiteralStringRef("/")))
			restoreContainer = std::string("file://") + restoreContainer;
		break;
	case OPT_PREFIX_ADD:
		addPrefix = args.OptionArg();
		break;
	case OPT_PREFIX_REMOVE:
		removePrefix = args.OptionArg();
		break;
	case OPT_TAGNAME:
		tagName = args.OptionArg();
		break;
	case OPT_INCREMENTALONLY:
		onlyApplyMutationLogs.set(true);
		break;
	case OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY:
		inconsistentSnapshotOnly.set(true);
		break;
	case OPT_RESTORE_BEGIN_VERSION: {
		const char* a = args.OptionArg();
		long long ver = 0;
		if (!sscanf(a, "%lld", &ver)) {
			fprintf(stderr, "ERROR: Could not parse database beginVersion `%s'\n", a);
			printHelpTeaser(programName);
			throw invalid_option_value();
		}
		beginVersion = ver;
		break;
	}
	case OPT_ENCRYPTION_KEY_FILE:
		encryptionKeyFile = args.OptionArg();
		break;
	default:
		break;
	}
}

bool RestoreDriverState::setup() {
	if (restoreClusterFileDest.empty()) {
		fprintf(stderr, "Restore destination cluster file must be specified explicitly.\n");
		return false;
	}

	if (!fileExists(restoreClusterFileDest)) {
		fprintf(stderr, "Restore destination cluster file '%s' does not exist.\n", restoreClusterFileDest.c_str());
		return false;
	}

	try {
		createDatabase();
	} catch (Error& e) {
		fprintf(
		    stderr, "Restore destination cluster file '%s' invalid: %s\n", restoreClusterFileDest.c_str(), e.what());
		return false;
	}

	initializeBackupKeys();

	return true;
}
