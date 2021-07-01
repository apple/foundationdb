/*
 * fdbrestore.actor.cpp
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
#include "fdbbackup/BackupTLSConfig.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/versions.h"
#include "flow/TLSConfig.actor.h"

extern const char* getSourceVersion();

namespace {

static std::string const programName = "fdbrestore";

// Submit the restore request to the database if "performRestore" is true. Otherwise,
// check if the restore can be performed.
ACTOR Future<Void> runRestore(Database db,
                              std::string originalClusterFile,
                              std::string tagName,
                              std::string container,
                              Standalone<VectorRef<KeyRangeRef>> ranges,
                              Version beginVersion,
                              Version targetVersion,
                              std::string targetTimestamp,
                              bool performRestore,
                              bool verbose,
                              bool waitForDone,
                              std::string addPrefix,
                              std::string removePrefix,
                              bool onlyApplyMutationLogs,
                              bool inconsistentSnapshotOnly) {
	if (ranges.empty()) {
		ranges.push_back_deep(ranges.arena(), normalKeys);
	}

	if (targetVersion != invalidVersion && !targetTimestamp.empty()) {
		fprintf(stderr, "Restore target version and target timestamp cannot both be specified\n");
		throw restore_error();
	}

	state Optional<Database> origDb;

	// Resolve targetTimestamp if given
	if (!targetTimestamp.empty()) {
		if (originalClusterFile.empty()) {
			fprintf(stderr,
			        "An original cluster file must be given in order to resolve restore target timestamp '%s'\n",
			        targetTimestamp.c_str());
			throw restore_error();
		}

		if (!fileExists(originalClusterFile)) {
			fprintf(
			    stderr, "Original source database cluster file '%s' does not exist.\n", originalClusterFile.c_str());
			throw restore_error();
		}

		origDb = Database::createDatabase(originalClusterFile, Database::API_VERSION_LATEST);
		Version v = wait(timeKeeperVersionFromDatetime(targetTimestamp, origDb.get()));
		printf("Timestamp '%s' resolves to version %" PRId64 "\n", targetTimestamp.c_str(), v);
		targetVersion = v;
	}

	try {
		state FileBackupAgent backupAgent;

		state Reference<IBackupContainer> bc = openBackupContainer(programName.c_str(), container);

		// If targetVersion is unset then use the maximum restorable version from the backup description
		if (targetVersion == invalidVersion) {
			if (verbose)
				printf(
				    "No restore target version given, will use maximum restorable version from backup description.\n");

			BackupDescription desc = wait(bc->describeBackup());

			if (onlyApplyMutationLogs && desc.contiguousLogEnd.present()) {
				targetVersion = desc.contiguousLogEnd.get() - 1;
			} else if (desc.maxRestorableVersion.present()) {
				targetVersion = desc.maxRestorableVersion.get();
			} else {
				fprintf(stderr, "The specified backup is not restorable to any version.\n");
				throw restore_error();
			}

			if (verbose)
				printf("Using target restore version %" PRId64 "\n", targetVersion);
		}

		if (performRestore) {
			Version restoredVersion = wait(backupAgent.restore(db,
			                                                   origDb,
			                                                   KeyRef(tagName),
			                                                   KeyRef(container),
			                                                   ranges,
			                                                   waitForDone,
			                                                   targetVersion,
			                                                   verbose,
			                                                   KeyRef(addPrefix),
			                                                   KeyRef(removePrefix),
			                                                   true,
			                                                   onlyApplyMutationLogs,
			                                                   inconsistentSnapshotOnly,
			                                                   beginVersion));

			if (waitForDone && verbose) {
				// If restore is now complete then report version restored
				printf("Restored to version %" PRId64 "\n", restoredVersion);
			}
		} else {
			state Optional<RestorableFileSet> rset = wait(bc->getRestoreSet(targetVersion, ranges));

			if (!rset.present()) {
				fprintf(stderr,
				        "Insufficient data to restore to version %" PRId64 ".  Describe backup for more information.\n",
				        targetVersion);
				throw restore_invalid_version();
			}

			printf("Backup can be used to restore to version %" PRId64 "\n", targetVersion);
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

class RestoreDriver : public Driver<RestoreDriver> {
	static CSimpleOpt::SOption const rgOptions[];
	enum class RestoreType { UNKNOWN, START, STATUS, ABORT, WAIT } restoreType;

	static RestoreType getRestoreType(std::string const& name) {
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

	Database db;
	std::string restoreTimestamp;
	std::string restoreClusterFileOrig;
	std::string restoreClusterFileDest;
	std::string restoreContainer;
	std::string addPrefix;
	std::string removePrefix;
	Optional<std::string> tagName;
	Standalone<VectorRef<KeyRangeRef>> backupKeys;
	bool waitForDone{ false };
	Version restoreVersion{ ::invalidVersion };

	bool dryRun{ false };
	bool onlyApplyMutationLogs{ false };
	bool inconsistentSnapshotOnly{ false };
	Version beginVersion{ ::invalidVersion };

	static void printUsage(bool devhelp) {
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
		printf(
		    "  --begin_version\n"
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

		return;
	}

public:
	void parseCommandLineArgs(int argc, char** argv) {
		if (argc < 2) {
			printUsage(false);
			// TODO: Add new error code
			throw restore_error();
		}
		// Get the restore operation type
		auto restoreType = getRestoreType(argv[1]);
		if (restoreType == RestoreType::UNKNOWN) {
			// TODO: Handle general case
			ASSERT(false);
		}
		auto args = std::make_unique<CSimpleOpt>(argc - 1, argv + 1, rgOptions, SO_O_EXACT);
		processArgs(*args);
	}

	// Returns true iff setup is successful
	bool setup() {
		if (dryRun) {
			if (restoreType != RestoreType::START) {
				fprintf(stderr, "Restore dry run only works for 'start' command\n");
				return false;
			}

			// Must explicitly call trace file options handling if not calling Database::createDatabase()
			openTraceFile(NetworkAddress{},
			              TRACE_DEFAULT_ROLL_SIZE,
			              TRACE_DEFAULT_MAX_LOGS_SIZE,
			              traceDir,
			              "trace",
			              traceLogGroup);
		} else {
			if (restoreClusterFileDest.empty()) {
				fprintf(stderr, "Restore destination cluster file must be specified explicitly.\n");
				return false;
			}

			if (!fileExists(restoreClusterFileDest)) {
				fprintf(
				    stderr, "Restore destination cluster file '%s' does not exist.\n", restoreClusterFileDest.c_str());
				return false;
			}

			try {
				db = Database::createDatabase(restoreClusterFileDest, Database::API_VERSION_LATEST);
			} catch (Error& e) {
				fprintf(stderr,
				        "Restore destination cluster file '%s' invalid: %s\n",
				        restoreClusterFileDest.c_str(),
				        e.what());
				return false;
			}
		}
		return true;
	}

	Future<Optional<Void>> run() {
		FileBackupAgent ba;

		switch (restoreType) {
		case RestoreType::START:
			return stopAfter(runRestore(db,
			                            restoreClusterFileOrig,
			                            tagName.orDefault(BackupAgentBase::getDefaultTag().toString()),
			                            restoreContainer,
			                            backupKeys,
			                            beginVersion,
			                            restoreVersion,
			                            restoreTimestamp,
			                            !dryRun,
			                            !quietDisplay,
			                            waitForDone,
			                            addPrefix,
			                            removePrefix,
			                            onlyApplyMutationLogs,
			                            inconsistentSnapshotOnly));
		case RestoreType::WAIT:
			return stopAfter(success(
			    ba.waitRestore(db, KeyRef(tagName.orDefault(BackupAgentBase::getDefaultTag().toString())), true)));

		case RestoreType::ABORT:
			return stopAfter(map(ba.abortRestore(db, KeyRef(tagName.orDefault(""))),
			                     [tagName = tagName.orDefault(BackupAgentBase::getDefaultTag().toString())](
			                         FileBackupAgent::ERestoreState s) -> Void {
				                     printf("RESTORE_ABORT Tag: %s  State: %s\n",
				                            tagName.c_str(),
				                            FileBackupAgent::restoreStateText(s).toString().c_str());
				                     return Void();
			                     }));
		case RestoreType::STATUS: {
			Key tag;
			// If no tag is specifically provided then print all tag status, don't just use "default"
			if (tagName.present()) {
				tag = tagName.get();
			}
			return stopAfter(map(ba.restoreStatus(db, KeyRef(tag)), [](std::string const& s) -> Void {
				printf("%s\n", s.c_str());
				return Void();
			}));
		}
		default:
			ASSERT(false);
			return {};
		}
	}

	void processArg(CSimpleOpt& args) {
		int optId = args.OptionId();
		switch (optId) {
		case OPT_RESTORE_TIMESTAMP:
			restoreTimestamp = args.OptionArg();
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
		default:
			break;
		}
	}

	static std::string getProgramName() { return "fdbrestore"; }
};

CSimpleOpt::SOption const RestoreDriver::rgOptions[] = {
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
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

} // namespace

int main(int argc, char** argv) {
	return commonMain<RestoreDriver>(argc, argv);
}
