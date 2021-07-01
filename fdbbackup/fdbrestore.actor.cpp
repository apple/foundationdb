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
#include "fdbbackup/RestoreDriver.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/versions.h"
#include "flow/TLSConfig.actor.h"

extern const char* getSourceVersion();

namespace {

class RestoreDriver : public Driver<RestoreDriver> {
	// Submit the restore request to the database if "performRestore" is true. Otherwise,
	// check if the restore can be performed.
	ACTOR static Future<Void> runRestore(Database db,
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
				fprintf(stderr,
				        "Original source database cluster file '%s' does not exist.\n",
				        originalClusterFile.c_str());
				throw restore_error();
			}

			origDb = Database::createDatabase(originalClusterFile, Database::API_VERSION_LATEST);
			Version v = wait(timeKeeperVersionFromDatetime(targetTimestamp, origDb.get()));
			printf("Timestamp '%s' resolves to version %" PRId64 "\n", targetTimestamp.c_str(), v);
			targetVersion = v;
		}

		try {
			state FileBackupAgent backupAgent;

			state Reference<IBackupContainer> bc = openBackupContainer(getProgramName().c_str(), container);

			// If targetVersion is unset then use the maximum restorable version from the backup description
			if (targetVersion == invalidVersion) {
				if (verbose)
					printf("No restore target version given, will use maximum restorable version from backup "
					       "description.\n");

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
					        "Insufficient data to restore to version %" PRId64
					        ".  Describe backup for more information.\n",
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
	RestoreType restoreType{ RestoreType::UNKNOWN };

	bool dryRun{ false };
	bool onlyApplyMutationLogs{ false };
	bool inconsistentSnapshotOnly{ false };
	Version beginVersion{ ::invalidVersion };

public:
	static std::string getProgramName() { return "fdbrestore"; }

	static void printUsage(bool devhelp) { printRestoreUsage(getProgramName(), devhelp); }

	void parseCommandLineArgs(int argc, char** argv) {
		if (argc < 2) {
			printUsage(false);
			throw invalid_command_line_arguments();
		}
		// Get the restore operation type
		auto restoreType = getRestoreType(argv[1]);
		if (restoreType == RestoreType::UNKNOWN) {
			auto args = std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT);
			if (!args->Next()) {
				throw invalid_command_line_arguments();
			}
			runTopLevelCommand(args->OptionId());
			// FIXME: Confusing to exit here?
			flushAndExit(FDB_EXIT_SUCCESS);
		}
		auto args = std::make_unique<CSimpleOpt>(argc - 1, argv + 1, rgRestoreOptions, SO_O_EXACT);
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
			initTraceFile();
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
		case OPT_INCREMENTALONLY:
			onlyApplyMutationLogs = true;
			break;
		case OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY:
			inconsistentSnapshotOnly = true;
			break;
		case OPT_RESTORE_BEGIN_VERSION: {
			const char* a = args.OptionArg();
			long long ver = 0;
			if (!sscanf(a, "%lld", &ver)) {
				fprintf(stderr, "ERROR: Could not parse database beginVersion `%s'\n", a);
				printHelpTeaser(getProgramName());
				throw invalid_option_value();
			}
			beginVersion = ver;
			break;
		}
		default:
			break;
		}
	}
};

} // namespace

int main(int argc, char** argv) {
	return commonMain<RestoreDriver>(argc, argv);
}
