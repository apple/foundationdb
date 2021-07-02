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
	ACTOR static Future<Void> runRestore(RestoreDriverState const* driverState, bool verbose) {
		state Version restoreVersion = driverState->getTargetVersion();

		if (driverState->getTargetVersion() != ::invalidVersion && !driverState->getTargetTimestamp().empty()) {
			fprintf(stderr, "Restore target version and target timestamp cannot both be specified\n");
			throw restore_error();
		}

		state Optional<Database> origDb;

		// Resolve targetTimestamp if given
		if (!driverState->getTargetTimestamp().empty()) {
			if (driverState->getOrigClusterFile().empty()) {
				fprintf(stderr,
				        "An original cluster file must be given in order to resolve restore target timestamp '%s'\n",
				        driverState->getTargetTimestamp().c_str());
				throw restore_error();
			}

			if (!fileExists(driverState->getOrigClusterFile())) {
				fprintf(stderr,
				        "Original source database cluster file '%s' does not exist.\n",
				        driverState->getOrigClusterFile().c_str());
				throw restore_error();
			}

			origDb = Database::createDatabase(driverState->getOrigClusterFile(), Database::API_VERSION_LATEST);
			Version v = wait(timeKeeperVersionFromDatetime(driverState->getTargetTimestamp(), origDb.get()));
			printf("Timestamp '%s' resolves to version %" PRId64 "\n", driverState->getTargetTimestamp().c_str(), v);
			restoreVersion = v;
		}

		try {
			state FileBackupAgent backupAgent;

			state Reference<IBackupContainer> bc =
			    openBackupContainer(getProgramName().c_str(), driverState->getRestoreContainer());

			// If targetVersion is unset then use the maximum restorable version from the backup description
			if (driverState->getTargetVersion() == ::invalidVersion) {
				if (verbose) {
					printf("No restore target version given, will use maximum restorable version from backup "
					       "description.\n");
				}

				BackupDescription desc = wait(bc->describeBackup());

				if (driverState->shouldOnlyApplyMutationLogs() && desc.contiguousLogEnd.present()) {
					restoreVersion = desc.contiguousLogEnd.get() - 1;
				} else if (desc.maxRestorableVersion.present()) {
					restoreVersion = desc.maxRestorableVersion.get();
				} else {
					fprintf(stderr, "The specified backup is not restorable to any version.\n");
					throw restore_error();
				}

				if (verbose)
					printf("Using target restore version %" PRId64 "\n", restoreVersion);
			}

			if (!driverState->isDryRun()) {
				Version restoredVersion = wait(backupAgent.restore(driverState->getDatabase(),
				                                                   origDb,
				                                                   KeyRef(driverState->getTagName()),
				                                                   KeyRef(driverState->getRestoreContainer()),
				                                                   driverState->getBackupKeys(),
				                                                   driverState->shouldWaitForDone(),
				                                                   restoreVersion,
				                                                   verbose,
				                                                   KeyRef(driverState->getAddPrefix()),
				                                                   KeyRef(driverState->getRemovePrefix()),
				                                                   true,
				                                                   driverState->shouldOnlyApplyMutationLogs(),
				                                                   driverState->restoreInconsistentSnapshotOnly(),
				                                                   driverState->getBeginVersion()));

				if (driverState->shouldWaitForDone() && verbose) {
					// If restore is now complete then report version restored
					printf("Restored to version %" PRId64 "\n", restoredVersion);
				}
			} else {
				state Optional<RestorableFileSet> rset =
				    wait(bc->getRestoreSet(restoreVersion, driverState->getBackupKeys()));

				if (!rset.present()) {
					fprintf(stderr,
					        "Insufficient data to restore to version %" PRId64
					        ".  Describe backup for more information.\n",
					        restoreVersion);
					throw restore_invalid_version();
				}

				printf("Backup can be used to restore to version %" PRId64 "\n", restoreVersion);
			}

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr, "ERROR: %s\n", e.what());
			throw;
		}

		return Void();
	}

	RestoreDriverState driverState;

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
			CSimpleOpt args(argc, argv, g_rgOptions, SO_O_EXACT);
			if (!args.Next()) {
				throw invalid_command_line_arguments();
			}
			runTopLevelCommand(args.OptionId());
			// FIXME: Confusing to exit here?
			flushAndExit(FDB_EXIT_SUCCESS);
		}
		processArgs(argc - 1, &argv[1], rgRestoreOptions);
	}

	// Returns true iff setup is successful
	bool setup() { return driverState.setup(); }

	Future<Optional<Void>> run() {
		FileBackupAgent ba;

		switch (driverState.getRestoreType()) {
		case RestoreType::START:
			return stopAfter(runRestore(&driverState, !quietDisplay));
		case RestoreType::WAIT:
			return stopAfter(
			    success(ba.waitRestore(driverState.getDatabase(), KeyRef(driverState.getTagName()), true)));

		case RestoreType::ABORT:
			return stopAfter(map(ba.abortRestore(driverState.getDatabase(), KeyRef(driverState.getTagName())),
			                     [tagName = driverState.getTagName()](FileBackupAgent::ERestoreState s) -> Void {
				                     printf("RESTORE_ABORT Tag: %s  State: %s\n",
				                            tagName.c_str(),
				                            FileBackupAgent::restoreStateText(s).toString().c_str());
				                     return Void();
			                     }));
		case RestoreType::STATUS: {
			Optional<Key> tag;
			// If no tag is specifically provided then print all tag status, don't just use "default"
			if (driverState.tagNameProvided()) {
				tag = driverState.getTagName();
			}
			return stopAfter(map(ba.restoreStatus(driverState.getDatabase(), tag), [](std::string const& s) -> Void {
				printf("%s\n", s.c_str());
				return Void();
			}));
		}
		default:
			ASSERT(false);
			return {};
		}
	}

	void processArg(CSimpleOpt const& args) { driverState.processArg(getProgramName(), args); }
};

} // namespace

int main(int argc, char** argv) {
	return commonMain<RestoreDriver>(argc, argv);
}
