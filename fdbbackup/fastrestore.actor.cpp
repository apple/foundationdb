/*
 * fastrestore.actor.cpp
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

namespace {

class FastRestoreDriver : public Driver<FastRestoreDriver> {
	RestoreDriverState driverState;

	// Fast restore agent that kicks off the restore: send restore requests to restore workers.
	ACTOR static Future<Void> runFastRestoreTool(RestoreDriverState* driverState, bool verbose) {
		try {
			state FileBackupAgent backupAgent;
			state Version restoreVersion = ::invalidVersion;

			if (driverState->getBackupKeys().size() > 1) {
				fprintf(stdout, "[WARNING] Currently only a single restore range is tested!\n");
			}

			printf("[INFO] runFastRestoreTool: restore_ranges:%d first range:%s\n",
			       driverState->getBackupKeys().size(),
			       driverState->getBackupKeys().front().toString().c_str());
			TraceEvent ev("FastRestoreTool");
			ev.detail("RestoreRanges", driverState->getBackupKeys().size());
			for (int i = 0; i < driverState->getBackupKeys().size(); ++i) {
				ev.detail(format("Range%d", i), driverState->getBackupKeys()[i]);
			}

			if (!driverState->isDryRun()) {
				if (driverState->getTargetVersion() == ::invalidVersion) {
					TraceEvent("FastRestoreTool").detail("TargetRestoreVersion", "Largest restorable version");
					BackupDescription desc =
					    wait(IBackupContainer::openContainer(driverState->getRestoreContainer())->describeBackup());
					if (!desc.maxRestorableVersion.present()) {
						fprintf(stderr, "The specified backup is not restorable to any version.\n");
						throw restore_error();
					}

					restoreVersion = desc.maxRestorableVersion.get();
					TraceEvent("FastRestoreTool").detail("TargetRestoreVersion", driverState->getTargetVersion());
				}
				state UID randomUID = deterministicRandom()->randomUniqueID();
				TraceEvent("FastRestoreTool")
				    .detail("SubmitRestoreRequests", driverState->getBackupKeys().size())
				    .detail("RestoreUID", randomUID);
				wait(backupAgent.submitParallelRestore(
				    driverState->getDatabase(),
				    KeyRef(driverState->getTagName()),
				    driverState->getBackupKeys(),
				    KeyRef(driverState->getRestoreContainer()),
				    driverState->getTargetVersion(), // FIXME: Shouldn't actual version be used here?
				    true,
				    randomUID,
				    ""_sr,
				    ""_sr));
				// TODO: Support addPrefix and removePrefix
				if (driverState->shouldWaitForDone()) {
					// Wait for parallel restore to finish and unlock DB after that
					TraceEvent("FastRestoreTool").detail("BackupAndParallelRestore", "WaitForRestoreToFinish");
					wait(backupAgent.parallelRestoreFinish(driverState->getDatabase(), randomUID));
					TraceEvent("FastRestoreTool").detail("BackupAndParallelRestore", "RestoreFinished");
				} else {
					TraceEvent("FastRestoreTool")
					    .detail("RestoreUID", randomUID)
					    .detail("OperationGuide", "Manually unlock DB when restore finishes");
					printf("WARNING: DB will be in locked state after restore. Need UID:%s to unlock DB\n",
					       randomUID.toString().c_str());
				}
				restoreVersion = driverState->getTargetVersion();
			} else {
				state Reference<IBackupContainer> bc =
				    IBackupContainer::openContainer(driverState->getRestoreContainer());
				state BackupDescription description = wait(bc->describeBackup());

				if (driverState->getTargetVersion() <= 0) {
					wait(description.resolveVersionTimes(driverState->getDatabase()));
					if (description.maxRestorableVersion.present())
						restoreVersion = description.maxRestorableVersion.get();
					else {
						fprintf(stderr, "Backup is not restorable\n");
						throw restore_invalid_version();
					}
				} else {
					restoreVersion = driverState->getTargetVersion();
				}

				state Optional<RestorableFileSet> rset = wait(bc->getRestoreSet(restoreVersion));
				if (!rset.present()) {
					fprintf(stderr, "Insufficient data to restore to version %" PRId64 "\n", restoreVersion);
					throw restore_invalid_version();
				}

				// Display the restore information, if requested
				if (verbose) {
					printf("[DRY RUN] Restoring backup to version: %" PRId64 "\n", restoreVersion);
					printf("%s\n", description.toString().c_str());
				}
			}

			if (driverState->shouldWaitForDone() && verbose) {
				// If restore completed then report version restored
				printf("Restored to version %" PRId64 "%s\n",
				       restoreVersion,
				       (driverState->isDryRun()) ? " (DRY RUN)" : "");
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr, "ERROR: %s\n", e.what());
			throw;
		}

		return Void();
	}

public:
	static void printUsage(bool devhelp) {
		printf(" NOTE: Fast restore aims to support the same fdbrestore option list.\n");
		printf("       But fast restore is still under development. The options may not be fully supported.\n");
		printf(" Supported options are: --dest_cluster_file, -r, --waitfordone, --logdir\n");
		printRestoreUsage(getProgramName(), devhelp);
		return;
	}

	void processArg(CSimpleOpt const& args) { driverState.processArg(getProgramName(), args); }

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
	bool setup() { return driverState.setup(); }

	Future<Optional<Void>> run() {
		// TODO: We have not implemented the code commented out in this case
		switch (driverState.getRestoreType()) {
		case RestoreType::START:
			return stopAfter(runFastRestoreTool(&driverState, !quietDisplay));
		case RestoreType::WAIT:
			printf("[TODO][ERROR] FastRestore does not support RESTORE_WAIT yet!\n");
			throw restore_error();
			// return stopAfter( success(ba.waitRestore(db, KeyRef(tagName), true)) );
			break;
		case RestoreType::ABORT:
			printf("[TODO][ERROR] FastRestore does not support RESTORE_ABORT yet!\n");
			throw restore_error();
			// return stopAfter( map(ba.abortRestore(db, KeyRef(tagName)),
			//  [tagName](FileBackupAgent::ERestoreState s) -> Void { 						printf("Tag: %s  State:
			//  %s\n", tagName.c_str(),
			//  FileBackupAgent::restoreStateText(s).toString().c_str()); 						return Void();
			// }));
			break;
		case RestoreType::STATUS:
			printf("[TODO][ERROR] FastRestore does not support RESTORE_STATUS yet!\n");
			throw restore_error();
			// If no tag is specifically provided then print all tag status, don't just use "default"
			// if (tagName.present())
			//  tag = tagName;
			// return stopAfter( map(ba.restoreStatus(db, KeyRef(tag)), [](std::string s) -> Void {
			//   printf("%s\n", s.c_str());
			//   return Void();
			// }) );
			break;
		default:
			throw restore_error();
		}
		return Optional<Void>{};
	}

	static std::string getProgramName() { return "fastrestore_tool"; }
};

} // namespace

int main(int argc, char** argv) {
	return commonMain<FastRestoreDriver>(argc, argv);
}
