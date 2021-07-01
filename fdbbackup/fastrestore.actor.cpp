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
	// FIXME: All of these fields are common to fdbrestore
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

	static void printUsage(bool devhelp) {
		printf(" NOTE: Fast restore aims to support the same fdbrestore option list.\n");
		printf("       But fast restore is still under development. The options may not be fully supported.\n");
		printf(" Supported options are: --dest_cluster_file, -r, --waitfordone, --logdir\n");
		printRestoreUsage(getProgramName(), devhelp);
		return;
	}

	// Fast restore agent that kicks off the restore: send restore requests to restore workers.
	ACTOR static Future<Void> runFastRestoreTool(Database db,
	                                             std::string tagName,
	                                             std::string container,
	                                             Standalone<VectorRef<KeyRangeRef>> ranges,
	                                             Version dbVersion,
	                                             bool performRestore,
	                                             bool verbose,
	                                             bool waitForDone) {
		try {
			state FileBackupAgent backupAgent;
			state Version restoreVersion = invalidVersion;

			if (ranges.size() > 1) {
				fprintf(stdout, "[WARNING] Currently only a single restore range is tested!\n");
			}

			if (ranges.size() == 0) {
				ranges.push_back(ranges.arena(), normalKeys);
			}

			printf("[INFO] runFastRestoreTool: restore_ranges:%d first range:%s\n",
			       ranges.size(),
			       ranges.front().toString().c_str());
			TraceEvent ev("FastRestoreTool");
			ev.detail("RestoreRanges", ranges.size());
			for (int i = 0; i < ranges.size(); ++i) {
				ev.detail(format("Range%d", i), ranges[i]);
			}

			if (performRestore) {
				if (dbVersion == invalidVersion) {
					TraceEvent("FastRestoreTool").detail("TargetRestoreVersion", "Largest restorable version");
					BackupDescription desc = wait(IBackupContainer::openContainer(container)->describeBackup());
					if (!desc.maxRestorableVersion.present()) {
						fprintf(stderr, "The specified backup is not restorable to any version.\n");
						throw restore_error();
					}

					dbVersion = desc.maxRestorableVersion.get();
					TraceEvent("FastRestoreTool").detail("TargetRestoreVersion", dbVersion);
				}
				state UID randomUID = deterministicRandom()->randomUniqueID();
				TraceEvent("FastRestoreTool")
				    .detail("SubmitRestoreRequests", ranges.size())
				    .detail("RestoreUID", randomUID);
				wait(backupAgent.submitParallelRestore(db,
				                                       KeyRef(tagName),
				                                       ranges,
				                                       KeyRef(container),
				                                       dbVersion,
				                                       true,
				                                       randomUID,
				                                       LiteralStringRef(""),
				                                       LiteralStringRef("")));
				// TODO: Support addPrefix and removePrefix
				if (waitForDone) {
					// Wait for parallel restore to finish and unlock DB after that
					TraceEvent("FastRestoreTool").detail("BackupAndParallelRestore", "WaitForRestoreToFinish");
					wait(backupAgent.parallelRestoreFinish(db, randomUID));
					TraceEvent("FastRestoreTool").detail("BackupAndParallelRestore", "RestoreFinished");
				} else {
					TraceEvent("FastRestoreTool")
					    .detail("RestoreUID", randomUID)
					    .detail("OperationGuide", "Manually unlock DB when restore finishes");
					printf("WARNING: DB will be in locked state after restore. Need UID:%s to unlock DB\n",
					       randomUID.toString().c_str());
				}

				restoreVersion = dbVersion;
			} else {
				state Reference<IBackupContainer> bc = IBackupContainer::openContainer(container);
				state BackupDescription description = wait(bc->describeBackup());

				if (dbVersion <= 0) {
					wait(description.resolveVersionTimes(db));
					if (description.maxRestorableVersion.present())
						restoreVersion = description.maxRestorableVersion.get();
					else {
						fprintf(stderr, "Backup is not restorable\n");
						throw restore_invalid_version();
					}
				} else {
					restoreVersion = dbVersion;
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

			if (waitForDone && verbose) {
				// If restore completed then report version restored
				printf("Restored to version %" PRId64 "%s\n", restoreVersion, (performRestore) ? "" : " (DRY RUN)");
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
	// FIXME: This is copy-pasted from fdbrestore.actor.cpp
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
		auto args = std::make_unique<CSimpleOpt>(argc - 1, argv + 1, rg_restoreOptions, SO_O_EXACT);
		processArgs(*args);
	}

	// TODO: Reduce duplicate code with fdbrestore
	bool setup() {
		// Support --dest_cluster_file option as fdbrestore does
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
		// TODO: We have not implemented the code commented out in this case
		switch (restoreType) {
		case RestoreType::START:
			return stopAfter(runFastRestoreTool(db,
			                                    tagName.orDefault(BackupAgentBase::getDefaultTag().toString()),
			                                    restoreContainer,
			                                    backupKeys,
			                                    restoreVersion,
			                                    !dryRun,
			                                    !quietDisplay,
			                                    waitForDone));
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
