/*
 * Backup.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "flow/IRandom.h"

// A workload which only performs backup operations. A separate workload is used to perform restore operations.
struct BackupWorkload : TestWorkload {
	static constexpr auto NAME = "Backup";
	double backupAfter, restoreAfter, abortAndRestartAfter;
	double minBackupAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key backupTag;
	bool differentialBackup;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	LockDB locked{ false };
	UsePartitionedLog usePartitionedLog{ true };
	bool allowPauses;
	Optional<std::string> encryptionKeyFileName;

	explicit BackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked.set(sharedRandomNumber % 2);
		bool partitioned = getOption(options, "usePartitionedLog"_sr, true);
		usePartitionedLog.set(partitioned);
		backupAfter = getOption(options, "backupAfter"_sr, 10.0);
		double minBackupAfter = getOption(options, "minBackupAfter"_sr, backupAfter);
		if (backupAfter > minBackupAfter) {
			backupAfter = deterministicRandom()->random01() * (backupAfter - minBackupAfter) + minBackupAfter;
		}
		restoreAfter = getOption(options, "restoreAfter"_sr, 35.0);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		abortAndRestartAfter =
		    getOption(options,
		              "abortAndRestartAfter"_sr,
		              deterministicRandom()->random01() < 0.5
		                  ? deterministicRandom()->random01() * (restoreAfter - backupAfter) + backupAfter
		                  : 0.0);
		differentialBackup =
		    getOption(options, "differentialBackup"_sr, deterministicRandom()->random01() < 0.5 ? true : false);
		stopDifferentialAfter =
		    getOption(options,
		              "stopDifferentialAfter"_sr,
		              differentialBackup ? deterministicRandom()->random01() *
		                                           (restoreAfter - std::max(abortAndRestartAfter, backupAfter)) +
		                                       std::max(abortAndRestartAfter, backupAfter)
		                                 : 0.0);
		allowPauses = getOption(options, "allowPauses"_sr, true);

		std::vector<std::string> restorePrefixesToInclude =
		    getOption(options, "restorePrefixesToInclude"_sr, std::vector<std::string>());

		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}

		TraceEvent("BW_ClientId").detail("Id", wcx.clientId);
		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

		TraceEvent(SevInfo, "BW_Param")
		    .detail("Locked", locked)
		    .detail("BackupAfter", backupAfter)
		    .detail("RestoreAfter", restoreAfter)
		    .detail("BackupTag", printable(backupTag).c_str())
		    .detail("AbortAndRestartAfter", abortAndRestartAfter)
		    .detail("DifferentialBackup", differentialBackup)
		    .detail("StopDifferentialAfter", stopDifferentialAfter)
		    .detail("Encrypted", encryptionKeyFileName.present());

		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		while (true) {
			co_await backupAgent->changePause(cx, true);
			TraceEvent("BW_AgentPaused").log();
			co_await delay(30 * deterministicRandom()->random01());
			co_await backupAgent->changePause(cx, false);
			TraceEvent("BW_AgentResumed").log();
			co_await delay(120 * deterministicRandom()->random01());
		}
	}

	// Resume the backup agent if it is paused
	static Future<Void> resumeAgent(Database cx, FileBackupAgent* backupAgent) {
		TraceEvent("BW_AgentResuming").log();
		co_await backupAgent->changePause(cx, false);
		TraceEvent("BW_AgentResumed").log();
	}

	static Future<Void> statusLoop(Database cx, std::string tag) {
		FileBackupAgent agent;
		while (true) {
			bool active = co_await agent.checkActive(cx);
			TraceEvent("BW_AgentActivityCheck").detail("IsActive", active);
			std::string status = co_await agent.getStatus(cx, ShowErrors::True, tag);
			puts(status.c_str());
			std::string statusJSON = co_await agent.getStatusJSON(cx, tag);
			puts(statusJSON.c_str());
			co_await delay(2.0);
		}
	}

	Future<Void> doBackup(double startDelay,
	                      FileBackupAgent* backupAgent,
	                      Database cx,
	                      Key tag,
	                      Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                      double stopDifferentialDelay) {

		UID randomID = nondeterministicRandom()->randomUniqueID();

		Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		co_await delay(startDelay);

		if (startDelay || BUGGIFY) {
			TraceEvent("BW_DoBackupAbortBackup1", randomID)
			    .detail("Tag", printable(tag))
			    .detail("StartDelay", startDelay);

			try {
				co_await backupAgent->abortBackup(cx, tag.toString());
			} catch (Error& e) {
				TraceEvent("BW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BW_DoBackupSubmitBackup", randomID)
		    .detail("Tag", printable(tag))
		    .detail("StopWhenDone", stopDifferentialDelay ? "False" : "True");

		std::string backupContainer = "file://simfdb/backups/";
		Future<Void> status = statusLoop(cx, tag.toString());
		try {
			co_await backupAgent->submitBackup(cx,
			                                   StringRef(backupContainer),
			                                   {},
			                                   deterministicRandom()->randomInt(0, 60),
			                                   deterministicRandom()->randomInt(0, 2000),
			                                   tag.toString(),
			                                   backupRanges,
			                                   StopWhenDone{ !stopDifferentialDelay },
			                                   usePartitionedLog,
			                                   IncrementalBackupOnly::False,
			                                   encryptionKeyFileName);
		} catch (Error& e) {
			TraceEvent("BW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			CODE_PROBE(!stopDifferentialFuture.isReady(),
			           "Restore starts at specified time - stopDifferential not ready");
			co_await stopDifferentialFuture;
			TraceEvent("BW_DoBackupWaitToDiscontinue", randomID)
			    .detail("Tag", printable(tag))
			    .detail("DifferentialAfter", stopDifferentialDelay);

			try {
				if (BUGGIFY) {
					KeyBackedTag backupTag = makeBackupTag(tag.toString());
					TraceEvent("BW_DoBackupWaitForRestorable", randomID).detail("Tag", backupTag.tagName);

					// Wait until the backup is in a restorable state and get the status, URL, and UID atomically
					Reference<IBackupContainer> lastBackupContainer;
					UID lastBackupUID;
					EBackupState resultWait = co_await backupAgent->waitBackup(
					    cx, backupTag.tagName, StopWhenDone::False, &lastBackupContainer, &lastBackupUID);

					TraceEvent("BW_DoBackupWaitForRestorable", randomID)
					    .detail("Tag", backupTag.tagName)
					    .detail("Result", BackupAgentBase::getStateText(resultWait));

					bool restorable = false;
					if (lastBackupContainer) {
						Future<BackupDescription> fdesc = lastBackupContainer->describeBackup();
						co_await ready(fdesc);

						if (!fdesc.isError()) {
							BackupDescription desc = fdesc.get();
							co_await desc.resolveVersionTimes(cx);
							printf("BackupDescription:\n%s\n", desc.toString().c_str());
							restorable = desc.maxRestorableVersion.present();
						}
					}

					TraceEvent("BW_LastBackupContainer", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
					    .detail("LastBackupUID", lastBackupUID)
					    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait))
					    .detail("Restorable", restorable);

					// Do not check the backup, if aborted
					if (resultWait == EBackupState::STATE_ABORTED) {
					}
					// Ensure that a backup container was found
					else if (!lastBackupContainer) {
						TraceEvent(SevError, "BW_MissingBackupContainer", randomID)
						    .detail("LastBackupUID", lastBackupUID)
						    .detail("BackupTag", printable(tag))
						    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait));
						printf("BackupCorrectnessMissingBackupContainer   tag: %s  status: %s\n",
						       printable(tag).c_str(),
						       BackupAgentBase::getStateText(resultWait));
					}
					// Check that backup is restorable
					else if (!restorable) {
						TraceEvent(SevError, "BW_NotRestorable", randomID)
						    .detail("LastBackupUID", lastBackupUID)
						    .detail("BackupTag", printable(tag))
						    .detail("BackupFolder", lastBackupContainer->getURL())
						    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait));
						printf("BackupCorrectnessNotRestorable:  tag: %s\n", printable(tag).c_str());
					}

					// Abort the backup, if not the first backup because the second backup may have aborted the backup
					// by now
					if (startDelay) {
						TraceEvent("BW_DoBackupAbortBackup2", randomID)
						    .detail("Tag", printable(tag))
						    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait))
						    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
						    .detail("Restorable", restorable);
						co_await backupAgent->abortBackup(cx, tag.toString());
					} else {
						TraceEvent("BW_DoBackupDiscontinueBackup", randomID)
						    .detail("Tag", printable(tag))
						    .detail("DifferentialAfter", stopDifferentialDelay);
						co_await backupAgent->discontinueBackup(cx, tag);
					}
				}

				else {
					TraceEvent("BW_DoBackupDiscontinueBackup", randomID)
					    .detail("Tag", printable(tag))
					    .detail("DifferentialAfter", stopDifferentialDelay);
					co_await backupAgent->discontinueBackup(cx, tag);
				}
			} catch (Error& e) {
				TraceEvent("BW_DoBackupDiscontinueBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}
		}

		// Wait for the backup to complete
		TraceEvent("BW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));
		EBackupState statusValue = co_await backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True);

		std::string statusText = co_await backupAgent->getStatus(cx, ShowErrors::True, tag.toString());
		// Can we validate anything about status?

		TraceEvent("BW_DoBackupComplete", randomID)
		    .detail("Tag", printable(tag))
		    .detail("Status", statusText)
		    .detail("StatusValue", BackupAgentBase::getStateText(statusValue));
	}

	Future<Void> _start(Database cx) {
		FileBackupAgent backupAgent;
		Future<Void> cp;
		TraceEvent("BW_Arguments")
		    .detail("BackupTag", printable(backupTag))
		    .detail("BackupAfter", backupAfter)
		    .detail("RestoreAfter", restoreAfter)
		    .detail("AbortAndRestartAfter", abortAndRestartAfter)
		    .detail("DifferentialAfter", stopDifferentialAfter);

		UID randomID = nondeterministicRandom()->randomUniqueID();
		if (allowPauses && BUGGIFY) {
			cp = changePaused(cx, &backupAgent);
		} else {
			cp = resumeAgent(cx, &backupAgent);
		}

		if (encryptionKeyFileName.present()) {
			co_await BackupContainerFileSystem::createTestEncryptionKeyFile(encryptionKeyFileName.get());
		}

		try {
			Future<Void> startRestore = delay(restoreAfter);

			// backup
			co_await delay(backupAfter);

			TraceEvent("BW_DoBackup1", randomID).detail("Tag", printable(backupTag));
			Future<Void> b = doBackup(0, &backupAgent, cx, backupTag, backupRanges, stopDifferentialAfter);

			TraceEvent("BW_DoBackupWait", randomID)
			    .detail("BackupTag", printable(backupTag))
			    .detail("AbortAndRestartAfter", abortAndRestartAfter);
			try {
				co_await b;
			} catch (Error& e) {
				if (e.code() != error_code_database_locked)
					throw;
				co_return;
			}
			TraceEvent("BW_DoBackupDone", randomID)
			    .detail("BackupTag", printable(backupTag))
			    .detail("AbortAndRestartAfter", abortAndRestartAfter);

			co_await startRestore;

			// We can't remove after backup agents since the restore also needs them.
			// I.e., g_simulator->backupAgents = ISimulator::BackupAgentType::NoBackupAgents
		} catch (Error& e) {
			TraceEvent(SevError, "BackupCorrectness").error(e).GetLastError();
			throw;
		}
	}
};

WorkloadFactory<BackupWorkload> BackupWorkloadFactory;
