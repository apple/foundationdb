/*
 * BackupCorrectness.actor.cpp
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

#include "flow/actorcompiler.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "workloads.h"
#include "BulkSetup.actor.h"


//A workload which test the correctness of backup and restore process
struct BackupAndRestoreCorrectnessWorkload : TestWorkload {
	double backupAfter, restoreAfter, abortAndRestartAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key		backupTag;
	int	 backupRangesCount, backupRangeLengthMax;
	bool differentialBackup, performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	static int backupAgentRequests;
	bool locked;
	bool allowPauses;

	BackupAndRestoreCorrectnessWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {
		locked = sharedRandomNumber % 2;
		backupAfter = getOption(options, LiteralStringRef("backupAfter"), 10.0);
		restoreAfter = getOption(options, LiteralStringRef("restoreAfter"), 35.0);
		performRestore = getOption(options, LiteralStringRef("performRestore"), true);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		backupRangesCount = getOption(options, LiteralStringRef("backupRangesCount"), 5);
		backupRangeLengthMax = getOption(options, LiteralStringRef("backupRangeLengthMax"), 1);
		abortAndRestartAfter = getOption(options, LiteralStringRef("abortAndRestartAfter"), g_random->random01() < 0.5 ? g_random->random01() * (restoreAfter - backupAfter) + backupAfter : 0.0);
		differentialBackup = getOption(options, LiteralStringRef("differentialBackup"), g_random->random01() < 0.5 ? true : false);
		stopDifferentialAfter = getOption(options, LiteralStringRef("stopDifferentialAfter"),
			differentialBackup ? g_random->random01() * (restoreAfter - std::max(abortAndRestartAfter,backupAfter)) + std::max(abortAndRestartAfter,backupAfter) : 0.0);
		agentRequest = getOption(options, LiteralStringRef("simBackupAgents"), true);
		allowPauses = getOption(options, LiteralStringRef("allowPauses"), true);

		KeyRef beginRange;
		KeyRef endRange;
		UID randomID = g_nondeterministic_random->randomUniqueID();

		if(backupRangesCount <= 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		} else {
			// Add backup ranges
			std::set<std::string> rangeEndpoints;
			while (rangeEndpoints.size() < backupRangesCount * 2) {
				rangeEndpoints.insert(g_random->randomAlphaNumeric(g_random->randomInt(1, backupRangeLengthMax + 1)));
			}

			// Create ranges from the keys, in order, to prevent overlaps
			std::vector<std::string> sortedEndpoints(rangeEndpoints.begin(), rangeEndpoints.end());
			sort(sortedEndpoints.begin(), sortedEndpoints.end());
			for (auto i = sortedEndpoints.begin(); i != sortedEndpoints.end(); ++i) {
				const std::string &start = *i++;
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(start, *i));

				// Track the added range
				TraceEvent("BARW_BackupCorrectness_Range", randomID).detail("rangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
					.detail("rangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}
	}

	virtual std::string description() {
		return "BackupAndRestoreCorrectness";
	}

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0)
			return Void();

		TraceEvent(SevInfo, "BARW_Param").detail("locked", locked);
		TraceEvent(SevInfo, "BARW_Param").detail("backupAfter", backupAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("restoreAfter", restoreAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("performRestore", performRestore);
		TraceEvent(SevInfo, "BARW_Param").detail("backupTag", printable(backupTag).c_str());
		TraceEvent(SevInfo, "BARW_Param").detail("backupRangesCount", backupRangesCount);
		TraceEvent(SevInfo, "BARW_Param").detail("backupRangeLengthMax", backupRangeLengthMax);
		TraceEvent(SevInfo, "BARW_Param").detail("abortAndRestartAfter", abortAndRestartAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("differentialBackup", differentialBackup);
		TraceEvent(SevInfo, "BARW_Param").detail("stopDifferentialAfter", stopDifferentialAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("agentRequest", agentRequest);

		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	ACTOR static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		loop {
			Void _ = wait( backupAgent->taskBucket->changePause(cx, true) );
			Void _ = wait( delay(30*g_random->random01()) );
			Void _ = wait( backupAgent->taskBucket->changePause(cx, false) );
			Void _ = wait( delay(120*g_random->random01()) );
		}
	}

	ACTOR static Future<Void> statusLoop(Database cx, std::string tag) {
		state FileBackupAgent agent;
		loop {
			std::string status = wait(agent.getStatus(cx, true, tag));
			puts(status.c_str());
			Void _ = wait(delay(2.0));
		}
	}

	ACTOR static Future<Void> doBackup(BackupAndRestoreCorrectnessWorkload* self, double startDelay, FileBackupAgent* backupAgent, Database cx,
		Key tag, Standalone<VectorRef<KeyRangeRef>> backupRanges, double stopDifferentialDelay, Promise<Void> submittted) {

		state UID	randomID = g_nondeterministic_random->randomUniqueID();

		state Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		Void _ = wait( delay( startDelay ));

		if (startDelay || BUGGIFY) {
			TraceEvent("BARW_doBackupAbortBackup1", randomID).detail("tag", printable(tag)).detail("startDelay", startDelay);

			try {
				Void _ = wait(backupAgent->abortBackup(cx, tag.toString()));
			}
			catch (Error& e) {
				TraceEvent("BARW_doBackupAbortBackupException", randomID).detail("tag", printable(tag)).error(e);
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BARW_doBackupSubmitBackup", randomID).detail("tag", printable(tag)).detail("stopWhenDone", stopDifferentialDelay ? "False" : "True");

		state std::string backupContainer = "file://simfdb/backups/";
		state Future<Void> status = statusLoop(cx, tag.toString());

		try {
			Void _ = wait(backupAgent->submitBackup(cx, StringRef(backupContainer), g_random->randomInt(0, 100), tag.toString(), backupRanges, stopDifferentialDelay ? false : true));
		}
		catch (Error& e) {
			TraceEvent("BARW_doBackupSubmitBackupException", randomID).detail("tag", printable(tag)).error(e);
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		submittted.send(Void());

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			TEST(!stopDifferentialFuture.isReady()); //Restore starts at specified time
			Void _ = wait(stopDifferentialFuture);
			TraceEvent("BARW_doBackupWaitToDiscontinue", randomID).detail("tag", printable(tag)).detail("differentialAfter", stopDifferentialDelay);

			try {
				if (BUGGIFY) {
					state KeyBackedTag backupTag = makeBackupTag(tag.toString());
					TraceEvent("BARW_doBackupWaitForRestorable", randomID).detail("tag", backupTag.tagName);
					// Wait until the backup is in a restorable state
					state int resultWait = wait(backupAgent->waitBackup(cx, backupTag.tagName, false));
					UidAndAbortedFlagT uidFlag = wait(backupTag.getOrThrow(cx));
					state UID logUid = uidFlag.first;
					state Reference<IBackupContainer> lastBackupContainer = wait(BackupConfig(logUid).backupContainer().getD(cx));

					state bool restorable = false;
					if(lastBackupContainer) {
						state BackupDescription desc = wait(lastBackupContainer->describeBackup());
						Void _ = wait(desc.resolveVersionTimes(cx));
						printf("BackupDescription:\n%s\n", desc.toString().c_str());
						restorable = desc.maxRestorableVersion.present();
					}

					TraceEvent("BARW_lastBackupContainer", randomID)
						.detail("backupTag", printable(tag))
						.detail("lastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
						.detail("logUid", logUid).detail("waitStatus", resultWait).detail("restorable", restorable);

					// Do not check the backup, if aborted
					if (resultWait == BackupAgentBase::STATE_ABORTED) {
					}
					// Ensure that a backup container was found
					else if (!lastBackupContainer) {
						TraceEvent("BARW_missingBackupContainer", randomID).detail("logUid", logUid).detail("backupTag", printable(tag)).detail("waitStatus", resultWait);
						printf("BackupCorrectnessMissingBackupContainer   tag: %s  status: %d\n", printable(tag).c_str(), resultWait);
					}
					// Check that backup is restorable
					else {
						if(!restorable) {
							TraceEvent("BARW_notRestorable", randomID).detail("logUid", logUid).detail("backupTag", printable(tag))
								.detail("backupFolder", lastBackupContainer->getURL()).detail("waitStatus", resultWait);
							printf("BackupCorrectnessNotRestorable:  tag: %s\n", printable(tag).c_str());
						}
					}

					// Abort the backup, if not the first backup because the second backup may have aborted the backup by now
					if (startDelay) {
						TraceEvent("BARW_doBackupAbortBackup2", randomID).detail("tag", printable(tag))
							.detail("waitStatus", resultWait)
							.detail("lastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
							.detail("restorable", restorable);
						Void _ = wait(backupAgent->abortBackup(cx, tag.toString()));
					}
					else {
						TraceEvent("BARW_doBackupDiscontinueBackup", randomID).detail("tag", printable(tag)).detail("differentialAfter", stopDifferentialDelay);
						Void _ = wait(backupAgent->discontinueBackup(cx, tag));
					}
				}

				else {
					TraceEvent("BARW_doBackupDiscontinueBackup", randomID).detail("tag", printable(tag)).detail("differentialAfter", stopDifferentialDelay);
					Void _ = wait(backupAgent->discontinueBackup(cx, tag));
				}
			}
			catch (Error& e) {
				TraceEvent("BARW_doBackupDiscontinueBackupException", randomID).detail("tag", printable(tag)).error(e);
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_doBackupWaitBackup", randomID).detail("tag", printable(tag));
		state int statusValue = wait(backupAgent->waitBackup(cx, tag.toString(), true));

		state std::string statusText;

		std::string _statusText = wait( backupAgent->getStatus(cx, 5, tag.toString()) );
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_doBackupComplete", randomID).detail("tag", printable(tag))
			.detail("status", statusText).detail("statusValue", statusValue);

		return Void();
	}

	/**
		This actor attempts to restore the database without clearing the keyspace.
	 */
	ACTOR static Future<Void> attemptDirtyRestore(BackupAndRestoreCorrectnessWorkload* self, Database cx, FileBackupAgent* backupAgent, Standalone<StringRef> lastBackupContainer, UID randomID) {
		state Transaction tr(cx);
		state int rowCount = 0;
		loop{
			try {
				Standalone<RangeResultRef> existingRows = wait(tr.getRange(normalKeys, 1));
				rowCount = existingRows.size();
				break;
			}
			catch (Error &e) {
				Void _ = wait(tr.onError(e));
			}
		}

		// Try doing a restore without clearing the keys
		if (rowCount > 0) {
			try {
				Version _ = wait(backupAgent->restore(cx, self->backupTag, KeyRef(lastBackupContainer), true, -1, true, normalKeys, Key(), Key(), self->locked));
				TraceEvent(SevError, "BARW_restore_allowed_overwritting_database", randomID);
				ASSERT(false);
			}
			catch (Error &e) {
				if (e.code() != error_code_restore_destination_not_empty) {
					throw;
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupAndRestoreCorrectnessWorkload* self) {
		state FileBackupAgent backupAgent;
		state Future<Void> extraBackup;
		state bool extraTasks = false;
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "backupAndRestore");
		TraceEvent("BARW_Arguments").detail("backupTag", printable(self->backupTag)).detail("performRestore", self->performRestore)
			.detail("backupAfter", self->backupAfter).detail("restoreAfter", self->restoreAfter)
			.detail("abortAndRestartAfter", self->abortAndRestartAfter).detail("differentialAfter", self->stopDifferentialAfter);

		state UID	randomID = g_nondeterministic_random->randomUniqueID();
		if(self->allowPauses && BUGGIFY) {
			state Future<Void> cp = changePaused(cx, &backupAgent);
		}

		// Increment the backup agent requets
		if (self->agentRequest) {
			BackupAndRestoreCorrectnessWorkload::backupAgentRequests ++;
		}

		try{
			state Future<Void> startRestore = delay(self->restoreAfter);

			// backup
			Void _ = wait(delay(self->backupAfter));

			TraceEvent("BARW_doBackup1", randomID).detail("tag", printable(self->backupTag));
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(self, 0, &backupAgent, cx, self->backupTag, self->backupRanges, self->stopDifferentialAfter, submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BARW_doBackup2", randomID).detail("tag", printable(self->backupTag)).detail("abortWait", self->abortAndRestartAfter);
				Void _ = wait(submitted.getFuture());
				b = b && doBackup(self, self->abortAndRestartAfter, &backupAgent, cx, self->backupTag, self->backupRanges, self->stopDifferentialAfter, Promise<Void>());
			}

			TraceEvent("BARW_doBackupWait", randomID).detail("backupTag", printable(self->backupTag)).detail("abortAndRestartAfter", self->abortAndRestartAfter);
			try {
				Void _ = wait(b);
			} catch( Error &e ) {
				if(e.code() != error_code_database_locked)
					throw;
				if(self->performRestore)
					throw;
				return Void();
			}
			TraceEvent("BARW_doBackupDone", randomID).detail("backupTag", printable(self->backupTag)).detail("abortAndRestartAfter", self->abortAndRestartAfter);

			state KeyBackedTag keyBackedTag = makeBackupTag(self->backupTag.toString());
			UidAndAbortedFlagT uidFlag = wait(keyBackedTag.getOrThrow(cx));
			state UID logUid = uidFlag.first;
			state Reference<IBackupContainer> lastBackupContainer = wait(BackupConfig(logUid).backupContainer().getD(cx));

			// Occasionally start yet another backup that might still be running when we restore
			if (!self->locked && BUGGIFY) {
				TraceEvent("BARW_submitBackup2", randomID).detail("tag", printable(self->backupTag));
				try {
					extraBackup = backupAgent.submitBackup(cx, LiteralStringRef("file://simfdb/backups/"), g_random->randomInt(0, 100), self->backupTag.toString(), self->backupRanges, true);
				}
				catch (Error& e) {
					TraceEvent("BARW_submitBackup2Exception", randomID).detail("backupTag", printable(self->backupTag)).error(e);
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
			}

			TEST(!startRestore.isReady()); //Restore starts at specified time
			Void _ = wait(startRestore);
			
			if (lastBackupContainer && self->performRestore) {
				if (g_random->random01() < 0.5) {
					Void _ = wait(attemptDirtyRestore(self, cx, &backupAgent, StringRef(lastBackupContainer->getURL()), randomID));
				}
				Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					for (auto &kvrange : self->backupRanges)
						tr->clear(kvrange);
					return Void();
				}));

				// restore database
				TraceEvent("BARW_restore", randomID).detail("lastBackupContainer", lastBackupContainer->getURL()).detail("restoreAfter", self->restoreAfter).detail("backupTag", printable(self->backupTag));
				
				state std::vector<Future<Version>> restores;
				state std::vector<Standalone<StringRef>> restoreTags;
				state int restoreIndex;

				for (restoreIndex = 0; restoreIndex < self->backupRanges.size(); restoreIndex++) {
					auto range = self->backupRanges[restoreIndex];
					Standalone<StringRef> restoreTag(self->backupTag.toString() + "_" + std::to_string(restoreIndex));
					restoreTags.push_back(restoreTag);
					restores.push_back(backupAgent.restore(cx, restoreTag, KeyRef(lastBackupContainer->getURL()), true, -1, true, range, Key(), Key(), self->locked));
				}
				
				// Sometimes kill and restart the restore
				if(BUGGIFY) {
					Void _ = wait(delay(g_random->randomInt(0, 10)));
					for(restoreIndex = 0; restoreIndex < restores.size(); restoreIndex++) {
						FileBackupAgent::ERestoreState rs = wait(backupAgent.abortRestore(cx, restoreTags[restoreIndex]));
						// The restore may have already completed, or the abort may have been done before the restore
						// was even able to start.  Only run a new restore if the previous one was actually aborted.
						if (rs == FileBackupAgent::ERestoreState::ABORTED) {
							Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
								tr->clear(self->backupRanges[restoreIndex]);
								return Void();
							}));
							restores[restoreIndex] = backupAgent.restore(cx, restoreTags[restoreIndex], KeyRef(lastBackupContainer->getURL()), true, -1, true, self->backupRanges[restoreIndex], Key(), Key(), self->locked);
						}
					}
				}

				Void _ = wait(waitForAll(restores));

				for (auto &restore : restores) {
					assert(!restore.isError());
				}
			}

			if (extraBackup.isValid()) {
				TraceEvent("BARW_waitExtraBackup", randomID).detail("backupTag", printable(self->backupTag));
				extraTasks = true;
				try {
					Void _ = wait(extraBackup);
				}
				catch (Error& e) {
					TraceEvent("BARW_extraBackupException", randomID).detail("backupTag", printable(self->backupTag)).error(e);
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				TraceEvent("BARW_abortBackupExtra", randomID).detail("backupTag", printable(self->backupTag));
				try {
					Void _ = wait(backupAgent.abortBackup(cx, self->backupTag.toString()));
				}
				catch (Error& e) {
					TraceEvent("BARW_abortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
			}

			state Key	backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
			state Key	backupLogValuesKey = uidPrefixKey(backupLogKeys.begin, logUid);
			state int displaySystemKeys = 0;

			// Ensure that there is no left over key within the backup subspace
			loop {
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

				TraceEvent("BARW_check_leftoverkeys", randomID).detail("backupTag", printable(self->backupTag));

				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					Standalone<RangeResultRef> agentValues = wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

					// Error if the system keyspace for the backup tag is not empty
					if (agentValues.size() > 0) {
						displaySystemKeys ++;
						printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n", agentValues.size(), printable(backupAgentKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", randomID).detail("backupTag", printable(self->backupTag))
							.detail("LeftOverKeys", agentValues.size()).detail("keySpace", printable(backupAgentKey));
						for (auto & s : agentValues) {
							TraceEvent("BARW_LeftOverKey", randomID).detail("key", printable(StringRef(s.key.toString()))).detail("value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n", printable(StringRef(s.key.toString())).c_str(), printable(StringRef(s.value.toString())).c_str());
						}
					}
					else {
						printf("No left over backup agent configuration keys\n");
					}

					Standalone<RangeResultRef> logValues = wait(tr->getRange(KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100));

					// Error if the log/mutation keyspace for the backup tag  is not empty
					if (logValues.size() > 0) {
						displaySystemKeys ++;
						printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n", logValues.size(), printable(backupLogValuesKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverLogKeys", randomID).detail("backupTag", printable(self->backupTag))
							.detail("LeftOverKeys", logValues.size()).detail("keySpace", printable(backupLogValuesKey));
						for (auto & s : logValues) {
							TraceEvent("BARW_LeftOverKey", randomID).detail("key", printable(StringRef(s.key.toString()))).detail("value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n", printable(StringRef(s.key.toString())).c_str(), printable(StringRef(s.value.toString())).c_str());
						}
					}
					else {
						printf("No left over backup log keys\n");
					}

					// Check the left over tasks
					// We have to wait for the list to empty since an abort and get status
					// can leave extra tasks in the queue
					TraceEvent("BARW_check_leftovertasks", randomID).detail("backupTag", printable(self->backupTag));
					state int64_t taskCount = wait( backupAgent.getTaskCount(tr) );
					state int waitCycles = 0;

					if ((taskCount) && (0)) {
						TraceEvent("BARW_EndingNonzeroTaskCount", randomID).detail("backupTag", printable(self->backupTag)).detail("taskCount", taskCount).detail("waitCycles", waitCycles);
						printf("EndingNonZeroTasks: %ld\n", (long) taskCount);
						Void _ = wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
					}

					loop {
						waitCycles ++;

						TraceEvent("BARW_NonzeroTaskWait", randomID).detail("backupTag", printable(self->backupTag)).detail("taskCount", taskCount).detail("waitCycles", waitCycles);
						printf("%.6f %-10s Wait #%4d for %lld tasks to end\n", now(), randomID.toString().c_str(), waitCycles, (long long) taskCount);

						Void _ = wait(delay(20.0));
						tr->commit();
						tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
						int64_t _taskCount = wait( backupAgent.getTaskCount(tr) );
						taskCount = _taskCount;

						if (!taskCount) {
							break;
						}
					}

					if (taskCount) {
						displaySystemKeys ++;
						TraceEvent(SevError, "BARW_NonzeroTaskCount", randomID).detail("backupTag", printable(self->backupTag)).detail("taskCount", taskCount).detail("waitCycles", waitCycles);
						printf("BackupCorrectnessLeftOverLogTasks: %ld\n", (long) taskCount);
					}

					break;
				}
				catch (Error &e) {
					TraceEvent("BARW_checkException", randomID).error(e);
					Void _ = wait(tr->onError(e));
				}
			}

			if (displaySystemKeys) {
				Void _ = wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
			}

			TraceEvent("BARW_complete", randomID).detail("backupTag", printable(self->backupTag));

			// Decrement the backup agent requets
			if (self->agentRequest) {
				BackupAndRestoreCorrectnessWorkload::backupAgentRequests --;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator.backupAgents == ISimulator::BackupToFile) && (!BackupAndRestoreCorrectnessWorkload::backupAgentRequests)) {
				g_simulator.backupAgents = ISimulator::NoBackupAgents;
			}
		}
		catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e).GetLastError();
			throw;
		}
		return Void();
	}
};

int BackupAndRestoreCorrectnessWorkload::backupAgentRequests = 0;

WorkloadFactory<BackupAndRestoreCorrectnessWorkload> BackupAndRestoreCorrectnessWorkloadFactory("BackupAndRestoreCorrectness");
