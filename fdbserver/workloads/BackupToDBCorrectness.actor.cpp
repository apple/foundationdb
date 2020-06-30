/*
 * BackupToDBCorrectness.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

//A workload which test the correctness of backup and restore process
struct BackupToDBCorrectnessWorkload : TestWorkload {
	double backupAfter, abortAndRestartAfter, restoreAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key	backupTag, restoreTag;
	Key	backupPrefix, extraPrefix;
	bool beforePrefix;
	int	 backupRangesCount, backupRangeLengthMax;
	bool differentialBackup, performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	static int drAgentRequests;
	Database extraDB;
	bool locked;
	bool shareLogRange;
	UID destUid;

	BackupToDBCorrectnessWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {
		locked = sharedRandomNumber % 2;
		backupAfter = getOption(options, LiteralStringRef("backupAfter"), 10.0);
		restoreAfter = getOption(options, LiteralStringRef("restoreAfter"), 35.0);
		performRestore = getOption(options, LiteralStringRef("performRestore"), true);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		restoreTag = getOption(options, LiteralStringRef("restoreTag"), LiteralStringRef("restore"));
		backupPrefix = getOption(options, LiteralStringRef("backupPrefix"), StringRef());
		backupRangesCount = getOption(options, LiteralStringRef("backupRangesCount"), 5); //tests can hangs if set higher than 1 + BACKUP_MAP_KEY_LOWER_LIMIT
		backupRangeLengthMax = getOption(options, LiteralStringRef("backupRangeLengthMax"), 1);
		abortAndRestartAfter = getOption(options, LiteralStringRef("abortAndRestartAfter"), (!locked && deterministicRandom()->random01() < 0.5) ? deterministicRandom()->random01() * (restoreAfter - backupAfter) + backupAfter : 0.0);
		differentialBackup = getOption(options, LiteralStringRef("differentialBackup"), deterministicRandom()->random01() < 0.5 ? true : false);
		stopDifferentialAfter = getOption(options, LiteralStringRef("stopDifferentialAfter"),
			differentialBackup ? deterministicRandom()->random01() * (restoreAfter - std::max(abortAndRestartAfter,backupAfter)) + std::max(abortAndRestartAfter,backupAfter) : 0.0);
		agentRequest = getOption(options, LiteralStringRef("simDrAgents"), true);
		shareLogRange = getOption(options, LiteralStringRef("shareLogRange"), false);

		// Use sharedRandomNumber if shareLogRange is true so that we can ensure backup and DR both backup the same range
		beforePrefix = shareLogRange ? (sharedRandomNumber & 1) : (deterministicRandom()->random01() < 0.5);

		if (beforePrefix) {
			extraPrefix = backupPrefix.withPrefix(LiteralStringRef("\xfe\xff\xfe"));
			backupPrefix = backupPrefix.withPrefix(LiteralStringRef("\xfe\xff\xff"));
		}
		else {
			extraPrefix = backupPrefix.withPrefix(LiteralStringRef("\x00\x00\x01"));
			backupPrefix = backupPrefix.withPrefix(LiteralStringRef("\x00\x00\00"));
		}

		ASSERT(backupPrefix != StringRef());

		KeyRef beginRange;
		KeyRef endRange;
		UID randomID = nondeterministicRandom()->randomUniqueID();

		if (shareLogRange) {
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(normalKeys.begin, LiteralStringRef("\xfe\xff\xfe")));
			else
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(strinc(LiteralStringRef("\x00\x00\x01")), normalKeys.end));
		} else if(backupRangesCount <= 0) {
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(normalKeys.begin, std::min(backupPrefix, extraPrefix)));
			else
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(strinc(std::max(backupPrefix, extraPrefix)), normalKeys.end));
		} else {
			// Add backup ranges
			for (int rangeLoop = 0; rangeLoop < backupRangesCount; rangeLoop++)
			{
				// Get a random range of a random sizes
				beginRange = KeyRef(backupRanges.arena(), deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));
				endRange = KeyRef(backupRanges.arena(), deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));

				// Add the range to the array
				backupRanges.push_back_deep(backupRanges.arena(), (beginRange < endRange) ? KeyRangeRef(beginRange, endRange) : KeyRangeRef(endRange, beginRange));

				// Track the added range
				TraceEvent("BackupCorrectness_Range", randomID).detail("RangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
					.detail("RangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		extraDB = Database::createDatabase(extraFile, -1);

		TraceEvent("BARW_Start").detail("Locked", locked);
	}

	virtual std::string description() {
		return "BackupToDBCorrectness";
	}

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	ACTOR static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges, StringRef backupPrefix, Database src, Database dest) {
		state int rangeIndex;
		for (rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			state KeyRangeRef range = ranges[rangeIndex];
			state Key begin = range.begin;
			loop {
				state Transaction tr(src);
				state Transaction tr2(dest);
				try {
					loop {
						state Future<Standalone<RangeResultRef>> srcFuture = tr.getRange(KeyRangeRef(begin, range.end), 1000);
						state Future<Standalone<RangeResultRef>> bkpFuture = tr2.getRange(KeyRangeRef(begin, range.end).withPrefix(backupPrefix), 1000);
						wait(success(srcFuture) && success(bkpFuture));

						auto src = srcFuture.get().begin();
						auto bkp = bkpFuture.get().begin();

						while (src != srcFuture.get().end() && bkp != bkpFuture.get().end()) {
							KeyRef bkpKey = bkp->key.substr(backupPrefix.size());
							if (src->key != bkpKey && src->value != bkp->value) {
								TraceEvent(SevError, "MismatchKeyAndValue").detail("SrcKey", printable(src->key)).detail("SrcVal", printable(src->value)).detail("BkpKey", printable(bkpKey)).detail("BkpVal", printable(bkp->value));
							}
							else if (src->key != bkpKey) {
								TraceEvent(SevError, "MismatchKey").detail("SrcKey", printable(src->key)).detail("SrcVal", printable(src->value)).detail("BkpKey", printable(bkpKey)).detail("BkpVal", printable(bkp->value));
							}
							else if (src->value != bkp->value) {
								TraceEvent(SevError, "MismatchValue").detail("SrcKey", printable(src->key)).detail("SrcVal", printable(src->value)).detail("BkpKey", printable(bkpKey)).detail("BkpVal", printable(bkp->value));
							}
							begin = std::min(src->key, bkpKey);
							if (src->key == bkpKey) {
								++src;
								++bkp;
							}
							else if (src->key < bkpKey) {
								++src;
							}
							else {
								++bkp;
							}
						}
						while (src != srcFuture.get().end() && !bkpFuture.get().more) {
							TraceEvent(SevError, "MissingBkpKey").detail("SrcKey", printable(src->key)).detail("SrcVal", printable(src->value));
							begin = src->key;
							++src;
						}
						while (bkp != bkpFuture.get().end() && !srcFuture.get().more) {
							TraceEvent(SevError, "MissingSrcKey").detail("BkpKey", printable(bkp->key.substr(backupPrefix.size()))).detail("BkpVal", printable(bkp->value));
							begin = bkp->key;
							++bkp;
						}

						if (!srcFuture.get().more && !bkpFuture.get().more) {
							break;
						}

						begin = keyAfter(begin);
					}

					break;
				}
				catch (Error &e) {
					wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> doBackup(BackupToDBCorrectnessWorkload* self, double startDelay, DatabaseBackupAgent* backupAgent, Database cx,
		Key tag, Standalone<VectorRef<KeyRangeRef>> backupRanges, double stopDifferentialDelay, Promise<Void> submitted) {

		state UID	randomID = nondeterministicRandom()->randomUniqueID();

		state Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		wait( delay( startDelay ));

		if (startDelay || BUGGIFY) {
			TraceEvent("BARW_DoBackupAbortBackup1", randomID).detail("Tag", printable(tag)).detail("StartDelay", startDelay);

			try {
				wait(backupAgent->abortBackup(cx, tag));
			}
			catch (Error& e) {
				TraceEvent("BARW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
			wait(backupAgent->unlockBackup(cx, tag));
		}

		// The range clear and submitBackup is being done here in the SAME transaction (which does make SubmitBackup's range emptiness check
		// pointless in this test) because separating them causes rare errors where the SubmitBackup commit result is indeterminite but the
		// submission was in fact successful and the backup actually completes before the retry of SubmitBackup so this second call to submit
		// fails because the destination range is no longer empty.
		TraceEvent("BARW_DoBackupClearAndSubmitBackup", randomID).detail("Tag", printable(tag)).detail("StopWhenDone", stopDifferentialDelay ? "False" : "True");

		try {
			state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(self->extraDB));
			loop{
				try {
					for (auto r : self->backupRanges) {
						if (!r.empty()) {
							auto targetRange = r.withPrefix(self->backupPrefix);
							printf("Clearing %s in destination\n", printable(targetRange).c_str());
							tr2->addReadConflictRange(targetRange);
							tr2->clear(targetRange);
						}
					}
					wait(backupAgent->submitBackup(tr2, tag, backupRanges, stopDifferentialDelay ? false : true, self->backupPrefix, StringRef(), self->locked));
					wait(tr2->commit());
					break;
				}
				catch (Error &e) {
					wait(tr2->onError(e));
				}
			}
		}
		catch (Error &e) {
			TraceEvent("BARW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) {
				throw e;
			}
		}

		submitted.send(Void());

		state UID logUid = wait(backupAgent->getLogUid(cx, tag));

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			TEST(!stopDifferentialFuture.isReady()); //Restore starts at specified time
			wait(stopDifferentialFuture);
			TraceEvent("BARW_DoBackupWaitToDiscontinue", randomID).detail("Tag", printable(tag)).detail("DifferentialAfter", stopDifferentialDelay);

			state bool aborted = false;
			try {
				if (BUGGIFY) {
					TraceEvent("BARW_DoBackupWaitForRestorable", randomID).detail("Tag", printable(tag));
					// Wait until the backup is in a restorable state
					state int resultWait = wait(backupAgent->waitBackup(cx, tag, false));

					TraceEvent("BARW_LastBackupFolder", randomID).detail("BackupTag", printable(tag))
						.detail("LogUid", logUid).detail("WaitStatus", resultWait);

					// Abort the backup, if not the first backup because the second backup may have aborted the backup by now
					if (startDelay) {
						TraceEvent("BARW_DoBackupAbortBackup2", randomID).detail("Tag", printable(tag)).detail("WaitStatus", resultWait);
						aborted = true;
						wait(backupAgent->abortBackup(cx, tag));
					}
					else {
						TraceEvent("BARW_DoBackupDiscontinueBackup", randomID).detail("Tag", printable(tag)).detail("DifferentialAfter", stopDifferentialDelay);
						wait(backupAgent->discontinueBackup(cx, tag));
					}
				}

				else {
					TraceEvent("BARW_DoBackupDiscontinueBackup", randomID).detail("Tag", printable(tag)).detail("DifferentialAfter", stopDifferentialDelay);
					wait(backupAgent->discontinueBackup(cx, tag));
				}
			}
			catch (Error& e) {
				TraceEvent("BARW_DoBackupDiscontinueBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}

			if(aborted) {
				wait(backupAgent->unlockBackup(cx, tag));
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));

		UID _destUid = wait(backupAgent->getDestUid(cx, logUid));
		self->destUid = _destUid;

		state int statusValue = wait(backupAgent->waitBackup(cx, tag, true));
		wait(backupAgent->unlockBackup(cx, tag));

		state std::string statusText;

		std::string _statusText = wait( backupAgent->getStatus(cx, 5, tag) );
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_DoBackupComplete", randomID).detail("Tag", printable(tag))
			.detail("Status", statusText).detail("StatusValue", statusValue);

		return Void();
	}

	ACTOR static Future<Void> checkData(Database cx, UID logUid, UID destUid, UID randomID, Key tag, DatabaseBackupAgent* backupAgent, bool shareLogRange) {
		state Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
		state Key backupLogValuesKey = uidPrefixKey(backupLogKeys.begin, destUid);
		state Key backupLatestVersionsPath = uidPrefixKey(backupLatestVersionsPrefix, destUid);
		state Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
		state int displaySystemKeys = 0;

		// Ensure that there is no left over key within the backup subspace
		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			TraceEvent("BARW_CheckLeftoverKeys", randomID).detail("BackupTag", printable(tag));

			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				// Check the left over tasks
				// We have to wait for the list to empty since an abort and get status
				// can leave extra tasks in the queue
				TraceEvent("BARW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(tag));
				state int64_t taskCount = wait( backupAgent->getTaskCount(tr) );
				state int waitCycles = 0;

				if ((taskCount) && false) {
					TraceEvent("BARW_EndingNonzeroTaskCount", randomID).detail("BackupTag", printable(tag)).detail("TaskCount", taskCount).detail("WaitCycles", waitCycles);
					printf("EndingNonZeroTasks: %ld\n", (long) taskCount);
					wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
				}

				loop {
					waitCycles ++;

					TraceEvent("BARW_NonzeroTaskWait", randomID).detail("BackupTag", printable(tag)).detail("TaskCount", taskCount).detail("WaitCycles", waitCycles);
					printf("%.6f %-10s Wait #%4d for %lld tasks to end\n", now(), randomID.toString().c_str(), waitCycles, (long long) taskCount);

					wait(delay(5.0));
					tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
					int64_t _taskCount = wait( backupAgent->getTaskCount(tr) );
					taskCount = _taskCount;

					if (!taskCount) {
						break;
					}
				}

				if (taskCount) {
					displaySystemKeys ++;
					TraceEvent(SevError, "BARW_NonzeroTaskCount", randomID).detail("BackupTag", printable(tag)).detail("TaskCount", taskCount).detail("WaitCycles", waitCycles);
					printf("BackupCorrectnessLeftoverLogTasks: %ld\n", (long) taskCount);
				}

				Standalone<RangeResultRef> agentValues = wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

				// Error if the system keyspace for the backup tag is not empty
				if (agentValues.size() > 0) {
					displaySystemKeys++;
					printf("BackupCorrectnessLeftoverMutationKeys: (%d) %s\n", agentValues.size(), printable(backupAgentKey).c_str());
					TraceEvent(SevError, "BackupCorrectnessLeftoverMutationKeys", randomID).detail("BackupTag", printable(tag))
						.detail("LeftoverKeys", agentValues.size()).detail("KeySpace", printable(backupAgentKey));
					for (auto & s : agentValues) {
						TraceEvent("BARW_LeftoverKey", randomID).detail("Key", printable(StringRef(s.key.toString()))).detail("Value", printable(StringRef(s.value.toString())));
						printf("   Key: %-50s  Value: %s\n", printable(StringRef(s.key.toString())).c_str(), printable(StringRef(s.value.toString())).c_str());
					}
				}
				else {
					printf("No left over backup agent configuration keys\n");
				}

				Optional<Value> latestVersion = wait(tr->get(backupLatestVersionsKey));
				if (latestVersion.present()) {
					TraceEvent(SevError, "BackupCorrectnessLeftoverVersionKey", randomID).detail("BackupTag", printable(tag)).detail("Key", backupLatestVersionsKey.printable()).detail("Value", BinaryReader::fromStringRef<Version>(latestVersion.get(), Unversioned()));
				} else {
					printf("No left over backup version key\n");
				}

				Standalone<RangeResultRef> versions = wait(tr->getRange(KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1));
				if (!shareLogRange || !versions.size()) {
					Standalone<RangeResultRef> logValues = wait(tr->getRange(KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100));

					// Error if the log/mutation keyspace for the backup tag is not empty
					if (logValues.size() > 0) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftoverLogKeys: (%d) %s\n", logValues.size(), printable(backupLogValuesKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftoverLogKeys", randomID).detail("BackupTag", printable(tag))
							.detail("LeftoverKeys", logValues.size()).detail("KeySpace", printable(backupLogValuesKey)).detail("Version", decodeBKMutationLogKey(logValues[0].key).first);
						for (auto & s : logValues) {
							TraceEvent("BARW_LeftoverKey", randomID).detail("Key", printable(StringRef(s.key.toString()))).detail("Value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n", printable(StringRef(s.key.toString())).c_str(), printable(StringRef(s.value.toString())).c_str());
						}
					}
					else {
						printf("No left over backup log keys\n");
					}
				}

				break;
			}
			catch (Error &e) {
				TraceEvent("BARW_CheckException", randomID).error(e);
				wait(tr->onError(e));
			}
		}

		if (displaySystemKeys) {
			wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupToDBCorrectnessWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state DatabaseBackupAgent restoreAgent(self->extraDB);
		state Future<Void> extraBackup;
		state bool extraTasks = false;
		TraceEvent("BARW_Arguments").detail("BackupTag", printable(self->backupTag)).detail("BackupAfter", self->backupAfter)
			.detail("AbortAndRestartAfter", self->abortAndRestartAfter).detail("DifferentialAfter", self->stopDifferentialAfter);

		state UID randomID = nondeterministicRandom()->randomUniqueID();

		// Increment the backup agent requests
		if (self->agentRequest) {
			BackupToDBCorrectnessWorkload::drAgentRequests++;
		}

		try{
			state Future<Void> startRestore = delay(self->restoreAfter);

			// backup
			wait(delay(self->backupAfter));

			TraceEvent("BARW_DoBackup1", randomID).detail("Tag", printable(self->backupTag));
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(self, 0, &backupAgent, self->extraDB, self->backupTag, self->backupRanges, self->stopDifferentialAfter, submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BARW_DoBackup2", randomID).detail("Tag", printable(self->backupTag)).detail("AbortWait", self->abortAndRestartAfter);
				wait(submitted.getFuture());

				b = b && doBackup(self, self->abortAndRestartAfter, &backupAgent, self->extraDB, self->backupTag, self->backupRanges, self->stopDifferentialAfter, Promise<Void>());
			}

			TraceEvent("BARW_DoBackupWait", randomID).detail("BackupTag", printable(self->backupTag)).detail("AbortAndRestartAfter", self->abortAndRestartAfter);
			wait(b);
			TraceEvent("BARW_DoBackupDone", randomID).detail("BackupTag", printable(self->backupTag)).detail("AbortAndRestartAfter", self->abortAndRestartAfter);

			state UID logUid = wait(backupAgent.getLogUid(self->extraDB, self->backupTag));

			// Occasionally start yet another backup that might still be running when we restore
			if (!self->locked && BUGGIFY) {
				TraceEvent("BARW_SubmitBackup2", randomID).detail("Tag", printable(self->backupTag));
				try {
					extraBackup = backupAgent.submitBackup(self->extraDB, self->backupTag, self->backupRanges, true, self->extraPrefix, StringRef(), self->locked);
				}
				catch (Error& e) {
					TraceEvent("BARW_SubmitBackup2Exception", randomID).error(e).detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
			}

			TEST(!startRestore.isReady()); //Restore starts at specified time
			wait(startRestore);

			if (self->performRestore) {
				// restore database
				TraceEvent("BARW_Restore", randomID).detail("RestoreAfter", self->restoreAfter).detail("BackupTag", printable(self->restoreTag));
				//wait(diffRanges(self->backupRanges, self->backupPrefix, cx, self->extraDB));

				state Transaction tr3(cx);
				loop {
					try {
						for (auto r : self->backupRanges) {
							if(!r.empty()) {
								tr3.addReadConflictRange(r);
								tr3.clear(r);
							}
						}
						wait( tr3.commit() );
						break;
					} catch( Error &e ) {
						wait( tr3.onError(e) );
					}
				}

				Standalone<VectorRef<KeyRangeRef>> restoreRange;

				for (auto r : self->backupRanges) {
					restoreRange.push_back_deep(restoreRange.arena(), KeyRangeRef( r.begin.withPrefix(self->backupPrefix), r.end.withPrefix(self->backupPrefix) ) );
				}

				try {
					wait(restoreAgent.submitBackup(cx, self->restoreTag, restoreRange, true, StringRef(), self->backupPrefix, self->locked));
				}
				catch (Error& e) {
					TraceEvent("BARW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(self->restoreTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				wait(success(restoreAgent.waitBackup(cx, self->restoreTag)));
				wait(restoreAgent.unlockBackup(cx, self->restoreTag));
			}

			if (extraBackup.isValid()) {
				TraceEvent("BARW_WaitExtraBackup", randomID).detail("BackupTag", printable(self->backupTag));
				extraTasks = true;
				try {
					wait(extraBackup);
				}
				catch (Error& e) {
					TraceEvent("BARW_ExtraBackupException", randomID).error(e).detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				TraceEvent("BARW_AbortBackupExtra", randomID).detail("BackupTag", printable(self->backupTag));
				try {
					wait(backupAgent.abortBackup(self->extraDB, self->backupTag));
				}
				catch (Error& e) {
					TraceEvent("BARW_AbortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
			}

			wait( checkData(self->extraDB, logUid, self->destUid, randomID, self->backupTag, &backupAgent, self->shareLogRange) );

			if (self->performRestore) {
				state UID restoreUid = wait(backupAgent.getLogUid(self->extraDB, self->restoreTag));
				wait( checkData(cx, restoreUid, restoreUid, randomID, self->restoreTag, &restoreAgent, self->shareLogRange) );
			}

			TraceEvent("BARW_Complete", randomID).detail("BackupTag", printable(self->backupTag));

			// Decrement the backup agent requets
			if (self->agentRequest) {
				BackupToDBCorrectnessWorkload::drAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator.drAgents == ISimulator::BackupToDB) && (!BackupToDBCorrectnessWorkload::drAgentRequests)) {
				g_simulator.drAgents = ISimulator::NoBackupAgents;
			}
		}
		catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e);
			throw;
		}

		return Void();
	}
};

int BackupToDBCorrectnessWorkload::drAgentRequests = 0;

WorkloadFactory<BackupToDBCorrectnessWorkload> BackupToDBCorrectnessWorkloadFactory("BackupToDBCorrectness");
