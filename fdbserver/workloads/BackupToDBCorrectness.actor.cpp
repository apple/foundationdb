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

#include "flow/actorcompiler.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "workloads.h"
#include "BulkSetup.actor.h"

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
	static int backupAgentRequests;
	Database extraDB;
	bool locked;

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
		abortAndRestartAfter = getOption(options, LiteralStringRef("abortAndRestartAfter"), (!locked && g_random->random01() < 0.5) ? g_random->random01() * (restoreAfter - backupAfter) + backupAfter : 0.0);
		differentialBackup = getOption(options, LiteralStringRef("differentialBackup"), g_random->random01() < 0.5 ? true : false);
		stopDifferentialAfter = getOption(options, LiteralStringRef("stopDifferentialAfter"),
			differentialBackup ? g_random->random01() * (restoreAfter - std::max(abortAndRestartAfter,backupAfter)) + std::max(abortAndRestartAfter,backupAfter) : 0.0);
		agentRequest = getOption(options, LiteralStringRef("simBackupAgents"), true);

		beforePrefix = g_random->random01() < 0.5;
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
		UID randomID = g_nondeterministic_random->randomUniqueID();

		if(backupRangesCount <= 0) {
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(normalKeys.begin, std::min(backupPrefix, extraPrefix)));
			else
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(strinc(std::max(backupPrefix, extraPrefix)), normalKeys.end));
		} else {
			// Add backup ranges
			for (int rangeLoop = 0; rangeLoop < backupRangesCount; rangeLoop++)
			{
				// Get a random range of a random sizes
				beginRange = KeyRef(backupRanges.arena(), g_random->randomAlphaNumeric(g_random->randomInt(1, backupRangeLengthMax + 1)));
				endRange = KeyRef(backupRanges.arena(), g_random->randomAlphaNumeric(g_random->randomInt(1, backupRangeLengthMax + 1)));

				// Add the range to the array
				backupRanges.push_back_deep(backupRanges.arena(), (beginRange < endRange) ? KeyRangeRef(beginRange, endRange) : KeyRangeRef(endRange, beginRange));

				// Track the added range
				TraceEvent("BackupCorrectness_Range", randomID).detail("rangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
					.detail("rangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
		extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();

		TraceEvent("BARW_start").detail("locked", locked);
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
						Void _ = wait(success(srcFuture) && success(bkpFuture));

						auto src = srcFuture.get().begin();
						auto bkp = bkpFuture.get().begin();

						while (src != srcFuture.get().end() && bkp != bkpFuture.get().end()) {
							KeyRef bkpKey = bkp->key.substr(backupPrefix.size());
							if (src->key != bkpKey && src->value != bkp->value) {
								TraceEvent(SevError, "MismatchKeyAndValue").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value)).detail("bkpKey", printable(bkpKey)).detail("bkpVal", printable(bkp->value));
							}
							else if (src->key != bkpKey) {
								TraceEvent(SevError, "MismatchKey").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value)).detail("bkpKey", printable(bkpKey)).detail("bkpVal", printable(bkp->value));
							}
							else if (src->value != bkp->value) {
								TraceEvent(SevError, "MismatchValue").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value)).detail("bkpKey", printable(bkpKey)).detail("bkpVal", printable(bkp->value));
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
							TraceEvent(SevError, "MissingBkpKey").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value));
							begin = src->key;
							++src;
						}
						while (bkp != bkpFuture.get().end() && !srcFuture.get().more) {
							TraceEvent(SevError, "MissingSrcKey").detail("bkpKey", printable(bkp->key.substr(backupPrefix.size()))).detail("bkpVal", printable(bkp->value));
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
					Void _ = wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> doBackup(BackupToDBCorrectnessWorkload* self, double startDelay, DatabaseBackupAgent* backupAgent, Database cx,
		Key tag, Standalone<VectorRef<KeyRangeRef>> backupRanges, double stopDifferentialDelay, Promise<Void> submitted) {

		state UID	randomID = g_nondeterministic_random->randomUniqueID();

		state Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		Void _ = wait( delay( startDelay ));

		if (startDelay || BUGGIFY) {
			TraceEvent("BARW_doBackup abortBackup1", randomID).detail("tag", printable(tag)).detail("startDelay", startDelay);

			try {
				Void _ = wait(backupAgent->abortBackup(cx, tag));
			}
			catch (Error& e) {
				TraceEvent("BARW_doBackup abortBackup Exception", randomID).detail("tag", printable(tag)).error(e);
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
			Void _ = wait(backupAgent->unlockBackup(cx, tag));
		}

		// The range clear and submitBackup is being done here in the SAME transaction (which does make SubmitBackup's range emptiness check
		// pointless in this test) because separating them causes rare errors where the SubmitBackup commit result is indeterminite but the
		// submission was in fact successful and the backup actually completes before the retry of SubmitBackup so this second call to submit
		// fails because the destination range is no longer empty.
		TraceEvent("BARW_doBackup clearAndSubmitBackup", randomID).detail("tag", printable(tag)).detail("stopWhenDone", stopDifferentialDelay ? "False" : "True");

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
					Void _ = wait(backupAgent->submitBackup(tr2, tag, backupRanges, stopDifferentialDelay ? false : true, self->backupPrefix, StringRef(), self->locked));
					Void _ = wait(tr2->commit());
					break;
				}
				catch (Error &e) {
					Void _ = wait(tr2->onError(e));
				}
			}
		}
		catch (Error &e) {
			TraceEvent("BARW_doBackup submitBackup Exception", randomID).detail("tag", printable(tag)).error(e);
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) {
				throw e;
			}
		}

		submitted.send(Void());

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			TEST(!stopDifferentialFuture.isReady()); //Restore starts at specified time
			Void _ = wait(stopDifferentialFuture);
			TraceEvent("BARW_doBackup waitToDiscontinue", randomID).detail("tag", printable(tag)).detail("differentialAfter", stopDifferentialDelay);

			state bool aborted = false;
			try {
				if (BUGGIFY) {
					TraceEvent("BARW_doBackup waitForRestorable", randomID).detail("tag", printable(tag));
					// Wait until the backup is in a restorable state
					state int resultWait = wait(backupAgent->waitBackup(cx, tag, false));
					state UID logUid = wait(backupAgent->getLogUid(cx, tag));

					TraceEvent("BARW_lastBackupFolder", randomID).detail("backupTag", printable(tag))
						.detail("logUid", logUid).detail("waitStatus", resultWait);

					// Abort the backup, if not the first backup because the second backup may have aborted the backup by now
					if (startDelay) {
						TraceEvent("BARW_doBackup abortBackup2", randomID).detail("tag", printable(tag)).detail("waitStatus", resultWait);
						aborted = true;
						Void _ = wait(backupAgent->abortBackup(cx, tag));
					}
					else {
						TraceEvent("BARW_doBackup discontinueBackup", randomID).detail("tag", printable(tag)).detail("differentialAfter", stopDifferentialDelay);
						Void _ = wait(backupAgent->discontinueBackup(cx, tag));
					}
				}

				else {
					TraceEvent("BARW_doBackup discontinueBackup", randomID).detail("tag", printable(tag)).detail("differentialAfter", stopDifferentialDelay);
					Void _ = wait(backupAgent->discontinueBackup(cx, tag));
				}
			}
			catch (Error& e) {
				TraceEvent("BARW_doBackup discontinueBackup Exception", randomID).detail("tag", printable(tag)).error(e);
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}

			if(aborted) {
				Void _ = wait(backupAgent->unlockBackup(cx, tag));
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_doBackup waitBackup", randomID).detail("tag", printable(tag));
		state int statusValue = wait(backupAgent->waitBackup(cx, tag, true));
		Void _ = wait(backupAgent->unlockBackup(cx, tag));

		state std::string statusText;

		std::string _statusText = wait( backupAgent->getStatus(cx, 5, tag) );
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_doBackup  complete", randomID).detail("tag", printable(tag))
			.detail("status", statusText).detail("statusValue", statusValue);

		return Void();
	}

	ACTOR static Future<Void> checkData(Database cx, UID logUid, UID randomID, Key tag, DatabaseBackupAgent* backupAgent) {
		state Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
		state Key backupLogValuesKey = uidPrefixKey(backupLogKeys.begin, logUid);
		state int displaySystemKeys = 0;

		// Ensure that there is no left over key within the backup subspace
		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			TraceEvent("BARW_check_leftoverkeys", randomID).detail("backupTag", printable(tag));

			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				// Check the left over tasks
				// We have to wait for the list to empty since an abort and get status
				// can leave extra tasks in the queue
				TraceEvent("BARW_check_leftovertasks", randomID).detail("backupTag", printable(tag));
				state int64_t taskCount = wait( backupAgent->getTaskCount(tr) );
				state int waitCycles = 0;

				if ((taskCount) && (0)) {
					TraceEvent("BARW_EndingNonzeroTaskCount", randomID).detail("backupTag", printable(tag)).detail("taskCount", taskCount).detail("waitCycles", waitCycles);
					printf("EndingNonZeroTasks: %ld\n", (long) taskCount);
					Void _ = wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
				}

				loop {
					waitCycles ++;

					TraceEvent("BARW_NonzeroTaskWait", randomID).detail("backupTag", printable(tag)).detail("taskCount", taskCount).detail("waitCycles", waitCycles);
					printf("%.6f %-10s Wait #%4d for %lld tasks to end\n", now(), randomID.toString().c_str(), waitCycles, (long long) taskCount);

					Void _ = wait(delay(20.0));
					tr->commit();
					tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
					int64_t _taskCount = wait( backupAgent->getTaskCount(tr) );
					taskCount = _taskCount;

					if (!taskCount) {
						break;
					}
				}

				if (taskCount) {
					displaySystemKeys ++;
					TraceEvent(SevError, "BARW_NonzeroTaskCount", randomID).detail("backupTag", printable(tag)).detail("taskCount", taskCount).detail("waitCycles", waitCycles);
					printf("BackupCorrectnessLeftOverLogTasks: %ld\n", (long) taskCount);
				}

				Standalone<RangeResultRef> agentValues = wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

				// Error if the system keyspace for the backup tag is not empty
				if (agentValues.size() > 0) {
					displaySystemKeys++;
					printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n", agentValues.size(), printable(backupAgentKey).c_str());
					TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", randomID).detail("backupTag", printable(tag))
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

				// Error if the log/mutation keyspace for the backup tag is not empty
				if (logValues.size() > 0) {
					displaySystemKeys++;
					printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n", logValues.size(), printable(backupLogValuesKey).c_str());
					TraceEvent(SevError, "BackupCorrectnessLeftOverLogKeys", randomID).detail("backupTag", printable(tag))
						.detail("LeftOverKeys", logValues.size()).detail("keySpace", printable(backupLogValuesKey)).detail("version", decodeBKMutationLogKey(logValues[0].key).first);
					for (auto & s : logValues) {
						TraceEvent("BARW_LeftOverKey", randomID).detail("key", printable(StringRef(s.key.toString()))).detail("value", printable(StringRef(s.value.toString())));
						printf("   Key: %-50s  Value: %s\n", printable(StringRef(s.key.toString())).c_str(), printable(StringRef(s.value.toString())).c_str());
					}
				}
				else {
					printf("No left over backup log keys\n");
				}

				break;
			}
			catch (Error &e) {
				TraceEvent("BARW_check Exception", randomID).error(e);
				Void _ = wait(tr->onError(e));
			}
		}

		if (displaySystemKeys) {
			Void _ = wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupToDBCorrectnessWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state DatabaseBackupAgent restoreAgent(self->extraDB);
		state Future<Void> extraBackup;
		state bool extraTasks = false;
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "backupAndRestore");
		TraceEvent("BARW_Arguments").detail("backupTag", printable(self->backupTag)).detail("backupAfter", self->backupAfter)
			.detail("abortAndRestartAfter", self->abortAndRestartAfter).detail("differentialAfter", self->stopDifferentialAfter);

		state UID randomID = g_nondeterministic_random->randomUniqueID();

		// Increment the backup agent requets
		if (self->agentRequest) {
			BackupToDBCorrectnessWorkload::backupAgentRequests++;
		}

		try{
			state Future<Void> startRestore = delay(self->restoreAfter);

			// backup
			Void _ = wait(delay(self->backupAfter));

			TraceEvent("BARW_doBackup1", randomID).detail("tag", printable(self->backupTag));
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(self, 0, &backupAgent, self->extraDB, self->backupTag, self->backupRanges, self->stopDifferentialAfter, submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BARW_doBackup2", randomID).detail("tag", printable(self->backupTag)).detail("abortWait", self->abortAndRestartAfter);
				Void _ = wait(submitted.getFuture());

				b = b && doBackup(self, self->abortAndRestartAfter, &backupAgent, self->extraDB, self->backupTag, self->backupRanges, self->stopDifferentialAfter, Promise<Void>());
			}

			TraceEvent("BARW_doBackupWait", randomID).detail("backupTag", printable(self->backupTag)).detail("abortAndRestartAfter", self->abortAndRestartAfter);
			Void _ = wait(b);
			TraceEvent("BARW_doBackupDone", randomID).detail("backupTag", printable(self->backupTag)).detail("abortAndRestartAfter", self->abortAndRestartAfter);

			state UID logUid = wait(backupAgent.getLogUid(self->extraDB, self->backupTag));

			// Occasionally start yet another backup that might still be running when we restore
			if (!self->locked && BUGGIFY) {
				TraceEvent("BARW_submitBackup2", randomID).detail("tag", printable(self->backupTag));
				try {
					extraBackup = backupAgent.submitBackup(self->extraDB, self->backupTag, self->backupRanges, true, self->extraPrefix, StringRef(), self->locked);
				}
				catch (Error& e) {
					TraceEvent("BARW_submitBackup2 Exception", randomID).detail("backupTag", printable(self->backupTag)).error(e);
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
			}

			TEST(!startRestore.isReady()); //Restore starts at specified time
			Void _ = wait(startRestore);

			if (self->performRestore) {
				// restore database
				TraceEvent("BARW_restore", randomID).detail("restoreAfter", self->restoreAfter).detail("backupTag", printable(self->restoreTag));
				//Void _ = wait(diffRanges(self->backupRanges, self->backupPrefix, cx, self->extraDB));

				state Transaction tr3(cx);
				loop {
					try {
						for (auto r : self->backupRanges) {
							if(!r.empty()) {
								tr3.addReadConflictRange(r);
								tr3.clear(r);
							}
						}
						Void _ = wait( tr3.commit() );
						break;
					} catch( Error &e ) {
						Void _ = wait( tr3.onError(e) );
					}
				}

				Standalone<VectorRef<KeyRangeRef>> restoreRange;

				for (auto r : self->backupRanges) {
					restoreRange.push_back_deep(restoreRange.arena(), KeyRangeRef( r.begin.withPrefix(self->backupPrefix), r.end.withPrefix(self->backupPrefix) ) );
				}

				try {
					Void _ = wait(restoreAgent.submitBackup(cx, self->restoreTag, restoreRange, true, StringRef(), self->backupPrefix, self->locked));
				}
				catch (Error& e) {
					TraceEvent("BARW_doBackup submitBackup Exception", randomID).detail("tag", printable(self->restoreTag)).error(e);
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				int _ = wait(restoreAgent.waitBackup(cx, self->restoreTag));
				Void _ = wait(restoreAgent.unlockBackup(cx, self->restoreTag));
			}

			if (extraBackup.isValid()) {
				TraceEvent("BARW_wait extraBackup", randomID).detail("backupTag", printable(self->backupTag));
				extraTasks = true;
				try {
					Void _ = wait(extraBackup);
				}
				catch (Error& e) {
					TraceEvent("BARW_extraBackup Exception", randomID).detail("backupTag", printable(self->backupTag)).error(e);
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				TraceEvent("BARW_abortBackup extra", randomID).detail("backupTag", printable(self->backupTag));
				try {
					Void _ = wait(backupAgent.abortBackup(self->extraDB, self->backupTag));
				}
				catch (Error& e) {
					TraceEvent("BARW_abortBackup extra Exception", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
			}

			Void _ = wait( checkData(self->extraDB, logUid, randomID, self->backupTag, &backupAgent) );

			if (self->performRestore) {
				state UID restoreUid = wait(backupAgent.getLogUid(self->extraDB, self->restoreTag));
				Void _ = wait( checkData(cx, restoreUid, randomID, self->restoreTag, &restoreAgent) );
			}

			TraceEvent("BARW_complete", randomID).detail("backupTag", printable(self->backupTag));

			// Decrement the backup agent requets
			if (self->agentRequest) {
				BackupToDBCorrectnessWorkload::backupAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator.backupAgents == ISimulator::BackupToDB) && (!BackupToDBCorrectnessWorkload::backupAgentRequests)) {
				g_simulator.backupAgents = ISimulator::NoBackupAgents;
			}
		}
		catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e);
			throw;
		}

		return Void();
	}
};

int BackupToDBCorrectnessWorkload::backupAgentRequests = 0;

WorkloadFactory<BackupToDBCorrectnessWorkload> BackupToDBCorrectnessWorkloadFactory("BackupToDBCorrectness");
