/*
 * BackupAndParallelRestoreCorrectness.actor.cpp
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
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process
struct BackupAndParallelRestoreCorrectnessWorkload : TestWorkload {
	double backupAfter, restoreAfter, abortAndRestartAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key backupTag;
	int backupRangesCount, backupRangeLengthMax;
	bool differentialBackup, performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	static int backupAgentRequests;
	bool locked;
	bool allowPauses;
	bool shareLogRange;

	std::map<Standalone<KeyRef>, Standalone<ValueRef>> dbKVs;

	BackupAndParallelRestoreCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked = sharedRandomNumber % 2;
		backupAfter = getOption(options, LiteralStringRef("backupAfter"), 10.0);
		restoreAfter = getOption(options, LiteralStringRef("restoreAfter"), 35.0);
		performRestore = getOption(options, LiteralStringRef("performRestore"), true);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		backupRangesCount = getOption(options, LiteralStringRef("backupRangesCount"), 5);
		backupRangeLengthMax = getOption(options, LiteralStringRef("backupRangeLengthMax"), 1);
		abortAndRestartAfter =
		    getOption(options, LiteralStringRef("abortAndRestartAfter"),
		              deterministicRandom()->random01() < 0.5
		                  ? deterministicRandom()->random01() * (restoreAfter - backupAfter) + backupAfter
		                  : 0.0);
		differentialBackup = getOption(options, LiteralStringRef("differentialBackup"),
		                               deterministicRandom()->random01() < 0.5 ? true : false);
		stopDifferentialAfter =
		    getOption(options, LiteralStringRef("stopDifferentialAfter"),
		              differentialBackup ? deterministicRandom()->random01() *
		                                           (restoreAfter - std::max(abortAndRestartAfter, backupAfter)) +
		                                       std::max(abortAndRestartAfter, backupAfter)
		                                 : 0.0);
		agentRequest = getOption(options, LiteralStringRef("simBackupAgents"), true);
		allowPauses = getOption(options, LiteralStringRef("allowPauses"), true);
		shareLogRange = getOption(options, LiteralStringRef("shareLogRange"), false);

		KeyRef beginRange;
		KeyRef endRange;
		UID randomID = nondeterministicRandom()->randomUniqueID();

		if (shareLogRange) {
			bool beforePrefix = sharedRandomNumber & 1;
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(normalKeys.begin, LiteralStringRef("\xfe\xff\xfe")));
			else
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(strinc(LiteralStringRef("\x00\x00\x01")), normalKeys.end));
		} else if (backupRangesCount <= 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		} else {
			// Add backup ranges
			// Q: why the range endpoints (the range interval) are randomly generated?
			// Won't this cause unbalanced range interval in backup?
			std::set<std::string> rangeEndpoints;
			while (rangeEndpoints.size() < backupRangesCount * 2) {
				rangeEndpoints.insert(deterministicRandom()->randomAlphaNumeric(
				    deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));
			}

			// Create ranges from the keys, in order, to prevent overlaps
			std::vector<std::string> sortedEndpoints(rangeEndpoints.begin(), rangeEndpoints.end());
			sort(sortedEndpoints.begin(), sortedEndpoints.end());
			for (auto i = sortedEndpoints.begin(); i != sortedEndpoints.end(); ++i) {
				const std::string& start = *i++;
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(start, *i));

				// Track the added range
				TraceEvent("BARW_BackupCorrectnessRange", randomID)
				    .detail("RangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
				    .detail("RangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}
	}

	static void compareDBKVs(Standalone<RangeResultRef> data, BackupAndParallelRestoreCorrectnessWorkload* self) {
		bool hasDiff = false;
		// Get the new KV pairs in the DB
		std::map<Standalone<KeyRef>, Standalone<ValueRef>> newDbKVs;
		for (auto kvRef = data.contents().begin(); kvRef != data.contents().end(); kvRef++) {
			newDbKVs.insert(std::make_pair(kvRef->key, kvRef->value));
		}

		if (self->dbKVs.empty()) {
			printf("[CheckDB] set DB kv for the first time.\n");
			self->dbKVs = newDbKVs;
			return;
		}

		printf("[CheckDB] KV Number. Prev DB:%d Current DB:%d\n", self->dbKVs.size(), newDbKVs.size());
		// compare the KV pairs in the DB
		printf("------------------Now print out the diff between the prev DB and current DB-------------------\n");
		if (self->dbKVs.size() >= newDbKVs.size()) {
			for (auto kv = self->dbKVs.begin(); kv != self->dbKVs.end(); kv++) {
				bool exist = (newDbKVs.find(kv->first) != newDbKVs.end());
				if (!exist) {
					printf("\tPrevKey:%s PrevValue:%s newValue:%s\n", getHexString(kv->first).c_str(),
					       getHexString(kv->second).c_str(), "[Not Exist]");
					hasDiff = true;
				}
				if (exist && (newDbKVs[kv->first] != self->dbKVs[kv->first])) {
					printf("\tPrevKey:%s PrevValue:%s newValue:%s\n", getHexString(kv->first).c_str(),
					       getHexString(kv->second).c_str(), getHexString(newDbKVs[kv->first]).c_str());
					hasDiff = true;
				}
			}
		} else {
			for (auto newKV = newDbKVs.begin(); newKV != newDbKVs.end(); newKV++) {
				bool exist = (self->dbKVs.find(newKV->first) != self->dbKVs.end());
				if (!exist) {
					printf("\tPrevKey:%s PrevValue:%s newValue:%s\n", "[Not Exist]", getHexString(newKV->first).c_str(),
					       getHexString(newKV->second).c_str());
					hasDiff = true;
				}
				if (exist && (newDbKVs[newKV->first] != self->dbKVs[newKV->first])) {
					printf("\tPrevKey:%s PrevValue:%s newValue:%s\n", getHexString(newKV->first).c_str(),
					       getHexString(self->dbKVs[newKV->first]).c_str(),
					       getHexString(newDbKVs[newKV->first]).c_str());
					hasDiff = true;
				}
			}
		}

		int numEntries = 10;
		int i = 0;
		if (hasDiff) {
			// print out the first and last 10 entries
			printf("\t---Prev DB first and last %d entries\n", numEntries);
			if (!self->dbKVs.empty()) {
				auto kv = self->dbKVs.begin();
				for (; kv != self->dbKVs.end(); kv++) {
					if (i >= numEntries) break;

					printf("\t[Entry:%d]Key:%s Value:%s\n", i++, getHexString(kv->first).c_str(),
					       getHexString(kv->second).c_str());
				}

				i = self->dbKVs.size();
				kv = self->dbKVs.end();
				for (--kv; kv != self->dbKVs.begin(); kv--) {
					if (i <= self->dbKVs.size() - numEntries) break;

					printf("\t[Entry:%d]Key:%s Value:%s\n", i--, getHexString(kv->first).c_str(),
					       getHexString(kv->second).c_str());
				}
			}

			printf("\t---Current DB first and last %d entries\n", numEntries);
			if (!newDbKVs.empty()) {
				auto kv = newDbKVs.begin();
				i = 0;
				for (; kv != newDbKVs.end(); kv++) {
					if (i >= numEntries) break;

					printf("\t[Entry:%d]Key:%s Value:%s\n", i++, getHexString(kv->first).c_str(),
					       getHexString(kv->second).c_str());
				}

				i = newDbKVs.size();
				kv = newDbKVs.end();
				for (--kv; kv != newDbKVs.begin(); kv--) {
					if (i <= newDbKVs.size() - numEntries) break;

					printf("\t[Entry:%d]Key:%s Value:%s\n", i--, getHexString(kv->first).c_str(),
					       getHexString(kv->second).c_str());
				}
			}
		}

		self->dbKVs = newDbKVs; // update the dbKVs
	}

	static void dumpDBKVs(Standalone<RangeResultRef> data, BackupAndParallelRestoreCorrectnessWorkload* self) {
		// bool hasDiff = false;
		// Get the new KV pairs in the DB
		std::map<Standalone<KeyRef>, Standalone<ValueRef>> newDbKVs;
		for (auto kvRef = data.contents().begin(); kvRef != data.contents().end(); kvRef++) {
			newDbKVs.insert(std::make_pair(kvRef->key, kvRef->value));
		}

		printf("---------------------Now print out the KV in the current DB---------------------\n");
		for (auto newKV = newDbKVs.begin(); newKV != newDbKVs.end(); newKV++) {
			printf("\tKey:%s Value:%s\n", getHexString(newKV->first).c_str(), getHexString(newKV->second).c_str());
		}
	}

	ACTOR static Future<Void> checkDB(Database cx, std::string when,
	                                  BackupAndParallelRestoreCorrectnessWorkload* self) {

		return Void();

		//    state Key keyPrefix = LiteralStringRef("");
		//    //              int numPrint = 20; //number of entries in the front and end to print out.
		//    state Transaction tr(cx);
		//    state int retryCount = 0;
		//    loop {
		//            try {
		//                    state Version v = wait( tr.getReadVersion() );
		//                    state Standalone<RangeResultRef> data = wait(tr.getRange(firstGreaterOrEqual(doubleToTestKey(0.0, keyPrefix)), firstGreaterOrEqual(doubleToTestKey(1.0, keyPrefix)), std::numeric_limits<int>::max()));
		//                    printf("Check DB, at %s. retryCount:%d Data size:%d, rangeResultInfo:%s\n", when.c_str(), retryCount,
		//                               data.size(), data.contents().toString().c_str());
		//                    compareDBKVs(data, self);
		//                    break;
		//            } catch (Error& e) {
		//                    retryCount++;
		//                    TraceEvent(retryCount > 20 ? SevWarnAlways : SevWarn, "CheckDBError").error(e);
		//                    wait(tr.onError(e));
		//            }
		//    }

		//    return Void();

	}

	ACTOR static Future<Void> dumpDB(Database cx, std::string when, BackupAndParallelRestoreCorrectnessWorkload* self) {
		state Key keyPrefix = LiteralStringRef("");
		//		int numPrint = 20; //number of entries in the front and end to print out.
		state Transaction tr(cx);
		state int retryCount = 0;
		loop {
			try {
				state Standalone<RangeResultRef> data = wait(
				    tr.getRange(firstGreaterOrEqual(doubleToTestKey(0.0, keyPrefix)),
				                firstGreaterOrEqual(doubleToTestKey(1.0, keyPrefix)), std::numeric_limits<int>::max()));
				printf("dump DB, at %s. retryCount:%d Data size:%d, rangeResultInfo:%s\n", when.c_str(), retryCount,
				       data.size(), data.contents().toString().c_str());
				dumpDBKVs(data, self);
				return Void();
			} catch (Error& e) {
				retryCount++;
				TraceEvent(retryCount > 20 ? SevWarnAlways : SevWarn, "dumpDBError").error(e);
				wait(tr.onError(e));
			}
		}
	}

	virtual std::string description() { return "BackupAndParallelRestoreCorrectness"; }

	virtual Future<Void> setup(Database const& cx) { return Void(); }

	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0) return Void();

		TraceEvent(SevInfo, "BARW_Param").detail("Locked", locked);
		TraceEvent(SevInfo, "BARW_Param").detail("BackupAfter", backupAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("RestoreAfter", restoreAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("PerformRestore", performRestore);
		TraceEvent(SevInfo, "BARW_Param").detail("BackupTag", printable(backupTag).c_str());
		TraceEvent(SevInfo, "BARW_Param").detail("BackupRangesCount", backupRangesCount);
		TraceEvent(SevInfo, "BARW_Param").detail("BackupRangeLengthMax", backupRangeLengthMax);
		TraceEvent(SevInfo, "BARW_Param").detail("AbortAndRestartAfter", abortAndRestartAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("DifferentialBackup", differentialBackup);
		TraceEvent(SevInfo, "BARW_Param").detail("StopDifferentialAfter", stopDifferentialAfter);
		TraceEvent(SevInfo, "BARW_Param").detail("AgentRequest", agentRequest);

		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) { return true; }

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		loop {
			wait(backupAgent->taskBucket->changePause(cx, true));
			wait(delay(30 * deterministicRandom()->random01()));
			wait(backupAgent->taskBucket->changePause(cx, false));
			wait(delay(120 * deterministicRandom()->random01()));
		}
	}

	ACTOR static Future<Void> statusLoop(Database cx, std::string tag) {
		state FileBackupAgent agent;
		loop {
			std::string status = wait(agent.getStatus(cx, true, tag));
			puts(status.c_str());
			wait(delay(2.0));
		}
	}

	ACTOR static Future<Void> doBackup(BackupAndParallelRestoreCorrectnessWorkload* self, double startDelay,
	                                   FileBackupAgent* backupAgent, Database cx, Key tag,
	                                   Standalone<VectorRef<KeyRangeRef>> backupRanges, double stopDifferentialDelay,
	                                   Promise<Void> submittted) {

		state UID randomID = nondeterministicRandom()->randomUniqueID();

		state Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		wait(delay(startDelay));

		if (startDelay || BUGGIFY) {
			TraceEvent("BARW_DoBackupAbortBackup1", randomID)
			    .detail("Tag", printable(tag))
			    .detail("StartDelay", startDelay);

			try {
				wait(backupAgent->abortBackup(cx, tag.toString()));
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded) throw;
			}
		}

		TraceEvent("BARW_DoBackupSubmitBackup", randomID)
		    .detail("Tag", printable(tag))
		    .detail("StopWhenDone", stopDifferentialDelay ? "False" : "True");

		state std::string backupContainer = "file://simfdb/backups/";
		state Future<Void> status = statusLoop(cx, tag.toString());

		try {
			wait(backupAgent->submitBackup(cx, StringRef(backupContainer), deterministicRandom()->randomInt(0, 100),
			                               tag.toString(), backupRanges, stopDifferentialDelay ? false : true));
		} catch (Error& e) {
			TraceEvent("BARW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) throw;
		}

		submittted.send(Void());

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			TEST(!stopDifferentialFuture.isReady()); // Restore starts at specified time
			wait(stopDifferentialFuture);
			TraceEvent("BARW_DoBackupWaitToDiscontinue", randomID)
			    .detail("Tag", printable(tag))
			    .detail("DifferentialAfter", stopDifferentialDelay);

			try {
				if (BUGGIFY) {
					state KeyBackedTag backupTag = makeBackupTag(tag.toString());
					TraceEvent("BARW_DoBackupWaitForRestorable", randomID).detail("Tag", backupTag.tagName);
					// Wait until the backup is in a restorable state
					state int resultWait = wait(backupAgent->waitBackup(cx, backupTag.tagName, false));
					UidAndAbortedFlagT uidFlag = wait(backupTag.getOrThrow(cx));
					state UID logUid = uidFlag.first;
					state Reference<IBackupContainer> lastBackupContainer =
					    wait(BackupConfig(logUid).backupContainer().getD(cx));

					state bool restorable = false;
					if (lastBackupContainer) {
						state BackupDescription desc = wait(lastBackupContainer->describeBackup());
						wait(desc.resolveVersionTimes(cx));
						printf("BackupDescription:\n%s\n", desc.toString().c_str());
						restorable = desc.maxRestorableVersion.present();
					}

					TraceEvent("BARW_LastBackupContainer", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
					    .detail("LogUid", logUid)
					    .detail("WaitStatus", resultWait)
					    .detail("Restorable", restorable);

					// Do not check the backup, if aborted
					if (resultWait == BackupAgentBase::STATE_ABORTED) {
					}
					// Ensure that a backup container was found
					else if (!lastBackupContainer) {
						TraceEvent("BARW_MissingBackupContainer", randomID)
						    .detail("LogUid", logUid)
						    .detail("BackupTag", printable(tag))
						    .detail("WaitStatus", resultWait);
						printf("BackupCorrectnessMissingBackupContainer   tag: %s  status: %d\n",
						       printable(tag).c_str(), resultWait);
					}
					// Check that backup is restorable
					else {
						if (!restorable) {
							TraceEvent("BARW_NotRestorable", randomID)
							    .detail("LogUid", logUid)
							    .detail("BackupTag", printable(tag))
							    .detail("BackupFolder", lastBackupContainer->getURL())
							    .detail("WaitStatus", resultWait);
							printf("BackupCorrectnessNotRestorable:  tag: %s\n", printable(tag).c_str());
						}
					}

					// Abort the backup, if not the first backup because the second backup may have aborted the backup
					// by now
					if (startDelay) {
						TraceEvent("BARW_DoBackupAbortBackup2", randomID)
						    .detail("Tag", printable(tag))
						    .detail("WaitStatus", resultWait)
						    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
						    .detail("Restorable", restorable);
						wait(backupAgent->abortBackup(cx, tag.toString()));
					} else {
						TraceEvent("BARW_DoBackupDiscontinueBackup", randomID)
						    .detail("Tag", printable(tag))
						    .detail("DifferentialAfter", stopDifferentialDelay);
						wait(backupAgent->discontinueBackup(cx, tag));
					}
				}

				else {
					TraceEvent("BARW_DoBackupDiscontinueBackup", randomID)
					    .detail("Tag", printable(tag))
					    .detail("DifferentialAfter", stopDifferentialDelay);
					wait(backupAgent->discontinueBackup(cx, tag));
				}
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupDiscontinueBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) throw;
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));
		state int statusValue = wait(backupAgent->waitBackup(cx, tag.toString(), true));

		state std::string statusText;

		std::string _statusText = wait(backupAgent->getStatus(cx, 5, tag.toString()));
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_DoBackupComplete", randomID)
		    .detail("Tag", printable(tag))
		    .detail("Status", statusText)
		    .detail("StatusValue", statusValue);

		return Void();
	}

	
	// This actor attempts to restore the database without clearing the keyspace.
	// TODO: Enable this function in correctness test
	ACTOR static Future<Void> attemptDirtyRestore(BackupAndParallelRestoreCorrectnessWorkload* self, Database cx,
	                                              FileBackupAgent* backupAgent,
	                                              Standalone<StringRef> lastBackupContainer, UID randomID) {
		state Transaction tr(cx);
		state int rowCount = 0;
		loop {
			try {
				Standalone<RangeResultRef> existingRows = wait(tr.getRange(normalKeys, 1));
				rowCount = existingRows.size();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Try doing a restore without clearing the keys
		if (rowCount > 0) {
			try {
				// TODO: Change to my restore agent code
				TraceEvent(SevError, "MXFastRestore").detail("RestoreFunction", "ShouldChangeToMyOwnRestoreLogic");
				wait(success(backupAgent->restore(cx, cx, self->backupTag, KeyRef(lastBackupContainer), true, -1, true,
				                                  normalKeys, Key(), Key(), self->locked)));
				TraceEvent(SevError, "BARW_RestoreAllowedOverwrittingDatabase", randomID);
				ASSERT(false);
			} catch (Error& e) {
				if (e.code() != error_code_restore_destination_not_empty) {
					throw;
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupAndParallelRestoreCorrectnessWorkload* self) {
		state FileBackupAgent backupAgent;
		state Future<Void> extraBackup;
		state bool extraTasks = false;
		TraceEvent("BARW_Arguments")
		    .detail("BackupTag", printable(self->backupTag))
		    .detail("PerformRestore", self->performRestore)
		    .detail("BackupAfter", self->backupAfter)
		    .detail("RestoreAfter", self->restoreAfter)
		    .detail("AbortAndRestartAfter", self->abortAndRestartAfter)
		    .detail("DifferentialAfter", self->stopDifferentialAfter);

		state UID randomID = nondeterministicRandom()->randomUniqueID();
		if (self->allowPauses && BUGGIFY) {
			state Future<Void> cp = changePaused(cx, &backupAgent);
		}

		// Increment the backup agent requests
		if (self->agentRequest) {
			BackupAndParallelRestoreCorrectnessWorkload::backupAgentRequests++;
		}

		try {
			state Future<Void> startRestore = delay(self->restoreAfter);

			// backup
			wait(delay(self->backupAfter));

			wait(checkDB(cx, "BeforeStartBackup", self));

			TraceEvent("BARW_DoBackup1", randomID).detail("Tag", printable(self->backupTag));
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(self, 0, &backupAgent, cx, self->backupTag, self->backupRanges,
			                                self->stopDifferentialAfter, submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BARW_DoBackup2", randomID)
				    .detail("Tag", printable(self->backupTag))
				    .detail("AbortWait", self->abortAndRestartAfter);
				wait(submitted.getFuture());
				b = b && doBackup(self, self->abortAndRestartAfter, &backupAgent, cx, self->backupTag,
				                  self->backupRanges, self->stopDifferentialAfter, Promise<Void>());
			}

			TraceEvent("BARW_DoBackupWait", randomID)
			    .detail("BackupTag", printable(self->backupTag))
			    .detail("AbortAndRestartAfter", self->abortAndRestartAfter);
			try {
				wait(b);
			} catch (Error& e) {
				if (e.code() != error_code_database_locked) throw;
				if (self->performRestore) throw;
				return Void();
			}
			TraceEvent("BARW_DoBackupDone", randomID)
			    .detail("BackupTag", printable(self->backupTag))
			    .detail("AbortAndRestartAfter", self->abortAndRestartAfter);

			wait(checkDB(cx, "BackupDone", self));

			state KeyBackedTag keyBackedTag = makeBackupTag(self->backupTag.toString());
			UidAndAbortedFlagT uidFlag = wait(keyBackedTag.getOrThrow(cx));
			state UID logUid = uidFlag.first;
			state Key destUidValue = wait(BackupConfig(logUid).destUidValue().getD(cx));
			state Reference<IBackupContainer> lastBackupContainer =
			    wait(BackupConfig(logUid).backupContainer().getD(cx));

			// Occasionally start yet another backup that might still be running when we restore
			if (!self->locked && BUGGIFY) {
				TraceEvent("BARW_SubmitBackup2", randomID).detail("Tag", printable(self->backupTag));
				try {
					extraBackup = backupAgent.submitBackup(cx, LiteralStringRef("file://simfdb/backups/"),
					                                       deterministicRandom()->randomInt(0, 100),
					                                       self->backupTag.toString(), self->backupRanges, true);
				} catch (Error& e) {
					TraceEvent("BARW_SubmitBackup2Exception", randomID)
					    .error(e)
					    .detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) throw;
				}
			}

			TEST(!startRestore.isReady()); // Restore starts at specified time
			wait(startRestore);

			if (lastBackupContainer && self->performRestore) {
				if (deterministicRandom()->random01() < 0.5) {
					printf("TODO: Check if restore can succeed if dirty restore is performed first\n");
					// TODO: To support restore even after we attempt dirty restore. Not implemented in the 1st version
					// fast restore
					// wait(attemptDirtyRestore(self, cx, &backupAgent, StringRef(lastBackupContainer->getURL()),
					// randomID));
				}
				// Clear DB before restore
				wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					for (auto& kvrange : self->backupRanges) tr->clear(kvrange);
					return Void();
				}));

				// restore database
				TraceEvent("BAFRW_Restore", randomID)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("RestoreAfter", self->restoreAfter)
				    .detail("BackupTag", printable(self->backupTag));

				auto container = IBackupContainer::openContainer(lastBackupContainer->getURL());
				BackupDescription desc = wait(container->describeBackup());

				state Version targetVersion = -1;
				if (desc.maxRestorableVersion.present()) {
					if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.minRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.maxRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.5) {
						targetVersion = deterministicRandom()->randomInt64(desc.minRestorableVersion.get(),
						                                                   desc.contiguousLogEnd.get());
					}
				}

				state std::vector<Future<Version>> restores;
				state std::vector<Standalone<StringRef>> restoreTags;
				state int restoreIndex;

				// Restore each range by calling backupAgent.restore()
				printf("Prepare for restore requests. Number of backupRanges:%d\n", self->backupRanges.size());
				state Transaction tr1(cx);
				loop {
					tr1.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr1.setOption(FDBTransactionOptions::LOCK_AWARE);
					try {
						printf("Prepare for restore requests. Number of backupRanges:%d\n", self->backupRanges.size());
						// Note: we always lock DB here in case DB is modified at the bacupRanges boundary.
						for (restoreIndex = 0; restoreIndex < self->backupRanges.size(); restoreIndex++) {
							auto range = self->backupRanges[restoreIndex];
							Standalone<StringRef> restoreTag(self->backupTag.toString() + "_" +
							                                 std::to_string(restoreIndex));
							restoreTags.push_back(restoreTag);
							// Register the request request in DB, which will be picked up by restore worker leader
							struct RestoreRequest restoreRequest(
							    restoreIndex, restoreTag, KeyRef(lastBackupContainer->getURL()), true, targetVersion,
							    true, range, Key(), Key(), self->locked, deterministicRandom()->randomUniqueID());
							tr1.set(restoreRequestKeyFor(restoreRequest.index), restoreRequestValue(restoreRequest));
						}
						tr1.set(restoreRequestTriggerKey,
						        restoreRequestTriggerValue(deterministicRandom()->randomUniqueID(),
						                                   self->backupRanges.size()));
						wait(tr1.commit()); // Trigger restore
						break;
					} catch (Error& e) {
						wait(tr1.onError(e));
					}
				};
				printf("FastRestore:Test workload triggers the restore by setting up restoreRequestTriggerKey\n");

				// Sometimes kill and restart the restore
				if (BUGGIFY) {
					TraceEvent(SevError, "FastRestore").detail("Buggify", "NotImplementedYet");
					wait(delay(deterministicRandom()->randomInt(0, 10)));
					for (restoreIndex = 0; restoreIndex < restores.size(); restoreIndex++) {
						FileBackupAgent::ERestoreState rs =
						    wait(backupAgent.abortRestore(cx, restoreTags[restoreIndex]));
						// The restore may have already completed, or the abort may have been done before the restore
						// was even able to start.  Only run a new restore if the previous one was actually aborted.
						if (rs == FileBackupAgent::ERestoreState::ABORTED) {
							wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
								tr->clear(self->backupRanges[restoreIndex]);
								return Void();
							}));
							// TODO: Not Implemented yet
							// restores[restoreIndex] = backupAgent.restore(cx, restoreTags[restoreIndex],
							// KeyRef(lastBackupContainer->getURL()), true, -1, true, self->backupRanges[restoreIndex],
							// Key(), Key(), self->locked);
						}
					}
				}

				// We should wait on all restore before proceeds
				TraceEvent("FastRestore").detail("BackupAndParallelRestore", "WaitForRestoreToFinish");
				state bool restoreDone = false;
				state ReadYourWritesTransaction tr2(cx);
				state Future<Void> watchForRestoreRequestDone;
				loop {
					try {
						if (restoreDone) break;
						tr2.reset();
						tr2.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr2.setOption(FDBTransactionOptions::LOCK_AWARE);
						Optional<Value> restoreRequestDoneKeyValue = wait(tr2.get(restoreRequestDoneKey));
						// Restore may finish before restoreAgent waits on the restore finish event.
						if (restoreRequestDoneKeyValue.present()) {
							restoreDone = true; // In case commit clears the key but in unknown_state
							tr2.clear(restoreRequestDoneKey);
							wait(tr2.commit());
							break;
						} else {
							watchForRestoreRequestDone = tr2.watch(restoreRequestDoneKey);
							wait(tr2.commit());
							wait(watchForRestoreRequestDone);
							break;
						}
					} catch (Error& e) {
						wait(tr2.onError(e));
					}
				}

				TraceEvent("FastRestore").detail("BackupAndParallelRestore", "RestoreFinished");
				wait(checkDB(cx, "FinishRestore", self));

				for (auto& restore : restores) {
					ASSERT(!restore.isError());
				}
			}

			// Q: What is the extra backup and why do we need to care about it?
			if (extraBackup.isValid()) {
				TraceEvent("BARW_WaitExtraBackup", randomID).detail("BackupTag", printable(self->backupTag));
				extraTasks = true;
				try {
					wait(extraBackup);
				} catch (Error& e) {
					TraceEvent("BARW_ExtraBackupException", randomID)
					    .error(e)
					    .detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) throw;
				}

				TraceEvent("BARW_AbortBackupExtra", randomID).detail("BackupTag", printable(self->backupTag));
				try {
					wait(backupAgent.abortBackup(cx, self->backupTag.toString()));
				} catch (Error& e) {
					TraceEvent("BARW_AbortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded) throw;
				}
			}

			state Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
			state Key backupLogValuesKey = destUidValue.withPrefix(backupLogKeys.begin);
			state Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);
			state Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
			state int displaySystemKeys = 0;

			// Ensure that there is no left over key within the backup subspace
			loop {
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

				TraceEvent("BARW_CheckLeftoverKeys", randomID).detail("BackupTag", printable(self->backupTag));

				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					// Check the left over tasks
					// We have to wait for the list to empty since an abort and get status
					// can leave extra tasks in the queue
					TraceEvent("BARW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(self->backupTag));
					state int64_t taskCount = wait(backupAgent.getTaskCount(tr));
					state int waitCycles = 0;

					loop {
						waitCycles++;

						TraceEvent("BARW_NonzeroTaskWait", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("%.6f %-10s Wait #%4d for %lld tasks to end\n", now(), randomID.toString().c_str(),
						       waitCycles, (long long)taskCount);

						wait(delay(5.0));
						wait(tr->commit());
						tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
						int64_t _taskCount = wait(backupAgent.getTaskCount(tr));
						taskCount = _taskCount;

						if (!taskCount) {
							break;
						}
					}

					if (taskCount) {
						displaySystemKeys++;
						TraceEvent(SevError, "BARW_NonzeroTaskCount", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("BackupCorrectnessLeftOverLogTasks: %ld\n", (long)taskCount);
					}

					Standalone<RangeResultRef> agentValues =
					    wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

					// Error if the system keyspace for the backup tag is not empty
					if (agentValues.size() > 0) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n", agentValues.size(),
						       printable(backupAgentKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("LeftOverKeys", agentValues.size())
						    .detail("KeySpace", printable(backupAgentKey));
						for (auto& s : agentValues) {
							TraceEvent("BARW_LeftOverKey", randomID)
							    .detail("Key", printable(StringRef(s.key.toString())))
							    .detail("Value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n", printable(StringRef(s.key.toString())).c_str(),
							       printable(StringRef(s.value.toString())).c_str());
						}
					} else {
						printf("No left over backup agent configuration keys\n");
					}

					Optional<Value> latestVersion = wait(tr->get(backupLatestVersionsKey));
					if (latestVersion.present()) {
						TraceEvent(SevError, "BackupCorrectnessLeftOverVersionKey", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("BackupLatestVersionsKey", backupLatestVersionsKey.printable())
						    .detail("DestUidValue", destUidValue.printable());
					} else {
						printf("No left over backup version key\n");
					}

					Standalone<RangeResultRef> versions = wait(tr->getRange(
					    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1));
					if (!self->shareLogRange || !versions.size()) {
						Standalone<RangeResultRef> logValues = wait(
						    tr->getRange(KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100));

						// Error if the log/mutation keyspace for the backup tag  is not empty
						if (logValues.size() > 0) {
							displaySystemKeys++;
							printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n", logValues.size(),
							       printable(backupLogValuesKey).c_str());
							TraceEvent(SevError, "BackupCorrectnessLeftOverLogKeys", randomID)
							    .detail("BackupTag", printable(self->backupTag))
							    .detail("LeftOverKeys", logValues.size())
							    .detail("KeySpace", printable(backupLogValuesKey));
						} else {
							printf("No left over backup log keys\n");
						}
					}

					break;
				} catch (Error& e) {
					TraceEvent("BARW_CheckException", randomID).error(e);
					wait(tr->onError(e));
				}
			}

			if (displaySystemKeys) {
				wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
			}

			TraceEvent("BARW_Complete", randomID).detail("BackupTag", printable(self->backupTag));

			// Decrement the backup agent requets
			if (self->agentRequest) {
				BackupAndParallelRestoreCorrectnessWorkload::backupAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator.backupAgents == ISimulator::BackupToFile) &&
			    (!BackupAndParallelRestoreCorrectnessWorkload::backupAgentRequests)) {
				g_simulator.backupAgents = ISimulator::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e).GetLastError();
			throw;
		}
		return Void();
	}
};

int BackupAndParallelRestoreCorrectnessWorkload::backupAgentRequests = 0;

WorkloadFactory<BackupAndParallelRestoreCorrectnessWorkload> BackupAndParallelRestoreCorrectnessWorkloadFactory(
    "BackupAndParallelRestoreCorrectness");
