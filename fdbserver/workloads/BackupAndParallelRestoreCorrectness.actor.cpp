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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define TEST_ABORT_FASTRESTORE	0

#define SevFRTestInfo SevVerbose
//#define SevFRTestInfo SevInfo

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
	bool usePartitionedLogs;
	Key addPrefix, removePrefix; // Orignal key will be first apply removePrefix and then addPrefix
	// CAVEAT: When removePrefix is used, we must ensure every key in backup have the removePrefix

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
		usePartitionedLogs = getOption(options, LiteralStringRef("usePartitionedLogs"),
		                               deterministicRandom()->random01() < 0.5 ? true : false);
		addPrefix = getOption(options, LiteralStringRef("addPrefix"), LiteralStringRef(""));
		removePrefix = getOption(options, LiteralStringRef("removePrefix"), LiteralStringRef(""));

		KeyRef beginRange;
		KeyRef endRange;
		UID randomID = nondeterministicRandom()->randomUniqueID();

		// Generate addPrefix
		if (addPrefix.size() == 0 && removePrefix.size() == 0) {
			if (deterministicRandom()->random01() < 2) { // Generate random addPrefix
				int len = deterministicRandom()->randomInt(1, 100);
				std::string randomStr = deterministicRandom()->randomAlphaNumeric(len);
				TraceEvent("BackupAndParallelRestoreCorrectness")
				    .detail("GenerateAddPrefix", randomStr)
				    .detail("Length", len)
				    .detail("StrLen", randomStr.size());
				addPrefix = Key(randomStr);
			}
		}
		TraceEvent("BackupAndParallelRestoreCorrectness")
		    .detail("AddPrefix", addPrefix)
		    .detail("RemovePrefix", removePrefix);
		// Do not support removePrefix right now because we must ensure all backup keys have the removePrefix
		// otherwise, test will fail because fast restore will simply add the removePrefix to every key in the end.
		ASSERT(removePrefix.size() == 0);

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

	bool hasPrefix() { return addPrefix != LiteralStringRef("") || removePrefix != LiteralStringRef(""); }

	virtual Future<bool> check(Database const& cx) { return true; }

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		loop {
			wait(backupAgent->changePause(cx, true));
			wait(delay(30 * deterministicRandom()->random01()));
			wait(backupAgent->changePause(cx, false));
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
			                               tag.toString(), backupRanges, stopDifferentialDelay ? false : true,
			                               self->usePartitionedLogs));
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
					// Wait until the backup is in a restorable state and get the status, URL, and UID atomically
					state Reference<IBackupContainer> lastBackupContainer;
					state UID lastBackupUID;
					state int resultWait = wait(backupAgent->waitBackup(cx, backupTag.tagName, false, &lastBackupContainer, &lastBackupUID));

					TraceEvent("BARW_DoBackupWaitForRestorable", randomID).detail("Tag", backupTag.tagName).detail("Result", resultWait);

					state bool restorable = false;
					if (lastBackupContainer) {
						state Future<BackupDescription> fdesc = lastBackupContainer->describeBackup();
						wait(ready(fdesc));

						if(!fdesc.isError()) {
							state BackupDescription desc = fdesc.get();
							wait(desc.resolveVersionTimes(cx));
							printf("BackupDescription:\n%s\n", desc.toString().c_str());
							restorable = desc.maxRestorableVersion.present();
						}
					}

					TraceEvent("BARW_LastBackupContainer", randomID)
						.detail("BackupTag", printable(tag))
						.detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
						.detail("LastBackupUID", lastBackupUID).detail("WaitStatus", resultWait).detail("Restorable", restorable);

					// Do not check the backup, if aborted
					if (resultWait == BackupAgentBase::STATE_ABORTED) {
					}
					// Ensure that a backup container was found
					else if (!lastBackupContainer) {
						TraceEvent(SevError, "BARW_MissingBackupContainer", randomID).detail("LastBackupUID", lastBackupUID).detail("BackupTag", printable(tag)).detail("WaitStatus", resultWait);
						printf("BackupCorrectnessMissingBackupContainer   tag: %s  status: %d\n",
						       printable(tag).c_str(), resultWait);
					}
					// Check that backup is restorable
					else if(!restorable) {
						TraceEvent(SevError, "BARW_NotRestorable", randomID).detail("LastBackupUID", lastBackupUID).detail("BackupTag", printable(tag))
							.detail("BackupFolder", lastBackupContainer->getURL()).detail("WaitStatus", resultWait);
						printf("BackupCorrectnessNotRestorable:  tag: %s\n", printable(tag).c_str());
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

	// Write [begin, end) in kvs to DB
	ACTOR static Future<Void> writeKVs(Database cx, Standalone<VectorRef<KeyValueRef>> kvs, int begin, int end) {
		wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			int index = begin;
			while (index < end) {
				TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKV")
				    .detail("Index", index)
				    .detail("KVs", kvs.size())
				    .detail("Key", kvs[index].key)
				    .detail("Value", kvs[index].value);
				tr->set(kvs[index].key, kvs[index].value);
				++index;
			}
			return Void();
		}));

		// Sanity check data has been written to DB
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				KeyRef k1 = kvs[begin].key;
				KeyRef k2 = end < kvs.size() ? kvs[end].key : normalKeys.end;
				TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVReadBack")
				    .detail("Range", KeyRangeRef(k1, k2))
				    .detail("Begin", begin)
				    .detail("End", end);
				Standalone<RangeResultRef> readKVs = wait(tr.getRange(KeyRangeRef(k1, k2), CLIENT_KNOBS->TOO_MANY));
				ASSERT(readKVs.size() > 0 || begin == end);
				break;
			} catch (Error& e) {
				TraceEvent("TransformDatabaseContentsWriteKVReadBackError").error(e);
				wait(tr.onError(e));
			}
		}

		TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVDone").detail("Begin", begin).detail("End", end);

		return Void();
	}

	static std::pair<bool, bool> insideValidRange(KeyValueRef kv, Standalone<VectorRef<KeyRangeRef>> restoreRanges,
	                                              Standalone<VectorRef<KeyRangeRef>> backupRanges) {
		bool insideRestoreRange = false;
		bool insideBackupRange = false;
		for (auto& range : restoreRanges) {
			TraceEvent("InsideValidRestoreRange")
			    .detail("Key", kv.key)
			    .detail("Range", range)
			    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
			if (kv.key >= range.begin && kv.key < range.end) {
				insideRestoreRange = true;
				break;
			}
		}
		for (auto& range : backupRanges) {
			TraceEvent("InsideValidBackupRange")
			    .detail("Key", kv.key)
			    .detail("Range", range)
			    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
			if (kv.key >= range.begin && kv.key < range.end) {
				insideBackupRange = true;
				break;
			}
		}
		return std::make_pair(insideBackupRange, insideRestoreRange);
	}

	// restoreRanges is the actual range that has applied removePrefix and addPrefix processed by restore system
	// Assume: restoreRanges do not overlap which is achieved by ensuring backup ranges do not overlap
	ACTOR static Future<Void> transformDatabaseContents(Database cx, Key addPrefix, Key removePrefix,
	                                                    Standalone<VectorRef<KeyRangeRef>> restoreRanges) {
		state ReadYourWritesTransaction tr(cx);
		state Standalone<VectorRef<KeyValueRef>> oldData;

		loop { // Read all data from DB
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				TraceEvent("FastRestoreWorkloadTransformDatabaseContents")
				    .detail("AddPrefix", addPrefix)
				    .detail("RemovePrefix", removePrefix);

				state int i = 0;
				for (i = 0; i < restoreRanges.size(); ++i) {
					Standalone<RangeResultRef> kvs = wait(tr.getRange(restoreRanges[i], CLIENT_KNOBS->TOO_MANY));
					ASSERT(!kvs.more);
					for (auto kv : kvs) {
						oldData.push_back_deep(oldData.arena(), KeyValueRef(kv.key, kv.value));
					}
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
				oldData = Standalone<VectorRef<KeyValueRef>>(); // clear the vector
			}
		}

		// Convert data by removePrefix and addPrefix in memory
		state Standalone<VectorRef<KeyValueRef>> newKVs;
		for (int i = 0; i < oldData.size(); ++i) {
			Key newKey(oldData[i].key);
			TraceEvent(SevFRTestInfo, "TransformDatabaseContents")
			    .detail("Keys", oldData.size())
			    .detail("Index", i)
			    .detail("GetKey", oldData[i].key)
			    .detail("GetValue", oldData[i].value);
			if (newKey.size() < removePrefix.size()) { // If true, must check why?!
				TraceEvent(SevError, "TransformDatabaseContents")
				    .detail("Key", newKey)
				    .detail("RemovePrefix", removePrefix);
				continue;
			}
			newKey = newKey.removePrefix(removePrefix).withPrefix(addPrefix);
			newKVs.push_back_deep(newKVs.arena(), KeyValueRef(newKey.contents(), oldData[i].value));
			TraceEvent(SevFRTestInfo, "TransformDatabaseContents")
			    .detail("Keys", newKVs.size())
			    .detail("Index", i)
			    .detail("NewKey", newKVs.back().key)
			    .detail("NewValue", newKVs.back().value);
		}

		state Standalone<VectorRef<KeyRangeRef>> backupRanges; // dest. ranges
		for (auto& range : restoreRanges) {
			KeyRange tmpRange = range;
			backupRanges.push_back_deep(backupRanges.arena(),
			                            tmpRange.removePrefix(removePrefix).withPrefix(addPrefix));
		}

		// Clear the transformed data (original data with removePrefix and addPrefix) in restoreRanges
		wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			for (int i = 0; i < restoreRanges.size(); i++) {
				TraceEvent(SevFRTestInfo, "TransformDatabaseContents")
				    .detail("ClearRestoreRange", restoreRanges[i])
				    .detail("ClearBackupRange", backupRanges[i]);
				tr->clear(restoreRanges[i]); // Clear the range.removePrefix().withPrefix()
				tr->clear(backupRanges[i]);
			}
			return Void();
		}));

		// Sanity check to ensure no data in the ranges
		loop {
			try {
				tr.reset();
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Standalone<RangeResultRef> emptyData = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
				for (int i = 0; i < emptyData.size(); ++i) {
					TraceEvent(SevError, "ExpectEmptyData")
					    .detail("Index", i)
					    .detail("Key", emptyData[i].key)
					    .detail("Value", emptyData[i].value);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Write transformed KVs (i.e., kv backup took) back to DB
		state std::vector<Future<Void>> fwrites;
		loop {
			try {
				state int begin = 0;
				state int len = 0;
				while (begin < newKVs.size()) {
					len = std::min(100, newKVs.size() - begin);
					fwrites.push_back(writeKVs(cx, newKVs, begin, begin + len));
					begin = begin + len;
				}
				wait(waitForAll(fwrites));
				break;
			} catch (Error& e) {
				TraceEvent(SevError, "FastRestoreWorkloadTransformDatabaseContentsUnexpectedErrorOnWriteKVs").error(e);
				wait(tr.onError(e));
			}
		}

		// Sanity check
		loop {
			try {
				tr.reset();
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Standalone<RangeResultRef> allData = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
				TraceEvent(SevFRTestInfo, "SanityCheckData").detail("Size", allData.size());
				for (int i = 0; i < allData.size(); ++i) {
					std::pair<bool, bool> backupRestoreValid =
					    insideValidRange(allData[i], restoreRanges, backupRanges);
					TraceEvent(backupRestoreValid.first ? SevFRTestInfo : SevError, "SanityCheckData")
					    .detail("Index", i)
					    .detail("Key", allData[i].key)
					    .detail("Value", allData[i].value)
					    .detail("InsideBackupRange", backupRestoreValid.first)
					    .detail("InsideRestoreRange", backupRestoreValid.second);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("FastRestoreWorkloadTransformDatabaseContentsFinish")
		    .detail("AddPrefix", addPrefix)
		    .detail("RemovePrefix", removePrefix);

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
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		state int restoreIndex = 0;
		state ReadYourWritesTransaction tr2(cx);

		TraceEvent("BARW_Arguments")
		    .detail("BackupTag", printable(self->backupTag))
		    .detail("PerformRestore", self->performRestore)
		    .detail("BackupAfter", self->backupAfter)
		    .detail("RestoreAfter", self->restoreAfter)
		    .detail("AbortAndRestartAfter", self->abortAndRestartAfter)
		    .detail("DifferentialAfter", self->stopDifferentialAfter);

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
					// Note the "partitionedLog" must be false, because we change
					// the configuration to disable backup workers before restore.
					extraBackup = backupAgent.submitBackup(
					    cx, LiteralStringRef("file://simfdb/backups/"), deterministicRandom()->randomInt(0, 100),
					    self->backupTag.toString(), self->backupRanges, true, false);
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

				// We must ensure no backup workers are running, otherwise the clear DB
				// below can be picked up by backup workers and applied during restore.
				wait(success(changeConfig(cx, "backup_worker_enabled:=0", true)));

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
				ASSERT(self->usePartitionedLogs == desc.partitioned);
				ASSERT(desc.minRestorableVersion.present()); // We must have a valid backup now.

				state Version targetVersion = -1;
				if (desc.maxRestorableVersion.present()) {
					if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.minRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.maxRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.5 &&
					           desc.minRestorableVersion.get() < desc.contiguousLogEnd.get()) {
						// The assertion may fail because minRestorableVersion may be decided by snapshot version.
						// ASSERT_WE_THINK(desc.minRestorableVersion.get() <= desc.contiguousLogEnd.get());
						// This assertion can fail when contiguousLogEnd < maxRestorableVersion and
						// the snapshot version > contiguousLogEnd. I.e., there is a gap between
						// contiguousLogEnd and snapshot version.
						// ASSERT_WE_THINK(desc.contiguousLogEnd.get() > desc.maxRestorableVersion.get());
						targetVersion = deterministicRandom()->randomInt64(desc.minRestorableVersion.get(),
						                                                   desc.contiguousLogEnd.get());
					}
				}

				TraceEvent("BAFRW_Restore", randomID)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("MinRestorableVersion", desc.minRestorableVersion.get())
				    .detail("MaxRestorableVersion", desc.maxRestorableVersion.get())
				    .detail("ContiguousLogEnd", desc.contiguousLogEnd.get())
				    .detail("TargetVersion", targetVersion);

				state std::vector<Future<Version>> restores;
				state std::vector<Standalone<StringRef>> restoreTags;

				// Submit parallel restore requests
				// TODO: Add addPrefix and removePrefix feature
				TraceEvent("BackupAndParallelRestoreWorkload")
				    .detail("PrepareRestores", self->backupRanges.size())
				    .detail("AddPrefix", self->addPrefix)
				    .detail("RemovePrefix", self->removePrefix);
				wait(backupAgent.submitParallelRestore(cx, self->backupTag, self->backupRanges,
				                                       KeyRef(lastBackupContainer->getURL()), targetVersion,
				                                       self->locked, randomID, self->addPrefix, self->removePrefix));
				TraceEvent("BackupAndParallelRestoreWorkload")
				    .detail("TriggerRestore", "Setting up restoreRequestTriggerKey");

				// Sometimes kill and restart the restore
				// In real cluster, aborting a restore needs:
				// (1) kill restore cluster; (2) clear dest. DB restore system keyspace.
				// TODO: Consider gracefully abort a restore and restart.
				if (BUGGIFY && TEST_ABORT_FASTRESTORE) {
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

				// Wait for parallel restore to finish before we can proceed
				TraceEvent("FastRestoreWorkload").detail("WaitForRestoreToFinish", randomID);
				// Do not unlock DB when restore finish because we need to transformDatabaseContents
				wait(backupAgent.parallelRestoreFinish(cx, randomID, !self->hasPrefix()));
				TraceEvent("FastRestoreWorkload").detail("RestoreFinished", randomID);

				for (auto& restore : restores) {
					ASSERT(!restore.isError());
				}

				// If addPrefix or removePrefix set, we want to transform the effect by copying data
				if (self->hasPrefix()) {
					try {
						Standalone<VectorRef<KeyRangeRef>> restoreRanges;
						for (int i = 0; i < self->backupRanges.size(); ++i) {
							KeyRange range(self->backupRanges[i]);
							Key begin = range.begin.removePrefix(self->removePrefix).withPrefix(self->addPrefix);
							Key end = range.end.removePrefix(self->removePrefix).withPrefix(self->addPrefix);
							TraceEvent("FastRestoreWorkloadTransformDatabaseContents")
							    .detail("Begin", begin)
							    .detail("End", end);
							restoreRanges.push_back_deep(restoreRanges.arena(),
							                             KeyRangeRef(begin.contents(), end.contents()));
						}
						wait(transformDatabaseContents(cx, self->removePrefix, self->addPrefix, restoreRanges));
						wait(unlockDatabase(cx, randomID));
					} catch (Error& e) {
						TraceEvent(SevError, "BackupAndParallelRestoreWorkloadTransformPrefix", randomID).error(e);
						throw;
					}
				}
			}

			// Q: What is the extra backup and why do we need to care about it?
			if (extraBackup.isValid()) { // SOMEDAY: Handle this case
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
			TraceEvent(SevError, "BackupAndParallelRestoreCorrectness").error(e).GetLastError();
			throw;
		}
		return Void();
	}
};

int BackupAndParallelRestoreCorrectnessWorkload::backupAgentRequests = 0;

WorkloadFactory<BackupAndParallelRestoreCorrectnessWorkload> BackupAndParallelRestoreCorrectnessWorkloadFactory(
    "BackupAndParallelRestoreCorrectness");
