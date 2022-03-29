/*
 * BackupToDBCorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process. The
// database must be idle after the restore completes, and this workload checks
// that the restore range does not change post restore.
struct BackupToDBCorrectnessWorkload : TestWorkload {
	double backupAfter, abortAndRestartAfter, restoreAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key backupTag, restoreTag;
	Key backupPrefix, extraPrefix;
	bool beforePrefix;
	int backupRangesCount, backupRangeLengthMax;
	bool differentialBackup, performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	static int drAgentRequests;
	Database extraDB;
	LockDB locked{ false };
	bool shareLogRange;
	UID destUid;

	BackupToDBCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked.set(sharedRandomNumber % 2);
		backupAfter = getOption(options, LiteralStringRef("backupAfter"), 10.0);
		restoreAfter = getOption(options, LiteralStringRef("restoreAfter"), 35.0);
		performRestore = getOption(options, LiteralStringRef("performRestore"), true);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		restoreTag = getOption(options, LiteralStringRef("restoreTag"), LiteralStringRef("restore"));
		backupPrefix = getOption(options, LiteralStringRef("backupPrefix"), StringRef());
		backupRangesCount = getOption(options,
		                              LiteralStringRef("backupRangesCount"),
		                              5); // tests can hangs if set higher than 1 + BACKUP_MAP_KEY_LOWER_LIMIT
		backupRangeLengthMax = getOption(options, LiteralStringRef("backupRangeLengthMax"), 1);
		abortAndRestartAfter =
		    getOption(options,
		              LiteralStringRef("abortAndRestartAfter"),
		              (!locked && deterministicRandom()->random01() < 0.5)
		                  ? deterministicRandom()->random01() * (restoreAfter - backupAfter) + backupAfter
		                  : 0.0);
		differentialBackup = getOption(
		    options, LiteralStringRef("differentialBackup"), deterministicRandom()->random01() < 0.5 ? true : false);
		stopDifferentialAfter =
		    getOption(options,
		              LiteralStringRef("stopDifferentialAfter"),
		              differentialBackup ? deterministicRandom()->random01() *
		                                           (restoreAfter - std::max(abortAndRestartAfter, backupAfter)) +
		                                       std::max(abortAndRestartAfter, backupAfter)
		                                 : 0.0);
		agentRequest = getOption(options, LiteralStringRef("simDrAgents"), true);
		shareLogRange = getOption(options, LiteralStringRef("shareLogRange"), false);

		// Use sharedRandomNumber if shareLogRange is true so that we can ensure backup and DR both backup the same
		// range
		beforePrefix = shareLogRange ? (sharedRandomNumber & 1) : (deterministicRandom()->random01() < 0.5);

		if (beforePrefix) {
			extraPrefix = backupPrefix.withPrefix(LiteralStringRef("\xfe\xff\xfe"));
			backupPrefix = backupPrefix.withPrefix(LiteralStringRef("\xfe\xff\xff"));
		} else {
			extraPrefix = backupPrefix.withPrefix(LiteralStringRef("\x00\x00\x01"));
			backupPrefix = backupPrefix.withPrefix(LiteralStringRef("\x00\x00\00"));
		}

		ASSERT(backupPrefix != StringRef());

		KeyRef beginRange;
		KeyRef endRange;
		UID randomID = nondeterministicRandom()->randomUniqueID();

		if (shareLogRange) {
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(normalKeys.begin, LiteralStringRef("\xfe\xff\xfe")));
			else
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(strinc(LiteralStringRef("\x00\x00\x01")), normalKeys.end));
		} else if (backupRangesCount <= 0) {
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(normalKeys.begin, std::min(backupPrefix, extraPrefix)));
			else
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(strinc(std::max(backupPrefix, extraPrefix)), normalKeys.end));
		} else {
			// Add backup ranges
			for (int rangeLoop = 0; rangeLoop < backupRangesCount; rangeLoop++) {
				// Get a random range of a random sizes
				beginRange = KeyRef(backupRanges.arena(),
				                    deterministicRandom()->randomAlphaNumeric(
				                        deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));
				endRange = KeyRef(backupRanges.arena(),
				                  deterministicRandom()->randomAlphaNumeric(
				                      deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));

				// Add the range to the array
				backupRanges.push_back_deep(backupRanges.arena(),
				                            (beginRange < endRange) ? KeyRangeRef(beginRange, endRange)
				                                                    : KeyRangeRef(endRange, beginRange));

				// Track the added range
				TraceEvent("BackupCorrectness_Range", randomID)
				    .detail("RangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
				    .detail("RangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}

		auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
		extraDB = Database::createDatabase(extraFile, -1);

		TraceEvent("BARW_Start").detail("Locked", locked);
	}

	std::string description() const override { return "BackupToDBCorrectness"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Reads a series of key ranges and returns each range.
	ACTOR static Future<std::vector<RangeResult>> readRanges(Database cx,
	                                                         Standalone<VectorRef<KeyRangeRef>> ranges,
	                                                         StringRef removePrefix) {
		loop {
			state Transaction tr(cx);
			try {
				state std::vector<Future<RangeResult>> results;
				for (auto& range : ranges) {
					results.push_back(tr.getRange(range.removePrefix(removePrefix), 1000));
				}
				wait(waitForAll(results));

				std::vector<RangeResult> ret;
				for (auto result : results) {
					ret.push_back(result.get());
				}
				return ret;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges,
	                                     StringRef backupPrefix,
	                                     Database src,
	                                     Database dest) {
		state int rangeIndex;
		for (rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			state KeyRangeRef range = ranges[rangeIndex];
			state Key begin = range.begin;
			loop {
				state Transaction tr(src);
				state Transaction tr2(dest);
				try {
					loop {
						state Future<RangeResult> srcFuture = tr.getRange(KeyRangeRef(begin, range.end), 1000);
						state Future<RangeResult> bkpFuture =
						    tr2.getRange(KeyRangeRef(begin, range.end).withPrefix(backupPrefix), 1000);
						wait(success(srcFuture) && success(bkpFuture));

						auto src = srcFuture.get().begin();
						auto bkp = bkpFuture.get().begin();

						while (src != srcFuture.get().end() && bkp != bkpFuture.get().end()) {
							KeyRef bkpKey = bkp->key.substr(backupPrefix.size());
							if (src->key != bkpKey && src->value != bkp->value) {
								TraceEvent(SevError, "MismatchKeyAndValue")
								    .detail("SrcKey", printable(src->key))
								    .detail("SrcVal", printable(src->value))
								    .detail("BkpKey", printable(bkpKey))
								    .detail("BkpVal", printable(bkp->value));
							} else if (src->key != bkpKey) {
								TraceEvent(SevError, "MismatchKey")
								    .detail("SrcKey", printable(src->key))
								    .detail("SrcVal", printable(src->value))
								    .detail("BkpKey", printable(bkpKey))
								    .detail("BkpVal", printable(bkp->value));
							} else if (src->value != bkp->value) {
								TraceEvent(SevError, "MismatchValue")
								    .detail("SrcKey", printable(src->key))
								    .detail("SrcVal", printable(src->value))
								    .detail("BkpKey", printable(bkpKey))
								    .detail("BkpVal", printable(bkp->value));
							}
							begin = std::min(src->key, bkpKey);
							if (src->key == bkpKey) {
								++src;
								++bkp;
							} else if (src->key < bkpKey) {
								++src;
							} else {
								++bkp;
							}
						}
						while (src != srcFuture.get().end() && !bkpFuture.get().more) {
							TraceEvent(SevError, "MissingBkpKey")
							    .detail("SrcKey", printable(src->key))
							    .detail("SrcVal", printable(src->value));
							begin = src->key;
							++src;
						}
						while (bkp != bkpFuture.get().end() && !srcFuture.get().more) {
							TraceEvent(SevError, "MissingSrcKey")
							    .detail("BkpKey", printable(bkp->key.substr(backupPrefix.size())))
							    .detail("BkpVal", printable(bkp->value));
							begin = bkp->key;
							++bkp;
						}

						if (!srcFuture.get().more && !bkpFuture.get().more) {
							break;
						}

						begin = keyAfter(begin);
					}

					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> doBackup(BackupToDBCorrectnessWorkload* self,
	                                   double startDelay,
	                                   DatabaseBackupAgent* backupAgent,
	                                   Database cx,
	                                   Key tag,
	                                   Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                   double stopDifferentialDelay,
	                                   Promise<Void> submitted) {

		state UID randomID = nondeterministicRandom()->randomUniqueID();

		state Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		wait(delay(startDelay));

		if (startDelay || BUGGIFY) {
			TraceEvent("BARW_DoBackupAbortBackup1", randomID)
			    .detail("Tag", printable(tag))
			    .detail("StartDelay", startDelay);

			try {
				wait(backupAgent->abortBackup(cx, tag));
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
			wait(backupAgent->unlockBackup(cx, tag));
		}

		// In prior versions of submitBackup, we have seen a rare bug where
		// submitBackup results in a commit_unknown_result, causing the backup
		// to retry when in fact it had successfully completed. On the retry,
		// the range being backed up into was checked to make sure it was
		// empty, and this check was failing because the backup had succeeded
		// the first time. The old solution for this was to clear the backup
		// range in the same transaction as the backup, but now we have
		// switched to passing a "pre-backup action" to either verify the range
		// being backed up into is empty, or clearing it first.
		TraceEvent("BARW_DoBackupClearAndSubmitBackup", randomID)
		    .detail("Tag", printable(tag))
		    .detail("StopWhenDone", stopDifferentialDelay ? "False" : "True");

		try {
			try {
				wait(backupAgent->submitBackup(cx,
				                               tag,
				                               backupRanges,
				                               StopWhenDone{ !stopDifferentialDelay },
				                               self->backupPrefix,
				                               StringRef(),
				                               LockDB{ self->locked },
				                               DatabaseBackupAgent::PreBackupAction::CLEAR));
			} catch (Error& e) {
				TraceEvent("BARW_SubmitBackup1Exception", randomID).error(e);
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) {
					throw;
				}
			}
		} catch (Error& e) {
			TraceEvent("BARW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) {
				throw e;
			}
		}

		submitted.send(Void());

		state UID logUid = wait(backupAgent->getLogUid(cx, tag));

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			TEST(!stopDifferentialFuture.isReady()); // Restore starts at specified time - stopDifferential not ready
			wait(stopDifferentialFuture);
			TraceEvent("BARW_DoBackupWaitToDiscontinue", randomID)
			    .detail("Tag", printable(tag))
			    .detail("DifferentialAfter", stopDifferentialDelay);

			state bool aborted = false;
			try {
				if (BUGGIFY) {
					TraceEvent("BARW_DoBackupWaitForRestorable", randomID).detail("Tag", printable(tag));
					// Wait until the backup is in a restorable state
					state EBackupState resultWait = wait(backupAgent->waitBackup(cx, tag, StopWhenDone::False));

					TraceEvent("BARW_LastBackupFolder", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("LogUid", logUid)
					    .detail("WaitStatus", resultWait);

					// Abort the backup, if not the first backup because the second backup may have aborted the backup
					// by now
					if (startDelay) {
						TraceEvent("BARW_DoBackupAbortBackup2", randomID)
						    .detail("Tag", printable(tag))
						    .detail("WaitStatus", resultWait);
						aborted = true;
						wait(backupAgent->abortBackup(cx, tag));
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
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}

			if (aborted) {
				wait(backupAgent->unlockBackup(cx, tag));
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));

		UID _destUid = wait(backupAgent->getDestUid(cx, logUid));
		self->destUid = _destUid;

		state EBackupState statusValue = wait(backupAgent->waitBackup(cx, tag, StopWhenDone::True));
		wait(backupAgent->unlockBackup(cx, tag));

		state std::string statusText;

		std::string _statusText = wait(backupAgent->getStatus(cx, 5, tag));
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_DoBackupComplete", randomID)
		    .detail("Tag", printable(tag))
		    .detail("Status", statusText)
		    .detail("StatusValue", statusValue);

		return Void();
	}

	ACTOR static Future<Void> checkData(Database cx,
	                                    UID logUid,
	                                    UID destUid,
	                                    UID randomID,
	                                    Key tag,
	                                    DatabaseBackupAgent* backupAgent,
	                                    bool shareLogRange) {
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
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Check the left over tasks
				// We have to wait for the list to empty since an abort and get status
				// can leave extra tasks in the queue
				TraceEvent("BARW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(tag));
				state int64_t taskCount = wait(backupAgent->getTaskCount(tr));
				state int waitCycles = 0;

				if ((taskCount) && false) {
					TraceEvent("BARW_EndingNonzeroTaskCount", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("TaskCount", taskCount)
					    .detail("WaitCycles", waitCycles);
					printf("EndingNonZeroTasks: %ld\n", (long)taskCount);
					wait(TaskBucket::debugPrintRange(cx, LiteralStringRef("\xff"), StringRef()));
				}

				loop {
					waitCycles++;

					TraceEvent("BARW_NonzeroTaskWait", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("TaskCount", taskCount)
					    .detail("WaitCycles", waitCycles);
					printf("%.6f %-10s Wait #%4d for %lld tasks to end\n",
					       now(),
					       randomID.toString().c_str(),
					       waitCycles,
					       (long long)taskCount);

					wait(delay(5.0));
					tr = makeReference<ReadYourWritesTransaction>(cx);
					int64_t _taskCount = wait(backupAgent->getTaskCount(tr));
					taskCount = _taskCount;

					if (!taskCount) {
						break;
					}
				}

				if (taskCount) {
					displaySystemKeys++;
					TraceEvent(SevError, "BARW_NonzeroTaskCount", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("TaskCount", taskCount)
					    .detail("WaitCycles", waitCycles);
					printf("BackupCorrectnessLeftoverLogTasks: %ld\n", (long)taskCount);
				}

				RangeResult agentValues =
				    wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

				// Error if the system keyspace for the backup tag is not empty
				if (agentValues.size() > 0) {
					displaySystemKeys++;
					printf("BackupCorrectnessLeftoverMutationKeys: (%d) %s\n",
					       agentValues.size(),
					       printable(backupAgentKey).c_str());
					TraceEvent(SevError, "BackupCorrectnessLeftoverMutationKeys", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("LeftoverKeys", agentValues.size())
					    .detail("KeySpace", printable(backupAgentKey));
					for (auto& s : agentValues) {
						TraceEvent("BARW_LeftoverKey", randomID)
						    .detail("Key", printable(StringRef(s.key.toString())))
						    .detail("Value", printable(StringRef(s.value.toString())));
						printf("   Key: %-50s  Value: %s\n",
						       printable(StringRef(s.key.toString())).c_str(),
						       printable(StringRef(s.value.toString())).c_str());
					}
				} else {
					printf("No left over backup agent configuration keys\n");
				}

				Optional<Value> latestVersion = wait(tr->get(backupLatestVersionsKey));
				if (latestVersion.present()) {
					TraceEvent(SevError, "BackupCorrectnessLeftoverVersionKey", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("Key", backupLatestVersionsKey.printable())
					    .detail("Value", BinaryReader::fromStringRef<Version>(latestVersion.get(), Unversioned()));
				} else {
					printf("No left over backup version key\n");
				}

				RangeResult versions = wait(
				    tr->getRange(KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1));
				if (!shareLogRange || !versions.size()) {
					RangeResult logValues =
					    wait(tr->getRange(KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100));

					// Error if the log/mutation keyspace for the backup tag is not empty
					if (logValues.size() > 0) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftoverLogKeys: (%d) %s\n",
						       logValues.size(),
						       printable(backupLogValuesKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftoverLogKeys", randomID)
						    .detail("BackupTag", printable(tag))
						    .detail("LeftoverKeys", logValues.size())
						    .detail("KeySpace", printable(backupLogValuesKey))
						    .detail("Version", decodeBKMutationLogKey(logValues[0].key).first);
						for (auto& s : logValues) {
							TraceEvent("BARW_LeftoverKey", randomID)
							    .detail("Key", printable(StringRef(s.key.toString())))
							    .detail("Value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n",
							       printable(StringRef(s.key.toString())).c_str(),
							       printable(StringRef(s.value.toString())).c_str());
						}
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
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupToDBCorrectnessWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state DatabaseBackupAgent restoreTool(self->extraDB);
		state Future<Void> extraBackup;
		state bool extraTasks = false;
		TraceEvent("BARW_Arguments")
		    .detail("BackupTag", printable(self->backupTag))
		    .detail("BackupAfter", self->backupAfter)
		    .detail("AbortAndRestartAfter", self->abortAndRestartAfter)
		    .detail("DifferentialAfter", self->stopDifferentialAfter);

		state UID randomID = nondeterministicRandom()->randomUniqueID();

		// Increment the backup agent requets
		if (self->agentRequest) {
			BackupToDBCorrectnessWorkload::drAgentRequests++;
		}

		try {
			state Future<Void> startRestore = delay(self->restoreAfter);

			// backup
			wait(delay(self->backupAfter));

			TraceEvent("BARW_DoBackup1", randomID).detail("Tag", printable(self->backupTag));
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(self,
			                                0,
			                                &backupAgent,
			                                self->extraDB,
			                                self->backupTag,
			                                self->backupRanges,
			                                self->stopDifferentialAfter,
			                                submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BARW_DoBackup2", randomID)
				    .detail("Tag", printable(self->backupTag))
				    .detail("AbortWait", self->abortAndRestartAfter);
				wait(submitted.getFuture());

				b = b && doBackup(self,
				                  self->abortAndRestartAfter,
				                  &backupAgent,
				                  self->extraDB,
				                  self->backupTag,
				                  self->backupRanges,
				                  self->stopDifferentialAfter,
				                  Promise<Void>());
			}

			TraceEvent("BARW_DoBackupWait", randomID)
			    .detail("BackupTag", printable(self->backupTag))
			    .detail("AbortAndRestartAfter", self->abortAndRestartAfter);
			wait(b);
			TraceEvent("BARW_DoBackupDone", randomID)
			    .detail("BackupTag", printable(self->backupTag))
			    .detail("AbortAndRestartAfter", self->abortAndRestartAfter);

			state UID logUid = wait(backupAgent.getLogUid(self->extraDB, self->backupTag));

			// Occasionally start yet another backup that might still be running when we restore
			if (!self->locked && BUGGIFY) {
				TraceEvent("BARW_SubmitBackup2", randomID).detail("Tag", printable(self->backupTag));
				try {
					extraBackup = backupAgent.submitBackup(self->extraDB,
					                                       self->backupTag,
					                                       self->backupRanges,
					                                       StopWhenDone::True,
					                                       self->extraPrefix,
					                                       StringRef(),
					                                       self->locked,
					                                       DatabaseBackupAgent::PreBackupAction::CLEAR);
				} catch (Error& e) {
					TraceEvent("BARW_SubmitBackup2Exception", randomID)
					    .error(e)
					    .detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
			}

			TEST(!startRestore.isReady()); // Restore starts at specified time
			wait(startRestore);

			if (self->performRestore) {
				// restore database
				TraceEvent("BARW_Restore", randomID)
				    .detail("RestoreAfter", self->restoreAfter)
				    .detail("BackupTag", printable(self->restoreTag));
				// wait(diffRanges(self->backupRanges, self->backupPrefix, cx, self->extraDB));

				state Standalone<VectorRef<KeyRangeRef>> restoreRange;
				for (auto r : self->backupRanges) {
					restoreRange.push_back_deep(
					    restoreRange.arena(),
					    KeyRangeRef(r.begin.withPrefix(self->backupPrefix), r.end.withPrefix(self->backupPrefix)));
				}

				try {
					wait(restoreTool.submitBackup(cx,
					                              self->restoreTag,
					                              restoreRange,
					                              StopWhenDone::True,
					                              StringRef(),
					                              self->backupPrefix,
					                              self->locked,
					                              DatabaseBackupAgent::PreBackupAction::CLEAR));
				} catch (Error& e) {
					TraceEvent("BARW_DoBackupSubmitBackupException", randomID)
					    .error(e)
					    .detail("Tag", printable(self->restoreTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				wait(success(restoreTool.waitBackup(cx, self->restoreTag)));
				wait(restoreTool.unlockBackup(cx, self->restoreTag));

				// Make sure no more data is written to the restored range
				// after the restore completes.
				state std::vector<RangeResult> res1 = wait(readRanges(cx, restoreRange, self->backupPrefix));
				wait(delay(5));
				state std::vector<RangeResult> res2 = wait(readRanges(cx, restoreRange, self->backupPrefix));
				ASSERT(res1.size() == res2.size());
				for (int i = 0; i < res1.size(); ++i) {
					auto range1 = res1.at(i);
					auto range2 = res2.at(i);
					ASSERT(range1.size() == range2.size());

					for (int j = 0; j < range1.size(); ++j) {
						ASSERT(range1[j].key == range2[j].key && range1[j].value == range2[j].value);
					}
				}
			}

			if (extraBackup.isValid()) {
				TraceEvent("BARW_WaitExtraBackup", randomID).detail("BackupTag", printable(self->backupTag));
				extraTasks = true;
				try {
					wait(extraBackup);
				} catch (Error& e) {
					TraceEvent("BARW_ExtraBackupException", randomID)
					    .error(e)
					    .detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				TraceEvent("BARW_AbortBackupExtra", randomID).detail("BackupTag", printable(self->backupTag));
				try {
					// This abort can race with submitBackup such that destUID may
					// not be set yet. Adding "waitForDestUID" flag to avoid the race.
					wait(backupAgent.abortBackup(self->extraDB,
					                             self->backupTag,
					                             PartialBackup::False,
					                             AbortOldBackup::False,
					                             DstOnly::False,
					                             WaitForDestUID::True));
				} catch (Error& e) {
					TraceEvent("BARW_AbortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
			}

			wait(checkData(
			    self->extraDB, logUid, self->destUid, randomID, self->backupTag, &backupAgent, self->shareLogRange));

			if (self->performRestore) {
				state UID restoreUid = wait(backupAgent.getLogUid(self->extraDB, self->restoreTag));
				wait(checkData(
				    cx, restoreUid, restoreUid, randomID, self->restoreTag, &restoreTool, self->shareLogRange));
			}

			TraceEvent("BARW_Complete", randomID).detail("BackupTag", printable(self->backupTag));

			// Decrement the backup agent requets
			if (self->agentRequest) {
				BackupToDBCorrectnessWorkload::drAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator.drAgents == ISimulator::BackupAgentType::BackupToDB) &&
			    (!BackupToDBCorrectnessWorkload::drAgentRequests)) {
				g_simulator.drAgents = ISimulator::BackupAgentType::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e);
			throw;
		}

		return Void();
	}
};

int BackupToDBCorrectnessWorkload::drAgentRequests = 0;

WorkloadFactory<BackupToDBCorrectnessWorkload> BackupToDBCorrectnessWorkloadFactory("BackupToDBCorrectness");
