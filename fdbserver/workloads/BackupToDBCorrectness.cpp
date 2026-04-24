/*
 * BackupToDBCorrectness.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"
#include "flow/ApiVersion.h"

// This workload tests backing up one cluster to another.

// A workload which test the correctness of backup and restore process. The
// database must be idle after the restore completes, and this workload checks
// that the restore range does not change post restore.
struct BackupToDBCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "BackupToDBCorrectness";
	double backupAfter, abortAndRestartAfter, restoreAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key backupTag, restoreTag;
	Key backupPrefix, extraPrefix;
	int backupRangesCount, backupRangeLengthMax;
	bool differentialBackup, performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	static int drAgentRequests;
	Database extraDB;
	LockDB locked{ false };
	bool shareLogRange;
	bool defaultBackup;
	UID destUid;

	// This workload is not compatible with RandomRangeLock workload because they will race in locked range
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	BackupToDBCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked.set(sharedRandomNumber % 2);
		backupAfter = getOption(options, "backupAfter"_sr, 10.0);
		double minBackupAfter = getOption(options, "minBackupAfter"_sr, backupAfter);
		if (backupAfter > minBackupAfter) {
			backupAfter = deterministicRandom()->random01() * (backupAfter - minBackupAfter) + minBackupAfter;
		}
		restoreAfter = getOption(options, "restoreAfter"_sr, 35.0);
		performRestore = getOption(options, "performRestore"_sr, true);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		restoreTag = getOption(options, "restoreTag"_sr, "restore"_sr);
		backupPrefix = getOption(options, "backupPrefix"_sr, StringRef());
		backupRangesCount = getOption(options,
		                              "backupRangesCount"_sr,
		                              5); // tests can hangs if set higher than 1 + BACKUP_MAP_KEY_LOWER_LIMIT
		backupRangeLengthMax = getOption(options, "backupRangeLengthMax"_sr, 1);
		abortAndRestartAfter =
		    getOption(options,
		              "abortAndRestartAfter"_sr,
		              (!locked && deterministicRandom()->random01() < 0.5)
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
		agentRequest = getOption(options, "simDrAgents"_sr, true);
		shareLogRange = getOption(options, "shareLogRange"_sr, false);
		defaultBackup = getOption(options, "defaultBackup"_sr, false);

		// Use sharedRandomNumber if shareLogRange is true so that we can ensure backup and DR both backup the same
		// range
		bool beforePrefix = shareLogRange ? (sharedRandomNumber & 1) : (deterministicRandom()->random01() < 0.5);

		if (!defaultBackup) {
			if (beforePrefix) {
				extraPrefix = backupPrefix.withPrefix("\xfe\xff\xfe"_sr);
				backupPrefix = backupPrefix.withPrefix("\xfe\xff\xff"_sr);
			} else {
				extraPrefix = backupPrefix.withPrefix("\x00\x00\x01"_sr);
				backupPrefix = backupPrefix.withPrefix("\x00\x00\00"_sr);
			}

			ASSERT(!backupPrefix.empty());
		}

		KeyRef beginRange;
		KeyRef endRange;
		UID randomID = nondeterministicRandom()->randomUniqueID();

		if (defaultBackup) {
			addDefaultBackupRanges(backupRanges);
		} else if (shareLogRange) {
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(normalKeys.begin, "\xfe\xff\xfe"_sr));
			else
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(strinc("\x00\x00\x01"_sr), normalKeys.end));
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

		ASSERT(g_simulator->extraDatabases.size() == 1);
		extraDB = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0]);

		TraceEvent("BARW_Start").detail("Locked", locked);
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return _setup(cx, this);
	}

	Future<Void> _setup(Database cx, BackupToDBCorrectnessWorkload* self) {
		if (BUGGIFY) {
			for (auto r : getSystemBackupRanges()) {
				self->backupRanges.push_back_deep(self->backupRanges.arena(), r);
			}
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Reads a series of key ranges and returns each range.
	static Future<std::vector<RangeResult>> readRanges(Database cx,
	                                                   Standalone<VectorRef<KeyRangeRef>> ranges,
	                                                   StringRef removePrefix) {
		while (true) {
			Transaction tr(cx);
			Error err;
			try {
				std::vector<Future<RangeResult>> results;
				for (auto& range : ranges) {
					results.push_back(tr.getRange(range.removePrefix(removePrefix), 1000));
				}
				co_await waitForAll(results);

				std::vector<RangeResult> ret;
				for (const auto& result : results) {
					ret.push_back(result.get());
				}
				co_return ret;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges,
	                               StringRef backupPrefix,
	                               Database src,
	                               Database dest) {
		for (int rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			KeyRangeRef range = ranges[rangeIndex];
			Key begin = range.begin;
			while (true) {
				Transaction tr(src);
				Transaction tr2(dest);
				Error err;
				try {
					while (true) {
						Future<RangeResult> srcFuture = tr.getRange(KeyRangeRef(begin, range.end), 1000);
						Future<RangeResult> bkpFuture =
						    tr2.getRange(KeyRangeRef(begin, range.end).withPrefix(backupPrefix), 1000);
						co_await (success(srcFuture) && success(bkpFuture));

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
					err = e;
				}
				co_await tr.onError(err);
			}
		}
	}

	Future<Void> doBackup(double startDelay,
	                      DatabaseBackupAgent* backupAgent,
	                      Database cx,
	                      Key tag,
	                      Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                      double stopDifferentialDelay,
	                      Promise<Void> submitted) {

		UID randomID = nondeterministicRandom()->randomUniqueID();

		Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		co_await delay(startDelay);

		if (startDelay || BUGGIFY) {
			TraceEvent("BARW_DoBackupAbortBackup1", randomID)
			    .detail("Tag", printable(tag))
			    .detail("StartDelay", startDelay);

			try {
				co_await backupAgent->abortBackup(cx, tag);
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
			co_await backupAgent->unlockBackup(cx, tag);
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
				co_await backupAgent->submitBackup(cx,
				                                   tag,
				                                   backupRanges,
				                                   StopWhenDone{ !stopDifferentialDelay },
				                                   backupPrefix,
				                                   StringRef(),
				                                   LockDB{ locked },
				                                   DatabaseBackupAgent::PreBackupAction::CLEAR);
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

		UID logUid = co_await backupAgent->getLogUid(cx, tag);

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			CODE_PROBE(!stopDifferentialFuture.isReady(),
			           "Restore starts at specified time - stopDifferential not ready");
			co_await stopDifferentialFuture;
			TraceEvent("BARW_DoBackupWaitToDiscontinue", randomID)
			    .detail("Tag", printable(tag))
			    .detail("DifferentialAfter", stopDifferentialDelay);

			bool aborted = false;
			try {
				if (BUGGIFY) {
					TraceEvent("BARW_DoBackupWaitForRestorable", randomID).detail("Tag", printable(tag));
					// Wait until the backup is in a restorable state
					EBackupState resultWait = co_await backupAgent->waitBackup(cx, tag, StopWhenDone::False);

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
						co_await backupAgent->abortBackup(cx, tag);
					} else {
						TraceEvent("BARW_DoBackupDiscontinueBackup", randomID)
						    .detail("Tag", printable(tag))
						    .detail("DifferentialAfter", stopDifferentialDelay);
						co_await backupAgent->discontinueBackup(cx, tag);
					}
				}

				else {
					TraceEvent("BARW_DoBackupDiscontinueBackup", randomID)
					    .detail("Tag", printable(tag))
					    .detail("DifferentialAfter", stopDifferentialDelay);
					co_await backupAgent->discontinueBackup(cx, tag);
				}
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupDiscontinueBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}

			if (aborted) {
				co_await backupAgent->unlockBackup(cx, tag);
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));

		UID _destUid = co_await backupAgent->getDestUid(cx, logUid);
		destUid = _destUid;

		EBackupState statusValue = co_await backupAgent->waitBackup(cx, tag, StopWhenDone::True);
		co_await backupAgent->unlockBackup(cx, tag);

		std::string statusText;

		std::string _statusText = co_await backupAgent->getStatus(cx, 5, tag);
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_DoBackupComplete", randomID)
		    .detail("Tag", printable(tag))
		    .detail("Status", statusText)
		    .detail("StatusValue", statusValue);
	}

	static Future<Void> checkData(Database cx,
	                              UID logUid,
	                              UID destUid,
	                              UID randomID,
	                              Key tag,
	                              DatabaseBackupAgent* backupAgent,
	                              bool shareLogRange) {
		Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
		Key backupLogValuesKey = uidPrefixKey(backupLogKeys.begin, destUid);
		Key backupLatestVersionsPath = uidPrefixKey(backupLatestVersionsPrefix, destUid);
		Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
		int displaySystemKeys = 0;

		// Ensure that there is no left over key within the backup subspace
		while (true) {
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			TraceEvent("BARW_CheckLeftoverKeys", randomID).detail("BackupTag", printable(tag));

			Error err;
			try {
				// Check the left over tasks
				// We have to wait for the list to empty since an abort and get status
				// can leave extra tasks in the queue
				TraceEvent("BARW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(tag));
				int64_t taskCount = co_await backupAgent->getTaskCount(tr);
				int waitCycles = 0;

				if ((taskCount) && false) {
					TraceEvent("BARW_EndingNonzeroTaskCount", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("TaskCount", taskCount)
					    .detail("WaitCycles", waitCycles);
					printf("EndingNonZeroTasks: %ld\n", (long)taskCount);
					co_await TaskBucket::debugPrintRange(cx, "\xff"_sr, StringRef());
				}

				while (taskCount > 0) {
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

					co_await delay(5.0);
					tr = makeReference<ReadYourWritesTransaction>(cx);
					taskCount = co_await backupAgent->getTaskCount(tr);
				}

				RangeResult agentValues =
				    co_await tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100);

				// Error if the system keyspace for the backup tag is not empty
				if (!agentValues.empty()) {
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

				Optional<Value> latestVersion = co_await tr->get(backupLatestVersionsKey);
				if (latestVersion.present()) {
					TraceEvent(SevError, "BackupCorrectnessLeftoverVersionKey", randomID)
					    .detail("BackupTag", printable(tag))
					    .detail("Key", backupLatestVersionsKey.printable())
					    .detail("Value", BinaryReader::fromStringRef<Version>(latestVersion.get(), Unversioned()));
				} else {
					printf("No left over backup version key\n");
				}

				RangeResult versions = co_await tr->getRange(
				    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1);
				if (!shareLogRange || versions.empty()) {
					RangeResult logValues = co_await tr->getRange(
					    KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100);

					// Error if the log/mutation keyspace for the backup tag is not empty
					if (!logValues.empty()) {
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
				err = e;
			}
			TraceEvent("BARW_CheckException", randomID).error(err);
			co_await tr->onError(err);
		}

		if (displaySystemKeys) {
			co_await TaskBucket::debugPrintRange(cx, "\xff"_sr, StringRef());
		}
	}

	Future<Void> _start(Database cx) {
		DatabaseBackupAgent backupAgent(cx);
		DatabaseBackupAgent restoreTool(extraDB);
		Future<Void> extraBackup;
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		TraceEvent("BARW_Arguments")
		    .detail("BackupTag", printable(backupTag))
		    .detail("BackupAfter", backupAfter)
		    .detail("AbortAndRestartAfter", abortAndRestartAfter)
		    .detail("DifferentialAfter", stopDifferentialAfter);

		UID randomID = nondeterministicRandom()->randomUniqueID();

		// Increment the backup agent requests
		if (agentRequest) {
			BackupToDBCorrectnessWorkload::drAgentRequests++;
		}

		try {
			Future<Void> startRestore = delay(restoreAfter);

			// backup
			co_await delay(backupAfter);

			TraceEvent("BARW_DoBackup1", randomID).detail("Tag", printable(backupTag));
			Promise<Void> submitted;
			Future<Void> b =
			    doBackup(0, &backupAgent, extraDB, backupTag, backupRanges, stopDifferentialAfter, submitted);

			if (abortAndRestartAfter) {
				TraceEvent("BARW_DoBackup2", randomID)
				    .detail("Tag", printable(backupTag))
				    .detail("AbortWait", abortAndRestartAfter);
				co_await submitted.getFuture();

				b = b && doBackup(abortAndRestartAfter,
				                  &backupAgent,
				                  extraDB,
				                  backupTag,
				                  backupRanges,
				                  stopDifferentialAfter,
				                  Promise<Void>());
			}

			TraceEvent("BARW_DoBackupWait", randomID)
			    .detail("BackupTag", printable(backupTag))
			    .detail("AbortAndRestartAfter", abortAndRestartAfter);
			co_await b;
			TraceEvent("BARW_DoBackupDone", randomID)
			    .detail("BackupTag", printable(backupTag))
			    .detail("AbortAndRestartAfter", abortAndRestartAfter);

			UID logUid = co_await backupAgent.getLogUid(extraDB, backupTag);

			// Occasionally start yet another backup that might still be running when we restore
			if (!locked && extraPrefix != backupPrefix && BUGGIFY) {
				TraceEvent("BARW_SubmitBackup2", randomID).detail("Tag", printable(backupTag));
				try {
					extraBackup = backupAgent.submitBackup(extraDB,
					                                       backupTag,
					                                       backupRanges,
					                                       StopWhenDone::True,
					                                       extraPrefix,
					                                       StringRef(),
					                                       locked,
					                                       DatabaseBackupAgent::PreBackupAction::CLEAR);
				} catch (Error& e) {
					TraceEvent("BARW_SubmitBackup2Exception", randomID)
					    .error(e)
					    .detail("BackupTag", printable(backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
			}

			CODE_PROBE(!startRestore.isReady(), "Restore starts at specified time");
			co_await startRestore;

			if (performRestore) {
				// restore database
				TraceEvent("BARW_Restore", randomID)
				    .detail("RestoreAfter", restoreAfter)
				    .detail("BackupTag", printable(restoreTag));
				// wait(diffRanges(backupRanges, backupPrefix, cx, extraDB));

				Standalone<VectorRef<KeyRangeRef>> restoreRange;
				Standalone<VectorRef<KeyRangeRef>> systemRestoreRange;
				for (auto r : backupRanges) {
					if (!r.intersects(getSystemBackupRanges())) {
						restoreRange.push_back_deep(
						    restoreRange.arena(),
						    KeyRangeRef(r.begin.withPrefix(backupPrefix), r.end.withPrefix(backupPrefix)));
					} else {
						KeyRangeRef normalKeyRange = r & normalKeys;
						KeyRangeRef systemKeyRange = r & systemKeys;
						if (!normalKeyRange.empty()) {
							restoreRange.push_back_deep(restoreRange.arena(),
							                            KeyRangeRef(normalKeyRange.begin.withPrefix(backupPrefix),
							                                        normalKeyRange.end.withPrefix(backupPrefix)));
						}
						if (!systemKeyRange.empty()) {
							systemRestoreRange.push_back_deep(systemRestoreRange.arena(), systemKeyRange);
						}
					}
				}

				// restore system keys first before restoring user data
				if (!systemRestoreRange.empty()) {
					Key systemRestoreTag = "restore_system"_sr;
					try {
						co_await restoreTool.submitBackup(cx,
						                                  systemRestoreTag,
						                                  systemRestoreRange,
						                                  StopWhenDone::True,
						                                  StringRef(),
						                                  backupPrefix,
						                                  locked,
						                                  DatabaseBackupAgent::PreBackupAction::CLEAR);
					} catch (Error& e) {
						TraceEvent("BARW_DoBackupSubmitBackupException", randomID)
						    .error(e)
						    .detail("Tag", printable(systemRestoreTag));
						if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
							throw;
					}
					co_await restoreTool.waitBackup(cx, systemRestoreTag);
					co_await restoreTool.unlockBackup(cx, systemRestoreTag);
				}

				try {
					co_await restoreTool.submitBackup(cx,
					                                  restoreTag,
					                                  restoreRange,
					                                  StopWhenDone::True,
					                                  StringRef(),
					                                  backupPrefix,
					                                  locked,
					                                  DatabaseBackupAgent::PreBackupAction::CLEAR);
				} catch (Error& e) {
					TraceEvent("BARW_DoBackupSubmitBackupException", randomID)
					    .error(e)
					    .detail("Tag", printable(restoreTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				co_await restoreTool.waitBackup(cx, restoreTag);
				co_await restoreTool.unlockBackup(cx, restoreTag);

				// Make sure no more data is written to the restored range
				// after the restore completes.
				std::vector<RangeResult> res1 = co_await readRanges(cx, restoreRange, backupPrefix);
				co_await delay(5);
				std::vector<RangeResult> res2 = co_await readRanges(cx, restoreRange, backupPrefix);
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
				TraceEvent("BARW_WaitExtraBackup", randomID).detail("BackupTag", printable(backupTag));
				try {
					co_await extraBackup;
				} catch (Error& e) {
					TraceEvent("BARW_ExtraBackupException", randomID)
					    .error(e)
					    .detail("BackupTag", printable(backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}

				TraceEvent("BARW_AbortBackupExtra", randomID).detail("BackupTag", printable(backupTag));
				try {
					// This abort can race with submitBackup such that destUID may
					// not be set yet. Adding "waitForDestUID" flag to avoid the race.
					co_await backupAgent.abortBackup(extraDB,
					                                 backupTag,
					                                 PartialBackup::False,
					                                 AbortOldBackup::False,
					                                 DstOnly::False,
					                                 WaitForDestUID::True);
				} catch (Error& e) {
					TraceEvent("BARW_AbortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
			}

			co_await checkData(extraDB, logUid, destUid, randomID, backupTag, &backupAgent, shareLogRange);

			if (performRestore) {
				UID restoreUid = co_await backupAgent.getLogUid(extraDB, restoreTag);
				co_await checkData(cx, restoreUid, restoreUid, randomID, restoreTag, &restoreTool, shareLogRange);
			}

			TraceEvent("BARW_Complete", randomID).detail("BackupTag", printable(backupTag));

			// Decrement the backup agent requests
			if (agentRequest) {
				BackupToDBCorrectnessWorkload::drAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator->drAgents == ISimulator::BackupAgentType::BackupToDB) &&
			    (!BackupToDBCorrectnessWorkload::drAgentRequests)) {
				g_simulator->drAgents = ISimulator::BackupAgentType::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e);
			throw;
		}
	}
};

int BackupToDBCorrectnessWorkload::drAgentRequests = 0;

WorkloadFactory<BackupToDBCorrectnessWorkload> BackupToDBCorrectnessWorkloadFactory;
