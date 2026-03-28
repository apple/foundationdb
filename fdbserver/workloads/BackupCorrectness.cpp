/*
 * BackupCorrectness.cpp
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
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "BulkSetup.h"
#include "flow/IRandom.h"

// TODO: explain the purpose of this workload and how it different from the
// 20+ (literally) other backup/restore workloads.

// A workload which test the correctness of backup and restore process
struct BackupAndRestoreCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "BackupAndRestoreCorrectness";
	double backupAfter, restoreAfter, abortAndRestartAfter;
	double minBackupAfter;
	double backupStartAt, restoreStartAfterBackupFinished, stopDifferentialAfter;
	Key backupTag;
	int backupRangesCount, backupRangeLengthMax;
	bool differentialBackup, performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	std::vector<KeyRange> skippedRestoreRanges;
	Standalone<VectorRef<KeyRangeRef>> restoreRanges;
	static int backupAgentRequests;
	LockDB locked{ false };
	bool allowPauses;
	bool shareLogRange;
	bool shouldSkipRestoreRanges;
	bool defaultBackup;
	Optional<std::string> encryptionKeyFileName;

	// This workload is not compatible with RandomRangeLock workload because they will race in locked range
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	BackupAndRestoreCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked.set(sharedRandomNumber % 2);
		backupAfter = getOption(options, "backupAfter"_sr, 10.0);
		double minBackupAfter = getOption(options, "minBackupAfter"_sr, backupAfter);
		if (backupAfter > minBackupAfter) {
			backupAfter = deterministicRandom()->random01() * (backupAfter - minBackupAfter) + minBackupAfter;
		}
		restoreAfter = getOption(options, "restoreAfter"_sr, 35.0);
		performRestore = getOption(options, "performRestore"_sr, true);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		backupRangesCount = getOption(options, "backupRangesCount"_sr, 5);
		backupRangeLengthMax = getOption(options, "backupRangeLengthMax"_sr, 1);
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
		agentRequest = getOption(options, "simBackupAgents"_sr, true);
		allowPauses = getOption(options, "allowPauses"_sr, true);
		shareLogRange = getOption(options, "shareLogRange"_sr, false);
		defaultBackup = getOption(options, "defaultBackup"_sr, false);

		std::vector<std::string> restorePrefixesToInclude =
		    getOption(options, "restorePrefixesToInclude"_sr, std::vector<std::string>());

		shouldSkipRestoreRanges = deterministicRandom()->random01() < 0.3 ? true : false;
		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}

		TraceEvent("BARW_ClientId").detail("Id", wcx.clientId);
		UID randomID = nondeterministicRandom()->randomUniqueID();
		TraceEvent("BARW_PerformRestore", randomID).detail("Value", performRestore);
		if (defaultBackup) {
			addDefaultBackupRanges(backupRanges);
		} else if (shareLogRange) {
			bool beforePrefix = sharedRandomNumber & 1;
			if (beforePrefix)
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(normalKeys.begin, "\xfe\xff\xfe"_sr));
			else
				backupRanges.push_back_deep(backupRanges.arena(),
				                            KeyRangeRef(strinc("\x00\x00\x01"_sr), normalKeys.end));
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
				TraceEvent("BARW_BackupCorrectnessRange", randomID).detail("RangeBegin", start).detail("RangeEnd", *i);
			}
		}

		if (performRestore && !restorePrefixesToInclude.empty() && shouldSkipRestoreRanges) {
			for (auto& range : backupRanges) {
				bool intersection = false;
				for (auto& prefix : restorePrefixesToInclude) {
					KeyRange prefixRange(KeyRangeRef(prefix, strinc(prefix)));
					if (range.intersects(prefixRange)) {
						intersection = true;
					}
					TraceEvent("BARW_PrefixSkipRangeDetails")
					    .detail("PrefixMandatory", printable(prefix))
					    .detail("BackupRange", printable(range))
					    .detail("Intersection", intersection);
				}
				// If the backup range intersects with restorePrefixesToInclude or a coin flip is true then use it as a
				// restore range as well, otherwise skip it.
				if (intersection || deterministicRandom()->coinflip()) {
					restoreRanges.push_back_deep(restoreRanges.arena(), range);
				} else {
					skippedRestoreRanges.push_back(range);
				}
			}
		} else {
			restoreRanges = backupRanges;
		}

		// If no random backup ranges intersected with restorePrefixesToInclude or won the coin flip then restoreRanges
		// will be empty, so move an item from skippedRestoreRanges to restoreRanges.
		if (restoreRanges.empty()) {
			ASSERT(!skippedRestoreRanges.empty());
			restoreRanges.push_back_deep(restoreRanges.arena(), skippedRestoreRanges.back());
			skippedRestoreRanges.pop_back();
		}

		for (auto& range : restoreRanges) {
			TraceEvent("BARW_RestoreRange", randomID)
			    .detail("RangeBegin", printable(range.begin))
			    .detail("RangeEnd", printable(range.end));
		}
		for (auto& range : skippedRestoreRanges) {
			TraceEvent("BARW_SkipRange", randomID)
			    .detail("RangeBegin", printable(range.begin))
			    .detail("RangeEnd", printable(range.end));
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}

		return _setup(cx, this);
	}

	Future<Void> _setup(Database cx, BackupAndRestoreCorrectnessWorkload* self) {
		if (BUGGIFY) {
			for (auto r : getSystemBackupRanges()) {
				self->backupRanges.push_back_deep(self->backupRanges.arena(), r);
			}
			for (auto r : getSystemBackupRanges()) {
				self->restoreRanges.push_back_deep(self->restoreRanges.arena(), r);
			}
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

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
		TraceEvent(SevInfo, "BARW_Param").detail("Encrypted", encryptionKeyFileName.present());

		return _start(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		else
			return _check(cx);
	}

	Future<bool> _check(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				for (int restoreIndex = 0; restoreIndex < skippedRestoreRanges.size(); restoreIndex++) {
					KeyRangeRef range = skippedRestoreRanges[restoreIndex];
					Standalone<StringRef> restoreTag(backupTag.toString() + "_" + std::to_string(restoreIndex));
					RangeResult res = co_await tr.getRange(range, GetRangeLimits::ROW_LIMIT_UNLIMITED);
					if (!res.empty()) {
						TraceEvent(SevError, "BARW_UnexpectedRangePresent").detail("Range", printable(range));
						co_return false;
					}
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		co_return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		while (true) {
			co_await backupAgent->changePause(cx, true);
			co_await delay(30 * deterministicRandom()->random01());
			co_await backupAgent->changePause(cx, false);
			co_await delay(120 * deterministicRandom()->random01());
		}
	}

	static Future<Void> statusLoop(Database cx, std::string tag) {
		FileBackupAgent agent;
		while (true) {
			bool active = co_await agent.checkActive(cx);
			TraceEvent("BARW_AgentActivityCheck").detail("IsActive", active);
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
				co_await backupAgent->abortBackup(cx, tag.toString());
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BARW_DoBackupSubmitBackup", randomID)
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
			                                   UsePartitionedLog::False,
			                                   IncrementalBackupOnly::False,
			                                   encryptionKeyFileName);
		} catch (Error& e) {
			TraceEvent("BARW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		submitted.send(Void());

		// Stop the differential backup, if enabled
		if (stopDifferentialDelay) {
			CODE_PROBE(!stopDifferentialFuture.isReady(),
			           "Restore starts at specified time - stopDifferential not ready");
			co_await stopDifferentialFuture;
			TraceEvent("BARW_DoBackupWaitToDiscontinue", randomID)
			    .detail("Tag", printable(tag))
			    .detail("DifferentialAfter", stopDifferentialDelay);

			try {
				if (BUGGIFY) {
					KeyBackedTag backupTag = makeBackupTag(tag.toString());
					TraceEvent("BARW_DoBackupWaitForRestorable", randomID).detail("Tag", backupTag.tagName);

					// Wait until the backup is in a restorable state and get the status, URL, and UID atomically
					Reference<IBackupContainer> lastBackupContainer;
					UID lastBackupUID;
					EBackupState resultWait = co_await backupAgent->waitBackup(
					    cx, backupTag.tagName, StopWhenDone::False, &lastBackupContainer, &lastBackupUID);

					TraceEvent("BARW_DoBackupWaitForRestorable", randomID)
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

					TraceEvent("BARW_LastBackupContainer", randomID)
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
						TraceEvent(SevError, "BARW_MissingBackupContainer", randomID)
						    .detail("LastBackupUID", lastBackupUID)
						    .detail("BackupTag", printable(tag))
						    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait));
						printf("BackupCorrectnessMissingBackupContainer   tag: %s  status: %s\n",
						       printable(tag).c_str(),
						       BackupAgentBase::getStateText(resultWait));
					}
					// Check that backup is restorable
					else if (!restorable) {
						TraceEvent(SevError, "BARW_NotRestorable", randomID)
						    .detail("LastBackupUID", lastBackupUID)
						    .detail("BackupTag", printable(tag))
						    .detail("BackupFolder", lastBackupContainer->getURL())
						    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait));
						printf("BackupCorrectnessNotRestorable:  tag: %s\n", printable(tag).c_str());
					}

					// Abort the backup, if not the first backup because the second backup may have aborted the backup
					// by now
					if (startDelay) {
						TraceEvent("BARW_DoBackupAbortBackup2", randomID)
						    .detail("Tag", printable(tag))
						    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait))
						    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
						    .detail("Restorable", restorable);
						co_await backupAgent->abortBackup(cx, tag.toString());
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
		}

		// Wait for the backup to complete
		TraceEvent("BARW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));
		EBackupState statusValue = co_await backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True);

		std::string statusText;

		std::string _statusText = co_await backupAgent->getStatus(cx, ShowErrors::True, tag.toString());
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_DoBackupComplete", randomID)
		    .detail("Tag", printable(tag))
		    .detail("Status", statusText)
		    .detail("StatusValue", BackupAgentBase::getStateText(statusValue));
	}

	/**
	    This actor attempts to restore the database without clearing the keyspace.
	 */
	Future<Void> attemptDirtyRestore(Database cx,
	                                 FileBackupAgent* backupAgent,
	                                 Standalone<StringRef> lastBackupContainer,
	                                 UID randomID,
	                                 Optional<std::string> encryptionKeyFileName) {
		Transaction tr(cx);
		int rowCount = 0;
		while (true) {
			Error err;
			try {
				RangeResult existingRows = co_await tr.getRange(normalKeys, 1);
				rowCount = existingRows.size();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		// Try doing a restore without clearing the keys
		if (rowCount > 0) {
			try {
				co_await backupAgent->restore(cx,
				                              cx,
				                              backupTag,
				                              KeyRef(lastBackupContainer),
				                              {},
				                              WaitForComplete::True,
				                              ::invalidVersion,
				                              Verbose::True,
				                              normalKeys,
				                              Key(),
				                              Key(),
				                              locked,
				                              OnlyApplyMutationLogs::False,
				                              InconsistentSnapshotOnly::False,
				                              ::invalidVersion,
				                              encryptionKeyFileName);
				TraceEvent(SevError, "BARW_RestoreAllowedOverwrittingDatabase", randomID).log();
				ASSERT(false);
			} catch (Error& e) {
				if (e.code() != error_code_restore_destination_not_empty) {
					throw;
				}
			}
		}
	}

	static Future<Void> clearAndRestoreSystemKeys(Database cx,
	                                              BackupAndRestoreCorrectnessWorkload* self,
	                                              FileBackupAgent* backupAgent,
	                                              Version targetVersion,
	                                              Reference<IBackupContainer> lastBackupContainer,
	                                              Standalone<VectorRef<KeyRangeRef>> systemRestoreRanges) {
		// restore system keys before restoring any other ranges
		co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			for (auto& range : systemRestoreRanges)
				tr->clear(range);
			return Void();
		});
		Standalone<StringRef> restoreTag(self->backupTag.toString() + "_system");
		printf("BackupCorrectness, backupAgent.restore is called for tag:%s\n", restoreTag.toString().c_str());
		co_await backupAgent->restore(cx,
		                              cx,
		                              restoreTag,
		                              KeyRef(lastBackupContainer->getURL()),
		                              lastBackupContainer->getProxy(),
		                              systemRestoreRanges,
		                              WaitForComplete::True,
		                              targetVersion,
		                              Verbose::True,
		                              Key(),
		                              Key(),
		                              self->locked,
		                              UnlockDB::True,
		                              OnlyApplyMutationLogs::False,
		                              InconsistentSnapshotOnly::False,
		                              ::invalidVersion,
		                              lastBackupContainer->getEncryptionKeyFileName());
		printf("BackupCorrectness, backupAgent.restore finished for tag:%s\n", restoreTag.toString().c_str());
	}

	Future<Void> _start(Database cx) {
		FileBackupAgent backupAgent;
		Future<Void> extraBackup;
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		TraceEvent("BARW_Arguments")
		    .detail("BackupTag", printable(backupTag))
		    .detail("PerformRestore", performRestore)
		    .detail("BackupAfter", backupAfter)
		    .detail("RestoreAfter", restoreAfter)
		    .detail("AbortAndRestartAfter", abortAndRestartAfter)
		    .detail("DifferentialAfter", stopDifferentialAfter);

		UID randomID = nondeterministicRandom()->randomUniqueID();
		if (allowPauses && BUGGIFY) {
			Future<Void> cp = changePaused(cx, &backupAgent);
		}

		// Increment the backup agent requests
		if (agentRequest) {
			BackupAndRestoreCorrectnessWorkload::backupAgentRequests++;
		}

		if (encryptionKeyFileName.present()) {
			co_await BackupContainerFileSystem::createTestEncryptionKeyFile(encryptionKeyFileName.get());
		}

		try {
			Future<Void> startRestore = delay(restoreAfter);

			// backup
			co_await delay(backupAfter);

			TraceEvent("BARW_DoBackup1", randomID).detail("Tag", printable(backupTag));
			Promise<Void> submitted;
			Future<Void> b = doBackup(0, &backupAgent, cx, backupTag, backupRanges, stopDifferentialAfter, submitted);

			if (abortAndRestartAfter) {
				TraceEvent("BARW_DoBackup2", randomID)
				    .detail("Tag", printable(backupTag))
				    .detail("AbortWait", abortAndRestartAfter);
				co_await submitted.getFuture();
				b = b && doBackup(abortAndRestartAfter,
				                  &backupAgent,
				                  cx,
				                  backupTag,
				                  backupRanges,
				                  stopDifferentialAfter,
				                  Promise<Void>());
			}

			TraceEvent("BARW_DoBackupWait", randomID)
			    .detail("BackupTag", printable(backupTag))
			    .detail("AbortAndRestartAfter", abortAndRestartAfter);
			try {
				co_await b;
			} catch (Error& e) {
				if (e.code() != error_code_database_locked)
					throw;
				if (performRestore)
					throw;
				co_return;
			}
			TraceEvent("BARW_DoBackupDone", randomID)
			    .detail("BackupTag", printable(backupTag))
			    .detail("AbortAndRestartAfter", abortAndRestartAfter);

			KeyBackedTag keyBackedTag = makeBackupTag(backupTag.toString());
			UidAndAbortedFlagT uidFlag = co_await keyBackedTag.getOrThrow(cx.getReference());
			UID logUid = uidFlag.first;
			Key destUidValue = co_await BackupConfig(logUid).destUidValue().getD(cx.getReference());
			Reference<IBackupContainer> lastBackupContainer =
			    co_await BackupConfig(logUid).backupContainer().getD(cx.getReference());

			// Occasionally start yet another backup that might still be running when we restore
			if (!locked && BUGGIFY) {
				TraceEvent("BARW_SubmitBackup2", randomID).detail("Tag", printable(backupTag));
				try {
					extraBackup = backupAgent.submitBackup(cx,
					                                       "file://simfdb/backups/"_sr,
					                                       {},
					                                       deterministicRandom()->randomInt(0, 60),
					                                       deterministicRandom()->randomInt(0, 100),
					                                       backupTag.toString(),
					                                       backupRanges,
					                                       StopWhenDone::True);
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

			if (lastBackupContainer && performRestore) {
				auto container = IBackupContainer::openContainer(lastBackupContainer->getURL(),
				                                                 lastBackupContainer->getProxy(),
				                                                 lastBackupContainer->getEncryptionKeyFileName());
				BackupDescription desc = co_await container->describeBackup();

				if (deterministicRandom()->random01() < 0.5) {
					co_await attemptDirtyRestore(cx,
					                             &backupAgent,
					                             StringRef(lastBackupContainer->getURL()),
					                             randomID,
					                             lastBackupContainer->getEncryptionKeyFileName());
				}

				co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					for (auto& kvrange : backupRanges)
						tr->clear(kvrange);
					return Void();
				});

				// restore database
				TraceEvent("BARW_Restore", randomID)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("RestoreAfter", restoreAfter)
				    .detail("BackupTag", printable(backupTag));

				Version targetVersion = -1;
				if (desc.maxRestorableVersion.present()) {
					if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.minRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.maxRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.5) {
						targetVersion = (desc.minRestorableVersion.get() != desc.maxRestorableVersion.get())
						                    ? deterministicRandom()->randomInt64(desc.minRestorableVersion.get(),
						                                                         desc.maxRestorableVersion.get())
						                    : desc.maxRestorableVersion.get();
					}
				}

				TraceEvent("BARW_RestoreDebug").detail("TargetVersion", targetVersion);

				std::vector<Future<Version>> restores;
				std::vector<Standalone<StringRef>> restoreTags;
				bool multipleRangesInOneTag = false;
				int restoreIndex = 0;
				// make sure system keys are not present in the restoreRanges as they will get restored first separately
				// from the rest
				Standalone<VectorRef<KeyRangeRef>> modifiedRestoreRanges;
				Standalone<VectorRef<KeyRangeRef>> systemRestoreRanges;
				for (int i = 0; i < restoreRanges.size(); ++i) {
					if (!restoreRanges[i].intersects(getSystemBackupRanges())) {
						modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), restoreRanges[i]);
					} else {
						KeyRangeRef normalKeyRange = restoreRanges[i] & normalKeys;
						KeyRangeRef systemKeyRange = restoreRanges[i] & systemKeys;
						if (!normalKeyRange.empty()) {
							modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), normalKeyRange);
						}
						if (!systemKeyRange.empty()) {
							systemRestoreRanges.push_back_deep(systemRestoreRanges.arena(), systemKeyRange);
						}
					}
				}
				restoreRanges = modifiedRestoreRanges;
				if (!systemRestoreRanges.empty()) {
					// We are able to restore system keys first since we restore an entire cluster at once rather than
					// partial key ranges.
					co_await clearAndRestoreSystemKeys(
					    cx, this, &backupAgent, targetVersion, lastBackupContainer, systemRestoreRanges);
				}
				if (deterministicRandom()->random01() < 0.5) {
					for (restoreIndex = 0; restoreIndex < restoreRanges.size(); restoreIndex++) {
						auto range = restoreRanges[restoreIndex];
						Standalone<StringRef> restoreTag(backupTag.toString() + "_" + std::to_string(restoreIndex));
						restoreTags.push_back(restoreTag);
						printf("BackupCorrectness, restore for each range: backupAgent.restore is called for "
						       "restoreIndex:%d tag:%s ranges:%s\n",
						       restoreIndex,
						       range.toString().c_str(),
						       restoreTag.toString().c_str());
						restores.push_back(backupAgent.restore(cx,
						                                       cx,
						                                       restoreTag,
						                                       KeyRef(lastBackupContainer->getURL()),
						                                       lastBackupContainer->getProxy(),
						                                       WaitForComplete::True,
						                                       targetVersion,
						                                       Verbose::True,
						                                       range,
						                                       Key(),
						                                       Key(),
						                                       locked,
						                                       OnlyApplyMutationLogs::False,
						                                       InconsistentSnapshotOnly::False,
						                                       ::invalidVersion,
						                                       lastBackupContainer->getEncryptionKeyFileName()));
					}
				} else {
					multipleRangesInOneTag = true;
					Standalone<StringRef> restoreTag(backupTag.toString() + "_" + std::to_string(restoreIndex));
					restoreTags.push_back(restoreTag);
					printf("BackupCorrectness, backupAgent.restore is called for restoreIndex:%d tag:%s\n",
					       restoreIndex,
					       restoreTag.toString().c_str());
					restores.push_back(backupAgent.restore(cx,
					                                       cx,
					                                       restoreTag,
					                                       KeyRef(lastBackupContainer->getURL()),
					                                       lastBackupContainer->getProxy(),
					                                       restoreRanges,
					                                       WaitForComplete::True,
					                                       targetVersion,
					                                       Verbose::True,
					                                       Key(),
					                                       Key(),
					                                       locked,
					                                       UnlockDB::True,
					                                       OnlyApplyMutationLogs::False,
					                                       InconsistentSnapshotOnly::False,
					                                       ::invalidVersion,
					                                       lastBackupContainer->getEncryptionKeyFileName()));
				}

				// Sometimes kill and restart the restore
				if (BUGGIFY) {
					co_await delay(deterministicRandom()->randomInt(0, 10));
					if (multipleRangesInOneTag) {
						FileBackupAgent::ERestoreState rs = co_await backupAgent.abortRestore(cx, restoreTags[0]);
						// The restore may have already completed, or the abort may have been done before the restore
						// was even able to start.  Only run a new restore if the previous one was actually aborted.
						if (rs == FileBackupAgent::ERestoreState::ABORTED) {
							co_await runRYWTransaction(cx,
							                           [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
								                           tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
								                           for (auto& range : restoreRanges)
									                           tr->clear(range);
								                           return Void();
							                           });
							restores[restoreIndex] =
							    backupAgent.restore(cx,
							                        cx,
							                        restoreTags[restoreIndex],
							                        KeyRef(lastBackupContainer->getURL()),
							                        lastBackupContainer->getProxy(),
							                        restoreRanges,
							                        WaitForComplete::True,
							                        ::invalidVersion,
							                        Verbose::True,
							                        Key(),
							                        Key(),
							                        locked,
							                        UnlockDB::True,
							                        OnlyApplyMutationLogs::False,
							                        InconsistentSnapshotOnly::False,
							                        ::invalidVersion,
							                        lastBackupContainer->getEncryptionKeyFileName());
						}
					} else {
						for (restoreIndex = 0; restoreIndex < restores.size(); restoreIndex++) {
							FileBackupAgent::ERestoreState rs =
							    co_await backupAgent.abortRestore(cx, restoreTags[restoreIndex]);
							// The restore may have already completed, or the abort may have been done before the
							// restore was even able to start.  Only run a new restore if the previous one was actually
							// aborted.
							if (rs == FileBackupAgent::ERestoreState::ABORTED) {
								co_await runRYWTransaction(
								    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
									    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
									    tr->clear(restoreRanges[restoreIndex]);
									    return Void();
								    });
								restores[restoreIndex] =
								    backupAgent.restore(cx,
								                        cx,
								                        restoreTags[restoreIndex],
								                        KeyRef(lastBackupContainer->getURL()),
								                        lastBackupContainer->getProxy(),
								                        WaitForComplete::True,
								                        ::invalidVersion,
								                        Verbose::True,
								                        restoreRanges[restoreIndex],
								                        Key(),
								                        Key(),
								                        locked,
								                        OnlyApplyMutationLogs::False,
								                        InconsistentSnapshotOnly::False,
								                        ::invalidVersion,
								                        lastBackupContainer->getEncryptionKeyFileName());
							}
						}
					}
				}

				co_await waitForAll(restores);

				for (auto& restore : restores) {
					ASSERT(!restore.isError());
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
					co_await backupAgent.abortBackup(cx, backupTag.toString());
				} catch (Error& e) {
					TraceEvent("BARW_AbortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
			}

			Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
			Key backupLogValuesKey = destUidValue.withPrefix(backupLogKeys.begin);
			Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);
			Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
			int displaySystemKeys = 0;

			// Ensure that there is no left over key within the backup subspace
			while (true) {
				Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

				TraceEvent("BARW_CheckLeftoverKeys", randomID).detail("BackupTag", printable(backupTag));

				Error err;
				try {
					// Check the left over tasks
					// We have to wait for the list to empty since an abort and get status
					// can leave extra tasks in the queue
					TraceEvent("BARW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(backupTag));
					int64_t taskCount = co_await backupAgent.getTaskCount(tr);
					int waitCycles = 0;

					if ((taskCount) && false) {
						TraceEvent("BARW_EndingNonzeroTaskCount", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("EndingNonZeroTasks: %ld\n", (long)taskCount);
						co_await TaskBucket::debugPrintRange(cx, normalKeys.end, StringRef());
					}

					while (taskCount > 0) {
						waitCycles++;

						TraceEvent("BARW_NonzeroTaskWait", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("%.6f %-10s Wait #%4d for %lld tasks to end\n",
						       now(),
						       randomID.toString().c_str(),
						       waitCycles,
						       (long long)taskCount);

						co_await delay(5.0);

						tr = makeReference<ReadYourWritesTransaction>(cx);
						taskCount = co_await backupAgent.getTaskCount(tr);
					}

					RangeResult agentValues =
					    co_await tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100);

					// Error if the system keyspace for the backup tag is not empty
					if (agentValues.size() > 0) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n",
						       agentValues.size(),
						       printable(backupAgentKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("LeftOverKeys", agentValues.size())
						    .detail("KeySpace", printable(backupAgentKey));
						for (auto& s : agentValues) {
							TraceEvent("BARW_LeftOverKey", randomID)
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
						TraceEvent(SevError, "BackupCorrectnessLeftOverVersionKey", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("BackupLatestVersionsKey", backupLatestVersionsKey.printable())
						    .detail("DestUidValue", destUidValue.printable());
					} else {
						printf("No left over backup version key\n");
					}

					RangeResult versions = co_await tr->getRange(
					    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1);
					if (!shareLogRange || !versions.size()) {
						RangeResult logValues = co_await tr->getRange(
						    KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100);

						// Error if the log/mutation keyspace for the backup tag  is not empty
						if (logValues.size() > 0) {
							displaySystemKeys++;
							printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n",
							       logValues.size(),
							       printable(backupLogValuesKey).c_str());
							TraceEvent(SevError, "BackupCorrectnessLeftOverLogKeys", randomID)
							    .detail("BackupTag", printable(backupTag))
							    .detail("LeftOverKeys", logValues.size())
							    .detail("KeySpace", printable(backupLogValuesKey));
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
				co_await TaskBucket::debugPrintRange(cx, normalKeys.end, StringRef());
			}

			TraceEvent("BARW_Complete", randomID).detail("BackupTag", printable(backupTag));

			// Decrement the backup agent requests
			if (agentRequest) {
				BackupAndRestoreCorrectnessWorkload::backupAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator->backupAgents == ISimulator::BackupAgentType::BackupToFile) &&
			    (!BackupAndRestoreCorrectnessWorkload::backupAgentRequests)) {
				g_simulator->backupAgents = ISimulator::BackupAgentType::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectness").error(e).GetLastError();
			throw;
		}
	}
};

int BackupAndRestoreCorrectnessWorkload::backupAgentRequests = 0;

std::string getTestEncryptionFileName() {
	return "test_encryption_key_file";
}

WorkloadFactory<BackupAndRestoreCorrectnessWorkload> BackupAndRestoreCorrectnessWorkloadFactory;
