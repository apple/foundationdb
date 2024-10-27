/*
 * BackupCorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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
		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.1)) {
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

	ACTOR Future<Void> _setup(Database cx, BackupAndRestoreCorrectnessWorkload* self) {
		state bool adjusted = false;
		state TenantMapEntry entry;

		if (!self->defaultBackup && (cx->defaultTenant.present() || BUGGIFY)) {
			if (cx->defaultTenant.present()) {
				wait(store(entry, TenantAPI::getTenant(cx.getReference(), cx->defaultTenant.get())));

				// If we are specifying sub-ranges (or randomly, if backing up normal keys), adjust them to be relative
				// to the tenant
				if (self->backupRanges.size() != 1 || self->backupRanges[0] != normalKeys ||
				    deterministicRandom()->coinflip()) {
					adjusted = true;
					Standalone<VectorRef<KeyRangeRef>> modifiedBackupRanges;
					for (int i = 0; i < self->backupRanges.size(); ++i) {
						modifiedBackupRanges.push_back_deep(
						    modifiedBackupRanges.arena(),
						    self->backupRanges[i].withPrefix(entry.prefix, self->backupRanges.arena()));
					}
					self->backupRanges = modifiedBackupRanges;
				}
			}
			for (auto r : getSystemBackupRanges()) {
				self->backupRanges.push_back_deep(self->backupRanges.arena(), r);
			}

			if (adjusted) {
				Standalone<VectorRef<KeyRangeRef>> modifiedRestoreRanges;
				for (int i = 0; i < self->restoreRanges.size(); ++i) {
					modifiedRestoreRanges.push_back_deep(
					    modifiedRestoreRanges.arena(),
					    self->restoreRanges[i].withPrefix(entry.prefix, self->restoreRanges.arena()));
				}
				self->restoreRanges = modifiedRestoreRanges;

				for (int i = 0; i < self->skippedRestoreRanges.size(); ++i) {
					self->skippedRestoreRanges[i] = self->skippedRestoreRanges[i].withPrefix(entry.prefix);
				}
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

		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		else
			return _check(cx, this);
	}

	ACTOR static Future<bool> _check(Database cx, BackupAndRestoreCorrectnessWorkload* self) {
		state Transaction tr(cx);
		loop {
			try {
				state int restoreIndex;
				for (restoreIndex = 0; restoreIndex < self->skippedRestoreRanges.size(); restoreIndex++) {
					state KeyRangeRef range = self->skippedRestoreRanges[restoreIndex];
					Standalone<StringRef> restoreTag(self->backupTag.toString() + "_" + std::to_string(restoreIndex));
					RangeResult res = wait(tr.getRange(range, GetRangeLimits::ROW_LIMIT_UNLIMITED));
					if (!res.empty()) {
						TraceEvent(SevError, "BARW_UnexpectedRangePresent").detail("Range", printable(range));
						return false;
					}
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

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
			bool active = wait(agent.checkActive(cx));
			TraceEvent("BARW_AgentActivityCheck").detail("IsActive", active);
			std::string status = wait(agent.getStatus(cx, ShowErrors::True, tag));
			puts(status.c_str());
			std::string statusJSON = wait(agent.getStatusJSON(cx, tag));
			puts(statusJSON.c_str());
			wait(delay(2.0));
		}
	}

	ACTOR static Future<Void> doBackup(BackupAndRestoreCorrectnessWorkload* self,
	                                   double startDelay,
	                                   FileBackupAgent* backupAgent,
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
				wait(backupAgent->abortBackup(cx, tag.toString()));
			} catch (Error& e) {
				TraceEvent("BARW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BARW_DoBackupSubmitBackup", randomID)
		    .detail("Tag", printable(tag))
		    .detail("StopWhenDone", stopDifferentialDelay ? "False" : "True");

		state std::string backupContainer = "file://simfdb/backups/";
		state Future<Void> status = statusLoop(cx, tag.toString());
		try {
			wait(backupAgent->submitBackup(cx,
			                               StringRef(backupContainer),
			                               {},
			                               deterministicRandom()->randomInt(0, 60),
			                               deterministicRandom()->randomInt(0, 2000),
			                               tag.toString(),
			                               backupRanges,
			                               true,
			                               StopWhenDone{ !stopDifferentialDelay },
			                               UsePartitionedLog::False,
			                               IncrementalBackupOnly::False,
			                               self->encryptionKeyFileName));
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
					state EBackupState resultWait = wait(backupAgent->waitBackup(
					    cx, backupTag.tagName, StopWhenDone::False, &lastBackupContainer, &lastBackupUID));

					TraceEvent("BARW_DoBackupWaitForRestorable", randomID)
					    .detail("Tag", backupTag.tagName)
					    .detail("Result", BackupAgentBase::getStateText(resultWait));

					state bool restorable = false;
					if (lastBackupContainer) {
						state Future<BackupDescription> fdesc = lastBackupContainer->describeBackup();
						wait(ready(fdesc));

						if (!fdesc.isError()) {
							state BackupDescription desc = fdesc.get();
							wait(desc.resolveVersionTimes(cx));
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
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}
		}

		// Wait for the backup to complete
		TraceEvent("BARW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));
		state EBackupState statusValue = wait(backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True));

		state std::string statusText;

		std::string _statusText = wait(backupAgent->getStatus(cx, ShowErrors::True, tag.toString()));
		statusText = _statusText;
		// Can we validate anything about status?

		TraceEvent("BARW_DoBackupComplete", randomID)
		    .detail("Tag", printable(tag))
		    .detail("Status", statusText)
		    .detail("StatusValue", BackupAgentBase::getStateText(statusValue));

		return Void();
	}

	/**
	    This actor attempts to restore the database without clearing the keyspace.
	 */
	ACTOR static Future<Void> attemptDirtyRestore(BackupAndRestoreCorrectnessWorkload* self,
	                                              Database cx,
	                                              FileBackupAgent* backupAgent,
	                                              Standalone<StringRef> lastBackupContainer,
	                                              UID randomID) {
		state Transaction tr(cx);
		state int rowCount = 0;
		loop {
			try {
				RangeResult existingRows = wait(tr.getRange(normalKeys, 1));
				rowCount = existingRows.size();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Try doing a restore without clearing the keys
		if (rowCount > 0) {
			try {
				wait(success(backupAgent->restore(cx,
				                                  cx,
				                                  self->backupTag,
				                                  KeyRef(lastBackupContainer),
				                                  {},
				                                  WaitForComplete::True,
				                                  ::invalidVersion,
				                                  Verbose::True,
				                                  normalKeys,
				                                  Key(),
				                                  Key(),
				                                  self->locked)));
				TraceEvent(SevError, "BARW_RestoreAllowedOverwrittingDatabase", randomID).log();
				ASSERT(false);
			} catch (Error& e) {
				if (e.code() != error_code_restore_destination_not_empty) {
					throw;
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> clearAndRestoreSystemKeys(Database cx,
	                                                    BackupAndRestoreCorrectnessWorkload* self,
	                                                    FileBackupAgent* backupAgent,
	                                                    Version targetVersion,
	                                                    Reference<IBackupContainer> lastBackupContainer,
	                                                    Standalone<VectorRef<KeyRangeRef>> systemRestoreRanges) {
		// restore system keys before restoring any other ranges
		wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			for (auto& range : systemRestoreRanges)
				tr->clear(range);
			return Void();
		}));
		state Standalone<StringRef> restoreTag(self->backupTag.toString() + "_system");
		printf("BackupCorrectness, backupAgent.restore is called for tag:%s\n", restoreTag.toString().c_str());
		wait(success(backupAgent->restore(cx,
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
		                                  self->encryptionKeyFileName)));
		printf("BackupCorrectness, backupAgent.restore finished for tag:%s\n", restoreTag.toString().c_str());
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupAndRestoreCorrectnessWorkload* self) {
		state FileBackupAgent backupAgent;
		state Future<Void> extraBackup;
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
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
			BackupAndRestoreCorrectnessWorkload::backupAgentRequests++;
		}

		if (self->encryptionKeyFileName.present()) {
			wait(BackupContainerFileSystem::createTestEncryptionKeyFile(self->encryptionKeyFileName.get()));
		}

		try {
			state Future<Void> startRestore = delay(self->restoreAfter);

			// backup
			wait(delay(self->backupAfter));

			TraceEvent("BARW_DoBackup1", randomID).detail("Tag", printable(self->backupTag));
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(
			    self, 0, &backupAgent, cx, self->backupTag, self->backupRanges, self->stopDifferentialAfter, submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BARW_DoBackup2", randomID)
				    .detail("Tag", printable(self->backupTag))
				    .detail("AbortWait", self->abortAndRestartAfter);
				wait(submitted.getFuture());
				b = b && doBackup(self,
				                  self->abortAndRestartAfter,
				                  &backupAgent,
				                  cx,
				                  self->backupTag,
				                  self->backupRanges,
				                  self->stopDifferentialAfter,
				                  Promise<Void>());
			}

			TraceEvent("BARW_DoBackupWait", randomID)
			    .detail("BackupTag", printable(self->backupTag))
			    .detail("AbortAndRestartAfter", self->abortAndRestartAfter);
			try {
				wait(b);
			} catch (Error& e) {
				if (e.code() != error_code_database_locked)
					throw;
				if (self->performRestore)
					throw;
				return Void();
			}
			TraceEvent("BARW_DoBackupDone", randomID)
			    .detail("BackupTag", printable(self->backupTag))
			    .detail("AbortAndRestartAfter", self->abortAndRestartAfter);

			state KeyBackedTag keyBackedTag = makeBackupTag(self->backupTag.toString());
			UidAndAbortedFlagT uidFlag = wait(keyBackedTag.getOrThrow(cx.getReference()));
			state UID logUid = uidFlag.first;
			state Key destUidValue = wait(BackupConfig(logUid).destUidValue().getD(cx.getReference()));
			state Reference<IBackupContainer> lastBackupContainer =
			    wait(BackupConfig(logUid).backupContainer().getD(cx.getReference()));

			// Occasionally start yet another backup that might still be running when we restore
			if (!self->locked && BUGGIFY) {
				TraceEvent("BARW_SubmitBackup2", randomID).detail("Tag", printable(self->backupTag));
				try {
					extraBackup = backupAgent.submitBackup(cx,
					                                       "file://simfdb/backups/"_sr,
					                                       {},
					                                       deterministicRandom()->randomInt(0, 60),
					                                       deterministicRandom()->randomInt(0, 100),
					                                       self->backupTag.toString(),
					                                       self->backupRanges,
					                                       true,
					                                       StopWhenDone::True);
				} catch (Error& e) {
					TraceEvent("BARW_SubmitBackup2Exception", randomID)
					    .error(e)
					    .detail("BackupTag", printable(self->backupTag));
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
			}

			CODE_PROBE(!startRestore.isReady(), "Restore starts at specified time");
			wait(startRestore);

			if (lastBackupContainer && self->performRestore) {
				if (deterministicRandom()->random01() < 0.5) {
					wait(attemptDirtyRestore(
					    self, cx, &backupAgent, StringRef(lastBackupContainer->getURL()), randomID));
				}
				wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					for (auto& kvrange : self->backupRanges)
						tr->clear(kvrange);
					return Void();
				}));

				// restore database
				TraceEvent("BARW_Restore", randomID)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("RestoreAfter", self->restoreAfter)
				    .detail("BackupTag", printable(self->backupTag));

				auto container = IBackupContainer::openContainer(lastBackupContainer->getURL(),
				                                                 lastBackupContainer->getProxy(),
				                                                 lastBackupContainer->getEncryptionKeyFileName());
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

				TraceEvent("BARW_RestoreDebug").detail("TargetVersion", targetVersion);

				state std::vector<Future<Version>> restores;
				state std::vector<Standalone<StringRef>> restoreTags;
				state bool multipleRangesInOneTag = false;
				state int restoreIndex = 0;
				// make sure system keys are not present in the restoreRanges as they will get restored first separately
				// from the rest
				Standalone<VectorRef<KeyRangeRef>> modifiedRestoreRanges;
				Standalone<VectorRef<KeyRangeRef>> systemRestoreRanges;
				for (int i = 0; i < self->restoreRanges.size(); ++i) {
					if (config.tenantMode != TenantMode::REQUIRED ||
					    !self->restoreRanges[i].intersects(getSystemBackupRanges())) {
						modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), self->restoreRanges[i]);
					} else {
						KeyRangeRef normalKeyRange = self->restoreRanges[i] & normalKeys;
						KeyRangeRef systemKeyRange = self->restoreRanges[i] & systemKeys;
						if (!normalKeyRange.empty()) {
							modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), normalKeyRange);
						}
						if (!systemKeyRange.empty()) {
							systemRestoreRanges.push_back_deep(systemRestoreRanges.arena(), systemKeyRange);
						}
					}
				}
				self->restoreRanges = modifiedRestoreRanges;
				if (!systemRestoreRanges.empty()) {
					// We are able to restore system keys first since we restore an entire cluster at once rather than
					// partial key ranges.
					wait(clearAndRestoreSystemKeys(
					    cx, self, &backupAgent, targetVersion, lastBackupContainer, systemRestoreRanges));
				}
				if (deterministicRandom()->random01() < 0.5) {
					for (restoreIndex = 0; restoreIndex < self->restoreRanges.size(); restoreIndex++) {
						auto range = self->restoreRanges[restoreIndex];
						Standalone<StringRef> restoreTag(self->backupTag.toString() + "_" +
						                                 std::to_string(restoreIndex));
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
						                                       self->locked,
						                                       OnlyApplyMutationLogs::False,
						                                       InconsistentSnapshotOnly::False,
						                                       ::invalidVersion,
						                                       self->encryptionKeyFileName));
					}
				} else {
					multipleRangesInOneTag = true;
					Standalone<StringRef> restoreTag(self->backupTag.toString() + "_" + std::to_string(restoreIndex));
					restoreTags.push_back(restoreTag);
					printf("BackupCorrectness, backupAgent.restore is called for restoreIndex:%d tag:%s\n",
					       restoreIndex,
					       restoreTag.toString().c_str());
					restores.push_back(backupAgent.restore(cx,
					                                       cx,
					                                       restoreTag,
					                                       KeyRef(lastBackupContainer->getURL()),
					                                       lastBackupContainer->getProxy(),
					                                       self->restoreRanges,
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
					                                       self->encryptionKeyFileName));
				}

				// Sometimes kill and restart the restore
				if (BUGGIFY) {
					wait(delay(deterministicRandom()->randomInt(0, 10)));
					if (multipleRangesInOneTag) {
						FileBackupAgent::ERestoreState rs = wait(backupAgent.abortRestore(cx, restoreTags[0]));
						// The restore may have already completed, or the abort may have been done before the restore
						// was even able to start.  Only run a new restore if the previous one was actually aborted.
						if (rs == FileBackupAgent::ERestoreState::ABORTED) {
							wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
								tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
								for (auto& range : self->restoreRanges)
									tr->clear(range);
								return Void();
							}));
							restores[restoreIndex] = backupAgent.restore(cx,
							                                             cx,
							                                             restoreTags[restoreIndex],
							                                             KeyRef(lastBackupContainer->getURL()),
							                                             lastBackupContainer->getProxy(),
							                                             self->restoreRanges,
							                                             WaitForComplete::True,
							                                             ::invalidVersion,
							                                             Verbose::True,
							                                             Key(),
							                                             Key(),
							                                             self->locked,
							                                             UnlockDB::True,
							                                             OnlyApplyMutationLogs::False,
							                                             InconsistentSnapshotOnly::False,
							                                             ::invalidVersion,
							                                             self->encryptionKeyFileName);
						}
					} else {
						for (restoreIndex = 0; restoreIndex < restores.size(); restoreIndex++) {
							FileBackupAgent::ERestoreState rs =
							    wait(backupAgent.abortRestore(cx, restoreTags[restoreIndex]));
							// The restore may have already completed, or the abort may have been done before the
							// restore was even able to start.  Only run a new restore if the previous one was actually
							// aborted.
							if (rs == FileBackupAgent::ERestoreState::ABORTED) {
								wait(
								    runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
									    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
									    tr->clear(self->restoreRanges[restoreIndex]);
									    return Void();
								    }));
								restores[restoreIndex] = backupAgent.restore(cx,
								                                             cx,
								                                             restoreTags[restoreIndex],
								                                             KeyRef(lastBackupContainer->getURL()),
								                                             lastBackupContainer->getProxy(),
								                                             WaitForComplete::True,
								                                             ::invalidVersion,
								                                             Verbose::True,
								                                             self->restoreRanges[restoreIndex],
								                                             Key(),
								                                             Key(),
								                                             self->locked,
								                                             OnlyApplyMutationLogs::False,
								                                             InconsistentSnapshotOnly::False,
								                                             ::invalidVersion,
								                                             self->encryptionKeyFileName);
							}
						}
					}
				}

				wait(waitForAll(restores));

				for (auto& restore : restores) {
					ASSERT(!restore.isError());
				}
			}

			if (extraBackup.isValid()) {
				TraceEvent("BARW_WaitExtraBackup", randomID).detail("BackupTag", printable(self->backupTag));
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
					wait(backupAgent.abortBackup(cx, self->backupTag.toString()));
				} catch (Error& e) {
					TraceEvent("BARW_AbortBackupExtraException", randomID).error(e);
					if (e.code() != error_code_backup_unneeded)
						throw;
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
					// Check the left over tasks
					// We have to wait for the list to empty since an abort and get status
					// can leave extra tasks in the queue
					TraceEvent("BARW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(self->backupTag));
					state int64_t taskCount = wait(backupAgent.getTaskCount(tr));
					state int waitCycles = 0;

					if ((taskCount) && false) {
						TraceEvent("BARW_EndingNonzeroTaskCount", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("EndingNonZeroTasks: %ld\n", (long)taskCount);
						wait(TaskBucket::debugPrintRange(cx, normalKeys.end, StringRef()));
					}

					while (taskCount > 0) {
						waitCycles++;

						TraceEvent("BARW_NonzeroTaskWait", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("%.6f %-10s Wait #%4d for %lld tasks to end\n",
						       now(),
						       randomID.toString().c_str(),
						       waitCycles,
						       (long long)taskCount);

						wait(delay(5.0));

						tr = makeReference<ReadYourWritesTransaction>(cx);
						wait(store(taskCount, backupAgent.getTaskCount(tr)));
					}

					RangeResult agentValues =
					    wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

					// Error if the system keyspace for the backup tag is not empty
					if (agentValues.size() > 0) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n",
						       agentValues.size(),
						       printable(backupAgentKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", randomID)
						    .detail("BackupTag", printable(self->backupTag))
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

					Optional<Value> latestVersion = wait(tr->get(backupLatestVersionsKey));
					if (latestVersion.present()) {
						TraceEvent(SevError, "BackupCorrectnessLeftOverVersionKey", randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("BackupLatestVersionsKey", backupLatestVersionsKey.printable())
						    .detail("DestUidValue", destUidValue.printable());
					} else {
						printf("No left over backup version key\n");
					}

					RangeResult versions = wait(tr->getRange(
					    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1));
					if (!self->shareLogRange || !versions.size()) {
						RangeResult logValues = wait(
						    tr->getRange(KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100));

						// Error if the log/mutation keyspace for the backup tag  is not empty
						if (logValues.size() > 0) {
							displaySystemKeys++;
							printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n",
							       logValues.size(),
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
				wait(TaskBucket::debugPrintRange(cx, normalKeys.end, StringRef()));
			}

			TraceEvent("BARW_Complete", randomID).detail("BackupTag", printable(self->backupTag));

			// Decrement the backup agent requests
			if (self->agentRequest) {
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
		return Void();
	}
};

int BackupAndRestoreCorrectnessWorkload::backupAgentRequests = 0;

std::string getTestEncryptionFileName() {
	return "test_encryption_key_file";
}

WorkloadFactory<BackupAndRestoreCorrectnessWorkload> BackupAndRestoreCorrectnessWorkloadFactory;
