/*
 * BackupAndRestoreValidation.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Simplified backup and restore workload specifically for restore validation testing
// This avoids the complexity of BackupAndRestoreCorrectness which is used by many tests

// Completion marker key to signal that restore is fully done
const KeyRef restoreValidationCompletionKey = "\xff\x02/restoreValidationComplete"_sr;
struct BackupAndRestoreValidationWorkload : TestWorkload {
	static constexpr auto NAME = "BackupAndRestoreValidation";
	double backupAfter, restoreAfter;
	Key backupTag;
	Key addPrefix; // Prefix to add during restore (e.g., \xff\x02/rlog/)

	BackupAndRestoreValidationWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupAfter = getOption(options, "backupAfter"_sr, 10.0);
		restoreAfter = getOption(options, "restoreAfter"_sr, 30.0);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		addPrefix = unprintable(getOption(options, "addPrefix"_sr, ""_sr).toString());

		TraceEvent("BARV_Init")
		    .detail("BackupAfter", backupAfter)
		    .detail("RestoreAfter", restoreAfter)
		    .detail("AddPrefix", printable(addPrefix));
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> doBackup(BackupAndRestoreValidationWorkload* self,
	                                   FileBackupAgent* backupAgent,
	                                   Database cx) {
		state std::string backupContainer = "file://simfdb/backups/";
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;

		// Only backup normal user keys (not system keys)
		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		TraceEvent("BARV_SubmitBackup").detail("Tag", printable(self->backupTag)).detail("Container", backupContainer);

		try {
			wait(backupAgent->submitBackup(cx,
			                               StringRef(backupContainer),
			                               {},
			                               deterministicRandom()->randomInt(0, 60),
			                               deterministicRandom()->randomInt(0, 100),
			                               self->backupTag.toString(),
			                               backupRanges,
			                               true,
			                               StopWhenDone{ true }));
		} catch (Error& e) {
			TraceEvent("BARV_SubmitBackupException").error(e);
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		// Wait for backup to complete
		TraceEvent("BARV_WaitBackup").detail("Tag", printable(self->backupTag));
		state EBackupState statusValue =
		    wait(backupAgent->waitBackup(cx, self->backupTag.toString(), StopWhenDone::True));

		TraceEvent("BARV_BackupComplete")
		    .detail("Tag", printable(self->backupTag))
		    .detail("Status", BackupAgentBase::getStateText(statusValue));

		return Void();
	}

	ACTOR static Future<Void> doRestore(BackupAndRestoreValidationWorkload* self,
	                                    FileBackupAgent* backupAgent,
	                                    Database cx,
	                                    Reference<IBackupContainer> backupContainer) {
		state Standalone<VectorRef<KeyRangeRef>> restoreRanges;

		// Restore normal user keys only
		restoreRanges.push_back_deep(restoreRanges.arena(), normalKeys);

		state Standalone<StringRef> restoreTag(self->backupTag.toString() + "_restore");

		TraceEvent("BARV_StartRestore")
		    .detail("Tag", printable(restoreTag))
		    .detail("Container", backupContainer->getURL())
		    .detail("AddPrefix", printable(self->addPrefix));

		// Don't clear keys - we want to keep original data for validation comparison
		// The restore will put data at the addPrefix location

		wait(success(backupAgent->restore(cx,
		                                  cx,
		                                  restoreTag,
		                                  KeyRef(backupContainer->getURL()),
		                                  backupContainer->getProxy(),
		                                  restoreRanges,
		                                  WaitForComplete::True,
		                                  ::invalidVersion,
		                                  Verbose::True,
		                                  self->addPrefix,
		                                  Key(), // removePrefix
		                                  LockDB{ false },
		                                  UnlockDB::True,
		                                  OnlyApplyMutationLogs::False,
		                                  InconsistentSnapshotOnly::False,
		                                  ::invalidVersion,
		                                  backupContainer->getEncryptionKeyFileName())));

		TraceEvent("BARV_RestoreComplete")
		    .detail("Tag", printable(restoreTag))
		    .detail("AddPrefix", printable(self->addPrefix));

		// Wait a bit to ensure all restored data is committed and visible
		// The restore API returns success before all data is fully flushed to storage servers
		wait(delay(5.0));
		TraceEvent("BARV_RestoreDataStabilizationWait").detail("WaitTime", 5.0);

		// Write a completion marker so RestoreValidation knows restore is fully done
		state Key completionMarker = restoreValidationCompletionKey;
		state Transaction markTr(cx);
		loop {
			try {
				markTr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				markTr.setOption(FDBTransactionOptions::LOCK_AWARE);
				markTr.set(completionMarker, "1"_sr);
				wait(markTr.commit());
				TraceEvent("BARV_RestoreCompletionMarkerSet").detail("MarkerKey", printable(completionMarker));
				break;
			} catch (Error& e) {
				wait(markTr.onError(e));
			}
		}

		// Unlock the database after restore completes
		wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(databaseLockedKey);
			return Void();
		}));

		TraceEvent("BARV_DatabaseUnlocked").detail("Tag", printable(restoreTag));

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupAndRestoreValidationWorkload* self) {
		// Only run on client 0 to avoid conflicts
		if (self->clientId != 0) {
			return Void();
		}

		state FileBackupAgent backupAgent;
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		state int retryCount = 0;

		loop {
			try {
				// Wait before starting backup
				wait(delay(self->backupAfter));

				// Perform backup
				TraceEvent("BARV_StartBackup", randomID)
				    .detail("Tag", printable(self->backupTag))
				    .detail("RetryCount", retryCount);
				wait(doBackup(self, &backupAgent, cx));

				// Get backup container info
				state KeyBackedTag keyBackedTag = makeBackupTag(self->backupTag.toString());
				UidAndAbortedFlagT uidFlag = wait(keyBackedTag.getOrThrow(cx.getReference()));
				state UID logUid = uidFlag.first;
				state Reference<IBackupContainer> backupContainer =
				    wait(BackupConfig(logUid).backupContainer().getD(cx.getReference()));

				// Wait before starting restore
				wait(delay(self->restoreAfter - self->backupAfter));

				// Perform restore with prefix
				TraceEvent("BARV_StartRestore", randomID)
				    .detail("Tag", printable(self->backupTag))
				    .detail("Container", backupContainer->getURL());
				wait(doRestore(self, &backupAgent, cx, backupContainer));

				TraceEvent("BARV_Complete", randomID).detail("Tag", printable(self->backupTag));
				break; // Success!

			} catch (Error& e) {
				// Retry on transient errors from buggify chaos injection
				if (e.code() == error_code_grv_proxy_memory_limit_exceeded ||
				    e.code() == error_code_commit_proxy_memory_limit_exceeded ||
				    e.code() == error_code_database_locked || e.code() == error_code_transaction_too_old ||
				    e.code() == error_code_future_version) {
					retryCount++;
					double backoff = std::min(1.0, 0.1 * retryCount);
					TraceEvent(SevWarn, "BARV_RetryableError", randomID)
					    .error(e)
					    .detail("RetryCount", retryCount)
					    .detail("BackoffSeconds", backoff);
					wait(delay(backoff));
					// Reset state and retry
					self->backupAfter = 0.0; // Don't wait again
					self->restoreAfter = self->restoreAfter - self->backupAfter;
					// Loop will retry
				} else {
					TraceEvent(SevError, "BARV_Error", randomID).error(e).detail("RetryCount", retryCount);
					throw;
				}
			}
		}

		return Void();
	}
};

WorkloadFactory<BackupAndRestoreValidationWorkload> BackupAndRestoreValidationWorkloadFactory;
