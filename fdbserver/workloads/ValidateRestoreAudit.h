/*
 * ValidateRestoreAudit.h
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

#pragma once

#ifndef FDBSERVER_VALIDATE_RESTORE_AUDIT_H
#define FDBSERVER_VALIDATE_RESTORE_AUDIT_H

#include "fdbclient/Audit.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "flow/flow.h"

// Runs an audit_storage ValidateRestore audit over `range` and waits for it to complete. The audit compares `range`
// of the live database with its copy under validateRestoreLogKeys.begin, as written by a restore with that add-prefix,
// at a single version, so both sides must be quiescent. Throws audit_storage_failed if the audit finds a difference
// (phase Error), and timed_out if it does not finish in time. An audit that fails without a verdict (phase Failed:
// the data distributor gave up reaching storage servers during recoveries or failures) is run again, up to
// `maxAttempts` times.
inline Future<Void> runValidateRestoreAudit(Database cx,
                                            KeyRange range,
                                            double pollIntervalSeconds = 2.0,
                                            double maxWaitSeconds = 300.0,
                                            int maxAttempts = 5) {
	constexpr int MAX_SCHEDULE_ATTEMPTS = 5;
	for (int auditAttempt = 1;; ++auditAttempt) {
		UID auditId;
		for (int attempt = 1;; ++attempt) {
			Error err;
			try {
				auditId = co_await timeoutError(auditStorage(cx->getConnectionRecord(),
				                                             range,
				                                             AuditType::ValidateRestore,
				                                             KeyValueStoreType::END,
				                                             maxWaitSeconds),
				                                60.0);
				break;
			} catch (Error& e) {
				err = e;
			}
			if ((err.code() != error_code_timed_out && err.code() != error_code_audit_storage_failed) ||
			    attempt == MAX_SCHEDULE_ATTEMPTS) {
				throw err;
			}
			TraceEvent(SevWarn, "ValidateRestoreAuditScheduleRetry").error(err).detail("Attempt", attempt);
			co_await delay(2.0 * attempt);
		}

		double startTime = now();
		AuditPhase phase = AuditPhase::Running;
		while (phase == AuditPhase::Running) {
			co_await delay(pollIntervalSeconds);
			if (now() - startTime > maxWaitSeconds) {
				TraceEvent(SevError, "ValidateRestoreAuditTimeout").detail("AuditID", auditId).detail("Range", range);
				throw timed_out();
			}
			std::vector<AuditStorageState> states;
			Error readError;
			try {
				std::vector<AuditStorageState> result = co_await getAuditStates(cx, AuditType::ValidateRestore, true);
				states = result;
			} catch (Error& e) {
				readError = e;
			}
			if (readError.isValid()) {
				// Transient errors from proxies under load or recoveries; try the read again on the next poll.
				if (readError.code() != error_code_grv_proxy_memory_limit_exceeded &&
				    readError.code() != error_code_commit_proxy_memory_limit_exceeded &&
				    readError.code() != error_code_transaction_too_old &&
				    readError.code() != error_code_future_version && readError.code() != error_code_tag_throttled &&
				    readError.code() != error_code_database_locked) {
					throw readError;
				}
				continue;
			}
			for (const auto& state : states) {
				if (state.id == auditId) {
					phase = state.getPhase();
				}
			}
		}

		if (phase == AuditPhase::Complete) {
			co_return;
		}
		if (phase == AuditPhase::Failed && auditAttempt < maxAttempts) {
			TraceEvent(SevWarn, "ValidateRestoreAuditRetry")
			    .detail("AuditID", auditId)
			    .detail("Range", range)
			    .detail("Attempt", auditAttempt);
			co_await delay(2.0 * auditAttempt);
			continue;
		}
		TraceEvent(SevError, "ValidateRestoreAuditFailed")
		    .detail("AuditID", auditId)
		    .detail("Range", range)
		    .detail("Phase", static_cast<int>(phase));
		throw audit_storage_failed();
	}
}

// Read version of the database once `delaySeconds` have passed. Taken after the workloads writing the data have
// finished, it is a version the backups must cover before they are discontinued.
inline Future<Version> readVersionAfter(Database cx, double delaySeconds) {
	co_await delay(delaySeconds);
	Transaction tr(cx);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	while (true) {
		Error err;
		try {
			Version v = co_await tr.getReadVersion();
			co_return v;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Waits until the differential backup `tag` is restorable to at least `target`. Discontinuing a restorable backup
// completes it immediately at whatever version its log copy has reached, so this must happen first for the backup to
// cover everything written up to `target`. Returns without waiting if the backup is not running differentially, and
// stops waiting if it is aborted or replaced under the same tag, since its log copy no longer advances. Throws
// timed_out if the log copy does not reach `target` in `maxWaitSeconds`.
inline Future<Void> waitForRestorableVersion(Database cx,
                                             FileBackupAgent* agent,
                                             std::string tag,
                                             Version target,
                                             double maxWaitSeconds = 600.0) {
	Reference<IBackupContainer> container;
	UID backupUid;
	EBackupState state = co_await agent->waitBackup(cx, tag, StopWhenDone::False, &container, &backupUid);
	if (state != EBackupState::STATE_RUNNING_DIFFERENTIAL || !container) {
		co_return;
	}
	double deadline = now() + maxWaitSeconds;
	while (true) {
		BackupDescription desc = co_await container->describeBackup();
		if (desc.maxRestorableVersion.present() && desc.maxRestorableVersion.get() >= target) {
			co_return;
		}

		Optional<UidAndAbortedFlagT> current = co_await makeBackupTag(tag).get(cx.getReference());
		if (!current.present() || current.get().first != backupUid) {
			co_return;
		}
		EBackupState currentState = co_await BackupConfig(backupUid).stateEnum().getD(
		    cx.getReference(), Snapshot::False, EBackupState::STATE_NEVERRAN);
		if (currentState != EBackupState::STATE_RUNNING_DIFFERENTIAL) {
			co_return;
		}

		if (now() > deadline) {
			TraceEvent(SevError, "WaitForRestorableVersionTimeout")
			    .detail("Tag", tag)
			    .detail("Target", target)
			    .detail("MaxRestorable", desc.maxRestorableVersion.present() ? desc.maxRestorableVersion.get() : -1);
			throw timed_out();
		}
		co_await delay(2.0);
	}
}

#endif
