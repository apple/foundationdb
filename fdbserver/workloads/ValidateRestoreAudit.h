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
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/workloads.h"
#include "BackupSteps.h"
#include "flow/flow.h"

inline constexpr double validateRestoreAuditRetrySeconds = 600.0;

// Runs an audit_storage ValidateRestore audit over `range` and waits for it to complete. The audit compares `range`
// of the live database with its copy under validateRestoreLogKeys.begin, as written by a restore with that add-prefix,
// at a single version, so both sides must be quiescent. Throws audit_storage_failed if the audit finds a difference
// (phase Error), and timed_out if it does not finish in time. An audit that fails without a verdict (phase Failed:
// the data distributor gave up reaching storage servers during recoveries or failures) is run again until
// `retryBudgetSeconds` have passed. `completedState`, when set, receives the state of the audit that completed.
inline Future<Void> runValidateRestoreAudit(Database cx,
                                            KeyRange range,
                                            AuditWaitOptions options = AuditWaitOptions(),
                                            double retryBudgetSeconds = validateRestoreAuditRetrySeconds,
                                            AuditStorageState* completedState = nullptr) {
	double firstAttemptTime = now();
	if (options.traceContext.empty()) {
		options.traceContext = "ValidateRestoreAudit";
	}
	for (int attempt = 1;; ++attempt) {
		AuditStorageState state =
		    co_await auditStorageAndWait(cx, range, AuditType::ValidateRestore, KeyValueStoreType::END, options);
		if (state.getPhase() == AuditPhase::Complete) {
			if (completedState) {
				*completedState = state;
			}
			co_return;
		}
		if (state.getPhase() == AuditPhase::Failed && now() - firstAttemptTime < retryBudgetSeconds) {
			TraceEvent(SevWarn, "ValidateRestoreAuditRetry")
			    .detail("AuditID", state.id)
			    .detail("Range", range)
			    .detail("Attempt", attempt);
			co_await delay(std::min(10.0, 2.0 * attempt));
			continue;
		}
		TraceEvent(SevError, "ValidateRestoreAuditFailed")
		    .detail("AuditID", state.id)
		    .detail("Range", range)
		    .detail("Phase", static_cast<int>(state.getPhase()))
		    .detail("Error", state.error);
		throw audit_storage_failed();
	}
}

// Process-wide coordination of validation restores. They share one destination prefix, so they run one at a time, and
// the in-place restore (which overwrites the live data they compare against) waits for all of them. All instances of
// a test run in the same tester, so the state is shared by every workload using it.
struct RestoreValidationGate {
	static inline FlowLock lock{ 1 };
	static inline Reference<AsyncVar<int>> pending = makeReference<AsyncVar<int>>(0);

	// Waits until no validation is pending.
	static Future<Void> waitUntilNonePending() {
		while (pending->get() > 0) {
			co_await pending->onChange();
		}
	}
};

// One workload instance's claim on the pending-validation count. Releases it on every exit path, so a failed instance
// cannot leave the in-place restore waiting forever.
struct PendingValidationClaim : NonCopyable {
	bool held;
	explicit PendingValidationClaim(bool held) : held(held) {}
	// Adds a claim that a PendingValidationClaim constructed with `true` later releases.
	static void add() { RestoreValidationGate::pending->set(RestoreValidationGate::pending->get() + 1); }
	void release() {
		if (held) {
			held = false;
			RestoreValidationGate::pending->set(RestoreValidationGate::pending->get() - 1);
		}
	}
	~PendingValidationClaim() { release(); }
};

// Where validateAgainstPrefixedRestore spent its time.
struct ValidationTimings {
	double restoreSeconds = 0.0;
	double auditSeconds = 0.0;
};

// How validateAgainstPrefixedRestore labels its trace events.
struct ValidationTraceOptions {
	// Prepended to the event names (ValidateRestoreStart, ValidateRestoreVersions, ValidateRestoreDone), so each
	// workload's events stay distinguishable.
	std::string eventPrefix;
	UID id;
	// Also read the backup's description before the restore and trace its maximum restorable version next to the
	// version that was restored.
	bool traceRestorableVersions = false;
	// Receives the restore and audit durations when set; must outlive the call.
	ValidationTimings* timings = nullptr;
	// Receives the completed state of each range's audit when set; must outlive the call.
	std::vector<AuditStorageState>* audits = nullptr;
	// Reads the validation restore's status after it completes, which traces its FileRestoreProgress (exact bytes
	// written and file block counts; the restore configuration is not readable from a workload).
	bool traceRestoreProgress = false;
	// After the audits pass, clears the validation prefix and checks that nothing is left, still holding the
	// RestoreValidationGate so that no other validation can start writing the prefix in between. The copy is kept
	// when an audit fails.
	bool clearWhenDone = false;
};

// Clears the validation prefix, the destination of validation restores.
inline Future<Void> clearValidationRestoreData(Database cx) {
	return runRYWTransaction(cx, [](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->clear(validateRestoreLogKeys);
		return Void();
	});
}

// Throws operation_failed if anything is left under the validation prefix.
inline Future<Void> verifyValidationRestoreDataCleared(Database cx) {
	Optional<Key> left = co_await BackupSteps::firstKeyIn(cx, validateRestoreLogKeys);
	if (left.present()) {
		TraceEvent(SevWarnAlways, "ValidationRestoreDataNotCleared").detail("FirstKey", left.get());
		throw operation_failed();
	}
}

// Restores the latest version of the backup in `container` under the validation prefix, which it clears first, and
// audits it against the live data, one `backupRanges` entry at a time. The data must be quiescent and covered by the
// backup. Runs one at a time per RestoreValidationGate.
inline Future<Void> validateAgainstPrefixedRestore(Database cx,
                                                   FileBackupAgent* backupAgent,
                                                   Reference<IBackupContainer> container,
                                                   Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                                   Key backupTag,
                                                   ValidationTraceOptions trace = ValidationTraceOptions()) {
	if (trace.timings) {
		*trace.timings = ValidationTimings();
	}
	const std::string startEvent = trace.eventPrefix + "ValidateRestoreStart";
	const std::string versionsEvent = trace.eventPrefix + "ValidateRestoreVersions";
	const std::string doneEvent = trace.eventPrefix + "ValidateRestoreDone";
	co_await RestoreValidationGate::lock.take();
	FlowLock::Releaser releaser(RestoreValidationGate::lock);
	Key prefix = validateRestoreLogKeys.begin;
	Standalone<StringRef> validationTag(backupTag.toString() + "_validate");
	// Start from an empty prefix, so that a key the restore failed to write cannot be masked by a row left by an
	// earlier validation.
	co_await clearValidationRestoreData(cx);
	TraceEvent(startEvent.c_str(), trace.id).detail("Tag", printable(backupTag));
	Version maxRestorable = -1;
	if (trace.traceRestorableVersions) {
		BackupDescription desc = co_await IBackupContainer::openContainer(container->getURL(),
		                                                                  container->getProxy(),
		                                                                  container->getEncryptionKeyFileName(),
		                                                                  container->getEncryptionBlockSize())
		                             ->describeBackup();
		maxRestorable = desc.maxRestorableVersion.present() ? desc.maxRestorableVersion.get() : -1;
	}
	double restoreStart = now();
	Version restoredVersion = co_await backupAgent->restore(cx,
	                                                        cx,
	                                                        validationTag,
	                                                        KeyRef(container->getURL()),
	                                                        container->getProxy(),
	                                                        backupRanges,
	                                                        WaitForComplete::True,
	                                                        ::invalidVersion,
	                                                        Verbose::True,
	                                                        prefix,
	                                                        Key(),
	                                                        LockDB::False,
	                                                        UnlockDB::False,
	                                                        OnlyApplyMutationLogs::False,
	                                                        InconsistentSnapshotOnly::False,
	                                                        ::invalidVersion,
	                                                        container->getEncryptionKeyFileName());
	if (trace.traceRestorableVersions) {
		TraceEvent(versionsEvent.c_str(), trace.id)
		    .detail("Tag", printable(backupTag))
		    .detail("RestoredVersion", restoredVersion)
		    .detail("MaxRestorable", maxRestorable);
	}
	double auditStart = now();
	if (trace.timings) {
		trace.timings->restoreSeconds = auditStart - restoreStart;
	}
	if (trace.traceRestoreProgress) {
		try {
			co_await backupAgent->restoreStatus(cx, validationTag);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			// Diagnostic only.
			TraceEvent(SevWarn, (trace.eventPrefix + "ValidateRestoreStatusFailed").c_str(), trace.id).error(e);
		}
	}
	for (const auto& range : backupRanges) {
		// The audit only covers user keys.
		KeyRangeRef userRange = range & normalKeys;
		if (!userRange.empty()) {
			AuditStorageState completed;
			co_await runValidateRestoreAudit(
			    cx, userRange, AuditWaitOptions(), validateRestoreAuditRetrySeconds, &completed);
			if (trace.audits) {
				trace.audits->push_back(completed);
			}
		}
	}
	if (trace.timings) {
		trace.timings->auditSeconds = now() - auditStart;
	}
	if (trace.clearWhenDone) {
		co_await clearValidationRestoreData(cx);
		co_await verifyValidationRestoreDataCleared(cx);
	}
	TraceEvent(doneEvent.c_str(), trace.id).detail("Tag", printable(backupTag));
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

// A backup workload's settings and state for validating its backup against the live data, which must be quiescent
// from `validateAfter` on.
struct QuiescentValidation {
	// Restore the backup under the validation prefix and audit it against the live data before the in-place restore.
	// Only valid when the data is quiescent and the backup captured all of it.
	bool enabled = false;
	// Earliest time (from workload start) at which the backup is validated.
	double validateAfter = 0.0;
	// Read version taken after the data has been written; backups being validated must cover it.
	Future<Version> quiescentVersion;
	bool claimed = false;

	void configure(VectorRef<KeyValueRef> options, double defaultValidateAfter) {
		enabled = getOption(options, "validateRestoredData"_sr, false);
		validateAfter = getOption(options, "validateAfter"_sr, defaultValidateAfter);
	}

	// Registers a pending validation. Called when the workload starts and not when it is constructed: a workload that
	// is constructed but never started (the test is set up again after an early failure) would otherwise keep the
	// in-place restore waiting forever.
	void claim() {
		if (enabled) {
			claimed = true;
			PendingValidationClaim::add();
		}
	}

	// Hands the claim to the caller's scope, which releases it on every exit path.
	PendingValidationClaim takeClaim() { return PendingValidationClaim(std::exchange(claimed, false)); }

	void begin(Database cx) {
		if (enabled) {
			quiescentVersion = readVersionAfter(cx, validateAfter);
		}
	}

	// Lets the differential backup `tag` catch up with the quiescent version, so that discontinuing it keeps all the
	// data. Does nothing when validation is off.
	Future<Void> waitForBackupToCover(Database cx, FileBackupAgent* agent, std::string tag) {
		if (enabled) {
			Version target = co_await quiescentVersion;
			co_await waitForRestorableVersion(cx, agent, tag, target);
		}
	}
};

#endif
