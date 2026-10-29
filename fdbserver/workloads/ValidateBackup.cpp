/*
 * ValidateBackup.cpp
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

#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/tester/workloads.h"
#include "BackupRangeSpec.h"
#include "BackupSteps.h"
#include "ValidateRestoreAudit.h"

// Proves that a backup restores to exactly the live data without touching the live data: restores the backup under
// the validation prefix and runs an audit_storage ValidateRestore audit against the live ranges. The data must be
// quiescent from validateAfter on: the restore is of the latest restorable version, which can be later than the
// quiescent version, so a commit to the validated ranges after that point is reported as a difference. A difference
// fails the workload with audit_storage_failed.
//
// By default the validation copy is cleared after a passing audit so that it does not stay on the storage servers
// (it is as large as the validated data). After a failure it is kept for debugging.
//
// Only backups made by the file backup agent (v1) are supported. The backup is looked up by tag. The restore uses the
// container, and so the encryption key file, recorded in the backup's configuration.
struct ValidateBackupWorkload : TestWorkload {
	static constexpr auto NAME = "ValidateBackup";

	FileBackupAgent backupAgent;

	Standalone<StringRef> tag;
	double delayFor;
	// Seconds from the workload start after which the quiescent version is read.
	double validateAfter;
	double maxWaitSeconds;
	bool discontinue;
	// Subset of the backup's ranges to validate; all of them when unset.
	Optional<Standalone<VectorRef<KeyRangeRef>>> validateRanges;
	// Clear the validation copy after a passing audit. Kept on failure.
	bool clearValidationData;
	// After clearing, wait up to this long for data distribution and the storage queues to drain; 0 does not wait.
	double settleSeconds;

	Optional<double> waitForRestorableSeconds;
	ValidationTimings timings;
	Optional<double> snapshotMB;
	Optional<int> validatedRangeCount;
	int auditedRangeCount = 0;
	bool passed = false;

	explicit ValidateBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		validateAfter = getOption(options, "validateAfter"_sr, 20.0);
		maxWaitSeconds = getOption(options, "maxWaitSeconds"_sr, 600.0);
		discontinue = getOption(options, "discontinue"_sr, true);
		validateRanges = BackupRangeSpec::readOptionalRanges(options, "validateRanges"_sr, "validateRangePrefix"_sr);
		clearValidationData = getOption(options, "clearValidationData"_sr, true);
		settleSeconds = getOption(options, "settleSeconds"_sr, 0.0);
	}

	// Waits until data distribution has no queued or in-flight work and the storage queues are small, or
	// settleSeconds pass. Not settling is not a failure.
	Future<Void> waitToSettle(Database cx) {
		double start = now();
		double deadline = start + settleSeconds;
		while (now() < deadline) {
			Error err;
			try {
				int64_t inFlight = co_await timeoutError(getDataInFlight(cx, dbInfo), deadline - now());
				int64_t ddQueue = co_await timeoutError(
				    getDataDistributionQueueSize(cx, dbInfo, /*reportInFlight=*/true), deadline - now());
				int64_t ssQueue =
				    co_await timeoutError(getMaxStorageServerQueueSize(cx, dbInfo, invalidVersion), deadline - now());
				if (inFlight == 0 && ddQueue == 0 && ssQueue < 5e6) {
					TraceEvent("ValidateBackupSettled").detail("Elapsed", now() - start);
					co_return;
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			// These metrics throw while the cluster churns; not observing quiescence is not evidence of it.
			co_await delay(std::min(2.0, std::max(0.0, deadline - now())));
		}
		TraceEvent(SevWarn, "ValidateBackupSettleTimeout").detail("SettleSeconds", settleSeconds);
	}

	Future<Void> _start(Database cx) {
		// An in-place restore in the same test waits for the pending validations, so it cannot overwrite the data under
		// audit.
		PendingValidationClaim::add();
		PendingValidationClaim claim(true);
		Future<Version> quiescentVersion = readVersionAfter(cx, validateAfter);
		co_await delay(delayFor);

		Reference<IBackupContainer> container;
		UID backupUid;
		EBackupState state =
		    co_await backupAgent.waitBackup(cx, tag.toString(), StopWhenDone::False, &container, &backupUid);
		if (state != EBackupState::STATE_COMPLETED && state != EBackupState::STATE_RUNNING_DIFFERENTIAL) {
			TraceEvent(SevWarnAlways, "ValidateBackupBadState")
			    .detail("Tag", tag)
			    .detail("BackupState", BackupAgentBase::getStateText(state));
			throw backup_error();
		}

		std::vector<KeyRange> backupRanges = co_await BackupSteps::readBackupRanges(cx, backupUid);
		Standalone<VectorRef<KeyRangeRef>> ranges =
		    BackupSteps::selectBackupRanges(backupRanges, validateRanges, "ValidateRangeNotInBackup");

		// Discontinuing a restorable backup completes it at whatever its log copy has reached, so it must first
		// cover everything written before the quiescent version.
		Version target = co_await quiescentVersion;
		double waitStart = now();
		co_await waitForRestorableVersion(cx, &backupAgent, tag.toString(), target, maxWaitSeconds);
		waitForRestorableSeconds = now() - waitStart;

		if (discontinue && state == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
			co_await BackupSteps::stopBackup(cx, &backupAgent, tag);
		}

		try {
			BackupDescription desc = co_await container->describeBackup();
			if (!desc.maxRestorableVersion.present() || desc.maxRestorableVersion.get() < target) {
				TraceEvent(SevWarnAlways, "ValidateBackupNotRestorableToTarget")
				    .detail("Tag", tag)
				    .detail("Target", target)
				    .detail("MaxRestorable",
				            desc.maxRestorableVersion.present() ? desc.maxRestorableVersion.get() : -1);
				throw backup_error();
			}
			if (const KeyspaceSnapshotFile* snapshot = BackupSteps::latestRestorableSnapshot(desc)) {
				snapshotMB = snapshot->totalSize / 1e6;
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_broken_promise ||
			    e.code() == error_code_backup_error) {
				throw;
			}
			TraceEvent(SevWarn, "ValidateBackupDescribeFailed").error(e);
		}

		validatedRangeCount = ranges.size();
		std::vector<AuditStorageState> audits;
		ValidationTraceOptions traceOptions;
		traceOptions.eventPrefix = "ValidateBackup_";
		traceOptions.id = deterministicRandom()->randomUniqueID();
		traceOptions.traceRestorableVersions = true;
		traceOptions.timings = &timings;
		traceOptions.audits = &audits;
		traceOptions.traceRestoreProgress = true;
		traceOptions.clearWhenDone = clearValidationData;
		co_await validateAgainstPrefixedRestore(cx, &backupAgent, container, ranges, tag, traceOptions);

		// The audit's per-range progress is cleared once it completes, so coverage cannot be read back from it, and the
		// key and byte totals of a ValidateRestore audit are only traced by the storage servers
		// (SSAuditRestoreComplete). An audit of a range that holds no data would pass without validating anything, so
		// the live ranges must hold data; a backup of several ranges may legitimately have an empty one, so only all of
		// them being empty fails.
		bool anyData = false;
		for (const auto& audit : audits) {
			bool hasData = (co_await BackupSteps::firstKeyIn(cx, audit.range)).present();
			if (!hasData) {
				TraceEvent(SevWarn, "ValidateBackupAuditEmpty")
				    .detail("AuditID", audit.id)
				    .detail("Range", audit.range);
			}
			anyData = anyData || hasData;
			++auditedRangeCount;
			TraceEvent("ValidateBackup_AuditDone")
			    .detail("AuditID", audit.id)
			    .detail("Range", audit.range)
			    .detail("Phase", static_cast<int>(audit.getPhase()));
		}
		if (!anyData) {
			TraceEvent(SevWarnAlways, "ValidateBackupNothingAudited")
			    .detail("Tag", tag)
			    .detail("Audits", audits.size());
			throw audit_storage_failed();
		}

		if (clearValidationData && settleSeconds > 0) {
			co_await waitToSettle(cx);
		}
		passed = true;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return clientId != 0 || passed; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId != 0) {
			return;
		}
		if (waitForRestorableSeconds.present()) {
			m.emplace_back("Wait for restorable version (s)", waitForRestorableSeconds.get(), Averaged::False);
		}
		if (timings.restoreSeconds > 0) {
			m.emplace_back("Validation restore duration (s)", timings.restoreSeconds, Averaged::False);
		}
		if (timings.auditSeconds > 0) {
			m.emplace_back("Validation audit duration (s)", timings.auditSeconds, Averaged::False);
		}
		m.emplace_back("Validated range count", validatedRangeCount.orDefault(0), Averaged::False);
		m.emplace_back("Audited range count", auditedRangeCount, Averaged::False);
		if (snapshotMB.present()) {
			m.emplace_back("Validated snapshot size (MB)", snapshotMB.get(), Averaged::False);
		}
		m.emplace_back("Validation passed", passed ? 1 : 0, Averaged::False);
	}
};

WorkloadFactory<ValidateBackupWorkload> ValidateBackupWorkloadFactory;
