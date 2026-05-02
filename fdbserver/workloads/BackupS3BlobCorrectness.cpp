/*
 * BackupS3BlobCorrectness.cpp
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

/*
 * S3-SPECIFIC BACKUP CORRECTNESS WORKLOAD
 *
 * This workload is a specialized version of BackupAndRestoreCorrectness specifically
 * designed for testing S3/blobstore:// backup URLs. It differs from the original in
 * several key ways to handle S3's eventual consistency and timing characteristics:
 *
 * KEY DIFFERENCES FROM BackupAndRestoreCorrectness:
 *
 * 1. MockS3Server Registration:
 *    - Registers a MockS3Server for blobstore:// URLs in simulation
 *    - Only client 0 registers to avoid duplicate server instances
 *
 * 2. Encryption Defaults:
 *    - Defaults to NO encryption (encrypted=false) to simplify S3 testing
 *    - Original uses random encryption (50% chance)
 *    - Tests can still explicitly enable encryption via TOML config
 *
 * 3. Status Loop Behavior:
 *    - Exits early when backup reaches "Completed" state or snapshot closes
 *    - Reduces unnecessary polling for S3 metadata that may be eventually consistent
 *    - Original polls continuously until external termination
 *
 * 4. Configurable Snapshot Intervals:
 *    - Accepts initSnapshotInterval and snapshotInterval parameters
 *    - Allows tests to control S3 backup timing characteristics
 *    - Original uses hardcoded random values
 *
 * 5. Configurable Backup URL:
 *    - Accepts backupURL parameter (defaults to file://simfdb/backups/)
 *    - Enables testing with blobstore:// URLs
 *    - Original hardcodes file:// URLs
 *
 * WHY A SEPARATE WORKLOAD?
 *
 * S3/blobstore backups have fundamentally different timing and consistency
 * characteristics than file-based backups. Modifying the shared BackupAndRestoreCorrectness
 * workload to handle both cases introduced subtle race conditions and timing changes
 * that caused flaky failures in file-based backup tests. By creating a separate workload,
 * we ensure:
 *
 * - File-based backup tests maintain their original, stable behavior
 * - S3-specific workarounds don't affect non-S3 tests
 * - S3 tests can be tuned for eventual consistency without impacting other tests
 * - Clear separation of concerns and easier maintenance
 *
 * USAGE:
 *
 * Use this workload in TOML files that test S3/blobstore:// backups:
 *
 *   [[test.workload]]
 *   testName = 'BackupS3BlobCorrectness'
 *   backupURL = 'blobstore://127.0.0.1:8080/bucket'
 *   encrypted = false
 *   initSnapshotInterval = 0
 *   snapshotInterval = 30
 *
 * For file-based backups, continue using the original BackupAndRestoreCorrectness workload.
 */

#include <atomic>
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "BulkSetup.h"
#include "fdbserver/mocks3/MockS3Server.h"
#include "fdbserver/mocks3/MockS3ServerChaos.h"
#include "flow/IRandom.h"

// Counters to verify BulkDump/BulkLoad were actually used
// These are incremented by trace event handlers in the backup/restore code paths
// and checked by the test to ensure the expected paths were taken
extern std::atomic<int> g_bulkDumpTaskCompleteCount;
extern std::atomic<int> g_bulkLoadRestoreTaskCompleteCount;

// S3-specific backup correctness workload - see file header for differences from BackupAndRestoreCorrectness
struct BackupS3BlobCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "BackupS3BlobCorrectness";
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

	// S3-specific additions
	std::string backupURL;
	bool skipDirtyRestore;
	int initSnapshotInterval;
	int snapshotInterval;

	// BulkDump/BulkLoad integration options
	// snapshotMode: 0=RANGEFILE (default), 1=BULKDUMP, 2=BOTH
	int snapshotMode;
	// useRangeFileRestore: false=use BulkLoad (default when backup uses BULKDUMP), true=use traditional rangefile
	bool useRangeFileRestore;
	// performValidation: if true, validates backup by restoring with prefix and running audit_storage validate_restore
	// This must happen BEFORE clearing the database so we can compare original vs restored data
	bool performValidation;

	// Chaos testing options
	bool enableChaos;
	double errorRate, throttleRate, delayRate, corruptionRate, maxDelay;

	// This workload is not compatible with RandomRangeLock workload because they will race in locked range
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	explicit BackupS3BlobCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked.set(sharedRandomNumber % 2);
		backupAfter = getOption(options, "backupAfter"_sr, 10.0);
		double minBackupAfter = getOption(options, "minBackupAfter"_sr, backupAfter);
		if (backupAfter > minBackupAfter) {
			backupAfter = deterministicRandom()->random01() * (backupAfter - minBackupAfter) + minBackupAfter;
		}
		restoreAfter = getOption(options, "restoreAfter"_sr, 35.0);
		restoreStartAfterBackupFinished = getOption(options, "restoreStartAfterBackupFinished"_sr, 10.0);
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

		// S3-specific options
		backupURL = getOption(options, "backupURL"_sr, "file://simfdb/backups/"_sr).toString();
		skipDirtyRestore = getOption(options, "skipDirtyRestore"_sr, true);
		initSnapshotInterval = getOption(options, "initSnapshotInterval"_sr, 0);
		snapshotInterval = getOption(options, "snapshotInterval"_sr, 30);

		// BulkDump/BulkLoad integration options
		// snapshotMode: 0=RANGEFILE (default), 1=BULKDUMP, 2=BOTH
		snapshotMode = getOption(options, "snapshotMode"_sr, 0);
		// useRangeFileRestore: When false and backup used BULKDUMP, restore uses BulkLoad
		// Default to true (traditional restore) for backward compatibility
		useRangeFileRestore = getOption(options, "useRangeFileRestore"_sr, true);
		// performValidation: Validates backup by comparing original data vs restored data
		// Uses audit_storage validate_restore - must happen BEFORE clearing database
		performValidation = getOption(options, "performValidation"_sr, false);

		// Chaos testing options
		enableChaos = getOption(options, "enableChaos"_sr, false);
		errorRate = getOption(options, "errorRate"_sr, 0.0);
		throttleRate = getOption(options, "throttleRate"_sr, 0.0);
		delayRate = getOption(options, "delayRate"_sr, 0.0);
		corruptionRate = getOption(options, "corruptionRate"_sr, 0.0);
		maxDelay = getOption(options, "maxDelay"_sr, 0.0);

		std::vector<std::string> restorePrefixesToInclude =
		    getOption(options, "restorePrefixesToInclude"_sr, std::vector<std::string>());

		shouldSkipRestoreRanges = deterministicRandom()->random01() < 0.3 ? true : false;

		// S3-specific: Default to no encryption to simplify S3 testing
		// Tests can explicitly enable encryption by setting encrypted=true in the toml file
		if (getOption(options, "encrypted"_sr, false)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}

		TraceEvent("BS3BCW_ClientId").detail("Id", wcx.clientId);

		if (backupRangesCount <= 0) {
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
			for (auto i = sortedEndpoints.begin(); i != sortedEndpoints.end(); ++i) {
				const std::string& start = *i++;
				backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(start, *i));
			}
		}

		if (shouldSkipRestoreRanges && backupRangesCount > 1) {
			skippedRestoreRanges.push_back(backupRanges[deterministicRandom()->randomInt(0, backupRanges.size())]);
		}

		for (const auto& range : backupRanges) {
			if (std::find(skippedRestoreRanges.begin(), skippedRestoreRanges.end(), range) ==
			    skippedRestoreRanges.end()) {
				restoreRanges.push_back_deep(restoreRanges.arena(), range);
			}
		}

		if (!restorePrefixesToInclude.empty()) {
			Standalone<VectorRef<KeyRangeRef>> filteredRestoreRanges;
			for (const auto& range : restoreRanges) {
				for (const auto& prefix : restorePrefixesToInclude) {
					if (range.begin.startsWith(StringRef(prefix))) {
						filteredRestoreRanges.push_back_deep(filteredRestoreRanges.arena(), range);
						break;
					}
				}
			}
			restoreRanges = filteredRestoreRanges;
		}

		TraceEvent("BS3BCW_Ranges");
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return _setup(cx, this);
	}

	Future<Void> _setup(Database cx, BackupS3BlobCorrectnessWorkload* self) {
		// S3-specific: Register MockS3Server or MockS3ServerChaos for blobstore URLs in simulation
		// Only client 0 registers the server to avoid duplicates
		// Persistence is automatically enabled in registration
		if (self->clientId == 0 && self->backupURL.rfind("blobstore://", 0) == 0 &&
		    (self->backupURL.find("127.0.0.1") != std::string::npos ||
		     self->backupURL.find("localhost") != std::string::npos) &&
		    g_network->isSimulated()) {
			TraceEvent("BS3BCW_RegisterMockS3")
			    .detail("URL", self->backupURL)
			    .detail("ClientId", self->clientId)
			    .detail("EnableChaos", self->enableChaos);

			if (self->enableChaos) {
				NetworkAddress listenAddress(IPAddress(0x7f000001), 8080);
				co_await startMockS3ServerChaos(listenAddress);

				// Configure chaos rates
				auto injector = S3FaultInjector::injector();
				injector->setErrorRate(self->errorRate);
				injector->setThrottleRate(self->throttleRate);
				injector->setDelayRate(self->delayRate);
				injector->setCorruptionRate(self->corruptionRate);
				injector->setMaxDelay(self->maxDelay);

				TraceEvent("BS3BCW_RegisteredMockS3Chaos")
				    .detail("Address", "127.0.0.1:8080")
				    .detail("ClientId", self->clientId)
				    .detail("ErrorRate", self->errorRate)
				    .detail("ThrottleRate", self->throttleRate)
				    .detail("DelayRate", self->delayRate)
				    .detail("CorruptionRate", self->corruptionRate)
				    .detail("MaxDelay", self->maxDelay);
			} else {
				co_await registerMockS3Server("127.0.0.1", "8080");
				TraceEvent("BS3BCW_RegisteredMockS3")
				    .detail("Address", "127.0.0.1:8080")
				    .detail("ClientId", self->clientId);
			}
		}

		// Backup everything
		self->backupRanges.push_back_deep(self->backupRanges.arena(), normalKeys);
		self->restoreRanges.push_back_deep(self->restoreRanges.arena(), normalKeys);
	}

	Future<Void> start(Database const& cx) override {
		// Only client 0 runs backup/restore operations
		// Other clients do nothing - the test harness waits for all clients to complete
		if (clientId != 0) {
			return Void();
		}
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		while (true) {
			co_await backupAgent->taskBucket->changePause(cx, deterministicRandom()->coinflip());
			co_await delay(30 * deterministicRandom()->random01());
		}
	}

	static Future<Void> statusLoop(Database cx, std::string tag) {
		FileBackupAgent agent;
		while (true) {
			bool active = co_await agent.checkActive(cx);
			TraceEvent("BS3BCW_AgentActivityCheck").detail("IsActive", active);
			std::string statusText = co_await agent.getStatus(cx, ShowErrors::True, tag);
			// S3-specific: Suppress backup status output during testing to reduce noise
			// puts(statusText.c_str());
			std::string statusJSON = co_await agent.getStatusJSON(cx, tag);
			// puts(statusJSON.c_str());
			co_await delay(2.0);
		}
	}

	static Future<Void> verifyBulkDumpObservability(Database cx, UID expectedBackupUID, std::string backupTag) {
		double startTime = now();
		double maxWaitTime = 30.0;

		TraceEvent("BS3BCW_ObservabilityCheckStart")
		    .detail("ExpectedBackupUID", expectedBackupUID)
		    .detail("BackupTag", backupTag);

		loop {
			Optional<BulkDumpProgress> progressOpt = co_await getBulkDumpProgress(cx);

			if (progressOpt.present()) {
				BulkDumpProgress progress = progressOpt.get();

				TraceEvent("BS3BCW_ObservabilityProgress")
				    .detail("JobId", progress.jobId)
				    .detail("TotalTasks", progress.totalTasks)
				    .detail("CompleteTasks", progress.completeTasks)
				    .detail("ProgressPercent", progress.progressPercent())
				    .detail("TotalBytes", progress.totalBytes)
				    .detail("CompletedBytes", progress.completedBytes);

				Optional<BulkDumpOwnerInfo> ownerInfo = co_await getBulkDumpOwner(cx, progress.jobId);
				if (ownerInfo.present() && ownerInfo.get().ownerUID == expectedBackupUID) {
					TraceEvent("BS3BCW_ObservabilityVerified").detail("BackupUID", expectedBackupUID);
					co_return;
				}
			}

			if (now() - startTime > maxWaitTime) {
				TraceEvent(SevWarn, "BS3BCW_ObservabilityTimeout");
				co_return;
			}

			co_await delay(5.0);
		}
	}

	static Future<Void> waitForRestorable(Reference<IBackupContainer> backupContainer, int maxAttempts) {
		int restorabilityCheckAttempts = 0;
		bool isRestorable = false;
		int64_t lastSnapshotBytes = 0;

		while (!isRestorable && restorabilityCheckAttempts < maxAttempts) {
			BackupDescription desc = co_await backupContainer->describeBackup();
			isRestorable = desc.maxRestorableVersion.present();
			lastSnapshotBytes = desc.snapshotBytes;
			if (!isRestorable) {
				TraceEvent("BS3BCW_WaitingForRestorable")
				    .detail("Attempt", restorabilityCheckAttempts)
				    .detail("SnapshotBytes", lastSnapshotBytes);
				co_await delay(2.0);
				restorabilityCheckAttempts++;
			}
		}

		// Do one final check after the loop to catch snapshots that completed
		// between the last check and now
		if (!isRestorable) {
			BackupDescription finalDesc = co_await backupContainer->describeBackup();
			isRestorable = finalDesc.maxRestorableVersion.present();
			lastSnapshotBytes = finalDesc.snapshotBytes;
			if (isRestorable) {
				TraceEvent("BS3BCW_BackupRestorableOnFinalCheck").detail("SnapshotBytes", lastSnapshotBytes);
			}
		}

		if (!isRestorable) {
			TraceEvent(SevError, "BS3BCW_BackupNotRestorableAfterWait")
			    .detail("Attempts", restorabilityCheckAttempts)
			    .detail("SnapshotBytes", lastSnapshotBytes);
			throw restore_invalid_version();
		}

		TraceEvent("BS3BCW_BackupRestorable")
		    .detail("AttemptsNeeded", restorabilityCheckAttempts)
		    .detail("SnapshotBytes", lastSnapshotBytes);
	}

	Future<Void> doBackup(double startDelay,
	                      FileBackupAgent* backupAgent,
	                      Database cx,
	                      Key tag,
	                      Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                      double stopDifferentialDelay,
	                      Promise<Void> submitted) {

		UID randomID = nondeterministicRandom()->randomUniqueID();

		// Increment the backup agent requests
		if (agentRequest) {
			BackupS3BlobCorrectnessWorkload::backupAgentRequests++;
		}

		Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		co_await delay(startDelay);

		// S3-specific: Conditional cleanup matching original BackupCorrectness behavior
		// Only abort existing backups on first call (startDelay > 0) or randomly (BUGGIFY)
		// This prevents excessive cleanup on test restarts that caused timeouts
		if (startDelay || BUGGIFY) {
			TraceEvent("BS3BCW_DoBackupAbortBackup1", randomID)
			    .detail("Tag", printable(tag))
			    .detail("StartDelay", startDelay);

			try {
				co_await backupAgent->abortBackup(cx, tag.toString());
			} catch (Error& e) {
				TraceEvent("BS3BCW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BS3BCW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));

		EBackupState statusValue;
		try {
			EBackupState _statusValue = co_await backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::False);
			statusValue = _statusValue;
		} catch (Error& e) {
			// If backup_unneeded, it means there's no active backup (possibly completed from previous test)
			// Treat this as STATE_NEVERRAN so we can start a fresh backup
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupWaitBackupUnneeded", randomID).detail("Tag", printable(tag));
				statusValue = EBackupState::STATE_NEVERRAN;
			} else {
				throw;
			}
		}

		TraceEvent("BS3BCW_DoBackupWaitBackupStatus", randomID)
		    .detail("Status", BackupAgentBase::getStateText(statusValue))
		    .detail("Tag", printable(tag));

		if (statusValue == EBackupState::STATE_COMPLETED) {
			TraceEvent("BS3BCW_DoBackupDiscontinued", randomID).detail("Tag", printable(tag));
			co_return;
		}

		if (statusValue != EBackupState::STATE_NEVERRAN) {
			TraceEvent("BS3BCW_DoBackupAbortBackup2", randomID).detail("Tag", printable(tag));

			try {
				co_await backupAgent->abortBackup(cx, tag.toString());
			} catch (Error& e) {
				TraceEvent("BS3BCW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BS3BCW_DoBackupSubmitBackup", randomID)
		    .detail("Tag", printable(tag))
		    .detail("StopWhenDone", stopDifferentialDelay ? "False" : "True");

		// S3-specific: Use configurable backup URL and snapshot intervals
		std::string backupContainer = backupURL;
		Future<Void> status = statusLoop(cx, tag.toString());

		// Testing v1 (non-partitioned) backup approach
		// This does not require backup workers
		try {
			co_await backupAgent->submitBackup(cx,
			                                   StringRef(backupContainer),
			                                   {},
			                                   initSnapshotInterval,
			                                   snapshotInterval,
			                                   tag.toString(),
			                                   backupRanges,
			                                   StopWhenDone{ !stopDifferentialDelay },
			                                   UsePartitionedLog::False,
			                                   IncrementalBackupOnly::False,
			                                   encryptionKeyFileName,
			                                   encryptionKeyFileName.present() ? DEFAULT_ENCRYPTION_BLOCK_SIZE : 0,
			                                   snapshotMode);
		} catch (Error& e) {
			TraceEvent("BS3BCW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		submitted.send(Void());

		Future<Void> observabilityCheck = Void();
		if (snapshotMode == 1 || snapshotMode == 2) {
			KeyBackedTag keyBackedTag = makeBackupTag(tag.toString());
			try {
				UidAndAbortedFlagT uidFlag = co_await keyBackedTag.getOrThrow(cx.getReference());
				UID backupUID = uidFlag.first;
				observabilityCheck = verifyBulkDumpObservability(cx, backupUID, tag.toString());
			} catch (Error& e) {
				TraceEvent(SevWarn, "BS3BCW_CouldNotGetBackupUID").error(e);
			}
		}

		TraceEvent("BS3BCW_DoBackupWaitToDiscontinue", randomID)
		    .detail("Tag", printable(tag))
		    .detail("DifferentialAfter", stopDifferentialDelay);

		try {
			co_await backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True);
		} catch (Error& e) {
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupWaitToDiscontinueUnneeded", randomID).detail("Tag", printable(tag));
				co_return;
			}
			throw;
		}

		TraceEvent("BS3BCW_DoBackupDiscontinueBackup", randomID).detail("Tag", printable(tag));

		try {
			co_await backupAgent->discontinueBackup(cx, tag);
		} catch (Error& e) {
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupDiscontinueBackupUnneeded", randomID).detail("Tag", printable(tag));
				co_return;
			}
			throw;
		}

		TraceEvent("BS3BCW_DoBackupWaitForDiscontinued", randomID).detail("Tag", printable(tag));

		try {
			co_await backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True);
		} catch (Error& e) {
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupWaitForDiscontinuedUnneeded", randomID).detail("Tag", printable(tag));
				co_return;
			}
			throw;
		}

		TraceEvent("BS3BCW_DoBackupComplete", randomID).detail("Tag", printable(tag));
	}

	Future<Void> _start(Database cx) {
		FileBackupAgent backupAgent;
		Future<Void> stopDifferentialBackup = delay(stopDifferentialAfter);

		TraceEvent("BS3BCW_Arguments")
		    .detail("BackupAfter", backupAfter)
		    .detail("RestoreAfter", restoreAfter)
		    .detail("RestoreStartAfterBackupFinished", restoreStartAfterBackupFinished)
		    .detail("AbortAndRestartAfter", abortAndRestartAfter)
		    .detail("DifferentialAfter", stopDifferentialAfter);

		// S3-specific: Clean up state on every test restart to prevent restore_invalid_version errors
		// The test harness restarts tests multiple times. Without cleanup, FDB metadata points to
		// backups that no longer exist in MockS3, causing restore_invalid_version when trying to restore.
		// We must:
		// 1. Abort any existing backup (clears FDB metadata)
		// 2. Clear MockS3 storage (removes old backup data)
		// This ensures FDB and S3 are always in sync at the start of each test run.
		if (agentRequest) {
			TraceEvent("BS3BCW_CleanupOnRestart").detail("Tag", printable(backupTag));
			try {
				co_await backupAgent.abortBackup(cx, backupTag.toString());
			} catch (Error& e) {
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		// Note: Do NOT clear MockS3 storage here! It would wipe out the persisted backup container
		// metadata that was just initialized when the server was registered. The persistence system
		// handles cleanup properly on its own.

		if (agentRequest) {
			Promise<Void> submitted;
			Future<Void> b =
			    doBackup(backupAfter, &backupAgent, cx, backupTag, backupRanges, stopDifferentialAfter, submitted);

			if (abortAndRestartAfter) {
				TraceEvent("BS3BCW_AbortAndRestartAfter").detail("AbortAndRestartAfter", abortAndRestartAfter);
				co_await submitted.getFuture();
				co_await delay(abortAndRestartAfter - backupAfter);
				TraceEvent("BS3BCW_AbortBackup").detail("Tag", printable(backupTag));
				try {
					co_await backupAgent.abortBackup(cx, backupTag.toString());
				} catch (Error& e) {
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
				TraceEvent("BS3BCW_AbortComplete").detail("Tag", printable(backupTag));
				co_await b;
				TraceEvent("BS3BCW_RestartBackup").detail("Tag", printable(backupTag));
				b = doBackup(0,
				             &backupAgent,
				             cx,
				             backupTag,
				             backupRanges,
				             stopDifferentialAfter - abortAndRestartAfter,
				             Promise<Void>());
			}

			if (performRestore) {
				// Adaptive timing: Wait for backup to complete, then wait additional time
				// This ensures the backup metadata is written before restore starts
				TraceEvent("BS3BCW_WaitingForBackupCompletion").detail("WaitingForBackup", true);
				co_await b;
				TraceEvent("BS3BCW_BackupCompleted").detail("BackupFinished", true);

				// Wait additional time after backup completes for metadata to be written
				if (restoreStartAfterBackupFinished > 0) {
					TraceEvent("BS3BCW_WaitingAfterBackupComplete")
					    .detail("DelaySeconds", restoreStartAfterBackupFinished);
					co_await delay(restoreStartAfterBackupFinished);
				}

				TraceEvent("BS3BCW_StartingRestore").detail("RestoreStarting", true);

				// Get the backup container to restore from
				KeyBackedTag keyBackedTag = makeBackupTag(backupTag.toString());
				UidAndAbortedFlagT uidFlag = co_await keyBackedTag.getOrThrow(cx.getReference());
				UID logUid = uidFlag.first;
				Reference<IBackupContainer> lastBackupContainer =
				    co_await BackupConfig(logUid).backupContainer().getD(cx.getReference());

				// Wait for backup to become restorable if it's still in progress
				// Increased timeout for complex multi-region configs
				if (lastBackupContainer) {
					co_await waitForRestorable(lastBackupContainer, 150);

					// Generate a lock UID for the entire clear+restore operation
					UID lockUID = deterministicRandom()->randomUniqueID();

					// Lock the database to prevent other workloads from seeing inconsistent state
					// during clear+restore. This makes workloads like Cycle get database_locked
					// errors (retriable) instead of encountering missing keys.
					co_await lockDatabase(cx, lockUID);

					TraceEvent("BS3BCW_DatabaseLocked")
					    .detail("LockUID", lockUID)
					    .detail("BackupTag", printable(backupTag));

					// BulkLoad validation: compare BulkLoad restore vs traditional restore
					// 1. Traditional restore with addPrefix to system keyspace (\xff\x02/rlog/)
					// 2. Clear normalKeys
					// 3. BulkLoad restore to normalKeys
					// 4. audit_storage validate_restore compares normalKeys vs prefixed data

					// Prefix for validation - restored data goes to system keyspace
					Key validationPrefix = "\xff\x02/rlog/"_sr;
					Key validationPrefixEnd = "\xff\x02/rlog0"_sr;

					if (performValidation) {
						// Step 1: Restore with prefix using TRADITIONAL (rangefile) mode
						TraceEvent("BS3BCW_ValidationStep1_TraditionalRestore")
						    .detail("BackupTag", printable(backupTag))
						    .detail("ValidationPrefix", printable(validationPrefix));

						Standalone<StringRef> validationRestoreTag(backupTag.toString() + "_validate");
						try {
							Version validationVersion =
							    co_await backupAgent.restore(cx,
							                                 cx,
							                                 validationRestoreTag,
							                                 KeyRef(lastBackupContainer->getURL()),
							                                 lastBackupContainer->getProxy(),
							                                 restoreRanges,
							                                 WaitForComplete::True,
							                                 ::invalidVersion,
							                                 Verbose::True,
							                                 validationPrefix, // addPrefix
							                                 Key(), // removePrefix
							                                 LockDB::False, // already locked
							                                 UnlockDB::False, // don't unlock yet
							                                 OnlyApplyMutationLogs::False,
							                                 InconsistentSnapshotOnly::False,
							                                 ::invalidVersion,
							                                 lastBackupContainer->getEncryptionKeyFileName(),
							                                 lockUID,
							                                 true); // useRangeFileRestore = true (traditional mode)

							TraceEvent("BS3BCW_ValidationStep1_Complete")
							    .detail("ValidationVersion", validationVersion)
							    .detail("ValidationPrefix", printable(validationPrefix));
						} catch (Error& e) {
							TraceEvent(SevError, "BS3BCW_ValidationStep1_Failed").error(e);
							throw;
						}
					}

					// Step 2: Clear the backup ranges before restoring (unless skipDirtyRestore is true)
					if (!skipDirtyRestore) {
						TraceEvent("BS3BCW_ClearingNormalKeys");
						co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
							tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
							tr->setOption(FDBTransactionOptions::LOCK_AWARE);
							for (auto& kvrange : backupRanges)
								tr->clear(kvrange);
							return Void();
						});
					}

					// Step 3: Perform the restore (with BulkLoad if configured)
					TraceEvent("BS3BCW_Restore")
					    .detail("LastBackupContainer", lastBackupContainer->getURL())
					    .detail("BackupTag", printable(backupTag))
					    .detail("LockUID", lockUID)
					    .detail("SkipDirtyRestore", skipDirtyRestore);

					Standalone<StringRef> restoreTag(backupTag.toString() + "_restore");
					// Pass lockDB=False since we already locked, unlockDB=True to release when done,
					// and our lockUID so restore uses the same lock for checkDatabaseLock calls
					Version v = co_await backupAgent.restore(cx,
					                                         cx,
					                                         restoreTag,
					                                         KeyRef(lastBackupContainer->getURL()),
					                                         lastBackupContainer->getProxy(),
					                                         restoreRanges,
					                                         WaitForComplete::True,
					                                         ::invalidVersion,
					                                         Verbose::True,
					                                         Key(),
					                                         Key(),
					                                         LockDB::False,
					                                         UnlockDB::True,
					                                         OnlyApplyMutationLogs::False,
					                                         InconsistentSnapshotOnly::False,
					                                         ::invalidVersion,
					                                         lastBackupContainer->getEncryptionKeyFileName(),
					                                         lockUID,
					                                         useRangeFileRestore);

					TraceEvent("BS3BCW_RestoreComplete")
					    .detail("BackupTag", printable(backupTag))
					    .detail("RestoreVersion", v);

					// ASSERT: When useRangeFileRestore=false, BulkLoad MUST have been used
					if (!useRangeFileRestore) {
						int bulkLoadCount = g_bulkLoadRestoreTaskCompleteCount.load();
						TraceEvent("BS3BCW_AssertBulkLoadUsed")
						    .detail("UseRangeFileRestore", useRangeFileRestore)
						    .detail("BulkLoadRestoreTaskCompleteCount", bulkLoadCount);
						// FAIL if BulkLoad didn't run
						ASSERT(bulkLoadCount > 0);
					}

					// Step 4: Run audit to compare BulkLoad-restored vs traditional-restored
					if (performValidation) {
						TraceEvent("BS3BCW_ValidationStep4_AuditStarting")
						    .detail("Comparing", "BulkLoad-restored (normalKeys) vs traditional-restored (prefix)");

						Reference<IClusterConnectionRecord> clusterFile = cx->getConnectionRecord();
						UID auditId;
						int auditRetryCount = 0;
						int maxAuditRetries = 5;

						Error auditError;
						while (true) {
							Error err;
							try {
								UID scheduleResult = co_await timeoutError(auditStorage(clusterFile,
								                                                        normalKeys,
								                                                        AuditType::ValidateRestore,
								                                                        KeyValueStoreType::END,
								                                                        300.0),
								                                           60.0);
								auditId = scheduleResult;
								break;
							} catch (Error& e) {
								err = e;
							}
							auditError = err;
							if (auditError.code() == error_code_timed_out ||
							    auditError.code() == error_code_audit_storage_failed) {
								auditRetryCount++;
								if (auditRetryCount < maxAuditRetries) {
									TraceEvent(SevWarn, "BS3BCW_ValidationAuditRetry")
									    .error(auditError)
									    .detail("RetryCount", auditRetryCount);
									co_await delay(2.0 * auditRetryCount);
									continue;
								}
							}
							throw auditError;
						}

						TraceEvent("BS3BCW_ValidationAuditScheduled").detail("AuditID", auditId);

						// Monitor audit progress
						double auditStartTime = now();
						double maxAuditWaitTime = 300.0;
						AuditPhase finalPhase = AuditPhase::Invalid;

						while (true) {
							co_await delay(5.0);

							std::vector<AuditStorageState> auditStates =
							    co_await getAuditStates(cx, AuditType::ValidateRestore, true);

							for (const auto& auditState : auditStates) {
								if (auditState.id == auditId) {
									if (auditState.getPhase() == AuditPhase::Complete) {
										finalPhase = AuditPhase::Complete;
										break;
									} else if (auditState.getPhase() == AuditPhase::Error ||
									           auditState.getPhase() == AuditPhase::Failed) {
										TraceEvent(SevError, "BS3BCW_ValidationAuditFailed")
										    .detail("AuditID", auditId)
										    .detail("Phase", (int)auditState.getPhase())
										    .detail("Error", auditState.error)
										    .detail(
										        "Meaning",
										        "BulkLoad restore produced different data than traditional restore!");
										throw audit_storage_failed();
									}
								}
							}

							if (finalPhase == AuditPhase::Complete) {
								break;
							}

							if (now() - auditStartTime > maxAuditWaitTime) {
								TraceEvent(SevError, "BS3BCW_ValidationAuditTimeout")
								    .detail("AuditID", auditId)
								    .detail("ElapsedTime", now() - auditStartTime);
								throw timed_out();
							}
						}

						TraceEvent("BS3BCW_ValidationAuditComplete")
						    .detail("AuditID", auditId)
						    .detail("Result", "BulkLoad produces identical results to traditional restore");

						// Step 5: Clean up validation data from system keyspace
						co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
							tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
							tr->setOption(FDBTransactionOptions::LOCK_AWARE);
							tr->clear(KeyRangeRef(validationPrefix, validationPrefixEnd));
							return Void();
						});

						TraceEvent("BS3BCW_ValidationComplete")
						    .detail("BackupTag", printable(backupTag))
						    .detail("AuditID", auditId);
					}
				}
			}

			// ASSERT: Verify BulkDump was used when snapshotMode=1 (BULKDUMP) or 2 (BOTH)
			if (snapshotMode == 1 || snapshotMode == 2) {
				int bulkDumpCount = g_bulkDumpTaskCompleteCount.load();
				TraceEvent("BS3BCW_AssertBulkDumpUsed")
				    .detail("SnapshotMode", snapshotMode)
				    .detail("BulkDumpTaskCompleteCount", bulkDumpCount);
				// FAIL if BulkDump didn't run
				ASSERT(bulkDumpCount > 0);
			}

			co_await b;
		}
	}
};

int BackupS3BlobCorrectnessWorkload::backupAgentRequests = 0;

WorkloadFactory<BackupS3BlobCorrectnessWorkload> BackupS3BlobCorrectnessWorkloadFactory;
