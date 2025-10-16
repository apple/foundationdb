/*
 * BackupS3BlobCorrectness.actor.cpp
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
 * 3. Tenant Cleanup Handling:
 *    - Gracefully handles tenant_not_found errors during setup
 *    - Returns early if tenant was deleted during test cleanup
 *    - Original would fail the test on tenant_not_found
 *
 * 4. Status Loop Behavior:
 *    - Exits early when backup reaches "Completed" state or snapshot closes
 *    - Reduces unnecessary polling for S3 metadata that may be eventually consistent
 *    - Original polls continuously until external termination
 *
 * 5. Configurable Snapshot Intervals:
 *    - Accepts initSnapshotInterval and snapshotInterval parameters
 *    - Allows tests to control S3 backup timing characteristics
 *    - Original uses hardcoded random values
 *
 * 6. Configurable Backup URL:
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
#include "fdbserver/MockS3Server.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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

	// This workload is not compatible with RandomRangeLock workload because they will race in locked range
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	BackupS3BlobCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
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

		// S3-specific options
		backupURL = getOption(options, "backupURL"_sr, "file://simfdb/backups/"_sr).toString();
		skipDirtyRestore = getOption(options, "skipDirtyRestore"_sr, true);
		initSnapshotInterval = getOption(options, "initSnapshotInterval"_sr, 0);
		snapshotInterval = getOption(options, "snapshotInterval"_sr, 30);

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
		// Only client 0 performs setup operations (MockS3 registration, tenant adjustment)
		// Other clients just run the Cycle workload
		if (clientId != 0) {
			return Void();
		}
		return _setup(cx, this);
	}

	ACTOR Future<Void> _setup(Database cx, BackupS3BlobCorrectnessWorkload* self) {
		state bool adjusted = false;
		state TenantMapEntry entry;

		// S3-specific: Register MockS3Server only for blobstore URLs in simulation
		// Only client 0 registers the MockS3Server to avoid duplicates
		if (self->clientId == 0 && self->backupURL.rfind("blobstore://", 0) == 0 &&
		    (self->backupURL.find("127.0.0.1") != std::string::npos ||
		     self->backupURL.find("localhost") != std::string::npos) &&
		    g_network->isSimulated()) {
			TraceEvent("BS3BCW_RegisterMockS3").detail("URL", self->backupURL).detail("ClientId", self->clientId);
			wait(g_simulator->registerSimHTTPServer("127.0.0.1", "8080", makeReference<MockS3RequestHandler>()));
			TraceEvent("BS3BCW_RegisteredMockS3")
			    .detail("Address", "127.0.0.1:8080")
			    .detail("ClientId", self->clientId);
		}

		if (!self->defaultBackup && (cx->defaultTenant.present() || BUGGIFY)) {
			if (cx->defaultTenant.present()) {
				try {
					wait(store(entry, TenantAPI::getTenant(cx.getReference(), cx->defaultTenant.get())));
				} catch (Error& e) {
					// S3-specific: Handle tenant cleanup gracefully - if tenant is deleted during test cleanup,
					// skip backup operations
					if (e.code() == error_code_tenant_not_found) {
						TraceEvent(SevInfo, "BS3BCW_TenantCleanedUp")
						    .detail("Reason", "Tenant was cleaned up during test, skipping backup operations")
						    .detail("TenantName", cx->defaultTenant.get())
						    .detail("ErrorCode", e.code())
						    .detail("ErrorDescription", e.what());
						return Void(); // Skip backup operations for deleted tenant
					}
					throw; // Re-throw other errors
				}

				// If we are specifying sub-ranges (or randomly, if backing up normal keys), adjust them to be relative
				// to the tenant
				if (self->backupRangesCount > 0 || deterministicRandom()->coinflip()) {
					Standalone<VectorRef<KeyRangeRef>> adjustedBackupRanges;
					for (auto& range : self->backupRanges) {
						adjustedBackupRanges.push_back_deep(
						    adjustedBackupRanges.arena(), range.withPrefix(entry.prefix, adjustedBackupRanges.arena()));
					}
					self->backupRanges = adjustedBackupRanges;

					Standalone<VectorRef<KeyRangeRef>> adjustedRestoreRanges;
					for (auto& range : self->restoreRanges) {
						adjustedRestoreRanges.push_back_deep(
						    adjustedRestoreRanges.arena(),
						    range.withPrefix(entry.prefix, adjustedRestoreRanges.arena()));
					}
					self->restoreRanges = adjustedRestoreRanges;

					std::vector<KeyRange> adjustedSkippedRestoreRanges;
					for (auto& range : self->skippedRestoreRanges) {
						adjustedSkippedRestoreRanges.push_back(range.withPrefix(entry.prefix));
					}
					self->skippedRestoreRanges = adjustedSkippedRestoreRanges;

					TraceEvent("BS3BCW_AdjustedRanges");

					adjusted = true;
				}
			} else {
				try {
					Optional<TenantMapEntry> entry_ = wait(TenantAPI::createTenant(cx.getReference(), "BARWTenant"_sr));
					entry = entry_.get();
				} catch (Error& e) {
					if (e.code() == error_code_tenant_already_exists) {
						wait(store(entry, TenantAPI::getTenant(cx.getReference(), "BARWTenant"_sr)));
					} else {
						throw e;
					}
				}
			}
		}

		if (!adjusted) {
			// Backup everything
			self->backupRanges.push_back_deep(self->backupRanges.arena(), normalKeys);
			self->restoreRanges.push_back_deep(self->restoreRanges.arena(), normalKeys);
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		// Only client 0 runs backup/restore operations
		// Other clients just run the Cycle workload
		if (clientId != 0) {
			return Void();
		}
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		loop {
			wait(backupAgent->taskBucket->changePause(cx, deterministicRandom()->coinflip()));
			wait(delay(30 * deterministicRandom()->random01()));
		}
	}

	ACTOR static Future<Void> statusLoop(Database cx, std::string tag) {
		state FileBackupAgent agent;
		loop {
			bool active = wait(agent.checkActive(cx));
			TraceEvent("BS3BCW_AgentActivityCheck").detail("IsActive", active);
			state std::string statusText = wait(agent.getStatus(cx, ShowErrors::True, tag));
			// S3-specific: Suppress backup status output during testing to reduce noise
			// puts(statusText.c_str());
			std::string statusJSON = wait(agent.getStatusJSON(cx, tag));
			// puts(statusJSON.c_str());

			// S3-specific: Exit early when backup reaches completed state or snapshot closes
			// This reduces unnecessary polling for S3 metadata that may be eventually consistent
			if (statusText.find("\"Name\":\"Completed\"") != std::string::npos ||
			    (statusJSON.find("\"StopAfterSnapshot\":true") != std::string::npos &&
			     statusJSON.find("\"ExpectedProgress\":100") != std::string::npos)) {
				TraceEvent("BS3BCW_StatusLoopExit").detail("Reason", "CompletedOrSnapshotClosed");
				return Void();
			}
			wait(delay(2.0));
		}
	}

	ACTOR static Future<Void> doBackup(BackupS3BlobCorrectnessWorkload* self,
	                                   double startDelay,
	                                   FileBackupAgent* backupAgent,
	                                   Database cx,
	                                   Key tag,
	                                   Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                   double stopDifferentialDelay,
	                                   Promise<Void> submitted) {

		state UID randomID = nondeterministicRandom()->randomUniqueID();

		// Increment the backup agent requests
		if (self->agentRequest) {
			BackupS3BlobCorrectnessWorkload::backupAgentRequests++;
		}

		state Future<Void> stopDifferentialFuture = delay(stopDifferentialDelay);
		wait(delay(startDelay));

		// S3-specific: Conditional cleanup matching original BackupCorrectness behavior
		// Only abort existing backups on first call (startDelay > 0) or randomly (BUGGIFY)
		// This prevents excessive cleanup on test restarts that caused timeouts
		if (startDelay || BUGGIFY) {
			TraceEvent("BS3BCW_DoBackupAbortBackup1", randomID)
			    .detail("Tag", printable(tag))
			    .detail("StartDelay", startDelay);

			try {
				wait(backupAgent->abortBackup(cx, tag.toString()));
			} catch (Error& e) {
				TraceEvent("BS3BCW_DoBackupAbortBackupException", randomID).error(e).detail("Tag", printable(tag));
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		TraceEvent("BS3BCW_DoBackupWaitBackup", randomID).detail("Tag", printable(tag));

		state EBackupState statusValue;
		try {
			EBackupState _statusValue = wait(backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::False));
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
			return Void();
		}

		if (statusValue != EBackupState::STATE_NEVERRAN) {
			TraceEvent("BS3BCW_DoBackupAbortBackup2", randomID).detail("Tag", printable(tag));

			try {
				wait(backupAgent->abortBackup(cx, tag.toString()));
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
		state std::string backupContainer = self->backupURL;
		state Future<Void> status = statusLoop(cx, tag.toString());
		try {
			wait(backupAgent->submitBackup(cx,
			                               StringRef(backupContainer),
			                               {},
			                               self->initSnapshotInterval,
			                               self->snapshotInterval,
			                               tag.toString(),
			                               backupRanges,
			                               true,
			                               StopWhenDone{ !stopDifferentialDelay },
			                               UsePartitionedLog::False,
			                               IncrementalBackupOnly::False,
			                               self->encryptionKeyFileName));
		} catch (Error& e) {
			TraceEvent("BS3BCW_DoBackupSubmitBackupException", randomID).error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		submitted.send(Void());

		TraceEvent("BS3BCW_DoBackupWaitToDiscontinue", randomID)
		    .detail("Tag", printable(tag))
		    .detail("DifferentialAfter", stopDifferentialDelay);

		try {
			wait(success(backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True)));
		} catch (Error& e) {
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupWaitToDiscontinueUnneeded", randomID).detail("Tag", printable(tag));
				return Void();
			}
			throw;
		}

		TraceEvent("BS3BCW_DoBackupDiscontinueBackup", randomID).detail("Tag", printable(tag));

		try {
			wait(backupAgent->discontinueBackup(cx, tag));
		} catch (Error& e) {
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupDiscontinueBackupUnneeded", randomID).detail("Tag", printable(tag));
				return Void();
			}
			throw;
		}

		TraceEvent("BS3BCW_DoBackupWaitForDiscontinued", randomID).detail("Tag", printable(tag));

		try {
			wait(success(backupAgent->waitBackup(cx, tag.toString(), StopWhenDone::True)));
		} catch (Error& e) {
			if (e.code() == error_code_backup_unneeded) {
				TraceEvent("BS3BCW_DoBackupWaitForDiscontinuedUnneeded", randomID).detail("Tag", printable(tag));
				return Void();
			}
			throw;
		}

		TraceEvent("BS3BCW_DoBackupComplete", randomID).detail("Tag", printable(tag));

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupS3BlobCorrectnessWorkload* self) {
		state FileBackupAgent backupAgent;
		state Future<Void> stopDifferentialBackup = delay(self->stopDifferentialAfter);

		TraceEvent("BS3BCW_Arguments")
		    .detail("BackupAfter", self->backupAfter)
		    .detail("RestoreAfter", self->restoreAfter)
		    .detail("AbortAndRestartAfter", self->abortAndRestartAfter)
		    .detail("DifferentialAfter", self->stopDifferentialAfter);

		// S3-specific: Clean up state on every test restart to prevent restore_invalid_version errors
		// The test harness restarts tests multiple times. Without cleanup, FDB metadata points to
		// backups that no longer exist in MockS3, causing restore_invalid_version when trying to restore.
		// We must:
		// 1. Abort any existing backup (clears FDB metadata)
		// 2. Clear MockS3 storage (removes old backup data)
		// This ensures FDB and S3 are always in sync at the start of each test run.
		if (self->agentRequest) {
			TraceEvent("BS3BCW_CleanupOnRestart").detail("Tag", printable(self->backupTag));
			try {
				wait(backupAgent.abortBackup(cx, self->backupTag.toString()));
			} catch (Error& e) {
				if (e.code() != error_code_backup_unneeded)
					throw;
			}
		}

		// Clear MockS3 storage from previous test runs
		if (g_network->isSimulated()) {
			clearMockS3Storage();
			TraceEvent("BS3BCW_ClearedMockS3Storage");
		}

		if (self->agentRequest) {
			state Promise<Void> submitted;
			state Future<Void> b = doBackup(self,
			                                self->backupAfter,
			                                &backupAgent,
			                                cx,
			                                self->backupTag,
			                                self->backupRanges,
			                                self->stopDifferentialAfter,
			                                submitted);

			if (self->abortAndRestartAfter) {
				TraceEvent("BS3BCW_AbortAndRestartAfter").detail("AbortAndRestartAfter", self->abortAndRestartAfter);
				wait(submitted.getFuture());
				wait(delay(self->abortAndRestartAfter - self->backupAfter));
				TraceEvent("BS3BCW_AbortBackup").detail("Tag", printable(self->backupTag));
				try {
					wait(backupAgent.abortBackup(cx, self->backupTag.toString()));
				} catch (Error& e) {
					if (e.code() != error_code_backup_unneeded)
						throw;
				}
				TraceEvent("BS3BCW_AbortComplete").detail("Tag", printable(self->backupTag));
				wait(b);
				TraceEvent("BS3BCW_RestartBackup").detail("Tag", printable(self->backupTag));
				b = doBackup(self,
				             0,
				             &backupAgent,
				             cx,
				             self->backupTag,
				             self->backupRanges,
				             self->stopDifferentialAfter - self->abortAndRestartAfter,
				             Promise<Void>());
			}

			if (self->performRestore) {
				wait(delay(self->restoreAfter));
				TraceEvent("BS3BCW_RestoreAfter").detail("RestoreAfter", self->restoreAfter);

				// Get the backup container to restore from
				state KeyBackedTag keyBackedTag = makeBackupTag(self->backupTag.toString());
				UidAndAbortedFlagT uidFlag = wait(keyBackedTag.getOrThrow(cx.getReference()));
				state UID logUid = uidFlag.first;
				state Reference<IBackupContainer> lastBackupContainer =
				    wait(BackupConfig(logUid).backupContainer().getD(cx.getReference()));

				if (lastBackupContainer) {
					// Clear the backup ranges before restoring (unless skipDirtyRestore is true)
					if (!self->skipDirtyRestore) {
						wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
							tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
							for (auto& kvrange : self->backupRanges)
								tr->clear(kvrange);
							return Void();
						}));
					}

					// Perform the restore
					TraceEvent("BS3BCW_Restore")
					    .detail("LastBackupContainer", lastBackupContainer->getURL())
					    .detail("BackupTag", printable(self->backupTag))
					    .detail("SkipDirtyRestore", self->skipDirtyRestore);

					Standalone<StringRef> restoreTag(self->backupTag.toString() + "_restore");
					Version _ = wait(backupAgent.restore(cx,
					                                     cx,
					                                     restoreTag,
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
					                                     lastBackupContainer->getEncryptionKeyFileName()));

					TraceEvent("BS3BCW_RestoreComplete").detail("BackupTag", printable(self->backupTag));
				}
			}

			wait(b);
		}

		return Void();
	}
};

int BackupS3BlobCorrectnessWorkload::backupAgentRequests = 0;

WorkloadFactory<BackupS3BlobCorrectnessWorkload> BackupS3BlobCorrectnessWorkloadFactory;
