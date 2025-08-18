/*
 * IncrementalBackup.actor.cpp
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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Arena.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct IncrementalBackupWorkload : TestWorkload {
	static constexpr auto NAME = "IncrementalBackup";

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	FileBackupAgent backupAgent;
	bool submitOnly;
	bool restoreOnly;
	bool waitForBackup;
	int waitRetries;
	bool stopBackup;
	bool checkBeginVersion;
	bool clearBackupAgentKeys;
	Standalone<StringRef> blobManifestUrl;
	Optional<std::string> encryptionKeyFileName;

	IncrementalBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		submitOnly = getOption(options, "submitOnly"_sr, false);
		restoreOnly = getOption(options, "restoreOnly"_sr, false);
		waitForBackup = getOption(options, "waitForBackup"_sr, false);
		waitRetries = getOption(options, "waitRetries"_sr, -1);
		stopBackup = getOption(options, "stopBackup"_sr, false);
		checkBeginVersion = getOption(options, "checkBeginVersion"_sr, false);
		clearBackupAgentKeys = getOption(options, "clearBackupAgentKeys"_sr, false);
		blobManifestUrl = getOption(options, "blobManifestUrl"_sr, ""_sr);

		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId) {
			return Void();
		}
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId) {
			return true;
		}
		return _check(cx, this);
	}

	ACTOR static Future<bool> _check(Database cx, IncrementalBackupWorkload* self) {
		if (self->waitForBackup) {
			// Undergoing recovery with the snapshot system keys set will pause the backup agent
			// Pre-emptively unpause any backup agents before attempting to wait to avoid getting stuck
			wait(self->backupAgent.changePause(cx, false));
			state Reference<IBackupContainer> backupContainer;
			state UID backupUID;
			state Version v;
			state Transaction tr(cx);
			loop {
				try {
					wait(store(v, tr.getReadVersion()));
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			loop {
				// Wait for backup container to be created and avoid race condition
				TraceEvent("IBackupWaitContainer").log();
				wait(success(self->backupAgent.waitBackup(
				    cx, self->tag.toString(), StopWhenDone::False, &backupContainer, &backupUID)));

				state Optional<std::string> restoreEncryptionKeyFileName;
				if (self->encryptionKeyFileName.present() && fileExists(self->encryptionKeyFileName.get())) {
					restoreEncryptionKeyFileName = self->encryptionKeyFileName.get();
				}

				if (!backupContainer.isValid()) {
					TraceEvent("IBackupCheckListContainersAttempt").log();
					state std::vector<std::string> containers =
					    wait(IBackupContainer::listContainers(self->backupDir.toString(), {}));
					TraceEvent("IBackupCheckListContainersSuccess")
					    .detail("Size", containers.size())
					    .detail("First", containers.front());
					if (containers.size()) {
						backupContainer =
						    IBackupContainer::openContainer(containers.front(), {}, restoreEncryptionKeyFileName);
					}
				}
				state bool e = wait(backupContainer->exists());
				if (e)
					break;
				wait(delay(5.0));
			}
			state int tries = 0;
			loop {
				tries++;
				state BackupDescription desc = wait(backupContainer->describeBackup(true));
				TraceEvent("IBackupVersionGate")
				    .detail("MaxLogEndVersion", desc.maxLogEnd.present() ? desc.maxLogEnd.get() : invalidVersion)
				    .detail("ContiguousLogEndVersion",
				            desc.contiguousLogEnd.present() ? desc.contiguousLogEnd.get() : invalidVersion)
				    .detail("TargetVersion", v);

				if (!desc.contiguousLogEnd.present()) {
					wait(delay(5.0));
					continue;
				}
				if (desc.contiguousLogEnd.get() >= v)
					break;
				if (self->waitRetries != -1 && tries > self->waitRetries)
					break;
				// Avoid spamming requests with a delay
				wait(delay(5.0));
			}
		}
		if (self->stopBackup) {
			try {
				TraceEvent("IBackupDiscontinueBackup").log();
				wait(self->backupAgent.discontinueBackup(cx, self->tag));
			} catch (Error& e) {
				TraceEvent("IBackupDiscontinueBackupException").error(e);
				if (e.code() != error_code_backup_unneeded) {
					throw;
				}
			}
		}
		return true;
	}

	ACTOR static Future<Void> _start(Database cx, IncrementalBackupWorkload* self) {
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		addDefaultBackupRanges(backupRanges);

		if (self->submitOnly) {
			if (self->encryptionKeyFileName.present()) {
				wait(BackupContainerFileSystem::createTestEncryptionKeyFile(self->encryptionKeyFileName.get()));
			}

			TraceEvent("IBackupSubmitAttempt").log();
			try {
				Optional<std::string> blobManifestUrl;
				if (!self->blobManifestUrl.empty()) {
					blobManifestUrl = self->blobManifestUrl.toString();
				}
				wait(self->backupAgent.submitBackup(cx,
				                                    self->backupDir,
				                                    {},
				                                    0,
				                                    1e8,
				                                    self->tag.toString(),
				                                    backupRanges,
				                                    true,
				                                    StopWhenDone::False,
				                                    UsePartitionedLog::False,
				                                    IncrementalBackupOnly::True,
				                                    self->encryptionKeyFileName,
				                                    blobManifestUrl));
			} catch (Error& e) {
				TraceEvent("IBackupSubmitError").error(e);
				if (e.code() != error_code_backup_duplicate) {
					throw;
				}
			}
			TraceEvent("IBackupSubmitSuccess").log();
		}
		if (self->restoreOnly) {
			if (self->clearBackupAgentKeys) {
				state Transaction clearTr(cx);
				// Clear Relevant System Keys
				loop {
					try {
						clearTr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						clearTr.setOption(FDBTransactionOptions::LOCK_AWARE);
						clearTr.clear(fileBackupPrefixRange);
						wait(clearTr.commit());
						break;
					} catch (Error& e) {
						wait(clearTr.onError(e));
					}
				}
			}
			state Reference<IBackupContainer> backupContainer;
			state UID backupUID;
			state Version beginVersion = invalidVersion;
			wait(success(self->backupAgent.waitBackup(
			    cx, self->tag.toString(), StopWhenDone::False, &backupContainer, &backupUID)));

			state Optional<std::string> restoreEncryptionKeyFileName;
			if (self->encryptionKeyFileName.present() && fileExists(self->encryptionKeyFileName.get())) {
				restoreEncryptionKeyFileName = self->encryptionKeyFileName.get();
			}

			if (self->checkBeginVersion) {
				TraceEvent("IBackupReadSystemKeys").log();
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				loop {
					try {
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);
						state Optional<Value> writeFlag = wait(tr->get(writeRecoveryKey));
						state Optional<Value> versionValue = wait(tr->get(snapshotEndVersionKey));
						TraceEvent("IBackupCheckSpecialKeys")
						    .detail("WriteRecoveryValue", writeFlag.present() ? writeFlag.get().toString() : "N/A")
						    .detail("EndVersionValue", versionValue.present() ? versionValue.get().toString() : "N/A");
						if (!versionValue.present()) {
							TraceEvent("IBackupCheckSpecialKeysFailure").log();
							// Snapshot failed to write to special keys, possibly due to snapshot itself failing
							throw key_not_found();
						}
						beginVersion = BinaryReader::fromStringRef<Version>(versionValue.get(), Unversioned());
						TraceEvent("IBackupCheckBeginVersion").detail("Version", beginVersion);
						break;
					} catch (Error& e) {
						TraceEvent("IBackupReadSystemKeysError").error(e);
						if (e.code() == error_code_key_not_found) {
							throw;
						}
						wait(tr->onError(e));
					}
				}
			}
			TraceEvent("IBackupStartListContainersAttempt").log();
			state std::vector<std::string> containers =
			    wait(IBackupContainer::listContainers(self->backupDir.toString(), {}));
			TraceEvent("IBackupStartListContainersSuccess")
			    .detail("Size", containers.size())
			    .detail("First", containers.front());
			state Key backupURL = Key(containers.front());

			state Standalone<VectorRef<KeyRangeRef>> restoreRange;
			state Standalone<VectorRef<KeyRangeRef>> systemRestoreRange;
			for (auto r : backupRanges) {
				if (config.tenantMode != TenantMode::REQUIRED || !r.intersects(getSystemBackupRanges())) {
					restoreRange.push_back_deep(restoreRange.arena(), r);
				} else {
					KeyRangeRef normalKeyRange = r & normalKeys;
					KeyRangeRef systemKeyRange = r & systemKeys;
					if (!normalKeyRange.empty()) {
						restoreRange.push_back_deep(restoreRange.arena(), normalKeyRange);
					}
					if (!systemKeyRange.empty()) {
						systemRestoreRange.push_back_deep(systemRestoreRange.arena(), systemKeyRange);
					}
				}
			}
			if (!systemRestoreRange.empty()) {
				TraceEvent("IBackupSystemRestoreAttempt").detail("BeginVersion", beginVersion);
				wait(success(self->backupAgent.restore(cx,
				                                       cx,
				                                       "system_restore"_sr,
				                                       backupURL,
				                                       {},
				                                       systemRestoreRange,
				                                       WaitForComplete::True,
				                                       invalidVersion,
				                                       Verbose::True,
				                                       Key(),
				                                       Key(),
				                                       LockDB::True,
				                                       UnlockDB::True,
				                                       OnlyApplyMutationLogs::True,
				                                       InconsistentSnapshotOnly::False,
				                                       beginVersion,
				                                       restoreEncryptionKeyFileName)));
			}
			TraceEvent("IBackupRestoreAttempt").detail("BeginVersion", beginVersion);
			wait(success(self->backupAgent.restore(cx,
			                                       cx,
			                                       Key(self->tag.toString()),
			                                       backupURL,
			                                       {},
			                                       restoreRange,
			                                       WaitForComplete::True,
			                                       invalidVersion,
			                                       Verbose::True,
			                                       Key(),
			                                       Key(),
			                                       LockDB::True,
			                                       UnlockDB::True,
			                                       OnlyApplyMutationLogs::True,
			                                       InconsistentSnapshotOnly::False,
			                                       beginVersion,
			                                       restoreEncryptionKeyFileName)));
			TraceEvent("IBackupRestoreSuccess").log();
		}
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<IncrementalBackupWorkload> IncrementalBackupWorkloadFactory;
