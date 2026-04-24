/*
 * IncrementalBackup.cpp
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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "flow/Arena.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/serialize.h"

// TODO: explain the purpose of this workload and how it different from the
// 20+ (literally) other backup/restore workloads.

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

	explicit IncrementalBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
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

		if (restoreOnly) {
			// During restore, the encryption key file depends on whether the backup was encrypted or not.
			std::string temp_encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
			if (fileExists(temp_encryptionKeyFileName)) {
				encryptionKeyFileName = temp_encryptionKeyFileName;
			}
		} else if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId) {
			return Void();
		}
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId) {
			return true;
		}
		return _check(cx);
	}

	Future<bool> _check(Database cx) {
		if (waitForBackup) {
			// Undergoing recovery with the snapshot system keys set will pause the backup agent
			// Pre-emptively unpause any backup agents before attempting to wait to avoid getting stuck
			co_await backupAgent.changePause(cx, false);
			Reference<IBackupContainer> backupContainer;
			UID backupUID;
			Version v{ 0 };
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					v = co_await tr.getReadVersion();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
			while (true) {
				// Wait for backup container to be created and avoid race condition
				TraceEvent("IBackupWaitContainer").log();
				co_await backupAgent.waitBackup(cx, tag.toString(), StopWhenDone::False, &backupContainer, &backupUID);

				Optional<std::string> restoreEncryptionKeyFileName;
				if (encryptionKeyFileName.present() && fileExists(encryptionKeyFileName.get())) {
					restoreEncryptionKeyFileName = encryptionKeyFileName.get();
				}

				if (!backupContainer.isValid()) {
					TraceEvent("IBackupCheckListContainersAttempt").log();
					std::vector<std::string> containers =
					    co_await IBackupContainer::listContainers(backupDir.toString(), {});
					TraceEvent("IBackupCheckListContainersSuccess")
					    .detail("Size", containers.size())
					    .detail("First", containers.front());
					if (!containers.empty()) {
						backupContainer =
						    IBackupContainer::openContainer(containers.front(), {}, restoreEncryptionKeyFileName);
					}
				}
				bool e = co_await backupContainer->exists();
				if (e)
					break;
				co_await delay(5.0);
			}
			int tries = 0;
			while (true) {
				tries++;
				BackupDescription desc = co_await backupContainer->describeBackup(true);
				TraceEvent("IBackupVersionGate")
				    .detail("MaxLogEndVersion", desc.maxLogEnd.present() ? desc.maxLogEnd.get() : invalidVersion)
				    .detail("ContiguousLogEndVersion",
				            desc.contiguousLogEnd.present() ? desc.contiguousLogEnd.get() : invalidVersion)
				    .detail("TargetVersion", v);

				if (!desc.contiguousLogEnd.present()) {
					co_await delay(5.0);
					continue;
				}
				if (desc.contiguousLogEnd.get() >= v)
					break;
				if (waitRetries != -1 && tries > waitRetries)
					break;
				// Avoid spamming requests with a delay
				co_await delay(5.0);
			}
		}
		if (stopBackup) {
			try {
				TraceEvent("IBackupDiscontinueBackup").log();
				co_await backupAgent.discontinueBackup(cx, tag);
			} catch (Error& e) {
				TraceEvent("IBackupDiscontinueBackupException").error(e);
				if (e.code() != error_code_backup_unneeded) {
					throw;
				}
			}
		}
		co_return true;
	}

	Future<Void> _start(Database cx) {
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		addDefaultBackupRanges(backupRanges);

		if (submitOnly) {
			if (encryptionKeyFileName.present()) {
				co_await BackupContainerFileSystem::createTestEncryptionKeyFile(encryptionKeyFileName.get());
			}

			TraceEvent("IBackupSubmitAttempt").log();
			try {
				Optional<std::string> blobManifestUrl;
				if (!this->blobManifestUrl.empty()) {
					blobManifestUrl = this->blobManifestUrl.toString();
				}
				co_await backupAgent.submitBackup(cx,
				                                  backupDir,
				                                  {},
				                                  0,
				                                  1e8,
				                                  tag.toString(),
				                                  backupRanges,
				                                  StopWhenDone::False,
				                                  UsePartitionedLog::False,
				                                  IncrementalBackupOnly::True,
				                                  encryptionKeyFileName);
			} catch (Error& e) {
				TraceEvent("IBackupSubmitError").error(e);
				if (e.code() != error_code_backup_duplicate) {
					throw;
				}
			}
			TraceEvent("IBackupSubmitSuccess").log();
		}
		if (restoreOnly) {
			if (clearBackupAgentKeys) {
				Transaction clearTr(cx);
				// Clear Relevant System Keys
				while (true) {
					Error err;
					try {
						clearTr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						clearTr.setOption(FDBTransactionOptions::LOCK_AWARE);
						clearTr.clear(fileBackupPrefixRange);
						co_await clearTr.commit();
						break;
					} catch (Error& e) {
						err = e;
					}
					co_await clearTr.onError(err);
				}
			}
			Reference<IBackupContainer> backupContainer;
			UID backupUID;
			Version beginVersion = invalidVersion;
			co_await backupAgent.waitBackup(cx, tag.toString(), StopWhenDone::False, &backupContainer, &backupUID);

			Optional<std::string> restoreEncryptionKeyFileName;
			if (encryptionKeyFileName.present() && fileExists(encryptionKeyFileName.get())) {
				restoreEncryptionKeyFileName = encryptionKeyFileName.get();
			}

			if (checkBeginVersion) {
				TraceEvent("IBackupReadSystemKeys").log();
				Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				while (true) {
					Error err;
					try {
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);
						Optional<Value> writeFlag = co_await tr->get(writeRecoveryKey);
						Optional<Value> versionValue = co_await tr->get(snapshotEndVersionKey);
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
						err = e;
					}
					TraceEvent("IBackupReadSystemKeysError").error(err);
					if (err.code() == error_code_key_not_found) {
						throw err;
					}
					co_await tr->onError(err);
				}
			}
			TraceEvent("IBackupStartListContainersAttempt").log();
			std::vector<std::string> containers = co_await IBackupContainer::listContainers(backupDir.toString(), {});
			TraceEvent("IBackupStartListContainersSuccess")
			    .detail("Size", containers.size())
			    .detail("First", containers.front());
			Key backupURL = Key(containers.front());

			Standalone<VectorRef<KeyRangeRef>> restoreRange;
			Standalone<VectorRef<KeyRangeRef>> systemRestoreRange;
			for (auto r : backupRanges) {
				if (!r.intersects(getSystemBackupRanges())) {
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
				co_await backupAgent.restore(cx,
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
				                             restoreEncryptionKeyFileName);
			}
			TraceEvent("IBackupRestoreAttempt").detail("BeginVersion", beginVersion);
			co_await backupAgent.restore(cx,
			                             cx,
			                             Key(tag.toString()),
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
			                             restoreEncryptionKeyFileName);
			TraceEvent("IBackupRestoreSuccess").log();
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<IncrementalBackupWorkload> IncrementalBackupWorkloadFactory;
