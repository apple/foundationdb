/*
 * RestoreBackup.cpp
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
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"

// TODO: explain the purpose of this workload and how it different from the
// 20+ (literally) other backup/restore workloads.

struct RestoreBackupWorkload : TestWorkload {

	FileBackupAgent backupAgent;
	Reference<IBackupContainer> backupContainer;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	StopWhenDone stopWhenDone{ false };
	Optional<std::string> encryptionKeyFileName;
	// The extra describeBackup() call is skipped in simulation so existing seeds are unaffected.
	bool reportBackupMetrics;
	std::vector<PerfMetric> backupMetrics;
	Optional<double> restoreDuration;
	Optional<double> snapshotMB;

	explicit RestoreBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, false));
		reportBackupMetrics = getOption(options, "reportBackupMetrics"_sr, !g_network->isSimulated());

		std::string keyFileName = "simfdb/" + getTestEncryptionFileName();
		// Only set encryptionKeyFileName if the encryption key file exists during backup.
		if (fileExists(keyFileName)) {
			encryptionKeyFileName = keyFileName;
		}
	}

	static constexpr auto NAME = "RestoreBackup";

	Future<Void> waitOnBackup(Database cx) {
		Version waitForVersion{ 0 };
		UID backupUID;
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Version v = co_await tr.getReadVersion();
				waitForVersion = v;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		EBackupState backupState =
		    co_await backupAgent.waitBackup(cx, tag.toString(), stopWhenDone, &backupContainer, &backupUID);
		if (backupState == EBackupState::STATE_COMPLETED) {
			co_return;
		} else if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
			ASSERT(!stopWhenDone);
			while (true) {
				BackupDescription desc = co_await backupContainer->describeBackup(true);
				TraceEvent("BackupVersionGate")
				    .detail("MaxLogEndVersion", desc.maxLogEnd.present() ? desc.maxLogEnd.get() : invalidVersion)
				    .detail("ContiguousLogEndVersion",
				            desc.contiguousLogEnd.present() ? desc.contiguousLogEnd.get() : invalidVersion)
				    .detail("TargetVersion", waitForVersion);
				if (desc.contiguousLogEnd.present() && desc.contiguousLogEnd.get() >= waitForVersion) {
					try {
						TraceEvent("DiscontinuingBackup").log();
						co_await backupAgent.discontinueBackup(cx, tag);
					} catch (Error& e) {
						TraceEvent("ErrorDiscontinuingBackup").error(e);
						if (e.code() != error_code_backup_unneeded) {
							throw;
						}
					}
					co_return;
				}
				co_await delay(5.0);
			}
		} else {
			TraceEvent(SevError, "BadBackupState").detail("BackupState", BackupAgentBase::getStateText(backupState));
			ASSERT(false);
			co_return;
		}
	}

	static double versionsToSeconds(Version v) { return double(v) / SERVER_KNOBS->VERSIONS_PER_SECOND; }

	// Records metrics about the backup being restored. The snapshot is the restorable one with the greatest
	// endVersion, which is the snapshot a restore to the latest restorable version uses.
	Future<Void> describeBackupMetrics() {
		BackupDescription desc = co_await backupContainer->describeBackup();
		const KeyspaceSnapshotFile* snapshot = nullptr;
		for (const auto& s : desc.snapshots) {
			if (s.restorable.orDefault(false) && (!snapshot || s.endVersion > snapshot->endVersion)) {
				snapshot = &s;
			}
		}
		if (snapshot) {
			snapshotMB = snapshot->totalSize / 1e6;
			backupMetrics.emplace_back("Backup snapshot duration (s)",
			                           versionsToSeconds(snapshot->endVersion - snapshot->beginVersion),
			                           Averaged::False);
			backupMetrics.emplace_back("Backup snapshot size (MB)", snapshotMB.get(), Averaged::False);
		}
		if (desc.minLogBegin.present() && desc.maxLogEnd.present()) {
			backupMetrics.emplace_back("Backup log coverage (s)",
			                           versionsToSeconds(desc.maxLogEnd.get() - desc.minLogBegin.get()),
			                           Averaged::False);
		}
		if (desc.minLogBegin.present() && desc.contiguousLogEnd.present()) {
			backupMetrics.emplace_back("Backup contiguous log coverage (s)",
			                           versionsToSeconds(desc.contiguousLogEnd.get() - desc.minLogBegin.get()),
			                           Averaged::False);
		}
		if (desc.minRestorableVersion.present() && desc.maxRestorableVersion.present()) {
			backupMetrics.emplace_back(
			    "Backup restorable window (s)",
			    versionsToSeconds(desc.maxRestorableVersion.get() - desc.minRestorableVersion.get()),
			    Averaged::False);
		}
	}

	static Future<Void> clearDatabase(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.clear(normalKeys);
				for (auto& r : getSystemBackupRanges()) {
					tr.clear(r);
				}
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> _start(Database cx) {
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		co_await delay(delayFor);
		co_await waitOnBackup(cx);
		if (reportBackupMetrics) {
			try {
				co_await describeBackupMetrics();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				TraceEvent(SevWarn, "RestoreBackupDescribeFailed").error(e);
			}
		}
		co_await clearDatabase(cx);

		double restoreStart = now();
		co_await backupAgent.restore(cx,
		                             cx,
		                             tag,
		                             Key(backupContainer->getURL()),
		                             backupContainer->getProxy(),
		                             WaitForComplete::True,
		                             ::invalidVersion,
		                             Verbose::True,
		                             KeyRange(),
		                             Key(),
		                             Key(),
		                             LockDB::True,
		                             OnlyApplyMutationLogs::False,
		                             InconsistentSnapshotOnly::False,
		                             ::invalidVersion,
		                             encryptionKeyFileName);
		restoreDuration = now() - restoreStart;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.insert(m.end(), backupMetrics.begin(), backupMetrics.end());
		if (!restoreDuration.present()) {
			return;
		}
		m.emplace_back("Restore duration (s)", restoreDuration.get(), Averaged::False);
		// Mutation log bytes are not exposed by BackupDescription, so this is snapshot-only.
		if (snapshotMB.present() && restoreDuration.get() > 0) {
			m.emplace_back(
			    "Restore snapshot throughput (MB/s)", snapshotMB.get() / restoreDuration.get(), Averaged::False);
		}
	}
};

WorkloadFactory<RestoreBackupWorkload> RestoreBackupWorkloadFactory;
