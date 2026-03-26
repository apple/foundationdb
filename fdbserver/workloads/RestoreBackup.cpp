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
#include "fdbserver/tester/workloads.actor.h"
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

	RestoreBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, false));

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
		co_await clearDatabase(cx);

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
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreBackupWorkload> RestoreBackupWorkloadFactory;
