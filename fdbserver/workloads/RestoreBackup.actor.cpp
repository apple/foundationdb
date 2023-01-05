/*
 * RestoreBackup.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreBackupWorkload : TestWorkload {

	FileBackupAgent backupAgent;
	Reference<IBackupContainer> backupContainer;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	StopWhenDone stopWhenDone{ false };

	RestoreBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, false));
	}

	static constexpr auto NAME = "RestoreBackup";

	ACTOR static Future<Void> waitOnBackup(RestoreBackupWorkload* self, Database cx) {
		state Version waitForVersion;
		state UID backupUID;
		state Transaction tr(cx);
		loop {
			try {
				Version v = wait(tr.getReadVersion());
				waitForVersion = v;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		EBackupState backupState = wait(self->backupAgent.waitBackup(
		    cx, self->tag.toString(), self->stopWhenDone, &self->backupContainer, &backupUID));
		if (backupState == EBackupState::STATE_COMPLETED) {
			return Void();
		} else if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
			ASSERT(!self->stopWhenDone);
			loop {
				BackupDescription desc = wait(self->backupContainer->describeBackup(true));
				TraceEvent("BackupVersionGate")
				    .detail("MaxLogEndVersion", desc.maxLogEnd.present() ? desc.maxLogEnd.get() : invalidVersion)
				    .detail("ContiguousLogEndVersion",
				            desc.contiguousLogEnd.present() ? desc.contiguousLogEnd.get() : invalidVersion)
				    .detail("TargetVersion", waitForVersion);
				if (desc.contiguousLogEnd.present() && desc.contiguousLogEnd.get() >= waitForVersion) {
					try {
						TraceEvent("DiscontinuingBackup").log();
						wait(self->backupAgent.discontinueBackup(cx, self->tag));
					} catch (Error& e) {
						TraceEvent("ErrorDiscontinuingBackup").error(e);
						if (e.code() != error_code_backup_unneeded) {
							throw;
						}
					}
					return Void();
				}
				wait(delay(5.0));
			}
		} else {
			TraceEvent(SevError, "BadBackupState").detail("BackupState", BackupAgentBase::getStateText(backupState));
			ASSERT(false);
			return Void();
		}
	}

	ACTOR static Future<Void> clearDatabase(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.clear(normalKeys);
				for (auto& r : getSystemBackupRanges()) {
					tr.clear(r);
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(RestoreBackupWorkload* self, Database cx) {
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		wait(delay(self->delayFor));
		wait(waitOnBackup(self, cx));
		wait(clearDatabase(cx));

		if (config.encryptionAtRestMode.isEncryptionEnabled()) {
			// restore system keys
			VectorRef<KeyRangeRef> systemBackupRanges = getSystemBackupRanges();
			state std::vector<Future<Version>> restores;
			for (int i = 0; i < systemBackupRanges.size(); i++) {
				restores.push_back((self->backupAgent.restore(cx,
				                                              cx,
				                                              "system_restore"_sr,
				                                              Key(self->backupContainer->getURL()),
				                                              self->backupContainer->getProxy(),
				                                              WaitForComplete::True,
				                                              ::invalidVersion,
				                                              Verbose::True,
				                                              systemBackupRanges[i])));
			}
			waitForAll(restores);
			// restore non-system keys
			wait(success(self->backupAgent.restore(cx,
			                                       cx,
			                                       self->tag,
			                                       Key(self->backupContainer->getURL()),
			                                       self->backupContainer->getProxy(),
			                                       WaitForComplete::True,
			                                       ::invalidVersion,
			                                       Verbose::True,
			                                       normalKeys)));
		} else {
			wait(success(self->backupAgent.restore(cx,
			                                       cx,
			                                       self->tag,
			                                       Key(self->backupContainer->getURL()),
			                                       self->backupContainer->getProxy(),
			                                       WaitForComplete::True,
			                                       ::invalidVersion,
			                                       Verbose::True)));
		}

		return Void();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreBackupWorkload> RestoreBackupWorkloadFactory;
