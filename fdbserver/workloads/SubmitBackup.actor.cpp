/*
 * SubmitBackup.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SubmitBackupWorkload : TestWorkload {
	static constexpr auto NAME = "SubmitBackup";

	FileBackupAgent backupAgent;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	int initSnapshotInterval;
	int snapshotInterval;
	StopWhenDone stopWhenDone{ false };
	IncrementalBackupOnly incremental{ false };
	Optional<std::string> encryptionKeyFileName;

	SubmitBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		initSnapshotInterval = getOption(options, "initSnapshotInterval"_sr, 0);
		snapshotInterval = getOption(options, "snapshotInterval"_sr, 1e8);
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, true));
		incremental.set(getOption(options, "incremental"_sr, false));

		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}
	}

	ACTOR static Future<Void> _start(SubmitBackupWorkload* self, Database cx) {
		wait(delay(self->delayFor));
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		addDefaultBackupRanges(backupRanges);

		if (self->encryptionKeyFileName.present()) {
			wait(BackupContainerFileSystem::createTestEncryptionKeyFile(self->encryptionKeyFileName.get()));
		}

		try {
			wait(self->backupAgent.submitBackup(cx,
			                                    self->backupDir,
			                                    {},
			                                    self->initSnapshotInterval,
			                                    self->snapshotInterval,
			                                    self->tag.toString(),
			                                    backupRanges,
			                                    true,
			                                    self->stopWhenDone,
			                                    UsePartitionedLog::False,
			                                    self->incremental,
			                                    self->encryptionKeyFileName));
		} catch (Error& e) {
			TraceEvent("BackupSubmitError").error(e);
			if (e.code() != error_code_backup_duplicate) {
				throw;
			}
		}
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SubmitBackupWorkload> SubmitBackupWorkloadFactory;
