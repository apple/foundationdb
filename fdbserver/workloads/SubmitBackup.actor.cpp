/*
 * SubmitBackup.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SubmitBackupWorkload final : TestWorkload {

	FileBackupAgent backupAgent;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	int initSnapshotInterval;
	int snapshotInterval;
	StopWhenDone stopWhenDone{ false };
	IncrementalBackupOnly incremental{ false };

	SubmitBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, LiteralStringRef("backupDir"), LiteralStringRef("file://simfdb/backups/"));
		tag = getOption(options, LiteralStringRef("tag"), LiteralStringRef("default"));
		delayFor = getOption(options, LiteralStringRef("delayFor"), 10.0);
		initSnapshotInterval = getOption(options, LiteralStringRef("initSnapshotInterval"), 0);
		snapshotInterval = getOption(options, LiteralStringRef("snapshotInterval"), 1e8);
		stopWhenDone.set(getOption(options, LiteralStringRef("stopWhenDone"), true));
		incremental.set(getOption(options, LiteralStringRef("incremental"), false));
	}

	static constexpr const char* DESCRIPTION = "SubmitBackup";

	ACTOR static Future<Void> _start(SubmitBackupWorkload* self, Database cx) {
		wait(delay(self->delayFor));
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		try {
			wait(self->backupAgent.submitBackup(cx,
			                                    self->backupDir,
			                                    {},
			                                    self->initSnapshotInterval,
			                                    self->snapshotInterval,
			                                    self->tag.toString(),
			                                    backupRanges,
			                                    self->stopWhenDone,
			                                    UsePartitionedLog::False,
			                                    self->incremental));
		} catch (Error& e) {
			TraceEvent("BackupSubmitError").error(e);
			if (e.code() != error_code_backup_duplicate) {
				throw;
			}
		}
		return Void();
	}

	std::string description() const override { return DESCRIPTION; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SubmitBackupWorkload> SubmitBackupWorkloadFactory(SubmitBackupWorkload::DESCRIPTION);
