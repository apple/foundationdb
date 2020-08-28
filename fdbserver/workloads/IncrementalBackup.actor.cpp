/*
 * IncrementalBackup.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct IncrementalBackupWorkload : TestWorkload {

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	FileBackupAgent backupAgent;
	bool submitOnly;
	bool restoreOnly;

	IncrementalBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, LiteralStringRef("backupDir"), LiteralStringRef("file://simfdb/backups/"));
		tag = getOption(options, LiteralStringRef("tag"), LiteralStringRef("default"));
		submitOnly = getOption(options, LiteralStringRef("submitOnly"), false);
		restoreOnly = getOption(options, LiteralStringRef("restoreOnly"), false);
	}

	virtual std::string description() { return "IncrementalBackup"; }

	virtual Future<Void> setup(Database const& cx) { return Void(); }

	virtual Future<Void> start(Database const& cx) {
		if (clientId) {
			return Void();
		}
		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) { return true; }

	ACTOR static Future<Void> _start(Database cx, IncrementalBackupWorkload* self) {
        // Add a commit both before the submit and restore to test that incremental backup
        // can be performed on non-empty database
		if (self->submitOnly) {
			Standalone<VectorRef<KeyRangeRef>> backupRanges;
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
            TraceEvent("IBackupSubmitAttempt");
            try {
                wait(self->backupAgent.submitBackup(cx, self->backupDir, 1e8, self->tag.toString(), backupRanges, false,
                                                    false, true));
            } catch (Error& e) {
                if (e.code() != error_code_backup_duplicate) {
                    throw;
                }
            }
            TraceEvent("IBackupSubmitSuccess");
		}
		if (self->restoreOnly) {
			state Reference<IBackupContainer> backupContainer;
			state UID backupUID;
            TraceEvent("IBackupRestoreAttempt");
			wait(success(self->backupAgent.waitBackup(cx, self->tag.toString(), false, &backupContainer, &backupUID)));
			wait(success(self->backupAgent.restore(cx, cx, Key(self->tag.toString()), Key(backupContainer->getURL()),
			                                       true, -1, true, normalKeys, Key(), Key(), true, true)));
            TraceEvent("IBackupRestoreSuccess");
		}
		return Void();
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}
};

WorkloadFactory<IncrementalBackupWorkload> IncrementalBackupWorkloadFactory("IncrementalBackup");