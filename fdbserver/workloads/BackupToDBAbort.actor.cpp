/*
 * BackupToDBAbort.actor.cpp
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BackupToDBAbort : TestWorkload {
	double abortDelay;
	Database extraDB;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	UID lockid;

	explicit BackupToDBAbort(const WorkloadContext& wcx) : TestWorkload(wcx) {
		abortDelay = getOption(options, LiteralStringRef("abortDelay"), 50.0);

		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
		extraDB = Database::createDatabase(extraFile, -1);

		lockid = UID(0xbeeffeed, 0xdecaf00d);
	}

	std::string description() const override { return "BackupToDBAbort"; }

	Future<Void> setup(const Database& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(this, cx);
	}

	ACTOR static Future<Void> _setup(BackupToDBAbort* self, Database cx) {
		state DatabaseBackupAgent backupAgent(cx);
		try {
			TraceEvent("BDBA_Submit1").log();
			wait(backupAgent.submitBackup(self->extraDB,
			                              BackupAgentBase::getDefaultTag(),
			                              self->backupRanges,
			                              StopWhenDone::False,
			                              StringRef(),
			                              StringRef(),
			                              LockDB::True));
			TraceEvent("BDBA_Submit2").log();
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate)
				throw;
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	ACTOR static Future<Void> _start(BackupToDBAbort* self, Database cx) {
		state DatabaseBackupAgent backupAgent(cx);

		TraceEvent("BDBA_Start").detail("Delay", self->abortDelay);
		wait(delay(self->abortDelay));
		TraceEvent("BDBA_Wait").log();
		wait(success(backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), StopWhenDone::False)));
		TraceEvent("BDBA_Lock").log();
		wait(lockDatabase(cx, self->lockid));
		TraceEvent("BDBA_Abort").log();
		wait(backupAgent.abortBackup(self->extraDB, BackupAgentBase::getDefaultTag()));
		TraceEvent("BDBA_Unlock").log();
		wait(backupAgent.unlockBackup(self->extraDB, BackupAgentBase::getDefaultTag()));
		TraceEvent("BDBA_End").log();

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.drAgents == ISimulator::BackupAgentType::BackupToDB) {
			g_simulator.drAgents = ISimulator::BackupAgentType::NoBackupAgents;
		}

		return Void();
	}

	ACTOR static Future<bool> _check(BackupToDBAbort* self, Database cx) {
		TraceEvent("BDBA_UnlockPrimary").log();
		// Too much of the tester framework expects the primary database to be unlocked, so we unlock it
		// once all of the workloads have finished.
		wait(unlockDatabase(cx, self->lockid));
		return true;
	}

	Future<bool> check(const Database& cx) override { return _check(this, cx); }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

REGISTER_WORKLOAD(BackupToDBAbort);
