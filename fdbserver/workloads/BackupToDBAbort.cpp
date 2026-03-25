/*
 * BackupToDBAbort.cpp
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

#include "fdbclient/BackupAgent.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/ApiVersion.h"

struct BackupToDBAbort : TestWorkload {
	static constexpr auto NAME = "BackupToDBAbort";
	double abortDelay;
	Database extraDB;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	UID lockid;

	explicit BackupToDBAbort(const WorkloadContext& wcx) : TestWorkload(wcx) {
		abortDelay = getOption(options, "abortDelay"_sr, 50.0);

		addDefaultBackupRanges(backupRanges);

		ASSERT(g_simulator->extraDatabases.size() == 1);
		extraDB = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0]);

		lockid = UID(0xbeeffeed, 0xdecaf00d);
	}

	Future<Void> setup(const Database& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(cx);
	}

	Future<Void> _setup(Database cx) {
		DatabaseBackupAgent backupAgent(cx);
		try {
			TraceEvent("BDBA_Submit1").log();
			co_await backupAgent.submitBackup(extraDB,
			                                  BackupAgentBase::getDefaultTag(),
			                                  backupRanges,
			                                  StopWhenDone::False,
			                                  StringRef(),
			                                  StringRef(),
			                                  LockDB::True);
			TraceEvent("BDBA_Submit2").log();
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate)
				throw;
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx);
	}

	Future<Void> _start(Database cx) {
		DatabaseBackupAgent backupAgent(cx);

		TraceEvent("BDBA_Start").detail("Delay", abortDelay);
		co_await delay(abortDelay);
		TraceEvent("BDBA_Wait").log();
		co_await backupAgent.waitBackup(extraDB, BackupAgentBase::getDefaultTag(), StopWhenDone::False);
		TraceEvent("BDBA_Lock").log();
		co_await lockDatabase(cx, lockid);
		TraceEvent("BDBA_Abort").log();
		co_await backupAgent.abortBackup(extraDB, BackupAgentBase::getDefaultTag());
		TraceEvent("BDBA_Unlock").log();
		co_await backupAgent.unlockBackup(extraDB, BackupAgentBase::getDefaultTag());
		TraceEvent("BDBA_End").log();

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator->drAgents == ISimulator::BackupAgentType::BackupToDB) {
			g_simulator->drAgents = ISimulator::BackupAgentType::NoBackupAgents;
		}
	}

	Future<bool> check(const Database& cx) override {
		TraceEvent("BDBA_UnlockPrimary").log();
		// Too much of the tester framework expects the primary database to be unlocked, so we unlock it
		// once all of the workloads have finished.
		co_await unlockDatabase(cx, lockid);
		co_return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

REGISTER_WORKLOAD(BackupToDBAbort);
