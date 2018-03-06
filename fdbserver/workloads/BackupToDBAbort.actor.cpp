/*
 * BackupToDBAbort.actor.cpp
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

#include "flow/actorcompiler.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "workloads.h"

struct BackupToDBAbort : TestWorkload {
	double abortDelay;
	Database extraDB;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	UID lockid;

	explicit BackupToDBAbort(const WorkloadContext& wcx)
	    : TestWorkload(wcx) {
		abortDelay = getOption(options, LiteralStringRef("abortDelay"), 50.0);

		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
		extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();

		lockid = UID(0xbeeffeed, 0xdecaf00d);
	}

	virtual std::string description() override {
		return "BackupToDBAbort";
	}

	virtual Future<Void> setup(const Database& cx) override {
		if (clientId != 0) return Void();
		return _setup(this, cx);
	}

	ACTOR static Future<Void> _setup(BackupToDBAbort* self, Database cx) {
		state DatabaseBackupAgent backupAgent(cx);
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "BackupToDBAbort");
		try {
			TraceEvent("BDBA_Submit1");
			Void _ = wait( backupAgent.submitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), self->backupRanges, false, StringRef(), StringRef(), true) );
			TraceEvent("BDBA_Submit2");
		} catch( Error &e ) {
			if( e.code() != error_code_backup_duplicate )
				throw;
		}
		return Void();
	}

	virtual Future<Void> start(Database const& cx) override {
		if (clientId != 0) return Void();
		return _start(this, cx);
	}

	ACTOR static Future<Void> _start(BackupToDBAbort* self, Database cx) {
		state DatabaseBackupAgent backupAgent(cx);
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "BackupToDBAbort");

		TraceEvent("BDBA_Start").detail("delay", self->abortDelay);
		Void _ = wait(delay(self->abortDelay));
		TraceEvent("BDBA_Wait");
		int _ = wait( backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), false) );
		TraceEvent("BDBA_Lock");
		Void _ = wait(lockDatabase(cx, self->lockid));
		TraceEvent("BDBA_Abort");
		Void _ = wait(backupAgent.abortBackup(self->extraDB, BackupAgentBase::getDefaultTag()));
		TraceEvent("BDBA_Unlock");
		Void _ = wait(backupAgent.unlockBackup(self->extraDB, BackupAgentBase::getDefaultTag()));
		TraceEvent("BDBA_End");

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.backupAgents == ISimulator::BackupToDB) {
			g_simulator.backupAgents = ISimulator::NoBackupAgents;
		}

		return Void();
	}

	ACTOR static Future<bool> _check(BackupToDBAbort* self, Database cx) {
		TraceEvent("BDBA_UnlockPrimary");
		// Too much of the tester framework expects the primary database to be unlocked, so we unlock it
		// once all of the workloads have finished.
		Void _ = wait(unlockDatabase(cx, self->lockid));
		return true;
	}

	virtual Future<bool> check(const Database& cx) override {
		return _check(this, cx);
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}
};

REGISTER_WORKLOAD(BackupToDBAbort);
