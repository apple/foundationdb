/*
 * AtomicRestore.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbserver/workloads/workloads.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

//A workload which test the correctness of backup and restore process
struct AtomicRestoreWorkload : TestWorkload {
	double startAfter, restoreAfter;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;

	AtomicRestoreWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {

		startAfter = getOption(options, LiteralStringRef("startAfter"), 10.0);
		restoreAfter = getOption(options, LiteralStringRef("restoreAfter"), 20.0);
		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
	}

	virtual std::string description() {
		return "AtomicRestore";
	}

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	ACTOR static Future<Void> _start(Database cx, AtomicRestoreWorkload* self) {
		state FileBackupAgent backupAgent;

		wait( delay(self->startAfter * g_random->random01()) );
		TraceEvent("AtomicRestore_Start");

		state std::string backupContainer = "file://simfdb/backups/";
		try {
			wait(backupAgent.submitBackup(cx, StringRef(backupContainer), g_random->randomInt(0, 100), BackupAgentBase::getDefaultTagName(), self->backupRanges, false));
		}
		catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("AtomicRestore_Wait");
		int _ = wait( backupAgent.waitBackup(cx, BackupAgentBase::getDefaultTagName(), false) );
		TraceEvent("AtomicRestore_BackupStart");
		wait( delay(self->restoreAfter * g_random->random01()) );
		TraceEvent("AtomicRestore_RestoreStart");

		loop {
			std::vector<Future<Version>> restores;

			for (auto &range : self->backupRanges) {
				restores.push_back(backupAgent.atomicRestore(cx, BackupAgentBase::getDefaultTag(), range, StringRef(), StringRef()));
			}
			try {
				wait(waitForAll(restores));
				break;
			}
			catch (Error& e) {
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}
			wait( delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
		}
		
		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.backupAgents == ISimulator::BackupToFile) {
			g_simulator.backupAgents = ISimulator::NoBackupAgents;
		}

		TraceEvent("AtomicRestore_Done");
		return Void();
	}
};

WorkloadFactory<AtomicRestoreWorkload> AtomicRestoreWorkloadFactory("AtomicRestore");
