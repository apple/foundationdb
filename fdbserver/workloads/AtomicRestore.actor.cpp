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

#include "flow/actorcompiler.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "workloads.h"
#include "BulkSetup.actor.h"

//A workload which test the correctness of backup and restore process
struct AtomicRestoreWorkload : TestWorkload {
	double startAfter, switch1After;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;

	AtomicRestoreWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {

		startAfter = getOption(options, LiteralStringRef("startAfter"), 10.0);
		switch1After = getOption(options, LiteralStringRef("switch1After"), 20.0);
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
		state Future<Void> switch1After = delay(self->switch1After);
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "atomicRestore");
		Void _ = wait( delay(self->startAfter) );
		TraceEvent("AR_Start");

		state std::string backupContainer = "file://simfdb/backups/";
		try {
			Void _ = wait(backupAgent.submitBackup(cx, StringRef(backupContainer), BackupAgentBase::getDefaultTagName(), self->backupRanges, false));
		}
		catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("AS_Wait1");
		int _ = wait( backupAgent.waitBackup(cx, BackupAgentBase::getDefaultTagName(), false) );
		TraceEvent("AS_Ready1");
		Void _ = wait( switch1After );
		TraceEvent("AS_Switch1");

		loop {
			std::vector<Future<Version>> restores;

			for (auto &range : self->backupRanges) {
				restores.push_back(backupAgent.atomicRestore(cx, BackupAgentBase::getDefaultTag(), range, StringRef(), StringRef()));
			}
			try {
				Void _ = wait(waitForAll(restores));
				break;
			}
			catch (Error& e) {
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}
			Void _ = wait( delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
		}
		
		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.backupAgents == ISimulator::BackupToFile) {
			g_simulator.backupAgents = ISimulator::NoBackupAgents;
		}

		TraceEvent("AS_Done");
		return Void();
	}
};

WorkloadFactory<AtomicRestoreWorkload> AtomicRestoreWorkloadFactory("AtomicRestore");
