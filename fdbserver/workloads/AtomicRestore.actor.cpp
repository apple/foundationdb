/*
 * AtomicRestore.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process
struct AtomicRestoreWorkload : TestWorkload {
	static constexpr auto NAME = "AtomicRestore";
	double startAfter, restoreAfter;
	bool fastRestore; // true: use fast restore, false: use old style restore
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	UsePartitionedLog usePartitionedLogs{ false };
	Key addPrefix, removePrefix; // Original key will be first applied removePrefix and then applied addPrefix
	// CAVEAT: When removePrefix is used, we must ensure every key in backup have the removePrefix
	Optional<std::string> encryptionKeyFileName;

	AtomicRestoreWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {

		startAfter = getOption(options, "startAfter"_sr, 10.0);
		restoreAfter = getOption(options, "restoreAfter"_sr, 20.0);
		fastRestore = getOption(options, "fastRestore"_sr, false);
		if (!fastRestore) {
			addDefaultBackupRanges(backupRanges);
		} else {
			// Fast restore doesn't support multiple ranges yet
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		}
		usePartitionedLogs.set(
		    getOption(options, "usePartitionedLogs"_sr, deterministicRandom()->random01() < 0.5 ? true : false));

		addPrefix = getOption(options, "addPrefix"_sr, ""_sr);
		removePrefix = getOption(options, "removePrefix"_sr, ""_sr);

		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}

		// Correctness is not clean for addPrefix feature yet. Uncomment below to enable the test
		// Generate addPrefix
		// if (addPrefix.size() == 0 && removePrefix.size() == 0) {
		// 	if (deterministicRandom()->random01() < 0.5) { // Generate random addPrefix
		// 		int len = deterministicRandom()->randomInt(1, 100);
		// 		std::string randomStr = deterministicRandom()->randomAlphaNumeric(len);
		// 		TraceEvent("AtomicRestoreWorkload")
		// 		    .detail("GenerateAddPrefix", randomStr)
		// 		    .detail("Length", len)
		// 		    .detail("StrLen", randomStr.size());
		// 		addPrefix = Key(randomStr);
		// 	}
		// }
		TraceEvent("AtomicRestoreWorkload").detail("AddPrefix", addPrefix).detail("RemovePrefix", removePrefix);
		ASSERT(addPrefix.size() == 0 && removePrefix.size() == 0);
		// Do not support removePrefix right now because we must ensure all backup keys have the removePrefix
		// otherwise, test will fail because fast restore will simply add the removePrefix to every key in the end.
		ASSERT(removePrefix.size() == 0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	bool hasPrefix() const { return addPrefix != ""_sr || removePrefix != ""_sr; }

	ACTOR static Future<Void> _start(Database cx, AtomicRestoreWorkload* self) {
		state FileBackupAgent backupAgent;

		wait(delay(self->startAfter * deterministicRandom()->random01()));
		TraceEvent("AtomicRestore_Start").detail("UsePartitionedLog", self->usePartitionedLogs);

		if (self->encryptionKeyFileName.present()) {
			wait(BackupContainerFileSystem::createTestEncryptionKeyFile(self->encryptionKeyFileName.get()));
		}

		state std::string backupContainer = "file://simfdb/backups/";
		try {
			wait(backupAgent.submitBackup(cx,
			                              StringRef(backupContainer),
			                              {},
			                              deterministicRandom()->randomInt(0, 60),
			                              deterministicRandom()->randomInt(0, 100),
			                              BackupAgentBase::getDefaultTagName(),
			                              self->backupRanges,
			                              true,
			                              StopWhenDone::False,
			                              self->usePartitionedLogs,
			                              IncrementalBackupOnly::False,
			                              self->encryptionKeyFileName));
		} catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("AtomicRestore_Wait").log();
		wait(success(backupAgent.waitBackup(cx, BackupAgentBase::getDefaultTagName(), StopWhenDone::False)));
		TraceEvent("AtomicRestore_BackupStart").log();
		wait(delay(self->restoreAfter * deterministicRandom()->random01()));
		TraceEvent("AtomicRestore_RestoreStart").log();

		if (self->fastRestore) { // New fast parallel restore
			TraceEvent(SevInfo, "AtomicParallelRestore").log();
			wait(backupAgent.atomicParallelRestore(
			    cx, BackupAgentBase::getDefaultTag(), self->backupRanges, self->addPrefix, self->removePrefix));
		} else { // Old style restore
			loop {
				try {
					wait(success(backupAgent.atomicRestore(
					    cx, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef())));
					break;
				} catch (Error& e) {
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			}
		}

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator->backupAgents == ISimulator::BackupAgentType::BackupToFile) {
			g_simulator->backupAgents = ISimulator::BackupAgentType::NoBackupAgents;
		}

		TraceEvent("AtomicRestore_Done").log();
		return Void();
	}
};

WorkloadFactory<AtomicRestoreWorkload> AtomicRestoreWorkloadFactory;
