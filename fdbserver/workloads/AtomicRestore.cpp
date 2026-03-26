/*
 * AtomicRestore.cpp
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

#include "fdbclient/ManagementAPI.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/restoreworker/RestoreCommon.h"
#include "fdbserver/tester/workloads.actor.h"
#include "BulkSetup.h"

// Workload to test atomicRestore().

struct AtomicRestoreWorkload : TestWorkload {
	static constexpr auto NAME = "AtomicRestore";
	double startAfter, restoreAfter;
	bool fastRestore; // true: use fast restore, false: use old style restore
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	UsePartitionedLog usePartitionedLogs{ false };
	Key addPrefix, removePrefix; // Original key will be first applied removePrefix and then applied addPrefix
	// CAVEAT: When removePrefix is used, we must ensure every key in backup have the removePrefix

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
		ASSERT(addPrefix.empty() && removePrefix.empty());
		// Do not support removePrefix right now because we must ensure all backup keys have the removePrefix
		// otherwise, test will fail because fast restore will simply add the removePrefix to every key in the end.
		ASSERT(removePrefix.empty());
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	bool hasPrefix() const { return !addPrefix.empty() || !removePrefix.empty(); }

	Future<Void> _start(Database cx) {
		FileBackupAgent backupAgent;

		co_await delay(startAfter * deterministicRandom()->random01());
		TraceEvent("AtomicRestore_Start").detail("UsePartitionedLog", usePartitionedLogs);

		std::string backupContainer = "file://simfdb/backups/";
		try {
			co_await backupAgent.submitBackup(cx,
			                                  StringRef(backupContainer),
			                                  {},
			                                  deterministicRandom()->randomInt(0, 60),
			                                  deterministicRandom()->randomInt(0, 100),
			                                  BackupAgentBase::getDefaultTagName(),
			                                  backupRanges,
			                                  StopWhenDone::False,
			                                  usePartitionedLogs,
			                                  IncrementalBackupOnly::False,
			                                  {});
		} catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("AtomicRestore_Wait").log();
		co_await backupAgent.waitBackup(cx, BackupAgentBase::getDefaultTagName(), StopWhenDone::False);
		TraceEvent("AtomicRestore_BackupStart").log();
		co_await delay(restoreAfter * deterministicRandom()->random01());
		TraceEvent("AtomicRestore_RestoreStart").log();

		if (fastRestore) { // New fast parallel restore
			TraceEvent(SevInfo, "AtomicParallelRestore").log();
			co_await backupAgent.atomicParallelRestore(
			    cx, BackupAgentBase::getDefaultTag(), backupRanges, addPrefix, removePrefix);
		} else { // Old style restore
			while (true) {
				try {
					co_await backupAgent.atomicRestore(
					    cx, BackupAgentBase::getDefaultTag(), backupRanges, StringRef(), StringRef());
					break;
				} catch (Error& e) {
					if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
						throw;
				}
				co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			}
		}

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator->backupAgents == ISimulator::BackupAgentType::BackupToFile) {
			g_simulator->backupAgents = ISimulator::BackupAgentType::NoBackupAgents;
		}

		TraceEvent("AtomicRestore_Done").log();
	}
};

WorkloadFactory<AtomicRestoreWorkload> AtomicRestoreWorkloadFactory;
