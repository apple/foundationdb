/*
 * SubmitRangePartitionedBackup.cpp
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
#include "fdbclient/BackupContainer.h"
#include "fdbserver/tester/workloads.h"

// Submits a range-partitioned (Backup v3) backup while another workload generates writes.
// Mirrors SubmitBackup.cpp but sets MutationLogType::RANGE_PARTITIONED_LOG.
struct SubmitRangePartitionedBackupWorkload : TestWorkload {
	static constexpr auto NAME = "SubmitRangePartitionedBackup";

	FileBackupAgent backupAgent;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	int initSnapshotInterval;
	int snapshotInterval;
	StopWhenDone stopWhenDone{ false };

	explicit SubmitRangePartitionedBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		initSnapshotInterval = getOption(options, "initSnapshotInterval"_sr, 0);
		snapshotInterval = getOption(options, "snapshotInterval"_sr, 1e8);
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, false));
	}

	Future<Void> _start(Database cx) {
		co_await delay(delayFor);

		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		addDefaultBackupRanges(backupRanges);

		try {
			co_await backupAgent.submitBackup(cx,
			                                  backupDir,
			                                  {},
			                                  initSnapshotInterval,
			                                  snapshotInterval,
			                                  tag.toString(),
			                                  backupRanges,
			                                  stopWhenDone,
			                                  MutationLogType::RANGE_PARTITIONED_LOG,
			                                  IncrementalBackupOnly::False,
			                                  /*encryptionKeyFileName=*/{},
			                                  /*encryptionKeyBlockSize=*/0,
			                                  /*snapshotMode=*/0);
		} catch (Error& e) {
			TraceEvent("SubmitRangePartitionedBackupError").error(e);
			if (e.code() != error_code_backup_duplicate) {
				throw;
			}
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SubmitRangePartitionedBackupWorkload> SubmitRangePartitionedBackupWorkloadFactory;
