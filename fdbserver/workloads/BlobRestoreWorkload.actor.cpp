/*
 * BlobRestoreWorkload.actor.cpp
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

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// This worload provides building blocks to test blob restore. The following 2 functions are offered:
//   1) SetupBlob - blobbify key ranges so that we could backup fdb to a blob storage
//   2) PerformRestore - Start blob restore to the extra db instance and wait until it finishes
//
// A general flow to test blob restore:
//   1) start two db instances and blobbify normalKeys for the default db
//   2) submit mutation log only backup to the default db with IncrementalBackup
//   3) start cycle workload to write data to the default db
//   4) perform blob restore to the extra db
//   5) verify data in the extra db
//
// Please refer to BlobRestoreBasic.toml to see how to run a blob restore test with the help from IncrementalBackup
// and Cycle.
//
struct BlobRestoreWorkload : TestWorkload {
	static constexpr auto NAME = "BlobRestoreWorkload";
	BlobRestoreWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		ASSERT(g_simulator->extraDatabases.size() == 1); // extra db must be enabled
		extraDb_ = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0]);
		setupBlob_ = getOption(options, "setupBlob"_sr, false);
		performRestore_ = getOption(options, "performRestore"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	ACTOR static Future<Void> _start(Database cx, BlobRestoreWorkload* self) {
		state bool result = false;
		if (self->setupBlob_) {
			fmt::print("Blobbify normal range\n");
			wait(store(result, cx->blobbifyRange(normalKeys)));
		}

		if (self->performRestore_) {
			fmt::print("Perform blob restore\n");
			wait(store(result, self->extraDb_->blobRestore(normalKeys)));

			state std::vector<Future<Void>> futures;
			futures.push_back(self->runBackupAgent(self));
			futures.push_back(self->monitorProgress(cx, self));
			wait(waitForAny(futures));
		}
		return Void();
	}

	// Start backup agent on the extra db
	ACTOR Future<Void> runBackupAgent(BlobRestoreWorkload* self) {
		state FileBackupAgent backupAgent;
		state Future<Void> future = backupAgent.run(
		    self->extraDb_, 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT);
		wait(Future<Void>(Never()));
		throw internal_error();
	}

	// Monitor restore progress and copy data back to original db after successful restore
	ACTOR Future<Void> monitorProgress(Database cx, BlobRestoreWorkload* self) {
		loop {
			Optional<BlobRestoreStatus> status = wait(getRestoreStatus(self->extraDb_, normalKeys));
			if (status.present()) {
				state BlobRestoreStatus s = status.get();
				if (s.phase == BlobRestorePhase::DONE) {
					wait(copyToOriginalDb(cx, self));
					return Void();
				}
				// TODO need to define more specific error handling
				if (s.phase == BlobRestorePhase::ERROR) {
					fmt::print("Unexpected restore error code = {}\n", s.status);
					return Void();
				}
			}
			wait(delay(1));
		}
	}

	ACTOR static Future<Void> copyToOriginalDb(Database cx, BlobRestoreWorkload* self) {
		state RangeResult data;

		// Read data from restored db
		state Transaction tr1(self->extraDb_->clone());
		loop {
			try {
				RangeResult result = wait(tr1.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
				data = result;
				break;
			} catch (Error& e) {
				wait(tr1.onError(e));
			}
		}

		// Write back to original db for Cycle worker load to verify
		state Transaction tr2(cx);
		loop {
			try {
				tr2.clear(normalKeys);
				for (auto kv : data) {
					tr2.set(kv.key, kv.value);
				}
				wait(tr2.commit());
				fmt::print("Copied {} rows to origin db\n", data.size());
				return Void();
			} catch (Error& e) {
				wait(tr2.onError(e));
			}
		}
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.emplace("Attrition"); }

private:
	Database extraDb_;
	bool setupBlob_;
	bool performRestore_;
};

WorkloadFactory<BlobRestoreWorkload> BlobRestoreWorkloadFactory;
