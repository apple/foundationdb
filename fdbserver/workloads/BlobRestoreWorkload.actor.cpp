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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbserver/Knobs.h"
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
		restoreToVersion_ = getOption(options, "restoreToVersion"_sr, false);
		readBatchSize_ = getOption(options, "readBatchSize"_sr, 3000);
		if (!blobConn_.isValid() && SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			blobConn_ = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
		}
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
			// disable manifest backup and log truncation
			KnobValueRef knobFalse = KnobValueRef::create(bool{ false });
			IKnobCollection::getMutableGlobalKnobCollection().setKnob("blob_manifest_backup", knobFalse);

			wait(store(self->restoreTargetVersion_, getRestoreVersion(cx, self)));
			fmt::print("Restore target version {}\n", self->restoreTargetVersion_);

			// Only need to pass the version if we are trying to restore to a previous version
			Optional<Version> targetVersion;
			if (self->restoreToVersion_) {
				targetVersion = self->restoreTargetVersion_;
			}
			wait(store(result, self->extraDb_->blobRestore(normalKeys, targetVersion)));

			state std::vector<Future<Void>> futures;
			futures.push_back(self->runBackupAgent(self));
			futures.push_back(self->monitorProgress(cx, self));
			wait(waitForAny(futures));
		}
		return Void();
	}

	ACTOR static Future<Version> getRestoreVersion(Database cx, BlobRestoreWorkload* self) {
		state Version targetVersion;
		state std::string baseUrl = SERVER_KNOBS->BLOB_RESTORE_MLOGS_URL;
		state std::vector<std::string> containers = wait(IBackupContainer::listContainers(baseUrl, {}));
		if (containers.size() == 0) {
			fmt::print("missing mutation logs {}\n", baseUrl);
			throw restore_missing_data();
		}
		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(containers.front(), {}, {});
		BackupDescription desc = wait(bc->describeBackup(true));
		if (!desc.contiguousLogEnd.present()) {
			fmt::print("missing mutation logs {}\n", baseUrl);
			throw restore_missing_data();
		}
		targetVersion = desc.contiguousLogEnd.get() - 1;
		if (self->restoreToVersion_) {
			// restore to a previous version
			targetVersion -= deterministicRandom()->randomInt(1, 100000);
		}
		return targetVersion;
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
			auto controller = makeReference<BlobRestoreController>(self->extraDb_, normalKeys);
			Optional<BlobRestoreState> restoreState = wait(BlobRestoreController::getState(controller));
			if (restoreState.present()) {
				state BlobRestoreState s = restoreState.get();
				if (s.phase == BlobRestorePhase::DONE) {
					wait(verify(cx, self));
					return Void();
				}
				// TODO need to define more specific error handling
				if (s.phase == BlobRestorePhase::ERROR) {
					fmt::print("Unexpected restore error code = {}\n", s.error.get());
					return Void();
				}
			}
			wait(delay(5)); // delay to avoid busy loop
		}
	}

	ACTOR static Future<Standalone<VectorRef<KeyValueRef>>> readFromStorageServer(Database cx,
	                                                                              KeySelectorRef begin,
	                                                                              BlobRestoreWorkload* self) {
		state Standalone<VectorRef<KeyValueRef>> data;
		state Transaction tr(cx);
		state KeySelectorRef end = firstGreaterOrEqual(normalKeys.end);
		state Arena arena;

		loop {
			try {
				GetRangeLimits limits(self->readBatchSize_ - data.size());
				limits.minRows = 0;
				RangeResult result = wait(tr.getRange(begin, end, limits, Snapshot::True));
				for (auto& row : result) {
					data.push_back_deep(data.arena(), KeyValueRef(row.key, row.value));
				}
				if (!result.more) {
					break;
				}
				if (data.size() == self->readBatchSize_) {
					break;
				}
				if (result.readThrough.present()) {
					begin = firstGreaterOrEqual(KeyRef(arena, result.readThrough.get()));
				} else {
					begin = firstGreaterThan(KeyRef(arena, result.back().key));
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return data;
	}

	ACTOR static Future<Standalone<VectorRef<KeyValueRef>>> readFromBlob(Database cx,
	                                                                     KeySelectorRef begin,
	                                                                     Version readVersion,
	                                                                     BlobRestoreWorkload* self) {
		state Transaction tr(cx);
		state Standalone<VectorRef<KeyValueRef>> data;
		state KeyRangeRef keys(begin.getKey(), normalKeys.end);
		state int count = 0;

		loop {
			try {
				state Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
				    wait(tr.readBlobGranules(keys, 0, readVersion));
				state int i;
				for (i = 0; i < chunks.size(); ++i) {
					state RangeResult rows = wait(readBlobGranule(chunks[i], keys, 0, readVersion, self->blobConn_));
					for (auto& r : rows) {
						if (begin.isDefinitelyGreater(r.key)) {
							continue;
						}
						data.push_back_deep(data.arena(), r);
						count++;
						if (count >= self->readBatchSize_) {
							return data;
						}
					}
				}
				return data;
			} catch (Error& e) {
				fmt::print("read blob error {} \n", e.what());
				wait(tr.onError(e));
			}
		}
	}

	static bool compare(VectorRef<KeyValueRef> src, VectorRef<KeyValueRef> dest) {
		if (src.size() != dest.size()) {
			fmt::print("Size mismatch src {} dest {}\n", src.size(), dest.size());
			int i = 0;
			for (; i < src.size() && i < dest.size(); ++i) {
				if (src[i].key != dest[i].key || src[i].value != dest[i].value) {
					fmt::print("First mismatch row at {}\n", i);
					fmt::print("  src {} = {}\n", src[i].key.printable(), src[i].value.printable());
					fmt::print("  dest {} = {}\n", dest[i].key.printable(), dest[i].value.printable());
					break;
				}
			}

			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "Size Mismatch")
			    .detail("Src", dest.size())
			    .detail("Dest", src.size());
			return false;
		}

		for (int i = 0; i < src.size(); ++i) {
			if (src[i].key != dest[i].key) {
				fmt::print("Key mismatch at {} src {} dest {}\n", i, src[i].key.printable(), dest[i].key.printable());
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Key Mismatch")
				    .detail("Index", i)
				    .detail("SrcKey", src[i].key.printable())
				    .detail("DestKey", dest[i].key.printable());
				return false;
			}
			if (src[i].value != dest[i].value) {
				fmt::print("Value mismatch at {}\n", i);
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Value Mismatch")
				    .detail("Index", i)
				    .detail("Key", src[i].key.printable())
				    .detail("SrcValue", src[i].value.printable())
				    .detail("DestValue", dest[i].value.printable());
				return false;
			}
		}
		fmt::print("Restore src({} rows) and dest({} rows) are consistent\n", src.size(), dest.size());
		return true;
	}

	ACTOR static Future<Void> verify(Database cx, BlobRestoreWorkload* self) {
		state Arena arena;
		state KeySelectorRef srcBegin = firstGreaterOrEqual(normalKeys.begin);
		state KeySelectorRef destBegin = firstGreaterOrEqual(normalKeys.begin);
		// flush src db
		bool flush = wait(cx->flushBlobRange(normalKeys, false, self->restoreTargetVersion_));
		if (!flush) {
			fmt::print("Cannot flush to version {} \n", self->restoreTargetVersion_);
			throw internal_error();
		}

		loop {
			// restore src. data before restore
			state Standalone<VectorRef<KeyValueRef>> src =
			    wait(readFromBlob(cx, srcBegin, self->restoreTargetVersion_, self));
			//  restore dest. data after restore
			state Standalone<VectorRef<KeyValueRef>> dest =
			    wait(readFromStorageServer(self->extraDb_, destBegin, self));
			if (src.size() == 0 && dest.size() == 0) {
				break;
			}
			if (!compare(src, dest)) {
				break;
			}
			srcBegin = firstGreaterThan(KeyRef(arena, src.back().key));
			destBegin = firstGreaterThan(KeyRef(arena, dest.back().key));
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.emplace("Attrition"); }

private:
	Database extraDb_;
	bool setupBlob_;
	bool performRestore_;
	int readBatchSize_;
	bool restoreToVersion_;
	Version restoreTargetVersion_;
	Reference<BlobConnectionProvider> blobConn_;
};

WorkloadFactory<BlobRestoreWorkload> BlobRestoreWorkloadFactory;
