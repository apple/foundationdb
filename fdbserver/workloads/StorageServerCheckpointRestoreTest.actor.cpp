/*
 *PhysicalShardMove.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include <cstdint>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
std::string printValue(const ErrorOr<Optional<Value>>& value) {
	if (value.isError()) {
		return value.getError().name();
	}
	return value.get().present() ? value.get().get().toString() : "Value Not Found.";
}
} // namespace

struct SSCheckpointRestoreWorkload : TestWorkload {
	static constexpr auto NAME = "SSCheckpointRestoreWorkload";
	const bool enabled;
	bool pass;

	SSCheckpointRestoreWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert("RandomMoveKeys");
		out.insert("Attrition");
	}

	ACTOR Future<Void> _start(SSCheckpointRestoreWorkload* self, Database cx) {
		state Key key = "TestKey"_sr;
		state Key endKey = "TestKey0"_sr;
		state Value oldValue = "TestValue"_sr;
		state KeyRange testRange = KeyRangeRef(key, endKey);
		state std::vector<CheckpointMetaData> records;

		TraceEvent("TestCheckpointRestoreBegin");
		int ignore = wait(setDDMode(cx, 0));
		state Version version = wait(self->writeAndVerify(self, cx, key, oldValue));

		TraceEvent("TestCreatingCheckpoint").detail("Range", testRange);
		// Create checkpoint.
		state Transaction tr(cx);
		state CheckpointFormat format = DataMoveRocksCF;
		state UID dataMoveId = deterministicRandom()->randomUniqueID();
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(createCheckpoint(&tr, { testRange }, format, dataMoveId));
				wait(tr.commit());
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestCheckpointCreated")
		    .detail("Range", testRange)
		    .detail("Version", version)
		    .detail("DataMoveID", dataMoveId);

		// Fetch checkpoint meta data.
		loop {
			records.clear();
			try {
				wait(store(records,
				           getCheckpointMetaData(cx, { testRange }, version, format, Optional<UID>(dataMoveId))));
				break;
			} catch (Error& e) {
				TraceEvent("TestFetchCheckpointMetadataError")
				    .errorUnsuppressed(e)
				    .detail("Range", testRange)
				    .detail("Version", version);

				// The checkpoint was just created, we don't expect this error.
				ASSERT(e.code() != error_code_checkpoint_not_found);
			}
		}

		TraceEvent("TestCheckpointFetched")
		    .detail("Range", testRange)
		    .detail("Version", version)
		    .detail("Checkpoints", describe(records));

		state std::string pwd = platform::getWorkingDirectory();
		state std::string folder = pwd + "/checkpoints";
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));

		// Fetch checkpoint.
		state std::vector<CheckpointMetaData> fetchedCheckpoints;
		state int i = 0;
		for (; i < records.size(); ++i) {
			loop {
				TraceEvent("TestFetchingCheckpoint").detail("Checkpoint", records[i].toString());
				try {
					state CheckpointMetaData record = wait(fetchCheckpoint(cx, records[0], folder, CheckpointAsKeyValues::False));
					fetchedCheckpoints.push_back(record);
					TraceEvent("TestCheckpointFetched").detail("Checkpoint", record.toString());
					break;
				} catch (Error& e) {
					TraceEvent("TestFetchCheckpointError")
					    .errorUnsuppressed(e)
					    .detail("Checkpoint", records[i].toString());
					wait(delay(1));
				}
			}
		}

		state std::string rocksDBTestDir = "rocksdb-kvstore-test-db";
		platform::eraseDirectoryRecursive(rocksDBTestDir);

		// Restore KVS.
		state IKeyValueStore* kvStore = keyValueStoreRocksDB(
		    rocksDBTestDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_ROCKSDB_V1);
		wait(kvStore->init());
		try {
			wait(kvStore->restore(fetchedCheckpoints));
		} catch (Error& e) {
			TraceEvent(SevError, "TestRestoreCheckpointError")
			    .errorUnsuppressed(e)
			    .detail("Checkpoint", describe(records));
		}

		// Compare the keyrange between the original database and the one restored from checkpoint.
		// For now, it should have been a single key.
		tr.reset();
		state RangeResult res;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(store(res, tr.getRange(KeyRangeRef(key, endKey), CLIENT_KNOBS->TOO_MANY)));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		RangeResult kvRange = wait(kvStore->readRange(testRange));
		ASSERT(res.size() == kvRange.size());
		for (int i = 0; i < res.size(); ++i) {
			ASSERT(res[i] == kvRange[i]);
		}

		Future<Void> close = kvStore->onClosed();
		kvStore->dispose();
		wait(close);

		{
			int ignore = wait(setDDMode(cx, 1));
			(void)ignore;
		}
		return Void();
	}

	ACTOR Future<Void> readAndVerify(SSCheckpointRestoreWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);

		loop {
			try {
				state Optional<Value> res = wait(timeoutError(tr.get(key), 30.0));
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				if (expectedValue.isError() && expectedValue.getError().code() == e.code()) {
					break;
				}
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Version> writeAndVerify(SSCheckpointRestoreWorkload* self,
	                                     Database cx,
	                                     Key key,
	                                     Optional<Value> value) {
		state Transaction tr(cx);
		state Version version;
		loop {
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(timeoutError(tr.commit(), 30.0));
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		wait(self->readAndVerify(self, cx, key, value));

		return version;
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SSCheckpointRestoreWorkload> SSCheckpointRestoreWorkloadFactory;