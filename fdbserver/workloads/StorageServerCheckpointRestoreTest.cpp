/*
 * StorageServerCheckpointRestoreTest.cpp
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
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/kvstore/IKeyValueStore.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/ServerCheckpoint.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include <cstdint>
#include <limits>

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

	explicit SSCheckpointRestoreWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), enabled(!clientId), pass(true) {}

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

	Future<Void> _start(SSCheckpointRestoreWorkload* self, Database cx) {
		Key key = "TestKey"_sr;
		Key endKey = "TestKey0"_sr;
		Value oldValue = "TestValue"_sr;
		KeyRange testRange = KeyRangeRef(key, endKey);
		std::vector<std::pair<KeyRange, CheckpointMetaData>> records;

		TraceEvent("TestCheckpointRestoreBegin");
		co_await setDDMode(cx, 0);
		Version version = co_await self->writeAndVerify(self, cx, key, oldValue);

		TraceEvent("TestCreatingCheckpoint").detail("Range", testRange);
		// Create checkpoint.
		Transaction tr(cx);
		CheckpointFormat format = DataMoveRocksCF;
		UID dataMoveId =
		    newDataMoveId(deterministicRandom()->randomUInt64(),
		                  AssignEmptyRange(false),
		                  deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
		                      ? DataMoveType::PHYSICAL
		                      : DataMoveType::LOGICAL,
		                  DataMovementReason::TEAM_HEALTHY,
		                  UnassignShard(false));
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				co_await createCheckpoint(&tr, std::vector<KeyRange>(1, testRange), format, dataMoveId);
				co_await tr.commit();
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		TraceEvent("TestCheckpointCreated")
		    .detail("Range", testRange)
		    .detail("Version", version)
		    .detail("DataMoveID", dataMoveId);

		// Fetch checkpoint meta data.
		while (true) {
			records.clear();
			try {
				records = co_await getCheckpointMetaData(
				    cx, std::vector<KeyRange>(1, testRange), version, format, Optional<UID>(dataMoveId));
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

		TraceEvent("TestCheckpointFetched").detail("Range", testRange).detail("Version", version);

		std::string pwd = platform::getWorkingDirectory();
		std::string folder = pwd + "/checkpoints";
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));

		// Fetch checkpoint.
		std::vector<CheckpointMetaData> fetchedCheckpoints;
		for (auto it = records.begin(); it != records.end(); ++it) {
			while (true) {
				TraceEvent("TestFetchingCheckpoint").detail("Checkpoint", it->second.toString());
				Error err;
				try {
					CheckpointMetaData record = co_await fetchCheckpoint(cx, it->second, folder);
					fetchedCheckpoints.push_back(record);
					TraceEvent("TestCheckpointFetched").detail("Checkpoint", record.toString());
					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("TestFetchCheckpointError")
				    .errorUnsuppressed(err)
				    .detail("Checkpoint", it->second.toString());
				co_await delay(1);
			}
		}

		std::string rocksDBTestDir = "rocksdb-kvstore-test-db";
		platform::eraseDirectoryRecursive(rocksDBTestDir);

		// Restore KVS.
		IKeyValueStore* kvStore = keyValueStoreRocksDB(
		    rocksDBTestDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_ROCKSDB_V1);
		co_await kvStore->init();
		try {
			co_await kvStore->restore(fetchedCheckpoints);
		} catch (Error& e) {
			TraceEvent(SevError, "TestRestoreCheckpointError").errorUnsuppressed(e);
		}

		// Compare the keyrange between the original database and the one restored from checkpoint.
		// For now, it should have been a single key.
		tr.reset();
		RangeResult res;
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				res = co_await tr.getRange(KeyRangeRef(key, endKey), CLIENT_KNOBS->TOO_MANY);
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		RangeResult kvRange = co_await kvStore->readRange(testRange);
		ASSERT(res.size() == kvRange.size());
		for (int i = 0; i < res.size(); ++i) {
			ASSERT(res[i] == kvRange[i]);
		}

		Future<Void> close = kvStore->onClosed();
		kvStore->dispose();
		co_await close;
		co_await setDDMode(cx, 1);
	}

	Future<Void> readAndVerify(SSCheckpointRestoreWorkload* self,
	                           Database cx,
	                           Key key,
	                           ErrorOr<Optional<Value>> expectedValue) {
		Transaction tr(cx);

		while (true) {
			Error err;
			try {
				Optional<Value> res = co_await timeoutError(tr.get(key), 30.0);
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			if (expectedValue.isError() && expectedValue.getError().code() == err.code()) {
				break;
			}
			co_await tr.onError(err);
		}
	}

	Future<Version> writeAndVerify(SSCheckpointRestoreWorkload* self, Database cx, Key key, Optional<Value> value) {
		Transaction tr(cx);
		Version version{ 0 };
		while (true) {
			Error err;
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				co_await timeoutError(tr.commit(), 30.0);
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		co_await self->readAndVerify(self, cx, key, value);

		co_return version;
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SSCheckpointRestoreWorkload> SSCheckpointRestoreWorkloadFactory;
