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

struct SSCheckpointWorkload : TestWorkload {
	const bool enabled;
	bool pass;

	SSCheckpointWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	std::string description() const override { return "SSCheckpoint"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(SSCheckpointWorkload* self, Database cx) {
		state Key key = "TestKey"_sr;
		state Key endKey = "TestKey0"_sr;
		state Value oldValue = "TestValue"_sr;

		int ignore = wait(setDDMode(cx, 0));
		state Version version = wait(self->writeAndVerify(self, cx, key, oldValue));

		// Create checkpoint.
		state Transaction tr(cx);
		state CheckpointFormat format = RocksDBColumnFamily;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(createCheckpoint(&tr, KeyRangeRef(key, endKey), format));
				wait(tr.commit());
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestCheckpointCreated")
		    .detail("Range", KeyRangeRef(key, endKey).toString())
		    .detail("Version", version);

		// Fetch checkpoint meta data.
		loop {
			try {
				state std::vector<CheckpointMetaData> records =
				    wait(getCheckpointMetaData(cx, KeyRangeRef(key, endKey), version, format));
				break;
			} catch (Error& e) {
				TraceEvent("TestFetchCheckpointMetadataError")
				    .errorUnsuppressed(e)
				    .detail("Range", KeyRangeRef(key, endKey).toString())
				    .detail("Version", version);

				// The checkpoint was just created, we don't expect this error.
				ASSERT(e.code() != error_code_checkpoint_not_found);
			}
		}

		TraceEvent("TestCheckpointFetched")
		    .detail("Range", KeyRangeRef(key, endKey).toString())
		    .detail("Version", version)
		    .detail("Shards", records.size());

		state std::string pwd = platform::getWorkingDirectory();
		state std::string folder = pwd + "/checkpoints";
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));

		// Fetch checkpoint.
		state int i = 0;
		for (; i < records.size(); ++i) {
			loop {
				TraceEvent("TestFetchingCheckpoint").detail("Checkpoint", records[i].toString());
				try {
					state CheckpointMetaData record = wait(fetchCheckpoint(cx, records[0], folder));
					TraceEvent("TestCheckpointFetched").detail("Checkpoint", records[i].toString());
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
		try {
			wait(kvStore->restore(records));
		} catch (Error& e) {
			TraceEvent(SevError, "TestRestoreCheckpointError")
			    .errorUnsuppressed(e)
			    .detail("Checkpoint", describe(records));
		}

		// Compare the keyrange between the original database and the one restored from checkpoint.
		// For now, it should have been a single key.
		tr.reset();
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				state RangeResult res = wait(tr.getRange(KeyRangeRef(key, endKey), CLIENT_KNOBS->TOO_MANY));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		for (i = 0; i < res.size(); ++i) {
			Optional<Value> value = wait(kvStore->readValue(res[i].key));
			ASSERT(value.present());
			ASSERT(value.get() == res[i].value);
		}

		int ignore = wait(setDDMode(cx, 1));
		return Void();
	}

	ACTOR Future<Void> readAndVerify(SSCheckpointWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
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

	ACTOR Future<Version> writeAndVerify(SSCheckpointWorkload* self, Database cx, Key key, Optional<Value> value) {
		state Transaction tr(cx);
		state Version version;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
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

WorkloadFactory<SSCheckpointWorkload> SSCheckpointWorkloadFactory("SSCheckpointWorkload");