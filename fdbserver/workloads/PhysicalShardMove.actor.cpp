/*
 *PhysicalShardMove.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
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
		state Key keyA = "TestKeyA"_sr;
		state Key keyB = "TestKeyB"_sr;
		state Key keyC = "TestKeyC"_sr;
		state Value testValue = "TestValue"_sr;

		{ Version ignore = wait(self->writeAndVerify(self, cx, keyA, testValue)); }
		{ Version ignore = wait(self->writeAndVerify(self, cx, keyB, testValue)); }
		{ Version ignore = wait(self->writeAndVerify(self, cx, keyC, testValue)); }

		std::cout << "Initialized" << std::endl;

		state std::unordered_set<UID> excludes;
		state int teamSize = 3;
		state std::vector<UID> teamA = wait(self->moveShard(self, cx, KeyRangeRef(keyA, keyB), teamSize, &excludes));
		state std::vector<UID> teamB = wait(self->moveShard(self, cx, KeyRangeRef(keyB, keyC), teamSize, &excludes));

		state Transaction tr(cx);
		state Version version;
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		loop {
			std::cout << "Creating checkpoint." << std::endl;
			try {
				wait(createCheckpoint(&tr, KeyRangeRef(keyA, keyC), RocksDB));
				std::cout << "Buffer write done." << std::endl;
				wait(tr.commit());
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		std::cout << "Created checkpoint." << std::endl;

		loop {
			try {
				state std::vector<CheckpointMetaData> records =
				    wait(getCheckpointMetaData(cx, KeyRangeRef(keyA, keyC), version, RocksDB));
				break;
			} catch (Error& e) {
				std::cout << "GetCheckpointMetaData error: " << e.code() << "Name: " << e.name() << "What: " << e.what()
				          << std::endl;
				ASSERT(e.code() != error_code_checkpoint_not_found);
			}
		}

		std::cout << "Got checkpoint metadata:" << std::endl;
		for (const auto& record : records) {
			std::cout << record.toString() << std::endl;
		}

		state std::string pwd = platform::getWorkingDirectory();
		state std::string folder = pwd + "/checkpoints";
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));

		state int idx = 0;
		state std::vector<CheckpointMetaData> localRecords;
		localRecords.resize(records.size());
		for (; idx < records.size(); ++idx) {
			loop {
				try {
					std::cout << "Fetching checkpoint." << std::endl;
					CheckpointMetaData record = wait(fetchCheckpoint(cx, records[idx], folder));
					localRecords[idx] = record;
					break;
				} catch (Error& e) {
					std::cout << "Getting checkpoint failure: " << e.name() << std::endl;
					wait(delay(1));
				}
			}
			std::cout << "Fetched checkpoint:" << localRecords[idx].toString() << std::endl;
		}

		std::vector<std::string> files = platform::listFiles(folder);
		std::cout << "Received checkpoint files on disk: " << folder << std::endl;
		for (auto& file : files) {
			std::cout << file << std::endl;
		}
		std::cout << std::endl;

		state std::string rocksDBTestDir = "rocksdb-kvstore-test-db";
		platform::eraseDirectoryRecursive(rocksDBTestDir);

		state IKeyValueStore* kvStore = keyValueStoreRocksDB(
		    rocksDBTestDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_ROCKSDB_V1);
		try {
			wait(kvStore->restore(localRecords));
		} catch (Error& e) {
			std::cout << e.name() << std::endl;
		}

		std::cout << "Restore complete" << std::endl;

		tr.reset();
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		loop {
			try {
				state RangeResult res = wait(tr.getRange(KeyRangeRef(keyA, keyC), CLIENT_KNOBS->TOO_MANY));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		state int i = 0;
		for (i = 0; i < res.size(); ++i) {
			std::cout << "Reading key:" << res[i].key.toString() << std::endl;
			Optional<Value> value = wait(kvStore->readValue(res[i].key));
			ASSERT(value.present());
			ASSERT(value.get() == res[i].value);
		}

		std::cout << "Verified." << std::endl;

		int ignore = wait(setDDMode(cx, 1));
		return Void();
		// ASSERT(files.size() == record.rocksCF.get().sstFiles.size());
		// std::unordered_set<std::string> sstFiles(files.begin(), files.end());
		// // for (const LiveFileMetaData& metaData : record.sstFiles) {
		// // 	std::cout << "Checkpoint file:" << metaData.db_path << metaData.name << std::endl;
		// // 	// ASSERT(sstFiles.count(metaData.name.subString) > 0);
		// // }

		// rocksdb::Options options;
		// rocksdb::ReadOptions ropts;
		// state std::unordered_map<Key, Value> kvs;
		// for (auto& file : files) {
		// 	rocksdb::SstFileReader reader(options);
		// 	std::cout << file << std::endl;
		// 	ASSERT(reader.Open(folder + "/" + file).ok());
		// 	ASSERT(reader.VerifyChecksum().ok());
		// 	std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(ropts));
		// 	iter->SeekToFirst();
		// 	while (iter->Valid()) {
		// 		if (normalKeys.contains(Key(iter->key().ToString()))) {
		// 			std::cout << "Key: " << iter->key().ToString() << ", Value: " << iter->value().ToString()
		// 			          << std::endl;
		// 		}
		// 		// std::endl; writer.Put(iter->key().ToString(), iter->value().ToString());
		// 		// kvs[Key(iter->key().ToString())] = Value(iter->value().ToString());
		// 		iter->Next();
		// 	}
		// }

		// state std::unordered_map<Key, Value>::iterator it = kvs.begin();
		// for (; it != kvs.end(); ++it) {
		// 	if (normalKeys.contains(it->first)) {
		// 		std::cout << "Key: " << it->first.toString() << ", Value: " << it->second.toString() << std::endl;
		// 		ErrorOr<Optional<Value>> value(Optional<Value>(it->second));
		// 		wait(self->readAndVerify(self, cx, it->first, value));
		// 	}
		// }
	}
	// ACTOR Future<Void> _start(SSCheckpointWorkload* self, Database cx) {
	// 	state Key key = "TestKey"_sr;
	// 	state Key endKey = "TestKey0"_sr;
	// 	state Value oldValue = "TestValue"_sr;

	// 	int ignore = wait(setDDMode(cx, 0));
	// 	state Version version = wait(self->writeAndVerify(self, cx, key, oldValue));

	// 	// Create checkpoint.
	// 	state Transaction tr(cx);
	// 	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	// 	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	// 	state CheckpointFormat format = RocksDBColumnFamily;
	// 	loop {
	// 		try {
	// 			wait(createCheckpoint(&tr, KeyRangeRef(key, endKey), format));
	// 			wait(tr.commit());
	// 			version = tr.getCommittedVersion();
	// 			break;
	// 		} catch (Error& e) {
	// 			wait(tr.onError(e));
	// 		}
	// 	}

	// 	TraceEvent("TestCheckpointCreated")
	// 	    .detail("Range", KeyRangeRef(key, endKey).toString())
	// 	    .detail("Version", version);

	// 	// Fetch checkpoint meta data.
	// 	loop {
	// 		try {
	// 			state std::vector<CheckpointMetaData> records =
	// 			    wait(getCheckpointMetaData(cx, KeyRangeRef(key, endKey), version, format));
	// 			break;
	// 		} catch (Error& e) {
	// 			TraceEvent("TestFetchCheckpointMetadataError")
	// 			    .detail("Range", KeyRangeRef(key, endKey).toString())
	// 			    .detail("Version", version)
	// 			    .error(e, true);

	// 			// The checkpoint was just created, we don't expect this error.
	// 			ASSERT(e.code() != error_code_checkpoint_not_found);
	// 		}
	// 	}

	// 	TraceEvent("TestCheckpointFetched")
	// 	    .detail("Range", KeyRangeRef(key, endKey).toString())
	// 	    .detail("Version", version)
	// 	    .detail("Shards", records.size());

	// 	state std::string pwd = platform::getWorkingDirectory();
	// 	state std::string folder = pwd + "/checkpoints";
	// 	platform::eraseDirectoryRecursive(folder);
	// 	ASSERT(platform::createDirectory(folder));

	// 	// Fetch checkpoint.
	// 	state int i = 0;
	// 	for (; i < records.size(); ++i) {
	// 		loop {
	// 			TraceEvent("TestFetchingCheckpoint").detail("Checkpoint", records[i].toString());
	// 			try {
	// 				state CheckpointMetaData record = wait(fetchCheckpoint(cx, records[0], folder));
	// 				TraceEvent("TestCheckpointFetched").detail("Checkpoint", records[i].toString());
	// 				break;
	// 			} catch (Error& e) {
	// 				TraceEvent("TestFetchCheckpointError").detail("Checkpoint", records[i].toString()).error(e, true);
	// 				wait(delay(1));
	// 			}
	// 		}
	// 	}

	// 	state std::string rocksDBTestDir = "rocksdb-kvstore-test-db";
	// 	platform::eraseDirectoryRecursive(rocksDBTestDir);

	// 	// Restore KVS.
	// 	state IKeyValueStore* kvStore = keyValueStoreRocksDB(
	// 	    rocksDBTestDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_ROCKSDB_V1);
	// 	try {
	// 		wait(kvStore->restore(records));
	// 	} catch (Error& e) {
	// 		TraceEvent("TestRestoreCheckpointError").detail("Checkpoint", records[0].toString()).error(e, true);
	// 	}

	// 	// Compare the keyrange between the original database and the one restored from checkpoint.
	// 	// For now, it should have been a single key.
	// 	tr.reset();
	// 	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	// 	loop {
	// 		try {
	// 			state RangeResult res = wait(tr.getRange(KeyRangeRef(key, endKey), CLIENT_KNOBS->TOO_MANY));
	// 			break;
	// 		} catch (Error& e) {
	// 			wait(tr.onError(e));
	// 		}
	// 	}

	// 	for (i = 0; i < res.size(); ++i) {
	// 		Optional<Value> value = wait(kvStore->readValue(res[i].key));
	// 		ASSERT(value.present());
	// 		ASSERT(value.get() == res[i].value);
	// 	}

	// 	int ignore = wait(setDDMode(cx, 1));
	// 	return Void();
	// }

	ACTOR Future<Void> readAndVerify(SSCheckpointWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

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

	ACTOR Future<Version> writeAndVerify(SSCheckpointWorkload* self, Database cx, Key key, Optional<Value> value) {
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

	// Move keys to a random selected team consisting of a single SS, after disabling DD, so that keys won't be
	// kept in the new team until DD is enabled.
	// Returns the address of the single SS of the new team.
	ACTOR Future<std::vector<UID>> moveShard(SSCheckpointWorkload* self,
	                                         Database cx,
	                                         KeyRange keys,
	                                         int teamSize,
	                                         std::unordered_set<UID>* excludes) {
		// Disable DD to avoid DD undoing of our move.
		int ignore = wait(setDDMode(cx, 0));
		state std::vector<UID> dests;

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
		ASSERT(interfs.size() > teamSize);
		while (dests.size() < teamSize) {
			const auto& interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
			if (excludes->count(interf.uniqueID) == 0) {
				dests.push_back(interf.uniqueID);
				excludes->insert(interf.uniqueID);
			}
		}

		state UID owner = deterministicRandom()->randomUniqueID();
		state DDEnabledState ddEnabledState;

		state Transaction tr(cx);

		loop {
			try {
				BinaryWriter wrMyOwner(Unversioned());
				wrMyOwner << owner;
				tr.set(moveKeysLockOwnerKey, wrMyOwner.toValue());
				wait(tr.commit());

				MoveKeysLock moveKeysLock;
				moveKeysLock.myOwner = owner;

				wait(moveKeys(cx,
				              keys,
				              dests,
				              dests,
				              moveKeysLock,
				              Promise<Void>(),
				              &self->startMoveKeysParallelismLock,
				              &self->finishMoveKeysParallelismLock,
				              false,
				              UID(), // for logging only
				              &ddEnabledState));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_movekeys_conflict) {
					// Conflict on moveKeysLocks with the current running DD is expected, just retry.
					tr.reset();
				} else {
					wait(tr.onError(e));
				}
			}
		}

		TraceEvent("TestMoveShardComplete").detail("Range", keys.toString()).detail("NewTeam", describe(dests));

		return dests;
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SSCheckpointWorkload> SSCheckpointWorkloadFactory("SSCheckpointWorkload");