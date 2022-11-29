/*
 * PhysicalShardMove.cpp
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
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerCheckpoint.actor.h"
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

struct PhysicalShardMoveWorkLoad : TestWorkload {
	static constexpr auto NAME = "PhysicalShardMove";

	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	FlowLock cleanUpDataMoveParallelismLock;
	const bool enabled;
	bool pass;

	PhysicalShardMoveWorkLoad(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

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

	ACTOR Future<Void> _start(PhysicalShardMoveWorkLoad* self, Database cx) {
		int ignore = wait(setDDMode(cx, 0));
		state std::vector<UID> teamA;
		state std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                                 { "TestKeyAB"_sr, "TestValueAB"_sr },
		                                 { "TestKeyAD"_sr, "TestValueAD"_sr },
		                                 { "TestKeyB"_sr, "TestValueB"_sr },
		                                 { "TestKeyC"_sr, "TestValueC"_sr },
		                                 { "TestKeyD"_sr, "TestValueD"_sr },
		                                 { "TestKeyE"_sr, "TestValueE"_sr },
		                                 { "TestKeyF"_sr, "TestValueF"_sr } });

		Version _ = wait(self->populateData(self, cx, &kvs));

		TraceEvent("TestValueWritten").log();

		state std::unordered_set<UID> excludes;
		state std::unordered_set<UID> includes;
		state int teamSize = 1;
		wait(store(teamA,
		           self->moveShard(self,
		                           cx,
		                           deterministicRandom()->randomUniqueID(),
		                           KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr),
		                           teamSize,
		                           includes,
		                           excludes)));
		excludes.insert(teamA.begin(), teamA.end());

		state uint64_t sh0 = deterministicRandom()->randomUInt64();
		state uint64_t sh1 = deterministicRandom()->randomUInt64();
		state uint64_t sh2 = deterministicRandom()->randomUInt64();

		// Move range [TestKeyA, TestKeyB) to sh0.
		wait(store(teamA,
		           self->moveShard(self,
		                           cx,
		                           UID(sh0, deterministicRandom()->randomUInt64()),
		                           KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr),
		                           teamSize,
		                           includes,
		                           excludes)));

		state std::vector<KeyRange> testRanges;
		testRanges.push_back(KeyRangeRef("TestKeyA"_sr, "TestKeyAC"_sr));
		wait(self->checkpointRestore(self, cx, testRanges, &kvs));
		TraceEvent(SevDebug, "TestMovedRange").detail("Range", KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr));

		// Move range [TestKeyD, TestKeyF) to sh0;
		includes.insert(teamA.begin(), teamA.end());
		state std::vector<UID> teamE = wait(self->moveShard(self,
		                                                    cx,
		                                                    UID(sh0, deterministicRandom()->randomUInt64()),
		                                                    KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr),
		                                                    teamSize,
		                                                    includes,
		                                                    excludes));
		ASSERT(std::equal(teamA.begin(), teamA.end(), teamE.begin()));

		state int teamIdx = 0;
		for (teamIdx = 0; teamIdx < teamA.size(); ++teamIdx) {
			TraceEvent("TestGettingServerShards", teamA[teamIdx])
			    .detail("Range", KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr));
			std::vector<StorageServerShard> shards =
			    wait(self->getStorageServerShards(cx, teamA[teamIdx], KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr)));
			ASSERT(shards.size() == 1);
			ASSERT(shards[0].desiredId == sh0);
			TraceEvent("TestStorageServerShards", teamA[teamIdx]).detail("Shards", describe(shards));
		}

		testRanges.clear();
		testRanges.push_back(KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr));
		testRanges.push_back(KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr));
		wait(self->checkpointRestore(self, cx, testRanges, &kvs));

		// Move range [TestKeyB, TestKeyC) to sh1, on the same server.
		includes.insert(teamA.begin(), teamA.end());
		state std::vector<UID> teamB = wait(self->moveShard(self,
		                                                    cx,
		                                                    UID(sh1, deterministicRandom()->randomUInt64()),
		                                                    KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr),
		                                                    teamSize,
		                                                    includes,
		                                                    excludes));
		ASSERT(std::equal(teamA.begin(), teamA.end(), teamB.begin()));

		teamIdx = 0;
		for (teamIdx = 0; teamIdx < teamA.size(); ++teamIdx) {
			std::vector<StorageServerShard> shards =
			    wait(self->getStorageServerShards(cx, teamA[teamIdx], KeyRangeRef("TestKeyA"_sr, "TestKeyC"_sr)));
			ASSERT(shards.size() == 2);
			ASSERT(shards[0].desiredId == sh0);
			ASSERT(shards[1].desiredId == sh1);
			TraceEvent("TestStorageServerShards", teamA[teamIdx]).detail("Shards", describe(shards));
		}

		state std::vector<UID> teamC = wait(self->moveShard(self,
		                                                    cx,
		                                                    UID(sh2, deterministicRandom()->randomUInt64()),
		                                                    KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr),
		                                                    teamSize,
		                                                    includes,
		                                                    excludes));
		ASSERT(std::equal(teamA.begin(), teamA.end(), teamC.begin()));

		for (teamIdx = 0; teamIdx < teamA.size(); ++teamIdx) {
			std::vector<StorageServerShard> shards =
			    wait(self->getStorageServerShards(cx, teamA[teamIdx], KeyRangeRef("TestKeyA"_sr, "TestKeyC"_sr)));
			ASSERT(shards.size() == 2);
			ASSERT(shards[0].desiredId == sh0);
			ASSERT(shards[1].id == sh1);
			ASSERT(shards[1].desiredId == sh2);
			TraceEvent("TestStorageServerShards", teamA[teamIdx]).detail("Shards", describe(shards));
		}

		wait(self->validateData(self, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr), &kvs));
		TraceEvent("TestValueVerified").log();

		{
			int _ = wait(setDDMode(cx, 1));
			(void)_;
		}
		return Void();
	}

	ACTOR Future<Void> checkpointRestore(PhysicalShardMoveWorkLoad* self,
	                                     Database cx,
	                                     std::vector<KeyRange> testRanges,
	                                     std::map<Key, Value>* kvs) {

		// Create checkpoint.
		TraceEvent(SevDebug, "TestCreatingCheckpoint").detail("Ranges", describe(testRanges));
		state Transaction tr(cx);
		state CheckpointFormat format = DataMoveRocksCF;
		state UID dataMoveId = deterministicRandom()->randomUniqueID();
		state Version version;

		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(createCheckpoint(&tr, testRanges, format, dataMoveId));
				wait(tr.commit());
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Fetch checkpoint meta data.
		state std::vector<CheckpointMetaData> records;
		loop {
			records.clear();
			try {
				wait(store(records, getCheckpointMetaData(cx, testRanges, version, format, Optional<UID>(dataMoveId))));
				TraceEvent(SevDebug, "TestCheckpointMetaDataFetched")
				    .detail("Range", describe(testRanges))
				    .detail("Version", version)
				    .detail("Checkpoints", describe(records));

				break;
			} catch (Error& e) {
				TraceEvent("TestFetchCheckpointMetadataError")
				    .errorUnsuppressed(e)
				    .detail("Range", describe(testRanges))
				    .detail("Version", version);

				// The checkpoint was just created, we don't expect this error.
				ASSERT(e.code() != error_code_checkpoint_not_found);
			}
		}

		// Fetch checkpoint.
		state std::string pwd = platform::getWorkingDirectory();
		state std::string folder = pwd + "/checkpoints";
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		state std::vector<CheckpointMetaData> fetchedCheckpoints;
		state int i = 0;
		for (; i < records.size(); ++i) {
			loop {
				TraceEvent(SevDebug, "TestFetchingCheckpoint").detail("Checkpoint", records[i].toString());
				try {
					CheckpointMetaData record = wait(fetchCheckpoint(cx, records[i], folder, FetchKvs::False));
					fetchedCheckpoints.push_back(record);
					TraceEvent(SevDebug, "TestCheckpointFetched").detail("Checkpoint", record.toString());
					break;
				} catch (Error& e) {
					TraceEvent(SevWarn, "TestFetchCheckpointError")
					    .errorUnsuppressed(e)
					    .detail("Checkpoint", records[i].toString());
					wait(delay(1));
				}
			}
		}

		// Restore KVS.
		state std::string rocksDBTestDir = "rocksdb-kvstore-test-restored-db";
		platform::eraseDirectoryRecursive(rocksDBTestDir);
		state std::string shardId = "restored-shard";
		state IKeyValueStore* kvStore = keyValueStoreShardedRocksDB(
		    rocksDBTestDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_SHARDED_ROCKSDB);
		wait(kvStore->init());
		try {
			wait(kvStore->restore(shardId, testRanges, fetchedCheckpoints));
		} catch (Error& e) {
			TraceEvent(SevError, "TestRestoreCheckpointError")
			    .errorUnsuppressed(e)
			    .detail("Checkpoint", describe(fetchedCheckpoints));
		}

		TraceEvent(SevDebug, "TestCheckpointRestored").detail("Checkpoint", describe(fetchedCheckpoints));

		// Validate the restored kv-store.
		RangeResult kvRange = wait(kvStore->readRange(normalKeys));
		std::unordered_map<Key, Value> kvsKvs;
		for (int i = 0; i < kvRange.size(); ++i) {
			kvsKvs[kvRange[i].key] = kvRange[i].value;
		}

		auto containsKey = [](std::vector<KeyRange> ranges, KeyRef key) {
			for (const auto& range : ranges) {
				if (range.contains(key)) {
					return true;
				}
			}
			return false;
		};

		for (const auto& [key, value] : *kvs) {
			auto it = kvsKvs.find(key);
			if (containsKey(testRanges, key)) {
				TraceEvent(SevVerbose, "TestExpectKeyValueMatch").detail("Key", key).detail("Value", value);
				ASSERT(it->second == value);
			} else {
				TraceEvent(SevVerbose, "TestExpectKeyNotExist").detail("Key", key);
				ASSERT(it == kvsKvs.end());
			}
		}

		TraceEvent(SevDebug, "TestCheckpointVerified").detail("Checkpoint", describe(fetchedCheckpoints));

		Future<Void> close = kvStore->onClosed();
		kvStore->dispose();
		wait(close);

		TraceEvent(SevDebug, "TestRocksDBClosed").detail("Checkpoint", describe(fetchedCheckpoints));

		return Void();
	}

	ACTOR Future<Version> populateData(PhysicalShardMoveWorkLoad* self, Database cx, std::map<Key, Value>* kvs) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state Version version;
		state UID debugID;
		loop {
			debugID = deterministicRandom()->randomUniqueID();
			try {
				tr->debugTransaction(debugID);
				for (const auto& [key, value] : *kvs) {
					tr->set(key, value);
				}
				wait(tr->commit());
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr->onError(e));
			}
		}

		TraceEvent("PopulateTestDataDone")
		    .detail("CommitVersion", tr->getCommittedVersion())
		    .detail("DebugID", debugID);

		return version;
	}

	ACTOR Future<Void> validateData(PhysicalShardMoveWorkLoad* self,
	                                Database cx,
	                                KeyRange range,
	                                std::map<Key, Value>* kvs) {
		state Transaction tr(cx);
		state UID debugID;
		loop {
			debugID = deterministicRandom()->randomUniqueID();
			try {
				tr.debugTransaction(debugID);
				RangeResult res = wait(tr.getRange(range, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!res.more && res.size() < CLIENT_KNOBS->TOO_MANY);

				for (const auto& kv : res) {
					ASSERT((*kvs)[kv.key] == kv.value);
				}
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}

		TraceEvent("ValidateTestDataDone").detail("DebugID", debugID);

		return Void();
	}

	ACTOR Future<Void> readAndVerify(PhysicalShardMoveWorkLoad* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);
		state Version readVersion;
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

		loop {
			try {
				Version _readVersion = wait(tr.getReadVersion());
				readVersion = _readVersion;
				state Optional<Value> res = wait(timeoutError(tr.get(key), 30.0));
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				TraceEvent("TestReadError").errorUnsuppressed(e);
				if (expectedValue.isError() && expectedValue.getError().code() == e.code()) {
					break;
				}
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestReadSuccess").detail("Version", readVersion);

		return Void();
	}

	ACTOR Future<Version> writeAndVerify(PhysicalShardMoveWorkLoad* self, Database cx, Key key, Optional<Value> value) {
		// state Transaction tr(cx);
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state Version version;
		state UID debugID;
		loop {
			debugID = deterministicRandom()->randomUniqueID();
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->debugTransaction(debugID);
				if (value.present()) {
					tr->set(key, value.get());
					tr->set("Test?"_sr, value.get());
					tr->set(key, value.get());
				} else {
					tr->clear(key);
				}
				wait(timeoutError(tr->commit(), 30.0));
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr->onError(e));
			}
		}

		TraceEvent("TestCommitSuccess").detail("CommitVersion", tr->getCommittedVersion()).detail("DebugID", debugID);

		wait(self->readAndVerify(self, cx, key, value));

		return version;
	}

	// Move keys to a random selected team consisting of a single SS, after disabling DD, so that keys won't be
	// kept in the new team until DD is enabled.
	// Returns the address of the single SS of the new team.
	ACTOR Future<std::vector<UID>> moveShard(PhysicalShardMoveWorkLoad* self,
	                                         Database cx,
	                                         UID dataMoveId,
	                                         KeyRange keys,
	                                         int teamSize,
	                                         std::unordered_set<UID> includes,
	                                         std::unordered_set<UID> excludes) {
		// Disable DD to avoid DD undoing of our move.
		int ignore = wait(setDDMode(cx, 0));

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
		ASSERT(interfs.size() > teamSize - includes.size());
		while (includes.size() < teamSize) {
			const auto& interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
			if (excludes.count(interf.uniqueID) == 0 && includes.count(interf.uniqueID) == 0) {
				includes.insert(interf.uniqueID);
			}
		}

		state std::vector<UID> dests(includes.begin(), includes.end());
		state UID owner = deterministicRandom()->randomUniqueID();
		// state Key ownerKey = "\xff/moveKeysLock/Owner"_sr;
		state DDEnabledState ddEnabledState;

		state Transaction tr(cx);

		loop {
			try {
				TraceEvent("TestMoveShard").detail("Range", keys.toString());
				state MoveKeysLock moveKeysLock = wait(takeMoveKeysLock(cx, owner));

				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state RangeResult dataMoves = wait(tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY));
				Version readVersion = wait(tr.getReadVersion());
				TraceEvent("TestMoveShardReadDataMoves")
				    .detail("DataMoves", dataMoves.size())
				    .detail("ReadVersion", readVersion);
				state int i = 0;
				for (; i < dataMoves.size(); ++i) {
					UID dataMoveId = decodeDataMoveKey(dataMoves[i].key);
					state DataMoveMetaData dataMove = decodeDataMoveValue(dataMoves[i].value);
					ASSERT(dataMoveId == dataMove.id);
					TraceEvent("TestCancelDataMoveBegin").detail("DataMove", dataMove.toString());
					wait(cleanUpDataMove(cx,
					                     dataMoveId,
					                     moveKeysLock,
					                     &self->cleanUpDataMoveParallelismLock,
					                     dataMove.ranges.front(),
					                     &ddEnabledState));
					TraceEvent("TestCancelDataMoveEnd").detail("DataMove", dataMove.toString());
				}

				TraceEvent("TestMoveShardStartMoveKeys").detail("DataMove", dataMoveId);
				wait(moveKeys(cx,
				              MoveKeysParams{ dataMoveId,
				                              keys,
				                              dests,
				                              dests,
				                              moveKeysLock,
				                              Promise<Void>(),
				                              &self->startMoveKeysParallelismLock,
				                              &self->finishMoveKeysParallelismLock,
				                              false,
				                              deterministicRandom()->randomUniqueID(), // for logging only
				                              &ddEnabledState }));
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

	ACTOR Future<std::vector<StorageServerShard>> getStorageServerShards(Database cx, UID ssId, KeyRange range) {
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> serverListValue = wait(tr.get(serverListKeyFor(ssId)));
				ASSERT(serverListValue.present());
				state StorageServerInterface ssi = decodeServerListValue(serverListValue.get());
				GetShardStateRequest req(range, GetShardStateRequest::READABLE, true);
				GetShardStateReply rep = wait(ssi.getShardState.getReply(req, TaskPriority::DefaultEndpoint));
				return rep.shards;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<PhysicalShardMoveWorkLoad> PhysicalShardMoveWorkLoadFactory;