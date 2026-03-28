/*
 * PhysicalShardMove.cpp
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
#include "fdbserver/core/IKeyValueStore.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/core/QuietDatabase.actor.h"
#include "fdbserver/core/ServerCheckpoint.h"
#include "fdbserver/tester/workloads.actor.h"
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
		return _start(cx);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert("RandomMoveKeys");
		out.insert("Attrition");
	}

	Future<Void> _start(Database cx) {
		co_await setDDMode(cx, 0);
		std::vector<UID> teamA;
		std::map<Key, Value> kvs({ { "TestKeyA"_sr, "TestValueA"_sr },
		                           { "TestKeyAB"_sr, "TestValueAB"_sr },
		                           { "TestKeyAD"_sr, "TestValueAD"_sr },
		                           { "TestKeyB"_sr, "TestValueB"_sr },
		                           { "TestKeyBA"_sr, "TestValueBA"_sr },
		                           { "TestKeyC"_sr, "TestValueC"_sr },
		                           { "TestKeyD"_sr, "TestValueD"_sr },
		                           { "TestKeyE"_sr, "TestValueE"_sr },
		                           { "TestKeyF"_sr, "TestValueF"_sr } });

		co_await populateData(this, cx, &kvs);

		TraceEvent("TestValueWritten").log();

		std::unordered_set<UID> excludes;
		std::unordered_set<UID> includes;
		int teamSize = 1;
		DataMovementReason dataMoveReason = static_cast<DataMovementReason>(
		    deterministicRandom()->randomInt(1, static_cast<int>(DataMovementReason::NUMBER_OF_REASONS)));
		KeyRangeRef currentRange = KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr);
		teamA = co_await moveShard(
		    this,
		    cx,
		    newDataMoveId(deterministicRandom()->randomUInt64(),
		                  AssignEmptyRange::False,
		                  deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
		                      ? DataMoveType::PHYSICAL
		                      : DataMoveType::LOGICAL,
		                  dataMoveReason),
		    currentRange,
		    teamSize,
		    includes,
		    excludes);
		TraceEvent(SevDebug, "TestMovedRange1").detail("Range", currentRange).detail("Team", describe(teamA));

		excludes.insert(teamA.begin(), teamA.end());

		uint64_t sh0 = deterministicRandom()->randomUInt64();
		uint64_t sh1 = deterministicRandom()->randomUInt64();
		uint64_t sh2 = deterministicRandom()->randomUInt64();

		// Move range [TestKeyA, TestKeyB) to sh0.
		currentRange = KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr);
		teamA = co_await moveShard(
		    this,
		    cx,
		    newDataMoveId(sh0,
		                  AssignEmptyRange::False,
		                  deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
		                      ? DataMoveType::PHYSICAL
		                      : DataMoveType::LOGICAL,
		                  dataMoveReason),
		    currentRange,
		    teamSize,
		    includes,
		    excludes);
		TraceEvent(SevDebug, "TestMovedRange2").detail("Range", currentRange).detail("Team", describe(teamA));

		std::vector<KeyRange> checkpointRanges;
		checkpointRanges.push_back(KeyRangeRef("TestKeyA"_sr, "TestKeyAC"_sr));
		co_await checkpointRestore(this, cx, checkpointRanges, checkpointRanges, CheckpointAsKeyValues::True, &kvs);
		TraceEvent(SevDebug, "TestCheckpointRestored1");

		// Move range [TestKeyD, TestKeyF) to sh0;
		includes.insert(teamA.begin(), teamA.end());
		currentRange = KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr);
		std::vector<UID> teamE = co_await moveShard(
		    this,
		    cx,
		    newDataMoveId(sh0,
		                  AssignEmptyRange::False,
		                  deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
		                      ? DataMoveType::PHYSICAL
		                      : DataMoveType::LOGICAL,
		                  dataMoveReason),
		    currentRange,
		    teamSize,
		    includes,
		    excludes);
		TraceEvent(SevDebug, "TestMovedRange3").detail("Range", currentRange).detail("Team", describe(teamE));
		ASSERT(std::equal(teamA.begin(), teamA.end(), teamE.begin()));

		int teamIdx = 0;
		for (teamIdx = 0; teamIdx < teamA.size(); ++teamIdx) {
			TraceEvent("TestGettingServerShards", teamA[teamIdx])
			    .detail("Range", KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr));
			std::vector<StorageServerShard> shards =
			    co_await getStorageServerShards(cx, teamA[teamIdx], KeyRangeRef("TestKeyD"_sr, "TestKeyF"_sr));
			ASSERT(shards.size() == 1);
			ASSERT(shards[0].desiredId == sh0);
			ASSERT(shards[0].id == sh0);
			TraceEvent("TestStorageServerShards", teamA[teamIdx]).detail("Shards", describe(shards));
		}

		checkpointRanges.clear();
		checkpointRanges.push_back(KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr));
		checkpointRanges.push_back(KeyRangeRef("TestKeyD"_sr, "TestKeyE"_sr));
		co_await checkpointRestore(this, cx, checkpointRanges, checkpointRanges, CheckpointAsKeyValues::True, &kvs);
		TraceEvent(SevDebug, "TestCheckpointRestored2");

		// Move range [TestKeyB, TestKeyC) to sh1, on the same server.
		currentRange = KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr);
		includes.insert(teamA.begin(), teamA.end());
		std::vector<UID> teamB = co_await moveShard(
		    this,
		    cx,
		    newDataMoveId(sh1,
		                  AssignEmptyRange::False,
		                  deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
		                      ? DataMoveType::PHYSICAL
		                      : DataMoveType::LOGICAL,
		                  dataMoveReason),
		    currentRange,
		    teamSize,
		    includes,
		    excludes);
		TraceEvent(SevDebug, "TestMovedRange4").detail("Range", currentRange).detail("Team", describe(teamB));
		ASSERT(std::equal(teamA.begin(), teamA.end(), teamB.begin()));

		teamIdx = 0;
		for (teamIdx = 0; teamIdx < teamA.size(); ++teamIdx) {
			std::vector<StorageServerShard> shards =
			    co_await getStorageServerShards(cx, teamA[teamIdx], KeyRangeRef("TestKeyA"_sr, "TestKeyC"_sr));
			TraceEvent("TestStorageServerShards", teamA[teamIdx]).detail("Shards", describe(shards));
			ASSERT(shards.size() == 2);
			ASSERT(shards[0].desiredId == sh0);
			ASSERT(shards[1].desiredId == sh1);
		}

		checkpointRanges.clear();
		checkpointRanges.push_back(KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr));
		checkpointRanges.push_back(KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr));
		std::vector<KeyRange> restoreRanges;
		restoreRanges.push_back(KeyRangeRef("TestKeyA"_sr, "TestKeyB"_sr));
		restoreRanges.push_back(KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr));
		co_await checkpointRestore(this, cx, checkpointRanges, restoreRanges, CheckpointAsKeyValues::True, &kvs);
		TraceEvent(SevDebug, "TestCheckpointRestored3");

		currentRange = KeyRangeRef("TestKeyB"_sr, "TestKeyC"_sr);
		std::vector<UID> teamC = co_await moveShard(
		    this,
		    cx,
		    newDataMoveId(sh2,
		                  AssignEmptyRange::False,
		                  deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
		                      ? DataMoveType::PHYSICAL
		                      : DataMoveType::LOGICAL,
		                  dataMoveReason),
		    currentRange,
		    teamSize,
		    includes,
		    excludes);
		TraceEvent(SevDebug, "TestMovedRange5").detail("Range", currentRange).detail("Team", describe(teamC));
		ASSERT(std::equal(teamA.begin(), teamA.end(), teamC.begin()));

		for (teamIdx = 0; teamIdx < teamA.size(); ++teamIdx) {
			std::vector<StorageServerShard> shards =
			    co_await getStorageServerShards(cx, teamA[teamIdx], KeyRangeRef("TestKeyA"_sr, "TestKeyC"_sr));
			ASSERT(shards.size() == 2);
			ASSERT(shards[0].desiredId == sh0);
			ASSERT(shards[1].id == sh1);
			ASSERT(shards[1].desiredId == sh2);
			TraceEvent("TestStorageServerShards", teamA[teamIdx]).detail("Shards", describe(shards));
		}

		{
			int _ = co_await setDDMode(cx, 1);
			(void)_;
		}

		co_await validateData(this, cx, KeyRangeRef("TestKeyA"_sr, "TestKeyF"_sr), &kvs);
		TraceEvent("TestValueVerified").log();
	}

	Future<Void> deleteCheckpoints(Database cx, std::vector<UID> checkpointIds) {
		TraceEvent(SevDebug, "DataMoveDeleteCheckpoints").detail("Checkpoints", describe(checkpointIds));

		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				std::vector<Future<Optional<Value>>> checkpointEntries;
				for (const UID& id : checkpointIds) {
					checkpointEntries.push_back(tr.get(checkpointKeyFor(id)));
				}
				std::vector<Optional<Value>> checkpointValues = co_await getAll(checkpointEntries);

				for (int i = 0; i < checkpointIds.size(); ++i) {
					const auto& value = checkpointValues[i];
					if (!value.present()) {
						TraceEvent(SevWarnAlways, "CheckpointNotFound");
						continue;
					}
					CheckpointMetaData checkpoint = decodeCheckpointValue(value.get());
					const Key key = checkpointKeyFor(checkpoint.checkpointID);
					// Setting the state as CheckpointMetaData::Deleting will trigger private mutations to instruct
					// all storage servers to delete their local checkpoints.
					checkpoint.setState(CheckpointMetaData::Deleting);
					tr.set(key, checkpointValue(checkpoint));
					tr.clear(singleKeyRange(key));
					TraceEvent(SevDebug, "DataMoveDeleteCheckpoint").detail("Checkpoint", checkpoint.toString());
				}
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> checkpointRestore(PhysicalShardMoveWorkLoad* self,
	                               Database cx,
	                               std::vector<KeyRange> checkpointRanges,
	                               std::vector<KeyRange> restoreRanges,
	                               CheckpointAsKeyValues asKeyValues,
	                               std::map<Key, Value>* kvs) {

		// Create checkpoint.
		TraceEvent(SevDebug, "TestCreatingCheckpoint").detail("Ranges", describe(checkpointRanges));
		Transaction tr(cx);
		CheckpointFormat format = DataMoveRocksCF;
		UID dataMoveId = deterministicRandom()->randomUniqueID();
		TraceEvent("CheckpointRestore").detail("DMID1", dataMoveId.first()).detail("DMID2", dataMoveId.second());
		Version version{ 0 };

		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				co_await createCheckpoint(&tr, checkpointRanges, format, dataMoveId);
				co_await tr.commit();
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		// Fetch checkpoint meta data.
		std::vector<std::pair<KeyRange, CheckpointMetaData>> records;
		while (true) {
			records.clear();
			try {
				records = co_await getCheckpointMetaData(cx, restoreRanges, version, format, Optional<UID>(dataMoveId));
				TraceEvent(SevDebug, "TestCheckpointMetaDataFetched")
				    .detail("Range", describe(checkpointRanges))
				    .detail("Version", version);

				break;
			} catch (Error& e) {
				TraceEvent("TestFetchCheckpointMetadataError")
				    .errorUnsuppressed(e)
				    .detail("Range", describe(checkpointRanges))
				    .detail("Version", version);

				// The checkpoint was just created, we don't expect this error.
				ASSERT(e.code() != error_code_checkpoint_not_found);
			}
		}

		// Fetch checkpoint.
		std::string checkpointDir = abspath("fetchedCheckpoints" + deterministicRandom()->randomAlphaNumeric(6));
		platform::eraseDirectoryRecursive(checkpointDir);
		ASSERT(platform::createDirectory(checkpointDir));
		std::vector<Future<CheckpointMetaData>> checkpointFutures;
		std::vector<CheckpointMetaData> fetchedCheckpoints;
		while (true) {
			checkpointFutures.clear();
			try {
				if (asKeyValues) {
					for (int i = 0; i < records.size(); ++i) {
						TraceEvent(SevDebug, "TestFetchingCheckpoint")
						    .detail("Checkpoint", records[i].second.toString());
						const std::string currentDir =
						    fetchedCheckpointDir(checkpointDir, records[i].second.checkpointID);
						platform::eraseDirectoryRecursive(currentDir);
						ASSERT(platform::createDirectory(currentDir));
						checkpointFutures.push_back(
						    fetchCheckpointRanges(cx, records[i].second, currentDir, { records[i].first }));
					}
				} else {
					for (int i = 1; i < records.size(); ++i) {
						ASSERT(records[i].second.checkpointID == records[i - 1].second.checkpointID);
					}
					const std::string currentDir =
					    fetchedCheckpointDir(checkpointDir, records.front().second.checkpointID);
					platform::eraseDirectoryRecursive(currentDir);
					ASSERT(platform::createDirectory(currentDir));
					checkpointFutures.push_back(fetchCheckpoint(cx, records.front().second, currentDir));
				}
				fetchedCheckpoints = co_await getAll(checkpointFutures);
				TraceEvent(SevDebug, "TestCheckpointFetched").detail("Checkpoints", describe(fetchedCheckpoints));
				break;
			} catch (Error& e) {
				TraceEvent("TestFetchCheckpointError").errorUnsuppressed(e);
			}
		}

		std::vector<UID> checkpointIds;
		for (const auto& it : records) {
			checkpointIds.push_back(it.second.checkpointID);
		}
		co_await self->deleteCheckpoints(cx, checkpointIds);

		// Restore KVS.
		std::string rocksDBTestDir = "rocksdb-kvstore-test-restored-db";
		platform::eraseDirectoryRecursive(rocksDBTestDir);
		std::string shardId = "restored-shard";
		IKeyValueStore* kvStore = keyValueStoreShardedRocksDB(
		    rocksDBTestDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_SHARDED_ROCKSDB);
		co_await kvStore->init();
		try {
			co_await kvStore->restore(shardId, restoreRanges, fetchedCheckpoints);
		} catch (Error& e) {
			TraceEvent(SevError, "TestRestoreCheckpointError")
			    .errorUnsuppressed(e)
			    .detail("Checkpoint", describe(fetchedCheckpoints));
		}

		TraceEvent(SevDebug, "TestCheckpointRestored").detail("Checkpoint", describe(fetchedCheckpoints));

		// Validate the restored kv-store.
		RangeResult kvRange = co_await kvStore->readRange(normalKeys);
		ASSERT(!kvRange.more);
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

		int count = 0;
		for (const auto& [key, value] : *kvs) {
			if (containsKey(restoreRanges, key)) {
				TraceEvent(SevDebug, "TestExpectKeyValueMatch").detail("Key", key).detail("Value", value);
				auto it = kvsKvs.find(key);
				ASSERT(it != kvsKvs.end() && it->second == value);
				++count;
			}
		}

		ASSERT(kvsKvs.size() == count);

		TraceEvent(SevDebug, "TestCheckpointVerified").detail("Checkpoint", describe(fetchedCheckpoints));

		Future<Void> close = kvStore->onClosed();
		kvStore->dispose();
		co_await close;
		platform::eraseDirectoryRecursive(rocksDBTestDir);
		platform::eraseDirectoryRecursive(checkpointDir);

		TraceEvent(SevDebug, "TestRocksDBClosed").detail("Checkpoint", describe(fetchedCheckpoints));
	}

	Future<Version> populateData(PhysicalShardMoveWorkLoad* self, Database cx, std::map<Key, Value>* kvs) {
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		Version version{ 0 };
		UID debugID;
		while (true) {
			debugID = deterministicRandom()->randomUniqueID();
			Error err;
			try {
				tr->debugTransaction(debugID);
				for (const auto& [key, value] : *kvs) {
					tr->set(key, value);
				}
				co_await tr->commit();
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("TestCommitError").errorUnsuppressed(err);
			co_await tr->onError(err);
		}

		TraceEvent("PopulateTestDataDone")
		    .detail("CommitVersion", tr->getCommittedVersion())
		    .detail("DebugID", debugID);

		co_return version;
	}

	Future<Void> validateData(PhysicalShardMoveWorkLoad* self, Database cx, KeyRange range, std::map<Key, Value>* kvs) {
		Transaction tr(cx);
		UID debugID;
		while (true) {
			debugID = deterministicRandom()->randomUniqueID();
			Error err;
			try {
				TraceEvent("TestValidateDataBegin").detail("DebugID", debugID);
				tr.debugTransaction(debugID);
				RangeResult res = co_await tr.getRange(range, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!res.more && res.size() < CLIENT_KNOBS->TOO_MANY);

				for (const auto& kv : res) {
					ASSERT((*kvs)[kv.key] == kv.value);
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("TestCommitError").errorUnsuppressed(err);
			co_await tr.onError(err);
		}

		TraceEvent("ValidateTestDataDone").detail("DebugID", debugID);
	}

	Future<Void> readAndVerify(PhysicalShardMoveWorkLoad* self,
	                           Database cx,
	                           Key key,
	                           ErrorOr<Optional<Value>> expectedValue) {
		Transaction tr(cx);
		Version readVersion{ 0 };
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

		while (true) {
			Error err;
			try {
				Version _readVersion = co_await tr.getReadVersion();
				readVersion = _readVersion;
				Optional<Value> res = co_await timeoutError(tr.get(key), 30.0);
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("TestReadError").errorUnsuppressed(err);
			if (expectedValue.isError() && expectedValue.getError().code() == err.code()) {
				break;
			}
			co_await tr.onError(err);
		}

		TraceEvent("TestReadSuccess").detail("Version", readVersion);
	}

	Future<Version> writeAndVerify(PhysicalShardMoveWorkLoad* self, Database cx, Key key, Optional<Value> value) {
		// state Transaction tr(cx);
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		Version version{ 0 };
		UID debugID;
		while (true) {
			debugID = deterministicRandom()->randomUniqueID();
			Error err;
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
				co_await timeoutError(tr->commit(), 30.0);
				version = tr->getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("TestCommitError").errorUnsuppressed(err);
			co_await tr->onError(err);
		}

		TraceEvent("TestCommitSuccess").detail("CommitVersion", tr->getCommittedVersion()).detail("DebugID", debugID);

		co_await self->readAndVerify(self, cx, key, value);

		co_return version;
	}

	// Move keys to a random selected team consisting of a single SS, this requires DD is disabled to prevent shards
	// being moved by DD automatically. Returns the address of the single SS of the new team.
	Future<std::vector<UID>> moveShard(PhysicalShardMoveWorkLoad* self,
	                                   Database cx,
	                                   UID dataMoveId,
	                                   KeyRange keys,
	                                   int teamSize,
	                                   std::unordered_set<UID> includes,
	                                   std::unordered_set<UID> excludes) {
		// Pick a random SS as the dest, keys will reside on a single server after the move.
		std::vector<StorageServerInterface> interfs = co_await getStorageServers(cx);
		ASSERT(interfs.size() > teamSize - includes.size());
		while (includes.size() < teamSize) {
			const auto& interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
			if (!excludes.contains(interf.uniqueID) && !includes.contains(interf.uniqueID)) {
				includes.insert(interf.uniqueID);
			}
		}

		std::vector<UID> dests(includes.begin(), includes.end());
		UID owner = deterministicRandom()->randomUniqueID();
		DDEnabledState ddEnabledState;

		Transaction tr(cx);

		while (true) {
			Error err;
			try {
				TraceEvent("TestMoveShard").detail("Range", keys.toString());
				MoveKeysLock moveKeysLock = co_await takeMoveKeysLock(cx, owner);

				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				RangeResult dataMoves = co_await tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY);
				Version readVersion = co_await tr.getReadVersion();
				TraceEvent("TestMoveShardReadDataMoves")
				    .detail("DataMoves", dataMoves.size())
				    .detail("ReadVersion", readVersion);
				for (int i = 0; i < dataMoves.size(); ++i) {
					UID dataMoveId = decodeDataMoveKey(dataMoves[i].key);
					DataMoveMetaData dataMove = decodeDataMoveValue(dataMoves[i].value);
					ASSERT(dataMoveId == dataMove.id);
					TraceEvent("TestCancelDataMoveBegin").detail("DataMove", dataMove.toString());
					if (dataMove.ranges.empty()) {
						// This dataMove cancellation is delayed to background cancellation
						// For this case, the dataMove has empty ranges but it is in Deleting phase
						// We simply bypass this case
						ASSERT(dataMove.getPhase() == DataMoveMetaData::Deleting);
						TraceEvent("TestCancelEmptyDataMoveEnd").detail("DataMove", dataMove.toString());
						continue;
					}
					co_await cleanUpDataMove(cx,
					                         dataMoveId,
					                         moveKeysLock,
					                         &self->cleanUpDataMoveParallelismLock,
					                         dataMove.ranges.front(),
					                         &ddEnabledState);
					TraceEvent("TestCancelDataMoveEnd").detail("DataMove", dataMove.toString());
				}

				TraceEvent("TestMoveShardStartMoveKeys").detail("DataMove", dataMoveId);
				co_await moveKeys(cx,
				                  MoveKeysParams(dataMoveId,
				                                 std::vector<KeyRange>(1, keys),
				                                 dests,
				                                 dests,
				                                 moveKeysLock,
				                                 Promise<Void>(),
				                                 &self->startMoveKeysParallelismLock,
				                                 &self->finishMoveKeysParallelismLock,
				                                 false,
				                                 deterministicRandom()->randomUniqueID(), // for logging only
				                                 &ddEnabledState,
				                                 CancelConflictingDataMoves::False,
				                                 Optional<BulkLoadTaskState>()));
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_movekeys_conflict) {
				// Conflict on moveKeysLocks with the current running DD is expected, just retry.
				tr.reset();
			} else {
				co_await tr.onError(err);
			}
		}

		TraceEvent("TestMoveShardComplete").detail("Range", keys.toString()).detail("NewTeam", describe(dests));

		co_return dests;
	}

	Future<std::vector<StorageServerShard>> getStorageServerShards(Database cx, UID ssId, KeyRange range) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Optional<Value> serverListValue = co_await tr.get(serverListKeyFor(ssId));
				ASSERT(serverListValue.present());
				StorageServerInterface ssi = decodeServerListValue(serverListValue.get());
				GetShardStateRequest req(range, GetShardStateRequest::READABLE, true);
				GetShardStateReply rep = co_await ssi.getShardState.getReply(req, TaskPriority::DefaultEndpoint);
				co_return rep.shards;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<PhysicalShardMoveWorkLoad> PhysicalShardMoveWorkLoadFactory;
