/*
 * BulkLoading.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BulkLoading.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const std::string simulationBulkLoadFolderForSimpleTest = "bulkLoadSimple";
const std::string simulationBulkLoadFolderForLargeDataProduce = "bulkLoadLargeData";
const std::string simulationBulkLoadFolderForComplexTest = "bulkLoadComplex";

struct BulkLoadTaskTestUnit {
	BulkLoadState bulkLoadTask;
	std::vector<KeyValue> data;
	BulkLoadTaskTestUnit() = default;
};

struct BulkLoading : TestWorkload {
	static constexpr auto NAME = "BulkLoadingWorkload";
	const bool enabled;
	bool pass;

	// This workload is not compatible with following workload because they will race in changing the DD mode
	// This workload is not compatible with RandomRangeLock for the conflict in range lock
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomMoveKeys",
		             "DataLossRecovery",
		             "IDDTxnProcessorApiCorrectness",
		             "PerpetualWiggleStatsWorkload",
		             "PhysicalShardMove",
		             "StorageCorruption",
		             "StorageServerCheckpointRestoreTest",
		             "ValidateStorage",
		             "RandomRangeLock" });
	}

	BulkLoading(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> submitBulkLoadTasks(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
		state int i = 0;
		for (; i < tasks.size(); i++) {
			loop {
				try {
					wait(submitBulkLoadTask(cx, tasks[i]));
					TraceEvent("BulkLoadingSubmitBulkLoadTask")
					    .setMaxEventLength(-1)
					    .setMaxFieldLength(-1)
					    .detail("BulkLoadState", tasks[i].toString());
					break;
				} catch (Error& e) {
					TraceEvent("BulkLoadingSubmitBulkLoadTaskError")
					    .setMaxEventLength(-1)
					    .setMaxFieldLength(-1)
					    .errorUnsuppressed(e)
					    .detail("BulkLoadState", tasks[i].toString());
					wait(delay(0.1));
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> acknowledgeBulkLoadTasks(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
		state int i = 0;
		for (; i < tasks.size(); i++) {
			loop {
				try {
					wait(acknowledgeBulkLoadTask(cx, tasks[i].getRange(), tasks[i].getTaskId()));
					TraceEvent("BulkLoadingAcknowledgeBulkLoadTask")
					    .setMaxEventLength(-1)
					    .setMaxFieldLength(-1)
					    .detail("BulkLoadState", tasks[i].toString());
					break;
				} catch (Error& e) {
					TraceEvent("BulkLoadingAcknowledgeBulkLoadTaskError")
					    .setMaxEventLength(-1)
					    .setMaxFieldLength(-1)
					    .errorUnsuppressed(e)
					    .detail("BulkLoadState", tasks[i].toString());
					if (e.code() == error_code_bulkload_task_outdated) {
						break; // has been erased or overwritten by other tasks
					}
					wait(delay(0.1));
				}
			}
		}
		return Void();
	}

	Key getRandomKey(const std::vector<Key>& keyCharList, size_t keySizeMin, size_t keySizeMax) {
		Key key = ""_sr;
		int keyLength = deterministicRandom()->randomInt(keySizeMin, keySizeMax);
		for (int j = 0; j < keyLength; j++) {
			Key appendedItem = deterministicRandom()->randomChoice(keyCharList);
			key = key.withSuffix(appendedItem);
		}
		return key;
	}

	std::vector<KeyValue> generateRandomData(KeyRange range, size_t count, const std::vector<Key>& keyCharList) {
		std::set<Key> keys;
		while (keys.size() < count) {
			Key key = getRandomKey(keyCharList, 1, 1000);
			if (!range.contains(key)) {
				continue;
			}
			keys.insert(key);
		}
		std::vector<KeyValue> res;
		for (const auto& key : keys) {
			UID randomId = deterministicRandom()->randomUniqueID();
			Value val = Standalone(StringRef(randomId.toString()));
			res.push_back(Standalone(KeyValueRef(key, val)));
		}
		ASSERT(res.size() == count);
		return res;
	}

	void produceFilesToLoad(BulkLoadTaskTestUnit task) {
		std::string folder = task.bulkLoadTask.getFolder();
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		std::string bytesSampleFile = task.bulkLoadTask.getBytesSampleFile().get();
		std::string dataFile = *(task.bulkLoadTask.getDataFiles().begin());

		std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
		sstWriter->open(abspath(dataFile));
		std::vector<KeyValue> bytesSample;
		for (const auto& kv : task.data) {
			ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
			if (sampleInfo.inSample) {
				Key sampleKey = kv.key;
				Value sampleValue = BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned());
				bytesSample.push_back(Standalone(KeyValueRef(sampleKey, sampleValue)));
			}
			sstWriter->write(kv.key, kv.value);
		}
		TraceEvent("BulkLoadingDataProduced")
		    .detail("LoadKeyCount", task.data.size())
		    .detail("BytesSampleSize", bytesSample.size())
		    .detail("Folder", folder)
		    .detail("DataFile", dataFile)
		    .detail("BytesSampleFile", bytesSampleFile);
		ASSERT(sstWriter->finish());

		if (bytesSample.size() > 0) {
			sstWriter->open(abspath(bytesSampleFile));
			for (const auto& kv : bytesSample) {
				sstWriter->write(kv.key, kv.value);
			}
			TraceEvent("BulkLoadingByteSampleProduced")
			    .detail("LoadKeyCount", task.data.size())
			    .detail("BytesSampleSize", bytesSample.size())
			    .detail("Folder", folder)
			    .detail("DataFile", dataFile)
			    .detail("BytesSampleFile", bytesSampleFile);
			ASSERT(sstWriter->finish());
		}
		TraceEvent("BulkLoadingProduceDataToLoad").detail("Folder", folder).detail("LoadKeyCount", task.data.size());
		return;
	}

	ACTOR Future<bool> checkDDEnabled(Database cx) {
		loop {
			state Transaction tr(cx);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			try {
				state int ddMode = 1;
				Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					rd >> ddMode;
				}
				return ddMode == 1;

			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<bool> allComplete(Database cx) {
		state Transaction tr(cx);
		state Key beginKey = allKeys.begin;
		state Key endKey = allKeys.end;
		while (beginKey < endKey) {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult res = wait(krmGetRanges(&tr,
				                                    bulkLoadPrefix,
				                                    Standalone(KeyRangeRef(beginKey, endKey)),
				                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
				for (int i = 0; i < res.size() - 1; i++) {
					if (!res[i].value.empty()) {
						BulkLoadState bulkLoadState = decodeBulkLoadState(res[i].value);
						ASSERT(bulkLoadState.isValid());
						if (bulkLoadState.getRange() != KeyRangeRef(res[i].key, res[i + 1].key)) {
							continue; // Ignore outdated task
						}
						if (bulkLoadState.phase != BulkLoadPhase::Complete) {
							TraceEvent("BulkLoadingWorkLoadIncompleteTasks")
							    .setMaxEventLength(-1)
							    .setMaxFieldLength(-1)
							    .detail("Task", bulkLoadState.toString());
							return false;
						}
					}
				}
				beginKey = res[res.size() - 1].key;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return true;
	}

	ACTOR Future<Void> waitUntilAllComplete(BulkLoading* self, Database cx) {
		loop {
			bool complete = wait(self->allComplete(cx));
			if (complete) {
				break;
			}
			wait(delay(10.0));
		}
		return Void();
	}

	bool checkData(std::vector<KeyValue> kvs, std::vector<KeyValue> kvsdb) {
		if (kvs.size() != kvsdb.size()) {
			TraceEvent(SevError, "BulkLoadingWorkLoadDataWrong")
			    .detail("Reason", "KeyValue count wrong")
			    .detail("KVS", kvs.size())
			    .detail("DB", kvsdb.size());
			return false;
		}
		std::sort(kvs.begin(), kvs.end(), [](KeyValue a, KeyValue b) { return a.key < b.key; });
		std::sort(kvsdb.begin(), kvsdb.end(), [](KeyValue a, KeyValue b) { return a.key < b.key; });
		for (int i = 0; i < kvs.size(); i++) {
			if (kvs[i].key != kvsdb[i].key) {
				TraceEvent(SevError, "BulkLoadingWorkLoadDataWrong")
				    .detail("Reason", "Key mismatch")
				    .detail("KVS", kvs[i])
				    .detail("DB", kvsdb[i]);
				return false;
			} else if (kvs[i].value != kvsdb[i].value) {
				TraceEvent(SevError, "BulkLoadingWorkLoadDataWrong")
				    .detail("Reason", "Value mismatch")
				    .detail("KVS", kvs[i])
				    .detail("DB", kvsdb[i]);
				return false;
			}
		}
		return true;
	}

	bool keyIsIgnored(Key key, const std::vector<KeyRange>& ignoreRanges) {
		for (const auto& range : ignoreRanges) {
			if (range.contains(key)) {
				return true;
			}
		}
		return false;
	}

	ACTOR Future<std::vector<KeyValue>> getKvsFromDB(BulkLoading* self,
	                                                 Database cx,
	                                                 std::vector<KeyRange> ignoreRanges) {
		state std::vector<KeyValue> res;
		state Transaction tr(cx);
		TraceEvent("BulkLoadingWorkLoadGetKVSFromDBStart");
		loop {
			try {
				RangeResult result = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more);
				for (int i = 0; i < result.size(); i++) {
					if (!self->keyIsIgnored(result[i].key, ignoreRanges)) {
						res.push_back(Standalone(KeyValueRef(result[i].key, result[i].value)));
					}
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		TraceEvent("BulkLoadingWorkLoadGetKVSFromDBDone");
		return res;
	}

	BulkLoadTaskTestUnit produceBulkLoadTaskUnit(BulkLoading* self,
	                                             const std::vector<Key>& keyCharList,
	                                             KeyRange range,
	                                             std::string folderName) {
		std::string dataFileName = generateRandomBulkLoadDataFileName();
		std::string bytesSampleFileName = generateRandomBulkLoadBytesSampleFileName();
		std::string folder = joinPath(simulationBulkLoadFolderForSimpleTest, folderName);
		BulkLoadTaskTestUnit taskUnit;
		taskUnit.bulkLoadTask = newBulkLoadTaskLocalSST(
		    range, folder, joinPath(folder, dataFileName), joinPath(folder, bytesSampleFileName));
		size_t dataSize = deterministicRandom()->randomInt(10, 30);
		taskUnit.data = self->generateRandomData(range, dataSize, keyCharList);
		self->produceFilesToLoad(taskUnit);
		return taskUnit;
	}

	std::vector<KeyValue> generateSortedKVS(StringRef prefix, size_t count) {
		std::vector<KeyValue> res;
		for (int i = 0; i < count; i++) {
			UID keyId = deterministicRandom()->randomUniqueID();
			Value key = Standalone(StringRef(keyId.toString())).withPrefix(prefix);
			UID valueId = deterministicRandom()->randomUniqueID();
			Value val = Standalone(StringRef(valueId.toString()));
			res.push_back(Standalone(KeyValueRef(key, val)));
		}
		std::sort(res.begin(), res.end(), [](KeyValue a, KeyValue b) { return a.key < b.key; });
		return res;
	}

	ACTOR Future<bool> checkBulkLoadMetadataCleared(BulkLoading* self, Database cx) {
		state Key beginKey = allKeys.begin;
		state Key endKey = allKeys.end;
		state KeyRange rangeToRead;
		while (beginKey < endKey) {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
				RangeResult res = wait(krmGetRanges(&tr,
				                                    bulkLoadPrefix,
				                                    allKeys,
				                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                                    CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
				beginKey = res.back().key;
				int emptyCount = 0;
				int nonEmptyCount = 0;
				for (int i = 0; i < res.size() - 1; i++) {
					if (!res[i].value.empty()) {
						BulkLoadState bulkLoadState = decodeBulkLoadState(res[i].value);
						KeyRange currentRange = Standalone(KeyRangeRef(res[i].key, res[i + 1].key));
						if (bulkLoadState.getRange() == currentRange) {
							TraceEvent("BulkLoadingWorkLoadMetadataNotCleared")
							    .setMaxEventLength(-1)
							    .setMaxFieldLength(-1)
							    .detail("BulkLoadTask", bulkLoadState.toString());
							return false;
						} else {
							ASSERT(bulkLoadState.getRange().contains(currentRange));
						}
						nonEmptyCount++;
					} else {
						emptyCount++;
					}
				}
				ASSERT(emptyCount - 1 - 1 <= nonEmptyCount);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return true;
	}

	// Issue three non-overlapping tasks and check data consistency and correctness
	// Repeat twice
	ACTOR Future<Void> simpleTest(BulkLoading* self, Database cx) {
		TraceEvent("BulkLoadingWorkLoadSimpleTestBegin");
		state std::vector<Key> keyCharList = { "0"_sr, "1"_sr, "2"_sr, "3"_sr, "4"_sr, "5"_sr };
		// First round of issuing tasks
		state std::vector<BulkLoadState> bulkLoadStates;
		state std::vector<std::vector<KeyValue>> bulkLoadDataList;
		for (int i = 0; i < 3; i++) {
			std::string strIdx = std::to_string(i);
			std::string strIdxPlusOne = std::to_string(i + 1);
			std::string folderName = strIdx;
			Key beginKey = Standalone(StringRef(strIdx));
			Key endKey = Standalone(StringRef(strIdxPlusOne));
			KeyRange range = Standalone(KeyRangeRef(beginKey, endKey));
			BulkLoadTaskTestUnit taskUnit = self->produceBulkLoadTaskUnit(self, keyCharList, range, folderName);
			bulkLoadStates.push_back(taskUnit.bulkLoadTask);
			bulkLoadDataList.push_back(taskUnit.data);
		}
		wait(self->submitBulkLoadTasks(self, cx, bulkLoadStates));
		TraceEvent("BulkLoadingWorkLoadSimpleTestIssuedTasks");
		int old1 = wait(setBulkLoadMode(cx, 1));
		TraceEvent("BulkLoadingWorkLoadSimpleTestSetMode").detail("OldMode", old1).detail("NewMode", 1);
		wait(self->waitUntilAllComplete(self, cx));
		TraceEvent("BulkLoadingWorkLoadSimpleTestAllComplete");

		// Second round of issuing tasks
		bulkLoadStates.clear();
		bulkLoadDataList.clear();
		for (int i = 0; i < 3; i++) {
			std::string strIdx = std::to_string(i);
			std::string strIdxPlusOne = std::to_string(i + 1);
			std::string folderName = strIdx;
			Key beginKey = Standalone(StringRef(strIdx));
			Key endKey = Standalone(StringRef(strIdxPlusOne));
			KeyRange range = Standalone(KeyRangeRef(beginKey, endKey));
			BulkLoadTaskTestUnit taskUnit = self->produceBulkLoadTaskUnit(self, keyCharList, range, folderName);
			bulkLoadStates.push_back(taskUnit.bulkLoadTask);
			bulkLoadDataList.push_back(taskUnit.data);
		}
		wait(self->submitBulkLoadTasks(self, cx, bulkLoadStates));
		TraceEvent("BulkLoadingWorkLoadSimpleTestIssuedTasks");
		wait(self->waitUntilAllComplete(self, cx));
		TraceEvent("BulkLoadingWorkLoadSimpleTestAllComplete");

		// Check data
		int old2 = wait(setBulkLoadMode(cx, 0));
		TraceEvent("BulkLoadingWorkLoadSimpleTestSetMode").detail("OldMode", old2).detail("NewMode", 0);
		state std::vector<KeyValue> dbkvs = wait(self->getKvsFromDB(self, cx, std::vector<KeyRange>()));
		state std::vector<KeyValue> kvs;
		for (int j = 0; j < bulkLoadDataList.size(); j++) {
			kvs.insert(kvs.end(), bulkLoadDataList[j].begin(), bulkLoadDataList[j].end());
		}
		ASSERT(self->checkData(kvs, dbkvs));

		// Check bulk load metadata
		int old3 = wait(setBulkLoadMode(cx, 1));
		TraceEvent("BulkLoadingWorkLoadSimpleTestSetMode").detail("OldMode", old3).detail("NewMode", 1);
		wait(self->acknowledgeBulkLoadTasks(self, cx, bulkLoadStates));
		loop {
			bool cleared = wait(self->checkBulkLoadMetadataCleared(self, cx));
			if (cleared) {
				break;
			}
			wait(delay(1.0));
		}
		TraceEvent("BulkLoadingWorkLoadSimpleTestComplete");
		return Void();
	}

	std::string getStringWithFixedLength(int number, int length) {
		std::string numStr = std::to_string(number);
		int zeroCount = length - numStr.size();
		std::string res;
		for (int i = 0; i < zeroCount; i++) {
			res = res + "0";
		}
		res = res + numStr;
		return res;
	}

	BulkLoadTaskTestUnit produceRandomBulkLoadTaskUnit(BulkLoading* self, std::string rootPath, int index) {
		std::string dataFileName = generateRandomBulkLoadDataFileName();
		std::string bytesSampleFileName = generateRandomBulkLoadBytesSampleFileName();
		std::string randomKey1 = deterministicRandom()->randomUniqueID().toString();
		std::string randomKey2 = deterministicRandom()->randomUniqueID().toString();
		while (randomKey2 == randomKey1) {
			randomKey2 = deterministicRandom()->randomUniqueID().toString();
		}
		StringRef randomKeyRef1 = StringRef(randomKey1);
		StringRef randomKeyRef2 = StringRef(randomKey2);

		StringRef firstKey = randomKeyRef1 < randomKeyRef2 ? randomKeyRef1 : randomKeyRef2;
		StringRef lastKey = randomKeyRef1 < randomKeyRef2 ? randomKeyRef2 : randomKeyRef1;
		KeyRange range = Standalone(KeyRangeRef(firstKey, lastKey.withSuffix("\xff"_sr)));
		std::string folderName = getStringWithFixedLength(index, 6);
		std::string folder = joinPath(rootPath, folderName);

		BulkLoadTaskTestUnit taskUnit;
		taskUnit.data.push_back(Standalone(KeyValueRef(firstKey, firstKey)));
		std::set<std::string> middleKeys;
		int keyCount = deterministicRandom()->randomInt(1, 20);
		for (int i = 0; i < keyCount; i++) {
			middleKeys.insert(deterministicRandom()->randomUniqueID().toString());
		}
		for (const auto& middleKey : middleKeys) {
			Key key = firstKey.withSuffix(middleKey);
			taskUnit.data.push_back(Standalone(KeyValueRef(key, key)));
		}
		taskUnit.data.push_back(Standalone(KeyValueRef(lastKey, lastKey)));

		taskUnit.bulkLoadTask = newBulkLoadTaskLocalSST(
		    range, folder, joinPath(folder, dataFileName), joinPath(folder, bytesSampleFileName));
		self->produceFilesToLoad(taskUnit);
		return taskUnit;
	}

	ACTOR Future<Void> complexTest(BulkLoading* self, Database cx) {
		int old1 = wait(setBulkLoadMode(cx, 1));
		TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", old1).detail("NewMode", 1);

		// Issue tasks
		state KeyRangeMap<Optional<BulkLoadTaskTestUnit>> taskMap;
		taskMap.insert(allKeys, Optional<BulkLoadTaskTestUnit>());
		state int i = 0;
		state int n = deterministicRandom()->randomInt(1, 5);
		state int frequencyFactorForWaitAll = std::max(2, (int)(n * deterministicRandom()->random01()));
		state int frequencyFactorForSwitchMode = std::max(2, (int)(n * deterministicRandom()->random01()));
		for (; i < n; i++) {
			state BulkLoadTaskTestUnit taskUnit =
			    self->produceRandomBulkLoadTaskUnit(self, simulationBulkLoadFolderForComplexTest, i);
			taskMap.insert(taskUnit.bulkLoadTask.getRange(), taskUnit);
			if (deterministicRandom()->coinflip()) {
				wait(delay(deterministicRandom()->random01() * 10));
			}
			wait(self->submitBulkLoadTasks(self, cx, { taskUnit.bulkLoadTask }));
			if (i % frequencyFactorForWaitAll == 0 && deterministicRandom()->coinflip()) {
				wait(self->waitUntilAllComplete(self, cx));
			}
			if (i % frequencyFactorForSwitchMode == 0 && deterministicRandom()->coinflip()) {
				int old2 = wait(setBulkLoadMode(cx, 0));
				TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", old2).detail("NewMode", 0);
				wait(delay(deterministicRandom()->random01() * 5));
				int old3 = wait(setBulkLoadMode(cx, 1));
				TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", old3).detail("NewMode", 1);
			}
		}
		// Wait until all tasks have completed
		wait(self->waitUntilAllComplete(self, cx));
		int old4 = wait(setBulkLoadMode(cx, 0)); // trigger DD restart
		TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", old4).detail("NewMode", 0);

		// Check correctness
		state std::vector<KeyValue> kvs;
		state std::vector<BulkLoadState> bulkLoadStates;
		state std::vector<KeyRange> incompleteRanges;
		for (auto& range : taskMap.ranges()) {
			if (!range.value().present()) {
				continue;
			}
			if (range.value().get().bulkLoadTask.getRange() != range.range()) {
				ASSERT(range.value().get().bulkLoadTask.getRange().contains(range.range()));
				incompleteRanges.push_back(range.range());
				continue; // outdated
			}
			std::vector<KeyValue> kvsToCheck = range.value().get().data;
			kvs.insert(std::end(kvs), std::begin(kvsToCheck), std::end(kvsToCheck));
			bulkLoadStates.push_back(range.value().get().bulkLoadTask);
		}
		std::vector<KeyValue> dbkvs = wait(self->getKvsFromDB(self, cx, incompleteRanges));
		ASSERT(self->checkData(kvs, dbkvs));

		// Clear all range lock
		wait(releaseReadLockOnRange(cx, normalKeys, "BulkLoad"));

		// Clear metadata
		int old5 = wait(setBulkLoadMode(cx, 1));
		TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", old5).detail("NewMode", 1);
		wait(self->acknowledgeBulkLoadTasks(self, cx, bulkLoadStates));
		loop {
			bool cleared = wait(self->checkBulkLoadMetadataCleared(self, cx));
			if (cleared) {
				break;
			}
			wait(delay(1.0));
		}
		TraceEvent("BulkLoadingWorkLoadComplexTestComplete");
		return Void();
	}

	void produceLargeDataToLoad(BulkLoadTaskTestUnit task, int count) {
		std::string folder = task.bulkLoadTask.getFolder();
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		std::string bytesSampleFile = task.bulkLoadTask.getBytesSampleFile().get();
		std::string dataFile = *(task.bulkLoadTask.getDataFiles().begin());

		std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
		sstWriter->open(abspath(dataFile));
		std::vector<KeyValue> bytesSample;
		int insertedKeyCount = 0;
		for (int i = 0; i < 10; i++) {
			std::string idxStr = std::to_string(i);
			Key prefix = Standalone(StringRef(idxStr)).withPrefix(task.bulkLoadTask.getRange().begin);
			std::vector<KeyValue> kvs = generateSortedKVS(prefix, std::max(count / 10, 1));
			for (const auto& kv : kvs) {
				ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
				if (sampleInfo.inSample) {
					Key sampleKey = kv.key;
					Value sampleValue = BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned());
					bytesSample.push_back(Standalone(KeyValueRef(sampleKey, sampleValue)));
				}
				sstWriter->write(kv.key, kv.value);
				insertedKeyCount++;
			}
		}
		TraceEvent("BulkLoadingDataProduced")
		    .detail("LoadKeyCount", insertedKeyCount)
		    .detail("BytesSampleSize", bytesSample.size())
		    .detail("Folder", folder)
		    .detail("DataFile", dataFile)
		    .detail("BytesSampleFile", bytesSampleFile);
		ASSERT(sstWriter->finish());

		if (bytesSample.size() > 0) {
			sstWriter->open(abspath(bytesSampleFile));
			for (const auto& kv : bytesSample) {
				sstWriter->write(kv.key, kv.value);
			}
			TraceEvent("BulkLoadingByteSampleProduced")
			    .detail("LoadKeyCount", task.data.size())
			    .detail("BytesSampleSize", bytesSample.size())
			    .detail("Folder", folder)
			    .detail("DataFile", dataFile)
			    .detail("BytesSampleFile", bytesSampleFile);
			ASSERT(sstWriter->finish());
		}
	}

	void produceDataSet(BulkLoading* self, KeyRange range, std::string folderName) {
		std::string dataFileName =
		    range.begin.toString() + "_" + range.end.toString() + "_" + generateRandomBulkLoadDataFileName();
		std::string bytesSampleFileName =
		    range.begin.toString() + "_" + range.end.toString() + "_" + generateRandomBulkLoadBytesSampleFileName();
		std::string folder = joinPath(simulationBulkLoadFolderForLargeDataProduce, folderName);
		BulkLoadTaskTestUnit taskUnit;
		taskUnit.bulkLoadTask = newBulkLoadTaskLocalSST(
		    range, folder, joinPath(folder, dataFileName), joinPath(folder, bytesSampleFileName));
		self->produceLargeDataToLoad(taskUnit, 5000000);
		return;
	}

	void produceLargeData(BulkLoading* self, Database cx) {
		std::string folderName1 = "1";
		KeyRange range1 = Standalone(KeyRangeRef("1"_sr, "2"_sr));
		self->produceDataSet(self, range1, folderName1);
		std::string folderName2 = "2";
		KeyRange range2 = Standalone(KeyRangeRef("2"_sr, "3"_sr));
		self->produceDataSet(self, range2, folderName2);
		std::string folderName3 = "3";
		KeyRange range3 = Standalone(KeyRangeRef("3"_sr, "4"_sr));
		self->produceDataSet(self, range3, folderName3);
		return;
	}

	ACTOR Future<Void> _start(BulkLoading* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		if (g_network->isSimulated()) {
			// Network partition between CC and DD can cause DD no longer existing,
			// which results in the bulk loading task cannot complete
			// So, this workload disable the network partition
			disableConnectionFailures("BulkLoading");
		}

		if (deterministicRandom()->coinflip()) {
			// Inject data to three non-overlapping ranges
			wait(self->simpleTest(self, cx));
		} else {
			// Inject data to many ranges and those ranges can be overlapping
			wait(self->complexTest(self, cx));
		}
		// self->produceLargeData(self, cx); // Produce data set that is used in loop back cluster test

		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
