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
#include "fdbclient/SystemData.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const std::string simulationBulkLoadFolder = joinPath("simfdb", "bulkload");

struct BulkLoadTaskTestUnit {
	BulkLoadTaskTestUnit() = default;

	size_t getTotalBytes() const {
		size_t bytes = 0;
		for (const auto& kv : data) {
			bytes = bytes + kv.expectedSize(); // This size is different from size used by fetchKeys
		}
		return bytes;
	}

	size_t getKeyCount() const { return data.size(); }

	KeyRange getRange() const { return bulkLoadTask.getRange(); }

	std::string toString() const {
		return "[BulkLoadTaskTestUnit]: [Bytes]: " + std::to_string(getTotalBytes()) +
		       ", [Keys]: " + std::to_string(getKeyCount());
	}

	BulkLoadTaskState bulkLoadTask;
	std::vector<KeyValue> data;
};

struct BulkLoading : TestWorkload {
	static constexpr auto NAME = "BulkLoadingWorkload";
	const bool enabled = true;
	bool pass = true;
	bool debugging = false;
	bool backgroundTrafficEnabled = deterministicRandom()->coinflip();
	UID jobId = deterministicRandom()->randomUniqueID();
	bool initializeBulkLoadMetadata = deterministicRandom()->coinflip();

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
		             "RandomRangeLock",
		             "BulkDumping" });
	}

	BulkLoading(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> clearAllBulkLoadTask(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(krmSetRange(&tr, bulkLoadTaskPrefix, normalKeys, bulkLoadTaskStateValue(BulkLoadTaskState())));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	// Submit task can be failed due to range lock reject
	ACTOR Future<bool> submitBulkLoadTask(Database cx, BulkLoadTaskState bulkLoadTask) {
		loop {
			try {
				state Transaction tr(cx);
				wait(setBulkLoadSubmissionTransaction(&tr, bulkLoadTask));
				wait(takeExclusiveReadLockOnRange(&tr, bulkLoadTask.getRange(), rangeLockNameForBulkLoad));
				wait(tr.commit());
				TraceEvent(SevDebug, "BulkLoadingSubmitBulkLoadTask")
				    .detail("BulkLoadTaskState", bulkLoadTask.toString());
				break;
			} catch (Error& e) {
				TraceEvent(SevWarn, "BulkLoadingSubmitBulkLoadTaskError")
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .errorUnsuppressed(e)
				    .detail("BulkLoadTaskState", bulkLoadTask.toString());
				if (e.code() == error_code_range_lock_reject) {
					return false;
				}
				wait(delay(0.1));
			}
		}
		return true;
	}

	// Finish task must always succeed
	ACTOR Future<Void> finalizeBulkLoadTask(Database cx, KeyRange range, UID taskId) {
		loop {
			try {
				state Transaction tr(cx);
				wait(setBulkLoadFinalizeTransaction(&tr, range, taskId));
				wait(releaseExclusiveReadLockOnRange(&tr, range, rangeLockNameForBulkLoad));
				wait(tr.commit());
				TraceEvent(SevDebug, "BulkLoadingAcknowledgeBulkLoadTask")
				    .detail("TaskID", taskId.toString())
				    .detail("TaskRange", range);
				break;
			} catch (Error& e) {
				TraceEvent(SevWarn, "BulkLoadingAcknowledgeBulkLoadTaskError")
				    .errorUnsuppressed(e)
				    .detail("TaskID", taskId.toString())
				    .detail("TaskRange", range);
				ASSERT(e.code() != error_code_bulkload_task_outdated && e.code() != error_code_range_lock_reject);
				wait(delay(0.1));
			}
		}
		return Void();
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

	// First return value is whether all tasks are complete or error.
	// Second return value is the error tasks.
	ACTOR Future<std::pair<bool, std::vector<BulkLoadTaskState>>> checkAllTaskCompleteOrError(Database cx) {
		state Transaction tr(cx);
		state Key beginKey = allKeys.begin;
		state Key endKey = allKeys.end;
		state std::vector<BulkLoadTaskState> errorTasks;
		while (beginKey < endKey) {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult res =
				    wait(krmGetRanges(&tr, bulkLoadTaskPrefix, Standalone(KeyRangeRef(beginKey, endKey))));
				for (int i = 0; i < res.size() - 1; i++) {
					if (!res[i].value.empty()) {
						BulkLoadTaskState bulkLoadTaskState = decodeBulkLoadTaskState(res[i].value);
						if (!bulkLoadTaskState.isValid()) {
							continue;
						}
						// We do not check manifest because we are not fully setting manifest in this simulation test
						if (bulkLoadTaskState.getRange() != KeyRangeRef(res[i].key, res[i + 1].key)) {
							continue; // Ignore outdated task
						}
						if (bulkLoadTaskState.phase != BulkLoadPhase::Complete &&
						    bulkLoadTaskState.phase != BulkLoadPhase::Error) {
							TraceEvent("BulkLoadingWorkLoadIncompleteTasks")
							    .setMaxEventLength(-1)
							    .setMaxFieldLength(-1)
							    .detail("Task", bulkLoadTaskState.toString());
							return std::make_pair(false, errorTasks);
						}
						if (bulkLoadTaskState.phase == BulkLoadPhase::Error) {
							TraceEvent("BulkLoadingWorkLoadFailedTasks")
							    .setMaxEventLength(-1)
							    .setMaxFieldLength(-1)
							    .detail("Task", bulkLoadTaskState.toString());
							errorTasks.push_back(bulkLoadTaskState);
						}
					}
				}
				beginKey = res[res.size() - 1].key;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return std::make_pair(true, errorTasks);
	}

	ACTOR Future<std::vector<BulkLoadTaskState>> waitUntilAllTaskCompleteOrError(BulkLoading* self, Database cx) {
		loop {
			std::pair<bool, std::vector<BulkLoadTaskState>> res = wait(self->checkAllTaskCompleteOrError(cx));
			if (res.first) { // If all tasks are complete or error
				return res.second; // Return errorTasks
			}
			wait(delay(10.0));
		}
	}

	ACTOR Future<bool> checkBulkLoadMetadataCleared(BulkLoading* self, Database cx) {
		state Key beginKey = normalKeys.begin;
		state Key endKey = normalKeys.end;
		state Transaction tr(cx);
		while (beginKey < endKey) {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult res = wait(krmGetRanges(&tr, bulkLoadTaskPrefix, KeyRangeRef(beginKey, endKey)));
				int clearedCount = 0;
				int nonEmptyCount = 0;
				for (int i = 0; i < res.size() - 1; i++) {
					ASSERT(!self->initializeBulkLoadMetadata || !res[i].value.empty());
					if (res[i].value.empty()) {
						continue;
					}
					BulkLoadTaskState bulkLoadTaskState = decodeBulkLoadTaskState(res[i].value);
					if (!bulkLoadTaskState.isValid()) {
						clearedCount++;
						continue;
					}
					KeyRange currentRange = Standalone(KeyRangeRef(res[i].key, res[i + 1].key));
					if (bulkLoadTaskState.getRange() == currentRange) {
						TraceEvent("BulkLoadingWorkLoadMetadataNotCleared")
						    .setMaxEventLength(-1)
						    .setMaxFieldLength(-1)
						    .detail("BulkLoadTask", bulkLoadTaskState.toString());
						return false;
					}
					ASSERT(bulkLoadTaskState.getRange().contains(currentRange));
					nonEmptyCount++;
				}
				if (self->initializeBulkLoadMetadata && (clearedCount > nonEmptyCount + 1)) {
					TraceEvent(SevError, "BulkLoadingWorkLoadTooManyClearedCount")
					    .detail("ClearedCount", clearedCount)
					    .detail("NonEmptyCount", nonEmptyCount);
				}
				beginKey = res.back().key;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return true;
	}

	bool keyContainedInRanges(const Key& key, const std::vector<KeyRange>& ranges) {
		for (const auto& range : ranges) {
			if (range.contains(key)) {
				return true;
			}
		}
		return false;
	}

	ACTOR Future<std::vector<KeyValue>> getKvsFromDB(BulkLoading* self,
	                                                 Database cx,
	                                                 std::vector<KeyRange> ignoreRanges,
	                                                 std::vector<KeyRange> loadedRanges) {
		state std::vector<KeyValue> res;
		state Transaction tr(cx);
		TraceEvent("BulkLoadingWorkLoadGetKVSFromDBStart");
		loop {
			try {
				RangeResult result = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more);
				for (int i = 0; i < result.size(); i++) {
					if (self->keyContainedInRanges(result[i].key, ignoreRanges)) {
						continue; // ignoreRanges
					}
					if (self->backgroundTrafficEnabled && !self->keyContainedInRanges(result[i].key, loadedRanges)) {
						continue; // When background traffic is enabled, ignore any data outside the loaded range
					}
					res.push_back(Standalone(KeyValueRef(result[i].key, result[i].value)));
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		TraceEvent("BulkLoadingWorkLoadGetKVSFromDBDone");
		return res;
	}

	Standalone<StringRef> getRandomStringRef() const {
		int stringLength = deterministicRandom()->randomInt(1, 10);
		Standalone<StringRef> stringBuffer = makeString(stringLength);
		deterministicRandom()->randomBytes(mutateString(stringBuffer), stringLength);
		return stringBuffer;
	}

	KeyRange getRandomRange(BulkLoading* self, KeyRange scope) const {
		loop {
			Standalone<StringRef> keyA = self->getRandomStringRef();
			Standalone<StringRef> keyB = self->getRandomStringRef();
			if (!scope.contains(keyA) || !scope.contains(keyB)) {
				continue;
			}
			KeyRange range = keyA < keyB ? KeyRangeRef(keyA, keyB) : KeyRangeRef(keyB, keyA);
			if (range.empty() || range.singleKeyRange()) {
				continue;
			}
			return range;
		}
	}

	std::vector<KeyValue> generateOrderedKVS(BulkLoading* self, KeyRange range, size_t count) {
		std::set<Key> keys; // ordered
		while (keys.size() < count) {
			Standalone<StringRef> str = self->getRandomStringRef();
			Key key = range.begin.withSuffix(str);
			if (keys.contains(key)) {
				continue;
			}
			if (!range.contains(key)) {
				continue;
			}
			keys.insert(key);
		}
		std::vector<KeyValue> res;
		for (const auto& key : keys) {
			Value val = self->getRandomStringRef();
			res.push_back(Standalone(KeyValueRef(key, val)));
		}
		return res; // ordered
	}

	BulkLoadFileSet generateSSTFiles(BulkLoading* self, std::string rootPath, BulkLoadTaskTestUnit task) {
		const std::string dataFileNameBase = deterministicRandom()->randomUniqueID().toString();
		const std::string dataFileName = dataFileNameBase + "-data.sst";
		const std::string sampleFileName = dataFileNameBase + "-sample.sst";
		BulkLoadFileSet res(rootPath, "", generateEmptyManifestFileName(), dataFileName, "", BulkLoadChecksum());
		std::string folder = res.getFolder();
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		std::string dataFile = res.getDataFileFullPath();
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
		    .detail("Task", task.bulkLoadTask.toString())
		    .detail("LoadKeyCount", task.data.size())
		    .detail("BytesSampleSize", bytesSample.size())
		    .detail("Folder", folder)
		    .detail("DataFile", dataFile);

		if (self->debugging) {
			TraceEvent e("DebugBulkLoadDataProducedKVS");
			e.setMaxEventLength(-1);
			e.setMaxFieldLength(-1);
			e.detail("Task", task.bulkLoadTask.toString());
			e.detail("LoadKeyCount", task.data.size());
			int counter = 0;
			for (const auto& kv : task.data) {
				e.detail("Key" + std::to_string(counter), kv.key);
				e.detail("Val" + std::to_string(counter), kv.value);
				counter++;
			}
		}
		ASSERT(sstWriter->finish());

		res.setByteSampleFileName(sampleFileName);
		std::string bytesSampleFile = res.getBytesSampleFileFullPath();
		if (bytesSample.size() > 0) {
			sstWriter->open(abspath(bytesSampleFile));
			for (const auto& kv : bytesSample) {
				sstWriter->write(kv.key, kv.value);
			}
			TraceEvent("BulkLoadingByteSampleProduced")
			    .detail("Task", task.bulkLoadTask.toString())
			    .detail("LoadKeyCount", task.data.size())
			    .detail("BytesSampleSize", bytesSample.size())
			    .detail("Folder", folder)
			    .detail("DataFile", dataFile)
			    .detail("BytesSampleFile", bytesSampleFile);
			ASSERT(sstWriter->finish());
		} else {
			res.removeByteSampleFile();
		}
		TraceEvent("BulkLoadingProduceDataToLoad").detail("Folder", folder).detail("LoadKeyCount", task.data.size());
		return res;
	}

	std::vector<Key> getAllKeys(const std::vector<KeyValue>& kvs) {
		std::vector<Key> res;
		for (const auto& kv : kvs) {
			res.push_back(kv.key);
		}
		return res;
	}

	BulkLoadTaskTestUnit generateBulkLoadTaskUnit(BulkLoading* self,
	                                              std::string folderPath,
	                                              int dataSize,
	                                              Optional<KeyRange> range = Optional<KeyRange>()) {
		KeyRange rangeToLoad = range.present() ? range.get() : self->getRandomRange(self, normalKeys);
		BulkLoadTaskTestUnit taskUnit;
		taskUnit.data = self->generateOrderedKVS(self, rangeToLoad, dataSize);
		BulkLoadFileSet fileSet = self->generateSSTFiles(self, folderPath, taskUnit);
		taskUnit.bulkLoadTask =
		    createBulkLoadTask(self->jobId,
		                       rangeToLoad,
		                       fileSet,
		                       BulkLoadByteSampleSetting(0,
		                                                 "hashlittle2", // use function name to represent the method
		                                                 SERVER_KNOBS->BYTE_SAMPLING_FACTOR,
		                                                 SERVER_KNOBS->BYTE_SAMPLING_OVERHEAD,
		                                                 SERVER_KNOBS->MIN_BYTE_SAMPLING_PROBABILITY),
		                       /*snapshotVersion=*/invalidVersion,
		                       taskUnit.getTotalBytes(),
		                       taskUnit.getKeyCount(),
		                       BulkLoadType::SST,
		                       BulkLoadTransportMethod::CP);
		TraceEvent("BulkLoadingWorkLoadTaskUnitGenerated")
		    .detail("TaskUnit", taskUnit.toString())
		    .detail("RangeToLoad", rangeToLoad)
		    .detail("Data", describe(self->getAllKeys(taskUnit.data)));
		return taskUnit;
	}

	bool checkSame(BulkLoading* self, std::vector<KeyValue> kvs, std::vector<KeyValue> kvsdb) {
		if (kvs.size() != kvsdb.size()) {
			TraceEvent(SevError, "BulkLoadingWorkLoadDataWrong")
			    .detail("Reason", "KeyValue count wrong")
			    .detail("KVS", kvs.size())
			    .detail("DB", kvsdb.size());
			if (self->debugging) {
				TraceEvent e("DebugBulkLoadKVS");
				e.setMaxEventLength(-1);
				e.setMaxFieldLength(-1);
				int counter = 0;
				for (const auto& kv : kvs) {
					e.detail("Key" + std::to_string(counter), kv.key);
					e.detail("Val" + std::to_string(counter), kv.value);
					counter++;
				}
				TraceEvent e1("DebugBulkLoadDB");
				e1.setMaxEventLength(-1);
				e1.setMaxFieldLength(-1);
				counter = 0;
				for (const auto& kv : kvsdb) {
					e1.detail("Key" + std::to_string(counter), kv.key);
					e1.detail("Val" + std::to_string(counter), kv.value);
					counter++;
				}
			}
			return false;
		}
		std::sort(kvs.begin(), kvs.end(), [](KeyValue a, KeyValue b) { return a.key < b.key; });
		std::sort(kvsdb.begin(), kvsdb.end(), [](KeyValue a, KeyValue b) { return a.key < b.key; });
		for (int i = 0; i < kvs.size(); i++) {
			if (kvs[i].key != kvsdb[i].key) {
				TraceEvent(SevError, "BulkLoadingWorkLoadDataWrong")
				    .detail("Reason", "Key mismatch")
				    .detail("KVS", kvs[i])
				    .detail("DB", kvsdb[i])
				    .detail("AllKVS", describe(self->getAllKeys(kvs)))
				    .detail("AllDB", describe(self->getAllKeys(kvsdb)));
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

	// Issue three non-overlapping tasks and check data consistency and correctness
	ACTOR Future<Void> simpleTest(BulkLoading* self, Database cx) {
		TraceEvent("BulkLoadingWorkLoadSimpleTestBegin");
		state int oldBulkLoadMode = 0;
		state std::vector<BulkLoadTaskState> bulkLoadTaskStates;
		state std::vector<KeyRange> taskRanges;
		state std::vector<KeyRange> errorRanges;
		state std::vector<BulkLoadTaskTestUnit> taskUnits;
		state int i = 0;
		for (i = 0; i < 2; i++) {
			std::string indexStr = std::to_string(i);
			std::string indexStrNext = std::to_string(i + 1);
			Key beginKey = StringRef(indexStr);
			Key endKey = StringRef(indexStrNext);
			std::string folderPath = joinPath(simulationBulkLoadFolder, indexStr);
			int dataSize = std::pow(10, deterministicRandom()->randomInt(0, 4));
			BulkLoadTaskTestUnit taskUnit =
			    self->generateBulkLoadTaskUnit(self, folderPath, dataSize, KeyRangeRef(beginKey, endKey));
			bulkLoadTaskStates.push_back(taskUnit.bulkLoadTask);
			taskRanges.push_back(taskUnit.bulkLoadTask.getRange());
			taskUnits.push_back(taskUnit);
			bool succeed = wait(self->submitBulkLoadTask(cx, taskUnit.bulkLoadTask));
			ASSERT(succeed);
		}

		TraceEvent("BulkLoadingWorkLoadSimpleTestIssuedTasks");
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1)));
		TraceEvent("BulkLoadingWorkLoadSimpleTestSetMode").detail("OldMode", oldBulkLoadMode).detail("NewMode", 1);
		std::vector<BulkLoadTaskState> errorTasks = wait(self->waitUntilAllTaskCompleteOrError(self, cx));
		for (const auto& errorTask : errorTasks) {
			errorRanges.push_back(errorTask.getRange());
		}
		TraceEvent("BulkLoadingWorkLoadSimpleTestAllComplete");

		// Check data
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 0)));
		TraceEvent("BulkLoadingWorkLoadSimpleTestSetMode").detail("OldMode", oldBulkLoadMode).detail("NewMode", 0);
		state std::vector<KeyValue> dbkvs = wait(self->getKvsFromDB(self, cx, errorRanges, taskRanges));
		state std::vector<KeyValue> kvs;
		for (int j = 0; j < taskUnits.size(); j++) {
			bool rangeTaskError = false;
			for (const auto& errorRange : errorRanges) {
				if (taskUnits[j].getRange() == errorRange) {
					rangeTaskError = true;
					break;
				}
			}
			if (rangeTaskError) {
				continue; // Ignore error ranges
			}
			kvs.insert(kvs.end(), taskUnits[j].data.begin(), taskUnits[j].data.end());
		}
		ASSERT(self->checkSame(self, kvs, dbkvs));

		// Check bulk load metadata
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1)));
		TraceEvent("BulkLoadingWorkLoadSimpleTestSetMode").detail("OldMode", oldBulkLoadMode).detail("NewMode", 1);
		for (i = 0; i < bulkLoadTaskStates.size(); i++) {
			wait(self->finalizeBulkLoadTask(cx, bulkLoadTaskStates[i].getRange(), bulkLoadTaskStates[i].getTaskId()));
		}
		wait(acknowledgeAllErrorBulkLoadTasks(cx, self->jobId, normalKeys));
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

	ACTOR Future<Void> setKeys(Database cx, std::vector<KeyValue> kvs) {
		state Transaction tr(cx);
		loop {
			try {
				for (const auto& kv : kvs) {
					tr.set(kv.key, kv.value);
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> backgroundWriteTraffic(BulkLoading* self, Database cx) {
		loop {
			int keyCount = deterministicRandom()->randomInt(1, 20);
			std::vector<KeyValue> kvs = self->generateOrderedKVS(self, normalKeys, keyCount);
			wait(self->setKeys(cx, kvs));
			double delayTime = deterministicRandom()->random01() * 5.0;
			wait(delay(delayTime));
		}
	}

	ACTOR Future<Void> complexTest(BulkLoading* self, Database cx) {
		state KeyRangeMap<Optional<BulkLoadTaskTestUnit>> taskMap;
		taskMap.insert(allKeys, Optional<BulkLoadTaskTestUnit>());
		state int i = 0;
		state int oldBulkLoadMode = 0;
		state BulkLoadTaskTestUnit taskUnit;
		state std::vector<KeyRange> outdatedRanges;
		state std::vector<KeyRange> errorRanges;

		// Run tasks
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1)));
		TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", oldBulkLoadMode).detail("NewMode", 1);
		for (; i < 3; i++) {
			std::string folderPath = joinPath(simulationBulkLoadFolder, std::to_string(i));
			int dataSize = std::pow(10, deterministicRandom()->randomInt(0, 4));
			taskUnit = self->generateBulkLoadTaskUnit(self, folderPath, dataSize);
			ASSERT(normalKeys.contains(taskUnit.bulkLoadTask.getRange()));
			bool succeed = wait(self->submitBulkLoadTask(cx, taskUnit.bulkLoadTask));
			if (succeed) {
				taskMap.insert(taskUnit.bulkLoadTask.getRange(), taskUnit);
			}
			if (deterministicRandom()->coinflip()) {
				std::vector<BulkLoadTaskState> errorTasks = wait(self->waitUntilAllTaskCompleteOrError(self, cx));
			}
			if (deterministicRandom()->coinflip()) {
				wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 0)));
				TraceEvent("BulkLoadingWorkLoadComplexTestSetMode")
				    .detail("OldMode", oldBulkLoadMode)
				    .detail("NewMode", 0);
				wait(delay(deterministicRandom()->random01() * 5));
				wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1)));
				TraceEvent("BulkLoadingWorkLoadComplexTestSetMode")
				    .detail("OldMode", oldBulkLoadMode)
				    .detail("NewMode", 1);
			}
			if (deterministicRandom()->coinflip()) {
				wait(delay(deterministicRandom()->random01() * 5));
			}
		}
		// Wait until all tasks have completed
		std::vector<BulkLoadTaskState> errorTasks = wait(self->waitUntilAllTaskCompleteOrError(self, cx));
		for (const auto& errorTask : errorTasks) {
			errorRanges.push_back(errorTask.getRange()); // for any error range, do not check data
		}
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 0))); // trigger DD restart
		TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", oldBulkLoadMode).detail("NewMode", 0);

		// Check correctness
		state std::vector<KeyValue> kvs;
		state std::vector<BulkLoadTaskState> bulkLoadTaskStates;
		state std::vector<KeyRange> completeTaskRanges;
		for (auto& range : taskMap.ranges()) {
			if (!range.value().present()) {
				continue;
			}
			// Check if the task is outdated
			if (range.value().get().bulkLoadTask.getRange() != range.range()) {
				ASSERT(range.value().get().bulkLoadTask.getRange().contains(range.range()));
				outdatedRanges.push_back(range.range());
				if (self->debugging) {
					TraceEvent("DebugBulkLoadOutdateTask").detail("Task", range.value().get().bulkLoadTask.toString());
				}
				continue;
			}
			// Check if the range is error
			bool taskError = false;
			for (const auto& errorRange : errorRanges) {
				if (errorRange == range.range()) {
					taskError = true;
				}
			}
			if (taskError) {
				if (self->debugging) {
					TraceEvent("DebugBulkLoadErrorTask").detail("Task", range.value().get().bulkLoadTask.toString());
				}
				bulkLoadTaskStates.push_back(range.value().get().bulkLoadTask);
				continue;
			}
			completeTaskRanges.push_back(range.range());
			std::vector<KeyValue> kvsToCheck = range.value().get().data;
			kvs.insert(std::end(kvs), std::begin(kvsToCheck), std::end(kvsToCheck));
			bulkLoadTaskStates.push_back(range.value().get().bulkLoadTask);
		}
		std::vector<KeyRange> ignoreRanges;
		ignoreRanges.reserve(outdatedRanges.size() + errorRanges.size());
		ignoreRanges.insert(ignoreRanges.end(), outdatedRanges.begin(), outdatedRanges.end());
		ignoreRanges.insert(ignoreRanges.end(), errorRanges.begin(), errorRanges.end());
		std::vector<KeyValue> dbkvs = wait(self->getKvsFromDB(self, cx, ignoreRanges, completeTaskRanges));
		ASSERT(self->checkSame(self, kvs, dbkvs));

		// Clear metadata
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1)));
		TraceEvent("BulkLoadingWorkLoadComplexTestSetMode").detail("OldMode", oldBulkLoadMode).detail("NewMode", 1);
		for (i = 0; i < bulkLoadTaskStates.size(); i++) {
			wait(self->finalizeBulkLoadTask(cx, bulkLoadTaskStates[i].getRange(), bulkLoadTaskStates[i].getTaskId()));
		}
		wait(acknowledgeAllErrorBulkLoadTasks(cx, self->jobId, normalKeys));
		loop {
			bool cleared = wait(self->checkBulkLoadMetadataCleared(self, cx));
			if (cleared) {
				break;
			}
			wait(delay(1.0));
		}

		// Make sure all ranges locked by the workload are unlocked
		std::vector<std::pair<KeyRange, RangeLockState>> res =
		    wait(findExclusiveReadLockOnRange(cx, normalKeys, rangeLockNameForBulkLoad));
		ASSERT(res.empty());
		TraceEvent("BulkLoadingWorkLoadComplexTestComplete");
		return Void();
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

		if (self->initializeBulkLoadMetadata) {
			wait(self->clearAllBulkLoadTask(cx));
		}

		// Run background traffic
		if (self->backgroundTrafficEnabled) {
			state std::vector<Future<Void>> trafficActors;
			int actorCount = deterministicRandom()->randomInt(1, 10);
			for (int i = 0; i < actorCount; i++) {
				trafficActors.push_back(self->backgroundWriteTraffic(self, cx));
			}
		}

		wait(registerRangeLockOwner(cx, rangeLockNameForBulkLoad, rangeLockNameForBulkLoad));

		std::vector<RangeLockOwner> lockOwners = wait(getAllRangeLockOwners(cx));
		ASSERT(lockOwners.size() == 1 && lockOwners[0].getOwnerUniqueId() == rangeLockNameForBulkLoad);

		// Run test
		if (deterministicRandom()->coinflip()) {
			// Inject data to three non-overlapping ranges
			wait(self->simpleTest(self, cx));
		} else {
			// Inject data to many ranges and those ranges can be overlapping
			wait(self->complexTest(self, cx));
		}

		wait(removeRangeLockOwner(cx, rangeLockNameForBulkLoad));

		std::vector<RangeLockOwner> lockOwnersAfterRemove = wait(getAllRangeLockOwners(cx));
		ASSERT(lockOwnersAfterRemove.empty());

		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
