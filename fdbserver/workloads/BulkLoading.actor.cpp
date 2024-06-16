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

const std::string simulationBulkLoadFolder = "bulkLoad";

struct BulkLoadTaskTestUnit {
	BulkLoadState bulkLoadTask;
	std::vector<KeyValue> data;
	BulkLoadTaskTestUnit() = default;
};

struct BulkLoading : TestWorkload {
	static constexpr auto NAME = "BulkLoadingWorkload";
	const bool enabled;
	bool pass;

	// We disable failure injection because there is an irrelevant issue:
	// Remote tLog is failed to rejoin to CC
	// Once this issue is fixed, we should be able to enable the failure injection
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert({ "Attrition" }); }

	BulkLoading(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	bool allComplete(RangeResult input) {
		TraceEvent e("BulkLoadingCheckStatusAllComplete");
		bool res = true;
		for (int i = 0; i < input.size() - 1; i++) {
			TraceEvent e("BulkLoadingCheckStatus");
			e.detail("Range", Standalone(KeyRangeRef(input[i].key, input[i + 1].key)));
			if (!input[i].value.empty()) {
				BulkLoadState bulkLoadState = decodeBulkLoadState(input[i].value);
				ASSERT(bulkLoadState.isValid());
				e.detail("BulkLoadState", bulkLoadState.toString());
				if (bulkLoadState.phase != BulkLoadPhase::Complete) {
					res = false;
					e.detail("Status", "Running");
				} else {
					e.detail("Status", "Complete");
				}
			} else {
				e.detail("Status", "N/A");
			}
		}
		return res;
	}

	ACTOR Future<Void> issueBulkLoadTasksFdbcli(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
		state int i = 0;
		for (; i < tasks.size(); i++) {
			loop {
				try {
					wait(submitBulkLoadTask(cx->getConnectionRecord(), tasks[i], /*timeoutSecond=*/300));
					TraceEvent("BulkLoadingIssueBulkLoadTask").detail("BulkLoadStates", describe(tasks[i]));
					break;
				} catch (Error& e) {
					TraceEvent("BulkLoadingIssueBulkLoadTaskError")
					    .errorUnsuppressed(e)
					    .detail("BulkLoadStates", describe(tasks));
					wait(delay(5.0));
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> issueBulkLoadTasksTr(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				for (const auto& task : tasks) {
					wait(krmSetRange(&tr, bulkLoadPrefix, task.range, bulkLoadStateValue(task)));
				}
				wait(tr.commit());
				TraceEvent("BulkLoadingIssueBulkLoadTask").detail("BulkLoadStates", describe(tasks));
				break;
			} catch (Error& e) {
				TraceEvent("BulkLoadingIssueBulkLoadTaskError")
				    .errorUnsuppressed(e)
				    .detail("BulkLoadStates", describe(tasks));
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> issueBulkLoadTasks(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
		if (deterministicRandom()->coinflip()) {
			wait(self->issueBulkLoadTasksTr(self, cx, tasks));
		} else {
			wait(self->issueBulkLoadTasksFdbcli(self, cx, tasks));
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
			Key key = getRandomKey(keyCharList, 1, 100);
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

	void produceFilesToLoad(const BulkLoadTaskTestUnit& task) {
		std::string folder = task.bulkLoadTask.folder;
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		std::string bytesSampleFile = task.bulkLoadTask.bytesSampleFile.get();
		std::string dataFile = *(task.bulkLoadTask.dataFiles.begin());

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
						if (bulkLoadState.phase != BulkLoadPhase::Complete) {
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

	ACTOR Future<Void> checkData(Database cx, std::vector<KeyValue> kvs) {
		state Key keyRead;
		state Transaction tr(cx);
		state int i = 0;
		loop {
			try {
				Optional<Value> value = wait(tr.get(kvs[i].key));
				if (!value.present() || value.get() != kvs[i].value) {
					TraceEvent(SevError, "BulkLoadingWorkLoadValueError")
					    .detail("Version", tr.getReadVersion().get())
					    .detail("ToCheckCount", kvs.size())
					    .detail("Key", kvs[i].key.toString())
					    .detail("ExpectedValue", kvs[i].value.toString())
					    .detail("Value", value.present() ? value.get().toString() : "None");
				}
				i = i + 1;
				if (i >= kvs.size()) {
					break;
				}
			} catch (Error& e) {
				TraceEvent(SevInfo, "BulkLoadingWorkLoadValueError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	BulkLoadTaskTestUnit produceBulkLoadTaskUnit(BulkLoading* self,
	                                             const std::vector<Key>& keyCharList,
	                                             KeyRange range,
	                                             std::string folderName) {
		std::string dataFileName = generateRandomBulkLoadDataFileName();
		std::string bytesSampleFileName = generateRandomBulkLoadBytesSampleFileName();
		std::string folder = joinPath(simulationBulkLoadFolder, folderName);
		BulkLoadTaskTestUnit taskUnit;
		taskUnit.bulkLoadTask = newBulkLoadTaskLocalSST(
		    range, folder, joinPath(folder, dataFileName), joinPath(folder, bytesSampleFileName));
		size_t dataSize = deterministicRandom()->randomInt(10, 100);
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

	void produceLargeDataToLoad(const BulkLoadTaskTestUnit& task, int count) {
		std::string folder = task.bulkLoadTask.folder;
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		std::string bytesSampleFile = task.bulkLoadTask.bytesSampleFile.get();
		std::string dataFile = *(task.bulkLoadTask.dataFiles.begin());

		std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
		sstWriter->open(abspath(dataFile));
		std::vector<KeyValue> bytesSample;
		int insertedKeyCount = 0;
		for (int i = 0; i < 10; i++) {
			std::string idxStr = std::to_string(i);
			Key prefix = Standalone(StringRef(idxStr)).withPrefix(task.bulkLoadTask.range.begin);
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
		std::string dataFileName = generateRandomBulkLoadDataFileName();
		std::string bytesSampleFileName = generateRandomBulkLoadBytesSampleFileName();
		std::string folder = joinPath(simulationBulkLoadFolder, folderName);
		BulkLoadTaskTestUnit taskUnit;
		taskUnit.bulkLoadTask = newBulkLoadTaskLocalSST(range, folder, dataFileName, bytesSampleFileName);
		self->produceLargeDataToLoad(taskUnit, 5000000);
		return;
	}

	ACTOR Future<Void> _start(BulkLoading* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		if (g_network->isSimulated()) {
			disableConnectionFailures("BulkLoading");
		}

		std::vector<Key> keyCharList = { "0"_sr, "1"_sr, "2"_sr, "3"_sr, "4"_sr, "5"_sr };

		std::string folderName1 = "1";
		KeyRange range1 = Standalone(KeyRangeRef("1"_sr, "2"_sr));
		state BulkLoadTaskTestUnit taskUnit1 = self->produceBulkLoadTaskUnit(self, keyCharList, range1, folderName1);
		std::string folderName2 = "2";
		KeyRange range2 = Standalone(KeyRangeRef("2"_sr, "3"_sr));
		state BulkLoadTaskTestUnit taskUnit2 = self->produceBulkLoadTaskUnit(self, keyCharList, range2, folderName2);
		std::string folderName3 = "4";
		KeyRange range3 = Standalone(KeyRangeRef("4"_sr, "5"_sr));
		state BulkLoadTaskTestUnit taskUnit3 = self->produceBulkLoadTaskUnit(self, keyCharList, range3, folderName3);

		wait(self->issueBulkLoadTasks(
		    self, cx, { taskUnit1.bulkLoadTask, taskUnit2.bulkLoadTask, taskUnit3.bulkLoadTask }));
		int old_ = wait(setBulkLoadMode(cx, 1));
		TraceEvent("BulkLoadingWorkLoadIssuedTasks");
		wait(self->waitUntilAllComplete(self, cx));
		TraceEvent("BulkLoadingWorkLoadAllComplete");
		int old_ = wait(setBulkLoadMode(cx, 0));

		wait(self->checkData(cx, taskUnit1.data));
		wait(self->checkData(cx, taskUnit2.data));
		wait(self->checkData(cx, taskUnit3.data));
		TraceEvent("BulkLoadingWorkLoadCheckedData");

		TraceEvent("BulkLoadingWorkLoadComplete");

		/*std::string folderName1 = "1";
		KeyRange range1 = Standalone(KeyRangeRef("1"_sr, "2"_sr));
		self->produceDataSet(self, range1, folderName1);
		std::string folderName2 = "2";
		KeyRange range2 = Standalone(KeyRangeRef("2"_sr, "3"_sr));
		self->produceDataSet(self, range2, folderName2);
		std::string folderName3 = "4";
		KeyRange range3 = Standalone(KeyRangeRef("4"_sr, "5"_sr));
		self->produceDataSet(self, range3, folderName3);*/
		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
