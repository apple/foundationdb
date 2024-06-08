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

	ACTOR Future<Void> setBulkLoadMode(Database cx, bool on) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.set(bulkLoadModeKey, BinaryWriter::toValue(on ? "1"_sr : "0"_sr, Unversioned()));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> issueBulkLoadTasks(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
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

	std::vector<KeyValue> generateRandomData(KeyRange range, int iterations, std::vector<Key> keyCandidates) {
		std::set<Key> keys;
		for (int i = 0; i < iterations; i++) {
			Key key = deterministicRandom()->randomChoice(keyCandidates);
			if (range.contains(key)) {
				keys.insert(key);
			}
		}
		std::vector<KeyValue> res;
		for (const auto& key : keys) {
			UID randomId = deterministicRandom()->randomUniqueID();
			Value val = Standalone(StringRef(randomId.toString()));
			res.push_back(Standalone(KeyValueRef(key, val)));
		}
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

	BulkLoadState newBulkLoadTask(KeyRange range,
	                              std::string folder,
	                              std::string dataFile,
	                              std::string bytesSampleFile) {
		BulkLoadState bulkLoadTask(range, BulkLoadType::ShardedRocksDB, folder);
		ASSERT(bulkLoadTask.addDataFile(joinPath(folder, dataFile)));
		ASSERT(bulkLoadTask.setByteSampleFile(joinPath(folder, bytesSampleFile)));
		ASSERT(bulkLoadTask.setTransportMethod(BulkLoadTransportMethod::CP)); // local file copy
		return bulkLoadTask;
	}

	std::vector<Key> getRandomKeyList() {
		int keyListCount = 1000000;
		std::vector<Key> keyList;
		std::vector<Key> keyCharList = { "0"_sr, "1"_sr, "2"_sr, "3"_sr, "4"_sr, "5"_sr };
		for (int i = 0; i < keyListCount; i++) {
			Key key = ""_sr;
			int keyLength = deterministicRandom()->randomInt(1, 100);
			for (int j = 0; j < keyLength; j++) {
				Key appendedItem = deterministicRandom()->randomChoice(keyCharList);
				key = key.withSuffix(appendedItem);
			}
			keyList.push_back(key);
		}
		return keyList;
	}

	ACTOR Future<Void> _start(BulkLoading* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		std::vector<Key> keyCandidates = self->getRandomKeyList();

		std::vector<Key> keyList = self->getRandomKeyList();
		std::string dataFileName = generateRandomBulkLoadDataFileName();
		std::string bytesSampleFileName = generateRandomBulkLoadBytesSampleFileName();
		KeyRange range = Standalone(KeyRangeRef("1"_sr, "2"_sr));
		std::string folder = joinPath(simulationBulkLoadFolder, "1");

		state BulkLoadTaskTestUnit taskUnit;
		taskUnit.bulkLoadTask = self->newBulkLoadTask(range, folder, dataFileName, bytesSampleFileName);
		taskUnit.data = self->generateRandomData(range, 10000, keyCandidates); // TODO(Zhe): increase size
		self->produceFilesToLoad(taskUnit);

		wait(self->setBulkLoadMode(cx, /*on=*/true));
		wait(self->issueBulkLoadTasks(self, cx, { taskUnit.bulkLoadTask }));
		TraceEvent("BulkLoadingWorkLoadIssuedTasks");
		wait(self->waitUntilAllComplete(self, cx));
		TraceEvent("BulkLoadingWorkLoadAllComplete");
		wait(self->setBulkLoadMode(cx, /*on=*/false));

		wait(self->checkData(cx, taskUnit.data));
		TraceEvent("BulkLoadingWorkLoadCheckedData");

		TraceEvent("BulkLoadingWorkLoadComplete");

		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
