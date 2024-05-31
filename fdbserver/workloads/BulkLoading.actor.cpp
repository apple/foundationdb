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
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define PERSIST_PREFIX "\xff\xff"
static const KeyRangeRef persistByteSampleKeys = KeyRangeRef(PERSIST_PREFIX "BS/"_sr, PERSIST_PREFIX "BS0"_sr);
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
					TraceEvent("BulkLoadingIssueBulkLoadTask")
					    .detail("Range", task.range)
					    .detail("BulkLoadState", task.toString());
					wait(krmSetRange(&tr, bulkLoadPrefix, task.range, bulkLoadStateValue(task)));
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	std::set<Key> produceDataToLoad(KeyRange range,
	                                std::string folder,
	                                int count,
	                                std::vector<Key> keyList,
	                                Value valueToLoad) {
		std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
		std::set<Key> res;
		res.insert(range.begin);
		for (int i = 0; i < count; i++) {
			Key key = deterministicRandom()->randomChoice(keyList);
			if (range.contains(key)) {
				res.insert(key);
			}
		}
		platform::eraseDirectoryRecursive(folder);
		ASSERT(platform::createDirectory(folder));
		sstWriter->open(folder + "data.sst");
		std::vector<KeyValue> sampleData;
		for (const auto& key : res) {
			KeyValue kv = Standalone(KeyValueRef(key, valueToLoad));
			ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
			if (sampleInfo.inSample) {
				Key sampleKey = kv.key.withPrefix(persistByteSampleKeys.begin);
				Value sampleValue = BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned());
				sampleData.push_back(Standalone(KeyValueRef(sampleKey, sampleValue)));
			}
			sstWriter->write(kv.key, kv.value);
		}
		TraceEvent("BulkLoadingProduceKeySample")
		    .detail("Keys", res.size())
		    .detail("Size", sampleData.size())
		    .detail("Folder", folder);
		bool anyFileCreated = sstWriter->finish();
		ASSERT(anyFileCreated);
		if (sampleData.size() > 0) {
			sstWriter->open(folder + "bytesample.sst");
			for (const auto& kv : sampleData) {
				sstWriter->write(kv.key, kv.value);
			}
			anyFileCreated = sstWriter->finish();
			ASSERT(anyFileCreated);
		}
		TraceEvent("BulkLoadingProduceDataToLoad")
		    .detail("Range", range)
		    .detail("Folder", folder)
		    .detail("LoadKeyCount", res.size())
		    .detail("CandidateKeyCount", keyList.size());
		return res;
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

	ACTOR Future<Void> checkData(Database cx, std::set<Key> keys, Value valueToLoad) {
		state Key keyRead;
		state Transaction tr(cx);
		for (const auto& key : keys) {
			try {
				keyRead = key;
				Optional<Value> value = wait(tr.get(key));
				if (!value.present() || value.get() != valueToLoad) {
					TraceEvent(SevError, "BulkLoadingWorkLoadValueError")
					    .detail("Version", tr.getReadVersion().get())
					    .detail("ToCheckCount", keys.size())
					    .detail("Key", keyRead.toString())
					    .detail("Value", value.present() ? value.get().toString() : "None");
					return Void();
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	BulkLoadState newBulkLoadTask(KeyRange range, std::string folder) {
		BulkLoadState bulkLoadTask(range, BulkLoadType::ShardedRocksDB, folder);
		// bulkLoadTask.setTaskId(deterministicRandom()->randomUniqueID());
		ASSERT(bulkLoadTask.addDataFile(folder + "data.sst"));
		ASSERT(bulkLoadTask.addByteSampleFile(folder + "bytesample.sst"));
		return bulkLoadTask;
	}

	std::vector<Key> getRandomKeyList() {
		int keyListCount = 100000;
		std::vector<Key> keyList;
		std::vector<Key> keyCharList = { "0"_sr, "1"_sr, "2"_sr, "3"_sr, "4"_sr, "5"_sr };
		for (int i = 0; i < keyListCount; i++) {
			Key key = ""_sr;
			int keyLength = deterministicRandom()->randomInt(1, 10);
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

		state Value valueToLoad = "1123456789043253654637567486425423532"_sr;

		state KeyRange range1 = Standalone(KeyRangeRef("1"_sr, "2"_sr));
		state KeyRange range2 = Standalone(KeyRangeRef("2"_sr, "3"_sr));
		state KeyRange range3 = Standalone(KeyRangeRef("3"_sr, "4"_sr));
		state std::string folder1 = "1/";
		state std::string folder2 = "2/";
		state std::string folder3 = "3/";

		int sampleCount = 10000;
		std::vector<Key> keyList = self->getRandomKeyList();
		state std::set<Key> data1 = self->produceDataToLoad(range1, folder1, sampleCount, keyList, valueToLoad);
		state std::set<Key> data2 = self->produceDataToLoad(range2, folder2, sampleCount, keyList, valueToLoad);
		state std::set<Key> data3 = self->produceDataToLoad(range3, folder3, sampleCount, keyList, valueToLoad);

		wait(self->setBulkLoadMode(cx, /*on=*/true));
		std::vector<BulkLoadState> tasks;
		tasks.push_back(self->newBulkLoadTask(range1, folder1));
		tasks.push_back(self->newBulkLoadTask(range2, folder2));
		tasks.push_back(self->newBulkLoadTask(range3, folder3));
		wait(self->issueBulkLoadTasks(self, cx, tasks));
		wait(self->waitUntilAllComplete(self, cx));
		wait(self->setBulkLoadMode(cx, /*on=*/false));

		wait(self->checkData(cx, data1, valueToLoad));
		wait(self->checkData(cx, data2, valueToLoad));
		wait(self->checkData(cx, data3, valueToLoad));

		TraceEvent("BulkLoadingWorkLoadComplete");

		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
