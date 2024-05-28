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

	void addBulkLoadTask(ReadYourWritesTransaction* tr, BulkLoadState bulkLoadState) {
		try {
			tr->set("\xff\xff/bulk_loading/task/"_sr, bulkLoadStateValue(bulkLoadState));
			TraceEvent("BulkLoadWorkloadAddTask").detail("Task", bulkLoadState.toString());
		} catch (Error& e) {
			TraceEvent("BulkLoadWorkloadAddTaskFailed").detail("Task", bulkLoadState.toString());
			throw e;
		}
	}

	std::string parseReadRangeResult(RangeResult input) {
		std::string res;
		for (int i = 0; i < input.size() - 1; i++) {
			if (!input[i].value.empty()) {
				BulkLoadState bulkLoadState = decodeBulkLoadState(input[i].value);
				ASSERT(bulkLoadState.isValid());
				if (bulkLoadState.range != Standalone(KeyRangeRef(input[i].key, input[i + 1].key))
				                               .removePrefix("\xff\xff/bulk_loading/status/"_sr)) {
					res = res + "[Not aligned] ";
				}
				res = res + bulkLoadState.toString() + "; ";
			}
		}
		return res;
	}

	bool allComplete(RangeResult input) {
		for (int i = 0; i < input.size() - 1; i++) {
			if (!input[i].value.empty()) {
				BulkLoadState bulkLoadState = decodeBulkLoadState(input[i].value);
				ASSERT(bulkLoadState.isValid());
				if (bulkLoadState.phase != BulkLoadPhase::Complete) {
					return false;
				}
			}
		}
		return true;
	}

	/*ACTOR Future<Void> issueTransactions(BulkLoading* self, Database cx) {
	    state ReadYourWritesTransaction tr(cx);
	    state Version version = wait(tr.getReadVersion());
	    TraceEvent("BulkLoadWorkloadStart").detail("Version", version);
	    state KeyRange range1 =
	        Standalone(KeyRangeRef("\xff\xff/bulk_loading/status/a"_sr, "\xff\xff/bulk_loading/status/b"_sr));
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    RangeResult res1 = wait(tr.getRange(range1, GetRangeLimits()));
	    TraceEvent("BulkLoadWorkloadReadRange").detail("Range", range1).detail("Res", self->parseReadRangeResult(res1));
	    try {
	        self->addBulkLoadTask(&tr, Standalone(KeyRangeRef(""_sr, "\xff/2"_sr)), "x");
	        ASSERT(false);
	    } catch (Error& e) {
	        ASSERT(e.code() == error_code_bulkload_add_task_input_error);
	    }
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("1"_sr, "2"_sr)), "1");
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("2"_sr, "3"_sr)), "2");
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("3"_sr, "4"_sr)), "3");
	    try {
	        self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("2"_sr, "5"_sr)), "2");
	        ASSERT(false);
	    } catch (Error& e) {
	        TraceEvent("BulkLoadWorkloadAddTaskFailed").errorUnsuppressed(e);
	        ASSERT(e.code() == error_code_bulkload_add_task_input_error);
	    }
	    wait(delay(1.0));
	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");

	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("1"_sr, "2"_sr)), "1");
	    try {
	        wait(tr.commit());
	        ASSERT(false);
	    } catch (Error& e) {
	        ASSERT(e.code() == error_code_bulkload_is_off_when_commit_task);
	    }
	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");

	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    tr.set("\xff\xff/bulk_loading/mode"_sr, BinaryWriter::toValue("1"_sr, Unversioned()));
	    wait(tr.commit());
	    TraceEvent("BulkLoadWorkloadEnableBulkLoad");
	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");

	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    ASSERT(!tr.getReadVersion().isReady());
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("1"_sr, "2"_sr)), "1");
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("2"_sr, "3"_sr)), "2");
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("3"_sr, "4"_sr)), "3");
	    wait(tr.commit());
	    TraceEvent("BulkLoadWorkloadTransactionCommitted")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("CommitVersion", tr.getCommittedVersion());

	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("11"_sr, "2"_sr)), "4");
	    try {
	        wait(tr.commit());
	        ASSERT(false);
	    } catch (Error& e) {
	        TraceEvent("BulkLoadWorkloadAddTaskFailed").errorUnsuppressed(e);
	        ASSERT(e.code() == error_code_bulkload_task_conflict);
	    }

	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

	    state KeyRange range3 =
	        Standalone(KeyRangeRef("\xff\xff/bulk_loading/status/"_sr, "\xff\xff/bulk_loading/status/\xff"_sr));
	    RangeResult res3 = wait(tr.getRange(range3, GetRangeLimits()));
	    TraceEvent("BulkLoadWorkloadReadRange")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("Range", range3)
	        .detail("Res", self->parseReadRangeResult(res3));

	    state KeyRange range4 =
	        Standalone(KeyRangeRef("\xff\xff/bulk_loading/status/2"_sr, "\xff\xff/bulk_loading/status/3"_sr));
	    RangeResult res4 = wait(tr.getRange(range4, GetRangeLimits()));
	    TraceEvent("BulkLoadWorkloadReadRange")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("Range", range4)
	        .detail("Res", self->parseReadRangeResult(res4));

	    state KeyRange range5 =
	        Standalone(KeyRangeRef("\xff\xff/bulk_loading/status/11"_sr, "\xff\xff/bulk_loading/status/12"_sr));
	    RangeResult res5 = wait(tr.getRange(range5, GetRangeLimits()));
	    TraceEvent("BulkLoadWorkloadReadRange")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("Range", range5)
	        .detail("Res", self->parseReadRangeResult(res5));

	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

	    state KeyRange range6 =
	        Standalone(KeyRangeRef("\xff\xff/bulk_loading/cancel/11"_sr, "\xff\xff/bulk_loading/cancel/2"_sr));
	    tr.clear(range6);
	    wait(tr.commit());
	    TraceEvent("BulkLoadWorkloadClearRange")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("CommitVersion", tr.getCommittedVersion())
	        .detail("Range", range6);

	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    RangeResult res7 = wait(tr.getRange(range3, GetRangeLimits()));
	    TraceEvent("BulkLoadWorkloadReadRange")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("Range", range3)
	        .detail("Res", self->parseReadRangeResult(res7));

	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    self->addBulkLoadTask(&tr, Standalone(KeyRangeRef("11"_sr, "2"_sr)), "5");

	    try {
	        RangeResult res8 = wait(tr.getRange(range3, GetRangeLimits()));
	        ASSERT(false);
	    } catch (Error& e) {
	        TraceEvent("BulkLoadWorkloadReadRangeError").errorUnsuppressed(e);
	        ASSERT(e.code() == error_code_bulkload_check_status_input_error);
	    }

	    wait(tr.commit());
	    TraceEvent("BulkLoadWorkloadTransactionCommitted")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("CommitVersion", tr.getCommittedVersion());

	    tr.reset();
	    TraceEvent("BulkLoadWorkloadTransactionReset");
	    tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	    tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	    tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	    RangeResult res9 = wait(tr.getRange(range3, GetRangeLimits()));
	    TraceEvent("BulkLoadWorkloadReadRange")
	        .detail("AtVersion", tr.getReadVersion().get())
	        .detail("Range", range3)
	        .detail("Res", self->parseReadRangeResult(res9));

	    loop {
	        try {
	            tr.reset();
	            tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	            tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	            tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	            RangeResult res10 = wait(tr.getRange(range3, GetRangeLimits()));
	            if (self->allComplete(res10)) {
	                break;
	            }
	            wait(delay(10.0));
	        } catch (Error& e) {
	            wait(tr.onError(e));
	        }
	    }

	    return Void();
	}*/

	ACTOR Future<Void> setBulkLoadMode(Database cx, bool on) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.set("\xff\xff/bulk_loading/mode"_sr, BinaryWriter::toValue(on ? "1"_sr : "0"_sr, Unversioned()));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> issueBulkLoadTasks(BulkLoading* self, Database cx, std::vector<BulkLoadState> tasks) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				for (const auto& task : tasks) {
					self->addBulkLoadTask(&tr, task);
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

	ACTOR Future<Void> _start(BulkLoading* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		state int sampleCount = 10000;
		state int keyListCount = 100000;
		state Value valueToLoad = "1123456789043253654637567486425423532"_sr;
		state std::vector<Key> keyList;
		state std::vector<Key> keyCharList = { "0"_sr, "1"_sr, "2"_sr, "3"_sr, "4"_sr, "5"_sr };
		for (int i = 0; i < keyListCount; i++) {
			Key key = ""_sr;
			int keyLength = deterministicRandom()->randomInt(1, 10);
			for (int j = 0; j < keyLength; j++) {
				Key appendedItem = deterministicRandom()->randomChoice(keyCharList);
				key = key.withSuffix(appendedItem);
			}
			keyList.push_back(key);
		}

		state KeyRange range1 = Standalone(KeyRangeRef("1"_sr, "2"_sr));
		state KeyRange range2 = Standalone(KeyRangeRef("2"_sr, "3"_sr));
		state KeyRange range3 = Standalone(KeyRangeRef("3"_sr, "4"_sr));

		state std::string folder1 = "1/";
		state std::string folder2 = "2/";
		state std::string folder3 = "3/";

		state std::set<Key> data1 = self->produceDataToLoad(range1, folder1, sampleCount, keyList, valueToLoad);
		state std::set<Key> data2 = self->produceDataToLoad(range2, folder2, sampleCount, keyList, valueToLoad);
		state std::set<Key> data3 = self->produceDataToLoad(range3, folder3, sampleCount, keyList, valueToLoad);

		wait(self->setBulkLoadMode(cx, /*on=*/true));

		std::vector<BulkLoadState> tasks;
		BulkLoadState bulkLoadTask1(range1, BulkLoadType::ShardedRocksDB, folder1);
		bulkLoadTask1.setTaskId(deterministicRandom()->randomUniqueID());
		ASSERT(bulkLoadTask1.addDataFile(folder1 + "data.sst"));
		ASSERT(bulkLoadTask1.addByteSampleFile(folder1 + "bytesample.sst"));
		tasks.push_back(bulkLoadTask1);

		BulkLoadState bulkLoadTask2(range2, BulkLoadType::ShardedRocksDB, folder2);
		bulkLoadTask2.setTaskId(deterministicRandom()->randomUniqueID());
		ASSERT(bulkLoadTask2.addDataFile(folder2 + "data.sst"));
		ASSERT(bulkLoadTask2.addByteSampleFile(folder2 + "bytesample.sst"));
		tasks.push_back(bulkLoadTask2);

		BulkLoadState bulkLoadTask3(range3, BulkLoadType::ShardedRocksDB, folder3);
		bulkLoadTask3.setTaskId(deterministicRandom()->randomUniqueID());
		ASSERT(bulkLoadTask3.addDataFile(folder3 + "data.sst"));
		ASSERT(bulkLoadTask3.addByteSampleFile(folder3 + "bytesample.sst"));
		tasks.push_back(bulkLoadTask3);

		wait(self->issueBulkLoadTasks(self, cx, tasks));

		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state KeyRange range =
				    Standalone(KeyRangeRef("\xff\xff/bulk_loading/status/"_sr, "\xff\xff/bulk_loading/status/\xff"_sr));
				RangeResult res = wait(tr.getRange(range, GetRangeLimits()));
				if (self->allComplete(res)) {
					break;
				}
				wait(delay(10.0));
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		wait(self->setBulkLoadMode(cx, /*on=*/false));

		tr.reset();
		for (const auto& key : data1) {
			try {
				Optional<Value> value = wait(tr.get(key));
				ASSERT(value.present() && value.get() == valueToLoad);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("BulkLoadingWorkLoadComplete");

		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
