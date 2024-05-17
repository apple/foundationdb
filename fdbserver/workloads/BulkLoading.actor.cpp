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
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BulkLoading : TestWorkload {
	static constexpr auto NAME = "BulkLoadingWorkload";
	const bool enabled;
	bool pass;

	BulkLoading(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	void addBulkLoadTask(ReadYourWritesTransaction* tr, KeyRange range, std::string path) {
		std::set<std::string> paths;
		paths.insert(path);
		BulkLoadState bulkLoadState(range, BulkLoadType::ShardedRocksDB, paths);
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

	ACTOR Future<Void> issueTransactions(BulkLoading* self, Database cx) {
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
	}

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

	ACTOR Future<Void> issueBulkLoadTasks(BulkLoading* self,
	                                      Database cx,
	                                      std::vector<std::pair<KeyRange, std::string>> tasks) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				for (const auto& task : tasks) {
					self->addBulkLoadTask(&tr, task.first, task.second);
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> _start(BulkLoading* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		wait(self->setBulkLoadMode(cx, /*on=*/true));

		std::vector<std::pair<KeyRange, std::string>> tasks;
		tasks.push_back(std::make_pair(Standalone(KeyRangeRef("1"_sr, "2"_sr)), "1"));
		tasks.push_back(std::make_pair(Standalone(KeyRangeRef("2"_sr, "3"_sr)), "2"));
		tasks.push_back(std::make_pair(Standalone(KeyRangeRef("3"_sr, "4"_sr)), "3"));

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

		return Void();
	}
};

WorkloadFactory<BulkLoading> BulkLoadingFactory;
