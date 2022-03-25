/*
 * TaskBucketCorrectness.actor.cpp
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

#include <iostream>
#include <sstream>
#include <cctype>

#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "flow/Error.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/TaskBucket.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SayHelloTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	StringRef getName() const override { return name; };
	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		// check task version
		uint32_t taskVersion = task->getVersion();
		if (taskVersion > SayHelloTaskFunc::version) {
			uint32_t v = SayHelloTaskFunc::version;
			TraceEvent("TaskBucketCorrectnessSayHello")
			    .detail("CheckTaskVersion", "taskVersion is larger than the funcVersion")
			    .detail("TaskVersion", taskVersion)
			    .detail("FuncVersion", v);
		}

		state Reference<TaskFuture> done = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		wait(taskBucket->finish(tr, task));

		if (BUGGIFY)
			wait(delay(10));

		state Key key = StringRef("Hello_" + deterministicRandom()->randomUniqueID().toString());
		state Key value;
		auto itor = task->params.find(LiteralStringRef("name"));
		if (itor != task->params.end()) {
			value = itor->value;
			TraceEvent("TaskBucketCorrectnessSayHello").detail("SayHelloTaskFunc", printable(itor->value));
		} else {
			ASSERT(false);
		}

		if (!task->params[LiteralStringRef("chained")].compare(LiteralStringRef("false"))) {
			wait(done->set(tr, taskBucket));
		} else {
			int subtaskCount = atoi(task->params[LiteralStringRef("subtaskCount")].toString().c_str());
			int currTaskNumber = atoi(value.removePrefix(LiteralStringRef("task_")).toString().c_str());
			TraceEvent("TaskBucketCorrectnessSayHello")
			    .detail("SubtaskCount", subtaskCount)
			    .detail("CurrTaskNumber", currTaskNumber);

			if (currTaskNumber < subtaskCount - 1) {
				state std::vector<Reference<TaskFuture>> vectorFuture;
				auto new_task = makeReference<Task>(SayHelloTaskFunc::name,
				                                    SayHelloTaskFunc::version,
				                                    StringRef(),
				                                    deterministicRandom()->randomInt(0, 2));
				new_task->params[LiteralStringRef("name")] = StringRef(format("task_%d", currTaskNumber + 1));
				new_task->params[LiteralStringRef("chained")] = task->params[LiteralStringRef("chained")];
				new_task->params[LiteralStringRef("subtaskCount")] = task->params[LiteralStringRef("subtaskCount")];
				Reference<TaskFuture> taskDone = futureBucket->future(tr);
				new_task->params[Task::reservedTaskParamKeyDone] = taskDone->key;
				taskBucket->addTask(tr, new_task);
				vectorFuture.push_back(taskDone);
				wait(done->join(tr, taskBucket, vectorFuture));
			} else {
				wait(done->set(tr, taskBucket));
			}
		}

		tr->set(key, value);

		return Void();
	}
};
StringRef SayHelloTaskFunc::name = LiteralStringRef("SayHello");
REGISTER_TASKFUNC(SayHelloTaskFunc);

struct SayHelloToEveryoneTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	StringRef getName() const override { return name; };
	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		Reference<TaskFuture> done = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		state std::vector<Reference<TaskFuture>> vectorFuture;

		int subtaskCount = 1;
		if (!task->params[LiteralStringRef("chained")].compare(LiteralStringRef("false"))) {
			subtaskCount = atoi(task->params[LiteralStringRef("subtaskCount")].toString().c_str());
		}
		for (int i = 0; i < subtaskCount; ++i) {
			auto new_task = makeReference<Task>(
			    SayHelloTaskFunc::name, SayHelloTaskFunc::version, StringRef(), deterministicRandom()->randomInt(0, 2));
			new_task->params[LiteralStringRef("name")] = StringRef(format("task_%d", i));
			new_task->params[LiteralStringRef("chained")] = task->params[LiteralStringRef("chained")];
			new_task->params[LiteralStringRef("subtaskCount")] = task->params[LiteralStringRef("subtaskCount")];
			Reference<TaskFuture> taskDone = futureBucket->future(tr);
			new_task->params[Task::reservedTaskParamKeyDone] = taskDone->key;
			taskBucket->addTask(tr, new_task);
			vectorFuture.push_back(taskDone);
		}

		wait(done->join(tr, taskBucket, vectorFuture));
		wait(taskBucket->finish(tr, task));

		Key key = StringRef("Hello_" + deterministicRandom()->randomUniqueID().toString());
		Value value = LiteralStringRef("Hello, Everyone!");
		TraceEvent("TaskBucketCorrectnessSayHello").detail("SayHelloToEveryoneTaskFunc", printable(value));
		tr->set(key, value);

		return Void();
	}
};
StringRef SayHelloToEveryoneTaskFunc::name = LiteralStringRef("SayHelloToEveryone");
REGISTER_TASKFUNC(SayHelloToEveryoneTaskFunc);

struct SaidHelloTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	StringRef getName() const override { return name; };
	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(taskBucket->finish(tr, task));

		Key key = StringRef("Hello_" + deterministicRandom()->randomUniqueID().toString());
		Value value = LiteralStringRef("Said hello to everyone!");
		TraceEvent("TaskBucketCorrectnessSayHello").detail("SaidHelloTaskFunc", printable(value));
		tr->set(key, value);

		return Void();
	}
};
StringRef SaidHelloTaskFunc::name = LiteralStringRef("SaidHello");
REGISTER_TASKFUNC(SaidHelloTaskFunc);

// A workload which test the correctness of TaskBucket
struct TaskBucketCorrectnessWorkload : TestWorkload {
	bool chained;
	int subtaskCount;

	TaskBucketCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		chained = getOption(options, LiteralStringRef("chained"), false);
		subtaskCount = getOption(options, LiteralStringRef("subtaskCount"), 20);
	}

	std::string description() const override { return "TaskBucketCorrectness"; }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	Future<bool> check(Database const& cx) override { return _check(cx, this); }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> addInitTasks(Reference<ReadYourWritesTransaction> tr,
	                                Reference<TaskBucket> taskBucket,
	                                Reference<FutureBucket> futureBucket,
	                                bool chained,
	                                int subtaskCount) {
		state Key addedInitKey(LiteralStringRef("addedInitTasks"));
		Optional<Standalone<StringRef>> res = wait(tr->get(addedInitKey));
		if (res.present())
			return Void();
		tr->set(addedInitKey, LiteralStringRef("true"));

		Reference<TaskFuture> allDone = futureBucket->future(tr);
		auto task = makeReference<Task>(SayHelloToEveryoneTaskFunc::name,
		                                SayHelloToEveryoneTaskFunc::version,
		                                allDone->key,
		                                deterministicRandom()->randomInt(0, 2));

		task->params[LiteralStringRef("chained")] = chained ? LiteralStringRef("true") : LiteralStringRef("false");
		task->params[LiteralStringRef("subtaskCount")] = StringRef(format("%d", subtaskCount));
		taskBucket->addTask(tr, task);
		auto taskDone = makeReference<Task>(
		    SaidHelloTaskFunc::name, SaidHelloTaskFunc::version, StringRef(), deterministicRandom()->randomInt(0, 2));
		wait(allDone->onSetAddTask(tr, taskBucket, taskDone));
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, TaskBucketCorrectnessWorkload* self) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state Subspace taskSubspace(LiteralStringRef("backup-agent"));
		state Reference<TaskBucket> taskBucket(new TaskBucket(taskSubspace.get(LiteralStringRef("tasks"))));
		state Reference<FutureBucket> futureBucket(new FutureBucket(taskSubspace.get(LiteralStringRef("futures"))));

		try {
			if (self->clientId == 0) {
				TraceEvent("TaskBucketCorrectness").detail("ClearingDb", "...");
				wait(taskBucket->clear(cx));

				TraceEvent("TaskBucketCorrectness").detail("AddingTasks", "...");
				wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
					return self->addInitTasks(tr, taskBucket, futureBucket, self->chained, self->subtaskCount);
				}));

				TraceEvent("TaskBucketCorrectness").detail("RunningTasks", "...");
			}

			loop {
				try {
					bool oneTaskDone = wait(taskBucket->doOne(cx, futureBucket));
					if (!oneTaskDone) {
						bool isEmpty = wait(taskBucket->isEmpty(cx));
						if (isEmpty) {
							wait(delay(5.0));
							state bool isFutureEmpty = wait(futureBucket->isEmpty(cx));
							if (isFutureEmpty)
								break;
							else {
								wait(TaskBucket::debugPrintRange(
								    cx, taskSubspace.key(), StringRef(format("client_%d", self->clientId))));
								TraceEvent("TaskBucketCorrectness").detail("FutureIsNotEmpty", "...");
							}
						} else {
							wait(delay(1.0));
						}
					}
				} catch (Error& e) {
					if (e.code() == error_code_timed_out)
						TraceEvent(SevWarn, "TaskBucketCorrectness").error(e);
					else
						wait(tr->onError(e));
				}
			}

			if (self->clientId == 0) {
				TraceEvent("TaskBucketCorrectness").detail("NotTasksRemain", "...");
				wait(TaskBucket::debugPrintRange(cx, StringRef(), StringRef()));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "TaskBucketCorrectness").error(e);
			wait(tr->onError(e));
		}

		return Void();
	}

	ACTOR Future<bool> _check(Database cx, TaskBucketCorrectnessWorkload* self) {
		bool ret = wait(runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return self->checkSayHello(tr, self->subtaskCount); }));
		return ret;
	}

	ACTOR Future<bool> checkSayHello(Reference<ReadYourWritesTransaction> tr, int subTaskCount) {
		state std::set<std::string> data = { "Hello, Everyone!", "Said hello to everyone!" };
		for (int i = 0; i < subTaskCount; i++) {
			data.insert(format("task_%d", i));
		}

		RangeResult values = wait(tr->getRange(
		    KeyRangeRef(LiteralStringRef("Hello_\x00"), LiteralStringRef("Hello_\xff")), CLIENT_KNOBS->TOO_MANY));
		if (values.size() != data.size()) {
			TraceEvent(SevError, "CheckSayHello")
			    .detail("CountNotMatchIs", values.size())
			    .detail("ShouldBe", data.size());
			for (auto& s : values) {
				TraceEvent("CheckSayHello").detail("Item", printable(s)).detail("Value", printable(s.value));
			}
			return false;
		}

		for (auto& s : values) {
			// TraceEvent("CheckSayHello").detail("Item", printable(s)).detail("Value", printable(s.value));
			data.erase(s.value.toString());
		}
		if (data.size() != 0) {
			TraceEvent(SevError, "CheckSayHello").detail("DataNotMatch", data.size());
			return false;
		}

		return true;
	}
};

WorkloadFactory<TaskBucketCorrectnessWorkload> TaskBucketCorrectnessWorkloadFactory("TaskBucketCorrectness");

void print_subspace_key(const Subspace& subspace, int id) {
	printf("%d==========%s===%d\n", id, printable(StringRef(subspace.key())).c_str(), subspace.key().size());
}

TEST_CASE("/fdbclient/TaskBucket/Subspace") {
	Subspace subspace_test;
	print_subspace_key(subspace_test, 0);
	ASSERT(subspace_test.key().toString() == "");

	Subspace subspace_test1(LiteralStringRef("abc"));
	print_subspace_key(subspace_test1, 1);
	ASSERT(subspace_test1.key() == LiteralStringRef("abc"));

	Tuple t;
	t.append(LiteralStringRef("user"));
	Subspace subspace_test2(t);
	print_subspace_key(subspace_test2, 2);
	ASSERT(subspace_test2.key() == LiteralStringRef("\x01user\x00"));

	Subspace subspace_test3(t, LiteralStringRef("abc"));
	print_subspace_key(subspace_test3, 3);
	ASSERT(subspace_test3.key() == LiteralStringRef("abc\x01user\x00"));

	Tuple t1;
	t1.append(1);
	Subspace subspace_test4(t1);
	print_subspace_key(subspace_test4, 4);
	ASSERT(subspace_test4.key() == LiteralStringRef("\x15\x01"));

	t.append(123);
	Subspace subspace_test5(t, LiteralStringRef("abc"));
	print_subspace_key(subspace_test5, 5);
	ASSERT(subspace_test5.key() == LiteralStringRef("abc\x01user\x00\x15\x7b"));

	// Subspace pack
	printf("%d==========%s===%d\n", 6, printable(subspace_test5.pack(t)).c_str(), subspace_test5.pack(t).size());
	ASSERT(subspace_test5.pack(t) == LiteralStringRef("abc\x01user\x00\x15\x7b\x01user\x00\x15\x7b"));

	printf("%d==========%s===%d\n", 7, printable(subspace_test5.pack(t1)).c_str(), subspace_test5.pack(t1).size());
	ASSERT(subspace_test5.pack(t1) == LiteralStringRef("abc\x01user\x00\x15\x7b\x15\x01"));

	// Subspace getItem
	Subspace subspace_test6(t);
	Subspace subspace_test7 = subspace_test6.get(LiteralStringRef("subitem"));
	print_subspace_key(subspace_test7, 8);
	ASSERT(subspace_test7.key() == LiteralStringRef("\x01user\x00\x15\x7b\x01subitem\x00"));

	// Subspace unpack
	Tuple t2 = subspace_test6.unpack(subspace_test7.key());
	Subspace subspace_test8(t2);
	print_subspace_key(subspace_test8, 9);
	ASSERT(subspace_test8.key() == LiteralStringRef("\x01subitem\x00"));

	// pack
	Tuple t3;
	t3.append(StringRef());
	printf("%d==========%s===%d\n", 10, printable(subspace_test5.pack(t3)).c_str(), subspace_test5.pack(t3).size());
	ASSERT(subspace_test5.pack(t3) == subspace_test5.pack(StringRef()));
	ASSERT(subspace_test5.pack(t3) == LiteralStringRef("abc\x01user\x00\x15\x7b\x01\x00"));

	printf("%d==========%s===%d\n",
	       11,
	       printable(subspace_test5.range(t3).begin).c_str(),
	       subspace_test5.range(t3).begin.size());
	ASSERT(subspace_test5.range(t3).begin == subspace_test5.get(StringRef()).range().begin);
	printf("%d==========%s===%d\n",
	       12,
	       printable(subspace_test5.range(t3).end).c_str(),
	       subspace_test5.range(t3).end.size());
	ASSERT(subspace_test5.range(t3).end == subspace_test5.get(StringRef()).range().end);

	StringRef def = LiteralStringRef("def");
	StringRef ghi = LiteralStringRef("ghi");
	t3.append(def);
	t3.append(ghi);
	printf("%d==========%s===%d\n", 13, printable(subspace_test5.pack(t3)).c_str(), subspace_test5.pack(t3).size());
	ASSERT(subspace_test5.pack(t3) == subspace_test5.get(StringRef()).get(def).pack(ghi));
	ASSERT(subspace_test5.pack(t3) == LiteralStringRef("abc\x01user\x00\x15\x7b\x01\x00\x01"
	                                                   "def\x00\x01ghi\x00"));

	printf("%d==========%s===%d\n",
	       14,
	       printable(subspace_test5.range(t3).begin).c_str(),
	       subspace_test5.range(t3).begin.size());
	ASSERT(subspace_test5.range(t3).begin == subspace_test5.get(StringRef()).get(def).get(ghi).range().begin);
	printf("%d==========%s===%d\n",
	       15,
	       printable(subspace_test5.range(t3).end).c_str(),
	       subspace_test5.range(t3).end.size());
	ASSERT(subspace_test5.range(t3).end == subspace_test5.get(StringRef()).get(def).get(ghi).range().end);

	return Void();
}
