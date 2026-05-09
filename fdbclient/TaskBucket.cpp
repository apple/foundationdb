/*
 * TaskBucket.cpp
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

#include "fdbclient/TaskBucket.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"

Reference<TaskFuture> Task::getDoneFuture(Reference<FutureBucket> fb) {
	return fb->unpack(params[reservedTaskParamKeyDone]);
}

struct UnblockFutureTaskFunc : TaskFuncBase {
	static StringRef name;

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

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		Reference<TaskFuture> future = futureBucket->unpack(task->params[Task::reservedTaskParamKeyFuture]);

		futureBucket->setOptions(tr);

		tr->clear(future->blocks.pack(task->params[Task::reservedTaskParamKeyBlockID]));

		bool is_set = co_await future->isSet(tr);
		if (is_set) {
			co_await future->performAllActions(tr, taskBucket);
		}
	}
};
StringRef UnblockFutureTaskFunc::name = "UnblockFuture"_sr;
REGISTER_TASKFUNC(UnblockFutureTaskFunc);

struct AddTaskFunc : TaskFuncBase {
	static StringRef name;

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
		task->params[Task::reservedTaskParamKeyType] = task->params[Task::reservedTaskParamKeyAddTask];
		tb->addTask(tr, task);
		return Void();
	};
};
StringRef AddTaskFunc::name = "AddTask"_sr;
REGISTER_TASKFUNC(AddTaskFunc);

struct IdleTaskFunc : TaskFuncBase {
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
		return tb->finish(tr, task);
	};
};
StringRef IdleTaskFunc::name = "idle"_sr;
REGISTER_TASKFUNC(IdleTaskFunc);

Key Task::reservedTaskParamKeyType = "type"_sr;
Key Task::reservedTaskParamKeyAddTask = "_add_task"_sr;
Key Task::reservedTaskParamKeyDone = "done"_sr;
Key Task::reservedTaskParamKeyPriority = "priority"_sr;
Key Task::reservedTaskParamKeyFuture = "future"_sr;
Key Task::reservedTaskParamKeyBlockID = "blockid"_sr;
Key Task::reservedTaskParamKeyVersion = "version"_sr;
Key Task::reservedTaskParamValidKey = "_validkey"_sr;
Key Task::reservedTaskParamValidValue = "_validvalue"_sr;

// IMPORTANT:  Task() must result in an EMPTY parameter set, so params should only
// be set for non-default constructor arguments.  To change this behavior look at all
// Task() default constructions to see if they require params to be empty and call clear.
Task::Task(Value type, uint32_t version, Value done, unsigned int priority) : extendMutex(1) {
	if (!type.empty())
		params[Task::reservedTaskParamKeyType] = type;

	if (version > 0)
		params[Task::reservedTaskParamKeyVersion] = BinaryWriter::toValue(version, Unversioned());

	if (!done.empty())
		params[Task::reservedTaskParamKeyDone] = done;

	priority = std::min<int64_t>(priority, CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY);
	if (priority != 0)
		params[Task::reservedTaskParamKeyPriority] = BinaryWriter::toValue<int64_t>(priority, Unversioned());
}

uint32_t Task::getVersion() const {
	uint32_t version(0);
	auto itor = params.find(Task::reservedTaskParamKeyVersion);
	if (itor != params.end()) {
		version = BinaryReader::fromStringRef<uint32_t>(itor->value, Unversioned());
	} else {
		TraceEvent(SevWarn, "InvalidTaskVersion").detail("TaskHasNoVersion", version);
	}

	return version;
}

unsigned int Task::getPriority() const {
	unsigned int priority = 0;
	auto i = params.find(Task::reservedTaskParamKeyPriority);
	if (i != params.end())
		priority = std::min<int64_t>(BinaryReader::fromStringRef<int64_t>(i->value, Unversioned()),
		                             CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY);
	return priority;
}

class TaskBucketImpl {
public:
	static Future<Optional<Key>> getTaskKey(Reference<ReadYourWritesTransaction> tr,
	                                        Reference<TaskBucket> taskBucket,
	                                        int priority = 0) {
		Standalone<StringRef> uid = StringRef(deterministicRandom()->randomUniqueID().toString());

		// Get keyspace for the specified priority level
		Subspace space = taskBucket->getAvailableSpace(priority);
		{
			// Get a task key that is <= a random UID task key, if successful then return it
			RangeResult value =
			    co_await tr->getRange(KeyRangeRef(space.key(), space.pack(uid)), 1, Snapshot::True, Reverse::True);
			if (!value.empty()) {
				co_return Optional<Key>(value[0].key);
			}
		}

		{
			// Get a task key that is <= the maximum possible UID, if successful return it.
			RangeResult value = co_await tr->getRange(
			    KeyRangeRef(space.key(), space.pack(maxUIDKey)), 1, Snapshot::True, Reverse::True);
			if (!value.empty()) {
				co_return Optional<Key>(value[0].key);
			}
		}

		co_return Optional<Key>();
	}

	static Future<Reference<Task>> getOne(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
		if (taskBucket->priority_batch)
			tr->setOption(FDBTransactionOptions::PRIORITY_BATCH);

		taskBucket->setOptions(tr);

		// give it some chances for the timed out tasks to get into the task loop in the case of
		// many other new tasks get added so that the timed out tasks never get chances to re-run
		if (deterministicRandom()->random01() < CLIENT_KNOBS->TASKBUCKET_CHECK_TIMEOUT_CHANCE) {
			bool anyTimeouts = co_await requeueTimedOutTasks(tr, taskBucket);
			CODE_PROBE(anyTimeouts, "Found a task that timed out");
		}

		std::vector<Future<Optional<Key>>> taskKeyFutures(CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY + 1);

		// Start looking for a task at each priority, highest first
		for (int pri = CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY; pri >= 0; --pri)
			taskKeyFutures[pri] = getTaskKey(tr, taskBucket, pri);

		// Task key and subspace it is located in.
		Optional<Key> taskKey;
		Subspace availableSpace;

		// In priority order from highest to lowest, wait for fetch to finish and if it found a task then cancel the
		// rest.
		for (int pri = CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY; pri >= 0; --pri) {
			// If we already have a task key then cancel this fetch
			if (taskKey.present())
				taskKeyFutures[pri].cancel();
			else {
				Optional<Key> key = co_await taskKeyFutures[pri];
				if (key.present()) {
					taskKey = key;
					availableSpace = taskBucket->getAvailableSpace(pri);
				}
			}
		}

		// If we don't have a task key, requeue timed out tasks and try again by calling self.
		if (!taskKey.present()) {
			bool anyTimeouts = co_await requeueTimedOutTasks(tr, taskBucket);
			// If there were timeouts, try to get a task since there should now be one in one of the available spaces.
			if (anyTimeouts) {
				CODE_PROBE(true, "Try to get one task from timeouts subspace");
				Reference<Task> task = co_await getOne(tr, taskBucket);
				co_return task;
			}
			co_return Reference<Task>();
		}

		// Now we know the task key is present and we have the available space for the task's priority
		Tuple t = availableSpace.unpack(taskKey.get());
		Key taskUID = t.getString(0);
		Subspace taskAvailableSpace = availableSpace.get(taskUID);

		Reference<Task> task(new Task());
		task->key = taskUID;

		RangeResult values = co_await tr->getRange(taskAvailableSpace.range(), CLIENT_KNOBS->TOO_MANY);
		Version version = co_await tr->getReadVersion();
		task->timeoutVersion =
		    version + (uint64_t)(taskBucket->timeout *
		                         (CLIENT_KNOBS->TASKBUCKET_TIMEOUT_JITTER_OFFSET +
		                          CLIENT_KNOBS->TASKBUCKET_TIMEOUT_JITTER_RANGE * deterministicRandom()->random01()));

		Subspace timeoutSpace = taskBucket->timeouts.get(task->timeoutVersion).get(taskUID);

		for (auto& s : values) {
			Key param = taskAvailableSpace.unpack(s.key).getString(0);
			task->params[param] = s.value;
			tr->set(timeoutSpace.pack(param), s.value);
		}

		// Clear task definition in the available keyspace
		tr->clear(taskAvailableSpace.range());
		tr->set(taskBucket->active.key(), deterministicRandom()->randomUniqueID().toString());

		co_return task;
	}

	// Verify that the user configured task verification key still has the user specified value
	static Future<bool> taskVerify(Reference<TaskBucket> tb,
	                               Reference<ReadYourWritesTransaction> tr,
	                               Reference<Task> task) {

		if (task->params.find(Task::reservedTaskParamValidKey) == task->params.end()) {
			TraceEvent("TaskBucketTaskVerifyInvalidTask")
			    .detail("Task", task->params[Task::reservedTaskParamKeyType])
			    .detail("ReservedTaskParamValidKey", "missing");
			co_return false;
		}

		if (task->params.find(Task::reservedTaskParamValidValue) == task->params.end()) {
			TraceEvent("TaskBucketTaskVerifyInvalidTask")
			    .detail("Task", task->params[Task::reservedTaskParamKeyType])
			    .detail("ReservedTaskParamValidKey", task->params[Task::reservedTaskParamValidKey])
			    .detail("ReservedTaskParamValidValue", "missing");
			co_return false;
		}

		tb->setOptions(tr);

		Optional<Value> keyValue = co_await tr->get(task->params[Task::reservedTaskParamValidKey]);

		if (!keyValue.present()) {
			TraceEvent("TaskBucketTaskVerifyInvalidTask")
			    .detail("Task", task->params[Task::reservedTaskParamKeyType])
			    .detail("ReservedTaskParamValidKey", task->params[Task::reservedTaskParamValidKey])
			    .detail("ReservedTaskParamValidValue", task->params[Task::reservedTaskParamValidValue])
			    .detail("KeyValue", "missing");
			co_return false;
		}

		if (keyValue.get().compare(StringRef(task->params[Task::reservedTaskParamValidValue]))) {
			TraceEvent("TaskBucketTaskVerifyAbortedTask")
			    .detail("Task", task->params[Task::reservedTaskParamKeyType])
			    .detail("ReservedTaskParamValidKey", task->params[Task::reservedTaskParamValidKey])
			    .detail("ReservedTaskParamValidValue", task->params[Task::reservedTaskParamValidValue])
			    .detail("KeyValue", keyValue.get());
			co_return false;
		}

		co_return true;
	}

	static Future<bool> taskVerify(Reference<TaskBucket> tb, Database cx, Reference<Task> task) {
		while (true) {
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			Error err;
			try {
				bool verified = co_await taskVerify(tb, tr, task);
				co_return verified;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}

	static Future<Void> finishTaskRun(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task,
	                                  Reference<TaskFuncBase> taskFunc,
	                                  VerifyTask verifyTask) {
		bool isFinished = co_await taskBucket->isFinished(tr, task);
		if (isFinished) {
			co_return;
		}

		bool validTask = true;
		if (verifyTask) {
			bool _validTask = co_await taskVerify(taskBucket, tr, task);
			validTask = _validTask;
		}
		if (!validTask) {
			co_await taskBucket->finish(tr, task);
		} else {
			co_await taskFunc->finish(tr, taskBucket, futureBucket, task);
		}
	}

	static Future<bool> doOne(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket) {
		Reference<Task> task = co_await taskBucket->getOne(cx);
		bool result = co_await taskBucket->doTask(cx, futureBucket, task);
		co_return result;
	}

	static Future<Void> extendTimeoutRepeatedly(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		double start = now();
		Version versionNow = co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
			taskBucket->setOptions(tr);
			return map(tr->getReadVersion(), [=](Version v) { return v; });
		});

		while (true) {
			FlowLock::Releaser releaser;

			// Wait until we are half way to the timeout version of this task
			co_await delay(0.8 * (BUGGIFY ? (2 * deterministicRandom()->random01()) : 1.0) *
			               (double)(task->timeoutVersion - (uint64_t)versionNow) /
			               CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

			if (now() - start > 300) {
				TraceEvent(SevWarnAlways, "TaskBucketLongExtend")
				    .detail("Duration", now() - start)
				    .detail("TaskUID", task->key)
				    .detail("TaskType", task->params[Task::reservedTaskParamKeyType])
				    .detail("Priority", task->getPriority());
			}
			// Take the extendMutex lock until we either succeed or stop trying to extend due to failure
			co_await task->extendMutex.take();
			releaser = FlowLock::Releaser(task->extendMutex, 1);

			while (true) {
				Error err;
				try {
					tr->reset();
					taskBucket->setOptions(tr);

					// Attempt to extend the task's timeout
					Version newTimeout = co_await taskBucket->extendTimeout(tr, task, UpdateParams::False);
					co_await tr->commit();
					task->timeoutVersion = newTimeout;
					versionNow = tr->getCommittedVersion();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}
	}

	static Future<bool> doTask(Database cx,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<FutureBucket> futureBucket,
	                           Reference<Task> task) {
		Reference<TaskFuncBase> taskFunc;
		VerifyTask verifyTask(false);

		if (!task || !TaskFuncBase::isValidTask(task))
			co_return false;

		Error err;
		try {
			taskFunc = TaskFuncBase::create(task->params[Task::reservedTaskParamKeyType]);
			if (taskFunc) {
				verifyTask.set(task->params.find(Task::reservedTaskParamValidKey) != task->params.end());

				if (verifyTask) {
					while (true) {
						Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
						taskBucket->setOptions(tr);
						Error innerErr;
						try {
							bool validTask = co_await taskVerify(taskBucket, tr, task);

							if (!validTask) {
								bool isFinished = co_await taskBucket->isFinished(tr, task);
								if (!isFinished) {
									co_await taskBucket->finish(tr, task);
								}
								co_await tr->commit();
								co_return true;
							}
							break;
						} catch (Error& e) {
							innerErr = e;
						}
						co_await tr->onError(innerErr);
					}
				}

				co_await (taskFunc->execute(cx, taskBucket, futureBucket, task) ||
				          extendTimeoutRepeatedly(cx, taskBucket, task));

				if (BUGGIFY)
					co_await delay(10.0);
				co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
					return finishTaskRun(tr, taskBucket, futureBucket, task, taskFunc, verifyTask);
				});
			}
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevWarn, "TaskBucketExecuteFailure")
		    .error(err)
		    .detail("TaskUID", task->key)
		    .detail("TaskType", task->params[Task::reservedTaskParamKeyType].printable())
		    .detail("Priority", task->getPriority());
		try {
			co_await taskFunc->handleError(cx, task, err);
		} catch (Error& handleErr) {
			TraceEvent(SevWarn, "TaskBucketExecuteFailureLogErrorFailed")
			    .error(handleErr) // output handleError() error instead of original task error
			    .detail("TaskUID", task->key.printable())
			    .detail("TaskType", task->params[Task::reservedTaskParamKeyType].printable())
			    .detail("Priority", task->getPriority());
		}

		// Return true to indicate that we did work.
		co_return true;
	}

	static Future<Void> dispatch(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             std::shared_ptr<double const> pollDelay,
	                             int maxConcurrentTasks) {
		std::vector<Future<bool>> tasks(maxConcurrentTasks);
		for (auto& f : tasks)
			f = Never();

		// Since the futures have to be kept in a vector to be compatible with waitForAny(), we'll keep a queue
		// of available slots in it.  Initially, they're all available.
		std::vector<int> availableSlots;
		availableSlots.reserve(tasks.size());
		for (int i = 0; i < tasks.size(); ++i)
			availableSlots.push_back(i);

		std::vector<Future<Reference<Task>>> getTasks;
		unsigned int getBatchSize = 1;

		while (true) {
			// Start running tasks while slots are available and we keep finding work to do
			++taskBucket->dispatchSlotChecksStarted;
			while (!availableSlots.empty()) {
				getTasks.clear();
				for (int i = 0, imax = std::min<unsigned int>(getBatchSize, availableSlots.size()); i < imax; ++i)
					getTasks.push_back(taskBucket->getOne(cx));
				co_await waitForAllReady(getTasks);

				bool done = false;
				for (int i = 0; i < getTasks.size(); ++i) {
					if (getTasks[i].isError()) {
						++taskBucket->dispatchErrors;
						done = true;
						continue;
					}
					Reference<Task> task = getTasks[i].get();
					if (task) {
						// Start the task
						++taskBucket->dispatchDoTasks;
						int slot = availableSlots.back();
						availableSlots.pop_back();
						tasks[slot] = taskBucket->doTask(cx, futureBucket, task);
					} else {
						++taskBucket->dispatchEmptyTasks;
						done = true;
					}
				}

				if (done) {
					getBatchSize = 1;
					break;
				} else
					getBatchSize = std::min<unsigned int>(getBatchSize * 2, maxConcurrentTasks);
			}
			++taskBucket->dispatchSlotChecksComplete;

			// Wait for a task to be done.  Also, if we have any slots available then stop waiting after pollDelay at
			// the latest.
			Future<Void> w = ready(waitForAny(tasks));
			if (!availableSlots.empty()) {
				if (*pollDelay > 600) {
					TraceEvent(SevWarnAlways, "TaskBucketLongPollDelay").suppressFor(1.0).detail("Delay", *pollDelay);
				}
				w = w ||
				    delay(*pollDelay * (0.9 + deterministicRandom()->random01() / 5)); // Jittered by 20 %, so +/- 10%
			}
			co_await w;

			// Check all of the task slots, any that are finished should be replaced with Never() and their slots added
			// back to availableSlots
			for (int i = 0; i < tasks.size(); ++i) {
				if (tasks[i].isReady()) {
					availableSlots.push_back(i);
					tasks[i] = Never();
				}
			}
		}
	}

	static Future<Void> watchPaused(Database cx, Reference<TaskBucket> taskBucket, Reference<AsyncVar<bool>> paused) {
		while (true) {
			auto tr = makeReference<ReadYourWritesTransaction>(cx);
			Error err;
			try {
				taskBucket->setOptions(tr);
				Optional<Value> pausedVal = co_await tr->get(taskBucket->pauseKey);
				paused->set(pausedVal.present());
				Future<Void> watchPausedFuture = tr->watch(taskBucket->pauseKey);
				co_await tr->commit();
				co_await watchPausedFuture;
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}

	static Future<Void> run(Database cx,
	                        Reference<TaskBucket> taskBucket,
	                        Reference<FutureBucket> futureBucket,
	                        std::shared_ptr<double const> pollDelay,
	                        int maxConcurrentTasks) {
		auto paused = makeReference<AsyncVar<bool>>(true);
		Future<Void> watchPausedFuture = watchPaused(cx, taskBucket, paused);
		taskBucket->metricLogger = taskBucket->cc.traceCounters(
		    "TaskBucketMetrics", taskBucket->dbgid, CLIENT_KNOBS->TASKBUCKET_LOGGING_DELAY);
		while (true) {
			while (paused->get()) {
				co_await (paused->onChange() || watchPausedFuture);
			}

			co_await race(dispatch(cx, taskBucket, futureBucket, pollDelay, maxConcurrentTasks),
			              paused->onChange(),
			              watchPausedFuture);
		}
	}

	static Future<Standalone<StringRef>> addIdle(Reference<ReadYourWritesTransaction> tr,
	                                             Reference<TaskBucket> taskBucket) {
		taskBucket->setOptions(tr);

		auto newTask = makeReference<Task>(IdleTaskFunc::name, IdleTaskFunc::version);
		return taskBucket->addTask(tr, newTask);
	}

	static Future<Standalone<StringRef>> addIdle(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return addIdle(tr, taskBucket); });
	}

	static Future<bool> isEmpty(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
		taskBucket->setOptions(tr);

		// Check all available priorities for keys
		std::vector<Future<RangeResult>> resultFutures;
		for (int pri = 0; pri <= CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY; ++pri)
			resultFutures.push_back(tr->getRange(taskBucket->getAvailableSpace(pri).range(), 1));

		// If any priority levels have any keys then the taskbucket is not empty so return false
		for (int i = 0; i < resultFutures.size(); ++i) {
			RangeResult results = co_await resultFutures[i];
			if (!results.empty())
				co_return false;
		}

		RangeResult values = co_await tr->getRange(taskBucket->timeouts.range(), 1);
		if (!values.empty())
			co_return false;

		co_return true;
	}

	static Future<bool> isBusy(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
		taskBucket->setOptions(tr);

		// Check all available priorities for emptiness
		std::vector<Future<RangeResult>> resultFutures;
		for (int pri = 0; pri <= CLIENT_KNOBS->TASKBUCKET_MAX_PRIORITY; ++pri)
			resultFutures.push_back(tr->getRange(taskBucket->getAvailableSpace(pri).range(), 1));

		// If any priority levels have any keys then return true as the level is 'busy'
		for (int i = 0; i < resultFutures.size(); ++i) {
			RangeResult results = co_await resultFutures[i];
			if (!results.empty())
				co_return true;
		}

		co_return false;
	}

	// Verify that the task's keys are still in the timeout space at the expected timeout prefix
	static Future<bool> isFinished(Reference<ReadYourWritesTransaction> tr,
	                               Reference<TaskBucket> taskBucket,
	                               Reference<Task> task) {
		taskBucket->setOptions(tr);

		Tuple t = Tuple::makeTuple(task->timeoutVersion, task->key);
		RangeResult values = co_await tr->getRange(taskBucket->timeouts.range(t), 1);
		if (!values.empty())
			co_return false;

		co_return true;
	}

	static Future<bool> getActiveKey(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Optional<Value> startingValue) {
		taskBucket->setOptions(tr);

		Optional<Value> new_value = co_await tr->get(taskBucket->active.key());
		if (new_value != startingValue) {
			co_return true;
		}
		co_return false;
	}

	static Future<bool> checkActive(Database cx, Reference<TaskBucket> taskBucket) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		Optional<Value> startingValue;

		while (true) {
			Error err;
			try {
				taskBucket->setOptions(tr);

				bool is_busy = co_await isBusy(tr, taskBucket);
				if (!is_busy) {
					co_await addIdle(tr, taskBucket);
				}

				Optional<Value> val = co_await tr->get(taskBucket->active.key());
				startingValue = val;

				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		int idx = 0;
		for (; idx < CLIENT_KNOBS->TASKBUCKET_CHECK_ACTIVE_AMOUNT; ++idx) {
			tr = makeReference<ReadYourWritesTransaction>(cx);
			while (true) {
				Error err;
				try {
					taskBucket->setOptions(tr);

					co_await delay(CLIENT_KNOBS->TASKBUCKET_CHECK_ACTIVE_DELAY);
					bool isActiveKey = co_await getActiveKey(tr, taskBucket, startingValue);
					if (isActiveKey) {
						CODE_PROBE(true, "checkActive return true");
						co_return true;
					}
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}

		CODE_PROBE(true, "checkActive return false");
		co_return false;
	}

	static Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
		taskBucket->setOptions(tr);

		Optional<Value> val = co_await tr->get(taskBucket->prefix.pack("task_count"_sr));

		if (!val.present())
			co_return 0;

		ASSERT(val.get().size() == sizeof(int64_t));

		int64_t intValue = 0;
		memcpy(&intValue, val.get().begin(), val.get().size());

		co_return intValue;
	}

	// Looks for tasks that have timed out and returns them to be available tasks.
	// Returns True if any tasks were affected.
	static Future<bool> requeueTimedOutTasks(Reference<ReadYourWritesTransaction> tr,
	                                         Reference<TaskBucket> taskBucket) {
		CODE_PROBE(true, "Looks for tasks that have timed out and returns them to be available tasks.");
		Version end = co_await tr->getReadVersion();
		KeyRange range(
		    KeyRangeRef(taskBucket->timeouts.get(0).range().begin, taskBucket->timeouts.get(end).range().end));

		RangeResult values = co_await tr->getRange(range, CLIENT_KNOBS->TASKBUCKET_MAX_TASK_KEYS);

		// Keys will be tuples of (taskUID, param) -> paramValue
		// Unfortunately we need to know the priority parameter for a taskUID before we can know which available-tasks
		// subspace to move its keys to.  The cleanest way to do this is to load a new Task() with parameters and once a
		// new task id is encountered flush the old one using taskBucket->getAvailableSpace(task->getPriority())

		Task task;
		Key lastKey;

		for (auto& iter : values) {
			Tuple t = taskBucket->timeouts.unpack(iter.key);
			Key uid = t.getString(1);
			Key param = t.getString(2);

			// If a new UID is seen, finish moving task to new available space. Safe if task == Task()
			if (uid != task.key) {
				// Get the space for this specific task within its available keyspace for its priority
				Subspace space = taskBucket->getAvailableSpace(task.getPriority()).get(task.key);
				for (auto& p : task.params) {
					tr->set(space.pack(p.key), p.value);
				}
				task.params.clear();
				task.key = uid;
				lastKey = iter.key;
			}

			task.params[param] = iter.value;
		}

		// Move the final task, if complete, to its new available keyspace. Safe if task == Task()
		if (!values.more) {
			Subspace space = taskBucket->getAvailableSpace(task.getPriority()).get(task.key);
			for (auto& p : task.params)
				tr->set(space.pack(p.key), p.value);

			if (!values.empty()) {
				tr->clear(range);
				co_return true;
			}
			co_return false;
		}

		ASSERT(!lastKey.empty());
		tr->clear(KeyRangeRef(range.begin, lastKey));
		co_return true;
	}

	static Future<Void> debugPrintRange(Reference<ReadYourWritesTransaction> tr, Subspace subspace, Key msg) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		RangeResult values = co_await tr->getRange(subspace.range(), CLIENT_KNOBS->TOO_MANY);
		TraceEvent("TaskBucketDebugPrintRange")
		    .detail("Key", subspace.key())
		    .detail("Count", values.size())
		    .detail("Msg", msg);

		/*printf("debugPrintRange  key: (%d) %s\n", values.size(), printable(subspace.key()).c_str());
		for (auto & s : values) {
		    printf("   key: %-40s   value: %s\n", printable(s.key).c_str(), s.value.c_str());
		    TraceEvent("TaskBucketDebugPrintKV").detail("Msg", msg)
		        .detail("Key", s.key)
		        .detail("Value", s.value);
		}*/
	}

	static Future<Version> extendTimeout(Reference<ReadYourWritesTransaction> tr,
	                                     Reference<TaskBucket> taskBucket,
	                                     Reference<Task> task,
	                                     UpdateParams updateParams,
	                                     Version newTimeoutVersion) {
		taskBucket->setOptions(tr);

		// First make sure it's safe to keep running
		co_await taskBucket->keepRunning(tr, task);

		// This is where the task definition currently exists
		Subspace oldTimeoutSpace = taskBucket->timeouts.get(task->timeoutVersion).get(task->key);
		// Update the task's timeout
		Version version = co_await tr->getReadVersion();

		if (newTimeoutVersion == invalidVersion)
			newTimeoutVersion = version + taskBucket->timeout;
		else if (newTimeoutVersion <= version) // Ensure that the time extension is to the future
			newTimeoutVersion = version + 1;

		// This can happen if extendTimeout is called shortly after task start and the task's timeout was jittered to be
		// longer
		if (newTimeoutVersion <= task->timeoutVersion) {
			newTimeoutVersion = task->timeoutVersion + 1;
		}

		// This is where the task definition is being moved to
		Subspace newTimeoutSpace = taskBucket->timeouts.get(newTimeoutVersion).get(task->key);

		tr->addReadConflictRange(oldTimeoutSpace.range());
		tr->addWriteConflictRange(newTimeoutSpace.range());

		// If we're updating the task params the clear the old space and write params to the new space
		if (updateParams) {
			CODE_PROBE(true, "Extended a task while updating parameters");
			for (auto& p : task->params) {
				tr->set(newTimeoutSpace.pack(p.key), p.value);
			}
		} else {
			CODE_PROBE(true, "Extended a task without updating parameters");
			// Otherwise, read and transplant the params from the old to new timeout spaces
			RangeResult params = co_await tr->getRange(oldTimeoutSpace.range(), CLIENT_KNOBS->TOO_MANY);
			for (auto& kv : params) {
				Tuple paramKey = oldTimeoutSpace.unpack(kv.key);
				tr->set(newTimeoutSpace.pack(paramKey), kv.value);
			}
		}

		tr->clear(oldTimeoutSpace.range());

		co_return newTimeoutVersion;
	}
};

TaskBucket::TaskBucket(const Subspace& subspace,
                       AccessSystemKeys sysAccess,
                       PriorityBatch priorityBatch,
                       LockAware lockAware)
  : dbgid(deterministicRandom()->randomUniqueID()), cc("TaskBucket", dbgid.toString()),
    dispatchSlotChecksStarted("DispatchSlotChecksStarted", cc), dispatchErrors("DispatchErrors", cc),
    dispatchDoTasks("DispatchDoTasks", cc), dispatchEmptyTasks("DispatchEmptyTasks", cc),
    dispatchSlotChecksComplete("DispatchSlotChecksComplete", cc), prefix(subspace), active(prefix.get("ac"_sr)),
    pauseKey(prefix.pack("pause"_sr)), available(prefix.get("av"_sr)), available_prioritized(prefix.get("avp"_sr)),
    timeouts(prefix.get("to"_sr)), timeout(CLIENT_KNOBS->TASKBUCKET_TIMEOUT_VERSIONS), system_access(sysAccess),
    priority_batch(priorityBatch), lockAware(lockAware) {}

TaskBucket::~TaskBucket() = default;

Future<Void> TaskBucket::clear(Reference<ReadYourWritesTransaction> tr) {
	setOptions(tr);

	tr->clear(prefix.range());

	return Void();
}

Future<Void> TaskBucket::changePause(Reference<ReadYourWritesTransaction> tr, bool pause) {
	setOptions(tr);

	if (pause) {
		tr->set(pauseKey, StringRef());
	} else {
		tr->clear(pauseKey);
	}

	return Void();
}

Key TaskBucket::addTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
	setOptions(tr);

	Key key(deterministicRandom()->randomUniqueID().toString());

	Subspace taskSpace;

	// If scheduledVersion is valid then place the task directly into the timeout
	// space for its scheduled time, otherwise place it in the available space by priority.
	Version scheduledVersion = ReservedTaskParams::scheduledVersion().getOrDefault(task, invalidVersion);
	if (scheduledVersion != invalidVersion) {
		taskSpace = timeouts.get(scheduledVersion).get(key);
	} else {
		taskSpace = getAvailableSpace(task->getPriority()).get(key);
	}

	for (auto& param : task->params)
		tr->set(taskSpace.pack(param.key), param.value);

	tr->atomicOp(prefix.pack("task_count"_sr), "\x01\x00\x00\x00\x00\x00\x00\x00"_sr, MutationRef::AddValue);

	return key;
}

void TaskBucket::setValidationCondition(Reference<Task> task, KeyRef vKey, KeyRef vValue) {
	task->params[Task::reservedTaskParamValidKey] = vKey;
	task->params[Task::reservedTaskParamValidValue] = vValue;
}

static Future<Key> actorAddTask(TaskBucket* tb,
                                Reference<ReadYourWritesTransaction> tr,
                                Reference<Task> task,
                                KeyRef validationKey) {
	tb->setOptions(tr);

	Optional<Value> validationValue = co_await tr->get(validationKey);

	if (!validationValue.present()) {
		TraceEvent(SevError, "TaskBucketAddTaskInvalidKey")
		    .detail("Task", task->params[Task::reservedTaskParamKeyType])
		    .detail("ValidationKey", validationKey);
		throw invalid_option_value();
	}

	TaskBucket::setValidationCondition(task, validationKey, validationValue.get());

	co_return tb->addTask(tr, task);
}

Future<Key> TaskBucket::addTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task, KeyRef validationKey) {
	return actorAddTask(this, tr, task, validationKey);
}

Key TaskBucket::addTask(Reference<ReadYourWritesTransaction> tr,
                        Reference<Task> task,
                        KeyRef validationKey,
                        KeyRef validationValue) {
	setValidationCondition(task, validationKey, validationValue);
	return addTask(tr, task);
}

Future<Reference<Task>> TaskBucket::getOne(Reference<ReadYourWritesTransaction> tr) {
	return TaskBucketImpl::getOne(tr, Reference<TaskBucket>::addRef(this));
}

Future<bool> TaskBucket::doOne(Database cx, Reference<FutureBucket> futureBucket) {
	return TaskBucketImpl::doOne(cx, Reference<TaskBucket>::addRef(this), futureBucket);
}

Future<bool> TaskBucket::doTask(Database cx, Reference<FutureBucket> futureBucket, Reference<Task> task) {
	return TaskBucketImpl::doTask(cx, Reference<TaskBucket>::addRef(this), futureBucket, task);
}

Future<Void> TaskBucket::run(Database cx,
                             Reference<FutureBucket> futureBucket,
                             std::shared_ptr<double const> pollDelay,
                             int maxConcurrentTasks) {
	return TaskBucketImpl::run(cx, Reference<TaskBucket>::addRef(this), futureBucket, pollDelay, maxConcurrentTasks);
}

Future<Void> TaskBucket::watchPaused(Database cx, Reference<AsyncVar<bool>> paused) {
	return TaskBucketImpl::watchPaused(cx, Reference<TaskBucket>::addRef(this), paused);
}

Future<bool> TaskBucket::isEmpty(Reference<ReadYourWritesTransaction> tr) {
	return TaskBucketImpl::isEmpty(tr, Reference<TaskBucket>::addRef(this));
}

Future<Void> TaskBucket::finish(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
	setOptions(tr);

	Tuple t = Tuple::makeTuple(task->timeoutVersion, task->key);

	tr->atomicOp(prefix.pack("task_count"_sr), "\xff\xff\xff\xff\xff\xff\xff\xff"_sr, MutationRef::AddValue);
	tr->clear(timeouts.range(t));

	return Void();
}

Future<Version> TaskBucket::extendTimeout(Reference<ReadYourWritesTransaction> tr,
                                          Reference<Task> task,
                                          UpdateParams updateParams,
                                          Version newTimeoutVersion) {
	return TaskBucketImpl::extendTimeout(
	    tr, Reference<TaskBucket>::addRef(this), task, updateParams, newTimeoutVersion);
}

Future<bool> TaskBucket::isFinished(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
	return TaskBucketImpl::isFinished(tr, Reference<TaskBucket>::addRef(this), task);
}

Future<bool> TaskBucket::isVerified(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
	return TaskBucketImpl::taskVerify(Reference<TaskBucket>::addRef(this), tr, task);
}

Future<bool> TaskBucket::checkActive(Database cx) {
	return TaskBucketImpl::checkActive(cx, Reference<TaskBucket>::addRef(this));
}

Future<int64_t> TaskBucket::getTaskCount(Reference<ReadYourWritesTransaction> tr) {
	return TaskBucketImpl::getTaskCount(tr, Reference<TaskBucket>::addRef(this));
}

Future<Void> TaskBucket::watchTaskCount(Reference<ReadYourWritesTransaction> tr) {
	return tr->watch(prefix.pack("task_count"_sr));
}

Future<Void> TaskBucket::debugPrintRange(Reference<ReadYourWritesTransaction> tr, Subspace subspace, Key msg) {
	return TaskBucketImpl::debugPrintRange(tr, subspace, msg);
}

class FutureBucketImpl {
public:
	static Future<bool> isEmpty(Reference<ReadYourWritesTransaction> tr, Reference<FutureBucket> futureBucket) {
		futureBucket->setOptions(tr);

		Key lastKey = co_await tr->getKey(lastLessOrEqual(futureBucket->prefix.pack(maxUIDKey)));
		co_return !futureBucket->prefix.contains(lastKey);
	}
};

FutureBucket::FutureBucket(const Subspace& subspace, AccessSystemKeys sysAccess, LockAware lockAware)
  : prefix(subspace), system_access(sysAccess), lockAware(lockAware) {}

FutureBucket::~FutureBucket() = default;

Future<Void> FutureBucket::clear(Reference<ReadYourWritesTransaction> tr) {
	setOptions(tr);
	tr->clear(prefix.range());

	return Void();
}

Reference<TaskFuture> FutureBucket::future(Reference<ReadYourWritesTransaction> tr) {
	setOptions(tr);

	auto taskFuture = makeReference<TaskFuture>(Reference<FutureBucket>::addRef(this));
	taskFuture->addBlock(tr, StringRef());

	return taskFuture;
}

Future<bool> FutureBucket::isEmpty(Reference<ReadYourWritesTransaction> tr) {
	return FutureBucketImpl::isEmpty(tr, Reference<FutureBucket>::addRef(this));
}

Reference<TaskFuture> FutureBucket::unpack(Key key) {
	return makeReference<TaskFuture>(Reference<FutureBucket>::addRef(this), key);
}

class TaskFutureImpl {
public:
	static Future<Void> join(Reference<ReadYourWritesTransaction> tr,
	                         Reference<TaskBucket> taskBucket,
	                         Reference<TaskFuture> taskFuture,
	                         std::vector<Reference<TaskFuture>> vectorFuture) {
		taskFuture->futureBucket->setOptions(tr);

		bool is_set = co_await isSet(tr, taskFuture);
		// A taskFuture cannot be already set for it to be joined with others.
		if (is_set) {
			co_return;
		}

		tr->clear(taskFuture->blocks.pack(StringRef()));

		co_await _join(tr, taskBucket, taskFuture, vectorFuture);
	}

	static Future<Void> _join(Reference<ReadYourWritesTransaction> tr,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<TaskFuture> taskFuture,
	                          std::vector<Reference<TaskFuture>> vectorFuture) {
		std::vector<Future<Void>> onSetFutures;
		for (int i = 0; i < vectorFuture.size(); ++i) {
			Key key = StringRef(deterministicRandom()->randomUniqueID().toString());
			taskFuture->addBlock(tr, key);
			auto task = makeReference<Task>();
			task->params[Task::reservedTaskParamKeyType] = "UnblockFuture"_sr;
			task->params[Task::reservedTaskParamKeyFuture] = taskFuture->key;
			task->params[Task::reservedTaskParamKeyBlockID] = key;
			onSetFutures.push_back(vectorFuture[i]->onSet(tr, taskBucket, task));
		}

		co_await waitForAll(onSetFutures);
	}

	static Future<bool> isSet(Reference<ReadYourWritesTransaction> tr, Reference<TaskFuture> taskFuture) {
		taskFuture->futureBucket->setOptions(tr);

		RangeResult values = co_await tr->getRange(taskFuture->blocks.range(), 1);
		if (!values.empty())
			co_return false;

		co_return true;
	}

	static Future<Void> onSet(Reference<ReadYourWritesTransaction> tr,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<TaskFuture> taskFuture,
	                          Reference<Task> task) {
		taskFuture->futureBucket->setOptions(tr);

		bool is_set = co_await isSet(tr, taskFuture);

		if (is_set) {
			CODE_PROBE(true, "is_set == true");
			co_await performAction(tr, taskBucket, taskFuture, task);
		} else {
			CODE_PROBE(true, "is_set == false");
			Subspace callbackSpace =
			    taskFuture->callbacks.get(StringRef(deterministicRandom()->randomUniqueID().toString()));
			for (auto& v : task->params) {
				tr->set(callbackSpace.pack(v.key), v.value);
			}
		}
	}

	static Future<Void> set(Reference<ReadYourWritesTransaction> tr,
	                        Reference<TaskBucket> taskBucket,
	                        Reference<TaskFuture> taskFuture) {
		taskFuture->futureBucket->setOptions(tr);

		tr->clear(taskFuture->blocks.range());

		co_await performAllActions(tr, taskBucket, taskFuture);
	}

	static Future<Void> performAction(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<TaskFuture> taskFuture,
	                                  Reference<Task> task) {
		taskFuture->futureBucket->setOptions(tr);

		if (task && TaskFuncBase::isValidTask(task)) {
			Reference<TaskFuncBase> taskFunc = TaskFuncBase::create(task->params[Task::reservedTaskParamKeyType]);
			if (taskFunc.getPtr()) {
				co_await taskFunc->finish(tr, taskBucket, taskFuture->futureBucket, task);
			}
		}
	}

	static Future<Void> performAllActions(Reference<ReadYourWritesTransaction> tr,
	                                      Reference<TaskBucket> taskBucket,
	                                      Reference<TaskFuture> taskFuture) {
		taskFuture->futureBucket->setOptions(tr);

		RangeResult values = co_await tr->getRange(taskFuture->callbacks.range(), CLIENT_KNOBS->TOO_MANY);
		tr->clear(taskFuture->callbacks.range());

		std::vector<Future<Void>> actions;

		if (!values.empty()) {
			Reference<Task> task(new Task());
			Key lastTaskID;
			for (auto& s : values) {
				Tuple t = taskFuture->callbacks.unpack(s.key);
				Key taskID = t.getString(0);
				Key key = t.getString(1);
				// If we see a new task ID and the old one isn't empty then process the task accumulated so far and make
				// a new task
				if (!taskID.empty() && taskID != lastTaskID) {
					actions.push_back(performAction(tr, taskBucket, taskFuture, task));
					task = makeReference<Task>();
				}
				task->params[key] = s.value;
				lastTaskID = taskID;
			}
			// Process the last task
			actions.push_back(performAction(tr, taskBucket, taskFuture, task));
		}

		co_await waitForAll(actions);
	}

	static Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<TaskFuture> taskFuture,
	                                 Reference<Task> task) {
		taskFuture->futureBucket->setOptions(tr);

		task->params[Task::reservedTaskParamKeyAddTask] = task->params[Task::reservedTaskParamKeyType];
		task->params[Task::reservedTaskParamKeyType] = "AddTask"_sr;
		co_await onSet(tr, taskBucket, taskFuture, task);
	}

	static Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<TaskFuture> taskFuture,
	                                 Reference<Task> task,
	                                 KeyRef validationKey) {
		taskFuture->futureBucket->setOptions(tr);

		Optional<Value> validationValue = co_await tr->get(validationKey);

		if (!validationValue.present()) {
			TraceEvent(SevError, "TaskBucketOnSetAddTaskInvalidKey")
			    .detail("Task", task->params[Task::reservedTaskParamKeyType])
			    .detail("ValidationKey", validationKey);
			throw invalid_option_value();
		}

		task->params[Task::reservedTaskParamValidKey] = validationKey;
		task->params[Task::reservedTaskParamValidValue] = validationValue.get();

		co_await onSetAddTask(tr, taskBucket, taskFuture, task);
	}

	static Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<TaskFuture> taskFuture,
	                                 Reference<Task> task,
	                                 KeyRef validationKey,
	                                 KeyRef validationValue) {
		taskFuture->futureBucket->setOptions(tr);

		task->params[Task::reservedTaskParamValidKey] = validationKey;
		task->params[Task::reservedTaskParamValidValue] = validationValue;

		return onSetAddTask(tr, taskBucket, taskFuture, task);
	}

	static Future<Reference<TaskFuture>> joinedFuture(Reference<ReadYourWritesTransaction> tr,
	                                                  Reference<TaskBucket> taskBucket,
	                                                  Reference<TaskFuture> taskFuture) {
		taskFuture->futureBucket->setOptions(tr);

		std::vector<Reference<TaskFuture>> vectorFuture;
		// the next line means generate a new task future with different key,
		// but share the same prefix of futureBucket with the input taskFuture
		Reference<TaskFuture> future = taskFuture->futureBucket->future(tr);
		vectorFuture.push_back(future);
		co_await join(tr, taskBucket, taskFuture, vectorFuture);
		co_return future;
	}
};

TaskFuture::TaskFuture() = default;

TaskFuture::TaskFuture(const Reference<FutureBucket> bucket, Key k) : futureBucket(bucket), key(k) {
	if (k.empty()) {
		key = deterministicRandom()->randomUniqueID().toString();
	}

	prefix = futureBucket->prefix.get(key); // this ::get actually append the key to the taskBucket prefix
	blocks = prefix.get("bl"_sr);
	callbacks = prefix.get("cb"_sr);
}

TaskFuture::~TaskFuture() = default;

void TaskFuture::addBlock(Reference<ReadYourWritesTransaction> tr, StringRef block_id) {
	tr->set(blocks.pack(block_id), ""_sr);
}

Future<Void> TaskFuture::set(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
	return TaskFutureImpl::set(tr, taskBucket, Reference<TaskFuture>::addRef(this));
}

Future<Void> TaskFuture::performAllActions(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
	return TaskFutureImpl::performAllActions(tr, taskBucket, Reference<TaskFuture>::addRef(this));
}

Future<Void> TaskFuture::join(Reference<ReadYourWritesTransaction> tr,
                              Reference<TaskBucket> taskBucket,
                              std::vector<Reference<TaskFuture>> vectorFuture) {
	return TaskFutureImpl::join(tr, taskBucket, Reference<TaskFuture>::addRef(this), vectorFuture);
}

Future<bool> TaskFuture::isSet(Reference<ReadYourWritesTransaction> tr) {
	return TaskFutureImpl::isSet(tr, Reference<TaskFuture>::addRef(this));
}

Future<Void> TaskFuture::onSet(Reference<ReadYourWritesTransaction> tr,
                               Reference<TaskBucket> taskBucket,
                               Reference<Task> task) {
	return TaskFutureImpl::onSet(tr, taskBucket, Reference<TaskFuture>::addRef(this), task);
}

Future<Void> TaskFuture::onSetAddTask(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<Task> task) {
	return TaskFutureImpl::onSetAddTask(tr, taskBucket, Reference<TaskFuture>::addRef(this), task);
}

Future<Void> TaskFuture::onSetAddTask(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<Task> task,
                                      KeyRef validationKey) {
	return TaskFutureImpl::onSetAddTask(tr, taskBucket, Reference<TaskFuture>::addRef(this), task, validationKey);
}

Future<Void> TaskFuture::onSetAddTask(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<Task> task,
                                      KeyRef validationKey,
                                      KeyRef validationValue) {
	return TaskFutureImpl::onSetAddTask(
	    tr, taskBucket, Reference<TaskFuture>::addRef(this), task, validationKey, validationValue);
}

Future<Reference<TaskFuture>> TaskFuture::joinedFuture(Reference<ReadYourWritesTransaction> tr,
                                                       Reference<TaskBucket> taskBucket) {
	return TaskFutureImpl::joinedFuture(tr, taskBucket, Reference<TaskFuture>::addRef(this));
}

Future<Key> getCompletionKey(TaskCompletionKey* self, Future<Reference<TaskFuture>> f) {
	Reference<TaskFuture> taskFuture = co_await f;
	self->joinFuture.clear();
	self->key = taskFuture->key;
	co_return self->key.get();
}

Future<Key> TaskCompletionKey::get(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket) {
	ASSERT(key.present() == (joinFuture.getPtr() == nullptr));
	// from the parent bucket, it generate a new taskfuture and returns the key of the new taskfuture
	return key.present() ? key.get() : getCompletionKey(this, joinFuture->joinedFuture(tr, taskBucket));
}
