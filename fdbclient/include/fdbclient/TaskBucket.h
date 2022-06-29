/*
 * TaskBucket.h
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

#ifndef FDBCLIENT_TASK_BUCKET_H
#define FDBCLIENT_TASK_BUCKET_H
#pragma once

#include "flow/flow.h"
#include "flow/IDispatched.h"
#include "flow/genericactors.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/Subspace.h"
#include "fdbclient/KeyBackedTypes.h"

class FutureBucket;
class TaskFuture;

FDB_DECLARE_BOOLEAN_PARAM(AccessSystemKeys);
FDB_DECLARE_BOOLEAN_PARAM(PriorityBatch);
FDB_DECLARE_BOOLEAN_PARAM(VerifyTask);
FDB_DECLARE_BOOLEAN_PARAM(UpdateParams);

// A Task is a set of key=value parameters that constitute a unit of work for a TaskFunc to perform.
// The parameter keys are specific to the TaskFunc that the Task is for, except for a set of reserved
// parameter keys which are used by TaskBucket to determine which TaskFunc to run and provide
// several other core features of TaskBucket.
//
// Task Life Cycle
//   1.  Task is created in database transaction.
//   2.  An executor (see TaskBucket class) will reserve an begin executing the task
//   3.  Task's _execute() function is run.  This is non-transactional, and can run indefinitely.
//   4.  If the executor loses contact with FDB, another executor may begin at step 2.  The first
//       Task execution can detect this by checking the result of keepRunning() periodically.
//   5.  Once a Task execution's _execute() call returns, the _finish() step is called.
//       _finish() is transactional and is guaranteed to never be called more than once for the
//       same Task
class Task : public ReferenceCounted<Task> {
public:
	Task(Value type = StringRef(), uint32_t version = 0, Value done = StringRef(), unsigned int priority = 0);
	~Task(){};

	// Methods that safely read values from task's params
	uint32_t getVersion() const;
	unsigned int getPriority() const;

	Key key;
	Version timeoutVersion;

	// Take this lock while you don't want Taskbucket to try to extend your task's timeout
	FlowLock extendMutex;

	Map<Key, Value> params; // SOMEDAY: use one arena?

	// New reserved task parameter keys should be added in ReservedTaskParams below instead of here.
	// Task priority, determines execution order relative to other queued Tasks
	static Key reservedTaskParamKeyPriority;
	// Name of the registered TaskFunc that this Task is for
	static Key reservedTaskParamKeyType;
	static Key reservedTaskParamKeyAddTask;
	static Key reservedTaskParamKeyDone;
	static Key reservedTaskParamKeyFuture;
	static Key reservedTaskParamKeyBlockID;
	static Key reservedTaskParamKeyVersion;

	// Optional parameters that specify a database Key that must have a specific Value in order for the Task
	// to be executed (for _execute() or _finish() to be called)  OR for keepRunning() to return true for the Task.
	static Key reservedTaskParamValidKey;
	static Key reservedTaskParamValidValue;

	Reference<TaskFuture> getDoneFuture(Reference<FutureBucket> fb);

	std::string toString() const {
		std::string s = format("TASK [key=%s timeoutVersion=%lld ", key.printable().c_str(), timeoutVersion);
		for (auto& param : params)
			s.append(format("%s=%s ", param.key.printable().c_str(), param.value.printable().c_str()));
		s.append("]");
		return s;
	}
};

// TaskParam is a convenience class to make storing non-string types into Task Parameters easier.
template <typename T>
class TaskParam {
public:
	TaskParam(StringRef key) : key(key) {}
	T get(Reference<Task> task) const { return Codec<T>::unpack(Tuple::unpack(task->params[key])); }
	void set(Reference<Task> task, T const& val) const { task->params[key] = Codec<T>::pack(val).pack(); }
	bool exists(Reference<Task> task) const { return task->params.find(key) != task->params.end(); }
	T getOrDefault(Reference<Task> task, const T defaultValue = T()) const {
		if (!exists(task))
			return defaultValue;
		return get(task);
	}
	StringRef key;
};

struct ReservedTaskParams {
	static TaskParam<Version> scheduledVersion() { return LiteralStringRef(__FUNCTION__); }
};

class FutureBucket;

// A TaskBucket is a subspace in which a set of Tasks and TaskFutures exists.  Within the subspace, there
// are several other subspaces including
//   available - Tasks that are waiting to run at default priority
//	 available_prioritized - Tasks with priorities 0 through max priority, all higher than default
//   timeouts - Tasks that are currently running and are scheduled to timeout a specific FDB commit version.
//   futures - TaskFutures that have not been completed
//
// One or more processes must instantiate a TaskBucket call run() or doOne() repeatedly in order for Tasks
// in the TaskBucket to make progress.  The calling process is directly used to execute the Task code.
//
// Tasks are added to a TaskBucket with addTask(), and this can be done at any time but is typically done
// in the _finish() step of a Task.  This makes the completion of one Task and the creation of its one or
// more child Tasks atomic.
//
// While a TaskBucket instance is executing a task, there is timeout set for the Task and periodically the
// executor will extend this timeout in the database.  If this fails to happen, then another TaskBucket
// instance may declare the Task a failure and move it back to the available subspace.
class TaskBucket : public ReferenceCounted<TaskBucket> {
public:
	TaskBucket(const Subspace& subspace,
	           AccessSystemKeys = AccessSystemKeys::False,
	           PriorityBatch = PriorityBatch::False,
	           LockAware = LockAware::False);
	virtual ~TaskBucket();

	void setOptions(Reference<ReadYourWritesTransaction> tr) {
		if (system_access)
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		if (lockAware)
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	}

	Future<Void> changePause(Reference<ReadYourWritesTransaction> tr, bool pause);
	Future<Void> changePause(Database cx, bool pause) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return changePause(tr, pause); });
	}

	Future<Void> clear(Reference<ReadYourWritesTransaction> tr);
	Future<Void> clear(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return clear(tr); });
	}

	// Transactions inside an execute() function should call this and stop without committing if it returns false.
	Future<Void> keepRunning(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
		Future<bool> finished = isFinished(tr, task);
		Future<bool> valid = isVerified(tr, task);
		return map(success(finished) && success(valid), [=](Void) {
			if (finished.get() || !valid.get()) {
				throw task_interrupted();
			}
			return Void();
		});
	}
	Future<Void> keepRunning(Database cx, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return keepRunning(tr, task); });
	}

	static void setValidationCondition(Reference<Task> task, KeyRef vKey, KeyRef vValue);

	Standalone<StringRef> addTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);

	Future<Standalone<StringRef>> addTask(Reference<ReadYourWritesTransaction> tr,
	                                      Reference<Task> task,
	                                      KeyRef validationKey);

	Standalone<StringRef> addTask(Reference<ReadYourWritesTransaction> tr,
	                              Reference<Task> task,
	                              KeyRef validationKey,
	                              KeyRef validationValue);

	Future<Reference<Task>> getOne(Reference<ReadYourWritesTransaction> tr);
	Future<Reference<Task>> getOne(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return getOne(tr); });
	}

	Future<bool> doTask(Database cx, Reference<FutureBucket> futureBucket, Reference<Task> task);

	Future<bool> doOne(Database cx, Reference<FutureBucket> futureBucket);

	Future<Void> run(Database cx,
	                 Reference<FutureBucket> futureBucket,
	                 std::shared_ptr<double const> pollDelay,
	                 int maxConcurrentTasks);
	Future<Void> watchPaused(Database cx, Reference<AsyncVar<bool>> paused);

	Future<bool> isEmpty(Reference<ReadYourWritesTransaction> tr);
	Future<bool> isEmpty(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return isEmpty(tr); });
	}

	Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<Void> finish(Database cx, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return finish(tr, task); });
	}

	// Extend the task's timeout as if it just started and also save any parameter changes made to the task
	Future<Version> extendTimeout(Reference<ReadYourWritesTransaction> tr,
	                              Reference<Task> task,
	                              UpdateParams updateParams,
	                              Version newTimeoutVersion = invalidVersion);
	Future<Void> extendTimeout(Database cx,
	                           Reference<Task> task,
	                           UpdateParams updateParams,
	                           Version newTimeoutVersion = invalidVersion) {
		return map(runRYWTransaction(cx,
		                             [=](Reference<ReadYourWritesTransaction> tr) {
			                             return extendTimeout(tr, task, updateParams, newTimeoutVersion);
		                             }),
		           [=](Version v) {
			           task->timeoutVersion = v;
			           return Void();
		           });
	}

	Future<bool> isFinished(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<bool> isFinished(Database cx, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return isFinished(tr, task); });
	}

	Future<bool> isVerified(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<bool> isVerified(Database cx, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return isVerified(tr, task); });
	}

	Future<bool> checkActive(Database cx);

	Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr);
	Future<int64_t> getTaskCount(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return getTaskCount(tr); });
	}

	Future<Void> watchTaskCount(Reference<ReadYourWritesTransaction> tr);

	static Future<Void> debugPrintRange(Reference<ReadYourWritesTransaction> tr, Subspace subspace, Key msg);
	static Future<Void> debugPrintRange(Database cx, Subspace subspace, Key msg) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return debugPrintRange(tr, subspace, msg); });
	}

	bool getSystemAccess() const { return system_access; }

	bool getLockAware() const { return lockAware; }

	Key getPauseKey() const { return pauseKey; }

	Subspace getAvailableSpace(int priority = 0) const {
		if (priority == 0)
			return available;
		return available_prioritized.get(priority);
	}

	Database src;
	Map<Key, Future<Reference<KeyRangeMap<Version>>>> key_version;

	CounterCollection cc;

	Counter dispatchSlotChecksStarted;
	Counter dispatchErrors;
	Counter dispatchDoTasks;
	Counter dispatchEmptyTasks;
	Counter dispatchSlotChecksComplete;
	UID dbgid;

	double getTimeoutSeconds() const { return (double)timeout / CLIENT_KNOBS->CORE_VERSIONSPERSECOND; }

private:
	friend class TaskBucketImpl;

	Future<Void> metricLogger;

	Subspace prefix;
	Subspace active;
	Key pauseKey;

	// Available task subspaces.  Priority 0, the default, will be under available which is backward
	// compatible with pre-priority TaskBucket processes.  Priority 1 and higher will be in
	// available_prioritized.  Currently only priority level 1 is allowed but adding more levels
	// in the future is possible and simple.
	Subspace available;
	Subspace available_prioritized;
	Subspace timeouts;
	uint32_t timeout;
	bool system_access;
	bool priority_batch;
	bool lockAware;
};

class TaskFuture;

class FutureBucket : public ReferenceCounted<FutureBucket> {
public:
	FutureBucket(const Subspace& subspace, AccessSystemKeys = AccessSystemKeys::False, LockAware = LockAware::False);
	virtual ~FutureBucket();

	void setOptions(Reference<ReadYourWritesTransaction> tr) {
		if (system_access)
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		if (lockAware)
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	}

	Future<Void> clear(Reference<ReadYourWritesTransaction> tr);
	Future<Void> clear(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return clear(tr); });
	}

	Reference<TaskFuture> future(Reference<ReadYourWritesTransaction> tr);

	Future<bool> isEmpty(Reference<ReadYourWritesTransaction> tr);
	Future<bool> isEmpty(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return isEmpty(tr); });
	}

	Reference<TaskFuture> unpack(Key key);
	bool isSystemAccess() const { return system_access; };
	bool isLockAware() const { return lockAware; };

private:
	friend class TaskFuture;
	friend class FutureBucketImpl;
	friend class TaskFutureImpl;

	Subspace prefix;
	bool system_access;
	bool lockAware;
};

class TaskFuture : public ReferenceCounted<TaskFuture> {
public:
	TaskFuture();
	TaskFuture(const Reference<FutureBucket> bucket, Standalone<StringRef> key = Standalone<StringRef>());
	virtual ~TaskFuture();

	Future<bool> isSet(Reference<ReadYourWritesTransaction> tr);
	Future<bool> isSet(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return isSet(tr); });
	}

	Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<Task> task);
	Future<Void> onSetAddTask(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return onSetAddTask(tr, taskBucket, task); });
	}

	Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<Task> task,
	                          KeyRef validationKey);
	Future<Void> onSetAddTask(Database cx,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<Task> task,
	                          KeyRef validationKey) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
			return onSetAddTask(tr, taskBucket, task, validationKey);
		});
	}

	Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<Task> task,
	                          KeyRef validationKey,
	                          KeyRef validationValue);
	Future<Void> onSetAddTask(Database cx,
	                          Reference<TaskBucket> taskBucket,
	                          Reference<Task> task,
	                          KeyRef validationKey,
	                          KeyRef validationValue) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
			return onSetAddTask(tr, taskBucket, task, validationKey, validationValue);
		});
	}

	Future<Void> onSet(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> task);
	Future<Void> onSet(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task) {
		return runRYWTransaction(cx,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return onSet(tr, taskBucket, task); });
	}

	Future<Void> set(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);
	Future<Void> set(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return set(tr, taskBucket); });
	}

	Future<Void> join(Reference<ReadYourWritesTransaction> tr,
	                  Reference<TaskBucket> taskBucket,
	                  std::vector<Reference<TaskFuture>> vectorFuture);
	Future<Void> join(Database cx, Reference<TaskBucket> taskBucket, std::vector<Reference<TaskFuture>> vectorFuture) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return join(tr, taskBucket, vectorFuture); });
	}

	Future<Void> performAllActions(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);
	Future<Void> performAllActions(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return performAllActions(tr, taskBucket); });
	}

	Future<Reference<TaskFuture>> joinedFuture(Reference<ReadYourWritesTransaction> tr,
	                                           Reference<TaskBucket> taskBucket);
	Future<Reference<TaskFuture>> joinedFuture(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(cx,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return joinedFuture(tr, taskBucket); });
	}

	Standalone<StringRef> pack() { return key; };
	void addBlock(Reference<ReadYourWritesTransaction> tr, StringRef block_id);

	Reference<FutureBucket> futureBucket;
	Standalone<StringRef> key;

	Subspace prefix;
	Subspace blocks;
	Subspace callbacks;
};

struct TaskFuncBase : IDispatched<TaskFuncBase, Standalone<StringRef>, std::function<TaskFuncBase*()>>,
                      ReferenceCounted<TaskFuncBase> {
	virtual ~TaskFuncBase(){};
	static Reference<TaskFuncBase> create(Standalone<StringRef> const& taskFuncType) {
		return Reference<TaskFuncBase>(dispatch(taskFuncType)());
	}

	static bool isValidTaskType(StringRef type) {
		return (type.size()) && (dispatches().find(type) != dispatches().end());
	}

	static bool isValidTask(Reference<Task> task) {
		auto itor = task->params.find(Task::reservedTaskParamKeyType);
		if (itor == task->params.end())
			return false;

		return isValidTaskType(itor->value);
	}

	virtual StringRef getName() const = 0;

	// At least once semantics; can take as long as it wants subject to the taskbucket timeout
	virtual Future<Void> execute(Database cx,
	                             Reference<TaskBucket> tb,
	                             Reference<FutureBucket> fb,
	                             Reference<Task> task) = 0;

	// *Database* operations here are exactly once; side effects are at least once; excessive time here may prevent task
	// from finishing!
	virtual Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> tb,
	                            Reference<FutureBucket> fb,
	                            Reference<Task> task) = 0;

	virtual Future<Void> handleError(Database cx, Reference<Task> task, Error const& error) { return Void(); }

	template <class TaskFuncBaseType>
	struct Factory {
		static TaskFuncBase* create() { return (TaskFuncBase*)(new TaskFuncBaseType()); }
	};
};
#define REGISTER_TASKFUNC(TaskFunc) REGISTER_FACTORY(TaskFuncBase, TaskFunc, name)
#define REGISTER_TASKFUNC_ALIAS(TaskFunc, Alias)                                                                       \
	REGISTER_DISPATCHED_ALIAS(TaskFunc, Alias, TaskFunc::name, LiteralStringRef(#Alias))

struct TaskCompletionKey {
	Future<Key> get(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);

	Optional<Key> key;
	Reference<TaskFuture> joinFuture;

	static TaskCompletionKey joinWith(Reference<TaskFuture> f) { return TaskCompletionKey(f); }

	static TaskCompletionKey signal(Key k) { return TaskCompletionKey(k); }

	static TaskCompletionKey signal(Reference<TaskFuture> f) { return TaskCompletionKey(f->key); }

	static TaskCompletionKey noSignal() { return TaskCompletionKey(StringRef()); }
	TaskCompletionKey() {}

private:
	TaskCompletionKey(Reference<TaskFuture> f) : joinFuture(f) {}
	TaskCompletionKey(Key k) : key(k) {}
};

#endif
