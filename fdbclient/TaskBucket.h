/*
 * TaskBucket.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "FDBTypes.h"
#include "NativeAPI.h"
#include "RunTransaction.actor.h"
#include "Subspace.h"

class Task : public ReferenceCounted<Task> {
public:
	Task(Value type = StringRef(), uint32_t version = 0, Value done = StringRef(), unsigned int priority = 0);
	~Task(){};

	// Methods that safely read values from task's params
	uint32_t getVersion() const;
	unsigned int getPriority() const;

	Key key;
	uint64_t timeout;

	Map<Key, Value> params; // SOMEDAY: use one arena?

	static Key reservedTaskParamKeyPriority;
	static Key reservedTaskParamKeyType;
	static Key reservedTaskParamKeyAddTask;
	static Key reservedTaskParamKeyDone;
	static Key reservedTaskParamKeyFuture;
	static Key reservedTaskParamKeyBlockID;
	static Key reservedTaskParamKeyVersion;
	static Key reservedTaskParamValidKey;
	static Key reservedTaskParamValidValue;
};

class FutureBucket;

class TaskBucket : public ReferenceCounted<TaskBucket> {
public:
	TaskBucket(const Subspace& subspace, bool sysAccess = false, bool priorityBatch = false, bool lockAware = false);
	virtual ~TaskBucket();
	
	void setOptions(Reference<ReadYourWritesTransaction> tr) {
		if (system_access)
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		if (lock_aware)
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	}

	Future<Void> clear(Reference<ReadYourWritesTransaction> tr);
	Future<Void> clear(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return clear(tr); });
	}

	// Transactions inside an execute() function should call this and stop without committing if it returns false.
	Future<bool> keepRunning(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
		Future<bool> finished = isFinished(tr, task);
		Future<bool> valid = isVerified(tr, task);
		return map(success(finished) && success(valid), [=](Void) -> bool { return !finished.get() && valid.get(); } );
	}

	static void setValidationCondition(Reference<Task> task, KeyRef vKey, KeyRef vValue);

	Standalone<StringRef> addTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);

	Future<Standalone<StringRef>> addTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task, KeyRef validationKey);

	Standalone<StringRef> addTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task, KeyRef validationKey, KeyRef validationValue);

	Future<Reference<Task>> getOne(Reference<ReadYourWritesTransaction> tr);
	Future<Reference<Task>> getOne(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getOne(tr); });
	}

	Future<bool> doTask(Database cx, Reference<FutureBucket> futureBucket, Reference<Task> task);

	Future<bool> doOne(Database cx, Reference<FutureBucket> futureBucket);

	Future<Void> run(Database cx, Reference<FutureBucket> futureBucket, double *pollDelay, int maxConcurrentTasks);

	Future<bool> isEmpty(Reference<ReadYourWritesTransaction> tr);
	Future<bool> isEmpty(Database cx){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return isEmpty(tr); });
	}

	Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<Void> finish(Database cx, Reference<Task> task){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return finish(tr, task); });
	}

	// Extend the task's timeout as if it just started and also save any parameter changes made to the task
	Future<bool> saveAndExtend(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<bool> saveAndExtend(Database cx, Reference<Task> task){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return saveAndExtend(tr, task); });
	}

	Future<bool> isFinished(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<bool> isFinished(Database cx, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return isFinished(tr, task); });
	}

	Future<bool> isVerified(Reference<ReadYourWritesTransaction> tr, Reference<Task> task);
	Future<bool> isVerified(Database cx, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return isVerified(tr, task); });
	}

	Future<bool> checkActive(Database cx);

	Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr);
	Future<int64_t> getTaskCount(Database cx){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getTaskCount(tr); });
	}

	Future<Void> watchTaskCount(Reference<ReadYourWritesTransaction> tr);

	static Future<Void> debugPrintRange(Reference<ReadYourWritesTransaction> tr, Subspace subspace, Key msg);
	static Future<Void> debugPrintRange(Database cx, Subspace subspace, Key msg){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return debugPrintRange(tr, subspace, msg); });
	}

	bool getSystemAccess() const {
		return system_access;	
	}

	bool getLockAware() const {
		return lock_aware;	
	}

	Subspace getAvailableSpace(int priority = 0) {
		if(priority == 0)
			return available;
		return available_prioritized.get(priority);
	}

	Database src;
	Map<Key, Future<Reference<KeyRangeMap<Version>>>> key_version;

	double getTimeoutSeconds() const {
		return (double)timeout / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
	}
private:
	friend class TaskBucketImpl;

	Subspace prefix;
	Subspace active;
	
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
	bool lock_aware;
};

class TaskFuture;

class FutureBucket : public ReferenceCounted<FutureBucket> {
public:
	FutureBucket(const Subspace& subspace, bool sysAccess = false, bool lockAware = false);
	virtual ~FutureBucket();

	void setOptions(Reference<ReadYourWritesTransaction> tr) {
		if (system_access)
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		if (lock_aware)
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	}

	Future<Void> clear(Reference<ReadYourWritesTransaction> tr);
	Future<Void> clear(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return clear(tr); });
	}

	Reference<TaskFuture> future(Reference<ReadYourWritesTransaction> tr);

	Future<bool> isEmpty(Reference<ReadYourWritesTransaction> tr);
	Future<bool> isEmpty(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return isEmpty(tr); });
	}

	Reference<TaskFuture> unpack(Key key);
	bool isSystemAccess() const { return system_access; };
	bool isLockAware() const { return lock_aware; };

private:
	friend class TaskFuture;
	friend class FutureBucketImpl;
	friend class TaskFutureImpl;

	Subspace prefix;
	bool system_access;
	bool lock_aware;
};

class TaskFuture : public ReferenceCounted<TaskFuture> {
public:
	TaskFuture();
	TaskFuture(const Reference<FutureBucket> bucket, Standalone<StringRef> key = Standalone<StringRef>());
	virtual ~TaskFuture();

	Future<bool> isSet(Reference<ReadYourWritesTransaction> tr);
	Future<bool> isSet(Database cx) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return isSet(tr); });
	}
	
	Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> task);
	Future<Void> onSetAddTask(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return onSetAddTask(tr, taskBucket, task); });
	}
	
	Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> task, KeyRef validationKey);
	Future<Void> onSetAddTask(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task, KeyRef validationKey){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return onSetAddTask(tr, taskBucket, task, validationKey); });
	}
	
	Future<Void> onSetAddTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> task, KeyRef validationKey, KeyRef validationValue);
	Future<Void> onSetAddTask(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task, KeyRef validationKey, KeyRef validationValue){
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return onSetAddTask(tr, taskBucket, task, validationKey, validationValue); });
	}

	Future<Void> onSet(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> task);
	Future<Void> onSet(Database cx, Reference<TaskBucket> taskBucket, Reference<Task> task) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return onSet(tr, taskBucket, task); });
	}

	Future<Void> set(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);
	Future<Void> set(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return set(tr, taskBucket); });
	}

	Future<Void> join(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, std::vector<Reference<TaskFuture>> vectorFuture);
	Future<Void> join(Database cx, Reference<TaskBucket> taskBucket, std::vector<Reference<TaskFuture>> vectorFuture) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return join(tr, taskBucket, vectorFuture); });
	}

	Future<Void> performAllActions(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);
	Future<Void> performAllActions(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return performAllActions(tr, taskBucket); });
	}

	Future<Reference<TaskFuture>> joinedFuture(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);
	Future<Reference<TaskFuture>> joinedFuture(Database cx, Reference<TaskBucket> taskBucket) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return joinedFuture(tr, taskBucket); });
	}

	Standalone<StringRef> pack() { return key; };
	void addBlock(Reference<ReadYourWritesTransaction> tr, StringRef block_id);

	Reference<FutureBucket> futureBucket;
	Standalone<StringRef> key;

	Subspace prefix;
	Subspace blocks;
	Subspace callbacks;
};

struct TaskFuncBase : IDispatched<TaskFuncBase, Standalone<StringRef>, std::function< TaskFuncBase*() >>, ReferenceCounted<TaskFuncBase> {
	virtual ~TaskFuncBase() {};
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
	virtual Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) = 0;

	// *Database* operations here are exactly once; side effects are at least once; excessive time here may prevent task from finishing!
	virtual Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) = 0;

	template <class TaskFuncBaseType>
	struct Factory {
		static TaskFuncBase* create() {
			return (TaskFuncBase*)(new TaskFuncBaseType());
		}
	};
};
#define REGISTER_TASKFUNC(TaskFunc) REGISTER_FACTORY(TaskFuncBase, TaskFunc, name)

struct TaskCompletionKey {
	Future<Key> get(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket);

	Optional<Key> key;
	Reference<TaskFuture> joinFuture;

	static TaskCompletionKey joinWith(Reference<TaskFuture> f) {
		return TaskCompletionKey(f);
	}

	static TaskCompletionKey signal(Key k) {
		return TaskCompletionKey(k);
	}

	static TaskCompletionKey signal(Reference<TaskFuture> f) {
		return TaskCompletionKey(f->key);
	}

	static TaskCompletionKey noSignal() {
		return TaskCompletionKey(StringRef());
	}

private:
	TaskCompletionKey(Reference<TaskFuture> f) : joinFuture(f) { }
	TaskCompletionKey(Key k) : key(k) { }
};

#endif
