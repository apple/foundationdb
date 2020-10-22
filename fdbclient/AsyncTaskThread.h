/*
 * AsyncTaskThread.h
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

#ifndef __ASYNC_TASK_THREAD_H__
#define __ASYNC_TASK_THREAD_H__

#include <future>
#include <thread>
#include <memory>

#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/ThreadSafeQueue.h"

class IAsyncTask {
public:
	virtual void operator()() = 0;
	virtual ~IAsyncTask() = default;
	virtual bool isTerminate() const = 0;
};

class TerminateTask final : public IAsyncTask {
public:
	void operator()() override { ASSERT(false); }
	bool isTerminate() const { return true; }
};

template <class F>
class AsyncTask final : public IAsyncTask {
	F func;

public:
	AsyncTask(const F& func) : func(func) {}

	void operator()() override { func(); }
	bool isTerminate() const override { return false; }
};

class AsyncTaskThread {
	ThreadSafeQueue<std::shared_ptr<IAsyncTask>> queue;
	std::promise<void> wakeUp;
	std::thread thread;

	static void run(AsyncTaskThread* self) {
		while (true) {
			if (self->queue.canSleep()) {
				self->wakeUp.get_future().get();
				self->wakeUp = {};
			}
			std::shared_ptr<IAsyncTask> task = self->queue.pop().get();
			if (task->isTerminate()) {
				return;
			}
			(*task)();
		}
	}

	template <class F>
	void addTask(const F& func) {
		if (queue.push(std::make_shared<AsyncTask<F>>(func))) {
			wakeUp.set_value();
		}
	}

	static const double meanDelay;

public:
	AsyncTaskThread() : thread([this] { run(this); }) {}

	~AsyncTaskThread() {
		if (queue.push(std::make_shared<TerminateTask>())) {
			wakeUp.set_value();
		}
		// Warning: This destructor can hang if a task hangs, so it is
		// up to the caller to prevent tasks from hanging indefinitely
		thread.join();
	}

	template <class F>
	auto execAsync(const F& func, TaskPriority priority = TaskPriority::DefaultOnMainThread)
	    -> Future<decltype(func())> {
		if (g_network->isSimulated()) {
			return map(delayJittered(meanDelay), [func](Void _) { return func(); });
		}
		Promise<decltype(func())> promise;
		addTask([promise, func, priority] {
			try {
				auto funcResult = func();
				onMainThreadVoid([promise, funcResult] { promise.send(funcResult); }, nullptr, priority);
			} catch (Error& e) {
				onMainThreadVoid([promise, e] { promise.sendError(e); }, nullptr, priority);
			}
		});
		return promise.getFuture();
	}
};

#endif
