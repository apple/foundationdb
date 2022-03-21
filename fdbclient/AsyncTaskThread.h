/*
 * AsyncTaskThread.h
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

#ifndef __ASYNC_TASK_THREAD_H__
#define __ASYNC_TASK_THREAD_H__

#include <thread>
#include <memory>
#include <mutex>

#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/ThreadSafeQueue.h"

class IAsyncTask {
public:
	virtual void operator()() = 0;
	virtual ~IAsyncTask() = default;
	virtual bool isTerminate() const = 0;
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
	ThreadSafeQueue<std::unique_ptr<IAsyncTask>> queue;
	std::condition_variable cv;
	std::mutex m;
	std::thread thread;

	static void run(AsyncTaskThread* self);

	template <class F>
	void addTask(const F& func) {
		bool wakeUp = false;
		{
			std::lock_guard<std::mutex> g(m);
			wakeUp = queue.push(std::make_unique<AsyncTask<F>>(func));
		}
		if (wakeUp) {
			cv.notify_one();
		}
	}

	static const double meanDelay;

public:
	AsyncTaskThread();

	// Warning: This destructor can hang if a task hangs, so it is
	// up to the caller to prevent tasks from hanging indefinitely
	~AsyncTaskThread();

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
				TraceEvent("ErrorExecutingAsyncTask").error(e);
				onMainThreadVoid([promise, e] { promise.sendError(e); }, nullptr, priority);
			}
		});
		return promise.getFuture();
	}
};

#endif
