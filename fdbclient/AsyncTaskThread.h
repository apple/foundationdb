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
};

template <class F>
class AsyncTask : public IAsyncTask {
	F func;

public:
	AsyncTask(const F& func) : func(func) {}

	void operator()() override { func(); }
};

class AsyncTaskThread {
	ThreadSafeQueue<std::shared_ptr<IAsyncTask>> queue;
	std::promise<void> wakeUp;
	std::thread thread;

	static void run(AsyncTaskThread* conn) {
		while (true) {
			std::shared_ptr<IAsyncTask> task;
			{
				if (conn->queue.canSleep()) {
					conn->wakeUp.get_future().get();
					conn->wakeUp = {};
				}
				task = conn->queue.pop().get();
			}
		}
	}

	template <class F>
	void addTask(const F& func) {
		if (queue.push(std::make_shared<AsyncTask<F>>(func))) {
			wakeUp.set_value();
		}
	}

public:
	AsyncTaskThread() : thread([this] { run(this); }) {}

	template <class F>
	auto execAsync(const F& func, TaskPriority priority = TaskPriority::DefaultOnMainThread)
	    -> Future<decltype(func())> {
		if (g_network->isSimulated()) {
			// TODO: Add some random delay
			return func();
		}
		Promise<decltype(func())> promise;
		addTask([&promise, &func, priority] {
			auto funcResult = func();
			onMainThreadVoid([&promise, &funcResult] { promise.send(funcResult); }, nullptr,
			                 priority); // TODO: Add error handling
		});
		return promise.getFuture();
	}
};

#endif
