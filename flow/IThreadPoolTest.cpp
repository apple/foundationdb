/*
 * IThreadPoolTest.cpp
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

// Thread naming only works on Linux.
#if defined(__linux__)

#include "flow/IThreadPool.h"

#include <pthread.h>
#include <iostream>

#include "flow/Coroutines.h"
#include "flow/UnitTest.h"

void forceLinkIThreadPoolTests() {}

struct ThreadNameReceiver final : IThreadPoolReceiver {
	void init() override {}

	struct GetNameAction final : TypedAction<ThreadNameReceiver, GetNameAction> {
		ThreadReturnPromise<std::string> name;

		double getTimeEstimate() const override { return 3.; }
	};

	void action(GetNameAction& a) {
		pthread_t t = pthread_self();
		const size_t arrayLen = 16;
		char name[arrayLen];
		int err = pthread_getname_np(t, name, arrayLen);
		if (err != 0) {
			std::cout << "Get name failed with error code: " << err << std::endl;
			a.name.sendError(platform_error());
			return;
		}
		std::string s = name;
		ASSERT(a.name.isValid());
		a.name.send(std::move(s));
		ASSERT(!a.name.isValid());
	}
};

TEST_CASE("/flow/IThreadPool/NamedThread") {
	noUnseed = true;

	Reference<IThreadPool> pool = createGenericThreadPool();
	pool->addThread(new ThreadNameReceiver(), "thread-foo");

	// Warning: this action is a little racy with the call to `pthread_setname_np`. In practice,
	// ~nothing should depend on the thread name being set instantaneously. If this test ever
	// flakes, we can make `startThread` in platform a little bit more complex to clearly order
	// the actions.
	auto* a = new ThreadNameReceiver::GetNameAction();
	auto fut = a->name.getFuture();
	pool->post(a);

	std::string name = co_await fut;
	if (name != "thread-foo") {
		std::cout << "Incorrect thread name: " << name << std::endl;
		ASSERT(false);
	}

	co_await pool->stop();
}

struct ThreadSafePromiseStreamSender final : IThreadPoolReceiver {
	explicit ThreadSafePromiseStreamSender(ThreadReturnPromiseStream<std::string>* notifications)
	  : notifications(notifications) {}
	void init() override {}

	struct GetNameAction final : TypedAction<ThreadSafePromiseStreamSender, GetNameAction> {
		double getTimeEstimate() const override { return 3.; }
	};

	void action(GetNameAction& a) {
		pthread_t t = pthread_self();
		const size_t arrayLen = 16;
		char name[arrayLen];
		int err = pthread_getname_np(t, name, arrayLen);
		if (err != 0) {
			std::cout << "Get name failed with error code: " << err << std::endl;
			notifications->sendError(platform_error());
			return;
		}
		notifications->send(name);
	}

	struct FaultyAction final : TypedAction<ThreadSafePromiseStreamSender, FaultyAction> {
		double getTimeEstimate() const override { return 3.; }
	};

	void action(FaultyAction& a) { notifications->sendError(platform_error().asInjectedFault()); }

private:
	ThreadReturnPromiseStream<std::string>* notifications;
};

TEST_CASE("/flow/IThreadPool/ThreadReturnPromiseStream") {
	noUnseed = true;

	std::unique_ptr<ThreadReturnPromiseStream<std::string>> notifications(new ThreadReturnPromiseStream<std::string>());

	Reference<IThreadPool> pool = createGenericThreadPool();
	pool->addThread(new ThreadSafePromiseStreamSender(notifications.get()), "thread-foo");

	// Warning: this action is a little racy with the call to `pthread_setname_np`. In practice,
	// ~nothing should depend on the thread name being set instantaneously. If this test ever
	// flakes, we can make `startThread` in platform a little bit more complex to clearly order
	// the actions.
	int num = 3;
	for (int i = 0; i < num; ++i) {
		auto* a = new ThreadSafePromiseStreamSender::GetNameAction();
		pool->post(a);
	}

	ThreadFutureStream<std::string> futs = notifications->getFuture();

	int n = 0;
	while (n < num) {
		std::string name = co_await futs;
		if (name != "thread-foo") {
			std::cout << "Incorrect thread name: " << name << std::endl;
			ASSERT(false);
		}
		++n;
	}

	ASSERT(n == num);

	auto* faultyAction = new ThreadSafePromiseStreamSender::FaultyAction();
	pool->post(faultyAction);

	try {
		std::string name = co_await futs;
		ASSERT(false);
	} catch (Error& e) {
		ASSERT(e.isInjectedFault());
	}

	co_await pool->stop();
}

struct MockReceiver : public IThreadPoolReceiver {
	void init() final {}
};

struct MockTask final : public ThreadAction {
	ThreadReturnPromise<Void> promise;

	void operator()(IThreadPoolReceiver*) final {
		promise.send(Void());
		delete this;
	}

	void cancel() final {}

	double getTimeEstimate() const final { return 0; }
};

Reference<IThreadPool> initTestPool() {
	auto pool = createGenericThreadPool();
	auto task = g_network->getCurrentTask();
	g_network->setCurrentTask(TaskPriority::Worker);
	pool->addThread(new MockReceiver(), "TestWorker");
	g_network->setCurrentTask(task);
	return pool;
}

// These two cases are used to verify the destruction of the ThreadPool.
// See the comments within ThreadPool::stop() for more details.

TEST_CASE("/flow/IThreadPool/ExplicitStop") {
	noUnseed = true;

	Reference<IThreadPool> pool = initTestPool();
	auto task = new MockTask();
	auto future = task->promise.getFuture();
	pool->post(task);
	co_await future;
	co_await pool->stop();
}

TEST_CASE("/flow/IThreadPool/ImplicitStop") {
	noUnseed = true;

	Reference<IThreadPool> pool = initTestPool();
	auto task = new MockTask();
	auto future = task->promise.getFuture();
	pool->post(task);
	co_await future;
}

#else
void forceLinkIThreadPoolTests() {}
#endif
