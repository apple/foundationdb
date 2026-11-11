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

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "flow/IThreadPool.h"
#include "flow/Coroutines.h"
#include "flow/UnitTest.h"

void forceLinkIThreadPoolTests() {}

// Thread naming only works on Linux.
#if defined(__linux__)

#include <pthread.h>

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

Future<std::string> getThreadName(Reference<IThreadPool> pool) {
	auto* a = new ThreadNameReceiver::GetNameAction();
	auto fut = a->name.getFuture();
	pool->post(a);
	return fut;
}

Future<bool> waitForThreadName(Reference<IThreadPool> pool, std::string const& expectedName) {
	// startThread() sets the pthread name from the creating thread after pthread_create(), so the worker may
	// briefly report its default name before the requested name is visible. Some environments also report
	// ENOENT from pthread_setname_np(), which startThread() treats as non-fatal.
	double deadline = now() + 5.0;
	std::string lastName;
	while (now() < deadline) {
		lastName = co_await getThreadName(pool);
		if (lastName == expectedName) {
			co_return true;
		}
		co_await delay(0.01);
	}
	std::cout << "Thread name was not set; last observed thread name: " << lastName << std::endl;
	co_return false;
}

TEST_CASE("/flow/IThreadPool/NamedThread") {
	noUnseed = true;

	Reference<IThreadPool> pool = createGenericThreadPool();
	pool->addThread(new ThreadNameReceiver(), "thread-foo");

	ASSERT(co_await waitForThreadName(pool, "thread-foo"));

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

	ThreadFutureStream<std::string> futs = notifications->getFuture();
	bool threadNameAvailable = false;
	double deadline = now() + 5.0;
	std::string lastName;
	while (now() < deadline) {
		auto* a = new ThreadSafePromiseStreamSender::GetNameAction();
		pool->post(a);
		lastName = co_await futs;
		if (lastName == "thread-foo") {
			threadNameAvailable = true;
			break;
		}
		co_await delay(0.01);
	}
	if (!threadNameAvailable) {
		std::cout << "Thread name was not set; last observed thread name: " << lastName << std::endl;
	}

	int num = 3;
	for (int i = 0; i < num; ++i) {
		auto* a = new ThreadSafePromiseStreamSender::GetNameAction();
		pool->post(a);
	}

	int n = 0;
	while (n < num) {
		std::string name = co_await futs;
		if (threadNameAvailable && name != "thread-foo") {
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

#endif

// Generic Dynamic ThreadPool tests
// will fallback to the standard generic threadpool if jthread is not supported
namespace details {

static std::mutex threadsMutex;
static uint32_t maxTemporaryThreads = 0;
static std::atomic<uint32_t> numTemporaryThreads = 0;
static std::unordered_set<std::thread::id> temporaryThreads;
static uint32_t maxThreads = 0;
static uint32_t numStaticThreads = 0;

static thread_local uint32_t threadTaskCountRunning = 0;
static thread_local uint32_t threadTaskCountCompleted = 0;
static thread_local uint32_t threadTaskCountCancelled = 0;

static std::atomic<uint32_t> taskIndex = 0;

void resetThreadPoolTestState() {
	maxTemporaryThreads = 0;
	numTemporaryThreads = 0;
	temporaryThreads.clear();
	maxThreads = 0;
	numStaticThreads = 0;
	threadTaskCountRunning = 0;
	threadTaskCountCompleted = 0;
	threadTaskCountCancelled = 0;
	taskIndex = 0;
}

class MockTask;

class ThreadLocalContext : public IThreadPoolReceiver {
	bool isRunningOnStaticThread;
	bool initCalled = false;

	uint32_t threadTaskCountLocal = 0;

public:
	ThreadLocalContext() : isRunningOnStaticThread(false) {}
	explicit ThreadLocalContext(const bool isRunningOnStaticThread_)
	  : isRunningOnStaticThread(isRunningOnStaticThread_) {}

	void init() final {
		// init should only be called once per thread
		ASSERT(!initCalled);
		initCalled = true;
		if (!isRunningOnStaticThread) {
			numTemporaryThreads.fetch_add(1);
		}

		// init should only run once before every task execution on this thread.
		std::unique_lock<std::mutex> lock(threadsMutex);
		if (!isRunningOnStaticThread) {
			temporaryThreads.insert(std::this_thread::get_id());
			maxTemporaryThreads = std::max(maxTemporaryThreads, static_cast<uint32_t>(temporaryThreads.size()));
		} else {
			++numStaticThreads;
		}
		maxThreads = std::max(maxThreads, numStaticThreads + static_cast<uint32_t>(temporaryThreads.size()));
	}

	void addTaskCount() {
		++threadTaskCountLocal;
	}

	uint32_t getTaskCount() const { return threadTaskCountLocal; }

	virtual ~ThreadLocalContext() {
		if (threadTaskCountRunning != 0 || threadTaskCountCompleted + threadTaskCountCancelled != threadTaskCountLocal) {
			std::abort();
		}

		std::unique_lock<std::mutex> lock(threadsMutex);
		if (!isRunningOnStaticThread) {
			temporaryThreads.erase(std::this_thread::get_id());
		} else {
			--numStaticThreads;
		}
	}

	void action(MockTask& task);
};

class MockTask final : public TypedAction<ThreadLocalContext, MockTask> {
	uint32_t myTaskIndex;

public:
	MockTask() { myTaskIndex = taskIndex.fetch_add(1); }

	uint32_t getTaskIndex() const { return myTaskIndex; }

	void cancel() override final;
	double getTimeEstimate() const override final;

	ThreadReturnPromise<uint32_t> done;
};

void ThreadLocalContext::action(MockTask& task) {
	++threadTaskCountRunning;

	int sleepTime = nondeterministicRandom()->randomInt(10, 100);
	std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
	std::cout << "Task executed " << task.getTaskIndex() << std::endl;
	task.done.send(task.getTaskIndex());
	std::cout << "Task send done message with task index " << task.getTaskIndex() << std::endl;

	--threadTaskCountRunning;
	++threadTaskCountCompleted;
	addTaskCount();
}

void MockTask::cancel() {
	// Intended to be blank
	++threadTaskCountCancelled;
	TypedAction::cancel();
}

double MockTask::getTimeEstimate() const {
	return 0.001;
}

Future<Void> testDynamicThreadPool(ExplicitVoid = {}) {
	constexpr uint32_t ALLOWED_TEMPORARY_THREADS = 64;

	resetThreadPoolTestState();

	auto pool = createDynamicThreadPool(
	    ALLOWED_TEMPORARY_THREADS, std::chrono::milliseconds(1000), []() { return new details::ThreadLocalContext(); });
	// auto pool = createGenericThreadPool();

	// Add static threads
	const uint16_t numStaticThreads = nondeterministicRandom()->randomInt(8, 16);
	{
		std::cout << "Adding static threads..." << std::endl;
		for (int i = 0; i < numStaticThreads; ++i) {
			pool->addThread(new details::ThreadLocalContext(true));
		}
		std::cout << "Added static threads " << numStaticThreads << std::endl;
	}

	std::vector<Future<uint32_t>> tasks;
	std::unordered_set<uint32_t> notCompletedTasks;
	for (int rep = 0; rep < 4; ++rep) {
		// Add tasks
		{
			const uint16_t numTasks = nondeterministicRandom()->randomInt(256, 1024);
			for (int i = 0; i < numTasks; ++i) {
				auto task = new details::MockTask();
				notCompletedTasks.insert(task->getTaskIndex());
				tasks.push_back(task->done.getFuture());
				pool->post(task);
			}
			std::cout << "Added tasks = " << numTasks << std::endl;
		}

		std::cout << "Waiting for all tasks to complete..." << std::endl;
		// Wait for all tasks to complete
		for (auto& task : tasks) {
			co_await task;
			auto taskIndex = task.get();
			// ASSERT(notCompletedTasks.count(taskIndex) > 0);
			notCompletedTasks.erase(taskIndex);
		}
		std::cout << "Number of tasks incomplete: " << notCompletedTasks.size() << std::endl;
		ASSERT(notCompletedTasks.empty());

		std::cout << "Static threads: " << details::numStaticThreads << std::endl;
		// Temporary threads should be created when needed
		std::cout << "Extra threads: " << details::numTemporaryThreads.load() << std::endl;
		std::cout << "Maximum extra threads running concurrently: " << details::maxThreads - details::numStaticThreads
		          << std::endl;
		// We do not assert the exact number of extra threads, as it is not always guaranteed to be created.
		ASSERT(details::maxThreads >= details::numStaticThreads);
		// And less than the maximum allowed temporary threads
		ASSERT(details::maxThreads - details::numStaticThreads <= ALLOWED_TEMPORARY_THREADS);

		std::this_thread::sleep_for(std::chrono::seconds(3));
	}

	pool->stop();

	// All temporary threads should be cleaned up properly.
	ASSERT(details::temporaryThreads.empty());

	co_return Void();
}

} // namespace details

TEST_CASE("flow/IThreadPool/DynamicThreadPool/") {
	// Run multiple times to bring more confidence on the code
	for (int i = 0; i < 3; ++i) {
		std::cout << "Test iteration " << i << std::endl;
		co_await details::testDynamicThreadPool();
	}
}
