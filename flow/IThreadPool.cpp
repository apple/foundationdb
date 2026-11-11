/*
 * IThreadPool.cpp
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

#include "flow/IThreadPool.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// The ifndef's allow us to compile with pre-built boost - depending on what
// libraries were enabled when it was built, we can get errors about double-defines.
#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include <boost/asio.hpp>

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911
namespace dynamic {

static thread_local std::unique_ptr<IThreadPoolReceiver> contextPerThread = nullptr;

struct TaskThreadCounter {
	std::atomic<uint32_t> numQueuedTasks = 0;
	std::atomic<uint32_t> numRunning = 0;
	std::atomic<uint32_t> numThreads = 0;
};

class Task {
	PThreadAction callable;

	TaskThreadCounter& taskThreadCounter;

public:
	Task(PThreadAction callable_, TaskThreadCounter& taskThreadCounter_)
	  : callable(std::move(callable_)), taskThreadCounter(taskThreadCounter_) {
		ASSERT(callable);
	}
	Task(Task&&) noexcept;

	Task(const Task&) = delete;
	Task& operator=(const Task&) = delete;

	~Task();

	void operator()();
};

Task::Task(Task&& other) noexcept : callable(std::move(other.callable)), taskThreadCounter(other.taskThreadCounter) {
	other.callable = nullptr;
}

void Task::operator()() {
	ASSERT(callable != nullptr);
	try {
		// The task is now running
		++taskThreadCounter.numRunning;
		--taskThreadCounter.numQueuedTasks;
		(*callable)(contextPerThread.get());
	} catch (...) {
		TraceEvent("TaskException").log();
	}
	--taskThreadCounter.numRunning;
	callable = nullptr;
}

Task::~Task() {
	if (callable) {
		// The task is cancelled
		--taskThreadCounter.numQueuedTasks;
		callable->cancel();
	}
}

bool initializeContextPerThread(IThreadPoolReceiver* threadContext) {
	ASSERT(threadContext);

	contextPerThread.reset(threadContext);
	try {
		contextPerThread->init();
		return true;
	} catch (...) {
		TraceEvent(SevError, "InitializeContextPerThreadException").log();
		return false;
	}
}

void taskExecutor(std::stop_token stopToken,
                  std::shared_ptr<boost::asio::io_context> ioContext,
                  IThreadPoolReceiver* threadContext) {
	static std::chrono::milliseconds interval(100);

	const auto initialized = initializeContextPerThread(threadContext);
	ASSERT(initialized);
	do {
		try {
			ioContext->run_one_for(interval);
		} catch (...) {
			TraceEvent("TaskExecutorException").log();
		}
	} while (!ioContext->stopped() && !stopToken.stop_requested());
}

class TemporaryThreadsContainer {
	std::mutex temporaryThreadsMutex;
	std::unordered_map<std::thread::id, std::jthread> temporaryThreads;
	std::unordered_set<std::thread::id> temporaryThreadsToSweep;
	const std::thread::id creatorThreadID;

public:
	TemporaryThreadsContainer() : creatorThreadID(std::this_thread::get_id()) {}
	~TemporaryThreadsContainer() {
		assert(std::this_thread::get_id() == creatorThreadID);
		for (auto& [threadID, thread] : temporaryThreads) {
			thread.request_stop();
		}
		for (auto& [threadID, thread] : temporaryThreads) {
			if (thread.joinable()) {
				thread.join();
			}
		}
	}

	void add(std::jthread&& thread);
	void sweep();
	void markRemove(const std::thread::id& id);
	std::size_t size() const noexcept { return temporaryThreads.size(); }
};

void TemporaryThreadsContainer::sweep() {
	// The sweep code MUST NOT run by the temporary threads themselves
	std::unique_lock<std::mutex> lock(temporaryThreadsMutex);
	ASSERT(std::this_thread::get_id() == creatorThreadID);
	for (auto threadID : temporaryThreadsToSweep) {
		if (temporaryThreads.contains(threadID)) {
			auto& thread = temporaryThreads.at(threadID);
			if (thread.joinable()) {
				thread.join();
			}
			temporaryThreads.erase(threadID);
		}
	}
	temporaryThreadsToSweep.clear();
}

void TemporaryThreadsContainer::add(std::jthread&& thread) {
	std::unique_lock<std::mutex> lock(temporaryThreadsMutex);
	temporaryThreads.emplace(thread.get_id(), std::move(thread));
}

void TemporaryThreadsContainer::markRemove(const std::thread::id& id) {
	std::unique_lock<std::mutex> lock(temporaryThreadsMutex);
	temporaryThreadsToSweep.insert(id);
}

class TemporaryTaskExecutor {
	std::shared_ptr<boost::asio::io_context> ioContext;
	IThreadPoolReceiver* threadContext;
	std::chrono::milliseconds maxIdleTime;
	TemporaryThreadsContainer* temporaryThreadsContainer;
	TaskThreadCounter& taskThreadCounter;

public:
	TemporaryTaskExecutor(std::shared_ptr<boost::asio::io_context> ioContext_,
	                      IThreadPoolReceiver* threadContext_,
	                      const std::chrono::milliseconds& maxIdleTime_,
	                      TemporaryThreadsContainer* temporaryThreadsContainer_,
	                      TaskThreadCounter& taskThreadCounter_);

	void operator()(std::stop_token stopToken);
};

TemporaryTaskExecutor::TemporaryTaskExecutor(std::shared_ptr<boost::asio::io_context> ioContext_,
                                             IThreadPoolReceiver* threadContext_,
                                             const std::chrono::milliseconds& maxIdleTime_,
                                             TemporaryThreadsContainer* temporaryThreadsContainer_,
                                             TaskThreadCounter& taskThreadCounter_)
  : ioContext(ioContext_), threadContext(threadContext_), maxIdleTime(maxIdleTime_),
    temporaryThreadsContainer(temporaryThreadsContainer_), taskThreadCounter(taskThreadCounter_) {
	ASSERT(temporaryThreadsContainer_);
	++taskThreadCounter.numThreads;
}

void TemporaryTaskExecutor::operator()(std::stop_token stopToken) {
	std::size_t numTasksDone = 0;

	const auto initialized = initializeContextPerThread(threadContext);
	ASSERT(initialized);
	do {
		try {
			numTasksDone = ioContext->run_one_for(maxIdleTime);
		} catch (...) {
			TraceEvent("TemporaryTaskExecutorException").log();
		}
	} while (!ioContext->stopped() && numTasksDone != 0 && !stopToken.stop_requested());

	temporaryThreadsContainer->markRemove(std::this_thread::get_id());
	--taskThreadCounter.numThreads;
}

class DynamicThreadPool {
	TaskThreadCounter taskThreadCounter;

	std::shared_ptr<boost::asio::io_context> ioContext;
	// The workerGuard ensures the io_context work even no job is available right
	// now. See:
	//  https://www.boost.org/doc/libs/latest/doc/html/boost_asio/reference/io_context.html#boost_asio.reference.io_context.stopping_the_io_context_from_running_out_of_work
	boost::asio::executor_work_guard<boost::asio::io_context::executor_type> workGuard;

	std::vector<std::jthread> staticExecutors;

	TemporaryThreadsContainer temporaryThreadsContainer;
	const uint32_t maxTemporaryThreads;
	const std::chrono::milliseconds maxIdleDuration;

	std::function<IThreadPoolReceiver*(void)> threadContextGenerator;

public:
	DynamicThreadPool(const uint32_t maxTemporaryThreads_,
	                  const std::chrono::milliseconds maxIdleDuration_,
	                  std::function<IThreadPoolReceiver*(void)> threadContextGenerator_);

	template <typename F>
	void post(F&& func);

	void addStaticThread(IThreadPoolReceiver* threadContext = nullptr);

	~DynamicThreadPool();
};

DynamicThreadPool::DynamicThreadPool(const uint32_t maxTemporaryThreads_,
                                     const std::chrono::milliseconds maxIdleDuration_,
                                     std::function<IThreadPoolReceiver*(void)> threadContextGenerator_)
  : taskThreadCounter(), ioContext(new boost::asio::io_context()), workGuard(boost::asio::make_work_guard(*ioContext)),
    temporaryThreadsContainer(), maxTemporaryThreads(maxTemporaryThreads_), maxIdleDuration(maxIdleDuration_),
    threadContextGenerator(threadContextGenerator_) {
	ASSERT(maxTemporaryThreads > 0);
}

void DynamicThreadPool::addStaticThread(IThreadPoolReceiver* threadContext) {
	if (!threadContext) {
		threadContext = threadContextGenerator();
	}
	++taskThreadCounter.numThreads;
	staticExecutors.emplace_back(
	    g_network->startJThread(std::bind(taskExecutor, std::placeholders::_1, ioContext, threadContext)));
}

template <typename F>
void DynamicThreadPool::post(F&& func) {
	auto task = Task(std::forward<F>(func), taskThreadCounter);
	++taskThreadCounter.numQueuedTasks;
	// post is called by main thread only, so we can safely check and spawn temporary threads without additional locking
	temporaryThreadsContainer.sweep();
	const auto numTemporaryThreads = temporaryThreadsContainer.size();
	if (taskThreadCounter.numRunning + taskThreadCounter.numQueuedTasks > taskThreadCounter.numThreads &&
	    numTemporaryThreads < maxTemporaryThreads) {
		// Possibly need to spawn new threads
		temporaryThreadsContainer.add(g_network->startJThread(TemporaryTaskExecutor(
		    ioContext, threadContextGenerator(), maxIdleDuration, &temporaryThreadsContainer, taskThreadCounter)));
		const auto numTotalThreads = staticExecutors.size() + numTemporaryThreads;
		TraceEvent(SevInfo, "DynamicThreadPool")
		    .detail("message", "create_new_thread")
		    .detail("numTotalThreads", numTotalThreads)
		    .detail("numTemporaryThreads", temporaryThreadsContainer.size())
		    .detail("queuedTasks", taskThreadCounter.numQueuedTasks.load());
	}
	// Wrapping the user-provided function into a Task object allows the func access the thread_local content
	boost::asio::post(*ioContext, std::move(task));
}

DynamicThreadPool::~DynamicThreadPool() {
	ioContext->stop();

	for (auto& staticExecutor : staticExecutors) {
		staticExecutor.request_stop();
	}
	// Calls destructors of each jthread and joins all of them
	staticExecutors.clear();
}

class ThreadPool final : public IThreadPool, public ReferenceCounted<ThreadPool> {
	std::unique_ptr<DynamicThreadPool> dynamicThreadPool;

	const uint32_t maxTemporaryThreads;
	const std::chrono::milliseconds maxIdleDuration;
	std::function<IThreadPoolReceiver*(void)> threadContextGenerator;

public:
	ThreadPool(const uint32_t maxTemporaryThreads_,
	           const std::chrono::milliseconds maxIdleDuration_,
	           std::function<IThreadPoolReceiver*(void)> threadContextGenerator_)
	  : maxTemporaryThreads(maxTemporaryThreads_), maxIdleDuration(maxIdleDuration_),
	    threadContextGenerator(threadContextGenerator_) {}
	~ThreadPool() override { stop(); }

	// FIXME: Implement proper error handling for the thread pool. Currently returns a default Void future.
	Future<Void> getError() const override { return Void(); }

	void addThread(IThreadPoolReceiver* userData, const char* name = nullptr) override;

	void post(PThreadAction action) override;

	Future<Void> stop(Error const& e = success()) override;

	void addref() override { ReferenceCounted<ThreadPool>::addref(); }
	void delref() override;
};

void ThreadPool::addThread(IThreadPoolReceiver* userData, const char* name) {
	// name is ignored as not supported by standard thread pool implementation until c++29
	if (!dynamicThreadPool) {
		dynamicThreadPool =
		    std::make_unique<DynamicThreadPool>(maxTemporaryThreads, maxIdleDuration, threadContextGenerator);
	}
	dynamicThreadPool->addStaticThread(userData);
}

void ThreadPool::post(PThreadAction action) {
	// This is intended, as ThreadPool will run on only the main thread, so any post after stop should fail
	ASSERT(dynamicThreadPool);

	dynamicThreadPool->post(std::move(action));
}

Future<Void> ThreadPool::stop(Error const&) {
	if (dynamicThreadPool) {
		dynamicThreadPool.reset();
	}

	return Void();
}

void ThreadPool::delref() {
	if (ReferenceCounted<ThreadPool>::delref_no_destroy()) {
		delete this;
	}
}

} // namespace dynamic

#endif // defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911

class ThreadPool final : public IThreadPool, public ReferenceCounted<ThreadPool> {
	struct Thread {
		ThreadPool* pool;
		IThreadPoolReceiver* userObject;
		THREAD_HANDLE handle; // Owned by main thread
		static thread_local IThreadPoolReceiver* threadUserObject;
		explicit Thread(ThreadPool* pool, IThreadPoolReceiver* userObject) : pool(pool), userObject(userObject) {}
		~Thread() { ASSERT_ABORT(!userObject); }

		void run() {
			setThreadPriority(pool->priority());
			threadUserObject = userObject;
			try {
				userObject->init();
				while (pool->ios.run_one() && (pool->mode == Mode::Run))
					;
			} catch (Error& e) {
				TraceEvent(SevError, "ThreadPoolError").error(e);
			}
			delete userObject;
			userObject = nullptr;
		}
		static void dispatch(PThreadAction action) { (*action)(threadUserObject); }
	};
	THREAD_FUNC start(void* p) {
		((Thread*)p)->run();
		THREAD_RETURN;
	}

	std::vector<Thread*> threads;
	boost::asio::io_context ios;
	boost::asio::executor_work_guard<boost::asio::io_context::executor_type> dontstop;
	enum Mode { Run = 0, Shutdown = 2 };
	volatile int mode;
	int stackSize;
	int pri;

	struct ActionWrapper {
		PThreadAction action;
		explicit ActionWrapper(PThreadAction action) : action(action) {}
		// HACK: Boost won't use move constructors, so we just assume the last copy made is the one that will be called
		// or cancelled
		ActionWrapper(ActionWrapper const& r) : action(r.action) { const_cast<ActionWrapper&>(r).action = nullptr; }
		void operator()() {
			Thread::dispatch(action);
			action = nullptr;
		}
		~ActionWrapper() {
			if (action) {
				action->cancel();
			}
		}
		ActionWrapper& operator=(ActionWrapper const&) = delete;
	};

public:
	ThreadPool(int stackSize, int pri) : dontstop(ios.get_executor()), mode(Run), stackSize(stackSize), pri(pri) {}
	~ThreadPool() override = default;
	Future<Void> stop(Error const& e = success()) override {
		if (mode == Shutdown)
			return Void();
		ReferenceCounted<ThreadPool>::addref();
		ios.stop(); // doesn't work?
		mode = Shutdown;
		for (int i = 0; i < threads.size(); i++) {
			waitThread(threads[i]->handle);
			delete threads[i];
		}
		// There are two primary cases for calling stop():
		// 1. Explicit stopped by an external caller.
		//    The caller still holds a reference, so refcount is at least 2.
		//    We only need to release the refcount we added above, so that
		//    use delref_no_destroy() here is safe.
		// 2. Implicit stopped by the final destruction, the backtrace is
		//    ~Reference<ThreadPool>() -> ThreadPool::delref() -> ThreadPool::stop().
		//    In this case, the refcount is 0 before invoking stop() and it
		//    becomes 1 at this point. We should not invoke delref() here
		//    as the destruction is handled by ThreadPool::delref() to avoid
		//    double free error.
		ReferenceCounted<ThreadPool>::delref_no_destroy();
		return Void();
	}

	Future<Void> getError() const override { return Never(); } // FIXME
	void addref() override { ReferenceCounted<ThreadPool>::addref(); }
	void delref() override {
		if (ReferenceCounted<ThreadPool>::delref_no_destroy()) {
			stop();
			delete this;
		}
	}
	void addThread(IThreadPoolReceiver* userData, const char* name) override {
		threads.push_back(new Thread(this, userData));
		threads.back()->handle = g_network->startThread(start, threads.back(), stackSize, name);
	}
	void post(PThreadAction action) override { boost::asio::post(ios, ActionWrapper(action)); }
	int priority() const { return pri; }
};

Reference<IThreadPool> createGenericThreadPool(int stackSize, int pri) {
	return makeReference<ThreadPool>(stackSize, pri);
}

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911
Reference<IThreadPool> createDynamicThreadPool(const uint32_t maxTemporaryThreads,
                                               const std::chrono::milliseconds maxIdleDuration,
                                               std::function<IThreadPoolReceiver*(void)> threadContextGenerator) {
	return makeReference<dynamic::ThreadPool>(maxTemporaryThreads, maxIdleDuration, threadContextGenerator);
}
#else
Reference<IThreadPool> createDynamicThreadPool(const uint32_t maxTemporaryThreads,
                                               const std::chrono::milliseconds maxIdleDuration,
                                               std::function<IThreadPoolReceiver*(void)> threadContextGenerator) {
	TraceEvent(SevWarn, "DynamicThreadPoolNotSupported");
	// Using a generic value in this case
	return makeReference<ThreadPool>(1024 * 1024, 10);
}
#endif

thread_local IThreadPoolReceiver* ThreadPool::Thread::threadUserObject;
