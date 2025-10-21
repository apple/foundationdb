/*
 * IThreadPool.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
// The ifndef's allow us to compile with pre-built boost.  Otherwise, we get
// errors about double-defines.  As of this writing, the automatically downloaded
// build of boost doesn't define these, but the pre-built version does.  (The old
// prebuilt version was being silently ignored by cmake, so it's unclear when the
// compatibility divergence started)
#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include "boost/asio.hpp"

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
	boost::asio::io_service ios;
	boost::asio::io_service::work dontstop;
	enum Mode { Run = 0, Shutdown = 2 };
	volatile int mode;
	int stackSize;
	int pri;

	struct ActionWrapper {
		PThreadAction action;
		ActionWrapper(PThreadAction action) : action(action) {}
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
	ThreadPool(int stackSize, int pri) : dontstop(ios), mode(Run), stackSize(stackSize), pri(pri) {}
	~ThreadPool() override {}
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
	void post(PThreadAction action) override { ios.post(ActionWrapper(action)); }
	int priority() const { return pri; }
};

Reference<IThreadPool> createGenericThreadPool(int stackSize, int pri) {
	return Reference<IThreadPool>(new ThreadPool(stackSize, pri));
}

thread_local IThreadPoolReceiver* ThreadPool::Thread::threadUserObject;
