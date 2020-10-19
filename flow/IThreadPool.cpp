/*
 * IThreadPool.cpp
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

#include "flow/IThreadPool.h"

#include <algorithm>
#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include "boost/asio.hpp"
#include "boost/bind.hpp"

class ThreadPool final : public IThreadPool, public ReferenceCounted<ThreadPool> {
	struct Thread {
		ThreadPool *pool;
		IThreadPoolReceiver* userObject;
		Event stopped;
		static thread_local IThreadPoolReceiver* threadUserObject;
		explicit Thread(ThreadPool *pool, IThreadPoolReceiver *userObject) : pool(pool), userObject(userObject) {}
		~Thread() { ASSERT_ABORT(!userObject); }

		void run() {
			deprioritizeThread();

			threadUserObject = userObject;
			try {
				userObject->init();
				while (pool->ios.run_one() && (pool->mode == Mode::Run));
			} catch (Error& e) {
				TraceEvent(SevError, "ThreadPoolError").error(e);
			}
			delete userObject;
			userObject = nullptr;
			stopped.set();
		}
		static void dispatch( PThreadAction action ) {
			(*action)(threadUserObject);
		}
	};
	THREAD_FUNC start( void* p ) {
		((Thread*)p)->run();
		THREAD_RETURN;
	}

	std::vector<Thread*> threads;
	boost::asio::io_service ios;
	boost::asio::io_service::work dontstop;
	enum Mode { Run=0, Shutdown=2 };
	volatile int mode;
	int stackSize;

	struct ActionWrapper {
		PThreadAction action;
		ActionWrapper(PThreadAction action) : action(action) {}
		// HACK: Boost won't use move constructors, so we just assume the last copy made is the one that will be called or cancelled
		ActionWrapper(ActionWrapper const& r) : action(r.action) { const_cast<ActionWrapper&>(r).action=nullptr; }
		void operator()() { Thread::dispatch(action); action = nullptr; }
		~ActionWrapper() { if (action) { action->cancel(); } }
		ActionWrapper &operator=(ActionWrapper const&)=delete;
	};
public:
	ThreadPool(int stackSize) : dontstop(ios), mode(Run), stackSize(stackSize) {}
	~ThreadPool() {}
	Future<Void> stop(Error const& e = success()) override {
		if (mode == Shutdown) return Void();
		ReferenceCounted<ThreadPool>::addref();
		ios.stop(); // doesn't work?
		mode = Shutdown;
		for(int i=0; i<threads.size(); i++) {
			threads[i]->stopped.block();
			delete threads[i];
		}
		ReferenceCounted<ThreadPool>::delref();
		return Void();
	}
	Future<Void> getError() const override { return Never(); } // FIXME
	void addref() override { ReferenceCounted<ThreadPool>::addref(); }
	void delref() override {
		if (ReferenceCounted<ThreadPool>::delref_no_destroy()) stop();
	}
	void addThread(IThreadPoolReceiver* userData) override {
		threads.push_back(new Thread(this, userData));
		startThread(start, threads.back(), stackSize);
	}
	void post(PThreadAction action) override { ios.post(ActionWrapper(action)); }
};

Reference<IThreadPool>	createGenericThreadPool(int stackSize)
{
	return Reference<IThreadPool>( new ThreadPool(stackSize) );
}

thread_local IThreadPoolReceiver* ThreadPool::Thread::threadUserObject;
