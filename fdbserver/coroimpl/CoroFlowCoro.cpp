/*
 * CoroFlowCoro.cpp
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

#include "fdbserver/CoroFlow.h"
#include "CoroWorkPool.h"
#include "Coro.h"
#include "flow/TDMetric.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"

// Old libcoroutine based implementation. Used on Windows until CI has
// boost context installed

Coro *current_coro = 0, *main_coro = 0;
Coro* swapCoro(Coro* n) {
	Coro* t = current_coro;
	current_coro = n;
	return t;
}

/*struct IThreadlike {
public:
    virtual void start() = 0;     // Call at most once!  Causes run() to be called on the 'thread'.
    virtual ~IThreadlike() {}     // Pre: start hasn't been called, or run() has returned
    virtual void unblock() = 0;   // Pre: block() has been called by run().  Causes block() to return.

protected:
    virtual void block() = 0;     // Call only from run().  Returns when unblock() is called elsewhere.
    virtual void run() = 0;       // To be overridden by client.  Returning causes the thread to block until it is
destroyed.
};*/

struct Coroutine /*: IThreadlike*/ {
	Coroutine() {
		coro = Coro_new();
		if (coro == nullptr)
			platform::outOfMemory();
	}

	~Coroutine() { Coro_free(coro); }

	void start() {
		int result = Coro_startCoro_(swapCoro(coro), coro, this, &entry);
		if (result == ENOMEM)
			platform::outOfMemory();
	}

	void unblock() {
		// Coro_switchTo_( swapCoro(coro), coro );
		blocked.send(Void());
	}

protected:
	void block() {
		// Coro_switchTo_( swapCoro(main_coro), main_coro );
		blocked = Promise<Void>();
		double before = now();
		CoroThreadPool::waitFor(blocked.getFuture());
		if (g_network->isSimulated() && g_simulator->getCurrentProcess()->rebooting)
			TraceEvent("CoroUnblocked").detail("After", now() - before);
	}

	virtual void run() = 0;

private:
	void wrapRun() {
		run();
		Coro_switchTo_(swapCoro(main_coro), main_coro);
		// block();
	}

	static void entry(void* _this) { ((Coroutine*)_this)->wrapRun(); }

	Coro* coro;
	Promise<Void> blocked;
};

using CoroPool = WorkPool<Coroutine, ThreadUnsafeSpinLock, true>;

coro::DetachedCoroutine coroSwitcher(Future<Void> what, TaskPriority taskID, Coro* coro) {
	try {
		// state double t = now();
		co_await what;
		// if (g_network->isSimulated() && g_simulator->getCurrentProcess()->rebooting && now()!=t)
		//	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID", taskID).detail("Elapsed", now()-t).backtrace("Flow");
	} catch (Error&) {
	}
	co_await delay(0, taskID);
	Coro_switchTo_(swapCoro(coro), coro);
}

void CoroThreadPool::waitFor(Future<Void> what) {
	ASSERT(current_coro != main_coro);
	if (what.isReady())
		return;
	// double t = now();
	coroSwitcher(what, g_network->getCurrentTask(), current_coro);
	Coro_switchTo_(swapCoro(main_coro), main_coro);
	// if (g_network->isSimulated() && g_simulator->getCurrentProcess()->rebooting && now()!=t)
	//	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID", currentTaskID).detail("Elapsed",
	// now()-t).backtrace("Coro");
	ASSERT(what.isReady());
}

// Right After INet2::run
void CoroThreadPool::init() {
	if (!current_coro) {
		current_coro = main_coro = Coro_new();
		if (main_coro == nullptr)
			platform::outOfMemory();

		Coro_initializeMainCoro(main_coro);
		// printf("Main thread: %d bytes stack presumed available\n", Coro_bytesLeftOnStack(current_coro));
	}
}

Reference<IThreadPool> CoroThreadPool::createThreadPool() {
	return Reference<IThreadPool>(new CoroPool);
}
