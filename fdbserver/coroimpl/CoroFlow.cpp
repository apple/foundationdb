/*
 * CoroFlow.cpp
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
#include "flow/TDMetric.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include <boost/coroutine2/all.hpp>
#include <boost/coroutine2/coroutine.hpp>
#include <functional>
#include "flow/flow.h"
#include "flow/network.h"

using coro_t = boost::coroutines2::coroutine<Future<Void>>;

// Coro *current_coro = 0, *main_coro = 0;
// Coro* swapCoro( Coro* n ) {
// 	Coro* t = current_coro;
// 	current_coro = n;
// 	return t;
// }

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

struct Coroutine;
Coroutine* current_coro;

struct Coroutine /*: IThreadlike*/ {
	Coroutine() = default;
	~Coroutine() { *alive = false; }

	static constexpr auto kStackSize = 256 * (1 << 10);

	void start() {
		coro.reset(new coro_t::pull_type(boost::coroutines2::fixedsize_stack(kStackSize),
		                                 [this](coro_t::push_type& sink) { entry(sink); }));
		switcher(this);
	}

	void unblock() {
		// Coro_switchTo_( swapCoro(coro), coro );

		// Copy blocked before calling send, since the call to send might destroy it.
		auto b = blocked;
		b.send(Void());
	}

	void waitFor(Future<Void> const& what) {
		ASSERT(current_coro == this);
		current_coro = nullptr;
		(*sink)(what); // Pass control back to the switcher coroutine
		ASSERT(what.isReady());
		current_coro = this;
	}

protected:
	static coro::DetachedCoroutine switcher(Coroutine* self) {
		std::shared_ptr<bool> alive = self->alive;
		while (*alive && *self->coro) {
			try {
				co_await self->coro->get();
			} catch (Error& e) {
				// We just want to transfer control back to the coroutine. The coroutine will handle the error.
			}
			(*self->coro)(); // Transfer control to the coroutine. This call "returns" after waitFor is called.
			co_await delay(0, g_network->getCurrentTask());
		}
	}

	void entry(coro_t::push_type& sink) {
		current_coro = this;
		this->sink = &sink;
		run();
	}

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
	coro_t::push_type* sink;
	Promise<Void> blocked;
	std::shared_ptr<bool> alive{ std::make_shared<bool>(true) };
	std::unique_ptr<coro_t::pull_type> coro;
};

using CoroPool = WorkPool<Coroutine, ThreadUnsafeSpinLock, true>;

void CoroThreadPool::waitFor(Future<Void> what) {
	ASSERT(current_coro != nullptr);
	if (what.isReady())
		return;
	// double t = now();
	current_coro->waitFor(what);
	what.get(); // Throw if |what| is an error
}

// Right After INet2::run
void CoroThreadPool::init() {}

Reference<IThreadPool> CoroThreadPool::createThreadPool() {
	return Reference<IThreadPool>(new CoroPool);
}
