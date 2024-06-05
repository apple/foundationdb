/*
 * CoroFlowCoro.actor.cpp
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

#include "fdbserver/CoroFlow.h"
#include "flow/ActorCollection.h"
#include "Coro.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/actorcompiler.h" // has to be last include

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

template <class Threadlike, class Mutex, bool IS_CORO>
class WorkPool final : public IThreadPool, public ReferenceCounted<WorkPool<Threadlike, Mutex, IS_CORO>> {
	struct Worker;

	// Pool can survive the destruction of WorkPool while it waits for workers to terminate
	struct Pool : ReferenceCounted<Pool> {
		Mutex queueLock;
		Deque<PThreadAction> work;
		std::vector<Worker*> idle, workers;
		ActorCollection anyError, allStopped;
		Future<Void> m_holdRefUntilStopped;

		Pool() : anyError(false), allStopped(true) { m_holdRefUntilStopped = holdRefUntilStopped(this); }

		~Pool() {
			for (int c = 0; c < workers.size(); c++)
				delete workers[c];
		}

		ACTOR Future<Void> holdRefUntilStopped(Pool* p) {
			p->addref();
			wait(p->allStopped.getResult());
			p->delref();
			return Void();
		}
	};

	struct Worker final : Threadlike {
		Pool* pool;
		IThreadPoolReceiver* userData;
		bool stop;
		ThreadReturnPromise<Void> stopped;
		ThreadReturnPromise<Void> error;
		bool immediate = false;

		Worker(Pool* pool, IThreadPoolReceiver* userData, bool immediate) : pool(pool), userData(userData), stop(false), immediate(immediate) {}

		void run() override {
			try {
				if (!stop)
					userData->init();

				while (!stop) {
					pool->queueLock.enter();
					if (pool->work.empty()) {
						pool->idle.push_back(this);
						pool->queueLock.leave();
						Threadlike::block();
					} else {
						PThreadAction a = pool->work.front();
						pool->work.pop_front();
						pool->queueLock.leave();
						if (immediate) {
							ASSERT(false);
						}
						(*a)(userData);
						if (IS_CORO)
							CoroThreadPool::waitFor(yield());
					}
				}

				TraceEvent("CoroStop").log();
				delete userData;
				stopped.send(Void());
				return;
			} catch (Error& e) {
				TraceEvent("WorkPoolError").errorUnsuppressed(e);
				error.sendError(e);
			} catch (...) {
				TraceEvent("WorkPoolError").log();
				error.sendError(unknown_error());
			}

			try {
				delete userData;
			} catch (...) {
				TraceEvent(SevError, "WorkPoolErrorShutdownError").log();
			}
			stopped.send(Void());
		}
	};

	Reference<Pool> pool;
	Future<Void> m_stopOnError; // must be last, because its cancellation calls stop()!
	Error error;
	bool immediate;

	ACTOR Future<Void> stopOnError(WorkPool* w) {
		try {
			wait(w->getError());
			ASSERT(false);
		} catch (Error& e) {
			w->stop(e);
		}
		return Void();
	}

	void checkError() {
		if (error.code() != invalid_error_code) {
			ASSERT(error.code() != error_code_success); // Calling post or addThread after stop is an error
			throw error;
		}
	}

public:
	WorkPool() : pool(new Pool), immediate(false) { m_stopOnError = stopOnError(this); }
	WorkPool(bool immediate) : pool(new Pool), immediate(immediate) { m_stopOnError = stopOnError(this); }

	Future<Void> getError() const override { return pool->anyError.getResult(); }
	void addThread(IThreadPoolReceiver* userData, const char*) override {
		checkError();

		auto w = new Worker(pool.getPtr(), userData, immediate);
		pool->queueLock.enter();
		pool->workers.push_back(w);
		pool->queueLock.leave();
		pool->anyError.add(w->error.getFuture());
		pool->allStopped.add(w->stopped.getFuture());
		startWorker(w);
	}
	ACTOR static void startWorker(Worker* w) {
		// We want to make sure that coroutines are always started after Net2::run() is called, so the main coroutine is
		// initialized.
		wait(delay(0, g_network->getCurrentTask()));
		w->start();
	}
	void post(PThreadAction action) override {
		checkError();

		if (!immediate) {
			pool->queueLock.enter();
			pool->work.push_back(action);
			if (!pool->idle.empty()) {
				Worker* c = pool->idle.back();
				pool->idle.pop_back();
				pool->queueLock.leave();
				c->unblock();
			} else
				pool->queueLock.leave();
		} else {
			ASSERT(pool->workers.size() > 0 && !pool->workers[0]->stop);
			(*action)(pool->workers[0]->userData);
		}
	}
	Future<Void> stop(Error const& e) override {
		if (error.code() == invalid_error_code) {
			error = e;
		}

		pool->queueLock.enter();
		TraceEvent("WorkPool_Stop")
		    .errorUnsuppressed(e)
		    .detail("Workers", pool->workers.size())
		    .detail("Idle", pool->idle.size())
		    .detail("Work", pool->work.size());

		for (uint32_t i = 0; i < pool->work.size(); i++)
			pool->work[i]->cancel(); // What if cancel() does something to this?
		pool->work.clear();
		for (int i = 0; i < pool->workers.size(); i++)
			pool->workers[i]->stop = true;

		std::vector<Worker*> idle;
		std::swap(idle, pool->idle);
		pool->queueLock.leave();

		for (int i = 0; i < idle.size(); i++)
			idle[i]->unblock();

		pool->allStopped.add(Void());

		return pool->allStopped.getResult();
	}
	bool isCoro() const override { return IS_CORO; }
	void addref() override { ReferenceCounted<WorkPool>::addref(); }
	void delref() override { ReferenceCounted<WorkPool>::delref(); }
};

typedef WorkPool<Coroutine, ThreadUnsafeSpinLock, true> CoroPool;

ACTOR void coroSwitcher(Future<Void> what, TaskPriority taskID, Coro* coro) {
	try {
		// state double t = now();
		wait(what);
		// if (g_network->isSimulated() && g_simulator->getCurrentProcess()->rebooting && now()!=t)
		//	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID", taskID).detail("Elapsed", now()-t).backtrace("Flow");
	} catch (Error&) {
	}
	wait(delay(0, taskID));
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

Reference<IThreadPool> CoroThreadPool::createThreadPool(bool immediate) {
	return Reference<IThreadPool>(new CoroPool(immediate));
}
