/*
 * CoroWorkPool.h
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

#pragma once

#include "fdbserver/CoroFlow.h"
#include "flow/ActorCollection.h"
#include "flow/CoroUtils.h"
#include "flow/Deque.h"
#include "flow/Trace.h"

#include <utility>
#include <vector>

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

		Future<Void> holdRefUntilStopped(Pool* p) {
			p->addref();
			co_await p->allStopped.getResult();
			p->delref();
		}
	};

	struct Worker final : Threadlike {
		Pool* pool;
		IThreadPoolReceiver* userData;
		bool stop;
		ThreadReturnPromise<Void> stopped;
		ThreadReturnPromise<Void> error;

		Worker(Pool* pool, IThreadPoolReceiver* userData) : pool(pool), userData(userData), stop(false) {}

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
	Error error;
	bool destroying{ false };
	Promise<Void> stopRequested;
	Future<Void> m_stopOnError; // Destroy before the shutdown signal and pool.

	Future<Void> stopOnError(WorkPool* w) {
		try {
			auto result = co_await race(w->getError(), w->stopRequested.getFuture());
			ASSERT(result.index() == 1);
		} catch (Error& e) {
			w->stop(e);
			co_return;
		}
		if (w->destroying) {
			w->stop(actor_cancelled());
		}
	}

	void checkError() {
		if (error.code() != invalid_error_code) {
			ASSERT(error.code() != error_code_success); // Calling post or addThread after stop is an error
			throw error;
		}
	}

public:
	WorkPool() : pool(new Pool) { m_stopOnError = stopOnError(this); }
	~WorkPool() override {
		destroying = true;
		// Run implicit stop inside the watcher's exception boundary, not this noexcept destructor.
		if (stopRequested.canBeSet()) {
			stopRequested.send(Void());
		}
	}

	Future<Void> getError() const override { return pool->anyError.getResult(); }
	void addThread(IThreadPoolReceiver* userData, const char*) override {
		checkError();

		auto w = new Worker(pool.getPtr(), userData);
		pool->queueLock.enter();
		pool->workers.push_back(w);
		pool->queueLock.leave();
		pool->anyError.add(w->error.getFuture());
		pool->allStopped.add(w->stopped.getFuture());
		startWorker(w);
	}
	static coro::DetachedCoroutine startWorker(Worker* w) {
		// We want to make sure that coroutines are always started after Net2::run() is called, so the main coroutine is
		// initialized.
		co_await delay(0, g_network->getCurrentTask());
		w->start();
	}
	void post(PThreadAction action) override {
		checkError();

		pool->queueLock.enter();
		pool->work.push_back(action);
		if (!pool->idle.empty()) {
			Worker* c = pool->idle.back();
			pool->idle.pop_back();
			pool->queueLock.leave();
			c->unblock();
		} else {
			pool->queueLock.leave();
		}
	}
	Future<Void> stop(Error const& e) override {
		// User cancellation callbacks can release the last owner, except during implicit destruction itself.
		Reference<WorkPool> keepAlive;
		if (!destroying) {
			keepAlive = Reference<WorkPool>::addRef(this);
		}
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

		// Keep implicit shutdown pending if synchronous cleanup throws; finish before stop waiters can release us.
		if (stopRequested.canBeSet()) {
			stopRequested.send(Void());
		}
		pool->allStopped.add(Void());

		return pool->allStopped.getResult();
	}
	bool isCoro() const override { return IS_CORO; }
	void addref() override { ReferenceCounted<WorkPool>::addref(); }
	void delref() override { ReferenceCounted<WorkPool>::delref(); }
};
