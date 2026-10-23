/*
 * ThreadHelper.cpp
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
#include <thread>
#include <vector>

#include "flow/flow.h"
#include "flow/network.h"
#include "flow/ThreadHelper.h"
#include "flow/Coroutines.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"

ThreadCallback* ThreadCallback::addCallback(ThreadCallback* cb) {
	return (new ThreadMultiCallback())->addCallback(this)->addCallback(cb);
}

// A simple thread object that sends the result
struct ThreadFutureSendObj {
	void operator()() { tsav->send(Void()); }
	ThreadSingleAssignmentVar<Void>* tsav;
};

// A simple thread object that cancels the threadFuture
struct ThreadFutureCancelObj {
	explicit ThreadFutureCancelObj(ThreadFuture<Void> f) : f(f) {}
	void operator()() { f.cancel(); }
	ThreadFuture<Void> f;
};

// This unit test should be running with TSAN enabled binary
TEST_CASE("/flow/safeThreadFutureToFuture/Send") {
	// std::thread is not working in simulation at present, disable this in simulation
	if (g_network->isSimulated())
		co_return;
	auto* tsav = new ThreadSingleAssignmentVar<Void>;
	std::thread thread = std::thread{ ThreadFutureSendObj{ tsav } };
	ThreadFuture<Void> f(tsav);
	// change this to unsafeThreadFutureToFuture will get a data-race failure
	co_await safeThreadFutureToFuture(f);
	thread.join();
}

// Test the case where the underlying threadFuture is cancelled
TEST_CASE("/flow/safeThreadFutureToFuture/Cancel") {
	// std::thread is not working in simulation at present, disable this in simulation
	if (g_network->isSimulated())
		co_return;
	ThreadFuture<Void> f = onMainThread([]() -> Future<Void> { return Never(); });
	std::thread thread = std::thread{ ThreadFutureCancelObj(f) };
	try {
		co_await safeThreadFutureToFuture(f); // this actor should get actor_cancelled
		ASSERT(false);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_actor_cancelled);
	}
	thread.join();
}

namespace {

struct OnMainThreadLifetimeResult final : ThreadSingleAssignmentVar<int> {
	Future<int> operation;
	Future<Void> const* completion = nullptr;
	bool& detachedBeforeRelease;
	bool& destroyed;

	OnMainThreadLifetimeResult(Future<int> operation, bool& detachedBeforeRelease, bool& destroyed)
	  : operation(std::move(operation)), detachedBeforeRelease(detachedBeforeRelease), destroyed(destroyed) {}

	void delref() override {
		if (isReady() && completion->getPromiseReferenceCount() != 0) {
			// Keep an observer reference so this is safe even on the broken implementation. Before dropping the
			// coroutine's result reference, only that observer may still own its completion future. Otherwise a
			// client can destroy the result and release cancelFuture while final_suspend() is using the same SAV.
			detachedBeforeRelease = completion->getFutureReferenceCount() == 1;
		}
		ThreadSingleAssignmentVar<int>::delref();
	}

	~OnMainThreadLifetimeResult() override { destroyed = true; }
};

void testOnMainThreadLifetime(bool ready, bool error, bool cancel, bool releaseEarly) {
	Promise<Void> signal;
	Promise<int> operation;
	bool detachedBeforeRelease = false;
	bool destroyed = false;
	Future<Void> completion;
	auto* result = new OnMainThreadLifetimeResult(operation.getFuture(), detachedBeforeRelease, destroyed);
	ThreadFuture<int> client(result);
	result->addref(); // Owned by doOnMainThread.
	completion = internal_thread_helper::doOnMainThread<int>(
	    signal.getFuture(),
	    { [](ThreadSingleAssignmentVar<int>* result) {
		     return static_cast<OnMainThreadLifetimeResult*>(result)->operation;
	     },
	      [](ThreadSingleAssignmentVar<int>* result) {
		      static_cast<OnMainThreadLifetimeResult*>(result)->operation = Future<int>();
	      } },
	    result);
	result->completion = &completion;
	result->setCancel(Future<Void>(completion));
	if (releaseEarly) {
		client = ThreadFuture<int>();
	}
	auto finish = [&] {
		if (cancel) {
			completion.cancel();
		} else if (error) {
			operation.sendError(operation_failed());
		} else {
			operation.send(42);
		}
	};
	if (ready) {
		finish();
	}
	signal.send(Void());
	if (!ready) {
		finish();
	}
	ASSERT(completion.isReady() && !completion.isError());
	ASSERT(detachedBeforeRelease);
	ASSERT(destroyed == releaseEarly);
	if (!releaseEarly) {
		if (cancel || error) {
			ASSERT(client.isError());
			ASSERT(client.getError().code() == (cancel ? error_code_actor_cancelled : error_code_operation_failed));
		} else {
			ASSERT(client.get() == 42);
		}
		client.cancel();
		ASSERT(destroyed);
	}
}

} // namespace

TEST_CASE("/flow/onMainThread/CompletionLifetime") {
	for (bool ready : { false, true }) {
		for (bool releaseEarly : { false, true }) {
			testOnMainThreadLifetime(ready, false, false, releaseEarly);
			testOnMainThreadLifetime(ready, true, false, releaseEarly);
			testOnMainThreadLifetime(ready, false, true, releaseEarly);
		}
	}
	return Void();
}

TEST_CASE("/flow/onMainThread/ConcurrentDestroy") {
	if (g_network->isSimulated()) {
		co_return;
	}
	std::atomic<int> completed{ 0 };
	std::atomic<int> failures{ 0 };
	std::vector<std::thread> workers;
	for (int worker = 0; worker < 4; ++worker) {
		workers.emplace_back([&] {
			for (int i = 0; i < 1000; ++i) {
				auto result = onMainThread([i]() -> Future<int> {
					if (i % 2) {
						co_await delay(0);
					}
					if (i % 3 == 0) {
						throw operation_failed();
					}
					co_return 42;
				});
				if (i % 5 == 0) {
					result.cancel(); // Also exercise destruction before the operation completes.
					continue;
				}
				result.blockUntilReady();
				if (i % 3 == 0 ? !result.isError() || result.getError().code() != error_code_operation_failed
				               : result.isError() || result.get() != 42) {
					++failures;
				}
				result.cancel(); // Mirrors fdb_future_destroy() on a client thread.
			}
			++completed;
		});
	}
	while (completed.load() != 4) {
		co_await delay(0.001);
	}
	for (auto& worker : workers) {
		worker.join();
	}
	ASSERT(failures.load() == 0);
}
