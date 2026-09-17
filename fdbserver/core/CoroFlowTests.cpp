/*
 * CoroFlowTests.cpp
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

#include <functional>
#include <memory>
#include <utility>

#include "flow/Coroutines.h"
#include "flow/UnitTest.h"

void forceLinkCoroFlowTests() {}

namespace {

// Notifications must return through the network task queue, not resume a test on a stackful worker's stack.
class ReceiverState final : public ReferenceCounted<ReceiverState> {
public:
	ReceiverState() : initializedFuture(initialized.getFuture()), destroyedFuture(destroyed.getFuture()) {}

	Future<Void> onInitialized() const { return initializedFuture; }
	Future<Void> onDestroyed() const { return destroyedFuture; }
	int getInitCount() const { return initCount; }
	int getDestroyCount() const { return destroyCount; }

	void initialize() {
		ASSERT_EQ(initCount, 0);
		ASSERT_EQ(destroyCount, 0);
		++initCount;
		initialized.send(Void());
	}

	void destroy() {
		ASSERT_EQ(destroyCount, 0);
		++destroyCount;
		if (initCount == 0) {
			initialized.sendError(actor_cancelled());
		}
		destroyed.send(Void());
	}

private:
	ThreadReturnPromise<Void> initialized;
	ThreadReturnPromise<Void> destroyed;
	Future<Void> initializedFuture;
	Future<Void> destroyedFuture;
	int initCount{ 0 };
	int destroyCount{ 0 };
};

class TestReceiver final : public IThreadPoolReceiver {
public:
	explicit TestReceiver(Reference<ReceiverState> state, Error initError = Error())
	  : state(std::move(state)), initError(initError) {}
	~TestReceiver() override { state->destroy(); }
	void init() override {
		state->initialize();
		if (initError.isValid()) {
			throw initError;
		}
	}

private:
	Reference<ReceiverState> state;
	Error initError;
};

class ActionState final : public ReferenceCounted<ActionState> {
public:
	ActionState() : startedFuture(started.getFuture()), doneFuture(done.getFuture()) {}

	Future<Void> onStarted() const { return startedFuture; }
	Future<Void> onDone() const { return doneFuture; }
	void assertQueued() const { ASSERT(phase == Phase::Queued); }
	void assertRunning() const { ASSERT(phase == Phase::Running); }
	void assertCompleted() const { ASSERT(phase == Phase::Completed && destroyed); }
	void assertCancelled() const { ASSERT(phase == Phase::Cancelled && destroyed); }

	void start() {
		assertQueued();
		phase = Phase::Running;
		started.send(Void());
	}

	void finish(Error error = Error()) {
		assertRunning();
		phase = Phase::Completed;
		workError = error;
	}

	void cancel() {
		assertQueued();
		phase = Phase::Cancelled;
		started.sendError(actor_cancelled());
	}

	void destroy() {
		ASSERT(!destroyed && (phase == Phase::Completed || phase == Phase::Cancelled));
		destroyed = true;
		if (workError.isValid()) {
			done.sendError(workError);
		} else {
			done.send(Void());
		}
	}

private:
	enum class Phase { Queued, Running, Completed, Cancelled };
	ThreadReturnPromise<Void> started;
	ThreadReturnPromise<Void> done;
	Future<Void> startedFuture;
	Future<Void> doneFuture;
	Phase phase{ Phase::Queued };
	Error workError;
	bool destroyed{ false };
};

class TestAction final : public ThreadAction {
public:
	TestAction(Reference<ActionState> state, std::function<void()> work, std::function<void()> beforeCancel)
	  : state(std::move(state)), work(std::move(work)), beforeCancel(std::move(beforeCancel)) {}
	~TestAction() { state->destroy(); }

	void operator()(IThreadPoolReceiver*) override {
		std::unique_ptr<TestAction> destroyOnReturn(this);
		state->start();
		Error error;
		try {
			work();
		} catch (Error& e) {
			error = e;
		} catch (...) {
			error = unknown_error();
		}
		state->finish(error);
	}

	void cancel() override {
		// A throwing callback must leave this action queued and available for a later stop() retry.
		if (beforeCancel) {
			beforeCancel();
		}
		state->cancel();
		delete this;
	}

	double getTimeEstimate() const override { return 0.; }

private:
	Reference<ActionState> state;
	std::function<void()> work;
	std::function<void()> beforeCancel;
};

Reference<ActionState> postAction(Reference<IThreadPool> const& pool,
                                  std::function<void()> work,
                                  std::function<void()> beforeCancel = {}) {
	auto state = makeReference<ActionState>();
	pool->post(new TestAction(state, std::move(work), std::move(beforeCancel)));
	return state;
}

template <class F>
void assertThrowsError(F&& f, int expectedCode) {
	try {
		std::forward<F>(f)();
	} catch (Error& e) {
		ASSERT_EQ(e.code(), expectedCode);
		return;
	}
	ASSERT(false);
}

Future<Void> assertActionCompleted(Reference<ActionState> action) {
	co_await action->onDone();
	action->assertCompleted();
}

} // namespace

TEST_CASE("/fdbserver/CoroFlow/DeferredStartAndReadyWaits") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	ASSERT(pool->isCoro());
	pool->addThread(new TestReceiver(receiver));
	ASSERT_EQ(receiver->getInitCount(), 0);

	auto action = postAction(pool, [] {
		waitFor(Future<Void>(Void()));
		ASSERT_EQ(waitForAndGet(Future<int>(42)), 42);
		assertThrowsError([] { waitFor(Future<Void>(operation_failed())); }, error_code_operation_failed);
		assertThrowsError([] { waitForAndGet(Future<int>(io_error())); }, error_code_io_error);
	});
	action->assertQueued();
	co_await assertActionCompleted(action);
	ASSERT_EQ(receiver->getInitCount(), 1);
	ASSERT(!pool->getError().isReady());

	co_await pool->stop();
	co_await receiver->onDestroyed();
	co_await pool->stop();
}

TEST_CASE("/fdbserver/CoroFlow/SuspendedWaits") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));

	Promise<int> value;
	auto valueAction = postAction(pool, [input = value.getFuture()] { ASSERT_EQ(waitForAndGet(input), 17); });
	co_await valueAction->onStarted();
	valueAction->assertRunning();
	value.send(17);
	co_await assertActionCompleted(valueAction);

	Promise<Void> ready;
	auto voidAction = postAction(pool, [input = ready.getFuture()] { waitFor(input); });
	co_await voidAction->onStarted();
	voidAction->assertRunning();
	ready.send(Void());
	co_await assertActionCompleted(voidAction);

	Promise<int> failedValue;
	auto valueErrorAction = postAction(pool, [input = failedValue.getFuture()] {
		assertThrowsError([&] { waitForAndGet(input); }, error_code_io_error);
	});
	co_await valueErrorAction->onStarted();
	valueErrorAction->assertRunning();
	failedValue.sendError(io_error());
	co_await assertActionCompleted(valueErrorAction);

	Promise<Void> failedVoid;
	auto voidErrorAction = postAction(pool, [input = failedVoid.getFuture()] {
		assertThrowsError([&] { waitFor(input); }, error_code_operation_failed);
	});
	co_await voidErrorAction->onStarted();
	voidErrorAction->assertRunning();
	failedVoid.sendError(operation_failed());
	co_await assertActionCompleted(voidErrorAction);

	ASSERT(!pool->getError().isReady());
	co_await pool->stop();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/DropBeforeStart") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	auto queued = postAction(pool, [] { ASSERT(false); });
	ASSERT_EQ(receiver->getInitCount(), 0);

	pool.clear();
	queued->assertCancelled();
	co_await queued->onDone();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 0);
}

TEST_CASE("/fdbserver/CoroFlow/DropIdlePool") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	co_await receiver->onInitialized();
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	pool.clear();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/DropBusyPool") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	Promise<Void> gate;
	auto active = postAction(
	    pool, [input = gate.getFuture()] { assertThrowsError([&] { waitFor(input); }, error_code_io_error); });
	co_await active->onStarted();
	auto queued = postAction(pool, [] { ASSERT(false); });

	pool.clear();
	queued->assertCancelled();
	active->assertRunning();
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	gate.sendError(io_error());
	co_await assertActionCompleted(active);
	co_await queued->onDone();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/EmptyPoolLifecycle") {
	auto pool = CoroThreadPool::createThreadPool();
	co_await pool->stop();
	pool = CoroThreadPool::createThreadPool();
	pool.clear();
}

TEST_CASE("/fdbserver/CoroFlow/ErrorStopDropsLastOwner") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	const Error expectedError = operation_failed().asInjectedFault();
	auto poolError = pool->getError();
	pool->addThread(new TestReceiver(receiver, expectedError));
	auto queued = postAction(pool, [] { ASSERT(false); }, [owner = pool]() mutable { owner.clear(); });

	// The queued action is the only external owner when stopOnError cancels it.
	pool.clear();
	ASSERT(!poolError.isReady());
	queued->assertQueued();

	ErrorOr<Void> result = co_await coro::errorOr(poolError);
	ASSERT(result.isError());
	ASSERT_EQ(result.getError().code(), expectedError.code());
	ASSERT(result.getError().isInjectedFault());
	// Cancellation must come from stopOnError, without an explicit stop call.
	co_await queued->onDone();
	co_await receiver->onDestroyed();
	queued->assertCancelled();
	ASSERT_EQ(receiver->getInitCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/StopRetryAfterCancelError") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	Promise<Void> gate;
	auto active = postAction(pool, [input = gate.getFuture()] { waitFor(input); });
	co_await active->onStarted();
	const Error expectedError = operation_failed().asInjectedFault();
	auto attempts = std::make_shared<int>(0);
	auto queued = postAction(
	    pool,
	    [] { ASSERT(false); },
	    [attempts, expectedError] {
		    if (++*attempts == 1) {
			    throw expectedError;
		    }
		    ASSERT_EQ(*attempts, 2);
	    });

	Error observedError;
	try {
		pool->stop();
	} catch (Error& e) {
		observedError = e;
	}
	ASSERT_EQ(observedError.code(), expectedError.code());
	ASSERT(observedError.isInjectedFault());
	ASSERT_EQ(*attempts, 1);
	queued->assertQueued();
	active->assertRunning();
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	pool.clear();
	ASSERT_EQ(*attempts, 2);
	queued->assertCancelled();
	active->assertRunning();
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	gate.send(Void());
	co_await assertActionCompleted(active);
	co_await queued->onDone();
	co_await receiver->onDestroyed();
	ASSERT_EQ(*attempts, 2);
	ASSERT_EQ(receiver->getInitCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/ExplicitStopDropsLastOwner") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	Promise<Void> gate;
	auto active = postAction(pool, [input = gate.getFuture()] { waitFor(input); });
	co_await active->onStarted();
	auto queued = postAction(pool, [] { ASSERT(false); }, [owner = pool]() mutable { owner.clear(); });

	// The queued action keeps this pointer alive until stop() begins canceling it.
	IThreadPool* poolToStop = pool.getPtr();
	pool.clear();
	auto stopped = poolToStop->stop();
	queued->assertCancelled();
	ASSERT(!stopped.isReady());
	active->assertRunning();
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	gate.send(Void());
	co_await assertActionCompleted(active);
	co_await queued->onDone();
	co_await stopped;
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 1);
}
