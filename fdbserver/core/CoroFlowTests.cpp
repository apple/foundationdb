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
	explicit TestReceiver(Reference<ReceiverState> state) : state(std::move(state)) {}
	~TestReceiver() override { state->destroy(); }
	void init() override { state->initialize(); }

private:
	Reference<ReceiverState> state;
};

class FailingTestReceiver final : public IThreadPoolReceiver {
public:
	FailingTestReceiver(Reference<ReceiverState> state, Error error) : state(std::move(state)), error(error) {}
	~FailingTestReceiver() override { state->destroy(); }
	void init() override {
		state->initialize();
		throw error;
	}

private:
	Reference<ReceiverState> state;
	Error error;
};

class ActionState final : public ReferenceCounted<ActionState> {
public:
	ActionState()
	  : startedFuture(started.getFuture()), completedFuture(completed.getFuture()),
	    destroyedFuture(destroyed.getFuture()) {}

	Future<Void> onStarted() const { return startedFuture; }
	Future<Void> onCompleted() const { return completedFuture; }
	Future<Void> onDestroyed() const { return destroyedFuture; }
	int getRunCount() const { return runCount; }
	int getCancelCount() const { return cancelCount; }

	void start() {
		ASSERT_EQ(runCount, 0);
		ASSERT_EQ(cancelCount, 0);
		++runCount;
		started.send(Void());
	}

	void complete() {
		finish();
		completed.send(Void());
	}

	void fail(Error error) {
		finish();
		completed.sendError(error);
	}

	void cancel() {
		ASSERT_EQ(runCount, 0);
		ASSERT_EQ(cancelCount, 0);
		ASSERT(!finished);
		++cancelCount;
		finished = true;
		started.sendError(actor_cancelled());
		completed.sendError(actor_cancelled());
	}

	void destroy() {
		ASSERT(finished);
		ASSERT_EQ(destroyCount, 0);
		++destroyCount;
		destroyed.send(Void());
	}

	void assertCompleted() const {
		ASSERT_EQ(runCount, 1);
		ASSERT_EQ(cancelCount, 0);
		ASSERT_EQ(destroyCount, 1);
	}

	void assertCancelled() const {
		ASSERT_EQ(runCount, 0);
		ASSERT_EQ(cancelCount, 1);
		ASSERT_EQ(destroyCount, 1);
	}

private:
	void finish() {
		ASSERT_EQ(runCount, 1);
		ASSERT_EQ(cancelCount, 0);
		ASSERT(!finished);
		finished = true;
	}

	ThreadReturnPromise<Void> started;
	ThreadReturnPromise<Void> completed;
	ThreadReturnPromise<Void> destroyed;
	Future<Void> startedFuture;
	Future<Void> completedFuture;
	Future<Void> destroyedFuture;
	int runCount{ 0 };
	int cancelCount{ 0 };
	int destroyCount{ 0 };
	bool finished{ false };
};

class TestAction final : public ThreadAction {
public:
	TestAction(Reference<ActionState> state, std::function<void()> work)
	  : state(std::move(state)), work(std::move(work)) {}
	~TestAction() { state->destroy(); }

	void operator()(IThreadPoolReceiver*) override {
		std::unique_ptr<TestAction> destroyOnReturn(this);
		state->start();
		try {
			work();
			state->complete();
		} catch (Error& e) {
			state->fail(e);
		} catch (...) {
			state->fail(unknown_error());
		}
	}

	void cancel() override {
		state->cancel();
		delete this;
	}

	double getTimeEstimate() const override { return 0.; }

private:
	Reference<ActionState> state;
	std::function<void()> work;
};

class DropPoolOnCancelAction final : public ThreadAction {
public:
	DropPoolOnCancelAction(Reference<ActionState> state, Reference<IThreadPool> pool)
	  : state(std::move(state)), pool(std::move(pool)) {}
	~DropPoolOnCancelAction() { state->destroy(); }

	void operator()(IThreadPoolReceiver*) override {
		std::unique_ptr<DropPoolOnCancelAction> destroyOnReturn(this);
		state->start();
		state->fail(operation_failed().asInjectedFault());
	}

	void cancel() override {
		state->cancel();
		pool.clear();
		delete this;
	}

	double getTimeEstimate() const override { return 0.; }

private:
	Reference<ActionState> state;
	Reference<IThreadPool> pool;
};

Reference<ActionState> postAction(Reference<IThreadPool> const& pool, std::function<void()> work) {
	auto state = makeReference<ActionState>();
	pool->post(new TestAction(state, std::move(work)));
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
	co_await action->onCompleted();
	co_await action->onDestroyed();
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
	ASSERT_EQ(action->getRunCount(), 0);
	co_await assertActionCompleted(action);
	ASSERT_EQ(receiver->getInitCount(), 1);
	ASSERT(!pool->getError().isReady());

	co_await pool->stop();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getDestroyCount(), 1);
	co_await pool->stop();
}

TEST_CASE("/fdbserver/CoroFlow/SuspendedWaits") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));

	Promise<int> value;
	auto valueAction = postAction(pool, [input = value.getFuture()] { ASSERT_EQ(waitForAndGet(input), 17); });
	co_await valueAction->onStarted();
	ASSERT(!valueAction->onCompleted().isReady());
	value.send(17);
	co_await assertActionCompleted(valueAction);

	Promise<Void> ready;
	auto voidAction = postAction(pool, [input = ready.getFuture()] { waitFor(input); });
	co_await voidAction->onStarted();
	ASSERT(!voidAction->onCompleted().isReady());
	ready.send(Void());
	co_await assertActionCompleted(voidAction);

	Promise<int> failedValue;
	auto valueErrorAction = postAction(pool, [input = failedValue.getFuture()] {
		assertThrowsError([&] { waitForAndGet(input); }, error_code_io_error);
	});
	co_await valueErrorAction->onStarted();
	ASSERT(!valueErrorAction->onCompleted().isReady());
	failedValue.sendError(io_error());
	co_await assertActionCompleted(valueErrorAction);

	Promise<Void> failedVoid;
	auto voidErrorAction = postAction(pool, [input = failedVoid.getFuture()] {
		assertThrowsError([&] { waitFor(input); }, error_code_operation_failed);
	});
	co_await voidErrorAction->onStarted();
	ASSERT(!voidErrorAction->onCompleted().isReady());
	failedVoid.sendError(operation_failed());
	co_await assertActionCompleted(voidErrorAction);

	ASSERT(!pool->getError().isReady());
	co_await pool->stop();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 1);
	ASSERT_EQ(receiver->getDestroyCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/StopCancelsQueuedAction") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	Promise<Void> gate;
	auto active = postAction(pool, [input = gate.getFuture()] { waitFor(input); });
	co_await active->onStarted();
	auto queued = postAction(pool, [] { ASSERT(false); });

	auto stopped = pool->stop();
	queued->assertCancelled();
	ASSERT_EQ(active->getCancelCount(), 0);
	ASSERT(!active->onCompleted().isReady());
	ASSERT(!stopped.isReady());
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	gate.send(Void());
	co_await assertActionCompleted(active);
	co_await queued->onDestroyed();
	co_await stopped;
	co_await receiver->onDestroyed();
	queued->assertCancelled();
	ASSERT_EQ(receiver->getDestroyCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/WorkerInitErrorStopsPool") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	const Error expectedError = operation_failed().asInjectedFault();
	auto poolError = pool->getError();
	pool->addThread(new FailingTestReceiver(receiver, expectedError));
	auto queued = postAction(pool, [] { ASSERT(false); });
	ASSERT(!poolError.isReady());

	bool caughtError{ false };
	try {
		co_await poolError;
	} catch (Error& e) {
		ASSERT_EQ(e.code(), expectedError.code());
		ASSERT(e.isInjectedFault());
		caughtError = true;
	}
	ASSERT(caughtError);

	// Cancellation must come from stopOnError, before any explicit stop call.
	co_await queued->onDestroyed();
	co_await receiver->onDestroyed();
	queued->assertCancelled();
	ASSERT_EQ(receiver->getInitCount(), 1);
	ASSERT_EQ(receiver->getDestroyCount(), 1);
	co_await pool->stop(expectedError);
}

TEST_CASE("/fdbserver/CoroFlow/DropBeforeStart") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	pool->addThread(new TestReceiver(receiver));
	auto queued = postAction(pool, [] { ASSERT(false); });
	ASSERT_EQ(receiver->getInitCount(), 0);

	pool.clear();
	queued->assertCancelled();
	co_await queued->onDestroyed();
	co_await receiver->onDestroyed();
	ASSERT_EQ(receiver->getInitCount(), 0);
	ASSERT_EQ(receiver->getDestroyCount(), 1);
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
	ASSERT_EQ(receiver->getDestroyCount(), 1);
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
	ASSERT_EQ(active->getCancelCount(), 0);
	ASSERT(!active->onCompleted().isReady());
	ASSERT_EQ(receiver->getDestroyCount(), 0);

	gate.sendError(io_error());
	co_await assertActionCompleted(active);
	co_await queued->onDestroyed();
	co_await receiver->onDestroyed();
	queued->assertCancelled();
	ASSERT_EQ(receiver->getInitCount(), 1);
	ASSERT_EQ(receiver->getDestroyCount(), 1);
}

TEST_CASE("/fdbserver/CoroFlow/EmptyPoolLifecycle") {
	{
		auto pool = CoroThreadPool::createThreadPool();
		co_await pool->stop();
	}
	auto pool = CoroThreadPool::createThreadPool();
	pool.clear();
}

TEST_CASE("/fdbserver/CoroFlow/ErrorStopDropsLastOwner") {
	auto receiver = makeReference<ReceiverState>();
	auto pool = CoroThreadPool::createThreadPool();
	const Error expectedError = operation_failed().asInjectedFault();
	auto poolError = pool->getError();
	pool->addThread(new FailingTestReceiver(receiver, expectedError));
	auto queued = makeReference<ActionState>();
	pool->post(new DropPoolOnCancelAction(queued, pool));

	// The queued action is the only external owner when stopOnError cancels it.
	pool.clear();
	ASSERT(!poolError.isReady());
	ASSERT_EQ(queued->getCancelCount(), 0);

	ErrorOr<Void> result = co_await coro::errorOr(poolError);
	ASSERT(result.isError());
	ASSERT_EQ(result.getError().code(), expectedError.code());
	ASSERT(result.getError().isInjectedFault());
	co_await queued->onDestroyed();
	co_await receiver->onDestroyed();
	queued->assertCancelled();
	ASSERT_EQ(receiver->getInitCount(), 1);
	ASSERT_EQ(receiver->getDestroyCount(), 1);
}
