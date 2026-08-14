/*
 * genericactors.cpp
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

#include "flow/flow.h"
#include "flow/UnitTest.h"
#include "flow/CoroUtils.h"

Future<bool> allTrue(std::vector<Future<bool>> all) {
	for (int i = 0; i != all.size(); ++i) {
		bool r = co_await all[i];
		if (!r)
			co_return false;
	}
	co_return true;
}

Future<Void> anyTrue(std::vector<Reference<AsyncVar<bool>>> input, Reference<AsyncVar<bool>> output) {
	while (true) {
		bool oneTrue = false;
		std::vector<Future<Void>> changes;
		for (const auto& it : input) {
			if (it->get())
				oneTrue = true;
			changes.push_back(it->onChange());
		}
		output->set(oneTrue);
		co_await waitForAny(changes);
	}
}

Future<Void> cancelOnly([[maybe_unused]] std::vector<Future<Void>> futures) {
	// We don't do anything with futures except hold them, we never return, but if we are cancelled we (naturally) drop
	// the futures
	co_await Future<Void>(Never());
}

Future<Void> timeoutWarningCollector(FutureStream<Void> input, double logDelay, const char* context, UID id) {
	uint64_t counter = 0;
	Future<Void> end = delay(logDelay);
	while (true) {
		auto res = co_await race(input, end);
		if (res.index() == 0) {
			counter++;
		} else {
			if (counter)
				TraceEvent(SevWarn, context, id).detail("LateProcessCount", counter).detail("LoggingDelay", logDelay);
			end = delay(logDelay);
			counter = 0;
		}
	}
}

Future<Void> waitForMost(std::vector<Future<ErrorOr<Void>>> futures,
                         int faultTolerance,
                         Error e,
                         double waitMultiplierForSlowFutures) {
	std::vector<Future<bool>> successFutures;
	double startTime = now();
	successFutures.reserve(futures.size());
	for (const auto& future : futures) {
		successFutures.push_back(fmap([](auto const& result) { return result.present(); }, future));
	}
	bool success = co_await quorumEqualsTrue(successFutures, successFutures.size() - faultTolerance);
	if (!success) {
		throw e;
	}
	co_await (delay((now() - startTime) * waitMultiplierForSlowFutures) || waitForAll(successFutures));
}

Future<bool> quorumEqualsTrue(std::vector<Future<bool>> futures, int required) {
	std::vector<Future<Void>> true_futures;
	std::vector<Future<Void>> false_futures;
	true_futures.reserve(futures.size());
	false_futures.reserve(futures.size());
	for (int i = 0; i < futures.size(); i++) {
		true_futures.push_back(onEqual(futures[i], true));
		false_futures.push_back(onEqual(futures[i], false));
	}

	auto res = co_await race(quorum(true_futures, required), quorum(false_futures, futures.size() - required + 1));
	co_return res.index() == 0;
}

Future<bool> shortCircuitAny(std::vector<Future<bool>> f) {
	std::vector<Future<Void>> sc;
	sc.reserve(f.size());
	for (const Future<bool>& fut : f) {
		sc.push_back(returnIfTrue(fut));
	}

	auto res = co_await race(waitForAll(f), waitForAny(sc));
	if (res.index() == 0) {
		// Handle a possible race condition? If the _last_ term to
		// be evaluated triggers the waitForAll before bubbling
		// out of the returnIfTrue quorum
		for (const auto& fut : f) {
			if (fut.get()) {
				co_return true;
			}
		}
		co_return false;
	}
	co_return true;
}

Future<Void> orYield(Future<Void> f) {
	if (f.isReady()) {
		if (f.isError()) {
			return tagError<Void>(yield(), f.getError());
		}
		return yield();
	}
	return f;
}

Future<Void> returnIfTrue(Future<bool> f) {
	bool b = co_await f;
	if (b) {
		co_return;
	}
	co_await Future<Void>(Never());
	throw internal_error();
}

Future<Void> lowPriorityDelay(double waitTime) {
	int totalLoops = std::max<int>(waitTime / FLOW_KNOBS->LOW_PRIORITY_MAX_DELAY, FLOW_KNOBS->LOW_PRIORITY_DELAY_COUNT);

	for (int loopCount = 0; loopCount < totalLoops; ++loopCount) {
		co_await delay(waitTime / totalLoops, TaskPriority::Low);
	}
}

Future<Void> delayAfterCleared(Reference<AsyncVar<bool>> condition, double time, TaskPriority taskID) {
	Future<Void> timer = condition->get() ? Never() : delay(time, taskID);
	bool previousState = condition->get();
	while (true) {
		auto res = co_await race(timer, condition->onChange());
		if (res.index() == 0) {
			co_return;
		}
		bool currentState = condition->get();
		if (currentState != previousState) {
			timer = currentState ? Never() : delay(time, taskID);
			previousState = currentState;
		}
	}
}

// Same as delayAfterCleared, but use lowPriorityDelay.
Future<Void> lowPriorityDelayAfterCleared(Reference<AsyncVar<bool>> condition, double time) {
	Future<Void> timer = condition->get() ? Never() : lowPriorityDelay(time);
	bool previousState = condition->get();
	while (true) {
		auto res = co_await race(timer, condition->onChange());
		if (res.index() == 0) {
			co_return;
		}
		bool currentState = condition->get();
		if (currentState != previousState) {
			timer = currentState ? Never() : lowPriorityDelay(time);
			previousState = currentState;
		}
	}
}

struct SetAsyncVarTrue {
	Reference<AsyncVar<bool>> value;
	void operator()() const { value->set(true); }
};

namespace {

struct DummyState {
	int changed{ 0 };
	int unchanged{ 0 };
	bool operator==(DummyState const& rhs) const { return changed == rhs.changed && unchanged == rhs.unchanged; }
	bool operator!=(DummyState const& rhs) const { return !(*this == rhs); }
};

Future<Void> testPublisher(Reference<AsyncVar<DummyState>> input) {
	for (int i = 0; i < 100; ++i) {
		co_await delay(deterministicRandom()->random01());
		auto var = input->get();
		++var.changed;
		input->set(var);
	}
}

Future<Void> testSubscriber(Reference<IAsyncListener<int>> output, Optional<int> expected) {
	while (true) {
		co_await output->onChange();
		ASSERT(expected.present());
		if (output->get() == expected.get()) {
			co_return;
		}
	}
}

static Future<ErrorOr<Void>> goodTestFuture(double duration) {
	return tag(delay(duration), ErrorOr<Void>(Void()));
}

static Future<ErrorOr<Void>> badTestFuture(double duration, Error e) {
	return tag(delay(duration), ErrorOr<Void>(e));
}

Future<int> getErrorCode(Future<int> future) {
	try {
		int value = co_await future;
		(void)value;
		co_return 0;
	} catch (Error& e) {
		co_return e.code();
	}
}

Future<int> getVoidErrorCode(Future<Void> future) {
	try {
		co_await future;
		co_return 0;
	} catch (Error& e) {
		co_return e.code();
	}
}

} // namespace

TEST_CASE("/flow/genericactors/AsyncListener") {
	auto input = makeReference<AsyncVar<DummyState>>();
	Future<Void> subscriber1 =
	    testSubscriber(IAsyncListener<int>::create(input, [](auto const& var) { return var.changed; }), 100);
	Future<Void> subscriber2 =
	    testSubscriber(IAsyncListener<int>::create(input, [](auto const& var) { return var.unchanged; }), {});
	co_await (subscriber1 && testPublisher(input));
	ASSERT(!subscriber2.isReady());
}

TEST_CASE("/flow/genericactors/DelayedAsyncVarPreservesReentrantInputChange") {
	Reference<AsyncVar<bool>> input = makeReference<AsyncVar<bool>>(true);
	Reference<AsyncVar<bool>> output = makeReference<AsyncVar<bool>>(true);
	Future<Void> feedback = trigger(SetAsyncVarTrue{ input }, output->onChange());
	Future<Void> publisher = delayedAsyncVar(input, output, 0);

	co_await delay(0);
	input->set(false);
	co_await feedback;
	co_await delay(0.01);
	ASSERT(input->get());
	ASSERT(output->get());

	publisher.cancel();
}

TEST_CASE("/flow/genericactors/WaitForMost") {
	std::vector<Future<ErrorOr<Void>>> futures;
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		co_await waitForMost(futures, 1, operation_failed(), 0.0); // Don't wait for slowest future
		ASSERT(!futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		co_await waitForMost(futures, 0, operation_failed(), 0.0); // Wait for all futures
		ASSERT(futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		co_await waitForMost(futures, 1, operation_failed(), 1.0); // Wait for slowest future
		ASSERT(futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), badTestFuture(1, success()) };
		co_await waitForMost(futures, 1, operation_failed(), 1.0); // Error ignored
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), badTestFuture(1, success()) };
		try {
			co_await waitForMost(futures, 0, operation_failed(), 1.0);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
}

Future<int64_t> notifiedWaitForForwardTest(NotifiedInt* version, bool* fired) {
	co_await version->whenAtLeast(10);
	*fired = true;
	co_return 10;
}

Future<Void> notifiedWaitForTimeoutVoidTest(NotifiedInt* version, bool* fired) {
	co_await version->whenAtLeast(10);
	*fired = true;
}

Future<Void> cancelForwardWhenReady(Future<int64_t> signal, Future<int64_t>* forwarded) {
	co_await signal;
	forwarded->cancel();
}

Future<Void> cancelForwardOnError(Future<int64_t> signal, Future<int64_t>* forwarded) {
	try {
		co_await signal;
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
		forwarded->cancel();
	}
}

TEST_CASE("/flow/genericactors/NotifiedCancellation") {
	NotifiedInt version(0);
	bool forwardedWaitFired = false;
	Promise<int64_t> forwardedReply;
	Future<int64_t> cancelledForward =
	    forward(notifiedWaitForForwardTest(&version, &forwardedWaitFired), forwardedReply);
	cancelledForward.cancel();
	ASSERT(cancelledForward.isError() && cancelledForward.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!forwardedWaitFired);

	version = NotifiedInt(0);
	bool chosenWaitFired = false;
	Promise<int64_t> otherChoice;
	Future<int64_t> cancelledChoice =
	    chooseActor(notifiedWaitForForwardTest(&version, &chosenWaitFired), otherChoice.getFuture());
	cancelledChoice.cancel();
	ASSERT(cancelledChoice.isError() && cancelledChoice.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!chosenWaitFired);

	version = NotifiedInt(0);
	bool brokenWaitFired = false;
	Future<int64_t> cancelledBroken = brokenPromiseToNever(notifiedWaitForForwardTest(&version, &brokenWaitFired));
	cancelledBroken.cancel();
	ASSERT(cancelledBroken.isError() && cancelledBroken.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!brokenWaitFired);

	version = NotifiedInt(0);
	bool timedWaitFired = false;
	Future<Optional<int64_t>> cancelledTimeout = timeout(notifiedWaitForForwardTest(&version, &timedWaitFired), 60.0);
	cancelledTimeout.cancel();
	ASSERT(cancelledTimeout.isError() && cancelledTimeout.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!timedWaitFired);

	version = NotifiedInt(0);
	bool expiredWaitFired = false;
	Future<Optional<int64_t>> expiredTimeout = timeout(notifiedWaitForForwardTest(&version, &expiredWaitFired), 0.0);
	Optional<int64_t> timeoutResult = co_await expiredTimeout;
	ASSERT(!timeoutResult.present());
	version.set(10);
	ASSERT(!expiredWaitFired);

	version = NotifiedInt(0);
	bool timedValueWaitFired = false;
	Future<int64_t> cancelledTimeoutValue =
	    timeout(notifiedWaitForForwardTest(&version, &timedValueWaitFired), 60.0, int64_t(-1));
	cancelledTimeoutValue.cancel();
	ASSERT(cancelledTimeoutValue.isError() && cancelledTimeoutValue.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!timedValueWaitFired);

	version = NotifiedInt(0);
	bool expiredValueWaitFired = false;
	Future<int64_t> expiredTimeoutValue =
	    timeout(notifiedWaitForForwardTest(&version, &expiredValueWaitFired), 0.0, int64_t(-1));
	int64_t timeoutValue = co_await expiredTimeoutValue;
	ASSERT_EQ(timeoutValue, -1);
	version.set(10);
	ASSERT(!expiredValueWaitFired);

	version = NotifiedInt(0);
	bool timedVoidWaitFired = false;
	Future<Void> cancelledTimeoutVoid =
	    timeout(notifiedWaitForTimeoutVoidTest(&version, &timedVoidWaitFired), 60.0, Void());
	cancelledTimeoutVoid.cancel();
	ASSERT(cancelledTimeoutVoid.isError() && cancelledTimeoutVoid.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!timedVoidWaitFired);

	version = NotifiedInt(0);
	bool expiredVoidWaitFired = false;
	Future<Void> expiredTimeoutVoid =
	    timeout(notifiedWaitForTimeoutVoidTest(&version, &expiredVoidWaitFired), 0.0, Void());
	co_await expiredTimeoutVoid;
	version.set(10);
	ASSERT(!expiredVoidWaitFired);

	version = NotifiedInt(0);
	bool timedErrorWaitFired = false;
	Future<int64_t> cancelledTimeoutError =
	    timeoutError(notifiedWaitForForwardTest(&version, &timedErrorWaitFired), 60.0);
	cancelledTimeoutError.cancel();
	ASSERT(cancelledTimeoutError.isError() && cancelledTimeoutError.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!timedErrorWaitFired);

	version = NotifiedInt(0);
	bool expiredErrorWaitFired = false;
	Future<int64_t> expiredTimeoutError =
	    timeoutError(notifiedWaitForForwardTest(&version, &expiredErrorWaitFired), 0.0);
	try {
		int64_t unused = co_await expiredTimeoutError;
		(void)unused;
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_timed_out);
	}
	version.set(10);
	ASSERT(!expiredErrorWaitFired);

	version = NotifiedInt(0);
	bool errorWaitFired = false;
	Future<int64_t> cancelledWaitOrError =
	    waitOrError(notifiedWaitForForwardTest(&version, &errorWaitFired), Future<Void>(Never()));
	cancelledWaitOrError.cancel();
	ASSERT(cancelledWaitOrError.isError() && cancelledWaitOrError.getError().code() == error_code_actor_cancelled);
	version.set(10);
	ASSERT(!errorWaitFired);

	Promise<int64_t> forwardedInput;
	Promise<int64_t> reentrantReply;
	Future<int64_t> reentrantForward = forward(forwardedInput.getFuture(), reentrantReply);
	Future<Void> reentrantCancel = cancelForwardWhenReady(reentrantReply.getFuture(), &reentrantForward);
	forwardedInput.send(10);
	co_await reentrantCancel;
	int64_t forwardedValue = co_await reentrantForward;
	ASSERT_EQ(forwardedValue, 10);

	Promise<int64_t> erroredInput;
	Promise<int64_t> erroredReply;
	Future<int64_t> erroredForward = forward(erroredInput.getFuture(), erroredReply);
	Future<Void> errorCancel = cancelForwardOnError(erroredReply.getFuture(), &erroredForward);
	erroredInput.sendError(operation_failed());
	co_await errorCancel;
	ASSERT(erroredForward.isError() && erroredForward.getError().code() == error_code_operation_failed);
}

TEST_CASE("/flow/genericactors/WaitForFirstReleasesCapturedState") {
	Promise<int64_t> pendingSuccess;
	Future<int64_t> completed =
	    waitForFirst(std::vector<Future<int64_t>>{ Future<int64_t>(10), pendingSuccess.getFuture() });
	ASSERT(completed.isReady() && !completed.isError() && completed.get() == 10);
	ASSERT_EQ(pendingSuccess.getFutureReferenceCount(), 0);

	Promise<int64_t> pendingError;
	Future<int64_t> failed =
	    waitForFirst(std::vector<Future<int64_t>>{ Future<int64_t>(operation_failed()), pendingError.getFuture() });
	ASSERT(failed.isError() && failed.getError().code() == error_code_operation_failed);
	ASSERT_EQ(pendingError.getFutureReferenceCount(), 0);

	Promise<int64_t> pendingCancellation;
	Future<int64_t> cancelled = waitForFirst(std::vector<Future<int64_t>>{ pendingCancellation.getFuture() });
	cancelled.cancel();
	ASSERT(cancelled.isError() && cancelled.getError().code() == error_code_actor_cancelled);
	ASSERT_EQ(pendingCancellation.getFutureReferenceCount(), 0);
	return Void();
}

TEST_CASE("/flow/genericactors/ReadyRaceReleasesCapturedState") {
	NotifiedInt version(0);
	bool pendingFirstChoiceFired = false;
	Future<int64_t> readySecondChoice =
	    chooseActor(notifiedWaitForForwardTest(&version, &pendingFirstChoiceFired), Future<int64_t>(10));
	ASSERT(readySecondChoice.isReady() && !readySecondChoice.isError() && readySecondChoice.get() == 10);
	version.set(10);
	ASSERT(!pendingFirstChoiceFired);

	version = NotifiedInt(0);
	bool pendingSecondChoiceFired = false;
	Future<int64_t> readyFirstChoice =
	    chooseActor(Future<int64_t>(10), notifiedWaitForForwardTest(&version, &pendingSecondChoiceFired));
	ASSERT(readyFirstChoice.isReady() && !readyFirstChoice.isError() && readyFirstChoice.get() == 10);
	version.set(10);
	ASSERT(!pendingSecondChoiceFired);

	version = NotifiedInt(0);
	bool failedWaitFired = false;
	Future<int64_t> failedWait =
	    waitOrError(notifiedWaitForForwardTest(&version, &failedWaitFired), Future<Void>(operation_failed()));
	ASSERT(failedWait.isError() && failedWait.getError().code() == error_code_operation_failed);
	version.set(10);
	ASSERT(!failedWaitFired);

	Promise<Void> unusedErrorSignal;
	Future<int64_t> readyValue = waitOrError(Future<int64_t>(10), unusedErrorSignal.getFuture());
	ASSERT(readyValue.isReady() && !readyValue.isError() && readyValue.get() == 10);
	ASSERT_EQ(unusedErrorSignal.getFutureReferenceCount(), 0);

	PromiseStream<int64_t> pendingStream;
	Future<int64_t> failedStreamWait = waitOrError(pendingStream.getFuture(), Future<Void>(operation_failed()));
	ASSERT(failedStreamWait.isError() && failedStreamWait.getError().code() == error_code_operation_failed);
	ASSERT_EQ(pendingStream.getFutureReferenceCount(), 0);
	return Void();
}

TEST_CASE("/flow/genericactors/HoldWhileReleasesCapturedState") {
	FlowLock lock(1);

	co_await lock.take();
	Promise<int64_t> completedInput;
	Future<int64_t> completed = holdWhile(std::make_shared<FlowLock::Releaser>(lock, 1), completedInput.getFuture());
	ASSERT_EQ(lock.available(), 0);
	completedInput.send(10);
	ASSERT(completed.isReady() && !completed.isError() && completed.get() == 10);
	ASSERT_EQ(lock.available(), 1);

	co_await lock.take();
	Promise<int64_t> erroredInput;
	Future<int64_t> errored = holdWhile(std::make_shared<FlowLock::Releaser>(lock, 1), erroredInput.getFuture());
	ASSERT_EQ(lock.available(), 0);
	erroredInput.sendError(operation_failed());
	ASSERT(errored.isError() && errored.getError().code() == error_code_operation_failed);
	ASSERT_EQ(lock.available(), 1);

	co_await lock.take();
	Promise<int64_t> cancelledInput;
	Future<int64_t> cancelled = holdWhile(std::make_shared<FlowLock::Releaser>(lock, 1), cancelledInput.getFuture());
	ASSERT_EQ(lock.available(), 0);
	cancelled.cancel();
	ASSERT(cancelled.isError() && cancelled.getError().code() == error_code_actor_cancelled);
	ASSERT_EQ(lock.available(), 1);
}

TEST_CASE("/flow/genericactors/ThrowErrorOr") {
	int value = co_await throwErrorOr<int>(Future<ErrorOr<int>>(ErrorOr<int>(7)));
	ASSERT_EQ(value, 7);

	int errorCode = co_await getErrorCode(throwErrorOr<int>(Future<ErrorOr<int>>(ErrorOr<int>(operation_failed()))));
	ASSERT_EQ(errorCode, error_code_operation_failed);
}

TEST_CASE("/flow/genericactors/TraceAfter") {
	int value = co_await traceAfter<int>(Future<int>(7), "GenericActorsTraceAfter");
	ASSERT_EQ(value, 7);

	int errorCode =
	    co_await getErrorCode(traceAfter<int>(Future<int>(operation_failed()), "GenericActorsTraceAfterError"));
	ASSERT_EQ(errorCode, error_code_operation_failed);
}

TEST_CASE("/flow/genericactors/TransformErrors") {
	int value = co_await transformErrors<int>(Future<int>(7), operation_failed());
	ASSERT_EQ(value, 7);

	int errorCode = co_await getErrorCode(transformErrors<int>(Future<int>(transaction_too_old()), operation_failed()));
	ASSERT_EQ(errorCode, error_code_operation_failed);
}

TEST_CASE("/flow/genericactors/TransformError") {
	int value = co_await transformError<int>(Future<int>(7), transaction_too_old(), operation_failed());
	ASSERT_EQ(value, 7);

	int transformedErrorCode = co_await getErrorCode(
	    transformError<int>(Future<int>(transaction_too_old()), transaction_too_old(), operation_failed()));
	ASSERT_EQ(transformedErrorCode, error_code_operation_failed);

	int preservedErrorCode = co_await getErrorCode(
	    transformError<int>(Future<int>(process_behind()), transaction_too_old(), operation_failed()));
	ASSERT_EQ(preservedErrorCode, error_code_process_behind);
}

TEST_CASE("/flow/genericactors/WaitForAllReady") {
	std::vector<Future<int>> results = { Future<int>(1), Future<int>(operation_failed()), Future<int>(3) };

	co_await waitForAllReady<int>(results);
}

TEST_CASE("/flow/genericactors/Timeout") {
	int readyValue = co_await timeout<int>(Future<int>(7), 0.0, -1);
	ASSERT_EQ(readyValue, 7);

	int timedOutValue = co_await timeout<int>(Future<int>(Never()), 0.0, -1);
	ASSERT_EQ(timedOutValue, -1);

	Optional<int> readyOptional = co_await timeout<int>(Future<int>(7), 0.0);
	ASSERT(readyOptional.present());
	ASSERT_EQ(readyOptional.get(), 7);

	Optional<int> timedOutOptional = co_await timeout<int>(Future<int>(Never()), 0.0);
	ASSERT(!timedOutOptional.present());

	int errorCode = co_await getErrorCode(timeoutError<int>(Future<int>(Never()), 0.0));
	ASSERT_EQ(errorCode, error_code_timed_out);
}

TEST_CASE("/flow/genericactors/Delayed") {
	int value = co_await delayed<int>(Future<int>(7));
	ASSERT_EQ(value, 7);

	int errorCode = co_await getErrorCode(delayed<int>(Future<int>(operation_failed())));
	ASSERT_EQ(errorCode, error_code_operation_failed);
}

TEST_CASE("/flow/genericactors/Trigger") {
	Reference<AsyncVar<bool>> called = makeReference<AsyncVar<bool>>(false);
	Promise<Void> signal;
	Future<Void> triggered = trigger(SetAsyncVarTrue{ called }, signal.getFuture());

	ASSERT(!called->get());
	ASSERT(!triggered.isReady());

	signal.send(Void());
	co_await triggered;
	ASSERT(called->get());

	called->set(false);
	int errorCode = co_await getVoidErrorCode(trigger(SetAsyncVarTrue{ called }, Future<Void>(operation_failed())));
	ASSERT_EQ(errorCode, error_code_operation_failed);
	ASSERT(!called->get());
}
