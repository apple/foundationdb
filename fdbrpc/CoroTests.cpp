/*
 * CoroTests.cpp
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

#include "flow/UnitTest.h"
#include "flow/IAsyncFile.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/TLSConfig.actor.h"

#include <sstream>
#include <cstdint>
#include <ranges>
#include <iterator>

#include <fmt/base.h>
#include <fmt/format.h>

void forceLinkCoroTests() {}

using namespace std::literals::string_literals;

TEST_CASE("/flow/coro/buggifiedDelay") {
	if (FLOW_KNOBS->MAX_BUGGIFIED_DELAY == 0) {
		co_return;
	}
	loop {
		double x = deterministicRandom()->random01();
		int last = 0;
		Future<Void> f1 = map(delay(x), [last = &last](const Void&) {
			*last = 1;
			return Void();
		});
		Future<Void> f2 = map(delay(x), [last = &last](const Void&) {
			*last = 2;
			return Void();
		});
		co_await (f1 && f2);
		if (last == 1) {
			CODE_PROBE(true, "Delays can become ready out of order", probe::decoration::rare);
			co_return;
		}
	}
}

template <class T, class Func, class ErrFunc, class CallbackType>
class LambdaCallback final : public CallbackType, public FastAllocated<LambdaCallback<T, Func, ErrFunc, CallbackType>> {
	Func func;
	ErrFunc errFunc;

	void fire(T const& t) override {
		CallbackType::remove();
		func(t);
		delete this;
	}
	void fire(T&& t) override {
		CallbackType::remove();
		func(std::move(t));
		delete this;
	}
	void error(Error e) override {
		CallbackType::remove();
		errFunc(e);
		delete this;
	}

public:
	LambdaCallback(Func&& f, ErrFunc&& e) : func(std::move(f)), errFunc(std::move(e)) {}
};

template <class T, class Func, class ErrFunc>
void onReady(Future<T>&& f, Func&& func, ErrFunc&& errFunc) {
	if (f.isReady()) {
		if (f.isError())
			errFunc(f.getError());
		else
			func(f.get());
	} else
		f.addCallbackAndClear(new LambdaCallback<T, Func, ErrFunc, Callback<T>>(std::move(func), std::move(errFunc)));
}

template <class T, class Func, class ErrFunc>
void onReady(FutureStream<T>&& f, Func&& func, ErrFunc&& errFunc) {
	if (f.isReady()) {
		if (f.isError())
			errFunc(f.getError());
		else
			func(f.pop());
	} else
		f.addCallbackAndClear(
		    new LambdaCallback<T, Func, ErrFunc, SingleCallback<T>>(std::move(func), std::move(errFunc)));
}

namespace {
void emptyVoidActor() {}

Future<Void> emptyActor() {
	return Void();
}

Future<Void> oneWaitVoidActor(Uncancellable, Future<Void> f) {
	co_await f;
	co_return;
}

Future<Void> oneWaitActor(Future<Void> f) {
	co_return co_await f;
}

Future<Void> g_cheese;
Future<Void> cheeseWaitActor() {
	co_await g_cheese;
	co_return;
}

void trivialVoidActor(int* result) {
	*result = 1;
}

Future<int> return42Actor() {
	return 42;
}

Future<Void> voidWaitActor(Uncancellable, Future<int> in, int* result) {
	int i = co_await in;
	*result = i;
}

Future<int> addOneActor(Future<int> in) {
	co_return (co_await in) + 1;
}

Future<Void> chooseTwoActor(Future<Void> f, Future<Void> g) {
	co_return co_await Choose().When(f, [](const Void&) {}).When(g, [](const Void&) {}).run();
}

Future<int> consumeOneActor(FutureStream<int> in) {
	int i = co_await in;
	co_return i;
}

Future<int> sumActor(FutureStream<int> in) {
	int total = 0;
	try {
		loop {
			int i = co_await in;
			total += i;
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}
	co_return total;
}

template <class T>
Future<T> templateActor(T t) {
	return t;
}

// bool expectActorCount(int x) { return actorCount == x; }
bool expectActorCount(int) {
	return true;
}

struct YieldMockNetwork final : INetwork, ReferenceCounted<YieldMockNetwork> {
	int ticks;
	Promise<Void> nextTick;
	int nextYield;
	INetwork* baseNetwork;

	flowGlobalType global(int id) const override { return baseNetwork->global(id); }
	void setGlobal(size_t id, flowGlobalType v) override {
		baseNetwork->setGlobal(id, v);
		return;
	}

	YieldMockNetwork() : ticks(0), nextYield(0) {
		baseNetwork = g_network;
		g_network = this;
	}
	~YieldMockNetwork() { g_network = baseNetwork; }

	void tick() {
		ticks++;
		Promise<Void> t;
		t.swap(nextTick);
		t.send(Void());
	}

	Future<class Void> delay(double seconds, TaskPriority taskID) override { return nextTick.getFuture(); }

	Future<class Void> orderedDelay(double seconds, TaskPriority taskID) override { return nextTick.getFuture(); }

	Future<class Void> yield(TaskPriority taskID) override {
		if (check_yield(taskID))
			return delay(0, taskID);
		return Void();
	}

	bool check_yield(TaskPriority taskID) override {
		if (nextYield > 0)
			--nextYield;
		return nextYield == 0;
	}

	// Delegate everything else.  TODO: Make a base class NetworkWrapper for delegating everything in INetwork
	TaskPriority getCurrentTask() const override { return baseNetwork->getCurrentTask(); }
	void setCurrentTask(TaskPriority taskID) override { baseNetwork->setCurrentTask(taskID); }
	double now() const override { return baseNetwork->now(); }
	double timer() override { return baseNetwork->timer(); }
	double timer_monotonic() override { return baseNetwork->timer_monotonic(); }
	void stop() override { return baseNetwork->stop(); }
	void addStopCallback(std::function<void()> fn) override {
		ASSERT(false);
		return;
	}
	bool isSimulated() const override { return baseNetwork->isSimulated(); }
	void onMainThread(Promise<Void>&& signal, TaskPriority taskID) override {
		return baseNetwork->onMainThread(std::move(signal), taskID);
	}
	bool isOnMainThread() const override { return baseNetwork->isOnMainThread(); }
	THREAD_HANDLE startThread(THREAD_FUNC_RETURN (*func)(void*), void* arg, int stackSize, const char* name) override {
		return baseNetwork->startThread(func, arg, stackSize, name);
	}
	Future<Reference<class IAsyncFile>> open(std::string filename, int64_t flags, int64_t mode) {
		return IAsyncFileSystem::filesystem()->open(filename, flags, mode);
	}
	Future<Void> deleteFile(std::string filename, bool mustBeDurable) {
		return IAsyncFileSystem::filesystem()->deleteFile(filename, mustBeDurable);
	}
	void run() override { return baseNetwork->run(); }
	bool checkRunnable() override { return baseNetwork->checkRunnable(); }
	void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) override {
		return baseNetwork->getDiskBytes(directory, free, total);
	}
	bool isAddressOnThisHost(NetworkAddress const& addr) const override {
		return baseNetwork->isAddressOnThisHost(addr);
	}
	const TLSConfig& getTLSConfig() const override {
		static TLSConfig emptyConfig;
		return emptyConfig;
	}
#ifdef ENABLE_SAMPLING
	ActorLineageSet& getActorLineageSet() override { throw std::exception(); }
#endif
	ProtocolVersion protocolVersion() const override { return baseNetwork->protocolVersion(); }
	void _swiftEnqueue(void* task) override { baseNetwork->_swiftEnqueue(task); }
};

Future<Void> testCancelled(bool* exits, Future<Void> f) {
	Error err = success();
	try {
		co_await Future<Void>(Never());
	} catch (Error& e) {
		err = e;
	}
	try {
		co_await Future<Void>(Never());
	} catch (Error& e) {
		*exits = true;
		throw;
	}
	if (err.code() != error_code_success) {
		throw err;
	}
}
} // namespace

TEST_CASE("/flow/coro/cancel1") {
	bool exits = false;
	Promise<Void> p;
	Future<Void> test = testCancelled(&exits, p.getFuture());
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 1);
	test.cancel();
	ASSERT(exits);
	ASSERT(test.getPromiseReferenceCount() == 0 && test.getFutureReferenceCount() == 1 && test.isReady() &&
	       test.isError() && test.getError().code() == error_code_actor_cancelled);
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 0);

	return Void();
}

namespace {

Future<Void> noteCancel(int* cancelled) {
	*cancelled = 0;
	try {
		co_await Future<Void>(Never());
		throw internal_error();
	} catch (...) {
		printf("Cancelled!\n");
		*cancelled = 1;
		throw;
	}
}

} // namespace

TEST_CASE("/flow/coro/cancel2") {
	int c1 = 0, c2 = 0, c3 = 0;

	Future<Void> cf = noteCancel(&c1);
	ASSERT(c1 == 0);
	cf = Future<Void>();
	ASSERT(c1 == 1);

	cf = noteCancel(&c2) && noteCancel(&c3);
	ASSERT(c2 == 0 && c3 == 0);
	cf = Future<Void>();
	ASSERT(c2 == 1 && c3 == 1);
	return Void();
}

TEST_CASE("/flow/coro/trivial_actors") {
	ASSERT(expectActorCount(0));

	int result = 0;
	trivialVoidActor(&result);
	ASSERT(result == 1);

	Future<int> f = return42Actor();
	ASSERT(f.isReady() && !f.isError() && f.get() == 42 && f.getFutureReferenceCount() == 1 &&
	       f.getPromiseReferenceCount() == 0);
	f = Future<int>();

	f = templateActor(24);
	ASSERT(f.isReady() && !f.isError() && f.get() == 24 && f.getFutureReferenceCount() == 1 &&
	       f.getPromiseReferenceCount() == 0);
	f = Future<int>();

	result = 0;
	voidWaitActor(Uncancellable(), 2, &result);
	ASSERT(result == 2 && expectActorCount(0));

	Promise<int> p;
	f = addOneActor(p.getFuture());
	ASSERT(!f.isReady() && expectActorCount(1));
	p.send(100);
	ASSERT(f.isReady() && f.get() == 101);
	f = Future<int>();

	PromiseStream<int> ps;
	f = consumeOneActor(ps.getFuture());
	ASSERT(!f.isReady() && expectActorCount(1));
	ps.send(101);
	ASSERT(f.get() == 101 && ps.isEmpty());
	ps.send(102);
	ASSERT(!ps.isEmpty());
	f = consumeOneActor(ps.getFuture());
	ASSERT(f.get() == 102 && ps.isEmpty());

	f = sumActor(ps.getFuture());
	ps.send(1);
	ps.send(10);
	ps.send(100);
	ps.sendError(end_of_stream());
	ASSERT(f.get() == 111);

	return Void();
}

TEST_CASE("/flow/coro/yieldedFuture/progress") {
	// Check that if check_yield always returns true, the yieldedFuture will do nothing immediately but will
	// get one thing done per "tick" (per delay(0) returning).

	auto yn = makeReference<YieldMockNetwork>();

	yn->nextYield = 0;

	Promise<Void> p;
	Future<Void> u = p.getFuture();
	Future<Void> i = success(u);

	std::vector<Future<Void>> v;
	for (int j = 0; j < 5; j++)
		v.push_back(yieldedFuture(u));
	auto numReady = [&v]() { return std::count_if(v.begin(), v.end(), [](Future<Void> v) { return v.isReady(); }); };

	ASSERT(numReady() == 0);
	p.send(Void());
	ASSERT(u.isReady() && i.isReady() && numReady() == 0);

	for (int j = 0; j < 5; j++) {
		yn->tick();
		ASSERT(numReady() == j + 1);
	}

	for (int j = 0; j < 5; j++) {
		ASSERT(v[j].getPromiseReferenceCount() == 0 && v[j].getFutureReferenceCount() == 1);
	}

	return Void();
}

TEST_CASE("/flow/coro/yieldedFuture/random") {
	// Check expectations about exactly how yieldedFuture responds to check_yield results

	auto yn = makeReference<YieldMockNetwork>();

	for (int r = 0; r < 100; r++) {
		Promise<Void> p;
		Future<Void> u = p.getFuture();
		Future<Void> i = success(u);

		std::vector<Future<Void>> v;
		for (int j = 0; j < 25; j++)
			v.push_back(yieldedFuture(u));
		auto numReady = [&v]() {
			return std::count_if(v.begin(), v.end(), [](Future<Void> v) { return v.isReady(); });
		};

		Future<Void> j = success(u);

		ASSERT(numReady() == 0);

		int expectYield = deterministicRandom()->randomInt(0, 4);
		int expectReady = expectYield;
		yn->nextYield = 1 + expectYield;

		p.send(Void());
		ASSERT(u.isReady() && i.isReady() && j.isReady() && numReady() == expectReady);

		while (numReady() != v.size()) {
			expectYield = deterministicRandom()->randomInt(0, 4);
			yn->nextYield = 1 + expectYield;
			expectReady += 1 + expectYield;
			yn->tick();
			// printf("Yielding %d times, expect %d/%d ready, got %d\n", expectYield, expectReady, v.size(), numReady()
			// );
			ASSERT(numReady() == std::min<int>(expectReady, v.size()));
		}

		for (int k = 0; k < v.size(); k++) {
			ASSERT(v[k].getPromiseReferenceCount() == 0 && v[k].getFutureReferenceCount() == 1);
		}
	}

	return Void();
}

TEST_CASE("/flow/coro/perf/yieldedFuture") {
	double start;
	int N = 1000000;

	auto yn = makeReference<YieldMockNetwork>();

	yn->nextYield = 2 * N + 100;

	Promise<Void> p;
	Future<Void> f = p.getFuture();
	std::vector<Future<Void>> ys;

	start = timer();
	for (int i = 0; i < N; i++)
		ys.push_back(yieldedFuture(f));
	printf("yieldedFuture(f) create: %0.1f M/sec\n", N / 1e6 / (timer() - start));
	p.send(Void());
	printf("yieldedFuture(f) total: %0.1f M/sec\n", N / 1e6 / (timer() - start));

	for (auto& y : ys)
		ASSERT(y.isReady());

	p = Promise<Void>();
	f = p.getFuture();

	start = timer();
	for (int i = 0; i < N; i++) {
		// We're only measuring how long it takes to cancel things, so no need to run any of them
		(void)yieldedFuture(f);
	}
	printf("yieldedFuture(f) cancel: %0.1f M/sec\n", N / 1e6 / (timer() - start));

	return Void();
}

TEST_CASE("/flow/coro/chooseTwoActor") {
	ASSERT(expectActorCount(0));

	Promise<Void> a, b;
	Future<Void> c = chooseTwoActor(a.getFuture(), b.getFuture());
	ASSERT(a.getFutureReferenceCount() == 2 && b.getFutureReferenceCount() == 2 && !c.isReady());
	b.send(Void());
	ASSERT(a.getFutureReferenceCount() == 0 && b.getFutureReferenceCount() == 0 && c.isReady() && !c.isError() &&
	       expectActorCount(1));
	c = Future<Void>();
	ASSERT(a.getFutureReferenceCount() == 0 && b.getFutureReferenceCount() == 0 && expectActorCount(0));
	return Void();
}

TEST_CASE("#flow/coro/perf/actor patterns") {
	double start;
	int N = 1000000;

	ASSERT(expectActorCount(0));

	start = timer();
	for (int i = 0; i < N; i++)
		emptyVoidActor();
	printf("emptyVoidActor(): %0.1f M/sec\n", N / 1e6 / (timer() - start));

	ASSERT(expectActorCount(0));

	start = timer();
	for (int i = 0; i < N; i++) {
		emptyActor();
	}
	printf("emptyActor(): %0.1f M/sec\n", N / 1e6 / (timer() - start));

	ASSERT(expectActorCount(0));

	Promise<Void> neverSet;
	Future<Void> never = neverSet.getFuture();
	Future<Void> already = Void();

	start = timer();
	for (int i = 0; i < N; i++)
		oneWaitVoidActor(Uncancellable(), already);
	printf("oneWaitVoidActor(already): %0.1f M/sec\n", N / 1e6 / (timer() - start));

	ASSERT(expectActorCount(0));

	/*start = timer();
	for (int i = 0; i < N; i++)
	    oneWaitVoidActor(never);
	printf("oneWaitVoidActor(never): %0.1f M/sec\n", N / 1e6 / (timer() - start));*/

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = oneWaitActor(already);
			ASSERT(f.isReady());
		}
		printf("oneWaitActor(already): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = oneWaitActor(never);
			ASSERT(!f.isReady());
		}
		printf("(cancelled) oneWaitActor(never): %0.1f M/sec\n", N / 1e6 / (timer() - start));
		ASSERT(expectActorCount(0));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Promise<Void> p;
			Future<Void> f = oneWaitActor(p.getFuture());
			p.send(Void());
			ASSERT(f.isReady());
		}
		printf("oneWaitActor(after): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			out[i] = oneWaitActor(pipe[i].getFuture());
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out[i].isReady());
		}
		printf("oneWaitActor(fifo): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			out[i] = oneWaitActor(pipe[i].getFuture());
		}
		for (int i = N - 1; i >= 0; i--) {
			pipe[i].send(Void());
			ASSERT(out[i].isReady());
		}
		printf("oneWaitActor(lifo): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = chooseTwoActor(already, already);
			ASSERT(f.isReady());
		}
		printf("chooseTwoActor(already, already): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = chooseTwoActor(already, never);
			ASSERT(f.isReady());
		}
		printf("chooseTwoActor(already, never): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = chooseTwoActor(never, already);
			ASSERT(f.isReady());
		}
		printf("chooseTwoActor(never, already): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = chooseTwoActor(never, never);
			ASSERT(!f.isReady());
		}
		// ASSERT(expectActorCount(0));
		printf("(cancelled) chooseTwoActor(never, never): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Promise<Void> p;
			Future<Void> f = chooseTwoActor(p.getFuture(), never);
			p.send(Void());
			ASSERT(f.isReady());
		}
		printf("chooseTwoActor(after, never): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			out[i] = chooseTwoActor(pipe[i].getFuture(), never);
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out[i].isReady());
		}
		printf("chooseTwoActor(fifo, never): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			out[i] = chooseTwoActor(pipe[i].getFuture(), pipe[i].getFuture());
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out[i].isReady());
		}
		printf("chooseTwoActor(fifo, fifo): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			out[i] = chooseTwoActor(chooseTwoActor(pipe[i].getFuture(), never), never);
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out[i].isReady());
		}
		printf("chooseTwoActor^2((fifo, never), never): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Promise<Void> p;
			Future<Void> f = oneWaitActor(chooseTwoActor(p.getFuture(), never));
			p.send(Void());
			ASSERT(f.isReady());
		}
		printf("oneWaitActor(chooseTwoActor(after, never)): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			out[i] = oneWaitActor(chooseTwoActor(pipe[i].getFuture(), never));
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out[i].isReady());
		}
		printf("oneWaitActor(chooseTwoActor(fifo, never)): %0.1f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		for (int i = 0; i < N; i++) {
			Promise<Void> p;
			Future<Void> f = chooseTwoActor(p.getFuture(), never);
			Future<Void> a = oneWaitActor(f);
			Future<Void> b = oneWaitActor(f);
			p.send(Void());
			ASSERT(f.isReady());
		}
		printf("2xoneWaitActor(chooseTwoActor(after, never)): %0.2f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out1(N);
		std::vector<Future<Void>> out2(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = chooseTwoActor(pipe[i].getFuture(), never);
			out1[i] = oneWaitActor(f);
			out2[i] = oneWaitActor(f);
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out2[i].isReady());
		}
		printf("2xoneWaitActor(chooseTwoActor(fifo, never)): %0.2f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out1(N);
		std::vector<Future<Void>> out2(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			Future<Void> f = chooseTwoActor(oneWaitActor(pipe[i].getFuture()), never);
			out1[i] = oneWaitActor(f);
			out2[i] = oneWaitActor(f);
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out2[i].isReady());
		}
		printf("2xoneWaitActor(chooseTwoActor(oneWaitActor(fifo), never)): %0.2f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		std::vector<Promise<Void>> pipe(N);
		std::vector<Future<Void>> out1(N);
		std::vector<Future<Void>> out2(N);
		start = timer();
		for (int i = 0; i < N; i++) {
			g_cheese = pipe[i].getFuture();
			Future<Void> f = chooseTwoActor(cheeseWaitActor(), never);
			g_cheese = f;
			out1[i] = cheeseWaitActor();
			out2[i] = cheeseWaitActor();
		}
		for (int i = 0; i < N; i++) {
			pipe[i].send(Void());
			ASSERT(out2[i].isReady());
		}
		printf("2xcheeseActor(chooseTwoActor(cheeseActor(fifo), never)): %0.2f M/sec\n", N / 1e6 / (timer() - start));
		// printf("sizeof(CheeseWaitActorActor) == %zu\n", cheeseWaitActorSize());
	}

	{
		PromiseStream<int> data;
		start = timer();
		Future<int> sum = sumActor(data.getFuture());
		for (int i = 0; i < N; i++)
			data.send(1);
		data.sendError(end_of_stream());
		ASSERT(sum.get() == N);
		printf("sumActor: %0.2f M/sec\n", N / 1e6 / (timer() - start));
	}

	{
		start = timer();
		std::vector<Promise<Void>> ps(3);
		std::vector<Future<Void>> fs(3);

		for (int i = 0; i < N; i++) {
			ps.clear();
			ps.resize(3);
			for (int j = 0; j < ps.size(); j++)
				fs[j] = ps[j].getFuture();

			Future<Void> q = quorum(fs, 2);
			for (auto& p : ps)
				p.send(Void());
		}
		printf("quorum(2/3): %0.2f M/sec\n", N / 1e6 / (timer() - start));
	}

	return Void();
}

namespace {

template <class YAM>
struct YAMRandom {
	YAM yam;
	std::vector<Future<Void>> onchanges;
	int kmax;

	YAMRandom() : kmax(3) {}

	void randomOp() {
		if (deterministicRandom()->random01() < 0.01)
			while (!check_yield())
				;

		int k = deterministicRandom()->randomInt(0, kmax);
		int op = deterministicRandom()->randomInt(0, 7);
		// printf("%d",op);
		if (op == 0) {
			onchanges.push_back(yam.onChange(k));
		} else if (op == 1) {
			onchanges.push_back(trigger([this]() { this->randomOp(); }, yam.onChange(k)));
		} else if (op == 2) {
			if (onchanges.size()) {
				int i = deterministicRandom()->randomInt(0, onchanges.size());
				onchanges[i] = onchanges.back();
				onchanges.pop_back();
			}
		} else if (op == 3) {
			onchanges.clear();
		} else if (op == 4) {
			int v = deterministicRandom()->randomInt(0, 3);
			yam.set(k, v);
		} else if (op == 5) {
			yam.trigger(k);
		} else if (op == 6) {
			int a = deterministicRandom()->randomInt(0, kmax);
			int b = deterministicRandom()->randomInt(0, kmax);
			yam.triggerRange(std::min(a, b), std::max(a, b) + 1);
		}
	}
};

} // namespace

TEST_CASE("/flow/coro/long_loop") {
	uint64_t res = 0;
	uint64_t n = 100'000;
	for (decltype(n) i = 0; i < n; ++i) {
		if (i < 99'999) {
			// prevent compiler optimizations by doing something "useful" in the loop
			res += i + 1;
			continue;
		}
		co_await delay(0.1);
	}
	ASSERT(res == n * (n - 1) / 2);
}

TEST_CASE("/flow/coro/YieldedAsyncMap/randomized") {
	YAMRandom<YieldedAsyncMap<int, int>> yamr;
	int it;
	for (it = 0; it < 100000; it++) {
		yamr.randomOp();
		co_await yield();
		if (it % 100 == 0) {
			fmt::print("/flow/coro/YieldedAsyncMap/randomized iteration {}\n", it);
		}
	}
}

TEST_CASE("/flow/coro/AsyncMap/randomized") {
	YAMRandom<AsyncMap<int, int>> yamr;
	for (int it = 0; it < 100000; it++) {
		yamr.randomOp();
		co_await yield();
	}
}

TEST_CASE("/flow/coro/YieldedAsyncMap/basic") {
	YieldedAsyncMap<int, int> yam;
	Future<Void> y0 = yam.onChange(1);
	yam.setUnconditional(1, 0);
	Future<Void> y1 = yam.onChange(1);
	Future<Void> y1a = yam.onChange(1);
	Future<Void> y1b = yam.onChange(1);
	yam.set(1, 1);
	// while (!check_yield()) {}
	// yam.triggerRange(0, 4);

	Future<Void> y2 = yam.onChange(1);
	co_await reportErrors(y0, "Y0");
	co_await reportErrors(y1, "Y1");
	co_await reportErrors(y1a, "Y1a");
	co_await reportErrors(y1b, "Y1b");
	co_await reportErrors(timeout(y2, 5, Void()), "Y2");
}

TEST_CASE("/flow/coro/YieldedAsyncMap/cancel") {
	YieldedAsyncMap<int, int> yam;
	// ASSERT(yam.count(1) == 0);
	// state Future<Void> y0 = yam.onChange(1);
	// ASSERT(yam.count(1) == 1);
	// yam.setUnconditional(1, 0);

	ASSERT(yam.count(1) == 0);
	Future<Void> y1 = yam.onChange(1);
	Future<Void> y1a = yam.onChange(1);
	Future<Void> y1b = yam.onChange(1);
	ASSERT(yam.count(1) == 1);
	y1.cancel();
	ASSERT(!y1a.isReady());
	y1a.cancel();
	ASSERT(!y1b.isReady());
	ASSERT(yam.count(1) == 1);
	y1b.cancel();
	ASSERT(y1b.getError().code() == error_code_actor_cancelled);
	ASSERT(yam.count(1) == 0);

	return Void();
}

TEST_CASE("/flow/coro/YieldedAsyncMap/cancel2") {
	YieldedAsyncMap<int, int> yam;

	Future<Void> y1 = yam.onChange(1);
	Future<Void> y2 = yam.onChange(2);

	auto* pyam = &yam;
	uncancellable(trigger(
	    [pyam]() {
		    printf("Triggered\n");
		    pyam->triggerAll();
	    },
	    delay(1)));

	co_await y1;
	printf("Got y1\n");
	y2.cancel();
}

TEST_CASE("/flow/coro/AsyncVar/basic") {
	AsyncVar<int> av;
	Future<Void> ch = av.onChange();
	ASSERT(!ch.isReady());
	av.set(5);
	ASSERT(ch.isReady());
	ASSERT(av.get() == 5);

	ch = av.onChange();
	ASSERT(!ch.isReady());
	av.set(6);
	ASSERT(ch.isReady());
	ASSERT(av.get() == 6);

	return Void();
}

namespace {

Future<Void> waitAfterCancel(int* output) {
	*output = 0;
	try {
		co_await Future<Void>(Never());
	} catch (...) {
	}
	co_await ((*output = 1, Future<Void>(Void())));
	ASSERT(false);
}

} // namespace

TEST_CASE("/fdbrpc/flow/wait_expression_after_cancel") {
	int a = -1;
	Future<Void> f = waitAfterCancel(&a);
	ASSERT(a == 0);
	f.cancel();
	ASSERT(a == 1);
	return Void();
}

// Tests for https://github.com/apple/foundationdb/issues/1226

namespace {

template <class>
struct ShouldNotGoIntoClassContextStack;

class Foo1 {
public:
	explicit Foo1(int x) : x(x) {}
	Future<int> foo();

private:
	int x;
};

Future<int> Foo1::foo() {
	co_await Future<Void>();
	co_return x;
}

class [[nodiscard]] Foo2 {
public:
	explicit Foo2(int x) : x(x) {}
	Future<int> foo();

private:
	int x;
};

Future<int> Foo2::foo() {
	co_await Future<Void>();
	co_return x;
}

class alignas(4) Foo3 {
public:
	explicit Foo3(int x) : x(x) {}
	Future<int> foo();

private:
	int x;
};

Future<int> Foo3::foo() {
	co_await Future<Void>();
	co_return x;
}

struct Super {};

class Foo4 : Super {
public:
	explicit Foo4(int x) : x(x) {}
	Future<int> foo();

private:
	int x;
};

Future<int> Foo4::foo() {
	co_await Future<Void>();
	co_return x;
}

struct Outer {
	class Foo5 : Super {
	public:
		explicit Foo5(int x) : x(x) {}
		Future<int> foo();

	private:
		int x;
	};
};

Future<int> Outer::Foo5::foo() {
	co_await Future<Void>();
	co_return x;
}

struct Tracker {
	int copied;
	bool moved;
	Tracker(int copied = 0) : copied(copied), moved(false) {}
	Tracker(Tracker&& other) : Tracker(other.copied) {
		ASSERT(!other.moved);
		other.moved = true;
	}
	Tracker& operator=(Tracker&& other) {
		ASSERT(!other.moved);
		other.moved = true;
		this->moved = false;
		this->copied = other.copied;
		return *this;
	}
	Tracker(const Tracker& other) : Tracker(other.copied + 1) { ASSERT(!other.moved); }
	Tracker& operator=(const Tracker& other) {
		ASSERT(!other.moved);
		this->moved = false;
		this->copied = other.copied + 1;
		return *this;
	}
	~Tracker() = default;

	static Future<Void> listen(FutureStream<Tracker> stream) {
		Tracker movedTracker = co_await stream;
		ASSERT(!movedTracker.moved);
		ASSERT(movedTracker.copied == 0);
	}
};

} // namespace

TEST_CASE("/flow/coro/PromiseStream/move") {
	PromiseStream<Tracker> stream;
	Future<Void> listener;
	{
		// This tests the case when a callback is added before
		// a movable value is sent
		listener = Tracker::listen(stream.getFuture());
		stream.send(Tracker{});
		co_await listener;
	}

	{
		// This tests the case when a callback is added before
		// a unmovable value is sent
		listener = Tracker::listen(stream.getFuture());
		Tracker namedTracker;
		stream.send(std::move(namedTracker));
		co_await listener;
	}
	{
		// This tests the case when no callback is added until
		// after a movable value is sent
		stream.send(Tracker{});
		stream.send(Tracker{});
		{
			const Tracker& movedTracker = co_await stream.getFuture();
			ASSERT(!movedTracker.moved);
			ASSERT(movedTracker.copied == 0);
		}
		{
			const Tracker& movedTracker = co_await stream.getFuture();
			ASSERT(!movedTracker.moved);
			ASSERT(movedTracker.copied == 0);
		}
	}
	{
		// This tests the case when no callback is added until
		// after an unmovable value is sent
		Tracker namedTracker1;
		Tracker namedTracker2;
		stream.send(namedTracker1);
		stream.send(namedTracker2);
		{
			const Tracker& copiedTracker = co_await stream.getFuture();
			ASSERT(!copiedTracker.moved);
			// must copy onto queue
			ASSERT(copiedTracker.copied == 1);
		}
		{
			const Tracker& copiedTracker = co_await stream.getFuture();
			ASSERT(!copiedTracker.moved);
			// must copy onto queue
			ASSERT(copiedTracker.copied == 1);
		}
	}
}

TEST_CASE("/flow/coro/PromiseStream/move2") {
	PromiseStream<Tracker> stream;
	stream.send(Tracker{});
	const Tracker& tracker = co_await stream.getFuture();
	Tracker movedTracker = std::move(const_cast<Tracker&>(tracker));
	ASSERT(tracker.moved);
	ASSERT(!movedTracker.moved);
	ASSERT(movedTracker.copied == 0);
}

namespace {

constexpr double mutexTestDelay = 0.00001;

Future<Void> mutexTest(int id, FlowMutex* mutex, int n, bool allowError, bool* verbose) {
	while (n-- > 0) {
		double d = deterministicRandom()->random01() * mutexTestDelay;
		if (*verbose) {
			printf("%d:%d wait %f while unlocked\n", id, n, d);
		}
		co_await delay(d);

		if (*verbose) {
			printf("%d:%d locking\n", id, n);
		}
		FlowMutex::Lock lock = co_await mutex->take();
		if (*verbose) {
			printf("%d:%d locked\n", id, n);
		}

		d = deterministicRandom()->random01() * mutexTestDelay;
		if (*verbose) {
			printf("%d:%d wait %f while locked\n", id, n, d);
		}
		co_await delay(d);

		// On the last iteration, send an error or drop the lock if allowError is true
		if (n == 0 && allowError) {
			if (deterministicRandom()->coinflip()) {
				// Send explicit error
				if (*verbose) {
					printf("%d:%d sending error\n", id, n);
				}
				lock.error(end_of_stream());
			} else {
				// Do nothing
				if (*verbose) {
					printf("%d:%d dropping promise, returning without unlock\n", id, n);
				}
			}
		} else {
			if (*verbose) {
				printf("%d:%d unlocking\n", id, n);
			}
			lock.release();
		}
	}

	if (*verbose) {
		printf("%d Returning\n", id);
	}
}

} // namespace

TEST_CASE("/flow/coro/FlowMutex") {
	int count = 100000;

	// Default verboseness
	bool verboseSetting = false;
	// Useful for debugging, enable verbose mode for this iteration number
	int verboseTestIteration = -1;

	try {
		bool verbose = verboseSetting || count == verboseTestIteration;

		while (--count > 0) {
			if (count % 1000 == 0) {
				printf("%d tests left\n", count);
			}

			FlowMutex mutex;
			std::vector<Future<Void>> tests;

			bool allowErrors = deterministicRandom()->coinflip();
			if (verbose) {
				printf("\nTesting allowErrors=%d\n", allowErrors);
			}

			Optional<Error> error;

			try {
				for (int i = 0; i < 10; ++i) {
					tests.push_back(mutexTest(i, &mutex, 10, allowErrors, &verbose));
				}
				co_await waitForAll(tests);

				if (allowErrors) {
					if (verbose) {
						printf("Final wait in case error was injected by the last actor to finish\n");
					}
					co_await mutex.take();
				}
			} catch (Error& e) {
				if (verbose) {
					printf("Caught error %s\n", e.what());
				}
				error = e;
			}
			if (error.present()) {
				// Some actors can still be running, waiting while locked or unlocked,
				// but all should become ready, some with errors.
				if (verbose) {
					printf("Waiting for completions.  Future end states:\n");
				}
				for (int i = 0; i < tests.size(); ++i) {
					ErrorOr<Void> f = co_await errorOr(tests[i]);
					if (verbose) {
						printf("  %d: %s\n", i, f.isError() ? f.getError().what() : "done");
					}
				}
			}

			// If an error was caused, one should have been detected.
			// Otherwise, no errors should be detected.
			ASSERT(error.present() == allowErrors);
		}
	} catch (Error& e) {
		printf("Error at count=%d\n", count + 1);
		ASSERT(false);
	}
}

namespace {

struct LifetimeLogger {
	LifetimeLogger(std::ostream& ss, int id) : ss(ss), id(id) {
		ss << "LifetimeLogger(" << id << "). ";
		std::cout << "LifetimeLogger(" << id << ").\n";
	}
	~LifetimeLogger() {
		ss << "~LifetimeLogger(" << id << "). ";
		std::cout << "~LifetimeLogger(" << id << ").\n";
	}

	std::ostream& ss;
	int id;
};

template <typename T>
Future<Void> simple_await_test(std::stringstream& ss, Future<T> f) {
	ss << "start. ";
	LifetimeLogger ll(ss, 0);

	co_await (f);
	ss << "wait returned. ";

	LifetimeLogger ll2(ss, 1);
	co_return;
	ss << "after co_return. ";
}

Future<Void> actor_cancel_test(std::stringstream& ss) {
	ss << "start. ";

	LifetimeLogger ll(ss, 0);

	try {
		co_await delay(100);
	} catch (Error& e) {
		ss << "error: " << e.what() << ". ";
	}

	std::string foo = "foo";

	co_await delay(1.0);

	ss << "wait returned. ";

	// foo is alive here!

	co_return;
	ss << "after co_return. ";
}

Future<Void> actor_throw_test(std::stringstream& ss) {
	ss << "start. ";

	LifetimeLogger ll(ss, 0);

	co_await delay(0);

	throw io_error();

	ss << "after throw. ";
	co_return;
	ss << "after co_return. ";
}

Future<Void> simpleWaitTestCoro() {
	std::cout << "simple_wait_test coro\n";
	std::cout << "=====================\n";
	std::stringstream ss;
	try {
		fmt::print("before wait\n");
		co_await simple_await_test(ss, Future<int>(io_error()));
	} catch (Error& e) {
		fmt::print("error\n");
		ss << "error: " << e.what() << ". ";
	}
	std::cout << ss.str() << std::endl << std::endl;
};

template <class T, class U>
Future<Void> tagAndForwardCoro(Uncancellable, Promise<T>* pOutputPromise, U value, Future<Void> signal) {
	Promise<T> out(std::move(*pOutputPromise));
	co_await signal;
	out.send(std::move(value));
}

// this is just there to make sure the stack is properly cleaned up during this test. However, in real code
// this indirection wouldn't be necessary
template <class T, class U>
void tagAndForwardWrapper(Promise<T>* pOutputPromise, U value, Future<Void> signal) {
	tagAndForwardCoro(Uncancellable(), pOutputPromise, std::move(value), std::move(signal));
}

Future<Void> testUncancellable() {
	Promise<int> p;
	Future<int> fut = p.getFuture();
	tagAndForwardWrapper(&p, 2, delay(0.2));
	ASSERT(co_await fut == 2);
}

Future<int> delayAndReturn(Uncancellable) {
	co_await delay(1.0);
	co_return 5;
}

Future<Void> testUncancellable2() {
	try {
		auto f = delayAndReturn(Uncancellable());
		co_await delay(0.1);
		f.cancel();
		ASSERT(co_await f == 5);
	} catch (Error& e) {
		ASSERT(e.code() != error_code_actor_cancelled);
	}
}

Future<int> sendToRandomPromise(Promise<Void> voidPromise,
                                Promise<int> intPromise,
                                Promise<double> doublePromise,
                                PromiseStream<Void> voidStream,
                                PromiseStream<int> intStream) {
	co_await delay(0.1);
	int branch = deterministicRandom()->randomInt(0, 5);
	switch (branch) {
	case 0:
		voidPromise.send(Void());
		break;
	case 1:
		intPromise.send(7);
		break;
	case 2:
		doublePromise.send(8.0);
		break;
	case 3:
		voidStream.send(Void());
		break;
	case 4:
		intStream.send(13);
		break;
	default:
		ASSERT(false);
	}
	co_return branch;
}

Future<Void> testChooseWhen() {
	co_await Choose()
	    .When(delay(1), [](Void const&) { ASSERT(false); })
	    .When(Future<Void>(Void()), [](Void const&) { /*success*/ })
	    .When(
	        []() {
		        ASSERT(false);
		        return Future<Void>(Void());
	        },
	        [](Void const&) { ASSERT(false); })
	    .run();
	try {
		co_await Choose()
		    .When(Future<Void>(Never()), [](Void const&) { ASSERT(false); })
		    .When([]() -> Future<Void> { throw io_error(); }, [](Void const&) { ASSERT(false); })
		    .run();
	} catch (Error const& e) {
		ASSERT(e.code() == error_code_io_error);
	}
	Promise<Void> voidPromise;
	Promise<int> intPromise;
	Promise<double> doublePromise;
	PromiseStream<Void> voidStream;
	PromiseStream<int> intStream;
	int chosenBranch = -1;
	Future<int> chooseAfter = sendToRandomPromise(voidPromise, intPromise, doublePromise, voidStream, intStream);
	co_await Choose()
	    .When(voidPromise.getFuture(),
	          [&chosenBranch](Void const&) {
		          fmt::print("Chose Branch {}\n", 0);
		          chosenBranch = 0;
	          })
	    .When(intPromise.getFuture(),
	          [&chosenBranch](int const& i) {
		          fmt::print("Chose Branch {}\n", 1);
		          chosenBranch = 1;
		          ASSERT(i == 7);
	          })
	    .When(doublePromise.getFuture(),
	          [&chosenBranch](double const& d) {
		          fmt::print("Chose Branch {}\n", 2);
		          chosenBranch = 2;
		          ASSERT(d == 8.0);
	          })
	    .When(voidStream.getFuture(),
	          [&chosenBranch](Void const&) {
		          fmt::print("Chose Branch {}\n", 3);
		          chosenBranch = 3;
	          })
	    .When(intStream.getFuture(),
	          [&chosenBranch](int const& i) {
		          fmt::print("Chose Branch {}\n", 4);
		          chosenBranch = 4;
		          ASSERT(i == 13);
	          })
	    .run();
	ASSERT(chosenBranch == co_await chooseAfter);
}

Future<Void> delaySequence(PromiseStream<double> p, std::vector<double>* nums) {
	for (auto n : *nums) {
		co_await delay(n);
		p.send(n);
	}
}

Future<Void> futureStreamTest() {
	std::vector<double> rnds;
	rnds.reserve(100);
	for (int i = 0; i < 100; ++i) {
		rnds.emplace_back(deterministicRandom()->random01() * 0.1);
	}
	PromiseStream<double> promise;
	auto fut = promise.getFuture();
	Future<Void> f = delaySequence(std::move(promise), &rnds);
	int i = 0;
	while (!f.isReady() || fut.isReady()) {
		try {
			ASSERT(co_await fut == rnds[i++]);
		} catch (Error& e) {
			if (e.code() == error_code_broken_promise) {
				break;
			}
			throw;
		}
	}
	ASSERT(i == 100);
}

template <class T>
Future<Void> verifyAddress(T* ptr, intptr_t address) {
	co_await delay(0.1);
	ASSERT(reinterpret_cast<intptr_t>(ptr) == address);
}

Future<Void> stackMemoryTest() {
	int a = deterministicRandom()->randomInt(0, 1000);
	auto aPtr = reinterpret_cast<intptr_t>(&a);
	co_await verifyAddress(&a, aPtr);
	ASSERT(aPtr == reinterpret_cast<intptr_t>(&a));
}

AsyncGenerator<int> simpleGeneratorTest() {
	for (int i = 0; true; ++i) {
		co_await delay(0.01);
		co_yield i;
	}
}

Future<Void> testSimpleGenerator() {
	auto gen = simpleGeneratorTest();
	for (int i = 0; i < 100; ++i) {
		ASSERT(gen);
		ASSERT(co_await gen() == i);
	}
}

AsyncGenerator<StringRef> readBlocks(Reference<IAsyncFile> file, size_t blockSize) {
	auto size = size_t(co_await file->size());
	decltype(size) offset = 0;
	Arena arena;
	auto buffer = new (arena) uint8_t[blockSize];
	while (size - offset > 0) {
		auto read = co_await file->read(buffer, int(std::min(size - offset, blockSize)), offset);
		offset += read;
		co_yield StringRef(buffer, read);
	}
}

AsyncGenerator<StringRef> readLines(Reference<IAsyncFile> file) {
	constexpr size_t blockSize = 4096;
	auto gen = readBlocks(file, blockSize);
	Arena arena;
	StringRef line;
	while (gen) {
		StringRef block;
		try {
			block = co_await gen();
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			throw;
		}
		bool lastIsNewline = char(block[block.size() - 1]) == '\n';
		ASSERT(!block.empty());
		if (!line.empty()) {
			// we have to carry over data from a previous block
			auto firstLine = block.eatAny("\n"_sr, nullptr);
			line = concatenateStrings(arena, line, firstLine);
			if (!block.empty() || lastIsNewline) {
				// we read at least one block
				co_yield line;
				line = StringRef();
				arena = Arena();
			}
		}
		ASSERT(line.empty() || block.empty());
		while (!block.empty()) {
			line = block.eatAny("\n"_sr, nullptr);
			if (!block.empty()) [[likely]] {
				co_yield line;
			} else {
				if (lastIsNewline) {
					co_yield line;
					line = StringRef();
					arena = Arena();
				} else {
					// this line might not be complete, so we need to potentially concatenate it with
					// the beginning of the next block
					line = StringRef(arena, line);
				}
			}
		}
	}
	if (!line.empty()) {
		co_yield line;
	}
}

AsyncGenerator<StringRef> lineGenerator(size_t minLen,
                                        size_t maxLen,
                                        size_t blockSize,
                                        Standalone<VectorRef<StringRef>>* lines) {
	size_t remainingLine = 0;
	bool firstBlock = true;
	bool startedLine = false;
	loop {
		Arena arena;
		auto block = new (arena) uint8_t[blockSize];
		size_t offset = 0;
		while (offset < blockSize) {
			size_t lineLength =
			    remainingLine == 0 ? deterministicRandom()->randomSkewedUInt32(minLen, maxLen) : remainingLine;
			auto toWrite = std::min(blockSize - offset, lineLength);
			remainingLine = lineLength - toWrite;
			for (size_t i = 0; i < toWrite; ++i) {
				block[offset + i] = uint8_t(deterministicRandom()->randomAlphaNumeric());
			}
			StringRef currLine;
			if (remainingLine == 0) {
				block[offset + toWrite - 1] = uint8_t('\n');
				currLine = StringRef(block + offset, int(toWrite - 1));
			} else {
				currLine = StringRef(block + offset, int(toWrite));
			}
			if (startedLine) {
				lines->back() = concatenateStrings(lines->arena(), lines->back(), currLine);
			} else {
				lines->push_back_deep(lines->arena(), currLine);
			}
			startedLine = remainingLine != 0;
			offset += toWrite;
		}
		if (firstBlock) {
			firstBlock = false;
		}
		co_yield StringRef(block, blockSize);
	}
}

Future<Standalone<VectorRef<StringRef>>> writeTestFile(Reference<IAsyncFile> file) {
	size_t length = (1 << 20); // 1MB
	constexpr size_t minLineLength = 10;
	constexpr size_t maxLineLength = 1024;
	constexpr size_t blockSize = 4096;
	Standalone<VectorRef<StringRef>> result;
	auto gen = lineGenerator(minLineLength, maxLineLength, blockSize, &result);
	size_t offset = 0;
	while (offset < length) {
		auto str = co_await gen();
		for (int i = 0; i < str.size(); ++i) {
			if (str[i] == '\0') {
				fmt::print("Attempted to write 0-byte at block-offset {}\n", i);
				ASSERT(false);
			}
		}
		co_await file->write(str.begin(), str.size(), size_t(offset));
		offset += str.size();
	}
	co_await file->flush();
	co_return result;
}

std::string rndFileName() {
	std::string rndString;
	rndString.reserve(16);
	for (int i = 0; i < 16; ++i) {
		rndString.push_back(deterministicRandom()->randomAlphaNumeric());
	}
	return fmt::format("cppcoro_test_readlines_{}_{}.txt", int(now()), rndString);
}

Future<Void> testReadLines() {
	auto filename = rndFileName();
	auto file = co_await IAsyncFileSystem::filesystem()->open(filename,
	                                                          IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
	                                                              IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE,
	                                                          0640);
	auto expectedLines = co_await writeTestFile(file);
	auto lines = readLines(file);
	for (int i = 0; i < expectedLines.size(); ++i) {
		ASSERT(lines);
		auto line = co_await lines();
		if (line != expectedLines[i]) {
			fmt::print("ERROR on line {}:\n", i);
			fmt::print("\t{}\n", line);
			fmt::print("\t!=\n");
			fmt::print("\t{}\n", expectedLines[i]);
			ASSERT(false);
		}
	}
	try {
		auto line = co_await lines();
		fmt::print("produced line after expected last line {}\n", expectedLines.size());
		fmt::print("\t{}\n", line.toString());
		// we should be done here
		ASSERT(false);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
	}
	// we only want to delete the file if the test was successful, so we don't wrap the above in a try-catch
	co_await IAsyncFileSystem::filesystem()->deleteFile(filename, false);
}

AsyncGenerator<int> emptyGenerator() {
	co_return;
}

Future<Void> testEmptyGenerator() {
	auto gen = emptyGenerator();
	while (gen) {
		try {
			fmt::print("Value: {}\n", co_await gen());
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_end_of_stream);
		}
	}
}

template <class T>
Generator<T> walkElements(std::vector<T> elements) {
	for (auto const& element : elements) {
		co_yield element;
	}
}

void testElementWalker() {
	int numElements = deterministicRandom()->randomInt(100, 1000);
	std::vector<int> elements;
	elements.reserve(numElements);
	for (int i = 0; i < numElements; ++i) {
		elements.push_back(deterministicRandom()->randomInt(0, std::numeric_limits<int>::max()));
	}
	auto verificationCopy = elements;
	auto gen = walkElements(std::move(elements));
	for (auto e : verificationCopy) {
		ASSERT(e == *gen);
		++gen;
	}
	try {
		ASSERT(!gen);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
	}
}

Future<Void> simpleCoro() {
	co_await delay(0.1);
}

Future<Void> testSimpleCoro() {
	Future<Void> c = simpleCoro();
	fmt::print("Coro created\n");
	co_await delay(1.0);
	fmt::print("After sleep\n");
	co_await c;
	fmt::print("After await\n");
}

Generator<unsigned> fibonacci() {
	unsigned curr = 1, next = 1;
	loop {
		co_yield curr;
		curr = std::exchange(next, next + curr);
	}
}

std::vector<unsigned> fibDivisible(unsigned n, unsigned by) {
	std::vector<unsigned> res;
	res.reserve(n);
	for (auto i = fibonacci(); res.size() < n; ++i) {
		if (*i % by == 0) {
			res.push_back(*i);
		}
	}
	return res;
}

std::vector<unsigned> fibDivisibleBad(unsigned n, unsigned by) {
	unsigned curr = 1, next = 1;
	std::vector<unsigned> res;
	res.reserve(n);
	while (res.size() < n) {
		if (curr % by == 0) {
			res.push_back(curr);
		}
		curr = std::exchange(next, curr + next);
	}
	return res;
}

void testFibDivisible() {
	{
		auto fibs = fibDivisibleBad(4, 3);
		std::vector<unsigned> expected({ 3, 21, 144, 987 });
		ASSERT(fibs.size() == expected.size());
		for (int i = 0; i < expected.size(); ++i) {
			ASSERT(fibs[i] == expected[i]);
		}
	}
	{
		auto fibs = fibDivisible(4, 3);
		std::vector<unsigned> expected({ 3, 21, 144, 987 });
		ASSERT(fibs.size() == expected.size());
		for (int i = 0; i < expected.size(); ++i) {
			ASSERT(fibs[i] == expected[i]);
		}
	}
}

} // namespace

// TODO: the test is excluded in RandomUnitTests due to failures that happen when run the test concurrently with other
// unit tests.
TEST_CASE("/flow/coro/generators") {
	testFibDivisible();
	co_await testEmptyGenerator();
	co_await testSimpleGenerator();
	co_await testReadLines();
	testElementWalker();
}

TEST_CASE("/flow/coro/actor") {
	co_await testSimpleCoro();
	std::cout << "simple_wait_test\n";
	std::cout << "================\n";
	{
		std::stringstream ss1;
		try {
			co_await simple_await_test(ss1, delay(1));
		} catch (Error& e) {
			ss1 << "error: " << e.what() << ". ";
		}
		std::cout << ss1.str() << std::endl;
		ASSERT(ss1.str() == "start. LifetimeLogger(0). wait returned. LifetimeLogger(1). ~LifetimeLogger(1). "
		                    "~LifetimeLogger(0). ");
	}

	co_await simpleWaitTestCoro();

	std::cout << std::endl;
	std::cout << "simple_wait_test\n";
	std::cout << "================\n";
	{
		std::stringstream ss2;
		try {
			fmt::print("before wait\n");
			co_await simple_await_test(ss2, Future<int>(io_error()));
		} catch (Error& e) {
			fmt::print("error\n");
			ss2 << "error: " << e.what() << ". ";
		}
		std::cout << ss2.str() << std::endl;
		ASSERT(ss2.str() == "start. LifetimeLogger(0). ~LifetimeLogger(0). error: Disk i/o operation failed. ");
	}

	std::cout << std::endl;
	std::cout << "actor_cancel_test\n";
	std::cout << "=================\n";
	{
		std::stringstream ss3;
		{
			Future<Void> f = actor_cancel_test(ss3);
			co_await delay(1);
		}
		std::cout << ss3.str() << std::endl;
		ASSERT(ss3.str() == "start. LifetimeLogger(0). error: Asynchronous operation cancelled. "
		                    "~LifetimeLogger(0). ");
	}

	std::cout << std::endl;
	std::cout << "actor_throw_test\n";
	std::cout << "================\n";
	{
		std::stringstream ss4;
		try {
			co_await actor_throw_test(ss4);
		} catch (Error& e) {
			ss4 << "error: " << e.what() << ". ";
		}
		std::cout << ss4.str() << std::endl;
		ASSERT(ss4.str() == "start. LifetimeLogger(0). ~LifetimeLogger(0). error: Disk i/o operation failed. ");
	}

	std::cout << std::endl;
	co_await delay(0.1);
	co_await testChooseWhen();
	co_await testUncancellable();
	co_await testUncancellable2();
	co_await futureStreamTest();
	co_await stackMemoryTest();
}

TEST_CASE("/flow/coro/chooseCancelWaiting") {
	Promise<Void> voidPromise;
	Promise<int> intPromise;
	Future<Void> chooseFuture = Choose()
	                                .When(voidPromise.getFuture(), [](const Void&) { ASSERT_ABORT(false); })
	                                .When(intPromise.getFuture(), [](const int&) { ASSERT_ABORT(false); })
	                                .run();
	chooseFuture.cancel();
	ASSERT(chooseFuture.getError().code() == error_code_actor_cancelled);
	voidPromise.send(Void());
	intPromise.sendError(end_of_stream());
	ASSERT(chooseFuture.getError().code() == error_code_actor_cancelled);
	return Void();
}

TEST_CASE("/flow/coro/chooseCancelReady") {
	Promise<int> intPromise;
	int res = 0;
	Future<Void> chooseFuture = Choose().When(intPromise.getFuture(), [&](const int& val) { res = val; }).run();
	intPromise.send(5);
	ASSERT(chooseFuture.isReady());
	ASSERT(res == 5);
	chooseFuture.cancel();
	ASSERT(chooseFuture.isReady());
	return Void();
}

TEST_CASE("/flow/coro/chooseRepeatedCancel") {
	Promise<Void> voidPromise;
	Promise<int> intPromise;
	Future<Void> chooseFuture = Choose()
	                                .When(voidPromise.getFuture(), [](const Void&) { ASSERT_ABORT(false); })
	                                .When(intPromise.getFuture(), [](const int&) { ASSERT_ABORT(false); })
	                                .run();
	chooseFuture.cancel();
	ASSERT(chooseFuture.getError().code() == error_code_actor_cancelled);
	chooseFuture.cancel();
	ASSERT(chooseFuture.getError().code() == error_code_actor_cancelled);
	voidPromise.sendError(end_of_stream());
	intPromise.send(3);
	ASSERT(chooseFuture.getError().code() == error_code_actor_cancelled);
	return Void();
}
