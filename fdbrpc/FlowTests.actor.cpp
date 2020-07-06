/*
 * FlowTests.actor.cpp
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

// Unit tests for the flow language and libraries

#include "flow/UnitTest.h"
#include "flow/DeterministicRandom.h"
#include "flow/IThreadPool.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

void forceLinkFlowTests() {}

using std::vector;

constexpr int firstLine = __LINE__;
TEST_CASE("/flow/actorcompiler/lineNumbers") {
	loop {
		try {
			ASSERT(__LINE__ == firstLine + 4);
			wait(Future<Void>(Void()));
			ASSERT(__LINE__ == firstLine + 6);
			throw success();
		} catch (Error& e) {
			ASSERT(__LINE__ == firstLine + 9);
			wait(Future<Void>(Void()));
			ASSERT(__LINE__ == firstLine + 11);
		}
		break;
	}
	ASSERT(LiteralStringRef(__FILE__).endsWith(LiteralStringRef("FlowTests.actor.cpp")));
	return Void();
}

TEST_CASE("/flow/buggifiedDelay") {
	if (FLOW_KNOBS->MAX_BUGGIFIED_DELAY == 0) {
		return Void();
	}
	loop {
		state double x = deterministicRandom()->random01();
		state int last = 0;
		state Future<Void> f1 = map(delay(x), [last = &last](const Void&) {
			*last = 1;
			return Void();
		});
		state Future<Void> f2 = map(delay(x), [last = &last](const Void&) {
			*last = 2;
			return Void();
		});
		wait(f1 && f2);
		if (last == 1) {
			TEST(true); // Delays can become ready out of order
			return Void();
		}
	}
}

template <class T, class Func, class ErrFunc, class CallbackType>
class LambdaCallback : public CallbackType, public FastAllocated<LambdaCallback<T,Func,ErrFunc,CallbackType>> {
	Func func;
	ErrFunc errFunc;

	virtual void fire(T const& t) { CallbackType::remove(); func(t); delete this; }
	virtual void fire(T && t) { CallbackType::remove(); func(std::move(t)); delete this; }
	virtual void error(Error e) { CallbackType::remove(); errFunc(e); delete this; }

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
	}
	else
		f.addCallbackAndClear(new LambdaCallback<T, Func, ErrFunc, Callback<T>>(std::move(func), std::move(errFunc)));
}

template <class T, class Func, class ErrFunc>
void onReady(FutureStream<T>&& f, Func&& func, ErrFunc&& errFunc) {
	if (f.isReady()) {
		if (f.isError())
			errFunc(f.getError());
		else
			func(f.pop());
	}
	else
		f.addCallbackAndClear(new LambdaCallback<T, Func, ErrFunc, SingleCallback<T>>(std::move(func), std::move(errFunc)));
}

ACTOR static void emptyVoidActor() {
}

ACTOR [[flow_allow_discard]] static Future<Void> emptyActor() {
	return Void();
}

ACTOR static void oneWaitVoidActor(Future<Void> f) {
	wait(f);
}

ACTOR static Future<Void> oneWaitActor(Future<Void> f) {
	wait(f);
	return Void();
}

Future<Void> g_cheese;
ACTOR static Future<Void> cheeseWaitActor() {
	wait(g_cheese);
	return Void();
}

ACTOR static void trivialVoidActor(int* result) {
	*result = 1;
}

ACTOR static Future<int> return42Actor() {
	return 42;
}

ACTOR static void voidWaitActor(Future<int> in, int* result) {
	int i = wait(in);
	*result = i;
}

ACTOR static Future<int> addOneActor(Future<int> in) {
	int i = wait(in);
	return i + 1;
}

ACTOR static Future<Void> chooseTwoActor(Future<Void> f, Future<Void> g) {
	choose{
		when(wait(f)) {}
		when(wait(g)) {}
	}
	return Void();
}

ACTOR static Future<int> consumeOneActor(FutureStream<int> in) {
	int i = waitNext(in);
	return i;
}

ACTOR static Future<int> sumActor(FutureStream<int> in) {
	state int total = 0;
	try {
		loop{
			int i = waitNext(in);
			total += i;
		}
	}
	catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}
	return total;
}

ACTOR template <class T> static Future<T> templateActor(T t) {
	return t;
}

static int destroy() { return 666; }
ACTOR static Future<Void> testHygeine() {
	ASSERT(destroy() == 666);  // Should fail to compile if SAV<Void>::destroy() is visible
	return Void();
}

//bool expectActorCount(int x) { return actorCount == x; }
bool expectActorCount(int) { return true; }

struct YieldMockNetwork : INetwork, ReferenceCounted<YieldMockNetwork> {
	int ticks;
	Promise<Void> nextTick;
	int nextYield;
	INetwork* baseNetwork;

	virtual flowGlobalType global(int id) const override { return baseNetwork->global(id); }
	virtual void setGlobal(size_t id, flowGlobalType v) override {
		baseNetwork->setGlobal(id, v);
		return;
	}

	YieldMockNetwork() : ticks(0), nextYield(0) {
		baseNetwork = g_network;
		g_network = this;
	}
	~YieldMockNetwork() {
		g_network = baseNetwork;
	}

	void tick() {
		ticks++;
		Promise<Void> t;
		t.swap(nextTick);
		t.send(Void());
	}

	virtual Future<class Void> delay(double seconds, TaskPriority taskID) override { return nextTick.getFuture(); }

	virtual Future<class Void> yield(TaskPriority taskID) override {
		if (check_yield(taskID))
			return delay(0,taskID);
		return Void();
	}

	virtual bool check_yield(TaskPriority taskID) override {
		if (nextYield > 0) --nextYield;
		return nextYield == 0;
	}

	// Delegate everything else.  TODO: Make a base class NetworkWrapper for delegating everything in INetwork
	virtual TaskPriority getCurrentTask() const override { return baseNetwork->getCurrentTask(); }
	virtual void setCurrentTask(TaskPriority taskID) override { baseNetwork->setCurrentTask(taskID); }
	virtual double now() const override { return baseNetwork->now(); }
	virtual double timer() override { return baseNetwork->timer(); }
	virtual void stop() override { return baseNetwork->stop(); }
	virtual void addStopCallback(std::function<void()> fn) override {
		ASSERT(false);
		return;
	}
	virtual bool isSimulated() const override { return baseNetwork->isSimulated(); }
	virtual void onMainThread(Promise<Void>&& signal, TaskPriority taskID) override {
		return baseNetwork->onMainThread(std::move(signal), taskID);
	}
	bool isOnMainThread() const override { return baseNetwork->isOnMainThread(); }
	virtual THREAD_HANDLE startThread(THREAD_FUNC_RETURN (*func)(void*), void* arg) override {
		return baseNetwork->startThread(func, arg);
	}
	Future<Reference<class IAsyncFile>> open(std::string filename, int64_t flags, int64_t mode) {
		return IAsyncFileSystem::filesystem()->open(filename, flags, mode);
	}
	Future<Void> deleteFile(std::string filename, bool mustBeDurable) {
		return IAsyncFileSystem::filesystem()->deleteFile(filename, mustBeDurable);
	}
	virtual void run() override { return baseNetwork->run(); }
	virtual bool checkRunnable() override { return baseNetwork->checkRunnable(); }
	virtual void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) override {
		return baseNetwork->getDiskBytes(directory, free, total);
	}
	virtual bool isAddressOnThisHost(NetworkAddress const& addr) const override {
		return baseNetwork->isAddressOnThisHost(addr);
	}
	virtual const TLSConfig& getTLSConfig() const override {
		static TLSConfig emptyConfig;
		return emptyConfig;
	}
};

struct NonserializableThing {};
ACTOR static Future<NonserializableThing> testNonserializableThing() {
	return NonserializableThing();
}

ACTOR Future<Void> testCancelled(bool *exits, Future<Void> f) {
	try {
		wait(Future<Void>(Never()));
	} catch( Error &e ) {
		state Error err = e;
		try {
			wait(Future<Void>(Never()));
		} catch( Error &e ) {
			*exits = true;
			throw;
		}
		throw err;
	}
	return Void();
}

TEST_CASE("/flow/flow/cancel1")
{
	bool exits = false;
	Promise<Void> p;
	Future<Void> test = testCancelled(&exits, p.getFuture());
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 1);
	test.cancel();
	ASSERT(exits);
	ASSERT(test.getPromiseReferenceCount() == 0 && test.getFutureReferenceCount() == 1 && test.isReady() && test.isError() && test.getError().code() == error_code_actor_cancelled);
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 0);


	return Void();
}

ACTOR static Future<Void> noteCancel(int* cancelled) {
	*cancelled = 0;
	try {
		wait(Future<Void>(Never()));
		throw internal_error();
	}
	catch (...) {
		printf("Cancelled!\n");
		*cancelled = 1;
		throw;
	}
}

TEST_CASE("/flow/flow/cancel2")
{
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

namespace {
// Simple message for flatbuffers unittests
struct Int {
	constexpr static FileIdentifier file_identifier = 12345;
	uint32_t value;
	Int() = default;
	Int(uint32_t value) : value(value) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};
} // namespace

TEST_CASE("/flow/flow/nonserializable futures")
{
	// Types no longer need to be statically serializable to make futures, promises, actors
	{
		Future<NonserializableThing> f = testNonserializableThing();
		Promise<NonserializableThing> p;
		p.send(NonserializableThing());
		f = p.getFuture();
	}

	// But this won't compile
	//ReplyPromise<NonserializableThing> rp;

	// ReplyPromise can be used like a normal promise
	{
		ReplyPromise<Int> rpInt;
		Future<Int> f = rpInt.getFuture();
		ASSERT(!f.isReady());
		rpInt.send(123);
		ASSERT(f.get().value == 123);
	}

	{
		RequestStream<Int> rsInt;
		FutureStream<Int> f = rsInt.getFuture();
		rsInt.send(1);
		rsInt.send(2);
		ASSERT(f.pop().value == 1);
		ASSERT(f.pop().value == 2);
	}

	return Void();
}

TEST_CASE("/flow/flow/networked futures")
{
	// RequestStream can be serialized
	{
		RequestStream<Int> locInt;
		BinaryWriter wr(IncludeVersion());
		wr << locInt;

		ASSERT(locInt.getEndpoint().isValid() && locInt.getEndpoint().isLocal() && locInt.getEndpoint().getPrimaryAddress() == FlowTransport::transport().getLocalAddress());

		BinaryReader rd(wr.toValue(), IncludeVersion());
		RequestStream<Int> remoteInt;
		rd >> remoteInt;

		ASSERT(remoteInt.getEndpoint() == locInt.getEndpoint());
	}


	// ReplyPromise can be serialized
	// TODO: This needs to fiddle with g_currentDeliveryPeerAddress
	if (0) {
		ReplyPromise<Int> locInt;
		BinaryWriter wr(IncludeVersion());
		wr << locInt;

		ASSERT(locInt.getEndpoint().isValid() && locInt.getEndpoint().isLocal());

		BinaryReader rd(wr.toValue(), IncludeVersion());
		ReplyPromise<Int> remoteInt;
		rd >> remoteInt;

		ASSERT(remoteInt.getEndpoint() == locInt.getEndpoint());
	}

	return Void();
}

TEST_CASE("/flow/flow/quorum")
{
	vector<Promise<int>> ps(5);
	vector<Future<int>> fs;
	vector<Future<Void>> qs;
	for (auto& p : ps) fs.push_back(p.getFuture());

	for (int i = 0; i <= ps.size(); i++)
		qs.push_back( quorum(fs, i) );

	for (int i = 0; i < ps.size(); i++) {
		ASSERT(qs[i].isReady());
		ASSERT(!qs[i + 1].isReady());
		ps[i].send(i);
	}
	ASSERT(qs[ps.size()].isReady());
	return Void();
}

TEST_CASE("/flow/flow/trivial futures")
{
	Future<int> invalid;
	ASSERT(!invalid.isValid());

	Future<int> never = Never();
	ASSERT(never.isValid() && !never.isReady());

	Future<int> one = 1;
	ASSERT(one.isValid() && one.isReady() && !one.isError());
	ASSERT(one.get() == 1);
	ASSERT(one.getFutureReferenceCount() == 1);
	return Void();
}

TEST_CASE("/flow/flow/trivial promises")
{
	Future<int> f;

	Promise<int> p;
	ASSERT(p.isValid());
	ASSERT(!p.isSet());
	p.send(1);
	ASSERT(p.isSet());
	ASSERT(p.getFuture().get() == 1);

	Promise<int> p2;
	f = p2.getFuture();
	ASSERT(f.isValid() && !f.isReady());
	p2.send(2);
	ASSERT(f.isValid() && f.isReady() && !f.isError());
	ASSERT(f.get() == 2);

	Promise<int> p3;
	f = p3.getFuture();
	p3.sendError(end_of_stream());
	ASSERT(f.isValid() && f.isReady() && f.isError());
	ASSERT(f.getError().code() == error_code_end_of_stream);

	Promise<int> p4;
	f = p4.getFuture();
	p4 = Promise<int>();
	ASSERT(p4.isValid() && !p4.isSet());
	ASSERT(f.isValid() && f.isReady() && f.isError());
	ASSERT(f.getError().code() == error_code_broken_promise);
	return Void();
}

TEST_CASE("/flow/flow/trivial promisestreams")
{
	FutureStream<int> f;

	PromiseStream<int> p;
	p.send(1);
	ASSERT(p.getFuture().isReady());
	ASSERT(p.getFuture().pop() == 1);

	PromiseStream<int> p2;
	f = p2.getFuture();
	ASSERT(f.isValid() && !f.isReady());
	p2.send(2);
	p2.send(3);
	ASSERT(f.isValid() && f.isReady() && !f.isError());
	ASSERT(f.pop() == 2);
	ASSERT(f.pop() == 3);

	PromiseStream<int> p3;
	f = p3.getFuture();
	p3.send(4);
	p3.sendError(end_of_stream());
	ASSERT(f.isReady() && !f.isError());
	ASSERT(f.pop() == 4);
	ASSERT(f.isError());
	ASSERT(f.getError().code() == error_code_end_of_stream);

	PromiseStream<int> p4;
	f = p4.getFuture();
	p4 = PromiseStream<int>();
	ASSERT(f.isValid() && f.isReady() && f.isError());
	ASSERT(f.getError().code() == error_code_broken_promise);
	return Void();
}

TEST_CASE("/flow/flow/callbacks")
{
	Promise<int> p;
	Future<int> f = p.getFuture();
	int result = 0;
	bool happened = false;

	onReady(std::move(f), [&result](int x) { result = x; }, [&result](Error e) { result = -1; });
	onReady(p.getFuture(), [&happened](int) { happened = true; }, [&happened](Error){ happened = true; });
	ASSERT(!f.isValid());
	ASSERT(p.isValid() && !p.isSet() && p.getFutureReferenceCount()==1);
	ASSERT(result == 0 && !happened);

	p.send(123);
	ASSERT(result == 123 && happened);
	ASSERT(p.isValid() && p.isSet() && p.getFutureReferenceCount() == 0 && p.getFuture().get() == 123);

	result = 0;
	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e) { result = -1; });
	ASSERT(result == 123);
	ASSERT(p.isValid() && p.isSet() && p.getFutureReferenceCount() == 0 && p.getFuture().get() == 123);

	p = Promise<int>();
	f = p.getFuture();
	result = 0;
	onReady(std::move(f), [&result](int x) { result = x; }, [&result](Error e) { result = -e.code(); });
	ASSERT(!f.isValid());
	ASSERT(p.isValid() && !p.isSet() && p.getFutureReferenceCount() == 1);
	ASSERT(result == 0);

	p = Promise<int>();
	ASSERT(result == -error_code_broken_promise);
	return Void();
}

TEST_CASE("/flow/flow/promisestream callbacks")
{
	PromiseStream<int> p;

	int result = 0;

	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e){ result = -1; });

	ASSERT(result == 0);

	p.send(123);
	p.send(456);

	ASSERT(result == 123);
	result = 0;

	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e){ result = -1; });

	ASSERT(result == 456);
	result = 0;

	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e){ result = -1; });

	ASSERT(result == 0);

	p = PromiseStream<int>();

	ASSERT(result == -1);
	return Void();
}

//Incompatible with --crash, so we are commenting it out for now
/*
TEST_CASE("/flow/flow/promisestream multiple wait error")
{
	state int result = 0;
	state PromiseStream<int> p;
	try {
		onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e){ result = -1; });
		result = 100;
		onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e){ result = -1; });
		ASSERT(false);
	}
	catch (Error& e) {
		ASSERT(e.code() == error_code_internal_error);
	}
	ASSERT(result == 100);
	p = PromiseStream<int>();
	ASSERT(result == -1);
	return Void();
}
*/

TEST_CASE("/flow/flow/trivial actors")
{
	ASSERT(expectActorCount(0));

	int result = 0;
	trivialVoidActor(&result);
	ASSERT(result == 1);
	ASSERT(expectActorCount(0));

	Future<int> f = return42Actor();
	ASSERT(f.isReady() && !f.isError() && f.get() == 42 && f.getFutureReferenceCount()==1 && f.getPromiseReferenceCount() == 0);
	ASSERT(expectActorCount(1));
	f = Future<int>();
	ASSERT(expectActorCount(0));

	f = templateActor(24);
	ASSERT(f.isReady() && !f.isError() && f.get() == 24 && f.getFutureReferenceCount() == 1 && f.getPromiseReferenceCount() == 0);
	ASSERT(expectActorCount(1));
	f = Future<int>();
	ASSERT(expectActorCount(0));

	result = 0;
	voidWaitActor(2, &result);
	ASSERT(result == 2 && expectActorCount(0));

	Promise<int> p;
	f = addOneActor(p.getFuture());
	ASSERT(!f.isReady() && expectActorCount(1));
	p.send(100);
	ASSERT(f.isReady() && f.get() == 101);
	ASSERT(expectActorCount(1)); //< hmm
	f = Future<int>();
	ASSERT(expectActorCount(0));

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

	ASSERT( testHygeine().isReady() );
	return Void();
}

TEST_CASE("/flow/flow/yieldedFuture/progress")
{
	// Check that if check_yield always returns true, the yieldedFuture will do nothing immediately but will
	// get one thing done per "tick" (per delay(0) returning).

	Reference<YieldMockNetwork> yn( new YieldMockNetwork );

	yn->nextYield = 0;

	Promise<Void> p;
	Future<Void> u = p.getFuture();
	Future<Void> i = success(u);

	std::vector<Future<Void>> v;
	for(int i=0; i<5; i++)
		v.push_back(yieldedFuture(u));
	auto numReady = [&v]() {
		return std::count_if(v.begin(), v.end(), [](Future<Void> v) { return v.isReady(); });
	};

	ASSERT( numReady()==0 );
	p.send(Void());
	ASSERT( u.isReady() && i.isReady() && numReady()==0 );

	for(int i=0; i<5; i++) {
		yn->tick();
		ASSERT( numReady() == i+1 );
	}

	for(int i=0; i<5; i++) {
		ASSERT( v[i].getPromiseReferenceCount() == 0 && v[i].getFutureReferenceCount() == 1 );
	}

	return Void();
}

TEST_CASE("/flow/flow/yieldedFuture/random")
{
	// Check expectations about exactly how yieldedFuture responds to check_yield results

	Reference<YieldMockNetwork> yn( new YieldMockNetwork );

	for(int r=0; r<100; r++) {
		Promise<Void> p;
		Future<Void> u = p.getFuture();
		Future<Void> i = success(u);

		std::vector<Future<Void>> v;
		for(int i=0; i<25; i++)
			v.push_back(yieldedFuture(u));
		auto numReady = [&v]() {
			return std::count_if(v.begin(), v.end(), [](Future<Void> v) { return v.isReady(); });
		};

		Future<Void> j = success(u);

		ASSERT( numReady()==0 );

		int expectYield = deterministicRandom()->randomInt(0, 4);
		int expectReady = expectYield;
		yn->nextYield = 1 + expectYield;

		p.send(Void());
		ASSERT( u.isReady() && i.isReady() && j.isReady() && numReady()==expectReady );

		while (numReady() != v.size()) {
			expectYield = deterministicRandom()->randomInt(0, 4);
			yn->nextYield = 1 + expectYield;
			expectReady += 1 + expectYield;
			yn->tick();
			//printf("Yielding %d times, expect %d/%d ready, got %d\n", expectYield, expectReady, v.size(), numReady() );
			ASSERT( numReady() == std::min<int>(expectReady, v.size()) );
		}

		for(int i=0; i<v.size(); i++) {
			ASSERT( v[i].getPromiseReferenceCount() == 0 && v[i].getFutureReferenceCount() == 1 );
		}
	}

	return Void();
}


TEST_CASE("/flow/perf/yieldedFuture")
{
	double start;
	int N = 1000000;

	Reference<YieldMockNetwork> yn( new YieldMockNetwork );

	yn->nextYield = 2*N + 100;

	Promise<Void> p;
	Future<Void> f = p.getFuture();
	vector<Future<Void>> ys;

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
	for (int i = 0; i < N; i++)
		yieldedFuture(f);
	printf("yieldedFuture(f) cancel: %0.1f M/sec\n", N / 1e6 / (timer() - start));

	return Void();
}

TEST_CASE("/flow/flow/chooseTwoActor")
{
	ASSERT(expectActorCount(0));

	Promise<Void> a, b;
	Future<Void> c = chooseTwoActor(a.getFuture(), b.getFuture());
	ASSERT(a.getFutureReferenceCount()==2 && b.getFutureReferenceCount()==2 && !c.isReady());
	b.send(Void());
	ASSERT(a.getFutureReferenceCount() == 0 && b.getFutureReferenceCount() == 0 && c.isReady() && !c.isError() && expectActorCount(1));
	c = Future<Void>();
	ASSERT(a.getFutureReferenceCount() == 0 && b.getFutureReferenceCount() == 0 && expectActorCount(0));
	return Void();
}

TEST_CASE("/flow/flow/perf/actor patterns")
{
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
		oneWaitVoidActor(already);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out(N);
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
		//ASSERT(expectActorCount(0));
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out1(N);
		vector<Future<Void>> out2(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out1(N);
		vector<Future<Void>> out2(N);
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
		vector<Promise<Void>> pipe(N);
		vector<Future<Void>> out1(N);
		vector<Future<Void>> out2(N);
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
		printf("sizeof(CheeseWaitActorActor) == %zu\n", sizeof(CheeseWaitActorActor));
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
		vector<Promise<Void>> ps(3);
		vector<Future<Void>> fs(3);

		for (int i = 0; i < N; i++) {
			ps.clear();
			ps.resize(3);
			for (int j = 0; j < ps.size(); j++)
				fs[j] = ps[j].getFuture();

			Future<Void> q = quorum(fs, 2);
			for (auto& p : ps) p.send(Void());
		}
		printf("quorum(2/3): %0.2f M/sec\n", N / 1e6 / (timer() - start));
	}

	return Void();
}

template <class YAM>
struct YAMRandom {
	YAM yam;
	std::vector<Future<Void>> onchanges;
	int kmax;

	YAMRandom() : kmax(3) {}

	void randomOp() {
		if (deterministicRandom()->random01() < 0.01)
			while (!check_yield());

		int k = deterministicRandom()->randomInt(0, kmax);
		int op = deterministicRandom()->randomInt(0, 7);
		//printf("%d",op);
		if (op == 0) {
			onchanges.push_back(yam.onChange(k));
		} else if (op == 1) {
			onchanges.push_back( trigger([this](){ this->randomOp(); }, yam.onChange(k)) );
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
			yam.triggerRange(std::min(a,b), std::max(a,b)+1);
		}
	}
};

TEST_CASE("/flow/flow/YieldedAsyncMap/randomized")
{
	state YAMRandom<YieldedAsyncMap<int, int>> yamr;
	state int it;
	for (it = 0; it < 100000; it++) {
		yamr.randomOp();
		wait(yield());
	}
	return Void();
}

TEST_CASE("/flow/flow/AsyncMap/randomized")
{
	state YAMRandom<AsyncMap<int, int>> yamr;
	state int it;
	for (it = 0; it < 100000; it++) {
		yamr.randomOp();
		wait(yield());
	}
	return Void();
}

TEST_CASE("/flow/flow/YieldedAsyncMap/basic")
{
	state YieldedAsyncMap<int, int> yam;
	state Future<Void> y0 = yam.onChange(1);
	yam.setUnconditional(1, 0);
	state Future<Void> y1 = yam.onChange(1);
	state Future<Void> y1a = yam.onChange(1);
	state Future<Void> y1b = yam.onChange(1);
	yam.set(1, 1);
	//while (!check_yield()) {}
	//yam.triggerRange(0, 4);

	state Future<Void> y2 = yam.onChange(1);
	wait(reportErrors(y0, "Y0"));
	wait(reportErrors(y1, "Y1"));
	wait(reportErrors(y1a, "Y1a"));
	wait(reportErrors(y1b, "Y1b"));
	wait(reportErrors(timeout(y2, 5, Void()), "Y2"));

	return Void();
}

TEST_CASE("/flow/flow/YieldedAsyncMap/cancel")
{
	state YieldedAsyncMap<int, int> yam;
	//ASSERT(yam.count(1) == 0);
	//state Future<Void> y0 = yam.onChange(1);
	//ASSERT(yam.count(1) == 1);
	//yam.setUnconditional(1, 0);

	ASSERT(yam.count(1) == 0);
	state Future<Void> y1 = yam.onChange(1);
	state Future<Void> y1a = yam.onChange(1);
	state Future<Void> y1b = yam.onChange(1);
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

TEST_CASE("/flow/flow/YieldedAsyncMap/cancel2")
{
	state YieldedAsyncMap<int, int> yam;

	state Future<Void> y1 = yam.onChange(1);
	state Future<Void> y2 = yam.onChange(2);

	auto* pyam = &yam;
	uncancellable(trigger(
		[pyam](){
			printf("Triggered\n");
			pyam->triggerAll();
		},
		delay(1)));

	wait(y1);
	printf("Got y1\n");
	y2.cancel();

	return Void();
}

TEST_CASE("/flow/flow/AsyncVar/basic")
{
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

ACTOR static Future<Void> waitAfterCancel( int* output ) {
	*output = 0;
	try {
		wait( Never() );
	} catch (...) {
		wait( (*output=1, Future<Void>(Void())) );
	}
	ASSERT(false);
	return Void();
}

TEST_CASE("/fdbrpc/flow/wait_expression_after_cancel")
{
	int a = -1;
	Future<Void> f = waitAfterCancel(&a);
	ASSERT( a == 0 );
	f.cancel();
	ASSERT( a == 1 );
	return Void();
}

// Tests for https://github.com/apple/foundationdb/issues/1226

template <class>
struct ShouldNotGoIntoClassContextStack;

ACTOR static Future<Void> shouldNotHaveFriends();

class Foo1 {
public:
	explicit Foo1(int x) : x(x) {}
	Future<int> foo() { return fooActor(this); }
	ACTOR static Future<int> fooActor(Foo1* self);

private:
	int x;
};
ACTOR Future<int> Foo1::fooActor(Foo1* self) {
	wait(Future<Void>());
	return self->x;
}

class [[nodiscard]] Foo2 {
public:
	explicit Foo2(int x) : x(x) {}
	Future<int> foo() { return fooActor(this); }
	ACTOR static Future<int> fooActor(Foo2 * self);

private:
	int x;
};
ACTOR Future<int> Foo2::fooActor(Foo2* self) {
	wait(Future<Void>());
	return self->x;
}

class alignas(4) Foo3 {
public:
	explicit Foo3(int x) : x(x) {}
	Future<int> foo() { return fooActor(this); }
	ACTOR static Future<int> fooActor(Foo3* self);

private:
	int x;
};
ACTOR Future<int> Foo3::fooActor(Foo3* self) {
	wait(Future<Void>());
	return self->x;
}

struct Super {};

class Foo4 : Super {
public:
	explicit Foo4(int x) : x(x) {}
	Future<int> foo() { return fooActor(this); }
	ACTOR static Future<int> fooActor(Foo4* self);

private:
	int x;
};
ACTOR Future<int> Foo4::fooActor(Foo4* self) {
	wait(Future<Void>());
	return self->x;
}

struct Outer {
	class Foo5 : Super {
	public:
		explicit Foo5(int x) : x(x) {}
		Future<int> foo() { return fooActor(this); }
		ACTOR static Future<int> fooActor(Foo5* self);

	private:
		int x;
	};
};
ACTOR Future<int> Outer::Foo5::fooActor(Outer::Foo5* self) {
	wait(Future<Void>());
	return self->x;
}

ACTOR static Future<Void> shouldNotHaveFriends2();

// Meant to be run with -fsanitize=undefined
TEST_CASE("/flow/DeterministicRandom/SignedOverflow") {
	deterministicRandom()->randomInt(std::numeric_limits<int>::min(), 0);
	deterministicRandom()->randomInt(0, std::numeric_limits<int>::max());
	deterministicRandom()->randomInt(std::numeric_limits<int>::min(), std::numeric_limits<int>::max());
	ASSERT(deterministicRandom()->randomInt(std::numeric_limits<int>::min(), std::numeric_limits<int>::min() + 1) ==
	       std::numeric_limits<int>::min());
	ASSERT(deterministicRandom()->randomInt(std::numeric_limits<int>::max() - 1, std::numeric_limits<int>::max()) ==
	       std::numeric_limits<int>::max() - 1);

	deterministicRandom()->randomInt64(std::numeric_limits<int64_t>::min(), 0);
	deterministicRandom()->randomInt64(0, std::numeric_limits<int64_t>::max());
	deterministicRandom()->randomInt64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
	ASSERT(deterministicRandom()->randomInt64(std::numeric_limits<int64_t>::min(),
	                                          std::numeric_limits<int64_t>::min() + 1) ==
	       std::numeric_limits<int64_t>::min());
	ASSERT(deterministicRandom()->randomInt64(std::numeric_limits<int64_t>::max() - 1,
	                                          std::numeric_limits<int64_t>::max()) ==
	       std::numeric_limits<int64_t>::max() - 1);
	return Void();
}

struct Tracker {
	int copied;
	bool moved;
	Tracker(int copied = 0) : moved(false), copied(copied) {}
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

	ACTOR static Future<Void> listen(FutureStream<Tracker> stream) {
		Tracker movedTracker = waitNext(stream);
		ASSERT(!movedTracker.moved);
		ASSERT(movedTracker.copied == 0);
		return Void();
	}
};

TEST_CASE("/flow/flow/PromiseStream/move") {
	state PromiseStream<Tracker> stream;
	state Future<Void> listener;
	{
		// This tests the case when a callback is added before
		// a movable value is sent
		listener = Tracker::listen(stream.getFuture());
		stream.send(Tracker{});
		wait(listener);
	}

	{
		// This tests the case when a callback is added before
		// a unmovable value is sent
		listener = Tracker::listen(stream.getFuture());
		Tracker namedTracker;
		stream.send(namedTracker);
		wait(listener);
	}
	{
		// This tests the case when no callback is added until
		// after a movable value is sent
		stream.send(Tracker{});
		stream.send(Tracker{});
		{
			state Tracker movedTracker = waitNext(stream.getFuture());
			ASSERT(!movedTracker.moved);
			ASSERT(movedTracker.copied == 0);
		}
		{
			Tracker movedTracker = waitNext(stream.getFuture());
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
			state Tracker copiedTracker = waitNext(stream.getFuture());
			ASSERT(!copiedTracker.moved);
			// must copy onto queue
			ASSERT(copiedTracker.copied == 1);
		}
		{
			Tracker copiedTracker = waitNext(stream.getFuture());
			ASSERT(!copiedTracker.moved);
			// must copy onto queue
			ASSERT(copiedTracker.copied == 1);
		}
	}

	return Void();
}

TEST_CASE("/flow/flow/PromiseStream/move2") {
	PromiseStream<Tracker> stream;
	stream.send(Tracker{});
	Tracker tracker = waitNext(stream.getFuture());
	Tracker movedTracker = std::move(tracker);
	ASSERT(tracker.moved);
	ASSERT(!movedTracker.moved);
	ASSERT(movedTracker.copied == 0);
	return Void();
}
