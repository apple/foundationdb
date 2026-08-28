/*
 * FlowTests.cpp
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

// Unit tests for the Flow runtime and libraries

#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/ProcessEvents.h"
#include "flow/ProtocolVersion.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/DeterministicRandom.h"
#include "flow/IThreadPool.h"
#include "flow/WriteOnlySet.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/AsyncFileNonDurable.h"
#include "flow/IAsyncFile.h"
#include "flow/TLSConfig.h"
#include "fdbrpc/grpc/AsyncTaskExecutor.h"
#include "flow/CoroUtils.h"

void forceLinkFlowTests() {}

extern bool g_crashOnError;

TEST_CASE("/flow/buggifiedDelay") {
	if (FLOW_KNOBS->MAX_BUGGIFIED_DELAY == 0) {
		co_return;
	}
	while (true) {
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
	} else {
		f.addCallbackAndClear(new LambdaCallback<T, Func, ErrFunc, Callback<T>>(std::forward<Func>(func),
		                                                                        std::forward<ErrFunc>(errFunc)));
	}
}

template <class T, class Func, class ErrFunc>
void onReady(FutureStream<T>&& f, Func&& func, ErrFunc&& errFunc) {
	if (f.isReady()) {
		if (f.isError())
			errFunc(f.getError());
		else
			func(f.pop());
	} else {
		f.addCallbackAndClear(new LambdaCallback<T, Func, ErrFunc, SingleCallback<T>>(std::forward<Func>(func),
		                                                                              std::forward<ErrFunc>(errFunc)));
	}
}

static Future<Void> emptyVoidActor(Uncancellable = Uncancellable()) {
	co_return;
}

static Future<Void> emptyActor() {
	return Void();
}

static Future<Void> oneWaitVoidActor(Future<Void> f, Uncancellable = Uncancellable()) {
	co_await f;
}

static Future<Void> oneWaitActor(Future<Void> f) {
	co_await f;
}

Future<Void> g_cheese;
static Future<Void> cheeseWaitActor() {
	// The global can change while this coroutine is suspended.
	Future<Void> f = g_cheese;
	co_await f;
}

static Future<Void> trivialVoidActor(int* result, Uncancellable = Uncancellable()) {
	*result = 1;
	co_return;
}

static Future<int> return42Actor() {
	return 42;
}

static Future<Void> voidWaitActor(Future<int> in, int* result, Uncancellable = Uncancellable()) {
	int i = co_await in;
	*result = i;
}

static Future<int> addOneActor(Future<int> in) {
	int i = co_await in;
	co_return i + 1;
}

static Future<Void> chooseTwoActor(Future<Void> f, Future<Void> g) {
	co_await race(f, g);
}

static Future<int> consumeOneActor(FutureStream<int> in) {
	int i = co_await in;
	co_return i;
}

static Future<int> sumActor(FutureStream<int> in) {
	int total = 0;
	try {
		while (true) {
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
static Future<T> templateActor(T t) {
	return t;
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

	void _swiftEnqueue(void* task) override { abort(); }

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
};

struct NonserializableThing {};
static Future<NonserializableThing> testNonserializableThing() {
	return NonserializableThing();
}

Future<Void> testCancelled(bool* exits, Future<Void> f) {
	Error err;
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
	throw err;
}

TEST_CASE("/flow/flow/cancel1") {
	bool exits = false;
	Promise<Void> p;
	Future<Void> test = testCancelled(&exits, p.getFuture());
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 1);
	test.cancel();
	ASSERT(exits);
	ASSERT(test.getPromiseReferenceCount() == 0 && test.getFutureReferenceCount() == 1 && test.isReady() &&
	       test.isError() && test.getError().code() == error_code_actor_cancelled);
	// Coroutine parameters remain alive until the last future releases the frame.
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 1);
	test = Future<Void>();
	ASSERT(p.getPromiseReferenceCount() == 1 && p.getFutureReferenceCount() == 0);

	return Void();
}

TEST_CASE("/fdbrpc/asyncFileNonDurable/sendErrorOnShutdownCancellation") {
	Promise<Void> input;
	Future<Void> wrapped = sendErrorOnShutdown(input.getFuture());
	ASSERT(input.getFutureReferenceCount() > 0);
	wrapped.cancel();
	ASSERT(wrapped.isReady() && wrapped.isError() && wrapped.getError().code() == error_code_actor_cancelled);
	ASSERT_EQ(input.getFutureReferenceCount(), 0);
	input.send(Void());
	return Void();
}

static Future<Void> noteCancel(int* cancelled) {
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

TEST_CASE("/flow/flow/cancel2") {
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

namespace flow_tests_details {
// Simple message for flatbuffers unittests
struct Int {
	constexpr static FileIdentifier file_identifier = 12345;
	uint32_t value;
	Int() = default;
	explicit(false) Int(uint32_t value) : value(value) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

template <class T>
SAV<T>* replyPromiseState(ReplyPromise<T>& promise) {
	auto* rawState = promise.extractRawPointer();
	promise = ReplyPromise<T>(rawState);
	return rawState;
}

struct ReplyPromiseReuseRequest {
	constexpr static FileIdentifier file_identifier = 1449982;
	ReplyPromise<Int> reply;
	SAV<Int>* defaultState;

	ReplyPromiseReuseRequest() : defaultState(replyPromiseState(reply)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

class RpcExceptionObserver : NonCopyable {
	const bool previousTraceProcessEvents;
	const char* expectedException;
	int observed = 0;
	int unexpected = 0;
	ProcessEvents::Event event;

	void observe(const std::any& data) {
		auto tracePtr = std::any_cast<BaseTraceEvent*>(&data);
		if (!tracePtr || !*tracePtr) {
			++unexpected;
			return;
		}
		auto* trace = *tracePtr;
		int errorCode = 0;
		std::string exception;
		if (!expectedException || observed != 0 || trace->getSeverity() != SevError ||
		    !trace->getFields().tryGetInt("ErrorCode", errorCode) || errorCode != error_code_unknown_error ||
		    !trace->getFields().tryGetValue("StdException", exception) || exception != expectedException) {
			++unexpected;
			return;
		}
		++observed;
		// Preserve the real severity check, but identify only this deliberately injected exception to trace scanners.
		trace->detail("ErrorIsInjectedFault", 1);
	}

public:
	explicit RpcExceptionObserver(const char* expectedException = nullptr)
	  : previousTraceProcessEvents(g_traceProcessEvents), expectedException(expectedException),
	    event("TraceEvent::SystemError"_sr, [this](StringRef, const std::any& data, const Error&) { observe(data); }) {
		g_traceProcessEvents = true;
	}
	~RpcExceptionObserver() { g_traceProcessEvents = previousTraceProcessEvents; }

	void check(int expectedCount) const {
		ASSERT_EQ(observed, expectedCount);
		ASSERT_EQ(unexpected, 0);
	}
};

template <bool ErrorReply>
struct ThrowingRpcReply {
	constexpr static FileIdentifier file_identifier = 1449983 + ErrorReply;
	uint32_t value = 0;

	static const char* exceptionMessage() {
		return ErrorReply ? "RpcUnexpectedException/networkSenderErrorReply"
		                  : "RpcUnexpectedException/networkSenderValue";
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
		if constexpr (is_fb_function<Ar>) {
			// Vtable collection visits the reply alternative even when ErrorOr contains an error.
			throw std::runtime_error(exceptionMessage());
		}
	}
};

Future<Void> checkNetworkSenderKnownErrors() {
	RpcExceptionObserver observer;
	ReplyPromise<Int> recipient;
	Future<Int> received = recipient.getFuture();
	Endpoint endpoint(FlowTransport::transport().getLocalAddresses(), recipient.getEndpoint().token);
	ReplyPromise<Int> sender;
	sender.loadRemoteEndpoint(endpoint);
	sender.sendError(operation_failed());
	ErrorOr<Int> result = co_await errorOr(timeoutError(received, 1.0));
	ASSERT(result.isError() && result.getError().code() == error_code_operation_failed);
	ASSERT_EQ(sender.getFutureReferenceCount(), 0);

	ReplyPromise<Int> noReplyRecipient;
	Future<Int> noReplyReceived = noReplyRecipient.getFuture();
	Endpoint noReplyEndpoint(FlowTransport::transport().getLocalAddresses(), noReplyRecipient.getEndpoint().token);
	ReplyPromise<Int> noReplySender;
	noReplySender.loadRemoteEndpoint(noReplyEndpoint);
	noReplySender.send(Never());
	co_await orderedDelay(0, TaskPriority::DefaultPromiseEndpoint);
	ASSERT(!noReplyReceived.isReady());
	ASSERT_EQ(noReplySender.getFutureReferenceCount(), 0);
	observer.check(0);
}

} // namespace flow_tests_details

TEST_CASE("/flow/flow/nonserializable futures") {
	// Types no longer need to be statically serializable to make futures, promises, actors
	{
		Future<NonserializableThing> f = testNonserializableThing();
		Promise<NonserializableThing> p;
		p.send(NonserializableThing());
		f = p.getFuture();
	}

	// But this won't compile
	// ReplyPromise<NonserializableThing> rp;

	// ReplyPromise can be used like a normal promise
	{
		ReplyPromise<flow_tests_details::Int> rpInt;
		Future<flow_tests_details::Int> f = rpInt.getFuture();
		ASSERT(!f.isReady());
		rpInt.send(123);
		ASSERT(f.get().value == 123);
	}

	{
		RequestStream<flow_tests_details::Int> rsInt;
		FutureStream<flow_tests_details::Int> f = rsInt.getFuture();
		rsInt.send(1);
		rsInt.send(2);
		ASSERT(f.pop().value == 1);
		ASSERT(f.pop().value == 2);
	}

	return Void();
}

TEST_CASE("/flow/flow/networked futures") {
	// RequestStream can be serialized
	{
		RequestStream<flow_tests_details::Int> locInt;
		BinaryWriter wr(IncludeVersion());
		wr << locInt;

		ASSERT(locInt.getEndpoint().isValid() && locInt.getEndpoint().isLocal() &&
		       locInt.getEndpoint().getPrimaryAddress() == FlowTransport::transport().getLocalAddress());

		BinaryReader rd(wr.toValue(), IncludeVersion());
		RequestStream<flow_tests_details::Int> remoteInt;
		rd >> remoteInt;

		ASSERT(remoteInt.getEndpoint() == locInt.getEndpoint());
	}

	// ReplyPromise can be serialized
	// TODO: This needs to fiddle with g_currentDeliveryPeerAddress
	if (0) {
		ReplyPromise<flow_tests_details::Int> locInt;
		BinaryWriter wr(IncludeVersion());
		wr << locInt;

		ASSERT(locInt.getEndpoint().isValid() && locInt.getEndpoint().isLocal());

		BinaryReader rd(wr.toValue(), IncludeVersion());
		ReplyPromise<flow_tests_details::Int> remoteInt;
		rd >> remoteInt;

		ASSERT(remoteInt.getEndpoint() == locInt.getEndpoint());
	}

	return Void();
}

TEST_CASE("/fdbrpc/ReplyPromise/reuse state on deserialize") {
	using flow_tests_details::Int;
	const Endpoint remote({ NetworkAddress(IPAddress(0x01010101), 1) }, UID(1, 2));

	{
		ReplyPromise<Int> promise;
		auto* original = flow_tests_details::replyPromiseState(promise);
		promise.loadRemoteEndpoint(remote);
		ASSERT(flow_tests_details::replyPromiseState(promise) == original);
		ASSERT(promise.getEndpoint() == remote);
		Future<Int> reply = promise.getFuture();
		promise.send(Int(17));
		ASSERT(reply.isReady() && !reply.isError() && reply.get().value == 17);
	}

	{
		ReplyPromise<Int> promise;
		Future<Int> oldReply = promise.getFuture();
		auto* original = flow_tests_details::replyPromiseState(promise);
		promise.loadRemoteEndpoint(remote);
		ASSERT(flow_tests_details::replyPromiseState(promise) != original);
		ASSERT(oldReply.isReady() && oldReply.isError() && oldReply.getError().code() == error_code_broken_promise);
		Future<Int> reply = promise.getFuture();
		promise.send(Int(23));
		ASSERT(reply.isReady() && !reply.isError() && reply.get().value == 23);
	}

	{
		ReplyPromise<Int> promise;
		Endpoint local = promise.getEndpoint();
		ASSERT(local.isValid());
		auto* original = flow_tests_details::replyPromiseState(promise);
		promise.loadRemoteEndpoint(remote);
		ASSERT(flow_tests_details::replyPromiseState(promise) != original);
		ASSERT(promise.getEndpoint() == remote);
		Future<Int> reply = promise.getFuture();
		promise.send(Int(31));
		ASSERT(reply.isReady() && !reply.isError() && reply.get().value == 31);
	}

	{
		ReplyPromise<Int> promise(
		    PeerCompatibilityPolicy{ RequirePeer::AtLeast, ProtocolVersion::withStableInterfaces() });
		auto* original = flow_tests_details::replyPromiseState(promise);
		promise.loadRemoteEndpoint(remote);
		ASSERT(flow_tests_details::replyPromiseState(promise) != original);
		Future<Int> reply = promise.getFuture();
		promise.send(Int(41));
		ASSERT(reply.isReady() && !reply.isError() && reply.get().value == 41);
	}

	return Void();
}

TEST_CASE("/fdbrpc/ReplyPromise/reuse state binary deserialize") {
	using flow_tests_details::ReplyPromiseReuseRequest;
	ReplyPromiseReuseRequest request;
	ProtocolVersion version = currentProtocolVersion();
	version.removeObjectSerializerFlag();
	BinaryWriter writer(IncludeVersion(version));
	writer << request;

	BinaryReader reader(writer.toValue(), IncludeVersion(version));
	ReplyPromiseReuseRequest received;
	reader >> received;
	ASSERT(flow_tests_details::replyPromiseState(received.reply) == received.defaultState);
	ASSERT(received.reply.getEndpoint().token == request.reply.getEndpoint().token);
	return Void();
}

TEST_CASE("/fdbrpc/ReplyPromise/reuse state exact reply") {
	RequestStream<flow_tests_details::ReplyPromiseReuseRequest> local;
	FutureStream<flow_tests_details::ReplyPromiseReuseRequest> incoming = local.getFuture();
	flow_tests_details::ReplyPromiseReuseRequest request;
	Future<flow_tests_details::Int> reply = request.reply.getFuture();
	{
		RequestStream<flow_tests_details::ReplyPromiseReuseRequest> remote(local.getEndpoint());
		remote.send(request);
	}

	flow_tests_details::ReplyPromiseReuseRequest received = co_await incoming;
	ASSERT(flow_tests_details::replyPromiseState(received.reply) == received.defaultState);
	received.reply.send(flow_tests_details::Int(59));
	flow_tests_details::Int value = co_await reply;
	ASSERT(value.value == 59);
}

TEST_CASE("noSim/fdbrpc/RpcUnexpectedException/networkSenderValue") {
	// Simulation counts SevError before the observer can mark an expected injection; crash-on-error must stay intact.
	if (g_network->isSimulated() || g_crashOnError) {
		return Void();
	}
	using Reply = flow_tests_details::ThrowingRpcReply<false>;
	flow_tests_details::RpcExceptionObserver observer(Reply::exceptionMessage());
	Endpoint endpoint(FlowTransport::transport().getLocalAddresses(), UID(1, 2));
	ReplyPromise<Reply> reply;
	reply.loadRemoteEndpoint(endpoint);
	ASSERT_GT(reply.getFutureReferenceCount(), 0);
	reply.send(Reply());
	observer.check(1);
	ASSERT_EQ(reply.getFutureReferenceCount(), 0);
	return Void();
}

TEST_CASE("noSim/fdbrpc/RpcUnexpectedException/networkSenderErrorReply") {
	if (g_network->isSimulated() || g_crashOnError) {
		return Void();
	}
	using Reply = flow_tests_details::ThrowingRpcReply<true>;
	flow_tests_details::RpcExceptionObserver observer(Reply::exceptionMessage());
	Endpoint endpoint(FlowTransport::transport().getLocalAddresses(), UID(1, 2));
	ReplyPromise<Reply> reply;
	reply.loadRemoteEndpoint(endpoint);
	ASSERT_GT(reply.getFutureReferenceCount(), 0);
	reply.sendError(operation_failed());
	observer.check(1);
	ASSERT_EQ(reply.getFutureReferenceCount(), 0);
	return Void();
}

TEST_CASE("noSim/fdbrpc/RpcUnexpectedException/networkSenderKnownErrors") {
	if (g_network->isSimulated() || g_crashOnError) {
		co_return;
	}
	co_await flow_tests_details::checkNetworkSenderKnownErrors();
}

TEST_CASE("/flow/flow/quorum") {
	std::vector<Promise<int>> ps(5);
	std::vector<Future<int>> fs;
	std::vector<Future<Void>> qs;
	for (auto& p : ps)
		fs.push_back(p.getFuture());

	for (int i = 0; i <= ps.size(); i++)
		qs.push_back(quorum(fs, i));

	for (int i = 0; i < ps.size(); i++) {
		ASSERT(qs[i].isReady());
		ASSERT(!qs[i + 1].isReady());
		ps[i].send(i);
	}
	ASSERT(qs[ps.size()].isReady());
	return Void();
}

TEST_CASE("/flow/flow/trivial futures") {
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

TEST_CASE("/flow/flow/trivial promises") {
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

TEST_CASE("/flow/flow/trivial promisestreams") {
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

TEST_CASE("/flow/flow/callbacks") {
	Promise<int> p;
	Future<int> f = p.getFuture();
	int result = 0;
	bool happened = false;

	onReady(std::move(f), [&result](int x) { result = x; }, [&result](Error e) { result = -1; });
	onReady(p.getFuture(), [&happened](int) { happened = true; }, [&happened](Error) { happened = true; });
	ASSERT(
	    !f.isValid()); // NOLINT(bugprone-use-after-move): this test intentionally checks the moved-from Future state.
	ASSERT(p.isValid() && !p.isSet() && p.getFutureReferenceCount() == 1);
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
	ASSERT(
	    !f.isValid()); // NOLINT(bugprone-use-after-move): this test intentionally checks the moved-from Future state.
	ASSERT(p.isValid() && !p.isSet() && p.getFutureReferenceCount() == 1);
	ASSERT(result == 0);

	p = Promise<int>();
	ASSERT(result == -error_code_broken_promise);
	return Void();
}

TEST_CASE("/flow/flow/promisestream callbacks") {
	PromiseStream<int> p;

	int result = 0;

	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e) { result = -1; });

	ASSERT(result == 0);

	p.send(123);
	p.send(456);

	ASSERT(result == 123);
	result = 0;

	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e) { result = -1; });

	ASSERT(result == 456);
	result = 0;

	onReady(p.getFuture(), [&result](int x) { result = x; }, [&result](Error e) { result = -1; });

	ASSERT(result == 0);

	p = PromiseStream<int>();

	ASSERT(result == -1);
	return Void();
}

// Incompatible with --crash, so we are commenting it out for now
/*
TEST_CASE("/flow/flow/promisestream multiple wait error")
{
    int result = 0;
    PromiseStream<int> p;
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

TEST_CASE("/flow/flow/trivial actors") {
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
	voidWaitActor(2, &result);
	ASSERT(result == 2);

	Promise<int> p;
	f = addOneActor(p.getFuture());
	ASSERT(!f.isReady());
	p.send(100);
	ASSERT(f.isReady() && f.get() == 101);
	f = Future<int>();

	PromiseStream<int> ps;
	f = consumeOneActor(ps.getFuture());
	ASSERT(!f.isReady());
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

TEST_CASE("/flow/flow/yieldedFuture/progress") {
	// Check that if check_yield always returns true, the yieldedFuture will do nothing immediately but will
	// get one thing done per "tick" (per delay(0) returning).

	auto yn = makeReference<YieldMockNetwork>();

	yn->nextYield = 0;

	Promise<Void> p;
	Future<Void> u = p.getFuture();
	Future<Void> i = success(u);

	std::vector<Future<Void>> v;
	for (int i = 0; i < 5; i++)
		v.push_back(yieldedFuture(u));
	auto numReady = [&v]() { return std::count_if(v.begin(), v.end(), [](Future<Void> v) { return v.isReady(); }); };

	ASSERT(numReady() == 0);
	p.send(Void());
	ASSERT(u.isReady() && i.isReady() && numReady() == 0);

	for (int i = 0; i < 5; i++) {
		yn->tick();
		ASSERT(numReady() == i + 1);
	}

	for (int i = 0; i < 5; i++) {
		ASSERT(v[i].getPromiseReferenceCount() == 0 && v[i].getFutureReferenceCount() == 1);
	}

	return Void();
}

TEST_CASE("/flow/flow/yieldedFuture/random") {
	// Check expectations about exactly how yieldedFuture responds to check_yield results

	auto yn = makeReference<YieldMockNetwork>();

	for (int r = 0; r < 100; r++) {
		Promise<Void> p;
		Future<Void> u = p.getFuture();
		Future<Void> i = success(u);

		std::vector<Future<Void>> v;
		for (int i = 0; i < 25; i++)
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

		for (int i = 0; i < v.size(); i++) {
			ASSERT(v[i].getPromiseReferenceCount() == 0 && v[i].getFutureReferenceCount() == 1);
		}
	}

	return Void();
}

TEST_CASE("/flow/perf/yieldedFuture") {
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
	for (int i = 0; i < N; i++)
		yieldedFuture(f);
	printf("yieldedFuture(f) cancel: %0.1f M/sec\n", N / 1e6 / (timer() - start));

	return Void();
}

TEST_CASE("/flow/flow/chooseTwoActor") {
	Promise<Void> a, b;
	Future<Void> c = chooseTwoActor(a.getFuture(), b.getFuture());
	// Parameters, race inputs, and callbacks each retain a reference while suspended.
	ASSERT(a.getFutureReferenceCount() == 3 && b.getFutureReferenceCount() == 3 && !c.isReady());
	b.send(Void());
	ASSERT(a.getFutureReferenceCount() == 1 && b.getFutureReferenceCount() == 1 && c.isReady() && !c.isError());
	c = Future<Void>();
	ASSERT(a.getFutureReferenceCount() == 0 && b.getFutureReferenceCount() == 0);
	return Void();
}

TEST_CASE("#flow/flow/perf/actor patterns") {
	double start;
	int N = 1000000;

	start = timer();
	for (int i = 0; i < N; i++)
		emptyVoidActor();
	printf("emptyVoidActor(): %0.1f M/sec\n", N / 1e6 / (timer() - start));

	start = timer();
	for (int i = 0; i < N; i++) {
		emptyActor();
	}
	printf("emptyActor(): %0.1f M/sec\n", N / 1e6 / (timer() - start));

	Promise<Void> neverSet;
	Future<Void> never = neverSet.getFuture();
	Future<Void> already = Void();

	start = timer();
	for (int i = 0; i < N; i++)
		oneWaitVoidActor(already);
	printf("oneWaitVoidActor(already): %0.1f M/sec\n", N / 1e6 / (timer() - start));

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
			if (!onchanges.empty()) {
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

TEST_CASE("/flow/flow/YieldedAsyncMap/randomized") {
	YAMRandom<YieldedAsyncMap<int, int>> yamr;
	for (int it = 0; it < 100000; it++) {
		yamr.randomOp();
		co_await yield();
	}
}

TEST_CASE("/flow/flow/AsyncMap/randomized") {
	YAMRandom<AsyncMap<int, int>> yamr;
	for (int it = 0; it < 100000; it++) {
		yamr.randomOp();
		co_await yield();
	}
}

TEST_CASE("/flow/flow/YieldedAsyncMap/basic") {
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

TEST_CASE("/flow/flow/YieldedAsyncMap/cancel") {
	YieldedAsyncMap<int, int> yam;
	// ASSERT(yam.count(1) == 0);
	// Future<Void> y0 = yam.onChange(1);
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

TEST_CASE("/flow/flow/YieldedAsyncMap/cancel2") {
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

TEST_CASE("/flow/flow/AsyncVar/basic") {
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

static Future<Void> waitAfterCancel(int* output) {
	*output = 0;
	bool cancelled = false;
	try {
		co_await Future<Void>(Never());
	} catch (...) {
		cancelled = true;
	}
	if (cancelled) {
		co_await (*output = 1, Future<Void>(Void()));
	}
	ASSERT(false);
}

TEST_CASE("/fdbrpc/flow/wait_expression_after_cancel_flow") {
	int a = -1;
	Future<Void> f = waitAfterCancel(&a);
	ASSERT(a == 0);
	f.cancel();
	ASSERT(a == 1);
	ASSERT(f.isReady() && f.isError() && f.getError().code() == error_code_actor_cancelled);
	return Void();
}

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
	explicit Tracker(int copied = 0) : copied(copied), moved(false) {}
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

	static Future<Void> listen(FutureStream<Tracker> stream, int expectedCopies) {
		Tracker movedTracker = co_await stream;
		ASSERT(!movedTracker.moved);
		ASSERT(movedTracker.copied == expectedCopies);
	}
};

TEST_CASE("/flow/flow/PromiseStream/move") {
	PromiseStream<Tracker> stream;
	Future<Void> listener;
	{
		// This tests the case when a callback is added before
		// a movable value is sent
		listener = Tracker::listen(stream.getFuture(), 0);
		stream.send(Tracker{});
		co_await listener;
	}

	{
		// This tests the case when a callback is added before
		// an lvalue is copied into the coroutine's awaiter storage.
		listener = Tracker::listen(stream.getFuture(), 1);
		Tracker namedTracker;
		stream.send(namedTracker);
		co_await listener;
	}
	{
		// This tests the case when no callback is added until
		// after a movable value is sent
		stream.send(Tracker{});
		stream.send(Tracker{});
		{
			Tracker movedTracker = co_await stream.getFuture();
			ASSERT(!movedTracker.moved);
			ASSERT(movedTracker.copied == 0);
		}
		{
			Tracker movedTracker = co_await stream.getFuture();
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
			Tracker copiedTracker = co_await stream.getFuture();
			ASSERT(!copiedTracker.moved);
			// must copy onto queue
			ASSERT(copiedTracker.copied == 1);
		}
		{
			Tracker copiedTracker = co_await stream.getFuture();
			ASSERT(!copiedTracker.moved);
			// must copy onto queue
			ASSERT(copiedTracker.copied == 1);
		}
	}
}

TEST_CASE("/flow/flow/PromiseStream/move2") {
	PromiseStream<Tracker> stream;
	stream.send(Tracker{});
	Tracker tracker = co_await stream.getFuture();
	Tracker movedTracker = std::move(tracker);
	ASSERT(
	    tracker.moved); // NOLINT(bugprone-use-after-move): this test intentionally checks the moved-from Tracker state.
	ASSERT(!movedTracker.moved);
	ASSERT(movedTracker.copied == 0);
}

constexpr double mutexTestDelay = 0.00001;

Future<Void> mutexTest(int id, FlowMutex* mutex, int n, bool allowError, bool* verbose) {
	FlowMutex::Lock lock;
	while (n-- > 0) {
		double d = deterministicRandom()->random01() * mutexTestDelay;
		if (*verbose) {
			printf("%d:%d wait %f while unlocked\n", id, n, d);
		}
		co_await delay(d);

		if (*verbose) {
			printf("%d:%d locking\n", id, n);
		}
		lock = co_await mutex->take();
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

TEST_CASE("/flow/flow/FlowMutex") {
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
					co_await success(mutex.take());
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

using namespace std::chrono_literals;

TEST_CASE("/flow/thread/ThreadReturnPromiseStream_Simple") {
	AsyncTaskExecutor exc(1);
	noUnseed = true;
	ThreadReturnPromiseStream<int> stream;
	ThreadFutureStream<int> f = stream.getFuture();
	Future<Void> t = exc.post([stream = std::move(stream)]() mutable {
		for (int i = 0; i < 10; ++i) {
			stream.send(i);
			std::cout << "Sent: " << i << std::endl;
			std::this_thread::sleep_for(10ms);
		}
		stream.sendError(end_of_stream());
		return Void();
	});

	int n = 0;
	while (true) {
		try {
			int i = co_await f;
			std::cout << "Got: " << i << std::endl;
			co_await delay(0.1);
			ASSERT(i == n);
			++n;
		} catch (Error& e) {
			ASSERT(e.code() == error_code_end_of_stream);
			ASSERT(n == 10);
			break;
		}
	}

	co_await t;
}

TEST_CASE("/flow/thread/ThreadReturnPromiseStream_Seq") {
	AsyncTaskExecutor exc(1);
	noUnseed = true;
	ThreadReturnPromiseStream<int> stream;
	ThreadFutureStream<int> f = stream.getFuture();
	Future<Void> t = exc.post([stream = std::move(stream)]() mutable {
		for (int i = 0; i <= 3; ++i) {
			stream.send(i);
			std::this_thread::sleep_for(100ms);
		}
		stream.sendError(end_of_stream());
		return Void();
	});

	int r0 = co_await f;
	ASSERT(r0 == 0);
	int r1 = co_await f;
	ASSERT(r1 == 1);
	int r2 = co_await f;
	ASSERT(r2 == 2);
	co_await delay(1.0);
	int r3 = co_await f;
	ASSERT(r3 == 3);
	co_await t;
}

TEST_CASE("/flow/thread/ThreadReturnPromiseStream_Error") {
	AsyncTaskExecutor exc(1);
	noUnseed = true;

	{
		ThreadReturnPromiseStream<int> s1;
		ThreadFutureStream<int> f1 = s1.getFuture();
		Future<Void> t1 = exc.post([s1 = std::move(s1)]() mutable {
			std::this_thread::sleep_for(2s);
			return Void();
		});

		try {
			co_await f1;
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_broken_promise);
		}

		co_await t1;
	}

	{
		ThreadReturnPromiseStream<int> s2;
		ThreadFutureStream<int> f2 = s2.getFuture();
		Future<Void> t2 = exc.post([s2 = std::move(s2)]() mutable {
			std::this_thread::sleep_for(2s);
			s2.sendError(transaction_too_old());
			return Void();
		});

		try {
			co_await f2;
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_transaction_too_old);
		}

		co_await t2;
	}
}

TEST_CASE("/fdbrpc/waitValueOrSignal/peerDisconnect") {
	// Test that waitValueOrSignal detects peer disconnect and returns request_maybe_delivered.
	// This reproduces the bug where DD would hang forever because waitValueOrSignal didn't
	// watch peer->disconnect, so dead connections (e.g., from K8s NAT timeouts) were never detected.

	// Construct a minimal Peer. We pass nullptr for TransportData because this test only relies on
	// peer->disconnect, and PeerHolder only touches outstandingReplies. Note that Peer construction
	// also updates the global failure monitor status for fakeAddr.
	NetworkAddress fakeAddr = NetworkAddress::parse("1.2.3.4:1234");
	Reference<Peer> peer = makeReference<Peer>(nullptr, fakeAddr);

	// Create a value future that never resolves (simulating a stuck RPC to unreachable storage server)
	Promise<Void> neverReply;

	// No failure signal either (simulating failure monitor not yet detecting the failure)
	Endpoint ep;

	// Call waitValueOrSignal with the peer
	Future<ErrorOr<Void>> result = waitValueOrSignal(neverReply.getFuture(), Never(), ep, ReplyPromise<Void>(), peer);

	// Result should not be ready yet - the reply hasn't come and disconnect hasn't fired
	ASSERT(!result.isReady());

	// Simulate peer disconnection (as would happen when connectionReader/connectionKeeper detects failure)
	peer->disconnect.send(Void());

	// Now waitValueOrSignal should detect the disconnect and return request_maybe_delivered
	ASSERT(result.isReady());
	ErrorOr<Void> r = result.get();
	ASSERT(r.isError());
	ASSERT(r.getError().code() == error_code_request_maybe_delivered);

	return Void();
}

TEST_CASE("/flow/IThreadPool/ThreadReturnPromiseStream_DestroyPromise") {
	noUnseed = true;

	{
		std::cout << "ThreadReturnPromiseStream with future > 0, promise == 0, end_of_stream sent\n";
		// After all references to PromiseStream are gone, FutureStream should still be able to get
		// all the values if PromiseStream had reached end_of_stream.
		ThreadFutureStream<int> fs1 = ([]() {
			ThreadReturnPromiseStream<int> p;
			auto ret = p.getFuture();
			for (int i = 0; i < 10; i++) {
				p.send(i);
			}
			p.sendError(end_of_stream());
			ASSERT(p.getFutureReferenceCount() == 1);
			return ret;
		})();

		co_await delay(1);

		int recvd = 0;
		try {
			while (true) {
				co_await fs1;
				recvd += 1;
			}
		} catch (Error& err) {
			if (err.code() == error_code_end_of_stream) {
				ASSERT_EQ(recvd, 10);
			} else {
				ASSERT(false);
			}
		}
	}

	std::cout << "ThreadReturnPromiseStream with future > 0,  promise == 0, no end_of_stream\n";
	{
		// After all references to PromiseStream are gone, but the stream was not ended cleanly
		// by sending end_of_stream, we should instead get broken_promise.
		ThreadFutureStream<int> fs2 = ([]() {
			ThreadReturnPromiseStream<int> p;
			auto ret = p.getFuture();
			for (int i = 0; i < 10; i++) {
				p.send(i);
			}
			ASSERT(p.getFutureReferenceCount() == 1);
			return ret;
		})();

		co_await delay(1);

		try {
			while (true) {
				if (fs2.isReady()) {
					co_await fs2;
				} else {
					auto res = co_await race(fs2, delay(1));
					if (res.index() == 1) {
						ASSERT(false); // shouldn't hangup if end_of_stream is not sent.
						break;
					}
				}
			}
		} catch (Error& err) {
			ASSERT(err.code() == error_code_broken_promise);
		}
	}
}

TEST_CASE("/fdbrpc/waitValueOrSignal/noPeerFallback") {
	// Test that waitValueOrSignal still works correctly when no peer is provided (the default).
	// The broken_promise path should still be handled: when the reply promise breaks,
	// the endpoint should be marked as not found and value set to Never().

	Promise<Void> reply;

	// Call waitValueOrSignal without a peer (default behavior)
	Future<ErrorOr<Void>> result = waitValueOrSignal(reply.getFuture(), Never(), Endpoint());

	ASSERT(!result.isReady());

	// Break the promise (simulating endpoint failure)
	reply.sendError(broken_promise());

	// waitValueOrSignal should handle broken_promise by setting value = Never() and looping.
	// Since signal is Never() and there's no peer disconnect, it should now wait forever.
	// We verify it doesn't crash and the result is NOT ready (it's stuck in the loop).
	co_await delay(0.1);
	ASSERT(!result.isReady());
}

TEST_CASE("/fdbrpc/waitValueOrSignal/retryOnDisconnect") {
	// End-to-end test of the retry pattern that loadBalance uses:
	// 1. First attempt: RPC to peer1, peer1 disconnects → request_maybe_delivered
	// 2. Second attempt: RPC to peer2, peer2 responds → success
	// 3. Verify numAttempts > 1
	//
	// This reproduces the exact scenario in production: DD sends waitMetrics to a storage
	// server, the K8s NAT kills the connection at ~3 minutes, and the fix ensures the
	// request_maybe_delivered error propagates so loadBalance can retry on another server.

	NetworkAddress addr1 = NetworkAddress::parse("1.2.3.4:1234");
	NetworkAddress addr2 = NetworkAddress::parse("1.2.3.5:1234");
	Reference<Peer> peer1 = makeReference<Peer>(nullptr, addr1);
	Reference<Peer> peer2 = makeReference<Peer>(nullptr, addr2);

	int numAttempts = 0;

	// --- Attempt 1: peer1 disconnects mid-request (simulating K8s NAT timeout) ---
	{
		Promise<Void> reply1;
		Endpoint ep1;

		Future<ErrorOr<Void>> result1 =
		    waitValueOrSignal(reply1.getFuture(), Never(), ep1, ReplyPromise<Void>(), peer1);

		numAttempts++;
		ASSERT(!result1.isReady());

		// Connection killed by external factor (K8s NAT, network partition, etc.)
		peer1->disconnect.send(Void());

		// waitValueOrSignal must detect the disconnect and return request_maybe_delivered
		ASSERT(result1.isReady());
		ErrorOr<Void> r1 = result1.get();
		ASSERT(r1.isError());
		ASSERT(r1.getError().code() == error_code_request_maybe_delivered);
	}

	// --- Attempt 2: retry to peer2, which responds successfully ---
	// This is what loadBalance does: on request_maybe_delivered, pick next alternative and retry
	{
		Promise<Void> reply2;
		Endpoint ep2;

		Future<ErrorOr<Void>> result2 =
		    waitValueOrSignal(reply2.getFuture(), Never(), ep2, ReplyPromise<Void>(), peer2);

		numAttempts++;

		// Second server is healthy and responds
		reply2.send(Void());

		ASSERT(result2.isReady());
		ErrorOr<Void> r2 = result2.get();
		ASSERT(r2.present()); // Success!
	}

	// The critical assertion: retries happened (numAttempts > 1)
	ASSERT_GE(numAttempts, 2);
	TraceEvent("WaitValueOrSignalRetryTest").detail("NumAttempts", numAttempts);

	return Void();
}
