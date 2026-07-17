/*
 * genericactors.actor.h
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

#include "flow/CodeProbe.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/TaskPriority.h"
#include "flow/network.h"
#include "flow/swift_support.h"
#include <utility>
#include <functional>
#include <unordered_set>

#include <list>
#include <optional>
#include <type_traits>

#include "flow/flow.h"
#include "flow/CoroUtils.h"
#include "flow/Knobs.h"
#include "flow/Util.h"
#include "flow/IndexedSet.h"

namespace coro {
template <class... Futures>
using RaceResult = std::variant<FutureReturnTypeT<std::decay_t<Futures>>...>;
}

template <class... Futures>
[[nodiscard]] auto race(Futures&&... futures) -> Future<coro::RaceResult<std::decay_t<Futures>...>>;

#ifdef _MSC_VER
#pragma warning(disable : 4355) // 'this' : used in base member initializer list
#endif

template <class T>
Future<T> traceAfter(Future<T> what, std::string type, bool traceErrors = true, ExplicitVoid = {}) {
	try {
		T val = co_await what;
		TraceEvent(type.c_str());
		co_return val;
	} catch (Error& e) {
		// Don't trace operation_cancelled as it's a normal control flow mechanism, not an error
		if (traceErrors && e.code() != error_code_operation_cancelled) {
			TraceEvent(type.c_str()).errorUnsuppressed(e);
		}
		throw;
	}
}

template <class T>
Future<Optional<T>> stopAfter(Future<T> what, ExplicitVoid = {}) {
	Optional<T> ret = T();
	try {
		T result = co_await what;
		ret = Optional<T>(std::move(result));
	} catch (Error& e) {
		bool ok = e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete ||
		          e.code() == error_code_actor_cancelled || e.code() == error_code_local_config_changed;
		TraceEvent(ok ? SevInfo : SevError, "StopAfterError").error(e);
		if (!ok) {
			fprintf(stderr, "Fatal Error: %s\n", e.what());
			ret = Optional<T>();
		}
	}
	g_network->stop();
	co_return ret;
}

template <class T>
T sorted(T range) {
	std::sort(range.begin(), range.end());
	return range;
}

template <class T>
std::vector<T> parseStringToVector(std::string str, char delim) {
	std::vector<T> result;
	std::stringstream stream(str);
	std::string token;
	while (stream.good()) {
		getline(stream, token, delim);
		std::istringstream tokenStream(token);
		T item;
		tokenStream >> item;
		result.push_back(item);
	}
	return result;
}

template <class T>
std::unordered_set<T> parseStringToUnorderedSet(std::string str, char delim) {
	std::unordered_set<T> result;
	std::stringstream stream(str);
	std::string token;
	while (stream.good()) {
		getline(stream, token, delim);
		std::istringstream tokenStream(token);
		T item;
		tokenStream >> item;
		result.emplace(item);
	}
	return result;
}

template <class T>
ErrorOr<T> errorOr(T t) {
	return ErrorOr<T>(t);
}

template <class T>
AsyncResult<ErrorOr<T>> errorOr(AsyncResult<T> result, ExplicitVoid = {}) {
	try {
		co_return ErrorOr<T>(co_await std::move(result));
	} catch (Error& e) {
		co_return ErrorOr<T>(e);
	}
}

template <class T>
Future<ErrorOr<T>> errorOr(Future<T> f, ExplicitVoid = {}) {
	try {
		T t = co_await f;
		co_return ErrorOr<T>(t);
	} catch (Error& e) {
		co_return ErrorOr<T>(e);
	}
}

template <class T>
Future<T> throwErrorOr(Future<ErrorOr<T>> f, ExplicitVoid = {}) {
	ErrorOr<T> t = co_await f;
	if (t.isError())
		throw t.getError();
	co_return t.get();
}

template <class T>
Future<T> transformErrors(Future<T> f, Error err, ExplicitVoid = {}) {
	try {
		T t = co_await f;
		co_return t;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw e;
		throw err;
	}
}

template <class T>
Future<T> transformError(Future<T> f, Error inErr, Error outErr, ExplicitVoid = {}) {
	try {
		T t = co_await f;
		co_return t;
	} catch (Error& e) {
		if (e.code() == inErr.code())
			throw outErr;
		throw e;
	}
}

// Note that the RequestStream<T> version of forwardPromise doesn't exist, because what to do with errors?

template <class T>
Future<Void> forwardEvent(Event* ev, Future<T> input, Uncancellable = Uncancellable()) {
	try {
		co_await input;
	} catch (Error&) {
	}
	ev->set();
}

template <class T>
Future<Void> forwardEvent(Event* ev, T* t, Error* err, FutureStream<T> input, Uncancellable = Uncancellable()) {
	try {
		T value = co_await input;
		*t = std::move(value);
		ev->set();
	} catch (Error& e) {
		*err = e;
		ev->set();
	}
}

template <class T>
Future<Void> waitForAllReady(std::vector<Future<T>> results) {
	for (const auto& result : results) {
		if (!result.isReady()) {
			co_await coro::errorOr(coro::ignore(result));
		}
	}
}

template <class T>
Future<T> timeout(Future<T> what,
                  double time,
                  T timedoutValue,
                  TaskPriority taskID = TaskPriority::DefaultDelay,
                  ExplicitVoid = {}) {
	Future<Void> end = delay(time, taskID);
	auto res = co_await race(what, end);
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	co_return timedoutValue;
}

template <class T>
Future<Optional<T>> timeout(Future<T> what,
                            double time,
                            TaskPriority taskID = TaskPriority::DefaultDelay,
                            ExplicitVoid = {}) {
	Future<Void> end = delay(time, taskID);
	auto res = co_await race(what, end);
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	co_return Optional<T>();
}

template <class T>
Future<T> timeoutError(Future<T> what,
                       double time,
                       TaskPriority taskID = TaskPriority::DefaultDelay,
                       ExplicitVoid = {}) {
	Future<Void> end = delay(time, taskID);
	auto res = co_await race(what, end);
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	throw timed_out();
}

template <class T>
AsyncResult<T> timeoutError(AsyncResult<T> what,
                            double time,
                            TaskPriority taskID = TaskPriority::DefaultDelay,
                            ExplicitVoid = {}) {
	if (what.canGet()) {
		co_return what.get();
	} else if (what.isError()) {
		throw what.getError();
	}
	auto res = co_await race(std::move(what), delay(time, taskID));
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	} else {
		throw timed_out();
	}
}

template <class T>
Future<T> delayed(Future<T> what,
                  double time = 0.0,
                  TaskPriority taskID = TaskPriority::DefaultDelay,
                  ExplicitVoid = {}) {
	Error err;
	try {
		T t = co_await what;
		co_await delay(time, taskID);
		co_return t;
	} catch (Error& e) {
		err = e;
	}
	co_await delay(time, taskID);
	throw err;
}

template <class Func>
Future<Void> trigger(Func what, Future<Void> signal) {
	co_await signal;
	what();
}

// Waits for a future to complete and cannot be cancelled
// Most situations will use the overload below, which does not require a promise
template <class T>
Future<Void> uncancellable(Future<T> what, Promise<T> result, Uncancellable = Uncancellable()) {
	ErrorOr<T> value = co_await coro::errorOr(what);
	if (value.present()) {
		result.send(std::move(value).get());
	} else {
		result.sendError(value.getError());
	}
}

// Waits for a future to complete and cannot be cancelled
template <class T>
Future<T> uncancellable(Future<T> what, ExplicitVoid = {}) {
	Promise<T> resultPromise;
	Future<T> result = resultPromise.getFuture();

	uncancellable(what, resultPromise);
	co_return co_await result;
}

// Holds onto an object until a future either completes or is cancelled
// Used to prevent the object from being reclaimed
//
// NOTE: the order of the arguments is important. The arguments will be destructed in
// reverse order, and we need the object to be destructed last.
template <class T, class X>
Future<T> holdWhile(X object, Future<T> what, ExplicitVoid = {}) {
	co_return co_await what;
}

// Assign the future value of what to out
template <class T, class X>
[[nodiscard]] Future<Void> store(X& out, Future<T> what) {
	return map(what, [&out](T const& v) {
		out = v;
		return Void();
	});
}

template <class T>
Future<Void> storeOrThrow(T& out, Future<Optional<T>> what, Error e = key_not_found()) {
	return map(what, [&out, e](Optional<T> const& o) {
		if (!o.present())
			throw e;
		out = o.get();
		return Void();
	});
}

// Waits for a future to be ready, and then applies an asynchronous function to it.
template <class T, class F>
Future<decltype(std::declval<F>()(std::declval<T>()).getValue())> mapAsync(Future<T> what,
                                                                           F actorFunc,
                                                                           ExplicitVoid = {}) {
	T val = co_await what;
	decltype(std::declval<F>()(std::declval<T>()).getValue()) ret = co_await actorFunc(val);
	co_return ret;
}

// maps a vector of futures with an asynchronous function
template <class T, class F>
auto mapAsync(std::vector<Future<T>> const& what, F const& actorFunc) {
	std::vector<std::invoke_result_t<F, T>> ret;
	ret.reserve(what.size());
	for (const auto& f : what)
		ret.push_back(mapAsync(f, actorFunc));
	return ret;
}

// maps a stream with an asynchronous function
template <class T, class F, class U = decltype(std::declval<F>()(std::declval<T>()).getValue())>
Future<Void> mapAsync(FutureStream<T> input, F actorFunc, PromiseStream<U> output) {
	Deque<Future<U>> futures;

	while (true) {
		try {
			auto res = co_await race(input, futures.size() == 0 ? Never() : futures.front());
			if (res.index() == 0) {
				T nextInput = std::get<0>(std::move(res));
				futures.push_back(actorFunc(nextInput));
			} else {
				U nextOutput = std::get<1>(std::move(res));
				output.send(nextOutput);
				futures.pop_front();
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			} else {
				output.sendError(e);
				throw e;
			}
		}
	}

	while (futures.size()) {
		U nextOutput = co_await futures.front();
		output.send(nextOutput);
		futures.pop_front();
	}

	output.sendError(end_of_stream());
}

// Waits for a future to be ready, and then applies a function to it.
template <class T, class F>
Future<std::invoke_result_t<F, T>> map(Future<T> what, F func, ExplicitVoid = {}) {
	T val = co_await what;
	co_return func(val);
}

// maps a vector of futures
template <class T, class F>
auto map(std::vector<Future<T>> const& what, F const& func) {
	std::vector<Future<std::invoke_result_t<F, T>>> ret;
	ret.reserve(what.size());
	for (const auto& f : what)
		ret.push_back(map(f, func));
	return ret;
}

// maps a stream
template <class T, class F>
Future<Void> map(FutureStream<T> input, F func, PromiseStream<std::invoke_result_t<F, T>> output) {
	while (true) {
		try {
			T nextInput = co_await input;
			output.send(func(nextInput));
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			} else
				throw;
		}
	}

	output.sendError(end_of_stream());
}

// X + Y will wait for X, then wait for and return the result of Y
template <class A, class B>
Future<B> operatorPlus(Future<A> a, Future<B> b, ExplicitVoid = {}) {
	A resultA = co_await a;
	(void)resultA;
	B resultB = co_await b;
	co_return resultB;
}

template <class A, class B>
Future<B> operator+(Future<A> a, Future<B> b) {
	return operatorPlus(a, b);
}

// Returns if the future returns true, otherwise waits forever.
Future<Void> returnIfTrue(Future<bool> const& f);

// Returns if the future, when waited on and then evaluated with the predicate, returns true, otherwise waits forever
template <class T, class F>
Future<Void> returnIfTrue(Future<T> what, F pred) {
	return returnIfTrue(map(what, pred));
}

template <class T>
struct WorkerCache {
	// SOMEDAY: Would we do better to use "unreliable" (at most once) transport for the initialize requests and get rid
	// of this? It doesn't provide true at most once behavior because things are removed from the cache after they have
	// terminated.
	bool exists(UID id) { return id_interface.count(id) != 0; }
	void set(UID id, const Future<T>& onReady) {
		ASSERT(!exists(id));
		id_interface[id] = onReady;
	}
	Future<T> get(UID id) {
		ASSERT(exists(id));
		return id_interface[id];
	}

	Future<Void> removeOnReady(UID id, Future<Void> const& ready) { return removeOnReady(this, id, ready); }

private:
	static Future<Void> removeOnReady(WorkerCache* self, UID id, Future<Void> ready) {
		try {
			co_await ready;
			self->id_interface.erase(id);
		} catch (Error& e) {
			self->id_interface.erase(id);
			throw;
		}
	}

	std::map<UID, Future<T>> id_interface;
};

template <class K, class V>
class AsyncMap : NonCopyable {
public:
	// Represents a complete function from keys to values (K -> V)
	// All values not explicitly inserted map to V()
	// If this isn't appropriate, use V=Optional<X>

	AsyncMap() : defaultValue(), destructing(false) {}

	virtual ~AsyncMap() {
		destructing = true;
		items.clear();
	}

	void set(K const& k, V const& v) {
		auto& i = items[k];
		if (i.value != v)
			setUnconditional(k, v, i);
	}
	void setUnconditional(K const& k, V const& v) { setUnconditional(k, v, items[k]); }

	void sendError(K const& begin, K const& end, Error const& e) {
		if (begin >= end)
			return;
		std::vector<Promise<Void>> ps = swapRangePromises(items.lower_bound(begin), items.lower_bound(end));
		sendError(ps, e);
	}

	void triggerAll() {
		std::vector<Promise<Void>> ps = swapRangePromises(items.begin(), items.end());
		send(ps);
	}

	void triggerRange(K const& begin, K const& end) {
		if (begin >= end)
			return;
		std::vector<Promise<Void>> ps = swapRangePromises(items.lower_bound(begin), items.lower_bound(end));
		send(ps);
	}

	void trigger(K const& key) {
		if (items.count(key) != 0) {
			auto& i = items[key];
			Promise<Void> trigger;
			i.change.swap(trigger);
			Promise<Void> noDestroy = trigger; // See explanation of noDestroy in setUnconditional()

			if (i.value == defaultValue)
				items.erase(key);

			trigger.send(Void());
		}
	}
	void clear(K const& k) { set(k, V()); }
	V const& get(K const& k) const {
		auto it = items.find(k);
		if (it != items.end())
			return it->second.value;
		else
			return defaultValue;
	}
	int count(K const& k) const {
		auto it = items.find(k);
		if (it != items.end())
			return 1;
		return 0;
	}
	virtual Future<Void> onChange(K const& k) { // throws broken_promise if this is destroyed
		auto& item = items[k];
		if (item.value == defaultValue)
			return destroyOnCancel(this, k, item.change.getFuture());
		return item.change.getFuture();
	}
	std::vector<K> getKeys() const {
		std::vector<K> keys;
		keys.reserve(items.size());
		for (auto i = items.begin(); i != items.end(); ++i)
			keys.push_back(i->first);
		return keys;
	}
	void resetNoWaiting() {
		for (auto i = items.begin(); i != items.end(); ++i)
			ASSERT(i->second.change.getFuture().getFutureReferenceCount() == 1);
		items.clear();
	}

protected:
	// Invariant: Every item in the map either has value!=defaultValue xor a destroyOnCancel actor waiting on
	// change.getFuture()
	struct P {
		V value;
		Promise<Void> change;
		P() : value() {}
	};
	std::map<K, P> items;
	const V defaultValue;
	bool destructing;

	template <typename Iterator>
	std::vector<Promise<Void>> swapRangePromises(Iterator begin, Iterator end) {
		std::vector<Promise<Void>> ps;
		for (auto it = begin; it != end; ++it) {
			ps.resize(ps.size() + 1);
			ps.back().swap(it->second.change);
		}
		return ps;
	}

	// ps can't be a reference. See explanation of noDestroy in setUnconditional()
	void send(std::vector<Promise<Void>> ps) {
		for (auto& p : ps) {
			p.send(Void());
		}
	}

	// ps can't be a reference. See explanation of noDestroy in setUnconditional()
	void sendError(std::vector<Promise<Void>> ps, Error const& e) {
		for (auto& p : ps) {
			p.sendError(e);
		}
	}

	void setUnconditional(K const& k, V const& v, P& i) {
		Promise<Void> trigger;
		i.change.swap(trigger);
		Promise<Void> noDestroy =
		    trigger; // The send(Void()) or even V::operator= could cause destroyOnCancel,
		             // which could undo the change to i.value here.  Keeping the promise reference count >= 2
		             // prevents destroyOnCancel from erasing anything from the map.
		if (v == defaultValue) {
			items.erase(k);
		} else {
			i.value = v;
		}

		trigger.send(Void());
	}

	Future<Void> destroyOnCancel(AsyncMap* self, K key, Future<Void> change) {
		try {
			co_await change;
			change = Future<Void>();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled && !self->destructing && change.getFutureReferenceCount() == 1 &&
			    change.getPromiseReferenceCount() == 1) {
				if (EXPENSIVE_VALIDATION) {
					auto& p = self->items[key];
					ASSERT(p.change.getFuture() == change);
				}
				self->items.erase(key);
			}
			change = Future<Void>();
			throw;
		}
	}
};

template <class V>
class ReferencedObject : NonCopyable, public ReferenceCounted<ReferencedObject<V>> {
public:
	ReferencedObject() : value() {}
	explicit ReferencedObject(V const& v) : value(v) {}
	explicit ReferencedObject(V&& v) : value(std::move(v)) {}
	explicit(false) ReferencedObject(ReferencedObject&& r) : value(std::move(r.value)) {}

	void operator=(ReferencedObject&& r) { value = std::move(r.value); }

	V const& get() const { return value; }

	V& mutate() { return value; }

	void set(V const& v) { value = v; }

	void set(V&& v) { value = std::move(v); }

	static Reference<ReferencedObject<V>> from(V const& v) { return makeReference<ReferencedObject<V>>(v); }

	static Reference<ReferencedObject<V>> from(V&& v) { return makeReference<ReferencedObject<V>>(std::move(v)); }

private:
	V value;
};

// FIXME(swift): Remove once https://github.com/apple/swift/issues/61620 is fixed.
#define SWIFT_CXX_REF_ASYNCVAR                                                                                         \
	__attribute__((swift_attr("import_reference"))) __attribute__((swift_attr("retain:immortal")))                     \
	__attribute__((swift_attr("release:immortal")))
// // TODO(swift): https://github.com/apple/swift/issues/62456 can't support retain/release funcs that are templates
// themselves
//    __attribute__((swift_attr("retain:addref_AsyncVar")))   \
//    __attribute__((swift_attr("release:delref_AsyncVar")))

template <class V>
class SWIFT_CXX_REF_ASYNCVAR AsyncVar : NonCopyable, public ReferenceCounted<AsyncVar<V>> {
public:
	AsyncVar() : value() {}
	AsyncVar(V const& v) : value(v) {}
	AsyncVar(AsyncVar&& av) : value(std::move(av.value)), nextChange(std::move(av.nextChange)) {}
	void operator=(AsyncVar&& av) {
		value = std::move(av.value);
		nextChange = std::move(av.nextChange);
	}

	V const& get() const { return value; }
	V getCopy() const __attribute__((swift_attr("import_unsafe"))) { return value; }
	Future<Void> onChange() const { return nextChange.getFuture(); }
	void set(V const& v) {
		if (v != value)
			setUnconditional(v);
	}
	void setUnconditional(V const& v) {
		Promise<Void> t;
		this->nextChange.swap(t);
		this->value = v;
		t.send(Void());
	}
	void trigger() {
		Promise<Void> t;
		this->nextChange.swap(t);
		t.send(Void());
	}

private:
	V value;
	Promise<Void> nextChange;
};

class AsyncTrigger : NonCopyable {
public:
	AsyncTrigger() {}
	explicit(false) AsyncTrigger(AsyncTrigger&& at) : v(std::move(at.v)) {}
	void operator=(AsyncTrigger&& at) { v = std::move(at.v); }
	Future<Void> onTrigger() const { return v.onChange(); }
	void trigger() { v.trigger(); }

private:
	AsyncVar<Void> v;
};

// Binds an AsyncTrigger object to an AsyncVar, so when the AsyncVar changes
// the AsyncTrigger is triggered.
template <class T>
Future<Void> forward(Reference<AsyncVar<T> const> from, AsyncTrigger* to) {
	while (true) {
		co_await from->onChange();
		to->trigger();
	}
}

class Debouncer : NonCopyable {
public:
	explicit Debouncer(double delay) { worker = debounceWorker(this, delay); }
	explicit(false) Debouncer(Debouncer&& at) = default;
	Debouncer& operator=(Debouncer&& at) = default;
	Future<Void> onTrigger() { return output.onChange(); }
	void trigger() { input.setUnconditional(Void()); }

private:
	AsyncVar<Void> input;
	AsyncVar<Void> output;
	Future<Void> worker;

	Future<Void> debounceWorker(Debouncer* self, double bounceTime) {
		while (true) {
			co_await self->input.onChange();
			while (true) {
				auto res = co_await race(self->input.onChange(), delay(bounceTime));
				if (res.index() == 1) {
					break;
				}
			}
			self->output.setUnconditional(Void());
		}
	}
};

template <class T>
Future<Void> asyncDeserialize(Reference<AsyncVar<Standalone<StringRef>>> input,
                              Reference<AsyncVar<Optional<T>>> output) {
	while (true) {
		if (input->get().size()) {
			ObjectReader reader(input->get().begin(), IncludeVersion());
			T res;
			reader.deserialize(res);
			output->set(res);
		} else
			output->set(Optional<T>());
		co_await input->onChange();
	}
}

template <class T>
Future<Void> delayedAsyncVar(Reference<AsyncVar<T>> in, Reference<AsyncVar<T>> out, double time) {
	try {
		while (true) {
			co_await delay(time);
			out->set(in->get());
			co_await in->onChange();
		}
	} catch (Error& e) {
		out->set(in->get());
		throw;
	}
}

template <class T>
Future<Void> setAfter(Reference<AsyncVar<T>> var, double time, T val) {
	co_await delay(time);
	var->set(val);
}

template <class T>
Future<Void> resetAfter(Reference<AsyncVar<T>> var,
                        double time,
                        T val,
                        int warningLimit = -1,
                        double warningResetDelay = 0,
                        const char* context = nullptr) {
	bool isEqual = var->get() == val;
	Future<Void> resetDelay = isEqual ? Never() : delay(time);
	int resetCount = 0;
	double lastReset = now();
	while (true) {
		auto res = co_await race(resetDelay, var->onChange());
		if (res.index() == 0) {
			var->set(val);
			if (now() - lastReset > warningResetDelay) {
				resetCount = 0;
			}
			resetCount++;
			if (context && warningLimit >= 0 && resetCount > warningLimit) {
				TraceEvent(SevWarnAlways, context)
				    .detail("ResetCount", resetCount)
				    .detail("LastReset", now() - lastReset);
			}
			lastReset = now();
			isEqual = true;
			resetDelay = Never();
		}
		if (isEqual && var->get() != val) {
			isEqual = false;
			resetDelay = delay(time);
		}
		if (!isEqual && var->get() == val) {
			isEqual = true;
			resetDelay = Never();
		}
	}
}

template <class T>
Future<Void> setWhenDoneOrError(Future<Void> condition, Reference<AsyncVar<T>> var, T val) {
	try {
		co_await condition;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
	}
	var->set(val);
}

Future<Void> lowPriorityDelay(double waitTime);

// Delay after condition is cleared (i.e. equal to false).
// If during delay, condition changes to true, wait till condition become false again, and repeat.
Future<Void> delayAfterCleared(Reference<AsyncVar<bool>> const& condition,
                               double const& time,
                               TaskPriority const& taskID = TaskPriority::DefaultDelay);

// Same as delayAfterCleared, but use lowPriorityDelay.
Future<Void> lowPriorityDelayAfterCleared(Reference<AsyncVar<bool>> const& condition, double const& time);

Future<bool> allTrue(std::vector<Future<bool>> all);
Future<Void> anyTrue(std::vector<Reference<AsyncVar<bool>>> const& input, Reference<AsyncVar<bool>> const& output);
Future<Void> cancelOnly(std::vector<Future<Void>> futures);
Future<Void> timeoutWarningCollector(FutureStream<Void> const& input,
                                     double const& logDelay,
                                     const char* const& context,
                                     UID const& id);
Future<bool> quorumEqualsTrue(std::vector<Future<bool>> const& futures, int const& required);

template <class T>
Future<Void> streamHelper(PromiseStream<T> output, PromiseStream<Error> errors, Future<T> input, ExplicitVoid = {}) {
	try {
		T value = co_await input;
		output.send(value);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		errors.send(e);
	}
	co_return Void();
}

template <class T>
Future<Void> makeStream(const std::vector<Future<T>>& futures, PromiseStream<T>& stream, PromiseStream<Error>& errors) {
	std::vector<Future<Void>> forwarders;
	forwarders.reserve(futures.size());
	for (int f = 0; f < futures.size(); f++)
		forwarders.push_back(streamHelper(stream, errors, futures[f]));
	return cancelOnly(forwarders);
}

template <class T>
class QuorumCallback;

// SAV-backed quorum bookkeeping for Future callbacks. AsyncResult quorum uses
// a separate state type because it owns producer cancellation.
template <class CallbackType>
struct QuorumState final : SAV<Void> {
	int antiQuorum;
	int count;

	static inline int sizeFor(int count) { return sizeof(QuorumState<CallbackType>) + sizeof(CallbackType) * count; }

	void destroy() override {
		int size = sizeFor(this->count);
		this->~QuorumState();
		freeFast(size, this);
	}
	void cancel() override {
		int cancelledCallbacks = 0;
		for (int i = 0; i < count; i++) {
			if (callbacks()[i].isRegistered()) {
				callbacks()[i].detach();
				++cancelledCallbacks;
			}
		}
		if (canBeSet())
			sendError(actor_cancelled());
		for (int i = 0; i < cancelledCallbacks; i++)
			delPromiseRef();
	}
	explicit QuorumState(int quorum, int count) : SAV<Void>(1, count), antiQuorum(count - quorum + 1), count(count) {
		if (!quorum)
			this->send(Void());
	}
	void oneSuccess() {
		if (getPromiseReferenceCount() == antiQuorum && canBeSet())
			this->sendAndDelPromiseRef(Void());
		else
			delPromiseRef();
	}
	void oneError(Error err) {
		if (canBeSet())
			this->sendErrorAndDelPromiseRef(err);
		else
			delPromiseRef();
	}

	CallbackType* callbacks() { return (CallbackType*)(this + 1); }
};

template <class T>
using Quorum = QuorumState<QuorumCallback<T>>;

template <class T>
class QuorumCallback : public Callback<T> {
public:
	void fire(const T& value) override {
		detach();
		head->oneSuccess();
	}
	void error(Error error) override {
		detach();
		head->oneError(error);
	}
	bool isRegistered() const { return Callback<T>::next != nullptr; }
	void detach() {
		if (isRegistered()) {
			Callback<T>::remove();
			Callback<T>::prev = nullptr;
			Callback<T>::next = nullptr;
		}
	}

private:
	template <class U>
	friend Future<Void> quorum(const Future<U>* pItems, int itemCount, int n);
	Quorum<T>* head;
	QuorumCallback() {
		Callback<T>::prev = nullptr;
		Callback<T>::next = nullptr;
	}
	QuorumCallback(Future<T> future, Quorum<T>* head) : head(head) { future.addCallbackAndClear(this); }
};

template <class T>
Future<Void> quorum(const Future<T>* pItems, int itemCount, int n) {
	ASSERT(n >= 0 && n <= itemCount);

	int size = Quorum<T>::sizeFor(itemCount);
	Quorum<T>* q = new (allocateFast(size)) Quorum<T>(n, itemCount);

	QuorumCallback<T>* nextCallback = q->callbacks();
	for (int i = 0; i < itemCount; ++i) {
		auto& r = pItems[i];
		if (r.isReady()) {
			new (nextCallback) QuorumCallback<T>();
			if (r.isError())
				q->oneError(r.getError());
			else
				q->oneSuccess();
		} else
			new (nextCallback) QuorumCallback<T>(r, q);
		++nextCallback;
	}
	return Future<Void>(q);
}

template <class T>
Future<Void> quorum(std::vector<Future<T>> const& results, int n) {
	return quorum(&results.front(), results.size(), n);
}

namespace coro {
template <class T>
struct QuorumAsyncResultCallback;

template <class T>
struct GetAllAsyncResultCallback;

template <class T>
struct GetAllAsyncResultStateCallback;
} // namespace coro

template <class T>
struct GetAllAsyncResultState;

template <class T>
struct QuorumAsyncResult final : SAV<Void> {
	int antiQuorum;
	int count;

	static inline int sizeFor(int count) {
		return sizeof(QuorumAsyncResult<T>) + sizeof(coro::QuorumAsyncResultCallback<T>) * count;
	}

	int detachPendingCallbacks() {
		int detachedCallbacks = 0;
		for (int i = 0; i < count; ++i) {
			if (callbacks()[i].isRegistered()) {
				callbacks()[i].detach();
				++detachedCallbacks;
			}
		}
		return detachedCallbacks;
	}

	void destroy() override {
		int size = sizeFor(this->count);
		this->~QuorumAsyncResult();
		freeFast(size, this);
	}
	void cancel() override {
		int cancelledCallbacks = detachPendingCallbacks();
		if (canBeSet())
			sendError(actor_cancelled());
		for (int i = 0; i < cancelledCallbacks; ++i)
			delPromiseRef();
	}
	explicit QuorumAsyncResult(int quorum, int count)
	  : SAV<Void>(1, count), antiQuorum(count - quorum + 1), count(count) {
		if (!quorum)
			this->send(Void());
	}
	void oneSuccess() {
		if (getPromiseReferenceCount() == antiQuorum && canBeSet()) {
			int cancelledCallbacks = detachPendingCallbacks();
			this->sendAndDelPromiseRef(Void());
			for (int i = 0; i < cancelledCallbacks; ++i)
				delPromiseRef();
		} else {
			delPromiseRef();
		}
	}
	void oneError(Error err) {
		if (canBeSet()) {
			int cancelledCallbacks = detachPendingCallbacks();
			this->sendErrorAndDelPromiseRef(err);
			for (int i = 0; i < cancelledCallbacks; ++i)
				delPromiseRef();
		} else {
			delPromiseRef();
		}
	}

	coro::QuorumAsyncResultCallback<T>* callbacks() { return (coro::QuorumAsyncResultCallback<T>*)(this + 1); }
};

namespace coro {
template <class T>
// AsyncResult has a single callback slot in its state, so quorum needs a
// callback that can unregister directly from AsyncResultState and cancel the
// producer if the aggregate no longer needs that result.
struct QuorumAsyncResultCallback final : AsyncResultCallback<typename AsyncResult<T>::StoredT> {
	using StoredT = typename AsyncResult<T>::StoredT;

	AsyncResultState<StoredT>* resultState = nullptr;
	QuorumAsyncResult<T>* head = nullptr;

	QuorumAsyncResultCallback() = default;
	QuorumAsyncResultCallback(AsyncResult<T>& result, QuorumAsyncResult<T>* head);

	void fire(StoredT const&) override;
	void fire(StoredT&&) override;
	void error(Error error) override;
	bool isRegistered() const { return resultState != nullptr; }
	void detach();
};
} // namespace coro

namespace coro {
template <class StoredT>
void detachAsyncResultStateCallback(AsyncResultState<StoredT>*& resultState, AsyncResultCallback<StoredT>* callback) {
	if (resultState) {
		auto* s = resultState;
		resultState = nullptr;
		s->clearCallback(callback);
		if (!s->isReady()) {
			s->cancelProducer();
		}
		s->delRef();
	}
}

template <class T>
QuorumAsyncResultCallback<T>::QuorumAsyncResultCallback(AsyncResult<T>& result, QuorumAsyncResult<T>* head)
  : resultState(result.resultState), head(head) {
	std::move(result).addCallbackAndClear(this);
}

template <class T>
void QuorumAsyncResultCallback<T>::detach() {
	detachAsyncResultStateCallback(resultState, this);
}

template <class T>
void QuorumAsyncResultCallback<T>::fire(StoredT const&) {
	detach();
	head->oneSuccess();
}

template <class T>
void QuorumAsyncResultCallback<T>::fire(StoredT&&) {
	detach();
	head->oneSuccess();
}

template <class T>
void QuorumAsyncResultCallback<T>::error(Error error) {
	detach();
	head->oneError(error);
}
} // namespace coro

template <class T>
Future<Void> quorum(AsyncResult<T>* pItems, int itemCount, int n) {
	ASSERT(n >= 0 && n <= itemCount);

	int size = QuorumAsyncResult<T>::sizeFor(itemCount);
	QuorumAsyncResult<T>* q = new (allocateFast(size)) QuorumAsyncResult<T>(n, itemCount);

	coro::QuorumAsyncResultCallback<T>* nextCallback = q->callbacks();
	for (int i = 0; i < itemCount; ++i) {
		auto& r = pItems[i];
		if (r.isReady()) {
			new (nextCallback) coro::QuorumAsyncResultCallback<T>();
			if (r.isError())
				q->oneError(r.getError());
			else
				q->oneSuccess();
			r = AsyncResult<T>();
		} else
			new (nextCallback) coro::QuorumAsyncResultCallback<T>(r, q);
		++nextCallback;
	}
	return Future<Void>(q);
}

// AsyncResult is single-consumer, so quorum requires an explicit ownership
// transfer from vector callers.
template <class T>
Future<Void> quorum(std::vector<AsyncResult<T>>& results, int n) = delete;

template <class T>
Future<Void> quorum(std::vector<AsyncResult<T>>&& results, int n) {
	return quorum(results.data(), results.size(), n);
}

template <class T>
// Collect AsyncResult values without first wrapping them in Future. Each slot
// is optional so move-only payloads can be transferred exactly once into the
// final output vector when the last callback fires.
struct GetAllAsyncResult final : SAV<std::vector<T>> {
	int remaining;
	int count;
	std::vector<std::optional<T>> values;

	static inline int sizeFor(int count) {
		return sizeof(GetAllAsyncResult<T>) + sizeof(coro::GetAllAsyncResultCallback<T>) * count;
	}

	int detachPendingCallbacks() {
		int detachedCallbacks = 0;
		for (int i = 0; i < count; ++i) {
			if (callbacks()[i].isRegistered()) {
				callbacks()[i].detach();
				++detachedCallbacks;
			}
		}
		return detachedCallbacks;
	}

	void destroy() override {
		int size = sizeFor(this->count);
		this->~GetAllAsyncResult();
		freeFast(size, this);
	}
	void cancel() override {
		int cancelledCallbacks = detachPendingCallbacks();
		if (this->canBeSet())
			this->sendError(actor_cancelled());
		for (int i = 0; i < cancelledCallbacks; ++i)
			this->delPromiseRef();
	}

	explicit GetAllAsyncResult(int count)
	  : SAV<std::vector<T>>(1, count), remaining(count), count(count), values(count) {}

	template <class U>
	void oneSuccess(int idx, U&& value) {
		values[idx].emplace(std::forward<U>(value));
		if (--remaining == 0 && this->canBeSet()) {
			std::vector<T> output;
			output.reserve(count);
			for (auto& item : values) {
				output.push_back(std::move(*item));
			}
			this->sendAndDelPromiseRef(std::move(output));
		} else {
			this->delPromiseRef();
		}
	}
	void oneError(Error err) {
		if (this->canBeSet()) {
			int cancelledCallbacks = detachPendingCallbacks();
			this->sendErrorAndDelPromiseRef(err);
			for (int i = 0; i < cancelledCallbacks; ++i)
				this->delPromiseRef();
		} else
			this->delPromiseRef();
	}

	coro::GetAllAsyncResultCallback<T>* callbacks() { return (coro::GetAllAsyncResultCallback<T>*)(this + 1); }
};

namespace coro {
template <class T>
// Mirrors the Future-based getAll callback path, but owns AsyncResult-specific
// detach/cancel behavior so pending producers are released when the aggregate
// finishes early.
struct GetAllAsyncResultCallback final : AsyncResultCallback<typename AsyncResult<T>::StoredT> {
	using StoredT = typename AsyncResult<T>::StoredT;

	AsyncResultState<StoredT>* resultState = nullptr;
	GetAllAsyncResult<T>* head = nullptr;
	int idx = -1;

	GetAllAsyncResultCallback() = default;
	GetAllAsyncResultCallback(AsyncResult<T>& result, GetAllAsyncResult<T>* head, int idx);

	void fire(StoredT const& value) override;
	void fire(StoredT&& value) override;
	void error(Error error) override;
	bool isRegistered() const { return resultState != nullptr; }
	void detach();
};
} // namespace coro

namespace coro {
template <class T>
struct GetAllAsyncResultStateCallback final : AsyncResultCallback<typename AsyncResult<T>::StoredT> {
	using StoredT = typename AsyncResult<T>::StoredT;

	AsyncResultState<StoredT>* resultState = nullptr;
	GetAllAsyncResultState<T>* head = nullptr;
	int idx = -1;

	void attach(AsyncResult<T>& result, GetAllAsyncResultState<T>* head, int idx);

	void fire(StoredT const& value) override;
	void fire(StoredT&& value) override;
	void error(Error error) override;
	bool isRegistered() const { return resultState != nullptr; }
	void detach();
};
} // namespace coro

template <class T>
struct GetAllAsyncResultState final : ReferenceCounted<GetAllAsyncResultState<T>> {
	int remaining;
	int count;
	Promise<Void> completion;
	std::vector<std::optional<T>> values;
	std::vector<coro::GetAllAsyncResultStateCallback<T>> callbacks;

	explicit GetAllAsyncResultState(int count) : remaining(count), count(count), values(count), callbacks(count) {}

	~GetAllAsyncResultState() { detachPendingCallbacks(); }

	void detachPendingCallbacks() {
		for (auto& callback : callbacks) {
			if (callback.isRegistered()) {
				callback.detach();
			}
		}
	}

	template <class U>
	void oneSuccess(int idx, U&& value) {
		if (!completion.canBeSet()) {
			return;
		}

		values[idx].emplace(std::forward<U>(value));
		if (--remaining == 0) {
			completion.send(Void());
		}
	}

	void oneError(Error err) {
		if (!completion.canBeSet()) {
			return;
		}

		detachPendingCallbacks();
		completion.sendError(err);
	}

	void attach(std::vector<AsyncResult<T>>& input) {
		for (int i = 0; i < input.size(); ++i) {
			auto& item = input[i];
			if (!completion.canBeSet()) {
				item = AsyncResult<T>();
			} else if (item.isReady()) {
				if (item.isError()) {
					Error err = item.getError();
					item = AsyncResult<T>();
					oneError(err);
				} else {
					T value = std::move(item).get();
					item = AsyncResult<T>();
					oneSuccess(i, std::move(value));
				}
			} else {
				callbacks[i].attach(item, this, i);
			}
		}
	}
};

namespace coro {
template <class T>
void GetAllAsyncResultStateCallback<T>::attach(AsyncResult<T>& result, GetAllAsyncResultState<T>* head, int idx) {
	resultState = result.resultState;
	this->head = head;
	this->idx = idx;
	std::move(result).addCallbackAndClear(this);
}

template <class T>
void GetAllAsyncResultStateCallback<T>::detach() {
	detachAsyncResultStateCallback(resultState, this);
}

template <class T>
void GetAllAsyncResultStateCallback<T>::fire(StoredT const& value) {
	Reference<GetAllAsyncResultState<T>> keepAlive = Reference<GetAllAsyncResultState<T>>::addRef(head);
	if constexpr (std::is_constructible_v<T, StoredT const&>) {
		T copied(value);
		detach();
		head->oneSuccess(idx, std::move(copied));
	} else {
		UNREACHABLE();
	}
}

template <class T>
void GetAllAsyncResultStateCallback<T>::fire(StoredT&& value) {
	Reference<GetAllAsyncResultState<T>> keepAlive = Reference<GetAllAsyncResultState<T>>::addRef(head);
	detach();
	head->oneSuccess(idx, std::move(value));
}

template <class T>
void GetAllAsyncResultStateCallback<T>::error(Error error) {
	Reference<GetAllAsyncResultState<T>> keepAlive = Reference<GetAllAsyncResultState<T>>::addRef(head);
	detach();
	head->oneError(error);
}
} // namespace coro

namespace coro {
template <class T>
GetAllAsyncResultCallback<T>::GetAllAsyncResultCallback(AsyncResult<T>& result, GetAllAsyncResult<T>* head, int idx)
  : resultState(result.resultState), head(head), idx(idx) {
	std::move(result).addCallbackAndClear(this);
}

template <class T>
void GetAllAsyncResultCallback<T>::detach() {
	detachAsyncResultStateCallback(resultState, this);
}

template <class T>
void GetAllAsyncResultCallback<T>::fire(StoredT const& value) {
	if constexpr (std::is_constructible_v<T, StoredT const&>) {
		T copied(value);
		detach();
		head->oneSuccess(idx, std::move(copied));
	} else {
		// AsyncResultState::complete() currently dispatches through fire(T&&). If
		// that ever changes, move-only AsyncResult payloads still need this path
		// to stay unreachable.
		UNREACHABLE();
	}
}

template <class T>
void GetAllAsyncResultCallback<T>::fire(StoredT&& value) {
	detach();
	head->oneSuccess(idx, std::move(value));
}

template <class T>
void GetAllAsyncResultCallback<T>::error(Error error) {
	detach();
	head->oneError(error);
}
} // namespace coro

template <class T>
Future<Void> smartQuorum(std::vector<Future<T>> results,
                         int required,
                         double extraSeconds,
                         TaskPriority taskID = TaskPriority::DefaultDelay) {
	if (results.empty() && required == 0)
		co_return;
	co_await quorum(results, required);
	co_await race(quorum(results, (int)results.size()), delay(extraSeconds, taskID));
}

template <class T>
Future<Void> waitForAll(std::vector<Future<T>> const& results) {
	if (results.empty())
		return Void();
	return quorum(results, (int)results.size());
}

// Wait for all futures in results to be ready and then throw the first (in execution order) error
// if any of them resulted in an error.
template <class T>
Future<Void> waitForAllReadyThenThrow(std::vector<Future<T>> const& results) {
	Future<Void> f = waitForAll(results);
	Future<Void> fReady = waitForAllReady(results);
	return fReady + f;
}

template <class T>
Future<Void> waitForAny(std::vector<Future<T>> const& results) {
	if (results.empty())
		return Void();
	return quorum(results, 1);
}

Future<Void> waitForMost(std::vector<Future<ErrorOr<Void>>> const& futures,
                         int const& faultTolerance,
                         Error const& e,
                         double const& waitMultiplierForSlowFutures = 1.0);

Future<bool> shortCircuitAny(std::vector<Future<bool>> const& f);

template <class T>
Future<std::vector<T>> getAll(std::vector<Future<T>> input) {
	if (input.empty())
		co_return std::vector<T>();
	co_await quorum(input, input.size());

	std::vector<T> output;
	output.reserve(input.size());
	for (int i = 0; i < input.size(); i++)
		output.push_back(input[i].get());
	co_return output;
}

template <class T>
AsyncResult<std::vector<T>> getAllAsync(std::vector<Future<T>> input) {
	if (input.empty())
		co_return std::vector<T>();
	co_await quorum(input, input.size());

	std::vector<T> output;
	output.reserve(input.size());
	for (int i = 0; i < input.size(); ++i) {
		output.push_back(input[i].get());
	}
	co_return output;
}

template <class T>
// AsyncResult is single-consumer, so getAll requires an explicit ownership
// transfer from vector callers.
Future<std::vector<T>> getAll(std::vector<AsyncResult<T>>& input) = delete;

template <class T>
// Preserve getAll's fail-fast behavior while keeping the aggregate result on a
// move-preserving AsyncResult path.
AsyncResult<std::vector<T>> getAllAsync(std::vector<AsyncResult<T>> input) {
	if (input.empty())
		co_return std::vector<T>();

	auto aggregateState = makeReference<GetAllAsyncResultState<T>>(input.size());
	aggregateState->attach(input);
	co_await aggregateState->completion.getFuture();

	std::vector<T> output;
	output.reserve(aggregateState->count);
	for (auto& item : aggregateState->values) {
		output.push_back(std::move(*item));
	}
	co_return output;
}

template <class T>
Future<std::vector<T>> getAll(std::vector<AsyncResult<T>>&& input) {
	if (input.empty())
		return std::vector<T>();

	int size = GetAllAsyncResult<T>::sizeFor(input.size());
	GetAllAsyncResult<T>* result = new (allocateFast(size)) GetAllAsyncResult<T>(input.size());

	coro::GetAllAsyncResultCallback<T>* nextCallback = result->callbacks();
	for (int i = 0; i < input.size(); ++i) {
		auto& item = input[i];
		if (item.isReady()) {
			new (nextCallback) coro::GetAllAsyncResultCallback<T>();
			if (item.isError()) {
				Error err = item.getError();
				item = AsyncResult<T>();
				result->oneError(err);
			} else {
				T value = std::move(item).get();
				item = AsyncResult<T>();
				result->oneSuccess(i, std::move(value));
			}
		} else {
			new (nextCallback) coro::GetAllAsyncResultCallback<T>(item, result, i);
		}
		++nextCallback;
	}
	return Future<std::vector<T>>(result);
}

template <class T>
Future<std::vector<T>> appendAll(std::vector<Future<std::vector<T>>> input) {
	co_await quorum(input, input.size());

	std::vector<T> output;
	size_t sz = 0;
	for (const auto& f : input) {
		sz += f.get().size();
	}
	output.reserve(sz);

	for (int i = 0; i < input.size(); i++) {
		auto const& r = input[i].get();
		output.insert(output.end(), r.begin(), r.end());
	}
	co_return output;
}

template <class T>
Future<Void> onEqual(Future<T> in, T equalTo) {
	T t = co_await in;
	if (t == equalTo)
		co_return;
	co_await Future<Void>(Never()); // never return
	throw internal_error(); // does not happen
}

template <class T>
Future<Void> success(Future<T> of) {
	co_await of;
}

template <class T>
Future<Void> ready(Future<T> f) {
	try {
		co_await f;
	} catch (...) {
	}
}

template <class T>
Future<T> waitAndForward(FutureStream<T> input, ExplicitVoid = {}) {
	T output = co_await input;
	co_return output;
}

template <class T>
Future<T> reportErrorsExcept(Future<T> in,
                             const char* context,
                             UID id,
                             std::set<int> const* pExceptErrors,
                             ExplicitVoid = {}) {
	try {
		T t = co_await in;
		co_return t;
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && (!pExceptErrors || !pExceptErrors->count(e.code())))
			TraceEvent(SevError, context, id).error(e);
		throw;
	}
}

template <class T>
Future<T> reportErrors(Future<T> const& in, const char* context, UID id = UID()) {
	return reportErrorsExcept(in, context, id, nullptr);
}

template <class T>
Future<T> require(Future<Optional<T>> in, int errorCode, ExplicitVoid = {}) {
	Optional<T> o = co_await in;
	if (o.present()) {
		co_return o.get();
	} else {
		throw Error(errorCode);
	}
}

template <class T>
Future<T> waitForFirst(std::vector<Future<T>> items, ExplicitVoid = {}) {
	PromiseStream<T> resultStream;
	PromiseStream<Error> errorStream;

	Future<Void> forCancellation = makeStream(items, resultStream, errorStream);

	FutureStream<T> resultFutureStream = resultStream.getFuture();
	FutureStream<Error> errorFutureStream = errorStream.getFuture();

	auto res = co_await race(resultFutureStream, errorFutureStream);
	forCancellation = Future<Void>();
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	throw std::get<1>(std::move(res));
}

template <class T>
Future<T> tag(Future<Void> future, T what, ExplicitVoid = {}) {
	co_await future;
	co_return what;
}

template <class T>
Future<Void> tag(Future<Void> future, T what, PromiseStream<T> stream) {
	co_await future;
	stream.send(what);
}

template <class T>
Future<T> tagError(Future<Void> future, Error e) {
	co_await future;
	throw e;
}

template <class T>
Future<T> detach(Future<T> f, ExplicitVoid = {}) {
	T x = co_await f;
	co_return x;
}

// If the future is ready, yields and returns. Otherwise, returns when future is set.
template <class T>
Future<T> orYield(Future<T> f) {
	if (f.isReady()) {
		if (f.isError())
			return tagError<T>(yield(), f.getError());
		else
			return tag(yield(), f.get());
	} else
		return f;
}

Future<Void> orYield(Future<Void> f);

template <class T>
Future<T> chooseActor(Future<T> lhs, Future<T> rhs, ExplicitVoid = {}) {
	auto res = co_await race(lhs, rhs);
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	co_return std::get<1>(std::move(res));
}

// set && set -> set
// error && x -> error
// all others -> unset
inline Future<Void> operator&&(Future<Void> const& lhs, Future<Void> const& rhs) {
	if (lhs.isReady()) {
		if (lhs.isError())
			return lhs;
		else
			return rhs;
	}
	if (rhs.isReady()) {
		if (rhs.isError())
			return rhs;
		else
			return lhs;
	}

	Future<Void> x[] = { lhs, rhs };
	return quorum(x, 2, 2);
}

// error || unset -> error
// unset || unset -> unset
// all others -> set
inline Future<Void> operator||(Future<Void> const& lhs, Future<Void> const& rhs) {
	if (lhs.isReady()) {
		if (lhs.isError())
			return lhs;
		if (rhs.isReady())
			return rhs;
		return lhs;
	}

	return chooseActor(lhs, rhs);
}

template <class T>
Future<T> joinWith(Future<T> f, Future<Void> other) {
	co_await other;
	co_return co_await f;
}

// wait <interval> then call what() in a loop forever
template <class Func>
Future<Void> recurring(Func what, double interval, TaskPriority taskID = TaskPriority::DefaultDelay) {
	while (true) {
		co_await delay(interval, taskID);
		what();
	}
}

template <class Func>
Future<Void> checkUntil(double checkInterval, Func f, TaskPriority taskID = TaskPriority::DefaultDelay) {
	while (true) {
		co_await delay(checkInterval, taskID);
		if (f()) {
			co_return;
		}
	}
}

// Invoke actorFunc() forever in a loop
// At least wait<interval> between two actor functor invocations
template <class F>
Future<Void> recurringAsync(
    F actorFunc, // Callback actor functor
    double interval, // Interval between two subsequent invocations of actor functor.
    bool absoluteIntervalDelay, // Flag guarantees "interval" delay between two subsequent actor functor invocations. If
                                // not selected, guarantees provided are "at least 'interval' delay" between two
                                // subsequent actor functor invocations, however, due to either 'poorly choose' interval
                                // value AND/OR actor functor taking longer than expected to return, could cause actor
                                // functor to run with no-delay
    double initialDelay, // Initial delay interval
    TaskPriority taskID = TaskPriority::DefaultDelay,
    bool jittered = false) {

	co_await delay(initialDelay);

	while (true) {
		Future<Void> val = actorFunc();

		if (absoluteIntervalDelay) {
			co_await val;
			// Ensure subsequent actorFunc executions observe client supplied delay interval.
			if (jittered) {
				co_await delayJittered(interval);
			} else {
				co_await delay(interval);
			}
		} else {
			// Guarantee at-least client supplied interval delay; two possible scenarios:
			// 1. The actorFunc executions finishes before 'interval' delay
			// 2. The actorFunc executions takes > 'interval' delay.
			if (jittered) {
				co_await (val && delayJittered(interval));
			} else {
				co_await (val && delay(interval));
			}
		}
	}
}

template <class T>
Future<T> brokenPromiseToNever(Future<T> in, ExplicitVoid = {}) {
	Error err;
	try {
		T t = co_await in;
		co_return t;
	} catch (Error& e) {
		err = e;
	}
	if (err.code() != error_code_broken_promise)
		throw err;
	co_await Future<Void>(Never()); // never return
	throw internal_error(); // does not happen
}

template <class T>
Future<T> brokenPromiseToMaybeDelivered(Future<T> in, ExplicitVoid = {}) {
	try {
		T t = co_await in;
		co_return t;
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) {
			throw request_maybe_delivered();
		}
		throw;
	}
}

template <class T, class U>
Future<Void> tagAndForward(Promise<T>* pOutputPromise, U value, Future<Void> signal, Uncancellable = Uncancellable()) {
	Promise<T> out(std::move(*pOutputPromise));
	co_await signal;
	out.send(std::move(value));
}

template <class T>
Future<Void> tagAndForward(PromiseStream<T>* pOutput, T value, Future<Void> signal, Uncancellable = Uncancellable()) {
	co_await signal;
	pOutput->send(std::move(value));
}

template <class T>
Future<Void> tagAndForwardError(Promise<T>* pOutputPromise,
                                Error value,
                                Future<Void> signal,
                                Uncancellable = Uncancellable()) {
	Promise<T> out(std::move(*pOutputPromise));
	co_await signal;
	out.sendError(value);
}

template <class T>
Future<Void> tagAndForwardError(PromiseStream<T>* pOutput,
                                Error value,
                                Future<Void> signal,
                                Uncancellable = Uncancellable()) {
	co_await signal;
	pOutput->sendError(value);
}

template <class T>
Future<T> waitOrError(Future<T> f, Future<Void> errorSignal, ExplicitVoid = {}) {
	auto res = co_await race(f, errorSignal);
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	ASSERT(false);
	throw internal_error();
}

template <class T>
Future<T> waitOrError(FutureStream<T> f, Future<Void> errorSignal, ExplicitVoid = {}) {
	auto res = co_await race(f, errorSignal);
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	}
	ASSERT(false);
	throw internal_error();
}

// A simple counter designed to track an ongoing count of something, such as how many actors are in a critical section,
// how many bytes are currently being processed, etc... Can be explicitly released idempotently, or will automatically
// release when destructed to handle actor ending or errors.
// Can be used for any type T so long as it has += and -= operators.

// Usage Example: tracking number of actors in code section
// ActiveCounter<int> counter;
//
//   state ActiveCounter::Releaser tracker = counter.take(1);
//   wait(perform my operation);
//   tracker.release();

template <class T>
struct ActiveCounter {
	struct Releaser : NonCopyable {
		ActiveCounter<T>* parent;
		T delta;
		std::function<void()> releaseCallback;

		Releaser() : parent(nullptr) {}
		Releaser(ActiveCounter<T>* parent, T delta, std::function<void()> releaseCallback)
		  : parent(parent), delta(delta), releaseCallback(releaseCallback) {
			parent->counter += delta;
		}
		explicit(false) Releaser(Releaser&& r) noexcept
		  : parent(r.parent), delta(r.delta), releaseCallback(r.releaseCallback) {
			r.parent = nullptr;
		}
		void operator=(Releaser&& r) {
			release();
			parent = r.parent;
			delta = r.delta;
			releaseCallback = r.releaseCallback;
			r.parent = nullptr;
		}

		void release() {
			if (parent) {
				parent->counter -= delta;
				parent = nullptr;
				if (releaseCallback) {
					releaseCallback();
				}
			}
		}

		~Releaser() { release(); }
	};

	T counter;

	explicit ActiveCounter(T initialValue) : counter(initialValue) {}

	T getValue() { return counter; }

	Releaser take(T delta, std::function<void()> releaseCallback = {}) {
		return Releaser(this, delta, releaseCallback);
	}
};

// A low-overhead FIFO mutex made with no internal queue structure (no list, deque, vector, etc)
// The lock is implemented as a Promise<Void>, which is returned to callers in a convenient wrapper
// called Lock.
//
// The default behavior is that if a Lock is dropped without error or release, existing and future
// waiters will see a broken_promise exception.
//
// If hangOnDroppedMutex is true, then if a Lock is dropped without error or release, existing and
// future waiters will never be signaled or see an error, equivalent to waiting on Never().
//
// Usage:
//   Lock lock = wait(mutex.take());
//   lock.release();  // Next waiter will get the lock, OR
//   lock.error(e);   // Next waiter will get e, future waiters will see broken_promise
//   lock = Lock();   // Or let Lock and any copies go out of scope.  All waiters will see broken_promise.
struct FlowMutex {
	explicit FlowMutex(bool hangOnDroppedMutex = false) : hangOnDroppedMutex(hangOnDroppedMutex) {
		lastPromise.send(Void());
	}

	bool available() const { return lastPromise.isSet(); }

	struct Lock {
		void release() { promise.send(Void()); }

		void error(Error e = broken_promise()) { promise.sendError(e); }

		// This is exposed in case the caller wants to use/copy it directly
		Promise<Void> promise;
	};

	Future<Lock> take() {
		Lock newLock;
		Future<Lock> f = lastPromise.isSet() ? newLock : tag(lastPromise.getFuture(), newLock);
		lastPromise = newLock.promise;
		if (hangOnDroppedMutex) {
			return brokenPromiseToNever(f);
		}
		return f;
	}

private:
	bool hangOnDroppedMutex;
	Promise<Void> lastPromise;
};

template <class T, class V>
Future<T> forwardErrors(Future<T> f, PromiseStream<V> output, ExplicitVoid = {}) {
	try {
		T val = co_await f;
		co_return val;
	} catch (Error& e) {
		output.sendError(e);
		throw;
	}
}

struct FlowLock : NonCopyable, public ReferenceCounted<FlowLock> {
	// FlowLock implements a nonblocking critical section: there can be only a limited number of clients executing code
	// between wait(take()) and release(). Not thread safe. take() returns only when the number of holders of the lock
	// is fewer than the number of permits, and release() makes the caller no longer a holder of the lock. release()
	// only runs waiting take()rs after the caller wait()s

	struct Releaser : NonCopyable {
		FlowLock* lock;
		int64_t remaining;
		Releaser() : lock(0), remaining(0) {}
		explicit(false) Releaser(FlowLock& lock, int64_t amount = 1) : lock(&lock), remaining(amount) {}
		explicit(false) Releaser(Releaser&& r) noexcept : lock(r.lock), remaining(r.remaining) { r.remaining = 0; }
		void operator=(Releaser&& r) {
			if (remaining)
				lock->release(remaining);
			lock = r.lock;
			remaining = r.remaining;
			r.remaining = 0;
		}

		void release(int64_t amount = -1) {
			if (amount == -1 || amount > remaining)
				amount = remaining;

			if (remaining)
				lock->release(amount);
			remaining -= amount;
		}

		~Releaser() {
			if (remaining)
				lock->release(remaining);
		}
	};

	FlowLock() : permits(1), active(0) {}
	explicit FlowLock(int64_t permits) : permits(permits), active(0) {}

	Future<Void> take(TaskPriority taskID = TaskPriority::DefaultYield, int64_t amount = 1) {
		if (active + amount <= permits || active == 0) {
			active += amount;
			return safeYieldActor(this, taskID, amount);
		}
		return takeActor(this, taskID, amount);
	}
	void release(int64_t amount = 1) {
		ASSERT((active > 0 || amount == 0) && active - amount >= 0);
		active -= amount;

		while (!takers.empty()) {
			if (active + takers.begin()->second <= permits || active == 0) {
				std::pair<Promise<Void>, int64_t> next = std::move(*takers.begin());
				active += next.second;
				takers.pop_front();
				next.first.send(Void());
			} else {
				break;
			}
		}
	}

	Future<Void> releaseWhen(Future<Void> const& signal, int amount = 1) {
		return releaseWhenActor(this, signal, amount);
	}

	// returns when any permits are available, having taken as many as possible up to the given amount, and modifies
	// amount to the number of permits taken
	Future<Void> takeUpTo(int64_t& amount) { return takeMoreActor(this, &amount); }

	int64_t available() const { return permits - active; }
	int64_t activePermits() const { return active; }
	int waiters() const { return takers.size(); }

	// Try to send error to all current and future waiters
	// Only works if broken_on_destruct.canBeSet()
	void kill(Error e = broken_promise()) {
		if (broken_on_destruct.canBeSet()) {
			auto local = broken_on_destruct;
			// It could be the case that calling broken_on_destruct destroys this FlowLock
			local.sendError(e);
		}
	}

private:
	std::list<std::pair<Promise<Void>, int64_t>> takers;
	const int64_t permits;
	int64_t active;
	Promise<Void> broken_on_destruct;

	static Future<Void> takeActor(FlowLock* lock, TaskPriority taskID, int64_t amount) {
		auto it = lock->takers.emplace(lock->takers.end(), Promise<Void>(), amount);

		try {
			co_await it->first.getFuture();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				lock->takers.erase(it);
				lock->release(0);
			}
			throw;
		}
		try {
			double duration =
			    buggify(.001) ? deterministicRandom()->random01() * FLOW_KNOBS->BUGGIFY_FLOW_LOCK_RELEASE_DELAY : 0.0;
			// Yield so releasing the lock does not run arbitrary code on the release() stack.
			co_await race(delay(duration, taskID), lock->broken_on_destruct.getFuture());
		} catch (...) {
			CODE_PROBE(true,
			           "If we get cancelled here, we are holding the lock but the caller doesn't know, so release it");
			lock->release(amount);
			throw;
		}
	}

	static Future<Void> takeMoreActor(FlowLock* lock, int64_t* amount) {
		co_await lock->take();
		int64_t extra = std::min(lock->available(), *amount - 1);
		lock->active += extra;
		*amount = 1 + extra;
	}

	static Future<Void> safeYieldActor(FlowLock* lock, TaskPriority taskID, int64_t amount) {
		try {
			co_await race(yield(taskID), lock->broken_on_destruct.getFuture());
		} catch (Error& e) {
			lock->release(amount);
			throw;
		}
	}

	static Future<Void> releaseWhenActor(FlowLock* self, Future<Void> signal, int64_t amount) {
		co_await signal;
		self->release(amount);
	}
};

struct NotifiedInt {
	explicit NotifiedInt(int64_t val = 0) : val(val) {}

	Future<Void> whenAtLeast(int64_t limit) {
		if (val >= limit)
			return Void();
		Promise<Void> p;
		waiting.emplace(limit, p);
		return p.getFuture();
	}

	int64_t get() const { return val; }

	void set(int64_t v) {
		ASSERT(v >= val);
		if (v != val) {
			val = v;

			std::vector<Promise<Void>> toSend;
			while (waiting.size() && v >= waiting.top().first) {
				Promise<Void> p = std::move(waiting.top().second);
				waiting.pop();
				toSend.push_back(p);
			}
			for (auto& p : toSend) {
				p.send(Void());
			}
		}
	}

	void operator=(int64_t v) { set(v); }

	explicit(false) NotifiedInt(NotifiedInt&& r) noexcept : waiting(std::move(r.waiting)), val(r.val) {}
	void operator=(NotifiedInt&& r) noexcept {
		waiting = std::move(r.waiting);
		val = r.val;
	}

private:
	typedef std::pair<int64_t, Promise<Void>> Item;
	struct ItemCompare {
		bool operator()(const Item& a, const Item& b) { return a.first > b.first; }
	};
	std::priority_queue<Item, std::vector<Item>, ItemCompare> waiting;
	int64_t val;
};

struct BoundedFlowLock : NonCopyable, public ReferenceCounted<BoundedFlowLock> {
	// BoundedFlowLock is different from a FlowLock in that it has a bound on how many locks can be taken from the
	// oldest outstanding lock. For instance, with a FlowLock that has two permits, if one permit is taken but never
	// released, the other permit can be reused an unlimited amount of times, but with a BoundedFlowLock, it can only be
	// reused a fixed number of times.

	struct Releaser : NonCopyable {
		BoundedFlowLock* lock;
		int64_t permitNumber;
		Releaser() : lock(nullptr), permitNumber(0) {}
		Releaser(BoundedFlowLock* lock, int64_t permitNumber) : lock(lock), permitNumber(permitNumber) {}
		explicit(false) Releaser(Releaser&& r) noexcept : lock(r.lock), permitNumber(r.permitNumber) {
			r.permitNumber = 0;
		}
		void operator=(Releaser&& r) {
			if (permitNumber)
				lock->release(permitNumber);
			lock = r.lock;
			permitNumber = r.permitNumber;
			r.permitNumber = 0;
		}

		void release() {
			if (permitNumber) {
				lock->release(permitNumber);
			}
			permitNumber = 0;
		}

		~Releaser() {
			if (permitNumber)
				lock->release(permitNumber);
		}
	};

	BoundedFlowLock() : minOutstanding(0), nextPermitNumber(0), unrestrictedPermits(1), boundedPermits(0) {}
	explicit BoundedFlowLock(int64_t unrestrictedPermits, int64_t boundedPermits)
	  : minOutstanding(0), nextPermitNumber(0), unrestrictedPermits(unrestrictedPermits),
	    boundedPermits(boundedPermits) {}

	Future<int64_t> take() { return takeActor(this); }
	void release(int64_t permitNumber) {
		outstanding.erase(permitNumber);
		updateMinOutstanding();
	}

private:
	IndexedSet<int64_t, int64_t> outstanding;
	NotifiedInt minOutstanding;
	int64_t nextPermitNumber;
	const int64_t unrestrictedPermits;
	const int64_t boundedPermits;

	void updateMinOutstanding() {
		auto it = outstanding.index(unrestrictedPermits - 1);
		if (it == outstanding.end()) {
			minOutstanding.set(nextPermitNumber);
		} else {
			minOutstanding.set(*it);
		}
	}

	static Future<int64_t> takeActor(BoundedFlowLock* lock) {
		int64_t permitNumber = ++lock->nextPermitNumber;
		lock->outstanding.insert(permitNumber, 1);
		lock->updateMinOutstanding();
		co_await lock->minOutstanding.whenAtLeast(std::max<int64_t>(0, permitNumber - lock->boundedPermits));
		co_return permitNumber;
	}
};

template <class T>
Future<Void> yieldPromiseStream(FutureStream<T> input,
                                PromiseStream<T> output,
                                TaskPriority taskID = TaskPriority::DefaultYield) {
	while (true) {
		T f = co_await input;
		output.send(f);
		co_await yield(taskID);
	}
}

struct YieldedFutureActor final : SAV<Void>,
                                  ActorCallback<YieldedFutureActor, 1, Void>,
                                  FastAllocated<YieldedFutureActor> {
	Error in_error_state;

	typedef ActorCallback<YieldedFutureActor, 1, Void> CB1;

	using FastAllocated<YieldedFutureActor>::operator new;
	using FastAllocated<YieldedFutureActor>::operator delete;

	explicit YieldedFutureActor(Future<Void>&& f) : SAV<Void>(1, 1), in_error_state(Error::fromCode(UNSET_ERROR_CODE)) {
		f.addYieldedCallbackAndClear(static_cast<ActorCallback<YieldedFutureActor, 1, Void>*>(this));
	}

	void cancel() override {
		if (!SAV<Void>::canBeSet())
			return; // Cancel could be invoked *by* a callback within finish().  Otherwise it's guaranteed that we are
			        // waiting either on the original future or on a delay().
		ActorCallback<YieldedFutureActor, 1, Void>::remove();
		SAV<Void>::sendErrorAndDelPromiseRef(actor_cancelled());
	}

	void destroy() override { delete this; }

#ifdef ENABLE_SAMPLING
	LineageReference* lineageAddr() { return currentLineage; }
#endif

	void a_callback_fire(ActorCallback<YieldedFutureActor, 1, Void>*, Void) {
		if (int16_t(in_error_state.code()) == UNSET_ERROR_CODE) {
			in_error_state = Error::fromCode(SET_ERROR_CODE);
			if (check_yield())
				doYield();
			else
				finish();
		} else {
			// We hit this case when and only when the delay() created by a previous doYield() fires.  Then we want to
			// get at least one task done, regardless of what check_yield() would say.
			finish();
		}
	}
	void a_callback_error(ActorCallback<YieldedFutureActor, 1, Void>*, Error const& err) {
		ASSERT(int16_t(in_error_state.code()) == UNSET_ERROR_CODE);
		in_error_state = err;
		if (check_yield())
			doYield();
		else
			finish();
	}
	void finish() {
		ActorCallback<YieldedFutureActor, 1, Void>::remove();
		if (int16_t(in_error_state.code()) == SET_ERROR_CODE)
			SAV<Void>::sendAndDelPromiseRef(Void());
		else
			SAV<Void>::sendErrorAndDelPromiseRef(in_error_state);
	}
	void doYield() {
		// Since we are being fired, we are the first callback in the ring, and `prev` is the source future
		Callback<Void>* source = CB1::prev;
		ASSERT(source->next == static_cast<CB1*>(this));

		// Remove the source future from the ring.  All the remaining callbacks in the ring should be yielded, since
		// yielded callbacks are installed at the end
		CB1::prev = source->prev;
		CB1::prev->next = static_cast<CB1*>(this);

		// The source future's ring is now empty, since we have removed all the callbacks
		source->next = source->prev = source;
		source->unwait();

		// Link all the callbacks, including this one, into the ring of a delay future so that after a short time they
		// will be fired again
		delay(0, g_network->getCurrentTask()).addCallbackChainAndClear(static_cast<CB1*>(this));
	}
};

inline Future<Void> yieldedFuture(Future<Void> f) {
	if (f.isReady())
		return yield();
	else
		return Future<Void>(new YieldedFutureActor(std::move(f)));
}

// An AsyncMap that uses a yieldedFuture in its onChange method.
template <class K, class V>
class YieldedAsyncMap : public AsyncMap<K, V> {
public:
	Future<Void> onChange(K const& k) override { // throws broken_promise if this is destroyed
		auto& item = AsyncMap<K, V>::items[k];
		if (item.value == AsyncMap<K, V>::defaultValue)
			return destroyOnCancelYield(this, k, item.change.getFuture());
		return yieldedFuture(item.change.getFuture());
	}

	static Future<Void> destroyOnCancelYield(YieldedAsyncMap* self, K key, Future<Void> change) {
		Future<Void> yielded = yieldedFuture(change);
		try {
			co_await yielded;
			yielded = Future<Void>();
			change = Future<Void>();
		} catch (Error& e) {
			yielded = Future<Void>();
			if (e.code() == error_code_actor_cancelled && !self->destructing && change.getFutureReferenceCount() == 1 &&
			    change.getPromiseReferenceCount() == 1) {
				if (EXPENSIVE_VALIDATION) {
					auto& p = self->items[key];
					ASSERT(p.change.getFuture() == change);
				}
				self->items.erase(key);
			}
			change = Future<Void>();
			throw;
		}
	}
};

class AndFuture {
public:
	AndFuture() = default;
	explicit(false) AndFuture(AndFuture const& f) = default;
	explicit(false) AndFuture(AndFuture&& f) noexcept = default;
	AndFuture& operator=(AndFuture const& f) = default;
	AndFuture& operator=(AndFuture&& f) noexcept = default;

	explicit(false) AndFuture(Future<Void> const& f) : futureCount(1), futures{ f } {}

	explicit(false) AndFuture(Error const& e) : futureCount(1), futures{ Future<Void>(e) } {}

	operator Future<Void>() { return getFuture(); }

	Future<Void> getFuture() {
		if (futures.empty())
			return Void();

		if (futures.size() == 1)
			return futures[0];

		Future<Void> f = waitForAll(futures);
		futures = std::vector<Future<Void>>{ f };
		return f;
	}

	bool isReady() {
		for (int i = futures.size() - 1; i >= 0; --i) {
			if (!futures[i].isReady()) {
				return false;
			} else if (!futures[i].isError()) {
				swapAndPop(&futures, i);
			}
		}
		return true;
	}

	bool isError() const {
		for (int i = 0; i < futures.size(); i++)
			if (futures[i].isError())
				return true;
		return false;
	}

	void cleanup() {
		for (int i = 0; i < futures.size(); i++) {
			if (futures[i].isReady() && !futures[i].isError()) {
				swapAndPop(&futures, i--);
			}
		}
	}

	void add(Future<Void> const& f) {
		++futureCount;
		if (!f.isReady() || f.isError())
			futures.push_back(f);
	}

	void add(AndFuture f) { add(f.getFuture()); }

	// The total number of futures which have ever been added to this AndFuture
	int64_t getFutureCount() const { return futureCount; }

private:
	int64_t futureCount = 0;
	std::vector<Future<Void>> futures;
};

template <class T>
Future<Void> timeReply(Future<T> replyToTime, PromiseStream<double> timeOutput) {
	double startTime = now();
	try {
		co_await replyToTime;
		co_await delay(0);
		timeOutput.send(now() - startTime);
	} catch (Error& e) {
		// Ignore broken promises.  They typically occur during shutdown and our callers don't want to have to create
		// brokenPromiseToNever actors to ignore them.  For what it's worth we are breaking timeOutput to pass the pain
		// along.
		if (e.code() != error_code_broken_promise)
			throw;
	}
}

template <class T>
Future<T> forward(Future<T> from, Promise<T> to, ExplicitVoid = {}) {
	try {
		T res = co_await from;
		to.send(res);
		co_return res;
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			to.sendError(e);
		}
		throw e;
	}
}

// Monad

template <class Fun, class T>
Future<decltype(std::declval<Fun>()(std::declval<T>()))> fmap(Fun fun, Future<T> f, ExplicitVoid = {}) {
	T val = co_await f;
	co_return fun(val);
}

/*
 * IAsyncListener is similar to AsyncVar, but it decouples the input and output, so the translation unit
 * responsible for handling the output does not need to have knowledge of how the output is generated
 */

template <class Output>
class IAsyncListener : public ReferenceCounted<IAsyncListener<Output>> {
public:
	virtual ~IAsyncListener() = default;
	virtual Output const& get() const = 0;
	virtual Future<Void> onChange() const = 0;
	template <class Input, class F>
	static Reference<IAsyncListener> create(Reference<AsyncVar<Input> const> const& input, F const& f);
	template <class Input, class F>
	static Reference<IAsyncListener> create(Reference<AsyncVar<Input>> const& input, F const& f);
	static Reference<IAsyncListener> create(Reference<AsyncVar<Output>> const& output);
};

namespace IAsyncListenerImpl {

template <class Input, class Output, class F>
class AsyncListener final : public IAsyncListener<Output> {
	// Order matters here, output must outlive monitorActor
	AsyncVar<Output> output;
	Future<Void> monitorActor;
	static Future<Void> monitor(Reference<AsyncVar<Input> const> input, AsyncVar<Output>* output, F f) {
		while (true) {
			co_await input->onChange();
			output->set(f(input->get()));
		}
	}

public:
	AsyncListener(Reference<AsyncVar<Input> const> const& input, F const& f)
	  : output(f(input->get())), monitorActor(monitor(input, &output, f)) {}
	Output const& get() const override { return output.get(); }
	Future<Void> onChange() const override { return output.onChange(); }
};

} // namespace IAsyncListenerImpl

template <class Output>
template <class Input, class F>
Reference<IAsyncListener<Output>> IAsyncListener<Output>::create(Reference<AsyncVar<Input> const> const& input,
                                                                 F const& f) {
	return makeReference<IAsyncListenerImpl::AsyncListener<Input, Output, F>>(input, f);
}

template <class Output>
template <class Input, class F>
Reference<IAsyncListener<Output>> IAsyncListener<Output>::create(Reference<AsyncVar<Input>> const& input, F const& f) {
	return create(Reference<AsyncVar<Input> const>(input), f);
}

template <class Output>
Reference<IAsyncListener<Output>> IAsyncListener<Output>::create(Reference<AsyncVar<Output>> const& input) {
	auto identity = [](const auto& x) { return x; };
	return makeReference<IAsyncListenerImpl::AsyncListener<Output, Output, decltype(identity)>>(input, identity);
}

// A weak reference type to wrap a future Reference<T> object.
// Once the future is complete, this object holds a pointer to the referenced object but does
// not contribute to its reference count.
//
// WARNING: this class will not be aware when the underlying object is destroyed. It is up to the
// user to make sure that an UnsafeWeakFutureReference is discarded at the same time the object is.
template <class T>
class UnsafeWeakFutureReference {
public:
	UnsafeWeakFutureReference() {}
	explicit UnsafeWeakFutureReference(Future<Reference<T>> future) : data(new UnsafeWeakFutureReferenceData(future)) {}

	// Returns a future to obtain a normal reference handle
	// If the future is ready, this creates a Reference<T> to wrap the object
	Future<Reference<T>> get() {
		if (!data) {
			return Reference<T>();
		} else if (data->ptr.present()) {
			return Reference<T>::addRef(data->ptr.get());
		} else {
			return data->future;
		}
	}

	// Returns the raw pointer, if the object is ready
	// Note: this should be used with care, as this pointer is not counted as a reference to the object and
	// it could be deleted if all normal references are destroyed.
	Optional<T*> getPtrIfReady() { return data->ptr; }

private:
	// A class to hold the state for an UnsafeWeakFutureReference
	struct UnsafeWeakFutureReferenceData : public ReferenceCounted<UnsafeWeakFutureReferenceData>, NonCopyable {
		Optional<T*> ptr;
		Future<Reference<T>> future;
		Future<Void> moveResultFuture;

		explicit UnsafeWeakFutureReferenceData(Future<Reference<T>> future) : future(future) {
			moveResultFuture = moveResult(this);
		}

		// Waits for the future to complete and then stores the pointer in local storage
		// When this completes, we will no longer be counted toward the reference count of the object
		Future<Void> moveResult(UnsafeWeakFutureReferenceData* self) {
			Reference<T> result = co_await self->future;
			self->ptr = result.getPtr();
			self->future = Future<Reference<T>>();
		}
	};

	Reference<UnsafeWeakFutureReferenceData> data;
};

// Utility class to provide FLOW compliant singleton pattern.
// In similuation, the approach allows per-virtual process singleton as desired compared to one singleton instance
// shared across all virtual processes if 'static singleton' pattern is implemented.
//
// API NOTE: Client are expected to pass functor allowing instantiation of the template class
template <class T>
class FlowSingleton {
public:
	static Reference<T> getInstance(std::function<Reference<T>()> func) {
		ASSERT(g_network->isSimulated());

		auto cItr = instanceMap.find(g_network->getLocalAddress());
		if (cItr == instanceMap.end()) {
			instanceMap.emplace(g_network->getLocalAddress(), func());
			return instanceMap[g_network->getLocalAddress()];
		} else {
			return cItr->second;
		}
	}

	static int getCount() { return instanceMap.size(); }
	static void resetInstances() { instanceMap.clear(); }

private:
	static std::unordered_map<NetworkAddress, Reference<T>> instanceMap;
};

template <class T>
std::unordered_map<NetworkAddress, Reference<T>> FlowSingleton<T>::instanceMap;
