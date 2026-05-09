/*
 * Coroutines.h
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

#ifndef FLOW_COROUTINES_H
#define FLOW_COROUTINES_H

#pragma once

#if __has_include(<coroutine>)
#include <coroutine>
namespace n_coroutine = ::std;
#elif __has_include(<experimental/coroutine>)
#include <experimental/coroutine>
namespace n_coroutine = ::std::experimental;
#endif
#include <concepts>
#include <array>
#include <cstring>

#include "flow/flow.h"
#include "flow/Error.h"

struct Uncancellable {};

// Marker parameter for coroutines that should destroy the coroutine frame on
// cancellation instead of resuming it to throw actor_cancelled(). Coroutine
// locals are cleaned up by RAII and catch blocks inside the coroutine are not
// run for cancellation.
struct NoThrowOnCancel {};

// Marker parameter for coroutines that want `co_await Future<Void>` to
// produce a value convertible to `Void` instead of `void`:
//
//   Future<Void> f(Future<Void> ready, ExplicitVoid = {}) {
//     Void v = co_await ready;
//     co_return;
//   }
//
// Unmarked coroutines resume with co_await Future<Void>` -> `void`
struct ExplicitVoid {
	operator Void() const { return Void(); }
};

template <class T>
class AsyncResult;

namespace coro {
template <class T>
struct FutureIgnore;

template <class SourceValue, class ResultValue = std::conditional_t<std::is_void_v<SourceValue>, Void, SourceValue>>
struct FutureErrorOr;

template <class T>
struct AsyncResultState;

template <class T>
struct AsyncResultCallback;

template <class T, bool IsCancellable, bool ReturnsExplicitVoid = false, bool NoThrowOnCancel = false>
struct AsyncResultPromise;

template <class PromiseType, class ValueType>
struct AwaitableAsyncResult;

template <class ValueType>
struct AsyncResultAwaiter;

template <class Parent, int Idx, class ValueType>
struct ActorAsyncResultCallback;

template <class ValueType>
struct QuorumAsyncResultCallback;

template <class ValueType>
struct GetAllAsyncResultCallback;

template <class PromiseType, class ValueType>
struct AwaitableFutureIgnore;

template <class PromiseType, class SourceValue, class ResultValue>
struct AwaitableFutureErrorOr;
} // namespace coro

namespace coro {

template <class T>
struct FutureIgnore {
	// Wrap a Future<T> so coroutine await_transform can resume on completion
	// without materializing the T payload at the await site.
	Future<T> future;
};

template <class T>
FutureIgnore<T> ignore(Future<T> future) {
	return FutureIgnore<T>{ std::move(future) };
}

template <class SourceValue, class ResultValue>
struct FutureErrorOr {
	// Reuse Future<T> storage but request a non-throwing await_resume() that
	// converts completion into ErrorOr<ResultValue>.
	Future<std::conditional_t<std::is_void_v<SourceValue>, Void, SourceValue>> future;
};

template <class T>
FutureErrorOr<T> errorOr(Future<T> future) {
	return FutureErrorOr<T>{ std::move(future) };
}

template <class T>
FutureErrorOr<T, Void> errorOr(FutureIgnore<T> future) {
	return FutureErrorOr<T, Void>{ std::move(future.future) };
}

} // namespace coro

// Move-only coroutine result that transfers ownership through co_await.
// Unlike Future<T>, awaiting AsyncResult<T> produces T by value so expensive
// payloads do not need an extra copy at the await site.
template <class T>
class SWIFT_SENDABLE AsyncResult {
public:
	static_assert(!std::is_void_v<T>, "Use AsyncResult<Void> instead of AsyncResult<void>");
	using Element = T;
	using StoredT = T;

	AsyncResult() noexcept : resultState(nullptr) {}
	AsyncResult(AsyncResult const&) = delete;
	AsyncResult& operator=(AsyncResult const&) = delete;
	AsyncResult(AsyncResult&& rhs) noexcept : resultState(rhs.resultState) { rhs.resultState = nullptr; }
	AsyncResult& operator=(AsyncResult&& rhs) noexcept {
		if (this != &rhs) {
			release();
			resultState = rhs.resultState;
			rhs.resultState = nullptr;
		}
		return *this;
	}
	~AsyncResult() { release(); }

	bool isValid() const { return resultState != nullptr; }
	bool isReady() const;
	bool isError() const;
	bool canGet() const;
	Error& getError() const;
	void cancel() const;
	void addCallbackAndClear(coro::AsyncResultCallback<StoredT>* cb) &&;

	T const& get() const&;
	T get() &&;
	T getValue() const& { return get(); }
	T getValue() && { return std::move(*this).get(); }

	auto operator co_await() &;
	auto operator co_await() &&;

private:
	explicit AsyncResult(coro::AsyncResultState<StoredT>* resultState) noexcept : resultState(resultState) {}
	void release();

	coro::AsyncResultState<StoredT>* resultState;

	template <class U, bool IsCancellable, bool ReturnsExplicitVoid, bool NoThrowOnCancel>
	friend struct coro::AsyncResultPromise;
	template <class PromiseType, class ValueType>
	friend struct coro::AwaitableAsyncResult;
	template <class ValueType>
	friend struct coro::AsyncResultAwaiter;
	template <class Parent, int Idx, class ValueType>
	friend struct coro::ActorAsyncResultCallback;
	template <class ValueType>
	friend struct coro::QuorumAsyncResultCallback;
	template <class ValueType>
	friend struct coro::GetAllAsyncResultCallback;
};

#include "flow/CoroutinesImpl.h"

template <class T>
class AsyncGenerator {
	PromiseStream<T>* nextPromise;
	n_coroutine::coroutine_handle<> handle;

public:
	explicit AsyncGenerator(PromiseStream<T>* promise, n_coroutine::coroutine_handle<> handle)
	  : nextPromise(promise), handle(handle) {}

	~AsyncGenerator() { handle.destroy(); }

	Future<T> operator()() {
		handle.resume();
		Error error;
		T res;
		try {
			res = co_await nextPromise->getFuture();
		} catch (Error& e) {
			error = e;
		} catch (...) {
			error = unknown_error();
		}
		co_await delay(0);
		if (error.isValid()) {
			throw error;
		}
		co_return res;
	}

	explicit operator bool() { return !handle.done(); }
};

// Inspired from https://www.scs.stanford.edu/~dm/blog/c++-coroutines.html
template <typename T>
class Generator {
public: // types
	using promise_type = coro::GeneratorPromise<T>;
	using handle_type = n_coroutine::coroutine_handle<promise_type>;
	using value_type = T;
	using difference_type = std::ptrdiff_t;

private:
	handle_type handle;

public:
	explicit Generator(handle_type h) : handle(h) {}
	Generator() {}
	explicit(false) Generator(Generator const& other) : handle(other.handle) {
		if (handle) {
			handle.promise().addRef();
		}
	}
	explicit(false) Generator(Generator&& other) : handle(std::move(other.handle)) { other.handle = handle_type{}; }
	~Generator() {
		if (handle) {
			handle.promise().delRef();
		}
	}

	Generator& operator=(Generator const& other) {
		if (handle) {
			handle.promise().delRef();
		}
		handle = other.handle;
		if (handle) {
			handle.promise().addRef();
		}
		return *this;
	}

	Generator& operator=(Generator&& other) {
		if (handle) {
			handle.promise().delRef();
		}
		handle = std::move(other.handle);
		other.handle = handle_type{};
		return *this;
	}

	explicit operator bool() { return handle && !handle.done(); }

	static Generator<T> end() { return Generator<T>{}; }

	bool operator==(Generator<T> const& other) const {
		bool selfValid = handle && !handle.done();
		bool otherValid = other.handle && !other.handle.done();
		if (selfValid == otherValid) {
			// if both generator are done, we consider them the same, otherwise they are only the same if both point to
			// the same coroutine
			return !selfValid || handle == other.handle;
		}
		return false;
	}

	bool operator!=(Generator<T> const& other) const { return !(*this == other); }

	const T& operator*() const& {
		auto& promise = handle.promise();
		if (promise.error.isValid()) {
			throw promise.error;
		}
		ASSERT(promise.value.has_value());
		return promise.value.value();
	}

	Generator& operator++() {
		handle.resume();
		return *this;
	}

	void operator++(int) { ++(*this); }
};

template <typename ReturnValue, typename... Args>
struct [[maybe_unused]] n_coroutine::coroutine_traits<Future<ReturnValue>, Args...> {
	static_assert(!(coro::hasUncancellable<Args...> && coro::hasNoThrowOnCancel<Args...>),
	              "NoThrowOnCancel and Uncancellable are mutually exclusive");
	using promise_type = coro::CoroPromise<ReturnValue,
	                                       !coro::hasUncancellable<Args...>,
	                                       coro::hasExplicitVoid<Args...>,
	                                       coro::hasNoThrowOnCancel<Args...>>;
};

template <typename ReturnValue, typename... Args>
struct [[maybe_unused]] n_coroutine::coroutine_traits<AsyncResult<ReturnValue>, Args...> {
	static_assert(!(coro::hasUncancellable<Args...> && coro::hasNoThrowOnCancel<Args...>),
	              "NoThrowOnCancel and Uncancellable are mutually exclusive");
	using promise_type = coro::AsyncResultPromise<ReturnValue,
	                                              !coro::hasUncancellable<Args...>,
	                                              coro::hasExplicitVoid<Args...>,
	                                              coro::hasNoThrowOnCancel<Args...>>;
};

template <typename ReturnValue, typename... Args>
struct [[maybe_unused]] n_coroutine::coroutine_traits<AsyncGenerator<ReturnValue>, Args...> {
	static_assert(!coro::hasUncancellable<Args...>, "AsyncGenerator can't be uncancellable");
	static_assert(!coro::hasNoThrowOnCancel<Args...>, "AsyncGenerator can't use NoThrowOnCancel");
	using promise_type = coro::AsyncGeneratorPromise<ReturnValue, coro::hasExplicitVoid<Args...>>;
};

template <class T>
bool AsyncResult<T>::isReady() const {
	return resultState && resultState->isReady();
}

template <class T>
bool AsyncResult<T>::isError() const {
	return resultState && resultState->isError();
}

template <class T>
bool AsyncResult<T>::canGet() const {
	return resultState && resultState->canGet();
}

template <class T>
Error& AsyncResult<T>::getError() const {
	ASSERT(resultState);
	return resultState->getError();
}

template <class T>
void AsyncResult<T>::cancel() const {
	if (resultState) {
		resultState->cancelProducer();
	}
}

template <class T>
void AsyncResult<T>::addCallbackAndClear(coro::AsyncResultCallback<StoredT>* cb) && {
	ASSERT(resultState);
	resultState->registerCallback(cb);
	resultState = nullptr;
}

template <class T>
T const& AsyncResult<T>::get() const& {
	ASSERT(resultState);
	return resultState->get();
}

template <class T>
T AsyncResult<T>::get() && {
	ASSERT(resultState);
	return resultState->take();
}

template <class T>
auto AsyncResult<T>::operator co_await() & {
	return coro::AsyncResultAwaiter<T>{ std::move(*this) };
}

template <class T>
auto AsyncResult<T>::operator co_await() && {
	return coro::AsyncResultAwaiter<T>{ std::move(*this) };
}

template <class T>
void AsyncResult<T>::release() {
	if (resultState) {
		if (!resultState->isReady()) {
			resultState->cancelProducer();
		}
		resultState->delRef();
		resultState = nullptr;
	}
}

#endif // FLOW_COROUTINES_H
