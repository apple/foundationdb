/*
 * CoroutinesImpl.h
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

#ifndef FLOW_COROUTINESIMPL_H
#define FLOW_COROUTINESIMPL_H

#include "flow/FlowThread.h"
#include "flow/flow.h"

template <class T>
class Generator;

template <class T>
class AsyncGenerator;

struct Uncancellable;

namespace coro {

template <class F>
struct FutureReturnType;

template <class T>
struct FutureReturnType<Future<T>> {
	using type = T;
};

template <class T>
struct FutureReturnType<FutureStream<T>> {
	using type = T;
};

template <class T>
struct FutureReturnType<Future<T> const&> {
	using type = T;
};

template <class T>
struct FutureReturnType<FutureStream<T> const&> {
	using type = T;
};

template <class F>
using FutureReturnTypeT = typename FutureReturnType<F>::type;

enum class FutureType { FutureStream, Future };

template <class F>
struct GetFutureType;

template <class T>
struct GetFutureType<Future<T>> {
	constexpr static FutureType value = FutureType::Future;
};

template <class T>
struct GetFutureType<FutureStream<T>> {
	constexpr static FutureType value = FutureType::FutureStream;
};

template <class T>
struct GetFutureType<Future<T> const&> {
	constexpr static FutureType value = FutureType::Future;
};

template <class T>
struct GetFutureType<FutureStream<T> const&> {
	constexpr static FutureType value = FutureType::FutureStream;
};

template <class F>
inline constexpr FutureType GetFutureTypeV = GetFutureType<F>::value;

template <class T, bool IsCancellable>
struct CoroActor final : Actor<std::conditional_t<std::is_void_v<T>, Void, T>> {
	using ValType = std::conditional_t<std::is_void_v<T>, Void, T>;

	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	n_coroutine::coroutine_handle<> handle;

	int8_t& waitState() { return Actor<ValType>::actor_wait_state; }

	template <class U>
	void set(U&& value) {
		new (&SAV<ValType>::value()) ValType(std::forward<U>(value));
		SAV<ValType>::error_state = Error(SAV<ValType>::SET_ERROR_CODE);
	}

	void setError(Error const& e) { SAV<ValType>::error_state = e; }

	void cancel() override {
		if constexpr (IsCancellable) {
			auto prev_wait_state = Actor<ValType>::actor_wait_state;

			// Set wait state to -1
			Actor<ValType>::actor_wait_state = -1;

			// If the actor is waiting, then resume the coroutine to throw actor_cancelled().
			if (prev_wait_state > 0) {
				handle.resume();
			}
		}
	}

	void destroy() override { delete this; }
};

template <class U>
struct AwaitableFutureStore {
	std::variant<Error, U> data;

	constexpr bool isSet() const noexcept { return data.index() != 0 || std::get<0>(data).isValid(); }
	void copy(U v) { data = std::move(v); }
	void set(U&& v) { data = std::move(v); }
	void set(U const& v) { data = v; }

	const U& getRef() const {
		switch (data.index()) {
		case 0:
			throw std::get<0>(data);
		case 1:
			return std::get<1>(data);
		}
		UNREACHABLE();
	}

	U&& get() && {
		switch (data.index()) {
		case 0:
			throw std::get<0>(data);
		case 1:
			return std::get<1>(std::move(data));
		}
		UNREACHABLE();
	}
};

template <class T>
using ToFutureVal = std::conditional_t<std::is_void_v<T>, Void, T>;

template <class F, class U, bool IsStream>
struct AwaitableResume;

template <class F>
struct AwaitableResume<F, Void, false> {
	[[maybe_unused]] void await_resume() {
		auto self = static_cast<F*>(this);
		self->resumeImpl();
		if (self->future.isError()) {
			throw self->future.getError();
		}
	}
};

template <class F, class T>
struct AwaitableResume<F, T, false> {
	T const& await_resume() {
		auto self = static_cast<F*>(this);
		if (self->resumeImpl()) {
			if (self->future.isError()) {
				throw self->future.getError();
			}
			return self->future.get();
		}
		return self->store.getRef();
	}
};

template <class F, class T>
struct AwaitableResume<F, T, true> {
	T await_resume() {
		auto self = static_cast<F*>(this);
		if (self->resumeImpl()) {
			if (self->future.isError()) {
				throw self->future.getError();
			}
			return self->future.pop();
		}
		return std::move(self->store).get();
	}
};

template <class promise_type, class U, bool IsStream>
struct AwaitableFuture : std::conditional_t<IsStream, SingleCallback<ToFutureVal<U>>, Callback<ToFutureVal<U>>>,
                         AwaitableResume<AwaitableFuture<promise_type, U, IsStream>, U, IsStream> {
	using FutureValue = ToFutureVal<U>;
	using FutureType = std::conditional_t<IsStream, FutureStream<FutureValue>, Future<FutureValue> const&>;
	FutureType future;
	promise_type* pt = nullptr;
	AwaitableFutureStore<FutureValue> store;

	AwaitableFuture(const FutureType& f, promise_type* pt) : future(f), pt(pt) {}

	void fire(FutureValue const& value) override {
		store.set(value);
		pt->resume();
	}
	void fire(FutureValue&& value) override {
		store.set(std::move(value));
		pt->resume();
	}

	void error(Error error) override {
		store.data = error;
		pt->resume();
	}

	[[maybe_unused]] [[nodiscard]] bool await_ready() const {
		if (pt->waitState() < 0) {
			pt->waitState() = -2;
			// actor was cancelled
			return true;
		}
		return future.isReady();
	}

	[[maybe_unused]] void await_suspend(n_coroutine::coroutine_handle<> h) {
		// Create a coroutine callback if it's the first time being suspended
		pt->setHandle(h);

		// Set wait_state and add callback
		pt->waitState() = 1;

		if constexpr (IsStream) {
			auto sf = future;
			sf.addCallbackAndClear(this);
		} else {
			StrictFuture<FutureValue> sf = future;
			sf.addCallbackAndClear(this);
		}
	}

	bool resumeImpl() {
		// If actor is cancelled, then throw actor_cancelled()
		switch (pt->waitState()) {
		case -1:
			this->remove();
		case -2:
			// -2 means that the `await_suspend` call returned `true`, so we shouldn't remove the callback.
			// if the wait_state is -1 we still have to throw, so we fall through to the -2 case
			throw actor_cancelled();
		}

		bool wasReady = pt->waitState() == 0;
		// Actor return from waiting, remove callback and reset wait_state.
		if (pt->waitState() > 0) {
			this->remove();

			pt->waitState() = 0;
		}
		return wasReady;
	}
};

// TODO: This can be merged with AwaitableFutureStream by passing more template arguments.
template <class promise_type, class U>
struct ThreadAwaitableFutureStream : SingleCallback<ToFutureVal<U>>,
                                     AwaitableResume<ThreadAwaitableFutureStream<promise_type, U>, U, true> {
	using FutureValue = ToFutureVal<U>;
	using FutureType = ThreadFutureStream<FutureValue>;
	FutureType future;
	promise_type* pt = nullptr;
	AwaitableFutureStore<FutureValue> store;

	ThreadAwaitableFutureStream(const FutureType& f, promise_type* pt) : future(f), pt(pt) {}

	void fire(FutureValue const& value) override {
		store.set(value);
		pt->resume();
	}
	void fire(FutureValue&& value) override {
		store.set(std::move(value));
		pt->resume();
	}

	void error(Error error) override {
		store.data = error;
		pt->resume();
	}

	[[maybe_unused]] [[nodiscard]] bool await_ready() const {
		if (pt->waitState() < 0) {
			pt->waitState() = -2;
			// actor was cancelled
			return true;
		}
		return future.isReady();
	}

	[[maybe_unused]] void await_suspend(n_coroutine::coroutine_handle<> h) {
		// Create a coroutine callback if it's the first time being suspended
		pt->setHandle(h);

		// Set wait_state and add callback
		pt->waitState() = 1;

		auto sf = future;
		sf.addCallbackAndClear(this);
	}

	bool resumeImpl() {
		// If actor is cancelled, then throw actor_cancelled()
		switch (pt->waitState()) {
		case -1:
			this->remove();
		case -2:
			// -2 means that the `await_suspend` call returned `true`, so we shouldn't remove the callback.
			// if the wait_state is -1 we still have to throw, so we fall through to the -2 case
			throw actor_cancelled();
		}

		bool wasReady = pt->waitState() == 0;
		// Actor return from waiting, remove callback and reset wait_state.
		if (pt->waitState() > 0) {
			this->remove();

			pt->waitState() = 0;
		}
		return wasReady;
	}
};

template <class T, bool>
struct ActorMember {
	T* member;
	explicit ActorMember(n_coroutine::coroutine_handle<> handle) : member(new T(handle)) {}
	T* ptr() { return member; }
	T* operator->() { return member; }
	const T* operator->() const { return member; }
};

template <class T>
struct ActorMember<T, true> {
	T member;
	explicit ActorMember(n_coroutine::coroutine_handle<> handle) : member(handle) {}
	T* ptr() { return &member; }
	T* operator->() { return &member; }
	const T* operator->() const { return &member; }
};

template <class T, class Promise>
struct CoroReturn {
	template <class U>
	void return_value(U&& value) {
		static_cast<Promise*>(this)->coroActor->set(std::forward<U>(value));
	}
};

template <class Promise>
struct CoroReturn<Void, Promise> {
	void return_void() { static_cast<Promise*>(this)->coroActor->set(Void()); }
};

template <class T, bool IsCancellable>
struct CoroPromise : CoroReturn<T, CoroPromise<T, IsCancellable>> {
	using promise_type = CoroPromise<T, IsCancellable>;
	using ActorType = coro::CoroActor<T, IsCancellable>;
	using ReturnValue = std::conditional_t<std::is_void_v<T>, Void, T>;
	using ReturnFutureType = Future<ReturnValue>;

	ActorType* coroActor;

	CoroPromise() : coroActor(new ActorType()) {}

	n_coroutine::coroutine_handle<promise_type> handle() {
		return n_coroutine::coroutine_handle<promise_type>::from_promise(*this);
	}

	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	ReturnFutureType get_return_object() noexcept { return ReturnFutureType(coroActor); }

	[[nodiscard]] n_coroutine::suspend_never initial_suspend() const noexcept { return {}; }

	auto final_suspend() noexcept {
		struct FinalAwaitable {
			ActorType* sav;
			// for debugging output only
			explicit FinalAwaitable(ActorType* sav) : sav(sav) {}

			[[nodiscard]] bool await_ready() const noexcept { return true; }
			void await_resume() const noexcept {
				if (sav->isError()) {
					sav->finishSendErrorAndDelPromiseRef();
				} else {
					sav->finishSendAndDelPromiseRef();
				}
			}
			constexpr void await_suspend(n_coroutine::coroutine_handle<>) const noexcept {}
		};
		return FinalAwaitable(coroActor);
	}

	void unhandled_exception() {
		// The exception should always be type Error.
		try {
			std::rethrow_exception(std::current_exception());
		} catch (const Error& error) {
			// if (Actor<ReturnValue>::actor_wait_state == -1 && error.code() == error_code_operation_cancelled) {
			// 	return;
			// }
			coroActor->setError(error);
			// SAV<ReturnValue>::sendErrorAndDelPromiseRef(error);
		} catch (...) {
			coroActor->setError(unknown_error());
			// SAV<ReturnValue>::sendErrorAndDelPromiseRef(unknown_error());
		}
	}

	void setHandle(n_coroutine::coroutine_handle<> h) { coroActor->handle = h; }

	void resume() { coroActor->handle.resume(); }

	int8_t& waitState() { return coroActor->waitState(); }

	template <class U>
	auto await_transform(const Future<U>& future) {
		return coro::AwaitableFuture<promise_type, U, false>{ future, this };
	}

	template <class U>
	auto await_transform(const FutureStream<U>& futureStream) {
		return coro::AwaitableFuture<promise_type, U, true>{ futureStream, this };
	}

	template <class U>
	auto await_transform(const ThreadFutureStream<U>& futureStream) {
		return coro::ThreadAwaitableFutureStream<promise_type, U>{ futureStream, this };
	}
};

template <class T>
struct GeneratorPromise {
	using handle_type = n_coroutine::coroutine_handle<GeneratorPromise<T>>;
	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	Error error;
	std::optional<T> value;
	mutable unsigned refCount = 1;

	void addRef() const { refCount += 1; }
	void delRef() const {
		if (--refCount == 0) {
			const_cast<GeneratorPromise<T>*>(this)->handle().destroy();
		}
	}

	n_coroutine::suspend_never initial_suspend() { return {}; }
	n_coroutine::suspend_always final_suspend() noexcept { return {}; }

	auto handle() { return handle_type::from_promise(*this); }

	Generator<T> get_return_object() { return Generator(handle_type::from_promise(*this)); }

	void unhandled_exception() {
		try {
			std::rethrow_exception(std::current_exception());
		} catch (Error& e) {
			error = e;
		} catch (...) {
			error = unknown_error();
		}
	}

	template <std::convertible_to<T> From> // C++20 concept
	n_coroutine::suspend_always yield_value(From&& from) {
		value = std::forward<From>(from);
		return {};
	}

	void return_void() {}
};

template <class T>
struct AsyncGeneratorPromise {
	using promise_type = AsyncGeneratorPromise<T>;

	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	n_coroutine::coroutine_handle<promise_type> handle() {
		return n_coroutine::coroutine_handle<promise_type>::from_promise(*this);
	}

	[[nodiscard]] n_coroutine::suspend_always initial_suspend() const noexcept { return {}; }
	[[nodiscard]] n_coroutine::suspend_always final_suspend() const noexcept { return {}; }

	AsyncGenerator<T> get_return_object() { return AsyncGenerator<T>(&nextPromise, handle()); }

	void return_void() { nextPromise.sendError(end_of_stream()); }

	void unhandled_exception() {
		// The exception should always be type Error.
		try {
			std::rethrow_exception(std::current_exception());
		} catch (const Error& error) {
			nextPromise.sendError(error);
		} catch (...) {
			nextPromise.sendError(unknown_error());
		}
	}

	template <std::convertible_to<T> U>
	n_coroutine::suspend_always yield_value(U&& value) {
		nextPromise.send(std::forward<U>(value));
		return {};
	}

	void setHandle(n_coroutine::coroutine_handle<> h) { mHandle = h; }
	int8_t& waitState() { return mWaitState; }
	void resume() { mHandle.resume(); }

	template <class U>
	auto await_transform(const Future<U>& future) {
		return coro::AwaitableFuture<promise_type, U, false>{ future, this };
	}

	template <class U>
	auto await_transform(const FutureStream<U>& futureStream) {
		return coro::AwaitableFuture<promise_type, U, true>{ futureStream, this };
	}

	n_coroutine::coroutine_handle<> mHandle;
	PromiseStream<T> nextPromise;
	int8_t mWaitState = 0;
};

template <class... Args>
struct HasUncancellable;

template <>
struct HasUncancellable<> {
	static constexpr bool value = false;
};

template <class First, class... Args>
struct HasUncancellable<First, Args...> {
	static constexpr bool value = HasUncancellable<Args...>::value;
};

template <class... Rest>
struct HasUncancellable<Uncancellable, Rest...> {
	static constexpr bool value = true;
};

template <class... Args>
inline constexpr bool hasUncancellable = HasUncancellable<Args...>::value;

} // namespace coro

#endif // FLOW_COROUTINESIMPL_H
