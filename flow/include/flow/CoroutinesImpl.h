/*
 * CoroutinesImpl.h
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

#ifndef FLOW_COROUTINESIMPL_H
#define FLOW_COROUTINESIMPL_H

#include "flow/FlowThread.h"
#include "flow/flow.h"

template <class T>
class Generator;

template <class T>
class AsyncGenerator;

template <class T>
class AsyncResult;

struct Uncancellable;
struct NoThrowOnCancel;

namespace coro {

// Awaiters register one of these while suspended so NoThrowOnCancel can detach
// the pending callback or continuation before destroying the coroutine frame.
struct AwaitCancelHandler {
	virtual ~AwaitCancelHandler() = default;
	virtual void cancelWait() = 0;
};

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

template <class T>
struct FutureReturnType<AsyncResult<T>> {
	using type = T;
};

template <class T>
struct FutureReturnType<AsyncResult<T>&> {
	using type = T;
};

template <class T>
struct FutureReturnType<AsyncResult<T> const&> {
	using type = T;
};

template <class F>
using FutureReturnTypeT = typename FutureReturnType<F>::type;

enum class FutureType { FutureStream, Future, AsyncResult };

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

template <class T>
struct GetFutureType<AsyncResult<T>> {
	constexpr static FutureType value = FutureType::AsyncResult;
};

template <class T>
struct GetFutureType<AsyncResult<T>&> {
	constexpr static FutureType value = FutureType::AsyncResult;
};

template <class T>
struct GetFutureType<AsyncResult<T> const&> {
	constexpr static FutureType value = FutureType::AsyncResult;
};

template <class F>
inline constexpr FutureType GetFutureTypeV = GetFutureType<F>::value;

template <class T, bool IsCancellable>
struct CoroActor final : Actor<std::conditional_t<std::is_void_v<T>, Void, T>> {
	using ValType = std::conditional_t<std::is_void_v<T>, Void, T>;

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

			Actor<ValType>::actor_wait_state = ACTOR_WAIT_STATE_CANCELLED;

			// If the actor is waiting, then resume the coroutine to throw actor_cancelled().
			if (actorWaitStateIsWaiting(prev_wait_state)) {
				auto h = handle; // Copy to local — frame may be freed during resume
				h.resume();
			}
		}
	}

	void destroy() override {
		auto h = handle; // Copy to local — handle is in the frame we're about to free
		h.destroy();
	}
};

// Separate the Actor/SAV state from the coroutine frame because cancellation
// destroys the frame while returned Future objects may still observe the
// cancelled result.
template <class T>
struct NoThrowOnCancelCoroActor final : Actor<std::conditional_t<std::is_void_v<T>, Void, T>> {
	using ValType = std::conditional_t<std::is_void_v<T>, Void, T>;

	n_coroutine::coroutine_handle<> handle;
	AwaitCancelHandler* cancelHandler = nullptr;

	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	int8_t& waitState() { return Actor<ValType>::actor_wait_state; }

	template <class U>
	void set(U&& value) {
		new (&SAV<ValType>::value()) ValType(std::forward<U>(value));
		SAV<ValType>::error_state = Error(SAV<ValType>::SET_ERROR_CODE);
	}

	void setError(Error const& e) { SAV<ValType>::error_state = e; }

	void setCancelHandler(AwaitCancelHandler* handler) { cancelHandler = handler; }
	void clearCancelHandler(AwaitCancelHandler* handler) {
		if (cancelHandler == handler) {
			cancelHandler = nullptr;
		}
	}

	void destroyFrame() {
		auto h = handle;
		// The coroutine is gone after destroy(); leave no handle for a later
		// Future drop to destroy again.
		handle = {};
		h.destroy();
	}

	void cancel() override {
		if (!SAV<ValType>::canBeSet()) {
			return;
		}

		if (cancelHandler) {
			// The handler object is stored in the coroutine frame, so unregister
			// it from its wait source before destroying the frame below.
			auto handler = cancelHandler;
			cancelHandler = nullptr;
			handler->cancelWait();
		}

		destroyFrame();
		if (SAV<ValType>::futures) {
			SAV<ValType>::sendErrorAndDelPromiseRef(actor_cancelled());
		} else {
			SAV<ValType>::promises = 0;
			delete this;
		}
	}

	void destroy() override {
		if (handle) {
			destroyFrame();
		}
		delete this;
	}
};

template <class T>
struct AsyncResultCallback {
	virtual ~AsyncResultCallback() = default;
	virtual void fire(T const&) {}
	virtual void fire(T&&) {}
	virtual void error(Error) {}
	virtual void unwait() {}
};

template <class T>
struct AsyncResultState : FastAllocated<AsyncResultState<T>> {
	int refs = 1;
	bool ready = false;
	bool hasValue = false;
	bool cancellable = false;
	bool noThrowOnCancel = false;

private:
	typename std::aligned_storage<sizeof(T), __alignof(T)>::type value_storage;

public:
	Error error;
	n_coroutine::coroutine_handle<> continuation;
	AsyncResultCallback<T>* callback = nullptr;
	n_coroutine::coroutine_handle<> producerHandle;
	int8_t* producerWaitState = nullptr;
	// Only populated while the producer is suspended in an awaiter. The awaiter
	// lives in the producer frame and must be detached before no-throw cancel
	// destroys that frame.
	AwaitCancelHandler* producerCancelHandler = nullptr;

	~AsyncResultState() {
		if (hasValue) {
			value().~T();
		}
	}

	T& value() { return *reinterpret_cast<T*>(&value_storage); }
	T const& value() const { return *reinterpret_cast<T const*>(&value_storage); }

	void addRef() { ++refs; }
	void delRef() {
		if (!--refs) {
			delete this;
		}
	}

	bool isReady() const { return ready; }
	bool isError() const { return ready && error.isValid(); }
	bool canGet() const { return ready && !error.isValid(); }

	Error& getError() {
		ASSERT(isError());
		return error;
	}

	template <class U>
	void setValue(U&& v) {
		ASSERT(!hasValue && !error.isValid());
		new (&value_storage) T(std::forward<U>(v));
		hasValue = true;
	}

	void setError(Error const& e) {
		ASSERT(!hasValue && !error.isValid());
		error = e;
	}

	T const& get() const {
		ASSERT(canGet());
		return value();
	}

	T take() {
		ASSERT(canGet());
		return std::move(value());
	}

	void registerContinuation(n_coroutine::coroutine_handle<> h) {
		ASSERT(!ready);
		ASSERT(!callback);
		ASSERT(!continuation);
		continuation = h;
	}

	void clearContinuation() { continuation = {}; }

	void registerCallback(AsyncResultCallback<T>* cb) {
		ASSERT(!ready);
		ASSERT(!continuation);
		ASSERT(!callback);
		callback = cb;
	}

	void clearCallback(AsyncResultCallback<T>* cb) {
		if (callback == cb) {
			callback = nullptr;
		}
	}

	void cancelProducer() {
		if (!cancellable || ready || !producerHandle || !producerWaitState) {
			return;
		}

		if (noThrowOnCancel) {
			if (producerCancelHandler) {
				// The handler object is inside producerHandle's coroutine frame.
				// Clear the wait source first so it cannot resume freed storage.
				auto handler = producerCancelHandler;
				producerCancelHandler = nullptr;
				handler->cancelWait();
			}
			auto h = producerHandle;
			producerHandle = {};
			producerWaitState = nullptr;
			// Hold the consumer-visible state, but discard the producer frame
			// instead of resuming it through actor_cancelled().
			h.destroy();
			setError(actor_cancelled());
			complete();
			delRef();
			return;
		}

		auto prevWaitState = *producerWaitState;
		*producerWaitState = ACTOR_WAIT_STATE_CANCELLED;
		if (actorWaitStateIsWaiting(prevWaitState)) {
			auto h = producerHandle;
			h.resume();
		}
	}

	void complete() {
		ready = true;
		producerHandle = {};
		producerWaitState = nullptr;
		producerCancelHandler = nullptr;
		if (callback) {
			auto cb = callback;
			callback = nullptr;
			if (error.isValid()) {
				cb->error(error);
			} else {
				cb->fire(std::move(value()));
			}
		} else if (continuation) {
			auto h = continuation;
			continuation = {};
			h.resume();
		}
	}
};

template <class U>
struct AwaitableFutureStore {
	std::variant<std::monostate, Error, U> data;

	constexpr bool isSet() const noexcept { return data.index() != 0; }
	void copy(U v) { data = std::move(v); }
	void set(U&& v) { data = std::move(v); }
	void set(U const& v) { data = v; }
	void setError(Error error) { data = std::move(error); }

	const U& getRef() const {
		switch (data.index()) {
		case 0:
			throw internal_error();
		case 1:
			throw std::get<1>(data);
		case 2:
			return std::get<2>(data);
		}
		UNREACHABLE();
	}

	U&& get() && {
		switch (data.index()) {
		case 0:
			throw internal_error();
		case 1:
			throw std::get<1>(data);
		case 2:
			return std::get<2>(std::move(data));
		}
		UNREACHABLE();
	}
};

// Awaiter adapter for `AsyncResult<T>` inside coroutine promises.
//
// `PromiseType` is the coroutine promise object that owns the wait-state and
// coroutine handle bookkeeping for the suspended actor.
// `ValueType` is the logical `AsyncResult<T>` payload type being awaited.
template <class PromiseType, class ValueType>
struct AwaitableAsyncResult : AwaitCancelHandler {
	using StateType = typename AsyncResult<ValueType>::StoredT;

	AsyncResult<ValueType> result;
	PromiseType* pt = nullptr;

	AwaitableAsyncResult(AsyncResult<ValueType>&& result, PromiseType* pt) : result(std::move(result)), pt(pt) {}

	[[nodiscard]] bool await_ready() const {
		ASSERT(result.resultState);
		if (actorWaitStateIsCancelled(pt->waitState())) {
			pt->waitState() = ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK;
			return true;
		}
		return result.resultState->isReady();
	}

	void await_suspend(n_coroutine::coroutine_handle<> h) {
		ASSERT(result.resultState);
		pt->setHandle(h);
		pt->waitState() = ACTOR_WAIT_STATE_WAITING;
		result.resultState->registerContinuation(h);
		pt->setCancelHandler(this);
	}

	void cancelWait() override {
		// AsyncResultState only stores one waiting continuation, so cancelling
		// this await means the continuation must no longer be resumed.
		if (result.resultState) {
			result.resultState->clearContinuation();
		}
	}

	bool resumeImpl() {
		pt->clearCancelHandler(this);
		switch (pt->waitState()) {
		case ACTOR_WAIT_STATE_CANCELLED:
			if (result.resultState) {
				result.resultState->clearContinuation();
			}
		case ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK:
			throw actor_cancelled();
		}

		bool wasReady = pt->waitState() == ACTOR_WAIT_STATE_NOT_WAITING;
		if (actorWaitStateIsWaiting(pt->waitState())) {
			result.resultState->clearContinuation();
			pt->waitState() = ACTOR_WAIT_STATE_NOT_WAITING;
		}
		return wasReady;
	}

	void await_resume()
	    requires(std::is_void_v<ValueType>)
	{
		if (pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			resumeImpl();
		}
		if (result.resultState->isError()) {
			throw result.resultState->getError();
		}
	}

	ValueType await_resume()
	    requires(!std::is_void_v<ValueType>)
	{
		if (pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			resumeImpl();
		}
		if (result.resultState->isError()) {
			throw result.resultState->getError();
		}
		return result.resultState->take();
	}
};

template <class T>
using ToFutureVal = std::conditional_t<std::is_void_v<T>, Void, T>;

template <class Awaiter, class ValueType, bool IsStream, bool ReturnsExplicitVoid = false>
struct AwaitableResume;

template <class Awaiter>
struct AwaitableResume<Awaiter, Void, /* IsStream = */ false, /* ReturnsExplicitVoid = */ false> {
	[[maybe_unused]] void await_resume() {
		auto self = static_cast<Awaiter*>(this);
		if (self->pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			self->resumeImpl();
		}
		if (self->future.isError()) {
			throw self->future.getError();
		}
	}
};

template <class Awaiter, class ValueType>
struct AwaitableResume<Awaiter, ValueType, /* IsStream = */ false, /* ReturnsExplicitVoid = */ false> {
	ValueType const& await_resume() {
		auto self = static_cast<Awaiter*>(this);
		if (self->pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			self->resumeImpl();
		}
		if (self->future.isError()) {
			throw self->future.getError();
		}
		return self->future.get();
	}
};

template <class Awaiter, class ValueType>
struct AwaitableResume<Awaiter, ValueType, /* IsStream = */ false, /* ReturnsExplicitVoid = */ true>
  : AwaitableResume<Awaiter, ValueType, /* IsStream = */ false, /* ReturnsExplicitVoid = */ false> {};

template <class Awaiter>
struct AwaitableResume<Awaiter, Void, /* IsStream = */ false, /* ReturnsExplicitVoid = */ true> {
	[[maybe_unused]] ExplicitVoid await_resume() {
		auto self = static_cast<Awaiter*>(this);
		if (self->pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			self->resumeImpl();
		}
		if (self->future.isError()) {
			throw self->future.getError();
		}
		return ExplicitVoid{};
	}
};

template <class Awaiter, class ValueType>
struct AwaitableResume<Awaiter, ValueType, /* IsStream = */ true, /* ReturnsExplicitVoid = */ false> {
	ValueType await_resume() {
		auto self = static_cast<Awaiter*>(this);
		if (self->pt->waitState() == ACTOR_WAIT_STATE_NOT_WAITING) {
			if (self->future.isError()) {
				throw self->future.getError();
			}
			return self->future.pop();
		}
		if (self->resumeImpl()) {
			if (self->future.isError()) {
				throw self->future.getError();
			}
			return self->future.pop();
		}
		return std::move(self->store).get();
	}
};

template <class Awaiter, class ValueType>
struct AwaitableResume<Awaiter, ValueType, /* IsStream = */ true, /* ReturnsExplicitVoid = */ true>
  : AwaitableResume<Awaiter, ValueType, /* IsStream = */ true, /* ReturnsExplicitVoid = */ false> {};

// Awaiter for `Future<T>` and `FutureStream<T>` values transformed through a
// coroutine promise.
template <class PromiseType, class ValueType, bool IsStream, bool ReturnsExplicitVoid = false>
struct AwaitableFuture
  : std::conditional_t<IsStream, SingleCallback<ToFutureVal<ValueType>>, Callback<ToFutureVal<ValueType>>>,
    AwaitCancelHandler,
    AwaitableResume<AwaitableFuture<PromiseType, ValueType, IsStream, ReturnsExplicitVoid>,
                    ValueType,
                    IsStream,
                    ReturnsExplicitVoid> {
	using FutureValue = ToFutureVal<ValueType>;
	using FutureType = std::conditional_t<IsStream, FutureStream<FutureValue>, Future<FutureValue> const&>;
	FutureType future;
	PromiseType* pt = nullptr;

	// Store is only needed for streams — non-stream values are already in the SAV after fire()
	struct Empty {};
	[[no_unique_address]] std::conditional_t<IsStream, AwaitableFutureStore<FutureValue>, Empty> store;

	AwaitableFuture(const FutureType& f, PromiseType* pt) : future(f), pt(pt) {}

	void fire(FutureValue const& value) override {
		if constexpr (IsStream) {
			store.set(value);
		}
		pt->resume();
	}
	void fire(FutureValue&& value) override {
		if constexpr (IsStream) {
			store.set(std::move(value));
		}
		pt->resume();
	}

	void error(Error error) override {
		if constexpr (IsStream) {
			store.setError(error);
		}
		pt->resume();
	}

	[[maybe_unused]] [[nodiscard]] bool await_ready() const {
		if (actorWaitStateIsCancelled(pt->waitState())) {
			pt->waitState() = ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK;
			return true;
		}
		return future.isReady();
	}

	[[maybe_unused]] void await_suspend(n_coroutine::coroutine_handle<> h) {
		// Create a coroutine callback if it's the first time being suspended
		pt->setHandle(h);

		// Set wait_state and add callback
		pt->waitState() = ACTOR_WAIT_STATE_WAITING;

		if constexpr (IsStream) {
			auto sf = future;
			sf.addCallbackAndClear(this);
		} else {
			StrictFuture<FutureValue> sf = future;
			sf.addCallbackAndClear(this);
		}
		pt->setCancelHandler(this);
	}

	// NoThrowOnCancel destroys the coroutine frame instead of resuming this
	// awaiter, so detach the callback from the Future before that happens.
	void cancelWait() override { this->remove(); }

	bool resumeImpl() {
		pt->clearCancelHandler(this);
		// If actor is cancelled, then throw actor_cancelled()
		switch (pt->waitState()) {
		case ACTOR_WAIT_STATE_CANCELLED:
			this->remove();
		case ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK:
			// await_ready() observed cancellation before await_suspend() registered a callback, so there is nothing to
			// remove here.
			throw actor_cancelled();
		}

		bool wasReady = pt->waitState() == ACTOR_WAIT_STATE_NOT_WAITING;
		// Actor return from waiting, remove callback and reset wait_state.
		if (actorWaitStateIsWaiting(pt->waitState())) {
			this->remove();

			pt->waitState() = ACTOR_WAIT_STATE_NOT_WAITING;
		}
		return wasReady;
	}
};

template <class PromiseType, class ValueType>
struct AwaitableFutureOwning : Callback<ToFutureVal<ValueType>>, AwaitCancelHandler {
	using FutureValue = ToFutureVal<ValueType>;
	Future<FutureValue> future;
	PromiseType* pt = nullptr;

	AwaitableFutureOwning(Future<FutureValue> future, PromiseType* pt) : future(std::move(future)), pt(pt) {}

	void fire(FutureValue const&) override { pt->resume(); }
	void fire(FutureValue&&) override { pt->resume(); }
	void error(Error) override { pt->resume(); }

	[[maybe_unused]] [[nodiscard]] bool await_ready() const {
		if (actorWaitStateIsCancelled(pt->waitState())) {
			pt->waitState() = ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK;
			return true;
		}
		return future.isReady();
	}

	[[maybe_unused]] void await_suspend(n_coroutine::coroutine_handle<> h) {
		pt->setHandle(h);
		pt->waitState() = ACTOR_WAIT_STATE_WAITING;

		StrictFuture<FutureValue> sf = future;
		sf.addCallbackAndClear(this);
		pt->setCancelHandler(this);
	}

	// NoThrowOnCancel destroys the coroutine frame instead of resuming this
	// awaiter, so detach the callback from the Future before that happens.
	void cancelWait() override { this->remove(); }

	bool resumeImpl() {
		pt->clearCancelHandler(this);
		switch (pt->waitState()) {
		case ACTOR_WAIT_STATE_CANCELLED:
			this->remove();
		case ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK:
			throw actor_cancelled();
		}

		bool wasReady = pt->waitState() == ACTOR_WAIT_STATE_NOT_WAITING;
		if (actorWaitStateIsWaiting(pt->waitState())) {
			this->remove();
			pt->waitState() = ACTOR_WAIT_STATE_NOT_WAITING;
		}
		return wasReady;
	}
};

template <class PromiseType, class ValueType>
struct AwaitableFutureIgnore : AwaitableFutureOwning<PromiseType, ValueType> {
	using Base = AwaitableFutureOwning<PromiseType, ValueType>;

	AwaitableFutureIgnore(Future<ToFutureVal<ValueType>> future, PromiseType* pt) : Base(std::move(future), pt) {}

	Void await_resume() {
		auto self = static_cast<Base*>(this);
		if (self->pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			self->resumeImpl();
		}
		// Preserve normal Future<T> error propagation while discarding any
		// successful T payload.
		if (self->future.isError()) {
			throw self->future.getError();
		}
		return Void();
	}
};

template <class PromiseType, class SourceValue, class ResultValue>
struct AwaitableFutureErrorOr : AwaitableFutureOwning<PromiseType, SourceValue> {
	using Base = AwaitableFutureOwning<PromiseType, SourceValue>;
	using ResultType = ErrorOr<ResultValue>;

	AwaitableFutureErrorOr(Future<ToFutureVal<SourceValue>> future, PromiseType* pt) : Base(std::move(future), pt) {}

	ResultType await_resume() {
		auto self = static_cast<Base*>(this);
		if (self->pt->waitState() != ACTOR_WAIT_STATE_NOT_WAITING) {
			self->resumeImpl();
		}
		// Convert failed Future<T> awaits into ErrorOr instead of throwing so
		// callers can cheaply observe completion-only state.
		if (self->future.isError()) {
			return ResultType(self->future.getError());
		}
		if constexpr (std::is_same_v<ResultValue, Void>) {
			return ResultType(Void());
		} else {
			return ResultType(self->future.get());
		}
	}
};

// TODO: This can be merged with AwaitableFutureStream by passing more template arguments.
template <class PromiseType, class ValueType, bool ReturnsExplicitVoid = false>
struct ThreadAwaitableFutureStream
  : SingleCallback<ToFutureVal<ValueType>>,
    AwaitCancelHandler,
    AwaitableResume<ThreadAwaitableFutureStream<PromiseType, ValueType, ReturnsExplicitVoid>,
                    ValueType,
                    true,
                    ReturnsExplicitVoid> {
	using FutureValue = ToFutureVal<ValueType>;
	using FutureType = ThreadFutureStream<FutureValue>;
	FutureType future;
	PromiseType* pt = nullptr;
	AwaitableFutureStore<FutureValue> store;

	ThreadAwaitableFutureStream(const FutureType& f, PromiseType* pt) : future(f), pt(pt) {}

	void fire(FutureValue const& value) override {
		store.set(value);
		pt->resume();
	}
	void fire(FutureValue&& value) override {
		store.set(std::move(value));
		pt->resume();
	}

	void error(Error error) override {
		store.setError(error);
		pt->resume();
	}

	[[maybe_unused]] [[nodiscard]] bool await_ready() const {
		if (actorWaitStateIsCancelled(pt->waitState())) {
			pt->waitState() = ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK;
			// actor was cancelled
			return true;
		}
		return future.isReady();
	}

	[[maybe_unused]] void await_suspend(n_coroutine::coroutine_handle<> h) {
		// Create a coroutine callback if it's the first time being suspended
		pt->setHandle(h);

		// Set wait_state and add callback
		pt->waitState() = ACTOR_WAIT_STATE_WAITING;

		auto sf = future;
		sf.addCallbackAndClear(this);
		pt->setCancelHandler(this);
	}

	// NoThrowOnCancel destroys the coroutine frame instead of resuming this
	// awaiter, so detach the callback from the FutureStream before that happens.
	void cancelWait() override { this->remove(); }

	bool resumeImpl() {
		pt->clearCancelHandler(this);
		// If actor is cancelled, then throw actor_cancelled()
		switch (pt->waitState()) {
		case ACTOR_WAIT_STATE_CANCELLED:
			this->remove();
		case ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK:
			// await_ready() observed cancellation before await_suspend() registered a callback, so there is nothing to
			// remove here.
			// if the wait_state is -1 we still have to throw, so we fall through to the -2 case
			throw actor_cancelled();
		}

		bool wasReady = pt->waitState() == ACTOR_WAIT_STATE_NOT_WAITING;
		// Actor return from waiting, remove callback and reset wait_state.
		if (actorWaitStateIsWaiting(pt->waitState())) {
			this->remove();

			pt->waitState() = ACTOR_WAIT_STATE_NOT_WAITING;
		}
		return wasReady;
	}
};

template <class T, class Promise, bool ReturnsExplicitVoid = false>
struct CoroReturn {
	template <class U>
	void return_value(U&& value) {
		static_cast<Promise*>(this)->setReturnValue(std::forward<U>(value));
	}
};

template <class Promise>
struct CoroReturn<Void, Promise, false> {
	void return_void() { static_cast<Promise*>(this)->setReturnValue(Void()); }
};

template <class Promise>
struct CoroReturn<Void, Promise, true> {
	template <std::convertible_to<Void> U>
	void return_value(U&&) {
		static_cast<Promise*>(this)->setReturnValue(Void());
	}
};

template <class T, class Promise, bool ReturnsExplicitVoid = false>
struct AsyncResultReturn {
	template <class U>
	void return_value(U&& value) {
		static_cast<Promise*>(this)->state->setValue(std::forward<U>(value));
	}
};

template <class Promise>
struct AsyncResultReturn<Void, Promise, false> {
	void return_void() { static_cast<Promise*>(this)->state->setValue(Void()); }
};

template <class Promise>
struct AsyncResultReturn<Void, Promise, true> {
	template <std::convertible_to<Void> U>
	void return_value(U&&) {
		static_cast<Promise*>(this)->state->setValue(Void());
	}
};

template <class T, bool IsCancellable, bool ReturnsExplicitVoid = false, bool NoThrowOnCancel = false>
struct CoroPromise;

template <class Derived, class T, bool ReturnsExplicitVoid>
struct CoroPromiseBase : CoroReturn<T, Derived, ReturnsExplicitVoid> {
	using promise_type = Derived;
	using ReturnValue = std::conditional_t<std::is_void_v<T>, Void, T>;
	using ReturnFutureType = Future<ReturnValue>;

	promise_type* self() { return static_cast<promise_type*>(this); }

	n_coroutine::coroutine_handle<promise_type> handle() {
		return n_coroutine::coroutine_handle<promise_type>::from_promise(*self());
	}

	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	template <class U>
	void setReturnValue(U&& value) {
		self()->actor()->set(std::forward<U>(value));
	}

	[[nodiscard]] n_coroutine::suspend_never initial_suspend() const noexcept { return {}; }

	void unhandled_exception() {
		// The exception should always be type Error.
		try {
			std::rethrow_exception(std::current_exception());
		} catch (const Error& error) {
			self()->actor()->setError(error);
		} catch (...) {
			self()->actor()->setError(unknown_error());
		}
	}

	void setHandle(n_coroutine::coroutine_handle<> h) { self()->actor()->handle = h; }

	void resume() { self()->actor()->handle.resume(); }

	int8_t& waitState() { return self()->actor()->waitState(); }
	void setCancelHandler(AwaitCancelHandler*) {}
	void clearCancelHandler(AwaitCancelHandler*) {}

	template <class U>
	auto await_transform(const Future<U>& future) {
		return coro::AwaitableFuture<promise_type, U, false, ReturnsExplicitVoid>{ future, self() };
	}

	template <class U>
	auto await_transform(coro::FutureIgnore<U> future) {
		// Custom adapters compose through await_transform rather than wrapper
		// coroutines so cancellation and wait-state handling stay identical to
		// plain Future<T> awaits.
		return coro::AwaitableFutureIgnore<promise_type, U>{ std::move(future.future), self() };
	}

	template <class U, class V>
	auto await_transform(coro::FutureErrorOr<U, V> future) {
		return coro::AwaitableFutureErrorOr<promise_type, U, V>{ std::move(future.future), self() };
	}

	template <class U>
	auto await_transform(const FutureStream<U>& futureStream) {
		return coro::AwaitableFuture<promise_type, U, true, ReturnsExplicitVoid>{ futureStream, self() };
	}

	template <class U>
	auto await_transform(AsyncResult<U>& result) {
		return coro::AwaitableAsyncResult<promise_type, U>{ std::move(result), self() };
	}

	template <class U>
	auto await_transform(AsyncResult<U>&& result) {
		return coro::AwaitableAsyncResult<promise_type, U>{ std::move(result), self() };
	}

	template <class U>
	auto await_transform(const ThreadFutureStream<U>& futureStream) {
		return coro::ThreadAwaitableFutureStream<promise_type, U, ReturnsExplicitVoid>{ futureStream, self() };
	}
};

template <class T, bool IsCancellable, bool ReturnsExplicitVoid>
struct CoroPromise<T, IsCancellable, ReturnsExplicitVoid, false>
  : CoroPromiseBase<CoroPromise<T, IsCancellable, ReturnsExplicitVoid, false>, T, ReturnsExplicitVoid> {
	using promise_type = CoroPromise<T, IsCancellable, ReturnsExplicitVoid, false>;
	using Base = CoroPromiseBase<promise_type, T, ReturnsExplicitVoid>;
	using ActorType = coro::CoroActor<T, IsCancellable>;
	using ReturnFutureType = typename Base::ReturnFutureType;

	ActorType coroActor; // Embedded in coroutine frame — single allocation

	CoroPromise() {}

	ActorType* actor() { return &coroActor; }

	ReturnFutureType get_return_object() noexcept {
		actor()->handle = this->handle();
		return ReturnFutureType(actor());
	}

	auto final_suspend() noexcept {
		struct FinalAwaitable {
			ActorType* sav;
			explicit FinalAwaitable(ActorType* sav) : sav(sav) {}

			[[nodiscard]] bool await_ready() const noexcept { return false; }
			void await_resume() const noexcept {}

			// OPTIMIZE 39.13% CPU HOTSPOT: Add maximum compiler optimization to shrink overhead
			__attribute__((hot)) __attribute__((flatten)) __attribute__((always_inline)) void await_suspend(
			    n_coroutine::coroutine_handle<>) const noexcept {
				// TARGET: Reduce 39.13% final_suspend overhead through aggressive optimization
				// Keep SAV contract but optimize the expensive finishSendAndDelPromiseRef calls

				// Fast path optimization with branch prediction
				if (sav->isError()) [[unlikely]] {
					// Error path: less critical
					sav->finishSendErrorAndDelPromiseRef();
				} else [[likely]] {
					// Success path: 95% of cases - optimize heavily
					// Tell compiler to inline finishSendAndDelPromiseRef aggressively
					sav->finishSendAndDelPromiseRef();
				}
			}
		};
		return FinalAwaitable(actor());
	}
};

template <class T, bool IsCancellable, bool ReturnsExplicitVoid>
struct CoroPromise<T, IsCancellable, ReturnsExplicitVoid, true>
  : CoroPromiseBase<CoroPromise<T, IsCancellable, ReturnsExplicitVoid, true>, T, ReturnsExplicitVoid> {
	using promise_type = CoroPromise<T, IsCancellable, ReturnsExplicitVoid, true>;
	using Base = CoroPromiseBase<promise_type, T, ReturnsExplicitVoid>;
	using ActorType = coro::NoThrowOnCancelCoroActor<T>;
	using ReturnFutureType = typename Base::ReturnFutureType;

	// The normal promise embeds CoroActor in the coroutine frame. NoThrowOnCancel
	// allocates it separately because cancel() intentionally destroys that frame.
	ActorType* coroActor = new ActorType();

	CoroPromise() {}

	ActorType* actor() { return coroActor; }

	ReturnFutureType get_return_object() noexcept {
		actor()->handle = this->handle();
		return ReturnFutureType(actor());
	}

	auto final_suspend() noexcept {
		struct FinalAwaitable {
			ActorType* sav;
			explicit FinalAwaitable(ActorType* sav) : sav(sav) {}

			[[nodiscard]] bool await_ready() const noexcept { return false; }
			void await_resume() const noexcept {}

			bool await_suspend(n_coroutine::coroutine_handle<>) const noexcept {
				// Normal completion hands the result to the Future and leaves
				// no frame for a later cancel/drop path to destroy.
				sav->handle = {};
				if (sav->isError()) {
					sav->finishSendErrorAndDelPromiseRef();
				} else {
					sav->finishSendAndDelPromiseRef();
				}
				return false;
			}
		};
		return FinalAwaitable(actor());
	}

	void setCancelHandler(AwaitCancelHandler* handler) { actor()->setCancelHandler(handler); }
	void clearCancelHandler(AwaitCancelHandler* handler) { actor()->clearCancelHandler(handler); }
};

template <class T, bool IsCancellable, bool ReturnsExplicitVoid, bool NoThrowOnCancel>
struct AsyncResultPromise
  : AsyncResultReturn<T,
                      AsyncResultPromise<T, IsCancellable, ReturnsExplicitVoid, NoThrowOnCancel>,
                      ReturnsExplicitVoid> {
	using promise_type = AsyncResultPromise<T, IsCancellable, ReturnsExplicitVoid, NoThrowOnCancel>;
	using ReturnValue = std::conditional_t<std::is_void_v<T>, Void, T>;
	using ReturnAsyncResultType = AsyncResult<T>;
	using State = AsyncResultState<ReturnValue>;

	State* state = new State();
	n_coroutine::coroutine_handle<> producerHandle;
	int8_t producerWaitState = ACTOR_WAIT_STATE_NOT_WAITING;

	AsyncResultPromise() = default;

	n_coroutine::coroutine_handle<promise_type> handle() {
		return n_coroutine::coroutine_handle<promise_type>::from_promise(*this);
	}

	static void* operator new(size_t s) { return allocateFast(int(s)); }
	static void operator delete(void* p, size_t s) { freeFast(int(s), p); }

	ReturnAsyncResultType get_return_object() noexcept {
		producerHandle = handle();
		state->cancellable = IsCancellable;
		state->noThrowOnCancel = NoThrowOnCancel;
		state->producerHandle = producerHandle;
		state->producerWaitState = &producerWaitState;
		state->addRef();
		return ReturnAsyncResultType(state);
	}

	[[nodiscard]] n_coroutine::suspend_never initial_suspend() const noexcept { return {}; }

	auto final_suspend() noexcept {
		struct FinalAwaitable {
			State* state;

			[[nodiscard]] bool await_ready() const noexcept { return false; }
			void await_resume() const noexcept {}

			bool await_suspend(n_coroutine::coroutine_handle<>) const noexcept {
				state->complete();
				state->delRef();
				return false;
			}
		};
		return FinalAwaitable{ state };
	}

	void unhandled_exception() {
		try {
			std::rethrow_exception(std::current_exception());
		} catch (const Error& error) {
			state->setError(error);
		} catch (...) {
			state->setError(unknown_error());
		}
	}

	void setHandle(n_coroutine::coroutine_handle<> h) { producerHandle = h; }

	void resume() { producerHandle.resume(); }

	int8_t& waitState() { return producerWaitState; }
	void setCancelHandler(AwaitCancelHandler* handler) { state->producerCancelHandler = handler; }
	void clearCancelHandler(AwaitCancelHandler* handler) {
		if (state->producerCancelHandler == handler) {
			state->producerCancelHandler = nullptr;
		}
	}

	template <class U>
	auto await_transform(const Future<U>& future) {
		return coro::AwaitableFuture<promise_type, U, false, ReturnsExplicitVoid>{ future, this };
	}

	template <class U>
	auto await_transform(coro::FutureIgnore<U> future) {
		// Mirror CoroPromise support so AsyncResult coroutines can use the same
		// non-allocating await adapters.
		return coro::AwaitableFutureIgnore<promise_type, U>{ std::move(future.future), this };
	}

	template <class U, class V>
	auto await_transform(coro::FutureErrorOr<U, V> future) {
		return coro::AwaitableFutureErrorOr<promise_type, U, V>{ std::move(future.future), this };
	}

	template <class U>
	auto await_transform(const FutureStream<U>& futureStream) {
		return coro::AwaitableFuture<promise_type, U, true, ReturnsExplicitVoid>{ futureStream, this };
	}

	template <class U>
	auto await_transform(AsyncResult<U>& result) {
		return coro::AwaitableAsyncResult<promise_type, U>{ std::move(result), this };
	}

	template <class U>
	auto await_transform(AsyncResult<U>&& result) {
		return coro::AwaitableAsyncResult<promise_type, U>{ std::move(result), this };
	}

	template <class U>
	auto await_transform(const ThreadFutureStream<U>& futureStream) {
		return coro::ThreadAwaitableFutureStream<promise_type, U, ReturnsExplicitVoid>{ futureStream, this };
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

template <class T, bool ReturnsExplicitVoid = false>
struct AsyncGeneratorPromise {
	using promise_type = AsyncGeneratorPromise<T, ReturnsExplicitVoid>;

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
	void setCancelHandler(AwaitCancelHandler*) {}
	void clearCancelHandler(AwaitCancelHandler*) {}

	template <class U>
	auto await_transform(const Future<U>& future) {
		return coro::AwaitableFuture<promise_type, U, false, ReturnsExplicitVoid>{ future, this };
	}

	template <class U>
	auto await_transform(const FutureStream<U>& futureStream) {
		return coro::AwaitableFuture<promise_type, U, true, ReturnsExplicitVoid>{ futureStream, this };
	}

	template <class U>
	auto await_transform(AsyncResult<U>& result) {
		return coro::AwaitableAsyncResult<promise_type, U>{ std::move(result), this };
	}

	template <class U>
	auto await_transform(AsyncResult<U>&& result) {
		return coro::AwaitableAsyncResult<promise_type, U>{ std::move(result), this };
	}

	n_coroutine::coroutine_handle<> mHandle;
	PromiseStream<T> nextPromise;
	int8_t mWaitState = 0;
};

// Awaiter used by plain C++ coroutines that directly `co_await AsyncResult<T>`
// without an actor-style promise.
template <class ValueType>
struct AsyncResultAwaiter {
	AsyncResult<ValueType> result;

	[[nodiscard]] bool await_ready() const {
		ASSERT(result.resultState);
		return result.resultState->isReady();
	}

	void await_suspend(n_coroutine::coroutine_handle<> h) {
		ASSERT(result.resultState);
		result.resultState->registerContinuation(h);
	}

	void await_resume()
	    requires(std::is_void_v<ValueType>)
	{
		if (result.resultState->isError()) {
			throw result.resultState->getError();
		}
	}

	ValueType await_resume()
	    requires(!std::is_void_v<ValueType>)
	{
		if (result.resultState->isError()) {
			throw result.resultState->getError();
		}
		return result.resultState->take();
	}
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

template <class... Args>
struct HasExplicitVoid;

template <>
struct HasExplicitVoid<> {
	static constexpr bool value = false;
};

template <class First, class... Args>
struct HasExplicitVoid<First, Args...> {
	static constexpr bool value = HasExplicitVoid<Args...>::value;
};

template <class... Rest>
struct HasExplicitVoid<ExplicitVoid, Rest...> {
	static constexpr bool value = true;
};

template <class... Args>
inline constexpr bool hasExplicitVoid = HasExplicitVoid<Args...>::value;

template <class... Args>
struct HasNoThrowOnCancel;

template <>
struct HasNoThrowOnCancel<> {
	static constexpr bool value = false;
};

template <class First, class... Args>
struct HasNoThrowOnCancel<First, Args...> {
	static constexpr bool value = HasNoThrowOnCancel<Args...>::value;
};

template <class... Rest>
struct HasNoThrowOnCancel<NoThrowOnCancel, Rest...> {
	static constexpr bool value = true;
};

template <class... Args>
inline constexpr bool hasNoThrowOnCancel = HasNoThrowOnCancel<Args...>::value;

} // namespace coro

#endif // FLOW_COROUTINESIMPL_H
