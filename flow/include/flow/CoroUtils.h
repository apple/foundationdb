/*
 * CoroUtils.h
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

#ifndef FLOW_COROUTILS_H
#define FLOW_COROUTILS_H

#include "flow/flow.h"

namespace coro {

// Adapts `AsyncResult<T>` callbacks to the same callback interface used by
// `choose()` and `race()` over Future/FutureStream inputs.
template <class Parent, int Idx, class ValueType>
struct ActorAsyncResultCallback : AsyncResultCallback<ValueType> {
	AsyncResultState<ValueType>* state = nullptr;

	void bind(AsyncResult<ValueType>& result) { state = result.resultState; }

	void remove() {
		if (state) {
			state->clearCallback(this);
			state = nullptr;
		}
	}

	void fire(ValueType const& value) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<Parent*>(this)->lineageAddr());
#endif
		static_cast<Parent*>(this)->a_callback_fire(this, value);
	}

	void fire(ValueType&& value) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<Parent*>(this)->lineageAddr());
#endif
		static_cast<Parent*>(this)->a_callback_fire(this, std::move(value));
	}

	void error(Error e) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<Parent*>(this)->lineageAddr());
#endif
		static_cast<Parent*>(this)->a_callback_error(this, e);
	}
};

template <class Parent, int Idx, class AwaitableType>
using ConditionalActorCallback =
    std::conditional_t<GetFutureTypeV<AwaitableType> == FutureType::Future,
                       ActorCallback<Parent, Idx, FutureReturnTypeT<AwaitableType>>,
                       std::conditional_t<GetFutureTypeV<AwaitableType> == FutureType::FutureStream,
                                          ActorSingleCallback<Parent, Idx, FutureReturnTypeT<AwaitableType>>,
                                          ActorAsyncResultCallback<Parent, Idx, FutureReturnTypeT<AwaitableType>>>>;

template <class Parent, int Idx, class... Args>
struct ChooseImplCallback;

template <class Parent, int Idx, class AwaitableType, class... Args>
struct ChooseImplCallback<Parent, Idx, AwaitableType, Args...>
  : ConditionalActorCallback<ChooseImplCallback<Parent, Idx, AwaitableType, Args...>, Idx, AwaitableType>,
    ChooseImplCallback<Parent, Idx + 1, Args...> {

	using ThisCallback =
	    ConditionalActorCallback<ChooseImplCallback<Parent, Idx, AwaitableType, Args...>, Idx, AwaitableType>;
	using ValueType = FutureReturnTypeT<AwaitableType>;
	static constexpr FutureType futureType = GetFutureTypeV<AwaitableType>;

	[[nodiscard]] Parent* getParent() { return static_cast<Parent*>(this); }

	void registerCallbacks() {
		if constexpr (futureType == FutureType::Future) {
			StrictFuture<ValueType> sf = std::get<Idx>(getParent()->futures);
			sf.addCallbackAndClear(static_cast<ThisCallback*>(this));
		} else {
			auto sf = std::get<Idx>(getParent()->futures);
			sf.addCallbackAndClear(static_cast<ThisCallback*>(this));
		}
		if constexpr (sizeof...(Args) > 0) {
			ChooseImplCallback<Parent, Idx + 1, Args...>::registerCallbacks();
		}
	}

	void a_callback_fire(ThisCallback*, ValueType const& value) {
		getParent()->actor_wait_state = ACTOR_WAIT_STATE_NOT_WAITING;
		getParent()->removeCallbacks();
		try {
			std::get<Idx>(getParent()->functions)(value);
			getParent()->SAV<Void>::sendAndDelPromiseRef(Void());
		} catch (Error& e) {
			getParent()->SAV<Void>::sendErrorAndDelPromiseRef(e);
		} catch (...) {
			getParent()->SAV<Void>::sendErrorAndDelPromiseRef(unknown_error());
		}
	}

	void a_callback_error(ThisCallback*, Error e) {
		getParent()->actor_wait_state = ACTOR_WAIT_STATE_NOT_WAITING;
		getParent()->removeCallbacks();
		getParent()->SAV<Void>::sendErrorAndDelPromiseRef(e);
	}

	void removeCallbacks() {
		ThisCallback::remove();
		if constexpr (sizeof...(Args) > 0) {
			ChooseImplCallback<Parent, Idx + 1, Args...>::removeCallbacks();
		}
	}
};

template <class Parent, int Idx>
struct ChooseImplCallback<Parent, Idx> {
#ifdef ENABLE_SAMPLING
	LineageReference* lineageAddr() { return currentLineage; }
#endif
};

template <class... Args>
struct ChooseImplActor final : Actor<Void>,
                               ChooseImplCallback<ChooseImplActor<Args...>, 0, Args...>,
                               FastAllocated<ChooseImplActor<Args...>> {
	std::tuple<Args...> futures;
	std::tuple<std::function<void(FutureReturnTypeT<Args> const&)>...> functions;

	using FastAllocated<ChooseImplActor<Args...>>::operator new;
	using FastAllocated<ChooseImplActor<Args...>>::operator delete;

	ChooseImplActor(std::tuple<Args...>&& futures,
	                std::tuple<std::function<void(FutureReturnTypeT<Args> const&)>...>&& functions)
	  : Actor<Void>(), futures(futures), functions(functions) {
		ChooseImplCallback<ChooseImplActor<Args...>, 0, Args...>::registerCallbacks();
		actor_wait_state = ACTOR_WAIT_STATE_WAITING;
	}

	void cancel() override {
		const auto waitState = actor_wait_state;
		actor_wait_state = ACTOR_WAIT_STATE_CANCELLED;
		if (actorWaitStateIsWaiting(waitState)) {
			ChooseImplCallback<ChooseImplActor<Args...>, 0, Args...>::removeCallbacks();
			SAV<Void>::sendErrorAndDelPromiseRef(actor_cancelled());
		}
	}

	void destroy() override { delete this; }
};

template <class... Args>
class ChooseClause {
	std::tuple<Args...> futures;
	std::tuple<std::function<void(FutureReturnTypeT<Args> const&)>...> functions;
	bool noop = false;

	template <class T, bool IsStream>
	auto getNoop() {
		using FType = std::conditional_t<IsStream, FutureStream<T>, Future<T>>;
		return ChooseClause<Args..., FType>(std::tuple_cat(std::move(futures), std::make_tuple(FType())), true);
	}

	template <class T>
	auto getNoop(const Future<T>& future) {
		return ChooseClause<Args..., Future<T> const&>(
		    std::tuple_cat(std::move(futures), std::make_tuple(std::cref(future))), true);
	}

	template <class T>
	auto getNoop(const FutureStream<T>& future) {
		return ChooseClause<Args..., FutureStream<T> const&>(
		    std::tuple_cat(std::move(futures), std::make_tuple(std::cref(future))), true);
	}

public:
	ChooseClause(std::tuple<Args...>&& futures,
	             std::tuple<std::function<void(FutureReturnTypeT<Args> const&)>...>&& functions)
	  : futures(std::move(futures)), functions(std::move(functions)) {}
	explicit ChooseClause(std::tuple<Args...>&& futures, bool noop = false) : futures(std::move(futures)), noop(noop) {}
	explicit ChooseClause() : futures(std::tuple<>()) {}

	auto When(std::invocable auto futureCallback, std::invocable<decltype(futureCallback().get())> auto fun) {
		using FType = decltype(futureCallback());
		using FReturnType = FutureReturnTypeT<FType>;
		constexpr bool isStream = GetFutureTypeV<FType> == FutureType::FutureStream;
		using ArgType = std::conditional_t<isStream, FutureStream<FReturnType>, Future<FReturnType>>;
		if (noop) {
			return getNoop<FReturnType, isStream>();
		}
		auto future = futureCallback();
		if (future.isReady()) {
			fun(future.get());
			return getNoop<FReturnType, isStream>();
		}
		std::function<void(FReturnType const&)> function = fun;
		return ChooseClause<Args..., ArgType>(std::tuple_cat(std::move(futures), std::make_tuple(future)),
		                                      std::tuple_cat(std::move(functions), std::make_tuple(function)));
	}

	template <class T>
	auto When(Future<T> const& future, std::invocable<T const&> auto fun) {
		static_assert(std::is_same_v<decltype(fun(std::declval<T const&>())), void>,
		              "When-handler must return void (and can't be awaitable)");
		if (noop) {
			return getNoop(future);
		}
		if (future.isReady()) {
			fun(future.get());
			return getNoop(future);
		}
		std::function<void(T const&)> function = fun;
		return ChooseClause<Args..., Future<T> const&>(
		    std::tuple_cat(std::move(futures), std::make_tuple(std::cref(future))),
		    std::tuple_cat(std::move(functions), std::make_tuple(function)));
	}

	template <class T>
	auto When(FutureStream<T> const& futureStream, std::invocable<T const&> auto fun) {
		static_assert(std::is_same_v<decltype(fun(std::declval<T const&>())), void>,
		              "When-handler must return void (and't can't be awaitable)");
		if (noop) {
			return getNoop(futureStream);
		}
		if (futureStream.isReady()) {
			auto fs = futureStream;
			fun(fs.pop());
			return getNoop(futureStream);
		}
		std::function<void(T const&)> function = fun;
		return ChooseClause<Args..., FutureStream<T> const&>(
		    std::tuple_cat(std::move(futures), std::make_tuple(std::cref(futureStream))),
		    std::tuple_cat(std::move(functions), std::make_tuple(function)));
	}

	[[nodiscard]] Future<Void> run() {
		if (noop) {
			return Void();
		}
		return Future<Void>(new ChooseImplActor<Args...>(std::move(futures), std::move(functions)));
	}
};

template <class... Futures>
using RaceResult = std::variant<FutureReturnTypeT<std::decay_t<Futures>>...>;

template <std::size_t Idx, class Result, class AwaitableType>
Future<Result> raceReadyResult(AwaitableType&& future) {
	if (future.isError()) {
		return future.getError();
	}
	if constexpr (GetFutureTypeV<std::remove_cvref_t<AwaitableType>> == FutureType::Future) {
		return Result(std::in_place_index<Idx>, future.get());
	} else if constexpr (GetFutureTypeV<std::remove_cvref_t<AwaitableType>> == FutureType::FutureStream) {
		auto fs = future;
		return Result(std::in_place_index<Idx>, fs.pop());
	} else {
		return Result(std::in_place_index<Idx>, std::forward<AwaitableType>(future).get());
	}
}

template <std::size_t Idx, class Result, class First, class... Rest>
Future<Result> raceReady(First&& first, Rest&&... rest) {
	if (first.isReady()) {
		return raceReadyResult<Idx, Result>(std::forward<First>(first));
	}
	if constexpr (sizeof...(Rest) > 0) {
		return raceReady<Idx + 1, Result>(std::forward<Rest>(rest)...);
	}
	return Future<Result>();
}

template <class Parent, int Idx, class... Futures>
struct RaceImplCallback;

template <class Parent, int Idx, class AwaitableType, class... Futures>
struct RaceImplCallback<Parent, Idx, AwaitableType, Futures...>
  : ConditionalActorCallback<RaceImplCallback<Parent, Idx, AwaitableType, Futures...>, Idx, AwaitableType>,
    RaceImplCallback<Parent, Idx + 1, Futures...> {
	using ThisCallback =
	    ConditionalActorCallback<RaceImplCallback<Parent, Idx, AwaitableType, Futures...>, Idx, AwaitableType>;
	using ValueType = FutureReturnTypeT<AwaitableType>;
	static constexpr FutureType futureType = GetFutureTypeV<AwaitableType>;

	[[nodiscard]] Parent* getParent() { return static_cast<Parent*>(this); }

	void registerCallbacks() {
		if constexpr (futureType == FutureType::Future) {
			StrictFuture<ValueType> sf = std::get<Idx>(getParent()->futures);
			sf.addCallbackAndClear(static_cast<ThisCallback*>(this));
		} else if constexpr (futureType == FutureType::FutureStream) {
			auto sf = std::get<Idx>(getParent()->futures);
			sf.addCallbackAndClear(static_cast<ThisCallback*>(this));
		} else {
			ThisCallback::bind(std::get<Idx>(getParent()->futures));
			std::move(std::get<Idx>(getParent()->futures)).addCallbackAndClear(static_cast<ThisCallback*>(this));
		}
		if constexpr (sizeof...(Futures) > 0) {
			RaceImplCallback<Parent, Idx + 1, Futures...>::registerCallbacks();
		}
	}

	void a_callback_fire(ThisCallback*, ValueType const& value) { getParent()->template finish<Idx>(value); }
	void a_callback_fire(ThisCallback*, ValueType&& value) { getParent()->template finish<Idx>(std::move(value)); }

	void a_callback_error(ThisCallback*, Error e) { getParent()->fail(e); }

	void removeCallbacks() {
		ThisCallback::remove();
		if constexpr (sizeof...(Futures) > 0) {
			RaceImplCallback<Parent, Idx + 1, Futures...>::removeCallbacks();
		}
	}
};

template <class Parent, int Idx>
struct RaceImplCallback<Parent, Idx> {
#ifdef ENABLE_SAMPLING
	LineageReference* lineageAddr() { return currentLineage; }
#endif
};

template <class Result, class... Futures>
struct RaceImplActor final : Actor<Result>,
                             RaceImplCallback<RaceImplActor<Result, Futures...>, 0, Futures...>,
                             FastAllocated<RaceImplActor<Result, Futures...>> {
	std::tuple<Futures...> futures;

	using FastAllocated<RaceImplActor<Result, Futures...>>::operator new;
	using FastAllocated<RaceImplActor<Result, Futures...>>::operator delete;

	explicit RaceImplActor(std::tuple<Futures...>&& futures) : Actor<Result>(), futures(std::move(futures)) {
		this->actor_wait_state = ACTOR_WAIT_STATE_WAITING;
		RaceImplCallback<RaceImplActor<Result, Futures...>, 0, Futures...>::registerCallbacks();
	}

	template <std::size_t Idx, class T>
	void finish(T&& value) {
		this->actor_wait_state = ACTOR_WAIT_STATE_NOT_WAITING;
		RaceImplCallback<RaceImplActor<Result, Futures...>, 0, Futures...>::removeCallbacks();
		this->SAV<Result>::sendAndDelPromiseRef(Result(std::in_place_index<Idx>, std::forward<T>(value)));
	}

	void fail(Error e) {
		this->actor_wait_state = ACTOR_WAIT_STATE_NOT_WAITING;
		RaceImplCallback<RaceImplActor<Result, Futures...>, 0, Futures...>::removeCallbacks();
		this->SAV<Result>::sendErrorAndDelPromiseRef(e);
	}

	void cancel() override {
		const auto waitState = this->actor_wait_state;
		this->actor_wait_state = ACTOR_WAIT_STATE_CANCELLED;
		if (actorWaitStateIsWaiting(waitState)) {
			RaceImplCallback<RaceImplActor<Result, Futures...>, 0, Futures...>::removeCallbacks();
			this->SAV<Result>::sendErrorAndDelPromiseRef(actor_cancelled());
		}
	}

	void destroy() override { delete this; }
};

} // namespace coro

using Choose = coro::ChooseClause<>;

// Waits for the first input Future/FutureStream to become ready and returns its value in a variant whose index
// matches the winning argument. If multiple inputs are already ready, the lowest-index argument wins; for streams,
// the next queued element is consumed. Errors and explicit cancellation propagate to the returned Future, while
// non-winning inputs are only detached from the race. They are not actively cancelled here, but may still cancel
// if dropping the race also drops their last reference.
template <class... Futures>
[[nodiscard]] auto race(Futures&&... futures) -> Future<coro::RaceResult<std::decay_t<Futures>...>> {
	static_assert(sizeof...(Futures) > 0, "race requires at least one Future argument");

	using Result = coro::RaceResult<std::decay_t<Futures>...>;
	Future<Result> ready = coro::raceReady<0, Result>(futures...);
	if (ready.isValid()) {
		return ready;
	}

	return Future<Result>(
	    new coro::RaceImplActor<Result, std::decay_t<Futures>...>(std::make_tuple(std::forward<Futures>(futures)...)));
}

template <class T, class F>
AsyncGenerator<T> map(AsyncGenerator<T> gen, F pred) {
	while (gen) {
		auto val = co_await gen();
		if (pred(val)) {
			co_yield val;
		}
	}
}

template <class T>
AsyncGenerator<T> toGenerator(FutureStream<T> stream) {
	while (true) {
		try {
			co_yield co_await stream;
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				co_return;
			}
			throw;
		}
	}
}

#endif // FLOW_COROUTILS_H
