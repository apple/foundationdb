/*
 * CoroUtils.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

template <class Parent, int Idx, class F>
using ConditionalActorCallback = std::conditional_t<GetFutureTypeV<F> == FutureType::Future,
                                                    ActorCallback<Parent, Idx, FutureReturnTypeT<F>>,
                                                    ActorSingleCallback<Parent, Idx, FutureReturnTypeT<F>>>;

template <class Parent, int Idx, class... Args>
struct ChooseImplCallback;

template <class Parent, int Idx, class F, class... Args>
struct ChooseImplCallback<Parent, Idx, F, Args...>
  : ConditionalActorCallback<ChooseImplCallback<Parent, Idx, F, Args...>, Idx, F>,
    ChooseImplCallback<Parent, Idx + 1, Args...> {

	using ThisCallback = ConditionalActorCallback<ChooseImplCallback<Parent, Idx, F, Args...>, Idx, F>;
	using ValueType = FutureReturnTypeT<F>;
	static constexpr FutureType futureType = GetFutureTypeV<F>;

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
		getParent()->actor_wait_state = 0;
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
		getParent()->actor_wait_state = 0;
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
		actor_wait_state = 1;
	}

	void cancel() override {
		const auto waitState = actor_wait_state;
		actor_wait_state = -1;
		if (waitState > 0) {
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

} // namespace coro

using Choose = coro::ChooseClause<>;

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
