/*
 * Coroutines.h
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
#include "flow/CoroutinesImpl.h"

struct Uncancellable {};

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
	Generator(Generator const& other) : handle(other.handle) {
		if (handle) {
			handle.promise().addRef();
		}
	}
	Generator(Generator&& other) : handle(std::move(other.handle)) { other.handle = handle_type{}; }
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
	using promise_type = coro::CoroPromise<ReturnValue, !coro::hasUncancellable<Args...>>;
};

template <typename ReturnValue, typename... Args>
struct [[maybe_unused]] n_coroutine::coroutine_traits<AsyncGenerator<ReturnValue>, Args...> {
	static_assert(!coro::hasUncancellable<Args...>, "AsyncGenerator can't be uncancellable");
	using promise_type = coro::AsyncGeneratorPromise<ReturnValue>;
};

#endif // FLOW_COROUTINES_H
