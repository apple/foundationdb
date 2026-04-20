/*
 * genericcoros.h
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

#include "flow/Coroutines.h"
#include "flow/flow.h"

#include <vector>

namespace generic_coro {

template <class T>
Future<T> traceAfter(Future<T> what, std::string eventType, bool traceErrors = true, ExplicitVoid = {}) {
	try {
		T val = co_await what;
		TraceEvent(eventType.c_str());
		co_return val;
	} catch (Error& e) {
		// Don't trace operation_cancelled as it's a normal control flow mechanism, not an error
		if (traceErrors && e.code() != error_code_operation_cancelled) {
			TraceEvent(eventType.c_str()).errorUnsuppressed(e);
		}
		throw;
	}
}

template <class T>
Future<Optional<T>> stopAfter(Future<T> what, ExplicitVoid = {}) {
	Optional<T> ret = T();
	try {
		T res = co_await what;
		ret = Optional<T>(res);
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
Future<T> throwErrorOr(Future<ErrorOr<T>> f, ExplicitVoid = {}) {
	ErrorOr<T> t = co_await f;
	if (t.isError()) {
		throw t.getError();
	}
	co_return std::move(t).get();
}

template <class T>
Future<T> transformErrors(Future<T> f, Error err, ExplicitVoid = {}) {
	ErrorOr<T> t = co_await coro::errorOr(f);
	if (t.present()) {
		co_return std::move(t).get();
	}
	Error e = t.getError();
	if (e.code() == error_code_actor_cancelled) {
		throw e;
	}
	throw err;
}

template <class T>
Future<T> transformError(Future<T> f, Error inErr, Error outErr, ExplicitVoid = {}) {
	ErrorOr<T> t = co_await coro::errorOr(f);
	if (t.present()) {
		co_return std::move(t).get();
	}
	Error e = t.getError();
	if (e.code() == inErr.code()) {
		throw outErr;
	}
	throw e;
}

template <class T>
Future<Void> waitForAllReady(std::vector<Future<T>> results) {
	for (auto const& result : results) {
		if (result.isReady()) {
			continue;
		}
		// waitForAllReady only cares that each future completes; composing
		// ignore() with errorOr() avoids both throwing on error and copying T.
		co_await coro::errorOr(coro::ignore(result));
	}
}

template <class T>
Future<T> timeout(Future<T> what,
                  double time,
                  T timedoutValue,
                  TaskPriority taskID = TaskPriority::DefaultDelay,
                  ExplicitVoid = {}) {
	if (what.canGet()) {
		co_return what.get();
	} else if (what.isError()) {
		throw what.getError();
	}
	auto res = co_await race(what, delay(time, taskID));
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	} else {
		co_return timedoutValue;
	}
}

template <class T>
Future<Optional<T>> timeout(Future<T> what,
                            double time,
                            TaskPriority taskID = TaskPriority::DefaultDelay,
                            ExplicitVoid = {}) {
	if (what.canGet()) {
		co_return what.get();
	} else if (what.isError()) {
		throw what.getError();
	}
	auto res = co_await race(what, delay(time, taskID));
	if (res.index() == 0) {
		co_return std::get<0>(std::move(res));
	} else {
		co_return Optional<T>();
	}
}

template <class T>
Future<T> timeoutError(Future<T> what,
                       double time,
                       TaskPriority taskID = TaskPriority::DefaultDelay,
                       ExplicitVoid = {}) {
	if (what.canGet()) {
		co_return what.get();
	} else if (what.isError()) {
		throw what.getError();
	}
	auto res = co_await race(what, delay(time, taskID));
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
	ErrorOr<T> t = co_await coro::errorOr(what);
	co_await delay(time, taskID);
	if (t.present()) {
		co_return std::move(t).get();
	} else {
		throw t.getError();
	}
}

template <class Func>
Future<Void> trigger(Func what, Future<Void> signal) {
	co_await signal;
	what();
}

template <class T>
Future<Void> uncancellable(Uncancellable, Future<T> what, Promise<T> result) {
	ErrorOr<T> res = co_await coro::errorOr(what);
	if (res.present()) {
		result.send(std::move(res).get());
	} else {
		result.sendError(res.getError());
	}
}

template <class T>
Future<T> uncancellable(Future<T> what, ExplicitVoid = {}) {
	Promise<T> resultPromise;
	Future<T> result = resultPromise.getFuture();

	uncancellable(Uncancellable(), what, resultPromise);
	co_return co_await result;
}

template <class T, class X>
Future<T> holdWhile(X object, Future<T> what) {
	co_return co_await what;
}

template <class T, class X>
Future<Void> store(X& out, Future<T> what) {
	out = co_await what;
}

} // namespace generic_coro
