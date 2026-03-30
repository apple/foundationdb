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
Future<Void> waitForAllReady(std::vector<Future<T>> results) {
	for (auto const& result : results) {
		if (result.isReady()) {
			continue;
		}
		// waitForAllReady only cares that each future completes; composing
		// ignore() with errorOr() avoids both throwing on error and copying T.
		co_await coro::errorOr(coro::ignore(result));
	}
	co_return;
}

template <class T>
Future<T> timeout(Future<T> what, double time, T timedoutValue, TaskPriority taskID = TaskPriority::DefaultDelay) {
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

} // namespace generic_coro
