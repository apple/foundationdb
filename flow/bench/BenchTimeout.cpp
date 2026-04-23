/*
 * BenchTimeout.cpp
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

#include "benchmark/benchmark.h"

#include "flow/ThreadHelper.actor.h"
#include "flow/genericactors.actor.h"
#include "genericcoros.h"

namespace {

enum class TimeoutImpl { Actor, Coroutine };
enum class TimeoutScenario { Ready, ConstructPending };

template <TimeoutImpl Impl, TimeoutScenario Scenario>
Future<Void> benchTimeoutActor(benchmark::State* state) {
	const int timedOutValue = -1;

	if constexpr (Scenario == TimeoutScenario::Ready) {
		const Future<int> readyFuture = Future<int>(7);
		int64_t sink = 0;
		while (state->KeepRunning()) {
			Future<int> done;
			if constexpr (Impl == TimeoutImpl::Actor) {
				done = ::timeout<int>(readyFuture, 0.0, timedOutValue);
			} else {
				done = generic_coro::timeout<int>(readyFuture, 0.0, timedOutValue);
			}
			sink += co_await done;
			benchmark::DoNotOptimize(done);
			benchmark::ClobberMemory();
		}
		benchmark::DoNotOptimize(sink);
	} else {
		const Future<int> neverFuture = Future<int>(Never());
		for (auto _ : *state) {
			benchmark::DoNotOptimize(_);
			state->ResumeTiming();
			// Measure the cost of constructing the timeout race itself. Using a
			// never-ready input keeps the returned future pending so timer delay
			// does not dominate the comparison.
			Future<int> done;
			if constexpr (Impl == TimeoutImpl::Actor) {
				done = ::timeout<int>(neverFuture, 1.0, timedOutValue);
			} else {
				done = generic_coro::timeout<int>(neverFuture, 1.0, timedOutValue);
			}
			ASSERT(!done.isReady());
			benchmark::DoNotOptimize(done);
			state->PauseTiming();
			done.cancel();
		}
	}
	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()));
}

template <TimeoutImpl Impl, TimeoutScenario Scenario>
void benchTimeout(benchmark::State& state) {
	onMainThread([&state] { return benchTimeoutActor<Impl, Scenario>(&state); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(benchTimeout, TimeoutImpl::Actor, TimeoutScenario::Ready)
    ->Name("Timeout/actor/ready")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchTimeout, TimeoutImpl::Coroutine, TimeoutScenario::Ready)
    ->Name("Timeout/coroutine/ready")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchTimeout, TimeoutImpl::Actor, TimeoutScenario::ConstructPending)
    ->Name("Timeout/actor/construct_pending")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchTimeout, TimeoutImpl::Coroutine, TimeoutScenario::ConstructPending)
    ->Name("Timeout/coroutine/construct_pending")
    ->ReportAggregatesOnly(true);

} // namespace
