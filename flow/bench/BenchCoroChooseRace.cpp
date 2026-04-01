/*
 * BenchCoroChooseRace.cpp
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

#include "flow/genericactors.actor.h"
#include "flow/ThreadHelper.actor.h"

namespace {

enum class Impl { Choose, Race };
enum class Scenario { ReadyFirst, ReadySecond, AfterFirst };

// Measure the control-flow case where callers only need the winning branch to
// perform side effects. This is the path Choose is designed for.
template <Impl impl>
void consumeReady(Future<int> const& first, Future<double> const& second, double* sink) {
	if constexpr (impl == Impl::Choose) {
		Future<Void> f = Choose()
		                     .When(first, [sink](int const& value) { *sink += value; })
		                     .When(second, [sink](double const& value) { *sink += value; })
		                     .run();
		ASSERT(f.isReady());
		benchmark::DoNotOptimize(f);
	} else {
		Future<std::variant<int, double>> f = race(first, second);
		ASSERT(f.isReady());
		std::visit([sink](auto const& value) { *sink += value; }, f.get());
		benchmark::DoNotOptimize(f);
	}
}

template <Impl impl>
void consumeAfter(Promise<int>& promise, Future<double> const& second, double* sink) {
	if constexpr (impl == Impl::Choose) {
		Future<Void> f = Choose()
		                     .When(promise.getFuture(), [sink](int const& value) { *sink += value; })
		                     .When(second, [sink](double const& value) { *sink += value; })
		                     .run();
		promise.send(1);
		ASSERT(f.isReady());
		benchmark::DoNotOptimize(f);
	} else {
		Future<std::variant<int, double>> f = race(promise.getFuture(), second);
		promise.send(1);
		ASSERT(f.isReady());
		std::visit([sink](auto const& value) { *sink += value; }, f.get());
		benchmark::DoNotOptimize(f);
	}
}

// Measure the value-selection case where callers need "first result wins" as a
// std::variant value. Choose has to emulate this with an extra Promise<Result>.
template <Impl impl>
void selectReadyAsValue(Future<int> const& first, Future<double> const& second, double* sink) {
	using Result = std::variant<int, double>;
	if constexpr (impl == Impl::Choose) {
		Promise<Result> resultPromise;
		Future<Result> resultFuture = resultPromise.getFuture();
		Future<Void> f =
		    Choose()
		        .When(first,
		              [&resultPromise](int const& value) { resultPromise.send(Result(std::in_place_index<0>, value)); })
		        .When(second,
		              [&resultPromise](double const& value) {
			              resultPromise.send(Result(std::in_place_index<1>, value));
		              })
		        .run();
		ASSERT(f.isReady());
		ASSERT(resultFuture.isReady());
		std::visit([sink](auto const& value) { *sink += value; }, resultFuture.get());
		benchmark::DoNotOptimize(f);
		benchmark::DoNotOptimize(resultFuture);
	} else {
		Future<Result> f = race(first, second);
		ASSERT(f.isReady());
		std::visit([sink](auto const& value) { *sink += value; }, f.get());
		benchmark::DoNotOptimize(f);
	}
}

template <Impl impl>
void selectAfterAsValue(Promise<int>& promise, Future<double> const& second, double* sink) {
	using Result = std::variant<int, double>;
	if constexpr (impl == Impl::Choose) {
		Promise<Result> resultPromise;
		Future<Result> resultFuture = resultPromise.getFuture();
		Future<Void> f =
		    Choose()
		        .When(promise.getFuture(),
		              [&resultPromise](int const& value) { resultPromise.send(Result(std::in_place_index<0>, value)); })
		        .When(second,
		              [&resultPromise](double const& value) {
			              resultPromise.send(Result(std::in_place_index<1>, value));
		              })
		        .run();
		promise.send(1);
		ASSERT(f.isReady());
		ASSERT(resultFuture.isReady());
		std::visit([sink](auto const& value) { *sink += value; }, resultFuture.get());
		benchmark::DoNotOptimize(f);
		benchmark::DoNotOptimize(resultFuture);
	} else {
		Future<Result> f = race(promise.getFuture(), second);
		promise.send(1);
		ASSERT(f.isReady());
		std::visit([sink](auto const& value) { *sink += value; }, f.get());
		benchmark::DoNotOptimize(f);
	}
}

// Measure only the cost to construct a pending selector over unresolved
// Futures. Cleanup is timed out so this isolates setup/registration overhead.
template <Impl impl>
static Future<Void> benchChooseRaceConstructPendingActor(benchmark::State* state) {
	double sink = 0;
	Promise<int> neverIntPromise;
	Promise<double> neverDoublePromise;
	Future<int> neverInt = neverIntPromise.getFuture();
	Future<double> neverDouble = neverDoublePromise.getFuture();

	for (auto _ : *state) {
		benchmark::DoNotOptimize(_);
		state->ResumeTiming();
		if constexpr (impl == Impl::Choose) {
			Future<Void> f = Choose()
			                     .When(neverInt, [&sink](int const&) { benchmark::DoNotOptimize(sink); })
			                     .When(neverDouble, [&sink](double const&) { benchmark::DoNotOptimize(sink); })
			                     .run();
			ASSERT(!f.isReady());
			benchmark::DoNotOptimize(f);
			state->PauseTiming();
			f.cancel();
		} else {
			Future<std::variant<int, double>> f = race(neverInt, neverDouble);
			ASSERT(!f.isReady());
			benchmark::DoNotOptimize(f);
			state->PauseTiming();
			f.cancel();
		}
	}

	benchmark::DoNotOptimize(sink);
	co_return;
}

template <Impl impl, Scenario scenario>
static Future<Void> benchChooseRaceActor(benchmark::State* state) {
	double sink = 0;
	Future<int> readyInt = 7;
	Future<double> readyDouble = 8.0;
	Promise<int> neverIntPromise;
	Promise<double> neverDoublePromise;
	Future<int> neverInt = neverIntPromise.getFuture();
	Future<double> neverDouble = neverDoublePromise.getFuture();

	while (state->KeepRunning()) {
		if constexpr (scenario == Scenario::ReadyFirst) {
			consumeReady<impl>(readyInt, readyDouble, &sink);
		} else if constexpr (scenario == Scenario::ReadySecond) {
			consumeReady<impl>(neverInt, readyDouble, &sink);
		} else {
			Promise<int> promise;
			consumeAfter<impl>(promise, neverDouble, &sink);
		}
	}

	benchmark::DoNotOptimize(sink);
	co_return;
}

template <Impl impl, Scenario scenario>
static Future<Void> benchChooseRaceValueActor(benchmark::State* state) {
	double sink = 0;
	Future<int> readyInt = 7;
	Future<double> readyDouble = 8.0;
	Promise<int> neverIntPromise;
	Promise<double> neverDoublePromise;
	Future<int> neverInt = neverIntPromise.getFuture();
	Future<double> neverDouble = neverDoublePromise.getFuture();

	while (state->KeepRunning()) {
		if constexpr (scenario == Scenario::ReadyFirst) {
			selectReadyAsValue<impl>(readyInt, readyDouble, &sink);
		} else if constexpr (scenario == Scenario::ReadySecond) {
			selectReadyAsValue<impl>(neverInt, readyDouble, &sink);
		} else {
			Promise<int> promise;
			selectAfterAsValue<impl>(promise, neverDouble, &sink);
		}
	}

	benchmark::DoNotOptimize(sink);
	co_return;
}

template <Impl impl, Scenario scenario>
static void benchChooseRace(benchmark::State& state) {
	onMainThread([&state] { return benchChooseRaceActor<impl, scenario>(&state); }).blockUntilReady();
}

template <Impl impl, Scenario scenario>
static void benchChooseRaceValue(benchmark::State& state) {
	onMainThread([&state] { return benchChooseRaceValueActor<impl, scenario>(&state); }).blockUntilReady();
}

template <Impl impl>
static void benchChooseRaceConstructPending(benchmark::State& state) {
	onMainThread([&state] { return benchChooseRaceConstructPendingActor<impl>(&state); }).blockUntilReady();
}

} // namespace

BENCHMARK_TEMPLATE(benchChooseRace, Impl::Choose, Scenario::ReadyFirst)
    ->Name("coro_choose/ready_first")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRace, Impl::Race, Scenario::ReadyFirst)
    ->Name("coro_race/ready_first")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchChooseRace, Impl::Choose, Scenario::ReadySecond)
    ->Name("coro_choose/ready_second")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRace, Impl::Race, Scenario::ReadySecond)
    ->Name("coro_race/ready_second")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchChooseRace, Impl::Choose, Scenario::AfterFirst)
    ->Name("coro_choose/after_first")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRace, Impl::Race, Scenario::AfterFirst)
    ->Name("coro_race/after_first")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchChooseRaceValue, Impl::Choose, Scenario::ReadyFirst)
    ->Name("coro_choose_as_race/ready_first")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRaceValue, Impl::Race, Scenario::ReadyFirst)
    ->Name("coro_race_as_value/ready_first")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchChooseRaceValue, Impl::Choose, Scenario::ReadySecond)
    ->Name("coro_choose_as_race/ready_second")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRaceValue, Impl::Race, Scenario::ReadySecond)
    ->Name("coro_race_as_value/ready_second")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchChooseRaceValue, Impl::Choose, Scenario::AfterFirst)
    ->Name("coro_choose_as_race/after_first")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRaceValue, Impl::Race, Scenario::AfterFirst)
    ->Name("coro_race_as_value/after_first")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchChooseRaceConstructPending, Impl::Choose)
    ->Name("coro_choose_construct/pending")
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(benchChooseRaceConstructPending, Impl::Race)
    ->Name("coro_race_construct/pending")
    ->ReportAggregatesOnly(true);
