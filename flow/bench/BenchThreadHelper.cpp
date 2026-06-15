/*
 * BenchThreadHelper.cpp
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
#include "flow/flow.h"

#include <atomic>
#include <cstdint>
#include <thread>

namespace {

struct ErrorSlot {
	Error error;
};

Future<int> readyIntFuture(int value) {
	co_return value;
}

Future<Void> readyVoidFuture() {
	co_return;
}

Future<Void> benchSafeThreadFutureToFutureReadyIntActor(benchmark::State* state) {
	ThreadFuture<int> threadFuture(7);
	int64_t sink = 0;

	while (state->KeepRunning()) {
		Future<int> f = safeThreadFutureToFuture(threadFuture);
		sink += co_await f;
		benchmark::DoNotOptimize(f);
		benchmark::ClobberMemory();
	}

	benchmark::DoNotOptimize(sink);
	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()));
}

Future<Void> benchUnsafeThreadFutureToFutureReadyIntActor(benchmark::State* state) {
	ThreadFuture<int> threadFuture(7);
	int64_t sink = 0;

	while (state->KeepRunning()) {
		Future<int> f = unsafeThreadFutureToFuture(threadFuture);
		sink += co_await f;
		benchmark::DoNotOptimize(f);
		benchmark::ClobberMemory();
	}

	benchmark::DoNotOptimize(sink);
	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()));
}

Future<Void> benchSafeThreadFutureToFuturePendingIntActor(benchmark::State* state) {
	int64_t sink = 0;

	while (state->KeepRunning()) {
		auto* sav = new ThreadSingleAssignmentVar<int>;
		ThreadFuture<int> threadFuture(sav);
		Future<int> f = safeThreadFutureToFuture(threadFuture);
		sav->send(7);
		sink += co_await f;
		benchmark::DoNotOptimize(f);
		benchmark::ClobberMemory();
	}

	benchmark::DoNotOptimize(sink);
	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()));
}

Future<Void> benchUnsafeThreadFutureToFuturePendingIntActor(benchmark::State* state) {
	int64_t sink = 0;

	while (state->KeepRunning()) {
		auto* sav = new ThreadSingleAssignmentVar<int>;
		ThreadFuture<int> threadFuture(sav);
		Future<int> f = unsafeThreadFutureToFuture(threadFuture);
		sav->send(7);
		sink += co_await f;
		benchmark::DoNotOptimize(f);
		benchmark::ClobberMemory();
	}

	benchmark::DoNotOptimize(sink);
	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()));
}

void benchOnMainThreadReadyInt(benchmark::State& state) {
	int64_t sink = 0;

	for (auto _ : state) {
		benchmark::DoNotOptimize(_);
		ThreadFuture<int> result = onMainThread([] { return readyIntFuture(7); });
		result.blockUntilReady();
		sink += result.get();
		benchmark::ClobberMemory();
	}

	benchmark::DoNotOptimize(sink);
	state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void benchOnMainThreadReadyVoid(benchmark::State& state) {
	for (auto _ : state) {
		benchmark::DoNotOptimize(_);
		ThreadFuture<Void> result = onMainThread([] { return readyVoidFuture(); });
		result.getBlocking();
		benchmark::ClobberMemory();
	}

	state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void benchOnMainThreadVoidEnqueue(benchmark::State& state) {
	const int batchSize = state.range(0);

	for (auto _ : state) {
		benchmark::DoNotOptimize(_);
		std::atomic<int> completed{ 0 };

		for (int i = 0; i < batchSize; ++i) {
			onMainThreadVoid([&completed] { completed.fetch_add(1, std::memory_order_release); });
		}

		state.PauseTiming();
		while (completed.load(std::memory_order_acquire) != batchSize) {
			std::this_thread::yield();
		}
		state.ResumeTiming();
	}

	state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * batchSize);
}

void benchOnMainThreadVoidEnqueueWithErrorMember(benchmark::State& state) {
	const int batchSize = state.range(0);
	ErrorSlot slot;

	for (auto _ : state) {
		benchmark::DoNotOptimize(_);
		std::atomic<int> completed{ 0 };

		for (int i = 0; i < batchSize; ++i) {
			onMainThreadVoid(
			    [&completed] { completed.fetch_add(1, std::memory_order_release); }, &slot, &ErrorSlot::error);
		}

		state.PauseTiming();
		while (completed.load(std::memory_order_acquire) != batchSize) {
			std::this_thread::yield();
		}
		ASSERT(slot.error.code() == invalid_error_code);
		state.ResumeTiming();
	}

	state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * batchSize);
}

void benchSafeThreadFutureToFutureReadyInt(benchmark::State& state) {
	onMainThread([&state] { return benchSafeThreadFutureToFutureReadyIntActor(&state); }).getBlocking();
}

void benchUnsafeThreadFutureToFutureReadyInt(benchmark::State& state) {
	onMainThread([&state] { return benchUnsafeThreadFutureToFutureReadyIntActor(&state); }).getBlocking();
}

void benchSafeThreadFutureToFuturePendingInt(benchmark::State& state) {
	onMainThread([&state] { return benchSafeThreadFutureToFuturePendingIntActor(&state); }).getBlocking();
}

void benchUnsafeThreadFutureToFuturePendingInt(benchmark::State& state) {
	onMainThread([&state] { return benchUnsafeThreadFutureToFuturePendingIntActor(&state); }).getBlocking();
}

BENCHMARK(benchOnMainThreadReadyInt)
    ->Name("ThreadHelper/onMainThread/ready_int")
    ->UseRealTime()
    ->ReportAggregatesOnly(true);
BENCHMARK(benchOnMainThreadReadyVoid)
    ->Name("ThreadHelper/onMainThread/ready_void")
    ->UseRealTime()
    ->ReportAggregatesOnly(true);

BENCHMARK(benchOnMainThreadVoidEnqueue)
    ->Name("ThreadHelper/onMainThreadVoid/enqueue")
    ->RangeMultiplier(4)
    ->Range(1, 1 << 10)
    ->ReportAggregatesOnly(true);

BENCHMARK(benchOnMainThreadVoidEnqueueWithErrorMember)
    ->Name("ThreadHelper/onMainThreadVoid/enqueue_error_member")
    ->RangeMultiplier(4)
    ->Range(1, 1 << 10)
    ->ReportAggregatesOnly(true);

BENCHMARK(benchSafeThreadFutureToFutureReadyInt)
    ->Name("ThreadHelper/safeThreadFutureToFuture/ready_int")
    ->ReportAggregatesOnly(true);

BENCHMARK(benchUnsafeThreadFutureToFutureReadyInt)
    ->Name("ThreadHelper/unsafeThreadFutureToFuture/ready_int")
    ->ReportAggregatesOnly(true);

BENCHMARK(benchSafeThreadFutureToFuturePendingInt)
    ->Name("ThreadHelper/safeThreadFutureToFuture/pending_int")
    ->ReportAggregatesOnly(true);

BENCHMARK(benchUnsafeThreadFutureToFuturePendingInt)
    ->Name("ThreadHelper/unsafeThreadFutureToFuture/pending_int")
    ->ReportAggregatesOnly(true);

} // namespace
