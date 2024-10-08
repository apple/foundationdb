/*
 * BenchCallback.cpp
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

#include "benchmark/benchmark.h"

#include <vector>

#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"
#include "flow/Coroutines.h"
#include "flow/ThreadHelper.actor.h"

template <size_t Size>
static Future<Void> increment(Future<Void> f, uint32_t* sum) {
	// commented out to avoid counting the memory allocation in the benchmark
	// std::array<uint8_t, Size> arr;
	co_await f;
	// benchmark::DoNotOptimize(arr);
	++(*sum);
}

template <size_t Size>
static Future<Void> benchCallbackCoroutine(benchmark::State* benchState) {
	size_t actorCount = benchState->range(0);
	uint32_t sum;
	while (benchState->KeepRunning()) {
		sum = 0;
		Promise<Void> trigger;
		std::vector<Future<Void>> futures;
		futures.reserve(actorCount);
		for (int i = 0; i < actorCount; ++i) {
			futures.push_back(increment<Size>(trigger.getFuture(), &sum));
		}
		trigger.send(Void());
		co_await waitForAll(futures);
		benchmark::DoNotOptimize(sum);
	}
	benchState->SetItemsProcessed(actorCount * static_cast<long>(benchState->iterations()));
	benchState->SetBytesProcessed(actorCount * Size * static_cast<long>(benchState->iterations()));
}

template <size_t Size>
static void coroutine_callback(benchmark::State& benchState) {
	onMainThread([&benchState]() { return benchCallbackCoroutine<Size>(&benchState); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(coroutine_callback, 1)->Range(1, 1 << 8)->ReportAggregatesOnly(true);
// BENCHMARK_TEMPLATE(coroutine_callback, 32)->Range(1, 1 << 8)->ReportAggregatesOnly(true);
// BENCHMARK_TEMPLATE(coroutine_callback, 1024)->Range(1, 1 << 8)->ReportAggregatesOnly(true);
