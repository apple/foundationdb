/*
 * BenchCallback.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR template <size_t Size>
static Future<Void> increment(Future<Void> f, uint32_t* sum) {
	state std::array<uint8_t, Size> arr;
	wait(f);
	benchmark::DoNotOptimize(arr);
	++(*sum);
	return Void();
}

ACTOR template <size_t Size>
static Future<Void> benchCallbackActor(benchmark::State* benchState) {
	state size_t actorCount = benchState->range(0);
	state uint32_t sum;
	while (benchState->KeepRunning()) {
		sum = 0;
		Promise<Void> trigger;
		std::vector<Future<Void>> futures;
		futures.reserve(actorCount);
		for (int i = 0; i < actorCount; ++i) {
			futures.push_back(increment<Size>(trigger.getFuture(), &sum));
		}
		trigger.send(Void());
		wait(waitForAll(futures));
		benchmark::DoNotOptimize(sum);
	}
	benchState->SetItemsProcessed(actorCount * static_cast<long>(benchState->iterations()));
	benchState->SetBytesProcessed(actorCount * Size * static_cast<long>(benchState->iterations()));
	return Void();
}

template <size_t Size>
static void bench_callback(benchmark::State& benchState) {
	onMainThread([&benchState]() { return benchCallbackActor<Size>(&benchState); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(bench_callback, 1)->Range(1, 1 << 8)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_callback, 32)->Range(1, 1 << 8)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_callback, 1024)->Range(1, 1 << 8)->ReportAggregatesOnly(true);
