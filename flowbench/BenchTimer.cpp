/*
 * BenchTimer.cpp
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

#include "flow/Platform.h"

static void bench_timer(benchmark::State& state) {
	while (state.KeepRunning()) {
		double time = timer();
		benchmark::DoNotOptimize(time);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

static void bench_timer_monotonic(benchmark::State& state) {
	while (state.KeepRunning()) {
		double time = timer_monotonic();
		benchmark::DoNotOptimize(time);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK(bench_timer)->ReportAggregatesOnly(true);
BENCHMARK(bench_timer_monotonic)->ReportAggregatesOnly(true);
