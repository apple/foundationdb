/*
 * BenchCoroutineOverhead.cpp
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

namespace {

Future<int> returnReadyInt() {
	co_return 1;
}

Future<Void> returnReadyVoid() {
	co_return;
}

Future<Void> benchCreateReadyIntActor(benchmark::State* state) {
	int sink = 0;
	while (state->KeepRunning()) {
		Future<int> f = returnReadyInt();
		ASSERT(f.isReady());
		sink += f.get();
		benchmark::DoNotOptimize(f);
	}
	benchmark::DoNotOptimize(sink);
	co_return;
}

Future<Void> benchCreateReadyVoidActor(benchmark::State* state) {
	while (state->KeepRunning()) {
		Future<Void> f = returnReadyVoid();
		ASSERT(f.isReady());
		benchmark::DoNotOptimize(f);
	}
	co_return;
}

Future<Void> benchAwaitReadyIntActor(benchmark::State* state) {
	Future<int> ready = 1;
	int sink = 0;
	while (state->KeepRunning()) {
		sink += co_await ready;
	}
	benchmark::DoNotOptimize(sink);
	co_return;
}

Future<Void> benchAwaitReadyVoidActor(benchmark::State* state) {
	Future<Void> ready = Void();
	while (state->KeepRunning()) {
		co_await ready;
	}
	co_return;
}

void benchCreateReadyInt(benchmark::State& state) {
	onMainThread([&state] { return benchCreateReadyIntActor(&state); }).blockUntilReady();
}

void benchCreateReadyVoid(benchmark::State& state) {
	onMainThread([&state] { return benchCreateReadyVoidActor(&state); }).blockUntilReady();
}

void benchAwaitReadyInt(benchmark::State& state) {
	onMainThread([&state] { return benchAwaitReadyIntActor(&state); }).blockUntilReady();
}

void benchAwaitReadyVoid(benchmark::State& state) {
	onMainThread([&state] { return benchAwaitReadyVoidActor(&state); }).blockUntilReady();
}

BENCHMARK(benchCreateReadyInt)->Name("coroutine_overhead/create_ready_int")->ReportAggregatesOnly(true);
BENCHMARK(benchCreateReadyVoid)->Name("coroutine_overhead/create_ready_void")->ReportAggregatesOnly(true);
BENCHMARK(benchAwaitReadyInt)->Name("coroutine_overhead/await_ready_int")->ReportAggregatesOnly(true);
BENCHMARK(benchAwaitReadyVoid)->Name("coroutine_overhead/await_ready_void")->ReportAggregatesOnly(true);

} // namespace
