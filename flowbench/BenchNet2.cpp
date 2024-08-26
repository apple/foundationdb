/*
 * BenchNet2.cpp
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

#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/DeterministicRandom.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

static Future<Void> increment(TaskPriority priority, uint32_t* sum) {
	co_await delay(0, priority);
	++(*sum);
}

static inline TaskPriority getRandomTaskPriority(DeterministicRandom& rand) {
	return static_cast<TaskPriority>(rand.randomInt(0, 100));
}

static Future<Void> benchNet2Actor(benchmark::State* benchState) {
	size_t actorCount = benchState->range(0);
	uint32_t sum;
	int seed = platform::getRandomSeed();
	while (benchState->KeepRunning()) {
		sum = 0;
		std::vector<Future<Void>> futures;
		futures.reserve(actorCount);
		DeterministicRandom rand(seed);
		for (int i = 0; i < actorCount; ++i) {
			futures.push_back(increment(getRandomTaskPriority(rand), &sum));
		}
		co_await waitForAll(futures);
		benchmark::DoNotOptimize(sum);
	}
	benchState->SetItemsProcessed(actorCount * static_cast<long>(benchState->iterations()));
}

static void coroutine_net2(benchmark::State& benchState) {
	onMainThread([&benchState] { return benchNet2Actor(&benchState); }).blockUntilReady();
}

BENCHMARK(coroutine_net2)->Range(1, 1 << 16)->ReportAggregatesOnly(true);

static constexpr bool DELAY = false;
static constexpr bool YIELD = true;

template <bool useYield>
static Future<Void> benchDelay(benchmark::State* benchState) {
	// Number of random delays to start to just to populate the run loop
	// priority queue
	int64_t timerCount = benchState->range(0);
	std::vector<Future<Void>> futures;
	DeterministicRandom rand(platform::getRandomSeed());
	while (--timerCount > 0) {
		futures.push_back(delay(1.0 + rand.random01(), getRandomTaskPriority(rand)));
	}

	while (benchState->KeepRunning()) {
		co_await (useYield ? yield() : delay(0));
	}
	benchState->SetItemsProcessed(static_cast<long>(benchState->iterations()));
}

template <bool useYield>
static void coroutine_delay(benchmark::State& benchState) {
	onMainThread([&benchState] { return benchDelay<useYield>(&benchState); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(coroutine_delay, DELAY)->Range(0, 1 << 16)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(coroutine_delay, YIELD)->Range(0, 1 << 16)->ReportAggregatesOnly(true);
