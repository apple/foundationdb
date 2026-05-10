/*
 * BenchWaitForAllReady.cpp
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

#include <vector>

namespace {

enum class WaitForAllReadyScenario { Ready, Error };

template <WaitForAllReadyScenario Scenario>
std::vector<Future<int>> makeResults(int futureCount) {
	std::vector<Future<int>> results;
	results.reserve(futureCount);
	for (int i = 0; i < futureCount; ++i) {
		if constexpr (Scenario == WaitForAllReadyScenario::Ready) {
			results.emplace_back(i);
		} else {
			results.emplace_back(operation_failed());
		}
	}
	return results;
}

template <WaitForAllReadyScenario Scenario>
Future<Void> benchWaitForAllReadyActor(benchmark::State* state) {
	const int futureCount = state->range(0);
	// Prebuild the futures so the benchmark isolates waitForAllReady itself
	// instead of vector growth or Promise/Future setup cost.
	const std::vector<Future<int>> results = makeResults<Scenario>(futureCount);

	while (state->KeepRunning()) {
		Future<Void> done = waitForAllReady<int>(results);
		co_await done;
		benchmark::DoNotOptimize(done);
		benchmark::ClobberMemory();
	}

	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()) * futureCount);
}

template <WaitForAllReadyScenario Scenario>
void benchWaitForAllReady(benchmark::State& state) {
	onMainThread([&state] { return benchWaitForAllReadyActor<Scenario>(&state); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(benchWaitForAllReady, WaitForAllReadyScenario::Ready)
    ->Name("WaitForAllReady/coroutine/ready")
    ->RangeMultiplier(4)
    ->Range(1, 1 << 12)
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchWaitForAllReady, WaitForAllReadyScenario::Error)
    ->Name("WaitForAllReady/coroutine/error")
    ->RangeMultiplier(4)
    ->Range(1, 1 << 12)
    ->ReportAggregatesOnly(true);

} // namespace
