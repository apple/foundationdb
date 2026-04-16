/*
 * BenchAsyncResult.cpp
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

#include <cstdint>
#include <numeric>
#include <vector>

namespace {

struct ExpensivePayload {
	std::vector<uint8_t> bytes;

	explicit ExpensivePayload(size_t payloadBytes) : bytes(payloadBytes) {
		std::iota(bytes.begin(), bytes.end(), uint8_t{ 0 });
	}

	uint64_t touch() const {
		ASSERT(!bytes.empty());
		return bytes.front() + bytes[bytes.size() / 2] + bytes.back();
	}
};

Future<ExpensivePayload> returnFuturePayload(ExpensivePayload const& payload) {
	co_return payload;
}

AsyncResult<ExpensivePayload> returnAsyncResultPayload(ExpensivePayload const& payload) {
	co_return payload;
}

enum class AwaitImpl { Future, AsyncResult };

template <AwaitImpl Impl>
Future<Void> benchAwaitPayloadActor(benchmark::State* state) {
	const size_t payloadBytes = state->range(0);
	const ExpensivePayload source(payloadBytes);
	uint64_t sink = 0;

	while (state->KeepRunning()) {
		if constexpr (Impl == AwaitImpl::Future) {
			ExpensivePayload payload = co_await returnFuturePayload(source);
			sink += payload.touch();
			benchmark::DoNotOptimize(payload.bytes.data());
		} else {
			ExpensivePayload payload = co_await returnAsyncResultPayload(source);
			sink += payload.touch();
			benchmark::DoNotOptimize(payload.bytes.data());
		}
		benchmark::ClobberMemory();
	}

	state->SetBytesProcessed(static_cast<int64_t>(state->iterations()) * payloadBytes);
	benchmark::DoNotOptimize(sink);
	co_return;
}

template <AwaitImpl Impl>
void benchAwaitPayload(benchmark::State& state) {
	onMainThread([&state] { return benchAwaitPayloadActor<Impl>(&state); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(benchAwaitPayload, AwaitImpl::Future)
    ->Name("CoroutineAwaitPayload/Future")
    ->RangeMultiplier(4)
    ->Range(1 << 10, 1 << 20)
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchAwaitPayload, AwaitImpl::AsyncResult)
    ->Name("CoroutineAwaitPayload/AsyncResult")
    ->RangeMultiplier(4)
    ->Range(1 << 10, 1 << 20)
    ->ReportAggregatesOnly(true);

} // namespace
