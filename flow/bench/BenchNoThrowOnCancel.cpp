/*
 * BenchNoThrowOnCancel.cpp
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
#include <vector>

namespace {

struct CleanupCounter {
	uint64_t* cleanupCount;
	~CleanupCounter() { ++*cleanupCount; }
};

// Baseline: cancellation resumes the coroutine through actor_cancelled(), so the
// catch count should match the number of cancelled futures.
Future<Void> cancelWithThrow(Future<Void> signal, uint64_t* cleanupCount, uint64_t* caughtCount) {
	CleanupCounter cleanup{ cleanupCount };
	try {
		co_await signal;
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_actor_cancelled);
		++*caughtCount;
		throw;
	}
}

// Treatment: cancellation destroys the frame directly. The catch block is kept
// as a sentinel to prove that RAII cleanup ran without exception unwinding.
Future<Void> cancelWithoutThrow(Future<Void> signal,
                                uint64_t* cleanupCount,
                                uint64_t* caughtCount,
                                NoThrowOnCancel = {}) {
	CleanupCounter cleanup{ cleanupCount };
	try {
		co_await signal;
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_actor_cancelled);
		++*caughtCount;
		throw;
	}
}

enum class CancelImpl { Throwing, NoThrow };
enum class CancelScenario { ConstructAndCancel, BatchCancel };

template <CancelImpl Impl>
Future<Void> makeCancelFuture(Future<Void> signal, uint64_t* cleanupCount, uint64_t* caughtCount) {
	if constexpr (Impl == CancelImpl::Throwing) {
		return cancelWithThrow(std::move(signal), cleanupCount, caughtCount);
	} else {
		return cancelWithoutThrow(std::move(signal), cleanupCount, caughtCount);
	}
}

template <CancelImpl Impl, CancelScenario Scenario>
Future<Void> benchNoThrowOnCancelActor(benchmark::State* state) {
	Promise<Void> signal;
	uint64_t cleanupCount = 0;
	uint64_t caughtCount = 0;

	if constexpr (Scenario == CancelScenario::ConstructAndCancel) {
		while (state->KeepRunning()) {
			Future<Void> f = makeCancelFuture<Impl>(signal.getFuture(), &cleanupCount, &caughtCount);
			ASSERT(!f.isReady());
			benchmark::DoNotOptimize(f);
			f.cancel();
			benchmark::DoNotOptimize(f);
			benchmark::ClobberMemory();
		}
	} else {
		const int batchSize = state->range(0);
		std::vector<Future<Void>> futures;
		futures.reserve(batchSize);
		for (auto _ : *state) {
			benchmark::DoNotOptimize(_);
			// Exclude construction from this scenario so the timed region focuses
			// on the cancellation path itself.
			state->PauseTiming();
			futures.clear();
			for (int i = 0; i < batchSize; ++i) {
				futures.push_back(makeCancelFuture<Impl>(signal.getFuture(), &cleanupCount, &caughtCount));
				ASSERT(!futures.back().isReady());
			}
			benchmark::DoNotOptimize(futures.data());
			state->ResumeTiming();
			for (auto& f : futures) {
				f.cancel();
				benchmark::DoNotOptimize(f);
			}
			benchmark::ClobberMemory();
			state->PauseTiming();
			for (auto const& f : futures) {
				ASSERT(f.isReady() && f.isError() && f.getError().code() == error_code_actor_cancelled);
			}
		}
		state->ResumeTiming();
	}

	uint64_t cancels = state->iterations();
	if constexpr (Scenario == CancelScenario::BatchCancel) {
		cancels *= static_cast<uint64_t>(state->range(0));
	}
	ASSERT_EQ(cleanupCount, cancels);
	if constexpr (Impl == CancelImpl::Throwing) {
		ASSERT_EQ(caughtCount, cancels);
	} else {
		ASSERT_EQ(caughtCount, 0);
	}
	state->SetItemsProcessed(static_cast<int64_t>(cancels));
	co_return;
}

template <CancelImpl Impl, CancelScenario Scenario>
void benchNoThrowOnCancel(benchmark::State& state) {
	onMainThread([&state] { return benchNoThrowOnCancelActor<Impl, Scenario>(&state); }).blockUntilReady();
}

BENCHMARK_TEMPLATE(benchNoThrowOnCancel, CancelImpl::Throwing, CancelScenario::ConstructAndCancel)
    ->Name("NoThrowOnCancel/throwing/construct_and_cancel")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchNoThrowOnCancel, CancelImpl::NoThrow, CancelScenario::ConstructAndCancel)
    ->Name("NoThrowOnCancel/no_throw/construct_and_cancel")
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchNoThrowOnCancel, CancelImpl::Throwing, CancelScenario::BatchCancel)
    ->Name("NoThrowOnCancel/throwing/batch_cancel")
    ->Arg(1024)
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchNoThrowOnCancel, CancelImpl::NoThrow, CancelScenario::BatchCancel)
    ->Name("NoThrowOnCancel/no_throw/batch_cancel")
    ->Arg(1024)
    ->ReportAggregatesOnly(true);

} // namespace
