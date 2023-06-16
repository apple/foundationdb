#include "benchmark/benchmark.h"

#include "fdbserver/TransactionTagCounter.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"

static void bench_addRequest(benchmark::State& state) {
	TransactionTagCounter counter(UID(), /*maxTagsTracked=*/2, /*minRateTracked=*/0);

	VectorRef<StringRef> tenantGroups;
	Arena arena;
	for (int i = 0; i < state.range(0); ++i) {
		tenantGroups.push_back(arena, StringRef(arena, deterministicRandom()->randomAlphaNumeric(10)));
	}

	for (auto _ : state) {
		counter.addRequest(
		    {}, deterministicRandom()->randomChoice(tenantGroups), 10 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	}

	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK(bench_addRequest)->RangeMultiplier(2)->Range(8 << 4, 8 << 14);

static void bench_startNewInterval(benchmark::State& state) {
	TransactionTagCounter counter(UID(), /*maxTagsTracked=*/state.range(0), /*minRateTracked=*/0);

	VectorRef<StringRef> tenantGroups;
	Arena arena;
	for (int i = 0; i < state.range(1); ++i)
		tenantGroups.push_back(arena, StringRef(arena, deterministicRandom()->randomAlphaNumeric(10)));

	for (auto _ : state) {
		state.PauseTiming();
		for (auto i : tenantGroups) {
			counter.addRequest({}, i, deterministicRandom()->randomInt(1, 10) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		}
		state.ResumeTiming();

		counter.startNewInterval();
	}

	state.SetItemsProcessed(static_cast<long>(state.range(1) * state.iterations()));
}

BENCHMARK(bench_startNewInterval)
    ->ArgsProduct({ benchmark::CreateRange(1, 1000, 10), benchmark::CreateRange(1, 100000, 10) });
