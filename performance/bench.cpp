#include <stdio.h>
#include "benchmark/benchmark.h"

template <bool reserve>
static void bench_populate(benchmark::State& state) {
	int items = 1 << state.range(0);
	while (state.KeepRunning()) {
		std::vector<int> vec;
		if constexpr (reserve) {
			vec.reserve(items);
		}
		for (int i = 0; i < items; ++i) {
			vec.push_back(i);
		}
		benchmark::DoNotOptimize(vec);
	}
	state.SetItemsProcessed(items * static_cast<long>(state.iterations()));
}

static void with_reserve(benchmark::State& state) {
	bench_populate<true>(state);
}

static void without_reserve(benchmark::State& state) {
	bench_populate<false>(state);
}

BENCHMARK(with_reserve)->DenseRange(0, 8)->ReportAggregatesOnly(true);
BENCHMARK(without_reserve)->DenseRange(0, 8)->ReportAggregatesOnly(true);

BENCHMARK_MAIN();
