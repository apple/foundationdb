#include "benchmark/benchmark.h"

#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/GrvProxyTagThrottler.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"

static void bench_grvProxyTagThrottler(benchmark::State& state) {
	GrvProxyTagThrottler throttler(5.0);

	std::vector<GetReadVersionRequest> reqs;
	Arena arena;

	for (int i = 0; i < state.range(0); ++i) {
		auto& req = reqs.emplace_back();

		TransactionTagMap<uint32_t> tags;
		tags[StringRef(arena, deterministicRandom()->randomAlphaNumeric(10))] = 1;

		req.priority = TransactionPriority::DEFAULT;
		req.tags = tags;
	}

	for (auto _ : state) {
		state.PauseTiming();
		for (const auto& req : reqs) {
			throttler.addRequest(req);
		}

		Deque<GetReadVersionRequest> outBatchPriority;
		Deque<GetReadVersionRequest> outDefaultPriority;

		state.ResumeTiming();

		throttler.releaseTransactions(/*elapsed=*/0.01, outBatchPriority, outDefaultPriority);
	}

	state.SetItemsProcessed(state.range(0) * static_cast<long>(state.iterations()));
}

BENCHMARK(bench_grvProxyTagThrottler)->RangeMultiplier(10)->Range(1, 100000);