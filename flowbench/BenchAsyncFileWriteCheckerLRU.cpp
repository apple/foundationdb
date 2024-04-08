#include "benchmark/benchmark.h"
#include "crc32/crc32c.h"
#include "flow/Hash3.h"
#include "flow/xxhash.h"
#include "flowbench/GlobalData.h"
#include "fdbrpc/AsyncFileWriteChecker.actor.h"

#include <stdint.h>

static void lru_test(benchmark::State& state) {
	int run = 10000;
	std::set<uint32_t> exist;
	int limit = 150000000; // 600GB file
	AsyncFileWriteChecker::LRU lru("TestLRU");

	// Benchmark
	for (auto _ : state) {
		for (int i = 0; i < run; ++i) {
			double r = deterministicRandom()->random01();
			// [0. 0,45] remove
			// [0.45, 0.5] truncate
			// [0.5, 1] update
			if (exist.size() < 2 || r > 0.5) {
				// to add/update
				uint32_t page = deterministicRandom()->randomInt(1, limit);
				// change the content each time
				auto wi = AsyncFileWriteChecker::WriteInfo();
				wi.timestamp = i;
				lru.update(page, wi);
				exist.insert(page);
			} else if (r < 0.45) {
				auto it = exist.begin();
				std::advance(it, deterministicRandom()->randomInt(0, (int)exist.size()));
				lru.remove(*it);
				exist.erase(it);
			} else {
				// to truncate, only truncate to first half
				auto it = exist.begin();
				std::advance(it, deterministicRandom()->randomInt(0, (int)exist.size() / 2));
				lru.truncate(*it);
				exist.erase(it, exist.end());
			}
		}
	}
}

BENCHMARK(lru_test);