/*
 * BenchConflictSet.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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
#include "flow/Error.h"
#include <vector>
#include <algorithm>
#include <cstdint>
#include <limits>

// ============================================================================
// Current MiniConflictSet implementation (from SkipList.cpp)
// ============================================================================
class MiniConflictSet {
	std::vector<bool> values;

public:
	explicit MiniConflictSet(int size) { values.assign(size, false); }
	void set(int begin, int end) {
		for (int i = begin; i < end; i++)
			values[i] = true;
	}
	bool any(int begin, int end) {
		for (int i = begin; i < end; i++)
			if (values[i])
				return true;
		return false;
	}
	void clear() { std::fill(values.begin(), values.end(), false); }
};

// ============================================================================
// WordBitsetConflictSet implementation
// ============================================================================
class WordBitsetConflictSet {
	std::vector<uint64_t> words;

public:
	explicit WordBitsetConflictSet(size_t size) {
		ASSERT(size <= std::numeric_limits<size_t>::max() - 63); // Prevent overflow in (size + 63)
		words.resize((size + 63) / 64, 0);
	}

	void set(int begin, int end) {
		if (end <= begin)
			return;

		const size_t wordBegin = static_cast<size_t>(begin) / 64;
		const size_t wordEnd = static_cast<size_t>(end - 1) / 64; // Last word containing bits (inclusive)

		if (wordBegin == wordEnd) {
			// Single word case
			const size_t bitStart = static_cast<size_t>(begin) % 64;
			const size_t numBits = static_cast<size_t>(end - begin);

			ASSERT(numBits <= 64); // Single word should never span > 64 bits
			uint64_t mask;
			if (numBits == 64) {
				mask = ~0ULL;
			} else {
				mask = ((1ULL << numBits) - 1) << bitStart;
			}
			words[wordBegin] |= mask;
		} else {
			// Multi-word case: wordBegin (partial) + middle words (full) + wordEnd (partial)
			words[wordBegin] |= ~0ULL << (static_cast<size_t>(begin) % 64);
			for (size_t w = wordBegin + 1; w < wordEnd; w++) { // Fill middle words completely
				words[w] = ~0ULL;
			}
			words[wordEnd] |= ~0ULL >> (63 - (static_cast<size_t>(end - 1) % 64));
		}
	}

	bool any(int begin, int end) const {
		if (end <= begin)
			return false;

		const size_t wordBegin = static_cast<size_t>(begin) / 64;
		const size_t wordEnd = static_cast<size_t>(end - 1) / 64; // Last word containing bits (inclusive)

		for (size_t w = wordBegin; w <= wordEnd; w++) { // Check all words including wordEnd
			uint64_t mask = ~0ULL;
			if (w == wordBegin) {
				mask &= (~0ULL << (static_cast<size_t>(begin) % 64));
			}
			if (w == wordEnd) {
				mask &= (~0ULL >> (63 - (static_cast<size_t>(end - 1) % 64)));
			}

			if (words[w] & mask)
				return true;
		}
		return false;
	}

	void clear() { std::fill(words.begin(), words.end(), 0); }
};

// ============================================================================
// Forward declarations and utility types
// ============================================================================
struct ConflictRange {
	int begin;
	int end;
};

// ============================================================================
// Benchmark workload generators
// ============================================================================

// Unified shared workload generator
template <int NumRanges, int KeySpace, int SparsityPercent, int WorkloadType = 0>
static const std::vector<ConflictRange>& getSharedWorkload() {
	static const auto workload = []() {
		// Different seed per workload type to ensure write/query workloads differ
		const uint32_t seed = (KeySpace + NumRanges + SparsityPercent) * (WorkloadType + 1);
		setThreadLocalDeterministicRandomSeed(seed);

		const double sparsity = SparsityPercent / 100.0;
		std::vector<ConflictRange> ranges;
		ranges.reserve(NumRanges);

		for (int i = 0; i < NumRanges; i++) {
			int begin = deterministicRandom()->randomInt(0, KeySpace);
			int rangeSize = std::max(1, (int)(deterministicRandom()->random01() * KeySpace * sparsity));
			int end = std::min(KeySpace, begin + rangeSize);
			ranges.push_back({ begin, end });
		}

		return ranges;
	}();
	return workload;
}

// Convenience aliases for clarity
template <int NumRanges, int KeySpace, int SparsityPercent>
static const std::vector<ConflictRange>& getSharedWorkloadTemplate() {
	return getSharedWorkload<NumRanges, KeySpace, SparsityPercent, 0>();
}

template <int NumRanges, int KeySpace, int SparsityPercent>
static const std::vector<ConflictRange>& getSharedQueryWorkloadTemplate() {
	return getSharedWorkload<NumRanges, KeySpace, SparsityPercent, 1>();
}

// ============================================================================
// Correctness test to verify all implementations produce same results
// ============================================================================
bool verifyCorrectness() {
	// Use shared workload template system for consistency
	const auto& ranges = getSharedWorkloadTemplate<50, 1000, 10>();
	const auto& queryRanges = getSharedQueryWorkloadTemplate<100, 1000, 5>();
	const int keySpace = 1000;

	MiniConflictSet mini(keySpace);
	WordBitsetConflictSet cache(keySpace);

	// Set the same ranges in both implementations
	for (const auto& range : ranges) {
		mini.set(range.begin, range.end);
		cache.set(range.begin, range.end);
	}

	// Verify both implementations report the same conflicts
	for (const auto& range : queryRanges) {
		bool miniResult = mini.any(range.begin, range.end);
		bool cacheResult = cache.any(range.begin, range.end);

		if (miniResult != cacheResult) {
			return false;
		}
	}

	return true;
}

// ============================================================================
// Benchmarks - Current std::vector<bool> implementation
// ============================================================================
template <int KeySpace, int NumRanges, int SparsityPercent>
static void bench_MiniConflictSet_set(benchmark::State& state) {
	const auto& ranges = getSharedWorkloadTemplate<NumRanges, KeySpace, SparsityPercent>();

	for (auto _ : state) {
		MiniConflictSet mcs(KeySpace);
		for (const auto& range : ranges) {
			mcs.set(range.begin, range.end);
		}
		benchmark::DoNotOptimize(mcs);
	}

	state.SetItemsProcessed(state.iterations() * NumRanges);
	state.SetBytesProcessed(state.iterations() * KeySpace / 8); // bits to bytes
}

template <int KeySpace, int NumRanges, int SparsityPercent>
static void bench_MiniConflictSet_query(benchmark::State& state) {
	const auto& writeRanges = getSharedWorkloadTemplate<NumRanges, KeySpace, SparsityPercent>();
	const auto& readRanges = getSharedQueryWorkloadTemplate<NumRanges, KeySpace, SparsityPercent>();

	MiniConflictSet mcs(KeySpace);
	for (const auto& range : writeRanges) {
		mcs.set(range.begin, range.end);
	}

	for (auto _ : state) {
		int conflicts = 0;
		for (const auto& range : readRanges) {
			if (mcs.any(range.begin, range.end)) {
				conflicts++;
			}
		}
		benchmark::DoNotOptimize(conflicts);
	}

	state.SetItemsProcessed(state.iterations() * readRanges.size());
}

// ============================================================================
// Benchmarks - WordBitsetConflictSet implementation
// ============================================================================
template <int KeySpace, int NumRanges, int SparsityPercent>
static void bench_WordBitsetConflictSet_set(benchmark::State& state) {
	const auto& ranges = getSharedWorkloadTemplate<NumRanges, KeySpace, SparsityPercent>();

	for (auto _ : state) {
		WordBitsetConflictSet mcs(KeySpace);
		for (const auto& range : ranges) {
			mcs.set(range.begin, range.end);
		}
		benchmark::DoNotOptimize(mcs);
	}

	state.SetItemsProcessed(state.iterations() * NumRanges);
	state.SetBytesProcessed(state.iterations() * KeySpace / 8); // bits to bytes
}

template <int KeySpace, int NumRanges, int SparsityPercent>
static void bench_WordBitsetConflictSet_query(benchmark::State& state) {
	const auto& writeRanges = getSharedWorkloadTemplate<NumRanges, KeySpace, SparsityPercent>();
	const auto& readRanges = getSharedQueryWorkloadTemplate<NumRanges, KeySpace, SparsityPercent>();

	WordBitsetConflictSet mcs(KeySpace);
	for (const auto& range : writeRanges) {
		mcs.set(range.begin, range.end);
	}

	for (auto _ : state) {
		int conflicts = 0;
		for (const auto& range : readRanges) {
			if (mcs.any(range.begin, range.end)) {
				conflicts++;
			}
		}
		benchmark::DoNotOptimize(conflicts);
	}

	state.SetItemsProcessed(state.iterations() * readRanges.size());
}

// Correctness verification benchmark
static void bench_CorrectnessTest(benchmark::State& state) {
	// Use a fixed seed for reproducible correctness testing

	for (auto _ : state) {
		bool correct = verifyCorrectness();
		benchmark::DoNotOptimize(correct);
		if (!correct) {
			state.SkipWithError("Correctness verification failed");
			return;
		}
	}
}

template <int Implementation>
static void bench_ConflictDetection_Realistic(benchmark::State& state) {
	// Use template system with realistic FoundationDB parameters:
	// ~200 write ranges (100 transactions × ~2 writes each)
	// ~400 read ranges (100 transactions × ~4 reads each)
	// 10K keyspace (typical sorted boundary count)
	// Low sparsity for realistic range sizes
	const auto& writeRanges = getSharedWorkloadTemplate<200, 10000, 2>();
	const auto& readRanges = getSharedQueryWorkloadTemplate<400, 10000, 3>();
	const int keySpace = 10000;

	for (auto _ : state) {
		if constexpr (Implementation == 0) {
			// MiniConflictSet (std::vector<bool>)
			MiniConflictSet mcs(keySpace);

			for (const auto& range : writeRanges) {
				mcs.set(range.begin, range.end);
			}

			int conflicts = 0;
			for (const auto& range : readRanges) {
				if (mcs.any(range.begin, range.end)) {
					conflicts++;
				}
			}
			benchmark::DoNotOptimize(conflicts);
		} else if constexpr (Implementation == 1) {
			// WordBitsetConflictSet
			WordBitsetConflictSet mcs(keySpace);

			for (const auto& range : writeRanges) {
				mcs.set(range.begin, range.end);
			}

			int conflicts = 0;
			for (const auto& range : readRanges) {
				if (mcs.any(range.begin, range.end)) {
					conflicts++;
				}
			}
			benchmark::DoNotOptimize(conflicts);
		}
	}

	state.SetItemsProcessed(state.iterations() * (writeRanges.size() + readRanges.size()));
}

// ============================================================================
// Benchmark registration - use BENCHMARK_TEMPLATE for templated functions
// ============================================================================

// Correctness verification (run this first!)
BENCHMARK(bench_CorrectnessTest)->Name("CorrectnessTest/VerifyAllImplementations");

// Small workloads (typical transaction batch)
BENCHMARK_TEMPLATE(bench_MiniConflictSet_set, 1000, 50, 5)->Name("MiniConflictSet/set/small_sparse");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_set, 1000, 50, 5)->Name("WordBitsetConflictSet/set/small_sparse");

BENCHMARK_TEMPLATE(bench_MiniConflictSet_query, 1000, 50, 5)->Name("MiniConflictSet/query/small_sparse");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_query, 1000, 50, 5)->Name("WordBitsetConflictSet/query/small_sparse");

// Medium workloads (busy transaction batch)
BENCHMARK_TEMPLATE(bench_MiniConflictSet_set, 5000, 200, 10)->Name("MiniConflictSet/set/medium_sparse");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_set, 5000, 200, 10)->Name("WordBitsetConflictSet/set/medium_sparse");

BENCHMARK_TEMPLATE(bench_MiniConflictSet_query, 5000, 200, 10)->Name("MiniConflictSet/query/medium_sparse");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_query, 5000, 200, 10)->Name("WordBitsetConflictSet/query/medium_sparse");

// Large workloads (high throughput scenario) - where optimization shines
BENCHMARK_TEMPLATE(bench_MiniConflictSet_set, 20000, 500, 5)->Name("MiniConflictSet/set/large_sparse");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_set, 20000, 500, 5)->Name("WordBitsetConflictSet/set/large_sparse");

BENCHMARK_TEMPLATE(bench_MiniConflictSet_query, 20000, 500, 5)->Name("MiniConflictSet/query/large_sparse");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_query, 20000, 500, 5)->Name("WordBitsetConflictSet/query/large_sparse");

// Dense workloads (challenging for optimizations)
BENCHMARK_TEMPLATE(bench_MiniConflictSet_set, 10000, 100, 50)->Name("MiniConflictSet/set/dense");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_set, 10000, 100, 50)->Name("WordBitsetConflictSet/set/dense");

BENCHMARK_TEMPLATE(bench_MiniConflictSet_query, 10000, 100, 50)->Name("MiniConflictSet/query/dense");
BENCHMARK_TEMPLATE(bench_WordBitsetConflictSet_query, 10000, 100, 50)->Name("WordBitsetConflictSet/query/dense");

// Realistic FoundationDB workload comparison
BENCHMARK_TEMPLATE(bench_ConflictDetection_Realistic, 0)->Name("ConflictDetection/MiniConflictSet/realistic");
BENCHMARK_TEMPLATE(bench_ConflictDetection_Realistic, 1)->Name("ConflictDetection/WordBitsetConflictSet/realistic");