/*
 * BenchVersionedMap.cpp
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

#include "fdbclient/VersionedMap.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace {

template <typename K>
struct VersionedMapHarness {
	using map = VersionedMap<K, int>;
	using key_type = K;

	struct result {
		typename map::iterator it;

		result(typename map::iterator it) : it(it) {}

		result& operator++() {
			++it;
			return *this;
		}

		const K& operator*() const { return it.key(); }

		const K& operator->() const { return it.key(); }

		bool operator==(result const& k) const { return it == k.it; }
		bool operator!=(result const& k) const { return !(*this == k); }
	};

	map s;

	void insert(K const& k) { s.insert(k, 1); }
	result find(K const& k) const { return result(s.atLatest().find(k)); }
	result not_found() const { return result(s.atLatest().end()); }
	result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(s.atLatest().end()); }
	result lower_bound(K const& k) const { return result(s.atLatest().lower_bound(k)); }
	result upper_bound(K const& k) const { return result(s.atLatest().upper_bound(k)); }
	void erase(K const& k) { s.erase(k); }
};

template <class K>
void sortUnique(std::vector<K>& keys) {
	std::sort(keys.begin(), keys.end());
	keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
}

struct IntFixture {
	using key_type = int;

	std::vector<int> keys;
	VersionedMapHarness<int> tree;

	static std::unique_ptr<IntFixture> create(int64_t keyCount, bool populateTree, bool uniqueSortedKeys = false) {
		auto fixture = std::make_unique<IntFixture>();
		fixture->keys.reserve(keyCount);
		std::mt19937_64 rng(0x4d595df4d0f33173ULL);
		std::uniform_int_distribution<int> dist(0, INT_MAX);
		for (int64_t i = 0; i < keyCount; ++i) {
			fixture->keys.push_back(dist(rng));
		}
		if (populateTree) {
			for (const auto& key : fixture->keys) {
				fixture->tree.insert(key);
			}
		}
		if (uniqueSortedKeys) {
			sortUnique(fixture->keys);
		}
		return fixture;
	}
};

struct StringRefFixture {
	using key_type = StringRef;

	Arena arena;
	std::vector<StringRef> keys;
	VersionedMapHarness<StringRef> tree;

	static std::unique_ptr<StringRefFixture> create(int64_t keyCount, bool populateTree, bool uniqueSortedKeys = false) {
		auto fixture = std::make_unique<StringRefFixture>();
		fixture->keys.reserve(keyCount);
		std::mt19937_64 rng(0x46a2f9b1d8c3e57bULL);
		constexpr char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		constexpr int keySize = 100;
		std::string key;
		key.resize(keySize);
		for (int64_t i = 0; i < keyCount; ++i) {
			for (int j = 0; j < keySize; ++j) {
				key[j] = alphabet[rng() % (sizeof(alphabet) - 1)];
			}
			fixture->keys.push_back(StringRef(fixture->arena, key));
		}
		if (populateTree) {
			for (const auto& keyRef : fixture->keys) {
				fixture->tree.insert(keyRef);
			}
		}
		if (uniqueSortedKeys) {
			sortUnique(fixture->keys);
		}
		return fixture;
	}
};

template <class Fixture>
static void bench_versioned_map_insert(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, false);
		state.ResumeTiming();

		for (const auto& key : fixture->keys) {
			fixture->tree.insert(key);
		}
		benchmark::DoNotOptimize(&fixture->tree);

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(keyCount * state.iterations());
}

template <class Fixture>
static void bench_versioned_map_find(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, true);
		state.ResumeTiming();

		for (const auto& key : fixture->keys) {
			auto found = fixture->tree.find(key);
			ASSERT(found != fixture->tree.not_found());
			benchmark::DoNotOptimize(found);
		}

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(keyCount * state.iterations());
}

template <class Fixture>
static void bench_versioned_map_lower_bound(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, true);
		state.ResumeTiming();

		for (const auto& key : fixture->keys) {
			auto found = fixture->tree.lower_bound(key);
			ASSERT(found != fixture->tree.not_found());
			benchmark::DoNotOptimize(found);
		}

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(keyCount * state.iterations());
}

template <class Fixture>
static void bench_versioned_map_upper_bound(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, true);
		state.ResumeTiming();

		for (const auto& key : fixture->keys) {
			auto found = fixture->tree.upper_bound(key);
			benchmark::DoNotOptimize(found);
		}

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(keyCount * state.iterations());
}

template <class Fixture>
static void bench_versioned_map_scan(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	int64_t scanned = 0;
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, true, true);
		state.ResumeTiming();

		auto iter = fixture->tree.lower_bound(fixture->keys.front());
		for (const auto& key : fixture->keys) {
			ASSERT(iter != fixture->tree.end());
			ASSERT(key == *iter);
			++iter;
		}
		scanned += fixture->keys.size();
		ASSERT(iter == fixture->tree.end());
		benchmark::DoNotOptimize(iter);

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(scanned);
}

template <class Fixture>
static void bench_versioned_map_find_sorted(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	int64_t foundCount = 0;
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, true, true);
		state.ResumeTiming();

		for (const auto& key : fixture->keys) {
			auto found = fixture->tree.find(key);
			ASSERT(found != fixture->tree.end());
			benchmark::DoNotOptimize(found);
		}
		foundCount += fixture->keys.size();

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(foundCount);
}

template <class Fixture>
static void bench_versioned_map_erase(benchmark::State& state) {
	const int64_t keyCount = state.range(0);
	int64_t erased = 0;
	for (auto _ : state) {
		state.PauseTiming();
		auto fixture = Fixture::create(keyCount, true, true);
		std::mt19937_64 rng(0x91e10da5c79e7b1dULL);
		std::shuffle(fixture->keys.begin(), fixture->keys.end(), rng);
		state.ResumeTiming();

		for (const auto& key : fixture->keys) {
			fixture->tree.erase(key);
		}
		erased += fixture->keys.size();
		ASSERT(fixture->tree.begin() == fixture->tree.end());
		benchmark::DoNotOptimize(&fixture->tree);

		state.PauseTiming();
		fixture.reset();
		state.ResumeTiming();
	}
	state.SetItemsProcessed(erased);
}

static void setKOpsCounter(benchmark::State& state, const char* name, int64_t opCount, double elapsed) {
	state.counters[name] = elapsed == 0 ? 0 : (opCount / 1000.0) / elapsed;
}

static void bench_versioned_map_multiversion_int(benchmark::State& state) {
	constexpr int initialKeys = 20000;
	constexpr int versions = 50000;
	constexpr int retainedVersions = 5000;
	constexpr int forgetEvery = 1000;
	constexpr int scanWidth = 8;

	double populateElapsed = 0;
	double createElapsed = 0;
	double latestInsertElapsed = 0;
	double latestEraseElapsed = 0;
	double historicalFindElapsed = 0;
	double historicalLowerBoundElapsed = 0;
	double historicalScanElapsed = 0;
	double compactForgetElapsed = 0;

	int64_t populateOps = 0;
	int64_t createOps = 0;
	int64_t latestInsertOps = 0;
	int64_t latestEraseOps = 0;
	int64_t historicalFindOps = 0;
	int64_t historicalLowerBoundOps = 0;
	int64_t historicalScanOps = 0;
	int64_t compactForgetOps = 0;

	for (auto _ : state) {
		double totalStart = timer();
		VersionedMap<int, int> map;
		map.createNewVersion(1);

		double start = timer();
		for (int key = 0; key < initialKeys; ++key) {
			map.insert(key, key);
		}
		populateElapsed += timer() - start;
		populateOps += initialKeys;

		for (int version = 2; version <= versions; ++version) {
			start = timer();
			map.createNewVersion(version);
			createElapsed += timer() - start;
			++createOps;

			start = timer();
			map.insert(version % initialKeys, version);
			map.insert(initialKeys + version, version);
			latestInsertElapsed += timer() - start;
			latestInsertOps += 2;

			if (version > retainedVersions + 1) {
				start = timer();
				map.erase(initialKeys + version - retainedVersions);
				latestEraseElapsed += timer() - start;
				++latestEraseOps;
			}

			Version readVersion = std::max<Version>(map.getOldestVersion(), version - retainedVersions / 2);
			readVersion = std::max<Version>(readVersion, 1);

			start = timer();
			auto historicalFind = map.at(readVersion).find(int(readVersion % initialKeys));
			ASSERT(historicalFind);
			historicalFindElapsed += timer() - start;
			++historicalFindOps;

			start = timer();
			auto historicalLower = map.at(readVersion).lower_bound(int((readVersion * 17) % initialKeys));
			ASSERT(historicalLower);
			historicalLowerBoundElapsed += timer() - start;
			++historicalLowerBoundOps;

			start = timer();
			auto scan = map.at(readVersion).lower_bound(int((readVersion * 31) % (initialKeys - scanWidth)));
			for (int i = 0; i < scanWidth; ++i) {
				ASSERT(scan);
				++scan;
				++historicalScanOps;
			}
			historicalScanElapsed += timer() - start;

			if (version > retainedVersions && version % forgetEvery == 0) {
				Version cutoff = version - retainedVersions;
				start = timer();
				map.compact(cutoff);
				map.forgetVersionsBefore(cutoff);
				compactForgetElapsed += timer() - start;
				compactForgetOps += 2;
			}
		}

		benchmark::DoNotOptimize(map.roots.size());
		double iterationElapsed = timer() - totalStart;
		state.SetIterationTime(iterationElapsed);
	}

	state.SetItemsProcessed((populateOps + createOps + latestInsertOps + latestEraseOps + historicalFindOps +
	                         historicalLowerBoundOps + historicalScanOps + compactForgetOps));
	setKOpsCounter(state, "populate_kops", populateOps, populateElapsed);
	setKOpsCounter(state, "create_version_kops", createOps, createElapsed);
	setKOpsCounter(state, "latest_insert_kops", latestInsertOps, latestInsertElapsed);
	setKOpsCounter(state, "latest_erase_kops", latestEraseOps, latestEraseElapsed);
	setKOpsCounter(state, "historical_find_kops", historicalFindOps, historicalFindElapsed);
	setKOpsCounter(state, "historical_lower_bound_kops", historicalLowerBoundOps, historicalLowerBoundElapsed);
	setKOpsCounter(state, "historical_scan_kops", historicalScanOps, historicalScanElapsed);
	setKOpsCounter(state, "compact_forget_kops", compactForgetOps, compactForgetElapsed);
}

} // namespace

BENCHMARK_TEMPLATE(bench_versioned_map_insert, IntFixture)
    ->Name("VersionedMap/int/insert")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_find, IntFixture)
    ->Name("VersionedMap/int/find")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_lower_bound, IntFixture)
    ->Name("VersionedMap/int/lower_bound")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_upper_bound, IntFixture)
    ->Name("VersionedMap/int/upper_bound")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_scan, IntFixture)
    ->Name("VersionedMap/int/scan")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_find_sorted, IntFixture)
    ->Name("VersionedMap/int/find_sorted")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_erase, IntFixture)
    ->Name("VersionedMap/int/erase")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_TEMPLATE(bench_versioned_map_insert, StringRefFixture)
    ->Name("VersionedMap/StringRef/insert")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_find, StringRefFixture)
    ->Name("VersionedMap/StringRef/find")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_lower_bound, StringRefFixture)
    ->Name("VersionedMap/StringRef/lower_bound")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_upper_bound, StringRefFixture)
    ->Name("VersionedMap/StringRef/upper_bound")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_scan, StringRefFixture)
    ->Name("VersionedMap/StringRef/scan")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_find_sorted, StringRefFixture)
    ->Name("VersionedMap/StringRef/find_sorted")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(bench_versioned_map_erase, StringRefFixture)
    ->Name("VersionedMap/StringRef/erase")
    ->Arg(1000000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(bench_versioned_map_multiversion_int)
    ->Name("VersionedMap/int/multiversion")
    ->Iterations(1)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
