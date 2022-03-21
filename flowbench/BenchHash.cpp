/*
 * BenchHash.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "flow/crc32c.h"
#include "flow/Hash3.h"
#include "flow/xxhash.h"
#include "flowbench/GlobalData.h"

#include <stdint.h>

enum class HashType {
	HashLittle2,
	CRC32C,
	XXHash3,
};

template <HashType hashType>
inline void hash(const KeyRef& key, size_t length) {}

template <>
inline void hash<HashType::HashLittle2>(const KeyRef& key, size_t length) {
	uint32_t part1;
	uint32_t part2;
	hashlittle2(key.begin(), length, &part1, &part2);
	benchmark::DoNotOptimize(part1);
	benchmark::DoNotOptimize(part2);
}

template <>
inline void hash<HashType::CRC32C>(const KeyRef& key, size_t length) {
	benchmark::DoNotOptimize(crc32c_append(0xfdbeefdb, key.begin(), length));
}

template <>
inline void hash<HashType::XXHash3>(const KeyRef& key, size_t length) {
	benchmark::DoNotOptimize(XXH3_64bits(key.begin(), length));
}

template <HashType hashType>
static void bench_hash(benchmark::State& state) {
	auto length = 1 << state.range(0);
	auto key = getKey(length);
	while (state.KeepRunning()) {
		hash<hashType>(key, length);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK_TEMPLATE(bench_hash, HashType::CRC32C)->DenseRange(2, 18)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_hash, HashType::HashLittle2)->DenseRange(2, 18)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_hash, HashType::XXHash3)->DenseRange(2, 18)->ReportAggregatesOnly(true);
