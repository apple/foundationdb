/*
 * BenchPopulate.cpp
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

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "flow/Arena.h"
#include "flow/FastAlloc.h"
#include "flowbench/GlobalData.h"

static constexpr bool EMPLACE_BACK = true;
static constexpr bool PUSH_BACK = false;

// Benchmarks the population of a VectorRef<MutationRef>
template <bool emplace>
static void bench_populate(benchmark::State& state) {
	size_t items = state.range(0);
	size_t size = state.range(1);
	auto kv = getKV(size, size);
	while (state.KeepRunning()) {
		Standalone<VectorRef<MutationRef>> mutations;
		mutations.reserve(mutations.arena(), items);
		for (int i = 0; i < items; ++i) {
			if constexpr (emplace) {
				mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::SetValue, kv.key, kv.value);
			} else {
				mutations.push_back_deep(mutations.arena(), MutationRef(MutationRef::Type::SetValue, kv.key, kv.value));
			}
		}
		benchmark::DoNotOptimize(mutations);
	}
	state.SetItemsProcessed(items * static_cast<long>(state.iterations()));
}

BENCHMARK_TEMPLATE(bench_populate, EMPLACE_BACK)->Ranges({ { 1, 1 << 20 }, { 1, 512 } })->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_populate, PUSH_BACK)->Ranges({ { 1, 1 << 20 }, { 1, 512 } })->ReportAggregatesOnly(true);
