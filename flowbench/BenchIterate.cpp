/*
 * BenchIterate.cpp
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
#include "fdbclient/MutationList.h"
#include "flow/Arena.h"
#include "flow/FastAlloc.h"
#include "flowbench/GlobalData.h"

void populate(Standalone<VectorRef<MutationRef>>& mutations, size_t items, size_t size, KeyRef key, ValueRef value) {
	mutations = Standalone<VectorRef<MutationRef>>{};
	mutations.reserve(mutations.arena(), items);
	for (int i = 0; i < items; ++i) {
		mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::SetValue, key, value);
	}
}

void populate(MutationList& mutations, size_t items, size_t size, KeyRef key, ValueRef value) {
	mutations = MutationList{};
	for (int i = 0; i < items; ++i) {
		mutations.push_back_deep(mutations.arena(), MutationRef(MutationRef::Type::SetValue, key, value));
	}
}

// Benchmarks iteration over a list of mutations
template <class ListImpl>
static void bench_iterate(benchmark::State& state) {
	size_t items = state.range(0);
	size_t size = state.range(1);
	auto kv = getKV(size, size);
	ListImpl mutations;
	populate(mutations, items, size, kv.key, kv.value);
	while (state.KeepRunning()) {
		for (const auto& mutation : mutations) {
			benchmark::DoNotOptimize(mutation);
		}
	}
	state.SetItemsProcessed(items * static_cast<long>(state.iterations()));
}

BENCHMARK_TEMPLATE(bench_iterate, Standalone<VectorRef<MutationRef>>)
    ->Ranges({ { 1, 1 << 20 }, { 1, 1 << 9 } })
    ->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_iterate, MutationList)->Ranges({ { 1, 1 << 20 }, { 1, 1 << 9 } })->ReportAggregatesOnly(true);
