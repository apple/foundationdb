/*
 * BenchMetadataCheck.cpp
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
#include "fdbclient/SystemData.h"

// These benchmarks test the performance of different checks methods
// of checking for metadata mutations in applyMetadataMutations

static const std::array<MutationRef, 5> mutations = {
	MutationRef(MutationRef::Type::ClearRange, normalKeys.begin, normalKeys.end),
	MutationRef(MutationRef::Type::ClearRange, LiteralStringRef("a"), LiteralStringRef("b")),
	MutationRef(MutationRef::Type::ClearRange, LiteralStringRef("aaaaaaaaaa"), LiteralStringRef("bbbbbbbbbb")),
	MutationRef(MutationRef::Type::ClearRange, normalKeys.begin, systemKeys.end),
	MutationRef(MutationRef::Type::ClearRange,
	            LiteralStringRef("a").withPrefix(systemKeys.begin),
	            LiteralStringRef("b").withPrefix(systemKeys.begin)),
};

static void bench_check_metadata1(benchmark::State& state) {
	const auto& m = mutations[state.range(0)];
	while (state.KeepRunning()) {
		benchmark::DoNotOptimize(KeyRangeRef(m.param1, m.param2).intersects(systemKeys));
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

static void bench_check_metadata2(benchmark::State& state) {
	const auto& m = mutations[state.range(0)];
	while (state.KeepRunning()) {
		benchmark::DoNotOptimize(m.param2.size() > 1 && m.param2[0] == systemKeys.begin[0]);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK(bench_check_metadata1)->DenseRange(0, mutations.size() - 1)->ReportAggregatesOnly(true);
BENCHMARK(bench_check_metadata2)->DenseRange(0, mutations.size() - 1)->ReportAggregatesOnly(true);
