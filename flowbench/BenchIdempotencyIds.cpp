/*
 * BenchIdempotencyIds.cpp
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

#include "fdbclient/BuildIdempotencyIdMutations.h"

// We don't want the compiler to know that this is always false. It is though.
static bool getRuntimeFalse() {
	return deterministicRandom()->randomInt(0, 2) == -1;
}

static void bench_add_idempotency_ids(benchmark::State& state) {
	auto numTransactions = state.range(0);
	auto idSize = state.range(1);
	auto trs = std::vector<CommitTransactionRequest>(numTransactions);
	IdempotencyIdKVBuilder idempotencyKVBuilder;
	Version commitVersion = 0;
	auto committed = std::vector<uint8_t>(numTransactions);
	auto committedValue = 3;
	for (auto& c : committed) {
		c = deterministicRandom()->coinflip() ? committedValue : 0;
	}
	for (auto& tr : trs) {
		if (idSize > 0) {
			auto id = makeString(idSize, tr.arena);
			deterministicRandom()->randomBytes(mutateString(id), idSize);
			tr.idempotencyId = IdempotencyIdRef(tr.arena, IdempotencyIdRef(id));
		}
	}
	bool locked = getRuntimeFalse();
	for (auto _ : state) {
		buildIdempotencyIdMutations(
		    trs, idempotencyKVBuilder, commitVersion++, committed, committedValue, locked, [](const KeyValue&) {
			    benchmark::DoNotOptimize(0);
		    });
	}
	state.counters["TimePerTransaction"] = benchmark::Counter(
	    state.iterations() * numTransactions, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(bench_add_idempotency_ids)->ArgsProduct({ benchmark::CreateRange(1, 16384, 4), { 0, 16, 255 } });
