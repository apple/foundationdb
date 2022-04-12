/*
 * BenchVersionVector.cpp
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
#include "fdbclient/VersionVector.h"
#include <cstdint>

static void bench_vv_getdelta(benchmark::State& benchState) {
	int64_t tags = benchState.range(0);
	Version version = 100000;
	VersionVector vv(version);

	int i = 0;
	for (int i = 0; i < tags; i++) {
		vv.setVersion(Tag(0, i), ++version);
	}

	i = 0;
	const int64_t numDeltas = benchState.range(1);
	while (benchState.KeepRunning()) {
		vv.setVersion(Tag(0, i++), ++version);
		i %= tags;

		for (int j = 0; j < numDeltas; j++) {
			VersionVector delta;
			vv.getDelta(version - j, delta);
			benchmark::DoNotOptimize(delta);
		}
	}
	benchState.SetItemsProcessed(numDeltas * static_cast<long>(benchState.iterations()));
	benchState.counters.insert({ { "Tags", tags }, { "getDeltaTimes", numDeltas } });
}

BENCHMARK(bench_vv_getdelta)->Ranges({ { 1 << 4, 1 << 10 }, { 1, 1 << 10 } })->ReportAggregatesOnly(true);
