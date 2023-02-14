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

#include "flow/Arena.h"
#include "benchmark/benchmark.h"
#include "fdbclient/VersionVector.h"
#include <cstdint>

struct TestContextArena {
	Arena& _arena;
	Arena& arena() { return _arena; }
	ProtocolVersion protocolVersion() const { return g_network->protocolVersion(); }
	uint8_t* allocate(size_t size) { return new (_arena) uint8_t[size]; }
};

static void bench_serializable_traits_version(benchmark::State& state) {
	int tagCount = state.range(0);

	Version version = 100000;
	VersionVector serializedVV(version);
	for (int i = 0; i < tagCount; i++) {
		serializedVV.setVersion(Tag(0, i), ++version);
	}

	size_t size = 0;
	VersionVector deserializedVV;
	for (auto _ : state) {
		Standalone<StringRef> msg = ObjectWriter::toValue(serializedVV, Unversioned());

		// Capture the serialized buffer size.
		state.PauseTiming();
		size = msg.size();
		state.ResumeTiming();

		ObjectReader rd(msg.begin(), Unversioned());
		rd.deserialize(deserializedVV);
	}
	ASSERT(serializedVV.compare(deserializedVV));
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
	state.counters.insert({ { "Tags", tagCount }, { "Size", size } });
}

static void bench_dynamic_size_traits_version(benchmark::State& state) {
	Arena arena;
	TestContextArena context{ arena };

	int tagCount = state.range(0);

	Version version = 100000;
	VersionVector serializedVV(version);
	for (int i = 0; i < tagCount; i++) {
		serializedVV.setVersion(Tag(0, i), ++version);
	}

	size_t size = 0;
	VersionVector deserializedVV;
	for (auto _ : state) {
		size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

		uint8_t* buf = context.allocate(size);
		dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

		dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);
	}
	ASSERT(serializedVV.compare(deserializedVV));
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
	state.counters.insert({ { "Tags", tagCount }, { "Size", size } });
}

BENCHMARK(bench_serializable_traits_version)->Ranges({ { 1 << 4, 1 << 10 } })->ReportAggregatesOnly(true);
BENCHMARK(bench_dynamic_size_traits_version)->Ranges({ { 1 << 4, 1 << 10 } })->ReportAggregatesOnly(true);
