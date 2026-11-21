/*
 * BenchFlatBuffers.cpp
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

#include "flow/Knobs.h"
#include "flow/ObjectSerializer.h"
#include "flow/serialize.h"

#include <string>
#include <vector>

namespace {

constexpr FileIdentifier benchFileIdentifier{ 4343 };

struct Mixed {
	std::vector<int64_t> ints;
	std::vector<Optional<int>> optionals;
	std::vector<std::string> strings;
	std::vector<bool> bools;

	static Mixed make(int) {
		Mixed m;
		for (int i = 0; i < 100; ++i) {
			m.ints.push_back(static_cast<int64_t>(i) * 7919);
		}
		for (int i = 0; i < 20; ++i) {
			m.optionals.push_back(i % 2 ? Optional<int>(i) : Optional<int>());
		}
		for (int i = 0; i < 20; ++i) {
			m.strings.push_back(std::string(8 + i, 'a' + i));
		}
		m.bools = { true, false, true };
		return m;
	}

	template <class Archiver>
	void serialize(Archiver& ar) {
		serializer(ar, ints, optionals, strings, bools);
	}
};

struct ScalarVector {
	std::vector<int64_t> ints;

	static ScalarVector make(int n) {
		ScalarVector s;
		s.ints.assign(n, 123456789);
		return s;
	}

	template <class Archiver>
	void serialize(Archiver& ar) {
		serializer(ar, ints);
	}
};

struct StringVector {
	std::vector<std::string> strings;

	static StringVector make(int n) {
		StringVector s;
		s.strings.assign(n, std::string(16, 'x'));
		return s;
	}

	template <class Archiver>
	void serialize(Archiver& ar) {
		serializer(ar, strings);
	}
};

// Present only once the knob exists, so the same file also builds against a tree without it.
template <class Knobs>
void setBoundsChecks(Knobs& knobs, bool enabled) {
	if constexpr (requires { knobs.FLATBUFFERS_BOUNDS_CHECKS_ENABLED; }) {
		knobs.FLATBUFFERS_BOUNDS_CHECKS_ENABLED = enabled;
	}
}

template <class T, bool UseArenaReader>
void bench_decode(benchmark::State& state) {
	setBoundsChecks(*const_cast<FlowKnobs*>(FLOW_KNOBS), state.range(0) != 0);
	T src = T::make(state.range(1));
	ObjectWriter writer(Unversioned());
	writer.serialize(benchFileIdentifier, src);
	Standalone<StringRef> msg(writer.toStringRef());

	for (auto _ : state) {
		T dst;
		if constexpr (UseArenaReader) {
			ArenaObjectReader reader(Arena(), msg, Unversioned());
			reader.deserialize(benchFileIdentifier, dst);
		} else {
			ObjectReader reader(msg, Unversioned());
			reader.deserialize(benchFileIdentifier, dst);
		}
		benchmark::DoNotOptimize(dst);
	}
	state.SetItemsProcessed(state.iterations());
	state.SetBytesProcessed(state.iterations() * msg.size());
}

} // namespace

#define DECODE_BENCH(T, UseArena, N)                                                                                   \
	BENCHMARK_TEMPLATE(bench_decode, T, UseArena)                                                                      \
	    ->ArgNames({ "checks", "n" })                                                                                  \
	    ->Args({ 1, N })                                                                                               \
	    ->Args({ 0, N })                                                                                               \
	    ->ReportAggregatesOnly(true)

DECODE_BENCH(Mixed, false, 0);
DECODE_BENCH(Mixed, true, 0);
DECODE_BENCH(ScalarVector, false, 4096);
DECODE_BENCH(ScalarVector, true, 4096);
DECODE_BENCH(StringVector, false, 256);
DECODE_BENCH(StringVector, true, 256);
