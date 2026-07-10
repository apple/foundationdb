/*
 * BenchMemoryTracker.cpp
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

// Microbenchmarks for the per-call-site memory tracker (see
// design/memory-tracker.md, Testing Considerations -> Microbenchmarks).
//
// Two questions, both about "is it cheap enough to leave on?" (R0):
//   * off-state cost  — the always-compiled hooks with sampling disabled;
//   * enabled-state cost — hooks at the production 1% rate and the pessimal
//     every-allocation rate, which includes both the sampled-alloc slow path
//     and the per-free lock+probe (the dominant enabled-state cost).
//
// Run: bin/flow_bench --benchmark_filter=memtracker
//
// FLOW_KNOBS points at the process-default (non-simulated) bootstrap knobs in
// flow_bench, so MEMORY_TRACKING_SAMPLE_INVERSE starts at 0 (off); we drive it
// per benchmark via const_cast, exactly like the unit tests do.

#include "benchmark/benchmark.h"

#include "flow/Knobs.h"
#include "flow/MemoryTracker.h"

#include <cstdlib>

namespace {

constexpr int kSize = 64;

// Set the sample-inverse knob (0=off, N=1-in-N) and clear tracker state so the
// run starts clean. Returns the previous inverse for restoration.
int setInverseAndReset(int inverse) {
	auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
	int prev = k->MEMORY_TRACKING_SAMPLE_INVERSE;
	k->MEMORY_TRACKING_SAMPLE_INVERSE = inverse;
	memTrackerResetForTest();
	return prev;
}

} // namespace

// Baseline: raw libc malloc/free. std::malloc is NOT hooked (we override
// operator new, not libc malloc), so this is the tracker-free reference the
// operator-new benchmark is compared against.
static void bench_memtracker_malloc_free(benchmark::State& state) {
	for (auto _ : state) {
		void* p = std::malloc(kSize);
		benchmark::DoNotOptimize(p);
		std::free(p);
	}
	state.SetItemsProcessed(state.iterations());
}

// End-to-end cost of a hooked allocation: global operator new[]/delete[] (which
// fire memTrackerOnAlloc/OnFree) at sample inverse Arg(0). 0 = off, 100 = prod
// 1%, 1 = every allocation. Compare Arg(0) against bench_memtracker_malloc_free
// for the disabled-hook cost, and Arg(100)/Arg(1) against Arg(0) for sampling.
static void bench_memtracker_operator_new(benchmark::State& state) {
	int prev = setInverseAndReset(state.range(0));
	for (auto _ : state) {
		char* p = new char[kSize];
		benchmark::DoNotOptimize(p);
		delete[] p;
	}
	state.SetItemsProcessed(state.iterations());
	setInverseAndReset(prev); // restore so later benchmarks aren't sampled
}

// Isolated tracker-hook cost: call memTrackerOnAlloc/OnFree directly on one
// preallocated buffer, with no real allocation in the loop, so only the
// tracker's own work is measured. At inverse>0 with live-tracking on, every
// OnFree still takes the global lock and probes the live table (the dominant
// enabled-state cost), while ~1/inverse of the OnAlloc calls take the sampling
// slow path (frame walk + table insert).
static void bench_memtracker_hooks(benchmark::State& state) {
	int prev = setInverseAndReset(state.range(0));
	void* p = std::malloc(kSize);
	for (auto _ : state) {
		memTrackerOnAlloc(p, kSize);
		memTrackerOnFree(p);
	}
	state.SetItemsProcessed(state.iterations());
	std::free(p);
	setInverseAndReset(prev);
}

BENCHMARK(bench_memtracker_malloc_free);
BENCHMARK(bench_memtracker_operator_new)->Arg(0)->Arg(100)->Arg(1);
BENCHMARK(bench_memtracker_hooks)->Arg(0)->Arg(100)->Arg(1);
