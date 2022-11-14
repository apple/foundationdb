/*
 * BenchSamples.cpp
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
#include "flow/IRandom.h"
#include "flowbench/GlobalData.h"
#include "fdbrpc/Stats.h"
#include "flow/Histogram.h"

static void bench_continuousSampleInt(benchmark::State& state) {
	ContinuousSample<int64_t> cs(state.range(0));
	InputGenerator<int64_t> data(1e6, []() { return deterministicRandom()->randomInt64(0, 1e9); });

	for (auto _ : state) {
		cs.addSample(data.next());
	}

	state.SetItemsProcessed(state.iterations());
}
BENCHMARK(bench_continuousSampleInt)->Arg(1000)->Arg(10000)->ReportAggregatesOnly(true);

static void bench_continuousSampleLatency(benchmark::State& state) {
	ContinuousSample<double> cs(state.range(0));
	InputGenerator<double> data(1e6, []() { return deterministicRandom()->random01() * 2.0; });

	for (auto _ : state) {
		cs.addSample(data.next());
	}

	state.SetItemsProcessed(state.iterations());
}
BENCHMARK(bench_continuousSampleLatency)->Arg(1000)->Arg(10000)->ReportAggregatesOnly(true);

static void bench_latencyBands(benchmark::State& state) {
	constexpr double max = 2.0;
	InputGenerator<double> data(1e6, []() { return deterministicRandom()->random01() * max; });
	LatencyBands lb("test", UID(), 5);
	for (int i = 0; i < state.range(0); ++i) {
		lb.addThreshold(max * (double)(i + 1) / (state.range(0) + 1));
	}

	for (auto _ : state) {
		lb.addMeasurement(data.next(), false);
	}

	state.SetItemsProcessed(state.iterations());
}
BENCHMARK(bench_latencyBands)->Arg(1)->Arg(4)->Arg(7)->Arg(10)->ReportAggregatesOnly(true);

static void bench_histogramInt(benchmark::State& state) {
	Reference<Histogram> h = Histogram::getHistogram("histogramTest"_sr, "bytes"_sr, Histogram::Unit::bytes);
	InputGenerator<int32_t> data(1e6, []() { return deterministicRandom()->randomInt64(0, 1e9); });

	for (auto _ : state) {
		h->sample(data.next());
	}

	state.SetItemsProcessed(state.iterations());
}
BENCHMARK(bench_histogramInt)->ReportAggregatesOnly(true);

static void bench_histogramPct(benchmark::State& state) {
	Reference<Histogram> h = Histogram::getHistogram("histogramTest"_sr, "pct"_sr, Histogram::Unit::percentageLinear);
	InputGenerator<double> data(1e6, []() { return deterministicRandom()->random01() * 1.5; });

	for (auto _ : state) {
		h->samplePercentage(data.next());
	}

	state.SetItemsProcessed(state.iterations());
}
BENCHMARK(bench_histogramPct)->ReportAggregatesOnly(true);

static void bench_histogramTime(benchmark::State& state) {
	Reference<Histogram> h = Histogram::getHistogram("histogramTest"_sr, "latency"_sr, Histogram::Unit::milliseconds);
	InputGenerator<double> data(1e6, []() { return deterministicRandom()->random01() * 5; });

	for (auto _ : state) {
		h->sampleSeconds(data.next());
	}

	state.SetItemsProcessed(state.iterations());
}
BENCHMARK(bench_histogramTime)->ReportAggregatesOnly(true);
