/*
 * BenchTrace.cpp
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

#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"

#include <cstdlib>

// Defined in flow/Trace.cpp, not declared in Trace.h.
ThreadFuture<Void> flushTraceFile();

// Cost of the TraceEvent hot path, for processes without an audit trace file (anything but fdbcli with audit logging
// on). Only uses APIs that exist with and without audit trace files, so the same file can be built against an older
// tree and compared.
//
// The benchmarks that need a trace file open one in $BENCH_TRACE_DIR and are skipped if it isn't set, since an open
// trace file can't be closed again and would change every benchmark that runs after it. Run each one in its own
// process:
//   BENCH_TRACE_DIR=/tmp/t flowbench --benchmark_filter='^bench_trace_event_file_open$' --benchmark_repetitions=10

// Opens the trace file in $BENCH_TRACE_DIR once. Returns false if it isn't set.
static bool openBenchTraceFile() {
	static bool opened = false;
	if (opened) {
		return true;
	}
	const char* dir = std::getenv("BENCH_TRACE_DIR");
	if (!dir) {
		return false;
	}
	opened = true;
	std::string directory = dir;
	// openTraceFile starts a recurring flush, so it has to run on the network thread.
	onMainThread([directory]() -> Future<Void> {
		openTraceFile(Optional<NetworkAddress>(), 10 << 20, 200 << 20, directory, "trace", "default");
		return Void();
	}).getBlocking();
	return true;
}

static void logOne(int64_t i) {
	TraceEvent("BenchTraceEvent").detail("Index", i).detail("Str", "some_value").detail("Num", 1.5);
}

// Trace file open, like fdbserver or a client with tracing on.
static void bench_trace_event_file_open(benchmark::State& state) {
	if (!openBenchTraceFile()) {
		state.SkipWithError("set BENCH_TRACE_DIR to run this");
		return;
	}
	int64_t i = 0;
	for (auto _ : state) {
		logOne(i++);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

// No trace file (run it in a process that hasn't opened one), like a client without tracing. Events buffer until the
// preopen buffer is full, then get dropped.
static void bench_trace_event_no_file(benchmark::State& state) {
	int64_t i = 0;
	for (auto _ : state) {
		logOne(i++);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

// Below the minimum severity, so the event is disabled.
static void bench_trace_event_disabled(benchmark::State& state) {
	int64_t i = 0;
	for (auto _ : state) {
		TraceEvent(SevDebug, "BenchTraceEventDebug").detail("Index", i++);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

// What the recurring flush (every TRACE_FLUSH_INTERVAL) pays, with nothing buffered.
static void bench_flush_trace_file(benchmark::State& state) {
	if (!openBenchTraceFile()) {
		state.SkipWithError("set BENCH_TRACE_DIR to run this");
		return;
	}
	for (auto _ : state) {
		ThreadFuture<Void> f = flushTraceFile();
		benchmark::DoNotOptimize(f);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK(bench_trace_event_file_open)->ReportAggregatesOnly(true);
BENCHMARK(bench_trace_event_no_file)->ReportAggregatesOnly(true);
BENCHMARK(bench_trace_event_disabled)->ReportAggregatesOnly(true);
BENCHMARK(bench_flush_trace_file)->ReportAggregatesOnly(true);
