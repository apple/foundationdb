/*
 * BenchTrach.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Stats.h"
#include "flow/network.h"
#include "flow/Platform.h"
#include "flow/Trace.h"

#include "benchmark/benchmark.h"

class SampleTrace {
	TraceEvent te;
	int numFields;

	inline std::string fieldKey() { return format("F%d", numFields++); }

public:
	SampleTrace() : te("Sample"), numFields(0) {}
	template <class T, class... Args>
	void createAndDetail(Args&&... args) {
		te.detail(fieldKey(), T(std::forward<Args>(args)...));
	}
	template <class U>
	void detail(U&& val) {
		te.detail(fieldKey(), std::forward<U>(val));
	}
};

static void bench_trace_simple(benchmark::State& state) {
	openTraceFile(NetworkAddress(),
	              TRACE_DEFAULT_ROLL_SIZE,
	              TRACE_DEFAULT_MAX_LOGS_SIZE,
	              platform::getWorkingDirectory(),
	              "trace",
	              "",
	              "");
	while (state.KeepRunning()) {
		SampleTrace te;
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

static void bench_trace(benchmark::State& state) {
	openTraceFile(NetworkAddress(),
	              TRACE_DEFAULT_ROLL_SIZE,
	              TRACE_DEFAULT_MAX_LOGS_SIZE,
	              platform::getWorkingDirectory(),
	              "trace",
	              "",
	              "");
	CounterCollection cc("CC");
	Counter c("C", cc);
	std::string shortString = "xxxxx";
	std::string longString(100, 'x');
	Standalone<VectorRef<int>> vec1;
	std::vector<int> vec2;
	for (int i = 0; i < 10; ++i) {
		vec1.push_back(vec1.arena(), i);
	}
	for (int i = 10; i < 20; ++i) {
		vec2.push_back(i);
	}
	while (state.KeepRunning()) {
		SampleTrace te;
		te.createAndDetail<bool>();
		te.createAndDetail<signed char>();
		te.createAndDetail<unsigned char>();
		te.createAndDetail<short>();
		te.createAndDetail<unsigned short>();
		te.createAndDetail<int>();
		te.createAndDetail<unsigned>();
		te.createAndDetail<long int>();
		te.createAndDetail<unsigned long int>();
		te.createAndDetail<long long int>();
		te.createAndDetail<unsigned long long int>();
		te.createAndDetail<std::string>();
		te.createAndDetail<std::string>(100, 'x');
		te.detail(shortString);
		te.detail(longString);
		te.detail(c);
		te.detail(vec1);
		te.detail(vec2);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK(bench_trace_simple)->ReportAggregatesOnly(true);
BENCHMARK(bench_trace)->ReportAggregatesOnly(true);
