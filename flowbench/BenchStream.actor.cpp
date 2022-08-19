/*
 * BenchStream.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"
#include "flow/TLSConfig.actor.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/network.h"
#include "flowbench/GlobalData.h"

#include <thread>

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> benchStreamActor(benchmark::State* benchState) {
	state size_t items = benchState->range(0);
	size_t size = benchState->range(1);
	state KeyRef key = getKey(size);
	state PromiseStream<Key> stream;
	state int i;
	while (benchState->KeepRunning()) {
		for (i = 0; i < items; ++i) {
			stream.send(key);
		}
		for (i = 0; i < items; ++i) {
			Key receivedKey = waitNext(stream.getFuture());
			benchmark::DoNotOptimize(receivedKey);
		}
	}
	benchState->SetItemsProcessed(items * static_cast<long>(benchState->iterations()));
	return Void();
}

static void bench_stream(benchmark::State& benchState) {
	onMainThread([&benchState]() { return benchStreamActor(&benchState); }).blockUntilReady();
}

BENCHMARK(bench_stream)->Ranges({ { 1, 1 << 16 }, { 1, 1 << 16 } })->ReportAggregatesOnly(true);
