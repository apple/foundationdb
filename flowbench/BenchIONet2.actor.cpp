/*
 * BenchIONet2.actor.cpp
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

#include "flow/Platform.h"
#include "flow/TreeBenchmark.h"
#include "flow/flow.h"
#include "flow/DeterministicRandom.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbrpc/IAsyncFile.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> increment(TaskPriority priority, uint32_t* sum) {
	wait(delay(0, priority));
	DeterministicRandom rand(1);
	int randSum = 0;
	for (int i = 0; i < 1e6; ++i) {
		randSum += rand.randomInt(0, 3);
	}
	benchmark::DoNotOptimize(randSum);
	++(*sum);
	return Void();
}

static inline TaskPriority getRandomTaskPriority(DeterministicRandom& rand) {
	return static_cast<TaskPriority>(rand.randomInt(0, 100));
}

ACTOR static Future<Void> benchIONet2Actor(benchmark::State* benchState) {
	state size_t actorCount = benchState->range(0);
	state uint32_t sum;
	state int seed = platform::getRandomSeed();
	state std::unique_ptr<char[]> data(new char[4096]);
	memset(data.get(), 0, 4096);
	while (benchState->KeepRunning()) {
		sum = 0;
		state std::vector<Future<Void>> futures;
		futures.reserve(actorCount);
		DeterministicRandom rand(seed);
		for (int i = 0; i < actorCount; ++i) {
			futures.push_back(increment(getRandomTaskPriority(rand), &sum));
		}
		state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
		    "/tmp/__test-benchmark-file__",
		    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE,
		    0600));
		wait(f->write(data.get(), 4096, 0));
		wait(f->sync());
		benchmark::DoNotOptimize(sum);
	}
	benchState->SetItemsProcessed(actorCount * static_cast<long>(benchState->iterations()));
	return Void();
}

static void bench_ionet2(benchmark::State& benchState) {
	onMainThread([&benchState] { return benchIONet2Actor(&benchState); }).blockUntilReady();
}

BENCHMARK(bench_ionet2)->Range(1, 1 << 16)->ReportAggregatesOnly(true);
