/*
 * BenchAndFuture.actor.cpp
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

#include <vector>

#include "fdbclient/FDBTypes.h"
#include "flow/ActorCollection.h"
#include "flow/flow.h"
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

template <class T>
struct FutureContainerProxy;

template <>
struct FutureContainerProxy<AndFuture> {
	void add(const Future<Void>& f) { futures.add(f); }

	Future<Void> getFuture() { return futures.getFuture(); }

private:
	AndFuture futures;
};

template <>
struct FutureContainerProxy<ActorCollection> {
	void add(const Future<Void>& f) { actors.add(f); }

	Future<Void> getFuture() const { return actors.getResult(); }

private:
	ActorCollection actors{ /*returnWhenEmptied*/ true };
};

template <class T>
static void bench_AndFuture(benchmark::State& benchState) {
	for (auto _ : benchState) {
		FutureContainerProxy<T> futures;
		Promise<Void> p;
		for (int i = 0; i < benchState.range(0); ++i) {
			futures.add(p.getFuture());
		}
		p.send(Void());
		ASSERT(futures.getFuture().isReady());
	}
}

BENCHMARK_TEMPLATE(bench_AndFuture, ActorCollection)->Range(1 << 8, 1 << 10);
BENCHMARK_TEMPLATE(bench_AndFuture, AndFuture)->Range(1 << 8, 1 << 10);