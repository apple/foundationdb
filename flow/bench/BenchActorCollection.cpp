/*
 * BenchActorCollection.cpp
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

#include <vector>

#include "flow/ActorCollection.h"
#include "flow/ThreadHelper.h"

namespace {

enum class ActorCollectionScenario { AddPending, CompletePending };

Future<Void> waitForActorCount(int* actorCount, int expectedCount) {
	while (*actorCount != expectedCount) {
		co_await yield();
	}
}

Future<Void> benchActorCollectionReadyActor(benchmark::State* state) {
	while (state->KeepRunning()) {
		ActorCollection actors(true);
		actors.add(Void());
		co_await actors.getResult();
		benchmark::ClobberMemory();
	}

	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()));
}

template <ActorCollectionScenario scenario>
Future<Void> benchActorCollectionPendingActor(benchmark::State* state) {
	const int actorCount = state->range(0);

	while (state->KeepRunning()) {
		state->PauseTiming();

		PromiseStream<Future<Void>> addActors;
		int activeActors = 0;
		Future<Void> collection = actorCollection(addActors.getFuture(),
		                                          &activeActors,
		                                          nullptr,
		                                          nullptr,
		                                          nullptr,
		                                          scenario == ActorCollectionScenario::CompletePending);
		std::vector<Promise<Void>> completions;
		completions.reserve(actorCount);
		for (int i = 0; i < actorCount; ++i) {
			completions.emplace_back();
		}

		if constexpr (scenario == ActorCollectionScenario::AddPending) {
			state->ResumeTiming();
			for (auto& completion : completions) {
				addActors.send(completion.getFuture());
			}
			co_await waitForActorCount(&activeActors, actorCount);
			state->PauseTiming();

			collection.cancel();
		} else {
			for (auto& completion : completions) {
				addActors.send(completion.getFuture());
			}
			co_await waitForActorCount(&activeActors, actorCount);

			state->ResumeTiming();
			for (auto& completion : completions) {
				completion.send(Void());
			}
			co_await collection;
			state->PauseTiming();
		}

		benchmark::ClobberMemory();
	}

	state->ResumeTiming();
	state->SetItemsProcessed(static_cast<int64_t>(state->iterations()) * actorCount);
}

void benchActorCollectionReady(benchmark::State& state) {
	onMainThread([&state] { return benchActorCollectionReadyActor(&state); }).blockUntilReady();
}

template <ActorCollectionScenario scenario>
void benchActorCollectionPending(benchmark::State& state) {
	onMainThread([&state] { return benchActorCollectionPendingActor<scenario>(&state); }).blockUntilReady();
}

} // namespace

BENCHMARK(benchActorCollectionReady)->Name("ActorCollection/ready")->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchActorCollectionPending, ActorCollectionScenario::AddPending)
    ->Name("ActorCollection/add_pending")
    ->RangeMultiplier(16)
    ->Range(1, 1 << 8)
    ->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(benchActorCollectionPending, ActorCollectionScenario::CompletePending)
    ->Name("ActorCollection/complete_pending")
    ->RangeMultiplier(16)
    ->Range(1, 1 << 8)
    ->ReportAggregatesOnly(true);
