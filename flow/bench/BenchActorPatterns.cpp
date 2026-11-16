/*
 * BenchActorPatterns.cpp
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

#include "flow/ThreadHelper.h"
#include "flow/genericactors.h"

#include <vector>

namespace {

Future<Void> emptyUncancellable(Uncancellable = {}) {
	co_return;
}

Future<Void> readyFuture() {
	return Void();
}

Future<Void> waitUncancellable(Future<Void> input, Uncancellable = {}) {
	co_await input;
}

Future<Void> waitOne(Future<Void> input) {
	co_await input;
}

Future<Void> waitEither(Future<Void> first, Future<Void> second) {
	co_await race(first, second);
}

Future<Void> g_input;

Future<Void> waitSnapshot() {
	Future<Void> input = g_input;
	co_await input;
}

Future<int> sumStream(FutureStream<int> input) {
	int sum = 0;
	try {
		while (true) {
			sum += co_await input;
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) {
			throw;
		}
	}
	co_return sum;
}

enum class ScalarPattern {
	EmptyUncancellable,
	ReadyFuture,
	WaitUncancellableReady,
	WaitReady,
	WaitCancel,
	WaitAfter,
	RaceBothReady,
	RaceFirstReady,
	RaceSecondReady,
	RaceCancel,
	RaceAfter,
	WaitRaceAfter,
	FanoutRaceAfter,
	Quorum
};

template <ScalarPattern pattern>
Future<Void> runScalar(benchmark::State* state) {
	Promise<Void> pending;
	Future<Void> never = pending.getFuture();
	Future<Void> ready = Void();
	std::vector<Promise<Void>> promises;
	std::vector<Future<Void>> inputs;
	if constexpr (pattern == ScalarPattern::Quorum) {
		promises.resize(3);
		inputs.resize(3);
	}

	// Destruction, including cancellation of unresolved actors, is part of each operation.
	for (auto _ : *state) {
		if constexpr (pattern == ScalarPattern::EmptyUncancellable) {
			Future<Void> f = emptyUncancellable();
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::ReadyFuture) {
			Future<Void> f = readyFuture();
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::WaitUncancellableReady) {
			Future<Void> f = waitUncancellable(ready);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::WaitReady) {
			Future<Void> f = waitOne(ready);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::WaitCancel) {
			Future<Void> f = waitOne(never);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::RaceBothReady) {
			Future<Void> f = waitEither(ready, ready);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::RaceFirstReady) {
			Future<Void> f = waitEither(ready, never);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::RaceSecondReady) {
			Future<Void> f = waitEither(never, ready);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::RaceCancel) {
			Future<Void> f = waitEither(never, never);
			benchmark::DoNotOptimize(f);
		} else if constexpr (pattern == ScalarPattern::Quorum) {
			promises.clear();
			promises.resize(3);
			for (int i = 0; i < 3; ++i) {
				inputs[i] = promises[i].getFuture();
			}
			Future<Void> f = quorum(inputs, 2);
			for (auto& promise : promises) {
				promise.send(Void());
			}
			benchmark::DoNotOptimize(f);
		} else {
			Promise<Void> signal;
			if constexpr (pattern == ScalarPattern::WaitAfter) {
				Future<Void> f = waitOne(signal.getFuture());
				signal.send(Void());
				benchmark::DoNotOptimize(f);
			} else if constexpr (pattern == ScalarPattern::RaceAfter) {
				Future<Void> f = waitEither(signal.getFuture(), never);
				signal.send(Void());
				benchmark::DoNotOptimize(f);
			} else if constexpr (pattern == ScalarPattern::WaitRaceAfter) {
				Future<Void> f = waitOne(waitEither(signal.getFuture(), never));
				signal.send(Void());
				benchmark::DoNotOptimize(f);
			} else if constexpr (pattern == ScalarPattern::FanoutRaceAfter) {
				Future<Void> f = waitEither(signal.getFuture(), never);
				Future<Void> first = waitOne(f);
				Future<Void> second = waitOne(f);
				signal.send(Void());
				benchmark::DoNotOptimize(first);
				benchmark::DoNotOptimize(second);
			}
		}
		benchmark::ClobberMemory();
	}
	ASSERT_EQ(pending.getFutureReferenceCount(), 1);
	state->SetItemsProcessed(state->iterations());
	co_return;
}

template <ScalarPattern pattern>
void benchScalar(benchmark::State& state) {
	onMainThread([&state] { return runScalar<pattern>(&state); }).getBlocking();
}

enum class BatchPattern {
	WaitFifo,
	WaitLifo,
	Race,
	RaceSameInput,
	NestedRace,
	WaitRace,
	FanoutRace,
	FanoutWaitRace,
	FanoutSnapshotRace
};

template <BatchPattern pattern>
Future<Void> runBatch(benchmark::State* state) {
	const int count = state->range(0);
	constexpr bool fanout = pattern == BatchPattern::FanoutRace || pattern == BatchPattern::FanoutWaitRace ||
	                        pattern == BatchPattern::FanoutSnapshotRace;
	Promise<Void> pending;
	Future<Void> never = pending.getFuture();
	for (auto _ : *state) {
		state->PauseTiming();
		{
			std::vector<Promise<Void>> signals(count);
			std::vector<Future<Void>> first(count);
			std::vector<Future<Void>> second(fanout ? count : 0);
			// Measure construction and completion with all frames live together; exclude input setup and final cleanup.
			state->ResumeTiming();
			for (int i = 0; i < count; ++i) {
				if constexpr (pattern == BatchPattern::WaitFifo || pattern == BatchPattern::WaitLifo) {
					first[i] = waitOne(signals[i].getFuture());
				} else if constexpr (pattern == BatchPattern::Race) {
					first[i] = waitEither(signals[i].getFuture(), never);
				} else if constexpr (pattern == BatchPattern::RaceSameInput) {
					first[i] = waitEither(signals[i].getFuture(), signals[i].getFuture());
				} else if constexpr (pattern == BatchPattern::NestedRace) {
					first[i] = waitEither(waitEither(signals[i].getFuture(), never), never);
				} else if constexpr (pattern == BatchPattern::WaitRace) {
					first[i] = waitOne(waitEither(signals[i].getFuture(), never));
				} else if constexpr (pattern == BatchPattern::FanoutRace) {
					Future<Void> f = waitEither(signals[i].getFuture(), never);
					first[i] = waitOne(f);
					second[i] = waitOne(f);
				} else if constexpr (pattern == BatchPattern::FanoutWaitRace) {
					Future<Void> f = waitEither(waitOne(signals[i].getFuture()), never);
					first[i] = waitOne(f);
					second[i] = waitOne(f);
				} else if constexpr (pattern == BatchPattern::FanoutSnapshotRace) {
					g_input = signals[i].getFuture();
					Future<Void> f = waitEither(waitSnapshot(), never);
					g_input = f;
					first[i] = waitSnapshot();
					second[i] = waitSnapshot();
				}
			}
			benchmark::DoNotOptimize(first.data());
			benchmark::DoNotOptimize(second.data());
			for (int i = 0; i < count; ++i) {
				const int index = pattern == BatchPattern::WaitLifo ? count - 1 - i : i;
				signals[index].send(Void());
			}
			benchmark::ClobberMemory();
			state->PauseTiming();
			for (auto const& f : first) {
				ASSERT(f.isReady() && !f.isError());
			}
			for (auto const& f : second) {
				ASSERT(f.isReady() && !f.isError());
			}
			if constexpr (pattern == BatchPattern::FanoutSnapshotRace) {
				g_input = Future<Void>();
			}
		}
		ASSERT_EQ(pending.getFutureReferenceCount(), 1);
		state->ResumeTiming();
	}
	state->SetItemsProcessed(state->iterations() * count);
	co_return;
}

template <BatchPattern pattern>
void benchBatch(benchmark::State& state) {
	onMainThread([&state] { return runBatch<pattern>(&state); }).getBlocking();
}

Future<Void> runStreamSum(benchmark::State* state) {
	const int count = state->range(0);
	for (auto _ : *state) {
		PromiseStream<int> stream;
		Future<int> sum = sumStream(stream.getFuture());
		for (int i = 0; i < count; ++i) {
			stream.send(1);
		}
		stream.sendError(end_of_stream());
		benchmark::DoNotOptimize(sum);
		ASSERT_EQ(sum.get(), count);
	}
	state->SetItemsProcessed(state->iterations() * count);
	co_return;
}

void benchStreamSum(benchmark::State& state) {
	onMainThread([&state] { return runStreamSum(&state); }).getBlocking();
}

BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::EmptyUncancellable)
    ->Name("actor_patterns/empty_uncancellable")
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::ReadyFuture)->Name("actor_patterns/ready_future")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::WaitUncancellableReady)
    ->Name("actor_patterns/wait_uncancellable_ready")
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::WaitReady)->Name("actor_patterns/wait_ready")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::WaitCancel)->Name("actor_patterns/wait_construct_cancel")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::WaitAfter)->Name("actor_patterns/wait_after")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::RaceBothReady)->Name("actor_patterns/race_both_ready")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::RaceFirstReady)->Name("actor_patterns/race_first_ready")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::RaceSecondReady)
    ->Name("actor_patterns/race_second_ready")
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::RaceCancel)->Name("actor_patterns/race_construct_cancel")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::RaceAfter)->Name("actor_patterns/race_after")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::WaitRaceAfter)->Name("actor_patterns/wait_race_after")->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::FanoutRaceAfter)
    ->Name("actor_patterns/fanout_race_after")
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchScalar, ScalarPattern::Quorum)->Name("actor_patterns/quorum_2_of_3")->UseRealTime();

BENCHMARK_TEMPLATE(benchBatch, BatchPattern::WaitFifo)
    ->Name("actor_patterns/batch_wait_fifo")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::WaitLifo)
    ->Name("actor_patterns/batch_wait_lifo")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::Race)
    ->Name("actor_patterns/batch_race")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::RaceSameInput)
    ->Name("actor_patterns/batch_race_same_input")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::NestedRace)
    ->Name("actor_patterns/batch_nested_race")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::WaitRace)
    ->Name("actor_patterns/batch_wait_race")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::FanoutRace)
    ->Name("actor_patterns/batch_fanout_race")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::FanoutWaitRace)
    ->Name("actor_patterns/batch_fanout_wait_race")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchBatch, BatchPattern::FanoutSnapshotRace)
    ->Name("actor_patterns/batch_fanout_snapshot_race")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();
BENCHMARK(benchStreamSum)
    ->Name("actor_patterns/stream_sum")
    ->Arg(64)
    ->Arg(4096)
    ->Arg(65536)
    ->Arg(1000000)
    ->UseRealTime();

} // namespace
