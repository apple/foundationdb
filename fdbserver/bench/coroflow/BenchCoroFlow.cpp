/*
 * BenchCoroFlow.cpp
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

#include "fdbserver/CoroFlow.h"
#include "flow/ThreadHelper.h"

namespace {

enum class WaitKind { Ready, Suspended };

class BenchReceiver final : public IThreadPoolReceiver {
public:
	Future<Void> onStarted() const { return started.getFuture(); }
	void init() override { started.send(Void()); }

private:
	ThreadReturnPromise<Void> started;
};

template <WaitKind kind>
class WaitAction final : public ThreadAction {
public:
	explicit WaitAction(benchmark::State* state) : state(state) {}

	Future<Void> getFuture() const { return completed.getFuture(); }

	void operator()(IThreadPoolReceiver*) override {
		Optional<Error> err;
		try {
			Future<Void> ready = Void();
			while (state->KeepRunning()) {
				if constexpr (kind == WaitKind::Ready) {
					CoroThreadPool::waitFor(ready);
					benchmark::DoNotOptimize(ready);
				} else {
					Future<Void> pending = delay(0, g_network->getCurrentTask());
					ASSERT(!pending.isReady());
					CoroThreadPool::waitFor(pending);
					benchmark::DoNotOptimize(pending);
				}
			}
			state->SetItemsProcessed(state->iterations());
		} catch (Error& e) {
			err = e;
		} catch (...) {
			err = unknown_error();
		}

		// Defer delivery until control is back on the network stack.
		if (err.present()) {
			completed.sendError(err.get());
		} else {
			completed.send(Void());
		}
		delete this;
	}

	void cancel() override {
		completed.sendError(actor_cancelled());
		delete this;
	}

	double getTimeEstimate() const override { return 0; }

private:
	benchmark::State* state;
	ThreadReturnPromise<Void> completed;
};

Future<Void> stopPoolAfter(Reference<IThreadPool> pool, Future<Void> done) {
	Optional<Error> err;
	try {
		co_await done;
	} catch (Error& e) {
		err = e;
	}
	co_await pool->stop();
	if (err.present()) {
		throw err.get();
	}
}

template <WaitKind kind>
Future<Void> runWaitAction(Reference<IThreadPool> pool, Future<Void> started, benchmark::State* state) {
	co_await started;
	auto* action = new WaitAction<kind>(state);
	Future<Void> completed = action->getFuture();
	pool->post(action);
	co_await completed;
}

template <WaitKind kind>
Future<Void> runWaitBenchmark(benchmark::State* state) {
	Reference<IThreadPool> pool = CoroThreadPool::createThreadPool();
	auto* receiver = new BenchReceiver();
	Future<Void> started = receiver->onStarted();
	pool->addThread(receiver);
	co_await stopPoolAfter(pool, runWaitAction<kind>(pool, started, state));
}

Future<Void> runStartStopBenchmark(benchmark::State* state) {
	while (state->KeepRunning()) {
		Reference<IThreadPool> pool = CoroThreadPool::createThreadPool();
		auto* receiver = new BenchReceiver();
		Future<Void> started = receiver->onStarted();
		pool->addThread(receiver);
		co_await stopPoolAfter(pool, started);
	}
	state->SetItemsProcessed(state->iterations());
}

Future<Void> runZeroWorkerCreateDropBenchmark(benchmark::State* state) {
	while (state->KeepRunning()) {
		Reference<IThreadPool> pool = CoroThreadPool::createThreadPool();
		benchmark::DoNotOptimize(pool.getPtr());
		pool.clear();
	}
	state->SetItemsProcessed(state->iterations());
	return Void();
}

// No owning pool reference may escape setup into the final-reference-drop measurement.
Future<Void> startBenchWorker(IThreadPool* pool) {
	auto* receiver = new BenchReceiver();
	Future<Void> started = receiver->onStarted();
	pool->addThread(receiver);
	co_await started;
}

enum class LifecyclePhase { ExplicitStop, FinalReferenceDrop };

template <LifecyclePhase phase>
Future<Void> runLifecyclePhaseBenchmark(benchmark::State* state) {
	while (state->KeepRunning()) {
		state->PauseTiming();
		Reference<IThreadPool> pool;
		Optional<Error> err;
		bool stopped = false;
		bool timing = false;
		try {
			pool = CoroThreadPool::createThreadPool();
			co_await startBenchWorker(pool.getPtr());
			if constexpr (phase == LifecyclePhase::FinalReferenceDrop) {
				co_await pool->stop();
				stopped = true;
			}

			state->ResumeTiming();
			timing = true;
			if constexpr (phase == LifecyclePhase::ExplicitStop) {
				co_await pool->stop();
				stopped = true;
			} else {
				pool.clear();
			}
			state->PauseTiming();
			timing = false;
		} catch (Error& e) {
			err = e;
		} catch (...) {
			err = unknown_error();
		}

		if (timing) {
			state->PauseTiming();
		}
		if (pool.isValid() && !stopped) {
			try {
				co_await pool->stop();
			} catch (Error& e) {
				if (!err.present()) {
					err = e;
				}
			}
		}
		pool.clear();
		state->ResumeTiming();
		if (err.present()) {
			throw err.get();
		}
	}
	state->SetItemsProcessed(state->iterations());
}

template <WaitKind kind>
void benchWait(benchmark::State& state) {
	onMainThread([&state] { return runWaitBenchmark<kind>(&state); }).getBlocking();
}

void benchStartStop(benchmark::State& state) {
	onMainThread([&state] { return runStartStopBenchmark(&state); }).getBlocking();
}

void benchZeroWorkerCreateDrop(benchmark::State& state) {
	onMainThread([&state] { return runZeroWorkerCreateDropBenchmark(&state); }).getBlocking();
}

template <LifecyclePhase phase>
void benchLifecyclePhase(benchmark::State& state) {
	onMainThread([&state] { return runLifecyclePhaseBenchmark<phase>(&state); }).getBlocking();
}

BENCHMARK_TEMPLATE(benchWait, WaitKind::Ready)->Name("coroflow_wait_ready")->UseRealTime();
BENCHMARK_TEMPLATE(benchWait, WaitKind::Suspended)->Name("coroflow_wait_suspended")->UseRealTime();
BENCHMARK(benchStartStop)->Name("coroflow_start_stop")->UseRealTime();
BENCHMARK(benchZeroWorkerCreateDrop)->Name("coroflow_zero_worker_create_drop")->UseRealTime();
// These rows exclude worker startup, so time-based calibration can spend excessive wall time on setup.
constexpr int lifecyclePhaseIterations = 16384;
BENCHMARK_TEMPLATE(benchLifecyclePhase, LifecyclePhase::ExplicitStop)
    ->Name("coroflow_explicit_stop_only")
    ->Iterations(lifecyclePhaseIterations)
    ->UseRealTime();
BENCHMARK_TEMPLATE(benchLifecyclePhase, LifecyclePhase::FinalReferenceDrop)
    ->Name("coroflow_final_reference_drop_only")
    ->Iterations(lifecyclePhaseIterations)
    ->UseRealTime();

} // namespace
