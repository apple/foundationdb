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

template <WaitKind kind>
void benchWait(benchmark::State& state) {
	onMainThread([&state] { return runWaitBenchmark<kind>(&state); }).getBlocking();
}

void benchStartStop(benchmark::State& state) {
	onMainThread([&state] { return runStartStopBenchmark(&state); }).getBlocking();
}

BENCHMARK_TEMPLATE(benchWait, WaitKind::Ready)->Name("coroflow_wait_ready")->UseRealTime();
BENCHMARK_TEMPLATE(benchWait, WaitKind::Suspended)->Name("coroflow_wait_suspended")->UseRealTime();
BENCHMARK(benchStartStop)->Name("coroflow_start_stop")->UseRealTime();

} // namespace
