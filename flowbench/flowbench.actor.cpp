/*
 * flowbench.actor.cpp
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
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/ThreadHelper.actor.h"
#include <thread>

ACTOR template <class T>
Future<T> stopNetworkAfter(Future<T> what) {
	try {
		T t = wait(what);
		g_network->stop();
		return t;
	} catch (...) {
		g_network->stop();
		throw;
	}
}

int main(int argc, char** argv) {
	benchmark::Initialize(&argc, argv);
	if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
		return 1;
	}
	setupNetwork();
	Promise<Void> benchmarksDone;
	std::thread benchmarkThread([&]() {
		benchmark::RunSpecifiedBenchmarks();
		onMainThreadVoid([&]() { benchmarksDone.send(Void()); }, nullptr);
	});
	auto f = stopNetworkAfter(benchmarksDone.getFuture());
	runNetwork();
	benchmarkThread.join();
}
