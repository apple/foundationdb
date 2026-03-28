/*
 * BenchMain.cpp
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
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include <thread>

Future<Void> stopNetworkAfter(Future<Void> what) {
	try {
		co_await what;
		g_network->stop();
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

	platformInit();
	Error::init();
	g_network = newNet2(TLSConfig());

	Promise<Void> benchmarksDone;
	std::thread benchmarkThread([&]() {
		benchmark::RunSpecifiedBenchmarks();
		onMainThreadVoid([&]() { benchmarksDone.send(Void()); });
	});
	auto f = stopNetworkAfter(benchmarksDone.getFuture());
	g_network->run();
	benchmarkThread.join();
}
