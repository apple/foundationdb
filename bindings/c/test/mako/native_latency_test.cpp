/*
 * native_latency_test.cpp
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

#include "native_latency.hpp"

#include <cstdio>
#include <cstdlib>
#include <initializer_list>
#include <limits>
#include <new>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace mako;

void check(bool condition, const char* message) {
	if (!condition) {
		std::fprintf(stderr, "%s\n", message);
		std::abort();
	}
}

int main() {
	NativeLatencyHistogram edges;
	NativeLatencySnapshot empty;
	check(!empty.percentile(0, 0.999), "empty histogram must not report a percentile");
	for (const auto value : { uint64_t{ 0 },
	                          uint64_t{ 1 },
	                          uint64_t{ 2 },
	                          uint64_t{ 1 } << 31,
	                          uint64_t{ 1 } << 32,
	                          std::numeric_limits<uint64_t>::max() }) {
		edges.add(0, value);
	}
	NativeLatencySnapshot edge_snapshot;
	edge_snapshot.merge(edges);
	check(edge_snapshot.samples(0) == 6, "all bounded histogram samples must be counted");
	check(*edge_snapshot.percentile(0, 0) == 0, "zero latency must retain its own bucket");
	check(*edge_snapshot.percentile(0, 1) >= (uint64_t{ 1 } << 31), "large latency must be bounded safely");

	const auto size = 2 * sizeof(NativeLatencyHistogram);
	auto* shared = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	check(shared != MAP_FAILED, "could not map shared histograms");
	auto* histograms = static_cast<NativeLatencyHistogram*>(shared);
	new (&histograms[0]) NativeLatencyHistogram();
	new (&histograms[1]) NativeLatencyHistogram();
	check(reinterpret_cast<uintptr_t>(&histograms[1]) % alignof(NativeLatencyHistogram) == 0,
	      "adjacent worker histograms must stay aligned");

	const auto child = fork();
	check(child >= 0, "could not fork histogram writer");
	if (child == 0) {
		for (int i = 0; i < 3000; ++i) {
			histograms[1].add(0, 100);
			histograms[1].add(1, 10000);
		}
		_exit(0);
	}

	auto previous = uint64_t{};
	for (int i = 0; i < 10000; ++i) {
		NativeLatencySnapshot current;
		current.merge(histograms[1]);
		check(current.samples(0) >= previous, "cross-process sample counts must not decrease");
		previous = current.samples(0);
		if (previous == 3000) {
			break;
		}
		usleep(100);
	}
	int status = 0;
	check(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
	      "child writer must finish successfully");
	NativeLatencySnapshot combined;
	combined.merge(histograms[0]);
	combined.merge(histograms[1]);
	check(combined.samples(0) == 3000 && combined.samples(1) == 3000, "adjacent worker histograms must not overlap");
	for (const auto quantile : { 0.5, 0.9, 0.99, 0.999 }) {
		const auto get = *combined.percentile(0, quantile);
		const auto commit = *combined.percentile(1, quantile);
		check(get >= 99 && get <= 101, "GET latency percentile outside histogram accuracy");
		check(commit >= 9900 && commit <= 10100, "COMMIT latency percentile outside histogram accuracy");
	}
	check(munmap(shared, size) == 0, "could not unmap shared histograms");
}
