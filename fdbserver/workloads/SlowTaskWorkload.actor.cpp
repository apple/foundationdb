/*
 * SlowTaskWorkload.actor.cpp
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

#include <cinttypes>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/SignalSafeUnwind.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Stress test the slow task profiler or flow profiler
struct SlowTaskWorkload : TestWorkload {

	SlowTaskWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	std::string description() const override { return "SlowTaskWorkload"; }

	Future<Void> start(Database const& cx) override {
		setupRunLoopProfiler();
		return go();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> go() {
		wait(delay(1));
		int64_t phc = dl_iterate_phdr_calls;
		int64_t startProfilesDeferred = getNumProfilesDeferred();
		int64_t startProfilesOverflowed = getNumProfilesOverflowed();
		int64_t startProfilesCaptured = getNumProfilesCaptured();
		int64_t exc = 0;
		fprintf(stderr, "Slow task starting\n");
		for (int i = 0; i < 10; i++) {
			fprintf(stderr, "  %d\n", i);
			double end = timer() + 1;
			while (timer() < end) {
				do_slow_exception_thing(&exc);
			}
		}
		fmt::print(stderr,
		           "Slow task complete: {0} exceptions; {1} calls to dl_iterate_phdr, {2}"
		           " profiles deferred, {3} profiles overflowed, {4} profiles captured\n",
		           exc,
		           dl_iterate_phdr_calls - phc,
		           getNumProfilesDeferred() - startProfilesDeferred,
		           getNumProfilesOverflowed() - startProfilesOverflowed,
		           getNumProfilesCaptured() - startProfilesCaptured);

		return Void();
	}

	static void do_slow_exception_thing(int64_t* exc_count) {
		// Has to be a non-actor function so that actual exception unwinding occurs
		for (int j = 0; j < 1000; j++)
			try {
				throw success();
			} catch (Error&) {
				++*exc_count;
			}
	}
};

WorkloadFactory<SlowTaskWorkload> SlowTaskWorkloadFactory("SlowTaskWorkload");
