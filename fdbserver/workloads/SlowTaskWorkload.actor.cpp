/*
 * SlowTaskWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "workloads.h"
#include "flow/SignalSafeUnwind.h"

// Stress test the slow task profiler or flow profiler
struct SlowTaskWorkload : TestWorkload {

	SlowTaskWorkload(WorkloadContext const& wcx)
	: TestWorkload(wcx) {
	}

	virtual std::string description() {
		return "SlowTaskWorkload";
	}

	virtual Future<Void> start(Database const& cx) {
		return go();
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	ACTOR static Future<Void> go() {
		Void _ = wait( delay(1) );
		int64_t phc = dl_iterate_phdr_calls;
		int64_t exc = 0;
		fprintf(stderr, "Slow task starting\n");
		for(int i=0; i<10; i++) {
			fprintf(stderr, "  %d\n", i);
			double end = timer() + 1;
			while( timer() < end ) {
				do_slow_exception_thing(&exc);
			}
		}
		fprintf(stderr, "Slow task complete: %lld exceptions; %lld calls to dl_iterate_phdr\n", exc, dl_iterate_phdr_calls - phc);
		return Void();
	}

	static void do_slow_exception_thing(int64_t* exc_count) {
		// Has to be a non-actor function so that actual exception unwinding occurs
		for(int j=0; j<1000; j++)
			try {
				throw success();
			} catch (Error& e) {
				++*exc_count;
			}
	}
};

WorkloadFactory<SlowTaskWorkload> SlowTaskWorkloadFactory("SlowTaskWorkload");
