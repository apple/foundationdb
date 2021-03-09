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

#include <cinttypes>

#include "fdbserver/CoroFlow.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/SignalSafeUnwind.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// Stress test the slow task profiler or flow profiler
struct SlowTaskWorkload : TestWorkload {

	Reference<IThreadPool> coroPool = CoroThreadPool::createThreadPool();

	struct CoroWorker : IThreadPoolReceiver {
		void init() override {}
		struct NoopAction : TypedAction<CoroWorker, NoopAction>, FastAllocated<NoopAction> {
			ThreadReturnPromise<Void> result;
			double getTimeEstimate() const override { return 0; }
		};
		void action(NoopAction& rv) {
			breakpoint_me();
			rv.result.send(Void());
		}
	};

	SlowTaskWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) { coroPool->addThread(new CoroWorker); }

	std::string description() const override { return "SlowTaskWorkload"; }

	Future<Void> start(Database const& cx) override {
		setupRunLoopProfiler();
		return go(this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> go(SlowTaskWorkload* self) {
		wait( delay(1) );

		int64_t phc = dl_iterate_phdr_calls;
		state int64_t startProfilesDeferred = getNumProfilesDeferred();
		state int64_t startProfilesOverflowed = getNumProfilesOverflowed();
		state int64_t startProfilesCaptured = getNumProfilesCaptured();
		int64_t exc = 0;
		fprintf(stderr, "Slow task starting\n");
		for (int i = 0; i < 10; i++) {
			fprintf(stderr, "  %d\n", i);
			double end = timer() + 1;
			while( timer() < end ) {
				do_slow_exception_thing(&exc);
			}
		}
		fprintf(stderr, "Slow task complete: %" PRId64 " exceptions; %" PRId64 " calls to dl_iterate_phdr, %" PRId64 " profiles deferred, %" PRId64 " profiles overflowed, %" PRId64 " profiles captured\n", 
		        exc, dl_iterate_phdr_calls - phc, 
		        getNumProfilesDeferred() - startProfilesDeferred, 
		        getNumProfilesOverflowed() - startProfilesOverflowed,
		        getNumProfilesCaptured() - startProfilesCaptured);

		startProfilesDeferred = getNumProfilesDeferred();
		startProfilesOverflowed = getNumProfilesOverflowed();
		startProfilesCaptured = getNumProfilesCaptured();
		state int i;
		state int j;
		state pthread_t mainThread = pthread_self();
		for (j = 0; j < 1000; ++j) {
			// Try to deliver SIGPROF somewhere in the coro switching call stack
			state std::thread t = std::thread{ [mainThread = mainThread]() { pthread_kill(mainThread, SIGPROF); } };
			try {
				for (i = 0; i < 1000; ++i) {
					wait(exercise_coro(self));
				}
				t.join();
			} catch (...) {
				t.join();
				throw;
			}
		}
		fprintf(stderr,
		        "Coro switch test complete: %" PRId64 " profiles deferred, %" PRId64 " profiles overflowed, %" PRId64
		        " profiles captured\n",
		        getNumProfilesDeferred() - startProfilesDeferred, getNumProfilesOverflowed() - startProfilesOverflowed,
		        getNumProfilesCaptured() - startProfilesCaptured);

		wait(self->coroPool->stop());

		return Void();
	}

	static void do_slow_exception_thing(int64_t* exc_count) {
		// Has to be a non-actor function so that actual exception unwinding occurs
		for(int j=0; j<1000; j++)
			try {
				throw success();
			} catch (Error& ) {
				++*exc_count;
			}
	}

	ACTOR static Future<Void> exercise_coro(SlowTaskWorkload* self) {
		auto* p = new CoroWorker::NoopAction;
		auto f = p->result.getFuture();
		self->coroPool->post(p);
		wait(f);
		return Void();
	}
};

WorkloadFactory<SlowTaskWorkload> SlowTaskWorkloadFactory("SlowTaskWorkload");
