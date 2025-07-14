/*
 * make_h2o.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fmt/format.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

// Flow solution to https://leetcode.com/problems/building-h2o/
//
// The description of this problem calls for threads running as
// hydrogen and oxygen.  To do this yourself, delete the code from
// this file other than main(), read the problem description above,
// and implement something in the spirit of what is requested.
//
// Caveat: this version was written on like day 2 of programming with
// Flow.  This seems to work but may have bugs, non-idiomatic code, or
// generally do naive stuff.
//
// Notes on this solution: we model these as actors and we start a lot
// of them simultaneously in batches from orchestrate() below.  Bonded
// sets of 2 H's and 1 O are supposed to complete together.  We
// arrange this by having the H's store off a Promise<int>, then block
// on the future.  The O's pass their ID and the H's simply log it.
// An O finishes when it has seen two H's.

static int h_seqno;
static int o_seqno;
std::queue<int> h_queue;
std::map<int, Promise<int>*> wakeup;

ACTOR Future<Void> hydrogen(AsyncTrigger* h_ready_trigger) {
	state int h_id = h_seqno++;
	std::cout << format("Hydrogen %d starting\n", h_id);
	state Promise<int> promise;
	state Future<int> future = promise.getFuture();
	ASSERT(wakeup.find(h_id) == wakeup.end());
	wakeup[h_id] = &promise;
	h_queue.push(h_id);
	h_ready_trigger->trigger();

	loop choose {
		when(int o = wait(future)) {
			std::cout << format("Hydrogen %d bound to Oxygen %d, returning\n", h_id, o);
			return Void();
		}
	}
}

ACTOR Future<Void> oxygen(AsyncTrigger* h_ready_trigger) {
	state int h_bound = 0;
	state int o_id = o_seqno++;
	std::cout << format("Oxygen %d starting\n", o_id);

	loop choose {
		when(wait(h_ready_trigger->onTrigger())) {
			while (h_queue.size() > 0) {
				state int h = h_queue.front();
				h_queue.pop();
				auto it = wakeup.find(h);
				ASSERT(it != wakeup.end());
				std::cout << format("Oxygen %d bound to Hydrogen %d\n", o_id, h);
				wakeup.erase(it);
				it->second->send(o_id);
				h_bound++;
				if (h_bound == 2) {
					std::cout << format("  Oxygen %d returning\n", o_id);
					return Void();
				}
			}
		}
	}
}

ACTOR Future<Void> orchestrate() {
	state int h_threads = 0;
	state int o_threads = 0;
	state int total_h_threads = 0;
	state int total_o_threads = 0;
	state int bigloops = 0;
	state std::vector<Future<Void>> all;
	state AsyncTrigger h_ready_trigger;

	// Use much smaller numbers, like say 10, to debug assertion failures,
	// deadlocks, or other weirdness.
	while (bigloops < 1000) {
		int n = deterministicRandom()->randomInt(0, 3);
		if (n < 2) {
			all.emplace_back(hydrogen(&h_ready_trigger));
			h_threads++;
		} else {
			ASSERT(n < 3);
			all.emplace_back(oxygen(&h_ready_trigger));
			o_threads++;
		}

		// FIXME: this is going to create some stop-and-go dynamics.
		// Probably there is a way to continuously generate new
		// H and O workers subject to some kind of low water mark
		// type of condition.
		if (h_threads + o_threads >= 1000) {
			while (2 * o_threads < h_threads) {
				all.emplace_back(oxygen(&h_ready_trigger));
				o_threads++;
			}
			while (h_threads < 2 * o_threads) {
				all.emplace_back(hydrogen(&h_ready_trigger));
				h_threads++;
			}
			if (h_threads != 2 * o_threads) {
				std::cout << format("WEIRDNESS: h_threads [%d] != 2*o_threads [%d]\n", h_threads, o_threads);
				ASSERT(false);
			}

			std::cout << format(
			    "orchestrate: blocking with %d H threads, %d O threads; %d bigloops\n", h_threads, o_threads, bigloops);

			// Without the following two lines, or if you just call
			// waitForAll(all) without the wait (e.g. due to cargo
			// culting it wrong), typically either you will get a
			// deadlock on the wait() or, without it, wakeup.size()
			// will be non-zero and you'll get WEIRDNESS output.
			h_ready_trigger.trigger();
			wait(waitForAll(all));

			if (wakeup.size() != 0) {
				std::cout << format("WEIRDNESS: wakeup.size() != 0 [%d]\n", wakeup.size());

				for (auto it = wakeup.begin(); it != wakeup.end(); it++) {
					std::cout << format("  %d -> %p\n", it->first, it->second);
				}

				// wait(delay(30.0));
			}

			all.clear();
			total_h_threads += h_threads;
			total_o_threads += o_threads;
			h_threads = 0;
			o_threads = 0;
			// wait(delay(2.0));
			bigloops++;
		}
	}

	return Void();
}

int main(int argc, char** argv) {
	// Cargo-culted from tutorial.actor.cpp.
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	std::vector<Future<Void>> all;

	all.emplace_back(orchestrate());

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
