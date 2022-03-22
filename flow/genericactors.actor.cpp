/*
 * genericactors.actor.cpp
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

#include "flow/flow.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<bool> allTrue(std::vector<Future<bool>> all) {
	state int i = 0;
	while (i != all.size()) {
		bool r = wait(all[i]);
		if (!r)
			return false;
		i++;
	}
	return true;
}

ACTOR Future<Void> anyTrue(std::vector<Reference<AsyncVar<bool>>> input, Reference<AsyncVar<bool>> output) {
	loop {
		bool oneTrue = false;
		std::vector<Future<Void>> changes;
		for (auto it : input) {
			if (it->get())
				oneTrue = true;
			changes.push_back(it->onChange());
		}
		output->set(oneTrue);
		wait(waitForAny(changes));
	}
}

ACTOR Future<Void> cancelOnly(std::vector<Future<Void>> futures) {
	// We don't do anything with futures except hold them, we never return, but if we are cancelled we (naturally) drop
	// the futures
	wait(Never());
	return Void();
}

ACTOR Future<Void> timeoutWarningCollector(FutureStream<Void> input, double logDelay, const char* context, UID id) {
	state uint64_t counter = 0;
	state Future<Void> end = delay(logDelay);
	loop choose {
		when(waitNext(input)) { counter++; }
		when(wait(end)) {
			if (counter)
				TraceEvent(SevWarn, context, id).detail("LateProcessCount", counter).detail("LoggingDelay", logDelay);
			end = delay(logDelay);
			counter = 0;
		}
	}
}

ACTOR Future<bool> quorumEqualsTrue(std::vector<Future<bool>> futures, int required) {
	state std::vector<Future<Void>> true_futures;
	state std::vector<Future<Void>> false_futures;
	true_futures.reserve(futures.size());
	false_futures.reserve(futures.size());
	for (int i = 0; i < futures.size(); i++) {
		true_futures.push_back(onEqual(futures[i], true));
		false_futures.push_back(onEqual(futures[i], false));
	}

	choose {
		when(wait(quorum(true_futures, required))) { return true; }
		when(wait(quorum(false_futures, futures.size() - required + 1))) { return false; }
	}
}

ACTOR Future<bool> shortCircuitAny(std::vector<Future<bool>> f) {
	std::vector<Future<Void>> sc;
	sc.reserve(f.size());
	for (Future<bool> fut : f) {
		sc.push_back(returnIfTrue(fut));
	}

	choose {
		when(wait(waitForAll(f))) {
			// Handle a possible race condition? If the _last_ term to
			// be evaluated triggers the waitForAll before bubbling
			// out of the returnIfTrue quorum
			for (const auto& fut : f) {
				if (fut.get()) {
					return true;
				}
			}
			return false;
		}
		when(wait(waitForAny(sc))) { return true; }
	}
}

Future<Void> orYield(Future<Void> f) {
	if (f.isReady()) {
		if (f.isError())
			return tagError<Void>(yield(), f.getError());
		else
			return yield();
	} else
		return f;
}

ACTOR Future<Void> returnIfTrue(Future<bool> f) {
	bool b = wait(f);
	if (b) {
		return Void();
	}
	wait(Never());
	throw internal_error();
}

ACTOR Future<Void> lowPriorityDelay(double waitTime) {
	state int loopCount = 0;
	state int totalLoops =
	    std::max<int>(waitTime / FLOW_KNOBS->LOW_PRIORITY_MAX_DELAY, FLOW_KNOBS->LOW_PRIORITY_DELAY_COUNT);

	while (loopCount < totalLoops) {
		wait(delay(waitTime / totalLoops, TaskPriority::Low));
		loopCount++;
	}
	return Void();
}

namespace {

struct DummyState {
	int changed{ 0 };
	int unchanged{ 0 };
	bool operator==(DummyState const& rhs) const { return changed == rhs.changed && unchanged == rhs.unchanged; }
	bool operator!=(DummyState const& rhs) const { return !(*this == rhs); }
};

ACTOR Future<Void> testPublisher(Reference<AsyncVar<DummyState>> input) {
	state int i = 0;
	for (; i < 100; ++i) {
		wait(delay(deterministicRandom()->random01()));
		auto var = input->get();
		++var.changed;
		input->set(var);
	}
	return Void();
}

ACTOR Future<Void> testSubscriber(Reference<IAsyncListener<int>> output, Optional<int> expected) {
	loop {
		wait(output->onChange());
		ASSERT(expected.present());
		if (output->get() == expected.get()) {
			return Void();
		}
	}
}

} // namespace

TEST_CASE("/flow/genericactors/AsyncListener") {
	auto input = makeReference<AsyncVar<DummyState>>();
	state Future<Void> subscriber1 =
	    testSubscriber(IAsyncListener<int>::create(input, [](auto const& var) { return var.changed; }), 100);
	state Future<Void> subscriber2 =
	    testSubscriber(IAsyncListener<int>::create(input, [](auto const& var) { return var.unchanged; }), {});
	wait(subscriber1 && testPublisher(input));
	ASSERT(!subscriber2.isReady());
	return Void();
}
