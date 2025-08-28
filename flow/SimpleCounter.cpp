/*
 * SimpleCounter.cpp
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

#include <thread>

#include "flow/SimpleCounter.h"
#include "flow/UnitTest.h"

// Trace.cpp::validateField insists on applying some rules to trace
// field names.  Instead of fighting this, for now just make our
// hierarchical names comply by converting / to 'U'.  Yes, 'U'.
//
// If this annoys you, there are several options:
// 1) Don't use hierarchical metric names.
// 2) Go figure out why Trace.cpp has this restriction (it itself offers
// no explanation whatsoever), and consider relaxing it. HOWEVER: consider that
// Prometheus imposes very strict rules on metric names.  The most useful thing one
// can do with Prometheus-compatible metric names and still retain some manner
// of hierarchical naming is to use underscore as the component separator.
// Longer term, if we are emitting Prometheus-compatible metrics, then replacing
// '/' with '_' could be a strategy.  However, we would want to ensure that we don't end
// up with random naming collisions from people who might use '_' for its normal purpose of
// separating any old words anywhere, including within the same path component.
// More background: https://chatgpt.com/share/68ad3e33-00b0-800b-9fa9-644ef201feb9
//
// So basically, Trace.cpp rules suck, and Prometheus rules suck, but we should be
// aware of them and play ball.
static std::string mungeName(const std::string input) {
	std::string output;
	for (char ch : input) {
		if (ch == '/' || ch == ':') {
			output += "U";
		} else {
			output += ch;
		}
	}
	return output;
}

// This should be called periodically by higher level code somewhere.
void simpleCounterReport(void) {
	static SimpleCounter<int64_t>* reportCount = SimpleCounter<int64_t>::makeCounter("/flow/counters/reports");
	reportCount->increment(1);

	std::vector<SimpleCounter<int64_t>*> intCounters = SimpleCounter<int64_t>::getCounters();
	std::vector<SimpleCounter<double>*> doubleCounters = SimpleCounter<double>::getCounters();
	auto traceEvent = TraceEvent("SimpleCounters");
	for (SimpleCounter<int64_t>* ic : intCounters) {
		std::string n = ic->name();
		n = mungeName(n);
		ASSERT(validateField(n.c_str(), /* allowUnderscores= */ true));
		traceEvent.detail(std::move(n), ic->get());
	}
	for (SimpleCounter<double>* dc : doubleCounters) {
		std::string n = dc->name();
		n = mungeName(n);
		ASSERT(validateField(n.c_str(), /* allowUnderscores= */ true));
		traceEvent.detail(std::move(n), dc->get());
	}
	int total = intCounters.size() + doubleCounters.size();
	traceEvent.detail("SimpleCountersTotalCounters", total);
}

TEST_CASE("/flow/simplecounter/int64") {
	SimpleCounter<int64_t>* foo = SimpleCounter<int64_t>::makeCounter("/flow/counters/foo");
	SimpleCounter<int64_t>* bar = SimpleCounter<int64_t>::makeCounter("/flow/counters/bar");

	foo->increment(5);
	foo->increment(1);

	bar->increment(10);

	ASSERT(foo->get() == 6);
	ASSERT(bar->get() == 10);

	for (int i = 0; i < 100; i++) {
		SimpleCounter<int64_t>* p =
		    SimpleCounter<int64_t>::makeCounter(std::string("/flow/counters/many") + std::to_string(i));
		p->increment(i);
		ASSERT(p->get() == i);
	}

	SimpleCounter<int64_t>* conflict = SimpleCounter<int64_t>::makeCounter("/flow/counters/lots");

	// Increment by all values in [1, 1000000] across 10 threads.
	// Expected sum: 10 * (min + max) * (num entries in series)/2
	// ==>  10 * ( 1 + 1M ) * 500K
	int64_t expectedSum = int64_t{ 10 } * int64_t{ 1'000'000 + 1 } * uint64_t{ 500'000 };

	auto inclots = [conflict]() {
		for (int i = 1; i <= 1'000'000; i++) {
			conflict->increment(i);
		}
	};

	std::vector<std::thread> threads;
	for (int i = 0; i < 10; i++) {
		threads.emplace_back(inclots);
	}
	for (int i = 0; i < 10; i++) {
		threads[i].join();
	}

	ASSERT(conflict->get() == expectedSum);

	std::vector<SimpleCounter<int64_t>*> intCounters = SimpleCounter<int64_t>::getCounters();

	// NOTE: the following is written as >= 103 and not == 103 because
	// "unit tests" actually run in fdbserver, so any background
	// logic, like for example simpleCounterReport() being called
	// above from fdbserver.actor.cpp, will affect the execution
	// environment.
	ASSERT(intCounters.size() >= 103);

	// Give asserts here a chance to run.
	simpleCounterReport();

	return Void();
}

TEST_CASE("/flow/simplecounter/double") {
	SimpleCounter<double>* baz = SimpleCounter<double>::makeCounter("/flow/counters/baz");

	// We intend to compute a floating point sum with an exact representation.
	// A way to do this is to only add values with exact representations.
	// Integers and powers of two (within the limits of the number of mantissa
	// and exponent bits) do have exact representations.  Hence the 0.5 increment.
	double expectedSum = 10.0 * (0.5 + 1'000'000.0) * 1'000'000.0;

	auto double_inclots = [baz]() {
		for (double i = 0.5; i <= 1'000'000; i += 0.5) {
			baz->increment(i);
		}
	};

	std::vector<std::thread> threads;
	for (int i = 0; i < 10; i++) {
		threads.emplace_back(double_inclots);
	}
	for (int i = 0; i < 10; i++) {
		threads[i].join();
	}

	ASSERT(baz->get() == expectedSum);

	std::vector<SimpleCounter<double>*> doubleCounters = SimpleCounter<double>::getCounters();
	ASSERT(doubleCounters.size() >= 1);

	// Give asserts here a chance to run.
	simpleCounterReport();

	return Void();
}

void forceLinkSimpleCounterTests() {}
