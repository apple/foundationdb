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

// Convert hierarchical metric names into Prometheus-compatible metric
// names.  Do this by a) removing initial '/' chars, and b) converting
// remaining '/' chars to '_' chars.
static std::string hierarchicalToPrometheus(const std::string input) {
	std::string output;
	for (char ch : input) {
		if (ch == '/') {
			if (output.size() > 0) {
				output += '_';
			}
		} else {
			output += ch;
		}
	}
	return output;
}

// ChatGPT generated code to return true iff `name` is a valid Prometheus
// metric name. Below we call this on trace event fields for the reason
// that we are contemplating converting fields in trace events to metrics
// in downstream Prometheus-compatible metrics systems.
static bool isValidPrometheusMetricName(std::string_view name) {
	if (name.empty()) {
		return false;
	}

	// First character: [a-zA-Z_:]
	char first = name.front();
	if (!(std::isalpha(static_cast<unsigned char>(first)) || first == '_' || first == ':')) {
		return false;
	}

	// Rest: [a-zA-Z0-9_:]*
	for (size_t i = 1; i < name.size(); ++i) {
		char c = name[i];
		if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == ':')) {
			return false;
		}
	}

	// Reserved prefix check: names starting with "__" are reserved
	if (name.size() >= 2 && name[0] == '_' && name[1] == '_') {
		return false;
	}

	return true;
}

// This should be called periodically by higher level code somewhere.
void simpleCounterReport(Severity severity) {
	static SimpleCounter<int64_t>* reportCount = SimpleCounter<int64_t>::makeCounter("/flow/counters/reports");
	reportCount->increment(1);

	std::vector<SimpleCounter<int64_t>*> intCounters = SimpleCounter<int64_t>::getCounters();
	std::vector<SimpleCounter<double>*> doubleCounters = SimpleCounter<double>::getCounters();

	int i = 0;
	// Avoid trace buffer overflow by assuming average field is O(100) bytes or less.
	int countersPerTrace = FLOW_KNOBS->MAX_TRACE_EVENT_LENGTH / 100;
	while (i < intCounters.size()) {
		auto traceEvent = TraceEvent(severity, "SimpleCounters");
		int c = 0;
		do {
			SimpleCounter<int64_t>* ic = intCounters[i];
			std::string n = ic->name();
			n = hierarchicalToPrometheus(n);
			ASSERT(isValidPrometheusMetricName(n));
			traceEvent.detail(std::move(n), ic->get());
			i++;
			c++;
		} while (i < intCounters.size() && c < countersPerTrace);
	}
	i = 0;
	while (i < doubleCounters.size()) {
		auto traceEvent = TraceEvent(severity, "SimpleCounters");
		int c = 0;
		do {
			SimpleCounter<double>* dc = doubleCounters[i];
			std::string n = dc->name();
			n = hierarchicalToPrometheus(n);
			ASSERT(isValidPrometheusMetricName(n));
			traceEvent.detail(std::move(n), dc->get());
			i++;
			c++;
		} while (i < doubleCounters.size() && c < countersPerTrace);
	}
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
