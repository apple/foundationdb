/*
 * SimpleCounter.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "flow/SimpleCounter.h"
#include "flow/UnitTest.h"

TEST_CASE("/flow/simplecounter/int64") {
	SimpleCounter<int64_t>* foo = SimpleCounter<int64_t>::makeCounter("foo");
	SimpleCounter<int64_t>* bar = SimpleCounter<int64_t>::makeCounter("bar");

	foo->increment(5);
	foo->increment(1);

	for (int i = 0; i < 100; i++) {
		SimpleCounter<int64_t>* p = SimpleCounter<int64_t>::makeCounter(std::string("many") + std::to_string(i));
		p->increment(i);
	}

	bar->increment(10);

	SimpleCounter<int64_t>* conflict = SimpleCounter<int64_t>::makeCounter("lots_of_increments");

	auto int_inclots = [conflict]() {
		for (int i = 1; i <= 1'000'000; i++) {
			conflict->increment(i);
		}
	};

	std::vector<std::thread> threads;

	for (int i = 0; i < 10; i++) {
		threads.emplace_back(int_inclots);
	}

	for (int i = 0; i < 10; i++) {
		threads[i].join();
	}

	int64_t int_expected_count = int64_t{ 10 } * int64_t{ 1'000'000 + 1 } * uint64_t{ 500'000 };
	ASSERT(conflict->get() == int_expected_count);

	std::vector<SimpleCounter<int64_t>*> intCounters = SimpleCounter<int64_t>::getCounters();
	ASSERT(intCounters.size() == 102);

	return Void();
}

TEST_CASE("/flow/simplecounter/double") {
	SimpleCounter<double>* baz = SimpleCounter<double>::makeCounter("baz");

	// We intend to compute a floating point sum with an exact representation.
	// A way to do this is to only add values with exact representations.
	// Integers and powers of two (within the limits of the number of mantissa
	// and exponent bits) do have exact representations.  Hence the 0.5 increment.
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

	double double_expected_count = 10.0 * (1'000'000 + 0.5) * 1'000'000.0;
	// printf("double_expected_count: %.10f; baz: %.10f\n", double_expected_count, baz->get());
	ASSERT(double_expected_count == baz->get());
	// double ratio = baz->get()/double_expected_count;
	// ASSERT(ratio >= 0.999 && ratio < 1.001);

	std::vector<SimpleCounter<double>*> doubleCounters = SimpleCounter<double>::getCounters();
	ASSERT(doubleCounters.size() == 1);

	return Void();
}
