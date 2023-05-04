/*
 * RandomKeyValueUtils.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/RandomKeyValueUtils.h"
#include "flow/UnitTest.h"

template <typename T>
void printNextN(T generator, int count = 10) {
	fmt::print("Generating from .next() on {}\n", generator.toString());
	for (int i = 0; i < count; ++i) {
		fmt::print("  {}\n", generator.next());
	}
	fmt::print("\n");
}

TEST_CASE("/randomKeyValueUtils/generate") {

	printNextN(RandomIntGenerator(3, 10, false), 5);
	printNextN(RandomIntGenerator("3..10"), 5);
	printNextN(RandomIntGenerator("a..z"), 5);
	// Works in reverse too
	printNextN(RandomIntGenerator("10..3"), 5);
	// Skewed low
	printNextN(RandomIntGenerator("^3..10"), 5);
	// Skewed high
	printNextN(RandomIntGenerator("^10..3"), 5);
	printNextN(RandomIntGenerator("5"), 5);

	printNextN(RandomStringGenerator(RandomIntGenerator(3, 10, false), RandomIntGenerator('d', 'g', false)), 10);
	printNextN(RandomStringGenerator("3..10", "d..g"), 10);
	printNextN(RandomStringGenerator("3..10/d..g"), 10);
	printNextN(RandomStringGenerator("5/a..c"), 5);
	printNextN(RandomStringGenerator("5/a..a"), 5);

	printNextN(RandomKeySetGenerator("0..5", "3..10/d..g"), 20);
	// Index generator will use a min of 0 so this is the same as 0:5
	printNextN(RandomKeySetGenerator("5", "3..10/d..g"), 20);

	std::vector<RandomKeySetGenerator> tupleParts{
		RandomKeySetGenerator(RandomIntGenerator(5),
		                      RandomStringGenerator(RandomIntGenerator(5), RandomIntGenerator('a', 'c', false))),
		RandomKeySetGenerator(
		    RandomIntGenerator(5),
		    RandomStringGenerator(RandomIntGenerator(3, 10, true), RandomIntGenerator('d', 'f', false)))
	};

	printNextN(RandomKeyTupleSetGenerator(RandomIntGenerator(10), RandomKeyTupleGenerator(tupleParts)), 10);

	// Same as above in string form
	printNextN(RandomKeyTupleSetGenerator("10::5::5/a..c,5::^3..10/d..f"), 10);

	// uniform random selection from 1000 pregenerated key tuples.  Tuples have 4 parts
	//    len 5 chars a-d with 2 choices
	//    len 10 chars k-t with 10000 choices
	//    len 5-8  chars z-z with 2 choices
	printNextN(RandomKeyTupleSetGenerator("1000::2::5/a..d,10000::10/k..t,2::5..8/z"), 100);

	printNextN(RandomValueGenerator("10..100/r..z"), 20);

	return Void();
}

void forceLinkRandomKeyValueUtilsTests() {}
