/*
 * CountedSecitonTest.cpp
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

#include <iostream>
#include "fdbrpc/Stats.h"
#include "flow/UnitTest.h"

void forceLinkCountedSectionTests() {}

struct TestCounters {
	CounterCollection cc;
	Counter start;
	Counter end;

	TestCounters() : cc("CountedSectionTest"), start("start", cc), end("end", cc) {}
};

Future<Void> doNothing(TestCounters* tc, Future<Void> f) {
	CountedSection cs(tc->start, tc->end);
	co_await f;
}

TEST_CASE("/fdbrpc/countedsection/uninterrupted") {
	TestCounters tc;

	Promise<Void> signal;
	auto nothing = doNothing(&tc, signal.getFuture());
	signal.send(Void());
	co_await nothing;

	ASSERT(tc.start.getValue() == 1);
	ASSERT(tc.end.getValue() == 1);
	co_return;
}

TEST_CASE("/fdbrpc/countedsection/interrupted") {
	TestCounters tc;

	{
		Promise<Void> signal;
		auto nothing = doNothing(&tc, signal.getFuture());
	}

	ASSERT(tc.start.getValue() == 1);
	ASSERT(tc.end.getValue() == 1);
	co_return;
}
