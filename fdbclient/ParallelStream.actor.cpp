/*
 * ParallelStream.actor.cpp
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

#include <vector>

#include "fdbclient/ParallelStream.actor.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace ParallelStreamTest {

ACTOR static Future<Void> produce(ParallelStream<int>::Fragment* fragment, int i) {
	wait(delay(deterministicRandom()->random01()));
	fragment->send(i);
	wait(delay(deterministicRandom()->random01()));
	fragment->finish();
	return Void();
}

ACTOR static Future<Void> consume(FutureStream<int> stream, int expected) {
	state int next;
	try {
		loop {
			int i = waitNext(stream);
			ASSERT(i == next++);
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
		ASSERT(next == expected);
		return Void();
	}
}

} // namespace ParallelStreamTest

TEST_CASE("/parallel_stream") {
	state PromiseStream<int> results;
	state ParallelStream<int> parallelStream(results, 10);
	state Future<Void> consumer = ParallelStreamTest::consume(results.getFuture(), 100);
	state std::vector<Future<Void>> producers;
	state int i = 0;
	for (; i < 100; ++i) {
		ParallelStream<int>::Fragment* fragment = wait(parallelStream.createFragment());
		producers.push_back(ParallelStreamTest::produce(fragment, i));
	}
	wait(waitForAll(producers));
	results.sendError(end_of_stream());
	wait(consumer);
	return Void();
}
