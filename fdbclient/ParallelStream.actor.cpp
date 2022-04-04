/*
 * ParallelStreamCorrectness.actor.cpp
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

#include <vector>

#include "fdbclient/ParallelStream.actor.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace ParallelStreamTest {

struct TestValue {
	int x;
	TestValue(int x) : x(x) {}
	int expectedSize() const { return sizeof(int); }
};

ACTOR static Future<Void> produce(ParallelStream<ParallelStreamTest::TestValue>::Fragment* fragment,
                                  ParallelStreamTest::TestValue value) {
	wait(delay(deterministicRandom()->random01()));
	fragment->send(value);
	wait(delay(deterministicRandom()->random01()));
	fragment->finish();
	return Void();
}

ACTOR static Future<Void> consume(FutureStream<ParallelStreamTest::TestValue> stream, int expected) {
	state int next;
	try {
		loop {
			ParallelStreamTest::TestValue value = waitNext(stream);
			ASSERT(value.x == next++);
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
		ASSERT(next == expected);
		return Void();
	}
}

} // namespace ParallelStreamTest

TEST_CASE("/fdbclient/ParallelStream") {
	state PromiseStream<ParallelStreamTest::TestValue> results;
	state size_t bufferLimit = deterministicRandom()->randomInt(0, 21);
	state size_t numProducers = deterministicRandom()->randomInt(1, 1001);
	state ParallelStream<ParallelStreamTest::TestValue> parallelStream(results, bufferLimit);
	state Future<Void> consumer = ParallelStreamTest::consume(results.getFuture(), numProducers);
	state std::vector<Future<Void>> producers;
	TraceEvent("StartingParallelStreamTest").detail("BufferLimit", bufferLimit).detail("NumProducers", numProducers);
	state int i = 0;
	for (; i < numProducers; ++i) {
		ParallelStream<ParallelStreamTest::TestValue>::Fragment* fragment = wait(parallelStream.createFragment());
		producers.push_back(ParallelStreamTest::produce(fragment, ParallelStreamTest::TestValue(i)));
	}
	wait(parallelStream.finish());
	wait(consumer);
	return Void();
}

void forceLinkParallelStreamTests() {}
