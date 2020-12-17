/*
 * ParallelStreamCorrectness.actor.cpp
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

ACTOR static Future<Void> produce(ParallelStream<int>::Fragment* fragment, int value) {
	wait(delay(deterministicRandom()->random01()));
	fragment->send(value);
	wait(delay(deterministicRandom()->random01()));
	fragment->finish();
	return Void();
}

ACTOR static Future<Void> consume(FutureStream<int> stream, int expected) {
	state int next;
	try {
		loop {
			int value = waitNext(stream);
			ASSERT(value == next++);
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
		ASSERT(next == expected);
		return Void();
	}
}

} // namespace ParallelStreamTest

TEST_CASE("/fdbclient/ParallelStream") {
	state PromiseStream<int> results;
	state size_t concurrency = deterministicRandom()->randomInt(1,11);
	state size_t bufferLimit = concurrency + deterministicRandom()->randomInt(0, 11);
	state size_t numProducers = deterministicRandom()->randomInt(1,1001);
	state ParallelStream<int> parallelStream(results, concurrency, bufferLimit);
	state Future<Void> consumer = ParallelStreamTest::consume(results.getFuture(), numProducers);
	state std::vector<Future<Void>> producers;
	TraceEvent("StartingParallelStreamTest")
	    .detail("Concurrency", concurrency)
	    .detail("BufferLimit", bufferLimit)
	    .detail("NumProducers", numProducers);
	state int i = 0;
	for (; i < numProducers; ++i) {
		ParallelStream<int>::Fragment* fragment = wait(parallelStream.createFragment());
		producers.push_back(ParallelStreamTest::produce(fragment, i));
	}
	wait(parallelStream.finish());
	wait(consumer);
	return Void();
}

void forceLinkParallelStreamTests() {}
