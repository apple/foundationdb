/**
 * TimedKVCache.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/TimedKVCache.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h"

TEST_CASE("fdbserver/TimedKVCache/testExpire") {
	state TimedKVCache<int, int> timedCache(1e6);
	state const int TEST_KEY = 3;
	state const int TEST_VALUE = 1;

	ASSERT(!timedCache.exists(TEST_KEY));
	timedCache.add(TEST_KEY, TEST_VALUE);
	ASSERT(timedCache.exists(3));
	ASSERT(timedCache.get(TEST_KEY) == TEST_VALUE);

	wait(delay(1.5));

	ASSERT(!timedCache.exists(TEST_KEY));

	return Void();
}
