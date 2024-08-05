/*
 * Atomic.cpp
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

#include "fdbclient/Atomic.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"

void forceLinkAtomicTests() {}

TEST_CASE("/Atomic/DoAppendIfFits") {
	Arena arena;
	{
		Value existingValue = ValueRef(arena, "existing"_sr);
		Value otherOperand = ValueRef(arena, "other"_sr);
		auto result = doAppendIfFits(existingValue, otherOperand, arena);
		ASSERT(compare("existingother"_sr, result) == 0);
	}
	{
		Value existingValue = makeString(CLIENT_KNOBS->VALUE_SIZE_LIMIT - 1, arena);
		Value otherOperand = makeString(2, arena);
		deterministicRandom()->randomBytes(mutateString(existingValue), existingValue.size());
		deterministicRandom()->randomBytes(mutateString(otherOperand), otherOperand.size());
		// Appended values cannot fit in result, should return existingValue
		auto result = doAppendIfFits(existingValue, otherOperand, arena);
		ASSERT(compare(existingValue, result) == 0);
	}
	return Void();
}

// TODO: Add more unit tests for atomic operations defined in Atomic.h
