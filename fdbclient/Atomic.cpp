/*
 * Atomic.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/SystemData.h"
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

TEST_CASE("/Atomic/VersionstampOffsetBounds") {
	for (int payloadSize : { 0, 9, 10, 11, 31 }) {
		for (int32_t offset : { std::numeric_limits<int32_t>::min(),
		                        -1,
		                        0,
		                        1,
		                        21,
		                        22,
		                        std::numeric_limits<int32_t>::max() - 9,
		                        std::numeric_limits<int32_t>::max() }) {
			Arena arena;
			StringRef encoded = makeString(payloadSize + 4, arena);
			memset(mutateString(encoded), 'x', encoded.size());
			int32_t littleEndianOffset = littleEndian32(offset);
			memcpy(mutateString(encoded) + payloadSize, &littleEndianOffset, sizeof(littleEndianOffset));
			const bool valid = offset >= 0 && int64_t(offset) + 10 <= payloadSize;
			bool rangeRejected = false;
			try {
				getVersionstampKeyRange(arena, encoded, Optional<Version>(), allKeys.end);
			} catch (Error& e) {
				ASSERT_EQ(e.code(), error_code_client_invalid_operation);
				rangeRejected = true;
			}
			ASSERT_EQ(rangeRejected, !valid);

			StringRef key(arena, encoded);
			bool keyRejected = false;
			try {
				transformVersionstampKey(key, 100, 7);
			} catch (Error& e) {
				ASSERT_EQ(e.code(), error_code_client_invalid_operation);
				keyRejected = true;
			}
			ASSERT_EQ(keyRejected, !valid);

			for (auto param : { &MutationRef::param1, &MutationRef::param2 }) {
				MutationRef mutation(
				    MutationRef::SetVersionstampedKey, StringRef(arena, encoded), StringRef(arena, encoded));
				transformVersionstampMutation(mutation, param, 100, 7);
				StringRef expected = makeString(payloadSize, arena);
				memset(mutateString(expected), 'x', payloadSize);
				if (valid) {
					placeVersionstamp(mutateString(expected) + offset, 100, 7);
					ASSERT_EQ(key.substr(0, payloadSize), expected);
					ASSERT_EQ(key.substr(payloadSize), encoded.substr(payloadSize));
				}
				ASSERT_EQ(mutation.type, MutationRef::SetValue);
				ASSERT_EQ(mutation.*param, expected);
			}
		}
	}
	return Void();
}
