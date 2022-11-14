/*
 * FDBTypes.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/NativeAPI.actor.h"

KeyRangeRef toPrefixRelativeRange(KeyRangeRef range, KeyRef prefix) {
	if (prefix.empty()) {
		return range;
	} else {
		KeyRef begin = range.begin.startsWith(prefix) ? range.begin.removePrefix(prefix) : allKeys.begin;
		KeyRef end = range.end.startsWith(prefix) ? range.end.removePrefix(prefix) : allKeys.end;
		return KeyRangeRef(begin, end);
	}
}

KeyRef keyBetween(const KeyRangeRef& keys) {
	int pos = 0; // will be the position of the first difference between keys.begin and keys.end
	int minSize = std::min(keys.begin.size(), keys.end.size());
	for (; pos < minSize && pos < CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT; pos++) {
		if (keys.begin[pos] != keys.end[pos]) {
			return keys.end.substr(0, pos + 1);
		}
	}

	// If one more character keeps us in the limit, and the latter key is simply
	// longer, then we only need one more byte of the end string.
	if (pos < CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT && keys.begin.size() < keys.end.size()) {
		return keys.end.substr(0, pos + 1);
	}

	return keys.end;
}

Key randomKeyBetween(const KeyRangeRef& keys) {
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;
	ASSERT(begin < end);
	if (begin.size() < end.size()) {
		// randomly append a char
		uint8_t newChar = deterministicRandom()->randomInt(0, end[begin.size()] + 1);
		return begin.withSuffix(StringRef(&newChar, 1));
	}

	int pos = 0; // will be the position of the first difference between keys.begin and keys.end
	for (; pos < end.size() && pos < CLIENT_KNOBS->KEY_SIZE_LIMIT; pos++) {
		if (keys.begin[pos] != keys.end[pos]) {
			break;
		}
	}
	ASSERT_LT(pos, end.size()); // otherwise, begin >= end

	// find the lowest char in range begin[pos+1, begin.size()) that is not \xff (255)
	int lowest = begin.size() - 1;
	for (; lowest > pos; lowest--) {
		if (begin[lowest] < 255) {
			Key res = begin;
			uint8_t* ptr = mutateString(res);
			*(ptr + lowest) = (uint8_t)deterministicRandom()->randomInt(begin[lowest] + 1, 256);
			return res;
		}
	}

	if (begin[pos] + 1 < end[pos]) {
		Key res = begin;
		uint8_t* ptr = mutateString(res);
		*(ptr + pos) = (uint8_t)deterministicRandom()->randomInt(begin[pos] + 1, end[pos]);
		return res;
	}

	if (begin.size() + 1 < CLIENT_KNOBS->KEY_SIZE_LIMIT) {
		// randomly append a char
		uint8_t newChar = deterministicRandom()->randomInt(1, 255);
		return begin.withSuffix(StringRef(&newChar, 1));
	}

	// no possible result
	return end;
}

TEST_CASE("/KeyRangeUtil/randomKeyBetween") {
	Key begin = "qwert"_sr;
	Key end = "qwertyu"_sr;
	Key res;
	for (int i = 0; i < 10; ++i) {
		res = randomKeyBetween(KeyRangeRef(begin, end));
		ASSERT(res > begin);
		ASSERT(res < end);
	}

	begin = "q"_sr;
	end = "q\x00"_sr;
	res = randomKeyBetween(KeyRangeRef(begin, end));
	ASSERT(res == end);

	begin = "aaaaaaa"_sr;
	end = "b"_sr;
	for (int i = 0; i < 10; ++i) {
		res = randomKeyBetween(KeyRangeRef(begin, end));
		ASSERT(res > begin);
		ASSERT(res < end);
	}
	return Void();
}

void KeySelectorRef::setKey(KeyRef const& key) {
	// There are no keys in the database with size greater than the max key size, so if this key selector has a key
	// which is large, then we can translate it to an equivalent key selector with a smaller key
	int64_t maxKeySize = getMaxKeySize(key);
	if (key.size() > maxKeySize) {
		this->key = key.substr(0, maxKeySize + 1);
	} else {
		this->key = key;
	}
}

void KeySelectorRef::setKeyUnlimited(KeyRef const& key) {
	this->key = key;
}

std::string KeySelectorRef::toString() const {
	if (offset > 0) {
		if (orEqual)
			return format("%d+firstGreaterThan(%s)", offset - 1, printable(key).c_str());
		else
			return format("%d+firstGreaterOrEqual(%s)", offset - 1, printable(key).c_str());
	} else {
		if (orEqual)
			return format("%d+lastLessOrEqual(%s)", offset, printable(key).c_str());
		else
			return format("%d+lastLessThan(%s)", offset, printable(key).c_str());
	}
}

std::string describe(const std::string& s) {
	return s;
}

std::string describe(UID const& item) {
	return item.shortString();
}
