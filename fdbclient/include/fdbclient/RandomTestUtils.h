/*
 * RandomTestUtils.h
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

#ifndef FDBCLIENT_RANDOMTESTUTILS_H
#define FDBCLIENT_RANDOMTESTUTILS_H
#pragma once

#include <string>

#include "fdbclient/FDBTypes.h"

class RandomTestImpl {
public:
	static ValueRef getRandomValue(Arena& arena) {
		return ValueRef(arena, std::string(deterministicRandom()->randomInt(0, 1000), 'x'));
	}

	static ValueRef getRandomVersionstampValue(Arena& arena) {
		int len = deterministicRandom()->randomInt(10, 98);
		std::string value = std::string(len, 'x');
		int32_t pos = deterministicRandom()->randomInt(0, len - 9);
		if (deterministicRandom()->random01() < 0.01) {
			pos = value.size() - 10;
		}
		pos = littleEndian32(pos);
		value += std::string((const char*)&pos, sizeof(int32_t));
		return ValueRef(arena, value);
	}

	static ValueRef getRandomVersionstampKey(Arena& arena) {
		int idx = deterministicRandom()->randomInt(0, 100);
		std::string key = format("%010d", idx / 3);
		if (idx % 3 >= 1)
			key += '\x00';
		if (idx % 3 >= 2)
			key += '\x00';
		int32_t pos = key.size() - deterministicRandom()->randomInt(0, 3);
		if (deterministicRandom()->random01() < 0.01) {
			pos = 0;
		}
		key = key.substr(0, pos);
		key += "XXXXXXXXYY";
		key += std::string(deterministicRandom()->randomInt(0, 3), 'z');
		pos = littleEndian32(pos);
		key += std::string((const char*)&pos, sizeof(int32_t));
		return ValueRef(arena, key);
	}

	static KeyRef getRandomKey(Arena& arena) { return getKeyForIndex(arena, deterministicRandom()->randomInt(0, 100)); }

	static KeyRef getKeyForIndex(Arena& arena, int idx) {
		std::string key = format("%010d", idx / 3);
		if (idx % 3 >= 1)
			key += '\x00';
		if (idx % 3 >= 2)
			key += '\x00';
		return KeyRef(arena, key);
	}

	static KeyRangeRef getRandomRange(Arena& arena) {
		int startLocation = deterministicRandom()->randomInt(0, 100);
		int endLocation = startLocation + deterministicRandom()->randomInt(1, 1 + 100 - startLocation);

		return KeyRangeRef(getKeyForIndex(arena, startLocation), getKeyForIndex(arena, endLocation));
	}

	static KeySelectorRef getRandomKeySelector(Arena& arena) {
		return KeySelectorRef(
		    getRandomKey(arena), deterministicRandom()->random01() < 0.5, deterministicRandom()->randomInt(-10, 10));
	}
};

#endif // FDBCLIENT_RANDOMTESTUTILS_H
