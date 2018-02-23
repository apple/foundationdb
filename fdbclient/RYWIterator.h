/*
 * RYWIterator.h
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

#ifndef FDBCLIENT_RYWITERATOR_H
#define FDBCLIENT_RYWITERATOR_H
#pragma once

#include "SnapshotCache.h"
#include "WriteMap.h"

class RYWIterator {
public:
	RYWIterator( SnapshotCache* snapshotCache, WriteMap* writeMap ) : cache(snapshotCache), writes(writeMap), begin_key_cmp(0), end_key_cmp(0) {}
	
	enum SEGMENT_TYPE { UNKNOWN_RANGE, EMPTY_RANGE, KV };
	static const SEGMENT_TYPE typeMap[12];

	SEGMENT_TYPE type();

	bool is_kv();
	bool is_unknown_range();
	bool is_empty_range();
	bool is_unreadable();
	bool is_dependent();

	ExtStringRef beginKey();
	ExtStringRef endKey();

	KeyValueRef const& kv( Arena& arena );

	RYWIterator& operator++();

	RYWIterator& operator--();

	bool operator == ( const RYWIterator& r ) const;

	void skip( KeyRef key );
	
	void skipContiguous( KeyRef key );

	void skipContiguousBack( KeyRef key );

	WriteMap::iterator& extractWriteMapIterator();
	// Really this should return an iterator by value, but for performance it's convenient to actually grab the internal one.  Consider copying the return value if performance isn't critical.
	// If you modify the returned iterator, it invalidates this iterator until the next call to skip()

	void dbg();

private:
	int begin_key_cmp;   // -1 if cache.beginKey() < writes.beginKey(), 0 if ==, +1 if >
	int end_key_cmp;    // 
	SnapshotCache::iterator cache;
	WriteMap::iterator writes;
	KeyValueRef temp;

	void updateCmp();
};

class RandomTestImpl {
public:
	static ValueRef getRandomValue(Arena& arena) {
		return ValueRef(arena, std::string(g_random->randomInt(0, 1000), 'x'));
	}

	static ValueRef getRandomVersionstampValue(Arena& arena) {
		int len = g_random->randomInt(10, 98);
		std::string value = std::string(len, 'x');
		return ValueRef(arena, value);
	}

	static ValueRef getRandomVersionstampKey(Arena& arena) {
		int idx = g_random->randomInt(0, 100);
		std::string key = format("%010d", idx / 3);
		if (idx % 3 >= 1)
			key += '\x00';
		if (idx % 3 >= 2)
			key += '\x00';
		int pos = key.size() - g_random->randomInt(0, 3);
		if (g_random->random01() < 0.01) {
			pos = 0;
		}
		key = key.substr(0, pos);
		key += "XXXXXXXXYY";
		key += std::string(g_random->randomInt(0, 3), 'z');
		key += (char)(pos & 0xFF);
		key += (char)((pos >> 8) & 0xFF);
		return ValueRef(arena, key);
	}

	static KeyRef getRandomKey(Arena& arena) {
		return getKeyForIndex(arena, g_random->randomInt(0, 100));
	}

	static KeyRef getKeyForIndex(Arena& arena, int idx) {
		std::string key = format("%010d", idx / 3);
		if (idx % 3 >= 1)
			key += '\x00';
		if (idx % 3 >= 2)
			key += '\x00';
		return KeyRef(arena, key);
	}

	static KeyRangeRef getRandomRange(Arena& arena) {
		int startLocation = g_random->randomInt(0, 100);
		int endLocation = startLocation + g_random->randomInt(1, 1 + 100 - startLocation);

		return KeyRangeRef(getKeyForIndex(arena, startLocation), getKeyForIndex(arena, endLocation));
	}

	static KeySelectorRef getRandomKeySelector(Arena& arena) {
		return KeySelectorRef(getRandomKey(arena), g_random->random01() < 0.5, g_random->randomInt(-10, 10));
	}
};

void testESR();
void testSnapshotCache();

#endif