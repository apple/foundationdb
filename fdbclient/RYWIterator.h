/*
 * RYWIterator.h
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

#ifndef FDBCLIENT_RYWITERATOR_H
#define FDBCLIENT_RYWITERATOR_H
#pragma once

#include "SnapshotCache.h"
#include "WriteMap.h"

class RYWIterator {
public:
	RYWIterator(SnapshotCache* snapshotCache, WriteMap* writeMap)
	  : begin_key_cmp(0), end_key_cmp(0), cache(snapshotCache), writes(writeMap), bypassUnreadable(false) {}

	enum SEGMENT_TYPE { UNKNOWN_RANGE, EMPTY_RANGE, KV };
	static const SEGMENT_TYPE typeMap[12];

	SEGMENT_TYPE type() const;

	bool is_kv() const;
	bool is_unknown_range() const;
	bool is_empty_range() const;
	bool is_unreadable() const;
	bool is_dependent() const;

	ExtStringRef beginKey();
	ExtStringRef endKey();

	virtual const KeyValueRef* kv(Arena& arena);

	RYWIterator& operator++();

	RYWIterator& operator--();

	bool operator==(const RYWIterator& r) const;
	bool operator!=(const RYWIterator& r) const;

	void skip(KeyRef key);

	void skipContiguous(KeyRef key);

	void skipContiguousBack(KeyRef key);

	void bypassUnreadableProtection() { bypassUnreadable = true; }

	// Really this should return an iterator by value, but for performance it's convenient to actually grab the internal
	// one.  Consider copying the return value if performance isn't critical. If you modify the returned iterator, it
	// invalidates this iterator until the next call to skip()
	virtual WriteMap::iterator& extractWriteMapIterator();

	void dbg();

protected:
	int begin_key_cmp; // -1 if cache.beginKey() < writes.beginKey(), 0 if ==, +1 if >
	int end_key_cmp; //
	SnapshotCache::iterator cache;
	WriteMap::iterator writes;
	KeyValueRef temp;
	bool bypassUnreadable; // When set, allows read from sections of keyspace that have become unreadable because of
	                       // versionstamp operations

	void updateCmp();
};

void testESR();
void testSnapshotCache();

#endif
