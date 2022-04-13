/*
 * IKeyValueContainer.h
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
#ifndef IKEYVALUECONTAINER_H
#define IKEYVALUECONTAINER_H
#pragma once

#include "flow/IndexedSet.h"

// Stored in the IndexedSets that hold the database.
// Each KeyValueMapPair is 32 bytes, excluding arena memory.
// It is stored in an IndexedSet<KeyValueMapPair, uint64_t>::Node, for a total size of 72 bytes.
struct KeyValueMapPair {
	Arena arena; // 8 Bytes (excluding arena memory)
	KeyRef key; // 12 Bytes
	ValueRef value; // 12 Bytes

	void operator=(KeyValueMapPair const& rhs) {
		arena = rhs.arena;
		key = rhs.key;
		value = rhs.value;
	}
	KeyValueMapPair(KeyValueMapPair const& rhs) : arena(rhs.arena), key(rhs.key), value(rhs.value) {}

	KeyValueMapPair(KeyRef key, ValueRef value)
	  : arena(key.expectedSize() + value.expectedSize()), key(arena, key), value(arena, value) {}

	int compare(KeyValueMapPair const& r) const { return ::compare(key, r.key); }

	template <class CompatibleWithKey>
	int compare(CompatibleWithKey const& r) const {
		return ::compare(key, r);
	}

	bool operator<(KeyValueMapPair const& r) const { return key < r.key; }
	bool operator>(KeyValueMapPair const& r) const { return r < *this; }
	bool operator<=(KeyValueMapPair const& r) const { return !(*this > r); }
	bool operator>=(KeyValueMapPair const& r) const { return !(*this < r); }
	bool operator==(KeyValueMapPair const& r) const { return key == r.key; }
	bool operator!=(KeyValueMapPair const& r) const { return key != r.key; }
};

template <class CompatibleWithKey>
int compare(CompatibleWithKey const& l, KeyValueMapPair const& r) {
	return ::compare(l, r.key);
}
template <class CompatibleWithKey>
bool operator<(KeyValueMapPair const& l, CompatibleWithKey const& r) {
	return l.key < r;
}

template <class CompatibleWithKey>
bool operator<(CompatibleWithKey const& l, KeyValueMapPair const& r) {
	return l < r.key;
}

class IKeyValueContainer {
public:
	using const_iterator = IndexedSet<KeyValueMapPair, uint64_t>::const_iterator;
	using iterator = IndexedSet<KeyValueMapPair, uint64_t>::iterator;

	IKeyValueContainer() = default;
	~IKeyValueContainer() = default;

	bool empty() const { return data.empty(); }
	void clear() { return data.clear(); }

	std::tuple<size_t, size_t, size_t> size() const { return std::make_tuple(0, 0, 0); }

	const_iterator find(const StringRef& key) const { return data.find(key); }
	iterator find(const StringRef& key) { return data.find(key); }
	const_iterator begin() const { return data.begin(); }
	iterator begin() { return data.begin(); }
	const_iterator cbegin() const { return begin(); }
	const_iterator end() const { return data.end(); }
	iterator end() { return data.end(); }
	const_iterator cend() const { return end(); }

	const_iterator lower_bound(const StringRef& key) const { return data.lower_bound(key); }
	iterator lower_bound(const StringRef& key) { return data.lower_bound(key); }
	const_iterator upper_bound(const StringRef& key) const { return data.upper_bound(key); }
	iterator upper_bound(const StringRef& key) { return data.upper_bound(key); }
	const_iterator previous(const_iterator i) const { return data.previous(i); }
	const_iterator previous(iterator i) const { return data.previous(const_iterator{ i }); }
	iterator previous(iterator i) { return data.previous(i); }

	void erase(iterator begin, iterator end) { data.erase(begin, end); }
	iterator insert(const StringRef& key, const StringRef& val, bool replaceExisting = true) {
		KeyValueMapPair pair(key, val);
		return data.insert(pair, pair.arena.getSize() + data.getElementBytes(), replaceExisting);
	}
	int insert(const std::vector<std::pair<KeyValueMapPair, uint64_t>>& pairs, bool replaceExisting = true) {
		return data.insert(pairs, replaceExisting);
	}

	uint64_t sumTo(const_iterator to) const { return data.sumTo(to); }
	uint64_t sumTo(iterator to) const { return data.sumTo(const_iterator{ to }); }

	static constexpr int getElementBytes() { return IndexedSet<KeyValueMapPair, uint64_t>::getElementBytes(); }

private:
	IKeyValueContainer(IKeyValueContainer const&); // unimplemented
	void operator=(IKeyValueContainer const&); // unimplemented
	IndexedSet<KeyValueMapPair, uint64_t> data;
};

#endif
