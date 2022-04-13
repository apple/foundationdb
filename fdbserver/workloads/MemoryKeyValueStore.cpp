/*
 * MemoryKeyValueStore.cpp
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

#include "fdbserver/workloads/MemoryKeyValueStore.h"

// Get the value associated with a key
Optional<Value> MemoryKeyValueStore::get(KeyRef key) const {
	std::map<Key, Value>::const_iterator value = store.find(key);
	if (value != store.end())
		return value->second;
	else
		return Optional<Value>();
}

// Returns the key designated by a key selector
Key MemoryKeyValueStore::getKey(KeySelectorRef selector) const {
	// Begin by getting the start key referenced by the key selector
	std::map<Key, Value>::const_iterator mapItr = store.lower_bound(selector.getKey());

	// Update the iterator position if necessary based on the value of orEqual
	int count = 0;
	if (selector.offset <= 0) {
		if (mapItr == store.end() || selector.getKey() != mapItr->first || !selector.orEqual) {
			if (mapItr == store.begin())
				return startKey();

			mapItr--;
		}
	} else {
		if (mapItr == store.end())
			return endKey();

		if (selector.getKey() == mapItr->first && selector.orEqual) {
			mapItr++;
		}

		count++;
	}

	// Increment the map iterator until the desired offset is reached
	for (; count < abs(selector.offset); count++) {
		if (selector.offset < 0) {
			if (mapItr == store.begin())
				break;

			mapItr--;
		} else {
			if (mapItr == store.end())
				break;

			mapItr++;
		}
	}

	if (mapItr == store.end())
		return endKey();
	else if (count == abs(selector.offset))
		return mapItr->first;
	else
		return startKey();
}

// Gets a range of key-value pairs, returning a maximum of <limit> results
RangeResult MemoryKeyValueStore::getRange(KeyRangeRef range, int limit, Reverse reverse) const {
	RangeResult results;
	if (!reverse) {
		std::map<Key, Value>::const_iterator mapItr = store.lower_bound(range.begin);

		for (; mapItr != store.end() && mapItr->first < range.end && results.size() < limit; mapItr++)
			results.push_back_deep(results.arena(), KeyValueRef(mapItr->first, mapItr->second));
	}

	// Support for reverse getRange queries is supported, but not tested at this time.  This is because reverse range
	// queries have been disallowed by the database at the API level
	else {
		std::map<Key, Value>::const_iterator mapItr = store.lower_bound(range.end);
		if (mapItr == store.begin())
			return results;

		for (--mapItr; mapItr->first >= range.begin && results.size() < abs(limit); mapItr--) {
			results.push_back_deep(results.arena(), KeyValueRef(mapItr->first, mapItr->second));
			if (mapItr == store.begin())
				break;
		}
	}

	return results;
}

// Stores a key-value pair in the database
void MemoryKeyValueStore::set(KeyRef key, ValueRef value) {
	store[key] = value;
}

// Removes a key from the database
void MemoryKeyValueStore::clear(KeyRef key) {
	store.erase(key);
}

// Removes a range of keys from the database
void MemoryKeyValueStore::clear(KeyRangeRef range) {
	store.erase(store.lower_bound(range.begin), store.lower_bound(range.end));
}

// The number of keys in the database
uint64_t MemoryKeyValueStore::size() const {
	return store.size();
}

// The first key in the database; returned by key selectors that choose a key off the front
Key MemoryKeyValueStore::startKey() const {
	return LiteralStringRef("");
}

// The last key in the database; returned by key selectors that choose a key off the back
Key MemoryKeyValueStore::endKey() const {
	return LiteralStringRef("\xff");
}

// Debugging function that prints all key-value pairs
void MemoryKeyValueStore::printContents() const {
	printf("Contents:\n");
	std::map<Key, Value>::const_iterator mapItr;
	for (mapItr = store.begin(); mapItr != store.end(); mapItr++)
		printf("%s\n", mapItr->first.toString().c_str());
}
