/*
 * TesterKeyValueStore.cpp
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

#include "TesterKeyValueStore.h"

namespace FdbApiTester {

// Get the value associated with a key
std::optional<fdb::Value> KeyValueStore::get(fdb::KeyRef key) const {
	std::unique_lock<std::mutex> lock(mutex);
	auto value = store.find(fdb::Key(key));
	if (value != store.end())
		return value->second;
	else
		return std::optional<fdb::Value>();
}

// Checks if the key exists
bool KeyValueStore::exists(fdb::KeyRef key) {
	std::unique_lock<std::mutex> lock(mutex);
	return (store.find(fdb::Key(key)) != store.end());
}

// Returns the key designated by a key selector
fdb::Key KeyValueStore::getKey(fdb::KeyRef keyName, bool orEqual, int offset) const {
	std::unique_lock<std::mutex> lock(mutex);
	// Begin by getting the start key referenced by the key selector
	std::map<fdb::Key, fdb::Value>::const_iterator mapItr = store.lower_bound(keyName);

	// Update the iterator position if necessary based on the value of orEqual
	int count = 0;
	if (offset <= 0) {
		if (mapItr == store.end() || keyName != mapItr->first || !orEqual) {
			if (mapItr == store.begin())
				return startKey();

			mapItr--;
		}
	} else {
		if (mapItr == store.end())
			return endKey();

		if (keyName == mapItr->first && orEqual) {
			mapItr++;
		}

		count++;
	}

	// Increment the map iterator until the desired offset is reached
	for (; count < abs(offset); count++) {
		if (offset < 0) {
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
	else if (count == abs(offset))
		return mapItr->first;
	else
		return startKey();
}

// Gets a range of key-value pairs, returning a maximum of <limit> results
std::vector<fdb::KeyValue> KeyValueStore::getRange(fdb::KeyRef begin, fdb::KeyRef end, int limit, bool reverse) const {
	std::unique_lock<std::mutex> lock(mutex);
	std::vector<fdb::KeyValue> results;
	if (!reverse) {
		std::map<fdb::Key, fdb::Value>::const_iterator mapItr = store.lower_bound(begin);

		for (; mapItr != store.end() && mapItr->first < end && results.size() < limit; mapItr++)
			results.push_back(fdb::KeyValue{ mapItr->first, mapItr->second });
	}

	// Support for reverse getRange queries is supported, but not tested at this time.  This is because reverse range
	// queries have been disallowed by the database at the API level
	else {
		std::map<fdb::Key, fdb::Value>::const_iterator mapItr = store.lower_bound(end);
		if (mapItr == store.begin())
			return results;

		for (--mapItr; mapItr->first >= begin && results.size() < abs(limit); mapItr--) {
			results.push_back(fdb::KeyValue{ mapItr->first, mapItr->second });
			if (mapItr == store.begin())
				break;
		}
	}

	return results;
}

// Stores a key-value pair in the database
void KeyValueStore::set(fdb::KeyRef key, fdb::ValueRef value) {
	std::unique_lock<std::mutex> lock(mutex);
	store[fdb::Key(key)] = value;
}

// Removes a key from the database
void KeyValueStore::clear(fdb::KeyRef key) {
	std::unique_lock<std::mutex> lock(mutex);
	auto iter = store.find(key);
	if (iter != store.end()) {
		store.erase(iter);
	}
}

// Removes a range of keys from the database
void KeyValueStore::clear(fdb::KeyRef begin, fdb::KeyRef end) {
	std::unique_lock<std::mutex> lock(mutex);
	store.erase(store.lower_bound(begin), store.lower_bound(end));
}

// The number of keys in the database
uint64_t KeyValueStore::size() const {
	std::unique_lock<std::mutex> lock(mutex);
	return store.size();
}

// The first key in the database; returned by key selectors that choose a key off the front
fdb::Key KeyValueStore::startKey() const {
	return fdb::Key();
}

// The last key in the database; returned by key selectors that choose a key off the back
fdb::Key KeyValueStore::endKey() const {
	return fdb::Key(1, '\xff');
}

// Debugging function that prints all key-value pairs
void KeyValueStore::printContents() const {
	std::unique_lock<std::mutex> lock(mutex);
	printf("Contents:\n");
	std::map<fdb::Key, fdb::Value>::const_iterator mapItr;
	for (mapItr = store.begin(); mapItr != store.end(); mapItr++)
		printf("%s\n", mapItr->first.c_str());
}

} // namespace FdbApiTester
