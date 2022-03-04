/*
 * TesterKeyValueStore.cpp
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

#include "TesterKeyValueStore.h"

namespace FdbApiTester {

// Get the value associated with a key
std::optional<std::string> KeyValueStore::get(std::string_view key) const {
	std::unique_lock<std::mutex> lock(mutex);
	auto value = store.find(std::string(key));
	if (value != store.end())
		return value->second;
	else
		return std::optional<std::string>();
}

// Checks if the key exists
bool KeyValueStore::exists(std::string_view key) {
	std::unique_lock<std::mutex> lock(mutex);
	return (store.find(std::string(key)) != store.end());
}

// Returns the key designated by a key selector
std::string KeyValueStore::getKey(std::string_view keyName, bool orEqual, int offset) const {
	std::unique_lock<std::mutex> lock(mutex);
	// Begin by getting the start key referenced by the key selector
	std::map<std::string, std::string>::const_iterator mapItr = store.lower_bound(keyName);

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
std::vector<KeyValue> KeyValueStore::getRange(std::string_view begin,
                                              std::string_view end,
                                              int limit,
                                              bool reverse) const {
	std::unique_lock<std::mutex> lock(mutex);
	std::vector<KeyValue> results;
	if (!reverse) {
		std::map<std::string, std::string>::const_iterator mapItr = store.lower_bound(begin);

		for (; mapItr != store.end() && mapItr->first < end && results.size() < limit; mapItr++)
			results.push_back(KeyValue{ mapItr->first, mapItr->second });
	}

	// Support for reverse getRange queries is supported, but not tested at this time.  This is because reverse range
	// queries have been disallowed by the database at the API level
	else {
		std::map<std::string, std::string>::const_iterator mapItr = store.lower_bound(end);
		if (mapItr == store.begin())
			return results;

		for (--mapItr; mapItr->first >= begin && results.size() < abs(limit); mapItr--) {
			results.push_back(KeyValue{ mapItr->first, mapItr->second });
			if (mapItr == store.begin())
				break;
		}
	}

	return results;
}

// Stores a key-value pair in the database
void KeyValueStore::set(std::string_view key, std::string_view value) {
	std::unique_lock<std::mutex> lock(mutex);
	store[std::string(key)] = value;
}

// Removes a key from the database
void KeyValueStore::clear(std::string_view key) {
	std::unique_lock<std::mutex> lock(mutex);
	auto iter = store.find(key);
	if (iter != store.end()) {
		store.erase(iter);
	}
}

// Removes a range of keys from the database
void KeyValueStore::clear(std::string_view begin, std::string_view end) {
	std::unique_lock<std::mutex> lock(mutex);
	store.erase(store.lower_bound(begin), store.lower_bound(end));
}

// The number of keys in the database
uint64_t KeyValueStore::size() const {
	std::unique_lock<std::mutex> lock(mutex);
	return store.size();
}

// The first key in the database; returned by key selectors that choose a key off the front
std::string KeyValueStore::startKey() const {
	return "";
}

// The last key in the database; returned by key selectors that choose a key off the back
std::string KeyValueStore::endKey() const {
	return "\xff";
}

// Debugging function that prints all key-value pairs
void KeyValueStore::printContents() const {
	std::unique_lock<std::mutex> lock(mutex);
	printf("Contents:\n");
	std::map<std::string, std::string>::const_iterator mapItr;
	for (mapItr = store.begin(); mapItr != store.end(); mapItr++)
		printf("%s\n", mapItr->first.c_str());
}

} // namespace FdbApiTester