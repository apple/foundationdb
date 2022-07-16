/*
 * MemoryKeyValueStore.h
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

#ifndef FDBSERVER_MEMORYKEYVALUESTORE_H
#define FDBSERVER_MEMORYKEYVALUESTORE_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbserver/workloads/workloads.actor.h"

#include <map>

// An in-memory key-value store designed to mirror the contents of the database after performing sequences of operations
class MemoryKeyValueStore {

public:
	// Get the value associated with a key
	Optional<Value> get(KeyRef key) const;

	// Returns the key designated by a key selector
	Key getKey(KeySelectorRef selector) const;

	// Gets a range of key-value pairs, returning a maximum of <limit> results
	RangeResult getRange(KeyRangeRef range, int limit, Reverse reverse) const;

	// Stores a key-value pair in the database
	void set(KeyRef key, ValueRef value);

	// Removes a key from the database
	void clear(KeyRef key);

	// Removes a range of keys from the database
	void clear(KeyRangeRef range);

	// The number of keys in the database
	uint64_t size() const;

	// The first key in the database; returned by key selectors that choose a key off the front
	Key startKey() const;

	// The last key in the database; returned by key selectors that choose a key off the back
	Key endKey() const;

	// Debugging function that prints all key-value pairs
	void printContents() const;

private:
	// A map holding the key-value pairs
	std::map<Key, Value> store;
};

#endif
