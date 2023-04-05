/*
 * ReplicationTypes.h
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

#ifndef FLOW_REPLICATION_TYPES_H
#define FLOW_REPLICATION_TYPES_H
#pragma once

#include <sstream>
#include "flow/flow.h"
#include "fdbrpc/Locality.h"

struct LocalityData;
struct LocalitySet;
struct LocalityGroup;
struct KeyValueMap;
struct LocalityRecord;
struct StringToIntMap;
struct IReplicationPolicy;

extern int g_replicationdebug;

struct AttribKey {
	int _id;
	explicit AttribKey() : _id(-1) {}
	explicit AttribKey(int id) : _id(id) {}
	bool operator==(AttribKey const& source) const { return _id == source._id; }
	bool operator<(AttribKey const& source) const { return _id < source._id; }
};
struct AttribValue {
	int _id;
	explicit AttribValue() : _id(-1) {}
	explicit AttribValue(int id) : _id(id) {}
	bool operator==(AttribValue const& source) const { return _id == source._id; }
	bool operator!=(AttribValue const& source) const { return !(*this == source); }
	bool operator<(AttribValue const& source) const { return _id < source._id; }
	bool operator>(AttribValue const& source) const { return source < *this; }
	bool operator<=(AttribValue const& source) const { return !(*this > source); }
	bool operator>=(AttribValue const& source) const { return !(*this < source); }
};
struct LocalityEntry {
	int _id;
	explicit LocalityEntry() : _id(-1) {}
	explicit LocalityEntry(int id) : _id(id) {}
	bool operator==(LocalityEntry const& source) const { return _id == source._id; }
	bool operator<(LocalityEntry const& source) const { return _id < source._id; }
};
typedef std::pair<AttribKey, AttribValue> AttribRecord;

// This structure represents the LocalityData class as an integer map
struct KeyValueMap final : public ReferenceCounted<KeyValueMap> {
	std::vector<AttribRecord> _keyvaluearray;

	KeyValueMap() {}
	KeyValueMap(const LocalityData& data);
	KeyValueMap(const KeyValueMap& entry) : _keyvaluearray(entry._keyvaluearray) {}
	KeyValueMap& operator=(KeyValueMap const& source) {
		_keyvaluearray = source._keyvaluearray;
		return *this;
	}

	int size() const { return _keyvaluearray.size(); }

	int getMemoryUsed() const { return sizeof(_keyvaluearray) + (_keyvaluearray.size() * sizeof(AttribRecord)); }

	Optional<AttribValue> getValue(AttribKey const& indexKey) const {
		auto itKey = std::lower_bound(
		    _keyvaluearray.begin(), _keyvaluearray.end(), AttribRecord(indexKey, AttribValue(0)), compareKey);
		return ((itKey != _keyvaluearray.end()) && (itKey->first == indexKey)) ? itKey->second
		                                                                       : Optional<AttribValue>();
	}

	bool isPresent(AttribKey const& indexKey) const {
		auto lower = std::lower_bound(
		    _keyvaluearray.begin(), _keyvaluearray.end(), AttribRecord(indexKey, AttribValue(0)), compareKey);
		return ((lower != _keyvaluearray.end()) && (lower->first == indexKey));
	}
	bool isPresent(AttribKey indexKey, AttribValue indexValue) const {
		auto lower = std::lower_bound(
		    _keyvaluearray.begin(), _keyvaluearray.end(), AttribRecord(indexKey, indexValue), compareKeyValue);
		return ((lower != _keyvaluearray.end()) && (lower->first == indexKey) && (lower->second == indexValue));
	}

	static bool compareKeyValue(const AttribRecord& lhs, const AttribRecord& rhs) {
		return (lhs.first < rhs.first) || (!(rhs.first < lhs.first) && (lhs.second < rhs.second));
	}

	static bool compareKey(const AttribRecord& lhs, const AttribRecord& rhs) { return (lhs.first < rhs.first); }
};

// This class stores the information for each entry within the locality map
struct LocalityRecord final : public ReferenceCounted<LocalityRecord> {
	Reference<KeyValueMap> _dataMap;
	LocalityEntry _entryIndex;
	LocalityRecord(Reference<KeyValueMap> const& dataMap, int arrayIndex)
	  : _dataMap(dataMap), _entryIndex(arrayIndex) {}
	LocalityRecord(LocalityRecord const& entry) : _dataMap(entry._dataMap), _entryIndex(entry._entryIndex) {}
	LocalityRecord& operator=(LocalityRecord const& source) {
		_dataMap = source._dataMap;
		_entryIndex = source._entryIndex;
		return *this;
	}

	Optional<AttribValue> getValue(AttribKey indexKey) const { return _dataMap->getValue(indexKey); }

	bool isPresent(AttribKey indexKey, AttribValue indexValue) const {
		return _dataMap->isPresent(indexKey, indexValue);
	}

	int getMemoryUsed() const { return sizeof(_entryIndex) + sizeof(_dataMap) + _dataMap->getMemoryUsed(); }

	std::string toString() {
		std::stringstream ss;
		ss << "KeyValueArraySize:" << _dataMap->_keyvaluearray.size();
		for (int i = 0; i < _dataMap->size(); ++i) {
			AttribRecord attribRecord = _dataMap->_keyvaluearray[i]; // first is key, second is value
			ss << " KeyValueArrayIndex:" << i << " Key:" << attribRecord.first._id
			   << " Value:" << attribRecord.second._id;
		}

		return ss.str();
	}
};

// This class stores the information for string to integer map for keys and values
struct StringToIntMap final : public ReferenceCounted<StringToIntMap> {
	std::map<std::string, int> _hashmap;
	std::vector<std::string> _lookuparray;
	StringToIntMap() {}
	StringToIntMap(StringToIntMap const& source) : _hashmap(source._hashmap), _lookuparray(source._lookuparray) {}
	StringToIntMap& operator=(StringToIntMap const& source) {
		_hashmap = source._hashmap;
		_lookuparray = source._lookuparray;
		return *this;
	}
	void clear() {
		_hashmap.clear();
		_lookuparray.clear();
	}
	void copy(StringToIntMap const& source) {
		_hashmap = source._hashmap;
		_lookuparray = source._lookuparray;
	}
	std::string lookupString(int hashValue) const {
		return (hashValue < _lookuparray.size()) ? _lookuparray[hashValue] : "<missing>";
	}
	int convertString(std::string const& value) {
		int hashValue;
		auto itValue = _hashmap.find(value);
		if (itValue != _hashmap.end()) {
			hashValue = itValue->second;
		} else {
			hashValue = _hashmap.size();
			_hashmap[value] = hashValue;
			_lookuparray.push_back(value);
		}
		return hashValue;
	}
	int convertString(char const* value) { return convertString(std::string(value)); }
	int convertString(StringRef const& value) { return convertString(value.printable()); }
	int convertString(Optional<Standalone<StringRef>> const& value) {
		return convertString((value.present()) ? value.get().printable() : "<undefined>");
	}

	int getMemoryUsed() const {
		int memSize = sizeof(_hashmap) + sizeof(_lookuparray);
		for (auto& hashRecord : _hashmap) {
			memSize += hashRecord.first.size() + sizeof(hashRecord.second);
		}
		for (auto& lookup : _lookuparray) {
			memSize += lookup.size();
		}
		return memSize;
	}
};

extern const std::vector<LocalityEntry> emptyEntryArray;

#endif
