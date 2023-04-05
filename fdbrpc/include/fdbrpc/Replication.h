/*
 * Replication.h
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

#ifndef FLOW_REPLICATION_H
#define FLOW_REPLICATION_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"

struct LocalitySet : public ReferenceCounted<LocalitySet> {
public:
	LocalitySet(LocalitySet const& source)
	  : _keymap(source._keymap), _entryArray(source._entryArray), _mutableEntryArray(source._mutableEntryArray),
	    _keyValueArray(source._keyValueArray), _keyIndexArray(source._keyIndexArray), _cacheArray(source._cacheArray),
	    _localitygroup(source._localitygroup), _cachehits(source._cachehits), _cachemisses(source._cachemisses) {}
	LocalitySet(LocalitySet& localityGroup)
	  : _keymap(new StringToIntMap()), _localitygroup(&localityGroup), _cachehits(0), _cachemisses(0) {}
	virtual ~LocalitySet() {}

	virtual void addref() { ReferenceCounted<LocalitySet>::addref(); }
	virtual void delref() { ReferenceCounted<LocalitySet>::delref(); }

	bool selectReplicas(Reference<IReplicationPolicy> const& policy,
	                    std::vector<LocalityEntry> const& alsoServers,
	                    std::vector<LocalityEntry>& results) {
		Reference<LocalitySet> fromServers = Reference<LocalitySet>::addRef(this);
		return policy->selectReplicas(fromServers, alsoServers, results);
	}

	bool selectReplicas(Reference<IReplicationPolicy> const& policy, std::vector<LocalityEntry>& results) {
		return selectReplicas(policy, std::vector<LocalityEntry>(), results);
	}

	bool validate(Reference<IReplicationPolicy> const& policy) const {
		Reference<LocalitySet> const solutionSet = Reference<LocalitySet>::addRef((LocalitySet*)this);
		return policy->validate(solutionSet);
	}

	virtual void clear() {
		_entryArray.clear();
		_mutableEntryArray.clear();
		_keyValueArray.clear();
		_keyIndexArray.clear();
		_cacheArray.clear();
		_keymap->clear();
	}

	LocalitySet& copy(LocalitySet const& source) {
		_entryArray = source._entryArray;
		_mutableEntryArray = source._mutableEntryArray;
		_keyValueArray = source._keyValueArray;
		_keyIndexArray = source._keyIndexArray;
		_cacheArray = source._cacheArray;
		_cachehits = source._cachehits;
		_cachemisses = source._cachemisses;
		_keymap = source._keymap;
		_localitygroup = (LocalitySet*)source._localitygroup;
		return *this;
	}

	LocalitySet& deep_copy(LocalitySet const& source) {
		_entryArray = source._entryArray;
		_mutableEntryArray = source._mutableEntryArray;
		_keyValueArray = source._keyValueArray;
		_keyIndexArray = source._keyIndexArray;
		_cacheArray = source._cacheArray;
		_cachehits = source._cachehits;
		_cachemisses = source._cachemisses;
		_keymap->copy(*source._keymap);

		ASSERT(source._localitygroup == (LocalitySet*)source._localitygroup);
		_localitygroup = (LocalitySet*)this;
		return *this;
	}

	LocalityEntry const& getEntry(int localIndex) const {
		ASSERT((localIndex >= 0) && (localIndex < _entryArray.size()));
		return _entryArray[localIndex];
	}

	virtual Reference<LocalityRecord> const& getRecord(int localIndex) const {
		return _localitygroup->getRecord(getEntry(localIndex)._id);
	}

	// Return record array to help debug the locality information for servers
	virtual std::vector<Reference<LocalityRecord>> const& getRecordArray() const {
		return _localitygroup->getRecordArray();
	}

	Reference<LocalityRecord> const& getRecordViaEntry(LocalityEntry localEntry) const {
		return _localitygroup->getRecord(localEntry._id);
	}

	AttribKey getGroupKeyIndex(AttribKey indexKey) const { return _localitygroup->keyIndex(keyText(indexKey)); }

	Optional<AttribValue> const getValue(int recordIndex, AttribKey const& indexKey) const {
		return getRecord(recordIndex)->getValue(getGroupKeyIndex(indexKey));
	}

	Optional<AttribValue> const getValueViaEntry(LocalityEntry recordEntry, AttribKey const& indexKey) const {
		return getRecordViaEntry(recordEntry)->getValue(getGroupKeyIndex(indexKey));
	}

	Optional<AttribValue> const getValueViaGroupKey(int recordIndex, AttribKey const& indexKey) const {
		return getRecord(recordIndex)->getValue(indexKey);
	}

	Optional<AttribValue> const getValueViaGroupKey(LocalityEntry recordEntry, AttribKey const& indexKey) const {
		return getRecordViaEntry(recordEntry)->getValue(indexKey);
	}

	std::vector<LocalityEntry> const& getEntries() const { return _entryArray; }

	std::vector<LocalityEntry>& getMutableEntries() { return _mutableEntryArray; }

	std::vector<LocalityEntry> const& getGroupEntries() const { return _localitygroup->_entryArray; }

	std::string getLocalEntryInfo(int localIndex) const {
		const AttribKey machineKey = keyIndex("zoneid");
		auto& entry = getEntry(localIndex);
		auto value = getValueViaEntry(entry, machineKey);
		return format("(%3d) %-10s", entry._id, valueText(value.get()).c_str());
	}

	std::string getEntryInfo(LocalityEntry const& entry) const {
		const AttribKey machineHash = keyIndex("zoneid");
		auto value = getValueViaEntry(entry, machineHash);
		return format("(%3d) %-10s", entry._id, value.present() ? valueText(value.get()).c_str() : "(unset)");
	}

	static void staticDisplayEntries(LocalitySet const& localitySet,
	                                 std::vector<LocalityEntry> const& entryArray,
	                                 const char* name = "zone") {
		for (auto& entry : entryArray) {
			printf("   %s: %s\n", name, localitySet.getEntryInfo(entry).c_str());
		}
	}

	static void staticDisplayEntries(Reference<LocalitySet> const& fromServers,
	                                 std::vector<LocalityEntry> const& entryArray,
	                                 const char* name = "zone") {
		staticDisplayEntries(*fromServers, entryArray, name);
	}

	void DisplayEntries(const char* name = "zone") const { staticDisplayEntries(*this, getEntries(), name); }

	// This function is used to create an subset containing all of the entries within
	// the specified value for the given key
	// The returned LocalitySet contains the LocalityRecords that have the same value as
	// the indexValue under the same indexKey (e.g., zoneid)
	Reference<LocalitySet> restrict(AttribKey indexKey, AttribValue indexValue) {
		Reference<LocalitySet> localitySet;
		LocalityCacheRecord searchRecord(AttribRecord(indexKey, indexValue), localitySet);
		auto itKeyValue = std::lower_bound(
		    _cacheArray.begin(), _cacheArray.end(), searchRecord, LocalityCacheRecord::compareKeyValue);

		if ((itKeyValue != _cacheArray.end()) && (itKeyValue->_attribute == searchRecord._attribute)) {
			if (g_replicationdebug > 2)
				printf("Cache Hit:  (%2d) %-5s => (%4d) %-10s %3d from %3lu items\n",
				       indexKey._id,
				       keyText(indexKey).c_str(),
				       indexValue._id,
				       valueText(indexValue).c_str(),
				       itKeyValue->_resultset->size(),
				       _entryArray.size());
			_cachehits++;
			localitySet = itKeyValue->_resultset;
		} else {
			localitySet = makeReference<LocalitySet>(*_localitygroup);
			_cachemisses++;
			// If the key is not within the current key set, skip it because no items within
			// the current entry array has the key
			if (indexKey._id >= _keyIndexArray.size()) {
				if (g_replicationdebug > 2) {
					printf("invalid key index:%3d  array:%3lu  value: (%3d) %-10s\n",
					       indexKey._id,
					       _keyIndexArray.size(),
					       indexValue._id,
					       valueText(indexValue).c_str());
				}
			}
			// Get the equivalent key index from the locality group
			else {
				auto& groupKeyIndex = _keyIndexArray[indexKey._id];
				localitySet->_entryArray.reserve(_entryArray.size());
				for (auto& entry : _entryArray) {
					auto& record = getRecordViaEntry(entry);
					if (record->isPresent(groupKeyIndex, indexValue)) {
						localitySet->add(record, *this);
					}
				}
			}
			searchRecord._resultset = localitySet;
			_cacheArray.insert(itKeyValue, searchRecord);
			if (g_replicationdebug > 2)
				printf("Cache Miss: (%2d) %-5s => (%4d) %-10s %3d for %3lu items\n",
				       indexKey._id,
				       keyText(indexKey).c_str(),
				       indexValue._id,
				       valueText(indexValue).c_str(),
				       localitySet->size(),
				       _entryArray.size());
		}
		return localitySet;
	}

	// This function is used to create an subset containing the specified entries
	Reference<LocalitySet> restrict(std::vector<LocalityEntry> const& entryArray) {
		auto localitySet = makeReference<LocalitySet>(*_localitygroup);
		for (auto& entry : entryArray) {
			localitySet->add(getRecordViaEntry(entry), *this);
		}
		return localitySet;
	}

	// This function will append all of the entries matching
	// the specified value for the given key
	std::vector<LocalityEntry>& getMatches(std::vector<LocalityEntry>& entryArray,
	                                       AttribKey const& indexKey,
	                                       AttribValue const& indexValue) {
		for (auto& entry : _entryArray) {
			auto& record = getRecordViaEntry(entry);
			if (record->isPresent(indexKey, indexValue)) {
				entryArray.push_back(entry);
			}
		}
		return entryArray;
	}

	// Return a random entry
	LocalityEntry const& random() const { return _entryArray[deterministicRandom()->randomInt(0, _entryArray.size())]; }

	// Return a given number of random entries that are not within the
	// specified exception array
	bool random(std::vector<LocalityEntry>& randomEntries,
	            std::vector<LocalityEntry> const& exceptionArray,
	            unsigned int nRandomItems) {
		bool bComplete = true;
		int nItemsLeft = _mutableEntryArray.size();

		while (nRandomItems > 0) {
			if (nRandomItems > nItemsLeft || nItemsLeft <= 0) {
				bComplete = false;
				break;
			}
			while (nItemsLeft > 0) {
				auto itemIndex = deterministicRandom()->randomInt(0, nItemsLeft);
				auto item = _mutableEntryArray[itemIndex];

				nItemsLeft--;
				// Move the item to the end of the array, if not the last
				if (itemIndex < nItemsLeft) {
					_mutableEntryArray[itemIndex] = _mutableEntryArray[nItemsLeft];
					_mutableEntryArray[nItemsLeft] = item;
				}

				// Break, if item is not in exception list
				if (std::find(exceptionArray.begin(), exceptionArray.end(), item) != exceptionArray.end()) {
					// Break, if unable to fulfil the request
					if (nItemsLeft <= 0) {
						bComplete = false;
						break;
					}
				} else {
					randomEntries.push_back(item);
					nRandomItems--;
					break;
				}
			}
		}
		return bComplete;
	}

	// Return a random value for a specific key
	Optional<AttribValue> randomValue(AttribKey const& indexKey, std::vector<AttribValue>& usedArray) const {
		auto& valueArray = _keyValueArray[indexKey._id];
		int usedValues = usedArray.size();
		int checksLeft = valueArray.size();
		int indexValue;
		bool validValue;
		AttribValue valueValue;
		Optional<AttribValue> result;

		while (checksLeft > 0) {
			indexValue = deterministicRandom()->randomInt(0, checksLeft);
			valueValue = valueArray[indexValue];
			validValue = true;
			for (int usedLoop = 0; usedLoop < usedValues; usedLoop++) {
				if (usedArray[usedLoop] == valueValue) {
					validValue = false;
					usedValues--;
					if (usedLoop < usedValues - 1) {
						auto lastValue = usedArray[usedValues];
						usedArray[usedValues] = valueValue;
						usedArray[usedLoop] = lastValue;
						break;
					}
				}
			}
			if (validValue) {
				result = valueValue;
				break;
			}
			checksLeft--;
			if (indexValue < checksLeft) {
				auto lastValue = usedArray[checksLeft];
				usedArray[checksLeft] = valueValue;
				usedArray[indexValue] = lastValue;
			}
		}
		return result;
	}

	void cacheReport() const {
		printf("Cache: size:%5lu  mem:%7ldKb  hits:%9llu  misses:%9llu  records:%5lu\n",
		       _cacheArray.size(),
		       getMemoryUsed() / 1024L,
		       _cachehits,
		       _cachemisses,
		       _entryArray.size());
	}

	void clearCache() { _cacheArray.clear(); }

	AttribKey keyIndex(std::string const& value) const { return AttribKey(_keymap->convertString(value)); }
	AttribKey keyIndex(char const* value) const { return keyIndex(std::string(value)); }
	AttribKey keyIndex(StringRef const& value) const { return keyIndex(value.printable()); }
	AttribKey keyIndex(Optional<Standalone<StringRef>> const& value) const {
		return keyIndex((value.present()) ? value.get().printable() : "<undefined>");
	}

	std::string keyText(AttribKey indexKey) const { return _keymap->lookupString(indexKey._id); }
	std::string keyText(Optional<AttribKey> indexKey) const {
		return (indexKey.present()) ? keyText(AttribKey(indexKey.get()._id)) : "<undefined>";
	}

	AttribValue valueIndex(std::string const& value) const {
		return AttribValue(getGroupValueMap()->convertString(value));
	}
	AttribValue valueIndex(char const* value) const { return valueIndex(std::string(value)); }
	AttribValue valueIndex(StringRef const& value) const { return valueIndex(value.printable()); }
	AttribValue valueIndex(Optional<Standalone<StringRef>> const& value) const {
		return valueIndex((value.present()) ? value.get().printable() : "<undefined>");
	}

	std::string valueText(AttribValue indexValue) const { return getGroupValueMap()->lookupString(indexValue._id); }
	std::string valueText(Optional<AttribValue> indexValue) const {
		return (indexValue.present()) ? valueText(AttribValue(indexValue.get()._id)) : "<undefined>";
	}

	int size() const { return _entryArray.size(); }
	int empty() const { return _entryArray.empty(); }

	virtual void swapMutableRecords(int recordIndex1, int recordIndex2) {
		ASSERT((recordIndex1 >= 0) && (recordIndex1 < _mutableEntryArray.size()));
		ASSERT((recordIndex2 >= 0) && (recordIndex2 < _mutableEntryArray.size()));
		auto entry = _mutableEntryArray[recordIndex1];
		_mutableEntryArray[recordIndex1] = _mutableEntryArray[recordIndex2];
		_mutableEntryArray[recordIndex2] = entry;
	}

	virtual int getMemoryUsed() const {
		int memorySize = sizeof(_entryArray) + sizeof(LocalityEntry) * _entryArray.capacity() + sizeof(_cacheArray) +
		                 sizeof(LocalityCacheRecord) * _cacheArray.capacity() + sizeof(_keyIndexArray) +
		                 sizeof(AttribKey) * _keyIndexArray.capacity() + sizeof(_cachehits) + sizeof(_cachemisses) +
		                 _keymap->getMemoryUsed();
		for (auto& cacheRecord : _cacheArray) {
			memorySize += cacheRecord.getMemoryUsed();
		}
		for (auto& valueArray : _keyValueArray) {
			memorySize += sizeof(AttribValue) * valueArray.capacity();
		}
		return memorySize;
	}

protected:
	LocalityEntry const& add(LocalityEntry const& entry, LocalityData const& data) {
		_entryArray.push_back(entry);
		_mutableEntryArray.push_back(entry);

		// Ensure that the key value array is large enough to hold the values
		if (_keyValueArray.capacity() < _keyValueArray.size() + data._data.size()) {
			_keyValueArray.reserve(_keyValueArray.size() + data._data.size());
		}
		for (auto& dataPair : data._data) {
			auto indexKey = keyIndex(dataPair.first);
			auto indexValue = valueIndex(dataPair.second);
			if (indexKey._id >= _keyIndexArray.size()) {
				_keyIndexArray.resize(indexKey._id + 1);
				_keyIndexArray[indexKey._id] = AttribKey(getGroupKeyMap()->convertString(dataPair.first));
			}
			if (indexKey._id >= _keyValueArray.size()) {
				_keyValueArray.resize(indexKey._id + 1);
			}
			auto& valueArray = _keyValueArray[indexKey._id];
			auto lowerBound = std::lower_bound(valueArray.begin(), valueArray.end(), indexValue);
			if ((lowerBound == valueArray.end()) || (*lowerBound != indexValue)) {
				valueArray.insert(lowerBound, indexValue);
			}
		}
		clearCache();
		return _entryArray.back();
	}

	LocalityEntry const& add(Reference<LocalityRecord> const& record, LocalitySet const& localitySet) {
		_entryArray.push_back(record->_entryIndex);
		_mutableEntryArray.push_back(record->_entryIndex);

		// Ensure that the key value array is large enough to hold the values
		if (_keyValueArray.capacity() < _keyValueArray.size() + record->_dataMap->size()) {
			_keyValueArray.reserve(_keyValueArray.size() + record->_dataMap->size());
		}

		for (auto& keyValuePair : record->_dataMap->_keyvaluearray) {
			auto keyString = _localitygroup->keyText(keyValuePair.first);
			auto indexKey = keyIndex(keyString);
			auto& indexValue = keyValuePair.second;

			if (indexKey._id >= _keyIndexArray.size()) {
				_keyIndexArray.resize(indexKey._id + 1);
				_keyIndexArray[indexKey._id] = AttribKey(getGroupKeyMap()->convertString(keyString));
			}

			if (indexKey._id >= _keyValueArray.size()) {
				_keyValueArray.resize(indexKey._id + 1);
			}

			auto& valueArray = _keyValueArray[indexKey._id];
			auto lowerBound = std::lower_bound(valueArray.begin(), valueArray.end(), indexValue);
			if ((lowerBound == valueArray.end()) || (*lowerBound != indexValue)) {
				valueArray.insert(lowerBound, indexValue);
			}
		}
		clearCache();
		return _entryArray.back();
	}

	// This class stores the cache record for each entry within the locality set
	struct LocalityCacheRecord {
		AttribRecord _attribute;
		Reference<LocalitySet> _resultset;
		LocalityCacheRecord(AttribRecord const& attribute, Reference<LocalitySet> resultset)
		  : _attribute(attribute), _resultset(resultset) {}
		LocalityCacheRecord(LocalityCacheRecord const& source)
		  : _attribute(source._attribute), _resultset(source._resultset) {}
		virtual ~LocalityCacheRecord() {}
		LocalityCacheRecord& operator=(LocalityCacheRecord const& source) {
			_attribute = source._attribute;
			_resultset = source._resultset;
			return *this;
		}
		int getMemoryUsed() const { return sizeof(_attribute) + sizeof(_resultset) + _resultset->getMemoryUsed(); }
		static bool compareKeyValue(const LocalityCacheRecord& lhs, const LocalityCacheRecord& rhs) {
			return (lhs._attribute.first < rhs._attribute.first) ||
			       (!(rhs._attribute.first < lhs._attribute.first) && (lhs._attribute.second < rhs._attribute.second));
		}
		static bool compareKey(const LocalityCacheRecord& lhs, const LocalityCacheRecord& rhs) {
			return (lhs._attribute.first < rhs._attribute.first);
		}
	};

public:
	virtual Reference<StringToIntMap> const& getGroupValueMap() const { return _localitygroup->getGroupValueMap(); }

	virtual Reference<StringToIntMap> const& getGroupKeyMap() const { return _localitygroup->getGroupKeyMap(); }

	Reference<StringToIntMap> _keymap;

	virtual std::vector<std::vector<AttribValue>> const& getKeyValueArray() const { return _keyValueArray; }

protected:
	virtual Reference<StringToIntMap>& getGroupValueMap() { return _localitygroup->getGroupValueMap(); }

	virtual Reference<StringToIntMap>& getGroupKeyMap() { return _localitygroup->getGroupKeyMap(); }

protected:
	std::vector<LocalityEntry> _entryArray;
	std::vector<LocalityEntry> _mutableEntryArray; // Use to rearrange entries for fun
	std::vector<std::vector<AttribValue>> _keyValueArray;

	std::vector<AttribKey> _keyIndexArray;
	std::vector<LocalityCacheRecord> _cacheArray;

	LocalitySet* _localitygroup;
	long long unsigned int _cachehits;
	long long unsigned int _cachemisses;
};

struct LocalityGroup : public LocalitySet {
	LocalityGroup() : LocalitySet(*this), _valuemap(new StringToIntMap()) {}
	LocalityGroup(LocalityGroup const& source)
	  : LocalitySet(source), _recordArray(source._recordArray), _valuemap(source._valuemap) {}
	~LocalityGroup() override {}

	LocalityEntry const& add(LocalityData const& data) {
		// _recordArray.size() is the new entry index for the new data
		auto record = makeReference<LocalityRecord>(convertToAttribMap(data), _recordArray.size());
		_recordArray.push_back(record);
		return LocalitySet::add(record, *this);
	}

	void clear() override {
		LocalitySet::clear();
		_valuemap->clear();
		_recordArray.clear();
	}

	LocalityGroup& copy(LocalityGroup const& source) {
		LocalitySet::copy(source);
		_valuemap = source._valuemap;
		_recordArray = source._recordArray;
		return *this;
	}

	LocalityGroup& deep_copy(LocalityGroup const& source) {
		LocalitySet::deep_copy(source);
		_valuemap->copy(*source._valuemap);
		_recordArray = source._recordArray;
		return *this;
	}

	Reference<LocalityRecord> const& getRecord(int recordIndex) const override {
		ASSERT((recordIndex >= 0) && (recordIndex < _recordArray.size()));
		return _recordArray[recordIndex];
	}

	// Get the locality info for debug purpose
	std::vector<Reference<LocalityRecord>> const& getRecordArray() const override { return _recordArray; }

	int getMemoryUsed() const override {
		int memorySize = sizeof(_recordArray) + _keymap->getMemoryUsed();
		for (auto& record : _recordArray) {
			memorySize += record->getMemoryUsed();
		}
		return LocalitySet::getMemoryUsed() + memorySize;
	}

	// Convert locality data to sorted vector of int pairs
	Reference<KeyValueMap> convertToAttribMap(LocalityData const& data) {
		auto attribHashMap = makeReference<KeyValueMap>();
		for (auto& dataPair : data._data) {
			auto indexKey = keyIndex(dataPair.first);
			auto indexValue = valueIndex(dataPair.second);
			attribHashMap->_keyvaluearray.push_back(AttribRecord(indexKey, indexValue));
		}
		// Sort the attribute array
		std::sort(
		    attribHashMap->_keyvaluearray.begin(), attribHashMap->_keyvaluearray.end(), KeyValueMap::compareKeyValue);
		return attribHashMap;
	}

	Reference<StringToIntMap> const& getGroupValueMap() const override { return _valuemap; }

	Reference<StringToIntMap> const& getGroupKeyMap() const override { return _keymap; }

protected:
	Reference<StringToIntMap>& getGroupValueMap() override { return _valuemap; }

	Reference<StringToIntMap>& getGroupKeyMap() override { return _keymap; }

protected:
	std::vector<Reference<LocalityRecord>> _recordArray;
	Reference<StringToIntMap> _valuemap;
};

template <class V>
struct LocalityMap : public LocalityGroup {
	LocalityMap() : LocalityGroup() {}
	LocalityMap(LocalityMap const& source) : LocalityGroup(source), _objectArray(source._objectArray) {}
	~LocalityMap() override {}

	bool selectReplicas(Reference<IReplicationPolicy> const& policy,
	                    std::vector<LocalityEntry> const& alsoServers,
	                    std::vector<LocalityEntry>& entryResults,
	                    std::vector<V*>& results) {
		bool result;
		int entrySize = entryResults.size();
		int extraSpace = results.capacity() - results.size();
		if (extraSpace > 0) {
			entryResults.reserve(entryResults.size() + extraSpace);
		}
		result = LocalityGroup::selectReplicas(policy, alsoServers, entryResults);
		append(results, entryResults, entrySize);
		return result;
	}

	bool selectReplicas(Reference<IReplicationPolicy> const& policy,
	                    std::vector<LocalityEntry> const& alsoServers,
	                    std::vector<V*>& results) {
		std::vector<LocalityEntry> entryResults;
		return selectReplicas(policy, alsoServers, entryResults, results);
	}

	bool selectReplicas(Reference<IReplicationPolicy> const& policy, std::vector<V*>& results) {
		return selectReplicas(policy, std::vector<LocalityEntry>(), results);
	}

	void append(std::vector<V*>& objects, std::vector<LocalityEntry> const& entries, int firstItem = 0) const {
		int newItems = entries.size() - firstItem;
		if (newItems > 0)
			objects.reserve(objects.size() + newItems + objects.size());
		for (auto i = firstItem; i < entries.size(); i++) {
			objects.push_back((V*)getObject(entries[i]));
		}
	}

	LocalityEntry const& add(LocalityData const& data, V const* object) {
		_objectArray.push_back(object);
		return LocalityGroup::add(data);
	}

	V const* getObject(LocalityEntry const& recordEntry) const {
		ASSERT((recordEntry._id >= 0) && (recordEntry._id < _objectArray.size()));
		return _objectArray[recordEntry._id];
	}

	V const* getObject(LocalityRecord const& record) const { return getObject(record._entryIndex); }

	V const* getObject(Reference<LocalityRecord> const& record) const { return getObject(record->_entryIndex); }

	void clear() override {
		LocalityGroup::clear();
		_objectArray.clear();
	}

	int getMemoryUsed() const override {
		return LocalitySet::getMemoryUsed() + sizeof(_objectArray) + (sizeof(V*) * _objectArray.size());
	}

	std::vector<V const*> const& getObjects() const { return _objectArray; }

protected:
	std::vector<V const*> _objectArray;
};

#endif
