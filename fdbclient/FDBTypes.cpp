/*
 * FDBTypes.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/NativeAPI.actor.h"
#include <boost/algorithm/string.hpp>

KeyRangeRef toPrefixRelativeRange(KeyRangeRef range, Optional<KeyRef> prefix) {
	if (!prefix.present() || prefix.get().empty()) {
		return range;
	} else {
		KeyRef begin = range.begin.startsWith(prefix.get()) ? range.begin.removePrefix(prefix.get()) : allKeys.begin;
		KeyRef end = range.end.startsWith(prefix.get()) ? range.end.removePrefix(prefix.get()) : allKeys.end;
		return KeyRangeRef(begin, end);
	}
}

KeyRef keyBetween(const KeyRangeRef& keys) {
	int pos = 0; // will be the position of the first difference between keys.begin and keys.end
	int minSize = std::min(keys.begin.size(), keys.end.size());
	for (; pos < minSize && pos < CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT; pos++) {
		if (keys.begin[pos] != keys.end[pos]) {
			return keys.end.substr(0, pos + 1);
		}
	}

	// If one more character keeps us in the limit, and the latter key is simply
	// longer, then we only need one more byte of the end string.
	if (pos < CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT && keys.begin.size() < keys.end.size()) {
		return keys.end.substr(0, pos + 1);
	}

	return keys.end;
}

Key randomKeyBetween(const KeyRangeRef& keys) {
	if (keys.empty() || keys.singleKeyRange()) {
		return keys.end;
	}

	KeyRef begin = keys.begin;
	KeyRef end = keys.end;
	ASSERT(begin < end);
	if (begin.size() < end.size()) {
		// randomly append a char
		uint8_t maxChar = end[begin.size()] > 0 ? end[begin.size()] : end[begin.size()] + 1;
		uint8_t newChar = deterministicRandom()->randomInt(0, maxChar);
		return begin.withSuffix(StringRef(&newChar, 1));
	}

	int pos = 0; // will be the position of the first difference between keys.begin and keys.end
	for (; pos < end.size() && pos < CLIENT_KNOBS->KEY_SIZE_LIMIT; pos++) {
		if (keys.begin[pos] != keys.end[pos]) {
			break;
		}
	}
	ASSERT_LT(pos, end.size()); // otherwise, begin >= end

	// find the lowest char in range begin[pos+1, begin.size()) that is not \xff (255)
	int lowest = begin.size() - 1;
	for (; lowest > pos; lowest--) {
		if (begin[lowest] < 255) {
			Key res = begin;
			uint8_t* ptr = mutateString(res);
			*(ptr + lowest) = (uint8_t)deterministicRandom()->randomInt(begin[lowest] + 1, 256);
			return res;
		}
	}

	if (begin[pos] + 1 < end[pos]) {
		Key res = begin;
		uint8_t* ptr = mutateString(res);
		*(ptr + pos) = (uint8_t)deterministicRandom()->randomInt(begin[pos] + 1, end[pos]);
		return res;
	}

	if (begin.size() + 1 < CLIENT_KNOBS->KEY_SIZE_LIMIT) {
		// randomly append a char
		uint8_t newChar = deterministicRandom()->randomInt(1, 255);
		return begin.withSuffix(StringRef(&newChar, 1));
	}

	// no possible result
	return end;
}

TEST_CASE("/KeyRangeUtil/randomKeyBetween") {
	Key begin = "qwert"_sr;
	Key end = "qwertyu"_sr;
	Key res;
	for (int i = 0; i < 10; ++i) {
		res = randomKeyBetween(KeyRangeRef(begin, end));
		ASSERT(res > begin);
		ASSERT(res < end);
	}

	begin = "q"_sr;
	end = "q\x00"_sr;
	res = randomKeyBetween(KeyRangeRef(begin, end));
	ASSERT(res == end);

	begin = "aaaaaaa"_sr;
	end = "b"_sr;
	for (int i = 0; i < 10; ++i) {
		res = randomKeyBetween(KeyRangeRef(begin, end));
		ASSERT(res > begin);
		ASSERT(res < end);
	}
	return Void();
}

void KeySelectorRef::setKey(KeyRef const& key) {
	// There are no keys in the database with size greater than the max key size, so if this key selector has a key
	// which is large, then we can translate it to an equivalent key selector with a smaller key
	int64_t maxKeySize = getMaxKeySize(key);
	if (key.size() > maxKeySize) {
		this->key = key.substr(0, maxKeySize + 1);
	} else {
		this->key = key;
	}
}

void KeySelectorRef::setKeyUnlimited(KeyRef const& key) {
	this->key = key;
}

std::string KeySelectorRef::toString() const {
	if (offset > 0) {
		if (orEqual)
			return format("%d+firstGreaterThan(%s)", offset - 1, printable(key).c_str());
		else
			return format("%d+firstGreaterOrEqual(%s)", offset - 1, printable(key).c_str());
	} else {
		if (orEqual)
			return format("%d+lastLessOrEqual(%s)", offset, printable(key).c_str());
		else
			return format("%d+lastLessThan(%s)", offset, printable(key).c_str());
	}
}

std::string describe(const std::string& s) {
	return s;
}

std::string describe(UID const& item) {
	return item.shortString();
}

TEST_CASE("/KeyRangeUtil/KeyRangeComplement") {
	Key begin = "b"_sr;
	Key end = "y"_sr;
	KeyRangeRef range(begin, end);

	{
		Key b = "c"_sr;
		Key e = "f"_sr;
		std::vector<KeyRangeRef> result = range - KeyRangeRef(b, e);
		ASSERT(result.size() == 2);
		ASSERT(result[0] == KeyRangeRef("b"_sr, "c"_sr));
		ASSERT(result[1] == KeyRangeRef("f"_sr, "y"_sr));
	}

	{
		Key b = "1"_sr;
		Key e = "9"_sr;
		std::vector<KeyRangeRef> result = range - KeyRangeRef(b, e);
		ASSERT(result.size() == 1);
		ASSERT(result[0] == KeyRangeRef("b"_sr, "y"_sr));
	}

	{
		Key b = "a"_sr;
		Key e = "f"_sr;
		std::vector<KeyRangeRef> result = range - KeyRangeRef(b, e);
		ASSERT(result.size() == 1);
		ASSERT(result[0] == KeyRangeRef("f"_sr, "y"_sr));
	}

	{
		Key b = "f"_sr;
		Key e = "z"_sr;
		std::vector<KeyRangeRef> result = range - KeyRangeRef(b, e);
		ASSERT(result.size() == 1);
		ASSERT(result[0] == KeyRangeRef("b"_sr, "f"_sr));
	}

	{
		Key b = "a"_sr;
		Key e = "z"_sr;
		std::vector<KeyRangeRef> result = range - KeyRangeRef(b, e);
		ASSERT(result.size() == 0);
	}

	return Void();
}

std::string KeyValueStoreType::getStoreTypeStr(const StoreType& storeType) {
	switch (storeType) {
	case SSD_BTREE_V1:
		return "ssd-1";
	case SSD_BTREE_V2:
		return "ssd-2";
	case SSD_REDWOOD_V1:
		return "ssd-redwood-1";
	case SSD_ROCKSDB_V1:
		return "ssd-rocksdb-v1";
	case SSD_SHARDED_ROCKSDB:
		return "ssd-sharded-rocksdb";
	case MEMORY:
		return "memory";
	case MEMORY_RADIXTREE:
		return "memory-radixtree";
	case NONE:
		return "none";
	default:
		return "unknown";
	}
}

KeyValueStoreType KeyValueStoreType::fromString(const std::string& str) {
	static std::map<std::string, StoreType> names = { { "ssd-1", SSD_BTREE_V1 },
		                                              { "ssd-2", SSD_BTREE_V2 },
		                                              { "ssd", SSD_BTREE_V2 },
		                                              { "redwood", SSD_REDWOOD_V1 },
		                                              { "ssd-redwood-1", SSD_REDWOOD_V1 },
		                                              { "ssd-redwood-1-experimental", SSD_REDWOOD_V1 },
		                                              { "ssd-rocksdb-v1", SSD_ROCKSDB_V1 },
		                                              { "ssd-sharded-rocksdb", SSD_SHARDED_ROCKSDB },
		                                              { "memory", MEMORY },
		                                              { "memory-radixtree", MEMORY_RADIXTREE },
		                                              { "none", NONE } };
	auto it = names.find(str);
	if (it == names.end()) {
		throw unknown_storage_engine();
	}
	return it->second;
}

TEST_CASE("/PerpetualStorageWiggleLocality/Validation") {
	ASSERT(isValidPerpetualStorageWiggleLocality("aaa:bbb"));
	ASSERT(isValidPerpetualStorageWiggleLocality("instance_id:FDB0401023121"));
	ASSERT(isValidPerpetualStorageWiggleLocality("machineid:pv47p01if-infs11081401.pv.if.apple.com"));
	ASSERT(isValidPerpetualStorageWiggleLocality("processid:0b36eaf96eb34b4b702d1bbcb1b49773"));
	ASSERT(isValidPerpetualStorageWiggleLocality("zoneid:pv47-1108"));
	ASSERT(isValidPerpetualStorageWiggleLocality(
	    "zoneid:pv47-1108;instance_id:FDB0401023121;processid:0b36eaf96eb34b4b702d1bbcb1b49773;machineid:pv47p01if-"
	    "infs11081401.pv.if.apple.com"));
	ASSERT(isValidPerpetualStorageWiggleLocality("0"));

	ASSERT(!isValidPerpetualStorageWiggleLocality("aaa:bbb;"));
	ASSERT(!isValidPerpetualStorageWiggleLocality("aaa:bbb;ccc"));
	ASSERT(!isValidPerpetualStorageWiggleLocality(""));

	return Void();
}

std::vector<std::pair<Optional<Value>, Optional<Value>>> ParsePerpetualStorageWiggleLocality(
    const std::string& localityKeyValues) {
	// parsing format is like "datahall:0<;locality:filter>"
	ASSERT(isValidPerpetualStorageWiggleLocality(localityKeyValues));

	std::vector<std::pair<Optional<Value>, Optional<Value>>> parsedLocalities;

	if (localityKeyValues == "0") {
		return parsedLocalities;
	}

	std::vector<std::string> splitLocalityKeyValues;
	boost::split(splitLocalityKeyValues, localityKeyValues, [](char c) { return c == ';'; });

	for (const auto& localityKeyValue : splitLocalityKeyValues) {
		ASSERT(!localityKeyValue.empty());

		// get key and value from perpetual_storage_wiggle_locality.
		int split = localityKeyValue.find(':');
		auto key = Optional<Value>(ValueRef((uint8_t*)localityKeyValue.c_str(), split));
		auto value = Optional<Value>(
		    ValueRef((uint8_t*)localityKeyValue.c_str() + split + 1, localityKeyValue.size() - split - 1));
		parsedLocalities.push_back(std::make_pair(key, value));
	}

	return parsedLocalities;
}

bool localityMatchInList(const std::vector<std::pair<Optional<Value>, Optional<Value>>>& localityKeyValues,
                         const LocalityData& locality) {
	for (const auto& [localityKey, localityValue] : localityKeyValues) {
		if (locality.get(localityKey.get()) == localityValue) {
			return true;
		}
	}
	return false;
}

TEST_CASE("/PerpetualStorageWiggleLocality/ParsePerpetualStorageWiggleLocality") {
	{
		auto localityKeyValues = ParsePerpetualStorageWiggleLocality("aaa:bbb");
		ASSERT(localityKeyValues.size() == 1);
		ASSERT(localityKeyValues[0].first.get() == "aaa");
		ASSERT(localityKeyValues[0].second.get() == "bbb");

		{
			LocalityData locality;
			locality.set("aaa"_sr, "bbb"_sr);
			ASSERT(localityMatchInList(localityKeyValues, locality));
		}

		{
			LocalityData locality;
			locality.set("aaa"_sr, "ccc"_sr);
			ASSERT(!localityMatchInList(localityKeyValues, locality));
		}
	}

	{
		auto localityKeyValues = ParsePerpetualStorageWiggleLocality("aaa:bbb;ccc:ddd");
		ASSERT(localityKeyValues.size() == 2);
		ASSERT(localityKeyValues[0].first.get() == "aaa");
		ASSERT(localityKeyValues[0].second.get() == "bbb");
		ASSERT(localityKeyValues[1].first.get() == "ccc");
		ASSERT(localityKeyValues[1].second.get() == "ddd");

		{
			LocalityData locality;
			locality.set("aaa"_sr, "bbb"_sr);
			ASSERT(localityMatchInList(localityKeyValues, locality));
		}

		{
			LocalityData locality;
			locality.set("ccc"_sr, "ddd"_sr);
			ASSERT(localityMatchInList(localityKeyValues, locality));
		}

		{
			LocalityData locality;
			locality.set("aaa"_sr, "ddd"_sr);
			ASSERT(!localityMatchInList(localityKeyValues, locality));
		}
	}

	{
		auto localityKeyValues = ParsePerpetualStorageWiggleLocality("aaa:111;bbb:222;ccc:3dd");
		ASSERT(localityKeyValues.size() == 3);
		ASSERT(localityKeyValues[0].first.get() == "aaa");
		ASSERT(localityKeyValues[0].second.get() == "111");
		ASSERT(localityKeyValues[1].first.get() == "bbb");
		ASSERT(localityKeyValues[1].second.get() == "222");
		ASSERT(localityKeyValues[2].first.get() == "ccc");
		ASSERT(localityKeyValues[2].second.get() == "3dd");

		{
			LocalityData locality;
			locality.set("aaa"_sr, "111"_sr);
			ASSERT(localityMatchInList(localityKeyValues, locality));
		}

		{
			LocalityData locality;
			locality.set("bbb"_sr, "222"_sr);
			ASSERT(localityMatchInList(localityKeyValues, locality));
		}

		{
			LocalityData locality;
			locality.set("ccc"_sr, "222"_sr);
			ASSERT(!localityMatchInList(localityKeyValues, locality));
		}
	}

	{
		auto localityKeyValues = ParsePerpetualStorageWiggleLocality("0");
		ASSERT(localityKeyValues.empty());

		{
			LocalityData locality;
			locality.set("aaa"_sr, "111"_sr);
			ASSERT(!localityMatchInList(localityKeyValues, locality));
		}
	}
	return Void();
}