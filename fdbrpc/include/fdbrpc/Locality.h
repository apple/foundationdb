/*
 * Locality.h
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

#ifndef FLOW_LOCALITY_H
#define FLOW_LOCALITY_H
#pragma once

#include "flow/flow.h"

struct LocalityData {
	std::map<Standalone<StringRef>, Optional<Standalone<StringRef>>> _data;

	alignas(8) static const StringRef keyProcessId;
	alignas(8) static const StringRef keyZoneId;
	alignas(8) static const StringRef keyDcId;
	alignas(8) static const StringRef keyMachineId;
	alignas(8) static const StringRef keyDataHallId;

public:
	LocalityData() = default;

	LocalityData(Optional<Standalone<StringRef>> processID,
	             Optional<Standalone<StringRef>> zoneID,
	             Optional<Standalone<StringRef>> MachineID,
	             Optional<Standalone<StringRef>> dcID) {
		_data[keyProcessId] = processID;
		_data[keyZoneId] = zoneID;
		_data[keyMachineId] = MachineID;
		_data[keyDcId] = dcID;
	}

	bool operator==(LocalityData const& rhs) const {
		return ((_data.size() == rhs._data.size()) && (std::equal(_data.begin(), _data.end(), rhs._data.begin())));
	}
	bool operator!=(LocalityData const& rhs) const { return !(*this == rhs); }

	Optional<Standalone<StringRef>> get(StringRef key) const {
		auto pos = _data.find(key);
		return (pos == _data.end()) ? Optional<Standalone<StringRef>>() : pos->second;
	}

	void set(StringRef key, Optional<Standalone<StringRef>> value) { _data[key] = value; }

	bool isPresent(StringRef key) const { return (_data.find(key) != _data.end()); }
	bool isPresent(StringRef key, Optional<Standalone<StringRef>> value) const {
		auto pos = _data.find(key);
		return (pos != _data.end()) ? false : (pos->second == value);
	}

	std::string describeValue(StringRef key) const {
		auto value = get(key);
		return (value.present()) ? value.get().toString() : "[unset]";
	}

	std::string describeZone() const { return describeValue(keyZoneId); }
	std::string describeDataHall() const { return describeValue(keyDataHallId); }
	std::string describeDcId() const { return describeValue(keyDcId); }
	std::string describeMachineId() const { return describeValue(keyMachineId); }
	std::string describeProcessId() const { return describeValue(keyProcessId); }

	Optional<Standalone<StringRef>> processId() const { return get(keyProcessId); }
	Optional<Standalone<StringRef>> zoneId() const { return get(keyZoneId); }
	Optional<Standalone<StringRef>> machineId() const { return get(keyMachineId); } // default is ""
	Optional<Standalone<StringRef>> dcId() const { return get(keyDcId); }
	Optional<Standalone<StringRef>> dataHallId() const { return get(keyDataHallId); }

	std::string toString() const {
		std::string infoString;
		for (auto it = _data.rbegin(); !(it == _data.rend()); ++it) {
			if (!infoString.empty()) {
				infoString += " ";
			}
			infoString += it->first.printable() + "=";
			infoString += (it->second.present()) ? it->second.get().printable() : "[unset]";
		}
		return infoString;
	}

	// Convert locality fields to a JSON object.  This is a template because it works with JSONBuilder, StatusObject,
	// and json_spirit::mObject, and none of these types are in the fdbrpc/ project.
	template <typename JSONType>
	JSONType toJSON() const {
		JSONType obj;

		for (auto it = _data.begin(); it != _data.end(); it++) {
			if (it->second.present()) {
				obj[it->first.toString()] = it->second.get().toString();
			} else {
				obj[it->first.toString()] = nullptr;
			}
		}

		return obj;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		// Locality is persisted in the database inside StorageServerInterface, so changes here have to be
		// versioned carefully!
		if constexpr (is_fb_function<Ar>) {
			serializer(ar, _data);
		} else {
			ASSERT_WE_THINK(ar.protocolVersion().hasLocality());
			Standalone<StringRef> key;
			Optional<Standalone<StringRef>> value;
			uint64_t mapSize = (uint64_t)_data.size();
			serializer(ar, mapSize);
			if (ar.isDeserializing) {
				for (size_t i = 0; i < mapSize; i++) {
					serializer(ar, key, value);
					_data[key] = value;
				}
			} else {
				for (auto it = _data.begin(); it != _data.end(); it++) {
					key = it->first;
					value = it->second;
					serializer(ar, key, value);
				}
			}
		}
	}

	std::map<std::string, std::string> getAllData() const {
		std::map<std::string, std::string> data;
		for (const auto& d : _data) {
			if (d.second.present()) {
				data[d.first.toString()] = d.second.get().toString();
			}
		}
		return data;
	}

	static const UID UNSET_ID;
	alignas(8) static const StringRef ExcludeLocalityPrefix;
};

static std::string describe(std::vector<LocalityData> const& items, StringRef const key, int max_items = -1) {
	if (items.empty())
		return "[no items]";
	std::string s;
	int count = 0;
	for (auto const& item : items) {
		if (++count > max_items && max_items >= 0)
			break;
		if (count > 1)
			s += ",";
		s += item.describeValue(key);
	}
	return s;
}
inline std::string describeZones(std::vector<LocalityData> const& items, int max_items = -1) {
	return describe(items, LocalityData::keyZoneId, max_items);
}
inline std::string describeDataHalls(std::vector<LocalityData> const& items, int max_items = -1) {
	return describe(items, LocalityData::keyDataHallId, max_items);
}

template <class Interface, class Enable = void>
struct LBLocalityData {
	enum { Present = 0 };
	static LocalityData getLocality(Interface const&) { return LocalityData(); }
	static NetworkAddress getAddress(Interface const&) { return NetworkAddress(); }
	static bool alwaysFresh() { return true; }
};

// Template specialization that only works for interfaces with a .locality member.
//   If an interface has a .locality it must also have a .address()
template <class Interface>
    requires(static_cast<bool>(Interface::LocationAwareLoadBalance))
struct LBLocalityData<Interface> {
	enum { Present = 1 };
	static LocalityData getLocality(Interface const& i) { return i.locality; }
	static NetworkAddress getAddress(Interface const& i) { return i.address(); }
	static bool alwaysFresh() { return Interface::AlwaysFresh; }
};

struct LBDistance {
	enum Type { SAME_MACHINE = 0, SAME_DC = 1, DISTANT = 2 };
};

LBDistance::Type loadBalanceDistance(LocalityData const& localLoc,
                                     LocalityData const& otherLoc,
                                     NetworkAddress const& otherAddr);

#endif
