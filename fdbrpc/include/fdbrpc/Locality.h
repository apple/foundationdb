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

struct ProcessClass {
	constexpr static FileIdentifier file_identifier = 6697257;
	// This enum is stored in restartInfo.ini for upgrade tests, so be very careful about changing the existing items!
	enum ClassType {
		UnsetClass,
		StorageClass,
		TransactionClass,
		ResolutionClass,
		TesterClass,
		CommitProxyClass,
		MasterClass,
		StatelessClass,
		LogClass,
		ClusterControllerClass,
		LogRouterClass,
		FastRestoreClass,
		DataDistributorClass,
		CoordinatorClass,
		RatekeeperClass,
		RemovedClassPlaceholder1, // removing the name of removed functionality, but don't renumber subsequent entries.
		BackupClass,
		GrvProxyClass,
		BlobManagerClass,
		BlobWorkerClass,
		EncryptKeyProxyClass,
		ConsistencyScanClass,
		BlobMigratorClass,
		SimHTTPServerClass,
		InvalidClass = -1
	};

	// class is serialized by enum value, so it's important not to change the
	// enum value of a class. New classes should only be added to the end.
	static_assert(ProcessClass::UnsetClass == 0);
	static_assert(ProcessClass::StorageClass == 1);
	static_assert(ProcessClass::TransactionClass == 2);
	static_assert(ProcessClass::ResolutionClass == 3);
	static_assert(ProcessClass::TesterClass == 4);
	static_assert(ProcessClass::CommitProxyClass == 5);
	static_assert(ProcessClass::MasterClass == 6);
	static_assert(ProcessClass::StatelessClass == 7);
	static_assert(ProcessClass::LogClass == 8);
	static_assert(ProcessClass::ClusterControllerClass == 9);
	static_assert(ProcessClass::LogRouterClass == 10);
	static_assert(ProcessClass::FastRestoreClass == 11);
	static_assert(ProcessClass::DataDistributorClass == 12);
	static_assert(ProcessClass::CoordinatorClass == 13);
	static_assert(ProcessClass::RatekeeperClass == 14);
	// intentional gap at 15
	static_assert(ProcessClass::BackupClass == 16);
	static_assert(ProcessClass::GrvProxyClass == 17);
	static_assert(ProcessClass::BlobManagerClass == 18);
	static_assert(ProcessClass::BlobWorkerClass == 19);
	static_assert(ProcessClass::EncryptKeyProxyClass == 20);
	static_assert(ProcessClass::ConsistencyScanClass == 21);
	static_assert(ProcessClass::BlobMigratorClass == 22);
	static_assert(ProcessClass::SimHTTPServerClass == 23);
	static_assert(ProcessClass::InvalidClass == -1);

	enum Fitness {
		BestFit,
		GoodFit,
		UnsetFit,
		OkayFit,
		WorstFit,
		ExcludeFit,
		NeverAssign
	}; // cannot be larger than 7 because of leader election mask
	enum ClusterRole {
		Storage,
		TLog,
		CommitProxy,
		GrvProxy,
		Master,
		Resolver,
		LogRouter,
		ClusterController,
		DataDistributor,
		Ratekeeper,
		ConsistencyScan,
		BlobManager,
		BlobWorker,
		BlobMigrator,
		RemovedRolePlaceholder1,
		Backup,
		EncryptKeyProxy,
		Worker, // used for actor lineage tracking
		NoRole
	};
	enum ClassSource { CommandLineSource, AutoSource, DBSource, InvalidSource = -1 };
	int16_t _class;
	int16_t _source;

	// source is serialized by enum value, so it's important not to change the
	// enum value of a source. New sources should only be added to the end.
	static_assert(ProcessClass::CommandLineSource == 0);
	static_assert(ProcessClass::AutoSource == 1);
	static_assert(ProcessClass::DBSource == 2);

public:
	ProcessClass() : _class(UnsetClass), _source(CommandLineSource) {}
	ProcessClass(ClassType type, ClassSource source) : _class(type), _source(source) {}
	explicit ProcessClass(std::string s, ClassSource source);

	ProcessClass(std::string classStr, std::string sourceStr);

	ClassType classType() const { return (ClassType)_class; }
	ClassSource classSource() const { return (ClassSource)_source; }

	bool operator==(const ClassType& rhs) const { return _class == rhs; }
	bool operator!=(const ClassType& rhs) const { return _class != rhs; }

	bool operator==(const ProcessClass& rhs) const { return _class == rhs._class && _source == rhs._source; }
	bool operator!=(const ProcessClass& rhs) const { return _class != rhs._class || _source != rhs._source; }

	std::string toString() const;

	std::string sourceString() const;

	Fitness machineClassFitness(ClusterRole role) const;

	// To change this serialization, ProtocolVersion::ProcessClassValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _class, _source);
	}
};

struct LocalityData {
	std::map<Standalone<StringRef>, Optional<Standalone<StringRef>>> _data;

	alignas(8) static const StringRef keyProcessId;
	alignas(8) static const StringRef keyZoneId;
	alignas(8) static const StringRef keyDcId;
	alignas(8) static const StringRef keyMachineId;
	alignas(8) static const StringRef keyDataHallId;

public:
	LocalityData() {}

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
			if (infoString.length()) {
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
	if (!items.size())
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

struct ProcessData {
	LocalityData locality;
	ProcessClass processClass;
	NetworkAddress address;
	Optional<NetworkAddress> grpcAddress;

	ProcessData() {}
	ProcessData(LocalityData locality,
	            ProcessClass processClass,
	            NetworkAddress address,
	            Optional<NetworkAddress> grpcAddress)
	  : locality(locality), processClass(processClass), address(address), grpcAddress(grpcAddress) {}

	// To change this serialization, ProtocolVersion::WorkerListValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locality, processClass, address);

		if constexpr (!is_fb_function<Ar>) {
			if (ar.protocolVersion().hasGrpcEndpoint()) {
				serializer(ar, grpcAddress);
			}
		} else {
			serializer(ar, grpcAddress);
		}
	}

	struct sort_by_address {
		bool operator()(ProcessData const& a, ProcessData const& b) const { return a.address < b.address; }
	};
};

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
struct LBLocalityData<Interface, typename std::enable_if<Interface::LocationAwareLoadBalance>::type> {
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
