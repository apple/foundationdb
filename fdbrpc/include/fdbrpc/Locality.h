/*
 * Locality.h
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
		StorageCacheClass,
		BackupClass,
		GrvProxyClass,
		BlobManagerClass,
		BlobWorkerClass,
		EncryptKeyProxyClass,
		InvalidClass = -1
	};

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
		BlobManager,
		BlobWorker,
		StorageCache,
		Backup,
		EncryptKeyProxy,
		Worker, // used for actor lineage tracking
		NoRole
	};
	enum ClassSource { CommandLineSource, AutoSource, DBSource, InvalidSource = -1 };
	int16_t _class;
	int16_t _source;

public:
	ProcessClass() : _class(UnsetClass), _source(CommandLineSource) {}
	ProcessClass(ClassType type, ClassSource source) : _class(type), _source(source) {}
	// clang-format off
	explicit ProcessClass( std::string s, ClassSource source ) : _source( source ) {
		if (s=="storage") _class = StorageClass;
		else if (s=="transaction") _class = TransactionClass;
		else if (s=="resolution") _class = ResolutionClass;
		else if (s=="commit_proxy") _class = CommitProxyClass;
		else if (s=="proxy") {
			_class = CommitProxyClass;
			printf("WARNING: 'proxy' machine class is deprecated and will be automatically converted "
					"'commit_proxy' machine class. Please use 'grv_proxy' or 'commit_proxy' specifically\n");
		}
		else if (s=="grv_proxy") _class = GrvProxyClass;
		else if (s=="master") _class = MasterClass;
		else if (s=="test") _class = TesterClass;
		else if (s=="unset") _class = UnsetClass;
		else if (s=="stateless") _class = StatelessClass;
		else if (s=="log") _class = LogClass;
		else if (s=="router") _class = LogRouterClass;
		else if (s=="cluster_controller") _class = ClusterControllerClass;
		else if (s=="fast_restore") _class = FastRestoreClass;
		else if (s=="data_distributor") _class = DataDistributorClass;
		else if (s=="coordinator") _class = CoordinatorClass;
		else if (s=="ratekeeper") _class = RatekeeperClass;
		else if (s=="blob_manager") _class = BlobManagerClass;
		else if (s=="blob_worker") _class = BlobWorkerClass;
		else if (s=="storage_cache") _class = StorageCacheClass;
		else if (s=="backup") _class = BackupClass;
		else if (s=="encrypt_key_proxy") _class = EncryptKeyProxyClass;
		else _class = InvalidClass;
	}

	ProcessClass( std::string classStr, std::string sourceStr ) {
		if (classStr=="storage") _class = StorageClass;
		else if (classStr=="transaction") _class = TransactionClass;
		else if (classStr=="resolution") _class = ResolutionClass;
		else if (classStr=="commit_proxy") _class = CommitProxyClass;
		else if (classStr=="proxy") {
			_class = CommitProxyClass;
			printf("WARNING: 'proxy' machine class is deprecated and will be automatically converted "
					"'commit_proxy' machine class. Please use 'grv_proxy' or 'commit_proxy' specifically\n");
		}
		else if (classStr=="grv_proxy") _class = GrvProxyClass;
		else if (classStr=="master") _class = MasterClass;
		else if (classStr=="test") _class = TesterClass;
		else if (classStr=="unset") _class = UnsetClass;
		else if (classStr=="stateless") _class = StatelessClass;
		else if (classStr=="log") _class = LogClass;
		else if (classStr=="router") _class = LogRouterClass;
		else if (classStr=="cluster_controller") _class = ClusterControllerClass;
		else if (classStr=="fast_restore") _class = FastRestoreClass;
		else if (classStr=="data_distributor") _class = DataDistributorClass;
		else if (classStr=="coordinator") _class = CoordinatorClass;
		else if (classStr=="ratekeeper") _class = RatekeeperClass;
		else if (classStr=="blob_manager") _class = BlobManagerClass;
		else if (classStr=="blob_worker") _class = BlobWorkerClass;
		else if (classStr=="storage_cache") _class = StorageCacheClass;
		else if (classStr=="backup") _class = BackupClass;
		else if (classStr=="encrypt_key_proxy") _class = EncryptKeyProxyClass;
		else _class = InvalidClass;

		if (sourceStr=="command_line") _source = CommandLineSource;
		else if (sourceStr=="configure_auto") _source = AutoSource;
		else if (sourceStr=="set_class") _source = DBSource;
		else _source = InvalidSource;
	}

	ClassType classType() const { return (ClassType)_class; }
	ClassSource classSource() const { return (ClassSource)_source; }

	bool operator == ( const ClassType& rhs ) const { return _class == rhs; }
	bool operator != ( const ClassType& rhs ) const { return _class != rhs; }

	bool operator == ( const ProcessClass& rhs ) const { return _class == rhs._class && _source == rhs._source; }
	bool operator != ( const ProcessClass& rhs ) const { return _class != rhs._class || _source != rhs._source; }

	std::string toString() const {
		switch (_class) {
			case UnsetClass: return "unset";
			case StorageClass: return "storage";
			case TransactionClass: return "transaction";
			case ResolutionClass: return "resolution";
			case CommitProxyClass: return "commit_proxy";
			case GrvProxyClass: return "grv_proxy";
			case MasterClass: return "master";
			case TesterClass: return "test";
			case StatelessClass: return "stateless";
			case LogClass: return "log";
			case LogRouterClass: return "router";
			case ClusterControllerClass: return "cluster_controller";
			case FastRestoreClass: return "fast_restore";
			case DataDistributorClass: return "data_distributor";
			case CoordinatorClass: return "coordinator";
			case RatekeeperClass: return "ratekeeper";
			case BlobManagerClass: return "blob_manager";
			case BlobWorkerClass: return "blob_worker";
			case StorageCacheClass: return "storage_cache";
			case BackupClass: return "backup";
			case EncryptKeyProxyClass: return "encrypt_key_proxy";
			default: return "invalid";
		}
	}
	// clang-format on

	std::string sourceString() const {
		switch (_source) {
		case CommandLineSource:
			return "command_line";
		case AutoSource:
			return "configure_auto";
		case DBSource:
			return "set_class";
		default:
			return "invalid";
		}
	}

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

	static const StringRef keyProcessId;
	static const StringRef keyZoneId;
	static const StringRef keyDcId;
	static const StringRef keyMachineId;
	static const StringRef keyDataHallId;

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

	template <class Ar>
	void serialize(Ar& ar) {
		// Locality is persisted in the database inside StorageServerInterface, so changes here have to be
		// versioned carefully!
		if constexpr (is_fb_function<Ar>) {
			serializer(ar, _data);
		} else {
			if (ar.protocolVersion().hasLocality()) {
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
			} else {
				ASSERT(ar.isDeserializing);
				UID zoneId, dcId, processId;
				serializer(ar, zoneId, dcId);
				set(keyZoneId, Standalone<StringRef>(zoneId.toString()));
				set(keyDcId, Standalone<StringRef>(dcId.toString()));

				if (ar.protocolVersion().hasProcessID()) {
					serializer(ar, processId);
					set(keyProcessId, Standalone<StringRef>(processId.toString()));
				} else {
					int _machineClass = ProcessClass::UnsetClass;
					serializer(ar, _machineClass);
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
	static const StringRef ExcludeLocalityKeyMachineIdPrefix;
	static const StringRef ExcludeLocalityPrefix;
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

	ProcessData() {}
	ProcessData(LocalityData locality, ProcessClass processClass, NetworkAddress address)
	  : locality(locality), processClass(processClass), address(address) {}

	// To change this serialization, ProtocolVersion::WorkerListValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locality, processClass, address);
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
