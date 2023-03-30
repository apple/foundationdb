/*
 * MetaclusterTypes.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#ifndef METACLUSTER_METACLUSTERTYPES_H
#define METACLUSTER_METACLUSTERTYPES_H
#pragma once

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "flow/flat_buffers.h"

namespace metacluster {

FDB_BOOLEAN_PARAM(IgnoreCapacityLimit);
FDB_BOOLEAN_PARAM(IsRestoring);

// Represents the various states that a tenant could be in. Only applies to metacluster, not standalone clusters.
// In a metacluster, a tenant on the management cluster could be in the other states while changes are applied to the
// data cluster.
//
// REGISTERING - the tenant has been created on the management cluster and is being created on the data cluster
// READY - the tenant has been created on both clusters, is active, and is consistent between the two clusters
// REMOVING - the tenant has been marked for removal and is being removed on the data cluster
// UPDATING_CONFIGURATION - the tenant configuration has changed on the management cluster and is being applied to the
//                          data cluster
// RENAMING - the tenant is in the process of being renamed
// ERROR - the tenant is in an error state
//
// A tenant in any configuration is allowed to be removed. Only tenants in the READY or UPDATING_CONFIGURATION phases
// can have their configuration updated. A tenant must not exist or be in the REGISTERING phase to be created. To be
// renamed, a tenant must be in the READY or RENAMING state. In the latter case, the rename destination must match
// the original rename attempt.
//
// If an operation fails and the tenant is left in a non-ready state, re-running the same operation is legal. If
// successful, the tenant will return to the READY state.
enum class TenantState { REGISTERING, READY, REMOVING, UPDATING_CONFIGURATION, RENAMING, ERROR };

std::string tenantStateToString(TenantState tenantState);
TenantState stringToTenantState(std::string stateStr);

struct ClusterUsage {
	int numTenantGroups = 0;

	ClusterUsage() = default;
	ClusterUsage(int numTenantGroups) : numTenantGroups(numTenantGroups) {}

	json_spirit::mObject toJson() const;

	bool operator==(const ClusterUsage& other) const noexcept { return numTenantGroups == other.numTenantGroups; }
	bool operator!=(const ClusterUsage& other) const noexcept { return !(*this == other); }
	bool operator<(const ClusterUsage& other) const noexcept { return numTenantGroups < other.numTenantGroups; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, numTenantGroups);
	}
};

// Represents the various states that a data cluster could be in.
//
// REGISTERING - the data cluster is being registered with the metacluster
// READY - the data cluster is active
// REMOVING - the data cluster is being removed and cannot have its configuration changed or any tenants created
// RESTORING - the data cluster is being restored and cannot have its configuration changed or any tenants
//             created/updated/deleted.
enum class DataClusterState { REGISTERING, READY, REMOVING, RESTORING };

struct DataClusterEntry {
	constexpr static FileIdentifier file_identifier = 929511;

	static std::string clusterStateToString(DataClusterState clusterState);
	static DataClusterState stringToClusterState(std::string stateStr);

	UID id;
	ClusterUsage capacity;
	ClusterUsage allocated;

	DataClusterState clusterState = DataClusterState::READY;

	DataClusterEntry() = default;
	DataClusterEntry(ClusterUsage capacity) : capacity(capacity) {}
	DataClusterEntry(UID id, ClusterUsage capacity, ClusterUsage allocated)
	  : id(id), capacity(capacity), allocated(allocated) {}

	// Returns true if all configurable properties match
	bool matchesConfiguration(DataClusterEntry const& other) const { return capacity == other.capacity; }

	bool hasCapacity() const { return allocated < capacity; }

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static DataClusterEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<DataClusterEntry>(value, IncludeVersion());
	}

	json_spirit::mObject toJson() const;

	bool operator==(DataClusterEntry const& other) const {
		return id == other.id && capacity == other.capacity && allocated == other.allocated &&
		       clusterState == other.clusterState;
	}

	bool operator!=(DataClusterEntry const& other) const { return !(*this == other); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, capacity, allocated, clusterState);
	}
};

struct DataClusterMetadata {
	constexpr static FileIdentifier file_identifier = 5573993;

	DataClusterEntry entry;
	ClusterConnectionString connectionString;

	DataClusterMetadata() = default;
	DataClusterMetadata(DataClusterEntry const& entry, ClusterConnectionString const& connectionString)
	  : entry(entry), connectionString(connectionString) {}

	bool matchesConfiguration(DataClusterMetadata const& other) const {
		return entry.matchesConfiguration(other.entry) && connectionString == other.connectionString;
	}

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static DataClusterMetadata decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<DataClusterMetadata>(value, IncludeVersion());
	}

	json_spirit::mValue toJson() const {
		json_spirit::mObject obj = entry.toJson();
		obj["connection_string"] = connectionString.toString();
		return obj;
	}

	bool operator==(DataClusterMetadata const& other) const {
		return entry == other.entry && connectionString == other.connectionString;
	}

	bool operator!=(DataClusterMetadata const& other) const { return !(*this == other); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connectionString, entry);
	}
};

struct MetaclusterTenantMapEntry {
	constexpr static FileIdentifier file_identifier = 12247338;

	int64_t id = -1;
	Key prefix;
	TenantName tenantName;
	metacluster::TenantState tenantState = metacluster::TenantState::READY;
	TenantAPI::TenantLockState tenantLockState = TenantAPI::TenantLockState::UNLOCKED;
	Optional<UID> tenantLockId;
	Optional<TenantGroupName> tenantGroup;
	ClusterName assignedCluster;
	int64_t configurationSequenceNum = 0;
	Optional<TenantName> renameDestination;

	// Can be set to an error string if the tenant is in the ERROR state
	std::string error;

	MetaclusterTenantMapEntry();
	MetaclusterTenantMapEntry(int64_t id, TenantName tenantName, metacluster::TenantState tenantState);
	MetaclusterTenantMapEntry(int64_t id,
	                          TenantName tenantName,
	                          metacluster::TenantState tenantState,
	                          Optional<TenantGroupName> tenantGroup);

	static MetaclusterTenantMapEntry fromTenantMapEntry(TenantMapEntry const& source);
	TenantMapEntry toTenantMapEntry() const;

	void setId(int64_t id);
	std::string toJson() const;

	bool matchesConfiguration(MetaclusterTenantMapEntry const& other) const;
	bool matchesConfiguration(TenantMapEntry const& other) const;
	void configure(Standalone<StringRef> parameter, Optional<Value> value);

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static MetaclusterTenantMapEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<MetaclusterTenantMapEntry>(value, IncludeVersion());
	}

	bool operator==(MetaclusterTenantMapEntry const& other) const;
	bool operator!=(MetaclusterTenantMapEntry const& other) const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           id,
		           tenantName,
		           tenantState,
		           tenantLockState,
		           tenantLockId,
		           tenantGroup,
		           assignedCluster,
		           configurationSequenceNum,
		           renameDestination,
		           error);
		if constexpr (Ar::isDeserializing) {
			if (id >= 0) {
				prefix = TenantAPI::idToPrefix(id);
			}
			ASSERT(tenantState >= metacluster::TenantState::REGISTERING &&
			       tenantState <= metacluster::TenantState::ERROR);
		}
	}
};

struct MetaclusterTenantGroupEntry {
	constexpr static FileIdentifier file_identifier = 1082739;

	ClusterName assignedCluster;

	MetaclusterTenantGroupEntry() = default;
	MetaclusterTenantGroupEntry(ClusterName assignedCluster) : assignedCluster(assignedCluster) {}

	json_spirit::mObject toJson() const;

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static MetaclusterTenantGroupEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<MetaclusterTenantGroupEntry>(value, IncludeVersion());
	}

	bool operator==(MetaclusterTenantGroupEntry const& other) const;
	bool operator!=(MetaclusterTenantGroupEntry const& other) const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, assignedCluster);
	}
};

class MetaclusterTenantTypes {
public:
	using TenantMapEntryT = MetaclusterTenantMapEntry;
	using TenantGroupEntryT = MetaclusterTenantGroupEntry;
};

} // namespace metacluster

template <>
struct Traceable<metacluster::ClusterUsage> : std::true_type {
	static std::string toString(const metacluster::ClusterUsage& value) {
		return format("NumTenantGroups: %d", value.numTenantGroups);
	}
};

#endif
