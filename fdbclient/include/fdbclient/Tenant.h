/*
 * Tenant.h
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

#ifndef FDBCLIENT_TENANT_H
#define FDBCLIENT_TENANT_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/VersionedMap.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbrpc/TenantInfo.h"
#include "flow/BooleanParam.h"
#include "flow/flat_buffers.h"

FDB_DECLARE_BOOLEAN_PARAM(EnforceValidTenantId);

namespace TenantAPI {
KeyRef idToPrefix(Arena& p, int64_t id);
Key idToPrefix(int64_t id);
int64_t prefixToId(KeyRef prefix, EnforceValidTenantId = EnforceValidTenantId::True);

// return true if begin and end has the same non-negative prefix id
bool withinSingleTenant(KeyRangeRef const&);

constexpr static int PREFIX_SIZE = sizeof(int64_t);
} // namespace TenantAPI

// Represents the various states that a tenant could be in.
// In a standalone cluster, a tenant should only ever be in the READY state.
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

// Represents the lock state the tenant could be in.
// Can be used in conjunction with the other tenant states above.
enum class TenantLockState : uint8_t { UNLOCKED, READ_ONLY, LOCKED };

struct TenantMapEntry {
	constexpr static FileIdentifier file_identifier = 12247338;

	static std::string tenantStateToString(TenantState tenantState);
	static TenantState stringToTenantState(std::string stateStr);

	static std::string tenantLockStateToString(TenantLockState tenantState);
	static TenantLockState stringToTenantLockState(std::string stateStr);

	int64_t id = -1;
	Key prefix;
	TenantName tenantName;
	TenantState tenantState = TenantState::READY;
	TenantLockState tenantLockState = TenantLockState::UNLOCKED;
	Optional<TenantGroupName> tenantGroup;
	Optional<ClusterName> assignedCluster;
	int64_t configurationSequenceNum = 0;
	Optional<TenantName> renameDestination;

	// Can be set to an error string if the tenant is in the ERROR state
	std::string error;

	TenantMapEntry();
	TenantMapEntry(int64_t id, TenantName tenantName, TenantState tenantState);
	TenantMapEntry(int64_t id, TenantName tenantName, TenantState tenantState, Optional<TenantGroupName> tenantGroup);

	void setId(int64_t id);
	std::string toJson() const;

	bool matchesConfiguration(TenantMapEntry const& other) const;
	void configure(Standalone<StringRef> parameter, Optional<Value> value);

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static TenantMapEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<TenantMapEntry>(value, IncludeVersion());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           id,
		           tenantName,
		           tenantState,
		           tenantLockState,
		           tenantGroup,
		           assignedCluster,
		           configurationSequenceNum,
		           renameDestination,
		           error);
		if constexpr (Ar::isDeserializing) {
			if (id >= 0) {
				prefix = TenantAPI::idToPrefix(id);
			}
			ASSERT(tenantState >= TenantState::REGISTERING && tenantState <= TenantState::ERROR);
		}
	}
};

struct TenantGroupEntry {
	constexpr static FileIdentifier file_identifier = 10764222;

	Optional<ClusterName> assignedCluster;

	TenantGroupEntry() = default;
	TenantGroupEntry(Optional<ClusterName> assignedCluster) : assignedCluster(assignedCluster) {}

	json_spirit::mObject toJson() const;

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static TenantGroupEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<TenantGroupEntry>(value, IncludeVersion());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, assignedCluster);
	}
};

struct TenantTombstoneCleanupData {
	constexpr static FileIdentifier file_identifier = 3291339;

	// All tombstones have been erased up to and including this id.
	// We should not generate new tombstones at IDs equal to or older than this.
	int64_t tombstonesErasedThrough = -1;

	// The version at which we will next erase tombstones.
	Version nextTombstoneEraseVersion = invalidVersion;

	// When we reach the nextTombstoneEraseVersion, we will erase tombstones up through this ID.
	int64_t nextTombstoneEraseId = -1;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tombstonesErasedThrough, nextTombstoneEraseVersion, nextTombstoneEraseId);
	}
};

// This is used so that tenant IDs will be ordered and so that we can easily map arbitrary ranges in the tenant map to
// the affected tenant IDs.
struct TenantIdCodec {
	static Standalone<StringRef> pack(int64_t val) {
		int64_t swapped = bigEndian64(val);
		return StringRef((uint8_t*)&swapped, sizeof(swapped));
	}
	static int64_t unpack(Standalone<StringRef> val) { return bigEndian64(*(int64_t*)val.begin()); }

	static Optional<int64_t> lowerBound(Standalone<StringRef> val) {
		if (val.startsWith("\xff"_sr)) {
			return {};
		}
		if (val.size() == 8) {
			return unpack(val);
		} else if (val.size() > 8) {
			int64_t result = unpack(val);
			if (result == std::numeric_limits<int64_t>::max()) {
				return {};
			}
			return result + 1;
		} else {
			int64_t result = 0;
			memcpy(&result, val.begin(), val.size());
			return bigEndian64(result);
		}
	}
};

struct TenantMetadataSpecification {
	Key subspace;

	KeyBackedObjectMap<int64_t, TenantMapEntry, decltype(IncludeVersion()), TenantIdCodec> tenantMap;
	KeyBackedMap<TenantName, int64_t> tenantNameIndex;
	KeyBackedProperty<int64_t> lastTenantId;
	KeyBackedBinaryValue<int64_t> tenantCount;
	KeyBackedSet<int64_t> tenantTombstones;
	KeyBackedObjectProperty<TenantTombstoneCleanupData, decltype(IncludeVersion())> tombstoneCleanupData;
	KeyBackedSet<Tuple> tenantGroupTenantIndex;
	KeyBackedObjectMap<TenantGroupName, TenantGroupEntry, decltype(IncludeVersion()), NullCodec> tenantGroupMap;
	KeyBackedBinaryValue<Versionstamp> lastTenantModification;

	TenantMetadataSpecification(KeyRef prefix)
	  : subspace(prefix.withSuffix("tenant/"_sr)), tenantMap(subspace.withSuffix("map/"_sr), IncludeVersion()),
	    tenantNameIndex(subspace.withSuffix("nameIndex/"_sr)), lastTenantId(subspace.withSuffix("lastId"_sr)),
	    tenantCount(subspace.withSuffix("count"_sr)), tenantTombstones(subspace.withSuffix("tombstones/"_sr)),
	    tombstoneCleanupData(subspace.withSuffix("tombstoneCleanup"_sr), IncludeVersion()),
	    tenantGroupTenantIndex(subspace.withSuffix("tenantGroup/tenantIndex/"_sr)),
	    tenantGroupMap(subspace.withSuffix("tenantGroup/map/"_sr), IncludeVersion()),
	    lastTenantModification(subspace.withSuffix("lastModification"_sr)) {}
};

struct TenantMetadata {
	static TenantMetadataSpecification& instance();

	static inline auto& subspace() { return instance().subspace; }
	static inline auto& tenantMap() { return instance().tenantMap; }
	static inline auto& tenantNameIndex() { return instance().tenantNameIndex; }
	static inline auto& lastTenantId() { return instance().lastTenantId; }
	static inline auto& tenantCount() { return instance().tenantCount; }
	static inline auto& tenantTombstones() { return instance().tenantTombstones; }
	static inline auto& tombstoneCleanupData() { return instance().tombstoneCleanupData; }
	static inline auto& tenantGroupTenantIndex() { return instance().tenantGroupTenantIndex; }
	static inline auto& tenantGroupMap() { return instance().tenantGroupMap; }
	static inline auto& lastTenantModification() { return instance().lastTenantModification; }

	static Key tenantMapPrivatePrefix();
};
#endif
