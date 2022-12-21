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
Key idToPrefix(int64_t id);
int64_t prefixToId(KeyRef prefix, EnforceValidTenantId = EnforceValidTenantId::True);

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
// RENAMING_FROM - the tenant is being renamed to a new name and is awaiting the rename to complete on the data cluster
// RENAMING_TO - the tenant is being created as a rename from an existing tenant and is awaiting the rename to complete
//               on the data cluster
// ERROR - the tenant is in an error state
//
// A tenant in any configuration is allowed to be removed. Only tenants in the READY or UPDATING_CONFIGURATION phases
// can have their configuration updated. A tenant must not exist or be in the REGISTERING phase to be created. To be
// renamed, a tenant must be in the READY or RENAMING_FROM state. In the latter case, the rename destination must match
// the original rename attempt.
//
// If an operation fails and the tenant is left in a non-ready state, re-running the same operation is legal. If
// successful, the tenant will return to the READY state.
enum class TenantState { REGISTERING, READY, REMOVING, UPDATING_CONFIGURATION, RENAMING_FROM, RENAMING_TO, ERROR };

// Represents the lock state the tenant could be in.
// Can be used in conjunction with the other tenant states above.
enum class TenantLockState : uint8_t { UNLOCKED, READ_ONLY, LOCKED };

// metadata used in proxies and stored in the txnStateStore
struct TenantMinimalMetadata {
	constexpr static FileIdentifier file_identifier = 265557;

	int64_t id = -1;
	TenantName tenantName;
	TenantLockState tenantLockState = TenantLockState::UNLOCKED;

	TenantMinimalMetadata() = default;
	TenantMinimalMetadata(int64_t id, TenantName tenantName);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, tenantName, tenantLockState);
	}
};

struct TenantMapEntry {
	constexpr static FileIdentifier file_identifier = 12247338;

	static std::string tenantStateToString(TenantState tenantState);
	static TenantState stringToTenantState(std::string stateStr);

	static std::string tenantLockStateToString(TenantLockState tenantState);
	static TenantLockState stringToTenantLockState(std::string stateStr);

	TenantMinimalMetadata tenantMinimalMetadata;
	Key prefix;
	TenantState tenantState = TenantState::READY;
	Optional<UID> lockID = Optional<UID>();
	Optional<TenantGroupName> tenantGroup;
	Optional<ClusterName> assignedCluster;
	int64_t configurationSequenceNum = 0;
	Optional<TenantName> renamePair;

	// Can be set to an error string if the tenant is in the ERROR state
	std::string error;

	TenantMapEntry() = default;
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
		           tenantMinimalMetadata,
		           tenantState,
		           tenantGroup,
		           assignedCluster,
		           configurationSequenceNum,
		           renamePair,
		           error,
		           lockID);
		if constexpr (Ar::isDeserializing) {
			if (tenantMinimalMetadata.id >= 0) {
				prefix = TenantAPI::idToPrefix(tenantMinimalMetadata.id);
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

struct TenantMetadataSpecification {
	Key subspace;

	KeyBackedObjectMap<TenantName, TenantMapEntry, decltype(IncludeVersion()), NullCodec> tenantMap;
	KeyBackedMap<int64_t, TenantName> tenantIdIndex;
	KeyBackedProperty<int64_t> lastTenantId;
	KeyBackedBinaryValue<int64_t> tenantCount;
	KeyBackedSet<int64_t> tenantTombstones;
	KeyBackedObjectProperty<TenantTombstoneCleanupData, decltype(IncludeVersion())> tombstoneCleanupData;
	KeyBackedSet<Tuple> tenantGroupTenantIndex;
	KeyBackedObjectMap<TenantGroupName, TenantGroupEntry, decltype(IncludeVersion()), NullCodec> tenantGroupMap;
	KeyBackedBinaryValue<Versionstamp> lastTenantModification;

	TenantMetadataSpecification(KeyRef prefix)
	  : subspace(prefix.withSuffix("tenant/"_sr)), tenantMap(subspace.withSuffix("map/"_sr), IncludeVersion()),
	    tenantIdIndex(subspace.withSuffix("idIndex/"_sr)), lastTenantId(subspace.withSuffix("lastId"_sr)),
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
	static inline auto& tenantIdIndex() { return instance().tenantIdIndex; }
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
