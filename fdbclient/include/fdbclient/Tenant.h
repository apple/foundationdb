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
#include "fdbrpc/TenantInfo.h"
#include "flow/flat_buffers.h"

typedef StringRef TenantNameRef;
typedef Standalone<TenantNameRef> TenantName;
typedef StringRef TenantGroupNameRef;
typedef Standalone<TenantGroupNameRef> TenantGroupName;

enum class TenantState { REGISTERING, READY, REMOVING, UPDATING_CONFIGURATION, ERROR };

struct TenantMapEntry {
	constexpr static FileIdentifier file_identifier = 12247338;

	static Key idToPrefix(int64_t id);
	static int64_t prefixToId(KeyRef prefix);

	static std::string tenantStateToString(TenantState tenantState);
	static TenantState stringToTenantState(std::string stateStr);

	int64_t id = -1;
	Key prefix;
	TenantState tenantState = TenantState::READY;
	Optional<TenantGroupName> tenantGroup;
	bool encrypted = false;

	constexpr static int PREFIX_SIZE = sizeof(id);

public:
	TenantMapEntry();
	TenantMapEntry(int64_t id, TenantState tenantState, bool encrypted);
	TenantMapEntry(int64_t id, TenantState tenantState, Optional<TenantGroupName> tenantGroup, bool encrypted);

	void setId(int64_t id);
	std::string toJson(int apiVersion) const;

	bool matchesConfiguration(TenantMapEntry const& other) const;
	void configure(Standalone<StringRef> parameter, Optional<Value> value);

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static TenantMapEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<TenantMapEntry>(value, IncludeVersion());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, tenantState, tenantGroup, encrypted);
		if constexpr (Ar::isDeserializing) {
			if (id >= 0) {
				prefix = idToPrefix(id);
			}
			ASSERT(tenantState >= TenantState::REGISTERING && tenantState <= TenantState::ERROR);
		}
	}
};

struct TenantGroupEntry {
	constexpr static FileIdentifier file_identifier = 10764222;

	TenantGroupEntry() = default;

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static TenantGroupEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<TenantGroupEntry>(value, IncludeVersion());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct TenantMetadataSpecification {
	static KeyRef subspace;

	KeyBackedObjectMap<TenantName, TenantMapEntry, decltype(IncludeVersion()), NullCodec> tenantMap;
	KeyBackedProperty<int64_t> lastTenantId;
	KeyBackedBinaryValue<int64_t> tenantCount;
	KeyBackedSet<Tuple> tenantGroupTenantIndex;
	KeyBackedObjectMap<TenantGroupName, TenantGroupEntry, decltype(IncludeVersion()), NullCodec> tenantGroupMap;

	TenantMetadataSpecification(KeyRef subspace)
	  : tenantMap(subspace.withSuffix("tenant/map/"_sr), IncludeVersion()),
	    lastTenantId(subspace.withSuffix("tenant/lastId"_sr)), tenantCount(subspace.withSuffix("tenant/count"_sr)),
	    tenantGroupTenantIndex(subspace.withSuffix("tenant/tenantGroup/tenantIndex/"_sr)),
	    tenantGroupMap(subspace.withSuffix("tenant/tenantGroup/map/"_sr), IncludeVersion()) {}
};

struct TenantMetadata {
private:
	static inline TenantMetadataSpecification instance = TenantMetadataSpecification("\xff/"_sr);

public:
	static inline auto& tenantMap = instance.tenantMap;
	static inline auto& lastTenantId = instance.lastTenantId;
	static inline auto& tenantCount = instance.tenantCount;
	static inline auto& tenantGroupTenantIndex = instance.tenantGroupTenantIndex;
	static inline auto& tenantGroupMap = instance.tenantGroupMap;

	static inline Key tenantMapPrivatePrefix = "\xff"_sr.withSuffix(tenantMap.subspace.begin);
};

typedef VersionedMap<TenantName, TenantMapEntry> TenantMap;
typedef VersionedMap<Key, TenantName> TenantPrefixIndex;

#endif
