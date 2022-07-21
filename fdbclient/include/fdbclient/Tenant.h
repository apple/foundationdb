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
#include "fdbclient/VersionedMap.h"
#include "flow/flat_buffers.h"

typedef StringRef TenantNameRef;
typedef Standalone<TenantNameRef> TenantName;

struct TenantMapEntry {
	constexpr static FileIdentifier file_identifier = 12247338;

	static Key idToPrefix(int64_t id);
	static int64_t prefixToId(KeyRef prefix);

	int64_t id;
	Key prefix;

	constexpr static int PREFIX_SIZE = sizeof(id);

public:
	TenantMapEntry();
	TenantMapEntry(int64_t id);

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withTenants())); }

	static TenantMapEntry decode(ValueRef const& value) {
		TenantMapEntry entry;
		ObjectReader reader(value.begin(), IncludeVersion(ProtocolVersion::withTenants()));
		reader.deserialize(entry);
		return entry;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id);
		if constexpr (Ar::isDeserializing) {
			if (id >= 0) {
				prefix = idToPrefix(id);
			}
		}
	}
};

typedef VersionedMap<TenantName, TenantMapEntry> TenantMap;
typedef VersionedMap<Key, TenantName> TenantPrefixIndex;

#endif