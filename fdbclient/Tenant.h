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

	static Key idToPrefix(int64_t id) {
		int64_t swapped = bigEndian64(id);
		return StringRef(reinterpret_cast<const uint8_t*>(&swapped), 8);
	}

	static int64_t prefixToId(KeyRef prefix) {
		ASSERT(prefix.size() == 8);
		int64_t id = *reinterpret_cast<const int64_t*>(prefix.begin());
		id = bigEndian64(id);
		ASSERT(id >= 0);
		return id;
	}

	int64_t id;
	Key prefix;

private:
	void initPrefix(KeyRef subspace) {
		ASSERT(id >= 0);
		prefix = makeString(8 + subspace.size());
		uint8_t* data = mutateString(prefix);
		memcpy(data, subspace.begin(), subspace.size());
		int64_t swapped = bigEndian64(id);
		memcpy(data + subspace.size(), &swapped, 8);
	}

public:
	TenantMapEntry() : id(-1) {}
	TenantMapEntry(int64_t id, KeyRef subspace) : id(id) { initPrefix(subspace); }

	template <class Ar>
	void serialize(Ar& ar) {
		KeyRef subspace;
		if (ar.isDeserializing) {
			serializer(ar, id, subspace);
			if (id >= 0) {
				initPrefix(subspace);
			}
		} else {
			ASSERT(prefix.size() >= 8 || (prefix.empty() && id == -1));
			if (!prefix.empty()) {
				subspace = prefix.substr(0, prefix.size() - 8);
			}
			serializer(ar, id, subspace);
		}
	}
};

typedef VersionedMap<TenantName, TenantMapEntry> TenantMap;
typedef VersionedMap<Key, TenantName> TenantPrefixIndex;

#endif