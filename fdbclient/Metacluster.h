/*
 * Metacluster.h
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

#ifndef FDBCLIENT_METACLUSTER_H
#define FDBCLIENT_METACLUSTER_H
#include "CoordinationInterface.h"
#include "json_spirit/json_spirit_value.h"
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/VersionedMap.h"
#include "flow/flat_buffers.h"

typedef StringRef ClusterNameRef;
typedef Standalone<ClusterNameRef> ClusterName;

struct ClusterUsage {
	int numTenantGroups = 0;

	ClusterUsage() = default;
	ClusterUsage(int numTenantGroups) : numTenantGroups(numTenantGroups) {}

	json_spirit::mObject toJson();

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, numTenantGroups);
	}
};

template <>
struct Traceable<ClusterUsage> : std::true_type {
	static std::string toString(const ClusterUsage& value) {
		return format("NumTenantGroups: %d", value.numTenantGroups);
	}
};

struct DataClusterEntry {
	constexpr static FileIdentifier file_identifier = 929511;

	static Value idToValue(int64_t id) {
		int64_t swapped = bigEndian64(id);
		return StringRef(reinterpret_cast<const uint8_t*>(&swapped), 8);
	}

	static int64_t valueToId(ValueRef value) {
		ASSERT(value.size() == 8);
		int64_t id = *reinterpret_cast<const int64_t*>(value.begin());
		id = bigEndian64(id);
		ASSERT(id >= 0);
		return id;
	}

	int64_t id = -1;
	ClusterUsage capacity;
	ClusterUsage allocated;

	DataClusterEntry() = default;
	DataClusterEntry(ClusterUsage capacity) : capacity(capacity) {}
	DataClusterEntry(int64_t id, ClusterUsage capacity, ClusterUsage allocated)
	  : id(id), capacity(capacity), allocated(allocated) {}

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withMetacluster())); }
	static DataClusterEntry decode(ValueRef const& value) {
		DataClusterEntry entry;
		ObjectReader reader(value.begin(), IncludeVersion());
		reader.deserialize(entry);
		return entry;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, capacity, allocated);
	}
};

struct DataClusterRegistrationEntry {
	constexpr static FileIdentifier file_identifier = 13448589;

	ClusterName name;

	DataClusterRegistrationEntry() = default;
	DataClusterRegistrationEntry(ClusterName name) : name(name) {}

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withMetacluster())); }
	static DataClusterRegistrationEntry decode(ValueRef const& value) {
		DataClusterRegistrationEntry entry;
		ObjectReader reader(value.begin(), IncludeVersion());
		reader.deserialize(entry);
		return entry;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, name);
	}
};

#endif