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

	bool operator==(const ClusterUsage& other) const noexcept { return numTenantGroups == other.numTenantGroups; }
	bool operator!=(const ClusterUsage& other) const noexcept { return !(*this == other); }

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

	UID id;
	ClusterUsage capacity;
	ClusterUsage allocated;

	DataClusterEntry() = default;
	DataClusterEntry(ClusterUsage capacity) : capacity(capacity) {}
	DataClusterEntry(UID id, ClusterUsage capacity, ClusterUsage allocated)
	  : id(id), capacity(capacity), allocated(allocated) {}

	// Returns true if all configurable properties match
	bool matchesConfiguration(DataClusterEntry const& other) const {
		return id == other.id && capacity == other.capacity;
	}

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
	UID metaclusterId;
	UID id;

	DataClusterRegistrationEntry() = default;
	DataClusterRegistrationEntry(ClusterName name, UID metaclusterId, UID id)
	  : name(name), metaclusterId(metaclusterId), id(id) {}

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withMetacluster())); }
	static DataClusterRegistrationEntry decode(ValueRef const& value) {
		DataClusterRegistrationEntry entry;
		ObjectReader reader(value.begin(), IncludeVersion());
		reader.deserialize(entry);
		return entry;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, name, metaclusterId, id);
	}
};

#endif