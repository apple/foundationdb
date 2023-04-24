/*
 * MetaclusterRegistration.h
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

#ifndef FDBCLIENT_METACLUSTERREGISTRATION_H
#define FDBCLIENT_METACLUSTERREGISTRATION_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "flow/BooleanParam.h"

std::string clusterTypeToString(const ClusterType& clusterType);

struct MetaclusterRegistrationEntry {
	constexpr static FileIdentifier file_identifier = 13448589;

	ClusterType clusterType;

	ClusterName metaclusterName;
	ClusterName name;
	UID metaclusterId;
	UID id;

	MetaclusterRegistrationEntry() = default;
	MetaclusterRegistrationEntry(ClusterName metaclusterName, UID metaclusterId)
	  : clusterType(ClusterType::METACLUSTER_MANAGEMENT), metaclusterName(metaclusterName), name(metaclusterName),
	    metaclusterId(metaclusterId), id(metaclusterId) {}
	MetaclusterRegistrationEntry(ClusterName metaclusterName, ClusterName name, UID metaclusterId, UID id)
	  : clusterType(ClusterType::METACLUSTER_DATA), metaclusterName(metaclusterName), name(name),
	    metaclusterId(metaclusterId), id(id) {
		ASSERT(metaclusterName != name && metaclusterId != id);
	}

	// Returns true if this entry is associated with the same cluster as the passed in entry. If one entry is from the
	// management cluster and the other is from a data cluster, this checks whether they are part of the same
	// metacluster.
	bool matches(MetaclusterRegistrationEntry const& other) const {
		if (metaclusterName != other.metaclusterName || metaclusterId != other.metaclusterId) {
			return false;
		} else if (clusterType == ClusterType::METACLUSTER_DATA && other.clusterType == ClusterType::METACLUSTER_DATA &&
		           (name != other.name || id != other.id)) {
			return false;
		}

		return true;
	}

	MetaclusterRegistrationEntry toManagementClusterRegistration() const {
		ASSERT(clusterType == ClusterType::METACLUSTER_DATA);
		return MetaclusterRegistrationEntry(metaclusterName, metaclusterId);
	}

	MetaclusterRegistrationEntry toDataClusterRegistration(ClusterName name, UID id) const {
		ASSERT(clusterType == ClusterType::METACLUSTER_MANAGEMENT);
		return MetaclusterRegistrationEntry(metaclusterName, name, metaclusterId, id);
	}

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static MetaclusterRegistrationEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<MetaclusterRegistrationEntry>(value, IncludeVersion());
	}
	static Optional<MetaclusterRegistrationEntry> decode(Optional<Value> value) {
		return value.map([](ValueRef const& v) { return MetaclusterRegistrationEntry::decode(v); });
	}

	std::string toString() const {
		if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
			return fmt::format(
			    "metacluster name: {}, metacluster id: {}", printable(metaclusterName), metaclusterId.shortString());
		} else {
			return fmt::format("metacluster name: {}, metacluster id: {}, data cluster name: {}, data cluster id: {}",
			                   printable(metaclusterName),
			                   metaclusterId.shortString(),
			                   printable(name),
			                   id.shortString());
		}
	}

	bool operator==(MetaclusterRegistrationEntry const& other) const {
		return clusterType == other.clusterType && metaclusterName == other.metaclusterName && name == other.name &&
		       metaclusterId == other.metaclusterId && id == other.id;
	}

	bool operator!=(MetaclusterRegistrationEntry const& other) const { return !(*this == other); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clusterType, metaclusterName, name, metaclusterId, id);
	}
};

template <>
struct Traceable<MetaclusterRegistrationEntry> : std::true_type {
	static std::string toString(MetaclusterRegistrationEntry const& entry) { return entry.toString(); }
};

// Registration information for a metacluster, stored on both management and data clusters
namespace metacluster::metadata {
KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())>& metaclusterRegistration();
}; // namespace metacluster::metadata

#endif
