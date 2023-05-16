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
#include "fdbclient/KeyBackedTypes.actor.h"
#include "flow/BooleanParam.h"

std::string clusterTypeToString(const ClusterType& clusterType);

enum class MetaclusterVersion {
	// Smaller than any legal version; used for testing
	BEGIN = 0,

	// This is the smallest version of metacluster metadata understood by this version of FDB. It should be updated any
	// time support for older versions is dropped. Our contract is that we will support at least one older version, but
	// likely we will stop supporting versions older than that.
	MIN_SUPPORTED = 1,

	// The initial version used for metacluster metadata
	V1 = 1,

	// This is the largest version of metacluster metadata understood by this version of FDB. It should be increased any
	// time an FDB version adds a new metacluster version.
	MAX_SUPPORTED = 1,

	// Larger than any legal version; used for testing
	END
};

template <bool Versioned>
struct MetaclusterRegistrationEntryImpl {
	constexpr static FileIdentifier file_identifier = 13448589;

	// Set to true to allow tests to write metacluster registrations that are invalid
	static inline bool allowUnsupportedRegistrationWrites = false;

	ClusterType clusterType;

	ClusterName metaclusterName;
	ClusterName name;
	UID metaclusterId;
	UID id;

	MetaclusterVersion version;

	MetaclusterRegistrationEntryImpl() = default;
	MetaclusterRegistrationEntryImpl(ClusterName metaclusterName, UID metaclusterId, MetaclusterVersion version)
	  : clusterType(ClusterType::METACLUSTER_MANAGEMENT), metaclusterName(metaclusterName), name(metaclusterName),
	    metaclusterId(metaclusterId), id(metaclusterId), version(version) {}
	MetaclusterRegistrationEntryImpl(ClusterName metaclusterName,
	                                 ClusterName name,
	                                 UID metaclusterId,
	                                 UID id,
	                                 MetaclusterVersion version)
	  : clusterType(ClusterType::METACLUSTER_DATA), metaclusterName(metaclusterName), name(name),
	    metaclusterId(metaclusterId), id(id), version(version) {
		ASSERT(metaclusterName != name && metaclusterId != id);
	}

	template <bool V>
	MetaclusterRegistrationEntryImpl(MetaclusterRegistrationEntryImpl<V> other)
	  : clusterType(other.clusterType), metaclusterName(other.metaclusterName), name(other.name),
	    metaclusterId(other.metaclusterId), id(other.id), version(other.version) {}

	// Returns true if this entry is associated with the same cluster as the passed in entry. If one entry is from the
	// management cluster and the other is from a data cluster, this checks whether they are part of the same
	// metacluster.
	bool matches(MetaclusterRegistrationEntryImpl const& other) const {
		if (metaclusterName != other.metaclusterName || metaclusterId != other.metaclusterId) {
			return false;
		} else if (clusterType == ClusterType::METACLUSTER_DATA && other.clusterType == ClusterType::METACLUSTER_DATA &&
		           (name != other.name || id != other.id)) {
			return false;
		}

		return true;
	}

	MetaclusterRegistrationEntryImpl toManagementClusterRegistration() const {
		ASSERT(clusterType == ClusterType::METACLUSTER_DATA);
		return MetaclusterRegistrationEntryImpl(metaclusterName, metaclusterId, version);
	}

	MetaclusterRegistrationEntryImpl toDataClusterRegistration(ClusterName name, UID id) const {
		ASSERT(clusterType == ClusterType::METACLUSTER_MANAGEMENT);
		return MetaclusterRegistrationEntryImpl(metaclusterName, name, metaclusterId, id, version);
	}

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static MetaclusterRegistrationEntryImpl decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<MetaclusterRegistrationEntryImpl>(value, IncludeVersion());
	}
	static Optional<MetaclusterRegistrationEntryImpl> decode(Optional<Value> value) {
		return value.map([](ValueRef const& v) { return MetaclusterRegistrationEntryImpl::decode(v); });
	}

	std::string toString() const {
		if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
			return fmt::format("metacluster name: {}, metacluster id: {}, version: {}",
			                   printable(metaclusterName),
			                   metaclusterId.shortString(),
			                   (int)version);
		} else {
			return fmt::format(
			    "metacluster name: {}, metacluster id: {}, data cluster name: {}, data cluster id: {}, version: {}",
			    printable(metaclusterName),
			    metaclusterId.shortString(),
			    printable(name),
			    id.shortString(),
			    (int)version);
		}
	}

	template <bool V>
	bool operator==(MetaclusterRegistrationEntryImpl<V> const& other) const {
		return clusterType == other.clusterType && metaclusterName == other.metaclusterName && name == other.name &&
		       metaclusterId == other.metaclusterId && id == other.id && version == other.version;
	}

	template <bool V>
	bool operator!=(MetaclusterRegistrationEntryImpl<V> const& other) const {
		return !(*this == other);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clusterType, metaclusterName, name, metaclusterId, id, version);
		if constexpr (Ar::isDeserializing && Versioned) {
			if (version < MetaclusterVersion::MIN_SUPPORTED || version > MetaclusterVersion::MAX_SUPPORTED) {
				CODE_PROBE(true, "Load unsupported metacluster registration");
				throw unsupported_metacluster_version();
			}
		}
	}
};

template <bool Versioned>
struct Traceable<MetaclusterRegistrationEntryImpl<Versioned>> : std::true_type {
	static std::string toString(MetaclusterRegistrationEntryImpl<Versioned> const& entry) { return entry.toString(); }
};

using MetaclusterRegistrationEntry = MetaclusterRegistrationEntryImpl<true>;
using UnversionedMetaclusterRegistrationEntry = MetaclusterRegistrationEntryImpl<false>;

// Registration information for a metacluster, stored on both management and data clusters
namespace metacluster::metadata {
// Use of this metacluster registration property will verify that the metacluster registration has a compatible version
// before using it. This should be the default choice for accessing the metacluster registration.
KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())>& metaclusterRegistration();

// Use of this metacluster registration property will not verify that the metacluster registration has a compatible
// version. It should only be used in limited circumstances.
KeyBackedObjectProperty<UnversionedMetaclusterRegistrationEntry, decltype(IncludeVersion())>&
unversionedMetaclusterRegistration();
}; // namespace metacluster::metadata

#endif
