/*
 * MetaclusterMetadata.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "metacluster/MetaclusterMetadata.h"

#include "libb64/decode.h"
#include "libb64/encode.h"

namespace metacluster::metadata {

Tuple RestoreId::pack() const {
	return tuple;
}
RestoreId RestoreId::unpack(Tuple tuple) {
	return RestoreId(tuple);
}

bool RestoreId::replaces(Versionstamp const& versionstamp) {
	return uid.isValid() || this->versionstamp >= versionstamp;
}

bool RestoreId::operator==(RestoreId const& other) const {
	return uid == other.uid && versionstamp == other.versionstamp;
}
bool RestoreId::operator!=(RestoreId const& other) const {
	return !(*this == other);
}

Future<Void> RestoreId::onSet() {
	if (uid.isValid()) {
		return Void();
	} else {
		return map(versionstampFuture, [this](Versionstamp versionstamp) {
			this->versionstamp = versionstamp;
			tuple = Tuple::makeTuple(BinaryWriter::toValue<UID>(uid, Unversioned()),
			                         TupleVersionstamp(versionstamp.version, versionstamp.batchNumber));
			return Void();
		});
	}
}

std::string RestoreId::toString() const {
	return fmt::format("{}.{}", uid.toString(), versionstamp.toString());
}

RestoreId::RestoreId(UID uid) : uid(uid) {
	if (uid.isValid()) {
		// This is used to simulate the behavior of older versions
		ASSERT(g_network->isSimulated());
		tuple = Tuple::makeTuple(BinaryWriter::toValue<UID>(uid, Unversioned()));
	} else {
		tuple = Tuple::makeTuple(BinaryWriter::toValue<UID>(uid, Unversioned()), TupleVersionstamp());
	}
}

RestoreId::RestoreId(Tuple tuple) : tuple(tuple) {
	uid = BinaryReader::fromStringRef<UID>(tuple.getString(0), Unversioned());

	if (tuple.size() == 2) {
		ASSERT(!uid.isValid());
		TupleVersionstamp tupleVersionstamp = tuple.getVersionstamp(1);
		ASSERT(tupleVersionstamp.getUserVersion() == 0);
		versionstamp = Versionstamp(tupleVersionstamp.getVersion(), tupleVersionstamp.getBatchNumber());
	} else {
		CODE_PROBE(true, "Processing a UID-based restore ID");
	}
}

KeyBackedSet<UID>& registrationTombstones() {
	static KeyBackedSet<UID> instance("\xff/metacluster/registrationTombstones"_sr);
	return instance;
}

KeyBackedMap<ClusterName, RestoreId>& activeRestoreIds() {
	static KeyBackedMap<ClusterName, RestoreId> instance("\xff/metacluster/activeRestoreIds"_sr);
	return instance;
}

KeyBackedProperty<Versionstamp> maxRestoreId() {
	static KeyBackedProperty<Versionstamp> instance("\xff/metacluster/maxRestoreId"_sr);
	return instance;
}

namespace management {
KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())>& dataClusters() {
	static KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())> instance(
	    "metacluster/dataCluster/metadata/"_sr, IncludeVersion());
	return instance;
}

KeyBackedMap<ClusterName, ClusterConnectionString, TupleCodec<ClusterName>, ConnectionStringCodec>&
dataClusterConnectionRecords() {
	static KeyBackedMap<ClusterName, ClusterConnectionString, TupleCodec<ClusterName>, ConnectionStringCodec> instance(
	    "metacluster/dataCluster/connectionString/"_sr);
	return instance;
}

KeyBackedSet<Tuple>& clusterCapacityIndex() {
	static KeyBackedSet<Tuple> instance("metacluster/clusterCapacityIndex/"_sr);
	return instance;
}

KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>>& clusterTenantCount() {
	static KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>> instance(
	    "metacluster/clusterTenantCount/"_sr);
	return instance;
}

KeyBackedSet<Tuple>& clusterTenantIndex() {
	static KeyBackedSet<Tuple> instance("metacluster/dataCluster/tenantMap/"_sr);
	return instance;
}

KeyBackedSet<Tuple>& clusterTenantGroupIndex() {
	static KeyBackedSet<Tuple> instance("metacluster/dataCluster/tenantGroupMap/"_sr);
	return instance;
}

TenantMetadataSpecification<MetaclusterTenantTypes>& tenantMetadata() {
	static TenantMetadataSpecification<MetaclusterTenantTypes> instance(""_sr);
	return instance;
}

} // namespace management
} // namespace metacluster::metadata