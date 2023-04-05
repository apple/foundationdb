/*
 * MetaclusterMetadata.cpp
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

#include "metacluster/MetaclusterMetadata.h"

#include "libb64/decode.h"
#include "libb64/encode.h"

namespace metacluster::metadata {

KeyBackedSet<UID>& registrationTombstones() {
	static KeyBackedSet<UID> instance("\xff/metacluster/registrationTombstones"_sr);
	return instance;
}

KeyBackedMap<ClusterName, UID>& activeRestoreIds() {
	static KeyBackedMap<ClusterName, UID> instance("\xff/metacluster/activeRestoreIds"_sr);
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