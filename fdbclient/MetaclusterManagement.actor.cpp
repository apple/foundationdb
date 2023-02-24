/*
 * MetaclusterManagement.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/actorcompiler.h" // has to be last include

namespace MetaclusterAPI {

std::pair<ClusterUsage, ClusterUsage> metaclusterCapacity(std::map<ClusterName, DataClusterMetadata> const& clusters) {
	ClusterUsage tenantGroupCapacity;
	ClusterUsage tenantGroupsAllocated;
	for (auto cluster : clusters) {
		tenantGroupCapacity.numTenantGroups +=
		    std::max(cluster.second.entry.capacity.numTenantGroups, cluster.second.entry.allocated.numTenantGroups);
		tenantGroupsAllocated.numTenantGroups += cluster.second.entry.allocated.numTenantGroups;
	}
	return { tenantGroupCapacity, tenantGroupsAllocated };
}

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString) {
	if (g_network->isSimulated()) {
		Reference<IClusterConnectionRecord> clusterFile =
		    makeReference<ClusterConnectionMemoryRecord>(connectionString);
		Database nativeDb = Database::createDatabase(clusterFile, ApiVersion::LATEST_VERSION);
		Reference<IDatabase> threadSafeDb =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(nativeDb)));
		MultiVersionApi::api->selectApiVersion(ApiVersion::LATEST_VERSION);
		return MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeDb);
	} else {
		return MultiVersionApi::api->createDatabaseFromConnectionString(connectionString.toString().c_str());
	}
}

KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())>&
ManagementClusterMetadata::dataClusters() {
	static KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())> instance(
	    "metacluster/dataCluster/metadata/"_sr, IncludeVersion());
	return instance;
}

KeyBackedMap<ClusterName,
             ClusterConnectionString,
             TupleCodec<ClusterName>,
             ManagementClusterMetadata::ConnectionStringCodec>
    ManagementClusterMetadata::dataClusterConnectionRecords("metacluster/dataCluster/connectionString/"_sr);

KeyBackedSet<Tuple> ManagementClusterMetadata::clusterCapacityIndex("metacluster/clusterCapacityIndex/"_sr);
KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>>
    ManagementClusterMetadata::clusterTenantCount("metacluster/clusterTenantCount/"_sr);
KeyBackedSet<Tuple> ManagementClusterMetadata::clusterTenantIndex("metacluster/dataCluster/tenantMap/"_sr);
KeyBackedSet<Tuple> ManagementClusterMetadata::clusterTenantGroupIndex("metacluster/dataCluster/tenantGroupMap/"_sr);

TenantMetadataSpecification<MetaclusterTenantTypes>& ManagementClusterMetadata::tenantMetadata() {
	static TenantMetadataSpecification<MetaclusterTenantTypes> instance(""_sr);
	return instance;
}

}; // namespace MetaclusterAPI