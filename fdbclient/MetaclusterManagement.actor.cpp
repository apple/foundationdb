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

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString) {
	if (g_network->isSimulated()) {
		Reference<IClusterConnectionRecord> clusterFile =
		    makeReference<ClusterConnectionMemoryRecord>(connectionString);
		Database nativeDb = Database::createDatabase(clusterFile, -1);
		Reference<IDatabase> threadSafeDb =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(nativeDb)));
		return MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeDb);
	} else {
		return MultiVersionApi::api->createDatabaseFromConnectionString(connectionString.toString().c_str());
	}
}

KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())> ManagementClusterMetadata::dataClusters(
    "metacluster/dataCluster/metadata/"_sr,
    IncludeVersion(ProtocolVersion::withMetacluster()));

KeyBackedMap<ClusterName,
             ClusterConnectionString,
             TupleCodec<ClusterName>,
             ManagementClusterMetadata::ConnectionStringCodec>
    ManagementClusterMetadata::dataClusterConnectionRecords("metacluster/dataCluster/connectionString/"_sr);

KeyBackedSet<Tuple> ManagementClusterMetadata::clusterCapacityIndex("metacluster/clusterCapacityIndex/"_sr);
Tuple ManagementClusterMetadata::getClusterCapacityTuple(ClusterNameRef const& clusterName,
                                                         DataClusterEntry const& entry) {
	return Tuple().append(entry.allocated.numTenantGroups).append(clusterName);
}

KeyBackedSet<Tuple> ManagementClusterMetadata::clusterTenantIndex("metacluster/dataCluster/tenantMap/"_sr);
Tuple ManagementClusterMetadata::getClusterTenantIndexTuple(ClusterNameRef const& clusterName,
                                                            TenantNameRef const& tenantName) {
	return Tuple().append(clusterName).append(tenantName);
}

KeyBackedSet<Tuple> ManagementClusterMetadata::clusterTenantGroupIndex("metacluster/dataCluster/tenantGroupMap/"_sr);
Tuple ManagementClusterMetadata::getClusterTenantGroupIndexTuple(ClusterNameRef const& clusterName,
                                                                 TenantGroupNameRef const& tenantGroupName) {
	return Tuple().append(clusterName).append(tenantGroupName);
}

std::pair<Tuple, Tuple> ManagementClusterMetadata::getClusterTupleRange(ClusterNameRef const& clusterName) {
	return std::make_pair(Tuple().append(clusterName), Tuple().append(keyAfter(clusterName)));
}

}; // namespace MetaclusterAPI