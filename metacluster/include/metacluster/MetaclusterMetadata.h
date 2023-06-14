/*
 * MetaclusterMetadata.h
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

#ifndef METACLUSTER_METACLUSTERMETADATA_H
#define METACLUSTER_METACLUSTERMETADATA_H
#pragma once

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "flow/flat_buffers.h"

#include "metacluster/MetaclusterTypes.h"

namespace metacluster::metadata {

// Metadata used on all clusters in a metacluster
KeyBackedSet<UID>& registrationTombstones();
KeyBackedMap<ClusterName, UID>& activeRestoreIds();

// Metadata used only on the management cluster
namespace management {
struct ConnectionStringCodec {
	static inline Standalone<StringRef> pack(ClusterConnectionString const& val) { return StringRef(val.toString()); }
	static inline ClusterConnectionString unpack(Standalone<StringRef> const& val) {
		return ClusterConnectionString(val.toString());
	}
};

// This enum class can be used to track the state of the data movement
// The state written to the cluster will be the last fully completed
// step of the movement
enum class MoveStep {
	START_METADATA = 0,
	START_LOCK = 1,
	START_CREATE = 2,
	SWITCH_HYBRID = 3,
	SWITCH_METADATA = 4,
	FINISH_UNLOCK = 5
};

struct MoveIdentifier {
	UID runId;
	ClusterName srcCluster;
	ClusterName dstCluster;
	MoveStep step;

	bool operator==(const MoveIdentifier& mi) const {
		return runId == mi.runId && srcCluster == mi.srcCluster && dstCluster == mi.dstCluster && step == mi.step;
	}

	Tuple pack() const {
		return Tuple::makeTuple(runId.toString(), srcCluster, dstCluster, static_cast<int64_t>(step));
	}
	static MoveIdentifier unpack(Tuple const& tuple) {
		MoveIdentifier mi;
		int i = 0;
		auto sr = tuple.getString(i++);
		mi.runId = UID::fromString(sr.toString());
		mi.srcCluster = tuple.getString(i++);
		mi.dstCluster = tuple.getString(i++);
		auto step = tuple.getInt(i++);
		mi.step = MoveStep(step);
		return mi;
	}
};

TenantMetadataSpecification<MetaclusterTenantTypes>& tenantMetadata();

// A map from cluster name to the metadata associated with a cluster
KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())>& dataClusters();

// A map from cluster name to the connection string for the cluster
KeyBackedMap<ClusterName, ClusterConnectionString, TupleCodec<ClusterName>, ConnectionStringCodec>&
dataClusterConnectionRecords();

// A set of non-full clusters where the key is the tuple (num tenant groups allocated, cluster name).
KeyBackedSet<Tuple>& clusterCapacityIndex();

// A map from cluster name to a count of tenants
KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>>& clusterTenantCount();

// A set of (cluster name, tenant name, tenant ID) tuples ordered by cluster
// Renaming tenants are stored twice in the index, with the destination name stored with ID -1
KeyBackedSet<Tuple>& clusterTenantIndex();

// A set of (cluster, tenant group name) tuples ordered by cluster
KeyBackedSet<Tuple>& clusterTenantGroupIndex();

namespace move {
// UID is not supported by Tuple.h
// use UID::toString() and static UID::fromString instead

// emergency_movement/move(tenantGroup) = (RunID, sourceCluster, destinationCluster, moveStep)
KeyBackedMap<TenantGroupName, MoveIdentifier>& emergencyMovements();

// emergency_movement/version(tenantGroup, RunID) = Version
KeyBackedMap<std::pair<TenantGroupName, std::string>, Version>& movementVersions();

// emergency_movement/queue(tenantGroup, RunID) = (tenantName, startKey)
KeyBackedMap<std::pair<TenantGroupName, std::string>, std::pair<TenantName, Key>>& movementQueue();

// emergency_movement/split_points(tenantGroup, runId, tenant, startKey) = endKey
KeyBackedMap<Tuple, Key>& splitPointsMap();

}; // namespace move

} // namespace management

} // namespace metacluster::metadata

#endif
