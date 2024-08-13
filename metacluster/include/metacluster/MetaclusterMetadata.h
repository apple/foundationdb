/*
 * MetaclusterMetadata.h
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

#ifndef METACLUSTER_METACLUSTERMETADATA_H
#define METACLUSTER_METACLUSTERMETADATA_H
#pragma once

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "flow/flat_buffers.h"

#include "metacluster/MetaclusterTypes.h"

namespace metacluster::metadata {

class RestoreId {
public:
	static inline bool simAllowUidRestoreId = true;

	UID uid;
	Versionstamp versionstamp;

	RestoreId() : RestoreId(UID()) {}

	template <class Transaction>
	static RestoreId createRestoreId(Transaction tr,
	                                 KeyBackedMap<ClusterName, RestoreId>& restoreIdMap,
	                                 ClusterName clusterName) {
		if (g_network->isSimulated() && BUGGIFY && simAllowUidRestoreId) {
			RestoreId restoreId = RestoreId(deterministicRandom()->randomUniqueID());
			restoreIdMap.set(tr, clusterName, restoreId);
			return restoreId;
		} else {
			RestoreId restoreId;
			restoreIdMap.setVersionstamp(
			    tr, clusterName, restoreId, restoreId.tuple.pack().size() - VERSIONSTAMP_TUPLE_SIZE);

			typename transaction_future_type<Transaction, Value>::type f = tr->getVersionstamp();
			restoreId.versionstampFuture =
			    holdWhile(f, map(safeThreadFutureToFuture(f), [](Value value) { return Versionstamp(value); }));

			return restoreId;
		}
	}

	Tuple pack() const;
	static RestoreId unpack(Tuple tuple);

	bool replaces(Versionstamp const& versionstamp);

	bool operator==(RestoreId const& other) const;
	bool operator!=(RestoreId const& other) const;

	Future<Void> onSet();

	std::string toString() const;

private:
	Future<Versionstamp> versionstampFuture;
	Tuple tuple;

	void setTuple(Tuple tuple);

	RestoreId(UID uid);
	RestoreId(Tuple tuple);
};

// Metadata used on all clusters in a metacluster
KeyBackedSet<UID>& registrationTombstones();
KeyBackedMap<ClusterName, RestoreId>& activeRestoreIds();
KeyBackedProperty<Versionstamp> maxRestoreId();

// Metadata used only on the management cluster
namespace management {
struct ConnectionStringCodec {
	static inline Standalone<StringRef> pack(ClusterConnectionString const& val) { return StringRef(val.toString()); }
	static inline ClusterConnectionString unpack(Standalone<StringRef> const& val) {
		return ClusterConnectionString(val.toString());
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
} // namespace management

} // namespace metacluster::metadata

template <>
struct Traceable<metacluster::metadata::RestoreId> : std::true_type {
	static std::string toString(metacluster::metadata::RestoreId const& value) { return value.toString(); }
};

#endif
