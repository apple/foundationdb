/*
 * ConfigureCluster.h
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

#pragma once
#if !defined(METACLUSTER_CONFIGURECLUSTER_H)
#define METACLUSTER_CONFIGURECLUSTER_H

#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterTypes.h"

namespace metacluster {

// This should only be called from a transaction that has already confirmed that the cluster entry
// is present. The updatedEntry should use the existing entry and modify only those fields that need
// to be changed.
template <class Transaction>
void updateClusterMetadata(Transaction tr,
                           ClusterNameRef name,
                           DataClusterMetadata const& previousMetadata,
                           Optional<ClusterConnectionString> const& updatedConnectionString,
                           Optional<DataClusterEntry> const& updatedEntry,
                           IsRestoring isRestoring = IsRestoring::False) {

	if (updatedEntry.present()) {
		if (previousMetadata.entry.clusterState == DataClusterState::REGISTERING &&
		    updatedEntry.get().clusterState != DataClusterState::READY &&
		    updatedEntry.get().clusterState != DataClusterState::REMOVING) {
			throw cluster_not_found();
		} else if (previousMetadata.entry.clusterState == DataClusterState::REMOVING) {
			throw cluster_removed();
		} else if (!isRestoring && previousMetadata.entry.clusterState == DataClusterState::RESTORING &&
		           (updatedEntry.get().clusterState != DataClusterState::READY &&
		            updatedEntry.get().clusterState != DataClusterState::REMOVING)) {
			throw cluster_restoring();
		} else if (isRestoring) {
			ASSERT(previousMetadata.entry.clusterState == DataClusterState::RESTORING &&
			       updatedEntry.get().clusterState == DataClusterState::RESTORING);
		}
		metadata::management::dataClusters().set(tr, name, updatedEntry.get());
		internal::updateClusterCapacityIndex(tr, name, previousMetadata.entry, updatedEntry.get());
	}
	if (updatedConnectionString.present()) {
		metadata::management::dataClusterConnectionRecords().set(tr, name, updatedConnectionString.get());
	}
}

} // namespace metacluster

#endif