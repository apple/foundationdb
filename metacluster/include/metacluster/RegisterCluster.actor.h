/*
 * RegisterCluster.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_REGISTERCLUSTER_ACTOR_G_H)
#define METACLUSTER_REGISTERCLUSTER_ACTOR_G_H
#include "metacluster/RegisterCluster.actor.g.h"
#elif !defined(METACLUSTER_REGISTERCLUSTER_ACTOR_H)
#define METACLUSTER_REGISTERCLUSTER_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/MetaclusterUtil.actor.h"
#include "metacluster/RemoveCluster.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

namespace internal {
template <class DB>
struct RegisterClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	ClusterName clusterName;
	ClusterConnectionString connectionString;
	DataClusterEntry clusterEntry;

	// Loaded from the management cluster
	int64_t tenantIdPrefix;

	RegisterClusterImpl(Reference<DB> managementDb,
	                    ClusterName clusterName,
	                    ClusterConnectionString connectionString,
	                    DataClusterEntry clusterEntry)
	  : ctx(managementDb), clusterName(clusterName), connectionString(connectionString), clusterEntry(clusterEntry) {}

	// Store the cluster entry for the new cluster in a registering state
	ACTOR static Future<Void> registerInManagementCluster(RegisterClusterImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state Future<Optional<int64_t>> tenantIdPrefixFuture = TenantMetadata::tenantIdPrefix().get(tr);
		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (!dataClusterMetadata.present()) {
			CODE_PROBE(true, "Register new cluster");
			self->clusterEntry.clusterState = DataClusterState::REGISTERING;
			self->clusterEntry.allocated = ClusterUsage();
			self->clusterEntry.id = deterministicRandom()->randomUniqueID();

			// If we happen to have any orphaned restore IDs from a previous time this cluster was in a metacluster,
			// erase them now.
			metadata::activeRestoreIds().erase(tr, self->clusterName);

			metadata::management::dataClusters().set(tr, self->clusterName, self->clusterEntry);
			metadata::management::dataClusterConnectionRecords().set(tr, self->clusterName, self->connectionString);
		} else if (dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING) {
			CODE_PROBE(true, "Registering cluster being removed");
			throw cluster_removed();
		} else if (!dataClusterMetadata.get().matchesConfiguration(
		               DataClusterMetadata(self->clusterEntry, self->connectionString)) ||
		           dataClusterMetadata.get().entry.clusterState != DataClusterState::REGISTERING) {
			CODE_PROBE(true, "Cluster already exists");
			throw cluster_already_exists();
		} else {
			CODE_PROBE(true, "Retrying cluster registration");
			self->clusterEntry = dataClusterMetadata.get().entry;
		}

		Optional<int64_t> tenantIdPrefix = wait(tenantIdPrefixFuture);
		ASSERT(tenantIdPrefix.present());

		self->tenantIdPrefix = tenantIdPrefix.get();

		TraceEvent("RegisteringDataCluster")
		    .detail("ClusterName", self->clusterName)
		    .detail("ClusterID", self->clusterEntry.id)
		    .detail("Capacity", self->clusterEntry.capacity)
		    .detail("ConnectionString", self->connectionString.toString())
		    .detail("AutoTenantAssignment",
		            DataClusterEntry::autoTenantAssignmentToString(self->clusterEntry.autoTenantAssignment));

		return Void();
	}

	ACTOR static Future<Void> configureDataCluster(RegisterClusterImpl* self) {
		state Reference<IDatabase> dataClusterDb = wait(util::openDatabase(self->connectionString));
		state Reference<ITransaction> tr = dataClusterDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Future<std::vector<std::pair<TenantName, int64_t>>> existingTenantsFuture =
				    TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
				state ThreadFuture<RangeResult> existingDataFuture = tr->getRange(normalKeys, 1);
				state Future<bool> tombstoneFuture =
				    metadata::registrationTombstones().exists(tr, self->clusterEntry.id);

				// Check whether this cluster has already been registered
				state Optional<MetaclusterRegistrationEntry> existingRegistration =
				    wait(metadata::metaclusterRegistration().get(tr));
				if (existingRegistration.present()) {
					if (existingRegistration.get().clusterType != ClusterType::METACLUSTER_DATA ||
					    existingRegistration.get().name != self->clusterName ||
					    !existingRegistration.get().matches(self->ctx.metaclusterRegistration.get()) ||
					    existingRegistration.get().id != self->clusterEntry.id) {
						CODE_PROBE(true, "Cluster already registered during data cluster configuration");
						throw cluster_already_registered();
					} else {
						// We already successfully registered the cluster with these details, so there's nothing to
						// do
						CODE_PROBE(true, "Cluster registration already done on data cluster");
						return Void();
					}
				}

				// Check if the cluster was removed concurrently
				bool tombstone = wait(tombstoneFuture);
				if (tombstone) {
					CODE_PROBE(true, "Registering cluster removed");
					throw cluster_removed();
				}

				// Check for any existing data
				std::vector<std::pair<TenantName, int64_t>> existingTenants =
				    wait(safeThreadFutureToFuture(existingTenantsFuture));
				if (!existingTenants.empty()) {
					CODE_PROBE(true, "Registering cluster with tenants");
					TraceEvent(SevWarn, "CannotRegisterClusterWithTenants").detail("ClusterName", self->clusterName);
					throw cluster_not_empty();
				}

				RangeResult existingData = wait(safeThreadFutureToFuture(existingDataFuture));
				if (!existingData.empty()) {
					CODE_PROBE(true, "Registering cluster with data", probe::decoration::rare);
					TraceEvent(SevWarn, "CannotRegisterClusterWithData").detail("ClusterName", self->clusterName);
					throw cluster_not_empty();
				}

				metadata::metaclusterRegistration().set(
				    tr,
				    self->ctx.metaclusterRegistration.get().toDataClusterRegistration(self->clusterName,
				                                                                      self->clusterEntry.id));

				TenantMetadata::tenantIdPrefix().set(tr, self->tenantIdPrefix);

				// The data cluster will track the last ID it allocated in this metacluster, so erase any prior tenant
				// ID state
				TenantMetadata::lastTenantId().clear(tr);

				// If we happen to have any orphaned restore IDs from a previous time this cluster was in a metacluster,
				// erase them now.
				metadata::activeRestoreIds().clear(tr);

				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

				TraceEvent("ConfiguredDataCluster")
				    .detail("ClusterName", self->clusterName)
				    .detail("ClusterID", self->clusterEntry.id)
				    .detail("Capacity", self->clusterEntry.capacity)
				    .detail("Version", tr->getCommittedVersion())
				    .detail("ConnectionString", self->connectionString.toString());

				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	// Store the cluster entry for the new cluster
	ACTOR static Future<Void> markClusterReady(RegisterClusterImpl* self, Reference<typename DB::TransactionT> tr) {
		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (!dataClusterMetadata.present() ||
		    dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING) {
			CODE_PROBE(true, "Registering cluster removed");
			throw cluster_removed();
		} else if (dataClusterMetadata.get().entry.id != self->clusterEntry.id) {
			CODE_PROBE(true, "Registering cluster exists with different ID");
			throw cluster_already_exists();
		} else if (dataClusterMetadata.get().entry.clusterState == DataClusterState::READY) {
			CODE_PROBE(true, "Registering cluster already ready");
			return Void();
		} else if (dataClusterMetadata.get().entry.clusterState == DataClusterState::RESTORING) {
			CODE_PROBE(true, "Registering cluster restoring");
			throw cluster_restoring();
		} else {
			ASSERT(dataClusterMetadata.get().entry.clusterState == DataClusterState::REGISTERING);
			dataClusterMetadata.get().entry.clusterState = DataClusterState::READY;

			if (dataClusterMetadata.get().entry.hasCapacity() &&
			    dataClusterMetadata.get().entry.autoTenantAssignment == AutoTenantAssignment::ENABLED) {
				metadata::management::clusterCapacityIndex().insert(
				    tr, Tuple::makeTuple(dataClusterMetadata.get().entry.allocated.numTenantGroups, self->clusterName));
			}
			metadata::management::dataClusters().set(tr, self->clusterName, dataClusterMetadata.get().entry);
			metadata::management::dataClusterConnectionRecords().set(tr, self->clusterName, self->connectionString);
		}

		TraceEvent("RegisteredDataCluster")
		    .detail("ClusterName", self->clusterName)
		    .detail("ClusterID", self->clusterEntry.id)
		    .detail("Capacity", dataClusterMetadata.get().entry.capacity)
		    .detail("ConnectionString", self->connectionString.toString())
		    .detail("AutoTenantAssignment",
		            DataClusterEntry::autoTenantAssignmentToString(self->clusterEntry.autoTenantAssignment));

		return Void();
	}

	ACTOR static Future<Void> run(RegisterClusterImpl* self) {
		// Used if we need to rollback
		state internal::RemoveClusterImpl<DB> removeCluster(self->ctx.managementDb,
		                                                    self->clusterName,
		                                                    ClusterType::METACLUSTER_MANAGEMENT,
		                                                    metacluster::ForceRemove::True,
		                                                    5.0);

		if (self->clusterName.startsWith("\xff"_sr)) {
			CODE_PROBE(true, "Registering cluster with invalid name");
			throw invalid_cluster_name();
		}

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return registerInManagementCluster(self, tr); }));

		// Don't use ctx to run this transaction because we have not set up the data cluster metadata on it and we
		// don't have a metacluster registration on the data cluster
		try {
			wait(configureDataCluster(self));
		} catch (Error& e) {
			state Error error = e;
			try {
				// Attempt to unregister the cluster if we could not configure the data cluster. We should only do this
				// if the data cluster state matches our ID and is in the REGISTERING in case somebody else has
				// attempted to complete the registration or start a new one.
				CODE_PROBE(true, "Rollback cluster registration");
				removeCluster.clusterId = self->clusterEntry.id;
				removeCluster.legalClusterStates.insert(DataClusterState::REGISTERING);
				wait(removeCluster.run());
				TraceEvent("RegisterClusterRolledBack")
				    .detail("ClusterName", self->clusterName)
				    .detail("ConnectionString", self->connectionString.toString());
			} catch (Error& e) {
				// Removing the cluster after failing to register the data cluster is a best effort attempt. If it
				// fails, the operator will need to remove it (or re-register it) themselves.
				TraceEvent(SevWarn, "RegisterClusterRollbackFailed")
				    .detail("ClusterName", self->clusterName)
				    .detail("ConnectionString", self->connectionString.toString());
			}
			throw error;
		}

		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return markClusterReady(self, tr); }));

		return Void();
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<Void> registerCluster(Reference<DB> db,
                             ClusterName name,
                             ClusterConnectionString connectionString,
                             DataClusterEntry entry) {
	state internal::RegisterClusterImpl<DB> impl(db, name, connectionString, entry);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif