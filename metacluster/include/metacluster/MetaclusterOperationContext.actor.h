/*
 * MetaclusterOperationContext.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_METACLUSTEROPERATIONCONTEXT_ACTOR_G_H)
#define METACLUSTER_METACLUSTEROPERATIONCONTEXT_ACTOR_G_H
#include "metacluster/MetaclusterOperationContext.actor.g.h"
#elif !defined(METACLUSTER_METACLUSTEROPERATIONCONTEXT_ACTOR_H)
#define METACLUSTER_METACLUSTEROPERATIONCONTEXT_ACTOR_H

#include "fdbclient/IClientApi.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/VersionedMap.h"
#include "fdbrpc/TenantName.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

#include "metacluster/GetCluster.actor.h"
#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/MetaclusterUtil.actor.h"

#include "flow/actorcompiler.h" // has to be last include

FDB_BOOLEAN_PARAM(RunOnDisconnectedCluster);
FDB_BOOLEAN_PARAM(RunOnMismatchedCluster);

namespace metacluster {

template <class DB>
struct MetaclusterOperationContext {
	Reference<DB> managementDb;
	Reference<IDatabase> dataClusterDb;

	Optional<ClusterName> clusterName;

	Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
	Optional<DataClusterMetadata> dataClusterMetadata;
	bool dataClusterIsRegistered = true;

	std::set<DataClusterState> extraSupportedDataClusterStates;

	MetaclusterOperationContext(Reference<DB> managementDb,
	                            Optional<ClusterName> clusterName = {},
	                            std::set<DataClusterState> extraSupportedDataClusterStates = {})
	  : managementDb(managementDb), clusterName(clusterName),
	    extraSupportedDataClusterStates(extraSupportedDataClusterStates) {}

	void checkClusterState() {
		DataClusterState clusterState =
		    dataClusterMetadata.present() ? dataClusterMetadata.get().entry.clusterState : DataClusterState::READY;
		if (clusterState != DataClusterState::READY && extraSupportedDataClusterStates.count(clusterState) == 0) {
			if (clusterState == DataClusterState::REGISTERING) {
				CODE_PROBE(true, "Run metacluster transaction on registering cluster");
				throw cluster_not_found();
			} else if (clusterState == DataClusterState::REMOVING) {
				CODE_PROBE(true, "Run metacluster transaction on removing cluster");
				throw cluster_removed();
			} else if (clusterState == DataClusterState::RESTORING) {
				CODE_PROBE(true, "Run metacluster transaction on restoring cluster");
				throw cluster_restoring();
			}

			ASSERT(false);
		}
	}

	// Run a transaction on the management cluster. This verifies that the cluster is a management cluster and
	// matches the same metacluster that we've run any previous transactions on. If a clusterName is set, it also
	// verifies that the specified cluster is present. Stores the metaclusterRegistration entry and, if a
	// clusterName is set, the dataClusterMetadata and dataClusterDb in the context.
	ACTOR template <class Function>
	static Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runManagementTransaction(MetaclusterOperationContext* self, Function func) {
		state Reference<typename DB::TransactionT> tr = self->managementDb->createTransaction();
		state bool clusterPresentAtStart = self->clusterName.present();
		loop {
			try {
				// If this transaction is retrying and didn't have the cluster name set at the beginning, clear it
				// out to be set again in the next iteration.
				if (!clusterPresentAtStart) {
					CODE_PROBE(true, "Retry management transaction with unconfigured cluster");
					self->clearCluster();
				}

				// Get the data cluster metadata for the specified cluster, if present
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Future<Optional<DataClusterMetadata>> dataClusterMetadataFuture;
				if (self->clusterName.present()) {
					dataClusterMetadataFuture = tryGetClusterTransaction(tr, self->clusterName.get());
				}

				// Get the metacluster registration information
				state Optional<MetaclusterRegistrationEntry> currentMetaclusterRegistration =
				    wait(metadata::metaclusterRegistration().get(tr));

				state Optional<DataClusterMetadata> currentDataClusterMetadata;
				if (self->clusterName.present()) {
					wait(store(currentDataClusterMetadata, dataClusterMetadataFuture));
				}

				// Check that this is a management cluster and is the same metacluster that any previous
				// transactions have run on.
				if (!currentMetaclusterRegistration.present() ||
				    currentMetaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
					CODE_PROBE(true, "Run management transaction on non-management cluster");
					throw invalid_metacluster_operation();
				} else if (self->metaclusterRegistration.present() &&
				           !self->metaclusterRegistration.get().matches(currentMetaclusterRegistration.get())) {
					CODE_PROBE(true, "Run management transaction on wrong management cluster", probe::decoration::rare);
					throw metacluster_mismatch();
				}

				// If a cluster was specified, check that the cluster metadata is present. If so, load it and store
				// it in the context. Additionally, store the data cluster details in the local metacluster
				// registration entry.
				if (self->clusterName.present()) {
					if (!currentDataClusterMetadata.present()) {
						CODE_PROBE(true, "Run management transaction using deleted data cluster");
						throw cluster_removed();
					} else {
						currentMetaclusterRegistration = currentMetaclusterRegistration.get().toDataClusterRegistration(
						    self->clusterName.get(), currentDataClusterMetadata.get().entry.id);
					}
				}

				// Store the metacluster registration entry
				if (!self->metaclusterRegistration.present()) {
					self->metaclusterRegistration = currentMetaclusterRegistration;
				}

				// Check that our data cluster has the same ID as previous transactions. If so, then store the
				// updated cluster metadata in the context and open a connection to the data DB.
				if (self->dataClusterMetadata.present() &&
				    self->dataClusterMetadata.get().entry.id != currentDataClusterMetadata.get().entry.id) {
					CODE_PROBE(true, "Run management transaction using wrong data cluster");
					throw cluster_removed();
				} else if (self->clusterName.present()) {
					self->dataClusterMetadata = currentDataClusterMetadata;
					if (!self->dataClusterDb) {
						wait(store(self->dataClusterDb,
						           util::openDatabase(self->dataClusterMetadata.get().connectionString)));
					}
				}

				self->checkClusterState();

				state decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue()) result =
				    wait(func(tr));

				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				return result;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	template <class Function>
	Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runManagementTransaction(Function func) {
		return runManagementTransaction(this, func);
	}

	// Runs a transaction on the data cluster. This requires that a cluster name be set and that a transaction has
	// already been run on the management cluster to populate the needed metadata. This verifies that the data
	// cluster has the expected ID and is part of the metacluster that previous transactions have run on.
	ACTOR template <class Function>
	static Future<decltype(std::declval<Function>()(Reference<ITransaction>()).getValue())> runDataClusterTransaction(
	    MetaclusterOperationContext* self,
	    Function func,
	    RunOnDisconnectedCluster runOnDisconnectedCluster,
	    RunOnMismatchedCluster runOnMismatchedCluster) {
		ASSERT(self->dataClusterDb);
		ASSERT(runOnDisconnectedCluster || self->dataClusterMetadata.present());
		ASSERT(self->metaclusterRegistration.present() &&
		       (runOnDisconnectedCluster ||
		        self->metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA));

		self->checkClusterState();

		state Reference<ITransaction> tr = self->dataClusterDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state bool checkRestoring = !self->extraSupportedDataClusterStates.count(DataClusterState::RESTORING);
				state Future<KeyBackedRangeResult<std::pair<ClusterName, metadata::RestoreId>>> activeRestoreIdFuture;
				if (checkRestoring && self->clusterName.present()) {
					activeRestoreIdFuture = metadata::activeRestoreIds().getRange(tr, {}, {}, 1);
				}

				state Optional<MetaclusterRegistrationEntry> currentMetaclusterRegistration =
				    wait(metadata::metaclusterRegistration().get(tr));

				// Check that this is the expected data cluster and is part of the right metacluster
				if (!currentMetaclusterRegistration.present()) {
					if (!runOnDisconnectedCluster) {
						CODE_PROBE(true, "Run data cluster transaction on non-data cluster");
						throw cluster_removed();
					}
				} else if (currentMetaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_DATA) {
					CODE_PROBE(true, "Run data cluster transaction on non-data cluster", probe::decoration::rare);
					throw cluster_removed();
				} else if (!self->metaclusterRegistration.get().matches(currentMetaclusterRegistration.get())) {
					if (!runOnMismatchedCluster) {
						CODE_PROBE(true, "Run data cluster transaction on wrong data cluster");
						throw cluster_removed();
					}
				}

				if (checkRestoring) {
					KeyBackedRangeResult<std::pair<ClusterName, metadata::RestoreId>> activeRestoreId =
					    wait(activeRestoreIdFuture);
					if (!activeRestoreId.results.empty()) {
						CODE_PROBE(true, "Run data cluster transaction on restoring data cluster");
						throw cluster_restoring();
					}
				}

				self->dataClusterIsRegistered = currentMetaclusterRegistration.present();
				state decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue()) result =
				    wait(func(tr));

				wait(safeThreadFutureToFuture(tr->commit()));
				return result;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	template <class Function>
	Future<decltype(std::declval<Function>()(Reference<ITransaction>()).getValue())> runDataClusterTransaction(
	    Function func,
	    RunOnDisconnectedCluster runOnDisconnectedCluster = RunOnDisconnectedCluster::False,
	    RunOnMismatchedCluster runOnMismatchedCluster = RunOnMismatchedCluster::False) {
		return runDataClusterTransaction(this, func, runOnDisconnectedCluster, runOnMismatchedCluster);
	}

	ACTOR static Future<Void> updateClusterName(MetaclusterOperationContext* self,
	                                            Reference<typename DB::TransactionT> tr) {
		state DataClusterMetadata currentDataClusterMetadata = wait(getClusterTransaction(tr, self->clusterName.get()));

		self->metaclusterRegistration = self->metaclusterRegistration.get().toDataClusterRegistration(
		    self->clusterName.get(), currentDataClusterMetadata.entry.id);

		self->dataClusterMetadata = currentDataClusterMetadata;
		if (!self->dataClusterDb) {
			wait(store(self->dataClusterDb, util::openDatabase(self->dataClusterMetadata.get().connectionString)));
		}

		self->checkClusterState();

		return Void();
	}

	// Sets the cluster used in this context. This must be called from a management cluster transaction, and it
	// will load the cluster metadata and connect to the cluster.
	Future<Void> setCluster(Reference<typename DB::TransactionT> tr, ClusterName clusterName) {
		ASSERT(!this->clusterName.present());
		ASSERT(!dataClusterMetadata.present());
		ASSERT(metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_MANAGEMENT);
		this->clusterName = clusterName;
		return updateClusterName(this, tr);
	}

	// Clears the chosen cluster for this context. This is useful if we are retrying a transaction that expects an
	// uninitialized cluster.
	void clearCluster() {
		clusterName = {};
		dataClusterMetadata = {};
		dataClusterDb = {};
		if (metaclusterRegistration.present() &&
		    metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA) {
			metaclusterRegistration = metaclusterRegistration.get().toManagementClusterRegistration();
		}
	}
};
} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif