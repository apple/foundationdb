/*
 * MetaclusterManagement.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_G_H)
#define FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_G_H
#include "fdbclient/MetaclusterManagement.actor.g.h"
#elif !defined(FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_H)
#define FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_H

#include <limits>

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/VersionedMap.h"
#include "fdbrpc/TenantName.h"
#include "flow/FastRef.h"
#include "flow/flat_buffers.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // has to be last include

// This file provides the interfaces to manage metacluster metadata.
//
// These transactions can operate on clusters at different versions, so care needs to be taken to update the metadata
// according to the cluster version.
//
// Support is maintained in this file for the current and the previous protocol versions.

struct DataClusterMetadata {
	constexpr static FileIdentifier file_identifier = 5573993;

	DataClusterEntry entry;
	ClusterConnectionString connectionString;

	DataClusterMetadata() = default;
	DataClusterMetadata(DataClusterEntry const& entry, ClusterConnectionString const& connectionString)
	  : entry(entry), connectionString(connectionString) {}

	bool matchesConfiguration(DataClusterMetadata const& other) const {
		return entry.matchesConfiguration(other.entry) && connectionString == other.connectionString;
	}

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static DataClusterMetadata decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<DataClusterMetadata>(value, IncludeVersion());
	}

	json_spirit::mValue toJson() const {
		json_spirit::mObject obj = entry.toJson();
		obj["connection_string"] = connectionString.toString();
		return obj;
	}

	bool operator==(DataClusterMetadata const& other) const {
		return entry == other.entry && connectionString == other.connectionString;
	}

	bool operator!=(DataClusterMetadata const& other) const { return !(*this == other); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connectionString, entry);
	}
};

FDB_DECLARE_BOOLEAN_PARAM(ApplyManagementClusterUpdates);
FDB_DECLARE_BOOLEAN_PARAM(RemoveMissingTenants);
FDB_DECLARE_BOOLEAN_PARAM(AssignClusterAutomatically);
FDB_DECLARE_BOOLEAN_PARAM(GroupAlreadyExists);
FDB_DECLARE_BOOLEAN_PARAM(IsRestoring);
FDB_DECLARE_BOOLEAN_PARAM(RunOnDisconnectedCluster);
FDB_DECLARE_BOOLEAN_PARAM(RunOnMismatchedCluster);
FDB_DECLARE_BOOLEAN_PARAM(RestoreDryRun);
FDB_DECLARE_BOOLEAN_PARAM(ForceJoin);
FDB_DECLARE_BOOLEAN_PARAM(ForceRemove);
FDB_DECLARE_BOOLEAN_PARAM(IgnoreCapacityLimit);

namespace MetaclusterAPI {

// This prefix is used during a cluster restore if the desired name is in use
//
// SOMEDAY: this should probably live in the `\xff` tenant namespace, but other parts of the code are not able to work
// with `\xff` tenants yet. In the unlikely event that we have regular tenants using this prefix, we have a rename
// cycle, and we have a collision between the names, a restore will fail with an error. This error can be resolved
// manually using tenant rename commands.
const StringRef metaclusterTemporaryRenamePrefix = "\xfe/restoreTenant/"_sr;

struct ManagementClusterMetadata {
	struct ConnectionStringCodec {
		static inline Standalone<StringRef> pack(ClusterConnectionString const& val) {
			return StringRef(val.toString());
		}
		static inline ClusterConnectionString unpack(Standalone<StringRef> const& val) {
			return ClusterConnectionString(val.toString());
		}
	};

	static TenantMetadataSpecification<MetaclusterTenantTypes>& tenantMetadata();

	// A map from cluster name to the metadata associated with a cluster
	static KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())>& dataClusters();

	// A map from cluster name to the connection string for the cluster
	static KeyBackedMap<ClusterName, ClusterConnectionString, TupleCodec<ClusterName>, ConnectionStringCodec>
	    dataClusterConnectionRecords;

	// A set of non-full clusters where the key is the tuple (num tenant groups allocated, cluster name).
	static KeyBackedSet<Tuple> clusterCapacityIndex;

	// A map from cluster name to a count of tenants
	static KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>> clusterTenantCount;

	// A set of (cluster name, tenant name, tenant ID) tuples ordered by cluster
	// Renaming tenants are stored twice in the index, with the destination name stored with ID -1
	static KeyBackedSet<Tuple> clusterTenantIndex;

	// A set of (cluster, tenant group name) tuples ordered by cluster
	static KeyBackedSet<Tuple> clusterTenantGroupIndex;
};

// Helper function to compute metacluster capacity by passing the result of MetaclusterAPI::listClusters
std::pair<ClusterUsage, ClusterUsage> metaclusterCapacity(std::map<ClusterName, DataClusterMetadata> const& clusters);

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString);

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> tryGetClusterTransaction(Transaction tr, ClusterName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Void> metaclusterRegistrationCheck =
	    TenantAPI::checkTenantMode(tr, ClusterType::METACLUSTER_MANAGEMENT);

	state Future<Optional<DataClusterEntry>> clusterEntryFuture =
	    ManagementClusterMetadata::dataClusters().get(tr, name);
	state Future<Optional<ClusterConnectionString>> connectionRecordFuture =
	    ManagementClusterMetadata::dataClusterConnectionRecords.get(tr, name);

	wait(metaclusterRegistrationCheck);

	state Optional<DataClusterEntry> clusterEntry = wait(clusterEntryFuture);
	Optional<ClusterConnectionString> connectionString = wait(connectionRecordFuture);

	if (clusterEntry.present()) {
		ASSERT(connectionString.present());
		return Optional<DataClusterMetadata>(DataClusterMetadata(clusterEntry.get(), connectionString.get()));
	} else {
		return Optional<DataClusterMetadata>();
	}
}

ACTOR template <class DB>
Future<Optional<DataClusterMetadata>> tryGetCluster(Reference<DB> db, ClusterName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
			return metadata;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<DataClusterMetadata> getClusterTransaction(Transaction tr, ClusterNameRef name) {
	Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
	if (!metadata.present()) {
		throw cluster_not_found();
	}

	return metadata.get();
}

ACTOR template <class DB>
Future<DataClusterMetadata> getCluster(Reference<DB> db, ClusterName name) {
	Optional<DataClusterMetadata> metadata = wait(tryGetCluster(db, name));
	if (!metadata.present()) {
		throw cluster_not_found();
	}

	return metadata.get();
}

ACTOR template <class Transaction>
Future<Reference<IDatabase>> getAndOpenDatabase(Transaction managementTr, ClusterName clusterName) {
	DataClusterMetadata clusterMetadata = wait(getClusterTransaction(managementTr, clusterName));
	Reference<IDatabase> db = wait(openDatabase(clusterMetadata.connectionString));
	return db;
}

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
				throw cluster_not_found();
			} else if (clusterState == DataClusterState::REMOVING) {
				throw cluster_removed();
			} else if (clusterState == DataClusterState::RESTORING) {
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
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				state Optional<DataClusterMetadata> currentDataClusterMetadata;
				if (self->clusterName.present()) {
					wait(store(currentDataClusterMetadata, dataClusterMetadataFuture));
				}

				// Check that this is a management cluster and is the same metacluster that any previous
				// transactions have run on.
				if (!currentMetaclusterRegistration.present() ||
				    currentMetaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
					throw invalid_metacluster_operation();
				} else if (self->metaclusterRegistration.present() &&
				           !self->metaclusterRegistration.get().matches(currentMetaclusterRegistration.get())) {
					throw metacluster_mismatch();
				}

				// If a cluster was specified, check that the cluster metadata is present. If so, load it and store
				// it in the context. Additionally, store the data cluster details in the local metacluster
				// registration entry.
				if (self->clusterName.present()) {
					if (!currentDataClusterMetadata.present()) {
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
					throw cluster_removed();
				} else if (self->clusterName.present()) {
					self->dataClusterMetadata = currentDataClusterMetadata;
					if (!self->dataClusterDb) {
						wait(
						    store(self->dataClusterDb, openDatabase(self->dataClusterMetadata.get().connectionString)));
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
				state Future<KeyBackedRangeResult<std::pair<ClusterName, UID>>> activeRestoreIdFuture;
				if (checkRestoring && self->clusterName.present()) {
					activeRestoreIdFuture = MetaclusterMetadata::activeRestoreIds().getRange(tr, {}, {}, 1);
				}

				state Optional<MetaclusterRegistrationEntry> currentMetaclusterRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				// Check that this is the expected data cluster and is part of the right metacluster
				if (!currentMetaclusterRegistration.present()) {
					if (!runOnDisconnectedCluster) {
						throw cluster_removed();
					}
				} else if (currentMetaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_DATA) {
					throw cluster_removed();
				} else if (!self->metaclusterRegistration.get().matches(currentMetaclusterRegistration.get())) {
					if (!runOnMismatchedCluster) {
						throw cluster_removed();
					}
				}

				if (checkRestoring) {
					KeyBackedRangeResult<std::pair<ClusterName, UID>> activeRestoreId = wait(activeRestoreIdFuture);
					if (!activeRestoreId.results.empty()) {
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
			wait(store(self->dataClusterDb, openDatabase(self->dataClusterMetadata.get().connectionString)));
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

template <class Transaction>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenantTransaction(Transaction tr, int64_t tenantId) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return ManagementClusterMetadata::tenantMetadata().tenantMap.get(tr, tenantId);
}

ACTOR template <class Transaction>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<int64_t> tenantId = wait(ManagementClusterMetadata::tenantMetadata().tenantNameIndex.get(tr, name));
	if (tenantId.present()) {
		Optional<MetaclusterTenantMapEntry> entry =
		    wait(ManagementClusterMetadata::tenantMetadata().tenantMap.get(tr, tenantId.get()));
		return entry;
	} else {
		return Optional<MetaclusterTenantMapEntry>();
	}
}

ACTOR template <class DB, class Tenant>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenant(Reference<DB> db, Tenant tenant) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction, class Tenant>
Future<MetaclusterTenantMapEntry> getTenantTransaction(Transaction tr, Tenant tenant) {
	Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class DB, class Tenant>
Future<MetaclusterTenantMapEntry> getTenant(Reference<DB> db, Tenant tenant) {
	Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenant(db, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class Transaction>
Future<Void> managementClusterCheckEmpty(Transaction tr) {
	state Future<KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>>> tenantsFuture =
	    TenantMetadata::tenantMap().getRange(tr, {}, {}, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type dbContentsFuture =
	    tr->getRange(normalKeys, 1);

	KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants = wait(tenantsFuture);
	if (!tenants.results.empty()) {
		throw cluster_not_empty();
	}

	RangeResult dbContents = wait(safeThreadFutureToFuture(dbContentsFuture));
	if (!dbContents.empty()) {
		throw cluster_not_empty();
	}

	return Void();
}

ACTOR template <class Transaction>
Future<TenantMode> getClusterConfiguredTenantMode(Transaction tr) {
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(tenantModeConfKey);
	Optional<Value> tenantModeValue = wait(safeThreadFutureToFuture(tenantModeFuture));
	return TenantMode::fromValue(tenantModeValue.castTo<ValueRef>());
}

ACTOR template <class DB>
Future<Optional<std::string>> createMetacluster(Reference<DB> db,
                                                ClusterName name,
                                                int64_t tenantIdPrefix,
                                                bool enableTenantModeCheck) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Optional<UID> metaclusterUid;
	ASSERT(tenantIdPrefix >= TenantAPI::TENANT_ID_PREFIX_MIN_VALUE &&
	       tenantIdPrefix <= TenantAPI::TENANT_ID_PREFIX_MAX_VALUE);

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state Future<Optional<MetaclusterRegistrationEntry>> metaclusterRegistrationFuture =
			    MetaclusterMetadata::metaclusterRegistration().get(tr);

			state Future<Void> metaclusterEmptinessCheck = managementClusterCheckEmpty(tr);
			state Future<TenantMode> tenantModeFuture =
			    enableTenantModeCheck ? getClusterConfiguredTenantMode(tr) : Future<TenantMode>(TenantMode::DISABLED);

			Optional<MetaclusterRegistrationEntry> existingRegistration = wait(metaclusterRegistrationFuture);
			if (existingRegistration.present()) {
				if (metaclusterUid.present() && metaclusterUid.get() == existingRegistration.get().metaclusterId) {
					return Optional<std::string>();
				} else {
					return fmt::format("cluster is already registered as a {} named `{}'",
					                   existingRegistration.get().clusterType == ClusterType::METACLUSTER_DATA
					                       ? "data cluster"
					                       : "metacluster",
					                   printable(existingRegistration.get().name));
				}
			}

			wait(metaclusterEmptinessCheck);
			TenantMode tenantMode = wait(tenantModeFuture);
			if (tenantMode != TenantMode::DISABLED) {
				return fmt::format("cluster is configured with tenant mode `{}' when tenants should be disabled",
				                   tenantMode);
			}

			if (!metaclusterUid.present()) {
				metaclusterUid = deterministicRandom()->randomUniqueID();
			}

			MetaclusterMetadata::metaclusterRegistration().set(
			    tr, MetaclusterRegistrationEntry(name, metaclusterUid.get()));

			TenantMetadata::tenantIdPrefix().set(tr, tenantIdPrefix);

			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	TraceEvent("CreatedMetacluster").detail("Name", name).detail("Prefix", tenantIdPrefix);

	return Optional<std::string>();
}

ACTOR template <class DB>
Future<Void> decommissionMetacluster(Reference<DB> db) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state bool firstTry = true;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
				if (firstTry) {
					throw invalid_metacluster_operation();
				} else {
					return Void();
				}
			}

			// Erase all metadata not associated with specific tenants prior to checking
			// cluster emptiness
			ManagementClusterMetadata::tenantMetadata().tenantCount.clear(tr);
			ManagementClusterMetadata::tenantMetadata().lastTenantId.clear(tr);
			ManagementClusterMetadata::tenantMetadata().tenantTombstones.clear(tr);
			ManagementClusterMetadata::tenantMetadata().tombstoneCleanupData.clear(tr);
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.clear(tr);

			wait(managementClusterCheckEmpty(tr));
			MetaclusterMetadata::metaclusterRegistration().clear(tr);

			firstTry = false;
			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	return Void();
}

template <class Transaction>
void updateClusterCapacityIndex(Transaction tr,
                                ClusterName name,
                                DataClusterEntry const& previousEntry,
                                DataClusterEntry const& updatedEntry) {
	// Entries are put in the cluster capacity index ordered by how many items are already allocated to them
	if (previousEntry.hasCapacity()) {
		ManagementClusterMetadata::clusterCapacityIndex.erase(
		    tr, Tuple::makeTuple(previousEntry.allocated.numTenantGroups, name));
	}
	if (updatedEntry.hasCapacity()) {
		ManagementClusterMetadata::clusterCapacityIndex.insert(
		    tr, Tuple::makeTuple(updatedEntry.allocated.numTenantGroups, name));
	}
}

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
		           (!updatedEntry.present() || (updatedEntry.get().clusterState != DataClusterState::READY &&
		                                        updatedEntry.get().clusterState != DataClusterState::REMOVING))) {
			throw cluster_restoring();
		} else if (isRestoring) {
			ASSERT(previousMetadata.entry.clusterState == DataClusterState::RESTORING ||
			       updatedEntry.get().clusterState == DataClusterState::RESTORING);
		}
		ManagementClusterMetadata::dataClusters().set(tr, name, updatedEntry.get());
		updateClusterCapacityIndex(tr, name, previousMetadata.entry, updatedEntry.get());
	}
	if (updatedConnectionString.present()) {
		ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, name, updatedConnectionString.get());
	}
}

template <class DB>
struct RemoveClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	Reference<DB> db;
	ClusterType clusterType;
	ClusterName clusterName;
	ForceRemove forceRemove;
	double dataClusterTimeout;

	// Optional parameters that are set by internal users
	Optional<UID> clusterId;
	std::set<DataClusterState> legalClusterStates;

	// Parameters set in markClusterRemoving
	Optional<int64_t> lastTenantId;

	// Output parameter indicating whether the data cluster was updated during the removal
	bool dataClusterUpdated = false;

	RemoveClusterImpl(Reference<DB> db,
	                  ClusterName clusterName,
	                  ClusterType clusterType,
	                  ForceRemove forceRemove,
	                  double dataClusterTimeout)
	  : ctx(db,
	        Optional<ClusterName>(),
	        { DataClusterState::REGISTERING, DataClusterState::REMOVING, DataClusterState::RESTORING }),
	    db(db), clusterType(clusterType), clusterName(clusterName), forceRemove(forceRemove),
	    dataClusterTimeout(dataClusterTimeout) {}

	// Returns false if the cluster is no longer present, or true if it is present and the removal should proceed.
	ACTOR static Future<Void> markClusterRemoving(RemoveClusterImpl* self, Reference<typename DB::TransactionT> tr) {
		state DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, self->clusterName));
		wait(self->ctx.setCluster(tr, self->clusterName));

		if ((self->clusterId.present() && clusterMetadata.entry.id != self->clusterId.get()) ||
		    (!self->legalClusterStates.empty() &&
		     !self->legalClusterStates.count(clusterMetadata.entry.clusterState))) {
			// The type of error is currently ignored, and this is only used to terminate the remove operation.
			// If that changes in the future, we may want to introduce a more suitable error type.
			throw operation_failed();
		}

		if (!self->forceRemove && self->ctx.dataClusterMetadata.get().entry.allocated.numTenantGroups > 0) {
			throw cluster_not_empty();
		} else if (self->ctx.dataClusterMetadata.get().entry.clusterState != DataClusterState::REMOVING) {
			// Mark the cluster in a removing state while we finish the remaining removal steps. This prevents new
			// tenants from being assigned to it.
			DataClusterEntry updatedEntry = self->ctx.dataClusterMetadata.get().entry;
			updatedEntry.clusterState = DataClusterState::REMOVING;
			updatedEntry.capacity.numTenantGroups = 0;

			updateClusterMetadata(tr,
			                      self->ctx.clusterName.get(),
			                      self->ctx.dataClusterMetadata.get(),
			                      Optional<ClusterConnectionString>(),
			                      updatedEntry);
		}

		ManagementClusterMetadata::clusterCapacityIndex.erase(
		    tr,
		    Tuple::makeTuple(self->ctx.dataClusterMetadata.get().entry.allocated.numTenantGroups,
		                     self->ctx.clusterName.get()));

		// Get the last allocated tenant ID to be used on the detached data cluster
		if (self->forceRemove) {
			Optional<int64_t> lastId = wait(ManagementClusterMetadata::tenantMetadata().lastTenantId.get(tr));
			self->lastTenantId = lastId;
		}

		TraceEvent("MarkedDataClusterRemoving").detail("Name", self->ctx.clusterName.get());
		return Void();
	}

	// Delete metacluster metadata from the data cluster
	ACTOR template <class Transaction>
	static Future<Void> updateDataCluster(RemoveClusterImpl* self, Reference<Transaction> tr, UID clusterId) {
		if (self->ctx.dataClusterIsRegistered) {
			// Delete metacluster related metadata
			MetaclusterMetadata::metaclusterRegistration().clear(tr);
			MetaclusterMetadata::activeRestoreIds().clear(tr);
			TenantMetadata::tenantTombstones().clear(tr);
			TenantMetadata::tombstoneCleanupData().clear(tr);

			// If we are force removing a cluster, then it will potentially contain tenants that have IDs
			// larger than the next tenant ID to be allocated on the cluster. To avoid collisions, we advance
			// the ID so that it will be the larger of the current one on the data cluster and the management
			// cluster.
			if (self->lastTenantId.present()) {
				Optional<int64_t> lastId = wait(TenantMetadata::lastTenantId().get(tr));
				if (!lastId.present() || lastId.get() < self->lastTenantId.get()) {
					TenantMetadata::lastTenantId().set(tr, self->lastTenantId.get());
				}
			}
		}

		// Insert a tombstone marking this tenant removed even if we aren't registered
		MetaclusterMetadata::registrationTombstones().insert(tr, clusterId);

		TraceEvent("RemovedMetaclusterRegistrationOnDataCluster")
		    .detail("Name", self->clusterName)
		    .detail("WasRegistered", self->ctx.dataClusterIsRegistered);

		return Void();
	}

	// Returns true if all tenants have been purged
	ACTOR static Future<bool> purgeTenants(RemoveClusterImpl* self,
	                                       Reference<typename DB::TransactionT> tr,
	                                       std::pair<Tuple, Tuple> clusterTupleRange) {
		ASSERT(self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING);

		// Get the list of tenants
		state Future<KeyBackedRangeResult<Tuple>> tenantEntriesFuture =
		    ManagementClusterMetadata::clusterTenantIndex.getRange(
		        tr, clusterTupleRange.first, clusterTupleRange.second, CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

		state KeyBackedRangeResult<Tuple> tenantEntries = wait(tenantEntriesFuture);

		// Erase each tenant from the tenant map on the management cluster
		int64_t erasedTenants = 0;
		for (Tuple entry : tenantEntries.results) {
			int64_t tenantId = entry.getInt(2);
			ASSERT(entry.getString(0) == self->ctx.clusterName.get());
			if (tenantId != TenantInfo::INVALID_TENANT) {
				++erasedTenants;
				ManagementClusterMetadata::tenantMetadata().tenantMap.erase(tr, tenantId);
			}
			ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(tr, entry.getString(1));
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		// Erase all of the tenants processed in this transaction from the cluster tenant index
		if (!tenantEntries.results.empty()) {
			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr,
			    clusterTupleRange.first,
			    Tuple::makeTuple(self->ctx.clusterName.get(), keyAfter(tenantEntries.results.rbegin()->getString(1))));
		}

		ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, -erasedTenants, MutationRef::AddValue);
		ManagementClusterMetadata::clusterTenantCount.atomicOp(
		    tr, self->ctx.clusterName.get(), -erasedTenants, MutationRef::AddValue);

		return !tenantEntries.more;
	}

	// Returns true if all tenant groups have been purged
	ACTOR static Future<bool> purgeTenantGroups(RemoveClusterImpl* self,
	                                            Reference<typename DB::TransactionT> tr,
	                                            std::pair<Tuple, Tuple> clusterTupleRange) {
		ASSERT(self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING);

		// Get the list of tenant groups
		state Future<KeyBackedRangeResult<Tuple>> tenantGroupEntriesFuture =
		    ManagementClusterMetadata::clusterTenantGroupIndex.getRange(
		        tr, clusterTupleRange.first, clusterTupleRange.second, CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

		// Erase each tenant group from the tenant group map and the tenant group tenant index
		state KeyBackedRangeResult<Tuple> tenantGroupEntries = wait(tenantGroupEntriesFuture);
		for (Tuple entry : tenantGroupEntries.results) {
			ASSERT(entry.getString(0) == self->ctx.clusterName.get());
			TenantGroupName tenantGroup = entry.getString(1);
			ManagementClusterMetadata::tenantMetadata().tenantGroupTenantIndex.erase(
			    tr, Tuple::makeTuple(tenantGroup), Tuple::makeTuple(keyAfter(tenantGroup)));
			ManagementClusterMetadata::tenantMetadata().tenantGroupMap.erase(tr, tenantGroup);
		}

		if (!tenantGroupEntries.results.empty()) {
			// Erase all of the tenant groups processed in this transaction from the cluster tenant group index
			ManagementClusterMetadata::clusterTenantGroupIndex.erase(
			    tr,
			    clusterTupleRange.first,
			    Tuple::makeTuple(self->ctx.clusterName.get(),
			                     keyAfter(tenantGroupEntries.results.rbegin()->getString(1))));
		}

		return !tenantGroupEntries.more;
	}

	// Removes the data cluster entry from the management cluster
	void removeDataClusterEntry(Reference<typename DB::TransactionT> tr) {
		ManagementClusterMetadata::dataClusters().erase(tr, ctx.clusterName.get());
		ManagementClusterMetadata::dataClusterConnectionRecords.erase(tr, ctx.clusterName.get());
		ManagementClusterMetadata::clusterTenantCount.erase(tr, ctx.clusterName.get());
		MetaclusterMetadata::activeRestoreIds().erase(tr, ctx.clusterName.get());
	}

	// Removes the next set of metadata from the management cluster; returns true when all specified
	// metadata is removed
	ACTOR static Future<bool> managementClusterPurgeSome(RemoveClusterImpl* self,
	                                                     Reference<typename DB::TransactionT> tr,
	                                                     std::pair<Tuple, Tuple> clusterTupleRange,
	                                                     bool* deleteTenants,
	                                                     bool* deleteTenantGroups) {
		if (deleteTenants) {
			bool deletedAllTenants = wait(purgeTenants(self, tr, clusterTupleRange));
			if (!deletedAllTenants) {
				return false;
			}
			*deleteTenants = false;
		}

		if (deleteTenantGroups) {
			bool deletedAllTenantGroups = wait(purgeTenantGroups(self, tr, clusterTupleRange));
			if (!deletedAllTenantGroups) {
				return false;
			}
			*deleteTenantGroups = false;
		}

		self->removeDataClusterEntry(tr);
		return true;
	}

	// Remove all metadata associated with the data cluster from the management cluster
	ACTOR static Future<Void> managementClusterPurgeDataCluster(RemoveClusterImpl* self) {
		state std::pair<Tuple, Tuple> clusterTupleRange = std::make_pair(
		    Tuple::makeTuple(self->ctx.clusterName.get()), Tuple::makeTuple(keyAfter(self->ctx.clusterName.get())));

		state bool deleteTenants = true;
		state bool deleteTenantGroups = true;

		loop {
			bool clearedAll = wait(self->ctx.runManagementTransaction(
			    [self = self,
			     clusterTupleRange = clusterTupleRange,
			     deleteTenants = &deleteTenants,
			     deleteTenantGroups = &deleteTenantGroups](Reference<typename DB::TransactionT> tr) {
				    return managementClusterPurgeSome(self, tr, clusterTupleRange, deleteTenants, deleteTenantGroups);
			    }));

			if (clearedAll) {
				break;
			}
		}

		TraceEvent("RemovedDataCluster").detail("Name", self->ctx.clusterName.get());
		return Void();
	}

	// Remove the metacluster registration entry on a data cluster without modifying the management cluster.
	// Useful when reconstructing a management cluster when the original is lost.
	ACTOR static Future<Void> dataClusterForgetMetacluster(RemoveClusterImpl* self) {
		state Reference<typename DB::TransactionT> tr = self->db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Optional<MetaclusterRegistrationEntry> metaclusterRegistrationEntry =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				if (!metaclusterRegistrationEntry.present()) {
					return Void();
				}

				if (metaclusterRegistrationEntry.get().clusterType != ClusterType::METACLUSTER_DATA) {
					TraceEvent(SevWarn, "CannotRemoveNonDataCluster")
					    .detail("ClusterName", self->clusterName)
					    .detail("MetaclusterRegistration",
					            metaclusterRegistrationEntry.map(&MetaclusterRegistrationEntry::toString));
					throw invalid_metacluster_operation();
				}

				if (metaclusterRegistrationEntry.get().name != self->clusterName) {
					TraceEvent(SevWarn, "CannotRemoveDataClusterWithNameMismatch")
					    .detail("ExpectedName", self->clusterName)
					    .detail("MetaclusterRegistration",
					            metaclusterRegistrationEntry.map(&MetaclusterRegistrationEntry::toString));
					throw metacluster_mismatch();
				}

				wait(updateDataCluster(self, tr, metaclusterRegistrationEntry.get().id));
				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> run(RemoveClusterImpl* self) {
		// On data clusters, we forget the metacluster information without updating the management cluster
		if (self->clusterType == ClusterType::METACLUSTER_DATA) {
			if (!self->forceRemove) {
				throw invalid_metacluster_operation();
			}

			wait(dataClusterForgetMetacluster(self));
			self->dataClusterUpdated = true;
			return Void();
		}

		try {
			wait(self->ctx.runManagementTransaction(
			    [self = self](Reference<typename DB::TransactionT> tr) { return markClusterRemoving(self, tr); }));
		} catch (Error& e) {
			// If the transaction retries after success or if we are trying a second time to remove the cluster, it
			// will throw an error indicating that the removal has already started
			if (e.code() != error_code_cluster_removed) {
				throw;
			}
		}

		try {
			Future<Void> f = self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) {
				    return updateDataCluster(self, tr, self->ctx.metaclusterRegistration.get().id);
			    },
			    RunOnDisconnectedCluster::True);

			if (self->forceRemove && self->dataClusterTimeout > 0) {
				f = timeoutError(f, self->dataClusterTimeout);
			}

			wait(f);
			self->dataClusterUpdated = true;
		} catch (Error& e) {
			// If this transaction gets retried, the metacluster information may have already been erased.
			if (e.code() == error_code_cluster_removed) {
				self->dataClusterUpdated = true;
			} else if (e.code() != error_code_timed_out) {
				throw;
			}
		}

		// This runs multiple transactions, so the run transaction calls are inside the function
		try {
			wait(managementClusterPurgeDataCluster(self));
		} catch (Error& e) {
			// If this transaction gets retried, the cluster may have already been deleted.
			if (e.code() != error_code_cluster_removed) {
				throw;
			}
		}
		return Void();
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<bool> removeCluster(Reference<DB> db,
                           ClusterName name,
                           ClusterType clusterType,
                           ForceRemove forceRemove,
                           double dataClusterTimeout = 0) {
	state RemoveClusterImpl<DB> impl(db, name, clusterType, forceRemove, dataClusterTimeout);
	wait(impl.run());
	return impl.dataClusterUpdated;
}

template <class DB>
struct RegisterClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	ClusterName clusterName;
	ClusterConnectionString connectionString;
	DataClusterEntry clusterEntry;

	RegisterClusterImpl(Reference<DB> managementDb,
	                    ClusterName clusterName,
	                    ClusterConnectionString connectionString,
	                    DataClusterEntry clusterEntry)
	  : ctx(managementDb), clusterName(clusterName), connectionString(connectionString), clusterEntry(clusterEntry) {}

	// Store the cluster entry for the new cluster in a registering state
	ACTOR static Future<Void> registerInManagementCluster(RegisterClusterImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (!dataClusterMetadata.present()) {
			self->clusterEntry.clusterState = DataClusterState::REGISTERING;
			self->clusterEntry.allocated = ClusterUsage();
			self->clusterEntry.id = deterministicRandom()->randomUniqueID();

			ManagementClusterMetadata::dataClusters().set(tr, self->clusterName, self->clusterEntry);
			ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, self->clusterName, self->connectionString);
		} else if (dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING) {
			throw cluster_removed();
		} else if (!dataClusterMetadata.get().matchesConfiguration(
		               DataClusterMetadata(self->clusterEntry, self->connectionString)) ||
		           dataClusterMetadata.get().entry.clusterState != DataClusterState::REGISTERING) {
			throw cluster_already_exists();
		} else {
			self->clusterEntry = dataClusterMetadata.get().entry;
		}

		TraceEvent("RegisteringDataCluster")
		    .detail("ClusterName", self->clusterName)
		    .detail("ClusterID", self->clusterEntry.id)
		    .detail("Capacity", self->clusterEntry.capacity)
		    .detail("ConnectionString", self->connectionString.toString());

		return Void();
	}

	ACTOR static Future<Void> configureDataCluster(RegisterClusterImpl* self) {
		state Reference<IDatabase> dataClusterDb = wait(openDatabase(self->connectionString));
		state Reference<ITransaction> tr = dataClusterDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Future<std::vector<std::pair<TenantName, int64_t>>> existingTenantsFuture =
				    TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
				state ThreadFuture<RangeResult> existingDataFuture = tr->getRange(normalKeys, 1);
				state Future<bool> tombstoneFuture =
				    MetaclusterMetadata::registrationTombstones().exists(tr, self->clusterEntry.id);

				// Check whether this cluster has already been registered
				state Optional<MetaclusterRegistrationEntry> existingRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));
				if (existingRegistration.present()) {
					if (existingRegistration.get().clusterType != ClusterType::METACLUSTER_DATA ||
					    existingRegistration.get().name != self->clusterName ||
					    !existingRegistration.get().matches(self->ctx.metaclusterRegistration.get()) ||
					    existingRegistration.get().id != self->clusterEntry.id) {
						throw cluster_already_registered();
					} else {
						// We already successfully registered the cluster with these details, so there's nothing to
						// do
						return Void();
					}
				}

				// Check if the cluster was removed concurrently
				bool tombstone = wait(tombstoneFuture);
				if (tombstone) {
					throw cluster_removed();
				}

				// Check for any existing data
				std::vector<std::pair<TenantName, int64_t>> existingTenants =
				    wait(safeThreadFutureToFuture(existingTenantsFuture));
				if (!existingTenants.empty()) {
					TraceEvent(SevWarn, "CannotRegisterClusterWithTenants").detail("ClusterName", self->clusterName);
					throw cluster_not_empty();
				}

				RangeResult existingData = wait(safeThreadFutureToFuture(existingDataFuture));
				if (!existingData.empty()) {
					TraceEvent(SevWarn, "CannotRegisterClusterWithData").detail("ClusterName", self->clusterName);
					throw cluster_not_empty();
				}

				MetaclusterMetadata::metaclusterRegistration().set(
				    tr,
				    self->ctx.metaclusterRegistration.get().toDataClusterRegistration(self->clusterName,
				                                                                      self->clusterEntry.id));

				// If we happen to have any orphaned restore IDs from a previous time this cluster was in a metacluster,
				// erase them now.
				MetaclusterMetadata::activeRestoreIds().clear(tr);

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
			throw cluster_removed();
		} else if (dataClusterMetadata.get().entry.id != self->clusterEntry.id) {
			throw cluster_already_exists();
		} else if (dataClusterMetadata.get().entry.clusterState == DataClusterState::READY) {
			return Void();
		} else if (dataClusterMetadata.get().entry.clusterState == DataClusterState::RESTORING) {
			throw cluster_restoring();
		} else {
			ASSERT(dataClusterMetadata.get().entry.clusterState == DataClusterState::REGISTERING);
			dataClusterMetadata.get().entry.clusterState = DataClusterState::READY;

			if (dataClusterMetadata.get().entry.hasCapacity()) {
				ManagementClusterMetadata::clusterCapacityIndex.insert(
				    tr, Tuple::makeTuple(dataClusterMetadata.get().entry.allocated.numTenantGroups, self->clusterName));
			}
			ManagementClusterMetadata::dataClusters().set(tr, self->clusterName, dataClusterMetadata.get().entry);
			ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, self->clusterName, self->connectionString);
		}

		TraceEvent("RegisteredDataCluster")
		    .detail("ClusterName", self->clusterName)
		    .detail("ClusterID", self->clusterEntry.id)
		    .detail("Capacity", dataClusterMetadata.get().entry.capacity)
		    .detail("ConnectionString", self->connectionString.toString());

		return Void();
	}

	ACTOR static Future<Void> run(RegisterClusterImpl* self) {
		// Used if we need to rollback
		state RemoveClusterImpl<DB> removeCluster(
		    self->ctx.managementDb, self->clusterName, ClusterType::METACLUSTER_MANAGEMENT, ForceRemove::True, 5.0);

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

ACTOR template <class DB>
Future<Void> registerCluster(Reference<DB> db,
                             ClusterName name,
                             ClusterConnectionString connectionString,
                             DataClusterEntry entry) {
	state RegisterClusterImpl<DB> impl(db, name, connectionString, entry);
	wait(impl.run());
	return Void();
}

ACTOR template <class Transaction>
Future<std::map<ClusterName, DataClusterMetadata>> listClustersTransaction(Transaction tr,
                                                                           ClusterNameRef begin,
                                                                           ClusterNameRef end,
                                                                           int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Void> tenantModeCheck = TenantAPI::checkTenantMode(tr, ClusterType::METACLUSTER_MANAGEMENT);

	state Future<KeyBackedRangeResult<std::pair<ClusterName, DataClusterEntry>>> clusterEntriesFuture =
	    ManagementClusterMetadata::dataClusters().getRange(tr, begin, end, limit);
	state Future<KeyBackedRangeResult<std::pair<ClusterName, ClusterConnectionString>>> connectionStringFuture =
	    ManagementClusterMetadata::dataClusterConnectionRecords.getRange(tr, begin, end, limit);

	wait(tenantModeCheck);

	state KeyBackedRangeResult<std::pair<ClusterName, DataClusterEntry>> clusterEntries =
	    wait(safeThreadFutureToFuture(clusterEntriesFuture));
	KeyBackedRangeResult<std::pair<ClusterName, ClusterConnectionString>> connectionStrings =
	    wait(safeThreadFutureToFuture(connectionStringFuture));

	ASSERT(clusterEntries.results.size() == connectionStrings.results.size());

	std::map<ClusterName, DataClusterMetadata> clusters;
	for (int i = 0; i < clusterEntries.results.size(); ++i) {
		ASSERT(clusterEntries.results[i].first == connectionStrings.results[i].first);
		clusters[clusterEntries.results[i].first] =
		    DataClusterMetadata(clusterEntries.results[i].second, connectionStrings.results[i].second);
	}

	return clusters;
}

ACTOR template <class DB>
Future<std::map<ClusterName, DataClusterMetadata>> listClusters(Reference<DB> db,
                                                                ClusterName begin,
                                                                ClusterName end,
                                                                int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			std::map<ClusterName, DataClusterMetadata> clusters = wait(listClustersTransaction(tr, begin, end, limit));

			return clusters;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

template <class Transaction>
void managementClusterAddTenantToGroup(Transaction tr,
                                       MetaclusterTenantMapEntry tenantEntry,
                                       DataClusterMetadata* clusterMetadata,
                                       GroupAlreadyExists groupAlreadyExists,
                                       IgnoreCapacityLimit ignoreCapacityLimit = IgnoreCapacityLimit::False,
                                       IsRestoring isRestoring = IsRestoring::False) {
	if (tenantEntry.tenantGroup.present()) {
		if (tenantEntry.tenantGroup.get().startsWith("\xff"_sr)) {
			throw invalid_tenant_group_name();
		}

		if (!groupAlreadyExists) {
			ManagementClusterMetadata::tenantMetadata().tenantGroupMap.set(
			    tr, tenantEntry.tenantGroup.get(), MetaclusterTenantGroupEntry(tenantEntry.assignedCluster));
			ManagementClusterMetadata::clusterTenantGroupIndex.insert(
			    tr, Tuple::makeTuple(tenantEntry.assignedCluster, tenantEntry.tenantGroup.get()));
		}
		ManagementClusterMetadata::tenantMetadata().tenantGroupTenantIndex.insert(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), tenantEntry.id));
	}

	if (!groupAlreadyExists && !isRestoring) {
		ASSERT(ignoreCapacityLimit || clusterMetadata->entry.hasCapacity());

		DataClusterEntry updatedEntry = clusterMetadata->entry;
		++updatedEntry.allocated.numTenantGroups;

		updateClusterMetadata(
		    tr, tenantEntry.assignedCluster, *clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		clusterMetadata->entry = updatedEntry;
	}
}

ACTOR template <class Transaction>
Future<Void> managementClusterRemoveTenantFromGroup(Transaction tr,
                                                    MetaclusterTenantMapEntry tenantEntry,
                                                    DataClusterMetadata* clusterMetadata) {
	state bool updateClusterCapacity = !tenantEntry.tenantGroup.present();
	if (tenantEntry.tenantGroup.present()) {
		ManagementClusterMetadata::tenantMetadata().tenantGroupTenantIndex.erase(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), tenantEntry.id));

		state KeyBackedSet<Tuple>::RangeResultType result =
		    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupTenantIndex.getRange(
		        tr,
		        Tuple::makeTuple(tenantEntry.tenantGroup.get()),
		        Tuple::makeTuple(keyAfter(tenantEntry.tenantGroup.get())),
		        1));

		if (result.results.size() == 0) {
			ManagementClusterMetadata::clusterTenantGroupIndex.erase(
			    tr, Tuple::makeTuple(tenantEntry.assignedCluster, tenantEntry.tenantGroup.get()));

			ManagementClusterMetadata::tenantMetadata().tenantGroupMap.erase(tr, tenantEntry.tenantGroup.get());
			updateClusterCapacity = true;
		}
	}

	// Update the tenant group count information for the assigned cluster if this tenant group was erased so we
	// can use the freed capacity.
	if (updateClusterCapacity) {
		DataClusterEntry updatedEntry = clusterMetadata->entry;
		--updatedEntry.allocated.numTenantGroups;
		updateClusterMetadata(
		    tr, tenantEntry.assignedCluster, *clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		clusterMetadata->entry = updatedEntry;
	}

	return Void();
}

template <class DB>
struct RestoreClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	ClusterName clusterName;
	ClusterConnectionString connectionString;
	ApplyManagementClusterUpdates applyManagementClusterUpdates;
	RestoreDryRun restoreDryRun;
	ForceJoin forceJoin;
	std::vector<std::string>& messages;

	// Unique ID generated for this restore. Used to avoid concurrent restores
	UID restoreId = deterministicRandom()->randomUniqueID();

	// Loaded from the data cluster
	UID dataClusterId;

	// Tenant list from data and management clusters
	std::unordered_map<int64_t, TenantMapEntry> dataClusterTenantMap;
	std::unordered_set<TenantName> dataClusterTenantNames;
	std::unordered_map<int64_t, MetaclusterTenantMapEntry> mgmtClusterTenantMap;
	std::unordered_set<int64_t> mgmtClusterTenantSetForCurrentDataCluster;

	RestoreClusterImpl(Reference<DB> managementDb,
	                   ClusterName clusterName,
	                   ClusterConnectionString connectionString,
	                   ApplyManagementClusterUpdates applyManagementClusterUpdates,
	                   RestoreDryRun restoreDryRun,
	                   ForceJoin forceJoin,
	                   std::vector<std::string>& messages)
	  : ctx(managementDb, {}, { DataClusterState::RESTORING }), clusterName(clusterName),
	    connectionString(connectionString), applyManagementClusterUpdates(applyManagementClusterUpdates),
	    restoreDryRun(restoreDryRun), forceJoin(forceJoin), messages(messages) {}

	ACTOR template <class Transaction>
	static Future<Void> checkRestoreId(RestoreClusterImpl* self, Transaction tr) {
		if (!self->restoreDryRun) {
			Optional<UID> activeRestoreId = wait(MetaclusterMetadata::activeRestoreIds().get(tr, self->clusterName));
			if (!activeRestoreId.present() || activeRestoreId.get() != self->restoreId) {
				throw conflicting_restore();
			}
		}

		return Void();
	}

	ACTOR template <class Transaction>
	static Future<Void> eraseRestoreId(RestoreClusterImpl* self, Transaction tr) {
		Optional<UID> transactionId = wait(MetaclusterMetadata::activeRestoreIds().get(tr, self->clusterName));
		if (!transactionId.present()) {
			return Void();
		} else if (transactionId.get() != self->restoreId) {
			throw conflicting_restore();
		} else {
			MetaclusterMetadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
			MetaclusterMetadata::activeRestoreIds().erase(tr, self->clusterName);
		}

		return Void();
	}

	template <class Function>
	Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runRestoreManagementTransaction(Function func) {
		return ctx.runManagementTransaction([this, func](Reference<typename DB::TransactionT> tr) {
			return joinWith(func(tr), checkRestoreId(this, tr));
		});
	}

	template <class Function>
	Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runRestoreDataClusterTransaction(
	    Function func,
	    RunOnDisconnectedCluster runOnDisconnectedCluster = RunOnDisconnectedCluster::False,
	    RunOnMismatchedCluster runOnMismatchedCluster = RunOnMismatchedCluster::False) {
		return ctx.runDataClusterTransaction(
		    [this, func](Reference<ITransaction> tr) { return joinWith(func(tr), checkRestoreId(this, tr)); },
		    runOnDisconnectedCluster,
		    runOnMismatchedCluster);
	}

	// If restoring a data cluster, verify that it has a matching registration entry
	ACTOR static Future<Void> loadDataClusterRegistration(RestoreClusterImpl* self) {
		state Reference<IDatabase> db = wait(openDatabase(self->connectionString));
		state Reference<ITransaction> tr = db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				if (!metaclusterRegistration.present()) {
					throw invalid_data_cluster();
				} else if (!metaclusterRegistration.get().matches(self->ctx.metaclusterRegistration.get())) {
					if (!self->forceJoin) {
						TraceEvent(SevWarn, "MetaclusterRestoreClusterMismatch")
						    .detail("ExistingRegistration", metaclusterRegistration.get())
						    .detail("ManagementClusterRegistration", self->ctx.metaclusterRegistration.get());
						throw cluster_already_registered();
					} else if (!self->restoreDryRun) {
						ASSERT(self->ctx.metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);
						MetaclusterMetadata::metaclusterRegistration().set(tr, self->ctx.metaclusterRegistration.get());
					} else {
						self->messages.push_back(fmt::format("Move data cluster to new metacluster\n"
						                                     "        original: {}\n"
						                                     "        updated:  {}",
						                                     metaclusterRegistration.get().toString(),
						                                     self->ctx.metaclusterRegistration.get().toString()));
					}
				} else if (metaclusterRegistration.get().name != self->clusterName) {
					TraceEvent(SevWarn, "MetaclusterRestoreClusterNameMismatch")
					    .detail("ExistingName", metaclusterRegistration.get().name)
					    .detail("ManagementClusterRegistration", self->clusterName);
					throw cluster_already_registered();
				}

				self->dataClusterId = metaclusterRegistration.get().id;
				self->ctx.dataClusterDb = db;

				if (!self->restoreDryRun) {
					wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				}

				return Void();
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	// Store the cluster entry for the restored cluster
	ACTOR static Future<Void> registerRestoringClusterInManagementCluster(RestoreClusterImpl* self,
	                                                                      Reference<typename DB::TransactionT> tr) {
		state DataClusterEntry clusterEntry;
		clusterEntry.id = self->dataClusterId;
		clusterEntry.clusterState = DataClusterState::RESTORING;

		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (dataClusterMetadata.present() &&
		    (dataClusterMetadata.get().entry.clusterState != DataClusterState::RESTORING ||
		     dataClusterMetadata.get().entry.id != clusterEntry.id ||
		     !dataClusterMetadata.get().matchesConfiguration(
		         DataClusterMetadata(clusterEntry, self->connectionString)))) {
			TraceEvent("RestoredClusterAlreadyExists").detail("ClusterName", self->clusterName);
			throw cluster_already_exists();
		} else if (!self->restoreDryRun) {
			MetaclusterMetadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
			MetaclusterMetadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);

			ManagementClusterMetadata::dataClusters().set(tr, self->clusterName, clusterEntry);
			ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, self->clusterName, self->connectionString);

			TraceEvent("RegisteredRestoringDataCluster")
			    .detail("ClusterName", self->clusterName)
			    .detail("ClusterID", clusterEntry.id)
			    .detail("Capacity", clusterEntry.capacity)
			    .detail("Version", tr->getCommittedVersion())
			    .detail("ConnectionString", self->connectionString.toString());
		}

		return Void();
	}

	// If adding a data cluster to a restored management cluster, write a metacluster registration entry
	// to attach it
	ACTOR static Future<Void> writeDataClusterRegistration(RestoreClusterImpl* self) {
		state Reference<IDatabase> db = wait(openDatabase(self->connectionString));
		state Reference<ITransaction> tr = db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Future<bool> tombstoneFuture =
				    MetaclusterMetadata::registrationTombstones().exists(tr, self->dataClusterId);

				state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				// Check if the cluster was removed concurrently
				bool tombstone = wait(tombstoneFuture);
				if (tombstone) {
					throw cluster_removed();
				}

				MetaclusterRegistrationEntry dataClusterEntry =
				    self->ctx.metaclusterRegistration.get().toDataClusterRegistration(self->clusterName,
				                                                                      self->dataClusterId);

				if (metaclusterRegistration.present()) {
					if (dataClusterEntry.matches(metaclusterRegistration.get())) {
						break;
					}

					TraceEvent(SevWarn, "MetaclusterRestoreClusterAlreadyRegistered")
					    .detail("ExistingRegistration", metaclusterRegistration.get())
					    .detail("NewRegistration", dataClusterEntry);
					throw cluster_already_registered();
				}

				if (!self->restoreDryRun) {
					MetaclusterMetadata::metaclusterRegistration().set(tr, dataClusterEntry);
					MetaclusterMetadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
					MetaclusterMetadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);
					wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				}

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		return Void();
	}

	void markClusterRestoring(Reference<typename DB::TransactionT> tr) {
		MetaclusterMetadata::activeRestoreIds().addReadConflictKey(tr, clusterName);
		MetaclusterMetadata::activeRestoreIds().set(tr, clusterName, restoreId);
		if (ctx.dataClusterMetadata.get().entry.clusterState != DataClusterState::RESTORING) {
			DataClusterEntry updatedEntry = ctx.dataClusterMetadata.get().entry;
			updatedEntry.clusterState = DataClusterState::RESTORING;

			updateClusterMetadata(tr, clusterName, ctx.dataClusterMetadata.get(), connectionString, updatedEntry);
			// Remove this cluster from the cluster capacity index, but leave its configured capacity intact in the
			// cluster entry. This allows us to retain the configured capacity while preventing the cluster from
			// being used to allocate new tenant groups.
			DataClusterEntry noCapacityEntry = updatedEntry;
			noCapacityEntry.capacity.numTenantGroups = 0;
			updateClusterCapacityIndex(tr, clusterName, updatedEntry, noCapacityEntry);
		}

		TraceEvent("MarkedDataClusterRestoring").detail("Name", clusterName);
	}

	Future<Void> markClusterAsReady(Reference<typename DB::TransactionT> tr) {
		if (ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::RESTORING) {
			DataClusterEntry updatedEntry = ctx.dataClusterMetadata.get().entry;
			updatedEntry.clusterState = DataClusterState::READY;

			updateClusterMetadata(tr, clusterName, ctx.dataClusterMetadata.get(), {}, updatedEntry);

			// Add this cluster back to the cluster capacity index so that it can be assigned to again.
			DataClusterEntry noCapacityEntry = updatedEntry;
			noCapacityEntry.capacity.numTenantGroups = 0;
			updateClusterCapacityIndex(tr, clusterName, noCapacityEntry, updatedEntry);

			return eraseRestoreId(this, tr);
		}

		return Void();
	}

	ACTOR static Future<Void> markManagementTenantsAsError(RestoreClusterImpl* self,
	                                                       Reference<typename DB::TransactionT> tr,
	                                                       std::vector<int64_t> tenants) {
		ASSERT(!self->restoreDryRun);
		state std::vector<Future<Optional<MetaclusterTenantMapEntry>>> getFutures;
		for (auto tenantId : tenants) {
			getFutures.push_back(tryGetTenantTransaction(tr, tenantId));
		}

		wait(waitForAll(getFutures));

		for (auto const& f : getFutures) {
			if (!f.get().present()) {
				continue;
			}

			MetaclusterTenantMapEntry entry = f.get().get();
			entry.tenantState = MetaclusterAPI::TenantState::ERROR;
			entry.error = "The tenant is missing after restoring its data cluster";
			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, entry.id, entry);
		}

		return Void();
	}

	ACTOR static Future<Void> getTenantsFromDataCluster(RestoreClusterImpl* self, Reference<ITransaction> tr) {
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants =
		    wait(TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));

		for (auto const& t : tenants.results) {
			self->dataClusterTenantMap.emplace(t.first, t.second);
			self->dataClusterTenantNames.insert(t.second.tenantName);
		}

		return Void();
	}

	ACTOR static Future<Optional<int64_t>> getTenantsFromManagementCluster(RestoreClusterImpl* self,
	                                                                       Reference<typename DB::TransactionT> tr,
	                                                                       int64_t initialTenantId) {
		state KeyBackedRangeResult<std::pair<int64_t, MetaclusterTenantMapEntry>> tenants =
		    wait(ManagementClusterMetadata::tenantMetadata().tenantMap.getRange(
		        tr, initialTenantId, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));

		for (auto const& t : tenants.results) {
			self->mgmtClusterTenantMap.emplace(t.first, t.second);
			if (self->clusterName == t.second.assignedCluster) {
				self->mgmtClusterTenantSetForCurrentDataCluster.emplace(t.first);
			}
		}

		return tenants.more ? Optional<int64_t>(tenants.results.rbegin()->first + 1) : Optional<int64_t>();
	}

	ACTOR static Future<Void> getAllTenantsFromManagementCluster(RestoreClusterImpl* self) {
		// get all tenants across all data clusters
		state Optional<int64_t> beginTenant = 0;
		while (beginTenant.present()) {
			wait(store(beginTenant,
			           self->runRestoreManagementTransaction(
			               [self = self, beginTenant = beginTenant](Reference<typename DB::TransactionT> tr) {
				               return getTenantsFromManagementCluster(self, tr, beginTenant.get());
			               })));
		}

		return Void();
	}

	ACTOR static Future<Void> renameTenant(RestoreClusterImpl* self,
	                                       Reference<ITransaction> tr,
	                                       int64_t tenantId,
	                                       TenantName oldTenantName,
	                                       TenantName newTenantName,
	                                       int configurationSequenceNum) {
		state Optional<TenantMapEntry> entry;
		state Optional<int64_t> newId;

		wait(store(entry, TenantAPI::tryGetTenantTransaction(tr, tenantId)) &&
		     store(newId, TenantMetadata::tenantNameIndex().get(tr, newTenantName)));

		if (entry.present()) {
			if (entry.get().tenantName == oldTenantName && !newId.present()) {
				wait(TenantAPI::renameTenantTransaction(tr,
				                                        oldTenantName,
				                                        newTenantName,
				                                        tenantId,
				                                        ClusterType::METACLUSTER_DATA,
				                                        configurationSequenceNum));
				return Void();
			} else if (entry.get().tenantName == newTenantName && newId.present() && newId.get() == tenantId) {
				// The tenant has already been renamed
				return Void();
			}
		}

		TraceEvent(SevWarnAlways, "RestoreDataClusterRenameError")
		    .detail("OldName", oldTenantName)
		    .detail("NewName", newTenantName)
		    .detail("TenantID", tenantId)
		    .detail("ActualTenantName", entry.map(&TenantMapEntry::tenantName))
		    .detail("OldEntryPresent", entry.present())
		    .detail("NewEntryPresent", newId.present());

		if (newId.present()) {
			self->messages.push_back(
			    fmt::format("Failed to rename the tenant `{}' to `{}' because the new name is already in use",
			                printable(oldTenantName),
			                printable(newTenantName)));
			throw tenant_already_exists();
		} else {
			self->messages.push_back(fmt::format(
			    "Failed to rename the tenant `{}' to `{}' because the tenant did not have the expected ID {}",
			    printable(oldTenantName),
			    printable(newTenantName),
			    tenantId));
			throw tenant_not_found();
		}
	}

	ACTOR static Future<Void> updateTenantConfiguration(RestoreClusterImpl* self,
	                                                    Reference<ITransaction> tr,
	                                                    int64_t tenantId,
	                                                    TenantMapEntry updatedEntry) {
		TenantMapEntry existingEntry = wait(TenantAPI::getTenantTransaction(tr, tenantId));

		// The tenant should have already been renamed, so in most cases its name will match.
		// If we had to break a rename cycle using temporary tenant names, use that in the updated
		// entry here since the rename will be completed later.
		if (existingEntry.tenantName != updatedEntry.tenantName) {
			ASSERT(existingEntry.tenantName.startsWith(metaclusterTemporaryRenamePrefix));
			updatedEntry.tenantName = existingEntry.tenantName;
		}

		if (existingEntry.configurationSequenceNum <= updatedEntry.configurationSequenceNum) {
			wait(TenantAPI::configureTenantTransaction(tr, existingEntry, updatedEntry));
		}

		return Void();
	}

	// Updates a tenant to match the management cluster state
	// Returns the name of the tenant after it has been reconciled
	ACTOR static Future<Optional<std::pair<TenantName, MetaclusterTenantMapEntry>>> reconcileTenant(
	    RestoreClusterImpl* self,
	    TenantMapEntry tenantEntry) {
		state std::unordered_map<int64_t, MetaclusterTenantMapEntry>::iterator managementEntry =
		    self->mgmtClusterTenantMap.find(tenantEntry.id);

		// A data cluster tenant is not present on the management cluster
		if (managementEntry == self->mgmtClusterTenantMap.end() ||
		    managementEntry->second.assignedCluster != self->clusterName ||
		    managementEntry->second.tenantState == TenantState::REMOVING) {
			if (self->restoreDryRun) {
				if (managementEntry == self->mgmtClusterTenantMap.end()) {
					self->messages.push_back(fmt::format("Delete missing tenant `{}' with ID {} on data cluster",
					                                     printable(tenantEntry.tenantName),
					                                     tenantEntry.id));
				} else if (managementEntry->second.assignedCluster != self->clusterName) {
					self->messages.push_back(fmt::format(
					    "Delete tenant `{}' with ID {} on data cluster because it is now located on the cluster `{}'",
					    printable(tenantEntry.tenantName),
					    tenantEntry.id,
					    printable(managementEntry->second.assignedCluster)));
				} else {
					self->messages.push_back(
					    fmt::format("Delete tenant `{}' with ID {} on data cluster because it is in the REMOVING state",
					                printable(tenantEntry.tenantName),
					                tenantEntry.id));
				}
			} else {
				wait(self->runRestoreDataClusterTransaction([tenantEntry = tenantEntry](Reference<ITransaction> tr) {
					return TenantAPI::deleteTenantTransaction(tr, tenantEntry.id, ClusterType::METACLUSTER_DATA);
				}));
			}

			return Optional<std::pair<TenantName, MetaclusterTenantMapEntry>>();
		} else {
			state TenantName tenantName = tenantEntry.tenantName;
			state MetaclusterTenantMapEntry managementTenant = managementEntry->second;

			// Rename
			state TenantName managementTenantName = managementTenant.tenantState != TenantState::RENAMING
			                                            ? managementTenant.tenantName
			                                            : managementTenant.renameDestination.get();
			state bool renamed = tenantName != managementTenantName;
			if (renamed) {
				state TenantName temporaryName;
				state bool usingTemporaryName = self->dataClusterTenantNames.count(managementTenantName) > 0;
				if (usingTemporaryName) {
					temporaryName = metaclusterTemporaryRenamePrefix.withSuffix(managementTenantName);
				} else {
					temporaryName = managementTenantName;
				}

				if (self->restoreDryRun) {
					self->messages.push_back(fmt::format("Rename tenant `{}' with ID {} to `{}' on data cluster{}",
					                                     printable(tenantEntry.tenantName),
					                                     tenantEntry.id,
					                                     printable(managementTenantName),
					                                     usingTemporaryName ? " via temporary name" : ""));
				} else {
					wait(self->runRestoreDataClusterTransaction(
					    [self = self,
					     tenantName = tenantName,
					     temporaryName = temporaryName,
					     tenantEntry = tenantEntry,
					     managementTenant = managementTenant](Reference<ITransaction> tr) {
						    return renameTenant(self,
						                        tr,
						                        tenantEntry.id,
						                        tenantName,
						                        temporaryName,
						                        managementTenant.configurationSequenceNum);
					    }));
					// SOMEDAY: we could mark the tenant in the management cluster as READY if it is in the RENAMING
					// state
				}
				tenantName = temporaryName;
			}

			// Update configuration
			bool configurationChanged = !managementTenant.matchesConfiguration(tenantEntry);
			if (configurationChanged ||
			    managementTenant.configurationSequenceNum != tenantEntry.configurationSequenceNum) {
				if (self->restoreDryRun) {
					// If this is an update to the internal sequence number only and we are also renaming the tenant,
					// we don't need to report anything. The internal metadata update is (at least partially) caused
					// by the rename in that case
					if (configurationChanged || !renamed) {
						self->messages.push_back(
						    fmt::format("Update tenant configuration for tenant `{}' with ID {} on data cluster{}",
						                printable(tenantEntry.tenantName),
						                tenantEntry.id,
						                configurationChanged ? "" : " (internal metadata only)"));
					}
				} else {
					wait(self->runRestoreDataClusterTransaction([self = self,
					                                             managementTenant = managementTenant,
					                                             tenantEntry = tenantEntry,
					                                             tenantName = tenantName](Reference<ITransaction> tr) {
						ASSERT_GE(managementTenant.configurationSequenceNum, tenantEntry.configurationSequenceNum);
						TenantMapEntry updatedEntry = managementTenant.toTenantMapEntry();
						updatedEntry.tenantName = tenantName;
						return updateTenantConfiguration(self, tr, managementTenant.id, updatedEntry);
					}));
					// SOMEDAY: we could mark the tenant in the management cluster as READY if it is in the
					// UPDATING_CONFIGURATION state
				}
			}

			return std::make_pair(tenantName, managementTenant);
		}
	}

	Future<Void> renameTenantBatch(std::map<TenantName, TenantMapEntry> tenantsToRename) {
		return runRestoreDataClusterTransaction([this, tenantsToRename](Reference<ITransaction> tr) {
			std::vector<Future<Void>> renameFutures;
			for (auto t : tenantsToRename) {
				renameFutures.push_back(renameTenant(
				    this, tr, t.second.id, t.first, t.second.tenantName, t.second.configurationSequenceNum));
			}
			return waitForAll(renameFutures);
		});
	}

	ACTOR static Future<Void> reconcileTenants(RestoreClusterImpl* self) {
		state std::vector<Future<Optional<std::pair<TenantName, MetaclusterTenantMapEntry>>>> reconcileFutures;
		for (auto itr = self->dataClusterTenantMap.begin(); itr != self->dataClusterTenantMap.end(); ++itr) {
			reconcileFutures.push_back(reconcileTenant(self, itr->second));
		}

		wait(waitForAll(reconcileFutures));

		if (!self->restoreDryRun) {
			state int reconcileIndex;
			state std::map<TenantName, TenantMapEntry> tenantsToRename;
			for (reconcileIndex = 0; reconcileIndex < reconcileFutures.size(); ++reconcileIndex) {
				Optional<std::pair<TenantName, MetaclusterTenantMapEntry>> const& result =
				    reconcileFutures[reconcileIndex].get();

				if (result.present() && result.get().first.startsWith(metaclusterTemporaryRenamePrefix)) {
					TenantMapEntry destinationTenant = result.get().second.toTenantMapEntry();
					if (result.get().second.renameDestination.present()) {
						destinationTenant.tenantName = result.get().second.renameDestination.get();
					}

					if (result.get().first != destinationTenant.tenantName) {
						tenantsToRename[result.get().first] = destinationTenant;

						if (tenantsToRename.size() >= CLIENT_KNOBS->METACLUSTER_RESTORE_BATCH_SIZE) {
							wait(self->renameTenantBatch(tenantsToRename));
						}
					}
				}
			}

			if (!tenantsToRename.empty()) {
				wait(self->renameTenantBatch(tenantsToRename));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> processMissingTenants(RestoreClusterImpl* self) {
		state std::unordered_set<int64_t>::iterator setItr = self->mgmtClusterTenantSetForCurrentDataCluster.begin();
		state std::vector<int64_t> missingTenants;
		state int64_t missingTenantCount = 0;
		while (setItr != self->mgmtClusterTenantSetForCurrentDataCluster.end()) {
			int64_t tenantId = *setItr;
			MetaclusterTenantMapEntry const& managementTenant = self->mgmtClusterTenantMap[tenantId];

			// If a tenant is present on the management cluster and not on the data cluster, mark it in an error
			// state unless it is already in certain states (e.g. REGISTERING, REMOVING) that allow the tenant to be
			// missing on the data cluster
			//
			// SOMEDAY: this could optionally complete the partial operations (e.g. finish creating or removing the
			// tenant)
			if (self->dataClusterTenantMap.find(tenantId) == self->dataClusterTenantMap.end() &&
			    managementTenant.tenantState != MetaclusterAPI::TenantState::REGISTERING &&
			    managementTenant.tenantState != MetaclusterAPI::TenantState::REMOVING) {
				if (self->restoreDryRun) {
					self->messages.push_back(fmt::format("The tenant `{}' with ID {} is missing on the data cluster",
					                                     printable(managementTenant.tenantName),
					                                     tenantId));
				} else {
					// Tenants in an error state that aren't on the data cluster count as missing tenants. This will
					// include tenants we previously marked as missing, and as new errors are added it could include
					// other tenants
					++missingTenantCount;
					if (managementTenant.tenantState != MetaclusterAPI::TenantState::ERROR) {
						missingTenants.push_back(tenantId);
						if (missingTenants.size() == CLIENT_KNOBS->METACLUSTER_RESTORE_BATCH_SIZE) {
							wait(self->runRestoreManagementTransaction([self = self, missingTenants = missingTenants](
							                                               Reference<typename DB::TransactionT> tr) {
								return markManagementTenantsAsError(self, tr, missingTenants);
							}));
							missingTenants.clear();
						}
					}
				}
			}
			++setItr;
		}

		if (!self->restoreDryRun && missingTenants.size() > 0) {
			wait(self->runRestoreManagementTransaction(
			    [self = self, missingTenants = missingTenants](Reference<typename DB::TransactionT> tr) {
				    return markManagementTenantsAsError(self, tr, missingTenants);
			    }));
		}

		if (missingTenantCount > 0) {
			self->messages.push_back(fmt::format(
			    "The metacluster has {} tenants that are missing in the restored data cluster", missingTenantCount));
		}
		return Void();
	}

	// Returns true if the group needs to be created
	ACTOR static Future<bool> addTenantToManagementCluster(RestoreClusterImpl* self,
	                                                       Reference<ITransaction> tr,
	                                                       TenantMapEntry tenantEntry) {
		state Future<Optional<MetaclusterTenantGroupEntry>> tenantGroupEntry = Optional<MetaclusterTenantGroupEntry>();
		if (tenantEntry.tenantGroup.present()) {
			tenantGroupEntry =
			    ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(tr, tenantEntry.tenantGroup.get());
		}

		Optional<MetaclusterTenantMapEntry> existingEntry = wait(tryGetTenantTransaction(tr, tenantEntry.tenantName));
		if (existingEntry.present()) {
			if (existingEntry.get().assignedCluster == self->clusterName) {
				if (existingEntry.get().id != tenantEntry.id ||
				    !existingEntry.get().matchesConfiguration(tenantEntry)) {
					ASSERT(self->restoreDryRun);
					self->messages.push_back(
					    fmt::format("The tenant `{}' was modified concurrently with the restore dry-run",
					                printable(tenantEntry.tenantName)));
					throw tenant_already_exists();
				}

				// This is a retry, so return success
				return false;
			} else {
				self->messages.push_back(fmt::format("The tenant `{}' already exists on cluster `{}'",
				                                     printable(tenantEntry.tenantName),
				                                     printable(existingEntry.get().assignedCluster)));
				throw tenant_already_exists();
			}
		}

		state MetaclusterTenantMapEntry managementEntry = MetaclusterTenantMapEntry::fromTenantMapEntry(tenantEntry);
		managementEntry.assignedCluster = self->clusterName;

		if (!self->restoreDryRun) {
			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, managementEntry.id, managementEntry);
			ManagementClusterMetadata::tenantMetadata().tenantNameIndex.set(
			    tr, managementEntry.tenantName, managementEntry.id);

			ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, 1, MutationRef::AddValue);
			ManagementClusterMetadata::clusterTenantCount.atomicOp(
			    tr, managementEntry.assignedCluster, 1, MutationRef::AddValue);

			// Updated indexes to include the new tenant
			ManagementClusterMetadata::clusterTenantIndex.insert(
			    tr, Tuple::makeTuple(managementEntry.assignedCluster, managementEntry.tenantName, managementEntry.id));
		}

		wait(success(tenantGroupEntry));

		if (tenantGroupEntry.get().present() && tenantGroupEntry.get().get().assignedCluster != self->clusterName) {
			self->messages.push_back(
			    fmt::format("The tenant `{}' is part of a tenant group `{}' that already exists on cluster `{}'",
			                printable(managementEntry.tenantName),
			                printable(managementEntry.tenantGroup.get()),
			                printable(tenantGroupEntry.get().get().assignedCluster)));
			throw invalid_tenant_configuration();
		}

		if (!self->restoreDryRun) {
			managementClusterAddTenantToGroup(tr,
			                                  managementEntry,
			                                  &self->ctx.dataClusterMetadata.get(),
			                                  GroupAlreadyExists(tenantGroupEntry.get().present()),
			                                  IgnoreCapacityLimit::True,
			                                  IsRestoring::True);
		}

		return !tenantGroupEntry.get().present();
	}

	ACTOR static Future<Void> addTenantBatchToManagementCluster(RestoreClusterImpl* self,
	                                                            Reference<typename DB::TransactionT> tr,
	                                                            std::vector<TenantMapEntry> tenants) {
		Optional<int64_t> tenantIdPrefix = wait(TenantMetadata::tenantIdPrefix().get(tr));
		ASSERT(tenantIdPrefix.present());

		state std::vector<Future<bool>> futures;
		state int64_t maxId = tenantIdPrefix.get() << 48;
		for (auto const& t : tenants) {
			if (TenantAPI::getTenantIdPrefix(t.id) == tenantIdPrefix.get()) {
				maxId = std::max(maxId, t.id);
			}
			futures.push_back(addTenantToManagementCluster(self, tr, t));
		}

		wait(waitForAll(futures));

		std::set<TenantGroupName> groupsCreated;
		state int numGroupsCreated = 0;
		for (int i = 0; i < tenants.size(); ++i) {
			if (futures[i].get()) {
				if (tenants[i].tenantGroup.present()) {
					groupsCreated.insert(tenants[i].tenantGroup.get());
				} else {
					++numGroupsCreated;
				}
			}
		}

		numGroupsCreated += groupsCreated.size();

		if (!self->restoreDryRun) {
			if (numGroupsCreated > 0) {
				state DataClusterMetadata clusterMetadata = wait(getClusterTransaction(tr, self->clusterName));

				DataClusterEntry updatedEntry = clusterMetadata.entry;
				updatedEntry.allocated.numTenantGroups += numGroupsCreated;
				updateClusterMetadata(tr,
				                      self->clusterName,
				                      clusterMetadata,
				                      Optional<ClusterConnectionString>(),
				                      updatedEntry,
				                      IsRestoring::True);
			}

			int64_t lastTenantId =
			    wait(ManagementClusterMetadata::tenantMetadata().lastTenantId.getD(tr, Snapshot::False, 0));

			ManagementClusterMetadata::tenantMetadata().lastTenantId.set(tr, std::max(lastTenantId, maxId));
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	ACTOR static Future<Void> addTenantsToManagementCluster(RestoreClusterImpl* self) {
		state std::unordered_map<int64_t, TenantMapEntry>::iterator itr;
		state std::vector<TenantMapEntry> tenantBatch;
		state int64_t tenantsToAdd = 0;

		for (itr = self->dataClusterTenantMap.begin(); itr != self->dataClusterTenantMap.end(); ++itr) {
			state std::unordered_map<int64_t, MetaclusterTenantMapEntry>::iterator managementEntry =
			    self->mgmtClusterTenantMap.find(itr->second.id);
			if (managementEntry == self->mgmtClusterTenantMap.end()) {
				++tenantsToAdd;
				tenantBatch.push_back(itr->second);
			} else if (managementEntry->second.tenantName != itr->second.tenantName ||
			           managementEntry->second.assignedCluster != self->clusterName ||
			           !managementEntry->second.matchesConfiguration(itr->second)) {
				self->messages.push_back(
				    fmt::format("The tenant `{}' has the same ID {} as an existing tenant `{}' on cluster `{}'",
				                printable(itr->second.tenantName),
				                itr->second.id,
				                printable(managementEntry->second.tenantName),
				                printable(managementEntry->second.assignedCluster)));
				throw tenant_already_exists();
			}

			if (tenantBatch.size() == CLIENT_KNOBS->METACLUSTER_RESTORE_BATCH_SIZE) {
				wait(runTransaction(self->ctx.managementDb,
				                    [self = self, tenantBatch = tenantBatch](Reference<typename DB::TransactionT> tr) {
					                    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					                    return addTenantBatchToManagementCluster(self, tr, tenantBatch);
				                    }));
				tenantBatch.clear();
			}
		}

		if (!tenantBatch.empty()) {
			wait(runTransaction(self->ctx.managementDb,
			                    [self = self, tenantBatch = tenantBatch](Reference<typename DB::TransactionT> tr) {
				                    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				                    return addTenantBatchToManagementCluster(self, tr, tenantBatch);
			                    }));
		}

		if (self->restoreDryRun) {
			self->messages.push_back(
			    fmt::format("Restore will add {} tenant(s) to the management cluster from the data cluster `{}'",
			                tenantsToAdd,
			                printable(self->clusterName)));
		}

		return Void();
	}

	ACTOR static Future<Void> runDataClusterRestore(RestoreClusterImpl* self) {
		// Run a management transaction to populate the data cluster metadata
		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return self->ctx.setCluster(tr, self->clusterName);
		}));

		// Make sure that the data cluster being restored has the appropriate metacluster registration entry and
		// name
		wait(loadDataClusterRegistration(self));

		// set state to restoring
		if (!self->restoreDryRun) {
			try {
				wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
					self->markClusterRestoring(tr);
					return Future<Void>(Void());
				}));
			} catch (Error& e) {
				// If the transaction retries after success or if we are trying a second time to restore the cluster, it
				// will throw an error indicating that the restore has already started
				if (e.code() != error_code_cluster_restoring) {
					throw;
				}
			}
		}

		// Set the restore ID in the data cluster
		if (!self->restoreDryRun) {
			wait(self->ctx.runDataClusterTransaction([self = self](Reference<ITransaction> tr) {
				MetaclusterMetadata::activeRestoreIds().addReadConflictKey(tr, self->clusterName);
				MetaclusterMetadata::activeRestoreIds().set(tr, self->clusterName, self->restoreId);
				return Future<Void>(Void());
			}));
		}

		// get all the tenants in the metacluster
		wait(getAllTenantsFromManagementCluster(self));

		// get all the tenant information from the newly registered data cluster
		wait(self->runRestoreDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getTenantsFromDataCluster(self, tr); },
		    RunOnDisconnectedCluster::False,
		    RunOnMismatchedCluster(self->restoreDryRun && self->forceJoin)));

		// Fix any differences between the data cluster and the management cluster
		wait(reconcileTenants(self));

		// Mark tenants that are missing from the data cluster in an error state on the management cluster
		wait(processMissingTenants(self));

		if (!self->restoreDryRun) {
			// Remove the active restore ID from the data cluster
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return eraseRestoreId(self, tr); }));

			// set restored cluster to ready state
			wait(self->ctx.runManagementTransaction(
			    [self = self](Reference<typename DB::TransactionT> tr) { return self->markClusterAsReady(tr); }));
			TraceEvent("MetaclusterRepopulatedFromDataCluster").detail("Name", self->clusterName);
		}

		return Void();
	}

	ACTOR static Future<Void> runManagementClusterRepopulate(RestoreClusterImpl* self) {
		self->dataClusterId = deterministicRandom()->randomUniqueID();

		// Record the data cluster in the management cluster
		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return registerRestoringClusterInManagementCluster(self, tr);
		}));

		// Write a metacluster registration entry in the data cluster
		wait(writeDataClusterRegistration(self));

		if (!self->restoreDryRun) {
			wait(self->runRestoreManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return self->ctx.setCluster(tr, self->clusterName);
			}));
		}

		// get all the tenants in the metacluster
		wait(getAllTenantsFromManagementCluster(self));

		if (self->restoreDryRun) {
			wait(store(self->ctx.dataClusterDb, openDatabase(self->connectionString)));
		}

		// get all the tenant information from the newly registered data cluster
		wait(self->runRestoreDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return getTenantsFromDataCluster(self, tr); },
		    RunOnDisconnectedCluster(self->restoreDryRun)));

		// Add all tenants from the data cluster to the management cluster
		wait(addTenantsToManagementCluster(self));

		if (!self->restoreDryRun) {
			// Remove the active restore ID from the data cluster
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return eraseRestoreId(self, tr); }));

			// set restored cluster to ready state
			wait(self->ctx.runManagementTransaction(
			    [self = self](Reference<typename DB::TransactionT> tr) { return self->markClusterAsReady(tr); }));
			TraceEvent("DataClusterRestoredToMetacluster").detail("Name", self->clusterName);
		}

		return Void();
	}

	Future<Void> run() {
		if (applyManagementClusterUpdates) {
			return runDataClusterRestore(this);
		} else {
			return runManagementClusterRepopulate(this);
		}
	}
};

ACTOR template <class DB>
Future<Void> restoreCluster(Reference<DB> db,
                            ClusterName name,
                            ClusterConnectionString connectionString,
                            ApplyManagementClusterUpdates applyManagementClusterUpdates,
                            RestoreDryRun restoreDryRun,
                            ForceJoin forceJoin,
                            std::vector<std::string>* messages) {
	state RestoreClusterImpl<DB> impl(
	    db, name, connectionString, applyManagementClusterUpdates, restoreDryRun, forceJoin, *messages);
	wait(impl.run());
	return Void();
}

template <class DB>
struct CreateTenantImpl {
	MetaclusterOperationContext<DB> ctx;
	AssignClusterAutomatically assignClusterAutomatically;

	// Initialization parameters
	MetaclusterTenantMapEntry tenantEntry;

	// Parameter set if tenant creation permanently fails on the data cluster
	Optional<int64_t> replaceExistingTenantId;

	CreateTenantImpl(Reference<DB> managementDb,
	                 MetaclusterTenantMapEntry tenantEntry,
	                 AssignClusterAutomatically assignClusterAutomatically)
	  : ctx(managementDb), tenantEntry(tenantEntry), assignClusterAutomatically(assignClusterAutomatically) {}

	ACTOR static Future<ClusterName> checkClusterAvailability(Reference<IDatabase> dataClusterDb,
	                                                          ClusterName clusterName) {
		state Reference<ITransaction> tr = dataClusterDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->addWriteConflictRange(KeyRangeRef("\xff/metacluster/availability_check"_sr,
				                                      "\xff/metacluster/availability_check\x00"_sr));
				wait(safeThreadFutureToFuture(tr->commit()));
				return clusterName;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	// Returns true if the tenant is already assigned and can proceed to the next step and false if it needs
	// to be created. Throws an error if the tenant already exists and cannot be created.
	ACTOR static Future<bool> checkForExistingTenant(CreateTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		// Check if the tenant already exists. If it's partially created and matches the parameters we
		// specified, continue creating it. Otherwise, fail with an error.
		state Optional<MetaclusterTenantMapEntry> existingEntry =
		    wait(tryGetTenantTransaction(tr, self->tenantEntry.tenantName));
		if (existingEntry.present()) {
			if (!existingEntry.get().matchesConfiguration(self->tenantEntry) ||
			    existingEntry.get().tenantState != MetaclusterAPI::TenantState::REGISTERING) {
				// The tenant already exists and is either completely created or has a different
				// configuration
				throw tenant_already_exists();
			} else if (!self->replaceExistingTenantId.present() ||
			           self->replaceExistingTenantId.get() != existingEntry.get().id) {
				// The tenant creation has already started, so resume where we left off
				if (!self->assignClusterAutomatically &&
				    existingEntry.get().assignedCluster != self->tenantEntry.assignedCluster) {
					TraceEvent("MetaclusterCreateTenantClusterMismatch")
					    .detail("Preferred", self->tenantEntry.assignedCluster)
					    .detail("Actual", existingEntry.get().assignedCluster);
					throw invalid_tenant_configuration();
				}
				self->tenantEntry = existingEntry.get();
				wait(self->ctx.setCluster(tr, existingEntry.get().assignedCluster));
				return true;
			} else {
				// The previous creation is permanently failed, so cleanup the tenant and create it again from
				// scratch. We don't need to remove it from the tenant name index because we will overwrite the
				// existing entry later in this transaction.
				ManagementClusterMetadata::tenantMetadata().tenantMap.erase(tr, existingEntry.get().id);
				ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, -1, MutationRef::AddValue);
				ManagementClusterMetadata::clusterTenantCount.atomicOp(
				    tr, existingEntry.get().assignedCluster, -1, MutationRef::AddValue);

				ManagementClusterMetadata::clusterTenantIndex.erase(
				    tr,
				    Tuple::makeTuple(
				        existingEntry.get().assignedCluster, self->tenantEntry.tenantName, existingEntry.get().id));

				state DataClusterMetadata previousAssignedClusterMetadata =
				    wait(getClusterTransaction(tr, existingEntry.get().assignedCluster));

				wait(managementClusterRemoveTenantFromGroup(tr, existingEntry.get(), &previousAssignedClusterMetadata));
			}
		} else if (self->replaceExistingTenantId.present()) {
			throw tenant_removed();
		}

		return false;
	}

	// Returns a pair with the name of the assigned cluster and whether the group was already assigned
	ACTOR static Future<std::pair<ClusterName, bool>> assignTenant(CreateTenantImpl* self,
	                                                               Reference<typename DB::TransactionT> tr) {
		// If our tenant group is already assigned, then we just use that assignment
		state Optional<MetaclusterTenantGroupEntry> groupEntry;
		if (self->tenantEntry.tenantGroup.present()) {
			Optional<MetaclusterTenantGroupEntry> _groupEntry =
			    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(
			        tr, self->tenantEntry.tenantGroup.get()));
			groupEntry = _groupEntry;

			if (groupEntry.present()) {
				if (!self->assignClusterAutomatically &&
				    groupEntry.get().assignedCluster != self->tenantEntry.assignedCluster) {
					TraceEvent("MetaclusterCreateTenantGroupClusterMismatch")
					    .detail("TenantGroupCluster", groupEntry.get().assignedCluster)
					    .detail("SpecifiedCluster", self->tenantEntry.assignedCluster);
					throw invalid_tenant_configuration();
				}
				return std::make_pair(groupEntry.get().assignedCluster, true);
			}
		}

		state std::vector<Future<Reference<IDatabase>>> dataClusterDbs;
		state std::vector<ClusterName> dataClusterNames;
		state std::vector<Future<ClusterName>> clusterAvailabilityChecks;
		// Get a set of the most full clusters that still have capacity
		// If preferred cluster is specified, look for that one.
		if (!self->assignClusterAutomatically) {
			DataClusterMetadata dataClusterMetadata =
			    wait(getClusterTransaction(tr, self->tenantEntry.assignedCluster));
			if (!dataClusterMetadata.entry.hasCapacity()) {
				throw cluster_no_capacity();
			}
			dataClusterNames.push_back(self->tenantEntry.assignedCluster);
		} else {
			state KeyBackedSet<Tuple>::RangeResultType availableClusters =
			    wait(ManagementClusterMetadata::clusterCapacityIndex.getRange(
			        tr,
			        {},
			        {},
			        CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_CLUSTERS_TO_CHECK,
			        Snapshot::False,
			        Reverse::True));
			if (availableClusters.results.empty()) {
				throw metacluster_no_capacity();
			}
			for (auto const& clusterTuple : availableClusters.results) {
				dataClusterNames.push_back(clusterTuple.getString(1));
			}
		}
		for (auto const& dataClusterName : dataClusterNames) {
			dataClusterDbs.push_back(getAndOpenDatabase(tr, dataClusterName));
		}
		wait(waitForAll(dataClusterDbs));
		// Check the availability of our set of clusters
		for (int i = 0; i < dataClusterDbs.size(); ++i) {
			clusterAvailabilityChecks.push_back(checkClusterAvailability(dataClusterDbs[i].get(), dataClusterNames[i]));
		}

		// Wait for a successful availability check from some cluster. We prefer the most full cluster, but if it
		// doesn't return quickly we may choose another.
		Optional<Void> clusterAvailabilityCheck = wait(timeout(
		    success(clusterAvailabilityChecks[0]) || (delay(CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_FIRST_CHOICE_DELAY) &&
		                                              waitForAny(clusterAvailabilityChecks)),
		    CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_AVAILABILITY_TIMEOUT));

		if (!clusterAvailabilityCheck.present()) {
			// If no clusters were available for long enough, then we throw an error and try again
			throw transaction_too_old();
		}

		// Get the first cluster that was available
		state Optional<ClusterName> chosenCluster;
		for (auto const& f : clusterAvailabilityChecks) {
			if (f.isReady()) {
				chosenCluster = f.get();
				break;
			}
		}

		ASSERT(chosenCluster.present());
		return std::make_pair(chosenCluster.get(), false);
	}

	ACTOR static Future<Void> assignTenantAndStoreInManagementCluster(CreateTenantImpl* self,
	                                                                  Reference<typename DB::TransactionT> tr) {
		// If the tenant already exists, we either throw an error from this function or move on to the next phase
		bool tenantExists = wait(checkForExistingTenant(self, tr));
		if (tenantExists) {
			return Void();
		}

		// Choose a cluster for the tenant
		state std::pair<ClusterName, bool> assignment = wait(assignTenant(self, tr));
		self->tenantEntry.assignedCluster = assignment.first;

		// Update the context with the chosen cluster
		state Future<Void> setClusterFuture = self->ctx.setCluster(tr, assignment.first);

		// Create a tenant entry in the management cluster
		state Optional<int64_t> lastId = wait(ManagementClusterMetadata::tenantMetadata().lastTenantId.get(tr));
		// If the last tenant id is not present fetch the prefix from system keys and make it the prefix for the
		// next allocated tenant id
		if (!lastId.present()) {
			Optional<int64_t> tenantIdPrefix = wait(TenantMetadata::tenantIdPrefix().get(tr));
			ASSERT(tenantIdPrefix.present());
			lastId = tenantIdPrefix.get() << 48;
		}
		if (!TenantAPI::nextTenantIdPrefixMatches(lastId.get(), lastId.get() + 1)) {
			throw cluster_no_capacity();
		}
		self->tenantEntry.setId(lastId.get() + 1);
		ManagementClusterMetadata::tenantMetadata().lastTenantId.set(tr, self->tenantEntry.id);

		self->tenantEntry.tenantState = MetaclusterAPI::TenantState::REGISTERING;
		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->tenantEntry.id, self->tenantEntry);
		ManagementClusterMetadata::tenantMetadata().tenantNameIndex.set(
		    tr, self->tenantEntry.tenantName, self->tenantEntry.id);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, 1, MutationRef::AddValue);
		ManagementClusterMetadata::clusterTenantCount.atomicOp(
		    tr, self->tenantEntry.assignedCluster, 1, MutationRef::AddValue);

		int64_t clusterTenantCount = wait(ManagementClusterMetadata::clusterTenantCount.getD(
		    tr, self->tenantEntry.assignedCluster, Snapshot::False, 0));

		if (clusterTenantCount > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			throw cluster_no_capacity();
		}

		// Updated indexes to include the new tenant
		ManagementClusterMetadata::clusterTenantIndex.insert(
		    tr,
		    Tuple::makeTuple(self->tenantEntry.assignedCluster, self->tenantEntry.tenantName, self->tenantEntry.id));

		wait(setClusterFuture);

		// If we are part of a tenant group that is assigned to a cluster being removed from the metacluster,
		// then we fail with an error.
		if (self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING) {
			throw cluster_removed();
		} else if (self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::RESTORING) {
			throw cluster_restoring();
		}

		ASSERT(self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::READY);

		managementClusterAddTenantToGroup(
		    tr, self->tenantEntry, &self->ctx.dataClusterMetadata.get(), GroupAlreadyExists(assignment.second));

		return Void();
	}

	ACTOR static Future<Void> storeTenantInDataCluster(CreateTenantImpl* self, Reference<ITransaction> tr) {
		std::pair<Optional<TenantMapEntry>, bool> dataClusterTenant = wait(TenantAPI::createTenantTransaction(
		    tr, self->tenantEntry.toTenantMapEntry(), ClusterType::METACLUSTER_DATA));

		// If the tenant map entry is empty, then we encountered a tombstone indicating that the tenant was
		// simultaneously removed.
		if (!dataClusterTenant.first.present()) {
			throw tenant_removed();
		}

		return Void();
	}

	ACTOR static Future<Void> markTenantReady(CreateTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> managementEntry =
		    wait(tryGetTenantTransaction(tr, self->tenantEntry.id));
		if (!managementEntry.present()) {
			throw tenant_removed();
		}

		if (managementEntry.get().tenantState == MetaclusterAPI::TenantState::REGISTERING) {
			MetaclusterTenantMapEntry updatedEntry = managementEntry.get();
			updatedEntry.tenantState = MetaclusterAPI::TenantState::READY;
			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, updatedEntry.id, updatedEntry);
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	ACTOR static Future<Void> run(CreateTenantImpl* self) {
		if (self->tenantEntry.tenantName.startsWith("\xff"_sr)) {
			throw invalid_tenant_name();
		}

		loop {
			wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return assignTenantAndStoreInManagementCluster(self, tr);
			}));

			self->replaceExistingTenantId = {};
			try {
				wait(self->ctx.runDataClusterTransaction(
				    [self = self](Reference<ITransaction> tr) { return storeTenantInDataCluster(self, tr); }));

				wait(self->ctx.runManagementTransaction(
				    [self = self](Reference<typename DB::TransactionT> tr) { return markTenantReady(self, tr); }));

				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_creation_permanently_failed) {
					// If the data cluster has permanently failed to create the tenant, then we can reassign it in
					// the management cluster and start over
					self->replaceExistingTenantId = self->tenantEntry.id;
					self->ctx.clearCluster();
				} else {
					throw;
				}
			}
		}
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<Void> createTenant(Reference<DB> db,
                          MetaclusterTenantMapEntry tenantEntry,
                          AssignClusterAutomatically assignClusterAutomatically) {
	state CreateTenantImpl<DB> impl(db, tenantEntry, assignClusterAutomatically);
	wait(impl.run());
	return Void();
}

template <class DB>
struct DeleteTenantImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	// Either one can be specified, and the other will be looked up
	// and filled in by reading the metacluster metadata
	Optional<TenantName> tenantName;
	int64_t tenantId = -1;

	DeleteTenantImpl(Reference<DB> managementDb, TenantName tenantName) : ctx(managementDb), tenantName(tenantName) {}
	DeleteTenantImpl(Reference<DB> managementDb, int64_t tenantId) : ctx(managementDb), tenantId(tenantId) {}

	// Loads the cluster details for the cluster where the tenant is assigned.
	// Returns true if the deletion is already in progress
	ACTOR static Future<std::pair<int64_t, bool>> getAssignedLocation(DeleteTenantImpl* self,
	                                                                  Reference<typename DB::TransactionT> tr) {
		state int64_t resolvedId = self->tenantId;
		if (self->tenantId == -1) {
			ASSERT(self->tenantName.present());
			wait(store(resolvedId,
			           ManagementClusterMetadata::tenantMetadata().tenantNameIndex.getD(
			               tr, self->tenantName.get(), Snapshot::False, TenantInfo::INVALID_TENANT)));
		}

		state MetaclusterTenantMapEntry tenantEntry = wait(getTenantTransaction(tr, resolvedId));

		// Disallow removing the "new" name of a renamed tenant before it completes
		if (self->tenantName.present() && tenantEntry.tenantName != self->tenantName.get()) {
			ASSERT(tenantEntry.tenantState == MetaclusterAPI::TenantState::RENAMING ||
			       tenantEntry.tenantState == MetaclusterAPI::TenantState::REMOVING);
			throw tenant_not_found();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.assignedCluster));
		return std::make_pair(resolvedId, tenantEntry.tenantState == MetaclusterAPI::TenantState::REMOVING);
	}

	// Does an initial check if the tenant is empty. This is an optimization to prevent us marking a tenant
	// in the deleted state while it has data, but it is still possible that data gets added to it after this
	// point.
	//
	// SOMEDAY: should this also lock the tenant when locking is supported?
	ACTOR static Future<Void> checkTenantEmpty(DeleteTenantImpl* self, Reference<ITransaction> tr) {
		state Optional<TenantMapEntry> tenantEntry = wait(TenantAPI::tryGetTenantTransaction(tr, self->tenantId));
		if (!tenantEntry.present()) {
			// The tenant must have been removed simultaneously
			return Void();
		}

		ThreadFuture<RangeResult> rangeFuture = tr->getRange(prefixRange(tenantEntry.get().prefix), 1);
		RangeResult result = wait(safeThreadFutureToFuture(rangeFuture));
		if (!result.empty()) {
			throw tenant_not_empty();
		}

		return Void();
	}

	// Mark the tenant as being in a removing state on the management cluster
	ACTOR static Future<Void> markTenantInRemovingState(DeleteTenantImpl* self,
	                                                    Reference<typename DB::TransactionT> tr) {
		state MetaclusterTenantMapEntry tenantEntry = wait(getTenantTransaction(tr, self->tenantId));

		if (tenantEntry.tenantState != MetaclusterAPI::TenantState::REMOVING) {
			tenantEntry.tenantState = MetaclusterAPI::TenantState::REMOVING;

			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, tenantEntry.id, tenantEntry);
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	// Delete the tenant and related metadata on the management cluster
	ACTOR static Future<Void> deleteTenantFromManagementCluster(DeleteTenantImpl* self,
	                                                            Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantId));

		if (!tenantEntry.present()) {
			return Void();
		}

		ASSERT(tenantEntry.get().tenantState == MetaclusterAPI::TenantState::REMOVING);

		// Erase the tenant entry itself
		ManagementClusterMetadata::tenantMetadata().tenantMap.erase(tr, tenantEntry.get().id);
		ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(tr, tenantEntry.get().tenantName);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		// This is idempotent because this function is only called if the tenant is in the map
		ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, -1, MutationRef::AddValue);
		ManagementClusterMetadata::clusterTenantCount.atomicOp(
		    tr, tenantEntry.get().assignedCluster, -1, MutationRef::AddValue);

		// Remove the tenant from the cluster -> tenant index
		ManagementClusterMetadata::clusterTenantIndex.erase(
		    tr, Tuple::makeTuple(tenantEntry.get().assignedCluster, tenantEntry.get().tenantName, self->tenantId));

		if (tenantEntry.get().renameDestination.present()) {
			// If renaming, remove the metadata associated with the tenant destination
			ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(
			    tr, tenantEntry.get().renameDestination.get());

			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr,
			    Tuple::makeTuple(tenantEntry.get().assignedCluster,
			                     tenantEntry.get().renameDestination.get(),
			                     TenantInfo::INVALID_TENANT));
		}

		// Remove the tenant from its tenant group
		wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry.get(), &self->ctx.dataClusterMetadata.get()));

		return Void();
	}

	ACTOR static Future<Void> run(DeleteTenantImpl* self) {
		// Get information about the tenant and where it is assigned
		std::pair<int64_t, bool> result = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return getAssignedLocation(self, tr); }));

		if (self->tenantId == -1) {
			self->tenantId = result.first;
		} else {
			ASSERT(result.first == self->tenantId);
		}

		if (!result.second) {
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return checkTenantEmpty(self, tr); }));

			wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return markTenantInRemovingState(self, tr);
			}));
		}

		// Delete tenant on the data cluster
		wait(self->ctx.runDataClusterTransaction([self = self](Reference<ITransaction> tr) {
			return TenantAPI::deleteTenantTransaction(tr, self->tenantId, ClusterType::METACLUSTER_DATA);
		}));
		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return deleteTenantFromManagementCluster(self, tr);
		}));

		return Void();
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, TenantName name) {
	state DeleteTenantImpl<DB> impl(db, name);
	wait(impl.run());
	return Void();
}

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, int64_t id) {
	state DeleteTenantImpl<DB> impl(db, id);
	wait(impl.run());
	return Void();
}

template <class Transaction>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenantsTransaction(Transaction tr,
                                                                           TenantName begin,
                                                                           TenantName end,
                                                                           int limit,
                                                                           int offset = 0) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	auto future = ManagementClusterMetadata::tenantMetadata().tenantNameIndex.getRange(tr, begin, end, limit + offset);
	return fmap(
	    [offset](auto f) {
		    std::vector<std::pair<TenantName, int64_t>>& results = f.results;
		    results.erase(results.begin(), results.begin() + offset);
		    return results;
	    },
	    future);
}

template <class DB>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenants(Reference<DB> db,
                                                                TenantName begin,
                                                                TenantName end,
                                                                int limit,
                                                                int offset = 0) {
	return runTransaction(db, [=](Reference<typename DB::TransactionT> tr) {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		return listTenantsTransaction(tr, begin, end, limit, offset);
	});
}

// Scan the tenant index to get a list of tenant IDs, and then lookup the metadata for each ID individually
ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>>> listTenantMetadataTransaction(
    Transaction tr,
    std::vector<std::pair<TenantName, int64_t>> tenantIds) {

	state int idIdx = 0;
	state std::vector<Future<Optional<MetaclusterTenantMapEntry>>> futures;
	for (; idIdx < tenantIds.size(); ++idIdx) {
		futures.push_back(MetaclusterAPI::tryGetTenantTransaction(tr, tenantIds[idIdx].second));
	}
	wait(waitForAll(futures));

	std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> results;
	results.reserve(futures.size());
	for (int i = 0; i < futures.size(); ++i) {
		const MetaclusterTenantMapEntry& entry = futures[i].get().get();
		results.emplace_back(entry.tenantName, entry);
	}

	return results;
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>>> listTenantMetadataTransaction(Transaction tr,
                                                                                                    TenantNameRef begin,
                                                                                                    TenantNameRef end,
                                                                                                    int limit) {
	std::vector<std::pair<TenantName, int64_t>> matchingTenants = wait(listTenantsTransaction(tr, begin, end, limit));
	std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> results =
	    wait(listTenantMetadataTransaction(tr, matchingTenants));
	return results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>>> listTenantMetadata(
    Reference<DB> db,
    TenantName begin,
    TenantName end,
    int limit,
    int offset = 0,
    std::vector<MetaclusterAPI::TenantState> filters = std::vector<MetaclusterAPI::TenantState>()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> results;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			if (filters.empty()) {
				std::vector<std::pair<TenantName, int64_t>> ids =
				    wait(MetaclusterAPI::listTenantsTransaction(tr, begin, end, limit, offset));
				wait(store(results, MetaclusterAPI::listTenantMetadataTransaction(tr, ids)));
				return results;
			}

			// read in batch
			state int count = 0;
			loop {
				std::vector<std::pair<TenantName, MetaclusterTenantMapEntry>> tenantBatch =
				    wait(MetaclusterAPI::listTenantMetadataTransaction(tr, begin, end, std::max(limit + offset, 1000)));

				if (tenantBatch.empty()) {
					return results;
				}

				for (auto const& [name, entry] : tenantBatch) {
					if (std::count(filters.begin(), filters.end(), entry.tenantState)) {
						++count;
						if (count > offset) {
							results.emplace_back(name, entry);
							if (count - offset == limit) {
								ASSERT(count - offset == results.size());
								return results;
							}
						}
					}
				}

				begin = keyAfter(tenantBatch.back().first);
			}
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

template <class DB>
struct ConfigureTenantImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantName tenantName;
	std::map<Standalone<StringRef>, Optional<Value>> configurationParameters;
	IgnoreCapacityLimit ignoreCapacityLimit = IgnoreCapacityLimit::False;

	// Parameters set in updateManagementCluster
	MetaclusterTenantMapEntry updatedEntry;

	ConfigureTenantImpl(Reference<DB> managementDb,
	                    TenantName tenantName,
	                    std::map<Standalone<StringRef>, Optional<Value>> configurationParameters,
	                    IgnoreCapacityLimit ignoreCapacityLimit)
	  : ctx(managementDb), tenantName(tenantName), configurationParameters(configurationParameters),
	    ignoreCapacityLimit(ignoreCapacityLimit) {}

	// This verifies that the tenant group can be changed, and if so it updates all of the tenant group data
	// structures. It does not update the TenantMapEntry stored in the tenant map.
	ACTOR static Future<Void> updateTenantGroup(ConfigureTenantImpl* self,
	                                            Reference<typename DB::TransactionT> tr,
	                                            MetaclusterTenantMapEntry tenantEntry,
	                                            Optional<TenantGroupName> desiredGroup) {

		state MetaclusterTenantMapEntry entryWithUpdatedGroup = tenantEntry;
		entryWithUpdatedGroup.tenantGroup = desiredGroup;

		if (tenantEntry.tenantGroup == desiredGroup) {
			return Void();
		}

		// Removing a tenant group is only possible if we have capacity for more groups on the current cluster
		else if (!desiredGroup.present()) {
			if (!self->ctx.dataClusterMetadata.get().entry.hasCapacity() && !self->ignoreCapacityLimit) {
				throw cluster_no_capacity();
			}

			wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			managementClusterAddTenantToGroup(tr,
			                                  entryWithUpdatedGroup,
			                                  &self->ctx.dataClusterMetadata.get(),
			                                  GroupAlreadyExists::False,
			                                  self->ignoreCapacityLimit);
			return Void();
		}

		state Optional<MetaclusterTenantGroupEntry> tenantGroupEntry =
		    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(tr, desiredGroup.get()));

		// If we are creating a new tenant group, we need to have capacity on the current cluster
		if (!tenantGroupEntry.present()) {
			if (!self->ctx.dataClusterMetadata.get().entry.hasCapacity() && !self->ignoreCapacityLimit) {
				throw cluster_no_capacity();
			}
			wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			managementClusterAddTenantToGroup(tr,
			                                  entryWithUpdatedGroup,
			                                  &self->ctx.dataClusterMetadata.get(),
			                                  GroupAlreadyExists::False,
			                                  self->ignoreCapacityLimit);
			return Void();
		}

		// Moves between groups in the same cluster are freely allowed
		else if (tenantGroupEntry.get().assignedCluster == tenantEntry.assignedCluster) {
			wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			managementClusterAddTenantToGroup(tr,
			                                  entryWithUpdatedGroup,
			                                  &self->ctx.dataClusterMetadata.get(),
			                                  GroupAlreadyExists::True,
			                                  self->ignoreCapacityLimit);
			return Void();
		}

		// We don't currently support movement between groups on different clusters
		else {
			TraceEvent("TenantGroupChangeToDifferentCluster")
			    .detail("Tenant", self->tenantName)
			    .detail("OriginalGroup", tenantEntry.tenantGroup)
			    .detail("DesiredGroup", desiredGroup)
			    .detail("TenantAssignedCluster", tenantEntry.assignedCluster)
			    .detail("DesiredGroupAssignedCluster", tenantGroupEntry.get().assignedCluster);

			throw invalid_tenant_configuration();
		}
	}

	// Updates the configuration in the management cluster and marks it as being in the UPDATING_CONFIGURATION state
	ACTOR static Future<bool> updateManagementCluster(ConfigureTenantImpl* self,
	                                                  Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantName));

		if (!tenantEntry.present()) {
			throw tenant_not_found();
		}

		if (tenantEntry.get().tenantState != MetaclusterAPI::TenantState::READY &&
		    tenantEntry.get().tenantState != MetaclusterAPI::TenantState::UPDATING_CONFIGURATION) {
			throw invalid_tenant_state();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.get().assignedCluster));

		self->updatedEntry = tenantEntry.get();
		self->updatedEntry.tenantState = MetaclusterAPI::TenantState::UPDATING_CONFIGURATION;

		state std::map<Standalone<StringRef>, Optional<Value>>::iterator configItr;
		for (configItr = self->configurationParameters.begin(); configItr != self->configurationParameters.end();
		     ++configItr) {
			if (configItr->first == "tenant_group"_sr) {
				wait(updateTenantGroup(self, tr, self->updatedEntry, configItr->second));
			} else if (configItr->first == "assigned_cluster"_sr &&
			           configItr->second != tenantEntry.get().assignedCluster) {
				auto& newClusterName = configItr->second;
				TraceEvent(SevWarn, "CannotChangeAssignedCluster")
				    .detail("TenantName", tenantEntry.get().tenantName)
				    .detail("OriginalAssignedCluster", tenantEntry.get().assignedCluster)
				    .detail("NewAssignedCluster", newClusterName);
				throw invalid_tenant_configuration();
			}
			self->updatedEntry.configure(configItr->first, configItr->second);
		}

		if (self->updatedEntry.matchesConfiguration(tenantEntry.get()) &&
		    tenantEntry.get().tenantState == MetaclusterAPI::TenantState::READY) {
			return false;
		}

		++self->updatedEntry.configurationSequenceNum;
		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->updatedEntry.id, self->updatedEntry);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		return true;
	}

	// Updates the configuration in the data cluster
	ACTOR static Future<Void> updateDataCluster(ConfigureTenantImpl* self, Reference<ITransaction> tr) {
		state Optional<TenantMapEntry> tenantEntry =
		    wait(TenantAPI::tryGetTenantTransaction(tr, self->updatedEntry.id));

		if (!tenantEntry.present() ||
		    tenantEntry.get().configurationSequenceNum >= self->updatedEntry.configurationSequenceNum) {
			// If the tenant isn't in the metacluster, it must have been concurrently removed
			return Void();
		}

		wait(TenantAPI::configureTenantTransaction(tr, tenantEntry.get(), self->updatedEntry.toTenantMapEntry()));
		return Void();
	}

	// Updates the tenant state in the management cluster to READY
	ACTOR static Future<Void> markManagementTenantAsReady(ConfigureTenantImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state Optional<MetaclusterTenantMapEntry> tenantEntry =
		    wait(tryGetTenantTransaction(tr, self->updatedEntry.id));

		if (!tenantEntry.present() ||
		    tenantEntry.get().tenantState != MetaclusterAPI::TenantState::UPDATING_CONFIGURATION ||
		    tenantEntry.get().configurationSequenceNum > self->updatedEntry.configurationSequenceNum) {
			return Void();
		}

		tenantEntry.get().tenantState = MetaclusterAPI::TenantState::READY;
		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, tenantEntry.get().id, tenantEntry.get());
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		return Void();
	}

	ACTOR static Future<Void> run(ConfigureTenantImpl* self) {
		bool configUpdated = wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return updateManagementCluster(self, tr); }));

		if (configUpdated) {
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return updateDataCluster(self, tr); }));
			wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				return markManagementTenantAsReady(self, tr);
			}));
		}

		return Void();
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<Void> configureTenant(Reference<DB> db,
                             TenantName name,
                             std::map<Standalone<StringRef>, Optional<Value>> configurationParameters,
                             IgnoreCapacityLimit ignoreCapacityLimit) {
	state ConfigureTenantImpl<DB> impl(db, name, configurationParameters, ignoreCapacityLimit);
	wait(impl.run());
	return Void();
}

template <class DB>
struct RenameTenantImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	TenantName oldName;
	TenantName newName;

	// Parameters set in markTenantsInRenamingState
	int64_t tenantId = -1;
	int64_t configurationSequenceNum = -1;

	RenameTenantImpl(Reference<DB> managementDb, TenantName oldName, TenantName newName)
	  : ctx(managementDb), oldName(oldName), newName(newName) {}

	ACTOR static Future<Void> markTenantsInRenamingState(RenameTenantImpl* self,
	                                                     Reference<typename DB::TransactionT> tr) {
		state MetaclusterTenantMapEntry tenantEntry;
		state Optional<int64_t> newNameId;
		wait(store(tenantEntry, getTenantTransaction(tr, self->oldName)) &&
		     store(newNameId, ManagementClusterMetadata::tenantMetadata().tenantNameIndex.get(tr, self->newName)));

		if (self->tenantId != -1 && tenantEntry.id != self->tenantId) {
			// The tenant must have been removed simultaneously
			CODE_PROBE(true, "Metacluster rename old tenant ID mismatch");
			throw tenant_removed();
		}

		self->tenantId = tenantEntry.id;

		// If marked for deletion, abort the rename
		if (tenantEntry.tenantState == MetaclusterAPI::TenantState::REMOVING) {
			CODE_PROBE(true, "Metacluster rename candidates marked for deletion");
			throw tenant_removed();
		}

		if (newNameId.present() && (newNameId.get() != self->tenantId || self->oldName == self->newName)) {
			CODE_PROBE(true, "Metacluster rename new name already exists");
			throw tenant_already_exists();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.assignedCluster));

		if (tenantEntry.tenantState == MetaclusterAPI::TenantState::RENAMING) {
			if (tenantEntry.tenantName != self->oldName) {
				CODE_PROBE(true, "Renaming a tenant that is currently the destination of another rename");
				throw tenant_not_found();
			}
			if (tenantEntry.renameDestination.get() != self->newName) {
				CODE_PROBE(true, "Metacluster concurrent rename with different name");
				throw tenant_already_exists();
			} else {
				CODE_PROBE(true, "Metacluster rename retry in progress");
				self->configurationSequenceNum = tenantEntry.configurationSequenceNum;
				return Void();
			}
		}

		if (tenantEntry.tenantState != MetaclusterAPI::TenantState::READY) {
			CODE_PROBE(true, "Metacluster unable to proceed with rename operation");
			throw invalid_tenant_state();
		}

		self->configurationSequenceNum = tenantEntry.configurationSequenceNum + 1;
		// Check cluster capacity. If we would exceed the amount due to temporary extra tenants
		// then we deny the rename request altogether.
		int64_t clusterTenantCount = wait(
		    ManagementClusterMetadata::clusterTenantCount.getD(tr, tenantEntry.assignedCluster, Snapshot::False, 0));

		if (clusterTenantCount + 1 > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			throw cluster_no_capacity();
		}

		MetaclusterTenantMapEntry updatedEntry = tenantEntry;
		updatedEntry.tenantState = MetaclusterAPI::TenantState::RENAMING;
		updatedEntry.renameDestination = self->newName;
		updatedEntry.configurationSequenceNum = self->configurationSequenceNum;

		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->tenantId, updatedEntry);
		ManagementClusterMetadata::tenantMetadata().tenantNameIndex.set(tr, self->newName, self->tenantId);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		// Updated indexes to include the new tenant
		ManagementClusterMetadata::clusterTenantIndex.insert(
		    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->newName, TenantInfo::INVALID_TENANT));

		return Void();
	}

	ACTOR static Future<Void> updateDataCluster(RenameTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		ASSERT(self->tenantId != -1);
		ASSERT(self->configurationSequenceNum != -1);
		wait(TenantAPI::renameTenantTransaction(tr,
		                                        self->oldName,
		                                        self->newName,
		                                        self->tenantId,
		                                        ClusterType::METACLUSTER_DATA,
		                                        self->configurationSequenceNum));
		return Void();
	}

	ACTOR static Future<Void> finishRenameFromManagementCluster(RenameTenantImpl* self,
	                                                            Reference<typename DB::TransactionT> tr) {
		Optional<MetaclusterTenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantId));

		// Another (or several other) operations have already removed/changed the old entry
		// Possible for the new entry to also have been tampered with,
		// so it may or may not be present with or without the same id, which are all
		// legal states. Assume the rename completed properly in this case
		if (!tenantEntry.present() || tenantEntry.get().tenantName != self->oldName ||
		    tenantEntry.get().configurationSequenceNum > self->configurationSequenceNum) {
			CODE_PROBE(true,
			           "Metacluster finished rename with missing entries, mismatched id, and/or mismatched "
			           "configuration sequence.");
			return Void();
		}
		if (tenantEntry.get().tenantState == MetaclusterAPI::TenantState::REMOVING) {
			throw tenant_removed();
		}

		MetaclusterTenantMapEntry updatedEntry = tenantEntry.get();

		// Only update if in the expected state
		if (updatedEntry.tenantState == MetaclusterAPI::TenantState::RENAMING) {
			updatedEntry.tenantName = self->newName;
			updatedEntry.tenantState = MetaclusterAPI::TenantState::READY;
			updatedEntry.renameDestination.reset();
			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->tenantId, updatedEntry);
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

			ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(tr, self->oldName);

			// Remove the tenant from the cluster -> tenant index
			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->oldName, self->tenantId));
			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->newName, TenantInfo::INVALID_TENANT));
			ManagementClusterMetadata::clusterTenantIndex.insert(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster, self->newName, self->tenantId));
		}

		return Void();
	}

	ACTOR static Future<Void> run(RenameTenantImpl* self) {
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return markTenantsInRenamingState(self, tr); }));

		// Rename tenant on the data cluster
		try {
			wait(self->ctx.runDataClusterTransaction(
			    [self = self](Reference<ITransaction> tr) { return updateDataCluster(self, tr); }));
		} catch (Error& e) {
			// Since we track the tenant entries on the management cluster, these error codes should only appear
			// on a retry of the transaction, typically caused by commit_unknown_result.
			// Operating on the assumption that the first transaction completed successfully, we keep going
			// so we can finish the rename on the management cluster.
			if (e.code() == error_code_tenant_not_found || e.code() == error_code_tenant_already_exists) {
				CODE_PROBE(true, "Metacluster rename ran into commit_unknown_result");
			} else {
				throw e;
			}
		}

		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			return finishRenameFromManagementCluster(self, tr);
		}));
		return Void();
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<Void> renameTenant(Reference<DB> db, TenantName oldName, TenantName newName) {
	state RenameTenantImpl<DB> impl(db, oldName, newName);
	wait(impl.run());
	return Void();
}

template <class Transaction>
Future<Optional<MetaclusterTenantGroupEntry>> tryGetTenantGroupTransaction(Transaction tr, TenantGroupName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(tr, name);
}

ACTOR template <class DB>
Future<Optional<MetaclusterTenantGroupEntry>> tryGetTenantGroup(Reference<DB> db, TenantGroupName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<MetaclusterTenantGroupEntry> entry = wait(tryGetTenantGroupTransaction(tr, name));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>>>
listTenantGroupsTransaction(Transaction tr, TenantGroupName begin, TenantGroupName end, int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	KeyBackedRangeResult<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>> results =
	    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupMap.getRange(tr, begin, end, limit));

	return results.results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>>> listTenantGroups(Reference<DB> db,
                                                                                              TenantGroupName begin,
                                                                                              TenantGroupName end,
                                                                                              int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			std::vector<std::pair<TenantGroupName, MetaclusterTenantGroupEntry>> tenantGroups =
			    wait(listTenantGroupsTransaction(tr, begin, end, limit));
			return tenantGroups;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif