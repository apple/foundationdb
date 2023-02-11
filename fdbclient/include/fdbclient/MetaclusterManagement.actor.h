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
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Tenant.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/ThreadHelper.actor.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_G_H)
#define FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_G_H
#include "fdbclient/MetaclusterManagement.actor.g.h"
#elif !defined(FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_H)
#define FDBCLIENT_METACLUSTER_MANAGEMENT_ACTOR_H

#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/VersionedMap.h"
#include "flow/flat_buffers.h"
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

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connectionString, entry);
	}
};

FDB_DECLARE_BOOLEAN_PARAM(AddNewTenants);
FDB_DECLARE_BOOLEAN_PARAM(RemoveMissingTenants);
FDB_DECLARE_BOOLEAN_PARAM(AssignClusterAutomatically);

namespace MetaclusterAPI {

struct ManagementClusterMetadata {
	struct ConnectionStringCodec {
		static inline Standalone<StringRef> pack(ClusterConnectionString const& val) {
			return StringRef(val.toString());
		}
		static inline ClusterConnectionString unpack(Standalone<StringRef> const& val) {
			return ClusterConnectionString(val.toString());
		}
	};

	static TenantMetadataSpecification& tenantMetadata();

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

	MetaclusterOperationContext(Reference<DB> managementDb, Optional<ClusterName> clusterName = {})
	  : managementDb(managementDb), clusterName(clusterName) {}

	// Run a transaction on the management cluster. This verifies that the cluster is a management cluster and matches
	// the same metacluster that we've run any previous transactions on. If a clusterName is set, it also verifies that
	// the specified cluster is present. Stores the metaclusterRegistration entry and, if a clusterName is set, the
	// dataClusterMetadata and dataClusterDb in the context.
	ACTOR template <class Function>
	static Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runManagementTransaction(MetaclusterOperationContext* self, Function func) {
		state Reference<typename DB::TransactionT> tr = self->managementDb->createTransaction();
		state bool clusterPresentAtStart = self->clusterName.present();
		loop {
			try {
				// If this transaction is retrying and didn't have the cluster name set at the beginning, clear it out
				// to be set again in the next iteration.
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

				// Check that this is a management cluster and is the same metacluster that any previous transactions
				// have run on.
				if (!currentMetaclusterRegistration.present() ||
				    currentMetaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
					throw invalid_metacluster_operation();
				} else if (self->metaclusterRegistration.present() &&
				           !self->metaclusterRegistration.get().matches(currentMetaclusterRegistration.get())) {
					throw invalid_metacluster_operation();
				}

				// If a cluster was specified, check that the cluster metadata is present. If so, load it and store it
				// in the context. Additionally, store the data cluster details in the local metacluster registration
				// entry.
				if (self->clusterName.present()) {
					if (!currentDataClusterMetadata.present()) {
						throw cluster_not_found();
					} else {
						currentMetaclusterRegistration = currentMetaclusterRegistration.get().toDataClusterRegistration(
						    self->clusterName.get(), currentDataClusterMetadata.get().entry.id);
					}
				}

				// Store the metacluster registration entry
				if (!self->metaclusterRegistration.present()) {
					self->metaclusterRegistration = currentMetaclusterRegistration;
				}

				// Check that our data cluster has the same ID as previous transactions. If so, then store the updated
				// cluster metadata in the context and open a connection to the data DB.
				if (self->dataClusterMetadata.present() &&
				    self->dataClusterMetadata.get().entry.id != currentDataClusterMetadata.get().entry.id) {
					throw cluster_not_found();
				} else if (self->clusterName.present()) {
					self->dataClusterMetadata = currentDataClusterMetadata;
					if (!self->dataClusterDb) {
						wait(
						    store(self->dataClusterDb, openDatabase(self->dataClusterMetadata.get().connectionString)));
					}
				}

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
	// already been run on the management cluster to populate the needed metadata. This verifies that the data cluster
	// has the expected ID and is part of the metacluster that previous transactions have run on.
	ACTOR template <class Function>
	static Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runDataClusterTransaction(MetaclusterOperationContext* self, Function func) {
		ASSERT(self->dataClusterDb);
		ASSERT(self->dataClusterMetadata.present());
		ASSERT(self->metaclusterRegistration.present() &&
		       self->metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);

		state Reference<ITransaction> tr = self->dataClusterDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Optional<MetaclusterRegistrationEntry> currentMetaclusterRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				// Check that this is the expected data cluster and is part of the right metacluster
				if (!currentMetaclusterRegistration.present() ||
				    currentMetaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_DATA) {
					throw invalid_metacluster_operation();
				} else if (!self->metaclusterRegistration.get().matches(currentMetaclusterRegistration.get())) {
					throw invalid_metacluster_operation();
				}

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
	Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())>
	runDataClusterTransaction(Function func) {
		return runDataClusterTransaction(this, func);
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
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, int64_t tenantId) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return ManagementClusterMetadata::tenantMetadata().tenantMap.get(tr, tenantId);
}

ACTOR template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<int64_t> tenantId = wait(ManagementClusterMetadata::tenantMetadata().tenantNameIndex.get(tr, name));
	if (tenantId.present()) {
		Optional<TenantMapEntry> entry =
		    wait(ManagementClusterMetadata::tenantMetadata().tenantMap.get(tr, tenantId.get()));
		return entry;
	} else {
		return Optional<TenantMapEntry>();
	}
}

ACTOR template <class DB, class Tenant>
Future<Optional<TenantMapEntry>> tryGetTenant(Reference<DB> db, Tenant tenant) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction, class Tenant>
Future<TenantMapEntry> getTenantTransaction(Transaction tr, Tenant tenant) {
	Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class DB, class Tenant>
Future<TenantMapEntry> getTenant(Reference<DB> db, Tenant tenant) {
	Optional<TenantMapEntry> entry = wait(tryGetTenant(db, tenant));
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

ACTOR template <class DB>
Future<Optional<std::string>> createMetacluster(Reference<DB> db, ClusterName name, int64_t tenantIdPrefix) {
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

			Optional<MetaclusterRegistrationEntry> existingRegistration = wait(metaclusterRegistrationFuture);
			if (existingRegistration.present()) {
				if (metaclusterUid.present() && metaclusterUid.get() == existingRegistration.get().metaclusterId) {
					return Optional<std::string>();
				} else {
					return format("cluster is already registered as a %s named `%s'",
					              existingRegistration.get().clusterType == ClusterType::METACLUSTER_DATA
					                  ? "data cluster"
					                  : "metacluster",
					              printable(existingRegistration.get().name).c_str());
				}
			}

			wait(metaclusterEmptinessCheck);

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
                           Optional<DataClusterEntry> const& updatedEntry) {

	if (updatedEntry.present()) {
		if (previousMetadata.entry.clusterState == DataClusterState::REMOVING) {
			throw cluster_removed();
		}
		ManagementClusterMetadata::dataClusters().set(tr, name, updatedEntry.get());
		updateClusterCapacityIndex(tr, name, previousMetadata.entry, updatedEntry.get());
	}
	if (updatedConnectionString.present()) {
		ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, name, updatedConnectionString.get());
	}
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

	// Check that cluster name is available
	ACTOR static Future<Void> registrationPrecheck(RegisterClusterImpl* self, Reference<typename DB::TransactionT> tr) {
		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (dataClusterMetadata.present()) {
			throw cluster_already_exists();
		}

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

				// Check whether this cluster has already been registered
				state Optional<MetaclusterRegistrationEntry> existingRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));
				if (existingRegistration.present()) {
					if (existingRegistration.get().clusterType != ClusterType::METACLUSTER_DATA ||
					    existingRegistration.get().name != self->clusterName ||
					    !existingRegistration.get().matches(self->ctx.metaclusterRegistration.get())) {
						throw cluster_already_registered();
					} else {
						// We already successfully registered the cluster with these details, so there's nothing to do
						self->clusterEntry.id = existingRegistration.get().id;
						return Void();
					}
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

				self->clusterEntry.id = deterministicRandom()->randomUniqueID();
				MetaclusterMetadata::metaclusterRegistration().set(
				    tr,
				    self->ctx.metaclusterRegistration.get().toDataClusterRegistration(self->clusterName,
				                                                                      self->clusterEntry.id));

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
	ACTOR static Future<Void> registerInManagementCluster(RegisterClusterImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, self->clusterName));
		if (dataClusterMetadata.present() && !dataClusterMetadata.get().matchesConfiguration(
		                                         DataClusterMetadata(self->clusterEntry, self->connectionString))) {
			throw cluster_already_exists();
		} else if (!dataClusterMetadata.present()) {
			self->clusterEntry.allocated = ClusterUsage();

			if (self->clusterEntry.hasCapacity()) {
				ManagementClusterMetadata::clusterCapacityIndex.insert(
				    tr, Tuple::makeTuple(self->clusterEntry.allocated.numTenantGroups, self->clusterName));
			}
			ManagementClusterMetadata::dataClusters().set(tr, self->clusterName, self->clusterEntry);
			ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, self->clusterName, self->connectionString);
		}

		TraceEvent("RegisteredDataCluster")
		    .detail("ClusterName", self->clusterName)
		    .detail("ClusterID", self->clusterEntry.id)
		    .detail("Capacity", self->clusterEntry.capacity)
		    .detail("Version", tr->getCommittedVersion())
		    .detail("ConnectionString", self->connectionString.toString());

		return Void();
	}

	ACTOR static Future<Void> run(RegisterClusterImpl* self) {
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return registrationPrecheck(self, tr); }));
		// Don't use ctx to run this transaction because we have not set up the data cluster metadata on it and we don't
		// have a metacluster registration on the data cluster
		wait(configureDataCluster(self));
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return registerInManagementCluster(self, tr); }));
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

ACTOR template <class DB>
Future<Void> restoreCluster(Reference<DB> db,
                            ClusterName name,
                            std::string connectionString,
                            DataClusterEntry entry,
                            AddNewTenants addNewTenants,
                            RemoveMissingTenants removeMissingTenants) {
	// TODO: add implementation
	wait(delay(0.0));
	return Void();
}

template <class DB>
struct RemoveClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	bool forceRemove;

	// Parameters set in markClusterRemoving
	Optional<int64_t> lastTenantId;

	RemoveClusterImpl(Reference<DB> managementDb, ClusterName clusterName, bool forceRemove)
	  : ctx(managementDb, clusterName), forceRemove(forceRemove) {}

	// Returns false if the cluster is no longer present, or true if it is present and the removal should proceed.
	ACTOR static Future<bool> markClusterRemoving(RemoveClusterImpl* self, Reference<typename DB::TransactionT> tr) {
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

		TraceEvent("MarkedDataClusterRemoving")
		    .detail("Name", self->ctx.clusterName.get())
		    .detail("Version", tr->getCommittedVersion());

		return true;
	}

	// Delete metacluster metadata from the data cluster
	ACTOR static Future<Void> updateDataCluster(RemoveClusterImpl* self, Reference<ITransaction> tr) {
		// Delete metacluster related metadata
		MetaclusterMetadata::metaclusterRegistration().clear(tr);
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

		TraceEvent("ReconfiguredDataCluster")
		    .detail("Name", self->ctx.clusterName.get())
		    .detail("Version", tr->getCommittedVersion());

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
		for (Tuple entry : tenantEntries.results) {
			ASSERT(entry.getString(0) == self->ctx.clusterName.get());
			ManagementClusterMetadata::tenantMetadata().tenantMap.erase(tr, entry.getInt(2));
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

		ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(
		    tr, -tenantEntries.results.size(), MutationRef::AddValue);
		ManagementClusterMetadata::clusterTenantCount.atomicOp(
		    tr, self->ctx.clusterName.get(), -tenantEntries.results.size(), MutationRef::AddValue);

		return !tenantEntries.more;
	}

	// Returns true if all tenant groups and the data cluster have been purged
	ACTOR static Future<bool> purgeTenantGroupsAndDataCluster(RemoveClusterImpl* self,
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

		// Erase the data cluster record from the management cluster if processing our last batch
		if (!tenantGroupEntries.more) {
			ManagementClusterMetadata::dataClusters().erase(tr, self->ctx.clusterName.get());
			ManagementClusterMetadata::dataClusterConnectionRecords.erase(tr, self->ctx.clusterName.get());
			ManagementClusterMetadata::clusterTenantCount.erase(tr, self->ctx.clusterName.get());
		}

		return !tenantGroupEntries.more;
	}

	// Remove all metadata associated with the data cluster from the management cluster
	ACTOR static Future<Void> managementClusterPurgeDataCluster(RemoveClusterImpl* self) {
		state std::pair<Tuple, Tuple> clusterTupleRange = std::make_pair(
		    Tuple::makeTuple(self->ctx.clusterName.get()), Tuple::makeTuple(keyAfter(self->ctx.clusterName.get())));

		// First remove all tenants associated with the data cluster from the management cluster
		loop {
			bool clearedAll = wait(self->ctx.runManagementTransaction(
			    [self = self, clusterTupleRange = clusterTupleRange](Reference<typename DB::TransactionT> tr) {
				    return purgeTenants(self, tr, clusterTupleRange);
			    }));

			if (clearedAll) {
				break;
			}
		}

		// Next remove all tenant groups associated with the data cluster from the management cluster
		loop {
			bool clearedAll = wait(self->ctx.runManagementTransaction(
			    [self = self, clusterTupleRange = clusterTupleRange](Reference<typename DB::TransactionT> tr) {
				    return purgeTenantGroupsAndDataCluster(self, tr, clusterTupleRange);
			    }));
			if (clearedAll) {
				break;
			}
		}

		TraceEvent("RemovedDataCluster").detail("Name", self->ctx.clusterName.get());
		return Void();
	}

	ACTOR static Future<Void> run(RemoveClusterImpl* self) {
		state bool clusterIsPresent;
		try {
			wait(store(clusterIsPresent,
			           self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
				           return markClusterRemoving(self, tr);
			           })));
		} catch (Error& e) {
			// If the transaction retries after success or if we are trying a second time to remove the cluster, it will
			// throw an error indicating that the removal has already started
			if (e.code() == error_code_cluster_removed) {
				clusterIsPresent = true;
			} else {
				throw;
			}
		}

		if (clusterIsPresent) {
			try {
				wait(self->ctx.runDataClusterTransaction(
				    [self = self](Reference<ITransaction> tr) { return updateDataCluster(self, tr); }));
			} catch (Error& e) {
				// If this transaction gets retried, the metacluster information may have already been erased.
				if (e.code() != error_code_invalid_metacluster_operation) {
					throw;
				}
			}

			// This runs multiple transactions, so the run transaction calls are inside the function
			try {
				wait(managementClusterPurgeDataCluster(self));
			} catch (Error& e) {
				// If this transaction gets retried, the cluster may have already been deleted.
				if (e.code() != error_code_cluster_not_found) {
					throw;
				}
			}
		}

		return Void();
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<Void> removeCluster(Reference<DB> db, ClusterName name, bool forceRemove) {
	state RemoveClusterImpl<DB> impl(db, name, forceRemove);
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
                                       TenantMapEntry tenantEntry,
                                       DataClusterMetadata* clusterMetadata,
                                       bool groupAlreadyExists) {
	if (tenantEntry.tenantGroup.present()) {
		if (tenantEntry.tenantGroup.get().startsWith("\xff"_sr)) {
			throw invalid_tenant_group_name();
		}

		if (!groupAlreadyExists) {
			ManagementClusterMetadata::tenantMetadata().tenantGroupMap.set(
			    tr, tenantEntry.tenantGroup.get(), TenantGroupEntry(tenantEntry.assignedCluster));
			ManagementClusterMetadata::clusterTenantGroupIndex.insert(
			    tr, Tuple::makeTuple(tenantEntry.assignedCluster.get(), tenantEntry.tenantGroup.get()));
		}
		ManagementClusterMetadata::tenantMetadata().tenantGroupTenantIndex.insert(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), tenantEntry.id));
	}

	if (!groupAlreadyExists) {
		ASSERT(clusterMetadata->entry.hasCapacity());

		DataClusterEntry updatedEntry = clusterMetadata->entry;
		++updatedEntry.allocated.numTenantGroups;

		updateClusterMetadata(
		    tr, tenantEntry.assignedCluster.get(), *clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		clusterMetadata->entry = updatedEntry;
	}
}

ACTOR template <class Transaction>
Future<Void> managementClusterRemoveTenantFromGroup(Transaction tr,
                                                    TenantMapEntry tenantEntry,
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
			    tr, Tuple::makeTuple(tenantEntry.assignedCluster.get(), tenantEntry.tenantGroup.get()));

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
		    tr, tenantEntry.assignedCluster.get(), *clusterMetadata, Optional<ClusterConnectionString>(), updatedEntry);

		clusterMetadata->entry = updatedEntry;
	}

	return Void();
}

template <class DB>
struct CreateTenantImpl {
	MetaclusterOperationContext<DB> ctx;
	AssignClusterAutomatically assignClusterAutomatically;

	// Initialization parameters
	TenantMapEntry tenantEntry;

	// Parameter set if tenant creation permanently fails on the data cluster
	Optional<int64_t> replaceExistingTenantId;

	CreateTenantImpl(Reference<DB> managementDb,
	                 TenantMapEntry tenantEntry,
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
		state Optional<TenantMapEntry> existingEntry = wait(tryGetTenantTransaction(tr, self->tenantEntry.tenantName));
		if (existingEntry.present()) {
			if (!existingEntry.get().matchesConfiguration(self->tenantEntry) ||
			    existingEntry.get().tenantState != TenantState::REGISTERING) {
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
				wait(self->ctx.setCluster(tr, existingEntry.get().assignedCluster.get()));
				return true;
			} else {
				// The previous creation is permanently failed, so cleanup the tenant and create it again from scratch
				// We don't need to remove it from the tenantNameIndex because we will overwrite the existing entry
				// later in this transaction.
				ManagementClusterMetadata::tenantMetadata().tenantMap.erase(tr, existingEntry.get().id);
				ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, -1, MutationRef::AddValue);
				ManagementClusterMetadata::clusterTenantCount.atomicOp(
				    tr, existingEntry.get().assignedCluster.get(), -1, MutationRef::AddValue);

				ManagementClusterMetadata::clusterTenantIndex.erase(
				    tr,
				    Tuple::makeTuple(existingEntry.get().assignedCluster.get(),
				                     self->tenantEntry.tenantName,
				                     existingEntry.get().id));

				state DataClusterMetadata previousAssignedClusterMetadata =
				    wait(getClusterTransaction(tr, existingEntry.get().assignedCluster.get()));

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
		state Optional<TenantGroupEntry> groupEntry;
		if (self->tenantEntry.tenantGroup.present()) {
			Optional<TenantGroupEntry> _groupEntry =
			    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(
			        tr, self->tenantEntry.tenantGroup.get()));
			groupEntry = _groupEntry;

			if (groupEntry.present()) {
				ASSERT(groupEntry.get().assignedCluster.present());
				if (!self->assignClusterAutomatically &&
				    groupEntry.get().assignedCluster.get() != self->tenantEntry.assignedCluster.get()) {
					TraceEvent("MetaclusterCreateTenantGroupClusterMismatch")
					    .detail("TenantGroupCluster", groupEntry.get().assignedCluster.get())
					    .detail("SpecifiedCluster", self->tenantEntry.assignedCluster.get());
					throw invalid_tenant_configuration();
				}
				return std::make_pair(groupEntry.get().assignedCluster.get(), true);
			}
		}

		state std::vector<Future<Reference<IDatabase>>> dataClusterDbs;
		state std::vector<ClusterName> dataClusterNames;
		state std::vector<Future<ClusterName>> clusterAvailabilityChecks;
		// Get a set of the most full clusters that still have capacity
		// If preferred cluster is specified, look for that one.
		if (!self->assignClusterAutomatically) {
			DataClusterMetadata dataClusterMetadata =
			    wait(getClusterTransaction(tr, self->tenantEntry.assignedCluster.get()));
			if (!dataClusterMetadata.entry.hasCapacity()) {
				throw cluster_no_capacity();
			}
			dataClusterNames.push_back(self->tenantEntry.assignedCluster.get());
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
			for (auto clusterTuple : availableClusters.results) {
				dataClusterNames.push_back(clusterTuple.getString(1));
			}
		}
		for (auto dataClusterName : dataClusterNames) {
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
		for (auto f : clusterAvailabilityChecks) {
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
		// If the last tenant id is not present fetch the prefix from system keys and make it the prefix for the next
		// allocated tenant id
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

		self->tenantEntry.tenantState = TenantState::REGISTERING;
		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->tenantEntry.id, self->tenantEntry);
		ManagementClusterMetadata::tenantMetadata().tenantNameIndex.set(
		    tr, self->tenantEntry.tenantName, self->tenantEntry.id);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, 1, MutationRef::AddValue);
		ManagementClusterMetadata::clusterTenantCount.atomicOp(
		    tr, self->tenantEntry.assignedCluster.get(), 1, MutationRef::AddValue);

		int64_t clusterTenantCount = wait(ManagementClusterMetadata::clusterTenantCount.getD(
		    tr, self->tenantEntry.assignedCluster.get(), Snapshot::False, 0));

		if (clusterTenantCount > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			throw cluster_no_capacity();
		}

		// Updated indexes to include the new tenant
		ManagementClusterMetadata::clusterTenantIndex.insert(tr,
		                                                     Tuple::makeTuple(self->tenantEntry.assignedCluster.get(),
		                                                                      self->tenantEntry.tenantName,
		                                                                      self->tenantEntry.id));

		wait(setClusterFuture);

		// If we are part of a tenant group that is assigned to a cluster being removed from the metacluster,
		// then we fail with an error.
		if (self->ctx.dataClusterMetadata.get().entry.clusterState == DataClusterState::REMOVING) {
			throw cluster_removed();
		}

		managementClusterAddTenantToGroup(
		    tr, self->tenantEntry, &self->ctx.dataClusterMetadata.get(), assignment.second);

		return Void();
	}

	ACTOR static Future<Void> storeTenantInDataCluster(CreateTenantImpl* self, Reference<ITransaction> tr) {
		std::pair<Optional<TenantMapEntry>, bool> dataClusterTenant =
		    wait(TenantAPI::createTenantTransaction(tr, self->tenantEntry, ClusterType::METACLUSTER_DATA));

		// If the tenant map entry is empty, then we encountered a tombstone indicating that the tenant was
		// simultaneously removed.
		if (!dataClusterTenant.first.present()) {
			throw tenant_removed();
		}

		return Void();
	}

	ACTOR static Future<Void> markTenantReady(CreateTenantImpl* self, Reference<typename DB::TransactionT> tr) {
		state Optional<TenantMapEntry> managementEntry = wait(tryGetTenantTransaction(tr, self->tenantEntry.id));
		if (!managementEntry.present()) {
			throw tenant_removed();
		}

		if (managementEntry.get().tenantState == TenantState::REGISTERING) {
			TenantMapEntry updatedEntry = managementEntry.get();
			updatedEntry.tenantState = TenantState::READY;
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
                          TenantMapEntry tenantEntry,
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

		state TenantMapEntry tenantEntry = wait(getTenantTransaction(tr, resolvedId));

		// Disallow removing the "new" name of a renamed tenant before it completes
		if (self->tenantName.present() && tenantEntry.tenantName != self->tenantName.get()) {
			ASSERT(tenantEntry.tenantState == TenantState::RENAMING ||
			       tenantEntry.tenantState == TenantState::REMOVING);
			throw tenant_not_found();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.assignedCluster.get()));
		return std::make_pair(resolvedId, tenantEntry.tenantState == TenantState::REMOVING);
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
		state TenantMapEntry tenantEntry = wait(getTenantTransaction(tr, self->tenantId));

		if (tenantEntry.tenantState != TenantState::REMOVING) {
			tenantEntry.tenantState = TenantState::REMOVING;

			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, tenantEntry.id, tenantEntry);
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		}

		return Void();
	}

	// Delete the tenant and related metadata on the management cluster
	ACTOR static Future<Void> deleteTenantFromManagementCluster(DeleteTenantImpl* self,
	                                                            Reference<typename DB::TransactionT> tr) {
		state Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantId));

		if (!tenantEntry.present()) {
			return Void();
		}

		ASSERT(tenantEntry.get().tenantState == TenantState::REMOVING);

		// Erase the tenant entry itself
		ManagementClusterMetadata::tenantMetadata().tenantMap.erase(tr, tenantEntry.get().id);
		ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(tr, tenantEntry.get().tenantName);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		// This is idempotent because this function is only called if the tenant is in the map
		ManagementClusterMetadata::tenantMetadata().tenantCount.atomicOp(tr, -1, MutationRef::AddValue);
		ManagementClusterMetadata::clusterTenantCount.atomicOp(
		    tr, tenantEntry.get().assignedCluster.get(), -1, MutationRef::AddValue);

		// Remove the tenant from the cluster -> tenant index
		ManagementClusterMetadata::clusterTenantIndex.erase(
		    tr,
		    Tuple::makeTuple(tenantEntry.get().assignedCluster.get(), tenantEntry.get().tenantName, self->tenantId));

		if (tenantEntry.get().renameDestination.present()) {
			// If renaming, remove the metadata associated with the tenant destination
			ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(
			    tr, tenantEntry.get().renameDestination.get());

			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr,
			    Tuple::makeTuple(tenantEntry.get().assignedCluster.get(),
			                     tenantEntry.get().renameDestination.get(),
			                     self->tenantId));
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
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantMetadataTransaction(
    Transaction tr,
    std::vector<std::pair<TenantName, int64_t>> tenantIds) {

	state int idIdx = 0;
	state std::vector<Future<Optional<TenantMapEntry>>> futures;
	for (; idIdx < tenantIds.size(); ++idIdx) {
		futures.push_back(MetaclusterAPI::tryGetTenantTransaction(tr, tenantIds[idIdx].second));
	}
	wait(waitForAll(futures));

	std::vector<std::pair<TenantName, TenantMapEntry>> results;
	results.reserve(futures.size());
	for (int i = 0; i < futures.size(); ++i) {
		const TenantMapEntry& entry = futures[i].get().get();
		results.emplace_back(tenantIds[i].first, entry);
	}

	return results;
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantMetadataTransaction(Transaction tr,
                                                                                         TenantNameRef begin,
                                                                                         TenantNameRef end,
                                                                                         int limit) {
	std::vector<std::pair<TenantName, int64_t>> matchingTenants = wait(listTenantsTransaction(tr, begin, end, limit));
	std::vector<std::pair<TenantName, TenantMapEntry>> results =
	    wait(listTenantMetadataTransaction(tr, matchingTenants));
	return results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantMetadata(
    Reference<DB> db,
    TenantName begin,
    TenantName end,
    int limit,
    int offset = 0,
    std::vector<TenantState> filters = std::vector<TenantState>()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state std::vector<std::pair<TenantName, TenantMapEntry>> results;

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
				std::vector<std::pair<TenantName, TenantMapEntry>> tenantBatch =
				    wait(MetaclusterAPI::listTenantMetadataTransaction(tr, begin, end, std::max(limit + offset, 1000)));

				if (tenantBatch.empty()) {
					return results;
				}

				for (auto const& [name, entry] : tenantBatch) {
					if (std::count(filters.begin(), filters.end(), entry.tenantState)) {
						++count;
						if (count > offset) {
							results.push_back(std::make_pair(name, entry));
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

	// Parameters set in updateManagementCluster
	TenantMapEntry updatedEntry;

	ConfigureTenantImpl(Reference<DB> managementDb,
	                    TenantName tenantName,
	                    std::map<Standalone<StringRef>, Optional<Value>> configurationParameters)
	  : ctx(managementDb), tenantName(tenantName), configurationParameters(configurationParameters) {}

	// This verifies that the tenant group can be changed, and if so it updates all of the tenant group data
	// structures. It does not update the TenantMapEntry stored in the tenant map.
	ACTOR static Future<Void> updateTenantGroup(ConfigureTenantImpl* self,
	                                            Reference<typename DB::TransactionT> tr,
	                                            TenantMapEntry tenantEntry,
	                                            Optional<TenantGroupName> desiredGroup) {

		state TenantMapEntry entryWithUpdatedGroup = tenantEntry;
		entryWithUpdatedGroup.tenantGroup = desiredGroup;

		if (tenantEntry.tenantGroup == desiredGroup) {
			return Void();
		}

		// Removing a tenant group is only possible if we have capacity for more groups on the current cluster
		else if (!desiredGroup.present()) {
			if (!self->ctx.dataClusterMetadata.get().entry.hasCapacity()) {
				throw cluster_no_capacity();
			}

			wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			managementClusterAddTenantToGroup(tr, entryWithUpdatedGroup, &self->ctx.dataClusterMetadata.get(), false);
			return Void();
		}

		state Optional<TenantGroupEntry> tenantGroupEntry =
		    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(tr, desiredGroup.get()));

		// If we are creating a new tenant group, we need to have capacity on the current cluster
		if (!tenantGroupEntry.present()) {
			if (!self->ctx.dataClusterMetadata.get().entry.hasCapacity()) {
				throw cluster_no_capacity();
			}
			wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			managementClusterAddTenantToGroup(tr, entryWithUpdatedGroup, &self->ctx.dataClusterMetadata.get(), false);
			return Void();
		}

		// Moves between groups in the same cluster are freely allowed
		else if (tenantGroupEntry.get().assignedCluster == tenantEntry.assignedCluster) {
			wait(managementClusterRemoveTenantFromGroup(tr, tenantEntry, &self->ctx.dataClusterMetadata.get()));
			managementClusterAddTenantToGroup(tr, entryWithUpdatedGroup, &self->ctx.dataClusterMetadata.get(), true);
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
	ACTOR static Future<Void> updateManagementCluster(ConfigureTenantImpl* self,
	                                                  Reference<typename DB::TransactionT> tr) {
		state Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantName));

		if (!tenantEntry.present()) {
			throw tenant_not_found();
		}

		if (tenantEntry.get().tenantState != TenantState::READY &&
		    tenantEntry.get().tenantState != TenantState::UPDATING_CONFIGURATION) {
			throw invalid_tenant_state();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.get().assignedCluster.get()));

		self->updatedEntry = tenantEntry.get();
		self->updatedEntry.tenantState = TenantState::UPDATING_CONFIGURATION;

		state std::map<Standalone<StringRef>, Optional<Value>>::iterator configItr;
		for (configItr = self->configurationParameters.begin(); configItr != self->configurationParameters.end();
		     ++configItr) {
			if (configItr->first == "tenant_group"_sr) {
				wait(updateTenantGroup(self, tr, self->updatedEntry, configItr->second));
			}
			self->updatedEntry.configure(configItr->first, configItr->second);
		}

		++self->updatedEntry.configurationSequenceNum;
		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->updatedEntry.id, self->updatedEntry);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		return Void();
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

		TenantMapEntry dataClusterEntry = self->updatedEntry;
		dataClusterEntry.tenantState = TenantState::READY;
		dataClusterEntry.assignedCluster = {};

		wait(TenantAPI::configureTenantTransaction(tr, tenantEntry.get(), dataClusterEntry));
		return Void();
	}

	// Updates the tenant state in the management cluster to READY
	ACTOR static Future<Void> markManagementTenantAsReady(ConfigureTenantImpl* self,
	                                                      Reference<typename DB::TransactionT> tr) {
		state Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->updatedEntry.id));

		if (!tenantEntry.present() || tenantEntry.get().tenantState != TenantState::UPDATING_CONFIGURATION ||
		    tenantEntry.get().configurationSequenceNum > self->updatedEntry.configurationSequenceNum) {
			return Void();
		}

		tenantEntry.get().tenantState = TenantState::READY;
		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, tenantEntry.get().id, tenantEntry.get());
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);
		return Void();
	}

	ACTOR static Future<Void> run(ConfigureTenantImpl* self) {
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return updateManagementCluster(self, tr); }));
		wait(self->ctx.runDataClusterTransaction(
		    [self = self](Reference<ITransaction> tr) { return updateDataCluster(self, tr); }));
		wait(self->ctx.runManagementTransaction(
		    [self = self](Reference<typename DB::TransactionT> tr) { return markManagementTenantAsReady(self, tr); }));

		return Void();
	}
	Future<Void> run() { return run(this); }
};

ACTOR template <class DB>
Future<Void> configureTenant(Reference<DB> db,
                             TenantName name,
                             std::map<Standalone<StringRef>, Optional<Value>> configurationParameters) {
	state ConfigureTenantImpl<DB> impl(db, name, configurationParameters);
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
		state TenantMapEntry tenantEntry;
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
		if (tenantEntry.tenantState == TenantState::REMOVING) {
			CODE_PROBE(true, "Metacluster rename candidates marked for deletion");
			throw tenant_removed();
		}

		if (newNameId.present() && (newNameId.get() != self->tenantId || self->oldName == self->newName)) {
			CODE_PROBE(true, "Metacluster rename new name already exists");
			throw tenant_already_exists();
		}

		wait(self->ctx.setCluster(tr, tenantEntry.assignedCluster.get()));

		if (tenantEntry.tenantState == TenantState::RENAMING) {
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

		if (tenantEntry.tenantState != TenantState::READY) {
			CODE_PROBE(true, "Metacluster unable to proceed with rename operation");
			throw invalid_tenant_state();
		}

		self->configurationSequenceNum = tenantEntry.configurationSequenceNum + 1;
		// Check cluster capacity. If we would exceed the amount due to temporary extra tenants
		// then we deny the rename request altogether.
		int64_t clusterTenantCount = wait(ManagementClusterMetadata::clusterTenantCount.getD(
		    tr, tenantEntry.assignedCluster.get(), Snapshot::False, 0));

		if (clusterTenantCount + 1 > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			throw cluster_no_capacity();
		}

		TenantMapEntry updatedEntry = tenantEntry;
		updatedEntry.tenantState = TenantState::RENAMING;
		updatedEntry.renameDestination = self->newName;
		updatedEntry.configurationSequenceNum = self->configurationSequenceNum;

		ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->tenantId, updatedEntry);
		ManagementClusterMetadata::tenantMetadata().tenantNameIndex.set(tr, self->newName, self->tenantId);
		ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

		// Updated indexes to include the new tenant
		ManagementClusterMetadata::clusterTenantIndex.insert(
		    tr, Tuple::makeTuple(updatedEntry.assignedCluster.get(), self->newName, self->tenantId));

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
		Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, self->tenantId));

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
		if (tenantEntry.get().tenantState == TenantState::REMOVING) {
			throw tenant_removed();
		}

		TenantMapEntry updatedEntry = tenantEntry.get();

		// Only update if in the expected state
		if (updatedEntry.tenantState == TenantState::RENAMING) {
			updatedEntry.tenantName = self->newName;
			updatedEntry.tenantState = TenantState::READY;
			updatedEntry.renameDestination.reset();
			ManagementClusterMetadata::tenantMetadata().tenantMap.set(tr, self->tenantId, updatedEntry);
			ManagementClusterMetadata::tenantMetadata().lastTenantModification.setVersionstamp(tr, Versionstamp(), 0);

			ManagementClusterMetadata::tenantMetadata().tenantNameIndex.erase(tr, self->oldName);

			// Remove the tenant from the cluster -> tenant index
			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr, Tuple::makeTuple(updatedEntry.assignedCluster.get(), self->oldName, self->tenantId));
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
Future<Optional<TenantGroupEntry>> tryGetTenantGroupTransaction(Transaction tr, TenantGroupName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return ManagementClusterMetadata::tenantMetadata().tenantGroupMap.get(tr, name);
}

ACTOR template <class DB>
Future<Optional<TenantGroupEntry>> tryGetTenantGroup(Reference<DB> db, TenantGroupName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<TenantGroupEntry> entry = wait(tryGetTenantGroupTransaction(tr, name));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantGroupName, TenantGroupEntry>>> listTenantGroupsTransaction(Transaction tr,
                                                                                              TenantGroupName begin,
                                                                                              TenantGroupName end,
                                                                                              int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> results =
	    wait(ManagementClusterMetadata::tenantMetadata().tenantGroupMap.getRange(tr, begin, end, limit));

	return results.results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantGroupName, TenantGroupEntry>>> listTenantGroups(Reference<DB> db,
                                                                                   TenantGroupName begin,
                                                                                   TenantGroupName end,
                                                                                   int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			std::vector<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroups =
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