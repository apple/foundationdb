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

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withMetacluster())); }
	static DataClusterMetadata decode(ValueRef const& value) {
		DataClusterMetadata metadata;
		ObjectReader reader(value.begin(), IncludeVersion());
		reader.deserialize(metadata);
		return metadata;
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

	static inline TenantMetadataSpecification tenantMetadata = TenantMetadataSpecification(""_sr);

	// A map from cluster name to the metadata associated with a cluster
	static KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())> dataClusters;

	// A map from cluster name to the connection string for the cluster
	static KeyBackedMap<ClusterName, ClusterConnectionString, TupleCodec<ClusterName>, ConnectionStringCodec>
	    dataClusterConnectionRecords;

	// A set of non-full clusters where the key is the tuple (num tenant groups allocated, cluster name).
	static KeyBackedSet<Tuple> clusterCapacityIndex;

	// A set of cluster/tenant pairings ordered by cluster
	static KeyBackedSet<Tuple> clusterTenantIndex;

	// A set of cluster/tenant group pairings ordered by cluster
	static KeyBackedSet<Tuple> clusterTenantGroupIndex;

	static KeyBackedObjectMap<TenantGroupName, TenantGroupEntry, decltype(IncludeVersion())> tenantGroupMap;
};

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString);

template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return ManagementClusterMetadata::tenantMetadata.tenantMap.get(tr, name);
}

ACTOR template <class DB>
Future<Optional<TenantMapEntry>> tryGetTenant(Reference<DB> db, TenantName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<TenantMapEntry> getTenantTransaction(Transaction tr, TenantName name) {
	Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class DB>
Future<TenantMapEntry> getTenant(Reference<DB> db, TenantName name) {
	Optional<TenantMapEntry> entry = wait(tryGetTenant(db, name));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> tryGetClusterTransaction(Transaction tr, ClusterName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<ClusterType> clusterTypeFuture = TenantAPI::getClusterType(tr);
	state Future<Optional<DataClusterEntry>> clusterEntryFuture = ManagementClusterMetadata::dataClusters.get(tr, name);
	state Future<Optional<ClusterConnectionString>> connectionRecordFuture =
	    ManagementClusterMetadata::dataClusterConnectionRecords.get(tr, name);

	ClusterType clusterType = wait(clusterTypeFuture);
	if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
		throw invalid_metacluster_operation();
	}

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
Future<Void> managementClusterCheckEmpty(Transaction tr) {
	state Future<KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>>> tenantsFuture =
	    ManagementClusterMetadata::tenantMetadata.tenantMap.getRange(tr, {}, {}, 1);
	state Future<KeyBackedRangeResult<std::pair<ClusterName, DataClusterEntry>>> dataClustersFuture =
	    ManagementClusterMetadata::dataClusters.getRange(tr, {}, {}, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type dbContentsFuture =
	    tr->getRange(normalKeys, 1);

	KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenants = wait(tenantsFuture);
	if (!tenants.results.empty()) {
		throw cluster_not_empty();
	}
	KeyBackedRangeResult<std::pair<ClusterName, DataClusterEntry>> dataClusters = wait(dataClustersFuture);
	if (!dataClusters.results.empty()) {
		throw cluster_not_empty();
	}

	RangeResult dbContents = wait(safeThreadFutureToFuture(dbContentsFuture));
	if (!dbContents.empty()) {
		throw cluster_not_empty();
	}

	return Void();
}

ACTOR template <class DB>
Future<Optional<std::string>> createMetacluster(Reference<DB> db, ClusterName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Optional<UID> metaclusterUid;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state Future<Optional<MetaclusterRegistrationEntry>> metaclusterRegistrationFuture =
			    MetaclusterMetadata::metaclusterRegistration.get(tr);

			wait(managementClusterCheckEmpty(tr));

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

			if (!metaclusterUid.present()) {
				metaclusterUid = deterministicRandom()->randomUniqueID();
			}

			MetaclusterMetadata::metaclusterRegistration.set(tr,
			                                                 MetaclusterRegistrationEntry(name, metaclusterUid.get()));

			wait(safeThreadFutureToFuture(tr->commit()));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	return Optional<std::string>();
}

ACTOR template <class DB>
Future<Void> decommissionMetacluster(Reference<DB> db) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state bool firstTry = true;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state Future<ClusterType> clusterTypeFuture = TenantAPI::getClusterType(tr);
			wait(managementClusterCheckEmpty(tr));

			ClusterType clusterType = wait(clusterTypeFuture);
			if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
				if (firstTry) {
					throw invalid_metacluster_operation();
				} else {
					return Void();
				}
			}

			MetaclusterMetadata::metaclusterRegistration.clear(tr);

			firstTry = false;
			wait(safeThreadFutureToFuture(tr->commit()));
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
                                DataClusterEntry previousEntry,
                                DataClusterEntry updatedEntry) {
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
                           DataClusterMetadata previousMetadata,
                           Optional<ClusterConnectionString> updatedConnectionString,
                           Optional<DataClusterEntry> updatedEntry) {

	if (updatedEntry.present()) {
		ManagementClusterMetadata::dataClusters.set(tr, name, updatedEntry.get());
		updateClusterCapacityIndex(tr, name, previousMetadata.entry, updatedEntry.get());
	}
	if (updatedConnectionString.present()) {
		ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, name, updatedConnectionString.get());
	}
}

ACTOR template <class Transaction>
Future<std::pair<MetaclusterRegistrationEntry, bool>>
managementClusterRegisterPrecheck(Transaction tr, ClusterNameRef name, Optional<DataClusterMetadata> metadata) {
	state Future<Optional<DataClusterMetadata>> dataClusterMetadataFuture = tryGetClusterTransaction(tr, name);
	state Future<Optional<MetaclusterRegistrationEntry>> metaclusterRegistrationFuture =
	    MetaclusterMetadata::metaclusterRegistration.get(tr);

	state Optional<MetaclusterRegistrationEntry> metaclusterRegistration = wait(metaclusterRegistrationFuture);
	if (!metaclusterRegistration.present() ||
	    metaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
		throw invalid_metacluster_operation();
	}

	state Optional<DataClusterMetadata> dataClusterMetadata = wait(dataClusterMetadataFuture);
	if (dataClusterMetadata.present() &&
	    (!metadata.present() || !metadata.get().matchesConfiguration(dataClusterMetadata.get()))) {
		throw cluster_already_exists();
	}

	return std::make_pair(metaclusterRegistration.get(), dataClusterMetadata.present());
}

ACTOR template <class Transaction>
Future<Void> managementClusterRegister(Transaction tr,
                                       ClusterNameRef name,
                                       ClusterConnectionString connectionString,
                                       DataClusterEntry entry) {
	std::pair<MetaclusterRegistrationEntry, bool> result =
	    wait(managementClusterRegisterPrecheck(tr, name, DataClusterMetadata(entry, connectionString)));

	if (!result.second) {
		entry.allocated = ClusterUsage();

		if (entry.hasCapacity()) {
			ManagementClusterMetadata::clusterCapacityIndex.insert(
			    tr, Tuple::makeTuple(entry.allocated.numTenantGroups, name));
		}
		ManagementClusterMetadata::dataClusters.set(tr, name, entry);
		ManagementClusterMetadata::dataClusterConnectionRecords.set(tr, name, connectionString);
	}

	return Void();
}

ACTOR template <class Transaction>
Future<UID> dataClusterRegister(Transaction tr,
                                ClusterNameRef name,
                                ClusterNameRef metaclusterName,
                                UID metaclusterId) {
	state Future<std::vector<std::pair<TenantName, TenantMapEntry>>> existingTenantsFuture =
	    TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type existingDataFuture =
	    tr->getRange(normalKeys, 1);
	state Future<Optional<MetaclusterRegistrationEntry>> clusterRegistrationFuture =
	    MetaclusterMetadata::metaclusterRegistration.get(tr);

	state Optional<MetaclusterRegistrationEntry> existingRegistration = wait(clusterRegistrationFuture);
	if (existingRegistration.present()) {
		if (existingRegistration.get().clusterType != ClusterType::METACLUSTER_DATA ||
		    existingRegistration.get().name != name || existingRegistration.get().metaclusterId != metaclusterId) {
			throw cluster_already_registered();
		} else {
			// We already successfully registered the cluster with these details, so there's nothing to do
			ASSERT(existingRegistration.get().metaclusterName == metaclusterName);
			return existingRegistration.get().id;
		}
	}

	std::vector<std::pair<TenantName, TenantMapEntry>> existingTenants =
	    wait(safeThreadFutureToFuture(existingTenantsFuture));
	if (!existingTenants.empty()) {
		TraceEvent(SevWarn, "CannotRegisterClusterWithTenants").detail("ClusterName", name);
		throw cluster_not_empty();
	}

	RangeResult existingData = wait(safeThreadFutureToFuture(existingDataFuture));
	if (!existingData.empty()) {
		TraceEvent(SevWarn, "CannotRegisterClusterWithData").detail("ClusterName", name);
		throw cluster_not_empty();
	}

	state UID clusterId = deterministicRandom()->randomUniqueID();
	MetaclusterMetadata::metaclusterRegistration.set(
	    tr, MetaclusterRegistrationEntry(metaclusterName, name, metaclusterId, clusterId));

	return clusterId;
}

ACTOR template <class DB>
Future<Void> registerCluster(Reference<DB> db,
                             ClusterName name,
                             ClusterConnectionString connectionString,
                             DataClusterEntry entry) {
	if (name.startsWith("\xff"_sr)) {
		throw invalid_cluster_name();
	}

	state MetaclusterRegistrationEntry managementClusterRegistration;

	// Step 1: Check for a conflicting cluster in the management cluster and get the metacluster ID
	state Reference<typename DB::TransactionT> precheckTr = db->createTransaction();
	loop {
		try {
			precheckTr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			std::pair<MetaclusterRegistrationEntry, bool> result =
			    wait(managementClusterRegisterPrecheck(precheckTr, name, Optional<DataClusterMetadata>()));
			managementClusterRegistration = result.first;

			wait(buggifiedCommit(precheckTr, BUGGIFY));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(precheckTr->onError(e)));
		}
	}

	// Step 2: Configure the data cluster as a subordinate cluster
	state Reference<IDatabase> dataClusterDb = wait(openDatabase(connectionString));
	state Reference<ITransaction> dataClusterTr = dataClusterDb->createTransaction();
	loop {
		try {
			dataClusterTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			UID clusterId = wait(dataClusterRegister(
			    dataClusterTr, name, managementClusterRegistration.name, managementClusterRegistration.metaclusterId));
			entry.id = clusterId;

			wait(buggifiedCommit(dataClusterTr, BUGGIFY));

			TraceEvent("ConfiguredDataCluster")
			    .detail("ClusterName", name)
			    .detail("ClusterID", entry.id)
			    .detail("Capacity", entry.capacity)
			    .detail("Version", dataClusterTr->getCommittedVersion())
			    .detail("ConnectionString", connectionString.toString());

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(dataClusterTr->onError(e)));
		}
	}

	// Step 3: Register the cluster in the management cluster
	state Reference<typename DB::TransactionT> registerTr = db->createTransaction();
	loop {
		try {
			registerTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(managementClusterRegister(registerTr, name, connectionString, entry));
			wait(buggifiedCommit(registerTr, BUGGIFY));

			TraceEvent("RegisteredDataCluster")
			    .detail("ClusterName", name)
			    .detail("ClusterID", entry.id)
			    .detail("Capacity", entry.capacity)
			    .detail("Version", registerTr->getCommittedVersion())
			    .detail("ConnectionString", connectionString.toString());

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(registerTr->onError(e)));
		}
	}

	return Void();
}

ACTOR template <class Transaction>
Future<Optional<DataClusterEntry>> restoreClusterTransaction(Transaction tr,
                                                             ClusterName name,
                                                             std::string connectionString,
                                                             DataClusterEntry entry,
                                                             AddNewTenants addNewTenants,
                                                             RemoveMissingTenants removeMissingTenants) {
	wait(delay(0)); // TODO: remove when implementation is added
	return Optional<DataClusterEntry>();
}

ACTOR template <class DB>
Future<Void> restoreCluster(Reference<DB> db,
                            ClusterName name,
                            std::string connectionString,
                            DataClusterEntry entry,
                            AddNewTenants addNewTenants,
                            RemoveMissingTenants removeMissingTenants) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state Optional<DataClusterEntry> newCluster =
			    wait(restoreCluster(tr, name, connectionString, entry, addNewTenants, removeMissingTenants));

			wait(buggifiedCommit(tr, BUGGIFY));

			TraceEvent("RestoredDataCluster")
			    .detail("ClusterName", name)
			    .detail("ClusterId", newCluster.present() ? newCluster.get().id : UID())
			    .detail("Version", tr->getCommittedVersion());

			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// Returns the cluster metadata for the cluster being deleted, as well as a boolean that will be true if the entry
// has been removed. If false, then it's the responsibility of the caller to purge the data cluster from the management
// cluster.
ACTOR template <class Transaction>
Future<std::pair<Optional<DataClusterMetadata>, bool>> managementClusterRemove(Transaction tr,
                                                                               ClusterNameRef name,
                                                                               bool checkEmpty) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
	if (!metadata.present()) {
		return std::make_pair(metadata, true);
	}

	bool purged = false;
	if (checkEmpty && metadata.get().entry.allocated.numTenantGroups > 0) {
		throw cluster_not_empty();
	} else if (metadata.get().entry.allocated.numTenantGroups == 0) {
		ManagementClusterMetadata::dataClusters.erase(tr, name);
		ManagementClusterMetadata::dataClusterConnectionRecords.erase(tr, name);
		purged = true;
	} else {
		// We need to clean up the tenant metadata for this cluster before erasing it. While we are doing that,
		// lock the entry to prevent other assignments.
		DataClusterEntry updatedEntry = metadata.get().entry;
		updatedEntry.locked = true;

		updateClusterMetadata(tr, name, metadata.get(), Optional<ClusterConnectionString>(), updatedEntry);
	}

	ManagementClusterMetadata::clusterCapacityIndex.erase(
	    tr, Tuple::makeTuple(metadata.get().entry.allocated.numTenantGroups, name));

	return std::make_pair(metadata, purged);
}

ACTOR template <class DB>
Future<Void> managementClusterPurgeDataCluster(Reference<DB> db, ClusterNameRef name, UID dataClusterId) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state std::pair<Tuple, Tuple> clusterTupleRange =
	    std::make_pair(Tuple::makeTuple(name), Tuple::makeTuple(keyAfter(name)));

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state Future<KeyBackedRangeResult<Tuple>> tenantEntriesFuture =
			    ManagementClusterMetadata::clusterTenantIndex.getRange(tr,
			                                                           clusterTupleRange.first,
			                                                           clusterTupleRange.second,
			                                                           CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

			Optional<DataClusterMetadata> clusterMetadata = wait(tryGetClusterTransaction(tr, name));
			if (!clusterMetadata.present() && clusterMetadata.get().entry.id != dataClusterId) {
				// Someone else must have already done the purge
				return Void();
			}

			state KeyBackedRangeResult<Tuple> tenantEntries = wait(tenantEntriesFuture);
			if (tenantEntries.results.empty()) {
				break;
			}

			for (Tuple entry : tenantEntries.results) {
				ASSERT(entry.getString(0) == name);
				ManagementClusterMetadata::tenantMetadata.tenantMap.erase(tr, entry.getString(1));
			}

			// Erase all of the tenants processed in this transaction from the cluster tenant index
			ManagementClusterMetadata::clusterTenantIndex.erase(
			    tr,
			    clusterTupleRange.first,
			    Tuple::makeTuple(name, keyAfter(tenantEntries.results.rbegin()->getString(1))));

			wait(buggifiedCommit(tr, BUGGIFY));
			tr->reset();

			if (!tenantEntries.more) {
				break;
			}
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state Future<KeyBackedRangeResult<Tuple>> tenantGroupEntriesFuture =
			    ManagementClusterMetadata::clusterTenantGroupIndex.getRange(
			        tr,
			        clusterTupleRange.first,
			        clusterTupleRange.second,
			        CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

			Optional<DataClusterMetadata> clusterMetadata = wait(tryGetClusterTransaction(tr, name));
			if (!clusterMetadata.present() && clusterMetadata.get().entry.id != dataClusterId) {
				// Someone else must have already done the purge
				return Void();
			}

			state KeyBackedRangeResult<Tuple> tenantGroupEntries = wait(tenantGroupEntriesFuture);
			for (Tuple entry : tenantGroupEntries.results) {
				ASSERT(entry.getString(0) == name);
				TenantGroupName tenantGroup = entry.getString(1);
				ManagementClusterMetadata::tenantMetadata.tenantGroupTenantIndex.erase(
				    tr, Tuple::makeTuple(tenantGroup), Tuple::makeTuple(keyAfter(tenantGroup)));
				ManagementClusterMetadata::tenantGroupMap.erase(tr, tenantGroup);
			}

			if (!tenantGroupEntries.results.empty()) {
				// Erase all of the tenants processed in this transaction from the cluster tenant index
				ManagementClusterMetadata::clusterTenantIndex.erase(
				    tr,
				    clusterTupleRange.first,
				    Tuple::makeTuple(name, keyAfter(tenantGroupEntries.results.rbegin()->getString(1))));
			}

			if (!tenantGroupEntries.more) {
				ManagementClusterMetadata::dataClusters.erase(tr, name);
				ManagementClusterMetadata::dataClusterConnectionRecords.erase(tr, name);
			}

			wait(buggifiedCommit(tr, BUGGIFY));
			tr->reset();

			if (!tenantGroupEntries.more) {
				TraceEvent("RemovedDataCluster").detail("Name", name).detail("Version", tr->getCommittedVersion());
				break;
			}
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	return Void();
}

ACTOR template <class Transaction>
Future<Void> dataClusterRemove(Transaction tr, Optional<int64_t> lastTenantId, UID dataClusterId) {
	state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
	    wait(MetaclusterMetadata::metaclusterRegistration.get(tr));
	if (!metaclusterRegistration.present()) {
		return Void();
	}

	if (metaclusterRegistration.get().id != dataClusterId) {
		return Void();
	}

	MetaclusterMetadata::metaclusterRegistration.clear(tr);
	TenantMetadata::tenantTombstones.clear(tr);

	// If we are force removing a cluster, then it will potentially contain tenants that have IDs
	// larger than the next tenant ID to be allocated on the cluster. To avoid collisions, we advance
	// the ID so that it will be the larger of the current one on the data cluster and the management
	// cluster.
	if (lastTenantId.present()) {
		Optional<int64_t> lastId = wait(TenantMetadata::lastTenantId.get(tr));
		if (!lastId.present() || lastId.get() < lastTenantId.get()) {
			TenantMetadata::lastTenantId.set(tr, lastTenantId.get());
		}
	}

	return Void();
}

ACTOR template <class DB>
Future<Void> removeCluster(Reference<DB> db, ClusterName name, bool forceRemove) {
	// Step 1: Remove the data cluster from the metacluster
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state DataClusterMetadata metadata;
	state Optional<int64_t> lastTenantId;
	state Optional<UID> removedId;
	state bool hasBeenPurged = false;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			std::pair<Optional<DataClusterMetadata>, bool> result =
			    wait(managementClusterRemove(tr, name, !forceRemove));
			if (!result.first.present()) {
				if (!removedId.present()) {
					throw cluster_not_found();
				} else {
					return Void();
				}
			}

			metadata = result.first.get();
			if (!removedId.present()) {
				removedId = metadata.entry.id;
			} else if (removedId.get() != metadata.entry.id) {
				// The cluster we were removing is gone and has already been replaced
				return Void();
			}

			hasBeenPurged = result.second;
			if (forceRemove) {
				Optional<int64_t> lastId = wait(ManagementClusterMetadata::tenantMetadata.lastTenantId.get(tr));
				lastTenantId = lastId;
			}

			wait(buggifiedCommit(tr, BUGGIFY));

			if (hasBeenPurged) {
				TraceEvent("RemovedDataCluster").detail("Name", name).detail("Version", tr->getCommittedVersion());
			} else {
				TraceEvent("LockedDataCluster").detail("Name", name).detail("Version", tr->getCommittedVersion());
			}
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	ASSERT(removedId.present());

	// Step 2: Purge all metadata associated with the data cluster from the management cluster if this was not already
	//         completed in step 1.
	if (!hasBeenPurged) {
		wait(managementClusterPurgeDataCluster(db, name, removedId.get()));
	}

	// Step 3: Update the data cluster to mark it as removed.
	//         Note that this is best effort; if it fails the cluster will still have been removed.
	state Reference<IDatabase> dataClusterDb = wait(openDatabase(metadata.connectionString));
	state Reference<ITransaction> dataClusterTr = dataClusterDb->createTransaction();
	loop {
		try {
			dataClusterTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			wait(dataClusterRemove(dataClusterTr, lastTenantId, removedId.get()));
			wait(buggifiedCommit(dataClusterTr, BUGGIFY));

			TraceEvent("ReconfiguredDataCluster")
			    .detail("Name", name)
			    .detail("Version", dataClusterTr->getCommittedVersion());
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(dataClusterTr->onError(e)));
		}
	}

	return Void();
}

ACTOR template <class Transaction>
Future<std::map<ClusterName, DataClusterMetadata>> listClustersTransaction(Transaction tr,
                                                                           ClusterNameRef begin,
                                                                           ClusterNameRef end,
                                                                           int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<ClusterType> clusterTypeFuture = TenantAPI::getClusterType(tr);

	state Future<KeyBackedRangeResult<std::pair<ClusterName, DataClusterEntry>>> clusterEntriesFuture =
	    ManagementClusterMetadata::dataClusters.getRange(tr, begin, end, limit);
	state Future<KeyBackedRangeResult<std::pair<ClusterName, ClusterConnectionString>>> connectionStringFuture =
	    ManagementClusterMetadata::dataClusterConnectionRecords.getRange(tr, begin, end, limit);

	ClusterType clusterType = wait(clusterTypeFuture);
	if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
		throw invalid_metacluster_operation();
	}

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

ACTOR template <class Transaction>
Future<std::pair<ClusterName, DataClusterMetadata>> assignTenant(Transaction tr, TenantMapEntry tenantEntry) {
	state Optional<TenantGroupEntry> groupEntry;
	if (tenantEntry.tenantGroup.present()) {
		if (tenantEntry.tenantGroup.get().startsWith("\xff"_sr)) {
			throw invalid_tenant_group_name();
		}

		Optional<TenantGroupEntry> _groupEntry =
		    wait(ManagementClusterMetadata::tenantGroupMap.get(tr, tenantEntry.tenantGroup.get()));
		groupEntry = _groupEntry;

		if (groupEntry.present()) {
			Optional<DataClusterMetadata> clusterMetadata =
			    wait(tryGetClusterTransaction(tr, groupEntry.get().assignedCluster));

			ASSERT(clusterMetadata.present());
			return std::make_pair(groupEntry.get().assignedCluster, clusterMetadata.get());
		}
	}

	state KeyBackedSet<Tuple>::RangeResultType availableClusters =
	    wait(ManagementClusterMetadata::clusterCapacityIndex.getRange(tr, {}, {}, 1, Snapshot::False, Reverse::True));

	state Optional<ClusterName> chosenCluster;
	if (!availableClusters.results.empty()) {
		// TODO: check that the chosen cluster is available, otherwise we can try another
		chosenCluster = availableClusters.results[0].getString(1);
	}

	if (chosenCluster.present()) {
		Optional<DataClusterMetadata> clusterMetadata = wait(tryGetClusterTransaction(tr, chosenCluster.get()));
		ASSERT(clusterMetadata.present());

		DataClusterEntry clusterEntry = clusterMetadata.get().entry;
		ASSERT(clusterEntry.hasCapacity());

		++clusterEntry.allocated.numTenantGroups;

		updateClusterMetadata(
		    tr, chosenCluster.get(), clusterMetadata.get(), Optional<ClusterConnectionString>(), clusterEntry);
		if (tenantEntry.tenantGroup.present()) {
			ManagementClusterMetadata::tenantGroupMap.set(
			    tr, tenantEntry.tenantGroup.get(), TenantGroupEntry(chosenCluster.get()));
		}

		return std::make_pair(chosenCluster.get(), clusterMetadata.get());
	}

	throw metacluster_no_capacity();
}

ACTOR template <class Transaction>
Future<std::pair<TenantMapEntry, bool>> managementClusterCreateTenant(Transaction tr,
                                                                      TenantNameRef name,
                                                                      TenantMapEntry tenantEntry) {
	ASSERT(tenantEntry.assignedCluster.present());
	ASSERT(tenantEntry.id >= 0);

	if (name.startsWith("\xff"_sr)) {
		throw invalid_tenant_name();
	}

	state Future<Optional<TenantMapEntry>> existingEntryFuture = tryGetTenantTransaction(tr, name);
	Optional<TenantMapEntry> existingEntry = wait(existingEntryFuture);
	if (existingEntry.present()) {
		return std::make_pair(existingEntry.get(), false);
	}

	tenantEntry.setSubspace(""_sr);
	tenantEntry.tenantState = TenantState::REGISTERING;
	ManagementClusterMetadata::tenantMetadata.tenantMap.set(tr, name, tenantEntry);

	if (tenantEntry.tenantGroup.present()) {
		ManagementClusterMetadata::tenantMetadata.tenantGroupTenantIndex.insert(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), name));
	}

	return std::make_pair(tenantEntry, true);
}

ACTOR template <class DB>
Future<Void> createTenant(Reference<DB> db, TenantName name, TenantMapEntry tenantEntry) {
	state DataClusterMetadata clusterMetadata;
	state TenantMapEntry createdTenant;

	// Step 1: assign the tenant and record its details in the management cluster
	state Reference<typename DB::TransactionT> assignTr = db->createTransaction();
	loop {
		try {
			assignTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state Future<std::pair<ClusterName, DataClusterMetadata>> assignmentFuture =
			    assignTenant(assignTr, tenantEntry);

			ClusterType clusterType = wait(TenantAPI::getClusterType(assignTr));
			if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
				throw invalid_metacluster_operation();
			}

			std::pair<ClusterName, DataClusterMetadata> assignment = wait(assignmentFuture);
			tenantEntry.assignedCluster = assignment.first;
			clusterMetadata = assignment.second;

			Optional<int64_t> lastId = wait(ManagementClusterMetadata::tenantMetadata.lastTenantId.get(assignTr));
			tenantEntry.id = lastId.orDefault(-1) + 1;

			std::pair<TenantMapEntry, bool> result = wait(managementClusterCreateTenant(assignTr, name, tenantEntry));

			createdTenant = result.first;

			if (!result.second) {
				if (!result.first.matchesConfiguration(tenantEntry) ||
				    result.first.tenantState != TenantState::REGISTERING) {
					throw tenant_already_exists();
				} else if (tenantEntry.assignedCluster != createdTenant.assignedCluster) {
					if (!result.first.assignedCluster.present()) {
						// This is an unexpected state in a metacluster, but if it happens then it wasn't created here
						throw tenant_already_exists();
					}

					Optional<DataClusterMetadata> actualMetadata =
					    wait(tryGetClusterTransaction(assignTr, createdTenant.assignedCluster.get()));

					// A cluster cannot be removed through these APIs unless it has no tenants assigned to it.
					ASSERT(actualMetadata.present());
					clusterMetadata = actualMetadata.get();
				}
			} else {
				ManagementClusterMetadata::clusterTenantIndex.insert(
				    assignTr, Tuple::makeTuple(createdTenant.assignedCluster.get(), name));

				if (tenantEntry.tenantGroup.present()) {
					ManagementClusterMetadata::clusterTenantGroupIndex.insert(
					    assignTr, Tuple::makeTuple(createdTenant.assignedCluster.get(), tenantEntry.tenantGroup.get()));
				}

				ManagementClusterMetadata::tenantMetadata.lastTenantId.set(assignTr, tenantEntry.id);
				wait(buggifiedCommit(assignTr, BUGGIFY));
			}

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(assignTr->onError(e)));
		}
	}

	// Step 2: store the tenant info in the data cluster
	state Reference<IDatabase> dataClusterDb = wait(openDatabase(clusterMetadata.connectionString));
	Optional<TenantMapEntry> dataClusterTenant =
	    wait(TenantAPI::createTenant(dataClusterDb, name, createdTenant, ClusterType::METACLUSTER_DATA));

	if (!dataClusterTenant.present()) {
		// We were deleted simultaneously
		return Void();
	}

	// Step 3: mark the tenant as ready in the management cluster
	state Reference<typename DB::TransactionT> finalizeTr = db->createTransaction();
	loop {
		try {
			finalizeTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<TenantMapEntry> managementEntry = wait(tryGetTenantTransaction(finalizeTr, name));
			if (!managementEntry.present()) {
				throw tenant_removed();
			} else if (managementEntry.get().id != createdTenant.id) {
				throw tenant_already_exists();
			}

			if (managementEntry.get().tenantState == TenantState::REGISTERING) {
				TenantMapEntry updatedEntry = managementEntry.get();
				updatedEntry.tenantState = TenantState::READY;
				ManagementClusterMetadata::tenantMetadata.tenantMap.set(finalizeTr, name, updatedEntry);
				wait(buggifiedCommit(finalizeTr, BUGGIFY));
			}

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(finalizeTr->onError(e)));
		}
	}

	return Void();
}

ACTOR template <class Transaction>
Future<Void> managementClusterDeleteTenant(Transaction tr, TenantNameRef name, int64_t tenantId) {
	state Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, name));
	if (tenantEntry.present() && tenantEntry.get().id == tenantId) {
		ASSERT(tenantEntry.get().tenantState == TenantState::REMOVING);

		ManagementClusterMetadata::tenantMetadata.tenantMap.erase(tr, name);
		if (tenantEntry.get().tenantGroup.present()) {
			ManagementClusterMetadata::tenantMetadata.tenantGroupTenantIndex.erase(
			    tr, Tuple::makeTuple(tenantEntry.get().tenantGroup.get(), name));
		}
	}

	return Void();
}

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, TenantName name) {
	state int64_t tenantId;
	state DataClusterMetadata clusterMetadata;
	state bool alreadyRemoving = false;
	state Future<Void> tenantModeCheck;

	// Step 1: get the assigned location of the tenant
	state Reference<typename DB::TransactionT> managementTr = db->createTransaction();
	loop {
		try {
			managementTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tenantModeCheck = TenantAPI::checkTenantMode(managementTr, ClusterType::METACLUSTER_MANAGEMENT);

			state Optional<TenantMapEntry> tenantEntry1 = wait(tryGetTenantTransaction(managementTr, name));
			wait(tenantModeCheck);

			if (!tenantEntry1.present()) {
				throw tenant_not_found();
			}

			tenantId = tenantEntry1.get().id;

			if (tenantEntry1.get().assignedCluster.present()) {
				Optional<DataClusterMetadata> _clusterMetadata =
				    wait(tryGetClusterTransaction(managementTr, tenantEntry1.get().assignedCluster.get()));

				// A cluster cannot be removed through these APIs unless it has no tenants assigned to it.
				ASSERT(_clusterMetadata.present());

				clusterMetadata = _clusterMetadata.get();
				alreadyRemoving = tenantEntry1.get().tenantState == TenantState::REMOVING;
			} else {
				// The record only exists on the management cluster, so we can just delete it.
				TenantMapEntry updatedEntry = tenantEntry1.get();
				updatedEntry.tenantState = TenantState::REMOVING;
				ManagementClusterMetadata::tenantMetadata.tenantMap.set(managementTr, name, updatedEntry);
				wait(managementClusterDeleteTenant(managementTr, name, tenantId));
				wait(buggifiedCommit(managementTr, BUGGIFY));
				return Void();
			}

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(managementTr->onError(e)));
		}
	}

	state Reference<IDatabase> dataClusterDb = wait(openDatabase(clusterMetadata.connectionString));

	if (!alreadyRemoving) {
		// Step 2: check that the tenant is empty
		state Reference<ITenant> dataTenant = dataClusterDb->openTenant(name);
		state Reference<ITransaction> dataTr = dataTenant->createTransaction();
		loop {
			try {
				ThreadFuture<RangeResult> rangeFuture = dataTr->getRange(normalKeys, 1);
				RangeResult result = wait(safeThreadFutureToFuture(rangeFuture));
				if (!result.empty()) {
					throw tenant_not_empty();
				}
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(dataTr->onError(e)));
			}
		}

		// Step 3: record that we are removing the tenant in the management cluster
		managementTr = db->createTransaction();
		loop {
			try {
				managementTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Optional<TenantMapEntry> tenantEntry2 = wait(tryGetTenantTransaction(managementTr, name));
				if (!tenantEntry2.present() || tenantEntry2.get().id != tenantId) {
					// The tenant must have been removed simultaneously
					return Void();
				}

				if (tenantEntry2.get().tenantState != TenantState::REMOVING) {
					TenantMapEntry updatedEntry = tenantEntry2.get();
					updatedEntry.tenantState = TenantState::REMOVING;
					ManagementClusterMetadata::tenantMetadata.tenantMap.set(managementTr, name, updatedEntry);
					wait(buggifiedCommit(managementTr, BUGGIFY));
				}

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(managementTr->onError(e)));
			}
		}
	}

	// Step 4: remove the tenant from the data cluster
	wait(TenantAPI::deleteTenant(dataClusterDb, name, tenantId, ClusterType::METACLUSTER_DATA));

	// Step 5: delete the tenant from the management cluster
	managementTr = db->createTransaction();
	loop {
		try {
			managementTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tenantModeCheck = TenantAPI::checkTenantMode(managementTr, ClusterType::METACLUSTER_MANAGEMENT);
			state Optional<TenantMapEntry> tenantEntry3 = wait(tryGetTenantTransaction(managementTr, name));
			wait(tenantModeCheck);

			if (!tenantEntry3.present() || tenantEntry3.get().id != tenantId) {
				return Void();
			}

			wait(managementClusterDeleteTenant(managementTr, name, tenantId));

			if (tenantEntry3.get().assignedCluster.present()) {
				state Future<KeyBackedSet<Tuple>::RangeResultType> tenantGroupIndexFuture;
				state Optional<TenantGroupName> tenantGroup = tenantEntry3.get().tenantGroup;
				if (tenantGroup.present()) {
					tenantGroupIndexFuture = ManagementClusterMetadata::tenantMetadata.tenantGroupTenantIndex.getRange(
					    managementTr,
					    Tuple::makeTuple(tenantGroup.get()),
					    Tuple::makeTuple(keyAfter(tenantGroup.get())),
					    1);
				}

				state Optional<DataClusterMetadata> finalClusterMetadata =
				    wait(tryGetClusterTransaction(managementTr, tenantEntry3.get().assignedCluster.get()));

				state DataClusterEntry updatedEntry = finalClusterMetadata.get().entry;
				state bool decrementTenantGroupCount = finalClusterMetadata.present() && !tenantGroup.present();

				if (finalClusterMetadata.present()) {
					ManagementClusterMetadata::clusterTenantIndex.erase(
					    managementTr, Tuple::makeTuple(tenantEntry3.get().assignedCluster.get(), name));

					if (tenantGroup.present()) {
						ManagementClusterMetadata::clusterTenantGroupIndex.erase(
						    managementTr,
						    Tuple::makeTuple(tenantEntry3.get().assignedCluster.get(), tenantGroup.get()));
						KeyBackedSet<Tuple>::RangeResultType result = wait(tenantGroupIndexFuture);
						if (result.results.size() == 0) {
							ManagementClusterMetadata::tenantGroupMap.erase(managementTr, tenantGroup.get());
							decrementTenantGroupCount = true;
						}
					}
				}
				if (decrementTenantGroupCount) {
					--updatedEntry.allocated.numTenantGroups;
					updateClusterMetadata(managementTr,
					                      tenantEntry3.get().assignedCluster.get(),
					                      finalClusterMetadata.get(),
					                      Optional<ClusterConnectionString>(),
					                      updatedEntry);
				}
			}

			wait(buggifiedCommit(managementTr, BUGGIFY));

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(managementTr->onError(e)));
		}
	}

	return Void();
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantsTransaction(Transaction tr,
                                                                                  TenantNameRef begin,
                                                                                  TenantNameRef end,
                                                                                  int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> results =
	    wait(ManagementClusterMetadata::tenantMetadata.tenantMap.getRange(tr, begin, end, limit));

	return results.results;
}

ACTOR template <class DB>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenants(Reference<DB> db,
                                                                       TenantName begin,
                                                                       TenantName end,
                                                                       int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
			    wait(listTenantsTransaction(tr, begin, end, limit));
			return tenants;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}
}; // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif