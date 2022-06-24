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

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString);

Key getDataClusterTenantIndexKey(ClusterNameRef cluster, Optional<TenantNameRef> tenant);
Key getDataClusterTenantGroupIndexKey(ClusterNameRef cluster, Optional<TenantGroupNameRef> tenantGroup);

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> tryGetClusterTransaction(Transaction tr, ClusterNameRef name) {
	state Key dataClusterMetadataKey = dataClusterMetadataPrefix.withSuffix(name);
	state Key dataClusterConnectionRecordKey = dataClusterConnectionRecordPrefix.withSuffix(name);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<ClusterType> clusterTypeFuture = TenantAPI::getClusterType(tr);
	state typename transaction_future_type<Transaction, Optional<Value>>::type metadataFuture =
	    tr->get(dataClusterMetadataKey);
	state typename transaction_future_type<Transaction, Optional<Value>>::type connectionRecordFuture =
	    tr->get(dataClusterConnectionRecordKey);

	ClusterType clusterType = wait(clusterTypeFuture);
	if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
		throw invalid_metacluster_operation();
	}

	state Optional<Value> metadata = wait(safeThreadFutureToFuture(metadataFuture));
	Optional<Value> connectionString = wait(safeThreadFutureToFuture(connectionRecordFuture));

	if (metadata.present()) {
		ASSERT(connectionString.present());
		return Optional<DataClusterMetadata>(DataClusterMetadata(
		    DataClusterEntry::decode(metadata.get()), ClusterConnectionString(connectionString.get().toString())));
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

ACTOR template <class DB>
Future<Optional<std::string>> createMetacluster(Reference<DB> db, ClusterName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Optional<UID> metaclusterUid;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state typename DB::TransactionT::template FutureT<Optional<Value>> metaclusterRegistrationFuture =
			    tr->get(metaclusterRegistrationKey);
			state Future<std::map<TenantName, TenantMapEntry>> tenantsFuture =
			    TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
			state typename DB::TransactionT::template FutureT<RangeResult> dataClustersFuture =
			    tr->getRange(dataClusterMetadataKeys, 1);
			state typename DB::TransactionT::template FutureT<RangeResult> dbContentsFuture =
			    tr->getRange(normalKeys, 1);

			Optional<Value> existingRegistrationValue = wait(safeThreadFutureToFuture(metaclusterRegistrationFuture));
			Optional<MetaclusterRegistrationEntry> existingRegistration =
			    MetaclusterRegistrationEntry::decode(existingRegistrationValue);
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

			std::map<TenantName, TenantMapEntry> tenants = wait(tenantsFuture);
			if (!tenants.empty()) {
				throw cluster_not_empty();
			}

			RangeResult dataClusters = wait(safeThreadFutureToFuture(dataClustersFuture));
			if (!dataClusters.empty()) {
				throw cluster_not_empty();
			}

			RangeResult dbContents = wait(safeThreadFutureToFuture(dbContentsFuture));
			if (!dbContents.empty()) {
				throw cluster_not_empty();
			}

			if (!metaclusterUid.present()) {
				metaclusterUid = deterministicRandom()->randomUniqueID();
			}
			tr->set(metaclusterRegistrationKey, MetaclusterRegistrationEntry(name, metaclusterUid.get()).encode());

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
			state Future<std::map<TenantName, TenantMapEntry>> tenantsFuture =
			    TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
			state typename DB::TransactionT::template FutureT<RangeResult> dataClustersFuture =
			    tr->getRange(dataClusterMetadataKeys, 1);

			ClusterType clusterType = wait(clusterTypeFuture);
			if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
				if (firstTry) {
					throw invalid_metacluster_operation();
				} else {
					return Void();
				}
			}

			std::map<TenantName, TenantMapEntry> tenants = wait(tenantsFuture);
			if (!tenants.empty()) {
				throw cluster_not_empty();
			}

			RangeResult dataClusters = wait(safeThreadFutureToFuture(dataClustersFuture));
			if (!dataClusters.empty()) {
				throw cluster_not_empty();
			}

			tr->clear(metaclusterRegistrationKey);

			firstTry = false;
			wait(safeThreadFutureToFuture(tr->commit()));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	return Void();
}

// This should only be called from a transaction that has already confirmed that the cluster entry
// is present. The updatedEntry should use the existing entry and modify only those fields that need
// to be changed.
template <class Transaction>
void updateClusterMetadata(Transaction tr,
                           ClusterNameRef name,
                           Optional<ClusterConnectionString> updatedConnectionString,
                           Optional<DataClusterEntry> updatedEntry) {

	if (updatedEntry.present()) {
		tr->set(dataClusterMetadataPrefix.withSuffix(name), updatedEntry.get().encode());
	}
	if (updatedConnectionString.present()) {
		tr->set(dataClusterConnectionRecordPrefix.withSuffix(name), updatedConnectionString.get().toString());
	}
}

ACTOR template <class Transaction>
Future<std::pair<MetaclusterRegistrationEntry, bool>>
managementClusterRegisterPrecheck(Transaction tr, ClusterNameRef name, Optional<DataClusterMetadata> metadata) {
	state Future<Optional<DataClusterMetadata>> dataClusterMetadataFuture = tryGetClusterTransaction(tr, name);
	state typename transaction_future_type<Transaction, Optional<Value>>::type metaclusterRegistrationFuture =
	    tr->get(metaclusterRegistrationKey);

	Optional<Value> metaclusterRegistrationValue = wait(safeThreadFutureToFuture(metaclusterRegistrationFuture));
	state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
	    MetaclusterRegistrationEntry::decode(metaclusterRegistrationValue);

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
	state Key dataClusterMetadataKey = dataClusterMetadataPrefix.withSuffix(name);
	state Key dataClusterConnectionRecordKey = dataClusterConnectionRecordPrefix.withSuffix(name);

	std::pair<MetaclusterRegistrationEntry, bool> result =
	    wait(managementClusterRegisterPrecheck(tr, name, DataClusterMetadata(entry, connectionString)));

	if (!result.second) {
		entry.allocated = ClusterUsage();

		tr->set(dataClusterMetadataKey, entry.encode());
		tr->set(dataClusterConnectionRecordKey, connectionString.toString());
	}

	return Void();
}

ACTOR template <class Transaction>
Future<UID> dataClusterRegister(Transaction tr,
                                ClusterNameRef name,
                                ClusterNameRef metaclusterName,
                                UID metaclusterId) {
	state Future<std::map<TenantName, TenantMapEntry>> existingTenantsFuture =
	    TenantAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type existingDataFuture =
	    tr->getRange(normalKeys, 1);
	state typename transaction_future_type<Transaction, Optional<Value>>::type clusterRegistrationFuture =
	    tr->get(metaclusterRegistrationKey);

	Optional<Value> storedClusterRegistration = wait(safeThreadFutureToFuture(clusterRegistrationFuture));
	if (storedClusterRegistration.present()) {
		MetaclusterRegistrationEntry existingRegistration =
		    MetaclusterRegistrationEntry::decode(storedClusterRegistration.get());

		if (existingRegistration.clusterType != ClusterType::METACLUSTER_DATA || existingRegistration.name != name ||
		    existingRegistration.metaclusterId != metaclusterId) {
			throw cluster_already_registered();
		} else {
			// We already successfully registered the cluster with these details, so there's nothing to do
			ASSERT(existingRegistration.metaclusterName == metaclusterName);
			return existingRegistration.id;
		}
	}

	std::map<TenantName, TenantMapEntry> existingTenants = wait(safeThreadFutureToFuture(existingTenantsFuture));
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
	tr->set(metaclusterRegistrationKey,
	        MetaclusterRegistrationEntry(metaclusterName, name, metaclusterId, clusterId).encode());

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
	state Key dataClusterMetadataKey = dataClusterMetadataPrefix.withSuffix(name);
	state Key dataClusterConnectionRecordKey = dataClusterConnectionRecordPrefix.withSuffix(name);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
	if (!metadata.present()) {
		return std::make_pair(metadata, true);
	}

	if (checkEmpty && metadata.get().entry.allocated.numTenantGroups > 0) {
		throw cluster_not_empty();
	} else if (metadata.get().entry.allocated.numTenantGroups == 0) {
		tr->clear(dataClusterMetadataKey);
		tr->clear(dataClusterConnectionRecordKey);
		return std::make_pair(metadata, true);
	} else {
		// We need to clean up the tenant metadata for this cluster before erasing it. While we are doing that,
		// lock the entry to prevent other assignments.
		DataClusterEntry updatedEntry = metadata.get().entry;
		updatedEntry.locked = true;

		updateClusterMetadata(tr, name, Optional<ClusterConnectionString>(), updatedEntry);
		return std::make_pair(metadata, false);
	}
}

ACTOR template <class DB>
Future<Void> managementClusterPurgeDataCluster(Reference<DB> db, ClusterNameRef name, UID dataClusterId) {
	// Remove all records associated with this cluster from the management cluster
	state KeyRange tenantIndexKeys = prefixRange(getDataClusterTenantIndexKey(name, Optional<TenantNameRef>()));
	state KeyRange tenantGroupIndexKeys =
	    prefixRange(getDataClusterTenantGroupIndexKey(name, Optional<TenantGroupNameRef>()));

	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state typename DB::TransactionT::template FutureT<RangeResult> tenantEntriesFuture =
			    tr->getRange(tenantIndexKeys, CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

			Optional<DataClusterMetadata> clusterMetadata = wait(tryGetClusterTransaction(tr, name));
			if (!clusterMetadata.present() && clusterMetadata.get().entry.id != dataClusterId) {
				// Someone else must have already done the purge
				return Void();
			}

			state RangeResult tenantEntries = wait(safeThreadFutureToFuture(tenantEntriesFuture));
			if (tenantEntries.empty()) {
				break;
			}

			for (auto entry : tenantEntries) {
				Tuple result = Tuple::unpack(entry.key.removePrefix(dataClusterTenantIndexKeys.begin));
				ASSERT(result.getString(0) == name);
				tr->clear(tenantMapPrefix.withSuffix(result.getString(1)));
			}

			tr->clear(KeyRangeRef(tenantIndexKeys.begin, keyAfter(tenantEntries.rbegin()->key)));
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
			state typename DB::TransactionT::template FutureT<RangeResult> tenantGroupEntriesFuture =
			    tr->getRange(tenantGroupIndexKeys, CLIENT_KNOBS->REMOVE_CLUSTER_TENANT_BATCH_SIZE);

			Optional<DataClusterMetadata> clusterMetadata = wait(tryGetClusterTransaction(tr, name));
			if (!clusterMetadata.present() && clusterMetadata.get().entry.id != dataClusterId) {
				// Someone else must have already done the purge
				return Void();
			}

			state RangeResult tenantGroupEntries = wait(safeThreadFutureToFuture(tenantGroupEntriesFuture));
			for (auto entry : tenantGroupEntries) {
				Tuple result = Tuple::unpack(entry.key.removePrefix(dataClusterTenantGroupIndexKeys.begin));
				ASSERT(result.getString(0) == name);
				TenantGroupName tenantGroup = result.getString(1);
				tr->clear(prefixRange(TenantAPI::getTenantGroupIndexKey(tenantGroup, Optional<TenantNameRef>())));
				tr->clear(tenantGroupMetadataKeys.begin.withSuffix(tenantGroup));
			}

			if (!tenantGroupEntries.empty()) {
				tr->clear(KeyRangeRef(tenantGroupIndexKeys.begin, keyAfter(tenantGroupEntries.rbegin()->key)));
			}

			if (!tenantGroupEntries.more) {
				tr->clear(dataClusterMetadataPrefix.withSuffix(name));
				tr->clear(dataClusterConnectionRecordPrefix.withSuffix(name));
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
	state typename transaction_future_type<Transaction, Optional<Value>>::type metaclusterRegistrationFuture =
	    tr->get(metaclusterRegistrationKey);
	Optional<Value> metaclusterRegistrationVal = wait(safeThreadFutureToFuture(metaclusterRegistrationFuture));
	if (!metaclusterRegistrationVal.present()) {
		return Void();
	}

	MetaclusterRegistrationEntry metaclusterRegistration =
	    MetaclusterRegistrationEntry::decode(metaclusterRegistrationVal.get());

	if (metaclusterRegistration.id != dataClusterId) {
		return Void();
	}

	tr->clear(metaclusterRegistrationKey);
	tr->clear(tenantTombstoneKeys);

	// If we are force removing a cluster, then it will potentially contain tenants that have IDs
	// larger than the next tenant ID to be allocated on the cluster. To avoid collisions, we advance
	// the ID so that it will be the larger of the current one on the data cluster and the management
	// cluster.
	if (lastTenantId.present()) {
		state typename transaction_future_type<Transaction, Optional<Value>>::type lastIdFuture =
		    tr->get(tenantLastIdKey);
		Optional<Value> lastIdVal = wait(safeThreadFutureToFuture(lastIdFuture));
		if (!lastIdVal.present() || TenantMapEntry::prefixToId(lastIdVal.get()) < lastTenantId.get()) {
			tr->set(tenantLastIdKey, TenantMapEntry::idToPrefix(lastTenantId.get()));
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
				state typename DB::TransactionT::template FutureT<Optional<Value>> lastIdFuture =
				    tr->get(tenantLastIdKey);
				Optional<Value> lastIdVal = wait(safeThreadFutureToFuture(lastIdFuture));
				if (lastIdVal.present()) {
					lastTenantId = TenantMapEntry::prefixToId(lastIdVal.get());
				}
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
Future<std::map<ClusterName, DataClusterMetadata>> managementClusterListClusters(Transaction tr,
                                                                                 ClusterNameRef begin,
                                                                                 ClusterNameRef end,
                                                                                 int limit) {
	state KeyRange metadataRange = KeyRangeRef(begin, end).withPrefix(dataClusterMetadataPrefix);
	state KeyRange connectionStringRange = KeyRangeRef(begin, end).withPrefix(dataClusterConnectionRecordPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state typename transaction_future_type<Transaction, RangeResult>::type metadataFuture =
	    tr->getRange(firstGreaterOrEqual(metadataRange.begin), firstGreaterOrEqual(metadataRange.end), limit);
	state typename transaction_future_type<Transaction, RangeResult>::type connectionStringFuture = tr->getRange(
	    firstGreaterOrEqual(connectionStringRange.begin), firstGreaterOrEqual(connectionStringRange.end), limit);

	state RangeResult metadata = wait(safeThreadFutureToFuture(metadataFuture));
	RangeResult connectionStrings = wait(safeThreadFutureToFuture(connectionStringFuture));

	ASSERT(metadata.size() == connectionStrings.size());

	std::map<ClusterName, DataClusterMetadata> clusters;
	for (int i = 0; i < metadata.size(); ++i) {
		clusters[metadata[i].key.removePrefix(dataClusterMetadataPrefix)] =
		    DataClusterMetadata(DataClusterEntry::decode(metadata[i].value),
		                        ClusterConnectionString(connectionStrings[i].value.toString()));
	}

	return clusters;
}

ACTOR template <class Transaction>
Future<std::map<ClusterName, DataClusterMetadata>> listClustersTransaction(Transaction tr,
                                                                           ClusterNameRef begin,
                                                                           ClusterNameRef end,
                                                                           int limit) {
	state KeyRange metadataRange = KeyRangeRef(begin, end).withPrefix(dataClusterMetadataPrefix);
	state KeyRange connectionStringRange = KeyRangeRef(begin, end).withPrefix(dataClusterConnectionRecordPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<ClusterType> clusterTypeFuture = TenantAPI::getClusterType(tr);
	state typename transaction_future_type<Transaction, RangeResult>::type metadataFuture =
	    tr->getRange(firstGreaterOrEqual(metadataRange.begin), firstGreaterOrEqual(metadataRange.end), limit);
	state typename transaction_future_type<Transaction, RangeResult>::type connectionStringFuture = tr->getRange(
	    firstGreaterOrEqual(connectionStringRange.begin), firstGreaterOrEqual(connectionStringRange.end), limit);

	ClusterType clusterType = wait(clusterTypeFuture);
	if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
		throw invalid_metacluster_operation();
	}

	state RangeResult metadata = wait(safeThreadFutureToFuture(metadataFuture));
	RangeResult connectionStrings = wait(safeThreadFutureToFuture(connectionStringFuture));

	ASSERT(metadata.size() == connectionStrings.size());

	std::map<ClusterName, DataClusterMetadata> clusters;
	for (int i = 0; i < metadata.size(); ++i) {
		clusters[metadata[i].key.removePrefix(dataClusterMetadataPrefix)] =
		    DataClusterMetadata(DataClusterEntry::decode(metadata[i].value),
		                        ClusterConnectionString(connectionStrings[i].value.toString()));
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
	// TODO: check for invalid tenant group name
	// TODO: check that the chosen cluster is available, otherwise we can try another

	state typename transaction_future_type<Transaction, Optional<Value>>::type groupMetadataFuture;
	state bool creatingTenantGroup = true;
	if (tenantEntry.tenantGroup.present()) {
		groupMetadataFuture = tr->get(tenantGroupMetadataKeys.begin.withSuffix(tenantEntry.tenantGroup.get()));
		Optional<Value> groupMetadata = wait(safeThreadFutureToFuture(groupMetadataFuture));
		if (groupMetadata.present()) {
			creatingTenantGroup = false;
			state TenantGroupEntry groupEntry = TenantGroupEntry::decode(groupMetadata.get());
			Optional<DataClusterMetadata> clusterMetadata =
			    wait(tryGetClusterTransaction(tr, groupEntry.assignedCluster));

			// TODO: This is only true if we clean up tenant state after force removal.
			ASSERT(clusterMetadata.present());
			return std::make_pair(groupEntry.assignedCluster, clusterMetadata.get());
		}
	}

	// TODO: more efficient
	std::map<ClusterName, DataClusterMetadata> clusters =
	    wait(listClustersTransaction(tr, ""_sr, "\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS));

	for (auto c : clusters) {
		if (!creatingTenantGroup || c.second.entry.hasCapacity()) {
			if (creatingTenantGroup) {
				++c.second.entry.allocated.numTenantGroups;
				updateClusterMetadata(tr, c.first, Optional<ClusterConnectionString>(), c.second.entry);
				if (tenantEntry.tenantGroup.present()) {
					tr->set(tenantGroupMetadataKeys.begin.withSuffix(tenantEntry.tenantGroup.get()),
					        TenantGroupEntry(c.first).encode());
				}
			}
			return c;
		}
	}

	throw metacluster_no_capacity();
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

			std::pair<ClusterName, DataClusterMetadata> assignment = wait(assignTenant(assignTr, tenantEntry));
			tenantEntry.assignedCluster = assignment.first;
			clusterMetadata = assignment.second;

			state typename DB::TransactionT::template FutureT<Optional<Value>> lastIdFuture =
			    assignTr->get(tenantLastIdKey);
			Optional<Value> lastIdVal = wait(safeThreadFutureToFuture(lastIdFuture));
			tenantEntry.id = lastIdVal.present() ? TenantMapEntry::prefixToId(lastIdVal.get()) + 1 : 0;

			std::pair<Optional<TenantMapEntry>, bool> result = wait(
			    TenantAPI::createTenantTransaction(assignTr, name, tenantEntry, ClusterType::METACLUSTER_MANAGEMENT));

			// The management cluster doesn't use tombstones, so we should always get back an entry
			ASSERT(result.first.present());
			createdTenant = result.first.get();

			if (!result.second) {
				if (!result.first.get().matchesConfiguration(tenantEntry) ||
				    result.first.get().tenantState != TenantState::REGISTERING) {
					throw tenant_already_exists();
				} else if (tenantEntry.assignedCluster != createdTenant.assignedCluster) {
					if (!result.first.get().assignedCluster.present()) {
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
				assignTr->set(getDataClusterTenantIndexKey(createdTenant.assignedCluster.get(), name), ""_sr);
				if (tenantEntry.tenantGroup.present()) {
					assignTr->set(getDataClusterTenantGroupIndexKey(createdTenant.assignedCluster.get(),
					                                                tenantEntry.tenantGroup.get()),
					              ""_sr);
				}

				assignTr->set(tenantLastIdKey, TenantMapEntry::idToPrefix(tenantEntry.id));
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
			Optional<TenantMapEntry> managementEntry = wait(TenantAPI::tryGetTenantTransaction(finalizeTr, name));
			if (!managementEntry.present()) {
				throw tenant_removed();
			} else if (managementEntry.get().id != createdTenant.id) {
				throw tenant_already_exists();
			}

			if (managementEntry.get().tenantState == TenantState::REGISTERING) {
				TenantMapEntry updatedEntry = managementEntry.get();
				updatedEntry.tenantState = TenantState::READY;
				TenantAPI::configureTenantTransaction(finalizeTr, name, updatedEntry);
				wait(buggifiedCommit(finalizeTr, BUGGIFY));
			}

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(finalizeTr->onError(e)));
		}
	}

	return Void();
}

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, TenantName name) {
	state int64_t tenantId;
	state DataClusterMetadata clusterMetadata;
	state bool alreadyRemoving = false;

	// Step 1: get the assigned location of the tenant
	state Reference<typename DB::TransactionT> managementTr = db->createTransaction();
	loop {
		try {
			managementTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state Optional<TenantMapEntry> tenantEntry1 = wait(TenantAPI::tryGetTenantTransaction(managementTr, name));
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
				TenantAPI::configureTenantTransaction(managementTr, name, updatedEntry);
				wait(TenantAPI::deleteTenantTransaction(
				    managementTr, name, ClusterType::METACLUSTER_MANAGEMENT, tenantId));
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
				state Optional<TenantMapEntry> tenantEntry2 =
				    wait(TenantAPI::tryGetTenantTransaction(managementTr, name));
				if (!tenantEntry2.present() || tenantEntry2.get().id != tenantId) {
					// The tenant must have been removed simultaneously
					return Void();
				}

				if (tenantEntry2.get().tenantState != TenantState::REMOVING) {
					TenantMapEntry updatedEntry = tenantEntry2.get();
					updatedEntry.tenantState = TenantState::REMOVING;
					TenantAPI::configureTenantTransaction(managementTr, name, updatedEntry);
					wait(buggifiedCommit(managementTr, BUGGIFY));
				}

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(managementTr->onError(e)));
			}
		}
	}

	// Step 4: remove the tenant from the data cluster
	wait(TenantAPI::deleteTenant(dataClusterDb, name, ClusterType::METACLUSTER_DATA, tenantId));

	// Step 5: delete the tenant from the management cluster
	managementTr = db->createTransaction();
	loop {
		try {
			managementTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state Optional<TenantMapEntry> tenantEntry3 = wait(TenantAPI::tryGetTenantTransaction(managementTr, name));

			if (!tenantEntry3.present() || tenantEntry3.get().id != tenantId) {
				return Void();
			}

			wait(TenantAPI::deleteTenantTransaction(managementTr, name, ClusterType::METACLUSTER_MANAGEMENT, tenantId));

			if (tenantEntry3.get().assignedCluster.present()) {
				state typename DB::TransactionT::template FutureT<RangeResult> tenantGroupIndexFuture;
				if (tenantEntry3.get().tenantGroup.present()) {
					tenantGroupIndexFuture =
					    managementTr->getRange(prefixRange(TenantAPI::getTenantGroupIndexKey(
					                               tenantEntry3.get().tenantGroup.get(), Optional<TenantNameRef>())),
					                           1);
				}

				state Optional<DataClusterMetadata> finalClusterMetadata =
				    wait(tryGetClusterTransaction(managementTr, tenantEntry3.get().assignedCluster.get()));

				state DataClusterEntry updatedEntry = finalClusterMetadata.get().entry;
				state bool decrementTenantGroupCount =
				    finalClusterMetadata.present() && !tenantEntry3.get().tenantGroup.present();

				if (finalClusterMetadata.present()) {
					managementTr->clear(getDataClusterTenantIndexKey(tenantEntry3.get().assignedCluster.get(), name),
					                    ""_sr);

					if (tenantEntry3.get().tenantGroup.present()) {
						managementTr->clear(getDataClusterTenantGroupIndexKey(tenantEntry3.get().assignedCluster.get(),
						                                                      tenantEntry3.get().tenantGroup.get()),

						                    ""_sr);
						RangeResult result = wait(safeThreadFutureToFuture(tenantGroupIndexFuture));
						if (result.size() == 0) {
							managementTr->clear(
							    tenantGroupMetadataKeys.begin.withSuffix(tenantEntry3.get().tenantGroup.get()));
							decrementTenantGroupCount = true;
						}
					}
				}
				if (decrementTenantGroupCount) {
					--updatedEntry.allocated.numTenantGroups;
					updateClusterMetadata(managementTr,
					                      tenantEntry3.get().assignedCluster.get(),
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
}; // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif