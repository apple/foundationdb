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
#include "fdbclient/VersionedMap.h"
#include "flow/flat_buffers.h"
#include "flow/actorcompiler.h" // has to be last include

struct DataClusterMetadata {
	constexpr static FileIdentifier file_identifier = 5573993;

	DataClusterEntry entry;
	ClusterConnectionString connectionString;

	DataClusterMetadata() = default;
	DataClusterMetadata(DataClusterEntry const& entry, ClusterConnectionString const& connectionString)
	  : entry(entry), connectionString(connectionString) {}

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withMetacluster())); }
	Value encode(Arena& arena) {
		return ObjectWriter::toValue(*this, IncludeVersion(ProtocolVersion::withMetacluster()), arena);
	}
	static DataClusterMetadata decode(ValueRef const& value) {
		DataClusterMetadata metadata;
		ObjectReader reader(value.begin(), IncludeVersion());
		reader.deserialize(metadata);
		return metadata;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connectionString, entry);
	}
};

FDB_DECLARE_BOOLEAN_PARAM(AddNewTenants);
FDB_DECLARE_BOOLEAN_PARAM(RemoveMissingTenants);

namespace MetaclusterAPI {

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> managementClusterTryGetCluster(Transaction tr, ClusterNameRef name) {
	state Key dataClusterMetadataKey = name.withPrefix(dataClusterMetadataPrefix);
	state Key dataClusterConnectionRecordKey = name.withPrefix(dataClusterConnectionRecordPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state typename transaction_future_type<Transaction, Optional<Value>>::type metadataFuture =
	    tr->get(dataClusterMetadataKey);
	state typename transaction_future_type<Transaction, Optional<Value>>::type connectionRecordFuture =
	    tr->get(dataClusterConnectionRecordKey);

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

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> tryGetClusterTransaction(Transaction tr, ClusterNameRef name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state typename transaction_future_type<Transaction, Optional<Value>>::type clusterEntryFuture =
	    tr->get("\xff\xff/metacluster_internal/management_cluster/data_cluster/map/"_sr.withSuffix(name));

	state Optional<Value> clusterEntry = wait(safeThreadFutureToFuture(clusterEntryFuture));

	if (clusterEntry.present()) {
		return DataClusterMetadata::decode(clusterEntry.get());
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
Future<Void> managementClusterUpdateClusterMetadata(Transaction tr, ClusterNameRef name, DataClusterMetadata metadata) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	// Check that the tenant exists
	Optional<DataClusterMetadata> existingMetadata = wait(managementClusterTryGetCluster(tr, name));
	if (!existingMetadata.present()) {
		throw cluster_not_found();
	}

	tr->set(name.withPrefix(dataClusterMetadataPrefix), metadata.entry.encode());
	tr->set(name.withPrefix(dataClusterConnectionRecordPrefix), metadata.connectionString.toString());

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
		tr->set(
		    name.withPrefix(
		        "\xff\xff/metacluster_internal/management_cluster/data_cluster/configure/capacity.num_tenant_groups/"_sr),
		    format("%d", updatedEntry.get().capacity.numTenantGroups));
	}
	if (updatedConnectionString.present()) {
		tr->set(name.withPrefix(
		            "\xff\xff/metacluster_internal/management_cluster/data_cluster/configure/connection_string/"_sr),
		        updatedConnectionString.get().toString());
	}
}

ACTOR template <class Transaction>
Future<Void> managementClusterRegister(Transaction tr,
                                       ClusterNameRef name,
                                       std::string connectionString,
                                       DataClusterEntry entry) {
	state Key dataClusterMetadataKey = name.withPrefix(dataClusterMetadataPrefix);
	state Key dataClusterConnectionRecordKey = name.withPrefix(dataClusterConnectionRecordPrefix);

	if (name.startsWith("\xff"_sr)) {
		throw invalid_cluster_name();
	}

	state Future<Optional<DataClusterMetadata>> dataClusterMetadataFuture = managementClusterTryGetCluster(tr, name);
	state typename transaction_future_type<Transaction, Optional<Value>>::type lastIdFuture =
	    tr->get(dataClusterLastIdKey);
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));

	Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tenantModeFuture));

	if (!tenantMode.present() || tenantMode.get() != StringRef(format("%d", TenantMode::MANAGEMENT))) {
		throw invalid_metacluster_operation();
	}

	Optional<DataClusterMetadata> dataClusterMetadata = wait(dataClusterMetadataFuture);
	if (dataClusterMetadata.present()) {
		return Void();
	}

	state Optional<Value> lastIdVal = wait(safeThreadFutureToFuture(lastIdFuture));

	entry.id = lastIdVal.present() ? DataClusterEntry::valueToId(lastIdVal.get()) + 1 : 0;
	entry.allocated = ClusterUsage();

	tr->set(dataClusterLastIdKey, DataClusterEntry::idToValue(entry.id));
	tr->set(dataClusterMetadataKey, entry.encode());
	tr->set(dataClusterConnectionRecordKey, connectionString);

	return Void();
}

ACTOR template <class Transaction>
Future<Void> dataClusterRegister(Transaction tr, ClusterNameRef name) {
	state Future<std::map<TenantName, TenantMapEntry>> existingTenantsFuture =
	    ManagementAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type existingDataFuture =
	    tr->getRange(normalKeys, 1);
	state typename transaction_future_type<Transaction, Optional<Value>>::type clusterNameFuture =
	    tr->get(dataClusterRegistrationKey);

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

	// TODO: this is not idempotent
	Optional<Value> storedClusterName = wait(safeThreadFutureToFuture(clusterNameFuture));
	if (storedClusterName.present()) {
		throw cluster_already_registered();
	}

	tr->set(dataClusterRegistrationKey, DataClusterRegistrationEntry(name).encode());

	std::vector<StringRef> tokens = { "tenant_mode=required"_sr };
	ConfigurationResult configResult =
	    wait(ManagementAPI::changeConfigTransaction(tr, tokens, Optional<ConfigureAutoResult>(), false, false));

	if (configResult != ConfigurationResult::SUCCESS) {
		TraceEvent(SevWarn, "CouldNotConfigureDataCluster")
		    .detail("Name", name)
		    .detail("ConfigurationResult", configResult);

		throw cluster_configuration_failure();
	}

	return Void();
}

ACTOR template <class DB>
Future<Void> registerCluster(Reference<DB> db,
                             ClusterName name,
                             ClusterConnectionString connectionString,
                             DataClusterEntry entry) {
	if (name.startsWith("\xff"_sr)) {
		throw invalid_cluster_name();
	}

	// Step 1: Configure the data cluster as a subordinate cluster
	state Reference<IDatabase> dataClusterDb =
	    MultiVersionApi::api->createDatabase(makeReference<ClusterConnectionMemoryRecord>(connectionString));

	state Reference<ITransaction> dataClusterTr = dataClusterDb->createTransaction();

	loop {
		try {
			dataClusterTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			dataClusterTr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);

			dataClusterTr->set("\xff\xff/metacluster_internal/data_cluster/data_cluster/register"_sr, name);

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(dataClusterTr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("ConfiguredDataCluster")
			    .detail("ClusterName", name)
			    .detail("Capacity", entry.capacity)
			    .detail("Version", dataClusterTr->getCommittedVersion())
			    .detail("ConnectionString", connectionString.toString());

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(dataClusterTr->onError(e)));
		}
	}

	// Step 2: Register the data cluster in the management cluster
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);

			tr->set(name.withPrefix("\xff\xff/metacluster_internal/management_cluster/data_cluster/map/"_sr),
			        connectionString.toString());
			tr->set(
			    name.withPrefix(
			        "\xff\xff/metacluster_internal/management_cluster/data_cluster/configure/capacity.num_tenant_groups/"_sr),
			    format("%d", entry.capacity.numTenantGroups));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("RegisteredDataCluster")
			    .detail("ClusterName", name)
			    .detail("Capacity", entry.capacity)
			    .detail("Version", tr->getCommittedVersion())
			    .detail("ConnectionString", connectionString.toString());

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
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

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("RestoredDataCluster")
			    .detail("ClusterName", name)
			    .detail("ClusterId", newCluster.present() ? newCluster.get().id : -1)
			    .detail("Version", tr->getCommittedVersion());

			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<Void> managementClusterRemove(Transaction tr, ClusterNameRef name) {
	state Key dataClusterMetadataKey = name.withPrefix(dataClusterMetadataPrefix);
	state Key dataClusterConnectionRecordKey = name.withPrefix(dataClusterConnectionRecordPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Optional<DataClusterMetadata> metadata = wait(managementClusterTryGetCluster(tr, name));
	if (!metadata.present()) {
		return Void();
	}

	// TODO: validate that no tenants are assigned to the target cluster

	tr->clear(dataClusterMetadataKey);
	tr->clear(dataClusterConnectionRecordKey);

	return Void();
}

template <class Transaction>
Future<Void> dataClusterRemove(Transaction tr) {
	tr->clear(dataClusterRegistrationKey);
	return Void();
}

ACTOR template <class DB>
Future<Void> removeCluster(Reference<DB> db, ClusterName name) {
	// Step 1: Remove the data cluster from the metacluster
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Optional<DataClusterMetadata> metadata;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			if (!metadata.present()) {
				DataClusterMetadata _metadata = wait(getClusterTransaction(tr, name));
				metadata = _metadata;
			}

			tr->clear(name.withPrefix("\xff\xff/metacluster_internal/management_cluster/data_cluster/map/"_sr));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("RemovedDataCluster").detail("Name", name).detail("Version", tr->getCommittedVersion());
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	// Step 2: Reconfigure data cluster and remove metadata.
	//         Note that this is best effort; if it fails the cluster will still have been removed.
	state Reference<IDatabase> dataClusterDb = MultiVersionApi::api->createDatabase(
	    makeReference<ClusterConnectionMemoryRecord>(metadata.get().connectionString));

	state Reference<ITransaction> dataClusterTr = dataClusterDb->createTransaction();
	loop {
		try {
			dataClusterTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			dataClusterTr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);

			dataClusterTr->set("\xff\xff/metacluster_internal/data_cluster/data_cluster/remove"_sr, ""_sr);

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(dataClusterTr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

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

ACTOR template <class DB>
Future<std::map<ClusterName, DataClusterMetadata>> listClusters(Reference<DB> db,
                                                                ClusterName begin,
                                                                ClusterName end,
                                                                int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state KeyRef prefix = "\xff\xff/metacluster_internal/management_cluster/data_cluster/map/"_sr;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state typename transaction_future_type<typename DB::TransactionT, RangeResult>::type listFuture =
			    tr->getRange(KeyRangeRef(begin, end).withPrefix(prefix), limit);

			RangeResult results = wait(safeThreadFutureToFuture(listFuture));
			std::map<ClusterName, DataClusterMetadata> clusters;

			for (auto result : results) {
				clusters[result.key.removePrefix(prefix)] = DataClusterMetadata::decode(result.value);
			}

			return clusters;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}
}; // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif