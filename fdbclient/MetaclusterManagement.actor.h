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
Future<Optional<DataClusterMetadata>> tryGetClusterTransaction(Transaction tr, ClusterNameRef name) {
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

// This should only be called from a transaction that has already confirmed that the cluster entry
// is present. The updatedEntry should use the existing entry and modify only those fields that need
// to be changed.
template <class Transaction>
void updateClusterMetadata(Transaction tr,
                           ClusterNameRef name,
                           Optional<ClusterConnectionString> updatedConnectionString,
                           Optional<DataClusterEntry> updatedEntry) {

	if (updatedEntry.present()) {
		tr->set(name.withPrefix(dataClusterMetadataPrefix), updatedEntry.get().encode());
	}
	if (updatedConnectionString.present()) {
		tr->set(name.withPrefix(dataClusterConnectionRecordPrefix), updatedConnectionString.get().toString());
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

	state Future<Optional<DataClusterMetadata>> dataClusterMetadataFuture = tryGetClusterTransaction(tr, name);
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));

	Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tenantModeFuture));

	if (!tenantMode.present() || tenantMode.get() != StringRef(format("%d", TenantMode::MANAGEMENT))) {
		throw invalid_metacluster_operation();
	}

	Optional<DataClusterMetadata> dataClusterMetadata = wait(dataClusterMetadataFuture);
	if (dataClusterMetadata.present()) {
		if (dataClusterMetadata.get().entry.registrationState == DataClusterEntry::RegistrationState::REGISTERING &&
		    dataClusterMetadata.get().connectionString.toString() == connectionString &&
		    dataClusterMetadata.get().entry.capacity == entry.capacity) {
			return Void();
		} else {
			throw cluster_already_exists();
		}
	}

	entry.allocated = ClusterUsage();

	tr->set(dataClusterMetadataKey, entry.encode());
	tr->set(dataClusterConnectionRecordKey, connectionString);

	return Void();
}

ACTOR template <class Transaction>
Future<Void> dataClusterRegister(Transaction tr, ClusterNameRef name, UID clusterId) {
	state Future<std::map<TenantName, TenantMapEntry>> existingTenantsFuture =
	    ManagementAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
	state typename transaction_future_type<Transaction, RangeResult>::type existingDataFuture =
	    tr->getRange(normalKeys, 1);
	state typename transaction_future_type<Transaction, Optional<Value>>::type clusterRegistrationFuture =
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

	Optional<Value> storedClusterRegistration = wait(safeThreadFutureToFuture(clusterRegistrationFuture));
	if (storedClusterRegistration.present()) {
		DataClusterRegistrationEntry existingRegistration =
		    DataClusterRegistrationEntry::decode(storedClusterRegistration.get());

		if (existingRegistration.name != name || existingRegistration.id != clusterId) {
			throw cluster_already_registered();
		} else {
			// We already successfully registered the cluster with these details, so there's nothing to do
			return Void();
		}
	}

	tr->set(dataClusterRegistrationKey, DataClusterRegistrationEntry(name, clusterId).encode());

	std::vector<StringRef> tokens = { "tenant_mode=required"_sr };

	// TODO: special keys?
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

ACTOR template <class Transaction>
Future<Void> finalizeRegistration(Transaction tr, ClusterNameRef name, UID clusterId) {
	state Optional<DataClusterMetadata> dataClusterMetadata = wait(tryGetClusterTransaction(tr, name));
	if (dataClusterMetadata.get().entry.id == clusterId &&
	    dataClusterMetadata.get().entry.registrationState == DataClusterEntry::RegistrationState::REGISTERING) {
		dataClusterMetadata.get().entry.registrationState = DataClusterEntry::RegistrationState::READY;
		updateClusterMetadata(tr, name, Optional<ClusterConnectionString>(), dataClusterMetadata.get().entry);
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

	entry.id = deterministicRandom()->randomUniqueID();

	// Step 1: Record that we are registering the new cluster
	state Reference<typename DB::TransactionT> registerTr = db->createTransaction();
	loop {
		try {
			registerTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(managementClusterRegister(registerTr, name, connectionString.toString(), entry));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(registerTr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

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

	// Step 2: Configure the data cluster as a subordinate cluster
	state Reference<IDatabase> dataClusterDb =
	    MultiVersionApi::api->createDatabase(makeReference<ClusterConnectionMemoryRecord>(connectionString));

	state Reference<ITransaction> dataClusterTr = dataClusterDb->createTransaction();

	try {
		loop {
			try {
				dataClusterTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(dataClusterRegister(dataClusterTr, name, entry.id));

				if (BUGGIFY) {
					throw commit_unknown_result();
				}

				wait(safeThreadFutureToFuture(dataClusterTr->commit()));

				if (BUGGIFY) {
					throw commit_unknown_result();
				}

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
	} catch (Error& e) {
		// TODO: Remove cluster if the parameters match and the cluster is in registering state
	}

	// Step 3: Record that the cluster is ready in the management cluster
	state Reference<typename DB::TransactionT> finalizeTr = db->createTransaction();
	loop {
		try {
			finalizeTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(finalizeRegistration(finalizeTr, name, entry.id));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(finalizeTr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("FinalizedDataCluster")
			    .detail("ClusterName", name)
			    .detail("ClusterID", entry.id)
			    .detail("Capacity", entry.capacity)
			    .detail("Version", finalizeTr->getCommittedVersion())
			    .detail("ConnectionString", connectionString.toString());

			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(finalizeTr->onError(e)));
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
			    .detail("ClusterId", newCluster.present() ? newCluster.get().id : UID())
			    .detail("Version", tr->getCommittedVersion());

			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> managementClusterRemove(Transaction tr, ClusterNameRef name, bool checkEmpty) {
	state Key dataClusterMetadataKey = name.withPrefix(dataClusterMetadataPrefix);
	state Key dataClusterConnectionRecordKey = name.withPrefix(dataClusterConnectionRecordPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
	if (!metadata.present()) {
		return metadata;
	}

	if (checkEmpty && metadata.get().entry.allocated.numTenantGroups > 0) {
		throw cluster_not_empty();
	}

	tr->clear(dataClusterMetadataKey);
	tr->clear(dataClusterConnectionRecordKey);

	return metadata;
}

template <class Transaction>
Future<Void> dataClusterRemove(Transaction tr) {
	tr->clear(dataClusterRegistrationKey);
	return Void();
}

ACTOR template <class DB>
Future<Void> removeCluster(Reference<DB> db, ClusterName name, bool forceRemove) {
	// Step 1: Remove the data cluster from the metacluster
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state DataClusterMetadata metadata;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			Optional<DataClusterMetadata> _metadata = wait(managementClusterRemove(tr, name, !forceRemove));
			if (!_metadata.present()) {
				return Void();
			}

			metadata = _metadata.get();

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
	state Reference<IDatabase> dataClusterDb =
	    MultiVersionApi::api->createDatabase(makeReference<ClusterConnectionMemoryRecord>(metadata.connectionString));

	state Reference<ITransaction> dataClusterTr = dataClusterDb->createTransaction();
	loop {
		try {
			dataClusterTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			wait(dataClusterRemove(dataClusterTr));

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
}; // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif