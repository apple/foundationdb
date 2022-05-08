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
	DataClusterEntry entry;
	ClusterConnectionString connectionString;

	DataClusterMetadata() = default;
	DataClusterMetadata(DataClusterEntry const& entry, ClusterConnectionString const& connectionString)
	  : entry(entry), connectionString(connectionString) {}
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
		    decodeDataClusterEntry(metadata.get()), ClusterConnectionString(connectionString.get().toString())));
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
void updateClusterMetadataTransaction(Transaction tr,
                                      ClusterNameRef name,
                                      Optional<std::string> updatedConnectionString,
                                      Optional<DataClusterEntry> updatedEntry) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	if (updatedEntry.present()) {
		tr->set(name.withPrefix(dataClusterMetadataPrefix), encodeDataClusterEntry(updatedEntry.get()));
	}
	if (updatedConnectionString.present()) {
		tr->set(name.withPrefix(dataClusterConnectionRecordPrefix), updatedConnectionString.get());
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
	tr->set(dataClusterMetadataKey, encodeDataClusterEntry(entry));
	tr->set(dataClusterConnectionRecordKey, connectionString);

	return Void();
}

ACTOR template <class Transaction>
Future<Void> dataClusterRegister(Transaction tr, ClusterNameRef name) {
	state Future<std::map<TenantName, TenantMapEntry>> existingTenantsFuture =
	    ManagementAPI::listTenantsTransaction(tr, ""_sr, "\xff\xff"_sr, 1);
	state ThreadFuture<RangeResult> existingDataFuture = tr->getRange(normalKeys, 1);

	std::map<TenantName, TenantMapEntry> existingTenants = wait(safeThreadFutureToFuture(existingTenantsFuture));
	if (!existingTenants.empty()) {
		TraceEvent(SevWarn, "CannotRegisterClusterWithTenants").detail("ClusterName", name);
		throw register_nonempty_cluster();
	}

	RangeResult existingData = wait(safeThreadFutureToFuture(existingDataFuture));
	if (!existingData.empty()) {
		TraceEvent(SevWarn, "CannotRegisterClusterWithData").detail("ClusterName", name);
		throw register_nonempty_cluster();
	}

	// TODO: change config to subordinate cluster
	return Void();
}

ACTOR template <class Transaction>
Future<Void> registerClusterTransaction(Transaction tr,
                                        ClusterNameRef name,
                                        std::string connectionString,
                                        DataClusterEntry entry) {
	if (name.startsWith("\xff"_sr)) {
		throw invalid_cluster_name();
	}

	state Reference<IDatabase> dbToRegister =
	    MultiVersionApi::api->createDatabase(makeReference<ClusterConnectionMemoryRecord>(connectionString));

	Reference<ITransaction> registerTr = dbToRegister->createTransaction();

	registerTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	registerTr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	// TODO: use the special key-space rather than running the logic ourselves

	// registerTr->set("\xff\xff/metacluster/management/data_cluster/register"_sr, ""_sr);
	wait(dataClusterRegister(registerTr, name));

	// Once the data cluster is configured, we can add it to the metacluster
	tr->set(name.withPrefix("\xff\xff/metacluster_internal/management_cluster/data_cluster/map/"_sr), connectionString);
	tr->set(
	    name.withPrefix(
	        "\xff\xff/metacluster_internal/management_cluster/data_cluster/configure/capacity.num_tenant_groups/"_sr),
	    format("%d", entry.capacity.numTenantGroups));
	return Void();
}

ACTOR template <class DB>
Future<Void> registerCluster(Reference<DB> db, ClusterName name, std::string connectionString, DataClusterEntry entry) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool firstTry = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);

			if (firstTry) {
				Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
				if (metadata.present()) {
					throw cluster_already_exists();
				}

				firstTry = false;
			}

			wait(registerClusterTransaction(tr, name, connectionString, entry));

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
			    .detail("ConnectionString", connectionString);

			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
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
Future<Void> removeClusterTransaction(Transaction tr, ClusterNameRef name) {
	state Key dataClusterMetadataKey = name.withPrefix(dataClusterMetadataPrefix);
	state Key dataClusterConnectionRecordKey = name.withPrefix(dataClusterConnectionRecordPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
	if (!metadata.present()) {
		return Void();
	}

	// TODO: verify tenant map for cluster is empty

	tr->clear(dataClusterMetadataKey);
	tr->clear(dataClusterConnectionRecordKey);

	return Void();
}

ACTOR template <class DB>
Future<Void> removeCluster(Reference<DB> db, ClusterName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool firstTry = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			if (firstTry) {
				Optional<DataClusterMetadata> metadata = wait(tryGetClusterTransaction(tr, name));
				if (!metadata.present()) {
					throw cluster_not_found();
				}

				firstTry = false;
			}

			wait(removeClusterTransaction(tr, name));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("RemovedDataCluster").detail("Name", name).detail("Version", tr->getCommittedVersion());
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
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
		clusters[metadata[i].key.removePrefix(dataClusterMetadataPrefix)] = DataClusterMetadata(
		    decodeDataClusterEntry(metadata[i].value), ClusterConnectionString(connectionStrings[i].value.toString()));
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