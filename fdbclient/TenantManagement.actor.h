/*
 * TenantManagement.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANT_MANAGEMENT_ACTOR_G_H)
#define FDBCLIENT_TENANT_MANAGEMENT_ACTOR_G_H
#include "fdbclient/TenantManagement.actor.g.h"
#elif !defined(FDBCLIENT_TENANT_MANAGEMENT_ACTOR_H)
#define FDBCLIENT_TENANT_MANAGEMENT_ACTOR_H

#include <string>
#include <map>
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/SystemData.h"
#include "flow/actorcompiler.h" // has to be last include

namespace ManagementAPI {

enum class TenantOperationType { STANDALONE_CLUSTER, MANAGEMENT_CLUSTER, DATA_CLUSTER };

ACTOR template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantFuture = tr->get(tenantMapKey);
	Optional<Value> val = wait(safeThreadFutureToFuture(tenantFuture));
	return val.map<TenantMapEntry>([](Optional<Value> v) { return decodeTenantEntry(v.get()); });
}

ACTOR template <class DB>
Future<Optional<TenantMapEntry>> tryGetTenant(Reference<DB> db, TenantName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
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

bool checkTenantMode(Optional<Value> tenantModeValue, bool isDataCluster, TenantOperationType operationType);

Key getTenantGroupIndexKey(TenantGroupNameRef tenantGroup, Optional<TenantNameRef> tenant);

// Creates a tenant with the given name. If the tenant already exists, an empty optional will be returned.
ACTOR template <class Transaction>
Future<std::pair<TenantMapEntry, bool>> createTenantTransaction(
    Transaction tr,
    TenantNameRef name,
    TenantMapEntry tenantEntry = TenantMapEntry(),
    TenantOperationType operationType = TenantOperationType::STANDALONE_CLUSTER) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	state bool generateId = operationType != TenantOperationType::DATA_CLUSTER;
	state bool allowSubspace = operationType == TenantOperationType::STANDALONE_CLUSTER;

	ASSERT(operationType != TenantOperationType::MANAGEMENT_CLUSTER || tenantEntry.assignedCluster.present());

	if (name.startsWith("\xff"_sr)) {
		throw invalid_tenant_name();
	}

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<Optional<TenantMapEntry>> existingEntryFuture = tryGetTenantTransaction(tr, name);
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantDataPrefixFuture;
	if (allowSubspace) {
		tenantDataPrefixFuture = tr->get(tenantDataPrefixKey);
	}
	state typename transaction_future_type<Transaction, Optional<Value>>::type lastIdFuture;
	if (generateId) {
		lastIdFuture = tr->get(tenantLastIdKey);
	}
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));
	state typename transaction_future_type<Transaction, Optional<Value>>::type metaclusterRegistrationFuture =
	    tr->get(dataClusterRegistrationKey);

	state Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tenantModeFuture));
	Optional<Value> metaclusterRegistration = wait(safeThreadFutureToFuture(metaclusterRegistrationFuture));

	if (!checkTenantMode(tenantMode, metaclusterRegistration.present(), operationType)) {
		throw tenants_disabled();
	}

	Optional<TenantMapEntry> existingEntry = wait(existingEntryFuture);
	if (existingEntry.present()) {
		return std::make_pair(existingEntry.get(), false);
	}

	if (generateId) {
		state Optional<Value> lastIdVal = wait(safeThreadFutureToFuture(lastIdFuture));
		tenantEntry.id = lastIdVal.present() ? TenantMapEntry::prefixToId(lastIdVal.get()) + 1 : 0;
		tr->set(tenantLastIdKey, TenantMapEntry::idToPrefix(tenantEntry.id));
	}

	if (allowSubspace) {
		Optional<Value> tenantDataPrefix = wait(safeThreadFutureToFuture(tenantDataPrefixFuture));
		if (tenantDataPrefix.present() &&
		    tenantDataPrefix.get().size() + TenantMapEntry::ROOT_PREFIX_SIZE > CLIENT_KNOBS->TENANT_PREFIX_SIZE_LIMIT) {
			TraceEvent(SevWarnAlways, "TenantPrefixTooLarge")
			    .detail("TenantSubspace", tenantDataPrefix.get())
			    .detail("TenantSubspaceLength", tenantDataPrefix.get().size())
			    .detail("RootPrefixLength", TenantMapEntry::ROOT_PREFIX_SIZE)
			    .detail("MaxTenantPrefixSize", CLIENT_KNOBS->TENANT_PREFIX_SIZE_LIMIT);

			throw client_invalid_operation();
		}
		tenantEntry.setSubspace(tenantDataPrefix.present() ? (KeyRef)tenantDataPrefix.get() : ""_sr);
	} else {
		tenantEntry.setSubspace(""_sr);
	}

	if (operationType != TenantOperationType::MANAGEMENT_CLUSTER) {
		state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
		    tr->getRange(prefixRange(tenantEntry.prefix), 1);

		RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
		if (!contents.empty()) {
			throw tenant_prefix_allocator_conflict();
		}

		tenantEntry.tenantState = TenantState::READY;
	} else {
		tenantEntry.tenantState = TenantState::REGISTERING;
	}

	// We don't store some metadata in the tenant entries on data clusters
	if (operationType == TenantOperationType::DATA_CLUSTER) {
		tenantEntry.assignedCluster = Optional<ClusterName>();
	}

	tr->set(tenantMapKey, encodeTenantEntry(tenantEntry));
	if (tenantEntry.tenantGroup.present()) {
		tr->set(getTenantGroupIndexKey(tenantEntry.tenantGroup.get(), name), ""_sr);
	}
	return std::make_pair(tenantEntry, true);
}

ACTOR template <class DB>
Future<TenantMapEntry> createTenant(Reference<DB> db,
                                    TenantName name,
                                    TenantMapEntry tenantEntry = TenantMapEntry(),
                                    TenantOperationType operationType = TenantOperationType::STANDALONE_CLUSTER) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool checkExistence = operationType != TenantOperationType::DATA_CLUSTER;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			if (checkExistence) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (entry.present()) {
					throw tenant_already_exists();
				}

				checkExistence = false;
			}

			state std::pair<TenantMapEntry, bool> newTenant =
			    wait(createTenantTransaction(tr, name, tenantEntry, operationType));

			if (newTenant.second) {
				wait(buggifiedCommit(tr, BUGGIFY));

				TraceEvent("CreatedTenant")
				    .detail("Tenant", name)
				    .detail("TenantId", newTenant.first.id)
				    .detail("Prefix", newTenant.first.prefix)
				    .detail("TenantGroup", tenantEntry.tenantGroup)
				    .detail("Version", tr->getCommittedVersion());
			}

			return newTenant.first;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<Void> deleteTenantTransaction(Transaction tr,
                                     TenantNameRef name,
                                     TenantOperationType operationType = TenantOperationType::STANDALONE_CLUSTER) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));
	state typename transaction_future_type<Transaction, Optional<Value>>::type metaclusterRegistrationFuture =
	    tr->get(dataClusterRegistrationKey);
	state Future<Optional<TenantMapEntry>> tenantEntryFuture = tryGetTenantTransaction(tr, name);

	state Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tenantModeFuture));
	Optional<Value> metaclusterRegistration = wait(safeThreadFutureToFuture(metaclusterRegistrationFuture));

	if (!checkTenantMode(tenantMode, metaclusterRegistration.present(), operationType)) {
		throw tenants_disabled();
	}

	state Optional<TenantMapEntry> tenantEntry = wait(tenantEntryFuture);
	if (!tenantEntry.present()) {
		return Void();
	}

	if (operationType == TenantOperationType::MANAGEMENT_CLUSTER &&
	    tenantEntry.get().tenantState != TenantState::REMOVING) {
		// TODO: better error
		throw operation_failed();
	}

	state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
	    tr->getRange(prefixRange(tenantEntry.get().prefix), 1);

	RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
	if (!contents.empty()) {
		throw tenant_not_empty();
	}

	tr->clear(tenantMapKey);
	if (tenantEntry.get().tenantGroup.present()) {
		tr->clear(getTenantGroupIndexKey(tenantEntry.get().tenantGroup.get(), name));
	}

	return Void();
}

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db,
                          TenantName name,
                          TenantOperationType operationType = TenantOperationType::STANDALONE_CLUSTER) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool checkExistence = operationType == TenantOperationType::STANDALONE_CLUSTER;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			if (checkExistence) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (!entry.present()) {
					throw tenant_not_found();
				}

				checkExistence = false;
			}

			wait(deleteTenantTransaction(tr, name, operationType));
			wait(buggifiedCommit(tr, BUGGIFY));

			TraceEvent("DeletedTenant").detail("Tenant", name).detail("Version", tr->getCommittedVersion());
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// This should only be called from a transaction that has already confirmed that the cluster entry
// is present. The updatedEntry should use the existing entry and modify only those fields that need
// to be changed.
template <class Transaction>
void configureTenantTransaction(Transaction tr, TenantNameRef tenantName, TenantMapEntry tenantEntry) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->set(tenantName.withPrefix(tenantMapPrefix), encodeTenantEntry(tenantEntry));
}

ACTOR template <class Transaction>
Future<std::map<TenantName, TenantMapEntry>> listTenantsTransaction(Transaction tr,
                                                                    TenantNameRef begin,
                                                                    TenantNameRef end,
                                                                    int limit) {
	state KeyRange range = KeyRangeRef(begin, end).withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state typename transaction_future_type<Transaction, RangeResult>::type listFuture =
	    tr->getRange(firstGreaterOrEqual(range.begin), firstGreaterOrEqual(range.end), limit);
	RangeResult results = wait(safeThreadFutureToFuture(listFuture));

	std::map<TenantName, TenantMapEntry> tenants;
	for (auto kv : results) {
		tenants[kv.key.removePrefix(tenantMapPrefix)] = decodeTenantEntry(kv.value);
	}

	return tenants;
}

ACTOR template <class DB>
Future<std::map<TenantName, TenantMapEntry>> listTenants(Reference<DB> db,
                                                         TenantName begin,
                                                         TenantName end,
                                                         int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			std::map<TenantName, TenantMapEntry> tenants = wait(listTenantsTransaction(tr, begin, end, limit));
			return tenants;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}
} // namespace ManagementAPI

#include "flow/unactorcompiler.h"
#endif
