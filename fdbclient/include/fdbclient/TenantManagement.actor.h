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

namespace TenantAPI {

Key getTenantGroupIndexKey(TenantGroupNameRef tenantGroup, Optional<TenantNameRef> tenant);

ACTOR template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantFuture = tr->get(tenantMapKey);
	Optional<Value> val = wait(safeThreadFutureToFuture(tenantFuture));
	return val.map<TenantMapEntry>([](Optional<Value> v) { return TenantMapEntry::decode(v.get()); });
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

// Creates a tenant with the given name. If the tenant already exists, an empty optional will be returned.
// The caller must enforce that the tenant ID be unique from all current and past tenants, and it must also be unique
// from all other tenants created in the same transaction.
ACTOR template <class Transaction>
Future<std::pair<TenantMapEntry, bool>> createTenantTransaction(Transaction tr,
                                                                TenantNameRef name,
                                                                TenantMapEntry tenantEntry) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	ASSERT(tenantEntry.id >= 0);

	if (name.startsWith("\xff"_sr)) {
		throw invalid_tenant_name();
	}

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Optional<TenantMapEntry>> existingEntryFuture = tryGetTenantTransaction(tr, name);
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantDataPrefixFuture =
	    tr->get(tenantDataPrefixKey);
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));

	Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tenantModeFuture));

	if (!tenantMode.present() || tenantMode.get() == StringRef(format("%d", TenantMode::DISABLED))) {
		throw tenants_disabled();
	}

	Optional<TenantMapEntry> existingEntry = wait(existingEntryFuture);
	if (existingEntry.present()) {
		return std::make_pair(existingEntry.get(), false);
	}

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

	tenantEntry.setSubspace(tenantDataPrefix.orDefault(""_sr));

	state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
	    tr->getRange(prefixRange(tenantEntry.prefix), 1);
	RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
	if (!contents.empty()) {
		throw tenant_prefix_allocator_conflict();
	}

	tr->set(tenantMapKey, tenantEntry.encode());
	if (tenantEntry.tenantGroup.present()) {
		tr->set(getTenantGroupIndexKey(tenantEntry.tenantGroup.get(), name), ""_sr);
	}

	return std::make_pair(tenantEntry, true);
}

ACTOR template <class DB>
Future<TenantMapEntry> createTenant(Reference<DB> db, TenantName name, TenantMapEntry tenantEntry = TenantMapEntry()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool firstTry = true;
	state bool generateTenantId = tenantEntry.id < 0;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state typename DB::TransactionT::template FutureT<Optional<Value>> lastIdFuture;
			if (generateTenantId) {
				lastIdFuture = tr->get(tenantLastIdKey);
			}

			if (firstTry) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (entry.present()) {
					throw tenant_already_exists();
				}

				firstTry = false;
			}

			if (generateTenantId) {
				Optional<Value> lastIdVal = wait(safeThreadFutureToFuture(lastIdFuture));
				tenantEntry.id = lastIdVal.present() ? TenantMapEntry::prefixToId(lastIdVal.get()) + 1 : 0;
				tr->set(tenantLastIdKey, TenantMapEntry::idToPrefix(tenantEntry.id));
			}

			state std::pair<TenantMapEntry, bool> newTenant = wait(createTenantTransaction(tr, name, tenantEntry));

			if (newTenant.second) {
				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

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
Future<Void> deleteTenantTransaction(Transaction tr, TenantNameRef name) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, name));
	if (!tenantEntry.present()) {
		return Void();
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
Future<Void> deleteTenant(Reference<DB> db, TenantName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool firstTry = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			if (firstTry) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (!entry.present()) {
					throw tenant_not_found();
				}

				firstTry = false;
			}

			wait(deleteTenantTransaction(tr, name));
			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

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
	tr->set(tenantName.withPrefix(tenantMapPrefix), tenantEntry.encode());
}

ACTOR template <class Transaction>
Future<std::map<TenantName, TenantMapEntry>> listTenantsTransaction(Transaction tr,
                                                                    TenantNameRef begin,
                                                                    TenantNameRef end,
                                                                    int limit) {
	state KeyRange range = KeyRangeRef(begin, end).withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state typename transaction_future_type<Transaction, RangeResult>::type listFuture =
	    tr->getRange(firstGreaterOrEqual(range.begin), firstGreaterOrEqual(range.end), limit);
	RangeResult results = wait(safeThreadFutureToFuture(listFuture));

	std::map<TenantName, TenantMapEntry> tenants;
	for (auto kv : results) {
		tenants[kv.key.removePrefix(tenantMapPrefix)] = TenantMapEntry::decode(kv.value);
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
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			std::map<TenantName, TenantMapEntry> tenants = wait(listTenantsTransaction(tr, begin, end, limit));
			return tenants;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<Void> renameTenantTransaction(Transaction tr, TenantNameRef oldName, TenantNameRef newName) {
	state Key oldNameKey = oldName.withPrefix(tenantMapPrefix);
	state Key newNameKey = newName.withPrefix(tenantMapPrefix);
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	state Optional<TenantMapEntry> oldEntry = wait(tryGetTenantTransaction(tr, oldName));
	tr->clear(oldNameKey);
	tr->set(newNameKey, oldEntry.get().encode());

	return Void();
}

ACTOR template <class DB>
Future<Void> renameTenant(Reference<DB> db, TenantName oldName, TenantName newName) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state Key oldNameKey = oldName.withPrefix(tenantMapPrefix);
	state Key newNameKey = newName.withPrefix(tenantMapPrefix);
	state bool firstTry = true;
	state int64_t id;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state Optional<TenantMapEntry> oldEntry;
			state Optional<TenantMapEntry> newEntry;
			wait(store(oldEntry, tryGetTenantTransaction(tr, oldName)) &&
			     store(newEntry, tryGetTenantTransaction(tr, newName)));
			if (firstTry) {
				if (!oldEntry.present()) {
					throw tenant_not_found();
				}
				if (newEntry.present()) {
					throw tenant_already_exists();
				}
				// Store the id we see when first reading this key
				id = oldEntry.get().id;

				firstTry = false;
			} else {
				// If we got commit_unknown_result, the rename may have already occurred.
				if (newEntry.present()) {
					int64_t checkId = newEntry.get().id;
					if (id == checkId) {
						ASSERT(!oldEntry.present() || oldEntry.get().id != id);
						return Void();
					}
					// If the new entry is present but does not match, then
					// the rename should fail, so we throw an error.
					throw tenant_already_exists();
				}
				if (!oldEntry.present()) {
					throw tenant_not_found();
				}
				int64_t checkId = oldEntry.get().id;
				// If the id has changed since we made our first attempt,
				// then it's possible we've already moved the tenant. Don't move it again.
				if (id != checkId) {
					throw tenant_not_found();
				}
			}
			wait(renameTenantTransaction(tr, oldName, newName));
			wait(safeThreadFutureToFuture(tr->commit()));
			TraceEvent("RenameTenantSuccess").detail("OldName", oldName).detail("NewName", newName);
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}
} // namespace TenantAPI

#include "flow/unactorcompiler.h"
#endif
