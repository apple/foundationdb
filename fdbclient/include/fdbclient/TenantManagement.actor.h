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
#include "flow/IRandom.h"
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

template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return TenantMetadata::tenantMap.get(tr, name);
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
Future<Void> checkTenantMode(Transaction tr) {
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));

	Optional<Value> tenantModeValue = wait(safeThreadFutureToFuture(tenantModeFuture));

	TenantMode tenantMode = TenantMode::fromValue(tenantModeValue.castTo<ValueRef>());
	if (tenantMode == TenantMode::DISABLED) {
		throw tenants_disabled();
	}

	return Void();
}

// Creates a tenant with the given name. If the tenant already exists, the boolean return parameter will be false
// and the existing entry will be returned. If the tenant cannot be created, then the optional will be empty.
ACTOR template <class Transaction>
Future<std::pair<Optional<TenantMapEntry>, bool>> createTenantTransaction(Transaction tr,
                                                                          TenantNameRef name,
                                                                          TenantMapEntry tenantEntry) {
	ASSERT(tenantEntry.id >= 0);

	if (name.startsWith("\xff"_sr)) {
		throw invalid_tenant_name();
	}

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Optional<TenantMapEntry>> existingEntryFuture = tryGetTenantTransaction(tr, name);
	wait(checkTenantMode(tr));

	Optional<TenantMapEntry> existingEntry = wait(existingEntryFuture);
	if (existingEntry.present()) {
		return std::make_pair(existingEntry.get(), false);
	}

	state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
	    tr->getRange(prefixRange(tenantEntry.prefix), 1);

	RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
	if (!contents.empty()) {
		throw tenant_prefix_allocator_conflict();
	}

	tenantEntry.tenantState = TenantState::READY;
	TenantMetadata::tenantMap.set(tr, name, tenantEntry);

	return std::make_pair(tenantEntry, true);
}

ACTOR template <class Transaction>
Future<int64_t> getNextTenantId(Transaction tr) {
	Optional<int64_t> lastId = wait(TenantMetadata::lastTenantId.get(tr));
	int64_t tenantId = lastId.orDefault(-1) + 1;
	if (BUGGIFY) {
		tenantId += deterministicRandom()->randomSkewedUInt32(1, 1e9);
	}
	return tenantId;
}

ACTOR template <class DB>
Future<Optional<TenantMapEntry>> createTenant(Reference<DB> db,
                                              TenantName name,
                                              TenantMapEntry tenantEntry = TenantMapEntry()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool checkExistence = true;
	state bool generateTenantId = tenantEntry.id < 0;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state Future<int64_t> tenantIdFuture;
			if (generateTenantId) {
				tenantIdFuture = getNextTenantId(tr);
			}

			if (checkExistence) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (entry.present()) {
					throw tenant_already_exists();
				}

				checkExistence = false;
			}

			if (generateTenantId) {
				int64_t tenantId = wait(tenantIdFuture);
				tenantEntry.setId(tenantId);
				TenantMetadata::lastTenantId.set(tr, tenantId);
			}

			state std::pair<Optional<TenantMapEntry>, bool> newTenant =
			    wait(createTenantTransaction(tr, name, tenantEntry));

			if (newTenant.second) {
				ASSERT(newTenant.first.present());
				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

				TraceEvent("CreatedTenant")
				    .detail("Tenant", name)
				    .detail("TenantId", newTenant.first.get().id)
				    .detail("Prefix", newTenant.first.get().prefix)
				    .detail("Version", tr->getCommittedVersion());
			}

			return newTenant.first;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// Deletes the tenant with the given name. If tenantId is specified, the tenant being deleted must also have the same
// ID. If no matching tenant is found, this function returns without deleting anything. This behavior allows the
// function to be used idempotently: if the transaction is retried after having succeeded, it will see that the tenant
// is absent (or optionally created with a new ID) and do nothing.
ACTOR template <class Transaction>
Future<Void> deleteTenantTransaction(Transaction tr,
                                     TenantNameRef name,
                                     Optional<int64_t> tenantId = Optional<int64_t>()) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Optional<TenantMapEntry>> tenantEntryFuture = tryGetTenantTransaction(tr, name);
	wait(checkTenantMode(tr));

	state Optional<TenantMapEntry> tenantEntry = wait(tenantEntryFuture);
	if (tenantEntry.present() && (!tenantId.present() || tenantEntry.get().id == tenantId.get())) {
		state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
		    tr->getRange(prefixRange(tenantEntry.get().prefix), 1);

		RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
		if (!contents.empty()) {
			throw tenant_not_empty();
		}

		TenantMetadata::tenantMap.erase(tr, name);
	}

	return Void();
}

// Deletes the tenant with the given name. If tenantId is specified, the tenant being deleted must also have the same
// ID.
ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, TenantName name, Optional<int64_t> tenantId = Optional<int64_t>()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool checkExistence = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			if (checkExistence) {
				TenantMapEntry entry = wait(getTenantTransaction(tr, name));

				// If an ID wasn't specified, use the current ID. This way we cannot inadvertently delete
				// multiple tenants if this transaction retries.
				if (!tenantId.present()) {
					tenantId = entry.id;
				}

				checkExistence = false;
			}

			wait(deleteTenantTransaction(tr, name, tenantId));
			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

			TraceEvent("DeletedTenant").detail("Tenant", name).detail("Version", tr->getCommittedVersion());
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantsTransaction(Transaction tr,
                                                                                  TenantNameRef begin,
                                                                                  TenantNameRef end,
                                                                                  int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> results =
	    wait(TenantMetadata::tenantMap.getRange(tr, begin, end, limit));

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

ACTOR template <class DB>
Future<Void> renameTenant(Reference<DB> db, TenantName oldName, TenantName newName) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

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

			TenantMetadata::tenantMap.erase(tr, oldName);
			TenantMetadata::tenantMap.set(tr, newName, oldEntry.get());

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
