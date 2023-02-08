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
#include "fdbclient/ClientBooleanParams.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANT_MANAGEMENT_ACTOR_G_H)
#define FDBCLIENT_TENANT_MANAGEMENT_ACTOR_G_H
#include "fdbclient/TenantManagement.actor.g.h"
#elif !defined(FDBCLIENT_TENANT_MANAGEMENT_ACTOR_H)
#define FDBCLIENT_TENANT_MANAGEMENT_ACTOR_H

#include <string>
#include <map>
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/SystemData.h"
#include "flow/actorcompiler.h" // has to be last include

namespace TenantAPI {

template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, int64_t tenantId) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return TenantMetadata::tenantMap().get(tr, tenantId);
}

ACTOR template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<int64_t> tenantId = wait(TenantMetadata::tenantNameIndex().get(tr, name));
	if (tenantId.present()) {
		Optional<TenantMapEntry> entry = wait(TenantMetadata::tenantMap().get(tr, tenantId.get()));
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
Future<ClusterType> getClusterType(Transaction tr) {
	Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
	    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

	return metaclusterRegistration.present() ? metaclusterRegistration.get().clusterType : ClusterType::STANDALONE;
}

ACTOR template <class Transaction>
Future<Void> checkTenantMode(Transaction tr, ClusterType expectedClusterType) {
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr));

	state ClusterType actualClusterType = wait(getClusterType(tr));
	Optional<Value> tenantModeValue = wait(safeThreadFutureToFuture(tenantModeFuture));

	TenantMode tenantMode = TenantMode::fromValue(tenantModeValue.castTo<ValueRef>());
	if (actualClusterType != expectedClusterType) {
		throw invalid_metacluster_operation();
	} else if (actualClusterType == ClusterType::STANDALONE && tenantMode == TenantMode::DISABLED) {
		throw tenants_disabled();
	}

	return Void();
}

TenantMode tenantModeForClusterType(ClusterType clusterType, TenantMode tenantMode);
int64_t extractTenantIdFromMutation(MutationRef m);
int64_t extractTenantIdFromKeyRef(StringRef s);

// Returns true if the specified ID has already been deleted and false if not. If the ID is old enough
// that we no longer keep tombstones for it, an error is thrown.
ACTOR template <class Transaction>
Future<bool> checkTombstone(Transaction tr, int64_t id) {
	state Future<bool> tombstoneFuture = TenantMetadata::tenantTombstones().exists(tr, id);

	// If we are trying to create a tenant older than the oldest tombstones we still maintain, then we fail it
	// with an error.
	Optional<TenantTombstoneCleanupData> tombstoneCleanupData = wait(TenantMetadata::tombstoneCleanupData().get(tr));
	if (tombstoneCleanupData.present() && tombstoneCleanupData.get().tombstonesErasedThrough >= id) {
		throw tenant_creation_permanently_failed();
	}

	state bool hasTombstone = wait(tombstoneFuture);
	return hasTombstone;
}

// Creates a tenant. If the tenant already exists, the boolean return parameter will be false
// and the existing entry will be returned. If the tenant cannot be created, then the optional will be empty.
ACTOR template <class Transaction>
Future<std::pair<Optional<TenantMapEntry>, bool>>
createTenantTransaction(Transaction tr, TenantMapEntry tenantEntry, ClusterType clusterType = ClusterType::STANDALONE) {
	ASSERT(clusterType != ClusterType::METACLUSTER_MANAGEMENT);
	ASSERT(tenantEntry.id >= 0);

	if (tenantEntry.tenantName.startsWith("\xff"_sr)) {
		throw invalid_tenant_name();
	}
	if (tenantEntry.tenantGroup.present() && tenantEntry.tenantGroup.get().startsWith("\xff"_sr)) {
		throw invalid_tenant_group_name();
	}

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Optional<TenantMapEntry>> existingEntryFuture = tryGetTenantTransaction(tr, tenantEntry.tenantName);
	state Future<Void> tenantModeCheck = checkTenantMode(tr, clusterType);
	state Future<bool> tombstoneFuture =
	    (clusterType == ClusterType::STANDALONE) ? false : checkTombstone(tr, tenantEntry.id);
	state Future<Optional<TenantGroupEntry>> existingTenantGroupEntryFuture;
	if (tenantEntry.tenantGroup.present()) {
		existingTenantGroupEntryFuture = TenantMetadata::tenantGroupMap().get(tr, tenantEntry.tenantGroup.get());
	}

	wait(tenantModeCheck);
	Optional<TenantMapEntry> existingEntry = wait(existingEntryFuture);
	if (existingEntry.present()) {
		return std::make_pair(existingEntry.get(), false);
	}

	state bool hasTombstone = wait(tombstoneFuture);
	if (hasTombstone) {
		return std::make_pair(Optional<TenantMapEntry>(), false);
	}

	state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
	    tr->getRange(prefixRange(tenantEntry.prefix), 1);

	RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
	if (!contents.empty()) {
		throw tenant_prefix_allocator_conflict();
	}

	TenantMetadata::tenantMap().set(tr, tenantEntry.id, tenantEntry);
	TenantMetadata::tenantNameIndex().set(tr, tenantEntry.tenantName, tenantEntry.id);
	TenantMetadata::lastTenantModification().setVersionstamp(tr, Versionstamp(), 0);

	if (tenantEntry.tenantGroup.present()) {
		TenantMetadata::tenantGroupTenantIndex().insert(
		    tr, Tuple::makeTuple(tenantEntry.tenantGroup.get(), tenantEntry.id));

		// Create the tenant group associated with this tenant if it doesn't already exist
		Optional<TenantGroupEntry> existingTenantGroup = wait(existingTenantGroupEntryFuture);
		if (!existingTenantGroup.present()) {
			TenantMetadata::tenantGroupMap().set(tr, tenantEntry.tenantGroup.get(), TenantGroupEntry());
		}
	}

	// This is idempotent because we only add an entry to the tenant map if it isn't already there
	TenantMetadata::tenantCount().atomicOp(tr, 1, MutationRef::AddValue);

	// Read the tenant count after incrementing the counter so that simultaneous attempts to create
	// tenants in the same transaction are properly reflected.
	int64_t tenantCount = wait(TenantMetadata::tenantCount().getD(tr, Snapshot::False, 0));
	if (tenantCount > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
		throw cluster_no_capacity();
	}

	return std::make_pair(tenantEntry, true);
}

ACTOR template <class Transaction>
Future<int64_t> getNextTenantId(Transaction tr) {
	Optional<int64_t> lastId = wait(TenantMetadata::lastTenantId().get(tr));
	int64_t tenantId = lastId.orDefault(-1) + 1;
	if (BUGGIFY) {
		tenantId += deterministicRandom()->randomSkewedUInt32(1, 1e9);
	}
	return tenantId;
}

ACTOR template <class DB>
Future<Optional<TenantMapEntry>> createTenant(Reference<DB> db,
                                              TenantName name,
                                              TenantMapEntry tenantEntry = TenantMapEntry(),
                                              ClusterType clusterType = ClusterType::STANDALONE) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool checkExistence = clusterType != ClusterType::METACLUSTER_DATA;
	state bool generateTenantId = tenantEntry.id < 0;

	ASSERT(clusterType == ClusterType::STANDALONE || !generateTenantId);

	tenantEntry.tenantName = name;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state Future<int64_t> tenantIdFuture;
			if (generateTenantId) {
				tenantIdFuture = getNextTenantId(tr);
			}

			if (checkExistence) {
				Optional<int64_t> existingId = wait(TenantMetadata::tenantNameIndex().get(tr, name));
				if (existingId.present()) {
					throw tenant_already_exists();
				}

				checkExistence = false;
			}

			if (generateTenantId) {
				int64_t tenantId = wait(tenantIdFuture);
				tenantEntry.setId(tenantId);
				TenantMetadata::lastTenantId().set(tr, tenantId);
			}

			state std::pair<Optional<TenantMapEntry>, bool> newTenant =
			    wait(createTenantTransaction(tr, tenantEntry, clusterType));

			if (newTenant.second) {
				ASSERT(newTenant.first.present());
				wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

				TraceEvent("CreatedTenant")
				    .detail("Tenant", name)
				    .detail("TenantId", newTenant.first.get().id)
				    .detail("Prefix", newTenant.first.get().prefix)
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
Future<Void> markTenantTombstones(Transaction tr, int64_t tenantId) {
	// In data clusters, we store a tombstone
	state Future<KeyBackedRangeResult<int64_t>> latestTombstoneFuture =
	    TenantMetadata::tenantTombstones().getRange(tr, {}, {}, 1, Snapshot::False, Reverse::True);
	state Optional<TenantTombstoneCleanupData> cleanupData = wait(TenantMetadata::tombstoneCleanupData().get(tr));
	state Version transactionReadVersion = wait(safeThreadFutureToFuture(tr->getReadVersion()));

	// If it has been long enough since we last cleaned up the tenant tombstones, we do that first
	if (!cleanupData.present() || cleanupData.get().nextTombstoneEraseVersion <= transactionReadVersion) {
		state int64_t deleteThroughId = cleanupData.present() ? cleanupData.get().nextTombstoneEraseId : -1;
		// Delete all tombstones up through the one currently marked in the cleanup data
		if (deleteThroughId >= 0) {
			TenantMetadata::tenantTombstones().erase(tr, 0, deleteThroughId + 1);
		}

		KeyBackedRangeResult<int64_t> latestTombstone = wait(latestTombstoneFuture);
		int64_t nextDeleteThroughId = std::max(deleteThroughId, tenantId);
		if (!latestTombstone.results.empty()) {
			nextDeleteThroughId = std::max(nextDeleteThroughId, latestTombstone.results[0]);
		}

		// The next cleanup will happen at or after TENANT_TOMBSTONE_CLEANUP_INTERVAL seconds have elapsed and
		// will clean up tombstones through the most recently allocated ID.
		TenantTombstoneCleanupData updatedCleanupData;
		updatedCleanupData.tombstonesErasedThrough = deleteThroughId;
		updatedCleanupData.nextTombstoneEraseId = nextDeleteThroughId;
		updatedCleanupData.nextTombstoneEraseVersion =
		    transactionReadVersion +
		    CLIENT_KNOBS->TENANT_TOMBSTONE_CLEANUP_INTERVAL * CLIENT_KNOBS->VERSIONS_PER_SECOND;

		TenantMetadata::tombstoneCleanupData().set(tr, updatedCleanupData);

		// If the tenant being deleted is within the tombstone window, record the tombstone
		if (tenantId > updatedCleanupData.tombstonesErasedThrough) {
			TenantMetadata::tenantTombstones().insert(tr, tenantId);
		}
	} else if (tenantId > cleanupData.get().tombstonesErasedThrough) {
		// If the tenant being deleted is within the tombstone window, record the tombstone
		TenantMetadata::tenantTombstones().insert(tr, tenantId);
	}
	return Void();
}

// Deletes a tenant with the given ID. If no matching tenant is found, this function returns without deleting anything.
// This behavior allows the function to be used idempotently: if the transaction is retried after having succeeded, it
// will see that the tenant is absent and do nothing.
ACTOR template <class Transaction>
Future<Void> deleteTenantTransaction(Transaction tr,
                                     int64_t tenantId,
                                     ClusterType clusterType = ClusterType::STANDALONE) {
	ASSERT(tenantId != TenantInfo::INVALID_TENANT);
	ASSERT(clusterType != ClusterType::METACLUSTER_MANAGEMENT);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Optional<TenantMapEntry>> tenantEntryFuture = tryGetTenantTransaction(tr, tenantId);
	wait(checkTenantMode(tr, clusterType));

	state Optional<TenantMapEntry> tenantEntry = wait(tenantEntryFuture);
	if (tenantEntry.present()) {
		state typename transaction_future_type<Transaction, RangeResult>::type prefixRangeFuture =
		    tr->getRange(prefixRange(tenantEntry.get().prefix), 1);

		RangeResult contents = wait(safeThreadFutureToFuture(prefixRangeFuture));
		if (!contents.empty()) {
			throw tenant_not_empty();
		}

		// This is idempotent because we only erase an entry from the tenant map if it is present
		TenantMetadata::tenantMap().erase(tr, tenantId);
		TenantMetadata::tenantNameIndex().erase(tr, tenantEntry.get().tenantName);
		TenantMetadata::tenantCount().atomicOp(tr, -1, MutationRef::AddValue);
		TenantMetadata::lastTenantModification().setVersionstamp(tr, Versionstamp(), 0);

		if (tenantEntry.get().tenantGroup.present()) {
			TenantMetadata::tenantGroupTenantIndex().erase(
			    tr, Tuple::makeTuple(tenantEntry.get().tenantGroup.get(), tenantId));
			KeyBackedSet<Tuple>::RangeResultType tenantsInGroup =
			    wait(TenantMetadata::tenantGroupTenantIndex().getRange(
			        tr,
			        Tuple::makeTuple(tenantEntry.get().tenantGroup.get()),
			        Tuple::makeTuple(keyAfter(tenantEntry.get().tenantGroup.get())),
			        2));
			if (tenantsInGroup.results.empty() ||
			    (tenantsInGroup.results.size() == 1 && tenantsInGroup.results[0].getInt(1) == tenantId)) {
				TenantMetadata::tenantGroupMap().erase(tr, tenantEntry.get().tenantGroup.get());
			}
		}
	}

	if (clusterType == ClusterType::METACLUSTER_DATA) {
		wait(markTenantTombstones(tr, tenantId));
	}

	return Void();
}

// Deletes the tenant with the given name. If tenantId is specified, the tenant being deleted must also have the same
// ID.
ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db,
                          TenantName name,
                          Optional<int64_t> tenantId = Optional<int64_t>(),
                          ClusterType clusterType = ClusterType::STANDALONE) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool checkExistence = clusterType == ClusterType::STANDALONE;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			if (checkExistence) {
				Optional<int64_t> actualId = wait(TenantMetadata::tenantNameIndex().get(tr, name));
				if (!actualId.present() || (tenantId.present() && tenantId != actualId)) {
					throw tenant_not_found();
				}

				tenantId = actualId;
				checkExistence = false;
			}

			wait(deleteTenantTransaction(tr, tenantId.get(), clusterType));
			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

			TraceEvent("DeletedTenant")
			    .detail("Tenant", name)
			    .detail("TenantId", tenantId)
			    .detail("Version", tr->getCommittedVersion());
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// This should only be called from a transaction that has already confirmed that the tenant entry
// is present. The tenantEntry should start with the existing entry and modify only those fields that need
// to be changed. This must only be called on a non-management cluster.
ACTOR template <class Transaction>
Future<Void> configureTenantTransaction(Transaction tr,
                                        TenantMapEntry originalEntry,
                                        TenantMapEntry updatedTenantEntry) {
	ASSERT(updatedTenantEntry.id == originalEntry.id);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	TenantMetadata::tenantMap().set(tr, updatedTenantEntry.id, updatedTenantEntry);
	TenantMetadata::lastTenantModification().setVersionstamp(tr, Versionstamp(), 0);

	// If the tenant group was changed, we need to update the tenant group metadata structures
	if (originalEntry.tenantGroup != updatedTenantEntry.tenantGroup) {
		if (updatedTenantEntry.tenantGroup.present() && updatedTenantEntry.tenantGroup.get().startsWith("\xff"_sr)) {
			throw invalid_tenant_group_name();
		}
		if (originalEntry.tenantGroup.present()) {
			// Remove this tenant from the original tenant group index
			TenantMetadata::tenantGroupTenantIndex().erase(
			    tr, Tuple::makeTuple(originalEntry.tenantGroup.get(), updatedTenantEntry.id));

			// Check if the original tenant group is now empty. If so, remove the tenant group.
			KeyBackedSet<Tuple>::RangeResultType tenants = wait(TenantMetadata::tenantGroupTenantIndex().getRange(
			    tr,
			    Tuple::makeTuple(originalEntry.tenantGroup.get()),
			    Tuple::makeTuple(keyAfter(originalEntry.tenantGroup.get())),
			    2));

			if (tenants.results.empty() ||
			    (tenants.results.size() == 1 && tenants.results[0].getInt(1) == updatedTenantEntry.id)) {
				TenantMetadata::tenantGroupMap().erase(tr, originalEntry.tenantGroup.get());
			}
		}
		if (updatedTenantEntry.tenantGroup.present()) {
			// If this is creating a new tenant group, add it to the tenant group map
			Optional<TenantGroupEntry> entry =
			    wait(TenantMetadata::tenantGroupMap().get(tr, updatedTenantEntry.tenantGroup.get()));
			if (!entry.present()) {
				TenantMetadata::tenantGroupMap().set(tr, updatedTenantEntry.tenantGroup.get(), TenantGroupEntry());
			}

			// Insert this tenant in the tenant group index
			TenantMetadata::tenantGroupTenantIndex().insert(
			    tr, Tuple::makeTuple(updatedTenantEntry.tenantGroup.get(), updatedTenantEntry.id));
		}
	}

	return Void();
}

template <class Transaction>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenantsTransaction(Transaction tr,
                                                                           TenantName begin,
                                                                           TenantName end,
                                                                           int limit) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	auto future = TenantMetadata::tenantNameIndex().getRange(tr, begin, end, limit);
	return fmap([](auto f) -> std::vector<std::pair<TenantName, int64_t>> { return f.results; }, future);
}

template <class DB>
Future<std::vector<std::pair<TenantName, int64_t>>> listTenants(Reference<DB> db,
                                                                TenantName begin,
                                                                TenantName end,
                                                                int limit) {
	return runTransaction(db, [=](Reference<typename DB::TransactionT> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		return listTenantsTransaction(tr, begin, end, limit);
	});
}

ACTOR template <class Transaction>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantMetadataTransaction(Transaction tr,
                                                                                         TenantName begin,
                                                                                         TenantName end,
                                                                                         int limit) {
	std::vector<std::pair<TenantName, int64_t>> matchingTenants = wait(listTenantsTransaction(tr, begin, end, limit));

	state std::vector<Future<TenantMapEntry>> tenantEntryFutures;
	for (auto const& [name, id] : matchingTenants) {
		tenantEntryFutures.push_back(getTenantTransaction(tr, id));
	}

	wait(waitForAll(tenantEntryFutures));

	std::vector<std::pair<TenantName, TenantMapEntry>> results;
	for (auto const& f : tenantEntryFutures) {
		results.emplace_back(f.get().tenantName, f.get());
	}

	return results;
}

template <class DB>
Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantMetadata(Reference<DB> db,
                                                                              TenantName begin,
                                                                              TenantName end,
                                                                              int limit) {
	return runTransaction(db, [=](Reference<typename DB::TransactionT> tr) {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		return listTenantMetadataTransaction(tr, begin, end, limit);
	});
}

ACTOR template <class Transaction>
Future<Void> renameTenantTransaction(Transaction tr,
                                     TenantName oldName,
                                     TenantName newName,
                                     Optional<int64_t> tenantId = Optional<int64_t>(),
                                     ClusterType clusterType = ClusterType::STANDALONE,
                                     Optional<int64_t> configureSequenceNum = Optional<int64_t>()) {
	ASSERT(clusterType == ClusterType::STANDALONE || (tenantId.present() && configureSequenceNum.present()));
	ASSERT(clusterType != ClusterType::METACLUSTER_MANAGEMENT);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Void> tenantModeCheck = checkTenantMode(tr, clusterType);
	state Future<Optional<int64_t>> oldNameIdFuture =
	    tenantId.present() ? Future<Optional<int64_t>>() : TenantMetadata::tenantNameIndex().get(tr, oldName);
	state Future<Optional<int64_t>> newNameIdFuture = TenantMetadata::tenantNameIndex().get(tr, newName);

	wait(tenantModeCheck);

	if (!tenantId.present()) {
		wait(store(tenantId, oldNameIdFuture));
		if (!tenantId.present()) {
			throw tenant_not_found();
		}
	}

	state TenantMapEntry entry = wait(getTenantTransaction(tr, tenantId.get()));
	Optional<int64_t> newNameId = wait(newNameIdFuture);
	if (entry.tenantName != oldName) {
		throw tenant_not_found();
	}
	if (newNameId.present()) {
		throw tenant_already_exists();
	}

	if (configureSequenceNum.present()) {
		if (entry.configurationSequenceNum >= configureSequenceNum.get()) {
			return Void();
		}
		entry.configurationSequenceNum = configureSequenceNum.get();
	}

	entry.tenantName = newName;

	TenantMetadata::tenantMap().set(tr, tenantId.get(), entry);
	TenantMetadata::tenantNameIndex().set(tr, newName, tenantId.get());
	TenantMetadata::tenantNameIndex().erase(tr, oldName);
	TenantMetadata::lastTenantModification().setVersionstamp(tr, Versionstamp(), 0);

	if (clusterType == ClusterType::METACLUSTER_DATA) {
		wait(markTenantTombstones(tr, tenantId.get()));
	}

	return Void();
}

ACTOR template <class DB>
Future<Void> renameTenant(Reference<DB> db,
                          TenantName oldName,
                          TenantName newName,
                          Optional<int64_t> tenantId = Optional<int64_t>(),
                          ClusterType clusterType = ClusterType::STANDALONE) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	ASSERT(clusterType == ClusterType::STANDALONE || tenantId.present());

	state bool firstTry = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (!tenantId.present()) {
				wait(store(tenantId, TenantMetadata::tenantNameIndex().get(tr, oldName)));
				if (!tenantId.present()) {
					throw tenant_not_found();
				}
			}

			state Future<Optional<int64_t>> newNameIdFuture = TenantMetadata::tenantNameIndex().get(tr, newName);
			state TenantMapEntry entry = wait(getTenantTransaction(tr, tenantId.get()));
			Optional<int64_t> newNameId = wait(newNameIdFuture);

			if (!firstTry && entry.tenantName == newName) {
				// On a retry, the rename may have already occurred
				return Void();
			} else if (entry.tenantName != oldName) {
				throw tenant_not_found();
			} else if (newNameId.present() && newNameId.get() != tenantId.get()) {
				throw tenant_already_exists();
			}

			firstTry = false;

			wait(renameTenantTransaction(tr, oldName, newName, tenantId, clusterType));
			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));

			TraceEvent("TenantRenamed")
			    .detail("OldName", oldName)
			    .detail("NewName", newName)
			    .detail("TenantId", tenantId.get());
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

template <class Transaction>
Future<Optional<TenantGroupEntry>> tryGetTenantGroupTransaction(Transaction tr, TenantGroupName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return TenantMetadata::tenantGroupMap().get(tr, name);
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
	    wait(TenantMetadata::tenantGroupMap().getRange(tr, begin, end, limit));

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

} // namespace TenantAPI

#include "flow/unactorcompiler.h"
#endif