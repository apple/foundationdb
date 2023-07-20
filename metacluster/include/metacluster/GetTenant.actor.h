/*
 * GetTenant.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_GETTENANT_ACTOR_G_H)
#define METACLUSTER_GETTENANT_ACTOR_G_H
#include "metacluster/GetTenant.actor.g.h"
#elif !defined(METACLUSTER_GETTENANT_ACTOR_H)
#define METACLUSTER_GETTENANT_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

template <class Transaction>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenantTransaction(Transaction tr, int64_t tenantId) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return metadata::management::tenantMetadata().tenantMap.get(tr, tenantId);
}

ACTOR template <class Transaction>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<int64_t> tenantId = wait(metadata::management::tenantMetadata().tenantNameIndex.get(tr, name));
	if (tenantId.present()) {
		Optional<MetaclusterTenantMapEntry> entry =
		    wait(metadata::management::tenantMetadata().tenantMap.get(tr, tenantId.get()));
		return entry;
	} else {
		return Optional<MetaclusterTenantMapEntry>();
	}
}

ACTOR template <class DB, class Tenant>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenant(Reference<DB> db, Tenant tenant) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction, class Tenant>
Future<MetaclusterTenantMapEntry> getTenantTransaction(Transaction tr, Tenant tenant) {
	Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class DB, class Tenant>
Future<MetaclusterTenantMapEntry> getTenant(Reference<DB> db, Tenant tenant) {
	Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenant(db, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif