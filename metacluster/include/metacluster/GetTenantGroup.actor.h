/*
 * GetTenantGroup.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_GETTENANTGROUP_ACTOR_G_H)
#define METACLUSTER_GETTENANTGROUP_ACTOR_G_H
#include "metacluster/GetTenantGroup.actor.g.h"
#elif !defined(METACLUSTER_GETTENANTGROUP_ACTOR_H)
#define METACLUSTER_GETTENANTGROUP_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

template <class Transaction>
Future<Optional<MetaclusterTenantGroupEntry>> tryGetTenantGroupTransaction(Transaction tr, TenantGroupName name) {
	CODE_PROBE(true, "Try get tenant group");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return metadata::management::tenantMetadata().tenantGroupMap.get(tr, name);
}

ACTOR template <class DB>
Future<Optional<MetaclusterTenantGroupEntry>> tryGetTenantGroup(Reference<DB> db, TenantGroupName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<MetaclusterTenantGroupEntry> entry = wait(tryGetTenantGroupTransaction(tr, name));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif