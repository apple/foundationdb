/*
 * DecommissionMetacluster.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_DECOMMISSIONMETACLUSTER_ACTOR_G_H)
#define METACLUSTER_DECOMMISSIONMETACLUSTER_ACTOR_G_H
#include "metacluster/DecommissionMetacluster.actor.g.h"
#elif !defined(METACLUSTER_DECOMMISSIONMETACLUSTER_ACTOR_H)
#define METACLUSTER_DECOMMISSIONMETACLUSTER_ACTOR_H

#include "fdbclient/TenantManagement.actor.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

ACTOR template <class DB>
Future<Void> decommissionMetacluster(Reference<DB> db) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state bool firstTry = true;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
				if (firstTry) {
					throw invalid_metacluster_operation();
				} else {
					return Void();
				}
			}

			// Erase all metadata not associated with specific tenants prior to checking
			// cluster emptiness
			metadata::management::tenantMetadata().tenantCount.clear(tr);
			metadata::management::tenantMetadata().lastTenantId.clear(tr);
			metadata::management::tenantMetadata().tenantTombstones.clear(tr);
			metadata::management::tenantMetadata().tombstoneCleanupData.clear(tr);
			metadata::management::tenantMetadata().lastTenantModification.clear(tr);

			wait(internal::managementClusterCheckEmpty(tr));
			metadata::metaclusterRegistration().clear(tr);

			firstTry = false;
			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	return Void();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif