/*
 * CreateMetacluster.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_CREATEMETACLUSTER_ACTOR_G_H)
#define METACLUSTER_CREATEMETACLUSTER_ACTOR_G_H
#include "metacluster/CreateMetacluster.actor.g.h"
#elif !defined(METACLUSTER_CREATEMETACLUSTER_ACTOR_H)
#define METACLUSTER_CREATEMETACLUSTER_ACTOR_H

#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster::internal {

ACTOR template <class Transaction>
Future<TenantMode> getClusterConfiguredTenantMode(Transaction tr) {
	state typename transaction_future_type<Transaction, Optional<Value>>::type tenantModeFuture =
	    tr->get(tenantModeConfKey);
	Optional<Value> tenantModeValue = wait(safeThreadFutureToFuture(tenantModeFuture));
	return TenantMode::fromValue(tenantModeValue.castTo<ValueRef>());
}

} // namespace metacluster::internal

namespace metacluster {

ACTOR template <class DB>
Future<Optional<std::string>> createMetacluster(Reference<DB> db,
                                                ClusterName name,
                                                int64_t tenantIdPrefix,
                                                bool enableTenantModeCheck) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Optional<UID> metaclusterUid;
	ASSERT(tenantIdPrefix >= TenantAPI::TENANT_ID_PREFIX_MIN_VALUE &&
	       tenantIdPrefix <= TenantAPI::TENANT_ID_PREFIX_MAX_VALUE);

	if (name.startsWith("\xff"_sr)) {
		throw invalid_cluster_name();
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state Future<Optional<MetaclusterRegistrationEntry>> metaclusterRegistrationFuture =
			    metadata::metaclusterRegistration().get(tr);

			state Future<Void> metaclusterEmptinessCheck = internal::managementClusterCheckEmpty(tr);
			state Future<TenantMode> tenantModeFuture = enableTenantModeCheck
			                                                ? internal::getClusterConfiguredTenantMode(tr)
			                                                : Future<TenantMode>(TenantMode::DISABLED);

			Optional<MetaclusterRegistrationEntry> existingRegistration = wait(metaclusterRegistrationFuture);
			if (existingRegistration.present()) {
				if (metaclusterUid.present() && metaclusterUid.get() == existingRegistration.get().metaclusterId) {
					return Optional<std::string>();
				} else {
					return fmt::format("cluster is already registered as a {} named `{}'",
					                   existingRegistration.get().clusterType == ClusterType::METACLUSTER_DATA
					                       ? "data cluster"
					                       : "metacluster",
					                   printable(existingRegistration.get().name));
				}
			}

			wait(metaclusterEmptinessCheck);
			TenantMode tenantMode = wait(tenantModeFuture);
			if (tenantMode != TenantMode::DISABLED) {
				return fmt::format("cluster is configured with tenant mode `{}' when tenants should be disabled",
				                   tenantMode.toString());
			}

			if (!metaclusterUid.present()) {
				metaclusterUid = deterministicRandom()->randomUniqueID();
			}

			metadata::metaclusterRegistration().set(tr, MetaclusterRegistrationEntry(name, metaclusterUid.get()));

			TenantMetadata::tenantIdPrefix().set(tr, tenantIdPrefix);

			wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}

	TraceEvent("CreatedMetacluster").detail("Name", name).detail("Prefix", tenantIdPrefix);

	return Optional<std::string>();
}
} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif