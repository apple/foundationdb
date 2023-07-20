/*
 * GetCluster.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_GETCLUSTER_ACTOR_G_H)
#define METACLUSTER_GETCLUSTER_ACTOR_G_H
#include "metacluster/GetCluster.actor.g.h"
#elif !defined(METACLUSTER_GETCLUSTER_ACTOR_H)
#define METACLUSTER_GETCLUSTER_ACTOR_H

#include "fdbclient/Tenant.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterTypes.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

ACTOR template <class Transaction>
Future<Optional<DataClusterMetadata>> tryGetClusterTransaction(Transaction tr, ClusterName name) {
	CODE_PROBE(true, "Try get cluster");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);

	state Future<Void> metaclusterRegistrationCheck =
	    TenantAPI::checkTenantMode(tr, ClusterType::METACLUSTER_MANAGEMENT);

	state Future<Optional<DataClusterEntry>> clusterEntryFuture = metadata::management::dataClusters().get(tr, name);
	state Future<Optional<ClusterConnectionString>> connectionRecordFuture =
	    metadata::management::dataClusterConnectionRecords().get(tr, name);

	wait(metaclusterRegistrationCheck);

	state Optional<DataClusterEntry> clusterEntry = wait(clusterEntryFuture);
	Optional<ClusterConnectionString> connectionString = wait(connectionRecordFuture);

	if (clusterEntry.present()) {
		ASSERT(connectionString.present());
		return Optional<DataClusterMetadata>(DataClusterMetadata(clusterEntry.get(), connectionString.get()));
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

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif