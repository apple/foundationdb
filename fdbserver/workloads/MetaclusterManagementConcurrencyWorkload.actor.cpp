/*
 * MetaclusterManagementConcurrencyWorkload.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/MetaclusterConsistency.actor.h"
#include "fdbserver/workloads/TenantConsistency.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterManagementConcurrencyWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterManagementConcurrency";

	Reference<IDatabase> managementDb;
	std::map<ClusterName, Database> dataDbs;
	std::vector<ClusterName> dataDbIndex;

	double testDuration;

	MetaclusterManagementConcurrencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 120.0);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert("MachineAttritionWorkload");
	}

	Future<Void> setup(Database const& cx) override {
		if (g_network->isSimulated() && BUGGIFY) {
			IKnobCollection::getMutableGlobalKnobCollection().setKnob(
			    "max_tenants_per_cluster", KnobValueRef::create(int{ deterministicRandom()->randomInt(20, 100) }));
		}

		return _setup(cx, this);
	}

	ACTOR static Future<Void> _setup(Database cx, MetaclusterManagementConcurrencyWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));

		MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
		self->managementDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

		ASSERT(g_simulator->extraDatabases.size() > 0);
		for (auto connectionString : g_simulator->extraDatabases) {
			ClusterConnectionString ccs(connectionString);
			self->dataDbIndex.push_back(ClusterName(format("cluster_%08d", self->dataDbs.size())));
			self->dataDbs[self->dataDbIndex.back()] =
			    Database::createSimulatedExtraDatabase(connectionString, cx->defaultTenant);
		}

		if (self->clientId == 0) {
			wait(success(MetaclusterAPI::createMetacluster(cx.getReference(), "management_cluster"_sr)));
		}
		return Void();
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	ACTOR static Future<Void> registerCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database* dataDb = &self->dataDbs[clusterName];
		try {
			state DataClusterEntry entry;
			entry.capacity.numTenantGroups = deterministicRandom()->randomInt(0, 4);
			loop {
				Future<Void> registerFuture = MetaclusterAPI::registerCluster(
				    self->managementDb,
				    clusterName,
				    dataDb->getReference()->getConnectionRecord()->getConnectionString(),
				    entry);

				Optional<Void> result = wait(timeout(registerFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					break;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_cluster_already_exists && e.code() != error_code_cluster_not_empty &&
			    e.code() != error_code_cluster_already_registered && e.code() != error_code_cluster_removed) {
				TraceEvent(SevError, "RegisterClusterFailure").error(e).detail("ClusterName", clusterName);
			}
			return Void();
		}

		// Get a version to know that the cluster has recovered
		wait(success(runTransaction(dataDb->getReference(),
		                            [](Reference<ReadYourWritesTransaction> tr) { return tr->getReadVersion(); })));
		return Void();
	}

	ACTOR static Future<Void> removeCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database* dataDb = &self->dataDbs[clusterName];
		try {
			loop {
				Future<Void> removeFuture = MetaclusterAPI::removeCluster(self->managementDb, clusterName, false);
				Optional<Void> result = wait(timeout(removeFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					break;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_cluster_not_found && e.code() != error_code_cluster_not_empty) {
				TraceEvent(SevError, "RemoveClusterFailure").error(e).detail("ClusterName", clusterName);
			}
			return Void();
		}

		// Get a version to know that the cluster has recovered
		wait(success(runTransaction(dataDb->getReference(),
		                            [](Reference<ReadYourWritesTransaction> tr) { return tr->getReadVersion(); })));
		return Void();
	}

	ACTOR static Future<Void> listClusters(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName1 = self->chooseClusterName();
		state ClusterName clusterName2 = self->chooseClusterName();
		state int limit = deterministicRandom()->randomInt(1, self->dataDbs.size() + 1);
		try {
			std::map<ClusterName, DataClusterMetadata> clusterList =
			    wait(MetaclusterAPI::listClusters(self->managementDb, clusterName1, clusterName2, limit));
			ASSERT(clusterName1 <= clusterName2);
		} catch (Error& e) {
			if (e.code() != error_code_inverted_range) {
				TraceEvent(SevError, "ListClusterFailure")
				    .error(e)
				    .detail("ClusterName1", clusterName1)
				    .detail("ClusterName2", clusterName2);
			}
			return Void();
		}
		return Void();
	}

	ACTOR static Future<Void> getCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database* dataDb = &self->dataDbs[clusterName];

		try {
			DataClusterMetadata clusterMetadata = wait(MetaclusterAPI::getCluster(self->managementDb, clusterName));
			ASSERT(dataDb->getReference()->getConnectionRecord()->getConnectionString() ==
			       clusterMetadata.connectionString);
		} catch (Error& e) {
			if (e.code() != error_code_cluster_not_found) {
				TraceEvent(SevError, "GetClusterFailure").error(e).detail("ClusterName", clusterName);
			}
			return Void();
		}

		return Void();
	}

	ACTOR static Future<Optional<DataClusterEntry>> configureImpl(MetaclusterManagementConcurrencyWorkload* self,
	                                                              ClusterName clusterName,
	                                                              Optional<int64_t> numTenantGroups,
	                                                              Optional<ClusterConnectionString> connectionString) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<DataClusterMetadata> clusterMetadata =
				    wait(MetaclusterAPI::tryGetClusterTransaction(tr, clusterName));
				state Optional<DataClusterEntry> entry;

				if (clusterMetadata.present()) {
					if (numTenantGroups.present()) {
						entry = clusterMetadata.get().entry;
						entry.get().capacity.numTenantGroups = numTenantGroups.get();
					}
					MetaclusterAPI::updateClusterMetadata(
					    tr, clusterName, clusterMetadata.get(), connectionString, entry);

					wait(buggifiedCommit(tr, BUGGIFY_WITH_PROB(0.1)));
				}

				return entry;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<Void> configureCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database* dataDb = &self->dataDbs[clusterName];

		state Optional<int64_t> newNumTenantGroups;
		state Optional<ClusterConnectionString> connectionString;
		if (deterministicRandom()->coinflip()) {
			newNumTenantGroups = deterministicRandom()->randomInt(0, 4);
		}
		if (deterministicRandom()->coinflip()) {
			connectionString = dataDb->getReference()->getConnectionRecord()->getConnectionString();
		}

		try {
			loop {
				Optional<Optional<DataClusterEntry>> result =
				    wait(timeout(configureImpl(self, clusterName, newNumTenantGroups, connectionString),
				                 deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					break;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_cluster_removed) {
				TraceEvent(SevError, "ConfigureClusterFailure").error(e).detail("ClusterName", clusterName);
			}
		}

		return Void();
	}

	// TODO: restoreCluster

	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	ACTOR static Future<Void> _start(Database cx, MetaclusterManagementConcurrencyWorkload* self) {
		state double start = now();

		// Run a random sequence of metacluster management operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 5);
			if (operation == 0) {
				wait(registerCluster(self));
			} else if (operation == 1) {
				wait(removeCluster(self));
			} else if (operation == 2) {
				wait(listClusters(self));
			} else if (operation == 3) {
				wait(getCluster(self));
			} else if (operation == 4) {
				wait(configureCluster(self));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	ACTOR static Future<bool> _check(Database cx, MetaclusterManagementConcurrencyWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, AllowPartialMetaclusterOperations::True);
		wait(metaclusterConsistencyCheck.run());

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MetaclusterManagementConcurrencyWorkload> MetaclusterManagementConcurrencyWorkloadFactory;
