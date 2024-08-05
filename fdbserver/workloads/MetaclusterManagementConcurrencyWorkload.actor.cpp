/*
 * MetaclusterManagementConcurrencyWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/TenantConsistency.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterManagementConcurrencyWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterManagementConcurrency";

	metacluster::util::SimulatedMetacluster simMetacluster;
	std::vector<ClusterName> dataDbIndex;

	double testDuration;
	bool createMetacluster;

	MetaclusterManagementConcurrencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 90.0);
		createMetacluster = getOption(options, "createMetacluster"_sr, true);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR static Future<Void> _setup(Database cx, MetaclusterManagementConcurrencyWorkload* self) {
		wait(store(self->simMetacluster,
		           metacluster::util::createSimulatedMetacluster(
		               cx,
		               deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                                TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1),
		               {},
		               metacluster::util::SkipMetaclusterCreation(self->clientId != 0 || !self->createMetacluster))));

		ASSERT_GT(self->simMetacluster.dataDbs.size(), 0);
		for (auto const& [name, db] : self->simMetacluster.dataDbs) {
			self->dataDbIndex.push_back(name);
		}

		return Void();
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	ACTOR static Future<Void> registerCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database dataDb = self->simMetacluster.dataDbs[clusterName];

		state UID debugId = deterministicRandom()->randomUniqueID();

		try {
			state metacluster::DataClusterEntry entry;
			entry.capacity.numTenantGroups = deterministicRandom()->randomInt(0, 4);
			if (deterministicRandom()->random01() < 0.2) {
				entry.autoTenantAssignment = metacluster::AutoTenantAssignment::DISABLED;
			}
			loop {
				TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRegisteringCluster", debugId)
				    .detail("ClusterName", clusterName)
				    .detail("NumTenantGroups", entry.capacity.numTenantGroups);
				Future<Void> registerFuture =
				    metacluster::registerCluster(self->simMetacluster.managementDb,
				                                 clusterName,
				                                 dataDb.getReference()->getConnectionRecord()->getConnectionString(),
				                                 entry);

				Optional<Void> result = wait(timeout(registerFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRegisteredCluster", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("NumTenantGroups", entry.capacity.numTenantGroups);
					break;
				}

				CODE_PROBE(true, "Register cluster timed out");
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRegisterClusterError", debugId)
			    .error(e)
			    .detail("ClusterName", clusterName);
			if (e.code() != error_code_cluster_already_exists && e.code() != error_code_cluster_not_empty &&
			    e.code() != error_code_cluster_already_registered && e.code() != error_code_cluster_removed &&
			    e.code() != error_code_cluster_restoring) {
				TraceEvent(SevError, "MetaclusterManagementConcurrencyRegisterClusterFailure", debugId)
				    .error(e)
				    .detail("ClusterName", clusterName);
				ASSERT(false);
			}

			wait(success(errorOr(metacluster::removeCluster(self->simMetacluster.managementDb,
			                                                clusterName,
			                                                ClusterType::METACLUSTER_MANAGEMENT,
			                                                metacluster::ForceRemove::True))));

			return Void();
		}

		return Void();
	}

	ACTOR static Future<Void> removeCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database dataDb = self->simMetacluster.dataDbs[clusterName];
		state metacluster::ForceRemove forceRemove(deterministicRandom()->coinflip());

		state UID debugId = deterministicRandom()->randomUniqueID();

		try {
			loop {
				TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRemovingCluster", debugId)
				    .detail("ClusterName", clusterName);
				Future<bool> removeFuture = metacluster::removeCluster(self->simMetacluster.managementDb,
				                                                       clusterName,
				                                                       ClusterType::METACLUSTER_MANAGEMENT,
				                                                       metacluster::ForceRemove::False);
				Optional<bool> result = wait(timeout(removeFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					ASSERT(result.get());
					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRemovedCluster", debugId)
					    .detail("ClusterName", clusterName);
					break;
				}

				CODE_PROBE(true, "Remove cluster timed out");
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRemoveClusterError", debugId)
			    .error(e)
			    .detail("ClusterName", clusterName);
			if (e.code() != error_code_cluster_not_found && e.code() != error_code_cluster_not_empty) {
				TraceEvent(SevError, "MetaclusterManagementConcurrencyRemoveClusterFailure", debugId)
				    .error(e)
				    .detail("ClusterName", clusterName);
				ASSERT(false);
			}
			return Void();
		}

		return Void();
	}

	ACTOR static Future<Void> listClusters(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName1 = self->chooseClusterName();
		state ClusterName clusterName2 = self->chooseClusterName();
		state int limit = deterministicRandom()->randomInt(1, self->simMetacluster.dataDbs.size() + 1);
		try {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyListClusters")
			    .detail("StartClusterName", clusterName1)
			    .detail("EndClusterName", clusterName2)
			    .detail("Limit", limit);

			std::map<ClusterName, metacluster::DataClusterMetadata> clusterList =
			    wait(metacluster::listClusters(self->simMetacluster.managementDb, clusterName1, clusterName2, limit));

			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyListedClusters")
			    .detail("StartClusterName", clusterName1)
			    .detail("EndClusterName", clusterName2)
			    .detail("Limit", limit);

			ASSERT(clusterName1 <= clusterName2);
			ASSERT(clusterList.size() <= limit);
		} catch (Error& e) {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyListClustersError")
			    .error(e)
			    .detail("StartClusterName", clusterName1)
			    .detail("EndClusterName", clusterName2)
			    .detail("Limit", limit);

			if (e.code() != error_code_inverted_range) {
				TraceEvent(SevError, "MetaclusterManagementConcurrencyListClusterFailure")
				    .error(e)
				    .detail("ClusterName1", clusterName1)
				    .detail("ClusterName2", clusterName2);
				ASSERT(false);
			}
			return Void();
		}
		return Void();
	}

	ACTOR static Future<Void> getCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database dataDb = self->simMetacluster.dataDbs[clusterName];

		try {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyGetCluster").detail("ClusterName", clusterName);
			metacluster::DataClusterMetadata clusterMetadata =
			    wait(metacluster::getCluster(self->simMetacluster.managementDb, clusterName));
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyGotCluster").detail("ClusterName", clusterName);

			ASSERT(dataDb.getReference()->getConnectionRecord()->getConnectionString() ==
			       clusterMetadata.connectionString);
		} catch (Error& e) {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyGetClusterError")
			    .error(e)
			    .detail("ClusterName", clusterName);
			if (e.code() != error_code_cluster_not_found) {
				TraceEvent(SevError, "MetaclusterManagementConcurrencyGetClusterFailure")
				    .error(e)
				    .detail("ClusterName", clusterName);
				ASSERT(false);
			}
			return Void();
		}

		return Void();
	}

	ACTOR static Future<Optional<metacluster::DataClusterEntry>> configureImpl(
	    MetaclusterManagementConcurrencyWorkload* self,
	    ClusterName clusterName,
	    Optional<int64_t> numTenantGroups,
	    Optional<ClusterConnectionString> connectionString,
	    Optional<metacluster::AutoTenantAssignment> autoTenantAssignment) {
		state Reference<ITransaction> tr = self->simMetacluster.managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<metacluster::DataClusterMetadata> clusterMetadata =
				    wait(metacluster::tryGetClusterTransaction(tr, clusterName));
				state Optional<metacluster::DataClusterEntry> entry;

				if (clusterMetadata.present()) {
					if (numTenantGroups.present()) {
						if (!entry.present()) {
							entry = clusterMetadata.get().entry;
						}
						entry.get().capacity.numTenantGroups = numTenantGroups.get();
					}
					if (autoTenantAssignment.present()) {
						if (!entry.present()) {
							entry = clusterMetadata.get().entry;
						}
						entry.get().autoTenantAssignment = autoTenantAssignment.get();
					}
					metacluster::updateClusterMetadata(tr, clusterName, clusterMetadata.get(), connectionString, entry);

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
		state Database dataDb = self->simMetacluster.dataDbs[clusterName];

		state UID debugId = deterministicRandom()->randomUniqueID();

		state Optional<int64_t> newNumTenantGroups;
		state Optional<ClusterConnectionString> connectionString;
		state Optional<metacluster::AutoTenantAssignment> autoTenantAssignment;
		if (deterministicRandom()->coinflip()) {
			newNumTenantGroups = deterministicRandom()->randomInt(0, 4);
		}
		if (deterministicRandom()->coinflip()) {
			connectionString = dataDb.getReference()->getConnectionRecord()->getConnectionString();
		}
		if (deterministicRandom()->coinflip()) {
			autoTenantAssignment = deterministicRandom()->coinflip() ? metacluster::AutoTenantAssignment::DISABLED
			                                                         : metacluster::AutoTenantAssignment::ENABLED;
		}

		try {
			loop {
				TraceEvent(SevDebug, "MetaclusterManagementConcurrencyConfigureCluster", debugId)
				    .detail("ClusterName", clusterName)
				    .detail("NewNumTenantGroups", newNumTenantGroups.orDefault(-1))
				    .detail("NewConnectionString",
				            connectionString.map(&ClusterConnectionString::toString).orDefault(""));

				Optional<Optional<metacluster::DataClusterEntry>> result = wait(timeout(
				    configureImpl(self, clusterName, newNumTenantGroups, connectionString, autoTenantAssignment),
				    deterministicRandom()->randomInt(1, 30)));

				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyConfiguredCluster", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("NewNumTenantGroups", newNumTenantGroups.orDefault(-1))
					    .detail("NewConnectionString",
					            connectionString.map(&ClusterConnectionString::toString).orDefault(""));
					break;
				}

				CODE_PROBE(true, "Configure cluster timed out");
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyConfigureClusterError", debugId)
			    .error(e)
			    .detail("ClusterName", clusterName)
			    .detail("NewNumTenantGroups", newNumTenantGroups.orDefault(-1))
			    .detail("NewConnectionString", connectionString.map(&ClusterConnectionString::toString).orDefault(""));
			if (e.code() != error_code_cluster_not_found && e.code() != error_code_cluster_removed &&
			    e.code() != error_code_invalid_metacluster_operation && e.code() != error_code_cluster_restoring) {
				TraceEvent(SevError, "MetaclusterManagementConcurrencyConfigureClusterFailure")
				    .error(e)
				    .detail("ClusterName", clusterName);
				ASSERT(false);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> restoreCluster(MetaclusterManagementConcurrencyWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Database db = self->simMetacluster.dataDbs[clusterName];
		state metacluster::ApplyManagementClusterUpdates applyManagementClusterUpdates(
		    deterministicRandom()->coinflip());
		state metacluster::ForceJoin forceJoin(deterministicRandom()->coinflip());
		state bool removeFirst = !applyManagementClusterUpdates && deterministicRandom()->coinflip();

		state UID debugId = deterministicRandom()->randomUniqueID();

		try {
			loop {
				TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestore", debugId)
				    .detail("ClusterName", clusterName)
				    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);

				if (removeFirst) {
					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestoreRemoveDataCluster", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);

					wait(success(errorOr(metacluster::removeCluster(self->simMetacluster.managementDb,
					                                                clusterName,
					                                                ClusterType::METACLUSTER_MANAGEMENT,
					                                                metacluster::ForceRemove::True))));

					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestoreRemovedDataCluster", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);
				}

				state std::vector<std::string> messages;
				if (deterministicRandom()->coinflip()) {
					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestoreDryRun", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);

					wait(metacluster::restoreCluster(self->simMetacluster.managementDb,
					                                 clusterName,
					                                 db->getConnectionRecord()->getConnectionString(),
					                                 applyManagementClusterUpdates,
					                                 metacluster::RestoreDryRun::True,
					                                 forceJoin,
					                                 metacluster::ForceReuseTenantIdPrefix::True,
					                                 &messages));

					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestoreDryRunDone", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);

					messages.clear();
				}

				Optional<Void> result =
				    wait(timeout(metacluster::restoreCluster(self->simMetacluster.managementDb,
				                                             clusterName,
				                                             db->getConnectionRecord()->getConnectionString(),
				                                             applyManagementClusterUpdates,
				                                             metacluster::RestoreDryRun::False,
				                                             forceJoin,
				                                             metacluster::ForceReuseTenantIdPrefix::True,
				                                             &messages),
				                 30.0));

				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestoreComplete", debugId)
					    .detail("ClusterName", clusterName)
					    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);
					break;
				}

				CODE_PROBE(true, "Restore cluster timed out");
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "MetaclusterManagementConcurrencyRestoreError", debugId)
			    .error(e)
			    .detail("ClusterName", clusterName)
			    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);

			if (e.code() == error_code_cluster_already_registered) {
				ASSERT(!forceJoin || !applyManagementClusterUpdates);
			} else if (applyManagementClusterUpdates && e.code() == error_code_invalid_data_cluster) {
				// Restoring a data cluster can fail if the cluster is not actually a data cluster registered with
				// the metacluster
			} else if (!applyManagementClusterUpdates &&
			           (e.code() == error_code_cluster_already_exists || e.code() == error_code_tenant_already_exists ||
			            e.code() == error_code_invalid_tenant_configuration)) {
				// Repopulating a management cluster can fail if the cluster is already in the metacluster
			} else if (e.code() != error_code_cluster_not_found && e.code() != error_code_cluster_removed &&
			           e.code() != error_code_conflicting_restore) {
				TraceEvent(SevError, "MetaclusterManagementConcurrencyRestoreFailure", debugId)
				    .error(e)
				    .detail("ClusterName", clusterName)
				    .detail("ApplyManagementClusterUpdates", applyManagementClusterUpdates);
				ASSERT(false);
			}

			wait(success(errorOr(metacluster::removeCluster(self->simMetacluster.managementDb,
			                                                clusterName,
			                                                ClusterType::METACLUSTER_MANAGEMENT,
			                                                metacluster::ForceRemove::True))));
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	ACTOR static Future<Void> _start(Database cx, MetaclusterManagementConcurrencyWorkload* self) {
		state double start = now();

		// Run a random sequence of metacluster management operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 6);
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
			} else if (operation == 5) {
				wait(restoreCluster(self));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(cx, this);
		} else {
			return true;
		}
	}
	ACTOR static Future<bool> _check(Database cx, MetaclusterManagementConcurrencyWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->simMetacluster.managementDb, metacluster::util::AllowPartialMetaclusterOperations::True);
		wait(metaclusterConsistencyCheck.run());

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MetaclusterManagementConcurrencyWorkload> MetaclusterManagementConcurrencyWorkloadFactory;
