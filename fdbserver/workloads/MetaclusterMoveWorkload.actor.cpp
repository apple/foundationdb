/*
 * MetaclusterMoveWorkload.actor.cpp
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
#include <cstdint>
#include <limits>
#include <utility>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/TenantName.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/MetaclusterData.actor.h"
#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterMove.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterMoveWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterMove";

	struct DataClusterData {
		Database db;
		std::set<int64_t> tenants;
		std::set<TenantGroupName> tenantGroups;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	struct TestTenantData {

		TenantName name;
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;

		TestTenantData() {}
		TestTenantData(TenantName name, ClusterName cluster, Optional<TenantGroupName> tenantGroup)
		  : name(name), cluster(cluster), tenantGroup(tenantGroup) {}
	};

	struct TenantGroupData {
		ClusterName cluster;
		std::set<int64_t> tenants;
	};

	int nodeCount;
	double transactionsPerSecond;
	Key keyPrefix;

	Reference<IDatabase> managementDb;
	std::map<ClusterName, DataClusterData> dataDbs;
	std::vector<ClusterName> dataDbIndex;

	std::map<int64_t, TestTenantData> createdTenants;
	std::map<TenantName, int64_t> tenantNameIndex;
	std::map<TenantGroupName, TenantGroupData> tenantGroups;

	int initialTenants;
	int maxTenants;
	int maxTenantGroups;
	int tenantGroupCapacity;
	metacluster::metadata::management::MovementRecord moveRecord;

	int64_t reservedQuota;
	int64_t totalQuota;
	int64_t storageQuota;

	MetaclusterMoveWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0);
		nodeCount = getOption(options, "nodeCount"_sr, transactionsPerSecond);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		maxTenants =
		    deterministicRandom()->randomInt(1, std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 100)) + 1);
		initialTenants = std::min<int>(maxTenants, getOption(options, "initialTenants"_sr, 40));
		maxTenantGroups = deterministicRandom()->randomInt(
		    1, std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20)) + 1);
		tenantGroupCapacity =
		    std::max<int>(1, (initialTenants / 2 + maxTenantGroups - 1) / g_simulator->extraDatabases.size());
		reservedQuota = getOption(options, "reservedQuota"_sr, 0);
		totalQuota = getOption(options, "totalQuota"_sr, 1e8);
		storageQuota = getOption(options, "storageQuota"_sr, 1e8);
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant%08d", deterministicRandom()->randomInt(0, maxTenants)));
		return tenant;
	}

	Optional<TenantGroupName> chooseTenantGroup(Optional<ClusterName> cluster = Optional<ClusterName>()) {
		Optional<TenantGroupName> tenantGroup;
		if (deterministicRandom()->coinflip()) {
			if (!cluster.present()) {
				tenantGroup =
				    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
			} else {
				auto const& existingGroups = dataDbs[cluster.get()].tenantGroups;
				if (!existingGroups.empty()) {
					tenantGroup = deterministicRandom()->randomChoice(
					    std::vector<TenantGroupName>(existingGroups.begin(), existingGroups.end()));
				}
			}
		}

		return tenantGroup;
	}

	static void updateTestData(MetaclusterMoveWorkload* self,
	                           TenantGroupName tenantGroup,
	                           ClusterName newCluster,
	                           ClusterName oldCluster) {
		// Update tenantGroups
		auto groupData = self->tenantGroups[tenantGroup];
		auto groupTenants = groupData.tenants;
		TraceEvent("BreakpointTestUpdateStart")
		    .detail("OldCluster", oldCluster)
		    .detail("NewCluster", newCluster)
		    .detail("TenantGroup", tenantGroup)
		    .detail("Size", groupTenants.size());
		groupData.cluster = newCluster;
		self->tenantGroups[tenantGroup] = groupData;

		// Update dataDbs
		auto newData = self->dataDbs[newCluster];
		auto oldData = self->dataDbs[oldCluster];

		// Take the set difference and update the old data
		std::set<int64_t> result;
		std::set_difference(oldData.tenants.begin(),
		                    oldData.tenants.end(),
		                    groupTenants.begin(),
		                    groupTenants.end(),
		                    std::inserter(result, result.end()));
		oldData.tenants = result;

		// Add all tenants to the new data
		newData.tenants.insert(groupTenants.begin(), groupTenants.end());

		// Erase tenant group from old data and add to new one
		oldData.tenantGroups.erase(tenantGroup);
		newData.tenantGroups.insert(tenantGroup);

		self->dataDbs[oldCluster] = oldData;
		self->dataDbs[newCluster] = newData;

		// Update createdTenants
		for (const auto& tId : groupTenants) {
			self->createdTenants[tId].tenantGroup = tenantGroup;
			ASSERT_EQ(self->createdTenants[tId].cluster, oldCluster);
			self->createdTenants[tId].cluster = newCluster;
		}
	}

	// Used to gradually increase capacity so that the tenants are somewhat evenly distributed across the clusters
	ACTOR static Future<Void> increaseMetaclusterCapacity(MetaclusterMoveWorkload* self) {
		self->tenantGroupCapacity = ceil(self->tenantGroupCapacity * 1.2);
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state int dbIndex;
				for (dbIndex = 0; dbIndex < self->dataDbIndex.size(); ++dbIndex) {
					metacluster::DataClusterMetadata clusterMetadata =
					    wait(metacluster::getClusterTransaction(tr, self->dataDbIndex[dbIndex]));
					metacluster::DataClusterEntry updatedEntry = clusterMetadata.entry;
					updatedEntry.capacity.numTenantGroups = self->tenantGroupCapacity;
					metacluster::updateClusterMetadata(
					    tr, self->dataDbIndex[dbIndex], clusterMetadata, {}, updatedEntry);
				}
				wait(safeThreadFutureToFuture(tr->commit()));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> createTenant(MetaclusterMoveWorkload* self) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(tenantName) == 0) {
				break;
			}
		}

		if (self->tenantNameIndex.count(tenantName)) {
			return Void();
		}

		loop {
			try {
				metacluster::MetaclusterTenantMapEntry tenantEntry;
				tenantEntry.tenantName = tenantName;
				tenantEntry.tenantGroup = self->chooseTenantGroup();
				wait(metacluster::createTenant(self->managementDb,
				                               tenantEntry,
				                               metacluster::AssignClusterAutomatically::True,
				                               metacluster::IgnoreCapacityLimit::False));
				metacluster::MetaclusterTenantMapEntry createdEntry =
				    wait(metacluster::getTenant(self->managementDb, tenantName));
				TraceEvent(SevDebug, "MetaclusterMoveWorkloadCreatedTenant")
				    .detail("Tenant", tenantName)
				    .detail("TenantId", createdEntry.id);
				self->createdTenants[createdEntry.id] =
				    TestTenantData(tenantName, createdEntry.assignedCluster, createdEntry.tenantGroup);
				self->tenantNameIndex[tenantName] = createdEntry.id;
				auto& dataDb = self->dataDbs[createdEntry.assignedCluster];
				dataDb.tenants.insert(createdEntry.id);
				if (createdEntry.tenantGroup.present()) {
					auto& tenantGroupData = self->tenantGroups[createdEntry.tenantGroup.get()];
					tenantGroupData.cluster = createdEntry.assignedCluster;
					tenantGroupData.tenants.insert(createdEntry.id);
					dataDb.tenantGroups.insert(createdEntry.tenantGroup.get());
				}
				return Void();
			} catch (Error& e) {
				if (e.code() != error_code_metacluster_no_capacity) {
					throw;
				}

				wait(increaseMetaclusterCapacity(self));
			}
		}
	}

	ACTOR static Future<bool> verifyTenantLocations(MetaclusterMoveWorkload* self,
	                                                TenantGroupName tenantGroup,
	                                                ClusterName expectedCluster) {
		state DataClusterData clusterData = self->dataDbs[expectedCluster];
		state Reference<ReadYourWritesTransaction> tr = clusterData.db->createTransaction();

		loop {
			try {
				state std::vector<std::pair<TenantName, int64_t>> tenantData =
				    wait(metacluster::listTenantGroupTenantsTransaction(tr,
				                                                        tenantGroup,
				                                                        TenantName(""_sr),
				                                                        TenantName("\xff"_sr),
				                                                        CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));
				for (const auto& [tName, tId] : tenantData) {
					auto testData = self->createdTenants[tId];
					if (testData.cluster != expectedCluster || !testData.tenantGroup.present() ||
					    testData.tenantGroup.get() != tenantGroup) {
						TraceEvent("MetaclusterMoveVerifyTenantLocationsFailed")
						    .detail("ExpectedTenantGroup", tenantGroup)
						    .detail("ActualTenantGroup", testData.tenantGroup)
						    .detail("ExpectedCluster", expectedCluster)
						    .detail("ActualCluster", testData.cluster);
						return false;
					}
				}

				return true;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<bool> verifyMoveMetadataErased(MetaclusterMoveWorkload* self, TenantGroupName tenantGroup) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Optional<metacluster::metadata::management::MovementRecord> mr;
				state
				    KeyBackedRangeResult<std::pair<std::pair<TenantGroupName, std::string>, std::pair<TenantName, Key>>>
				        mq;
				state KeyBackedRangeResult<std::pair<Tuple, Key>> splitPoints;

				state Tuple beginTuple = Tuple::makeTuple(tenantGroup, "", TenantName(""_sr), KeyRef(""_sr));
				state Tuple endTuple = Tuple::makeTuple(tenantGroup, "\xff", TenantName("\xff"_sr), KeyRef("\xff"_sr));
				state std::pair<TenantGroupName, std::string> beginPair = std::make_pair(tenantGroup, "");
				state std::pair<TenantGroupName, std::string> endPair = std::make_pair(tenantGroup, "\xff");
				state int limit = 10;

				state Future<Void> mrFuture = store(
				    mr,
				    metacluster::metadata::management::emergency_movement::emergencyMovements().get(tr, tenantGroup));
				state Future<Void> mqFuture =
				    store(mq,
				          metacluster::metadata::management::emergency_movement::movementQueue().getRange(
				              tr, beginPair, endPair, limit));
				state Future<Void> splitPointsFuture =
				    store(splitPoints,
				          metacluster::metadata::management::emergency_movement::splitPointsMap().getRange(
				              tr, beginTuple, endTuple, limit));

				wait(mrFuture && mqFuture && splitPointsFuture);

				TraceEvent("MetaclusterMoveVerifyMetadataErased")
				    .detail("MrPresent", mr.present())
				    .detail("MqEmpty", mq.results.empty())
				    .detail("SplitpointsEmpty", splitPoints.results.empty());
				return (!mr.present() && mq.results.empty() && splitPoints.results.empty());
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR static Future<bool> finishVerification(MetaclusterMoveWorkload* self,
	                                             TenantGroupName tenantGroup,
	                                             ClusterName expectedCluster) {
		state bool locationSuccess = wait(verifyTenantLocations(self, tenantGroup, expectedCluster));
		state bool eraseSuccess = wait(verifyMoveMetadataErased(self, tenantGroup));
		return locationSuccess && eraseSuccess;
	}

	// returns true if move is aborted
	ACTOR static Future<bool> abortMove(MetaclusterMoveWorkload* self,
	                                    TenantGroupName tenantGroup,
	                                    ClusterName srcCluster,
	                                    ClusterName dstCluster) {
		loop {
			try {
				TraceEvent("MetaclusterMoveAbortBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				Future<Void> abortFuture =
				    metacluster::abortTenantMovement(self->managementDb, tenantGroup, srcCluster, dstCluster);
				Optional<Void> result = wait(timeout(abortFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterMoveAbortComplete")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster);
					if (self->tenantGroups[tenantGroup].cluster == dstCluster) {
						updateTestData(self, tenantGroup, srcCluster, dstCluster);
					}
					break;
				}
				TraceEvent("MetaclusterMoveAbortTimedOut")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				CODE_PROBE(true, "Metacluster move start timed out");
			} catch (Error& e) {
				TraceEvent("MetaclusterMoveAbortFailed")
				    .error(e)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				// Abort failed
				if (e.code() == error_code_invalid_tenant_move) {
					return false;
				}
				// If the move record is missing, the operation likely completed
				// and this is a retry OR this is aborting after a finish completed
				if (e.code() == error_code_tenant_move_record_missing) {
					return true;
				}
				throw e;
			}
		}
		return true;
	}

	// returns true if move is aborted
	ACTOR static Future<bool> startMove(MetaclusterMoveWorkload* self,
	                                    TenantGroupName tenantGroup,
	                                    ClusterName srcCluster,
	                                    ClusterName dstCluster) {
		loop {
			try {
				TraceEvent("MetaclusterMoveStartBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				Future<Void> startFuture =
				    metacluster::startTenantMovement(self->managementDb, tenantGroup, srcCluster, dstCluster);
				Optional<Void> result = wait(timeout(startFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterMoveStartComplete")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster);
					break;
				}
				TraceEvent("MetaclusterMoveStartTimedOut")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				CODE_PROBE(true, "Metacluster move start timed out");
				if (deterministicRandom()->random01() < 0.2) {
					bool aborted = wait(abortMove(self, tenantGroup, srcCluster, dstCluster));
					TraceEvent("MetaclusterMoveStartAttemptedAbort")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster)
					    .detail("AbortedSuccessfully", aborted);
					if (aborted) {
						return aborted;
					}
				}
			} catch (Error& e) {
				state Error err(e);
				TraceEvent("MetaclusterMoveWorkloadStartFailed")
				    .error(err)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				if (err.code() == error_code_metacluster_no_capacity) {
					wait(increaseMetaclusterCapacity(self));
					continue;
				}
				throw err;
			}
		}
		return false;
	}

	// returns true if move is aborted
	ACTOR static Future<bool> switchMove(MetaclusterMoveWorkload* self,
	                                     TenantGroupName tenantGroup,
	                                     ClusterName srcCluster,
	                                     ClusterName dstCluster) {
		state std::vector<std::string> messages;
		loop {
			try {
				TraceEvent("MetaclusterMoveSwitchBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				Future<Void> switchFuture = metacluster::switchTenantMovement(
				    self->managementDb, tenantGroup, srcCluster, dstCluster, &messages);
				Optional<Void> result = wait(timeout(switchFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterMoveSwitchComplete")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster);
					if (self->tenantGroups[tenantGroup].cluster == srcCluster) {
						updateTestData(self, tenantGroup, dstCluster, srcCluster);
					}
					break;
				}
				TraceEvent("MetaclusterMoveSwitchTimedOut")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				CODE_PROBE(true, "Metacluster move switch timed out");
				if (deterministicRandom()->random01() < 0.2) {
					bool aborted = wait(abortMove(self, tenantGroup, srcCluster, dstCluster));
					TraceEvent("MetaclusterMoveSwitchAttemptedAbort")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster)
					    .detail("AbortedSuccessfully", aborted);
					if (aborted) {
						return aborted;
					}
				}
			} catch (Error& e) {
				TraceEvent("MetaclusterMoveWorkloadSwitchFailed")
				    .error(e)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				if (e.code() == error_code_tenant_move_failed) {
					// Retryable error
					continue;
				} else if (e.code() == error_code_invalid_tenant_move) {
					// Completed and retry threw error
					return false;
				}
				throw;
			}
		}
		return false;
	}

	// returns true if move is aborted
	ACTOR static Future<bool> finishMove(MetaclusterMoveWorkload* self,
	                                     TenantGroupName tenantGroup,
	                                     ClusterName srcCluster,
	                                     ClusterName dstCluster) {
		// finish tenant move will fail to abort if movement record has been updated
		// expect this case and keep trying to finish
		loop {
			try {
				TraceEvent("MetaclusterMoveFinishBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				Future<Void> finishFuture =
				    metacluster::finishTenantMovement(self->managementDb, tenantGroup, srcCluster, dstCluster);
				Optional<Void> result = wait(timeout(finishFuture, deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					TraceEvent(SevDebug, "MetaclusterMoveFinishComplete")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster);
					break;
				}
				TraceEvent("MetaclusterMoveFinishTimedOut")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				CODE_PROBE(true, "Metacluster move finish timed out");
				if (deterministicRandom()->random01() < 0.2) {
					// Keep track of move record before calling abort
					state Optional<metacluster::metadata::management::MovementRecord> moveRecord =
					    wait(self->tryGetMoveRecord(tenantGroup));
					// Move completed and metadata was erased
					if (!moveRecord.present()) {
						return false;
					}
					state bool aborted = wait(abortMove(self, tenantGroup, srcCluster, dstCluster));
					TraceEvent("MetaclusterMoveFinishAttemptedAbort")
					    .detail("TenantGroup", tenantGroup)
					    .detail("SourceCluster", srcCluster)
					    .detail("DestinationCluster", dstCluster)
					    .detail("AbortedSuccessfully", aborted);
					if (aborted) {
						// This scenario can be reached when a finishMove retries, completes, times out,
						// and then abort is attempted. Return false in this case to verify the correct data
						if (moveRecord.get().mState ==
						    metacluster::metadata::management::MovementState::FINISH_UNLOCK) {
							return false;
						}
						return aborted;
					}
				}
			} catch (Error& e) {
				TraceEvent("MetaclusterMoveWorkloadFinishFailed")
				    .error(e)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				// If the move record is missing, the operation likely completed
				// and this is a retry
				if (e.code() == error_code_tenant_move_record_missing) {
					return false;
				}
				throw;
			}
		}
		return false;
	}

	ACTOR static Future<Void> _setup(Database cx, MetaclusterMoveWorkload* self) {
		metacluster::DataClusterEntry clusterEntry;
		clusterEntry.capacity.numTenantGroups = self->tenantGroupCapacity;

		metacluster::util::SimulatedMetacluster simMetacluster = wait(metacluster::util::createSimulatedMetacluster(
		    cx,
		    deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                     TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1),
		    clusterEntry));

		self->managementDb = simMetacluster.managementDb;
		ASSERT(!simMetacluster.dataDbs.empty());
		for (auto const& [name, db] : simMetacluster.dataDbs) {
			self->dataDbs[name] = DataClusterData(db);
			self->dataDbIndex.push_back(name);
		}

		TraceEvent(SevDebug, "MetaclusterMoveWorkloadCreateTenants").detail("NumTenants", self->initialTenants);

		while (self->createdTenants.size() < self->initialTenants) {
			wait(createTenant(self));
		}

		TraceEvent(SevDebug, "MetaclusterMoveWorkloadCreateTenantsComplete");

		// bulkSetup usually expects all clients
		// but this is being circumvented by the clientId==0 check
		self->clientCount = 1;
		TraceEvent("BreakpointStuck1");
		// container of range-based for with continuation must be a state variable
		state std::map<ClusterName, DataClusterData> dataDbs = self->dataDbs;
		for (auto const& [clusterName, dataDb] : dataDbs) {
			state std::vector<Reference<Tenant>> dataTenants;
			state Database dbObj = dataDb.db;
			// Iterate over each data cluster and attempt to fill some of the tenants with data
			for (auto const& tId : dataDb.tenants) {
				TestTenantData testData = self->createdTenants[tId];
				TenantName tName = testData.name;
				TenantLookupInfo const tenantLookupInfo(tId, testData.tenantGroup);
				dataTenants.push_back(makeReference<Tenant>(tenantLookupInfo, tName));
			}
			if (dataTenants.size()) {
				wait(bulkSetup(dbObj,
				               self,
				               self->nodeCount,
				               Promise<double>(),
				               false,
				               0.0,
				               1e12,
				               std::vector<uint64_t>(),
				               Promise<std::vector<std::pair<uint64_t, double>>>(),
				               0,
				               0.1,
				               0,
				               0,
				               dataTenants));
			}
			TraceEvent("BreakpointStuck2");
			// Blobbify all tenants
			std::vector<Future<bool>> blobFutures;
			for (Reference<Tenant> tenantRef : dataTenants) {
				blobFutures.push_back(dbObj->blobbifyRangeBlocking(normalKeys, tenantRef));
			}
			TraceEvent("BreakpointStuck3");
			wait(waitForAll(blobFutures));
			TraceEvent("BreakpointStuck4");
		}

		// Set storage and tag quotas
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				for (auto const& [tGroupName, tGroupData] : self->tenantGroups) {
					ThrottleApi::setTagQuota(tr, tGroupName, self->reservedQuota, self->totalQuota);
					TenantMetadata::storageQuota().set(tr, tGroupName, self->storageQuota);
				}
				wait(safeThreadFutureToFuture(tr->commit()));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		TraceEvent(SevDebug, "MetaclusterMoveWorkloadPopulateTenantDataComplete");

		return Void();
	}

	struct MoveBlock {
		TenantName tenant;
		Key begin;
		Key end;

		MoveBlock(TenantName tenant, Key begin, Key end) : tenant(tenant), begin(begin), end(end) {}
	};

	ACTOR static Future<Void> copyTenantData(MetaclusterMoveWorkload* self,
	                                         TenantGroupName tenantGroup,
	                                         MoveBlock moveBlock,
	                                         ClusterName srcDb,
	                                         ClusterName dstDb) {
		state KeyRangeRef keyRange(moveBlock.begin, moveBlock.end);

		state Database srcDbObj = self->dataDbs[srcDb].db;
		state Reference<Tenant> srcTenant = makeReference<Tenant>(srcDbObj, moveBlock.tenant);
		state Reference<ReadYourWritesTransaction> srcTr =
		    makeReference<ReadYourWritesTransaction>(srcDbObj, srcTenant);

		state RangeReadResult srcRange;
		loop {
			try {
				srcTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(store(srcRange, srcTr->getRange(keyRange, 100000)));
				ASSERT(!srcRange.more);
				break;
			} catch (Error& e) {
				wait(srcTr->onError(e));
			}
		}

		TraceEvent("BreakpointSrcRange")
		    .detail("TenantName", moveBlock.tenant)
		    .detail("KeyRange", keyRange)
		    .detail("SrcRangeSize", srcRange.size());

		state Database dstDbObj = self->dataDbs[dstDb].db;
		state Reference<Tenant> dstTenant = makeReference<Tenant>(dstDbObj, moveBlock.tenant);
		state Reference<ReadYourWritesTransaction> dstTr =
		    makeReference<ReadYourWritesTransaction>(dstDbObj, dstTenant);
		loop {
			try {
				dstTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				for (const auto& [k, v] : srcRange) {
					dstTr->set(k, v);
				}
				TraceEvent("BreakpointCopyCommit1").detail("TenantName", moveBlock.tenant);
				wait(dstTr->commit());
				TraceEvent("BreakpointCopyCommit2").detail("TenantName", moveBlock.tenant);
				break;
			} catch (Error& e) {
				wait(dstTr->onError(e));
			}
		}

		return Void();
	}

	ACTOR static Future<Optional<MoveBlock>> getNextMoveBlock(MetaclusterMoveWorkload* self,
	                                                          Reference<ITransaction> tr,
	                                                          TenantGroupName tenantGroup) {
		state UID runId = self->moveRecord.runId;
		tr->setOption(FDBTransactionOptions::RAW_ACCESS);

		state Optional<std::pair<TenantName, Key>> optionalQueueHead =
		    wait(metacluster::metadata::management::emergency_movement::movementQueue().get(
		        tr, std::make_pair(tenantGroup, runId.toString())));

		state Tuple endTuple = Tuple::makeTuple(tenantGroup, runId.toString(), TenantName("\xff"_sr));

		if (!optionalQueueHead.present()) {
			KeyBackedRangeResult<std::pair<Tuple, Key>> firstSplitPoint =
			    wait(metacluster::metadata::management::emergency_movement::splitPointsMap().getRange(
			        tr, Tuple::makeTuple(tenantGroup, runId.toString()), endTuple, 1));

			if (firstSplitPoint.results.empty()) {
				return {};
			}

			auto const& headTuple = firstSplitPoint.results[0].first;
			optionalQueueHead = std::make_pair(headTuple.getString(2), headTuple.getString(3));
		}

		state std::pair<TenantName, Key> queueHead = optionalQueueHead.get();

		state Tuple beginTuple = Tuple::makeTuple(tenantGroup, runId.toString(), queueHead.first, queueHead.second);

		state KeyBackedRangeResult<std::pair<Tuple, Key>> splitPoints =
		    wait(metacluster::metadata::management::emergency_movement::splitPointsMap().getRange(
		        tr, beginTuple, endTuple, 2));
		TraceEvent("BreakpointSplitPoints")
		    .detail("BeginTuple", Tuple::tupleToString(beginTuple))
		    .detail("EndTuple", Tuple::tupleToString(endTuple));

		ASSERT(!splitPoints.results.empty());
		state KeyRef headEnd = splitPoints.results[0].second;

		if (splitPoints.results.size() == 2) {
			Tuple nextTuple = splitPoints.results[1].first;
			TenantName nextTenantName = nextTuple.getString(2);
			Key nextKey = nextTuple.getString(3);
			// The tuple ending with "\xff" will not exist as a key in splitPointsMap
			// The next Tuple key should have the next tenant with the empty key ""
			if (headEnd != nextKey) {
				ASSERT_NE(nextTenantName, queueHead.first);
				ASSERT_EQ(headEnd, "\xff"_sr);
				ASSERT_EQ(nextKey, ""_sr);
			}
			metacluster::metadata::management::emergency_movement::movementQueue().set(
			    tr, std::make_pair(tenantGroup, runId.toString()), std::make_pair(nextTenantName, nextKey));
		} else {
			metacluster::metadata::management::emergency_movement::movementQueue().erase(
			    tr, std::make_pair(tenantGroup, runId.toString()));
		}

		return MoveBlock(queueHead.first, queueHead.second, headEnd);
	}

	ACTOR static Future<Void> processQueue(MetaclusterMoveWorkload* self,
	                                       TenantGroupName tenantGroup,
	                                       ClusterName srcDb,
	                                       ClusterName dstDb) {
		state UID runId = self->moveRecord.runId;
		loop {
			state Optional<MoveBlock> block = wait(runTransaction(
			    self->managementDb, [self = self, tenantGroup = tenantGroup](Reference<ITransaction> tr) {
				    return getNextMoveBlock(self, tr, tenantGroup);
			    }));

			if (!block.present()) {
				return Void();
			}
			TraceEvent("BreakpointMoveBlock")
			    .detail("Tenant", block.get().tenant)
			    .detail("Begin", block.get().begin)
			    .detail("End", block.get().end);

			wait(copyTenantData(self, tenantGroup, block.get(), srcDb, dstDb));
			TraceEvent("BreakpointCopy")
			    .detail("Begin", block.get().begin)
			    .detail("End", block.get().end)
			    .detail("TenantGroup", tenantGroup)
			    .detail("TenantName", block.get().tenant);

			wait(runTransactionVoid(
			    self->managementDb,
			    [runId = runId, tenantGroup = tenantGroup, block = block](Reference<ITransaction> tr) {
				    tr->setOption(FDBTransactionOptions::RAW_ACCESS);
				    metacluster::metadata::management::emergency_movement::splitPointsMap().erase(
				        tr, Tuple::makeTuple(tenantGroup, runId.toString(), block.get().tenant, block.get().begin));
				    return Future<Void>(Void());
			    }));
		}
	}

	Future<Optional<metacluster::metadata::management::MovementRecord>> tryGetMoveRecord(TenantGroupName tenantGroup) {
		return runTransaction(managementDb, [tenantGroup](Reference<ITransaction> tr) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			return map(metacluster::metadata::management::emergency_movement::emergencyMovements().get(tr, tenantGroup),
			           [](Optional<metacluster::metadata::management::MovementRecord> record) { return record; });
		});
	}

	Future<metacluster::metadata::management::MovementRecord> getMoveRecord(TenantGroupName tenantGroup) {
		return runTransaction(managementDb, [tenantGroup](Reference<ITransaction> tr) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			return map(metacluster::metadata::management::emergency_movement::emergencyMovements().get(tr, tenantGroup),
			           [](Optional<metacluster::metadata::management::MovementRecord> record) {
				           ASSERT(record.present());
				           return record.get();
			           });
		});
	}

	// TODO: update the test to run arbitrary stages despite the current state to verify that it doesn’t break anything
	// (i.e. run switch on a move that has already switched or aborted, etc.)
	ACTOR static Future<Void> _start(Database cx, MetaclusterMoveWorkload* self) {
		// Expect an error if the same cluster is picked
		state ClusterName srcCluster = self->chooseClusterName();
		state ClusterName dstCluster = self->chooseClusterName();

		Optional<TenantGroupName> optionalTenantGroup;
		while (!optionalTenantGroup.present()) {
			optionalTenantGroup = self->chooseTenantGroup(srcCluster);
		}
		state TenantGroupName tenantGroup = optionalTenantGroup.get();
		state bool aborted;
		TraceEvent("BreakpointTestStart")
		    .detail("Src", srcCluster)
		    .detail("Dst", dstCluster)
		    .detail("TenantGroup", tenantGroup)
		    .detail("GroupsOnSrc", self->dataDbs[srcCluster].tenantGroups.size())
		    .detail("GroupsOnDst", self->dataDbs[dstCluster].tenantGroups.size());
		loop {
			try {
				wait(store(aborted, startMove(self, tenantGroup, srcCluster, dstCluster)));
				if (aborted) {
					break;
				}
				// If start completes successfully, the move identifier should be written
				wait(store(self->moveRecord, self->getMoveRecord(tenantGroup)));
				wait(processQueue(self, tenantGroup, srcCluster, dstCluster));
				wait(store(aborted, switchMove(self, tenantGroup, srcCluster, dstCluster)));
				if (aborted) {
					break;
				}
				wait(store(self->moveRecord, self->getMoveRecord(tenantGroup)));

				wait(store(aborted, finishMove(self, tenantGroup, srcCluster, dstCluster)));
				break;
			} catch (Error& e) {
				state Error err(e);
				TraceEvent("MetaclusterMoveWorkloadError").error(err);
				if (err.code() == error_code_invalid_tenant_move && srcCluster == dstCluster) {
					TraceEvent("MetaclusterMoveWorkloadSameSrcDst")
					    .detail("TenantGroup", tenantGroup)
					    .detail("ClusterName", srcCluster);
					// Change dst cluster since src is linked to the tenant group
					dstCluster = self->chooseClusterName();
					continue;
				}
				throw err;
			}
		}
		bool success = wait(finishVerification(self, tenantGroup, aborted ? srcCluster : dstCluster));
		if (!success) {
			TraceEvent("MetaclusterMoveFinalVerificationFailed")
			    .detail("Aborted", aborted)
			    .detail("TenantGroup", tenantGroup)
			    .detail("DestinationCluster", dstCluster)
			    .detail("SourceCluster", srcCluster);
			throw operation_failed();
		}

		return Void();
	}

	ACTOR static Future<bool> _check(MetaclusterMoveWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, metacluster::util::AllowPartialMetaclusterOperations::False);

		wait(metaclusterConsistencyCheck.run());

		return true;
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(this);
		} else {
			return true;
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }

	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }
};

WorkloadFactory<MetaclusterMoveWorkload> MetaclusterMoveWorkloadFactory;