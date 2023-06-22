/*
 * MetaclusterManagementWorkload.actor.cpp
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
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/BooleanParam.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"
#include "fdbserver/ServerDBInfo.actor.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterManagementWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterManagement";

	struct TenantTestData : ReferenceCounted<TenantTestData> {
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;
		Optional<UID> lockId;
		TenantAPI::TenantLockState lockState = TenantAPI::TenantLockState::UNLOCKED;

		TenantTestData() {}
		TenantTestData(ClusterName cluster, Optional<TenantGroupName> tenantGroup)
		  : cluster(cluster), tenantGroup(tenantGroup) {}
	};

	struct TenantGroupData : ReferenceCounted<TenantGroupData> {
		ClusterName cluster;
		std::set<TenantName> tenants;

		TenantGroupData() {}
		TenantGroupData(ClusterName cluster) : cluster(cluster) {}
	};

	struct DataClusterData : ReferenceCounted<DataClusterData> {
		Database db;
		bool registered = false;
		bool detached = false;
		int tenantGroupCapacity = 0;
		MetaclusterVersion version;

		std::map<TenantName, Reference<TenantTestData>> tenants;
		std::map<TenantGroupName, Reference<TenantGroupData>> tenantGroups;
		std::set<TenantName> ungroupedTenants;

		metacluster::AutoTenantAssignment autoTenantAssignment = metacluster::AutoTenantAssignment::ENABLED;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	bool metaclusterCreated;

	Reference<IDatabase> managementDb;
	MetaclusterVersion managementVersion;

	std::map<ClusterName, Reference<DataClusterData>> dataDbs;
	std::map<TenantGroupName, Reference<TenantGroupData>> tenantGroups;
	std::set<TenantName> ungroupedTenants;
	std::vector<ClusterName> dataDbIndex;

	int64_t totalTenantGroupCapacity = 0;
	std::map<TenantName, Reference<TenantTestData>> createdTenants;

	int maxTenants;
	int maxTenantGroups;
	bool allowTenantIdPrefixReuse;
	int64_t tenantIdPrefix = -1;
	std::set<int64_t> usedPrefixes;
	double testDuration;
	bool useExistingMetacluster;

	MetaclusterManagementWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), metaclusterCreated(deterministicRandom()->coinflip()) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		useExistingMetacluster = getOption(options, "useExistingMetacluster"_sr, false);
		allowTenantIdPrefixReuse = deterministicRandom()->coinflip();
		MetaclusterRegistrationEntry::allowUnsupportedRegistrationWrites = true;
	}

	Optional<int64_t> generateTenantIdPrefix() {
		for (int i = 0; i < 20; ++i) {
			int64_t newPrefix = deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
			                                                     TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1);
			if (allowTenantIdPrefixReuse || !usedPrefixes.count(newPrefix)) {
				CODE_PROBE(usedPrefixes.count(newPrefix), "Reusing tenant ID prefix", probe::decoration::rare);
				return newPrefix;
			}
		}

		return false;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			if (!useExistingMetacluster && g_network->isSimulated() && BUGGIFY) {
				IKnobCollection::getMutableGlobalKnobCollection().setKnob(
				    "max_tenants_per_cluster", KnobValueRef::create(int{ deterministicRandom()->randomInt(20, 100) }));
			}
			return _setup(cx, this);
		} else {
			return Void();
		}
	}
	ACTOR static Future<Void> _setup(Database cx, MetaclusterManagementWorkload* self) {
		ASSERT(g_simulator->extraDatabases.size() > 0);

		if (!self->useExistingMetacluster) {
			if (self->metaclusterCreated) {
				self->tenantIdPrefix = self->generateTenantIdPrefix().get();
				self->usedPrefixes.insert(self->tenantIdPrefix);
			}

			metacluster::util::SimulatedMetacluster simMetacluster = wait(metacluster::util::createSimulatedMetacluster(
			    cx,
			    self->tenantIdPrefix,
			    Optional<metacluster::DataClusterEntry>(),
			    metacluster::util::SkipMetaclusterCreation(!self->metaclusterCreated)));

			self->managementDb = simMetacluster.managementDb;
			for (auto const& [name, db] : simMetacluster.dataDbs) {
				self->dataDbIndex.push_back(name);
				self->dataDbs[name] = makeReference<DataClusterData>(db);
			}

			if (self->metaclusterCreated) {
				Optional<MetaclusterRegistrationEntry> registration =
				    wait(metacluster::metadata::metaclusterRegistration().get(self->managementDb));
				ASSERT(registration.present());
				self->managementVersion = registration.get().version;
			}
		} else {
			metacluster::util::SimulatedMetacluster simMetacluster = wait(metacluster::util::createSimulatedMetacluster(
			    cx, {}, {}, metacluster::util::SkipMetaclusterCreation::True));
			self->managementDb = simMetacluster.managementDb;
			wait(loadExistingMetacluster(self));
		}

		return Void();
	}

	ACTOR static Future<Void> loadExistingMetacluster(MetaclusterManagementWorkload* self) {
		state metacluster::util::MetaclusterData<IDatabase> metaclusterData(self->managementDb);
		wait(metaclusterData.load());

		self->metaclusterCreated = true;
		self->managementVersion = metaclusterData.managementMetadata.metaclusterRegistration.get().version;
		self->tenantIdPrefix = metaclusterData.managementMetadata.tenantIdPrefix.get();
		self->usedPrefixes.insert(self->tenantIdPrefix);

		for (auto const& [name, metadata] : metaclusterData.managementMetadata.dataClusters) {
			metacluster::util::MetaclusterData<IDatabase>::DataClusterData dataClusterMetadata =
			    metaclusterData.dataClusterMetadata[name];

			self->dataDbIndex.push_back(name);
			Reference<DataClusterData> dataDb = makeReference<DataClusterData>(
			    Database::createSimulatedExtraDatabase(metadata.connectionString.toString()));
			self->dataDbs[name] = dataDb;

			dataDb->registered = true;
			dataDb->detached = false;
			dataDb->tenantGroupCapacity = metadata.entry.capacity.numTenantGroups;
			dataDb->version = dataClusterMetadata.metaclusterRegistration.get().version;
			dataDb->autoTenantAssignment = metadata.entry.autoTenantAssignment;

			self->totalTenantGroupCapacity +=
			    std::max(metadata.entry.capacity.numTenantGroups, metadata.entry.allocated.numTenantGroups);
		}

		for (auto const& [id, entry] : metaclusterData.managementMetadata.tenantData.tenantMap) {
			Reference<TenantTestData> tenantData =
			    makeReference<TenantTestData>(entry.assignedCluster, entry.tenantGroup);
			self->createdTenants[entry.tenantName] = tenantData;

			Reference<DataClusterData> dataDb = self->dataDbs[entry.assignedCluster];
			dataDb->tenants[entry.tenantName] = tenantData;

			tenantData->lockId = entry.tenantLockId;
			tenantData->lockState = entry.tenantLockState;

			if (!entry.tenantGroup.present()) {
				self->ungroupedTenants.insert(entry.tenantName);
				dataDb->ungroupedTenants.insert(entry.tenantName);
			} else {
				auto itr = self->tenantGroups.find(entry.tenantGroup.get());
				if (itr == self->tenantGroups.end()) {
					itr =
					    self->tenantGroups
					        .try_emplace(entry.tenantGroup.get(), makeReference<TenantGroupData>(entry.assignedCluster))
					        .first;
				}
				auto tenantGroup = itr->second;
				tenantGroup->tenants.insert(entry.tenantName);
				dataDb->tenantGroups[entry.tenantGroup.get()] = tenantGroup;
			}
		}

		return Void();
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant%08d", deterministicRandom()->randomInt(0, maxTenants)));
		return tenant;
	}

	Optional<TenantGroupName> chooseTenantGroup() {
		Optional<TenantGroupName> tenantGroup;
		if (deterministicRandom()->coinflip()) {
			tenantGroup =
			    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
		}

		return tenantGroup;
	}

	bool isValidVersion(Optional<Reference<DataClusterData>> dataDb = {}) {
		bool managementValid = managementVersion >= MetaclusterVersion::MIN_SUPPORTED &&
		                       managementVersion <= MetaclusterVersion::MAX_SUPPORTED;

		bool dataValid = !dataDb.present() || (dataDb.get()->version >= MetaclusterVersion::MIN_SUPPORTED &&
		                                       dataDb.get()->version <= MetaclusterVersion::MAX_SUPPORTED);

		CODE_PROBE(!managementValid, "Management cluster invalid version");
		CODE_PROBE(!dataValid, "Data cluster invalid version");
		return managementValid && dataValid;
	}

	ACTOR static Future<Void> waitForFullyRecovered(MetaclusterManagementWorkload* self) {
		while (self->dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		return Void();
	}

	ACTOR template <class DB>
	static Future<Void> setMetaclusterVersion(Reference<DB> db, MetaclusterVersion version) {
		state Reference<typename DB::TransactionT> tr = db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Optional<UnversionedMetaclusterRegistrationEntry> registration =
				    wait(metacluster::metadata::unversionedMetaclusterRegistration().get(tr));
				ASSERT(registration.present());

				if (registration.get().version == version) {
					return Void();
				}

				MetaclusterRegistrationEntry newRegistration(registration.get());
				newRegistration.version = version;
				metacluster::metadata::metaclusterRegistration().set(tr, newRegistration);
				wait(safeThreadFutureToFuture(tr->commit()));

				// The metacluster registration entry causes a recovery, so it cannot succeed
				ASSERT(false);
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	Future<Void> checkAndResetMetaclusterVersion(ClusterName clusterName) {
		auto dataDbItr = dataDbs.find(clusterName);
		ASSERT(dataDbItr != dataDbs.end());
		ASSERT(!isValidVersion(dataDbItr->second));

		dataDbItr->second->version = (MetaclusterVersion)deterministicRandom()->randomInt(
		    (int)MetaclusterVersion::MIN_SUPPORTED, (int)MetaclusterVersion::MAX_SUPPORTED + 1);

		return setMetaclusterVersion(dataDbItr->second->db.getReference(), dataDbItr->second->version);
	}

	ACTOR static Future<Void> createMetacluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = "metacluster"_sr;
		if (deterministicRandom()->random01() < 0.01) {
			clusterName = clusterName.withPrefix("\xff"_sr);
		}

		try {
			state Optional<int64_t> newTenantIdPrefix = self->generateTenantIdPrefix();
			if (!newTenantIdPrefix.present()) {
				return Void();
			}

			Optional<std::string> result =
			    wait(metacluster::createMetacluster(self->managementDb, clusterName, newTenantIdPrefix.get(), false));

			ASSERT(!clusterName.startsWith("\xff"_sr));
			if (!result.present()) {
				ASSERT(!self->metaclusterCreated);
				self->metaclusterCreated = true;

				self->tenantIdPrefix = newTenantIdPrefix.get();
				self->usedPrefixes.insert(self->tenantIdPrefix);

				Optional<MetaclusterRegistrationEntry> registrationEntry =
				    wait(metacluster::metadata::metaclusterRegistration().get(self->managementDb));
				ASSERT(registrationEntry.present());

				self->managementVersion = registrationEntry.get().version;
				wait(waitForFullyRecovered(self));
			} else {
				ASSERT(self->metaclusterCreated);
			}
		} catch (Error& e) {
			if (e.code() == error_code_invalid_cluster_name) {
				ASSERT(clusterName.startsWith("\xff"_sr));
				return Void();
			} else if (e.code() == error_code_unsupported_metacluster_version) {
				ASSERT(!self->isValidVersion());
				return Void();
			}

			TraceEvent(SevError, "CreateMetaclusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> decommissionMetacluster(MetaclusterManagementWorkload* self) {
		state bool empty = self->createdTenants.empty();
		for (auto db : self->dataDbs) {
			empty = empty && !db.second->registered;
		}

		try {
			wait(metacluster::decommissionMetacluster(self->managementDb));
			ASSERT(self->metaclusterCreated);
			ASSERT(self->isValidVersion());
			ASSERT(empty);

			self->metaclusterCreated = false;
		} catch (Error& e) {
			if (e.code() == error_code_cluster_not_empty) {
				ASSERT(!empty);
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			} else if (e.code() == error_code_unsupported_metacluster_version) {
				ASSERT(!self->isValidVersion());
				return Void();
			}

			TraceEvent(SevError, "DecommissionMetaclusterFailure").error(e);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> registerCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state bool retried = false;

		if (deterministicRandom()->random01() < 0.1) {
			clusterName = "\xff"_sr.withSuffix(clusterName);
		}

		try {
			state metacluster::DataClusterEntry entry;
			entry.capacity.numTenantGroups = deterministicRandom()->randomInt(0, 4);
			if (deterministicRandom()->random01() < 0.25) {
				entry.autoTenantAssignment = metacluster::AutoTenantAssignment::DISABLED;
			}

			loop {
				try {
					Future<Void> registerFuture =
					    metacluster::registerCluster(self->managementDb,
					                                 clusterName,
					                                 dataDb->db->getConnectionRecord()->getConnectionString(),
					                                 entry);

					Optional<Void> result = wait(timeout(registerFuture, deterministicRandom()->randomInt(1, 30)));
					if (result.present()) {
						break;
					} else {
						retried = true;
					}
				} catch (Error& e) {
					state Error registerError = e;
					if (registerError.code() == error_code_cluster_removed ||
					    registerError.code() == error_code_cluster_not_empty) {
						if (registerError.code() == error_code_cluster_removed) {
							ASSERT(retried);
						} else if (registerError.code() == error_code_cluster_not_empty) {
							ASSERT(dataDb->detached);
						}

						wait(success(errorOr(metacluster::removeCluster(self->managementDb,
						                                                clusterName,
						                                                ClusterType::METACLUSTER_MANAGEMENT,
						                                                metacluster::ForceRemove::True))));

						return Void();
					} else if (registerError.code() == error_code_cluster_already_exists && retried &&
					           !dataDb->registered) {
						Optional<metacluster::DataClusterMetadata> clusterMetadata =
						    wait(metacluster::tryGetCluster(self->managementDb, clusterName));
						ASSERT(clusterMetadata.present());
						break;
					} else {
						throw registerError;
					}
				}
			}

			ASSERT(!clusterName.startsWith("\xff"_sr));
			ASSERT(!dataDb->registered);
			ASSERT(!dataDb->detached || dataDb->tenants.empty());
			ASSERT(self->isValidVersion());
			ASSERT(self->metaclusterCreated);

			Optional<MetaclusterRegistrationEntry> registration =
			    wait(metacluster::metadata::metaclusterRegistration().get(dataDb->db.getReference()));
			ASSERT(registration.present());
			dataDb->version = registration.get().version;

			dataDb->tenantGroupCapacity = entry.capacity.numTenantGroups;
			self->totalTenantGroupCapacity += entry.capacity.numTenantGroups;
			dataDb->registered = true;
			dataDb->detached = false;
			dataDb->autoTenantAssignment = entry.autoTenantAssignment;

			// Get a version to know that the cluster has recovered
			wait(success(runTransaction(dataDb->db.getReference(),
			                            [](Reference<ReadYourWritesTransaction> tr) { return tr->getReadVersion(); })));
		} catch (Error& e) {
			if (e.code() == error_code_cluster_already_exists) {
				ASSERT(dataDb->registered);
				return Void();
			} else if (e.code() == error_code_cluster_not_empty) {
				ASSERT(dataDb->detached && !dataDb->tenants.empty());
				return Void();
			} else if (e.code() == error_code_invalid_cluster_name) {
				ASSERT(clusterName.startsWith("\xff"_sr));
				return Void();
			} else if (e.code() == error_code_unsupported_metacluster_version) {
				// If we reach here, it is either because
				// 1) the management cluster version is invalid and the registration failed, thus the version is
				//    meaningless, or
				// 2) the data cluster is already registered and invalid.
				// If the data cluster is not registered, we shouldn't check its version.
				ASSERT(!self->isValidVersion(dataDb->registered ? dataDb : Optional<Reference<DataClusterData>>()));
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "RegisterClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> removeCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state bool retried = false;
		state metacluster::ForceRemove detachCluster(deterministicRandom()->coinflip());

		try {
			loop {
				Future<bool> removeFuture = metacluster::removeCluster(self->managementDb,
				                                                       clusterName,
				                                                       ClusterType::METACLUSTER_MANAGEMENT,
				                                                       metacluster::ForceRemove(detachCluster));
				try {
					Optional<bool> result = wait(timeout(removeFuture, deterministicRandom()->randomInt(1, 30)));
					if (result.present()) {
						ASSERT(result.get());
						break;
					} else {
						retried = true;
					}
				} catch (Error& e) {
					if (e.code() == error_code_cluster_not_found && retried && dataDb->registered) {
						Optional<metacluster::DataClusterMetadata> clusterMetadata =
						    wait(metacluster::tryGetCluster(self->managementDb, clusterName));

						ASSERT(!clusterMetadata.present());
						break;
					} else if (e.code() == error_code_unsupported_metacluster_version) {
						if (!self->isValidVersion()) {
							return Void();
						} else {
							wait(self->checkAndResetMetaclusterVersion(clusterName));
						}
					} else {
						throw;
					}
				}
			}

			ASSERT(dataDb->registered);
			ASSERT(detachCluster || dataDb->tenants.empty());
			ASSERT(self->isValidVersion(dataDb));
			ASSERT(self->metaclusterCreated);

			self->totalTenantGroupCapacity -= std::max<int64_t>(
			    dataDb->tenantGroups.size() + dataDb->ungroupedTenants.size(), dataDb->tenantGroupCapacity);
			dataDb->tenantGroupCapacity = 0;
			dataDb->registered = false;
			dataDb->autoTenantAssignment = metacluster::AutoTenantAssignment::ENABLED;

			if (detachCluster) {
				dataDb->detached = true;
				for (auto const& t : dataDb->ungroupedTenants) {
					self->ungroupedTenants.erase(t);
				}
				for (auto const& t : dataDb->tenantGroups) {
					self->tenantGroups.erase(t.first);
				}
				for (auto const& t : dataDb->tenants) {
					self->createdTenants.erase(t.first);
				}
			}

			// Get a version to know that the cluster has recovered
			wait(success(runTransaction(dataDb->db.getReference(),
			                            [](Reference<ReadYourWritesTransaction> tr) { return tr->getReadVersion(); })));
		} catch (Error& e) {
			if (e.code() == error_code_cluster_not_found) {
				ASSERT(!dataDb->registered);
				return Void();
			} else if (e.code() == error_code_cluster_not_empty && !detachCluster) {
				ASSERT(!dataDb->tenants.empty());
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "RemoveClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> removeFailedRestoredCluster(MetaclusterManagementWorkload* self,
	                                                      ClusterName clusterName) {
		// On retries, we need to remove the cluster if it was partially added
		try {
			wait(success(metacluster::removeCluster(
			    self->managementDb, clusterName, ClusterType::METACLUSTER_MANAGEMENT, metacluster::ForceRemove::True)));
		} catch (Error& e) {
			if (e.code() != error_code_cluster_not_found) {
				throw;
			}
		}

		return Void();
	}

	ACTOR static Future<bool> resolveCollisions(MetaclusterManagementWorkload* self,
	                                            ClusterName clusterName,
	                                            Reference<DataClusterData> dataDb) {
		state std::set<TenantName> tenantsToRemove;

		state bool foundTenantCollision = false;
		for (auto t : dataDb->tenants) {
			if (self->createdTenants.count(t.first)) {
				foundTenantCollision = true;
				tenantsToRemove.insert(t.first);
			}
		}

		state bool foundGroupCollision = false;
		for (auto t : dataDb->tenantGroups) {
			if (self->tenantGroups.count(t.first)) {
				foundGroupCollision = true;
				tenantsToRemove.insert(t.second->tenants.begin(), t.second->tenants.end());
			}
		}

		state Reference<ReadYourWritesTransaction> tr = dataDb->db->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state std::vector<Future<Optional<int64_t>>> getFutures;
				for (auto const& t : tenantsToRemove) {
					getFutures.push_back(TenantMetadata::tenantNameIndex().get(tr, t));
					auto tenantItr = dataDb->tenants.find(t);
					if (tenantItr != dataDb->tenants.end()) {
						if (tenantItr->second->tenantGroup.present()) {
							auto groupItr = dataDb->tenantGroups.find(tenantItr->second->tenantGroup.get());
							ASSERT(groupItr != dataDb->tenantGroups.end());
							groupItr->second->tenants.erase(t);
							if (groupItr->second->tenants.empty()) {
								dataDb->tenantGroups.erase(groupItr);
							}
						}
						dataDb->tenants.erase(tenantItr);
					}
					dataDb->ungroupedTenants.erase(t);
				}

				wait(waitForAll(getFutures));

				state std::vector<Future<Void>> deleteFutures;
				for (auto const& f : getFutures) {
					if (f.get().present()) {
						deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, f.get().get()));
					}
				}

				wait(waitForAll(deleteFutures));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		CODE_PROBE(foundTenantCollision, "Resolved tenant collision for restore", probe::decoration::rare);
		CODE_PROBE(foundGroupCollision, "Resolved group collision for restore");
		return foundTenantCollision || foundGroupCollision;
	}

	ACTOR static Future<Void> restoreCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state bool dryRun = deterministicRandom()->coinflip();
		state bool forceJoin = deterministicRandom()->coinflip();
		state bool forceReuseTenantIdPrefix = deterministicRandom()->coinflip();

		state std::vector<std::string> messages;
		state bool retried = false;

		loop {
			try {
				if (dataDb->detached) {
					if (retried) {
						wait(removeFailedRestoredCluster(self, clusterName));
					} else {
						// On the first try, we need to remove the metacluster registration entry from the data
						// cluster
						wait(success(metacluster::removeCluster(dataDb->db.getReference(),
						                                        clusterName,
						                                        ClusterType::METACLUSTER_DATA,
						                                        metacluster::ForceRemove::True)));
					}
				}

				Future<Void> restoreFuture =
				    metacluster::restoreCluster(self->managementDb,
				                                clusterName,
				                                dataDb->db->getConnectionRecord()->getConnectionString(),
				                                metacluster::ApplyManagementClusterUpdates(!dataDb->detached),
				                                metacluster::RestoreDryRun(dryRun),
				                                metacluster::ForceJoin(forceJoin),
				                                metacluster::ForceReuseTenantIdPrefix(forceReuseTenantIdPrefix),
				                                &messages);
				Optional<Void> result = wait(timeout(restoreFuture, deterministicRandom()->randomInt(1, 30)));
				if (!result.present()) {
					retried = true;
					continue;
				}

				ASSERT(self->metaclusterCreated);
				ASSERT(dataDb->registered || dataDb->detached);
				ASSERT(self->isValidVersion(dataDb));

				if (dataDb->detached && !dryRun) {
					dataDb->detached = false;
					dataDb->registered = true;
					for (auto const& t : dataDb->ungroupedTenants) {
						self->ungroupedTenants.insert(t);
					}
					for (auto const& t : dataDb->tenantGroups) {
						ASSERT(self->tenantGroups.try_emplace(t.first, t.second).second);
					}
					for (auto const& t : dataDb->tenants) {
						ASSERT(self->createdTenants.try_emplace(t.first, t.second).second);
						ASSERT(self->createdTenants[t.first]->cluster == clusterName);
					}

					self->totalTenantGroupCapacity += dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size();
				}

				break;
			} catch (Error& e) {
				state Error error = e;
				if (error.code() == error_code_conflicting_restore ||
				    error.code() == error_code_cluster_already_exists) {
					ASSERT(retried);
					CODE_PROBE(true,
					           "MetaclusterManagementWorkload: timed out restore conflicts with retried restore",
					           probe::decoration::rare);
					continue;
				} else if (error.code() == error_code_cluster_not_found) {
					ASSERT(!dataDb->registered);
					return Void();
				} else if (error.code() == error_code_tenant_already_exists ||
				           error.code() == error_code_invalid_tenant_configuration) {
					ASSERT(dataDb->detached);
					wait(removeFailedRestoredCluster(self, clusterName));

					bool resolvedCollisions = wait(resolveCollisions(self, clusterName, dataDb));
					if (resolvedCollisions) {
						continue;
					} else {
						// We attempted to restore a cluster with a tenant ID matching an existing tenant, which can
						// happen if we create a new metacluster with the same ID as an old one
						ASSERT(self->allowTenantIdPrefixReuse);
						ASSERT(!messages.empty());
						ASSERT(messages[0].find("has the same ID"));
						CODE_PROBE(true, "Reused tenant ID", probe::decoration::rare);
						return Void();
					}
				} else if (error.code() == error_code_invalid_metacluster_configuration) {
					ASSERT(!forceReuseTenantIdPrefix && dataDb->detached);
					wait(removeFailedRestoredCluster(self, clusterName));
					forceReuseTenantIdPrefix = true;
					continue;
				} else if (error.code() == error_code_unsupported_metacluster_version) {
					if (!self->isValidVersion()) {
						return Void();
					} else {
						wait(self->checkAndResetMetaclusterVersion(clusterName));
						continue;
					}
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(!self->metaclusterCreated);
					return Void();
				}

				TraceEvent(SevError, "RestoreClusterFailure").error(error).detail("ClusterName", clusterName);
				ASSERT(false);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> listClusters(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName1 = self->chooseClusterName();
		state ClusterName clusterName2 = self->chooseClusterName();
		state int limit = deterministicRandom()->randomInt(1, self->dataDbs.size() + 1);

		try {
			std::map<ClusterName, metacluster::DataClusterMetadata> clusterList =
			    wait(metacluster::listClusters(self->managementDb, clusterName1, clusterName2, limit));

			ASSERT(clusterName1 <= clusterName2);
			ASSERT(self->isValidVersion());
			ASSERT(self->metaclusterCreated);

			auto resultItr = clusterList.begin();

			int count = 0;
			for (auto localItr = self->dataDbs.find(clusterName1);
			     localItr != self->dataDbs.find(clusterName2) && count < limit;
			     ++localItr) {
				if (localItr->second->registered) {
					ASSERT(resultItr != clusterList.end());
					ASSERT(resultItr->first == localItr->first);
					ASSERT(resultItr->second.connectionString ==
					       localItr->second->db->getConnectionRecord()->getConnectionString());
					++resultItr;
					++count;
				}
			}

			ASSERT(resultItr == clusterList.end());
		} catch (Error& e) {
			if (e.code() == error_code_inverted_range) {
				ASSERT(clusterName1 > clusterName2);
				return Void();
			} else if (e.code() == error_code_unsupported_metacluster_version) {
				ASSERT(!self->isValidVersion());
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "ListClustersFailure")
			    .error(e)
			    .detail("BeginClusterName", clusterName1)
			    .detail("EndClusterName", clusterName2)
			    .detail("Limit", limit);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> getCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];

		try {
			metacluster::DataClusterMetadata clusterMetadata =
			    wait(metacluster::getCluster(self->managementDb, clusterName));

			ASSERT(dataDb->registered);
			ASSERT(self->isValidVersion());
			ASSERT(self->metaclusterCreated);
			ASSERT(dataDb->db->getConnectionRecord()->getConnectionString() == clusterMetadata.connectionString);
			ASSERT_EQ(dataDb->autoTenantAssignment, clusterMetadata.entry.autoTenantAssignment);
		} catch (Error& e) {
			if (e.code() == error_code_cluster_not_found) {
				ASSERT(!dataDb->registered);
				return Void();
			} else if (e.code() == error_code_unsupported_metacluster_version) {
				ASSERT(!self->isValidVersion());
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "GetClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Optional<metacluster::DataClusterEntry>> configureImpl(
	    MetaclusterManagementWorkload* self,
	    ClusterName clusterName,
	    Reference<DataClusterData> dataDb,
	    Optional<int64_t> numTenantGroups,
	    Optional<ClusterConnectionString> connectionString,
	    Optional<metacluster::AutoTenantAssignment> autoTenantAssignment) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
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

	ACTOR static Future<Void> configureCluster(MetaclusterManagementWorkload* self) {
		state ClusterName clusterName = self->chooseClusterName();
		state Reference<DataClusterData> dataDb = self->dataDbs[clusterName];
		state Optional<metacluster::DataClusterEntry> updatedEntry;

		state Optional<int64_t> newNumTenantGroups;
		state Optional<ClusterConnectionString> connectionString;
		state Optional<metacluster::AutoTenantAssignment> autoTenantAssignment;
		if (deterministicRandom()->coinflip()) {
			newNumTenantGroups = deterministicRandom()->randomInt(0, 4);
		}
		if (deterministicRandom()->coinflip()) {
			connectionString = dataDb->db->getConnectionRecord()->getConnectionString();
		}
		if (deterministicRandom()->coinflip()) {
			autoTenantAssignment = deterministicRandom()->coinflip() ? metacluster::AutoTenantAssignment::DISABLED
			                                                         : metacluster::AutoTenantAssignment::ENABLED;
		}

		try {
			loop {
				Optional<Optional<metacluster::DataClusterEntry>> result = wait(
				    timeout(configureImpl(
				                self, clusterName, dataDb, newNumTenantGroups, connectionString, autoTenantAssignment),
				            deterministicRandom()->randomInt(1, 30)));
				if (result.present()) {
					updatedEntry = result.get();
					break;
				}
			}

			ASSERT(self->metaclusterCreated);
			ASSERT(self->isValidVersion());

			if (updatedEntry.present()) {
				int allocatedGroups = dataDb->tenantGroups.size() + dataDb->ungroupedTenants.size();
				int64_t tenantGroupDelta =
				    std::max<int64_t>(updatedEntry.get().capacity.numTenantGroups, allocatedGroups) -
				    std::max<int64_t>(dataDb->tenantGroupCapacity, allocatedGroups);

				self->totalTenantGroupCapacity += tenantGroupDelta;
				dataDb->tenantGroupCapacity = updatedEntry.get().capacity.numTenantGroups;
				dataDb->autoTenantAssignment = updatedEntry.get().autoTenantAssignment;
			}
		} catch (Error& e) {
			if (e.code() == error_code_unsupported_metacluster_version) {
				ASSERT(!self->isValidVersion());
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "ConfigureClusterFailure").error(e).detail("ClusterName", clusterName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> verifyListFilter(MetaclusterManagementWorkload* self,
	                                           TenantName tenant,
	                                           const char* context) {
		try {
			state metacluster::MetaclusterTenantMapEntry checkEntry =
			    wait(metacluster::getTenant(self->managementDb, tenant));
			state metacluster::TenantState checkState = checkEntry.tenantState;
			state std::vector<metacluster::TenantState> filters;
			filters.push_back(checkState);

			state std::vector<std::pair<TenantName, metacluster::MetaclusterTenantMapEntry>> tenantList =
			    wait(metacluster::listTenantMetadata(self->managementDb, ""_sr, "\xff\xff"_sr, 10e6, 0, filters));
			// Possible to have changed state between now and the getTenant call above
			state metacluster::MetaclusterTenantMapEntry checkEntry2 =
			    wait(metacluster::getTenant(self->managementDb, tenant));
			DisabledTraceEvent(SevDebug, "VerifyListFilter")
			    .detail("Context", context)
			    .detail("Tenant", tenant)
			    .detail("CheckState", (int)checkState)
			    .detail("Entry2State", (int)checkEntry2.tenantState);

			bool found = false;
			for (auto pair : tenantList) {
				ASSERT(pair.second.tenantState == checkState);
				if (pair.first == tenant) {
					found = true;
				}
			}
			ASSERT(found || checkEntry2.tenantState != checkState);
		} catch (Error& e) {
			if (e.code() != error_code_tenant_not_found) {
				TraceEvent(SevError, "VerifyListFilterFailure").error(e).detail("Tenant", tenant);
				throw;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> createTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state Optional<TenantGroupName> tenantGroup = self->chooseTenantGroup();
		state metacluster::AssignClusterAutomatically assignClusterAutomatically(deterministicRandom()->coinflip());
		state metacluster::IgnoreCapacityLimit ignoreCapacityLimit(deterministicRandom()->coinflip());

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state bool tenantGroupExists = tenantGroup.present() && self->tenantGroups.count(tenantGroup.get());
		state bool hasCapacity = tenantGroupExists || self->ungroupedTenants.size() + self->tenantGroups.size() <
		                                                  self->totalTenantGroupCapacity;

		if (assignClusterAutomatically && !tenantGroupExists && hasCapacity) {
			// In this case, the condition of `hasCapacity` will be further tightened since we will exclude those
			// data clusters with autoTenantAssignment being false.
			// It's possible that all the data clusters are excluded from the auto-assignment pool.
			// Consequently, even if the the metacluster has capacity, the capacity index has no available data
			// clusters. In this case, trying to assign a cluster to the tenant automatically will fail.
			bool emptyCapacityIndex = true;
			for (auto dataDb : self->dataDbs) {
				if (metacluster::AutoTenantAssignment::DISABLED == dataDb.second->autoTenantAssignment ||
				    !dataDb.second->registered || dataDb.second->detached) {
					continue;
				}
				if (dataDb.second->ungroupedTenants.size() + dataDb.second->tenantGroups.size() <
				    dataDb.second->tenantGroupCapacity) {
					emptyCapacityIndex = false;
					break;
				}
			}
			hasCapacity = !emptyCapacityIndex;
		}
		state bool retried = false;

		state metacluster::MetaclusterTenantMapEntry tenantMapEntry;
		tenantMapEntry.tenantName = tenant;
		tenantMapEntry.tenantGroup = tenantGroup;

		// Choose between two preferred clusters because if we get a partial completion and
		// retry, we want the operation to eventually succeed instead of having a chance of
		// never re-visiting the original preferred cluster.
		state std::vector<ClusterName> preferredClusters;
		state int preferredClusterIndex = 0;
		if (!assignClusterAutomatically) {
			preferredClusters.push_back(self->chooseClusterName());
			preferredClusters.push_back(self->chooseClusterName());
			tenantMapEntry.assignedCluster = preferredClusters[preferredClusterIndex];
		}

		try {
			loop {
				try {
					if (!assignClusterAutomatically && deterministicRandom()->coinflip()) {
						preferredClusterIndex = deterministicRandom()->randomInt(0, preferredClusters.size());
						tenantMapEntry.assignedCluster = preferredClusters[preferredClusterIndex];
					}
					Future<Void> createFuture = metacluster::createTenant(
					    self->managementDb, tenantMapEntry, assignClusterAutomatically, ignoreCapacityLimit);
					Optional<Void> result = wait(timeout(createFuture, deterministicRandom()->randomInt(1, 30)));
					if (result.present()) {
						break;
					} else {
						retried = true;
						wait(verifyListFilter(self, tenant, "createTenant"));
					}
				} catch (Error& e) {
					if (e.code() == error_code_tenant_already_exists && retried && !exists) {
						Optional<metacluster::MetaclusterTenantMapEntry> entry =
						    wait(metacluster::tryGetTenant(self->managementDb, tenant));
						ASSERT(entry.present());
						tenantMapEntry = entry.get();
						break;
					} else if (!assignClusterAutomatically && (e.code() == error_code_cluster_no_capacity ||
					                                           e.code() == error_code_cluster_not_found ||
					                                           e.code() == error_code_invalid_tenant_configuration)) {
						state Error error = e;
						Optional<metacluster::MetaclusterTenantMapEntry> entry =
						    wait(metacluster::tryGetTenant(self->managementDb, tenant));

						// When picking a different assigned cluster, it is possible to leave the
						// tenant creation in a partially completed state, which we want to avoid.
						// Continue retrying if the new preferred cluster throws errors rather than
						// exiting immediately so we can allow the operation to finish.
						if (preferredClusters.size() > 1 &&
						    (!entry.present() || entry.get().assignedCluster != tenantMapEntry.assignedCluster)) {
							preferredClusters.erase(preferredClusters.begin() + preferredClusterIndex);
							preferredClusterIndex = 0;
							tenantMapEntry.assignedCluster = preferredClusters[preferredClusterIndex];
							continue;
						}

						throw error;
					} else if (e.code() == error_code_unsupported_metacluster_version) {
						if (!self->isValidVersion()) {
							return Void();
						} else {
							Optional<metacluster::MetaclusterTenantMapEntry> entry =
							    wait(metacluster::tryGetTenant(self->managementDb, tenant));
							ASSERT(entry.present());
							wait(self->checkAndResetMetaclusterVersion(entry.get().assignedCluster));
							continue;
						}
					} else {
						throw;
					}
				}
			}

			metacluster::MetaclusterTenantMapEntry entry = wait(metacluster::getTenant(self->managementDb, tenant));

			ASSERT(self->metaclusterCreated);
			ASSERT(!exists);
			ASSERT(ignoreCapacityLimit || hasCapacity);
			if (!hasCapacity) {
				ASSERT(!tenantGroupExists);
				ASSERT(ignoreCapacityLimit);
			}
			ASSERT(!ignoreCapacityLimit || !assignClusterAutomatically);
			ASSERT(entry.tenantGroup == tenantGroup);
			ASSERT(TenantAPI::getTenantIdPrefix(entry.id) == self->tenantIdPrefix);

			Reference<TenantTestData> tenantData = makeReference<TenantTestData>(entry.assignedCluster, tenantGroup);
			self->createdTenants[tenant] = tenantData;

			auto assignedCluster = self->dataDbs.find(entry.assignedCluster);
			ASSERT(assignClusterAutomatically || tenantMapEntry.assignedCluster == assignedCluster->first);
			ASSERT(assignedCluster != self->dataDbs.end());
			ASSERT(assignedCluster->second->tenants.try_emplace(tenant, tenantData).second);

			Optional<ClusterName> clusterAssignedToTenantGroup;
			ASSERT(self->isValidVersion(assignedCluster->second));

			bool allocationAdded = false;
			if (tenantGroup.present()) {
				auto [tenantGroupData, _allocationAdded] = self->tenantGroups.try_emplace(
				    tenantGroup.get(), makeReference<TenantGroupData>(entry.assignedCluster));

				allocationAdded = _allocationAdded;
				ASSERT(tenantGroupData->second->cluster == entry.assignedCluster);
				clusterAssignedToTenantGroup = tenantGroupData->second->cluster;
				tenantGroupData->second->tenants.insert(tenant);
				assignedCluster->second->tenantGroups[tenantGroup.get()] = tenantGroupData->second;
			} else {
				allocationAdded = true;
				self->ungroupedTenants.insert(tenant);
				assignedCluster->second->ungroupedTenants.insert(tenant);
			}

			if (allocationAdded &&
			    assignedCluster->second->ungroupedTenants.size() + assignedCluster->second->tenantGroups.size() >
			        assignedCluster->second->tenantGroupCapacity) {
				++self->totalTenantGroupCapacity;
			}

			// In two cases, we bypass the search on capacity index:
			// i. assignClusterAutomatically is false. This indicates that the user explicitly specifies which cluster
			//    to assign to a tenant
			// ii. the tenant belongs to a tenant group to which a cluster has already been assigned.
			// Except for these two cases, any chosen cluster must have autoTenantAssignment enabled.
			ASSERT(!assignClusterAutomatically || clusterAssignedToTenantGroup.present() ||
			       assignedCluster->second->autoTenantAssignment == metacluster::AutoTenantAssignment::ENABLED);

			ASSERT(tenantGroupExists || ignoreCapacityLimit ||
			       assignedCluster->second->tenantGroupCapacity >=
			           assignedCluster->second->tenantGroups.size() + assignedCluster->second->ungroupedTenants.size());
		} catch (Error& e) {
			if (e.code() == error_code_tenant_already_exists) {
				ASSERT(exists);
				return Void();
			} else if (e.code() == error_code_metacluster_no_capacity) {
				ASSERT(!exists && !hasCapacity);
				return Void();
			} else if (e.code() == error_code_cluster_no_capacity) {
				ASSERT(!assignClusterAutomatically);
				// When trying to create a new tenant, there are several cases in which "cluster_no_capacity" can be
				// thrown. The only one case in which ignoreCapacityLimit is true is when the number of tenants exceed
				// a threshold configured via knob. But this is not possible due to the way we pick tenant names in this
				// test, thus we can have the following assertion.
				ASSERT(!ignoreCapacityLimit);
				return Void();
			} else if (e.code() == error_code_cluster_not_found) {
				ASSERT(!assignClusterAutomatically);
				return Void();
			} else if (e.code() == error_code_invalid_tenant_configuration) {
				bool tenantGroupClusterMismatch = false;
				if (tenantGroup.present()) {
					auto itr = self->tenantGroups.find(tenantGroup.get());
					if (itr != self->tenantGroups.end() && itr->second->cluster != tenantMapEntry.assignedCluster) {
						tenantGroupClusterMismatch = true;
					}
				}
				bool invalidIgnoreCapacityLimit = assignClusterAutomatically && ignoreCapacityLimit;
				ASSERT(tenantGroupClusterMismatch || invalidIgnoreCapacityLimit);
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state bool retried = false;

		try {
			loop {
				try {
					Future<Void> deleteFuture = metacluster::deleteTenant(self->managementDb, tenant);
					Optional<Void> result = wait(timeout(deleteFuture, deterministicRandom()->randomInt(1, 30)));

					if (result.present()) {
						break;
					} else {
						retried = true;
						wait(verifyListFilter(self, tenant, "deleteTenant"));
					}
				} catch (Error& e) {
					if (e.code() == error_code_tenant_not_found && retried && exists) {
						Optional<metacluster::MetaclusterTenantMapEntry> entry =
						    wait(metacluster::tryGetTenant(self->managementDb, tenant));
						ASSERT(!entry.present());
						break;
					} else if (e.code() == error_code_unsupported_metacluster_version) {
						if (!self->isValidVersion()) {
							return Void();
						} else {
							auto tenantItr = self->createdTenants.find(tenant);
							ASSERT(tenantItr != self->createdTenants.end());
							wait(self->checkAndResetMetaclusterVersion(tenantItr->second->cluster));
							continue;
						}
					} else {
						throw;
					}
				}
			}

			ASSERT(self->metaclusterCreated);
			ASSERT(exists);
			auto tenantData = self->createdTenants.find(tenant);
			ASSERT(tenantData != self->createdTenants.end());

			bool erasedTenantGroup = false;
			if (tenantData->second->tenantGroup.present()) {
				auto tenantGroupData = self->tenantGroups.find(tenantData->second->tenantGroup.get());
				ASSERT(tenantGroupData != self->tenantGroups.end());
				tenantGroupData->second->tenants.erase(tenant);
				if (tenantGroupData->second->tenants.empty()) {
					erasedTenantGroup = true;
					self->tenantGroups.erase(tenantData->second->tenantGroup.get());
				}
			} else {
				self->ungroupedTenants.erase(tenant);
			}

			auto dataDb = self->dataDbs[tenantData->second->cluster];
			ASSERT(dataDb->registered);
			auto tenantItr = dataDb->tenants.find(tenant);
			ASSERT(tenantItr != dataDb->tenants.end());
			ASSERT(self->isValidVersion(dataDb));

			bool reducedAllocatedCount = false;
			if (erasedTenantGroup) {
				reducedAllocatedCount = true;
				dataDb->tenantGroups.erase(tenantData->second->tenantGroup.get());
			} else if (!tenantData->second->tenantGroup.present()) {
				reducedAllocatedCount = true;
				dataDb->ungroupedTenants.erase(tenant);
			}

			if (reducedAllocatedCount &&
			    dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size() >= dataDb->tenantGroupCapacity) {
				--self->totalTenantGroupCapacity;
			}
			dataDb->tenants.erase(tenantItr);
			self->createdTenants.erase(tenant);
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "DeleteTenantFailure").error(e).detail("TenantName", tenant);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> configureTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state Optional<TenantGroupName> newTenantGroup = self->chooseTenantGroup();
		state metacluster::IgnoreCapacityLimit ignoreCapacityLimit(deterministicRandom()->coinflip());

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state bool tenantGroupExists =
		    newTenantGroup.present() && self->tenantGroups.find(newTenantGroup.get()) != self->tenantGroups.end();

		state bool hasCapacity = false;
		state Optional<ClusterName> oldClusterName;
		if (exists) {
			auto& dataDb = self->dataDbs[itr->second->cluster];
			hasCapacity = dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size() < dataDb->tenantGroupCapacity;
			oldClusterName = itr->second->cluster;
		}

		state Optional<ClusterName> newClusterName = oldClusterName;
		if (deterministicRandom()->coinflip()) {
			newClusterName = self->chooseClusterName();
		}

		state std::map<Standalone<StringRef>, Optional<Value>> configurationParameters = {
			{ "assigned_cluster"_sr, newClusterName }, { "tenant_group"_sr, newTenantGroup }
		};

		state bool configChanged =
		    exists && (itr->second->tenantGroup != newTenantGroup || oldClusterName != newClusterName);

		try {
			loop {
				Future<Void> configureFuture = metacluster::configureTenant(
				    self->managementDb, tenant, configurationParameters, ignoreCapacityLimit);
				try {
					Optional<Void> result = wait(timeout(configureFuture, deterministicRandom()->randomInt(1, 30)));

					if (result.present()) {
						break;
					}
					wait(verifyListFilter(self, tenant, "configureTenant"));
				} catch (Error& e) {
					state Error error = e;
					if (e.code() == error_code_unsupported_metacluster_version) {
						if (!self->isValidVersion()) {
							return Void();
						} else {
							auto tenantItr = self->createdTenants.find(tenant);
							ASSERT(tenantItr != self->createdTenants.end());
							wait(self->checkAndResetMetaclusterVersion(tenantItr->second->cluster));
							continue;
						}
					}

					throw error;
				}
			}

			ASSERT(self->metaclusterCreated);
			ASSERT(exists);
			auto tenantData = self->createdTenants.find(tenant);
			ASSERT(tenantData != self->createdTenants.end());

			auto& dataDb = self->dataDbs[tenantData->second->cluster];
			ASSERT(dataDb->registered);

			if (configChanged) {
				ASSERT(self->isValidVersion(dataDb));
			} else {
				ASSERT(self->isValidVersion());
			}

			bool allocationRemoved = false;
			bool allocationAdded = false;
			if (tenantData->second->tenantGroup != newTenantGroup) {
				if (tenantData->second->tenantGroup.present()) {
					auto& tenantGroupData = self->tenantGroups[tenantData->second->tenantGroup.get()];
					tenantGroupData->tenants.erase(tenant);
					if (tenantGroupData->tenants.empty()) {
						allocationRemoved = true;
						self->tenantGroups.erase(tenantData->second->tenantGroup.get());
						dataDb->tenantGroups.erase(tenantData->second->tenantGroup.get());
					}
				} else {
					allocationRemoved = true;
					self->ungroupedTenants.erase(tenant);
					dataDb->ungroupedTenants.erase(tenant);
				}

				if (newTenantGroup.present()) {
					auto [tenantGroupData, inserted] = self->tenantGroups.try_emplace(
					    newTenantGroup.get(), makeReference<TenantGroupData>(tenantData->second->cluster));
					tenantGroupData->second->tenants.insert(tenant);
					if (inserted) {
						allocationAdded = true;
						ASSERT(dataDb->tenantGroups.try_emplace(newTenantGroup.get(), tenantGroupData->second).second);
					}
				} else {
					allocationAdded = true;
					self->ungroupedTenants.insert(tenant);
					dataDb->ungroupedTenants.insert(tenant);
				}

				tenantData->second->tenantGroup = newTenantGroup;

				if (allocationAdded && !allocationRemoved) {
					ASSERT(ignoreCapacityLimit || hasCapacity);
					if (!hasCapacity) {
						++self->totalTenantGroupCapacity;
					}
				} else if (allocationRemoved && !allocationAdded &&
				           dataDb->ungroupedTenants.size() + dataDb->tenantGroups.size() >=
				               dataDb->tenantGroupCapacity) {
					--self->totalTenantGroupCapacity;
				}
			}
			ASSERT(oldClusterName == newClusterName);
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			} else if (e.code() == error_code_cluster_no_capacity) {
				ASSERT(exists && !hasCapacity && !ignoreCapacityLimit);
				return Void();
			} else if (e.code() == error_code_invalid_tenant_configuration) {
				ASSERT(exists);
				if (oldClusterName == newClusterName) {
					ASSERT(tenantGroupExists &&
					       self->createdTenants[tenant]->cluster != self->tenantGroups[newTenantGroup.get()]->cluster);
				}
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "ConfigureTenantFailure")
			    .error(e)
			    .detail("TenantName", tenant)
			    .detail("TenantGroup", newTenantGroup);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> lockTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state TenantAPI::TenantLockState lockState = (TenantAPI::TenantLockState)deterministicRandom()->randomInt(0, 3);

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state UID lockId = exists && itr->second->lockId.present() && deterministicRandom()->coinflip()
		                       ? itr->second->lockId.get()
		                       : deterministicRandom()->randomUniqueID();

		try {
			loop {
				Future<Void> lockFuture =
				    metacluster::changeTenantLockState(self->managementDb, tenant, lockState, lockId);
				try {
					Optional<Void> result = wait(timeout(lockFuture, deterministicRandom()->randomInt(1, 30)));

					if (result.present()) {
						break;
					}
				} catch (Error& e) {
					state Error error = e;
					if (e.code() == error_code_unsupported_metacluster_version) {
						if (!self->isValidVersion()) {
							return Void();
						} else {
							auto tenantItr = self->createdTenants.find(tenant);
							ASSERT(tenantItr != self->createdTenants.end());
							wait(self->checkAndResetMetaclusterVersion(tenantItr->second->cluster));
							continue;
						}
					}

					throw error;
				}
			}

			ASSERT(self->metaclusterCreated);
			ASSERT(exists);
			auto tenantData = self->createdTenants.find(tenant);
			ASSERT(tenantData != self->createdTenants.end());

			ASSERT(!tenantData->second->lockId.present() || lockId == tenantData->second->lockId.get());

			auto& dataDb = self->dataDbs[tenantData->second->cluster];
			ASSERT(dataDb->registered);

			if (lockState == tenantData->second->lockState) {
				ASSERT(self->isValidVersion());
			} else {
				ASSERT(self->isValidVersion(dataDb));
			}

			if (lockState == TenantAPI::TenantLockState::UNLOCKED) {
				tenantData->second->lockId = {};
			} else {
				tenantData->second->lockId = lockId;
			}

			tenantData->second->lockState = lockState;
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			} else if (e.code() == error_code_tenant_locked) {
				ASSERT(exists);
				auto tenantData = self->createdTenants.find(tenant);
				ASSERT(tenantData != self->createdTenants.end());
				ASSERT(tenantData->second->lockId.present() && lockId != tenantData->second->lockId.get());
				return Void();
			}

			TraceEvent(SevError, "LockTenantFailure")
			    .error(e)
			    .detail("TenantName", tenant)
			    .detail("LockState", TenantAPI::tenantLockStateToString(lockState))
			    .detail("LockId", lockId);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> renameTenant(MetaclusterManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state TenantName newTenantName = self->chooseTenantName();

		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();

		itr = self->createdTenants.find(newTenantName);
		state bool newTenantExists = itr != self->createdTenants.end();

		try {
			state bool retried = false;
			loop {
				try {
					Future<Void> renameFuture = metacluster::renameTenant(self->managementDb, tenant, newTenantName);
					Optional<Void> result = wait(timeout(renameFuture, deterministicRandom()->randomInt(1, 30)));

					if (result.present()) {
						break;
					}

					retried = true;
					wait(verifyListFilter(self, tenant, "renameTenant"));
				} catch (Error& e) {
					state Error error = e;
					// If we retry the rename after it had succeeded, we will get an error that we should ignore
					if (e.code() == error_code_tenant_not_found && exists && !newTenantExists && retried) {
						break;
					} else if (e.code() == error_code_unsupported_metacluster_version) {
						if (!self->isValidVersion()) {
							return Void();
						} else {
							auto tenantItr = self->createdTenants.find(tenant);
							ASSERT(tenantItr != self->createdTenants.end());
							wait(self->checkAndResetMetaclusterVersion(tenantItr->second->cluster));
							continue;
						}
					}

					throw error;
				}
			}

			wait(verifyListFilter(self, newTenantName, "renameTenantNew"));

			ASSERT(self->metaclusterCreated);
			ASSERT(exists);
			ASSERT(!newTenantExists);

			Optional<metacluster::MetaclusterTenantMapEntry> oldEntry =
			    wait(metacluster::tryGetTenant(self->managementDb, tenant));
			ASSERT(!oldEntry.present());

			metacluster::MetaclusterTenantMapEntry newEntry =
			    wait(metacluster::getTenant(self->managementDb, newTenantName));

			auto tenantDataItr = self->createdTenants.find(tenant);
			ASSERT(tenantDataItr != self->createdTenants.end());

			Reference<TenantTestData> tenantData = tenantDataItr->second;
			ASSERT(tenantData->tenantGroup == newEntry.tenantGroup);
			ASSERT(tenantData->cluster == newEntry.assignedCluster);

			self->createdTenants[newTenantName] = tenantData;
			self->createdTenants.erase(tenantDataItr);

			auto& dataDb = self->dataDbs[newEntry.assignedCluster];
			ASSERT(dataDb->registered);
			ASSERT(self->isValidVersion(dataDb));

			dataDb->tenants.erase(tenant);
			dataDb->tenants[newTenantName] = tenantData;

			if (newEntry.tenantGroup.present()) {
				auto& tenantGroup = self->tenantGroups[newEntry.tenantGroup.get()];
				tenantGroup->tenants.erase(tenant);
				tenantGroup->tenants.insert(newTenantName);
			} else {
				dataDb->ungroupedTenants.erase(tenant);
				dataDb->ungroupedTenants.insert(newTenantName);
				self->ungroupedTenants.erase(tenant);
				self->ungroupedTenants.insert(newTenantName);
			}
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!exists);
				return Void();
			} else if (e.code() == error_code_tenant_already_exists) {
				ASSERT(newTenantExists);
				return Void();
			} else if (e.code() == error_code_invalid_metacluster_operation) {
				ASSERT(!self->metaclusterCreated);
				return Void();
			}

			TraceEvent(SevError, "RenameTenantFailure")
			    .error(e)
			    .detail("OldTenantName", tenant)
			    .detail("NewTenantName", newTenantName);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> changeClusterVersion(MetaclusterManagementWorkload* self) {
		state MetaclusterVersion newVersion = (MetaclusterVersion)deterministicRandom()->randomInt(
		    (int)MetaclusterVersion::BEGIN, (int)MetaclusterVersion::END + 1);

		if (deterministicRandom()->coinflip()) {
			if (self->metaclusterCreated) {
				self->managementVersion = newVersion;
				wait(self->setMetaclusterVersion(self->managementDb, self->managementVersion));
				wait(waitForFullyRecovered(self));
			}
		} else {
			ClusterName clusterName = self->chooseClusterName();
			auto& dataDb = self->dataDbs[clusterName];
			if (dataDb->registered) {
				dataDb->version = newVersion;
				wait(self->setMetaclusterVersion(dataDb->db.getReference(), dataDb->version));
			}
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}
	ACTOR static Future<Void> _start(Database cx, MetaclusterManagementWorkload* self) {
		state double start = now();

		// Run a random sequence of operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 14);
			if (operation == 0) {
				wait(createMetacluster(self));
			} else if (operation == 1) {
				wait(decommissionMetacluster(self));
			} else if (operation == 2) {
				wait(registerCluster(self));
			} else if (operation == 3) {
				wait(removeCluster(self));
			} else if (operation == 4) {
				wait(listClusters(self));
			} else if (operation == 5) {
				wait(getCluster(self));
			} else if (operation == 6) {
				wait(configureCluster(self));
			} else if (operation == 7) {
				wait(restoreCluster(self));
			} else if (operation == 8) {
				wait(createTenant(self));
			} else if (operation == 9) {
				wait(deleteTenant(self));
			} else if (operation == 10) {
				wait(configureTenant(self));
			} else if (operation == 11) {
				wait(lockTenant(self));
			} else if (operation == 12) {
				wait(renameTenant(self));
			} else if (operation == 13) {
				wait(changeClusterVersion(self));
			}
		}

		return Void();
	}

	// Checks that the data cluster state matches our local state
	ACTOR static Future<Void> checkDataCluster(MetaclusterManagementWorkload* self,
	                                           ClusterName clusterName,
	                                           Reference<DataClusterData> clusterData) {
		state Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants;
		state Reference<ReadYourWritesTransaction> tr = clusterData->db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(metaclusterRegistration, metacluster::metadata::metaclusterRegistration().get(tr)) &&
				     store(tenants,
				           TenantAPI::listTenantMetadataTransaction(
				               tr, ""_sr, "\xff\xff"_sr, clusterData->tenants.size() + 1)));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		if (clusterData->registered) {
			ASSERT(metaclusterRegistration.present() &&
			       metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);
		} else {
			ASSERT(!metaclusterRegistration.present());
		}

		ASSERT_EQ(tenants.size(), clusterData->tenants.size());
		for (auto [tenantName, tenantEntry] : tenants) {
			ASSERT(clusterData->tenants.count(tenantName));
			auto tenantData = clusterData->tenants.find(tenantName);
			ASSERT(tenantData != clusterData->tenants.end());
			ASSERT(tenantData->second->cluster == clusterName);
			ASSERT(tenantData->second->tenantGroup == tenantEntry.tenantGroup);

			if (!clusterData->detached) {
				auto itr = self->createdTenants.find(tenantName);
				ASSERT(itr != self->createdTenants.end());
				ASSERT(itr->second == tenantData->second);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> teardownMetacluster(MetaclusterManagementWorkload* self) {
		state Reference<ITransaction> tr = self->managementDb->createTransaction();

		state bool deleteTenants = deterministicRandom()->coinflip();

		if (deleteTenants) {
			state std::vector<std::pair<TenantName, int64_t>> tenants =
			    wait(metacluster::listTenants(self->managementDb, ""_sr, "\xff\xff"_sr, 10e6));

			state std::vector<Future<Void>> deleteTenantFutures;
			for (auto [tenantName, tid] : tenants) {
				deleteTenantFutures.push_back(metacluster::deleteTenant(self->managementDb, tenantName));
			}

			wait(waitForAll(deleteTenantFutures));
		}

		state std::map<ClusterName, metacluster::DataClusterMetadata> dataClusters =
		    wait(metacluster::listClusters(self->managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS));

		std::vector<Future<Void>> removeClusterFutures;
		for (auto [clusterName, clusterMetadata] : dataClusters) {
			removeClusterFutures.push_back(
			    success(metacluster::removeCluster(self->managementDb,
			                                       clusterName,
			                                       ClusterType::METACLUSTER_MANAGEMENT,
			                                       metacluster::ForceRemove(!deleteTenants))));
		}

		wait(waitForAll(removeClusterFutures));
		wait(metacluster::decommissionMetacluster(self->managementDb));

		Optional<MetaclusterRegistrationEntry> entry =
		    wait(metacluster::metadata::metaclusterRegistration().get(self->managementDb));
		ASSERT(!entry.present());

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(this);
		} else {
			return true;
		}
	}
	ACTOR static Future<bool> _check(MetaclusterManagementWorkload* self) {
		std::vector<Future<Void>> setVersionFutures;
		if (self->metaclusterCreated) {
			self->managementVersion = MetaclusterVersion::MAX_SUPPORTED;
			setVersionFutures.push_back(self->setMetaclusterVersion(self->managementDb, self->managementVersion));
		}
		for (auto& [name, dataDb] : self->dataDbs) {
			if (dataDb->registered) {
				dataDb->version = MetaclusterVersion::MAX_SUPPORTED;
				setVersionFutures.push_back(self->setMetaclusterVersion(dataDb->db.getReference(), dataDb->version));
			}
		}

		wait(waitForAll(setVersionFutures));

		// The metacluster consistency check runs the tenant consistency check for each cluster
		if (self->metaclusterCreated) {
			state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
			    self->managementDb, metacluster::util::AllowPartialMetaclusterOperations::False);
			wait(metaclusterConsistencyCheck.run());

			std::map<ClusterName, metacluster::DataClusterMetadata> dataClusters = wait(metacluster::listClusters(
			    self->managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS + 1));

			int totalTenantGroupsAllocated = 0;
			std::vector<Future<Void>> dataClusterChecks;
			for (auto [clusterName, dataClusterData] : self->dataDbs) {
				auto dataClusterItr = dataClusters.find(clusterName);
				if (dataClusterData->registered) {
					ASSERT(dataClusterItr != dataClusters.end());
					ASSERT(dataClusterItr->second.entry.capacity.numTenantGroups ==
					       dataClusterData->tenantGroupCapacity);
					ASSERT_EQ(dataClusterItr->second.entry.autoTenantAssignment, dataClusterData->autoTenantAssignment);
					totalTenantGroupsAllocated +=
					    dataClusterData->tenantGroups.size() + dataClusterData->ungroupedTenants.size();
				} else {
					ASSERT(dataClusterItr == dataClusters.end());
				}

				dataClusterChecks.push_back(checkDataCluster(self, clusterName, dataClusterData));
			}
			auto capacityNumbers = metacluster::util::metaclusterCapacity(dataClusters);
			ASSERT(capacityNumbers.first.numTenantGroups == self->totalTenantGroupCapacity);
			ASSERT(capacityNumbers.second.numTenantGroups == totalTenantGroupsAllocated);

			wait(waitForAll(dataClusterChecks));
			wait(teardownMetacluster(self));
		} else {
			Optional<MetaclusterRegistrationEntry> managementRegistration =
			    wait(metacluster::metadata::metaclusterRegistration().get(self->managementDb));
			ASSERT(!managementRegistration.present());

			state std::map<ClusterName, Reference<DataClusterData>>::iterator dataDbItr;
			for (dataDbItr = self->dataDbs.begin(); dataDbItr != self->dataDbs.end(); ++dataDbItr) {
				ASSERT(!dataDbItr->second->registered);
				Optional<MetaclusterRegistrationEntry> dataDbRegistration =
				    wait(metacluster::metadata::metaclusterRegistration().get(dataDbItr->second->db.getReference()));
				ASSERT(!dataDbRegistration.present());
			}
		}

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MetaclusterManagementWorkload> MetaclusterManagementWorkloadFactory;
