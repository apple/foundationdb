#include <cstdint>
#include <limits>
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/MetaclusterData.actor.h"
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

	ClusterName sourceCluster;
	ClusterName destinationCluster;
	UID runID;

	MetaclusterMoveWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

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
				if (deterministicRandom()->coinflip() && !existingGroups.empty()) {
					tenantGroup = deterministicRandom()->randomChoice(
					    std::vector<TenantGroupName>(existingGroups.begin(), existingGroups.end()));
				} else if (tenantGroups.size() < maxTenantGroups) {
					do {
						tenantGroup = TenantGroupNameRef(
						    format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
					} while (tenantGroups.count(tenantGroup.get()) > 0);
				}
			}
		}

		return tenantGroup;
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

	ACTOR static Future<Void> populateData(MetaclusterMoveWorkload* self) {
		wait(delay(1.0));
		return Void();
	}

	ACTOR static Future<Void> startMove(MetaclusterMoveWorkload* self) {
		self->sourceCluster = deterministicRandom()->randomChoice(self->dataDbIndex);
		self->destinationCluster = deterministicRandom()->randomChoice(self->dataDbIndex);
		self->runID = deterministicRandom()->randomUniqueID();
		while (self->sourceCluster == self->destinationCluster) {
			self->destinationCluster = deterministicRandom()->randomChoice(self->dataDbIndex);
		}

		Optional<TenantGroupName> tenantGroup = self->chooseTenantGroup(self->sourceCluster);
		wait(metacluster::startTenantMovement(
		    self->managementDb, tenantGroup.get(), self->sourceCluster, self->destinationCluster, self->runID));
		return Void();
	}

	ACTOR static Future<Void> switchMove(MetaclusterMoveWorkload* self) {
		wait(delay(1.0));
		return Void();
	}

	ACTOR static Future<Void> finishMove(MetaclusterMoveWorkload* self) {
		wait(delay(1.0));
		return Void();
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

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, MetaclusterMoveWorkload* self) {
		state ClusterName srcCluster = self->chooseClusterName();
		state ClusterName dstCluster = self->chooseClusterName();
		// Expect an error if the same cluster is picked

		Optional<TenantGroupName> optionalTenantGroup = self->chooseTenantGroup(srcCluster);
		while (!optionalTenantGroup.present()) {
			optionalTenantGroup = self->chooseTenantGroup(srcCluster);
		}
		state TenantGroupName tenantGroup = optionalTenantGroup.get();

		return Void();
	}

	ACTOR static Future<bool> _check(MetaclusterMoveWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, metacluster::util::AllowPartialMetaclusterOperations::True);

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
};

WorkloadFactory<MetaclusterMoveWorkload> MetaclusterMoveWorkloadFactory;