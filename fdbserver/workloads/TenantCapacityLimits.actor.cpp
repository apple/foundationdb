/*
 * TenantCapacityLimits.actor.cpp
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
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/BooleanParam.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantCapacityLimits : TestWorkload {
	static constexpr auto NAME = "TenantCapacityLimits";

	Reference<IDatabase> managementDb;
	Database dataDb;

	int64_t tenantIdPrefix;
	bool useMetacluster = false;
	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl::mapSubRange.begin);

	TenantCapacityLimits(WorkloadContext const& wcx) : TestWorkload(wcx) {
		tenantIdPrefix = getOption(options,
		                           "tenantIdPrefix"_sr,
		                           deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                                            TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1));
		if (clientId == 0) {
			useMetacluster = deterministicRandom()->coinflip();
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}
	ACTOR static Future<Void> _setup(Database cx, TenantCapacityLimits* self) {
		if (self->useMetacluster) {
			metacluster::DataClusterEntry entry;
			entry.capacity.numTenantGroups = 1e9;

			metacluster::util::SimulatedMetacluster simMetacluster =
			    wait(metacluster::util::createSimulatedMetacluster(cx, self->tenantIdPrefix, entry));

			self->managementDb = simMetacluster.managementDb;
			ASSERT_EQ(simMetacluster.dataDbs.size(), 1);
			self->dataDb = simMetacluster.dataDbs.begin()->second;
		} else {
			self->dataDb = cx;
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

	ACTOR static Future<Void> setLastTenantId(Database dataDb, int64_t tenantId) {
		// set the max tenant id for the standalone cluster
		state Reference<ReadYourWritesTransaction> dataTr = makeReference<ReadYourWritesTransaction>(dataDb);
		loop {
			try {
				dataTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				TenantMetadata::lastTenantId().set(dataTr, tenantId);
				wait(dataTr->commit());
				break;
			} catch (Error& e) {
				wait(dataTr->onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, TenantCapacityLimits* self) {
		if (self->useMetacluster) {
			// Set the max tenant id for the metacluster
			state Reference<ITransaction> tr = self->managementDb->createTransaction();
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					int64_t maxTenantId = TenantAPI::getMaxAllowableTenantId(self->tenantIdPrefix << 48);
					metacluster::metadata::management::tenantMetadata().lastTenantId.set(tr, maxTenantId);
					wait(safeThreadFutureToFuture(tr->commit()));
					break;
				} catch (Error& e) {
					wait(safeThreadFutureToFuture(tr->onError(e)));
				}
			}
			// Attempt to create a tenant on the metacluster which should fail since the cluster is at capacity
			try {
				metacluster::MetaclusterTenantMapEntry entry;
				entry.tenantName = "test_tenant_metacluster"_sr;
				wait(metacluster::createTenant(self->managementDb,
				                               entry,
				                               metacluster::AssignClusterAutomatically::True,
				                               metacluster::IgnoreCapacityLimit::False));
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_cluster_no_capacity);
			}
		} else {
			// set the max tenant id for the standalone cluster
			wait(setLastTenantId(self->dataDb, TenantAPI::getMaxAllowableTenantId(0)));
			// Use the management database api to create a tenant which should fail since the cluster is at capacity
			try {
				wait(success(TenantAPI::createTenant(self->dataDb.getReference(), "test_tenant_management_api"_sr)));
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_cluster_no_capacity);
			}

			wait(setLastTenantId(self->dataDb, TenantAPI::getMaxAllowableTenantId(0) - 2));
			// use special keys to create a tenant which should fail since the cluster is at capacity
			state Reference<ReadYourWritesTransaction> dataTr = makeReference<ReadYourWritesTransaction>(self->dataDb);
			loop {
				try {
					dataTr->reset();
					dataTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					dataTr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					dataTr->set(self->specialKeysTenantMapPrefix.withSuffix("test_tenant_special_keys_1"_sr), ""_sr);
					dataTr->set(self->specialKeysTenantMapPrefix.withSuffix("test_tenant_special_keys_2"_sr), ""_sr);
					dataTr->set(self->specialKeysTenantMapPrefix.withSuffix("test_tenant_special_keys_3"_sr), ""_sr);
					wait(dataTr->commit());
					ASSERT(false);
				} catch (Error& e) {
					if (e.code() == error_code_cluster_no_capacity) {
						break;
					}
					wait(dataTr->onError(e));
				}
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantCapacityLimits> TenantCapacityLimitsFactory;
