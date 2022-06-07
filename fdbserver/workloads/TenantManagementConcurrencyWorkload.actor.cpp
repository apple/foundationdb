/*
 * TenantManagementConcurrencyWorkload.actor.cpp
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
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantManagementConcurrencyWorkload : TestWorkload {
	const TenantName tenantNamePrefix = "tenant_management_concurrency_workload_"_sr;

	int maxTenants;
	int maxTenantGroups;
	double testDuration;
	bool useMetacluster;

	Reference<IDatabase> mvDb;
	Database dataDb;

	TenantManagementConcurrencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 100));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 60.0);

		useMetacluster = false;
	}

	std::string description() const override { return "TenantManagementConcurrency"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementConcurrencyWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		TraceEvent("CreatedThreadSafeHandle");

		MultiVersionApi::api->selectApiVersion(cx->apiVersion);
		self->mvDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

		if (self->useMetacluster) {
			auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
			self->dataDb = Database::createDatabase(extraFile, -1);

			if (self->clientId == 0) {
				wait(success(ManagementAPI::changeConfig(cx.getReference(), "tenant_mode=management", true)));

				DataClusterEntry entry;
				entry.capacity.numTenantGroups = 1e9;
				wait(MetaclusterAPI::registerCluster(self->mvDb, "cluster1"_sr, *g_simulator.extraDB, entry));
			}
		} else {
			self->dataDb = cx;
		}

		return Void();
	}

	TenantName chooseTenantName() {
		TenantName tenant(
		    format("%s%08d", tenantNamePrefix.toString().c_str(), deterministicRandom()->randomInt(0, maxTenants)));

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

	ACTOR Future<Void> createTenant(Database cx, TenantManagementConcurrencyWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state TenantMapEntry entry;

		try {
			loop {
				try {
					Future<Void> createFuture =
					    self->useMetacluster
					        ? MetaclusterAPI::createTenant(self->mvDb, tenant, entry)
					        : success(ManagementAPI::createTenant(self->dataDb.getReference(), tenant, entry));
					Optional<Void> result = wait(timeout(createFuture, 30));
					if (result.present()) {
						break;
					}
				} catch (Error& e) {
					// If we retried the creation after our initial attempt succeeded, then we proceed with the rest
					// of the creation steps normally. Otherwise, the creation happened elsewhere and we failed
					// here, so we can rethrow the error.
					if (e.code() == error_code_tenant_already_exists) {
						break;
					} else if (e.code() == error_code_tenant_removed) {
						ASSERT(self->useMetacluster);
						break;
					} else {
						throw;
					}
				}
			}

			return Void();
		} catch (Error& e) {
			if (e.code() != error_code_tenant_already_exists) {
				TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
			}

			return Void();
		}
	}

	ACTOR Future<Void> deleteTenant(Database cx, TenantManagementConcurrencyWorkload* self) {
		state TenantName tenant = self->chooseTenantName();

		try {
			loop {
				try {
					Future<Void> deleteFuture = self->useMetacluster
					                                ? MetaclusterAPI::deleteTenant(self->mvDb, tenant)
					                                : ManagementAPI::deleteTenant(self->dataDb.getReference(), tenant);
					Optional<Void> result = wait(timeout(deleteFuture, 30));

					if (result.present()) {
						break;
					}
				} catch (Error& e) {
					// If we retried the deletion after our initial attempt succeeded, then we proceed with the
					// rest of the deletion steps normally. Otherwise, the deletion happened elsewhere and we
					// failed here, so we can rethrow the error.
					if (e.code() == error_code_tenant_not_found) {
						break;
					} else {
						throw;
					}
				}
			}

			return Void();
		} catch (Error& e) {
			if (e.code() != error_code_tenant_not_found) {
				TraceEvent(SevError, "DeleteTenantFailure").error(e).detail("TenantName", tenant);
			}
			return Void();
		}
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	ACTOR Future<Void> _start(Database cx, TenantManagementConcurrencyWorkload* self) {
		state double start = now();

		// Run a random sequence of tenant management operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 2);
			if (operation == 0) {
				wait(self->createTenant(cx, self));
			} else if (operation == 1) {
				wait(self->deleteTenant(cx, self));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	ACTOR Future<bool> _check(Database cx, TenantManagementConcurrencyWorkload* self) {
		state std::map<TenantName, TenantMapEntry> metaclusterMap = wait(ManagementAPI::listTenants(
		    self->mvDb, self->tenantNamePrefix, self->tenantNamePrefix.withSuffix("\xff"_sr), self->maxTenants + 1));

		state std::map<TenantName, TenantMapEntry> dataClusterMap =
		    wait(ManagementAPI::listTenants(self->dataDb.getReference(),
		                                    self->tenantNamePrefix,
		                                    self->tenantNamePrefix.withSuffix("\xff"_sr),
		                                    self->maxTenants + 1));

		auto metaclusterItr = metaclusterMap.begin();
		auto dataItr = dataClusterMap.begin();

		while (metaclusterItr != metaclusterMap.end() || dataItr != dataClusterMap.end()) {
			bool matches = metaclusterItr != metaclusterMap.end() && dataItr != dataClusterMap.end() &&
			               metaclusterItr->first == dataItr->first;

			if (matches) {
				ASSERT(metaclusterItr->second.matchesConfiguration(dataItr->second));
				++metaclusterItr;
				++dataItr;
			} else {
				ASSERT(metaclusterItr != metaclusterMap.end() &&
				       (dataItr == dataClusterMap.end() || metaclusterItr->first < dataItr->first));

				ASSERT(metaclusterItr->second.tenantState == TenantState::REGISTERING ||
				       metaclusterItr->second.tenantState == TenantState::REMOVING);

				++metaclusterItr;
			}
		}

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementConcurrencyWorkload> TenantManagementConcurrencyWorkload("TenantManagementConcurrency");
