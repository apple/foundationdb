/*
 * StorageQuota.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/TenantName.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct StorageQuotaWorkload : TestWorkload {
	static constexpr auto NAME = "StorageQuota";
	TenantGroupName group;
	TenantName tenantName;
	Reference<Tenant> tenant;
	int nodeCount;
	TenantName emptyTenantName;
	Reference<Tenant> emptyTenant;

	StorageQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		group = getOption(options, "group"_sr, "DefaultGroup"_sr);
		tenantName = getOption(options, "tenant"_sr, "DefaultTenant"_sr);
		nodeCount = getOption(options, "nodeCount"_sr, 10000);
		emptyTenantName = getOption(options, "emptyTenant"_sr, "DefaultTenant"_sr);
	}

	Future<Void> setup(Database const& cx) override {
		tenant = makeReference<Tenant>(cx, tenantName);
		emptyTenant = makeReference<Tenant>(cx, emptyTenantName);

		// Use default values for arguments between (and including) postSetupWarming and endNodeIdx params.
		return bulkSetup(cx,
		                 this,
		                 nodeCount,
		                 Promise<double>(),
		                 true,
		                 0.0,
		                 1e12,
		                 std::vector<uint64_t>(),
		                 Promise<std::vector<std::pair<uint64_t, double>>>(),
		                 0,
		                 0.1,
		                 0,
		                 0,
		                 { tenant });
	}

	Future<Void> start(Database const& cx) override { return (clientId == 0) ? _start(this, cx) : Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Functions required by `bulkSetup()`
	Key keyForIndex(int n) { return doubleToTestKey((double)n / nodeCount); }
	Value value(int n) { return doubleToTestKey(n); }
	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(keyForIndex(n), value((n + 1) % nodeCount)); }

	ACTOR Future<Void> _start(StorageQuotaWorkload* self, Database cx) {
		state TenantMapEntry entry1 = wait(TenantAPI::getTenant(cx.getReference(), self->tenantName));
		state TenantMapEntry entry2 = wait(TenantAPI::getTenant(cx.getReference(), self->emptyTenantName));
		ASSERT(entry1.tenantGroup.present() && entry1.tenantGroup.get() == self->group &&
		       entry2.tenantGroup.present() && entry2.tenantGroup.get() == self->group);

		// Get the size of the non-empty tenant. We will set the quota of the tenant group
		// to just below the current size of this tenant.
		state int64_t size = wait(getSize(cx, self->tenant));
		state int64_t quota = size - 1;

		// Check that the quota set/get functions work as expected.
		wait(setStorageQuota(cx, self->group, quota));
		state Optional<int64_t> quotaRead = wait(getStorageQuota(cx, self->group));
		ASSERT(quotaRead.present() && quotaRead.get() == quota);

		if (!SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
			return Void();
		}

		// Check that writes to both the tenants are rejected when the group is over quota.
		state bool rejected1 = wait(tryWrite(self, cx, self->tenant, /*bypassQuota=*/false, /*expectOk=*/false));
		ASSERT(rejected1);
		state bool rejected2 = wait(tryWrite(self, cx, self->emptyTenant, /*bypassQuota=*/false, /*expectOk=*/false));
		ASSERT(rejected2);

		// Check that transaction is able to commit if we use the FDBTransactionOptions to bypass quota.
		state bool bypassed = wait(tryWrite(self, cx, self->tenant, /*bypassQuota=*/true, /*expectOk=*/true));
		ASSERT(bypassed);

		// Increase the quota or clear the quota. Check that writes to both the tenants are now able to commit.
		if (deterministicRandom()->coinflip()) {
			quota = size * 2;
			wait(setStorageQuota(cx, self->group, quota));
		} else {
			wait(clearStorageQuota(cx, self->group));
		}
		state bool committed1 = wait(tryWrite(self, cx, self->tenant, /*bypassQuota=*/false, /*expectOk=*/true));
		ASSERT(committed1);
		state bool committed2 = wait(tryWrite(self, cx, self->emptyTenant, /*bypassQuota=*/false, /*expectOk=*/true));
		ASSERT(committed2);

		return Void();
	}

	ACTOR static Future<int64_t> getSize(Database cx, Reference<Tenant> tenant) {
		state ReadYourWritesTransaction tr(cx, tenant);
		state double totalDelay = 0.0;
		state int64_t previousSize = -1;

		loop {
			try {
				state int64_t size = wait(tr.getEstimatedRangeSizeBytes(normalKeys));
				// Wait until the estimated size stabilizes
				if (size > previousSize && totalDelay < 50.0) {
					totalDelay += 5.0;
					wait(delay(5.0));
				} else {
					TraceEvent(SevDebug, "GetSizeResult").detail("Tenant", tenant).detail("Size", size);
					return size;
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "GetSizeError").errorUnsuppressed(e).detail("Tenant", tenant);
				wait(tr.onError(e));
			}
		}
	}

	static Future<Void> setStorageQuota(Database cx, TenantGroupName tenantGroupName, int64_t quota) {
		return runRYWTransactionVoid(cx,
		                             [tenantGroupName = tenantGroupName,
		                              quota = quota](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			                             tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			                             TenantMetadata::storageQuota().set(tr, tenantGroupName, quota);
			                             return Void();
		                             });
	}

	static Future<Void> clearStorageQuota(Database cx, TenantGroupName tenantGroupName) {
		return runRYWTransactionVoid(
		    cx, [tenantGroupName = tenantGroupName](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			    TenantMetadata::storageQuota().erase(tr, tenantGroupName);
			    return Void();
		    });
	}

	static Future<Optional<int64_t>> getStorageQuota(Database cx, TenantGroupName tenantGroupName) {
		return runRYWTransaction(cx, [tenantGroupName = tenantGroupName](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return TenantMetadata::storageQuota().get(tr, tenantGroupName);
		});
	}

	ACTOR static Future<bool> tryWrite(StorageQuotaWorkload* self,
	                                   Database cx,
	                                   Reference<Tenant> tenant,
	                                   bool bypassQuota,
	                                   bool expectOk) {
		state int i;
		// Retry the transaction a few times if needed; this allows us wait for a while for all
		// the storage usage and quota related monitors to fetch and propagate the latest information
		// about the tenants that are over storage quota.
		for (i = 0; i < 10; i++) {
			state Transaction tr(cx, tenant);
			if (bypassQuota) {
				tr.setOption(FDBTransactionOptions::BYPASS_STORAGE_QUOTA);
			}
			loop {
				try {
					Standalone<KeyValueRef> kv =
					    (*self)(deterministicRandom()->randomInt(0, std::numeric_limits<int>::max()));
					tr.set(kv.key, kv.value);
					wait(tr.commit());
					if (expectOk) {
						return true;
					}
					break;
				} catch (Error& e) {
					if (e.code() == error_code_storage_quota_exceeded) {
						if (!expectOk) {
							return true;
						}
						break;
					} else {
						wait(tr.onError(e));
					}
				}
			}
			wait(delay(5.0));
		}
		return false;
	}
};

WorkloadFactory<StorageQuotaWorkload> StorageQuotaWorkloadFactory;
