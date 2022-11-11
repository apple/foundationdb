/*
 * StorageQuota.actor.cpp
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

#include "fdbrpc/TenantName.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/TenantName.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct StorageQuotaWorkload : TestWorkload {
	static constexpr auto NAME = "StorageQuota";
	TenantName tenant;
	int nodeCount;

	StorageQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nodeCount = getOption(options, "nodeCount"_sr, 10000);
		tenant = getOption(options, "tenant"_sr, "DefaultTenant"_sr);
	}

	Future<Void> setup(Database const& cx) override {
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
		// Check that the quota set/get functions work as expected.
		// Set the quota to just below the current size.
		state int64_t size = wait(getSize(cx, self->tenant));
		state int64_t quota = size - 1;
		wait(setStorageQuotaHelper(cx, self->tenant, quota));
		state Optional<int64_t> quotaRead = wait(getStorageQuotaHelper(cx, self->tenant));
		ASSERT(quotaRead.present() && quotaRead.get() == quota);

		if (!SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
			return Void();
		}

		// Check that writes are rejected when the tenant is over quota.
		state bool rejected = wait(tryWrite(self, cx, /*expectOk=*/false));
		ASSERT(rejected);

		// Increase the quota. Check that writes are now able to commit.
		quota = size * 2;
		wait(setStorageQuotaHelper(cx, self->tenant, quota));
		state bool committed = wait(tryWrite(self, cx, /*expectOk=*/true));
		ASSERT(committed);

		return Void();
	}

	ACTOR static Future<int64_t> getSize(Database cx, TenantName tenantName) {
		state ReadYourWritesTransaction tr(cx, tenantName);
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
					TraceEvent(SevDebug, "GetSizeResult").detail("Tenant", tr.getTenant().get()).detail("Size", size);
					return size;
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "GetSizeError").errorUnsuppressed(e).detail("Tenant", tr.getTenant().get());
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> setStorageQuotaHelper(Database cx, TenantName tenantName, int64_t quota) {
		state Transaction tr(cx);
		loop {
			try {
				setStorageQuota(tr, tenantName, quota);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Optional<int64_t>> getStorageQuotaHelper(Database cx, TenantName tenantName) {
		state Transaction tr(cx);
		loop {
			try {
				state Optional<int64_t> quota = wait(getStorageQuota(&tr, tenantName));
				wait(tr.commit());
				return quota;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<bool> tryWrite(StorageQuotaWorkload* self, Database cx, bool expectOk) {
		state int i;
		// Retry the transaction a few times if needed; this allows us wait for a while for all
		// the storage usage and quota related monitors to fetch and propagate the latest information
		// about the tenants that are over storage quota.
		for (i = 0; i < 10; i++) {
			state Transaction tr(cx, self->tenant);
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
