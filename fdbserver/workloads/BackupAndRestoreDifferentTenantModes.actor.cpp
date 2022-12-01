/*
 * BackupAndRestoreDifferentTenantModes.actor.cpp
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

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/MemoryKeyValueStore.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <unordered_map>
#include <utility>

struct BackupAndRestoreDifferentTenantModes : TestWorkload {
	static constexpr auto NAME = "BackupAndRestoreDifferentTenantModes";

	bool doBackup;
	MemoryKeyValueStore memoryKVStore;

	BackupAndRestoreDifferentTenantModes(WorkloadContext const& wcx) : TestWorkload(wcx) {
		doBackup = getOption(options, "doBackup"_sr, true);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> _setup(BackupAndRestoreDifferentTenantModes* workload, Database cx) {
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		ASSERT(config.tenantMode == TenantMode::OPTIONAL_TENANT);
		state TenantName name = "BackupAndRestoreDifferentTenantModes_Tenant"_sr;
		wait(success(TenantAPI::createTenant(cx.getReference(), name)));

		state int i;
		// Insert a 1000 KV pairs into the db (either no tenant or use the tenant created above)
		for (i = 0; i < 1000; i++) {
			state Key k = StringRef(deterministicRandom()->randomUniqueID().toString());
			state Value v = StringRef(deterministicRandom()->randomAlphaNumeric(10));
			Optional<TenantName> tenant;
			if (deterministicRandom()->coinflip()) {
				tenant = name;
			}
			state ReadYourWritesTransaction tr = ReadYourWritesTransaction(cx, tenant);
			loop {
				try {
					tr.set(k, v);
					wait(tr.commit());
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			workload->memoryKVStore.set(k, v);
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override { return Void(); }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(this, cx);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
};

WorkloadFactory<BackupAndRestoreDifferentTenantModes> BulkSetupWorkloadFactory;
