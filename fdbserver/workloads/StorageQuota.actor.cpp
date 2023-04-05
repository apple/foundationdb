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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct StorageQuotaWorkload : TestWorkload {
	static constexpr auto NAME = "StorageQuota";
	StorageQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return (clientId == 0) ? _start(cx) : Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> _start(Database cx) {
		wait(setStorageQuotaHelper(cx, "name1"_sr, 100));
		wait(setStorageQuotaHelper(cx, "name2"_sr, 200));
		wait(setStorageQuotaHelper(cx, "name1"_sr, 300));

		state Optional<uint64_t> quota1 = wait(getStorageQuotaHelper(cx, "name1"_sr));
		ASSERT(quota1.present() && quota1.get() == 300);
		state Optional<uint64_t> quota2 = wait(getStorageQuotaHelper(cx, "name2"_sr));
		ASSERT(quota2.present() && quota2.get() == 200);
		state Optional<uint64_t> quota3 = wait(getStorageQuotaHelper(cx, "name3"_sr));
		ASSERT(!quota3.present());

		return Void();
	}

	ACTOR static Future<Void> setStorageQuotaHelper(Database cx, StringRef tenantName, uint64_t quota) {
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

	ACTOR static Future<Optional<uint64_t>> getStorageQuotaHelper(Database cx, StringRef tenantName) {
		state Transaction tr(cx);
		loop {
			try {
				state Optional<uint64_t> quota = wait(getStorageQuota(&tr, tenantName));
				wait(tr.commit());
				return quota;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
};

WorkloadFactory<StorageQuotaWorkload> StorageQuotaWorkloadFactory;
