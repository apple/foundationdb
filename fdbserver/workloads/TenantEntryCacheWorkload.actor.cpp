/*
 * TenantEntryCacheWorkload.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TenantEntryCache.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
TenantEntryCachePayload<int64_t> createPayload(const TenantName& name,
                                               const TenantMapEntry& entry,
                                               const TenantEntryCacheGen gen) {
	TenantEntryCachePayload<int64_t> payload;
	payload.name = name;
	payload.entry = entry;
	payload.gen = gen;
	payload.payload = entry.id;

	return payload;
}
} // namespace

struct TenantEntryCacheWorkload : TestWorkload {
	TenantEntryCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	~TenantEntryCacheWorkload() {}

	ACTOR static Future<Void> testTenantNotFound(Database cx) {
		state TenantEntryCache<int64_t> cache(cx, createPayload);
		TraceEvent("TenantNotFound_Start").log();

		wait(cache.init());

		TraceEvent("TenantNotFound_InitDone").log();

		try {
			TenantEntryCachePayload<int64_t> value = wait(cache.getById(1));
		} catch (Error& e) {
			ASSERT(e.code() == error_code_key_not_found);
		}

		TraceEvent("TenantNotFound_End").log();
		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(cx); }
	ACTOR Future<Void> _start(Database cx) {
		wait(testTenantNotFound(cx));
		return Void();
	}

	std::string description() const override { return "TenantEntryCacheWorkload"; }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantEntryCacheWorkload> TenantEntryCacheWorkloadFactory("TenantEntryCache");