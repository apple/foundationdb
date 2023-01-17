/*
 * GetEstimatedRangeSize.actor.cpp
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

#include <cstring>

#include "fdbrpc/TenantName.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct GetEstimatedRangeSizeWorkload : TestWorkload {
	static constexpr auto NAME = "GetEstimatedRangeSize";
	int nodeCount;
	double testDuration;
	Key keyPrefix;
	bool hasTenant;
	Optional<TenantName> tenant;
	bool checkOnly;

	GetEstimatedRangeSizeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		nodeCount = getOption(options, "nodeCount"_sr, 10000);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		hasTenant = hasOption(options, "tenant"_sr);
		tenant = hasTenant ? getOption(options, "tenant"_sr, "DefaultNeverUsed"_sr) : Optional<TenantName>();
		checkOnly = getOption(options, "checkOnly"_sr, false);
	}

	Future<Void> setup(Database const& cx) override {
		if (checkOnly) {
			return Void();
		}
		// The following call to bulkSetup() assumes that we have a valid tenant.
		ASSERT(hasTenant);
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
		                 { tenant.get() });
	}

	Future<Void> start(Database const& cx) override {
		if (clientId > 0) {
			return Void();
		}
		return checkSize(this, cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }
	int fromValue(const ValueRef& v) { return testKeyToDouble(v, keyPrefix); }
	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }

	ACTOR static Future<Void> checkSize(GetEstimatedRangeSizeWorkload* self, Database cx) {
		state int64_t size = wait(getSize(self, cx));
		ASSERT(sizeIsAsExpected(self, size));
		return Void();
	}

	static bool sizeIsAsExpected(GetEstimatedRangeSizeWorkload* self, int64_t size) {
		int nodeSize = self->key(0).size() + self->value(0).size();
		// We use a wide range to avoid flakiness because the underlying function
		// is making an estimation.
		return size > self->nodeCount * nodeSize / 2 && size < self->nodeCount * nodeSize * 5;
	}

	ACTOR static Future<int64_t> getSize(GetEstimatedRangeSizeWorkload* self, Database cx) {
		state ReadYourWritesTransaction tr(cx, self->tenant);
		state double totalDelay = 0.0;
		TraceEvent(SevDebug, "GetSizeStart").detail("Tenant", tr.getTenant());

		loop {
			try {
				state int64_t size = wait(tr.getEstimatedRangeSizeBytes(normalKeys));
				TraceEvent(SevDebug, "GetSizeResult").detail("Tenant", tr.getTenant()).detail("Size", size);
				if (!sizeIsAsExpected(self, size) && totalDelay < 300.0) {
					totalDelay += 5.0;
					wait(delay(5.0));
				} else {
					return size;
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "GetSizeError").errorUnsuppressed(e).detail("Tenant", tr.getTenant());
				wait(tr.onError(e));
			}
		}
	}
};

WorkloadFactory<GetEstimatedRangeSizeWorkload> GetEstimatedRangeSizeWorkloadFactory;
