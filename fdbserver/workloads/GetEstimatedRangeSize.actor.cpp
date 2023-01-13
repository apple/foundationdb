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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct GetEstimatedRangeSizeWorkload : TestWorkload {
	static constexpr auto NAME = "GetEstimatedRangeSize";
	int nodeCount;
	double testDuration;
	Key keyPrefix;
	bool hasTenant;
	TenantName tenant;

	GetEstimatedRangeSizeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		nodeCount = getOption(options, "nodeCount"_sr, 10000.0);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		hasTenant = hasOption(options, "tenant"_sr);
		tenant = getOption(options, "tenant"_sr, "DefaultTenant"_sr);
	}

	std::string description() const override { return "GetEstimatedRangeSizeWorkload"; }

	Future<Void> setup(Database const& cx) override {
		if (!hasTenant) {
			return Void();
		}
		// Use default values for arguments between (and including) postSetupWarming and endNodeIdx params
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
		ASSERT_GT(size, 0);
		return Void();
	}

	ACTOR static Future<int64_t> getSize(GetEstimatedRangeSizeWorkload* self, Database cx) {
		state Optional<TenantName> tenant = self->hasTenant ? self->tenant : Optional<TenantName>();
		state ReadYourWritesTransaction tr(cx, tenant);
		TraceEvent(SevDebug, "GetSize1").detail("Tenant", tr.getTenant().present() ? tr.getTenant().get() : "none"_sr);

		loop {
			try {
				state int64_t size = wait(tr.getEstimatedRangeSizeBytes(normalKeys));
				TraceEvent(SevDebug, "GetSize2")
				    .detail("Tenant", tr.getTenant().present() ? tr.getTenant().get() : "none"_sr)
				    .detail("Size", size);
				tr.reset();
				return size;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
};

WorkloadFactory<GetEstimatedRangeSizeWorkload> GetEstimatedRangeSizeWorkloadFactory;
