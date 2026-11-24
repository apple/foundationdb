/*
 * RangeReplication.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DataDistributionConfig.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbserver/tester/workloads.h"

namespace {
struct RangeReplicationWorkload : TestWorkload {
	static constexpr auto NAME = "RangeReplication";
	using TestWorkload::TestWorkload;

	Future<Void> start(Database const& db) override {
		if (clientId != 0) {
			co_return;
		}
		Database cx = db;
		KeyRange range = KeyRangeRef("r0"_sr, "r4"_sr);
		KeyRange middle = KeyRangeRef("r1"_sr, "r3"_sr);
		Database old = cx->clone();
		old->apiVersion = ApiVersion(899);
		ASSERT((co_await errorOr(setRangeReplication(old, range, 4))).isError(error_code_unsupported_operation));
		ASSERT((co_await errorOr(getRangeReplicationOverrides(old))).isError(error_code_unsupported_operation));
		ASSERT((co_await errorOr(clearRangeReplication(old, range))).isError(error_code_unsupported_operation));
		ASSERT((co_await errorOr(setRangeReplication(cx, range, 0))).isError(error_code_invalid_option_value));

		auto sys = SystemDBWriteLockedNow(cx.getReference());
		auto config = DDConfiguration().userRangeConfig();
		co_await config.updateRange(sys, middle.begin, middle.end, DDRangeConfig({}, 7), true);
		co_await setRangeReplication(cx, range, 4);

		auto all = co_await getRangeReplicationOverrides(cx);
		ASSERT(std::count(all.begin(), all.end(), RangeReplicationOverride{ range, 4 }) == 1);
		KeyRange filter = KeyRangeRef("r11"_sr, "r35"_sr);
		auto filtered = co_await getRangeReplicationOverrides(cx, filter);
		ASSERT(filtered.size() == 1 && filtered[0] == (RangeReplicationOverride{ filter, 4 }));

		auto configVersion = [&] {
			return runTransaction(sys, [](auto tr) { return DDConfiguration().trigger.get(tr); });
		};
		auto before = co_await configVersion();
		co_await setRangeReplication(cx, range, 4);
		ASSERT((co_await configVersion()) == before);

		KeyRange cut = KeyRangeRef("r2"_sr, "r25"_sr);
		co_await clearRangeReplication(cx, cut);
		before = co_await configVersion();
		co_await clearRangeReplication(cx, cut);
		ASSERT((co_await configVersion()) == before);

		std::vector<RangeReplicationOverride> expected{ { KeyRangeRef("r0"_sr, "r2"_sr), 4 },
			                                            { KeyRangeRef("r25"_sr, "r4"_sr), 4 } };
		ASSERT((co_await getRangeReplicationOverrides(cx, range)) == expected);
		auto snapshot = co_await config.getSnapshot(sys, range.begin, range.end);
		ASSERT(snapshot->rangeContaining("r1"_sr).value() == DDRangeConfig(4, 7));
		ASSERT(snapshot->rangeContaining("r2"_sr).value() == DDRangeConfig({}, 7));
		co_await clearRangeReplication(cx, range);
		ASSERT((co_await getRangeReplicationOverrides(cx, range)).empty());
	}

	Future<bool> check(Database const&) override { return true; }
	void getMetrics(std::vector<PerfMetric>&) override {}
};

WorkloadFactory<RangeReplicationWorkload> RangeReplicationWorkloadFactory;
} // namespace
