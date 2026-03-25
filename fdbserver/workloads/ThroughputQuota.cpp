/*
 * ThroughputQuota.cpp
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

#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/tester/workloads.actor.h"

// This workload sets the throughput quota of a tag during the setup phase
class ThroughputQuotaWorkload : public TestWorkload {
	TransactionTag transactionTag;
	int64_t reservedQuotaInPages{ 0 };
	int64_t totalQuotaInPages{ 0 };

	int64_t getReservedQuota() const { return reservedQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	int64_t getTotalQuota() const { return totalQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	Future<Void> setupImpl(Database cx) {
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				TraceEvent("ThroughputQuotaWorkload_SettingTagQuota")
				    .detail("Tag", printable(transactionTag))
				    .detail("ReservedQuota", getReservedQuota())
				    .detail("TotalQuota", getTotalQuota());
				ThrottleApi::setTagQuota(tr, transactionTag, getReservedQuota(), getTotalQuota());
				co_await tr->commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("ThroughputQuotaWorkload_SetupError").error(err);
			co_await tr->onError(err);
		}
	}

public:
	static constexpr auto NAME = "ThroughputQuota";
	explicit ThroughputQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		transactionTag = getOption(options, "transactionTag"_sr, "sampleTag"_sr);
		reservedQuotaInPages = getOption(options, "reservedQuotaInPages"_sr, 0);
		totalQuotaInPages = getOption(options, "totalQuotaInPages"_sr, 0);
	}

	Future<Void> setup(Database const& cx) override { return clientId ? Void() : setupImpl(cx); }
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ThroughputQuotaWorkload> ThroughputQuotaWorkloadFactory;
