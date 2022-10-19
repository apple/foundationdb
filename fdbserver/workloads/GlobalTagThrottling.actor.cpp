/*
 * GlobalTagThrottling.actor.cpp
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

#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

class GlobalTagThrottlingWorkload : public TestWorkload {
	TransactionTag transactionTag;
	double reservedQuota{ 0.0 };
	double totalQuota{ 0.0 };

	ACTOR static Future<Void> setup(GlobalTagThrottlingWorkload* self, Database cx) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				TraceEvent("GlobalTagThrottlingWorkload_SettingTagQuota")
				    .detail("Tag", self->transactionTag)
				    .detail("ReservedQuota", self->reservedQuota)
				    .detail("TotalQuota", self->totalQuota);
				ThrottleApi::setTagQuota(tr, self->transactionTag, self->reservedQuota, self->totalQuota);
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("GlobalTagThrottlingWorkload_SetupError").error(e);
				wait(tr->onError(e));
			}
		};
	}

public:
	static constexpr auto NAME = "GlobalTagThrottling";
	explicit GlobalTagThrottlingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		transactionTag = getOption(options, "transactionTag"_sr, "sampleTag"_sr);
		reservedQuota = getOption(options, "reservedQuota"_sr, 0.0);
		totalQuota = getOption(options, "totalQuota"_sr, 0.0);
	}

	Future<Void> setup(Database const& cx) override { return clientId ? Void() : setup(this, cx); }
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GlobalTagThrottlingWorkload> GlobalTagThrottlingWorkloadFactory;
