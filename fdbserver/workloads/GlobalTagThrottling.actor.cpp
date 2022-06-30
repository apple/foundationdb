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
	double reservedReadQuota{ 0.0 };
	double totalReadQuota{ 0.0 };
	double reservedWriteQuota{ 0.0 };
	double totalWriteQuota{ 0.0 };

	ACTOR static Future<Void> setup(GlobalTagThrottlingWorkload* self, Database cx) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				TraceEvent("GlobalTagThrottlingWorkload_SettingTagQuota")
				    .detail("Tag", self->transactionTag)
				    .detail("ReservedReadQuota", self->reservedReadQuota)
				    .detail("TotalReadQuota", self->totalReadQuota)
				    .detail("ReservedWriteQuota", self->reservedWriteQuota)
				    .detail("TotalWriteQuota", self->totalWriteQuota);
				ThrottleApi::setTagQuota(tr,
				                         self->transactionTag,
				                         self->reservedReadQuota,
				                         self->totalReadQuota,
				                         self->reservedWriteQuota,
				                         self->totalWriteQuota);
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		};
	}

public:
	explicit GlobalTagThrottlingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		transactionTag = getOption(options, "transactionTag"_sr, "sampleTag"_sr);
		reservedReadQuota = getOption(options, "reservedReadQuota"_sr, 0.0);
		totalReadQuota = getOption(options, "totalReadQuota"_sr, 0.0);
		reservedWriteQuota = getOption(options, "reservedWriteQuota"_sr, 0.0);
		totalWriteQuota = getOption(options, "totalWriteQuota"_sr, 0.0);
	}

	std::string description() const override { return "GlobalTagThrottling"; }
	Future<Void> setup(Database const& cx) override { return clientId ? Void() : setup(this, cx); }
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GlobalTagThrottlingWorkload> GlobalTagThrottlingWorkloadFactory("GlobalTagThrottling");
