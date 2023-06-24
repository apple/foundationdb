/*
 * ThroughputQuota.actor.cpp
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

#include "fdbclient/TagThrottle.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// This workload sets the throughput quota of a tag during the setup phase
class ThroughputQuotaWorkload : public TestWorkload {
	bool nameRepresentsTenantGroup{ false };
	Standalone<StringRef> name;
	int64_t reservedQuota{ 0 };
	int64_t totalQuota{ 0 };

	void setTagQuota(Reference<ReadYourWritesTransaction> tr) {
		TraceEvent("ThroughputQuotaWorkload_SettingTagQuota")
		    .detail("Tag", printable(name))
		    .detail("ReservedQuota", reservedQuota)
		    .detail("TotalQuota", totalQuota);
		ThrottleApi::setTagQuota(tr, name, reservedQuota, totalQuota);
	}

	void setTenantGroupQuota(Reference<ReadYourWritesTransaction> tr) {
		TraceEvent("ThroughputQuotaWorkload_SettingTenantGruopQuota")
		    .detail("TenantGroup", printable(name))
		    .detail("ReservedQuota", reservedQuota)
		    .detail("TotalQuota", totalQuota);
		ThrottleApi::ThroughputQuotaValue throughputQuotaValue;
		throughputQuotaValue.reservedQuota = reservedQuota;
		throughputQuotaValue.totalQuota = totalQuota;
		TenantMetadata::throughputQuota().set(tr, name, throughputQuotaValue);
	}

	ACTOR static Future<Void> setup(ThroughputQuotaWorkload* self, Database cx) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				if (self->nameRepresentsTenantGroup) {
					self->setTenantGroupQuota(tr);
				} else {
					self->setTagQuota(tr);
				}
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				TraceEvent("ThroughputQuotaWorkload_SetupError").error(e);
				wait(tr->onError(e));
			}
		};
	}

public:
	static constexpr auto NAME = "ThroughputQuota";
	explicit ThroughputQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nameRepresentsTenantGroup = getOption(options, "nameRepresentsTenantGroup"_sr, false);
		name = getOption(options, "name"_sr, nameRepresentsTenantGroup ? "sampleTenantGroup"_sr : "sampleTag"_sr);
		reservedQuota = getOption(options, "reservedQuota"_sr, 0.0);
		totalQuota = getOption(options, "totalQuota"_sr, 0.0);
	}

	Future<Void> setup(Database const& cx) override {
		cx->debugUseTags = true;
		return clientId ? Void() : setup(this, cx);
	}
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ThroughputQuotaWorkload> ThroughputQuotaWorkloadFactory;
