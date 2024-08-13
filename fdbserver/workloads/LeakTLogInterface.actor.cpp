/*
 * LeakTLogInterface.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <cstdint>

#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct LeakTLogInterfaceWorkload : TestWorkload {
	static constexpr auto NAME = "LeakTLogInterface";
	TenantName tenantName;
	Reference<Tenant> tenant;
	Standalone<StringRef> fieldName;
	double testDuration;

	LeakTLogInterfaceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		tenantName = getOption(options, "tenant"_sr, "DefaultTenant"_sr);
		fieldName = getOption(options, "key"_sr, "TLogInterface"_sr);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override {
		tenant = makeReference<Tenant>(cx, tenantName);
		return persistSerializedTLogInterface(this, cx);
	}

	Future<Void> start(Database const& cx) override { return timeout(updateLoop(this, cx), testDuration, Void()); }
	Future<bool> check(Database const& cx) override { return true; }
	virtual void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> persistSerializedTLogInterface(LeakTLogInterfaceWorkload* self, Database cx) {
		state Transaction tr(cx, self->tenant);
		loop {
			ObjectWriter writer(IncludeVersion());
			writer.serialize(self->dbInfo->get().logSystemConfig);
			state Standalone<StringRef> logSystemString = writer.toString();
			try {
				tr.set(self->fieldName, logSystemString);
				wait(tr.commit());
				TraceEvent("LeakTLogInterface").detail("BytesWritten", logSystemString.size()).log();
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> updateLoop(LeakTLogInterfaceWorkload* self, Database cx) {
		loop {
			wait(self->dbInfo->onChange());
			wait(persistSerializedTLogInterface(self, cx));
		}
	}
};

WorkloadFactory<LeakTLogInterfaceWorkload> LeakTLogInterfaceWorkload;
