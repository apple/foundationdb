/*
 * LeakTLogInterface.actor.cpp
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

#include <cstdint>

#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct LeakTLogInterfaceWorkload : TestWorkload {
	TenantName tenant;
	Standalone<StringRef> fieldName;

	LeakTLogInterfaceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		tenant = getOption(options, "tenant"_sr, "DefaultTenant"_sr);
		fieldName = getOption(options, "key"_sr, "TLogInterface"_sr);
	}

	std::string description() const override { return "LeakTLogInterface"; }
	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return checkKeyValueExists(this, cx); }
	virtual void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<bool> checkKeyValueExists(LeakTLogInterfaceWorkload* self, Database db) { return true; }

	ACTOR static Future<Void> _setup(LeakTLogInterfaceWorkload* self, Database db) {
		state Transaction tr(db, self->tenant);
		loop {
			ObjectWriter writer(IncludeVersion());
			writer.serialize(self->dbInfo->get().logSystemConfig);
			state Standalone<StringRef> logSystemString = writer.toString();
			try {
				tr.set(self->fieldName, logSystemString);
				wait(tr.commit());
				TraceEvent("LeakTLogInterface").detail("BytesWritten", logSystemString.size()).log();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}
};

WorkloadFactory<LeakTLogInterfaceWorkload> LeakTLogInterfaceWorkload("LeakTLogInterface");
