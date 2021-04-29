/*
 * Cycle.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/IConfigTransaction.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include

const Key testConfigClass = "testConfigClass"_sr;
const Key testKnobName = "test"_sr;
const Key testKnobValue = "5"_sr;
const Key testUpdateDescription = "test knob update"_sr;

class DynamicKnobsWorkload : public TestWorkload {

	ACTOR static Future<Void> start(DynamicKnobsWorkload* self, Database cx) {
		state Arena arena;
		state Reference<IConfigTransaction> tr =
		    makeReference<SimpleConfigTransaction>(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				ConfigUpdateKey key = ConfigUpdateKeyRef(arena, testConfigClass, testKnobName);
				ConfigUpdateValue value = ConfigUpdateValueRef(arena, testUpdateDescription, testKnobValue, now());
				tr->set(BinaryWriter::toValue(key, IncludeVersion()), BinaryWriter::toValue(value, IncludeVersion()));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

public:
	DynamicKnobsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	std::string description() const override { return "DynamicKnobs"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DynamicKnobsWorkload> DynamicKnobsWorkloadFactory("DynamicKnobs");
