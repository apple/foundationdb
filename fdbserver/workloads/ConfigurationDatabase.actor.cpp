/*
 * ConfigurationDatabase.actor.cpp
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

#include "fdbclient/IConfigTransaction.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

class ConfigurationDatabaseWorkload : public TestWorkload {

	ACTOR static Future<Optional<Value>> get(Database cx, Key key) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				Optional<Value> result = wait(tr.get(key));
				return result;
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> set(Database cx, Key key, Value value) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				tr.set(key, value);
				wait(tr.commit());
				return Void();
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> start(ConfigurationDatabaseWorkload* self, Database cx) {
		state Key key = LiteralStringRef("key");
		state Key value = LiteralStringRef("value");
		Optional<Value> currentValue = wait(get(cx, key));
		ASSERT(!currentValue.present());
		wait(set(cx, key, value));
		{
			Optional<Value> currentValue = wait(get(cx, key));
			ASSERT(currentValue.get() == value);
		}
		return Void();
	}

public:
	ConfigurationDatabaseWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	std::string description() const override { return "ConfigurationDatabase"; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ConfigurationDatabaseWorkload> ConfigurationDatabaseWorkloadFactory("ConfigurationDatabase");
