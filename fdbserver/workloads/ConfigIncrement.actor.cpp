/*
 * ConfigIncrement.actor.cpp
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

#include "fdbclient/ISingleThreadTransaction.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class ConfigIncrementWorkload : public TestWorkload {
	int actorsPerClient{ 0 };
	int incrementsPerActor{ 0 };
	Version lastKnownCommittedVersion{ 0 };
	bool useSimpleConfigDB{ true };

	static KeyRef const testKnobName;
	static Key configKey;

	PerfIntCounter transactions, retries;
	PerfDoubleCounter totalLatency;

	static Key getConfigKey() {
		Tuple tuple;
		tuple.appendNull(); // config class
		tuple << testKnobName;
		return tuple.pack();
	}

	ACTOR static Future<int> get(Reference<ISingleThreadTransaction> tr) {
		Optional<Value> serializedValue = wait(tr->get(getConfigKey()));
		if (!serializedValue.present()) {
			return 0;
		} else {
			return BinaryReader::fromStringRef<int>(serializedValue.get(), Unversioned());
		}
	}

	static void set(Reference<ISingleThreadTransaction> tr, int value) { tr->set(getConfigKey(), format("%d", value)); }

	ACTOR static Future<Void> incrementActor(ConfigIncrementWorkload* self, Database cx) {
		state int trsComplete = 0;
		while (trsComplete < self->incrementsPerActor) {
			try {
				state Reference<ISingleThreadTransaction> tr = self->getTransaction(cx);
				int currentValue = wait(get(tr));
				set(tr, currentValue + 1);
				wait(tr->commit());
				ASSERT_GT(tr->getCommittedVersion(), self->lastKnownCommittedVersion);
				self->lastKnownCommittedVersion = tr->getCommittedVersion();
				++self->transactions;
			} catch (Error& e) {
				// TODO: Increment error counters
				wait(tr->onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<bool> check(ConfigIncrementWorkload* self, Database cx) {
		state Reference<ISingleThreadTransaction> tr = self->getTransaction(cx);
		state int currentValue = wait(get(tr));
		return currentValue > (self->actorsPerClient * self->incrementsPerActor);
	}

	Reference<ISingleThreadTransaction> getTransaction(Database cx) const {
		auto type = useSimpleConfigDB ? ISingleThreadTransaction::Type::SIMPLE_CONFIG
		                              : ISingleThreadTransaction::Type::PAXOS_CONFIG;
		return ISingleThreadTransaction::create(type, cx);
	}

public:
	ConfigIncrementWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), totalLatency("Latency") {
		// TODO: Read test params
	}

	std::string description() const override { return "ConfigIncrementWorkload"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		std::vector<Future<Void>> actors;
		for (int i = 0; i < actorsPerClient; ++i) {
			actors.push_back(incrementActor(this, cx));
		}
		return waitForAll(actors);
	}

	Future<bool> check(Database const& cx) override { return check(this, cx); }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ConfigIncrementWorkload> ConfigIncrementWorkloadFactory("ConfigIncrement");

KeyRef const ConfigIncrementWorkload::testKnobName = "test_int"_sr;
