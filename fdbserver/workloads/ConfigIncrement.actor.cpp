/*
 * ConfigIncrement.actor.cpp
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

#include "fdbclient/ISingleThreadTransaction.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class ConfigIncrementWorkload : public TestWorkload {
	int incrementActors{ 0 };
	int incrementsPerActor{ 0 };
	Version lastKnownCommittedVersion{ ::invalidVersion };
	int lastKnownValue{ -1 };
	double meanSleepWithinTransactions{ 0.01 };
	double meanSleepBetweenTransactions{ 0.1 };

	static KeyRef const testKnobName;
	static Key configKey;

	PerfIntCounter transactions, retries, commitUnknownResult;

	static Key getConfigKey() {
		Tuple tuple;
		tuple.appendNull(); // config class
		tuple << testKnobName;
		return tuple.pack();
	}

	ACTOR static Future<int> get(Reference<ISingleThreadTransaction> tr) {
		TraceEvent(SevDebug, "ConfigIncrementGet");
		Optional<Value> serializedValue = wait(tr->get(getConfigKey()));
		if (!serializedValue.present()) {
			return 0;
		} else {
			return BinaryReader::fromStringRef<int>(serializedValue.get(), Unversioned());
		}
	}

	static void set(Reference<ISingleThreadTransaction> tr, int value) {
		TraceEvent(SevDebug, "ConfigIncrementSet").detail("Value", value);
		tr->set(getConfigKey(), format("%d", value));
	}

	ACTOR static Future<Void> incrementActor(ConfigIncrementWorkload* self, Database cx) {
		TraceEvent(SevDebug, "ConfigIncrementStartIncrementActor");
		state int trsComplete = 0;
		while (trsComplete < self->incrementsPerActor) {
			try {
				loop {
					try {
						state Reference<ISingleThreadTransaction> tr = self->getTransaction(cx);
						state int currentValue = wait(get(tr));
						ASSERT_GE(currentValue, self->lastKnownValue);
						set(tr, currentValue + 1);
						wait(delay(deterministicRandom()->random01() * 2 * self->meanSleepWithinTransactions));
						wait(tr->commit());
						ASSERT_GT(tr->getCommittedVersion(), self->lastKnownCommittedVersion);
						self->lastKnownCommittedVersion = tr->getCommittedVersion();
						self->lastKnownValue = currentValue + 1;
						TraceEvent("ConfigIncrementSucceeded")
						    .detail("CommittedVersion", self->lastKnownCommittedVersion)
						    .detail("CommittedValue", self->lastKnownValue);
						++self->transactions;
						++trsComplete;
						wait(delay(deterministicRandom()->random01() * 2 * self->meanSleepBetweenTransactions));
						break;
					} catch (Error& e) {
						TraceEvent(SevDebug, "ConfigIncrementError")
						    .errorUnsuppressed(e)
						    .detail("LastKnownValue", self->lastKnownValue);
						wait(tr->onError(e));
						++self->retries;
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_commit_unknown_result) {
					++self->commitUnknownResult;
					wait(delayJittered(0.1));
					tr->reset();
				} else {
					throw e;
				}
			}
		}
		return Void();
	}

	ACTOR static Future<bool> check(ConfigIncrementWorkload* self, Database cx) {
		state Reference<ISingleThreadTransaction> tr = self->getTransaction(cx);
		loop {
			try {
				state int currentValue = wait(get(tr));
				auto expectedValue = self->incrementActors * self->incrementsPerActor;
				TraceEvent("ConfigIncrementCheck")
				    .detail("CurrentValue", currentValue)
				    .detail("ExpectedValue", expectedValue);
				return currentValue >= expectedValue; // >= because we may have maybe_committed errors
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	Reference<ISingleThreadTransaction> getTransaction(Database cx) const {
		ASSERT(g_network->isSimulated()); // TODO: Enforce elsewhere
		ASSERT(g_simulator.configDBType != ConfigDBType::DISABLED);
		auto type = (g_simulator.configDBType == ConfigDBType::SIMPLE) ? ISingleThreadTransaction::Type::SIMPLE_CONFIG
		                                                               : ISingleThreadTransaction::Type::PAXOS_CONFIG;
		return ISingleThreadTransaction::create(type, cx);
	}

public:
	ConfigIncrementWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"),
	    commitUnknownResult("CommitUnknownResult") {
		incrementActors = getOption(options, "incrementActors"_sr, 10);
		incrementsPerActor = getOption(options, "incrementsPerActor"_sr, 10);
		meanSleepWithinTransactions = getOption(options, "meanSleepWithinTransactions"_sr, 0.01);
		meanSleepBetweenTransactions = getOption(options, "meanSleepBetweenTransactions"_sr, 0.1);
	}

	std::string description() const override { return "ConfigIncrementWorkload"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> actors;
		auto localIncrementActors =
		    (clientId < incrementActors) ? ((incrementActors - clientId - 1) / clientCount + 1) : 0;
		for (int i = 0; i < localIncrementActors; ++i) {
			actors.push_back(incrementActor(this, cx));
		}
		return waitForAll(actors);
	}

	Future<bool> check(Database const& cx) override { return clientId ? Future<bool>{ true } : check(this, cx); }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.push_back(commitUnknownResult.getMetric());
		m.emplace_back("Last Known Value", lastKnownValue, Averaged::False);
	}
};

WorkloadFactory<ConfigIncrementWorkload> ConfigIncrementWorkloadFactory("ConfigIncrement");

KeyRef const ConfigIncrementWorkload::testKnobName = "test_int"_sr;
